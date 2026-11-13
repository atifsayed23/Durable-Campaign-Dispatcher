from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any, Optional

from .clock import IST, format_ist, parse_ist
from .db import Database
from .rules import (
    compute_next_lead_transition,
    is_within_calling_window,
)


def claim_next_calls(
    db: Database,
    sim_now: datetime,
    close_buffer_sec: int = 1800,
) -> list[dict[str, Any]]:
    """
    Atomically claim the next eligible calls obeying all business rules:
    - R1: Calling window (only from campaigns currently within their calling window)
    - R2: Tenant concurrency limit (shared across tenant campaigns)
    - R3: One active call per phone (across all campaigns)
    - R6: Priority ordering (due callbacks > retries > fresh leads)
    - R8: Global DND suppression
    """
    sim_now_ist = sim_now.astimezone(IST)
    sim_now_str = sim_now_ist.isoformat()

    with db.atomic():
        # 0. Self-healing: Revert any lead stuck in DIALING that has no active attempt in call_attempts
        db.execute(
            """
            UPDATE leads
            SET state = CASE
                WHEN phone IN (SELECT phone FROM global_dnd) THEN 'DND_BLOCKED'
                ELSE 'WAITING'
            END
            WHERE state = 'DIALING'
              AND (campaign_id, lead_id) NOT IN (
                  SELECT campaign_id, lead_id
                  FROM call_attempts
                  WHERE state IN ('INTENT', 'ACTIVE', 'UNKNOWN_504')
              )
            """
        )

        # 1. Identify which campaigns are currently inside their calling window
        all_campaigns = db.fetchall("SELECT * FROM campaigns")
        open_campaign_ids: list[str] = []
        for camp in all_campaigns:
            if is_within_calling_window(
                sim_now=sim_now_ist,
                days=camp["window_days"],
                window_start=camp["window_start"],
                window_end=camp["window_end"],
                close_buffer_sec=close_buffer_sec,
            ):
                open_campaign_ids.append(camp["campaign_id"])

        if not open_campaign_ids:
            return []

        # 2. Count active calls per tenant (INTENT, ACTIVE, UNKNOWN_504 all count toward concurrency)
        active_tenant_counts: dict[str, int] = {}
        counts = db.fetchall(
            """
            SELECT tenant_id, COUNT(*) as cnt
            FROM call_attempts
            WHERE state IN ('INTENT', 'ACTIVE', 'UNKNOWN_504')
            GROUP BY tenant_id
            """
        )
        for row in counts:
            active_tenant_counts[row["tenant_id"]] = row["cnt"]

        tenant_limits: dict[str, int] = {}
        for t in db.fetchall("SELECT tenant_id, max_concurrent_calls FROM tenants"):
            tenant_limits[t["tenant_id"]] = t["max_concurrent_calls"]

        # 3. Active phones and Global DND phones
        active_phones = {
            r["phone"]
            for r in db.fetchall(
                """
                SELECT DISTINCT phone FROM call_attempts
                WHERE state IN ('INTENT', 'ACTIVE', 'UNKNOWN_504')
                """
            )
        }
        dnd_phones = {
            r["phone"]
            for r in db.fetchall("SELECT phone FROM global_dnd")
        }

        # 4. Immediate DND cleanup for any waiting leads whose phone is in global_dnd
        if dnd_phones:
            db.execute(
                """
                UPDATE leads
                SET state = 'DND_BLOCKED'
                WHERE state = 'WAITING'
                  AND phone IN (SELECT phone FROM global_dnd)
                """
            )

        # 5. Query candidate waiting leads from open campaigns
        placeholders = ",".join("?" for _ in open_campaign_ids)
        candidates = db.fetchall(
            f"""
            SELECT * FROM leads
            WHERE state = 'WAITING'
              AND campaign_id IN ({placeholders})
              AND next_eligible_at <= ?
            ORDER BY priority_tier ASC, next_eligible_at ASC, campaign_id ASC, lead_id ASC
            """,
            (*open_campaign_ids, sim_now_str),
        )

        claimed: list[dict[str, Any]] = []

        for lead in candidates:
            tid = lead["tenant_id"]
            phone = lead["phone"]
            max_conc = tenant_limits.get(tid, 0)
            cur_conc = active_tenant_counts.get(tid, 0)

            # R2: Check tenant concurrency
            if cur_conc >= max_conc:
                continue

            # R3: Check phone uniqueness across active calls
            if phone in active_phones:
                continue

            # R8: Check global DND
            if phone in dnd_phones:
                db.execute(
                    "UPDATE leads SET state = 'DND_BLOCKED' WHERE campaign_id = ? AND lead_id = ?",
                    (lead["campaign_id"], lead["lead_id"]),
                )
                continue

            # Claim the call: Write-Ahead Intent
            attempt_no = lead["attempts"] + 1
            request_id = f"req_{lead['campaign_id']}_{lead['lead_id']}_{attempt_no}_{uuid.uuid4().hex[:8]}"

            # Lock the lead state to DIALING
            db.execute(
                "UPDATE leads SET state = 'DIALING' WHERE campaign_id = ? AND lead_id = ?",
                (lead["campaign_id"], lead["lead_id"]),
            )

            # Insert Write-Ahead Intent ledger row
            db.execute(
                """
                INSERT INTO call_attempts (
                    request_id, campaign_id, lead_id, tenant_id, phone,
                    state, attempt_no, counted_attempt,
                    created_at_real, created_at_sim
                ) VALUES (?, ?, ?, ?, ?, 'INTENT', ?, 0, ?, ?)
                """,
                (
                    request_id,
                    lead["campaign_id"],
                    lead["lead_id"],
                    tid,
                    phone,
                    attempt_no,
                    time.time(),
                    sim_now_str,
                ),
            )

            # Update in-memory tracking for this batch
            active_tenant_counts[tid] = cur_conc + 1
            active_phones.add(phone)

            claimed.append(
                {
                    "request_id": request_id,
                    "campaign_id": lead["campaign_id"],
                    "lead_id": lead["lead_id"],
                    "tenant_id": tid,
                    "phone": phone,
                    "attempt_no": attempt_no,
                }
            )

        return claimed


def mark_call_accepted(db: Database, request_id: str, call_sid: str) -> bool:
    """
    Called when provider returns 201/200 for POST /v1/calls.
    Includes early-webhook guard: if webhook arrived before this HTTP response,
    the attempt is already ENDED; preserve ENDED state and ensure call_sid is saved.
    """
    with db.atomic():
        attempt = db.fetchone(
            "SELECT * FROM call_attempts WHERE request_id = ?", (request_id,)
        )
        if not attempt:
            return False

        cid = attempt["campaign_id"]
        lid = attempt["lead_id"]

        # Race Condition Guard: If early webhook already ended the call
        if attempt["state"] == "ENDED":
            if attempt["counted_attempt"] == 0:
                db.execute(
                    "UPDATE call_attempts SET counted_attempt = 1, call_sid = COALESCE(call_sid, ?) WHERE request_id = ?",
                    (call_sid, request_id),
                )
                db.execute(
                    "UPDATE leads SET attempts = attempts + 1 WHERE campaign_id = ? AND lead_id = ?",
                    (cid, lid),
                )
            elif not attempt["call_sid"]:
                db.execute(
                    "UPDATE call_attempts SET call_sid = ? WHERE request_id = ?",
                    (call_sid, request_id),
                )
            return True

        # Normal case: Transition from INTENT (or UNKNOWN_504) to ACTIVE
        if attempt["counted_attempt"] == 0:
            db.execute(
                "UPDATE leads SET attempts = attempts + 1 WHERE campaign_id = ? AND lead_id = ?",
                (cid, lid),
            )

        db.execute(
            """
            UPDATE call_attempts
            SET state = 'ACTIVE', call_sid = ?, counted_attempt = 1
            WHERE request_id = ?
            """,
            (call_sid, request_id),
        )
        return True


def mark_call_aborted_503(
    db: Database, request_id: str, sim_now: Optional[datetime] = None
) -> bool:
    """
    Called when POST /v1/calls returns 503 (transient error), or 404 from GET /v1/calls.
    Reverts lead state back to WAITING without incrementing attempts (R4).
    """
    with db.atomic():
        attempt = db.fetchone(
            "SELECT * FROM call_attempts WHERE request_id = ?", (request_id,)
        )
        if not attempt:
            return False

        if attempt["state"] in ("ENDED", "ACTIVE"):
            return False  # Already placed or completed

        cid = attempt["campaign_id"]
        lid = attempt["lead_id"]
        phone = attempt["phone"]

        # Mark attempt as ABORTED
        db.execute(
            "UPDATE call_attempts SET state = 'ABORTED' WHERE request_id = ?",
            (request_id,),
        )

        # Revert lead back to WAITING unless phone is in global_dnd
        is_dnd = db.fetchone("SELECT 1 FROM global_dnd WHERE phone = ?", (phone,))
        if is_dnd:
            db.execute(
                "UPDATE leads SET state = 'DND_BLOCKED' WHERE campaign_id = ? AND lead_id = ?",
                (cid, lid),
            )
        else:
            db.execute(
                "UPDATE leads SET state = 'WAITING' WHERE campaign_id = ? AND lead_id = ?",
                (cid, lid),
            )

        return True


def mark_call_unknown_504(db: Database, request_id: str) -> bool:
    """
    Called when POST /v1/calls times out with 504.
    Marks intent as UNKNOWN_504 so reconciler/sweeper resolves it before lead is re-dialled.
    """
    with db.atomic():
        attempt = db.fetchone(
            "SELECT * FROM call_attempts WHERE request_id = ?", (request_id,)
        )
        if not attempt:
            return False

        if attempt["state"] == "INTENT":
            db.execute(
                "UPDATE call_attempts SET state = 'UNKNOWN_504' WHERE request_id = ?",
                (request_id,),
            )
            return True
        return False


def process_call_result(db: Database, result_payload: dict[str, Any]) -> bool:
    """
    Process result from incoming webhook or reconciler GET /v1/calls.
    Strictly idempotent: ignores duplicate delivery.
    Enforces R4 (max attempts), R5 (retries), R6 (callbacks), R7 (terminal),
    R8 (global DND cascade), and R9 (precedence: completed > exhausted > dnd_blocked).
    """
    res = result_payload.get("result", result_payload)
    request_id = res.get("request_id")
    call_sid = res.get("call_sid")
    status = res.get("status", "")
    disposition = res.get("disposition")
    callback_at = res.get("callback_at")
    started_at = res.get("started_at")
    ended_at = res.get("ended_at")
    duration_sec = int(res.get("duration_sec", 0) or 0)

    with db.atomic():
        # 1. Lookup call_attempt by request_id, call_sid, or (campaign_id, lead_id)
        attempt = None
        if request_id:
            attempt = db.fetchone(
                "SELECT * FROM call_attempts WHERE request_id = ?", (request_id,)
            )
        if not attempt and call_sid:
            attempt = db.fetchone(
                "SELECT * FROM call_attempts WHERE call_sid = ?", (call_sid,)
            )
        if not attempt and res.get("campaign_id") and res.get("lead_id"):
            attempt = db.fetchone(
                """
                SELECT * FROM call_attempts
                WHERE campaign_id = ? AND lead_id = ? AND state IN ('INTENT', 'ACTIVE', 'UNKNOWN_504')
                ORDER BY attempt_no DESC LIMIT 1
                """,
                (res["campaign_id"], res["lead_id"]),
            )

        if not attempt:
            return False

        # Idempotency Guard: already processed
        if attempt["state"] == "ENDED":
            # Ensure lead is not left stuck in DIALING
            db.execute(
                """
                UPDATE leads
                SET state = CASE
                    WHEN phone IN (SELECT phone FROM global_dnd) THEN 'DND_BLOCKED'
                    ELSE 'WAITING'
                END
                WHERE campaign_id = ? AND lead_id = ? AND state = 'DIALING'
                """,
                (attempt["campaign_id"], attempt["lead_id"]),
            )
            return False

        cid = attempt["campaign_id"]
        lid = attempt["lead_id"]
        phone = attempt["phone"]

        # Ensure counted_attempt = 1 and lead attempts incremented
        if attempt["counted_attempt"] == 0:
            db.execute(
                "UPDATE leads SET attempts = attempts + 1 WHERE campaign_id = ? AND lead_id = ?",
                (cid, lid),
            )

        # Mark attempt ENDED
        db.execute(
            """
            UPDATE call_attempts
            SET state = 'ENDED',
                call_sid = COALESCE(call_sid, ?),
                counted_attempt = 1,
                status = ?,
                disposition = ?,
                callback_at = ?,
                started_at = ?,
                ended_at = ?,
                duration_sec = ?
            WHERE request_id = ?
            """,
            (
                call_sid,
                status,
                disposition,
                callback_at,
                started_at,
                ended_at,
                duration_sec,
                attempt["request_id"],
            ),
        )

        # Fetch current lead state and campaign config
        lead = db.fetchone(
            "SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?", (cid, lid)
        )
        campaign = db.fetchone(
            "SELECT * FROM campaigns WHERE campaign_id = ?", (cid,)
        )

        # 2. Compute transition using pure deterministic rules
        transition = compute_next_lead_transition(
            attempts=lead["attempts"],
            max_attempts=campaign["max_attempts"],
            status=status,
            disposition=disposition,
            ended_at=ended_at or datetime.now(IST).isoformat(),
            callback_at=callback_at,
            window_days=campaign["window_days"],
            window_start=campaign["window_start"],
            window_end=campaign["window_end"],
            no_answer_wait_min=campaign["no_answer_wait_min"],
            busy_wait_min=campaign["busy_wait_min"],
            failed_wait_min=campaign["failed_wait_min"],
        )

        final_state = transition.new_state

        # 3. Global DND handling (R8 & R9)
        if transition.is_dnd:
            # Add phone to global_dnd
            db.execute(
                """
                INSERT OR IGNORE INTO global_dnd (phone, source_call_sid, created_at)
                VALUES (?, ?, ?)
                """,
                (phone, call_sid, ended_at or datetime.now(IST).isoformat()),
            )
            # The lead that triggered DND is COMPLETED (completed > dnd_blocked)
            final_state = "COMPLETED"

            # Cascade DND_BLOCKED to all other active/waiting leads with this phone
            db.execute(
                """
                UPDATE leads
                SET state = 'DND_BLOCKED'
                WHERE phone = ?
                  AND NOT (campaign_id = ? AND lead_id = ?)
                  AND state NOT IN ('COMPLETED', 'EXHAUSTED')
                """,
                (phone, cid, lid),
            )
        else:
            # If not terminal/exhausted, check if phone was added to global_dnd by another call
            if final_state not in ("COMPLETED", "EXHAUSTED"):
                in_dnd = db.fetchone(
                    "SELECT 1 FROM global_dnd WHERE phone = ?", (phone,)
                )
                if in_dnd:
                    final_state = "DND_BLOCKED"

        # 4. Update the lead row
        next_eligible_str = (
            format_ist(transition.next_eligible_at)
            if transition.next_eligible_at
            else lead["next_eligible_at"]
        )
        callback_str = (
            format_ist(transition.callback_at)
            if transition.callback_at
            else None
        )

        db.execute(
            """
            UPDATE leads
            SET state = ?,
                priority_tier = ?,
                next_eligible_at = ?,
                callback_at = ?,
                last_call_sid = COALESCE(?, last_call_sid),
                last_status = ?,
                last_disposition = ?,
                last_ended_at = ?
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (
                final_state,
                transition.priority_tier,
                next_eligible_str,
                callback_str,
                call_sid,
                status,
                disposition,
                ended_at,
                cid,
                lid,
            ),
        )

        return True

