from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from dispatcher.clock import IST, SimClock, parse_ist
from dispatcher.config import Config
from dispatcher.db import Database
from dispatcher.engine import DispatcherEngine


def test_crash_recovery_reconciliation(tmp_path: Path):
    async def _test():
        db_file = tmp_path / "test_reconcile.db"
        db = Database(str(db_file))

        # Setup tenant and campaign
        with db.atomic():
            db.execute(
                "INSERT INTO tenants (tenant_id, max_concurrent_calls, webhook_secret) VALUES (?, ?, ?)",
                ("t_recon", 5, "whsec_test"),
            )
            db.execute(
                """
                INSERT INTO campaigns (
                    campaign_id, tenant_id, leads_file, window_start, window_end,
                    window_days, max_attempts, no_answer_wait_min, busy_wait_min, failed_wait_min, webhook_secret
                ) VALUES (?, ?, ?, ?, ?, ?, 3, 60, 60, 15, ?)
                """,
                ("c_recon", "t_recon", "leads.csv", "10:00", "19:00", "mon,tue,wed,thu,fri,sat", "whsec_test"),
            )

            sim_now_str = "2026-11-12T14:00:00+05:30"

            # 1. Lead & Attempt 1: Crashed while in INTENT, provider returns 404 (never dialled)
            db.execute(
                """
                INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts)
                VALUES (?, ?, ?, ?, 'DIALING', 3, ?, 0)
                """,
                ("c_recon", "l_intent", "t_recon", "+919000000001", sim_now_str),
            )
            db.execute(
                """
                INSERT INTO call_attempts (
                    request_id, campaign_id, lead_id, tenant_id, phone,
                    state, attempt_no, counted_attempt, created_at_real, created_at_sim
                ) VALUES (?, ?, ?, ?, ?, 'INTENT', 1, 0, ?, ?)
                """,
                ("req_intent_404", "c_recon", "l_intent", "t_recon", "+919000000001", time.time() - 2.0, sim_now_str),
            )

            # 2. Lead & Attempt 2: Crashed in UNKNOWN_504, provider placed call and ended with 'interested'
            db.execute(
                """
                INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts)
                VALUES (?, ?, ?, ?, 'DIALING', 3, ?, 0)
                """,
                ("c_recon", "l_504", "t_recon", "+919000000002", sim_now_str),
            )
            db.execute(
                """
                INSERT INTO call_attempts (
                    request_id, campaign_id, lead_id, tenant_id, phone,
                    state, attempt_no, counted_attempt, created_at_real, created_at_sim
                ) VALUES (?, ?, ?, ?, ?, 'UNKNOWN_504', 1, 0, ?, ?)
                """,
                ("req_504_ended", "c_recon", "l_504", "t_recon", "+919000000002", time.time() - 2.0, sim_now_str),
            )

            # 3. Lead & Attempt 3: In ACTIVE, call ended with 'no_answer', but webhook was lost
            db.execute(
                """
                INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts)
                VALUES (?, ?, ?, ?, 'DIALING', 3, ?, 1)
                """,
                ("c_recon", "l_active", "t_recon", "+919000000003", sim_now_str),
            )
            db.execute(
                """
                INSERT INTO call_attempts (
                    request_id, call_sid, campaign_id, lead_id, tenant_id, phone,
                    state, attempt_no, counted_attempt, created_at_real, created_at_sim
                ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', 1, 1, ?, ?)
                """,
                ("req_active_lost", "CA_ACTIVE_003", "c_recon", "l_active", "t_recon", "+919000000003", time.time() - 2.0, sim_now_str),
            )

        # Define mock HTTP transport
        def mock_handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "req_intent_404" in url:
                return httpx.Response(404, json={"error": "not_found"})
            elif "req_504_ended" in url:
                return httpx.Response(
                    200,
                    json={
                        "call_sid": "CA_504_ACCEPTED",
                        "request_id": "req_504_ended",
                        "campaign_id": "c_recon",
                        "lead_id": "l_504",
                        "state": "ended",
                        "result": {
                            "event": "call.completed",
                            "call_sid": "CA_504_ACCEPTED",
                            "request_id": "req_504_ended",
                            "campaign_id": "c_recon",
                            "lead_id": "l_504",
                            "phone": "+919000000002",
                            "status": "completed",
                            "disposition": "interested",
                            "duration_sec": 45,
                            "started_at": "2026-11-12T14:00:00+05:30",
                            "ended_at": "2026-11-12T14:01:00+05:30",
                        },
                    },
                )
            elif "req_active_lost" in url or "CA_ACTIVE_003" in url:
                return httpx.Response(
                    200,
                    json={
                        "call_sid": "CA_ACTIVE_003",
                        "request_id": "req_active_lost",
                        "campaign_id": "c_recon",
                        "lead_id": "l_active",
                        "state": "ended",
                        "result": {
                            "event": "call.completed",
                            "call_sid": "CA_ACTIVE_003",
                            "request_id": "req_active_lost",
                            "campaign_id": "c_recon",
                            "lead_id": "l_active",
                            "phone": "+919000000003",
                            "status": "no_answer",
                            "disposition": None,
                            "duration_sec": 30,
                            "started_at": "2026-11-12T14:00:00+05:30",
                            "ended_at": "2026-11-12T14:05:00+05:30",
                        },
                    },
                )
            return httpx.Response(404, json={"error": "unknown_url"})

        sim_start = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
        clock = SimClock(sim_epoch=sim_start, real_epoch=time.time(), speed=1800.0)
        cfg = Config(db_path=str(db_file), provider_url="http://mock-provider:8099")
        engine = DispatcherEngine(db=db, clock=clock, config=cfg)

        # Perform crash recovery reconciliation
        transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            await engine.reconcile_in_flight(client, force_all=True)

        # Verification:
        # 1. Attempt 1 (INTENT -> 404): Aborted, reverted to WAITING, attempts = 0
        att1 = db.fetchone("SELECT * FROM call_attempts WHERE request_id = 'req_intent_404'")
        assert att1["state"] == "ABORTED"
        lead1 = db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_intent'")
        assert lead1["state"] == "WAITING"
        assert lead1["attempts"] == 0

        # 2. Attempt 2 (UNKNOWN_504 -> 200 ended interested): Counted once, COMPLETED, attempts = 1
        att2 = db.fetchone("SELECT * FROM call_attempts WHERE request_id = 'req_504_ended'")
        assert att2["state"] == "ENDED"
        assert att2["counted_attempt"] == 1
        assert att2["call_sid"] == "CA_504_ACCEPTED"
        lead2 = db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_504'")
        assert lead2["state"] == "COMPLETED"
        assert lead2["attempts"] == 1
        assert lead2["last_disposition"] == "interested"

        # 3. Attempt 3 (ACTIVE lost webhook -> 200 ended no_answer): WAITING with retry priority tier 2 and +60m backoff
        att3 = db.fetchone("SELECT * FROM call_attempts WHERE request_id = 'req_active_lost'")
        assert att3["state"] == "ENDED"
        lead3 = db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_active'")
        assert lead3["state"] == "WAITING"
        assert lead3["priority_tier"] == 2  # PRIORITY_RETRY
        assert lead3["attempts"] == 1
        assert lead3["last_status"] == "no_answer"
        # ended_at was 14:05, no_answer wait is 60m -> next_eligible_at is 15:05
        expected_eligible = parse_ist("2026-11-12T15:05:00+05:30")
        assert parse_ist(lead3["next_eligible_at"]) == expected_eligible

        db.close()

    asyncio.run(_test())
