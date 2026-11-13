from __future__ import annotations

from datetime import datetime
from pathlib import Path
import pytest

from dispatcher.clock import IST
from dispatcher.db import Database
from dispatcher.state_machine import (
    claim_next_calls,
    mark_call_accepted,
    mark_call_aborted_503,
    mark_call_unknown_504,
    process_call_result,
)


@pytest.fixture
def test_db(tmp_path: Path) -> Database:
    db = Database(str(tmp_path / "test_sm.db"))
    with db.atomic():
        # Setup tenants
        db.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls, webhook_secret) VALUES (?, ?, ?)",
            ("tenant_a", 2, "secret_a"),
        )
        db.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls, webhook_secret) VALUES (?, ?, ?)",
            ("tenant_b", 1, "secret_b"),
        )
        # Setup campaigns
        db.execute(
            """
            INSERT INTO campaigns (
                campaign_id, tenant_id, leads_file, window_start, window_end,
                window_days, max_attempts, no_answer_wait_min, busy_wait_min, failed_wait_min
            ) VALUES (?, ?, ?, ?, ?, ?, 3, 60, 60, 15)
            """,
            ("camp_a1", "tenant_a", "leads1.csv", "10:00", "19:00", "mon,tue,wed,thu,fri,sat"),
        )
        db.execute(
            """
            INSERT INTO campaigns (
                campaign_id, tenant_id, leads_file, window_start, window_end,
                window_days, max_attempts, no_answer_wait_min, busy_wait_min, failed_wait_min
            ) VALUES (?, ?, ?, ?, ?, ?, 3, 60, 60, 15)
            """,
            ("camp_b1", "tenant_b", "leads2.csv", "10:00", "19:00", "mon,tue,wed,thu,fri,sat"),
        )
    yield db
    db.close()


def test_r2_r3_concurrency_and_same_phone_locking(test_db: Database):
    sim_now = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)  # Thursday 14:00 (inside window)
    sim_str = sim_now.isoformat()

    # Insert 3 leads for tenant_a (max 2)
    # Insert 1 lead for tenant_b (max 1), but with SAME phone as lead_a1!
    shared_phone = "+919999900001"
    phone_a2 = "+919999900002"
    phone_a3 = "+919999900003"

    with test_db.atomic():
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_a1", "l_a1", "tenant_a", shared_phone, sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_a1", "l_a2", "tenant_a", phone_a2, sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_a1", "l_a3", "tenant_a", phone_a3, sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_b1", "l_b1", "tenant_b", shared_phone, sim_str),
        )

    # 1. Claim calls
    claimed = claim_next_calls(test_db, sim_now)

    # Tenant A allows max 2 calls -> l_a1 and l_a2 claimed
    # Tenant B has l_b1 which shares phone with l_a1 -> cannot be claimed!
    assert len(claimed) == 2
    claimed_leads = {c["lead_id"] for c in claimed}
    assert claimed_leads == {"l_a1", "l_a2"}

    # Second claim attempt immediately afterwards -> nothing eligible (Tenant A is at capacity, l_b1 phone active)
    claimed_second = claim_next_calls(test_db, sim_now)
    assert len(claimed_second) == 0


def test_r6_priority_ordering(test_db: Database):
    sim_now = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    sim_str = sim_now.isoformat()

    # Insert 3 leads for tenant_a with different priority tiers:
    # l_fresh: tier 3
    # l_retry: tier 2
    # l_callback: tier 1
    with test_db.atomic():
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_a1", "l_fresh", "tenant_a", "+919111111111", sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 2, ?, 1)",
            ("camp_a1", "l_retry", "tenant_a", "+919222222222", sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 1, ?, 1)",
            ("camp_a1", "l_callback", "tenant_a", "+919333333333", sim_str),
        )

    # Claim batch
    claimed = claim_next_calls(test_db, sim_now)
    assert len(claimed) == 2  # Tenant A capacity is 2

    # Must be callback first, then retry
    assert claimed[0]["lead_id"] == "l_callback"
    assert claimed[1]["lead_id"] == "l_retry"


def test_early_webhook_race_condition_and_idempotency(test_db: Database):
    sim_now = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    sim_str = sim_now.isoformat()

    with test_db.atomic():
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_a1", "l_race", "tenant_a", "+919876543210", sim_str),
        )

    # 1. Claim call -> state becomes DIALING, attempt created with state INTENT
    claimed = claim_next_calls(test_db, sim_now)
    assert len(claimed) == 1
    rid = claimed[0]["request_id"]

    # Verify initial DB state
    lead_init = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_race'")
    assert lead_init["state"] == "DIALING"
    assert lead_init["attempts"] == 0

    # 2. Race condition: Webhook arrives BEFORE mark_call_accepted!
    webhook_payload = {
        "event": "call.completed",
        "call_sid": "CA_EARLY_001",
        "request_id": rid,
        "tenant_id": "tenant_a",
        "campaign_id": "camp_a1",
        "lead_id": "l_race",
        "phone": "+919876543210",
        "status": "completed",
        "disposition": "interested",
        "started_at": sim_str,
        "ended_at": sim_str,
        "duration_sec": 45,
    }

    processed = process_call_result(test_db, webhook_payload)
    assert processed is True

    # Verify attempt and lead are updated
    attempt_after_wh = test_db.fetchone("SELECT * FROM call_attempts WHERE request_id = ?", (rid,))
    assert attempt_after_wh["state"] == "ENDED"
    assert attempt_after_wh["call_sid"] == "CA_EARLY_001"
    assert attempt_after_wh["counted_attempt"] == 1

    lead_after_wh = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_race'")
    assert lead_after_wh["state"] == "COMPLETED"
    assert lead_after_wh["attempts"] == 1
    assert lead_after_wh["last_status"] == "completed"
    assert lead_after_wh["last_disposition"] == "interested"

    # 3. HTTP response 201 finally arrives and mark_call_accepted is called
    accepted = mark_call_accepted(test_db, rid, "CA_EARLY_001")
    assert accepted is True

    # Verify state was NOT reverted back to ACTIVE and attempts not double counted
    attempt_after_acc = test_db.fetchone("SELECT * FROM call_attempts WHERE request_id = ?", (rid,))
    assert attempt_after_acc["state"] == "ENDED"

    lead_after_acc = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_race'")
    assert lead_after_acc["state"] == "COMPLETED"
    assert lead_after_acc["attempts"] == 1

    # 4. Duplicate webhook delivery
    dup_processed = process_call_result(test_db, webhook_payload)
    assert dup_processed is False  # Safely ignored


def test_503_rollback(test_db: Database):
    sim_now = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    sim_str = sim_now.isoformat()

    with test_db.atomic():
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_b1", "l_503", "tenant_b", "+919444444444", sim_str),
        )

    # Tenant B has max 1 concurrent call
    claimed = claim_next_calls(test_db, sim_now)
    assert len(claimed) == 1
    rid = claimed[0]["request_id"]

    # In-flight: concurrency is full
    assert len(claim_next_calls(test_db, sim_now)) == 0

    # 503 error received
    aborted = mark_call_aborted_503(test_db, rid, sim_now)
    assert aborted is True

    # Verify attempt is ABORTED, lead reverted to WAITING, attempts = 0
    attempt = test_db.fetchone("SELECT * FROM call_attempts WHERE request_id = ?", (rid,))
    assert attempt["state"] == "ABORTED"

    lead = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_503'")
    assert lead["state"] == "WAITING"
    assert lead["attempts"] == 0

    # Concurrency is freed! Can be claimed again
    claimed_again = claim_next_calls(test_db, sim_now)
    assert len(claimed_again) == 1
    assert claimed_again[0]["lead_id"] == "l_503"


def test_r8_r9_global_dnd_cascade_and_precedence(test_db: Database):
    sim_now = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    sim_str = sim_now.isoformat()
    dnd_phone = "+919888888888"

    # Setup 4 leads sharing the same phone:
    # Lead A (Camp A): WAITING -> will be dialed and say DND
    # Lead B (Camp B): WAITING -> should become DND_BLOCKED
    # Lead C (Camp A): COMPLETED -> should STAY COMPLETED
    # Lead D (Camp B): EXHAUSTED -> should STAY EXHAUSTED
    with test_db.atomic():
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_a1", "l_dnd_trigger", "tenant_a", dnd_phone, sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'WAITING', 3, ?, 0)",
            ("camp_b1", "l_dnd_target", "tenant_b", dnd_phone, sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts, last_status, last_disposition) VALUES (?, ?, ?, ?, 'COMPLETED', 3, ?, 1, 'completed', 'interested')",
            ("camp_a1", "l_already_completed", "tenant_a", dnd_phone, sim_str),
        )
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts, last_status) VALUES (?, ?, ?, ?, 'EXHAUSTED', 3, ?, 3, 'no_answer')",
            ("camp_b1", "l_already_exhausted", "tenant_b", dnd_phone, sim_str),
        )

    # Claim l_dnd_trigger
    claimed = claim_next_calls(test_db, sim_now)
    assert len(claimed) == 1
    assert claimed[0]["lead_id"] == "l_dnd_trigger"
    rid = claimed[0]["request_id"]

    # Deliver DND result for l_dnd_trigger
    dnd_payload = {
        "call_sid": "CA_DND_100",
        "request_id": rid,
        "tenant_id": "tenant_a",
        "campaign_id": "camp_a1",
        "lead_id": "l_dnd_trigger",
        "phone": dnd_phone,
        "status": "completed",
        "disposition": "dnd",
        "started_at": sim_str,
        "ended_at": sim_str,
        "duration_sec": 30,
    }

    process_call_result(test_db, dnd_payload)

    # 1. Phone is registered in global_dnd
    dnd_record = test_db.fetchone("SELECT * FROM global_dnd WHERE phone = ?", (dnd_phone,))
    assert dnd_record is not None
    assert dnd_record["source_call_sid"] == "CA_DND_100"

    # 2. Triggering lead is COMPLETED (completed > dnd_blocked)
    lead_a = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_dnd_trigger'")
    assert lead_a["state"] == "COMPLETED"
    assert lead_a["last_disposition"] == "dnd"

    # 3. Other waiting lead with same phone is DND_BLOCKED
    lead_b = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_dnd_target'")
    assert lead_b["state"] == "DND_BLOCKED"

    # 4. Precedence: Already COMPLETED lead remains COMPLETED
    lead_c = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_already_completed'")
    assert lead_c["state"] == "COMPLETED"

    # 5. Precedence: Already EXHAUSTED lead remains EXHAUSTED
    lead_d = test_db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_already_exhausted'")
    assert lead_d["state"] == "EXHAUSTED"


def test_self_healing_stuck_dialing_lead(test_db: Database):
    """
    Verify that a lead stuck in DIALING with no active attempt in call_attempts
    is automatically self-healed back to WAITING.
    """
    sim_now = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    sim_str = sim_now.isoformat()

    with test_db.atomic():
        # Insert a lead stuck in DIALING with no active attempt
        test_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts) VALUES (?, ?, ?, ?, 'DIALING', 3, ?, 0)",
            ("camp_a1", "l_stuck", "tenant_a", "+919999977777", sim_str),
        )

    # Calling claim_next_calls self-heals it to WAITING and claims it
    claimed = claim_next_calls(test_db, sim_now)
    assert len(claimed) >= 1
    assert any(c["lead_id"] == "l_stuck" for c in claimed)

    # Now verify that the attempt was created with state INTENT
    attempt = test_db.fetchone("SELECT * FROM call_attempts WHERE lead_id = 'l_stuck'")
    assert attempt is not None
    assert attempt["state"] == "INTENT"

