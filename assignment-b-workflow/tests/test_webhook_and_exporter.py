from __future__ import annotations

import csv
import hashlib
import hmac
import json
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from dispatcher.db import Database
from dispatcher.exporter import is_all_leads_terminal, export_final_state_csv
from dispatcher.webhook import create_webhook_app


@pytest.fixture
def webhook_test_setup(tmp_path: Path):
    db = Database(str(tmp_path / "test_wh.db"))
    secret = "whsec_au_assignment_local"

    with db.atomic():
        db.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls, webhook_secret) VALUES (?, ?, ?)",
            ("tenant_test", 2, secret),
        )
        db.execute(
            """
            INSERT INTO campaigns (
                campaign_id, tenant_id, leads_file, window_start, window_end,
                window_days, max_attempts, no_answer_wait_min, busy_wait_min, failed_wait_min, webhook_secret
            ) VALUES (?, ?, ?, ?, ?, ?, 3, 60, 60, 15, ?)
            """,
            ("camp_test", "tenant_test", "leads.csv", "10:00", "19:00", "mon,tue,wed,thu,fri", secret),
        )
        db.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts)
            VALUES (?, ?, ?, ?, 'DIALING', 3, ?, 0)
            """,
            ("camp_test", "l_001", "tenant_test", "+919876543210", "2026-11-12T14:00:00+05:30"),
        )
        db.execute(
            """
            INSERT INTO call_attempts (
                request_id, campaign_id, lead_id, tenant_id, phone, state, attempt_no,
                counted_attempt, created_at_real, created_at_sim
            ) VALUES (?, ?, ?, ?, ?, 'INTENT', 1, 0, 1000.0, '2026-11-12T14:00:00+05:30')
            """,
            ("req_001", "camp_test", "l_001", "tenant_test", "+919876543210"),
        )

    app = create_webhook_app(db)
    client = TestClient(app)

    yield db, client, secret
    db.close()


def test_webhook_valid_signature_and_processing(webhook_test_setup):
    db, client, secret = webhook_test_setup

    payload = {
        "event": "call.completed",
        "call_sid": "CA_WH_TEST_001",
        "request_id": "req_001",
        "tenant_id": "tenant_test",
        "campaign_id": "camp_test",
        "lead_id": "l_001",
        "phone": "+919876543210",
        "status": "completed",
        "disposition": "interested",
        "started_at": "2026-11-12T14:00:00+05:30",
        "ended_at": "2026-11-12T14:02:00+05:30",
        "duration_sec": 120,
    }
    raw_body = json.dumps(payload).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()

    resp = client.post(
        "/webhooks/telephony",
        content=raw_body,
        headers={"Content-Type": "application/json", "X-AU-Signature": sig},
    )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    # Verify database update
    lead = db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_001'")
    assert lead["state"] == "COMPLETED"
    assert lead["attempts"] == 1
    assert lead["last_disposition"] == "interested"

    attempt = db.fetchone("SELECT * FROM call_attempts WHERE request_id = 'req_001'")
    assert attempt["state"] == "ENDED"
    assert attempt["call_sid"] == "CA_WH_TEST_001"
    assert attempt["counted_attempt"] == 1


def test_webhook_invalid_and_missing_signature(webhook_test_setup):
    db, client, secret = webhook_test_setup

    payload = {
        "event": "call.completed",
        "call_sid": "CA_WH_TEST_BAD",
        "request_id": "req_001",
        "tenant_id": "tenant_test",
        "campaign_id": "camp_test",
        "lead_id": "l_001",
        "phone": "+919876543210",
        "status": "completed",
        "disposition": "interested",
    }
    raw_body = json.dumps(payload).encode("utf-8")

    # 1. Missing signature header
    resp_missing = client.post(
        "/webhooks/telephony",
        content=raw_body,
        headers={"Content-Type": "application/json"},
    )
    assert resp_missing.status_code == 401
    assert resp_missing.json() == {"error": "missing_signature"}

    # 2. Invalid signature header
    resp_bad = client.post(
        "/webhooks/telephony",
        content=raw_body,
        headers={"Content-Type": "application/json", "X-AU-Signature": "invalid_hex_sig_here"},
    )
    assert resp_bad.status_code == 401
    assert resp_bad.json() == {"error": "invalid_signature"}

    # Verify database state was NOT changed
    lead = db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_001'")
    assert lead["state"] == "DIALING"
    assert lead["attempts"] == 0


def test_webhook_idempotency(webhook_test_setup):
    db, client, secret = webhook_test_setup

    payload = {
        "event": "call.completed",
        "call_sid": "CA_WH_DUP",
        "request_id": "req_001",
        "tenant_id": "tenant_test",
        "campaign_id": "camp_test",
        "lead_id": "l_001",
        "phone": "+919876543210",
        "status": "completed",
        "disposition": "interested",
        "started_at": "2026-11-12T14:00:00+05:30",
        "ended_at": "2026-11-12T14:02:00+05:30",
        "duration_sec": 120,
    }
    raw_body = json.dumps(payload).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json", "X-AU-Signature": sig}

    # 1st delivery
    r1 = client.post("/webhooks/telephony", content=raw_body, headers=headers)
    assert r1.status_code == 200

    # 2nd delivery (duplicate)
    r2 = client.post("/webhooks/telephony", content=raw_body, headers=headers)
    assert r2.status_code == 200

    # Attempts must still be exactly 1
    lead = db.fetchone("SELECT * FROM leads WHERE lead_id = 'l_001'")
    assert lead["attempts"] == 1


def test_status_endpoint(webhook_test_setup):
    db, client, secret = webhook_test_setup
    resp = client.get("/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "campaigns" in data
    assert "active_tenant_concurrency" in data


def test_exporter_and_terminal_detection(tmp_path: Path):
    db = Database(str(tmp_path / "test_exp.db"))
    with db.atomic():
        db.execute("INSERT INTO tenants VALUES ('t1', 2, 'sec')")
        db.execute("INSERT INTO campaigns VALUES ('c_b', 't1', 'f.csv', '10:00', '19:00', 'mon', 3, 60, 60, 15, 'sec')")
        db.execute("INSERT INTO campaigns VALUES ('c_a', 't1', 'f.csv', '10:00', '19:00', 'mon', 3, 60, 60, 15, 'sec')")

        # Insert leads out of sorted order
        db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts, last_status, last_disposition) VALUES ('c_b', 'l_2', 't1', 'p2', 'WAITING', 3, '2026-11-12', 0, NULL, NULL)"
        )
        db.execute(
            "INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, state, priority_tier, next_eligible_at, attempts, last_status, last_disposition) VALUES ('c_a', 'l_1', 't1', 'p1', 'COMPLETED', 3, '2026-11-12', 2, 'completed', 'interested')"
        )

    # Currently l_2 is WAITING -> not terminal
    assert is_all_leads_terminal(db) is False

    # Mark l_2 EXHAUSTED
    with db.atomic():
        db.execute("UPDATE leads SET state = 'EXHAUSTED', attempts = 3, last_status = 'no_answer' WHERE lead_id = 'l_2'")

    assert is_all_leads_terminal(db) is True

    # Export final_state.csv
    out_csv = tmp_path / "final_state.csv"
    export_final_state_csv(db, out_csv)
    assert out_csv.exists()

    with open(out_csv, "r", encoding="utf-8") as f:
        reader = list(csv.reader(f))

    # Check header
    assert reader[0] == ["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"]

    # Check rows ordered by campaign_id, lead_id
    assert reader[1] == ["c_a", "l_1", "completed", "2", "completed", "interested"]
    assert reader[2] == ["c_b", "l_2", "exhausted", "3", "no_answer", ""]

    db.close()

