from __future__ import annotations

import os
from pathlib import Path
import pytest

from dispatcher.db import Database
from dispatcher.loader import load_fixtures
from dispatcher.config import config


def test_fixture_loading_and_idempotency(tmp_path: Path):
    db_file = tmp_path / "test_dispatcher.db"
    db = Database(str(db_file))

    fixtures_dir = config.resolve_fixtures_dir()
    assert fixtures_dir.exists(), f"Fixtures directory not found: {fixtures_dir}"

    # 1. First run of load_fixtures
    counts = load_fixtures(db, fixtures_dir)
    assert counts["tenants"] == 2
    assert counts["campaigns"] == 3
    assert counts["leads"] == 200

    # Verify database counts directly
    tenants = db.fetchall("SELECT * FROM tenants")
    campaigns = db.fetchall("SELECT * FROM campaigns")
    leads = db.fetchall("SELECT * FROM leads")

    assert len(tenants) == 2
    assert len(campaigns) == 3
    assert len(leads) == 200

    # Verify campaign details
    tenant_ids = {t["tenant_id"] for t in tenants}
    assert tenant_ids == {"acme_insurance", "zen_retail"}

    camp_ids = {c["campaign_id"] for c in campaigns}
    assert camp_ids == {"c_acme_renewals", "c_acme_feedback", "c_zen_winback"}

    # 2. Modify one lead's state to COMPLETED
    sample_lead = leads[0]
    cid = sample_lead["campaign_id"]
    lid = sample_lead["lead_id"]

    with db.atomic():
        db.execute(
            """
            UPDATE leads
            SET state = 'COMPLETED', attempts = 2, last_status = 'completed', last_disposition = 'interested'
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (cid, lid),
        )

    modified_lead = db.fetchone(
        "SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?", (cid, lid)
    )
    assert modified_lead["state"] == "COMPLETED"
    assert modified_lead["attempts"] == 2
    assert modified_lead["last_status"] == "completed"
    assert modified_lead["last_disposition"] == "interested"

    # 3. Second run of load_fixtures (simulating process restart)
    load_fixtures(db, fixtures_dir)

    reloaded_lead = db.fetchone(
        "SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?", (cid, lid)
    )
    # Assert state was NOT overwritten by load_fixtures
    assert reloaded_lead["state"] == "COMPLETED"
    assert reloaded_lead["attempts"] == 2
    assert reloaded_lead["last_status"] == "completed"
    assert reloaded_lead["last_disposition"] == "interested"

    # Total leads count should still be exactly 200
    all_leads_after = db.fetchall("SELECT * FROM leads")
    assert len(all_leads_after) == 200

    db.close()


def test_dnd_cascade_on_load(tmp_path: Path):
    db_file = tmp_path / "test_dnd.db"
    db = Database(str(db_file))
    fixtures_dir = config.resolve_fixtures_dir()

    # Pre-populate global_dnd with a phone number known to be in leads
    target_phone = "+916588139986"  # phone for ren_0001
    with db.atomic():
        db.execute(
            "INSERT INTO global_dnd (phone, created_at) VALUES (?, ?)",
            (target_phone, "2026-11-12T17:30:00+05:30"),
        )

    # Load fixtures
    load_fixtures(db, fixtures_dir)

    # Verify that the lead with target_phone was set to DND_BLOCKED
    blocked_leads = db.fetchall(
        "SELECT * FROM leads WHERE phone = ?", (target_phone,)
    )
    assert len(blocked_leads) > 0
    for lead in blocked_leads:
        assert lead["state"] == "DND_BLOCKED"

    db.close()

