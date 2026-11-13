from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Union

from .db import Database


def load_fixtures(db: Database, fixtures_dir: Union[str, Path]) -> dict:
    """
    Idempotently load campaigns and leads from fixtures into SQLite database.
    Preserves existing lead states, attempt counts, and dispositions across restarts.
    """
    fdir = Path(fixtures_dir)
    campaigns_file = fdir / "campaigns.json" if fdir.is_dir() else fdir
    if not campaigns_file.exists():
        raise FileNotFoundError(f"Campaigns fixtures file not found at: {campaigns_file}")

    actual_dir = campaigns_file.parent
    with open(campaigns_file, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    sim_start = cfg.get("sim_start", "2026-11-12T17:30:00+05:30")
    webhook_secret = cfg.get("webhook_secret", "")
    retry_policy = cfg.get("retry_policy", {})
    no_answer_wait = retry_policy.get("no_answer_minutes", 60)
    busy_wait = retry_policy.get("busy_minutes", 60)
    failed_wait = retry_policy.get("failed_minutes", 15)

    loaded_counts = {"tenants": 0, "campaigns": 0, "leads": 0}

    with db.atomic():
        # 1. Load Tenants
        for t in cfg.get("tenants", []):
            db.execute(
                """
                INSERT OR REPLACE INTO tenants (tenant_id, max_concurrent_calls, webhook_secret)
                VALUES (?, ?, ?)
                """,
                (t["tenant_id"], t["max_concurrent_calls"], webhook_secret),
            )
            loaded_counts["tenants"] += 1

        # 2. Load Campaigns
        for c in cfg.get("campaigns", []):
            w = c.get("window", {})
            days_str = ",".join(w.get("days", []))
            db.execute(
                """
                INSERT OR REPLACE INTO campaigns (
                    campaign_id, tenant_id, leads_file, window_start, window_end, window_days,
                    max_attempts, no_answer_wait_min, busy_wait_min, failed_wait_min, webhook_secret
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    c["campaign_id"],
                    c["tenant_id"],
                    c.get("leads_file"),
                    w.get("start", "09:00"),
                    w.get("end", "21:00"),
                    days_str,
                    c.get("max_attempts", 3),
                    no_answer_wait,
                    busy_wait,
                    failed_wait,
                    webhook_secret,
                ),
            )
            loaded_counts["campaigns"] += 1

            # 3. Load Leads for each campaign
            leads_filename = c.get("leads_file")
            if leads_filename:
                leads_path = actual_dir / leads_filename
                if leads_path.exists():
                    with open(leads_path, "r", encoding="utf-8") as lf:
                        reader = csv.DictReader(lf)
                        for row in reader:
                            lead_id = row["lead_id"].strip()
                            phone = row["phone"].strip()
                            cust_name = row.get("customer_name", "").strip()

                            # Idempotent insert: only insert if lead does not already exist
                            db.execute(
                                """
                                INSERT OR IGNORE INTO leads (
                                    campaign_id, lead_id, tenant_id, phone, customer_name,
                                    state, priority_tier, next_eligible_at, attempts
                                ) VALUES (?, ?, ?, ?, ?, 'WAITING', 3, ?, 0)
                                """,
                                (c["campaign_id"], lead_id, c["tenant_id"], phone, cust_name, sim_start),
                            )
                            loaded_counts["leads"] += 1

        # 4. Cascade existing global DND numbers to non-terminal leads
        db.execute(
            """
            UPDATE leads
            SET state = 'DND_BLOCKED'
            WHERE phone IN (SELECT phone FROM global_dnd)
              AND state NOT IN ('COMPLETED', 'EXHAUSTED')
            """
        )

    return loaded_counts

