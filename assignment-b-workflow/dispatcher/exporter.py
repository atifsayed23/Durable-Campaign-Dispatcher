from __future__ import annotations

import csv
import os
from pathlib import Path

from .db import Database


STATE_MAPPING = {
    "COMPLETED": "completed",
    "EXHAUSTED": "exhausted",
    "DND_BLOCKED": "dnd_blocked",
    "WAITING": "waiting",
    "DIALING": "dialing",
}


def is_all_leads_terminal(db: Database) -> bool:
    """
    Returns True only when all leads have reached a final terminal state
    and no call attempts are currently unresolved or active.
    """
    active_leads = db.fetchone(
        "SELECT COUNT(*) as cnt FROM leads WHERE state IN ('WAITING', 'DIALING')"
    )
    if active_leads and active_leads["cnt"] > 0:
        return False

    active_attempts = db.fetchone(
        "SELECT COUNT(*) as cnt FROM call_attempts WHERE state IN ('INTENT', 'ACTIVE', 'UNKNOWN_504')"
    )
    if active_attempts and active_attempts["cnt"] > 0:
        return False

    return True


def export_final_state_csv(db: Database, output_path: str | Path) -> None:
    """
    Export final_state.csv according to the assignment output contract:
    - Columns: campaign_id,lead_id,final_state,attempts,last_status,last_disposition
    - Sorted strictly by campaign_id, lead_id
    - Written atomically via temporary file and atomic rename to survive crashes.
    """
    out_file = Path(output_path).resolve()
    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = out_file.with_suffix(".tmp")

    leads = db.fetchall(
        """
        SELECT campaign_id, lead_id, state, attempts, last_status, last_disposition
        FROM leads
        ORDER BY campaign_id ASC, lead_id ASC
        """
    )

    with open(tmp_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            ["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"]
        )
        for row in leads:
            raw_state = row["state"]
            final_state = STATE_MAPPING.get(raw_state, raw_state.lower())
            attempts = row["attempts"]
            last_status = row["last_status"] or ""
            last_disp = row["last_disposition"] or ""

            writer.writerow(
                [
                    row["campaign_id"],
                    row["lead_id"],
                    final_state,
                    attempts,
                    last_status,
                    last_disp,
                ]
            )

    # Atomic replace
    os.replace(tmp_file, out_file)

