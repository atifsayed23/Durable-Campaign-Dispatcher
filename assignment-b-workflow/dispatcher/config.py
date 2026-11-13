from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    provider_url: str = os.getenv("PROVIDER_URL", "http://127.0.0.1:8099")
    webhook_host: str = os.getenv("WEBHOOK_HOST", "127.0.0.1")
    webhook_port: int = int(os.getenv("WEBHOOK_PORT", "8200"))
    db_path: str = os.getenv("DB_PATH", "dispatcher_state.db")
    fixtures_dir: str = os.getenv("FIXTURES_DIR", "fixtures")
    output_csv: str = os.getenv("OUTPUT_CSV", "final_state.csv")

    @property
    def callback_url(self) -> str:
        return f"http://{self.webhook_host}:{self.webhook_port}/webhooks/telephony"

    def resolve_fixtures_dir(self) -> Path:
        """Resolve fixtures_dir whether running from root or assignment-b-workflow/."""
        p = Path(self.fixtures_dir)
        if p.is_dir():
            return p
        # Check relative to assignment-b-workflow
        alt = Path(__file__).resolve().parent.parent / self.fixtures_dir
        if alt.is_dir():
            return alt
        # Check current working directory / assignment-b-workflow / fixtures
        alt2 = Path.cwd() / "assignment-b-workflow" / self.fixtures_dir
        if alt2.is_dir():
            return alt2
        return p


config = Config()

