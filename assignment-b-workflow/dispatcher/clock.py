from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional
import httpx

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except ImportError:
    IST = timezone(timedelta(hours=5, minutes=30))


def parse_ist(ts_str: str) -> datetime:
    """Parse ISO-8601 timestamp string into timezone-aware IST datetime."""
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def format_ist(dt: datetime) -> str:
    """Format timezone-aware datetime into ISO-8601 IST string."""
    return dt.astimezone(IST).isoformat()


class SimClock:
    """
    Simulated clock synchronized with the telephony provider's clock.
    sim_now = sim_epoch + (time.time() - real_epoch) * speed
    """
    def __init__(
        self,
        sim_epoch: Optional[datetime] = None,
        real_epoch: Optional[float] = None,
        speed: float = 600.0,
    ):
        self.sim_epoch: Optional[datetime] = sim_epoch
        self.real_epoch: Optional[float] = real_epoch
        self.speed: float = speed

    async def sync(self, provider_url: str, client: Optional[httpx.AsyncClient] = None) -> SimClock:
        """Fetch clock parameters from GET /v1/clock."""
        url = f"{provider_url.rstrip('/')}/v1/clock"
        if client is not None:
            resp = await client.get(url, timeout=10.0)
            data = resp.json()
        else:
            async with httpx.AsyncClient(timeout=10.0) as cl:
                resp = await cl.get(url)
                data = resp.json()

        self.sim_epoch = parse_ist(data["sim_epoch"])
        self.real_epoch = float(data["real_epoch"])
        self.speed = float(data["speed"])
        return self

    def set_manual(self, sim_epoch: datetime, real_epoch: float, speed: float = 600.0) -> None:
        """Manually configure clock (useful for unit tests or initialization)."""
        if sim_epoch.tzinfo is None:
            self.sim_epoch = sim_epoch.replace(tzinfo=IST)
        else:
            self.sim_epoch = sim_epoch.astimezone(IST)
        self.real_epoch = real_epoch
        self.speed = speed

    def now(self) -> datetime:
        """Compute current simulated time in IST."""
        if self.sim_epoch is None or self.real_epoch is None:
            raise RuntimeError("SimClock not synchronized. Call sync() or set_manual() first.")
        elapsed_real = time.time() - self.real_epoch
        sim_now = self.sim_epoch + timedelta(seconds=elapsed_real * self.speed)
        return sim_now.astimezone(IST)

    def sleep_sim_seconds(self, sim_seconds: float) -> float:
        """Returns the real seconds required to sleep for the given sim_seconds."""
        return max(0.0, sim_seconds / self.speed)

