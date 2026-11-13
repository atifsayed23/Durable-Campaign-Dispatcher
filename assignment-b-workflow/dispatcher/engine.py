from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Optional, Set

import httpx

from .clock import SimClock
from .config import Config, config as default_config
from .db import Database
from .exporter import export_final_state_csv, is_all_leads_terminal
from .loader import load_fixtures
from .rules import is_within_calling_window
from .state_machine import (
    claim_next_calls,
    mark_call_aborted_503,
    mark_call_accepted,
    mark_call_unknown_504,
    process_call_result,
)


class DispatcherEngine:
    """
    Core Dispatcher Engine managing the lifecycle, write-ahead dialing,
    reconciliation of in-flight/lost calls, and graceful completion.
    """

    def __init__(
        self,
        db: Database,
        clock: Optional[SimClock] = None,
        config: Optional[Config] = None,
    ):
        self.db = db
        self.config = config or default_config
        self.clock = clock or SimClock()
        self.wakeup_event = asyncio.Event()
        self.in_flight_requests: Set[str] = set()
        self.active_tasks: Set[asyncio.Task] = set()
        self.stopped = False
        self.uvicorn_server: Optional[Any] = None
        self._last_clock_sync_real = 0.0
        self._last_status_print_real = 0.0

        limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
        self.dial_client: Optional[httpx.AsyncClient] = None
        self.reconcile_client: Optional[httpx.AsyncClient] = None

    async def wait_for_webhook_server(self, max_wait_sec: float = 10.0) -> None:
        """Ensure local webhook receiver is healthy and accepting requests."""
        url = f"http://{self.config.webhook_host}:{self.config.webhook_port}/status"
        start = time.time()
        async with httpx.AsyncClient(timeout=1.0) as cl:
            while time.time() - start < max_wait_sec:
                try:
                    resp = await cl.get(url)
                    if resp.status_code == 200:
                        return
                except Exception:
                    await asyncio.sleep(0.05)
        raise RuntimeError(
            f"Webhook receiver on {url} failed to become ready within {max_wait_sec}s"
        )

    async def sync_clock_with_retry(self, max_retries: int = 20) -> None:
        """Sync with simulated clock from provider with retry."""
        async with httpx.AsyncClient(timeout=5.0) as cl:
            for attempt in range(max_retries):
                try:
                    await self.clock.sync(self.config.provider_url, cl)
                    self._last_clock_sync_real = time.time()
                    return
                except Exception:
                    await asyncio.sleep(0.2)
        raise ConnectionError(
            f"Could not connect to mock telephony provider at {self.config.provider_url}"
        )

    async def reconcile_single(
        self, client: httpx.AsyncClient, request_id: str, call_sid: Optional[str] = None
    ) -> None:
        """
        Reconcile a single call attempt via GET /v1/calls.
        Handles recovery from 504 timeouts, lost webhooks, and crash recovery.
        """
        base_url = self.config.provider_url.rstrip("/")
        if call_sid:
            url = f"{base_url}/v1/calls/{call_sid}"
        else:
            url = f"{base_url}/v1/calls?request_id={request_id}"

        try:
            resp = await client.get(url, timeout=2.0)
            if resp.status_code == 200:
                data = resp.json()
                sid = data.get("call_sid") or call_sid
                if data.get("state") == "ended" and data.get("result"):
                    process_call_result(self.db, data["result"])
                    self.wakeup_event.set()
                elif data.get("state") == "active" and sid:
                    mark_call_accepted(self.db, request_id, sid)
            elif resp.status_code == 404:
                # If not found on provider, it was never placed before crash or aborted
                attempt = self.db.fetchone(
                    "SELECT state FROM call_attempts WHERE request_id = ?",
                    (request_id,),
                )
                if attempt and attempt["state"] in ("INTENT", "UNKNOWN_504"):
                    mark_call_aborted_503(self.db, request_id, self.clock.now())
                    self.wakeup_event.set()
        except Exception:
            pass

    async def reconcile_in_flight(
        self, client: httpx.AsyncClient, force_all: bool = False
    ) -> None:
        """
        Scan for unresolved call attempts and reconcile with provider concurrently.
        Catches:
        1. Self-healing: Revert any lead in DIALING with no active attempt.
        2. INTENT / UNKNOWN_504 rows leftover from a prior process crash or timeout.
        3. ACTIVE calls that finished but whose webhooks were lost.
        """
        # Self-healing check: restore any DIALING lead with no active attempt
        with self.db.atomic():
            self.db.execute(
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

        rows = self.db.fetchall(
            """
            SELECT request_id, call_sid, state, created_at_real
            FROM call_attempts
            WHERE state IN ('INTENT', 'UNKNOWN_504', 'ACTIVE')
            """
        )
        now_real = time.time()
        tasks = []
        for r in rows:
            rid = r["request_id"]
            if rid in self.in_flight_requests:
                continue

            state = r["state"]
            age_real = now_real - r["created_at_real"]

            if state in ("INTENT", "UNKNOWN_504"):
                tasks.append(self.reconcile_single(client, rid, r["call_sid"]))
            elif state == "ACTIVE" and (force_all or age_real > 0.5):
                tasks.append(self.reconcile_single(client, rid, r["call_sid"]))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def place_single_call(
        self, client: httpx.AsyncClient, claim: dict[str, Any]
    ) -> None:
        """
        Place a single call with write-ahead intent tracking and error handling.
        """
        request_id = claim["request_id"]
        cid = claim["campaign_id"]

        try:
            # Check window immediately before firing request (buffer: 1800 sim seconds = 30 min)
            camp = self.db.fetchone(
                "SELECT * FROM campaigns WHERE campaign_id = ?", (cid,)
            )
            sim_now = self.clock.now()
            buffer_sec = max(1800, int(2.0 * self.clock.speed))
            if not is_within_calling_window(
                sim_now=sim_now,
                days=camp["window_days"],
                window_start=camp["window_start"],
                window_end=camp["window_end"],
                close_buffer_sec=buffer_sec,
            ):
                mark_call_aborted_503(self.db, request_id, sim_now)
                return

            payload = {
                "request_id": request_id,
                "campaign_id": claim["campaign_id"],
                "lead_id": claim["lead_id"],
                "phone": claim["phone"],
                "callback_url": self.config.callback_url,
            }
            url = f"{self.config.provider_url.rstrip('/')}/v1/calls"

            resp = await client.post(url, json=payload, timeout=5.0)

            if resp.status_code == 201:
                data = resp.json()
                mark_call_accepted(self.db, request_id, data["call_sid"])
            elif resp.status_code == 200:
                data = resp.json()
                mark_call_accepted(self.db, request_id, data["call_sid"])
                if data.get("state") == "ended" and data.get("result"):
                    process_call_result(self.db, data["result"])
            elif resp.status_code == 503:
                mark_call_aborted_503(self.db, request_id, self.clock.now())
                self.wakeup_event.set()
            elif resp.status_code == 504:
                # Ambiguous timeout: mark UNKNOWN_504 and spawn reconciliation in background
                mark_call_unknown_504(self.db, request_id)
                self.in_flight_requests.discard(request_id)
                self.wakeup_event.set()
                if self.reconcile_client:
                    asyncio.create_task(self.reconcile_single(self.reconcile_client, request_id))
            else:
                # 4xx rejection or unexpected error
                mark_call_aborted_503(self.db, request_id, self.clock.now())
                self.wakeup_event.set()

        except (httpx.TimeoutException, httpx.RequestError):
            mark_call_unknown_504(self.db, request_id)
            self.in_flight_requests.discard(request_id)
            self.wakeup_event.set()
            if self.reconcile_client:
                asyncio.create_task(self.reconcile_single(self.reconcile_client, request_id))
        except Exception:
            mark_call_aborted_503(self.db, request_id, self.clock.now())
            self.wakeup_event.set()
        finally:
            self.in_flight_requests.discard(request_id)
            self.wakeup_event.set()

    async def _clock_sync_loop(self) -> None:
        """Periodic clock synchronization in the background every 5 seconds."""
        while not self.stopped:
            try:
                await asyncio.sleep(5.0)
                if not self.stopped and self.dial_client:
                    await self.clock.sync(self.config.provider_url, self.dial_client)
                    self._last_clock_sync_real = time.time()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _sweeper_loop(self) -> None:
        """Background sweeper resolving lost webhooks and ambiguous 504s rapidly."""
        while not self.stopped:
            try:
                await asyncio.sleep(0.5)
                if not self.stopped and self.reconcile_client:
                    await self.reconcile_in_flight(self.reconcile_client)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def run(self) -> None:
        """
        Main execution loop for the dispatcher.
        Runs until all leads reach a terminal state and exports final_state.csv.
        """
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
        self.dial_client = httpx.AsyncClient(timeout=5.0, limits=limits)
        self.reconcile_client = httpx.AsyncClient(timeout=3.0, limits=limits)

        try:
            # 1. Wait for local webhook server to be ready
            await self.wait_for_webhook_server()

            # 2. Sync clock with provider
            await self.sync_clock_with_retry()

            # 3. Idempotently load fixtures
            fixtures_path = self.config.resolve_fixtures_dir()
            load_fixtures(self.db, fixtures_path)

            # 4. Crash Recovery: Reconcile any left-over in-flight states from prior runs
            await self.reconcile_in_flight(self.reconcile_client, force_all=True)

            # 5. Start background sweeper and clock sync
            sweeper_task = asyncio.create_task(self._sweeper_loop())
            sync_task = asyncio.create_task(self._clock_sync_loop())

            try:
                while not self.stopped:
                    # Check if all leads reached terminal state
                    if is_all_leads_terminal(self.db) and len(self.in_flight_requests) == 0:
                        export_final_state_csv(self.db, self.config.output_csv)
                        self.stopped = True
                        if self.uvicorn_server is not None:
                            self.uvicorn_server.should_exit = True
                        sim_now = self.clock.now()
                        sim_str = sim_now.strftime("%Y-%m-%d %H:%M:%S")
                        rows = self.db.fetchall("SELECT state, COUNT(*) as cnt FROM leads GROUP BY state")
                        counts = {r["state"]: r["cnt"] for r in rows}
                        print(
                            f"[{sim_str}] WAITING={counts.get('WAITING', 0)} DIALING={counts.get('DIALING', 0)} "
                            f"COMPLETED={counts.get('COMPLETED', 0)} EXHAUSTED={counts.get('EXHAUSTED', 0)} "
                            f"DND_BLOCKED={counts.get('DND_BLOCKED', 0)} active_attempts=0",
                            flush=True,
                        )
                        break

                    now_real = time.time()
                    sim_now = self.clock.now()

                    # Print status line every 2 real seconds
                    if now_real - self._last_status_print_real >= 2.0:
                        rows = self.db.fetchall("SELECT state, COUNT(*) as cnt FROM leads GROUP BY state")
                        counts = {r["state"]: r["cnt"] for r in rows}
                        w = counts.get("WAITING", 0)
                        d = counts.get("DIALING", 0)
                        c = counts.get("COMPLETED", 0)
                        e = counts.get("EXHAUSTED", 0)
                        b = counts.get("DND_BLOCKED", 0)
                        act_row = self.db.fetchone(
                            "SELECT COUNT(*) as cnt FROM call_attempts WHERE state IN ('INTENT', 'ACTIVE', 'UNKNOWN_504')"
                        )
                        act = act_row["cnt"] if act_row else 0
                        sim_str = sim_now.strftime("%Y-%m-%d %H:%M:%S")
                        print(
                            f"[{sim_str}] WAITING={w} DIALING={d} COMPLETED={c} EXHAUSTED={e} DND_BLOCKED={b} active_attempts={act}",
                            flush=True,
                        )
                        self._last_status_print_real = now_real

                    # Clear wakeup event immediately before claiming next calls
                    self.wakeup_event.clear()

                    # Claim and launch eligible calls
                    buffer_sec = max(1800, int(2.0 * self.clock.speed))
                    claims = claim_next_calls(self.db, sim_now, close_buffer_sec=buffer_sec)
                    for claim in claims:
                        # Add immediately to in_flight_requests before spawning task
                        self.in_flight_requests.add(claim["request_id"])
                        task = asyncio.create_task(self.place_single_call(self.dial_client, claim))
                        self.active_tasks.add(task)
                        task.add_done_callback(self.active_tasks.discard)

                    # Sleep briefly or wait for webhook wakeup event (5ms timeout!)
                    try:
                        await asyncio.wait_for(self.wakeup_event.wait(), timeout=0.005)
                    except asyncio.TimeoutError:
                        pass

            finally:
                sweeper_task.cancel()
                sync_task.cancel()
                try:
                    await asyncio.gather(sweeper_task, sync_task, return_exceptions=True)
                except Exception:
                    pass
                if self.active_tasks:
                    await asyncio.gather(*list(self.active_tasks), return_exceptions=True)

        finally:
            await self.dial_client.aclose()
            await self.reconcile_client.aclose()
