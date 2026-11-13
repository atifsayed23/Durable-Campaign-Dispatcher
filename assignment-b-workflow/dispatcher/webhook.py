from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
from typing import Any, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .db import Database
from .state_machine import process_call_result


def create_webhook_app(
    db: Database,
    on_call_ended_callback: Optional[Any] = None,
) -> FastAPI:
    """
    Factory creating the FastAPI webhook receiver application.
    Enforces HMAC-SHA256 signature verification and idempotent call processing.
    """
    app = FastAPI(title="Dispatcher Webhook Receiver", version="1.0")
    secret_cache: dict[str, str] = {}

    @app.post("/webhooks/telephony")
    async def telephony_webhook(request: Request):
        raw_body = await request.body()
        sig = request.headers.get("x-au-signature") or request.headers.get("X-AU-Signature")
        if not sig:
            return JSONResponse({"error": "missing_signature"}, status_code=401)

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)

        # Lookup webhook secret with caching
        tenant_id = payload.get("tenant_id")
        campaign_id = payload.get("campaign_id")
        secret = None

        if tenant_id and tenant_id in secret_cache:
            secret = secret_cache[tenant_id]
        elif tenant_id:
            t_row = db.fetchone(
                "SELECT webhook_secret FROM tenants WHERE tenant_id = ?", (tenant_id,)
            )
            if t_row and t_row["webhook_secret"]:
                secret = t_row["webhook_secret"]
                secret_cache[tenant_id] = secret

        if not secret and campaign_id and campaign_id in secret_cache:
            secret = secret_cache[campaign_id]
        elif not secret and campaign_id:
            c_row = db.fetchone(
                "SELECT webhook_secret FROM campaigns WHERE campaign_id = ?", (campaign_id,)
            )
            if c_row and c_row["webhook_secret"]:
                secret = c_row["webhook_secret"]
                secret_cache[campaign_id] = secret

        if not secret:
            secret = secret_cache.get("__default__")
            if not secret:
                d_row = db.fetchone(
                    "SELECT webhook_secret FROM campaigns WHERE webhook_secret IS NOT NULL LIMIT 1"
                )
                secret = d_row["webhook_secret"] if d_row else "whsec_au_assignment_local"
                secret_cache["__default__"] = secret

        # Verify HMAC-SHA256 signature
        expected_sig = hmac.new(
            secret.encode("utf-8"), raw_body, hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected_sig, sig):
            return JSONResponse({"error": "invalid_signature"}, status_code=401)

        # Idempotently process call result
        process_call_result(db, payload)

        # Notify dispatcher loop to wake up immediately (freed concurrency slot)
        if on_call_ended_callback is not None:
            try:
                if hasattr(on_call_ended_callback, "set"):
                    on_call_ended_callback.set()
                elif callable(on_call_ended_callback):
                    res = on_call_ended_callback()
                    if inspect.isawaitable(res):
                        asyncio.create_task(res)
            except Exception:
                pass

        return JSONResponse({"ok": True}, status_code=200)

    @app.get("/status")
    async def dispatcher_status():
        """
        Observability endpoint: current status of campaigns and tenant concurrency.
        """
        rows = db.fetchall(
            """
            SELECT campaign_id, state, COUNT(*) as cnt
            FROM leads
            GROUP BY campaign_id, state
            """
        )
        campaign_stats: dict[str, dict[str, int]] = {}
        for r in rows:
            cid = r["campaign_id"]
            if cid not in campaign_stats:
                campaign_stats[cid] = {}
            campaign_stats[cid][r["state"]] = r["cnt"]

        tenant_rows = db.fetchall(
            """
            SELECT tenant_id, COUNT(*) as active_cnt
            FROM call_attempts
            WHERE state IN ('INTENT', 'ACTIVE', 'UNKNOWN_504')
            GROUP BY tenant_id
            """
        )
        active_tenants = {r["tenant_id"]: r["active_cnt"] for r in tenant_rows}

        return {
            "campaigns": campaign_stats,
            "active_tenant_concurrency": active_tenants,
        }

    return app

