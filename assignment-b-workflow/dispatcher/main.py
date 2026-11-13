from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

import uvicorn

from .config import Config
from .db import Database
from .engine import DispatcherEngine
from .webhook import create_webhook_app


def parse_args():
    parser = argparse.ArgumentParser(description="Durable Campaign Dispatcher")
    parser.add_argument(
        "--db",
        default=os.getenv("DB_PATH", "dispatcher_state.db"),
        help="Path to SQLite database file",
    )
    parser.add_argument(
        "--provider-url",
        default=os.getenv("PROVIDER_URL", "http://127.0.0.1:8099"),
        help="Telephony provider base URL",
    )
    parser.add_argument(
        "--webhook-host",
        default=os.getenv("WEBHOOK_HOST", "127.0.0.1"),
        help="Host for local webhook server",
    )
    parser.add_argument(
        "--webhook-port",
        type=int,
        default=int(os.getenv("WEBHOOK_PORT", "8200")),
        help="Port for local webhook server",
    )
    parser.add_argument(
        "--fixtures",
        default=os.getenv("FIXTURES_DIR", "fixtures"),
        help="Path to fixtures directory",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_CSV", "final_state.csv"),
        help="Path for output final_state.csv",
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Delete existing database before starting",
    )
    return parser.parse_args()


async def async_main(args):
    db_path = Path(args.db)
    if args.reset_db and db_path.exists():
        try:
            db_path.unlink()
            for suffix in ["-wal", "-shm"]:
                p = Path(f"{args.db}{suffix}")
                if p.exists():
                    p.unlink()
        except Exception:
            pass

    cfg = Config(
        provider_url=args.provider_url,
        webhook_host=args.webhook_host,
        webhook_port=args.webhook_port,
        db_path=args.db,
        fixtures_dir=args.fixtures,
        output_csv=args.output,
    )

    db = Database(args.db)
    engine = DispatcherEngine(db=db, config=cfg)
    app = create_webhook_app(db=db, on_call_ended_callback=engine.wakeup_event)

    uvicorn_config = uvicorn.Config(
        app=app,
        host=cfg.webhook_host,
        port=cfg.webhook_port,
        log_level="error",
    )
    server = uvicorn.Server(uvicorn_config)
    engine.uvicorn_server = server

    server_task = asyncio.create_task(server.serve())

    try:
        await engine.run()
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=1.0)
        except Exception:
            server_task.cancel()
            try:
                await server_task
            except Exception:
                pass
        db.close()

    print(f"Dispatcher finished successfully. Output written to {cfg.output_csv}")


def main():
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()

