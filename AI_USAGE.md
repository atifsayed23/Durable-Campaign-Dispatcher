# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
- Gemini 2.5 Pro via Antigravity IDE Agent

## What I used them for
- **Architecture & Planning**: Designing the SQLite WAL schema with indexing, formalizing the Write-Ahead Intent ledger (`INTENT` -> `ACTIVE` / `UNKNOWN_504` -> `ENDED` / `ABORTED`), and mapping rule dependencies (`R1`–`R9`).
- **Code Generation & Scaffolding**: Generating the modular package structure in `dispatcher/` (`clock.py`, `config.py`, `db.py`, `rules.py`, `state_machine.py`, `webhook.py`, `exporter.py`, `engine.py`, `main.py`).
- **Unit Test Suite Generation**: Drafting 20 comprehensive unit tests across `tests/` including fake-clock window math, priority sorting, early-webhook race condition simulation, and `httpx.MockTransport` crash-recovery tests.

## What I wrote or decided myself
- **Architectural Trade-Off (Zero External Dependencies)**: Made the explicit design decision to reject Temporal/Redis in favor of a single-node Python `asyncio` + SQLite WAL architecture utilizing `BEGIN IMMEDIATE` exclusive write locks to guarantee strict local reproducibility, zero Docker setup, and deterministic crash recovery.
- **Dynamic Window Close Guard**: Identified that at high simulation clock speeds (`1800x`–`3600x`), a static 30-simulated-second buffer is only 8.3ms–16.6ms in real time, causing latency-induced window boundary violations. Formulated the dynamic buffer `buffer_sec = max(1800, int(2.0 * speed))` to guarantee a safe 2.0-real-second buffer margin.
- **Dual-Client Connection Isolation**: Separated outbound dials (`dial_client`) from background reconciliation sweeps (`reconcile_client`) to ensure background recovery sweeps never saturate connection limits or starve outbound dial calls.
- **Self-Healing State Restoration**: Formulated the self-healing SQL query executed on startup and during dialing sweeps that automatically resets orphaned `DIALING` leads to `WAITING` if no active attempt ledger row exists.

## Where the AI was wrong or unhelpful
- **504 Reconciler Concurrency Deadlock**: During live simulation testing, throughput abruptly stalled after ~16 leads with `DIALING=6` and `active_attempts=0`. Upon auditing the code, I discovered the AI had written an early return `if request_id in self.in_flight_requests: return` inside `reconcile_single()`. Because `place_single_call` held `request_id` in `in_flight_requests` until its `finally` block, `reconcile_single` returned immediately without making the GET request. The attempt remained permanently stranded in `UNKNOWN_504`, which consumed tenant concurrency until all slots were blocked. I fixed this by removing the request from `in_flight_requests` before triggering the background reconciler.
- **Clock Sync Blocking in the Hot Loop**: An earlier draft called `clock.sync()` inline within the main dispatch loop whenever 5 seconds elapsed. At `1800x` speed, waiting 100ms for an HTTP GET round-trip stalled the loop for 180 simulated seconds, introducing unacceptable `callback_lateness_minutes`. I refactored clock synchronization into an independent background task (`_clock_sync_loop`) so the hot dispatch loop evaluates time purely in-memory in nanoseconds.

## How I verified AI-generated code
- **Line-by-Line Code Review**: Audited every database query, lock scope, and error branch to ensure strict `BEGIN IMMEDIATE` transaction boundaries and prevent SQL deadlocks.
- **Comprehensive Unit Testing**: Ran `pytest` locally after every module implementation, ensuring all 20 tests pass in under 2 seconds.
- **Live Carrier Simulation Verification**: Executed full end-to-end runs against `mock_telephony` across various simulation speeds (`600x`, `1200x`, `1800x`), auditing the mock server's `/admin/report` endpoint to verify `violation_count == 0` and diffing `final_state.csv` against `/admin/leads`.
