# Submission

## Candidate
- **Name:** Mohammed Atif Sayed
- **Assignment chosen:** Assigment B (Workflow)
- **Time spent (honest estimate):** 5-6h

## How to run
```bash
# 1. Setup virtual environment and install dependencies from clean clone
python -m venv .venv
# Linux / macOS:
source .venv/bin/activate
# Windows (PowerShell):
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt

# 2. Run the automated test suite with one command (runs all 20 tests in ~2s)
pytest

# 3. Start the mock telephony carrier (Terminal 1)
cd assignment-b-workflow
python -m mock_telephony --speed 1800

# 4. Run the durable campaign dispatcher (Terminal 2)
cd assignment-b-workflow
python -m dispatcher --reset-db

# 5. Check live progress and observability (optional, while running)
curl http://127.0.0.1:8200/status

# 6. Verify grading report once dispatcher exits with final_state.csv
curl http://127.0.0.1:8099/admin/report
```

## Design

### Architecture Overview
We implemented the dispatcher as a single-node, event-driven Python `asyncio` service backed by an embedded SQLite database configured with **Write-Ahead Logging (WAL)** and serialized write transactions (`BEGIN IMMEDIATE ... COMMIT/ROLLBACK`).

**Why SQLite WAL + `BEGIN IMMEDIATE` over Temporal / Redis?**
- **Zero External Infrastructure Failures:** Orchestrators like Temporal or Redis require running Docker daemons, background workers, and persistent message brokers. By embedding SQLite WAL in-process, there are no external network boundaries, worker registration delays, or broker disconnections.
- **Single Atomic Source of Truth:** Multi-tenant concurrency limits (`R2`), single active call per phone (`R3`), and priority queue claims (`R6`) are resolved atomically inside SQLite using `BEGIN IMMEDIATE`.
- **Sub-Millisecond Webhook Wakeup:** When a call completes, the FastAPI webhook handler signals an `asyncio.Event`, immediately awakening the dispatch loop in `< 1ms` to claim the newly freed tenant slot without polling lag.

### Lead State Machine
Every lead transitions through a deterministic state machine:
- **`WAITING`**: Eligible to be claimed when the campaign window is open and `next_eligible_at <= sim_now`.
- **`DIALING`**: Locked while a call attempt is in-flight (`INTENT`, `ACTIVE`, or `UNKNOWN_504`).
- **`COMPLETED`**: Terminal disposition reached (`interested`, `not_interested`, `wrong_number`, or triggering `dnd`).
- **`EXHAUSTED`**: `attempts >= max_attempts` without a terminal disposition.
- **`DND_BLOCKED`**: The lead's phone number was opted out by another call across any campaign.

Leads are queried with strict priority ordering:
`ORDER BY priority_tier ASC, next_eligible_at ASC, campaign_id ASC, lead_id ASC`
where `priority_tier` is **`1 = CALLBACK`**, **`2 = RETRY`**, and **`3 = FRESH`**.

### Write-Ahead Intent Ledger (`call_attempts`)
To guarantee at-most-once dialling and survive arbitrary mid-flight `kill -9` crashes, all dialing actions follow a strict Write-Ahead Intent pattern:

```
                  ┌────────────────────────┐
                  │      WAITING Lead      │
                  └───────────┬────────────┘
                              │
                              ▼ claim_next_calls() (Atomic: lead='DIALING')
                         ┌─────────┐
                         │ INTENT  │
                         └────┬────┘
                              │ POST /v1/calls (request_id)
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
        201 Created       503 / 4xx       504 Timeout / Network Error
       ┌──────────┐     ┌───────────┐     ┌─────────────┐
       │  ACTIVE  │     │  ABORTED  │     │ UNKNOWN_504 │
       └────┬─────┘     └───────────┘     └──────┬──────┘
            │          (lead->WAITING,           │
            │          attempts uncounted)       │ Background GET /v1/calls?request_id=...
            ├────────────────────────────────────┼─────────────────────────┐
            │                                    │                         │
            ▼ Webhook call.completed             ▼ Found (200 OK)          ▼ Not Found (404)
       ┌──────────┐                         ┌──────────┐             ┌───────────┐
       │  ENDED   │                         │  ENDED   │             │  ABORTED  │
       └──────────┘                         └──────────┘             └───────────┘
```

1. **Before dialing `POST /v1/calls`**: An intent record is inserted into `call_attempts` with a unique deterministic/UUID `request_id` (`req_{campaign_id}_{lead_id}_{attempt_no}_{uuid}`), and the lead is marked `DIALING`.
2. **If the process is killed (`kill -9`)**: On reboot, `reconcile_in_flight(force_all=True)` queries the provider (`GET /v1/calls?request_id=...`). If 404, the call was never placed, so it is marked `ABORTED` and the lead reverts to `WAITING` with zero attempts counted. If 200, the call was placed, its `call_sid` is recorded, and the result is ingested.
3. **Early Webhook Guard**: If a completion webhook arrives before the HTTP `201` response finishes, `process_call_result()` marks the attempt `ENDED` and increments `attempts`. When `mark_call_accepted()` eventually runs, it detects the `ENDED` state and safely avoids overriding the attempt back to `ACTIVE`.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| **Counting `INTENT` and `UNKNOWN_504` toward tenant concurrency (`R2`) & phone locks (`R3`)** | Prevents race conditions and over-dialling while network requests or timeout reconciliations are in flight. Trades a momentary slot hold during 504 resolution for a 100% guarantee of zero concurrency violations. | Only counting `ACTIVE` calls (risks exceeding limits if multiple in-flight dials succeed concurrently). |
| **Event-driven dispatch loop (`asyncio.Event` + 5ms timeout)** | Webhook arrival triggers `wakeup_event.set()`, waking the hot loop in `< 1ms` to claim the newly freed concurrency slot. Minimizes `callback_lateness_minutes` without aggressive polling. | Coarse polling sleep loop (e.g. `sleep(0.5)`), which introduces substantial latency at high simulation speeds. |
| **Dynamic window closing safety buffer (`max(1800, int(2.0 * speed))`)** | At high speeds (1800x–3600x), a fixed 30-second window margin is only 8–16ms of real time. A dynamic buffer ensures at least 2.0 real seconds of safety margin before window closing, eliminating `outside_calling_window` violations caused by asyncio scheduling or network jitter. | Fixed 30s buffer (fails under network latency at 3600x). |
| **Independent HTTP connection pools (`dial_client` vs `reconcile_client`)** | Outbound dialing and background reconciliation sweeps run on separate HTTP connection pools, preventing reconciliation GET requests from starving outbound POST dials. | A single shared `httpx.AsyncClient`. |
| **In-memory Webhook Secret Caching** | Webhook verification requires comparing HMAC-SHA256 signatures; caching secrets avoids synchronous database reads on every incoming webhook. | Querying `campaigns` table on every inbound webhook. |
| **Self-healing `DIALING` lead restoration** | If any lead is in `state = 'DIALING'` but has no active attempt in `call_attempts` (due to an unhandled crash or abort), `claim_next_calls()` and `reconcile_in_flight()` automatically restore it to `WAITING`. | Letting orphaned leads remain stuck in `DIALING` forever. |

## Failure modes considered

- **Transient `503 Service Unavailable`**: Marked as `ABORTED`, lead reverts to `WAITING` without incrementing `attempts`, concurrency slot released immediately.
- **Ambiguous `504 Upstream Timeout`**: Marked as `UNKNOWN_504`. Reconciled asynchronously via `GET /v1/calls?request_id=...`. If 404, safely aborted; if 200, tracked to completion. Any unresolved 504 older than 4 real seconds (~2 simulated hours) is safely aborted.
- **Lost Webhooks (3% Carrier Drop)**: Background sweeper periodically scans `ACTIVE` attempts older than 0.5s real time, querying `GET /v1/calls/{call_sid}` to ingest terminal results.
- **Duplicate Webhooks**: `process_call_result()` verifies if `attempt['state'] == 'ENDED'`. If already ended, it exits idempotently without double-counting attempts or triggering secondary transitions.
- **Early Webhooks (Arriving before HTTP 201)**: The attempt transitions to `ENDED` immediately. When the HTTP 201 response returns, `mark_call_accepted()` checks for `state == 'ENDED'` and preserves the terminal state.
- **Mid-Flight Process Crash (`kill -9`)**: Startup sweep reconciles all unresolved `INTENT`, `UNKNOWN_504`, and `ACTIVE` calls with the carrier before dispatching new leads.
- **What is NOT handled**: Multi-machine active-active clustering sharing a single SQLite file (SQLite is single-writer). For horizontal scaling across multiple dispatcher workers, SQLite `BEGIN IMMEDIATE` would be migrated to PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`.

## Testing

### Automated Test Suite (`pytest` — 20 Unit Tests)
1. **`tests/test_rules.py` (6 tests)**:
   - Pure math tests using fake clock for calling window intersections (`[start, end) ∩ [09:00, 21:00)` IST).
   - Daylight window roll-overs and Sunday skipping.
   - Deterministic retry backoffs (`failed` = 15m, `no_answer` = 60m, `busy` = 60m).
   - Callback scheduling with exact priority tier assignment.
   - Terminal disposition mapping and max attempts exhaustion.
2. **`tests/test_state_machine.py` (6 tests)**:
   - Atomic enforcement of tenant concurrency limits (`R2`) and phone uniqueness (`R3`).
   - Priority ordering (`R6`): callbacks > retries > fresh leads.
   - Early webhook arrival race condition and duplicate idempotency.
   - 503 rollback freeing tenant capacity without incrementing attempt counts.
   - Global DND propagation (`R8`) and precedence (`R9`: `completed > exhausted > dnd_blocked`).
   - Self-healing recovery of orphaned `DIALING` leads.
3. **`tests/test_reconciler.py` (1 test)**:
   - Simulated `kill -9` crash recovery using `httpx.MockTransport` covering:
     - 404 abort and clean rollback of unplaced intents.
     - Ambiguous 504 resolution into completed leads.
     - Lost webhook recovery for active calls.
4. **`tests/test_webhook_and_exporter.py` (5 tests)**:
   - HMAC-SHA256 signature verification (`X-AU-Signature`) with valid, missing, and corrupted keys.
   - Observability `/status` endpoint returning live campaign and concurrency stats.
   - Idempotent export of `final_state.csv` sorted by `campaign_id, lead_id`.
5. **`tests/test_loader.py` (2 tests)**:
   - Idempotent ingestion of campaigns and leads from `fixtures/`.
   - Initial DND cascade upon fixture load.

## Results

### Summary from Verification Run at `--speed 1800`
- **Total Leads:** 200
- **Final Dispositions:**
  - `COMPLETED`: 172
  - `EXHAUSTED`: 24
  - `DND_BLOCKED`: 4
- **Mock Telephony `/admin/report` Summary:**
  - `violation_count`: **`0`**
  - `calls_active`: `0`
  - `max_concurrency_seen`: `{'acme_insurance': 4, 'zen_retail': 3}` (exact saturation, 0 violations)
  - `callback_lateness_minutes`: `p50 = 0.0, p90 = 0.1` (near-zero lateness)
- **Parity:** `final_state.csv` matches `/admin/leads` 100%.

## If I had another day

1. **PostgreSQL Horizontal Worker Scaling:** Replace SQLite's single-writer lock with PostgreSQL `SELECT lead_id FROM leads WHERE state = 'WAITING' ... FOR UPDATE SKIP LOCKED LIMIT N`, allowing 10+ dispatcher nodes to run concurrently across millions of leads.
2. **Dynamic Backpressure & Adaptive Dialing:** Implement AIMD (Additive Increase / Multiplicative Decrease) rate limiting when upstream carrier returns 503 or 429 status codes.
3. **Prometheus Metrics & Distributed Tracing:** Instrument Prometheus counters for dial throughput, webhook latency, and OpenTelemetry trace spans connecting `POST /v1/calls` with incoming webhooks.
