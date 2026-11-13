from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional, Union

from .clock import IST, parse_ist

# ---------------------------------------------------------------------------
# Priority Constants (R6: due callbacks > retries > fresh leads)
# ---------------------------------------------------------------------------
PRIORITY_CALLBACK = 1
PRIORITY_RETRY = 2
PRIORITY_FRESH = 3

# Terminal Dispositions (R7: ends the lead immediately)
TERMINAL_DISPOSITIONS = frozenset({"interested", "not_interested", "wrong_number", "dnd"})

# Days of week mapping (matching mock_telephony)
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

# India National Regulatory Calling Window (R1)
REGULATORY_START = "09:00"
REGULATORY_END = "21:00"


def _hm(s: str) -> tuple[int, int]:
    parts = s.split(":")
    return int(parts[0]), int(parts[1])


def _normalize_days(days: Union[list[str], str]) -> list[str]:
    if isinstance(days, str):
        return [d.strip().lower() for d in days.split(",") if d.strip()]
    return [d.lower() for d in days]


# ---------------------------------------------------------------------------
# Calling Window Logic (R1)
# ---------------------------------------------------------------------------
def effective_window(window_start: str, window_end: str) -> tuple[time, time]:
    """
    Intersect campaign calling window [window_start, window_end) with
    India's regulatory window [09:00, 21:00) IST.
    """
    c_sh, c_sm = _hm(window_start)
    c_eh, c_em = _hm(window_end)
    r_sh, r_sm = _hm(REGULATORY_START)
    r_eh, r_em = _hm(REGULATORY_END)

    c_s = time(c_sh, c_sm)
    c_e = time(c_eh, c_em)
    r_s = time(r_sh, r_sm)
    r_e = time(r_eh, r_em)

    eff_start = max(c_s, r_s)
    eff_end = min(c_e, r_e)
    if eff_start >= eff_end:
        raise ValueError(
            f"Invalid calling window: effective start {eff_start} >= effective end {eff_end}"
        )
    return eff_start, eff_end


def is_within_calling_window(
    sim_now: datetime,
    days: Union[list[str], str],
    window_start: str,
    window_end: str,
    close_buffer_sec: int = 30,
) -> bool:
    """
    Check if sim_now falls on an allowed day and within [effective_start, effective_end - close_buffer_sec).
    A close_buffer_sec prevents dispatching right before window closing so in-flight requests do not
    cross closing boundary.
    """
    lt = sim_now.astimezone(IST)
    allowed_days = _normalize_days(days)
    day_name = DAYS[lt.weekday()]
    if day_name not in allowed_days:
        return False

    eff_start, eff_end = effective_window(window_start, window_end)
    cur_date = lt.date()
    s_dt = datetime.combine(cur_date, eff_start, tzinfo=IST)
    e_dt = datetime.combine(cur_date, eff_end, tzinfo=IST)
    e_buffered = e_dt - timedelta(seconds=close_buffer_sec)

    return s_dt <= lt < e_buffered


def next_window_opening(
    target_dt: datetime,
    days: Union[list[str], str],
    window_start: str,
    window_end: str,
) -> datetime:
    """
    Returns earliest instant >= target_dt that falls inside an allowed window.
    - If target_dt is already inside [effective_start, effective_end) on an allowed day, returns target_dt.
    - If target_dt is before effective_start on an allowed day, returns effective_start on that day.
    - If target_dt is at/after effective_end or on a disallowed day, rolls forward to the effective_start
      of the next allowed day.
    """
    lt = target_dt.astimezone(IST)
    allowed_days = _normalize_days(days)
    eff_start, eff_end = effective_window(window_start, window_end)

    for d in range(0, 14):
        probe_date = (lt + timedelta(days=d)).date()
        day_name = DAYS[probe_date.weekday()]
        if day_name not in allowed_days:
            continue

        s_dt = datetime.combine(probe_date, eff_start, tzinfo=IST)
        e_dt = datetime.combine(probe_date, eff_end, tzinfo=IST)

        if d == 0:
            if lt < s_dt:
                return s_dt
            if lt < e_dt:
                return lt  # Already open
            continue  # lt >= e_dt: window closed today, move to subsequent days

        return s_dt

    return lt


# ---------------------------------------------------------------------------
# Post-Call Lead Transition Logic (R4, R5, R6, R7, R8, R9)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LeadTransition:
    new_state: str  # COMPLETED | EXHAUSTED | WAITING | DND_BLOCKED
    priority_tier: int  # 1 (Callback), 2 (Retry), 3 (Fresh)
    next_eligible_at: Optional[datetime]
    callback_at: Optional[datetime]
    is_dnd: bool


def compute_next_lead_transition(
    attempts: int,
    max_attempts: int,
    status: str,
    disposition: Optional[str],
    ended_at: Union[datetime, str],
    callback_at: Optional[Union[datetime, str]],
    window_days: Union[list[str], str],
    window_start: str,
    window_end: str,
    no_answer_wait_min: int = 60,
    busy_wait_min: int = 60,
    failed_wait_min: int = 15,
) -> LeadTransition:
    """
    Pure deterministic transition calculation for a lead after an attempt completes.
    Strictly follows rules R4–R9 and matches mock_telephony /admin/leads evaluation:
    - Terminal dispositions (interested, not_interested, wrong_number, dnd) -> COMPLETED immediately.
    - If attempts >= max_attempts and not terminal -> EXHAUSTED.
    - Callbacks -> WAITING with PRIORITY_CALLBACK (1), scheduled at next_window_opening(max(callback_at, ended_at)).
    - Retries (no_answer, busy, failed) -> WAITING with PRIORITY_RETRY (2), scheduled at next_window_opening(ended_at + wait).
    """
    end_dt = parse_ist(ended_at) if isinstance(ended_at, str) else ended_at.astimezone(IST)
    disp = disposition.strip().lower() if disposition else None
    st = status.strip().lower()

    # R7 & R8: Terminal dispositions immediately complete the lead
    if st == "completed" and disp in TERMINAL_DISPOSITIONS:
        return LeadTransition(
            new_state="COMPLETED",
            priority_tier=PRIORITY_FRESH,
            next_eligible_at=None,
            callback_at=None,
            is_dnd=(disp == "dnd"),
        )

    # R4 & R9: Non-terminal outcomes that reach or exceed max_attempts become EXHAUSTED
    if attempts >= max_attempts:
        return LeadTransition(
            new_state="EXHAUSTED",
            priority_tier=PRIORITY_FRESH,
            next_eligible_at=None,
            callback_at=None,
            is_dnd=False,
        )

    # R6: Callback requested
    if st == "completed" and disp == "callback_requested":
        if not callback_at:
            cb_dt = end_dt + timedelta(minutes=60)
        else:
            cb_dt = parse_ist(callback_at) if isinstance(callback_at, str) else callback_at.astimezone(IST)

        earliest = max(cb_dt, end_dt)
        scheduled_dt = next_window_opening(earliest, window_days, window_start, window_end)
        return LeadTransition(
            new_state="WAITING",
            priority_tier=PRIORITY_CALLBACK,
            next_eligible_at=scheduled_dt,
            callback_at=cb_dt,
            is_dnd=False,
        )

    # R5: Retries
    if st == "failed":
        wait_min = failed_wait_min
    elif st in ("no_answer", "busy"):
        wait_min = busy_wait_min if st == "busy" else no_answer_wait_min
    else:
        wait_min = 60  # Default fallback retry wait

    earliest_retry = end_dt + timedelta(minutes=wait_min)
    scheduled_dt = next_window_opening(earliest_retry, window_days, window_start, window_end)

    return LeadTransition(
        new_state="WAITING",
        priority_tier=PRIORITY_RETRY,
        next_eligible_at=scheduled_dt,
        callback_at=None,
        is_dnd=False,
    )

