from __future__ import annotations

from datetime import datetime, time, timedelta
import pytest

from dispatcher.clock import IST
from dispatcher.rules import (
    PRIORITY_CALLBACK,
    PRIORITY_RETRY,
    PRIORITY_FRESH,
    TERMINAL_DISPOSITIONS,
    effective_window,
    is_within_calling_window,
    next_window_opening,
    compute_next_lead_transition,
)


def test_regulatory_window_clamping():
    # Case 1: Campaign wider than regulatory [08:00, 22:00) -> clamped to [09:00, 21:00)
    s, e = effective_window("08:00", "22:00")
    assert s == time(9, 0)
    assert e == time(21, 0)

    # Case 2: Campaign inside regulatory [10:00, 19:00) -> unchanged
    s, e = effective_window("10:00", "19:00")
    assert s == time(10, 0)
    assert e == time(19, 0)

    # Case 3: Early start [07:00, 15:00) -> clamped start [09:00, 15:00)
    s, e = effective_window("07:00", "15:00")
    assert s == time(9, 0)
    assert e == time(15, 0)

    # Case 4: Late end [15:00, 23:30) -> clamped end [15:00, 21:00)
    s, e = effective_window("15:00", "23:30")
    assert s == time(15, 0)
    assert e == time(21, 0)

    # Case 5: Inverted / Invalid window -> raises ValueError
    with pytest.raises(ValueError):
        effective_window("22:00", "08:00")


def test_is_within_calling_window():
    # Campaign window: 10:00 - 19:00, Mon-Fri
    days = ["mon", "tue", "wed", "thu", "fri"]

    # Thu 12 Nov 2026, 14:00 IST -> inside window
    t1 = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    assert is_within_calling_window(t1, days, "10:00", "19:00") is True

    # Thu 12 Nov 2026, 09:30 IST -> before window
    t2 = datetime(2026, 11, 12, 9, 30, 0, tzinfo=IST)
    assert is_within_calling_window(t2, days, "10:00", "19:00") is False

    # Thu 12 Nov 2026, 19:05 IST -> after window
    t3 = datetime(2026, 11, 12, 19, 5, 0, tzinfo=IST)
    assert is_within_calling_window(t3, days, "10:00", "19:00") is False

    # Thu 12 Nov 2026, 18:59:45 IST -> within 30s close buffer of 19:00
    t4 = datetime(2026, 11, 12, 18, 59, 45, tzinfo=IST)
    assert is_within_calling_window(t4, days, "10:00", "19:00", close_buffer_sec=30) is False
    assert is_within_calling_window(t4, days, "10:00", "19:00", close_buffer_sec=0) is True

    # Sun 15 Nov 2026, 14:00 IST -> disallowed day
    t_sun = datetime(2026, 11, 15, 14, 0, 0, tzinfo=IST)
    assert is_within_calling_window(t_sun, days, "10:00", "19:00") is False


def test_next_window_opening_rollover():
    # Window: 10:00 - 19:00, Mon-Fri
    days = ["mon", "tue", "wed", "thu", "fri"]

    # Case 1: Target is already inside open window -> returned unchanged
    t_in = datetime(2026, 11, 12, 14, 30, 0, tzinfo=IST)  # Thursday
    assert next_window_opening(t_in, days, "10:00", "19:00") == t_in

    # Case 2: Target is earlier today (Thursday 08:15) -> opens at 10:00 today
    t_early = datetime(2026, 11, 12, 8, 15, 0, tzinfo=IST)
    assert next_window_opening(t_early, days, "10:00", "19:00") == datetime(
        2026, 11, 12, 10, 0, 0, tzinfo=IST
    )

    # Case 3: Target is after window close on Thursday (20:30) -> rolls to Friday 10:00
    t_late_thu = datetime(2026, 11, 12, 20, 30, 0, tzinfo=IST)
    assert next_window_opening(t_late_thu, days, "10:00", "19:00") == datetime(
        2026, 11, 13, 10, 0, 0, tzinfo=IST
    )

    # Case 4: Target is Friday night (21:30) -> skips Sat/Sun and rolls to Monday 10:00
    t_fri_night = datetime(2026, 11, 13, 21, 30, 0, tzinfo=IST)  # Friday
    assert next_window_opening(t_fri_night, days, "10:00", "19:00") == datetime(
        2026, 11, 16, 10, 0, 0, tzinfo=IST
    )

    # Case 5: Target is Sunday afternoon (15:00) -> rolls to Monday 10:00
    t_sun = datetime(2026, 11, 15, 15, 0, 0, tzinfo=IST)
    assert next_window_opening(t_sun, days, "10:00", "19:00") == datetime(
        2026, 11, 16, 10, 0, 0, tzinfo=IST
    )


def test_retry_backoff_transitions():
    ended_at = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)  # Thursday 14:00
    days = ["mon", "tue", "wed", "thu", "fri"]

    # 1. no_answer on attempt 1 -> +60m wait (15:00)
    trans = compute_next_lead_transition(
        attempts=1,
        max_attempts=3,
        status="no_answer",
        disposition=None,
        ended_at=ended_at,
        callback_at=None,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans.new_state == "WAITING"
    assert trans.priority_tier == PRIORITY_RETRY
    assert trans.next_eligible_at == datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)

    # 2. busy on attempt 1 -> +60m wait (15:00)
    trans_busy = compute_next_lead_transition(
        attempts=1,
        max_attempts=3,
        status="busy",
        disposition=None,
        ended_at=ended_at,
        callback_at=None,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans_busy.new_state == "WAITING"
    assert trans_busy.priority_tier == PRIORITY_RETRY
    assert trans_busy.next_eligible_at == datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)

    # 3. failed on attempt 1 -> +15m wait (14:15)
    trans_failed = compute_next_lead_transition(
        attempts=1,
        max_attempts=3,
        status="failed",
        disposition=None,
        ended_at=ended_at,
        callback_at=None,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans_failed.new_state == "WAITING"
    assert trans_failed.priority_tier == PRIORITY_RETRY
    assert trans_failed.next_eligible_at == datetime(2026, 11, 12, 14, 15, 0, tzinfo=IST)

    # 4. no_answer ending at 18:30 (window closes at 19:00) -> 18:30 + 60m = 19:30 -> rolls to Friday 10:00
    late_end = datetime(2026, 11, 12, 18, 30, 0, tzinfo=IST)
    trans_late = compute_next_lead_transition(
        attempts=1,
        max_attempts=3,
        status="no_answer",
        disposition=None,
        ended_at=late_end,
        callback_at=None,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans_late.new_state == "WAITING"
    assert trans_late.priority_tier == PRIORITY_RETRY
    assert trans_late.next_eligible_at == datetime(2026, 11, 13, 10, 0, 0, tzinfo=IST)


def test_callback_scheduling_and_priority():
    ended_at = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    days = ["mon", "tue", "wed", "thu", "fri"]

    # 1. Callback requested inside window: 16:30 today
    cb_time_in = datetime(2026, 11, 12, 16, 30, 0, tzinfo=IST)
    trans_in = compute_next_lead_transition(
        attempts=1,
        max_attempts=3,
        status="completed",
        disposition="callback_requested",
        ended_at=ended_at,
        callback_at=cb_time_in,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans_in.new_state == "WAITING"
    assert trans_in.priority_tier == PRIORITY_CALLBACK
    assert trans_in.next_eligible_at == cb_time_in
    assert trans_in.callback_at == cb_time_in

    # 2. Callback requested outside window: 21:30 tonight -> rolled to Friday 10:00
    cb_time_out = datetime(2026, 11, 12, 21, 30, 0, tzinfo=IST)
    trans_out = compute_next_lead_transition(
        attempts=1,
        max_attempts=3,
        status="completed",
        disposition="callback_requested",
        ended_at=ended_at,
        callback_at=cb_time_out,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans_out.new_state == "WAITING"
    assert trans_out.priority_tier == PRIORITY_CALLBACK
    assert trans_out.next_eligible_at == datetime(2026, 11, 13, 10, 0, 0, tzinfo=IST)
    assert trans_out.callback_at == cb_time_out

    # 3. Assert Priority Tier ordering contract (Rule R6: CALLBACK < RETRY < FRESH)
    assert PRIORITY_CALLBACK < PRIORITY_RETRY < PRIORITY_FRESH
    assert (PRIORITY_CALLBACK, PRIORITY_RETRY, PRIORITY_FRESH) == (1, 2, 3)


def test_terminal_and_exhausted_transitions():
    ended_at = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    days = ["mon", "tue", "wed", "thu", "fri"]

    # Attempt 1 terminal dispositions immediately mark COMPLETED
    for disp in ["interested", "not_interested", "wrong_number"]:
        trans = compute_next_lead_transition(
            attempts=1,
            max_attempts=3,
            status="completed",
            disposition=disp,
            ended_at=ended_at,
            callback_at=None,
            window_days=days,
            window_start="10:00",
            window_end="19:00",
        )
        assert trans.new_state == "COMPLETED"
        assert trans.is_dnd is False
        assert trans.next_eligible_at is None

    # dnd marks COMPLETED and sets is_dnd = True
    trans_dnd = compute_next_lead_transition(
        attempts=1,
        max_attempts=3,
        status="completed",
        disposition="dnd",
        ended_at=ended_at,
        callback_at=None,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans_dnd.new_state == "COMPLETED"
    assert trans_dnd.is_dnd is True

    # Attempt 3 non-terminal outcomes -> EXHAUSTED
    for non_terminal_status, non_terminal_disp in [
        ("no_answer", None),
        ("busy", None),
        ("failed", None),
        ("completed", "callback_requested"),
    ]:
        trans_ex = compute_next_lead_transition(
            attempts=3,
            max_attempts=3,
            status=non_terminal_status,
            disposition=non_terminal_disp,
            ended_at=ended_at,
            callback_at=datetime(2026, 11, 12, 16, 0, 0, tzinfo=IST),
            window_days=days,
            window_start="10:00",
            window_end="19:00",
        )
        assert trans_ex.new_state == "EXHAUSTED"
        assert trans_ex.next_eligible_at is None

    # Precedence test: Attempt 3 with terminal disposition 'interested' -> COMPLETED (completed > exhausted)
    trans_prec = compute_next_lead_transition(
        attempts=3,
        max_attempts=3,
        status="completed",
        disposition="interested",
        ended_at=ended_at,
        callback_at=None,
        window_days=days,
        window_start="10:00",
        window_end="19:00",
    )
    assert trans_prec.new_state == "COMPLETED"

