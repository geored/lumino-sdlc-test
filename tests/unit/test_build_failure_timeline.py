"""
Tests for build_failure_timeline() in src/helpers/failure_analysis.py

Covers the requirements from issue #104:
  - datetime objects → ISO-8601 string
  - non-empty string timestamps → preserved as-is
  - missing / None timestamps → "timestamp": None (not datetime.now())
  - sort is stable when None timestamps are present (no TypeError)
  - None-timestamp entries are pushed to the end of the sorted result
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

# Ensure helpers are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from helpers.failure_analysis import build_failure_timeline

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeLogger:
    def error(self, *a, **kw):
        pass

    def info(self, *a, **kw):
        pass


def _make_events_func(events: list):
    """Return an async callable that yields the given events list."""

    async def _func(namespace):
        return {"events": events}

    return _func


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Test 1 - datetime object -> isoformat string
# ---------------------------------------------------------------------------


def test_datetime_timestamp_converted_to_isoformat():
    dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "timestamp": dt,
            "reason": "Started",
            "event_string": "pod started",
            "severity": "low",
        }
    ]
    result = _run(
        build_failure_timeline("ns", "id", 1, _make_events_func(events), _FakeLogger())
    )
    assert len(result) == 1
    assert result[0]["timestamp"] == dt.isoformat()


# ---------------------------------------------------------------------------
# Test 2 - string timestamp -> preserved as-is
# ---------------------------------------------------------------------------


def test_string_timestamp_preserved():
    events = [
        {
            "timestamp": "2024-06-01T12:00:00Z",
            "reason": "R",
            "event_string": "e",
            "severity": "low",
        }
    ]
    result = _run(
        build_failure_timeline("ns", "id", 1, _make_events_func(events), _FakeLogger())
    )
    assert result[0]["timestamp"] == "2024-06-01T12:00:00Z"


# ---------------------------------------------------------------------------
# Test 3 - None / missing timestamp -> entry has "timestamp": None (not now())
# ---------------------------------------------------------------------------


def test_none_timestamp_stored_as_none():
    events = [
        {
            "timestamp": None,
            "reason": "NoTS",
            "event_string": "no ts event",
            "severity": "medium",
        },
        {
            "reason": "Missing",
            "event_string": "missing key event",
            "severity": "medium",
        },
    ]
    result = _run(
        build_failure_timeline("ns", "id", 1, _make_events_func(events), _FakeLogger())
    )
    timestamps = [e["timestamp"] for e in result]
    assert all(ts is None for ts in timestamps), "Expected all None, got: {}".format(
        timestamps
    )


# ---------------------------------------------------------------------------
# Test 4 - no TypeError when None and real timestamps coexist
# ---------------------------------------------------------------------------


def test_sort_with_mixed_none_and_string_timestamps_no_type_error():
    dt = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        {"timestamp": None, "reason": "NoTS", "event_string": "a", "severity": "low"},
        {"timestamp": dt, "reason": "HasTS", "event_string": "b", "severity": "low"},
        {
            "timestamp": "2024-06-01T09:00:00Z",
            "reason": "StrTS",
            "event_string": "c",
            "severity": "low",
        },
    ]
    # Must not raise TypeError
    result = _run(
        build_failure_timeline("ns", "id", 1, _make_events_func(events), _FakeLogger())
    )
    assert isinstance(result, list)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# Test 5 - None-timestamp entries appear after entries with real timestamps
# ---------------------------------------------------------------------------


def test_none_timestamps_pushed_to_end():
    dt_early = datetime(2024, 6, 1, 8, 0, 0, tzinfo=timezone.utc)
    dt_late = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "timestamp": None,
            "reason": "NoTS",
            "event_string": "no ts",
            "severity": "low",
        },
        {
            "timestamp": dt_early,
            "reason": "Early",
            "event_string": "early",
            "severity": "low",
        },
        {
            "timestamp": dt_late,
            "reason": "Late",
            "event_string": "late",
            "severity": "low",
        },
    ]
    result = _run(
        build_failure_timeline("ns", "id", 1, _make_events_func(events), _FakeLogger())
    )
    # The last entry must be the one with timestamp=None
    assert (
        result[-1]["timestamp"] is None
    ), "Expected None-timestamp entry last, but got: {}".format(
        [e["timestamp"] for e in result]
    )


# ---------------------------------------------------------------------------
# Test 6 - empty events list -> empty timeline (no crash)
# ---------------------------------------------------------------------------


def test_empty_events_returns_empty_list():
    result = _run(
        build_failure_timeline("ns", "id", 1, _make_events_func([]), _FakeLogger())
    )
    assert result == []


# ---------------------------------------------------------------------------
# Test 7 - result capped at 10 entries
# ---------------------------------------------------------------------------


def test_result_capped_at_ten_entries():
    events = [
        {
            "timestamp": "2024-06-01T{:02d}:00:00Z".format(i),
            "reason": "R",
            "event_string": "e",
            "severity": "low",
        }
        for i in range(15)
    ]
    result = _run(
        build_failure_timeline("ns", "id", 1, _make_events_func(events), _FakeLogger())
    )
    assert len(result) <= 10
