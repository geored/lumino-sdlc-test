"""Smoke tests for core helper functions — P0 coverage."""
import sys
import time
import pytest

sys.path.insert(0, "src")

from helpers.utils import (
    calculate_duration,
    calculate_duration_seconds,
    parse_time_period,
    detect_anomalies_in_data,
)
from helpers.log_analysis import (
    LogAnalysisStrategy,
    AnalysisCache,
    LogStreamProcessor,
    truncate_to_token_limit,
)


def test_calculate_duration_one_minute():
    result = calculate_duration("2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z")
    assert "1" in result


def test_calculate_duration_zero():
    result = calculate_duration("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z")
    assert result is not None


def test_calculate_duration_seconds_basic():
    result = calculate_duration_seconds("2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z")
    assert result == 60


def test_calculate_duration_seconds_same_time():
    result = calculate_duration_seconds("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z")
    assert result == 0


def test_parse_time_period_hours():
    result = parse_time_period("1h")
    assert result is not None
    assert result.total_seconds() == 3600


def test_parse_time_period_minutes():
    result = parse_time_period("30m")
    assert result is not None


def test_detect_anomalies_insufficient_data():
    result = detect_anomalies_in_data([1.0, 2.0], ["a", "b"])
    assert result["anomalies_detected"] is False
    assert "Insufficient" in result["message"]


def test_detect_anomalies_no_variance():
    result = detect_anomalies_in_data([5.0, 5.0, 5.0, 5.0, 5.0], ["a", "b", "c", "d", "e"])
    assert result["anomalies_detected"] is False


def test_detect_anomalies_valid_dict_returned():
    result = detect_anomalies_in_data(
        [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 100.0],
        ["a", "b", "c", "d", "e", "f", "outlier"],
    )
    assert "anomalies_detected" in result
    assert "message" in result


def test_detect_anomalies_exact_five_points():
    result = detect_anomalies_in_data(
        [1.0, 2.0, 3.0, 4.0, 5.0], ["a", "b", "c", "d", "e"]
    )
    assert "anomalies_detected" in result


def test_analysis_cache_hit():
    cache = AnalysisCache(ttl_seconds=60)
    cache.set("ns", "pod", "hash1", {"result": "ok"})
    result = cache.get("ns", "pod", "hash1")
    assert result == {"result": "ok"}


def test_analysis_cache_miss():
    cache = AnalysisCache(ttl_seconds=60)
    result = cache.get("ns", "pod", "nonexistent")
    assert result is None


def test_analysis_cache_ttl_expiry():
    cache = AnalysisCache(ttl_seconds=1)
    cache.set("ns", "pod", "hash2", {"result": "ok"})
    time.sleep(1.2)
    result = cache.get("ns", "pod", "hash2")
    assert result is None


def test_analysis_cache_different_keys():
    cache = AnalysisCache(ttl_seconds=60)
    cache.set("ns1", "pod1", "hash", {"result": "one"})
    cache.set("ns2", "pod2", "hash", {"result": "two"})
    assert cache.get("ns1", "pod1", "hash") == {"result": "one"}
    assert cache.get("ns2", "pod2", "hash") == {"result": "two"}


def test_truncate_reduces_long_text():
    text = "word " * 10000
    result = truncate_to_token_limit(text, 100)
    assert len(result) < len(text)


def test_truncate_short_text_passthrough():
    text = "hello world"
    result = truncate_to_token_limit(text, 10000)
    assert isinstance(result, str)
    assert len(result) <= len(text) + 200


def test_log_analysis_strategy_values():
    assert LogAnalysisStrategy.SMART_SUMMARY is not None
    assert LogAnalysisStrategy.STREAMING is not None
    assert LogAnalysisStrategy.HYBRID is not None


def test_log_stream_processor_basic():
    processor = LogStreamProcessor(chunk_size=5, mode="error_focus")
    for i in range(10):
        processor.add_line(f"ERROR line {i}")
    result = processor.finalize()
    assert isinstance(result, dict)


def test_log_stream_processor_empty():
    processor = LogStreamProcessor(chunk_size=10, mode="error_focus")
    result = processor.finalize()
    assert isinstance(result, dict)


def test_analysis_cache_eviction_basic():
    """Eviction removes oldest entry when cache is full."""
    cache = AnalysisCache(max_size=2, ttl_seconds=60)
    cache.set("ns", "pod1", "h1", {"r": 1})
    cache.set("ns", "pod2", "h2", {"r": 2})
    cache.set("ns", "pod3", "h3", {"r": 3})
    # pod1 was oldest, should be evicted
    assert cache.get("ns", "pod1", "h1") is None
    assert cache.get("ns", "pod2", "h2") == {"r": 2}
    assert cache.get("ns", "pod3", "h3") == {"r": 3}


def test_analysis_cache_eviction_empty_access_times():
    """Bug 1 fix: no ValueError when access_times is empty but cache is full."""
    cache = AnalysisCache(max_size=2, ttl_seconds=60)
    cache.set("ns", "pod1", "h1", {"r": 1})
    cache.set("ns", "pod2", "h2", {"r": 2})
    cache.access_times.clear()  # simulate desync
    # Must not raise ValueError
    cache.set("ns", "pod3", "h3", {"r": 3})
    assert cache.get("ns", "pod3", "h3") == {"r": 3}
    assert len(cache.cache) <= 2


def test_analysis_cache_eviction_stale_access_times_key():
    """Bug 2 fix: no KeyError when oldest_key not in cache."""
    cache = AnalysisCache(max_size=2, ttl_seconds=60)
    cache.set("ns", "pod1", "h1", {"r": 1})
    cache.set("ns", "pod2", "h2", {"r": 2})
    cache.access_times["stale_key"] = 0  # oldest, but not in cache
    # Must not raise KeyError
    cache.set("ns", "pod3", "h3", {"r": 3})
    assert cache.get("ns", "pod3", "h3") == {"r": 3}
