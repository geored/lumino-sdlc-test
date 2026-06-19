"""
Time and duration utility functions for LUMINO MCP Server.
"""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

logger = __import__("logging").getLogger("lumino-mcp")


def calculate_duration(
    start_time, end_time, use_current_if_missing: bool = False
) -> str:
    if not start_time or start_time == "unknown":
        return "unknown"
    is_running = False
    if not end_time or end_time == "unknown":
        if use_current_if_missing:
            end_time = datetime.now(tz=None)
            is_running = True
        else:
            return "unknown"
    try:
        if isinstance(start_time, str):
            start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        else:
            start = start_time
        if isinstance(end_time, str):
            end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        elif isinstance(end_time, datetime):
            if start.tzinfo is not None and end_time.tzinfo is None:
                from datetime import timezone
                end = end_time.replace(tzinfo=timezone.utc)
            else:
                end = end_time
        else:
            end = end_time
        duration = end - start
        seconds = duration.total_seconds()
        if seconds < 60:
            duration_str = f"{seconds:.2f} seconds"
        elif seconds < 3600:
            duration_str = f"{seconds/60:.2f} minutes"
        elif seconds < 86400:
            duration_str = f"{seconds/3600:.2f} hours"
        else:
            days = int(seconds // 86400)
            remaining_hours = (seconds % 86400) / 3600
            duration_str = f"{days}d {remaining_hours:.1f}h"
        if is_running:
            duration_str = f"{duration_str} (running)"
        return duration_str
    except Exception:
        return "unknown"


def calculate_duration_seconds(
    start_time, end_time, use_current_if_missing: bool = False
) -> Optional[int]:
    if not start_time or start_time == "unknown":
        return None
    if not end_time or end_time == "unknown":
        if use_current_if_missing:
            end_time = datetime.now(tz=None)
        else:
            return None
    try:
        if isinstance(start_time, str):
            start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        else:
            start = start_time
        if isinstance(end_time, str):
            end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        elif isinstance(end_time, datetime):
            if start.tzinfo is not None and end_time.tzinfo is None:
                from datetime import timezone
                end = end_time.replace(tzinfo=timezone.utc)
            else:
                end = end_time
        else:
            end = end_time
        return int((end - start).total_seconds())
    except Exception:
        return None


def parse_time_period(time_period: str) -> timedelta:
    """Parse time period string like '1h', '30m', '2d' into timedelta object."""
    pattern = r"^(\d+)([smhd])$"
    match = re.match(pattern, time_period.lower())
    if not match:
        raise ValueError(
            f"Invalid time period format: {time_period}. "
            "Expected format: number followed by s/m/h/d (e.g., '1h', '30m', '2d')"
        )
    value, unit = int(match.group(1)), match.group(2)
    if unit == "s":
        return timedelta(seconds=value)
    elif unit == "m":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    else:
        raise ValueError(f"Unsupported time unit: {unit}")


def parse_time_parameters(
    since_seconds: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> Dict[str, Any]:
    log_params: Dict[str, Any] = {}
    time_info: Dict[str, Any] = {}
    if since_seconds is not None:
        log_params["since_seconds"] = since_seconds
        time_info["method"] = "since_seconds"
        time_info["value"] = since_seconds
    elif start_time is not None or end_time is not None:
        if start_time:
            try:
                if not isinstance(start_time, str):
                    raise ValueError(
                        f"start_time must be a string, got {type(start_time).__name__}: {start_time}"
                    )
                from datetime import timezone
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                now_dt = datetime.now(timezone.utc)
                time_diff = (now_dt - start_dt).total_seconds()
                if time_diff > 0:
                    log_params["since_seconds"] = int(time_diff)
                    time_info["method"] = "start_time"
                    time_info["start_time"] = start_time
                    time_info["calculated_since_seconds"] = int(time_diff)
                    if end_time:
                        time_info["end_time"] = end_time
                else:
                    time_info["method"] = "start_time_future_fallback"
                    time_info["warning"] = (
                        f"start_time {start_time} is in the future, using default time range"
                    )
            except ValueError:
                raise ValueError(
                    f"Invalid start_time format: {start_time}. Use ISO format like '2024-01-15T10:30:00Z'"
                )
    elif time_period is not None:
        try:
            time_delta = parse_time_period(time_period)
            since_seconds_calc = int(time_delta.total_seconds())
            log_params["since_seconds"] = since_seconds_calc
            time_info["method"] = "time_period"
            time_info["period"] = time_period
            time_info["calculated_seconds"] = since_seconds_calc
        except ValueError as e:
            raise ValueError(f"Invalid time_period: {e}")
    if not log_params:
        default_seconds = 3600
        log_params["since_seconds"] = default_seconds
        time_info["method"] = "default"
        time_info["value"] = default_seconds
    return {"log_params": log_params, "time_info": time_info}


def convert_duration_to_seconds(duration: str) -> int:
    duration_map = {"1h": 3600, "24h": 86400, "7d": 604800}
    return duration_map.get(duration, 86400)


def convert_duration_to_hours(duration: str) -> int:
    duration_map = {"1h": 1, "24h": 24, "7d": 168}
    return duration_map.get(duration, 24)


def calculate_forecast_intervals(forecast_horizon: str) -> int:
    period = parse_time_period(forecast_horizon)
    intervals_per_hour = 12
    total_hours = int(period.total_seconds() / 3600)
    return total_hours * intervals_per_hour


def detect_performance_trend(durations) -> str:
    if not durations or len(durations) < 3:
        return "Insufficient data for trend analysis"
    x = list(range(len(durations)))
    n = len(durations)
    sum_x = sum(x)
    sum_y = sum(durations)
    sum_xy = sum(x[i] * durations[i] for i in range(n))
    sum_xx = sum(x[i] ** 2 for i in range(n))
    try:
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x**2)
    except ZeroDivisionError:
        return "Unable to determine trend (calculation error)"
    avg_duration = sum_y / n
    relative_slope = (slope / avg_duration) * 100 if avg_duration > 0 else 0
    if abs(relative_slope) < 5:
        return "Stable performance (no significant trend)"
    elif relative_slope > 10:
        return "Significant performance degradation trend"
    elif relative_slope > 5:
        return "Moderate performance degradation trend"
    elif relative_slope < -10:
        return "Significant performance improvement trend"
    elif relative_slope < -5:
        return "Moderate performance improvement trend"
    else:
        return "Slight performance variation (no clear trend)"
