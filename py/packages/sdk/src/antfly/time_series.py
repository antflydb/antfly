"""Time-series constructors using the ordinary query API.

Pass the result to client.query(table, query=series_query(...)). Timestamp strings
are preserved, including their nanosecond precision. Batch writes remain upserts;
use conditional writes when corrections must be protected from older retries.
"""

import re
from datetime import UTC, datetime
from typing import Any


def fixed_duration_nanoseconds(text: str) -> int:
    units = {
        "ns": 1,
        "us": 1000,
        "ms": 10**6,
        "s": 10**9,
        "m": 60 * 10**9,
        "h": 3600 * 10**9,
        "d": 86400 * 10**9,
        "w": 604800 * 10**9,
    }
    if not re.fullmatch(r"(?:\d+(?:ns|us|ms|s|m|h|d|w))+", text):
        raise ValueError("Invalid fixed duration")
    value = sum(int(n) * units[u] for n, u in re.findall(r"(\d+)(ns|us|ms|s|m|h|d|w)", text))
    if not 0 < value < 2**64:
        raise ValueError("Duration must fit positive uint64 nanoseconds")
    return value


def series_query(
    *,
    timestamp: str,
    start: str,
    end: str,
    measurement: str,
    every: str = "5m",
    group_by: list[str] | None = None,
    dimensions: dict[str, str] | None = None,
    max_series: int = 100,
) -> dict[str, Any]:
    """Average/min/max per fixed bucket, optionally grouped by keyword fields."""
    duration = fixed_duration_nanoseconds(every)
    # datetime validates the timezone and coarse range; originals go on the wire.
    first, last = (
        datetime.fromisoformat(start.replace("Z", "+00:00")),
        datetime.fromisoformat(end.replace("Z", "+00:00")),
    )

    def nanos(text: str, value: datetime) -> int:
        fraction = re.search(r"\.(\d{1,9})", text)
        if value.tzinfo is None:
            raise ValueError("Timestamp timezone is required")
        epoch = value.replace(microsecond=0).astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        return (epoch.days * 86400 + epoch.seconds) * 10**9 + int((fraction[1] if fraction else "").ljust(9, "0"))

    start_ns, end_ns = nanos(start, first), nanos(end, last)
    if first.tzinfo is None or last.tzinfo is None or start_ns >= end_ns:
        raise ValueError("Expected an increasing timezone-aware [start, end) range")
    if (end_ns - 1) // duration - start_ns // duration + 1 > 10000:
        raise ValueError("Select a coarser interval: at most 10000 buckets")
    if not 1 <= max_series <= 1000:
        raise ValueError("max_series must be between 1 and 1000")
    points = {
        "type": "date_histogram",
        "field": timestamp,
        "fixed_interval": every,
        "date_bounds": {"start": start, "end": end},
        "min_doc_count": 0,
        "sub_aggregations": {op: {"type": op, "field": measurement} for op in ("avg", "min", "max")},
    }
    aggregates = (
        {
            "series": {
                "type": "terms",
                "fields": group_by,
                "exclude_null": True,
                "size": max_series,
                "sub_aggregations": {"points": points},
            }
        }
        if group_by
        else {"points": points}
    )
    return {
        "limit": 0,
        "filter_query": {
            "conjuncts": [
                {"field": timestamp, "start": start, "end": end, "inclusive_start": True, "inclusive_end": False},
                *({"field": key, "equals": value} for key, value in (dimensions or {}).items()),
            ]
        },
        "aggregations": aggregates,
    }
