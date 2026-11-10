import pytest

from antfly.time_series import fixed_duration_nanoseconds, series_query


def test_series_query_preserves_nanosecond_bounds_and_half_open_range():
    query = series_query(
        timestamp="ts",
        start="2026-11-06T00:00:00.000000001Z",
        end="2026-11-06T00:00:00.000000002Z",
        measurement="value",
        dimensions={"device_id": "sensor"},
    )
    assert query["filter_query"]["conjuncts"][0]["inclusive_end"] is False
    assert query["filter_query"]["conjuncts"][1] == {"field": "device_id", "equals": "sensor"}
    assert query["aggregations"]["points"]["date_bounds"]["start"].endswith("000000001Z")


@pytest.mark.parametrize("text", ["0s", "-1m", "1M", "1.5s", "18446744073709551615w"])
def test_invalid_duration(text):
    with pytest.raises(ValueError):
        fixed_duration_nanoseconds(text)


def test_composite_elapsed_duration():
    assert fixed_duration_nanoseconds("1h30m1ns") == 5400000000001


def test_exact_bucket_limit():
    series_query(
        timestamp="ts",
        measurement="v",
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T02:46:40Z",
        every="1s",
    )
    with pytest.raises(ValueError, match="10000"):
        series_query(
            timestamp="ts",
            measurement="v",
            start="2026-01-01T00:00:00Z",
            end="2026-01-01T02:46:40.000000001Z",
            every="1s",
        )


def test_series_grouping_excludes_null_keys():
    query = series_query(
        timestamp="ts",
        measurement="v",
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T00:05:00Z",
        group_by=["device_id"],
    )
    assert query["aggregations"]["series"]["exclude_null"] is True
