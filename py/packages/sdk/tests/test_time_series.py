import pytest

from antfly.time_series import fixed_duration_nanoseconds, series_query


def test_series_query_preserves_nanosecond_bounds_and_half_open_range():
    query = series_query(
        timestamp="ts",
        start="2026-11-06T00:00:00.000000001Z",
        end="2026-11-06T00:00:00.000000002Z",
        measurement="value",
    )
    assert query["filter_query"]["conjuncts"][0]["inclusive_end"] is False
    assert query["aggregations"]["points"]["date_bounds"]["start"].endswith("000000001Z")


@pytest.mark.parametrize("text", ["0s", "-1m", "1M", "1.5s", "18446744073709551615w"])
def test_invalid_duration(text):
    with pytest.raises(ValueError):
        fixed_duration_nanoseconds(text)


def test_composite_elapsed_duration():
    assert fixed_duration_nanoseconds("1h30m1ns") == 5400000000001
