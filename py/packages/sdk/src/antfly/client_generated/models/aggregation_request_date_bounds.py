from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from dateutil.parser import isoparse

T = TypeVar("T", bound="AggregationRequestDateBounds")


@_attrs_define
class AggregationRequestDateBounds:
    """Explicit half-open RFC3339 extent for date_histogram empty buckets. Does not filter documents; use a matching date
    range filter. Limited to 10000 buckets per histogram and 100000 generated cells (buckets plus metrics) per request.
    Only scalar metric sub-aggregations are supported with these bounds.

        Attributes:
            start (datetime.datetime):
            end (datetime.datetime):
    """

    start: datetime.datetime
    end: datetime.datetime

    def to_dict(self) -> dict[str, Any]:
        start = self.start.isoformat()

        end = self.end.isoformat()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "start": start,
                "end": end,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        start = isoparse(d.pop("start"))

        end = isoparse(d.pop("end"))

        aggregation_request_date_bounds = cls(
            start=start,
            end=end,
        )

        return aggregation_request_date_bounds
