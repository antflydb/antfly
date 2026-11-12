from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryRetainedDatasetBody")


@_attrs_define
class QueryRetainedDatasetBody:
    """
    Attributes:
        start_ns (str): Inclusive UTC nanoseconds as a signed decimal string.
        end_ns (str): Exclusive UTC nanoseconds as a signed decimal string.
        max_rows (int | Unset):  Default: 4096.
        after (str | Unset): Opaque exclusive continuation key from next. Reuse the same range and dimension filter.
        dimensions (list[Any] | Unset): Optional exact tuple in the definition's dimension order. Values are normalized
            using the frozen column types; null is supported.
    """

    start_ns: str
    end_ns: str
    max_rows: int | Unset = 4096
    after: str | Unset = UNSET
    dimensions: list[Any] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        start_ns = self.start_ns

        end_ns = self.end_ns

        max_rows = self.max_rows

        after = self.after

        dimensions: list[Any] | Unset = UNSET
        if not isinstance(self.dimensions, Unset):
            dimensions = self.dimensions

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "start_ns": start_ns,
                "end_ns": end_ns,
            }
        )
        if max_rows is not UNSET:
            field_dict["max_rows"] = max_rows
        if after is not UNSET:
            field_dict["after"] = after
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        start_ns = d.pop("start_ns")

        end_ns = d.pop("end_ns")

        max_rows = d.pop("max_rows", UNSET)

        after = d.pop("after", UNSET)

        dimensions = cast(list[Any], d.pop("dimensions", UNSET))

        query_retained_dataset_body = cls(
            start_ns=start_ns,
            end_ns=end_ns,
            max_rows=max_rows,
            after=after,
            dimensions=dimensions,
        )

        return query_retained_dataset_body
