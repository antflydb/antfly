from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphCountAggregate")


@_attrs_define
class GraphCountAggregate:
    """
    Attributes:
        count (str): Use `*` to count rows, or an alias to count non-null bindings.
        distinct (bool | Unset):  Default: False.
    """

    count: str
    distinct: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        count = self.count

        distinct = self.distinct

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "count": count,
            }
        )
        if distinct is not UNSET:
            field_dict["distinct"] = distinct

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        count = d.pop("count")

        distinct = d.pop("distinct", UNSET)

        graph_count_aggregate = cls(
            count=count,
            distinct=distinct,
        )

        return graph_count_aggregate
