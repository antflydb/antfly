from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_count_aggregate_type import GraphCountAggregateType
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphCountAggregate")


@_attrs_define
class GraphCountAggregate:
    """
    Attributes:
        type_ (GraphCountAggregateType):
        of (str): Use `*` to count rows, or an alias to count non-null bindings.
        distinct (bool | Unset):  Default: False.
    """

    type_: GraphCountAggregateType
    of: str
    distinct: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        of = self.of

        distinct = self.distinct

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "of": of,
            }
        )
        if distinct is not UNSET:
            field_dict["distinct"] = distinct

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = GraphCountAggregateType(d.pop("type"))

        of = d.pop("of")

        distinct = d.pop("distinct", UNSET)

        graph_count_aggregate = cls(
            type_=type_,
            of=of,
            distinct=distinct,
        )

        return graph_count_aggregate
