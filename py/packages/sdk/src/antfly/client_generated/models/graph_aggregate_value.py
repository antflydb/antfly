from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphAggregateValue")


@_attrs_define
class GraphAggregateValue:
    """
    Attributes:
        value (str): Decimal string so counts remain lossless in JavaScript.
        exact (bool): Always true. Exact aggregate execution fails instead of returning a partial value.
    """

    value: str
    exact: bool

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        exact = self.exact

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "value": value,
                "exact": exact,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        exact = d.pop("exact")

        graph_aggregate_value = cls(
            value=value,
            exact=exact,
        )

        return graph_aggregate_value
