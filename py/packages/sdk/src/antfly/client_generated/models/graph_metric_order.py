from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_order_direction import GraphMetricOrderDirection
from ..models.graph_metric_order_nulls import GraphMetricOrderNulls
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricOrder")


@_attrs_define
class GraphMetricOrder:
    """
    Attributes:
        metric (str):
        direction (GraphMetricOrderDirection | Unset):
        nulls (GraphMetricOrderNulls | Unset):
    """

    metric: str
    direction: GraphMetricOrderDirection | Unset = UNSET
    nulls: GraphMetricOrderNulls | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        metric = self.metric

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        nulls: str | Unset = UNSET
        if not isinstance(self.nulls, Unset):
            nulls = self.nulls.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "metric": metric,
            }
        )
        if direction is not UNSET:
            field_dict["direction"] = direction
        if nulls is not UNSET:
            field_dict["nulls"] = nulls

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        metric = d.pop("metric")

        _direction = d.pop("direction", UNSET)
        direction: GraphMetricOrderDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = GraphMetricOrderDirection(_direction)

        _nulls = d.pop("nulls", UNSET)
        nulls: GraphMetricOrderNulls | Unset
        if isinstance(_nulls, Unset):
            nulls = UNSET
        else:
            nulls = GraphMetricOrderNulls(_nulls)

        graph_metric_order = cls(
            metric=metric,
            direction=direction,
            nulls=nulls,
        )

        graph_metric_order.additional_properties = d
        return graph_metric_order

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
