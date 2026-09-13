from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_filter_op import GraphMetricFilterOp

T = TypeVar("T", bound="GraphMetricFilter")


@_attrs_define
class GraphMetricFilter:
    """
    Attributes:
        metric (str):
        op (GraphMetricFilterOp): Semantic comparison operator. Named values keep generated SDK enums portable and
            readable.
        value (float):
    """

    metric: str
    op: GraphMetricFilterOp
    value: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        metric = self.metric

        op = self.op.value

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "metric": metric,
                "op": op,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        metric = d.pop("metric")

        op = GraphMetricFilterOp(d.pop("op"))

        value = d.pop("value")

        graph_metric_filter = cls(
            metric=metric,
            op=op,
            value=value,
        )

        graph_metric_filter.additional_properties = d
        return graph_metric_filter

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
