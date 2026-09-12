from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_metric_config import GraphMetricConfig


T = TypeVar("T", bound="GraphIndexConfigMetrics")


@_attrs_define
class GraphIndexConfigMetrics:
    """Named published graph metrics. Serverless supports background refresh only and limits configurations to 16 metrics
    per graph, 64 total per publication, 64 types per filter, and 128 UTF-8 bytes per metric name.

    """

    additional_properties: dict[str, GraphMetricConfig] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_metric_config import GraphMetricConfig

        d = dict(src_dict)
        graph_index_config_metrics = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = GraphMetricConfig.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        graph_index_config_metrics.additional_properties = additional_properties
        return graph_index_config_metrics

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> GraphMetricConfig:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: GraphMetricConfig) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
