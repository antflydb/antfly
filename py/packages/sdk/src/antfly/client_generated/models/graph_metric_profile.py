from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_metric_status import GraphMetricStatus


T = TypeVar("T", bound="GraphMetricProfile")


@_attrs_define
class GraphMetricProfile:
    """
    Attributes:
        query_name (str): Name of the graph query or graph metric query that used the metric.
        source (str): Profile source, such as `graph_query`, `graph_metric`, or `graph_metric_rerank`.
        index_name (str): Graph index that owns the metric.
        metric_name (str): Graph metric name within the index.
        freshness (str): Effective freshness mode requested for this metric use.
        status (GraphMetricStatus):
    """

    query_name: str
    source: str
    index_name: str
    metric_name: str
    freshness: str
    status: GraphMetricStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        query_name = self.query_name

        source = self.source

        index_name = self.index_name

        metric_name = self.metric_name

        freshness = self.freshness

        status = self.status.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "query_name": query_name,
                "source": source,
                "index_name": index_name,
                "metric_name": metric_name,
                "freshness": freshness,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_metric_status import GraphMetricStatus

        d = dict(src_dict)
        query_name = d.pop("query_name")

        source = d.pop("source")

        index_name = d.pop("index_name")

        metric_name = d.pop("metric_name")

        freshness = d.pop("freshness")

        status = GraphMetricStatus.from_dict(d.pop("status"))

        graph_metric_profile = cls(
            query_name=query_name,
            source=source,
            index_name=index_name,
            metric_name=metric_name,
            freshness=freshness,
            status=status,
        )

        graph_metric_profile.additional_properties = d
        return graph_metric_profile

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
