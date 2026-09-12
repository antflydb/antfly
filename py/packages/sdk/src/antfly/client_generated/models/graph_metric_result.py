from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_metric_score import GraphMetricScore
    from ..models.graph_metric_status import GraphMetricStatus


T = TypeVar("T", bound="GraphMetricResult")


@_attrs_define
class GraphMetricResult:
    """
    Attributes:
        index_name (str):
        metric (str):
        scores (list[GraphMetricScore]):
        status (GraphMetricStatus):
    """

    index_name: str
    metric: str
    scores: list[GraphMetricScore]
    status: GraphMetricStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_name = self.index_name

        metric = self.metric

        scores = []
        for scores_item_data in self.scores:
            scores_item = scores_item_data.to_dict()
            scores.append(scores_item)

        status = self.status.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_name": index_name,
                "metric": metric,
                "scores": scores,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_metric_score import GraphMetricScore
        from ..models.graph_metric_status import GraphMetricStatus

        d = dict(src_dict)
        index_name = d.pop("index_name")

        metric = d.pop("metric")

        scores = []
        _scores = d.pop("scores")
        for scores_item_data in _scores:
            scores_item = GraphMetricScore.from_dict(scores_item_data)

            scores.append(scores_item)

        status = GraphMetricStatus.from_dict(d.pop("status"))

        graph_metric_result = cls(
            index_name=index_name,
            metric=metric,
            scores=scores,
            status=status,
        )

        graph_metric_result.additional_properties = d
        return graph_metric_result

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
