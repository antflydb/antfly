from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_rerank_metric_freshness import GraphMetricRerankMetricFreshness
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricRerank")


@_attrs_define
class GraphMetricRerank:
    """Blends a published graph metric into hit scores. Multi-shard tables require a globally coordinated metric snapshot
    and otherwise return graph_metric_global_materialization_required.

        Attributes:
            index (str): Graph index that owns the published metric.
            metric (str): Graph metric name to blend into the search hit score.
            base_weight (float | Unset): Multiplier applied to the existing hit score before adding the graph metric
                feature. Default: 1.0.
            weight (float | Unset): Multiplier applied to the graph metric score before it is added to the existing hit
                score. Default: 1.0.
            missing_score (float | Unset): Metric feature value to use for hits that do not have a score in the published
                metric generation. Default: 0.0.
            metric_freshness (GraphMetricRerankMetricFreshness | Unset): Whether stale published generations are acceptable
                or the metric must be fresh. Default: GraphMetricRerankMetricFreshness.PUBLISHED.
    """

    index: str
    metric: str
    base_weight: float | Unset = 1.0
    weight: float | Unset = 1.0
    missing_score: float | Unset = 0.0
    metric_freshness: GraphMetricRerankMetricFreshness | Unset = GraphMetricRerankMetricFreshness.PUBLISHED
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        metric = self.metric

        base_weight = self.base_weight

        weight = self.weight

        missing_score = self.missing_score

        metric_freshness: str | Unset = UNSET
        if not isinstance(self.metric_freshness, Unset):
            metric_freshness = self.metric_freshness.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
                "metric": metric,
            }
        )
        if base_weight is not UNSET:
            field_dict["base_weight"] = base_weight
        if weight is not UNSET:
            field_dict["weight"] = weight
        if missing_score is not UNSET:
            field_dict["missing_score"] = missing_score
        if metric_freshness is not UNSET:
            field_dict["metric_freshness"] = metric_freshness

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index = d.pop("index")

        metric = d.pop("metric")

        base_weight = d.pop("base_weight", UNSET)

        weight = d.pop("weight", UNSET)

        missing_score = d.pop("missing_score", UNSET)

        _metric_freshness = d.pop("metric_freshness", UNSET)
        metric_freshness: GraphMetricRerankMetricFreshness | Unset
        if isinstance(_metric_freshness, Unset):
            metric_freshness = UNSET
        else:
            metric_freshness = GraphMetricRerankMetricFreshness(_metric_freshness)

        graph_metric_rerank = cls(
            index=index,
            metric=metric,
            base_weight=base_weight,
            weight=weight,
            missing_score=missing_score,
            metric_freshness=metric_freshness,
        )

        graph_metric_rerank.additional_properties = d
        return graph_metric_rerank

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
