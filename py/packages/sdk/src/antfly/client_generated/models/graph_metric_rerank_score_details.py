from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricRerankScoreDetails")


@_attrs_define
class GraphMetricRerankScoreDetails:
    """
    Attributes:
        index_name (str): Graph index that provided the metric score.
        metric_name (str): Graph metric used as a score feature.
        base_score (float): Hit score before graph metric rerank composition.
        base_weight (float): Weight applied to the base score.
        metric_score_used (float): Metric feature value used in the formula after applying missing_score fallback if
            needed.
        metric_weight (float): Weight applied to the metric score feature.
        missing_score_used (bool): True when metric_score was missing and the request's missing_score fallback was used.
        final_score (float): Final hit score after graph metric rerank composition.
        published_generation (int): Published graph metric score generation used for this hit.
        metric_score (float | None | Unset): Published metric score for this hit, or null when the hit was missing from
            the metric generation.
    """

    index_name: str
    metric_name: str
    base_score: float
    base_weight: float
    metric_score_used: float
    metric_weight: float
    missing_score_used: bool
    final_score: float
    published_generation: int
    metric_score: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_name = self.index_name

        metric_name = self.metric_name

        base_score = self.base_score

        base_weight = self.base_weight

        metric_score_used = self.metric_score_used

        metric_weight = self.metric_weight

        missing_score_used = self.missing_score_used

        final_score = self.final_score

        published_generation = self.published_generation

        metric_score: float | None | Unset
        if isinstance(self.metric_score, Unset):
            metric_score = UNSET
        else:
            metric_score = self.metric_score

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_name": index_name,
                "metric_name": metric_name,
                "base_score": base_score,
                "base_weight": base_weight,
                "metric_score_used": metric_score_used,
                "metric_weight": metric_weight,
                "missing_score_used": missing_score_used,
                "final_score": final_score,
                "published_generation": published_generation,
            }
        )
        if metric_score is not UNSET:
            field_dict["metric_score"] = metric_score

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index_name = d.pop("index_name")

        metric_name = d.pop("metric_name")

        base_score = d.pop("base_score")

        base_weight = d.pop("base_weight")

        metric_score_used = d.pop("metric_score_used")

        metric_weight = d.pop("metric_weight")

        missing_score_used = d.pop("missing_score_used")

        final_score = d.pop("final_score")

        published_generation = d.pop("published_generation")

        def _parse_metric_score(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        metric_score = _parse_metric_score(d.pop("metric_score", UNSET))

        graph_metric_rerank_score_details = cls(
            index_name=index_name,
            metric_name=metric_name,
            base_score=base_score,
            base_weight=base_weight,
            metric_score_used=metric_score_used,
            metric_weight=metric_weight,
            missing_score_used=missing_score_used,
            final_score=final_score,
            published_generation=published_generation,
            metric_score=metric_score,
        )

        graph_metric_rerank_score_details.additional_properties = d
        return graph_metric_rerank_score_details

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
