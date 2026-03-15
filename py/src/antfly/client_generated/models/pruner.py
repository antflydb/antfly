from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="Pruner")


@_attrs_define
class Pruner:
    """Configuration for pruning search results based on score quality.
    Helps filter out low-relevance results in RAG pipelines by detecting
    score gaps or deviations from top results.

        Attributes:
            min_score_ratio (Union[Unset, float]): Keep only results with score >= max_score * min_score_ratio.
                For example, 0.5 keeps results scoring at least half of the top result.
                Applied after fusion scoring.
                 Example: 0.5.
            max_score_gap_percent (Union[Unset, float]): Stop returning results when the gap between consecutive scores
                exceeds this percentage of the total score range (max - min).
                Detects "elbows" in score distributions regardless of score scale.
                For example, 30.0 stops when a gap spans 30% of the score range.
                 Example: 30.
            min_absolute_score (Union[Unset, float]): Hard minimum score threshold. Results with scores below this value
                are excluded regardless of other pruning settings.
                 Example: 0.01.
            require_multi_index (Union[Unset, bool]): Only keep results that appear in multiple indexes (both full-text
                and vector search). Useful for increasing precision by requiring
                agreement between different retrieval methods.
                 Default: False.
            std_dev_threshold (Union[Unset, float]): Keep results within N standard deviations below the mean score.
                For example, 1.0 keeps results with score >= mean - 1*stddev.
                Useful for statistical outlier detection in result sets.
                 Example: 1.5.
    """

    min_score_ratio: Union[Unset, float] = UNSET
    max_score_gap_percent: Union[Unset, float] = UNSET
    min_absolute_score: Union[Unset, float] = UNSET
    require_multi_index: Union[Unset, bool] = False
    std_dev_threshold: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        min_score_ratio = self.min_score_ratio

        max_score_gap_percent = self.max_score_gap_percent

        min_absolute_score = self.min_absolute_score

        require_multi_index = self.require_multi_index

        std_dev_threshold = self.std_dev_threshold

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if min_score_ratio is not UNSET:
            field_dict["min_score_ratio"] = min_score_ratio
        if max_score_gap_percent is not UNSET:
            field_dict["max_score_gap_percent"] = max_score_gap_percent
        if min_absolute_score is not UNSET:
            field_dict["min_absolute_score"] = min_absolute_score
        if require_multi_index is not UNSET:
            field_dict["require_multi_index"] = require_multi_index
        if std_dev_threshold is not UNSET:
            field_dict["std_dev_threshold"] = std_dev_threshold

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        min_score_ratio = d.pop("min_score_ratio", UNSET)

        max_score_gap_percent = d.pop("max_score_gap_percent", UNSET)

        min_absolute_score = d.pop("min_absolute_score", UNSET)

        require_multi_index = d.pop("require_multi_index", UNSET)

        std_dev_threshold = d.pop("std_dev_threshold", UNSET)

        pruner = cls(
            min_score_ratio=min_score_ratio,
            max_score_gap_percent=max_score_gap_percent,
            min_absolute_score=min_absolute_score,
            require_multi_index=require_multi_index,
            std_dev_threshold=std_dev_threshold,
        )

        pruner.additional_properties = d
        return pruner

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
