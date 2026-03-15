from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.eval_scores import EvalScores
    from ..models.eval_summary import EvalSummary


T = TypeVar("T", bound="EvalResult")


@_attrs_define
class EvalResult:
    """Complete evaluation result

    Attributes:
        scores (Union[Unset, EvalScores]): Scores organized by category
        summary (Union[Unset, EvalSummary]): Aggregate statistics across all evaluators
        duration_ms (Union[Unset, int]): Total evaluation duration in milliseconds
    """

    scores: Union[Unset, "EvalScores"] = UNSET
    summary: Union[Unset, "EvalSummary"] = UNSET
    duration_ms: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        scores: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.scores, Unset):
            scores = self.scores.to_dict()

        summary: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.summary, Unset):
            summary = self.summary.to_dict()

        duration_ms = self.duration_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if scores is not UNSET:
            field_dict["scores"] = scores
        if summary is not UNSET:
            field_dict["summary"] = summary
        if duration_ms is not UNSET:
            field_dict["duration_ms"] = duration_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_scores import EvalScores
        from ..models.eval_summary import EvalSummary

        d = dict(src_dict)
        _scores = d.pop("scores", UNSET)
        scores: Union[Unset, EvalScores]
        if isinstance(_scores, Unset):
            scores = UNSET
        else:
            scores = EvalScores.from_dict(_scores)

        _summary = d.pop("summary", UNSET)
        summary: Union[Unset, EvalSummary]
        if isinstance(_summary, Unset):
            summary = UNSET
        else:
            summary = EvalSummary.from_dict(_summary)

        duration_ms = d.pop("duration_ms", UNSET)

        eval_result = cls(
            scores=scores,
            summary=summary,
            duration_ms=duration_ms,
        )

        eval_result.additional_properties = d
        return eval_result

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
