from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_decide_answer_type import InferenceDecideAnswerType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_decide_answer_legend import InferenceDecideAnswerLegend
    from ..models.inference_decide_answer_probabilities import InferenceDecideAnswerProbabilities


T = TypeVar("T", bound="InferenceDecideAnswer")


@_attrs_define
class InferenceDecideAnswer:
    """
    Attributes:
        type_ (InferenceDecideAnswerType):
        choice (str | Unset):
        score (float | Unset):
        noul (float | Unset):
        legend (InferenceDecideAnswerLegend | Unset):
        probabilities (InferenceDecideAnswerProbabilities | Unset):
    """

    type_: InferenceDecideAnswerType
    choice: str | Unset = UNSET
    score: float | Unset = UNSET
    noul: float | Unset = UNSET
    legend: InferenceDecideAnswerLegend | Unset = UNSET
    probabilities: InferenceDecideAnswerProbabilities | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        choice = self.choice

        score = self.score

        noul = self.noul

        legend: dict[str, Any] | Unset = UNSET
        if not isinstance(self.legend, Unset):
            legend = self.legend.to_dict()

        probabilities: dict[str, Any] | Unset = UNSET
        if not isinstance(self.probabilities, Unset):
            probabilities = self.probabilities.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if choice is not UNSET:
            field_dict["choice"] = choice
        if score is not UNSET:
            field_dict["score"] = score
        if noul is not UNSET:
            field_dict["noul"] = noul
        if legend is not UNSET:
            field_dict["legend"] = legend
        if probabilities is not UNSET:
            field_dict["probabilities"] = probabilities

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_decide_answer_legend import InferenceDecideAnswerLegend
        from ..models.inference_decide_answer_probabilities import InferenceDecideAnswerProbabilities

        d = dict(src_dict)
        type_ = InferenceDecideAnswerType(d.pop("type"))

        choice = d.pop("choice", UNSET)

        score = d.pop("score", UNSET)

        noul = d.pop("noul", UNSET)

        _legend = d.pop("legend", UNSET)
        legend: InferenceDecideAnswerLegend | Unset
        if isinstance(_legend, Unset):
            legend = UNSET
        else:
            legend = InferenceDecideAnswerLegend.from_dict(_legend)

        _probabilities = d.pop("probabilities", UNSET)
        probabilities: InferenceDecideAnswerProbabilities | Unset
        if isinstance(_probabilities, Unset):
            probabilities = UNSET
        else:
            probabilities = InferenceDecideAnswerProbabilities.from_dict(_probabilities)

        inference_decide_answer = cls(
            type_=type_,
            choice=choice,
            score=score,
            noul=noul,
            legend=legend,
            probabilities=probabilities,
        )

        inference_decide_answer.additional_properties = d
        return inference_decide_answer

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
