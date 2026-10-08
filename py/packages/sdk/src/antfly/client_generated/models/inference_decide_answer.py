from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_decide_answer_abstention_reason import InferenceDecideAnswerAbstentionReason
from ..models.inference_decide_answer_decision_method import InferenceDecideAnswerDecisionMethod
from ..models.inference_decide_answer_status import InferenceDecideAnswerStatus
from ..models.inference_decide_answer_type import InferenceDecideAnswerType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_decide_answer_legend import InferenceDecideAnswerLegend
    from ..models.inference_decide_answer_probabilities import InferenceDecideAnswerProbabilities
    from ..models.inference_decide_answer_similarities import InferenceDecideAnswerSimilarities


T = TypeVar("T", bound="InferenceDecideAnswer")


@_attrs_define
class InferenceDecideAnswer:
    """
    Attributes:
        type_ (InferenceDecideAnswerType):
        prototype_set_hash (str | Unset):
        calibration_id (str | Unset):
        choice (None | str | Unset): Selected option, or null when a similarity decision abstains.
        decision_method (InferenceDecideAnswerDecisionMethod | Unset):
        similarities (InferenceDecideAnswerSimilarities | Unset):
        margin (float | Unset):
        status (InferenceDecideAnswerStatus | Unset):
        abstention_reason (InferenceDecideAnswerAbstentionReason | Unset):
        score (float | Unset):
        noul (float | Unset):
        legend (InferenceDecideAnswerLegend | Unset):
        probabilities (InferenceDecideAnswerProbabilities | Unset):
    """

    type_: InferenceDecideAnswerType
    prototype_set_hash: str | Unset = UNSET
    calibration_id: str | Unset = UNSET
    choice: None | str | Unset = UNSET
    decision_method: InferenceDecideAnswerDecisionMethod | Unset = UNSET
    similarities: InferenceDecideAnswerSimilarities | Unset = UNSET
    margin: float | Unset = UNSET
    status: InferenceDecideAnswerStatus | Unset = UNSET
    abstention_reason: InferenceDecideAnswerAbstentionReason | Unset = UNSET
    score: float | Unset = UNSET
    noul: float | Unset = UNSET
    legend: InferenceDecideAnswerLegend | Unset = UNSET
    probabilities: InferenceDecideAnswerProbabilities | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        prototype_set_hash = self.prototype_set_hash

        calibration_id = self.calibration_id

        choice: None | str | Unset
        if isinstance(self.choice, Unset):
            choice = UNSET
        else:
            choice = self.choice

        decision_method: str | Unset = UNSET
        if not isinstance(self.decision_method, Unset):
            decision_method = self.decision_method.value

        similarities: dict[str, Any] | Unset = UNSET
        if not isinstance(self.similarities, Unset):
            similarities = self.similarities.to_dict()

        margin = self.margin

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        abstention_reason: str | Unset = UNSET
        if not isinstance(self.abstention_reason, Unset):
            abstention_reason = self.abstention_reason.value

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
        if prototype_set_hash is not UNSET:
            field_dict["prototype_set_hash"] = prototype_set_hash
        if calibration_id is not UNSET:
            field_dict["calibration_id"] = calibration_id
        if choice is not UNSET:
            field_dict["choice"] = choice
        if decision_method is not UNSET:
            field_dict["decision_method"] = decision_method
        if similarities is not UNSET:
            field_dict["similarities"] = similarities
        if margin is not UNSET:
            field_dict["margin"] = margin
        if status is not UNSET:
            field_dict["status"] = status
        if abstention_reason is not UNSET:
            field_dict["abstention_reason"] = abstention_reason
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
        from ..models.inference_decide_answer_similarities import InferenceDecideAnswerSimilarities

        d = dict(src_dict)
        type_ = InferenceDecideAnswerType(d.pop("type"))

        prototype_set_hash = d.pop("prototype_set_hash", UNSET)

        calibration_id = d.pop("calibration_id", UNSET)

        def _parse_choice(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        choice = _parse_choice(d.pop("choice", UNSET))

        _decision_method = d.pop("decision_method", UNSET)
        decision_method: InferenceDecideAnswerDecisionMethod | Unset
        if isinstance(_decision_method, Unset):
            decision_method = UNSET
        else:
            decision_method = InferenceDecideAnswerDecisionMethod(_decision_method)

        _similarities = d.pop("similarities", UNSET)
        similarities: InferenceDecideAnswerSimilarities | Unset
        if isinstance(_similarities, Unset):
            similarities = UNSET
        else:
            similarities = InferenceDecideAnswerSimilarities.from_dict(_similarities)

        margin = d.pop("margin", UNSET)

        _status = d.pop("status", UNSET)
        status: InferenceDecideAnswerStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = InferenceDecideAnswerStatus(_status)

        _abstention_reason = d.pop("abstention_reason", UNSET)
        abstention_reason: InferenceDecideAnswerAbstentionReason | Unset
        if isinstance(_abstention_reason, Unset):
            abstention_reason = UNSET
        else:
            abstention_reason = InferenceDecideAnswerAbstentionReason(_abstention_reason)

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
            prototype_set_hash=prototype_set_hash,
            calibration_id=calibration_id,
            choice=choice,
            decision_method=decision_method,
            similarities=similarities,
            margin=margin,
            status=status,
            abstention_reason=abstention_reason,
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
