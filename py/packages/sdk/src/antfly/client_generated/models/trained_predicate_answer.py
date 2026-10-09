from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.trained_predicate_answer_confidence_method import TrainedPredicateAnswerConfidenceMethod
from ..models.trained_predicate_answer_decision_method import TrainedPredicateAnswerDecisionMethod
from ..models.trained_predicate_answer_type import TrainedPredicateAnswerType
from ..types import UNSET, Unset

T = TypeVar("T", bound="TrainedPredicateAnswer")


@_attrs_define
class TrainedPredicateAnswer:
    """
    Attributes:
        name (str):
        type_ (TrainedPredicateAnswerType):
        decision_method (TrainedPredicateAnswerDecisionMethod):
        probability (float): The model-estimated probability that the condition in instructions holds.
        confidence (float | Unset): Model diagnostic; entropy confidence is not the probability that the chosen label is
            correct.
        confidence_method (TrainedPredicateAnswerConfidenceMethod | Unset):
        act_probability (float | Unset): Applicable action-head diagnostic. It does not authorize or execute an action.
    """

    name: str
    type_: TrainedPredicateAnswerType
    decision_method: TrainedPredicateAnswerDecisionMethod
    probability: float
    confidence: float | Unset = UNSET
    confidence_method: TrainedPredicateAnswerConfidenceMethod | Unset = UNSET
    act_probability: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        probability = self.probability

        confidence = self.confidence

        confidence_method: str | Unset = UNSET
        if not isinstance(self.confidence_method, Unset):
            confidence_method = self.confidence_method.value

        act_probability = self.act_probability

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
                "decision_method": decision_method,
                "probability": probability,
            }
        )
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if confidence_method is not UNSET:
            field_dict["confidence_method"] = confidence_method
        if act_probability is not UNSET:
            field_dict["act_probability"] = act_probability

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        type_ = TrainedPredicateAnswerType(d.pop("type"))

        decision_method = TrainedPredicateAnswerDecisionMethod(d.pop("decision_method"))

        probability = d.pop("probability")

        confidence = d.pop("confidence", UNSET)

        _confidence_method = d.pop("confidence_method", UNSET)
        confidence_method: TrainedPredicateAnswerConfidenceMethod | Unset
        if isinstance(_confidence_method, Unset):
            confidence_method = UNSET
        else:
            confidence_method = TrainedPredicateAnswerConfidenceMethod(_confidence_method)

        act_probability = d.pop("act_probability", UNSET)

        trained_predicate_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            probability=probability,
            confidence=confidence,
            confidence_method=confidence_method,
            act_probability=act_probability,
        )

        return trained_predicate_answer
