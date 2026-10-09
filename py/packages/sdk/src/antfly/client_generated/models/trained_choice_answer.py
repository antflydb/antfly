from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.trained_choice_answer_confidence_method import TrainedChoiceAnswerConfidenceMethod
from ..models.trained_choice_answer_decision_method import TrainedChoiceAnswerDecisionMethod
from ..models.trained_choice_answer_type import TrainedChoiceAnswerType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_choice_probability import DecisionChoiceProbability


T = TypeVar("T", bound="TrainedChoiceAnswer")


@_attrs_define
class TrainedChoiceAnswer:
    """
    Attributes:
        name (str):
        type_ (TrainedChoiceAnswerType):
        decision_method (TrainedChoiceAnswerDecisionMethod):
        confidence (float): Model diagnostic; entropy confidence is not the probability that the chosen label is
            correct.
        confidence_method (TrainedChoiceAnswerConfidenceMethod):
        choice (str):
        probabilities (list[DecisionChoiceProbability]):
        act_probability (float | Unset): Applicable action-head diagnostic. It does not authorize or execute an action.
    """

    name: str
    type_: TrainedChoiceAnswerType
    decision_method: TrainedChoiceAnswerDecisionMethod
    confidence: float
    confidence_method: TrainedChoiceAnswerConfidenceMethod
    choice: str
    probabilities: list[DecisionChoiceProbability]
    act_probability: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        confidence = self.confidence

        confidence_method = self.confidence_method.value

        choice = self.choice

        probabilities = []
        for probabilities_item_data in self.probabilities:
            probabilities_item = probabilities_item_data.to_dict()
            probabilities.append(probabilities_item)

        act_probability = self.act_probability

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
                "decision_method": decision_method,
                "confidence": confidence,
                "confidence_method": confidence_method,
                "choice": choice,
                "probabilities": probabilities,
            }
        )
        if act_probability is not UNSET:
            field_dict["act_probability"] = act_probability

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_choice_probability import DecisionChoiceProbability

        d = dict(src_dict)
        name = d.pop("name")

        type_ = TrainedChoiceAnswerType(d.pop("type"))

        decision_method = TrainedChoiceAnswerDecisionMethod(d.pop("decision_method"))

        confidence = d.pop("confidence")

        confidence_method = TrainedChoiceAnswerConfidenceMethod(d.pop("confidence_method"))

        choice = d.pop("choice")

        probabilities = []
        _probabilities = d.pop("probabilities")
        for probabilities_item_data in _probabilities:
            probabilities_item = DecisionChoiceProbability.from_dict(probabilities_item_data)

            probabilities.append(probabilities_item)

        act_probability = d.pop("act_probability", UNSET)

        trained_choice_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            confidence=confidence,
            confidence_method=confidence_method,
            choice=choice,
            probabilities=probabilities,
            act_probability=act_probability,
        )

        return trained_choice_answer
