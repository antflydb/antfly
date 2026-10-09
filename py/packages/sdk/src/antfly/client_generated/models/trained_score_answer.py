from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.trained_score_answer_confidence_method import TrainedScoreAnswerConfidenceMethod
from ..models.trained_score_answer_decision_method import TrainedScoreAnswerDecisionMethod
from ..models.trained_score_answer_type import TrainedScoreAnswerType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_level_probability import DecisionLevelProbability


T = TypeVar("T", bound="TrainedScoreAnswer")


@_attrs_define
class TrainedScoreAnswer:
    """
    Attributes:
        name (str):
        type_ (TrainedScoreAnswerType):
        decision_method (TrainedScoreAnswerDecisionMethod):
        confidence (float): Model diagnostic; entropy confidence is not the probability that the chosen label is
            correct.
        confidence_method (TrainedScoreAnswerConfidenceMethod):
        score (float):
        probabilities (list[DecisionLevelProbability]):
        act_probability (float | Unset): Applicable action-head diagnostic. It does not authorize or execute an action.
    """

    name: str
    type_: TrainedScoreAnswerType
    decision_method: TrainedScoreAnswerDecisionMethod
    confidence: float
    confidence_method: TrainedScoreAnswerConfidenceMethod
    score: float
    probabilities: list[DecisionLevelProbability]
    act_probability: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        confidence = self.confidence

        confidence_method = self.confidence_method.value

        score = self.score

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
                "score": score,
                "probabilities": probabilities,
            }
        )
        if act_probability is not UNSET:
            field_dict["act_probability"] = act_probability

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_level_probability import DecisionLevelProbability

        d = dict(src_dict)
        name = d.pop("name")

        type_ = TrainedScoreAnswerType(d.pop("type"))

        decision_method = TrainedScoreAnswerDecisionMethod(d.pop("decision_method"))

        confidence = d.pop("confidence")

        confidence_method = TrainedScoreAnswerConfidenceMethod(d.pop("confidence_method"))

        score = d.pop("score")

        probabilities = []
        _probabilities = d.pop("probabilities")
        for probabilities_item_data in _probabilities:
            probabilities_item = DecisionLevelProbability.from_dict(probabilities_item_data)

            probabilities.append(probabilities_item)

        act_probability = d.pop("act_probability", UNSET)

        trained_score_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            confidence=confidence,
            confidence_method=confidence_method,
            score=score,
            probabilities=probabilities,
            act_probability=act_probability,
        )

        return trained_score_answer
