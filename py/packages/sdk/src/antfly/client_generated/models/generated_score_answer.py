from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.generated_score_answer_decision_method import GeneratedScoreAnswerDecisionMethod
from ..models.generated_score_answer_probability_source import GeneratedScoreAnswerProbabilitySource
from ..models.generated_score_answer_type import GeneratedScoreAnswerType

T = TypeVar("T", bound="GeneratedScoreAnswer")


@_attrs_define
class GeneratedScoreAnswer:
    """
    Attributes:
        name (str):
        type_ (GeneratedScoreAnswerType):
        decision_method (GeneratedScoreAnswerDecisionMethod):
        probability_source (GeneratedScoreAnswerProbabilitySource): Measured decoder evidence, or an explicit verdict-
            only 0/1 assertion. No model-written confidence.
        raw_structured_answer (float):
        score (float):
    """

    name: str
    type_: GeneratedScoreAnswerType
    decision_method: GeneratedScoreAnswerDecisionMethod
    probability_source: GeneratedScoreAnswerProbabilitySource
    raw_structured_answer: float
    score: float

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        probability_source = self.probability_source.value

        raw_structured_answer = self.raw_structured_answer

        score = self.score

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
                "decision_method": decision_method,
                "probability_source": probability_source,
                "raw_structured_answer": raw_structured_answer,
                "score": score,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        type_ = GeneratedScoreAnswerType(d.pop("type"))

        decision_method = GeneratedScoreAnswerDecisionMethod(d.pop("decision_method"))

        probability_source = GeneratedScoreAnswerProbabilitySource(d.pop("probability_source"))

        raw_structured_answer = d.pop("raw_structured_answer")

        score = d.pop("score")

        generated_score_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            probability_source=probability_source,
            raw_structured_answer=raw_structured_answer,
            score=score,
        )

        return generated_score_answer
