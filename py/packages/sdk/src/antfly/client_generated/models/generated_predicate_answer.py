from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.generated_predicate_answer_decision_method import GeneratedPredicateAnswerDecisionMethod
from ..models.generated_predicate_answer_probability_source import GeneratedPredicateAnswerProbabilitySource
from ..models.generated_predicate_answer_type import GeneratedPredicateAnswerType

T = TypeVar("T", bound="GeneratedPredicateAnswer")


@_attrs_define
class GeneratedPredicateAnswer:
    """
    Attributes:
        name (str):
        type_ (GeneratedPredicateAnswerType):
        decision_method (GeneratedPredicateAnswerDecisionMethod):
        probability_source (GeneratedPredicateAnswerProbabilitySource): Measured decoder evidence, or an explicit
            verdict-only 0/1 assertion. No model-written confidence.
        raw_structured_answer (bool):
        probability (float):
    """

    name: str
    type_: GeneratedPredicateAnswerType
    decision_method: GeneratedPredicateAnswerDecisionMethod
    probability_source: GeneratedPredicateAnswerProbabilitySource
    raw_structured_answer: bool
    probability: float

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        probability_source = self.probability_source.value

        raw_structured_answer = self.raw_structured_answer

        probability = self.probability

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
                "decision_method": decision_method,
                "probability_source": probability_source,
                "raw_structured_answer": raw_structured_answer,
                "probability": probability,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        type_ = GeneratedPredicateAnswerType(d.pop("type"))

        decision_method = GeneratedPredicateAnswerDecisionMethod(d.pop("decision_method"))

        probability_source = GeneratedPredicateAnswerProbabilitySource(d.pop("probability_source"))

        raw_structured_answer = d.pop("raw_structured_answer")

        probability = d.pop("probability")

        generated_predicate_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            probability_source=probability_source,
            raw_structured_answer=raw_structured_answer,
            probability=probability,
        )

        return generated_predicate_answer
