from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.generated_choice_answer_decision_method import GeneratedChoiceAnswerDecisionMethod
from ..models.generated_choice_answer_probability_source import GeneratedChoiceAnswerProbabilitySource
from ..models.generated_choice_answer_type import GeneratedChoiceAnswerType

if TYPE_CHECKING:
    from ..models.decision_choice_probability import DecisionChoiceProbability


T = TypeVar("T", bound="GeneratedChoiceAnswer")


@_attrs_define
class GeneratedChoiceAnswer:
    """
    Attributes:
        name (str):
        type_ (GeneratedChoiceAnswerType):
        decision_method (GeneratedChoiceAnswerDecisionMethod):
        probability_source (GeneratedChoiceAnswerProbabilitySource): Measured decoder evidence, or an explicit verdict-
            only 0/1 assertion. No model-written confidence.
        raw_structured_answer (str):
        choice (str):
        probabilities (list[DecisionChoiceProbability]):
    """

    name: str
    type_: GeneratedChoiceAnswerType
    decision_method: GeneratedChoiceAnswerDecisionMethod
    probability_source: GeneratedChoiceAnswerProbabilitySource
    raw_structured_answer: str
    choice: str
    probabilities: list[DecisionChoiceProbability]

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        probability_source = self.probability_source.value

        raw_structured_answer = self.raw_structured_answer

        choice = self.choice

        probabilities = []
        for probabilities_item_data in self.probabilities:
            probabilities_item = probabilities_item_data.to_dict()
            probabilities.append(probabilities_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
                "decision_method": decision_method,
                "probability_source": probability_source,
                "raw_structured_answer": raw_structured_answer,
                "choice": choice,
                "probabilities": probabilities,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_choice_probability import DecisionChoiceProbability

        d = dict(src_dict)
        name = d.pop("name")

        type_ = GeneratedChoiceAnswerType(d.pop("type"))

        decision_method = GeneratedChoiceAnswerDecisionMethod(d.pop("decision_method"))

        probability_source = GeneratedChoiceAnswerProbabilitySource(d.pop("probability_source"))

        raw_structured_answer = d.pop("raw_structured_answer")

        choice = d.pop("choice")

        probabilities = []
        _probabilities = d.pop("probabilities")
        for probabilities_item_data in _probabilities:
            probabilities_item = DecisionChoiceProbability.from_dict(probabilities_item_data)

            probabilities.append(probabilities_item)

        generated_choice_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            probability_source=probability_source,
            raw_structured_answer=raw_structured_answer,
            choice=choice,
            probabilities=probabilities,
        )

        return generated_choice_answer
