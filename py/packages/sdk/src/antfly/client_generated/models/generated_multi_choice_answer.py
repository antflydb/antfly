from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.generated_multi_choice_answer_decision_method import GeneratedMultiChoiceAnswerDecisionMethod
from ..models.generated_multi_choice_answer_probability_source import GeneratedMultiChoiceAnswerProbabilitySource
from ..models.generated_multi_choice_answer_type import GeneratedMultiChoiceAnswerType

if TYPE_CHECKING:
    from ..models.decision_choice_probability import DecisionChoiceProbability


T = TypeVar("T", bound="GeneratedMultiChoiceAnswer")


@_attrs_define
class GeneratedMultiChoiceAnswer:
    """
    Attributes:
        name (str):
        type_ (GeneratedMultiChoiceAnswerType):
        decision_method (GeneratedMultiChoiceAnswerDecisionMethod):
        probability_source (GeneratedMultiChoiceAnswerProbabilitySource): Measured decoder evidence, or an explicit
            verdict-only 0/1 assertion. No model-written confidence.
        raw_structured_answer (list[str]):
        choices (list[str]):
        probabilities (list[DecisionChoiceProbability]):
    """

    name: str
    type_: GeneratedMultiChoiceAnswerType
    decision_method: GeneratedMultiChoiceAnswerDecisionMethod
    probability_source: GeneratedMultiChoiceAnswerProbabilitySource
    raw_structured_answer: list[str]
    choices: list[str]
    probabilities: list[DecisionChoiceProbability]

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        decision_method = self.decision_method.value

        probability_source = self.probability_source.value

        raw_structured_answer = self.raw_structured_answer

        choices = self.choices

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
                "choices": choices,
                "probabilities": probabilities,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_choice_probability import DecisionChoiceProbability

        d = dict(src_dict)
        name = d.pop("name")

        type_ = GeneratedMultiChoiceAnswerType(d.pop("type"))

        decision_method = GeneratedMultiChoiceAnswerDecisionMethod(d.pop("decision_method"))

        probability_source = GeneratedMultiChoiceAnswerProbabilitySource(d.pop("probability_source"))

        raw_structured_answer = cast(list[str], d.pop("raw_structured_answer"))

        choices = cast(list[str], d.pop("choices"))

        probabilities = []
        _probabilities = d.pop("probabilities")
        for probabilities_item_data in _probabilities:
            probabilities_item = DecisionChoiceProbability.from_dict(probabilities_item_data)

            probabilities.append(probabilities_item)

        generated_multi_choice_answer = cls(
            name=name,
            type_=type_,
            decision_method=decision_method,
            probability_source=probability_source,
            raw_structured_answer=raw_structured_answer,
            choices=choices,
            probabilities=probabilities,
        )

        return generated_multi_choice_answer
