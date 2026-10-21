from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.generated_multi_choice_decision_question_type import GeneratedMultiChoiceDecisionQuestionType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_choice import DecisionChoice


T = TypeVar("T", bound="GeneratedMultiChoiceDecisionQuestion")


@_attrs_define
class GeneratedMultiChoiceDecisionQuestion:
    """
    Attributes:
        type_ (GeneratedMultiChoiceDecisionQuestionType):
        name (str):
        choices (list[DecisionChoice]):
        instructions (str | Unset): May inherit nonempty generated decider instructions.
    """

    type_: GeneratedMultiChoiceDecisionQuestionType
    name: str
    choices: list[DecisionChoice]
    instructions: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        name = self.name

        choices = []
        for choices_item_data in self.choices:
            choices_item = choices_item_data.to_dict()
            choices.append(choices_item)

        instructions = self.instructions

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "name": name,
                "choices": choices,
            }
        )
        if instructions is not UNSET:
            field_dict["instructions"] = instructions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_choice import DecisionChoice

        d = dict(src_dict)
        type_ = GeneratedMultiChoiceDecisionQuestionType(d.pop("type"))

        name = d.pop("name")

        choices = []
        _choices = d.pop("choices")
        for choices_item_data in _choices:
            choices_item = DecisionChoice.from_dict(choices_item_data)

            choices.append(choices_item)

        instructions = d.pop("instructions", UNSET)

        generated_multi_choice_decision_question = cls(
            type_=type_,
            name=name,
            choices=choices,
            instructions=instructions,
        )

        return generated_multi_choice_decision_question
