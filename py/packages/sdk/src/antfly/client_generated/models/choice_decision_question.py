from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.choice_decision_question_type import ChoiceDecisionQuestionType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_choice import DecisionChoice
    from ..models.embedding_decision_acceptance import EmbeddingDecisionAcceptance


T = TypeVar("T", bound="ChoiceDecisionQuestion")


@_attrs_define
class ChoiceDecisionQuestion:
    """
    Attributes:
        type_ (ChoiceDecisionQuestionType):
        name (str):
        instructions (str):
        choices (list[DecisionChoice]):
        embedding_options (EmbeddingDecisionAcceptance | Unset): Question-specific acceptance policy for embedding
            similarity. Trained decision models reject it.
    """

    type_: ChoiceDecisionQuestionType
    name: str
    instructions: str
    choices: list[DecisionChoice]
    embedding_options: EmbeddingDecisionAcceptance | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        name = self.name

        instructions = self.instructions

        choices = []
        for choices_item_data in self.choices:
            choices_item = choices_item_data.to_dict()
            choices.append(choices_item)

        embedding_options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding_options, Unset):
            embedding_options = self.embedding_options.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "name": name,
                "instructions": instructions,
                "choices": choices,
            }
        )
        if embedding_options is not UNSET:
            field_dict["embedding_options"] = embedding_options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_choice import DecisionChoice
        from ..models.embedding_decision_acceptance import EmbeddingDecisionAcceptance

        d = dict(src_dict)
        type_ = ChoiceDecisionQuestionType(d.pop("type"))

        name = d.pop("name")

        instructions = d.pop("instructions")

        choices = []
        _choices = d.pop("choices")
        for choices_item_data in _choices:
            choices_item = DecisionChoice.from_dict(choices_item_data)

            choices.append(choices_item)

        _embedding_options = d.pop("embedding_options", UNSET)
        embedding_options: EmbeddingDecisionAcceptance | Unset
        if isinstance(_embedding_options, Unset):
            embedding_options = UNSET
        else:
            embedding_options = EmbeddingDecisionAcceptance.from_dict(_embedding_options)

        choice_decision_question = cls(
            type_=type_,
            name=name,
            instructions=instructions,
            choices=choices,
            embedding_options=embedding_options,
        )

        return choice_decision_question
