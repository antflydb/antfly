from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.predicate_decision_question_type import PredicateDecisionQuestionType

T = TypeVar("T", bound="PredicateDecisionQuestion")


@_attrs_define
class PredicateDecisionQuestion:
    """Estimate whether the condition in instructions holds for the input. The trained answer reports probability in [0,1].

    Attributes:
        type_ (PredicateDecisionQuestionType):
        name (str):
        instructions (str):
    """

    type_: PredicateDecisionQuestionType
    name: str
    instructions: str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        name = self.name

        instructions = self.instructions

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "name": name,
                "instructions": instructions,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = PredicateDecisionQuestionType(d.pop("type"))

        name = d.pop("name")

        instructions = d.pop("instructions")

        predicate_decision_question = cls(
            type_=type_,
            name=name,
            instructions=instructions,
        )

        return predicate_decision_question
