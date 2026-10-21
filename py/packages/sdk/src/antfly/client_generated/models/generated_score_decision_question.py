from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.generated_score_decision_question_type import GeneratedScoreDecisionQuestionType
from ..types import UNSET, Unset

T = TypeVar("T", bound="GeneratedScoreDecisionQuestion")


@_attrs_define
class GeneratedScoreDecisionQuestion:
    """
    Attributes:
        type_ (GeneratedScoreDecisionQuestionType):
        name (str):
        instructions (str | Unset): May inherit nonempty generated decider instructions.
    """

    type_: GeneratedScoreDecisionQuestionType
    name: str
    instructions: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        name = self.name

        instructions = self.instructions

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "name": name,
            }
        )
        if instructions is not UNSET:
            field_dict["instructions"] = instructions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = GeneratedScoreDecisionQuestionType(d.pop("type"))

        name = d.pop("name")

        instructions = d.pop("instructions", UNSET)

        generated_score_decision_question = cls(
            type_=type_,
            name=name,
            instructions=instructions,
        )

        return generated_score_decision_question
