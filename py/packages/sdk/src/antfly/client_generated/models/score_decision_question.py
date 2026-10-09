from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.score_decision_question_type import ScoreDecisionQuestionType

if TYPE_CHECKING:
    from ..models.decision_level import DecisionLevel


T = TypeVar("T", bound="ScoreDecisionQuestion")


@_attrs_define
class ScoreDecisionQuestion:
    """
    Attributes:
        type_ (ScoreDecisionQuestionType):
        name (str):
        instructions (str):
        levels (list[DecisionLevel]):
    """

    type_: ScoreDecisionQuestionType
    name: str
    instructions: str
    levels: list[DecisionLevel]

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        name = self.name

        instructions = self.instructions

        levels = []
        for levels_item_data in self.levels:
            levels_item = levels_item_data.to_dict()
            levels.append(levels_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "name": name,
                "instructions": instructions,
                "levels": levels,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_level import DecisionLevel

        d = dict(src_dict)
        type_ = ScoreDecisionQuestionType(d.pop("type"))

        name = d.pop("name")

        instructions = d.pop("instructions")

        levels = []
        _levels = d.pop("levels")
        for levels_item_data in _levels:
            levels_item = DecisionLevel.from_dict(levels_item_data)

            levels.append(levels_item)

        score_decision_question = cls(
            type_=type_,
            name=name,
            instructions=instructions,
            levels=levels,
        )

        return score_decision_question
