from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.inference_decide_question_type import InferenceDecideQuestionType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_decide_question_criteria_type_0 import InferenceDecideQuestionCriteriaType0


T = TypeVar("T", bound="InferenceDecideQuestion")


@_attrs_define
class InferenceDecideQuestion:
    """
    Attributes:
        type_ (InferenceDecideQuestionType):
        instructions (str):
        criteria (InferenceDecideQuestionCriteriaType0 | list[str] | Unset): Choice uses option IDs mapped to
            descriptions; score uses ordered descriptions; noul omits criteria.
    """

    type_: InferenceDecideQuestionType
    instructions: str
    criteria: InferenceDecideQuestionCriteriaType0 | list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.inference_decide_question_criteria_type_0 import InferenceDecideQuestionCriteriaType0

        type_ = self.type_.value

        instructions = self.instructions

        criteria: dict[str, Any] | list[str] | Unset
        if isinstance(self.criteria, Unset):
            criteria = UNSET
        elif isinstance(self.criteria, InferenceDecideQuestionCriteriaType0):
            criteria = self.criteria.to_dict()
        else:
            criteria = self.criteria

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "instructions": instructions,
            }
        )
        if criteria is not UNSET:
            field_dict["criteria"] = criteria

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_decide_question_criteria_type_0 import InferenceDecideQuestionCriteriaType0

        d = dict(src_dict)
        type_ = InferenceDecideQuestionType(d.pop("type"))

        instructions = d.pop("instructions")

        def _parse_criteria(data: object) -> InferenceDecideQuestionCriteriaType0 | list[str] | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                criteria_type_0 = InferenceDecideQuestionCriteriaType0.from_dict(data)

                return criteria_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, list):
                raise TypeError()
            criteria_type_1 = cast(list[str], data)

            return criteria_type_1

        criteria = _parse_criteria(d.pop("criteria", UNSET))

        inference_decide_question = cls(
            type_=type_,
            instructions=instructions,
            criteria=criteria,
        )

        return inference_decide_question
