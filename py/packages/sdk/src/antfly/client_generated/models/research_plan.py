from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_sub_question import ResearchSubQuestion


T = TypeVar("T", bound="ResearchPlan")


@_attrs_define
class ResearchPlan:
    """
    Attributes:
        brief (str): Research brief restating scope, assumptions and deliverable.
        sub_questions (list[ResearchSubQuestion]): Planned and reflection-added sub-questions.
        success_criteria (list[str] | Unset): What a complete answer must cover.
    """

    brief: str
    sub_questions: list[ResearchSubQuestion]
    success_criteria: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        brief = self.brief

        sub_questions = []
        for sub_questions_item_data in self.sub_questions:
            sub_questions_item = sub_questions_item_data.to_dict()
            sub_questions.append(sub_questions_item)

        success_criteria: list[str] | Unset = UNSET
        if not isinstance(self.success_criteria, Unset):
            success_criteria = self.success_criteria

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "brief": brief,
                "sub_questions": sub_questions,
            }
        )
        if success_criteria is not UNSET:
            field_dict["success_criteria"] = success_criteria

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_sub_question import ResearchSubQuestion

        d = dict(src_dict)
        brief = d.pop("brief")

        sub_questions = []
        _sub_questions = d.pop("sub_questions")
        for sub_questions_item_data in _sub_questions:
            sub_questions_item = ResearchSubQuestion.from_dict(sub_questions_item_data)

            sub_questions.append(sub_questions_item)

        success_criteria = cast(list[str], d.pop("success_criteria", UNSET))

        research_plan = cls(
            brief=brief,
            sub_questions=sub_questions,
            success_criteria=success_criteria,
        )

        research_plan.additional_properties = d
        return research_plan

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
