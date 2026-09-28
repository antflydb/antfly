from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchReflection")


@_attrs_define
class ResearchReflection:
    """
    Attributes:
        round_ (int | Unset): Round that was reflected on.
        done (bool | Unset): Whether the reflector judged coverage sufficient.
        gaps (list[str] | Unset): Coverage gaps against the brief and success criteria.
        contradictions (list[str] | Unset): Conflicting findings that need resolution or disclosure.
        new_sub_questions (list[str] | Unset): Sub-questions added for the next round.
    """

    round_: int | Unset = UNSET
    done: bool | Unset = UNSET
    gaps: list[str] | Unset = UNSET
    contradictions: list[str] | Unset = UNSET
    new_sub_questions: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        round_ = self.round_

        done = self.done

        gaps: list[str] | Unset = UNSET
        if not isinstance(self.gaps, Unset):
            gaps = self.gaps

        contradictions: list[str] | Unset = UNSET
        if not isinstance(self.contradictions, Unset):
            contradictions = self.contradictions

        new_sub_questions: list[str] | Unset = UNSET
        if not isinstance(self.new_sub_questions, Unset):
            new_sub_questions = self.new_sub_questions

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if round_ is not UNSET:
            field_dict["round"] = round_
        if done is not UNSET:
            field_dict["done"] = done
        if gaps is not UNSET:
            field_dict["gaps"] = gaps
        if contradictions is not UNSET:
            field_dict["contradictions"] = contradictions
        if new_sub_questions is not UNSET:
            field_dict["new_sub_questions"] = new_sub_questions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        round_ = d.pop("round", UNSET)

        done = d.pop("done", UNSET)

        gaps = cast(list[str], d.pop("gaps", UNSET))

        contradictions = cast(list[str], d.pop("contradictions", UNSET))

        new_sub_questions = cast(list[str], d.pop("new_sub_questions", UNSET))

        research_reflection = cls(
            round_=round_,
            done=done,
            gaps=gaps,
            contradictions=contradictions,
            new_sub_questions=new_sub_questions,
        )

        research_reflection.additional_properties = d
        return research_reflection

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
