from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.research_sub_question_sources_item import ResearchSubQuestionSourcesItem
from ..models.research_sub_question_status import ResearchSubQuestionStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchSubQuestion")


@_attrs_define
class ResearchSubQuestion:
    """
    Attributes:
        id (str): Stable sub-question identifier within the run. Example: q1.
        question (str): Self-contained question a researcher can answer.
        rationale (str | Unset): Why this sub-question matters for the brief.
        sources (list[ResearchSubQuestionSourcesItem] | Unset): Evidence sources the planner expects to be useful.
        round_ (int | Unset): Research round that introduced the sub-question.
        status (ResearchSubQuestionStatus | Unset): Research status.
    """

    id: str
    question: str
    rationale: str | Unset = UNSET
    sources: list[ResearchSubQuestionSourcesItem] | Unset = UNSET
    round_: int | Unset = UNSET
    status: ResearchSubQuestionStatus | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        question = self.question

        rationale = self.rationale

        sources: list[str] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.value
                sources.append(sources_item)

        round_ = self.round_

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "question": question,
            }
        )
        if rationale is not UNSET:
            field_dict["rationale"] = rationale
        if sources is not UNSET:
            field_dict["sources"] = sources
        if round_ is not UNSET:
            field_dict["round"] = round_
        if status is not UNSET:
            field_dict["status"] = status

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        question = d.pop("question")

        rationale = d.pop("rationale", UNSET)

        _sources = d.pop("sources", UNSET)
        sources: list[ResearchSubQuestionSourcesItem] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = ResearchSubQuestionSourcesItem(sources_item_data)

                sources.append(sources_item)

        round_ = d.pop("round", UNSET)

        _status = d.pop("status", UNSET)
        status: ResearchSubQuestionStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = ResearchSubQuestionStatus(_status)

        research_sub_question = cls(
            id=id,
            question=question,
            rationale=rationale,
            sources=sources,
            round_=round_,
            status=status,
        )

        research_sub_question.additional_properties = d
        return research_sub_question

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
