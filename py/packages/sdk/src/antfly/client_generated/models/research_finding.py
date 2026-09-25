from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_status import AgentStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_claim import ResearchClaim


T = TypeVar("T", bound="ResearchFinding")


@_attrs_define
class ResearchFinding:
    """Compressed researcher output. Raw tool transcripts are not retained.

    Attributes:
        sub_question_id (str): Sub-question this finding answers.
        summary (str): Concise answer grounded in evidence.
        question (str | Unset): The sub-question text.
        claims (list[ResearchClaim] | Unset): Individual claims with supporting evidence.
        open_questions (list[str] | Unset): What the researcher could not establish.
        evidence_ids (list[str] | Unset): Every evidence item the researcher retrieved.
        status (AgentStatus | Unset): Shared bounded-agent execution status
        round_ (int | Unset): Research round.
        llm_calls (int | Unset): Model calls used by this researcher.
        tool_calls (int | Unset): Tool calls used by this researcher.
    """

    sub_question_id: str
    summary: str
    question: str | Unset = UNSET
    claims: list[ResearchClaim] | Unset = UNSET
    open_questions: list[str] | Unset = UNSET
    evidence_ids: list[str] | Unset = UNSET
    status: AgentStatus | Unset = UNSET
    round_: int | Unset = UNSET
    llm_calls: int | Unset = UNSET
    tool_calls: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        sub_question_id = self.sub_question_id

        summary = self.summary

        question = self.question

        claims: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.claims, Unset):
            claims = []
            for claims_item_data in self.claims:
                claims_item = claims_item_data.to_dict()
                claims.append(claims_item)

        open_questions: list[str] | Unset = UNSET
        if not isinstance(self.open_questions, Unset):
            open_questions = self.open_questions

        evidence_ids: list[str] | Unset = UNSET
        if not isinstance(self.evidence_ids, Unset):
            evidence_ids = self.evidence_ids

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        round_ = self.round_

        llm_calls = self.llm_calls

        tool_calls = self.tool_calls

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "sub_question_id": sub_question_id,
                "summary": summary,
            }
        )
        if question is not UNSET:
            field_dict["question"] = question
        if claims is not UNSET:
            field_dict["claims"] = claims
        if open_questions is not UNSET:
            field_dict["open_questions"] = open_questions
        if evidence_ids is not UNSET:
            field_dict["evidence_ids"] = evidence_ids
        if status is not UNSET:
            field_dict["status"] = status
        if round_ is not UNSET:
            field_dict["round"] = round_
        if llm_calls is not UNSET:
            field_dict["llm_calls"] = llm_calls
        if tool_calls is not UNSET:
            field_dict["tool_calls"] = tool_calls

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_claim import ResearchClaim

        d = dict(src_dict)
        sub_question_id = d.pop("sub_question_id")

        summary = d.pop("summary")

        question = d.pop("question", UNSET)

        _claims = d.pop("claims", UNSET)
        claims: list[ResearchClaim] | Unset = UNSET
        if _claims is not UNSET:
            claims = []
            for claims_item_data in _claims:
                claims_item = ResearchClaim.from_dict(claims_item_data)

                claims.append(claims_item)

        open_questions = cast(list[str], d.pop("open_questions", UNSET))

        evidence_ids = cast(list[str], d.pop("evidence_ids", UNSET))

        _status = d.pop("status", UNSET)
        status: AgentStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = AgentStatus(_status)

        round_ = d.pop("round", UNSET)

        llm_calls = d.pop("llm_calls", UNSET)

        tool_calls = d.pop("tool_calls", UNSET)

        research_finding = cls(
            sub_question_id=sub_question_id,
            summary=summary,
            question=question,
            claims=claims,
            open_questions=open_questions,
            evidence_ids=evidence_ids,
            status=status,
            round_=round_,
            llm_calls=llm_calls,
            tool_calls=tool_calls,
        )

        research_finding.additional_properties = d
        return research_finding

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
