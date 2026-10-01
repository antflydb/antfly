from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchBudget")


@_attrs_define
class ResearchBudget:
    """Declared upper bounds for one research run. The worst-case LLM and
    tool-call cost is computable before execution; requests whose worst
    case exceeds the server ceiling are rejected, not clamped.

        Attributes:
            max_rounds (int | Unset): Maximum research rounds (plan or reflect, then fan-out). Default: 2.
            max_sub_questions (int | Unset): Maximum sub-questions researched per round. Default: 4.
            max_parallel (int | Unset): Maximum researchers in flight at once. Default: 2.
            researcher_iterations (int | Unset): Model-generation rounds available to each researcher. Default: 6.
            researcher_tool_calls (int | Unset): Tool calls available to each researcher. Default: 8.
            max_llm_calls (int | Unset): Hard cap on model calls across every role in the run. Default: 80.
            max_tool_calls (int | Unset): Hard cap on tool calls across every researcher in the run. Default: 120.
            max_evidence (int | Unset): Maximum distinct evidence items retained in the registry. Default: 80.
            max_report_tokens (int | Unset): Output token budget for the report writer. Default: 4000.
            deadline_ms (int | Unset): Wall-clock budget for a synchronous run or a single job advance. Default: 600000.
    """

    max_rounds: int | Unset = 2
    max_sub_questions: int | Unset = 4
    max_parallel: int | Unset = 2
    researcher_iterations: int | Unset = 6
    researcher_tool_calls: int | Unset = 8
    max_llm_calls: int | Unset = 80
    max_tool_calls: int | Unset = 120
    max_evidence: int | Unset = 80
    max_report_tokens: int | Unset = 4000
    deadline_ms: int | Unset = 600000
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        max_rounds = self.max_rounds

        max_sub_questions = self.max_sub_questions

        max_parallel = self.max_parallel

        researcher_iterations = self.researcher_iterations

        researcher_tool_calls = self.researcher_tool_calls

        max_llm_calls = self.max_llm_calls

        max_tool_calls = self.max_tool_calls

        max_evidence = self.max_evidence

        max_report_tokens = self.max_report_tokens

        deadline_ms = self.deadline_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if max_rounds is not UNSET:
            field_dict["max_rounds"] = max_rounds
        if max_sub_questions is not UNSET:
            field_dict["max_sub_questions"] = max_sub_questions
        if max_parallel is not UNSET:
            field_dict["max_parallel"] = max_parallel
        if researcher_iterations is not UNSET:
            field_dict["researcher_iterations"] = researcher_iterations
        if researcher_tool_calls is not UNSET:
            field_dict["researcher_tool_calls"] = researcher_tool_calls
        if max_llm_calls is not UNSET:
            field_dict["max_llm_calls"] = max_llm_calls
        if max_tool_calls is not UNSET:
            field_dict["max_tool_calls"] = max_tool_calls
        if max_evidence is not UNSET:
            field_dict["max_evidence"] = max_evidence
        if max_report_tokens is not UNSET:
            field_dict["max_report_tokens"] = max_report_tokens
        if deadline_ms is not UNSET:
            field_dict["deadline_ms"] = deadline_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_rounds = d.pop("max_rounds", UNSET)

        max_sub_questions = d.pop("max_sub_questions", UNSET)

        max_parallel = d.pop("max_parallel", UNSET)

        researcher_iterations = d.pop("researcher_iterations", UNSET)

        researcher_tool_calls = d.pop("researcher_tool_calls", UNSET)

        max_llm_calls = d.pop("max_llm_calls", UNSET)

        max_tool_calls = d.pop("max_tool_calls", UNSET)

        max_evidence = d.pop("max_evidence", UNSET)

        max_report_tokens = d.pop("max_report_tokens", UNSET)

        deadline_ms = d.pop("deadline_ms", UNSET)

        research_budget = cls(
            max_rounds=max_rounds,
            max_sub_questions=max_sub_questions,
            max_parallel=max_parallel,
            researcher_iterations=researcher_iterations,
            researcher_tool_calls=researcher_tool_calls,
            max_llm_calls=max_llm_calls,
            max_tool_calls=max_tool_calls,
            max_evidence=max_evidence,
            max_report_tokens=max_report_tokens,
            deadline_ms=deadline_ms,
        )

        research_budget.additional_properties = d
        return research_budget

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
