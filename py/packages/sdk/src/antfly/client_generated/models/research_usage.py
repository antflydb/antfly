from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchUsage")


@_attrs_define
class ResearchUsage:
    """
    Attributes:
        llm_calls (int | Unset): Model calls across every role.
        tool_calls (int | Unset): Tool calls across every researcher.
        researcher_runs (int | Unset): Researcher executions.
        rounds (int | Unset): Research rounds completed.
        evidence_count (int | Unset): Evidence items in the registry.
        elapsed_ms (int | Unset): Wall-clock time consumed so far.
    """

    llm_calls: int | Unset = UNSET
    tool_calls: int | Unset = UNSET
    researcher_runs: int | Unset = UNSET
    rounds: int | Unset = UNSET
    evidence_count: int | Unset = UNSET
    elapsed_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        llm_calls = self.llm_calls

        tool_calls = self.tool_calls

        researcher_runs = self.researcher_runs

        rounds = self.rounds

        evidence_count = self.evidence_count

        elapsed_ms = self.elapsed_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if llm_calls is not UNSET:
            field_dict["llm_calls"] = llm_calls
        if tool_calls is not UNSET:
            field_dict["tool_calls"] = tool_calls
        if researcher_runs is not UNSET:
            field_dict["researcher_runs"] = researcher_runs
        if rounds is not UNSET:
            field_dict["rounds"] = rounds
        if evidence_count is not UNSET:
            field_dict["evidence_count"] = evidence_count
        if elapsed_ms is not UNSET:
            field_dict["elapsed_ms"] = elapsed_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        llm_calls = d.pop("llm_calls", UNSET)

        tool_calls = d.pop("tool_calls", UNSET)

        researcher_runs = d.pop("researcher_runs", UNSET)

        rounds = d.pop("rounds", UNSET)

        evidence_count = d.pop("evidence_count", UNSET)

        elapsed_ms = d.pop("elapsed_ms", UNSET)

        research_usage = cls(
            llm_calls=llm_calls,
            tool_calls=tool_calls,
            researcher_runs=researcher_runs,
            rounds=rounds,
            evidence_count=evidence_count,
            elapsed_ms=elapsed_ms,
        )

        research_usage.additional_properties = d
        return research_usage

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
