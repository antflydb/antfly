from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.prune_stats import PruneStats


T = TypeVar("T", bound="RetrievalAgentUsage")


@_attrs_define
class RetrievalAgentUsage:
    """Token usage and resource statistics from the retrieval agent execution

    Attributes:
        input_tokens (int | Unset): Total input tokens across all LLM calls
        output_tokens (int | Unset): Total output tokens across all LLM calls
        total_tokens (int | Unset): Sum of input + output tokens
        cached_input_tokens (int | Unset): Input tokens served from cache
        llm_calls (int | Unset): Number of LLM invocations made
        resources_retrieved (int | Unset): Total resources found across all search queries
        prune_stats (PruneStats | Unset): Statistics from token-based document pruning
    """

    input_tokens: int | Unset = UNSET
    output_tokens: int | Unset = UNSET
    total_tokens: int | Unset = UNSET
    cached_input_tokens: int | Unset = UNSET
    llm_calls: int | Unset = UNSET
    resources_retrieved: int | Unset = UNSET
    prune_stats: PruneStats | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        input_tokens = self.input_tokens

        output_tokens = self.output_tokens

        total_tokens = self.total_tokens

        cached_input_tokens = self.cached_input_tokens

        llm_calls = self.llm_calls

        resources_retrieved = self.resources_retrieved

        prune_stats: dict[str, Any] | Unset = UNSET
        if not isinstance(self.prune_stats, Unset):
            prune_stats = self.prune_stats.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if input_tokens is not UNSET:
            field_dict["input_tokens"] = input_tokens
        if output_tokens is not UNSET:
            field_dict["output_tokens"] = output_tokens
        if total_tokens is not UNSET:
            field_dict["total_tokens"] = total_tokens
        if cached_input_tokens is not UNSET:
            field_dict["cached_input_tokens"] = cached_input_tokens
        if llm_calls is not UNSET:
            field_dict["llm_calls"] = llm_calls
        if resources_retrieved is not UNSET:
            field_dict["resources_retrieved"] = resources_retrieved
        if prune_stats is not UNSET:
            field_dict["prune_stats"] = prune_stats

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.prune_stats import PruneStats

        d = dict(src_dict)
        input_tokens = d.pop("input_tokens", UNSET)

        output_tokens = d.pop("output_tokens", UNSET)

        total_tokens = d.pop("total_tokens", UNSET)

        cached_input_tokens = d.pop("cached_input_tokens", UNSET)

        llm_calls = d.pop("llm_calls", UNSET)

        resources_retrieved = d.pop("resources_retrieved", UNSET)

        _prune_stats = d.pop("prune_stats", UNSET)
        prune_stats: PruneStats | Unset
        if isinstance(_prune_stats, Unset):
            prune_stats = UNSET
        else:
            prune_stats = PruneStats.from_dict(_prune_stats)

        retrieval_agent_usage = cls(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            cached_input_tokens=cached_input_tokens,
            llm_calls=llm_calls,
            resources_retrieved=resources_retrieved,
            prune_stats=prune_stats,
        )

        retrieval_agent_usage.additional_properties = d
        return retrieval_agent_usage

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
