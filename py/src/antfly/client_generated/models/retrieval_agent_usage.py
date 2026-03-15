from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

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
        input_tokens (Union[Unset, int]): Total input tokens across all LLM calls
        output_tokens (Union[Unset, int]): Total output tokens across all LLM calls
        total_tokens (Union[Unset, int]): Sum of input + output tokens
        cached_input_tokens (Union[Unset, int]): Input tokens served from cache
        llm_calls (Union[Unset, int]): Number of LLM invocations made
        resources_retrieved (Union[Unset, int]): Total resources found across all search queries
        prune_stats (Union[Unset, PruneStats]): Statistics from token-based document pruning
    """

    input_tokens: Union[Unset, int] = UNSET
    output_tokens: Union[Unset, int] = UNSET
    total_tokens: Union[Unset, int] = UNSET
    cached_input_tokens: Union[Unset, int] = UNSET
    llm_calls: Union[Unset, int] = UNSET
    resources_retrieved: Union[Unset, int] = UNSET
    prune_stats: Union[Unset, "PruneStats"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        input_tokens = self.input_tokens

        output_tokens = self.output_tokens

        total_tokens = self.total_tokens

        cached_input_tokens = self.cached_input_tokens

        llm_calls = self.llm_calls

        resources_retrieved = self.resources_retrieved

        prune_stats: Union[Unset, dict[str, Any]] = UNSET
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
        prune_stats: Union[Unset, PruneStats]
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
