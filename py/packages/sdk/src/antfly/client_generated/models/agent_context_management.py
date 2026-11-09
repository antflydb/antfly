from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_context_management_mode import AgentContextManagementMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="AgentContextManagement")


@_attrs_define
class AgentContextManagement:
    """Opt-in conversation/evidence preparation before agent model calls. The
    summarizer uses the same GeneratorConfig, secrets and provider dispatch
    as generation; omitted summarizer inherits the effective generator
    (the first link when a chain is configured). Summary calls have no
    fallback or retries and consume the agent's model-call budget.
    A remote summarizer receives the selected conversation and tool evidence.
    Input accounting conservatively charges serialized UTF-8 bytes plus
    framing and tool schemas. Configure windows supported by both models.
    Failure returns a typed error without replacing authoritative history.
    Derived memory is request-local; continuations supply authoritative
    history/research_state and are prepared again. This does not alter KV caches.

        Attributes:
            context_window_tokens (int): Destination model capacity including reserved generation output.
            mode (AgentContextManagementMode | Unset): Inspect only reports; reject fails oversized prompts; window drops
                eligible exchanges; compact summarizes them. Default: AgentContextManagementMode.COMPACT.
            target_input_tokens (int | Unset): Compaction threshold and final input ceiling; defaults to
                context_window_tokens minus reserved_output_tokens.
            reserved_output_tokens (int | Unset): Must cover every destination generator's configured output limit. Default:
                4000.
            keep_recent_turns (int | Unset): Prior completed user turns retained verbatim. Current question, most recent
                exchange and open tool transactions are always retained. Default: 2.
            summarizer (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            summarizer_context_window_tokens (int | Unset): Independent summary capacity, including instructions, schema,
                prior memory, one complete exchange and summary output. Default: 8192.
            summary_output_tokens (int | Unset):  Default: 1024.
            max_summary_batches (int | Unset): Maximum summary calls per preparation. No automatic retries or provider
                fallback. Default: 8.
    """

    context_window_tokens: int
    mode: AgentContextManagementMode | Unset = AgentContextManagementMode.COMPACT
    target_input_tokens: int | Unset = UNSET
    reserved_output_tokens: int | Unset = 4000
    keep_recent_turns: int | Unset = 2
    summarizer: GeneratorConfig | Unset = UNSET
    summarizer_context_window_tokens: int | Unset = 8192
    summary_output_tokens: int | Unset = 1024
    max_summary_batches: int | Unset = 8
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        context_window_tokens = self.context_window_tokens

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        target_input_tokens = self.target_input_tokens

        reserved_output_tokens = self.reserved_output_tokens

        keep_recent_turns = self.keep_recent_turns

        summarizer: dict[str, Any] | Unset = UNSET
        if not isinstance(self.summarizer, Unset):
            summarizer = self.summarizer.to_dict()

        summarizer_context_window_tokens = self.summarizer_context_window_tokens

        summary_output_tokens = self.summary_output_tokens

        max_summary_batches = self.max_summary_batches

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "context_window_tokens": context_window_tokens,
            }
        )
        if mode is not UNSET:
            field_dict["mode"] = mode
        if target_input_tokens is not UNSET:
            field_dict["target_input_tokens"] = target_input_tokens
        if reserved_output_tokens is not UNSET:
            field_dict["reserved_output_tokens"] = reserved_output_tokens
        if keep_recent_turns is not UNSET:
            field_dict["keep_recent_turns"] = keep_recent_turns
        if summarizer is not UNSET:
            field_dict["summarizer"] = summarizer
        if summarizer_context_window_tokens is not UNSET:
            field_dict["summarizer_context_window_tokens"] = summarizer_context_window_tokens
        if summary_output_tokens is not UNSET:
            field_dict["summary_output_tokens"] = summary_output_tokens
        if max_summary_batches is not UNSET:
            field_dict["max_summary_batches"] = max_summary_batches

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        context_window_tokens = d.pop("context_window_tokens")

        _mode = d.pop("mode", UNSET)
        mode: AgentContextManagementMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = AgentContextManagementMode(_mode)

        target_input_tokens = d.pop("target_input_tokens", UNSET)

        reserved_output_tokens = d.pop("reserved_output_tokens", UNSET)

        keep_recent_turns = d.pop("keep_recent_turns", UNSET)

        _summarizer = d.pop("summarizer", UNSET)
        summarizer: GeneratorConfig | Unset
        if isinstance(_summarizer, Unset):
            summarizer = UNSET
        else:
            summarizer = GeneratorConfig.from_dict(_summarizer)

        summarizer_context_window_tokens = d.pop("summarizer_context_window_tokens", UNSET)

        summary_output_tokens = d.pop("summary_output_tokens", UNSET)

        max_summary_batches = d.pop("max_summary_batches", UNSET)

        agent_context_management = cls(
            context_window_tokens=context_window_tokens,
            mode=mode,
            target_input_tokens=target_input_tokens,
            reserved_output_tokens=reserved_output_tokens,
            keep_recent_turns=keep_recent_turns,
            summarizer=summarizer,
            summarizer_context_window_tokens=summarizer_context_window_tokens,
            summary_output_tokens=summary_output_tokens,
            max_summary_batches=max_summary_batches,
        )

        agent_context_management.additional_properties = d
        return agent_context_management

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
