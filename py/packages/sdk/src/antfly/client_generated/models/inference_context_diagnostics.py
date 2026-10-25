from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_context_diagnostics_kv_cache_mode import InferenceContextDiagnosticsKvCacheMode
from ..models.inference_context_diagnostics_kv_cache_version import InferenceContextDiagnosticsKvCacheVersion
from ..models.inference_context_diagnostics_policy import InferenceContextDiagnosticsPolicy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_context_tool_result_outcome import InferenceContextToolResultOutcome


T = TypeVar("T", bound="InferenceContextDiagnostics")


@_attrs_define
class InferenceContextDiagnostics:
    """
    Attributes:
        policy (InferenceContextDiagnosticsPolicy): Requested action: generation admission uses reject/window; explicit
            preparation uses inspect/window/compact. The flags below describe actual
            changes independently. For example, window with an existing checkpoint
            can report compacted=true without creating another checkpoint.
        applied (bool): Whether the returned generation context differs from the supplied transcript.
        original_input_tokens (int):
        effective_input_tokens (int): Token count after checkpoint application and windowing.
        target_input_tokens (int): Effective input-token ceiling after output reservation and client limits.
        reserved_output_tokens (int):
        dropped_messages (int):
        retained_messages (int):
        retained_from_message (int): Original zero-based boundary for the retained active suffix. Instruction
            messages before this boundary may also be returned verbatim.
        checkpoint_applied (bool):  Default: False.
        windowed (bool):
        compacted (bool):
        checkpoint_sha256 (str | Unset):
        checkpoint_created (bool | Unset): Whether this operation created a new structured checkpoint. Default: False.
        tool_results (list[InferenceContextToolResultOutcome] | Unset): Always emitted, including an empty array, by
            /generate/context/prepare
            and generation with context_management. The property remains optional
            for older servers and responses without context planning; omission must
            not be interpreted as an empty outcome list.
            Context-planning outcomes for identified tool-result messages, in original
            input order, followed by archived checkpoint evidence. Summarized means
            represented by attributed checkpoint evidence (possibly verbatim), not a
            retained raw tool message. Archived records omit message_index; reused
            IDs can therefore have distinct outcomes. Messages without tool_call_id
            are omitted. This reports input presence, not whether the model used it.
        kv_cache_mode (InferenceContextDiagnosticsKvCacheMode | Unset): Effective KV policy, when generation selected
            one.
        kv_cache_version (InferenceContextDiagnosticsKvCacheVersion | Unset):
    """

    policy: InferenceContextDiagnosticsPolicy
    applied: bool
    original_input_tokens: int
    effective_input_tokens: int
    target_input_tokens: int
    reserved_output_tokens: int
    dropped_messages: int
    retained_messages: int
    retained_from_message: int
    windowed: bool
    compacted: bool
    checkpoint_applied: bool = False
    checkpoint_sha256: str | Unset = UNSET
    checkpoint_created: bool | Unset = False
    tool_results: list[InferenceContextToolResultOutcome] | Unset = UNSET
    kv_cache_mode: InferenceContextDiagnosticsKvCacheMode | Unset = UNSET
    kv_cache_version: InferenceContextDiagnosticsKvCacheVersion | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        policy = self.policy.value

        applied = self.applied

        original_input_tokens = self.original_input_tokens

        effective_input_tokens = self.effective_input_tokens

        target_input_tokens = self.target_input_tokens

        reserved_output_tokens = self.reserved_output_tokens

        dropped_messages = self.dropped_messages

        retained_messages = self.retained_messages

        retained_from_message = self.retained_from_message

        checkpoint_applied = self.checkpoint_applied

        windowed = self.windowed

        compacted = self.compacted

        checkpoint_sha256 = self.checkpoint_sha256

        checkpoint_created = self.checkpoint_created

        tool_results: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tool_results, Unset):
            tool_results = []
            for tool_results_item_data in self.tool_results:
                tool_results_item = tool_results_item_data.to_dict()
                tool_results.append(tool_results_item)

        kv_cache_mode: str | Unset = UNSET
        if not isinstance(self.kv_cache_mode, Unset):
            kv_cache_mode = self.kv_cache_mode.value

        kv_cache_version: str | Unset = UNSET
        if not isinstance(self.kv_cache_version, Unset):
            kv_cache_version = self.kv_cache_version.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "policy": policy,
                "applied": applied,
                "original_input_tokens": original_input_tokens,
                "effective_input_tokens": effective_input_tokens,
                "target_input_tokens": target_input_tokens,
                "reserved_output_tokens": reserved_output_tokens,
                "dropped_messages": dropped_messages,
                "retained_messages": retained_messages,
                "retained_from_message": retained_from_message,
                "checkpoint_applied": checkpoint_applied,
                "windowed": windowed,
                "compacted": compacted,
            }
        )
        if checkpoint_sha256 is not UNSET:
            field_dict["checkpoint_sha256"] = checkpoint_sha256
        if checkpoint_created is not UNSET:
            field_dict["checkpoint_created"] = checkpoint_created
        if tool_results is not UNSET:
            field_dict["tool_results"] = tool_results
        if kv_cache_mode is not UNSET:
            field_dict["kv_cache_mode"] = kv_cache_mode
        if kv_cache_version is not UNSET:
            field_dict["kv_cache_version"] = kv_cache_version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_context_tool_result_outcome import InferenceContextToolResultOutcome

        d = dict(src_dict)
        policy = InferenceContextDiagnosticsPolicy(d.pop("policy"))

        applied = d.pop("applied")

        original_input_tokens = d.pop("original_input_tokens")

        effective_input_tokens = d.pop("effective_input_tokens")

        target_input_tokens = d.pop("target_input_tokens")

        reserved_output_tokens = d.pop("reserved_output_tokens")

        dropped_messages = d.pop("dropped_messages")

        retained_messages = d.pop("retained_messages")

        retained_from_message = d.pop("retained_from_message")

        checkpoint_applied = d.pop("checkpoint_applied")

        windowed = d.pop("windowed")

        compacted = d.pop("compacted")

        checkpoint_sha256 = d.pop("checkpoint_sha256", UNSET)

        checkpoint_created = d.pop("checkpoint_created", UNSET)

        _tool_results = d.pop("tool_results", UNSET)
        tool_results: list[InferenceContextToolResultOutcome] | Unset = UNSET
        if _tool_results is not UNSET:
            tool_results = []
            for tool_results_item_data in _tool_results:
                tool_results_item = InferenceContextToolResultOutcome.from_dict(tool_results_item_data)

                tool_results.append(tool_results_item)

        _kv_cache_mode = d.pop("kv_cache_mode", UNSET)
        kv_cache_mode: InferenceContextDiagnosticsKvCacheMode | Unset
        if isinstance(_kv_cache_mode, Unset):
            kv_cache_mode = UNSET
        else:
            kv_cache_mode = InferenceContextDiagnosticsKvCacheMode(_kv_cache_mode)

        _kv_cache_version = d.pop("kv_cache_version", UNSET)
        kv_cache_version: InferenceContextDiagnosticsKvCacheVersion | Unset
        if isinstance(_kv_cache_version, Unset):
            kv_cache_version = UNSET
        else:
            kv_cache_version = InferenceContextDiagnosticsKvCacheVersion(_kv_cache_version)

        inference_context_diagnostics = cls(
            policy=policy,
            applied=applied,
            original_input_tokens=original_input_tokens,
            effective_input_tokens=effective_input_tokens,
            target_input_tokens=target_input_tokens,
            reserved_output_tokens=reserved_output_tokens,
            dropped_messages=dropped_messages,
            retained_messages=retained_messages,
            retained_from_message=retained_from_message,
            checkpoint_applied=checkpoint_applied,
            windowed=windowed,
            compacted=compacted,
            checkpoint_sha256=checkpoint_sha256,
            checkpoint_created=checkpoint_created,
            tool_results=tool_results,
            kv_cache_mode=kv_cache_mode,
            kv_cache_version=kv_cache_version,
        )

        return inference_context_diagnostics
