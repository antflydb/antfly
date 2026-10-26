from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_generate_context_policy_mode import InferenceGenerateContextPolicyMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_generate_compaction_target import InferenceGenerateCompactionTarget


T = TypeVar("T", bound="InferenceGenerateContextPolicy")


@_attrs_define
class InferenceGenerateContextPolicy:
    """Separate positional context limits from retained KV storage. Exact is the default.
    Approximate modes require a qualified Metal f16 implementation and reject unsupported
    combinations; setting a policy never extends the artifact's positional limit.

        Attributes:
            compaction_targets (list[InferenceGenerateCompactionTarget] | Unset): Explicit user/tool text payloads eligible
                for attention matching. Other roles,
                metadata, special tokens and boundary-crossing tokens remain exact. Unsupported
                renderer/tokenizer provenance is rejected. Approximate serving is not yet qualified.
            mode (InferenceGenerateContextPolicyMode | Unset):  Default: InferenceGenerateContextPolicyMode.EXACT.
            max_context_tokens (int | Unset): Optional cap on prompt plus output positions, bounded by the model limit.
            max_kv_tokens (int | Unset): Retained rows per global KV head. Required for approximate modes.
            keep_prefix_tokens (int | Unset): Exact prefix protection in approximate modes; does not identify message
                boundaries. Default: 256.
            keep_recent_tokens (int | Unset): Minimum recent exact tail in approximate modes. Default: 2048.
    """

    compaction_targets: list[InferenceGenerateCompactionTarget] | Unset = UNSET
    mode: InferenceGenerateContextPolicyMode | Unset = InferenceGenerateContextPolicyMode.EXACT
    max_context_tokens: int | Unset = UNSET
    max_kv_tokens: int | Unset = UNSET
    keep_prefix_tokens: int | Unset = 256
    keep_recent_tokens: int | Unset = 2048

    def to_dict(self) -> dict[str, Any]:
        compaction_targets: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.compaction_targets, Unset):
            compaction_targets = []
            for compaction_targets_item_data in self.compaction_targets:
                compaction_targets_item = compaction_targets_item_data.to_dict()
                compaction_targets.append(compaction_targets_item)

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        max_context_tokens = self.max_context_tokens

        max_kv_tokens = self.max_kv_tokens

        keep_prefix_tokens = self.keep_prefix_tokens

        keep_recent_tokens = self.keep_recent_tokens

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if compaction_targets is not UNSET:
            field_dict["compaction_targets"] = compaction_targets
        if mode is not UNSET:
            field_dict["mode"] = mode
        if max_context_tokens is not UNSET:
            field_dict["max_context_tokens"] = max_context_tokens
        if max_kv_tokens is not UNSET:
            field_dict["max_kv_tokens"] = max_kv_tokens
        if keep_prefix_tokens is not UNSET:
            field_dict["keep_prefix_tokens"] = keep_prefix_tokens
        if keep_recent_tokens is not UNSET:
            field_dict["keep_recent_tokens"] = keep_recent_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_compaction_target import InferenceGenerateCompactionTarget

        d = dict(src_dict)
        _compaction_targets = d.pop("compaction_targets", UNSET)
        compaction_targets: list[InferenceGenerateCompactionTarget] | Unset = UNSET
        if _compaction_targets is not UNSET:
            compaction_targets = []
            for compaction_targets_item_data in _compaction_targets:
                compaction_targets_item = InferenceGenerateCompactionTarget.from_dict(compaction_targets_item_data)

                compaction_targets.append(compaction_targets_item)

        _mode = d.pop("mode", UNSET)
        mode: InferenceGenerateContextPolicyMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = InferenceGenerateContextPolicyMode(_mode)

        max_context_tokens = d.pop("max_context_tokens", UNSET)

        max_kv_tokens = d.pop("max_kv_tokens", UNSET)

        keep_prefix_tokens = d.pop("keep_prefix_tokens", UNSET)

        keep_recent_tokens = d.pop("keep_recent_tokens", UNSET)

        inference_generate_context_policy = cls(
            compaction_targets=compaction_targets,
            mode=mode,
            max_context_tokens=max_context_tokens,
            max_kv_tokens=max_kv_tokens,
            keep_prefix_tokens=keep_prefix_tokens,
            keep_recent_tokens=keep_recent_tokens,
        )

        return inference_generate_context_policy
