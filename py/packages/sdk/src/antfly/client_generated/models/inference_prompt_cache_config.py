from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_prompt_cache_config_mode import InferencePromptCacheConfigMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferencePromptCacheConfig")


@_attrs_define
class InferencePromptCacheConfig:
    """Native generator prompt KV cache configuration.

    Attributes:
        enabled (bool | Unset): Enable inference-native prompt KV cache reuse for generator requests. Default: False.
        mode (InferencePromptCacheConfigMode | Unset): Prompt KV cache implementation. `block_hash` (default) uses hash-
            addressed
            full KV blocks under prompt_cache_key with O(1) block lookup. `radix` is an
            opt-in page-aligned compressed radix tree with shared-prefix ownership and
            leaf-only LRU eviction; it is currently qualified for native and Metal
            backends. Eligible Metal requests use eager paged attention; explicit compiled
            generation is incompatible with prompt caching. `simple` keeps the linear-scan
            retained-prefix cache and is only suitable for small caches or debugging.
             Default: InferencePromptCacheConfigMode.BLOCK_HASH.
        max_bytes_mb (int | Unset): Node-wide target for live prompt-cache entries. The runtime divides it
            across participating model caches and evicts using estimated metadata
            and logical host/device KV bytes. Backend allocators may retain reusable
            capacity, so this is not a hard cap on process or accelerator memory.
             Default: 512.
        min_tokens (int | Unset): Minimum prompt length eligible for prompt KV caching. Default: 64.
        ttl_ms (int | Unset): Idle time-to-live for prompt KV cache entries. Refreshed on every cache
            hit, so only entries left unused for this duration expire.
             Default: 300000.
    """

    enabled: bool | Unset = False
    mode: InferencePromptCacheConfigMode | Unset = InferencePromptCacheConfigMode.BLOCK_HASH
    max_bytes_mb: int | Unset = 512
    min_tokens: int | Unset = 64
    ttl_ms: int | Unset = 300000
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        max_bytes_mb = self.max_bytes_mb

        min_tokens = self.min_tokens

        ttl_ms = self.ttl_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if mode is not UNSET:
            field_dict["mode"] = mode
        if max_bytes_mb is not UNSET:
            field_dict["max_bytes_mb"] = max_bytes_mb
        if min_tokens is not UNSET:
            field_dict["min_tokens"] = min_tokens
        if ttl_ms is not UNSET:
            field_dict["ttl_ms"] = ttl_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        _mode = d.pop("mode", UNSET)
        mode: InferencePromptCacheConfigMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = InferencePromptCacheConfigMode(_mode)

        max_bytes_mb = d.pop("max_bytes_mb", UNSET)

        min_tokens = d.pop("min_tokens", UNSET)

        ttl_ms = d.pop("ttl_ms", UNSET)

        inference_prompt_cache_config = cls(
            enabled=enabled,
            mode=mode,
            max_bytes_mb=max_bytes_mb,
            min_tokens=min_tokens,
            ttl_ms=ttl_ms,
        )

        inference_prompt_cache_config.additional_properties = d
        return inference_prompt_cache_config

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
