from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.runtime_config_generation_batching_mode import RuntimeConfigGenerationBatchingMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="RuntimeConfigGenerationBatching")


@_attrs_define
class RuntimeConfigGenerationBatching:
    """Experimental execution batching; mode on explicitly enables qualified CUDA scheduling. Resource and request ceilings
    still apply.

        Attributes:
            mode (RuntimeConfigGenerationBatchingMode | Unset):  Default: RuntimeConfigGenerationBatchingMode.OFF.
            max_step_items (int | Unset):  Default: 2.
            max_step_query_tokens (int | Unset):  Default: 512.
            max_decode_wait_us (int | Unset):  Default: 1000.
            max_idle_prefill_chunk_size (int | Unset):  Default: 2048.
    """

    mode: RuntimeConfigGenerationBatchingMode | Unset = RuntimeConfigGenerationBatchingMode.OFF
    max_step_items: int | Unset = 2
    max_step_query_tokens: int | Unset = 512
    max_decode_wait_us: int | Unset = 1000
    max_idle_prefill_chunk_size: int | Unset = 2048

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        max_step_items = self.max_step_items

        max_step_query_tokens = self.max_step_query_tokens

        max_decode_wait_us = self.max_decode_wait_us

        max_idle_prefill_chunk_size = self.max_idle_prefill_chunk_size

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if max_step_items is not UNSET:
            field_dict["max_step_items"] = max_step_items
        if max_step_query_tokens is not UNSET:
            field_dict["max_step_query_tokens"] = max_step_query_tokens
        if max_decode_wait_us is not UNSET:
            field_dict["max_decode_wait_us"] = max_decode_wait_us
        if max_idle_prefill_chunk_size is not UNSET:
            field_dict["max_idle_prefill_chunk_size"] = max_idle_prefill_chunk_size

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: RuntimeConfigGenerationBatchingMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = RuntimeConfigGenerationBatchingMode(_mode)

        max_step_items = d.pop("max_step_items", UNSET)

        max_step_query_tokens = d.pop("max_step_query_tokens", UNSET)

        max_decode_wait_us = d.pop("max_decode_wait_us", UNSET)

        max_idle_prefill_chunk_size = d.pop("max_idle_prefill_chunk_size", UNSET)

        runtime_config_generation_batching = cls(
            mode=mode,
            max_step_items=max_step_items,
            max_step_query_tokens=max_step_query_tokens,
            max_decode_wait_us=max_decode_wait_us,
            max_idle_prefill_chunk_size=max_idle_prefill_chunk_size,
        )

        return runtime_config_generation_batching
