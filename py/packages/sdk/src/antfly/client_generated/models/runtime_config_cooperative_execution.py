from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.runtime_config_cooperative_execution_mode import RuntimeConfigCooperativeExecutionMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="RuntimeConfigCooperativeExecution")


@_attrs_define
class RuntimeConfigCooperativeExecution:
    """Opt-in interleaving of independently owned requests on backends declaring safe step sharing. Independent of row
    batching; graph, prefix-cache, speculative and multimodal routes remain exclusive. Currently qualified for at most
    two active E4B CUDA requests with one row per step.

        Attributes:
            mode (RuntimeConfigCooperativeExecutionMode | Unset):  Default: RuntimeConfigCooperativeExecutionMode.OFF.
            max_active_requests (int | Unset):  Default: 2.
            short_context_burst (int | Unset):  Default: 8.
            decode_handoff_wait_us (int | Unset):  Default: 5000.
    """

    mode: RuntimeConfigCooperativeExecutionMode | Unset = RuntimeConfigCooperativeExecutionMode.OFF
    max_active_requests: int | Unset = 2
    short_context_burst: int | Unset = 8
    decode_handoff_wait_us: int | Unset = 5000

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        max_active_requests = self.max_active_requests

        short_context_burst = self.short_context_burst

        decode_handoff_wait_us = self.decode_handoff_wait_us

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if max_active_requests is not UNSET:
            field_dict["max_active_requests"] = max_active_requests
        if short_context_burst is not UNSET:
            field_dict["short_context_burst"] = short_context_burst
        if decode_handoff_wait_us is not UNSET:
            field_dict["decode_handoff_wait_us"] = decode_handoff_wait_us

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: RuntimeConfigCooperativeExecutionMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = RuntimeConfigCooperativeExecutionMode(_mode)

        max_active_requests = d.pop("max_active_requests", UNSET)

        short_context_burst = d.pop("short_context_burst", UNSET)

        decode_handoff_wait_us = d.pop("decode_handoff_wait_us", UNSET)

        runtime_config_cooperative_execution = cls(
            mode=mode,
            max_active_requests=max_active_requests,
            short_context_burst=short_context_burst,
            decode_handoff_wait_us=decode_handoff_wait_us,
        )

        return runtime_config_cooperative_execution
