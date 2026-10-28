from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceMemoryConfig")


@_attrs_define
class InferenceMemoryConfig:
    """Inference memory ceilings in bytes. Omitted or zero values select automatic policy; physical capacity and the
    process envelope remain authoritative.

        Attributes:
            max_host_bytes (int | Unset): Host memory ceiling.
            max_device_bytes (int | Unset): Accelerator memory ceiling.
            max_combined_bytes (int | Unset): Combined host and accelerator ceiling; integrated devices also enforce this
                across CPU and GPU leases.
            max_kv_bytes (int | Unset): KV cache memory ceiling.
            max_scratch_bytes (int | Unset): Scratch memory ceiling.
    """

    max_host_bytes: int | Unset = UNSET
    max_device_bytes: int | Unset = UNSET
    max_combined_bytes: int | Unset = UNSET
    max_kv_bytes: int | Unset = UNSET
    max_scratch_bytes: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        max_host_bytes = self.max_host_bytes

        max_device_bytes = self.max_device_bytes

        max_combined_bytes = self.max_combined_bytes

        max_kv_bytes = self.max_kv_bytes

        max_scratch_bytes = self.max_scratch_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_host_bytes is not UNSET:
            field_dict["max_host_bytes"] = max_host_bytes
        if max_device_bytes is not UNSET:
            field_dict["max_device_bytes"] = max_device_bytes
        if max_combined_bytes is not UNSET:
            field_dict["max_combined_bytes"] = max_combined_bytes
        if max_kv_bytes is not UNSET:
            field_dict["max_kv_bytes"] = max_kv_bytes
        if max_scratch_bytes is not UNSET:
            field_dict["max_scratch_bytes"] = max_scratch_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_host_bytes = d.pop("max_host_bytes", UNSET)

        max_device_bytes = d.pop("max_device_bytes", UNSET)

        max_combined_bytes = d.pop("max_combined_bytes", UNSET)

        max_kv_bytes = d.pop("max_kv_bytes", UNSET)

        max_scratch_bytes = d.pop("max_scratch_bytes", UNSET)

        inference_memory_config = cls(
            max_host_bytes=max_host_bytes,
            max_device_bytes=max_device_bytes,
            max_combined_bytes=max_combined_bytes,
            max_kv_bytes=max_kv_bytes,
            max_scratch_bytes=max_scratch_bytes,
        )

        return inference_memory_config
