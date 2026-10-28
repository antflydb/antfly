from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.kernel_jit_mode import KernelJitMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="KernelJitConfig")


@_attrs_define
class KernelJitConfig:
    """Runtime Metal and CUDA kernel JIT configuration. Live compilation and
    GPU qualification run only for models in the configured startup
    `preload` list, before serving begins. A cold dynamic model load never
    benchmarks against an in-use GPU: `on` and `shadow` retain bundled
    kernels, while `required` rejects the load. When runtime JIT is enabled,
    startup also materializes every vision, audio, and projection session
    declared by each preloaded model without running media inference.
    Preload every model that must use runtime-JIT kernels; `required`
    rejects an empty preload list.

        Attributes:
            mode (KernelJitMode | Unset): Runtime kernel JIT operating mode. `off` disables compilation. `shadow`
                compiles and validates without dispatching JIT kernels. `on` may activate
                qualified kernels while preserving the production fallback. `required`
                fails model loading when there are no eligible routes or any scoped
                route cannot qualify, rejects sessions that are not direct Metal or
                CUDA sessions, and requires at least one configured startup preload.
                Backend preference remains separately configured; this mode neither
                enables nor selects a backend. Runtime JIT is currently supported on
                Linux and macOS; other platforms reject non-`off` modes rather than
                silently degrading.
            cache_dir (str | Unset): Persistent artifact cache directory on Linux and macOS. Omit to use
                ~/.antfly/inference/jit when HOME is available.
            max_cache_bytes_mb (int | Unset): Persistent cache size limit in MiB. Set to 0 to disable disk persistence.
                Default: 1024.
            preload_budget_ms (int | Unset): Per direct model session pre-publication start budget for
                best-effort `on` and `shadow` JIT work. A compiler or qualification
                operation already started may overrun the budget. Multimodal model
                preloads may create several direct sessions, each with this budget.
                `required` completes or fails all required routes and may exceed
                this value.
                 Default: 300000.
    """

    mode: KernelJitMode | Unset = UNSET
    cache_dir: str | Unset = UNSET
    max_cache_bytes_mb: int | Unset = 1024
    preload_budget_ms: int | Unset = 300000
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        cache_dir = self.cache_dir

        max_cache_bytes_mb = self.max_cache_bytes_mb

        preload_budget_ms = self.preload_budget_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if cache_dir is not UNSET:
            field_dict["cache_dir"] = cache_dir
        if max_cache_bytes_mb is not UNSET:
            field_dict["max_cache_bytes_mb"] = max_cache_bytes_mb
        if preload_budget_ms is not UNSET:
            field_dict["preload_budget_ms"] = preload_budget_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: KernelJitMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = KernelJitMode(_mode)

        cache_dir = d.pop("cache_dir", UNSET)

        max_cache_bytes_mb = d.pop("max_cache_bytes_mb", UNSET)

        preload_budget_ms = d.pop("preload_budget_ms", UNSET)

        kernel_jit_config = cls(
            mode=mode,
            cache_dir=cache_dir,
            max_cache_bytes_mb=max_cache_bytes_mb,
            preload_budget_ms=preload_budget_ms,
        )

        kernel_jit_config.additional_properties = d
        return kernel_jit_config

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
