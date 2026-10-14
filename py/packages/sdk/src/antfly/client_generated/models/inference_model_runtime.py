from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_runtime_kind import InferenceModelRuntimeKind
from ..models.inference_model_runtime_residency_mode import InferenceModelRuntimeResidencyMode
from ..models.inference_model_runtime_state import InferenceModelRuntimeState
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceModelRuntime")


@_attrs_define
class InferenceModelRuntime:
    """
    Attributes:
        runtime_id (str): Opaque process-scoped incarnation identifier used for exact unload.
        state (InferenceModelRuntimeState):
        runtime_kind (InferenceModelRuntimeKind):
        active_requests (int):
        startup_managed (bool): Whether startup preload configuration prevents explicit unload.
        residency_mode (InferenceModelRuntimeResidencyMode): Weight storage policy reported by the selected runtime.
        last_used_ms (int): Elapsed milliseconds since the runtime was last released after use.
        backend (str | Unset): Selected execution backend when known.
        device (str | Unset): Selected device class when known.
    """

    runtime_id: str
    state: InferenceModelRuntimeState
    runtime_kind: InferenceModelRuntimeKind
    active_requests: int
    startup_managed: bool
    residency_mode: InferenceModelRuntimeResidencyMode
    last_used_ms: int
    backend: str | Unset = UNSET
    device: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        runtime_id = self.runtime_id

        state = self.state.value

        runtime_kind = self.runtime_kind.value

        active_requests = self.active_requests

        startup_managed = self.startup_managed

        residency_mode = self.residency_mode.value

        last_used_ms = self.last_used_ms

        backend = self.backend

        device = self.device

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "runtime_id": runtime_id,
                "state": state,
                "runtime_kind": runtime_kind,
                "active_requests": active_requests,
                "startup_managed": startup_managed,
                "residency_mode": residency_mode,
                "last_used_ms": last_used_ms,
            }
        )
        if backend is not UNSET:
            field_dict["backend"] = backend
        if device is not UNSET:
            field_dict["device"] = device

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        runtime_id = d.pop("runtime_id")

        state = InferenceModelRuntimeState(d.pop("state"))

        runtime_kind = InferenceModelRuntimeKind(d.pop("runtime_kind"))

        active_requests = d.pop("active_requests")

        startup_managed = d.pop("startup_managed")

        residency_mode = InferenceModelRuntimeResidencyMode(d.pop("residency_mode"))

        last_used_ms = d.pop("last_used_ms")

        backend = d.pop("backend", UNSET)

        device = d.pop("device", UNSET)

        inference_model_runtime = cls(
            runtime_id=runtime_id,
            state=state,
            runtime_kind=runtime_kind,
            active_requests=active_requests,
            startup_managed=startup_managed,
            residency_mode=residency_mode,
            last_used_ms=last_used_ms,
            backend=backend,
            device=device,
        )

        inference_model_runtime.additional_properties = d
        return inference_model_runtime

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
