from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_instance_residency_mode import InferenceModelInstanceResidencyMode
from ..models.inference_model_instance_state import InferenceModelInstanceState
from ..models.inference_model_instance_unload_blocked_reason import InferenceModelInstanceUnloadBlockedReason
from ..models.inference_model_kind import InferenceModelKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceModelInstance")


@_attrs_define
class InferenceModelInstance:
    """One loading or loaded incarnation of a model in this inference server. Backend session and composite-cache
    implementation details are private.

        Attributes:
            id (str): Opaque incarnation identifier; changes after eviction and reload.
            model (str):
            tasks (list[InferenceModelKind]):
            state (InferenceModelInstanceState):
            active_handles (int): Internal owners or loading waiters preventing eviction. This is not a count of HTTP
                requests.
            startup_managed (bool):
            unloadable (bool): Whether an unload would be permitted at snapshot time; concurrent use can change this.
            residency_mode (InferenceModelInstanceResidencyMode): Weight residency policy; loaded does not imply full device
                residency.
            idle_ms (int): Elapsed milliseconds since last use, not a wall-clock timestamp.
            unload_blocked_reason (InferenceModelInstanceUnloadBlockedReason | Unset):
            backend (str | Unset): Selected execution backend, when known.
    """

    id: str
    model: str
    tasks: list[InferenceModelKind]
    state: InferenceModelInstanceState
    active_handles: int
    startup_managed: bool
    unloadable: bool
    residency_mode: InferenceModelInstanceResidencyMode
    idle_ms: int
    unload_blocked_reason: InferenceModelInstanceUnloadBlockedReason | Unset = UNSET
    backend: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        model = self.model

        tasks = []
        for tasks_item_data in self.tasks:
            tasks_item = tasks_item_data.value
            tasks.append(tasks_item)

        state = self.state.value

        active_handles = self.active_handles

        startup_managed = self.startup_managed

        unloadable = self.unloadable

        residency_mode = self.residency_mode.value

        idle_ms = self.idle_ms

        unload_blocked_reason: str | Unset = UNSET
        if not isinstance(self.unload_blocked_reason, Unset):
            unload_blocked_reason = self.unload_blocked_reason.value

        backend = self.backend

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "model": model,
                "tasks": tasks,
                "state": state,
                "active_handles": active_handles,
                "startup_managed": startup_managed,
                "unloadable": unloadable,
                "residency_mode": residency_mode,
                "idle_ms": idle_ms,
            }
        )
        if unload_blocked_reason is not UNSET:
            field_dict["unload_blocked_reason"] = unload_blocked_reason
        if backend is not UNSET:
            field_dict["backend"] = backend

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        model = d.pop("model")

        tasks = []
        _tasks = d.pop("tasks")
        for tasks_item_data in _tasks:
            tasks_item = InferenceModelKind(tasks_item_data)

            tasks.append(tasks_item)

        state = InferenceModelInstanceState(d.pop("state"))

        active_handles = d.pop("active_handles")

        startup_managed = d.pop("startup_managed")

        unloadable = d.pop("unloadable")

        residency_mode = InferenceModelInstanceResidencyMode(d.pop("residency_mode"))

        idle_ms = d.pop("idle_ms")

        _unload_blocked_reason = d.pop("unload_blocked_reason", UNSET)
        unload_blocked_reason: InferenceModelInstanceUnloadBlockedReason | Unset
        if isinstance(_unload_blocked_reason, Unset):
            unload_blocked_reason = UNSET
        else:
            unload_blocked_reason = InferenceModelInstanceUnloadBlockedReason(_unload_blocked_reason)

        backend = d.pop("backend", UNSET)

        inference_model_instance = cls(
            id=id,
            model=model,
            tasks=tasks,
            state=state,
            active_handles=active_handles,
            startup_managed=startup_managed,
            unloadable=unloadable,
            residency_mode=residency_mode,
            idle_ms=idle_ms,
            unload_blocked_reason=unload_blocked_reason,
            backend=backend,
        )

        inference_model_instance.additional_properties = d
        return inference_model_instance

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
