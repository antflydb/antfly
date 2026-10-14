from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_kind import InferenceModelKind
from ..models.inference_model_runtime_model_load_state import InferenceModelRuntimeModelLoadState

if TYPE_CHECKING:
    from ..models.inference_model_runtime import InferenceModelRuntime


T = TypeVar("T", bound="InferenceModelRuntimeModel")


@_attrs_define
class InferenceModelRuntimeModel:
    """
    Attributes:
        task (InferenceModelKind): Model registry kind.
        model (str):
        load_state (InferenceModelRuntimeModelLoadState):
        runtimes (list[InferenceModelRuntime]):
    """

    task: InferenceModelKind
    model: str
    load_state: InferenceModelRuntimeModelLoadState
    runtimes: list[InferenceModelRuntime]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        task = self.task.value

        model = self.model

        load_state = self.load_state.value

        runtimes = []
        for runtimes_item_data in self.runtimes:
            runtimes_item = runtimes_item_data.to_dict()
            runtimes.append(runtimes_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "task": task,
                "model": model,
                "load_state": load_state,
                "runtimes": runtimes,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_model_runtime import InferenceModelRuntime

        d = dict(src_dict)
        task = InferenceModelKind(d.pop("task"))

        model = d.pop("model")

        load_state = InferenceModelRuntimeModelLoadState(d.pop("load_state"))

        runtimes = []
        _runtimes = d.pop("runtimes")
        for runtimes_item_data in _runtimes:
            runtimes_item = InferenceModelRuntime.from_dict(runtimes_item_data)

            runtimes.append(runtimes_item)

        inference_model_runtime_model = cls(
            task=task,
            model=model,
            load_state=load_state,
            runtimes=runtimes,
        )

        inference_model_runtime_model.additional_properties = d
        return inference_model_runtime_model

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
