from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_kind import InferenceModelKind

T = TypeVar("T", bound="InferenceModelInstanceLoadRequest")


@_attrs_define
class InferenceModelInstanceLoadRequest:
    """
    Attributes:
        model (str):
        task (InferenceModelKind): Model registry kind.
    """

    model: str
    task: InferenceModelKind
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        task = self.task.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "task": task,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        task = InferenceModelKind(d.pop("task"))

        inference_model_instance_load_request = cls(
            model=model,
            task=task,
        )

        inference_model_instance_load_request.additional_properties = d
        return inference_model_instance_load_request

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
