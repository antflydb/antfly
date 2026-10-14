from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_runtime_response_object import InferenceModelRuntimeResponseObject

if TYPE_CHECKING:
    from ..models.inference_model_runtime_model import InferenceModelRuntimeModel


T = TypeVar("T", bound="InferenceModelRuntimeResponse")


@_attrs_define
class InferenceModelRuntimeResponse:
    """
    Attributes:
        object_ (InferenceModelRuntimeResponseObject):
        models (list[InferenceModelRuntimeModel]):
    """

    object_: InferenceModelRuntimeResponseObject
    models: list[InferenceModelRuntimeModel]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        models = []
        for models_item_data in self.models:
            models_item = models_item_data.to_dict()
            models.append(models_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "models": models,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_model_runtime_model import InferenceModelRuntimeModel

        d = dict(src_dict)
        object_ = InferenceModelRuntimeResponseObject(d.pop("object"))

        models = []
        _models = d.pop("models")
        for models_item_data in _models:
            models_item = InferenceModelRuntimeModel.from_dict(models_item_data)

            models.append(models_item)

        inference_model_runtime_response = cls(
            object_=object_,
            models=models,
        )

        inference_model_runtime_response.additional_properties = d
        return inference_model_runtime_response

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
