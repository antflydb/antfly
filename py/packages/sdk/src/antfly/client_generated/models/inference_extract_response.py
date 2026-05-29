from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_extract_response_object import InferenceExtractResponseObject

if TYPE_CHECKING:
    from ..models.inference_extract_object import InferenceExtractObject
    from ..models.inference_generate_usage import InferenceGenerateUsage


T = TypeVar("T", bound="InferenceExtractResponse")


@_attrs_define
class InferenceExtractResponse:
    """
    Attributes:
        object_ (InferenceExtractResponseObject): Object type, always "list"
        data (list[InferenceExtractObject]): Extraction result objects, one per input.
        model (str): Name of model used for extraction
        usage (InferenceGenerateUsage):
    """

    object_: InferenceExtractResponseObject
    data: list[InferenceExtractObject]
    model: str
    usage: InferenceGenerateUsage
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        model = self.model

        usage = self.usage.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "data": data,
                "model": model,
                "usage": usage,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_extract_object import InferenceExtractObject
        from ..models.inference_generate_usage import InferenceGenerateUsage

        d = dict(src_dict)
        object_ = InferenceExtractResponseObject(d.pop("object"))

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = InferenceExtractObject.from_dict(data_item_data)

            data.append(data_item)

        model = d.pop("model")

        usage = InferenceGenerateUsage.from_dict(d.pop("usage"))

        inference_extract_response = cls(
            object_=object_,
            data=data,
            model=model,
            usage=usage,
        )

        inference_extract_response.additional_properties = d
        return inference_extract_response

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
