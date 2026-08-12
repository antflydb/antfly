from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_classify_object_object import InferenceClassifyObjectObject

if TYPE_CHECKING:
    from ..models.inference_classify_result import InferenceClassifyResult


T = TypeVar("T", bound="InferenceClassifyObject")


@_attrs_define
class InferenceClassifyObject:
    """
    Attributes:
        object_ (InferenceClassifyObjectObject):
        index (int): Original input text index.
        classifications (list[InferenceClassifyResult]): Classification results for this input text.
    """

    object_: InferenceClassifyObjectObject
    index: int
    classifications: list[InferenceClassifyResult]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        classifications = []
        for classifications_item_data in self.classifications:
            classifications_item = classifications_item_data.to_dict()
            classifications.append(classifications_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "classifications": classifications,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_classify_result import InferenceClassifyResult

        d = dict(src_dict)
        object_ = InferenceClassifyObjectObject(d.pop("object"))

        index = d.pop("index")

        classifications = []
        _classifications = d.pop("classifications")
        for classifications_item_data in _classifications:
            classifications_item = InferenceClassifyResult.from_dict(classifications_item_data)

            classifications.append(classifications_item)

        inference_classify_object = cls(
            object_=object_,
            index=index,
            classifications=classifications,
        )

        inference_classify_object.additional_properties = d
        return inference_classify_object

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
