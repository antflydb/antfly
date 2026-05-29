from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_rewrite_object_object import InferenceRewriteObjectObject

T = TypeVar("T", bound="InferenceRewriteObject")


@_attrs_define
class InferenceRewriteObject:
    """
    Attributes:
        object_ (InferenceRewriteObjectObject):
        index (int): Original input text index.
        texts (list[str]): Rewritten texts for this input, one per beam.
    """

    object_: InferenceRewriteObjectObject
    index: int
    texts: list[str]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        texts = self.texts

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "texts": texts,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        object_ = InferenceRewriteObjectObject(d.pop("object"))

        index = d.pop("index")

        texts = cast(list[str], d.pop("texts"))

        inference_rewrite_object = cls(
            object_=object_,
            index=index,
            texts=texts,
        )

        inference_rewrite_object.additional_properties = d
        return inference_rewrite_object

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
