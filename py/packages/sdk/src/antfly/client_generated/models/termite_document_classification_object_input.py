from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TermiteDocumentClassificationObjectInput")


@_attrs_define
class TermiteDocumentClassificationObjectInput:
    """
    Attributes:
        image_path (str):
        num_tokens (int):
    """

    image_path: str
    num_tokens: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        image_path = self.image_path

        num_tokens = self.num_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "image_path": image_path,
                "num_tokens": num_tokens,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        image_path = d.pop("image_path")

        num_tokens = d.pop("num_tokens")

        termite_document_classification_object_input = cls(
            image_path=image_path,
            num_tokens=num_tokens,
        )

        termite_document_classification_object_input.additional_properties = d
        return termite_document_classification_object_input

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
