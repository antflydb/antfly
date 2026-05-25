from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TermiteDocumentTokenBox")


@_attrs_define
class TermiteDocumentTokenBox:
    """
    Attributes:
        text (str):
        bbox (list[int]): Bounding box normalized to the same 0-1000 layout space used by training Example: [0, 0, 120,
            24].
    """

    text: str
    bbox: list[int]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        bbox = self.bbox

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "bbox": bbox,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        bbox = cast(list[int], d.pop("bbox"))

        termite_document_token_box = cls(
            text=text,
            bbox=bbox,
        )

        termite_document_token_box.additional_properties = d
        return termite_document_token_box

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
