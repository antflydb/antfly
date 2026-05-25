from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TermiteTextContent")


@_attrs_define
class TermiteTextContent:
    """Text content with character offsets.

    Attributes:
        text (str): The chunk text content
        start_char (int): Character position in original text where chunk starts
        end_char (int): Character position in original text where chunk ends (exclusive)
    """

    text: str
    start_char: int
    end_char: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        start_char = self.start_char

        end_char = self.end_char

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "start_char": start_char,
                "end_char": end_char,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        start_char = d.pop("start_char")

        end_char = d.pop("end_char")

        termite_text_content = cls(
            text=text,
            start_char=start_char,
            end_char=end_char,
        )

        termite_text_content.additional_properties = d
        return termite_text_content

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
