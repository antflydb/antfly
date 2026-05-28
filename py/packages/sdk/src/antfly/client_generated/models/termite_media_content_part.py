from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_media_content_part_type import TermiteMediaContentPartType

T = TypeVar("T", bound="TermiteMediaContentPart")


@_attrs_define
class TermiteMediaContentPart:
    """Inline binary media content for providers that support non-image media parts.

    Attributes:
        type_ (TermiteMediaContentPartType):
        data (str): Base64-encoded binary data.
        mime_type (str): MIME type such as image/png, audio/wav, or application/pdf.
    """

    type_: TermiteMediaContentPartType
    data: str
    mime_type: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        data = self.data

        mime_type = self.mime_type

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "data": data,
                "mime_type": mime_type,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = TermiteMediaContentPartType(d.pop("type"))

        data = d.pop("data")

        mime_type = d.pop("mime_type")

        termite_media_content_part = cls(
            type_=type_,
            data=data,
            mime_type=mime_type,
        )

        termite_media_content_part.additional_properties = d
        return termite_media_content_part

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
