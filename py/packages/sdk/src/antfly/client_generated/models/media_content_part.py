from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.media_content_part_type import MediaContentPartType
from ..types import UNSET, Unset

T = TypeVar("T", bound="MediaContentPart")


@_attrs_define
class MediaContentPart:
    """Binary or URL media content for providers that support non-image media parts.

    Attributes:
        type_ (MediaContentPartType):
        data (str | Unset): Base64-encoded binary data. Use either data or url.
        url (str | Unset): URL or data URI media reference. Use either url or data.
        mime_type (str | Unset): MIME type such as image/png, audio/wav, or application/pdf. Required with data and
            optional with url when the URL can resolve content type.
    """

    type_: MediaContentPartType
    data: str | Unset = UNSET
    url: str | Unset = UNSET
    mime_type: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        data = self.data

        url = self.url

        mime_type = self.mime_type

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if data is not UNSET:
            field_dict["data"] = data
        if url is not UNSET:
            field_dict["url"] = url
        if mime_type is not UNSET:
            field_dict["mime_type"] = mime_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = MediaContentPartType(d.pop("type"))

        data = d.pop("data", UNSET)

        url = d.pop("url", UNSET)

        mime_type = d.pop("mime_type", UNSET)

        media_content_part = cls(
            type_=type_,
            data=data,
            url=url,
            mime_type=mime_type,
        )

        media_content_part.additional_properties = d
        return media_content_part

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
