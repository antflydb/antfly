from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.image_url_content_part_type import ImageURLContentPartType

if TYPE_CHECKING:
    from ..models.image_url import ImageURL


T = TypeVar("T", bound="ImageURLContentPart")


@_attrs_define
class ImageURLContentPart:
    """Image content in the OpenAI-compatible content part format.

    Attributes:
        type_ (ImageURLContentPartType):
        image_url (ImageURL): Image URL or data URI.
    """

    type_: ImageURLContentPartType
    image_url: ImageURL
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        image_url = self.image_url.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "image_url": image_url,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.image_url import ImageURL

        d = dict(src_dict)
        type_ = ImageURLContentPartType(d.pop("type"))

        image_url = ImageURL.from_dict(d.pop("image_url"))

        image_url_content_part = cls(
            type_=type_,
            image_url=image_url,
        )

        image_url_content_part.additional_properties = d
        return image_url_content_part

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
