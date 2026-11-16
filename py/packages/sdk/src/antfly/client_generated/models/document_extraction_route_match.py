from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentExtractionRouteMatch")


@_attrs_define
class DocumentExtractionRouteMatch:
    """Conditions a source must meet. Every condition given must match.

    Attributes:
        content_type (str | Unset): Exact content type.
        content_type_prefix (str | Unset): Content type prefix, such as `image/`.
        extension (list[str] | str | Unset): Filename extension or list of extensions.
        magic_prefix (list[str] | str | Unset): Leading bytes or list of leading-byte prefixes.
    """

    content_type: str | Unset = UNSET
    content_type_prefix: str | Unset = UNSET
    extension: list[str] | str | Unset = UNSET
    magic_prefix: list[str] | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        content_type = self.content_type

        content_type_prefix = self.content_type_prefix

        extension: list[str] | str | Unset
        if isinstance(self.extension, Unset):
            extension = UNSET
        elif isinstance(self.extension, list):
            extension = self.extension

        else:
            extension = self.extension

        magic_prefix: list[str] | str | Unset
        if isinstance(self.magic_prefix, Unset):
            magic_prefix = UNSET
        elif isinstance(self.magic_prefix, list):
            magic_prefix = self.magic_prefix

        else:
            magic_prefix = self.magic_prefix

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if content_type is not UNSET:
            field_dict["content_type"] = content_type
        if content_type_prefix is not UNSET:
            field_dict["content_type_prefix"] = content_type_prefix
        if extension is not UNSET:
            field_dict["extension"] = extension
        if magic_prefix is not UNSET:
            field_dict["magic_prefix"] = magic_prefix

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        content_type = d.pop("content_type", UNSET)

        content_type_prefix = d.pop("content_type_prefix", UNSET)

        def _parse_extension(data: object) -> list[str] | str | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                extension_type_1 = cast(list[str], data)

                return extension_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | str | Unset, data)

        extension = _parse_extension(d.pop("extension", UNSET))

        def _parse_magic_prefix(data: object) -> list[str] | str | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                magic_prefix_type_1 = cast(list[str], data)

                return magic_prefix_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | str | Unset, data)

        magic_prefix = _parse_magic_prefix(d.pop("magic_prefix", UNSET))

        document_extraction_route_match = cls(
            content_type=content_type,
            content_type_prefix=content_type_prefix,
            extension=extension,
            magic_prefix=magic_prefix,
        )

        document_extraction_route_match.additional_properties = d
        return document_extraction_route_match

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
