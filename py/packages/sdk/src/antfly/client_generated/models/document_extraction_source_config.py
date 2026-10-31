from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentExtractionSourceConfig")


@_attrs_define
class DocumentExtractionSourceConfig:
    """Names of row fields that carry source metadata. Each overrides the matching static field.

    Attributes:
        filename_field (str | Unset): Row field holding the source filename.
        content_type_field (str | Unset): Row field holding the source content type.
        etag_field (str | Unset): Row field holding the source ETag.
        checksum_field (str | Unset): Row field holding the source checksum.
        version_field (str | Unset): Row field holding the source version.
        last_modified_field (str | Unset): Row field holding the source last-modified timestamp.
    """

    filename_field: str | Unset = UNSET
    content_type_field: str | Unset = UNSET
    etag_field: str | Unset = UNSET
    checksum_field: str | Unset = UNSET
    version_field: str | Unset = UNSET
    last_modified_field: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        filename_field = self.filename_field

        content_type_field = self.content_type_field

        etag_field = self.etag_field

        checksum_field = self.checksum_field

        version_field = self.version_field

        last_modified_field = self.last_modified_field

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if filename_field is not UNSET:
            field_dict["filename_field"] = filename_field
        if content_type_field is not UNSET:
            field_dict["content_type_field"] = content_type_field
        if etag_field is not UNSET:
            field_dict["etag_field"] = etag_field
        if checksum_field is not UNSET:
            field_dict["checksum_field"] = checksum_field
        if version_field is not UNSET:
            field_dict["version_field"] = version_field
        if last_modified_field is not UNSET:
            field_dict["last_modified_field"] = last_modified_field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        filename_field = d.pop("filename_field", UNSET)

        content_type_field = d.pop("content_type_field", UNSET)

        etag_field = d.pop("etag_field", UNSET)

        checksum_field = d.pop("checksum_field", UNSET)

        version_field = d.pop("version_field", UNSET)

        last_modified_field = d.pop("last_modified_field", UNSET)

        document_extraction_source_config = cls(
            filename_field=filename_field,
            content_type_field=content_type_field,
            etag_field=etag_field,
            checksum_field=checksum_field,
            version_field=version_field,
            last_modified_field=last_modified_field,
        )

        document_extraction_source_config.additional_properties = d
        return document_extraction_source_config

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
