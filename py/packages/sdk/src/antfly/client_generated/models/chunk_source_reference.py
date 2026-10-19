from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_source_reference_type import ChunkSourceReferenceType
from ..models.chunk_source_reference_version_kind import ChunkSourceReferenceVersionKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkSourceReference")


@_attrs_define
class ChunkSourceReference:
    """
    Attributes:
        type_ (ChunkSourceReferenceType):
        uri (str): HTTP(S) or S3 object URI resolved through configured content security and credentials.
        mime_type (str):
        version (str): Quoted strong HTTP/S3 ETag or immutable S3 version ID; enforced on every range read.
        length (int): Exact immutable object length, verified against Content-Range.
        version_kind (ChunkSourceReferenceVersionKind | Unset): Defaults to strong_etag. S3 version IDs are supported
            only for S3 sources.
    """

    type_: ChunkSourceReferenceType
    uri: str
    mime_type: str
    version: str
    length: int
    version_kind: ChunkSourceReferenceVersionKind | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        uri = self.uri

        mime_type = self.mime_type

        version = self.version

        length = self.length

        version_kind: str | Unset = UNSET
        if not isinstance(self.version_kind, Unset):
            version_kind = self.version_kind.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "uri": uri,
                "mime_type": mime_type,
                "version": version,
                "length": length,
            }
        )
        if version_kind is not UNSET:
            field_dict["version_kind"] = version_kind

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ChunkSourceReferenceType(d.pop("type"))

        uri = d.pop("uri")

        mime_type = d.pop("mime_type")

        version = d.pop("version")

        length = d.pop("length")

        _version_kind = d.pop("version_kind", UNSET)
        version_kind: ChunkSourceReferenceVersionKind | Unset
        if isinstance(_version_kind, Unset):
            version_kind = UNSET
        else:
            version_kind = ChunkSourceReferenceVersionKind(_version_kind)

        chunk_source_reference = cls(
            type_=type_,
            uri=uri,
            mime_type=mime_type,
            version=version,
            length=length,
            version_kind=version_kind,
        )

        chunk_source_reference.additional_properties = d
        return chunk_source_reference

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
