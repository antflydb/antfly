from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.source_content_part_source_type import SourceContentPartSourceType
from ..models.source_content_part_source_version_kind import SourceContentPartSourceVersionKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="SourceContentPartSource")


@_attrs_define
class SourceContentPartSource:
    """
    Attributes:
        type_ (SourceContentPartSourceType):
        uri (str): HTTP(S) URL. Treat source media URLs as private.
        mime_type (str): image/png, image/jpeg, image/webp, image/gif, or an audio/* or video/* type.
        version (str): Quoted strong ETag. The fetch fails if the source no longer has this version.
        length (int): Exact object length.
        version_kind (SourceContentPartSourceVersionKind | Unset):  Default:
            SourceContentPartSourceVersionKind.STRONG_ETAG.
    """

    type_: SourceContentPartSourceType
    uri: str
    mime_type: str
    version: str
    length: int
    version_kind: SourceContentPartSourceVersionKind | Unset = SourceContentPartSourceVersionKind.STRONG_ETAG

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
        type_ = SourceContentPartSourceType(d.pop("type"))

        uri = d.pop("uri")

        mime_type = d.pop("mime_type")

        version = d.pop("version")

        length = d.pop("length")

        _version_kind = d.pop("version_kind", UNSET)
        version_kind: SourceContentPartSourceVersionKind | Unset
        if isinstance(_version_kind, Unset):
            version_kind = UNSET
        else:
            version_kind = SourceContentPartSourceVersionKind(_version_kind)

        source_content_part_source = cls(
            type_=type_,
            uri=uri,
            mime_type=mime_type,
            version=version,
            length=length,
            version_kind=version_kind,
        )

        return source_content_part_source
