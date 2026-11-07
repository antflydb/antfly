from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.source_content_part_type import SourceContentPartType

if TYPE_CHECKING:
    from ..models.source_content_part_source import SourceContentPartSource


T = TypeVar("T", bound="SourceContentPart")


@_attrs_define
class SourceContentPart:
    """Image, audio, or video named by a source record's `content.resource`. Accepted only on the Antfly gateway's generate
    and chat routes, which fetch the pinned version under `remote_content.security` and replace this part with inline
    media, so the source URL never reaches a model vendor. Fetching requires a nonempty `allowed_hosts`. Images are
    inlined within a 16 MiB expanded request. Audio and video travel as binary attachments, up to 256 MiB per request
    and `max_download_size_bytes`. Hosted providers receive them inline and may reject large requests. Models that do
    not support the media type reject the request.

        Attributes:
            type_ (SourceContentPartType):
            source (SourceContentPartSource):
    """

    type_: SourceContentPartType
    source: SourceContentPartSource

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        source = self.source.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "source": source,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.source_content_part_source import SourceContentPartSource

        d = dict(src_dict)
        type_ = SourceContentPartType(d.pop("type"))

        source = SourceContentPartSource.from_dict(d.pop("source"))

        source_content_part = cls(
            type_=type_,
            source=source,
        )

        return source_content_part
