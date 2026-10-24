from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.document_source_media_type import DocumentSourceMediaType
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentSource")


@_attrs_define
class DocumentSource:
    """
    Attributes:
        id (str): Unique source identity within the request. Separate sources are never implicitly merged.
        url (str): Inline data URI or location allowed by the deployment content security policy.
        media_type (DocumentSourceMediaType):
        pages (list[int] | Unset): Optional one-based page/slide selection, returned in original source order. Omission
            selects every visible page/slide.
        include_notes (bool | Unset): Include slide speaker notes as separate evidence, never silently as visible slide
            text. Default: False.
        include_hidden (bool | Unset): Include hidden slides. Default: False.
    """

    id: str
    url: str
    media_type: DocumentSourceMediaType
    pages: list[int] | Unset = UNSET
    include_notes: bool | Unset = False
    include_hidden: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        url = self.url

        media_type = self.media_type.value

        pages: list[int] | Unset = UNSET
        if not isinstance(self.pages, Unset):
            pages = self.pages

        include_notes = self.include_notes

        include_hidden = self.include_hidden

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "url": url,
                "media_type": media_type,
            }
        )
        if pages is not UNSET:
            field_dict["pages"] = pages
        if include_notes is not UNSET:
            field_dict["include_notes"] = include_notes
        if include_hidden is not UNSET:
            field_dict["include_hidden"] = include_hidden

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        url = d.pop("url")

        media_type = DocumentSourceMediaType(d.pop("media_type"))

        pages = cast(list[int], d.pop("pages", UNSET))

        include_notes = d.pop("include_notes", UNSET)

        include_hidden = d.pop("include_hidden", UNSET)

        document_source = cls(
            id=id,
            url=url,
            media_type=media_type,
            pages=pages,
            include_notes=include_notes,
            include_hidden=include_hidden,
        )

        return document_source
