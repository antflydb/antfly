from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemorillaDiagnosticRegion")


@_attrs_define
class MemorillaDiagnosticRegion:
    """A region of one caller document, mapped from encoder copy positions the way citations are.

    Attributes:
        document_index (int): Index into the request's document_texts.
        start (int): Inclusive UTF-8 byte offset into document_texts[document_index], on a character boundary.
        end (int): Exclusive UTF-8 byte offset; end > start.
        text (str): Exactly the bytes document_texts[document_index][start:end], surrounding whitespace trimmed.
    """

    document_index: int
    start: int
    end: int
    text: str

    def to_dict(self) -> dict[str, Any]:
        document_index = self.document_index

        start = self.start

        end = self.end

        text = self.text

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "document_index": document_index,
                "start": start,
                "end": end,
                "text": text,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        document_index = d.pop("document_index")

        start = d.pop("start")

        end = d.pop("end")

        text = d.pop("text")

        memorilla_diagnostic_region = cls(
            document_index=document_index,
            start=start,
            end=end,
            text=text,
        )

        return memorilla_diagnostic_region
