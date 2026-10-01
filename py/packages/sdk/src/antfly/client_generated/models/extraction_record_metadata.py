from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_record_metadata_anchor import ExtractionRecordMetadataAnchor


T = TypeVar("T", bound="ExtractionRecordMetadata")


@_attrs_define
class ExtractionRecordMetadata:
    """
    Attributes:
        score (float | Unset):
        anchor (ExtractionRecordMetadataAnchor | Unset):
    """

    score: float | Unset = UNSET
    anchor: ExtractionRecordMetadataAnchor | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        score = self.score

        anchor: dict[str, Any] | Unset = UNSET
        if not isinstance(self.anchor, Unset):
            anchor = self.anchor.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if score is not UNSET:
            field_dict["score"] = score
        if anchor is not UNSET:
            field_dict["anchor"] = anchor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_record_metadata_anchor import ExtractionRecordMetadataAnchor

        d = dict(src_dict)
        score = d.pop("score", UNSET)

        _anchor = d.pop("anchor", UNSET)
        anchor: ExtractionRecordMetadataAnchor | Unset
        if isinstance(_anchor, Unset):
            anchor = UNSET
        else:
            anchor = ExtractionRecordMetadataAnchor.from_dict(_anchor)

        extraction_record_metadata = cls(
            score=score,
            anchor=anchor,
        )

        return extraction_record_metadata
