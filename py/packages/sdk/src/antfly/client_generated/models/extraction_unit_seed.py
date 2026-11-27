from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.extraction_unit_seed_page_rotation import ExtractionUnitSeedPageRotation
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.text_box_seed import TextBoxSeed
    from ..models.transcript_span_seed import TranscriptSpanSeed


T = TypeVar("T", bound="ExtractionUnitSeed")


@_attrs_define
class ExtractionUnitSeed:
    """One unit of a rich document extraction seed. Only data the caller has
    is recorded; nothing absent (coordinates, timing) is fabricated. Unit
    ids are `<unit_type>:<ordinal>`, numbered from 000001 in order.

        Attributes:
            text (str):
            unit_type (str | Unset): Defaults to `page` with a page number, `transcript` with spans, else `document` (the
                plain text route's unit).
            page_number (int | Unset):
            page_label (str | Unset):
            page_bbox (list[float] | Unset):
            page_rotation (ExtractionUnitSeedPageRotation | Unset):
            spans (list[TranscriptSpanSeed] | Unset): Ordered, disjoint transcript phrases in unit text coordinates.
            text_regions (list[TextBoxSeed] | Unset): Grounded text regions (citations, highlights), ordered by span start.
                Requires `page_bbox`.
            text_blocks (list[TextBoxSeed] | Unset): Reading-order text blocks, listed in reading order (an explicit
                `reading_order` must equal the position). Requires `page_bbox`.
    """

    text: str
    unit_type: str | Unset = UNSET
    page_number: int | Unset = UNSET
    page_label: str | Unset = UNSET
    page_bbox: list[float] | Unset = UNSET
    page_rotation: ExtractionUnitSeedPageRotation | Unset = UNSET
    spans: list[TranscriptSpanSeed] | Unset = UNSET
    text_regions: list[TextBoxSeed] | Unset = UNSET
    text_blocks: list[TextBoxSeed] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        unit_type = self.unit_type

        page_number = self.page_number

        page_label = self.page_label

        page_bbox: list[float] | Unset = UNSET
        if not isinstance(self.page_bbox, Unset):
            page_bbox = self.page_bbox

        page_rotation: int | Unset = UNSET
        if not isinstance(self.page_rotation, Unset):
            page_rotation = self.page_rotation.value

        spans: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.spans, Unset):
            spans = []
            for spans_item_data in self.spans:
                spans_item = spans_item_data.to_dict()
                spans.append(spans_item)

        text_regions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.text_regions, Unset):
            text_regions = []
            for text_regions_item_data in self.text_regions:
                text_regions_item = text_regions_item_data.to_dict()
                text_regions.append(text_regions_item)

        text_blocks: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.text_blocks, Unset):
            text_blocks = []
            for text_blocks_item_data in self.text_blocks:
                text_blocks_item = text_blocks_item_data.to_dict()
                text_blocks.append(text_blocks_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "text": text,
            }
        )
        if unit_type is not UNSET:
            field_dict["unit_type"] = unit_type
        if page_number is not UNSET:
            field_dict["page_number"] = page_number
        if page_label is not UNSET:
            field_dict["page_label"] = page_label
        if page_bbox is not UNSET:
            field_dict["page_bbox"] = page_bbox
        if page_rotation is not UNSET:
            field_dict["page_rotation"] = page_rotation
        if spans is not UNSET:
            field_dict["spans"] = spans
        if text_regions is not UNSET:
            field_dict["text_regions"] = text_regions
        if text_blocks is not UNSET:
            field_dict["text_blocks"] = text_blocks

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.text_box_seed import TextBoxSeed
        from ..models.transcript_span_seed import TranscriptSpanSeed

        d = dict(src_dict)
        text = d.pop("text")

        unit_type = d.pop("unit_type", UNSET)

        page_number = d.pop("page_number", UNSET)

        page_label = d.pop("page_label", UNSET)

        page_bbox = cast(list[float], d.pop("page_bbox", UNSET))

        _page_rotation = d.pop("page_rotation", UNSET)
        page_rotation: ExtractionUnitSeedPageRotation | Unset
        if isinstance(_page_rotation, Unset):
            page_rotation = UNSET
        else:
            page_rotation = ExtractionUnitSeedPageRotation(_page_rotation)

        _spans = d.pop("spans", UNSET)
        spans: list[TranscriptSpanSeed] | Unset = UNSET
        if _spans is not UNSET:
            spans = []
            for spans_item_data in _spans:
                spans_item = TranscriptSpanSeed.from_dict(spans_item_data)

                spans.append(spans_item)

        _text_regions = d.pop("text_regions", UNSET)
        text_regions: list[TextBoxSeed] | Unset = UNSET
        if _text_regions is not UNSET:
            text_regions = []
            for text_regions_item_data in _text_regions:
                text_regions_item = TextBoxSeed.from_dict(text_regions_item_data)

                text_regions.append(text_regions_item)

        _text_blocks = d.pop("text_blocks", UNSET)
        text_blocks: list[TextBoxSeed] | Unset = UNSET
        if _text_blocks is not UNSET:
            text_blocks = []
            for text_blocks_item_data in _text_blocks:
                text_blocks_item = TextBoxSeed.from_dict(text_blocks_item_data)

                text_blocks.append(text_blocks_item)

        extraction_unit_seed = cls(
            text=text,
            unit_type=unit_type,
            page_number=page_number,
            page_label=page_label,
            page_bbox=page_bbox,
            page_rotation=page_rotation,
            spans=spans,
            text_regions=text_regions,
            text_blocks=text_blocks,
        )

        return extraction_unit_seed
