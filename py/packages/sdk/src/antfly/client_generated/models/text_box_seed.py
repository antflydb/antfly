from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="TextBoxSeed")


@_attrs_define
class TextBoxSeed:
    """Geometry of a span of unit text on its page. `span` holds UTF-8 byte
    offsets into the unit text on character boundaries. `bbox` is
    `[x0, y0, x1, y1]` in the page's own coordinate space, the space of
    `page_bbox` (source page points, as native extraction records them),
    and lies within the page box.

        Attributes:
            span (list[int]):
            bbox (list[float]):
            font_size (float | Unset):
            bold (bool | Unset):
            italic (bool | Unset):
            reading_order (int | Unset): Blocks only; must equal the block's position.
    """

    span: list[int]
    bbox: list[float]
    font_size: float | Unset = UNSET
    bold: bool | Unset = UNSET
    italic: bool | Unset = UNSET
    reading_order: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        span = self.span

        bbox = self.bbox

        font_size = self.font_size

        bold = self.bold

        italic = self.italic

        reading_order = self.reading_order

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "span": span,
                "bbox": bbox,
            }
        )
        if font_size is not UNSET:
            field_dict["font_size"] = font_size
        if bold is not UNSET:
            field_dict["bold"] = bold
        if italic is not UNSET:
            field_dict["italic"] = italic
        if reading_order is not UNSET:
            field_dict["reading_order"] = reading_order

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        span = cast(list[int], d.pop("span"))

        bbox = cast(list[float], d.pop("bbox"))

        font_size = d.pop("font_size", UNSET)

        bold = d.pop("bold", UNSET)

        italic = d.pop("italic", UNSET)

        reading_order = d.pop("reading_order", UNSET)

        text_box_seed = cls(
            span=span,
            bbox=bbox,
            font_size=font_size,
            bold=bold,
            italic=italic,
            reading_order=reading_order,
        )

        return text_box_seed
