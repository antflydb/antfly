from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memorilla_diagnostic_region import MemorillaDiagnosticRegion


T = TypeVar("T", bound="MemorillaDiagnosticWindow")


@_attrs_define
class MemorillaDiagnosticWindow:
    """
    Attributes:
        text (str): The evidence window as the decoder reads it when the gate opens (its tokens padded with spaces to
            window_rows), decoded.
        regions (list[MemorillaDiagnosticRegion]): Where the window sits in document_texts, one region per document it
            touches (a window never crosses documents today). Empty when citations_unavailable.
    """

    text: str
    regions: list[MemorillaDiagnosticRegion]

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        regions = []
        for regions_item_data in self.regions:
            regions_item = regions_item_data.to_dict()
            regions.append(regions_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "text": text,
                "regions": regions,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memorilla_diagnostic_region import MemorillaDiagnosticRegion

        d = dict(src_dict)
        text = d.pop("text")

        regions = []
        _regions = d.pop("regions")
        for regions_item_data in _regions:
            regions_item = MemorillaDiagnosticRegion.from_dict(regions_item_data)

            regions.append(regions_item)

        memorilla_diagnostic_window = cls(
            text=text,
            regions=regions,
        )

        return memorilla_diagnostic_window
