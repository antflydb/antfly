from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.composed_source_overlay import ComposedSourceOverlay
    from ..models.composed_table_source import ComposedTableSource


T = TypeVar("T", bound="ComposedQuerySource")


@_attrs_define
class ComposedQuerySource:
    """Specify exactly one of union or overlay. Union preserves duplicates and table provenance; overlay suppresses base
    keys using unfiltered change lookups before global ordering. Disjoint RRF unions support the first 4096 global
    positions over exact leaf totals. Overlays and field ordering require complete matching sets of at most 4096 per
    input; larger sets fail without truncation.

        Attributes:
            union (list[ComposedTableSource] | Unset):
            overlay (ComposedSourceOverlay | Unset):
    """

    union: list[ComposedTableSource] | Unset = UNSET
    overlay: ComposedSourceOverlay | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        union: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.union, Unset):
            union = []
            for union_item_data in self.union:
                union_item = union_item_data.to_dict()
                union.append(union_item)

        overlay: dict[str, Any] | Unset = UNSET
        if not isinstance(self.overlay, Unset):
            overlay = self.overlay.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if union is not UNSET:
            field_dict["union"] = union
        if overlay is not UNSET:
            field_dict["overlay"] = overlay

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composed_source_overlay import ComposedSourceOverlay
        from ..models.composed_table_source import ComposedTableSource

        d = dict(src_dict)
        _union = d.pop("union", UNSET)
        union: list[ComposedTableSource] | Unset = UNSET
        if _union is not UNSET:
            union = []
            for union_item_data in _union:
                union_item = ComposedTableSource.from_dict(union_item_data)

                union.append(union_item)

        _overlay = d.pop("overlay", UNSET)
        overlay: ComposedSourceOverlay | Unset
        if isinstance(_overlay, Unset):
            overlay = UNSET
        else:
            overlay = ComposedSourceOverlay.from_dict(_overlay)

        composed_query_source = cls(
            union=union,
            overlay=overlay,
        )

        return composed_query_source
