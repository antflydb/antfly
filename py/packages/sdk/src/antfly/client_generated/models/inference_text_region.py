from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_text_region_origin import InferenceTextRegionOrigin
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_text_region_children_item import InferenceTextRegionChildrenItem
    from ..models.inference_text_region_fields_type_0 import InferenceTextRegionFieldsType0
    from ..models.inference_text_region_tables_item import InferenceTextRegionTablesItem


T = TypeVar("T", bound="InferenceTextRegion")


@_attrs_define
class InferenceTextRegion:
    """
    Attributes:
        text (str): Recognized text within the region
        bbox (list[float]): Bounding box [x1, y1, x2, y2] in the document source coordinate space, or pixels for
            ordinary image requests
        id (int | Unset): Region identity within its parent; nested regions use qualified paths.
        origin (InferenceTextRegionOrigin | Unset): Default provenance of this region; nested region origins override
            it.
        text_start (int | None | Unset): Start UTF-8 byte offset in the containing page text; null when unmapped.
        text_end (int | None | Unset): Exclusive end UTF-8 byte offset in the containing page text.
        children (list[InferenceTextRegionChildrenItem] | Unset): Accepted subregions in the same source coordinate
            space and page text offsets.
        tables (list[InferenceTextRegionTablesItem] | Unset): Tables owned by this region, including cells and
            provenance.
        fields (InferenceTextRegionFieldsType0 | None | Unset): Structured output owned by this region.
        confidence (float | Unset): Recognition confidence score (0-1)
        label (str | Unset): Semantic label from layout analysis (e.g., text, title, table)
    """

    text: str
    bbox: list[float]
    id: int | Unset = UNSET
    origin: InferenceTextRegionOrigin | Unset = UNSET
    text_start: int | None | Unset = UNSET
    text_end: int | None | Unset = UNSET
    children: list[InferenceTextRegionChildrenItem] | Unset = UNSET
    tables: list[InferenceTextRegionTablesItem] | Unset = UNSET
    fields: InferenceTextRegionFieldsType0 | None | Unset = UNSET
    confidence: float | Unset = UNSET
    label: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.inference_text_region_fields_type_0 import InferenceTextRegionFieldsType0

        text = self.text

        bbox = self.bbox

        id = self.id

        origin: str | Unset = UNSET
        if not isinstance(self.origin, Unset):
            origin = self.origin.value

        text_start: int | None | Unset
        if isinstance(self.text_start, Unset):
            text_start = UNSET
        else:
            text_start = self.text_start

        text_end: int | None | Unset
        if isinstance(self.text_end, Unset):
            text_end = UNSET
        else:
            text_end = self.text_end

        children: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.children, Unset):
            children = []
            for children_item_data in self.children:
                children_item = children_item_data.to_dict()
                children.append(children_item)

        tables: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tables, Unset):
            tables = []
            for tables_item_data in self.tables:
                tables_item = tables_item_data.to_dict()
                tables.append(tables_item)

        fields: dict[str, Any] | None | Unset
        if isinstance(self.fields, Unset):
            fields = UNSET
        elif isinstance(self.fields, InferenceTextRegionFieldsType0):
            fields = self.fields.to_dict()
        else:
            fields = self.fields

        confidence = self.confidence

        label = self.label

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "bbox": bbox,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if origin is not UNSET:
            field_dict["origin"] = origin
        if text_start is not UNSET:
            field_dict["text_start"] = text_start
        if text_end is not UNSET:
            field_dict["text_end"] = text_end
        if children is not UNSET:
            field_dict["children"] = children
        if tables is not UNSET:
            field_dict["tables"] = tables
        if fields is not UNSET:
            field_dict["fields"] = fields
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if label is not UNSET:
            field_dict["label"] = label

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_text_region_children_item import InferenceTextRegionChildrenItem
        from ..models.inference_text_region_fields_type_0 import InferenceTextRegionFieldsType0
        from ..models.inference_text_region_tables_item import InferenceTextRegionTablesItem

        d = dict(src_dict)
        text = d.pop("text")

        bbox = cast(list[float], d.pop("bbox"))

        id = d.pop("id", UNSET)

        _origin = d.pop("origin", UNSET)
        origin: InferenceTextRegionOrigin | Unset
        if isinstance(_origin, Unset):
            origin = UNSET
        else:
            origin = InferenceTextRegionOrigin(_origin)

        def _parse_text_start(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        text_start = _parse_text_start(d.pop("text_start", UNSET))

        def _parse_text_end(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        text_end = _parse_text_end(d.pop("text_end", UNSET))

        _children = d.pop("children", UNSET)
        children: list[InferenceTextRegionChildrenItem] | Unset = UNSET
        if _children is not UNSET:
            children = []
            for children_item_data in _children:
                children_item = InferenceTextRegionChildrenItem.from_dict(children_item_data)

                children.append(children_item)

        _tables = d.pop("tables", UNSET)
        tables: list[InferenceTextRegionTablesItem] | Unset = UNSET
        if _tables is not UNSET:
            tables = []
            for tables_item_data in _tables:
                tables_item = InferenceTextRegionTablesItem.from_dict(tables_item_data)

                tables.append(tables_item)

        def _parse_fields(data: object) -> InferenceTextRegionFieldsType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                fields_type_0 = InferenceTextRegionFieldsType0.from_dict(data)

                return fields_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(InferenceTextRegionFieldsType0 | None | Unset, data)

        fields = _parse_fields(d.pop("fields", UNSET))

        confidence = d.pop("confidence", UNSET)

        label = d.pop("label", UNSET)

        inference_text_region = cls(
            text=text,
            bbox=bbox,
            id=id,
            origin=origin,
            text_start=text_start,
            text_end=text_end,
            children=children,
            tables=tables,
            fields=fields,
            confidence=confidence,
            label=label,
        )

        inference_text_region.additional_properties = d
        return inference_text_region

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
