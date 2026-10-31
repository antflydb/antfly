from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_read_result_kind import InferenceReadResultKind
from ..models.inference_read_result_origin import InferenceReadResultOrigin
from ..models.inference_read_result_status import InferenceReadResultStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_audio_read_metadata import InferenceAudioReadMetadata
    from ..models.inference_read_result_fields_type_0 import InferenceReadResultFieldsType0
    from ..models.inference_read_result_inherited_parts_item import InferenceReadResultInheritedPartsItem
    from ..models.inference_read_result_raster_to_source_type_0 import InferenceReadResultRasterToSourceType0
    from ..models.inference_read_result_tables_item import InferenceReadResultTablesItem
    from ..models.inference_read_result_transform import InferenceReadResultTransform
    from ..models.inference_text_region import InferenceTextRegion


T = TypeVar("T", bound="InferenceReadResult")


@_attrs_define
class InferenceReadResult:
    """
    Attributes:
        text (str): Extracted text from the image Example: Invoice Total: $123.45.
        audio (InferenceAudioReadMetadata | Unset): Recognition evidence for the audio track only. Video frames are not
            captioned. Absent segments and speakers make no alignment or speaker claim.
        source_id (str | Unset): Original document source ID.
        number (int | Unset): One-based original page or slide number.
        kind (InferenceReadResultKind | Unset):
        inherited_parts (list[InferenceReadResultInheritedPartsItem] | Unset): Native layout/master content, source part
            identity, and visibility/coverage metadata; unsupported composition remains unresolved.
        native_text (str | Unset):
        notes (str | Unset): Speaker notes, separate from slide text.
        status (InferenceReadResultStatus | Unset):
        origin (InferenceReadResultOrigin | Unset): Default provenance for page text; mapped region origins override it.
        coordinate_space (str | Unset): Coordinate space of regions; PDF points, slide EMUs or image pixels.
        transform (InferenceReadResultTransform | Unset): Projective source transform.
        raster_to_source (InferenceReadResultRasterToSourceType0 | None | Unset): Mapping from rendered pixels to
            original source coordinates, including page rotation and actual render DPI.
        tables (list[InferenceReadResultTablesItem] | Unset): Tables and cells in source coordinates with provenance.
        findings (list[str] | Unset):
        fields (InferenceReadResultFieldsType0 | None | Unset): Structured fields extracted by document understanding
            models (Donut, Florence-2).
            Fields are flattened with dot notation for nested structures.
            Only present for models that output structured data.
             Example: {'menu.nm': 'Coffee', 'menu.price': '$3.50', 'total': '$123.45'}.
        regions (list[InferenceTextRegion] | Unset): Individual text regions with bounding boxes and recognized text.
            Populated by multi-stage OCR models (Surya, PaddleOCR).
    """

    text: str
    audio: InferenceAudioReadMetadata | Unset = UNSET
    source_id: str | Unset = UNSET
    number: int | Unset = UNSET
    kind: InferenceReadResultKind | Unset = UNSET
    inherited_parts: list[InferenceReadResultInheritedPartsItem] | Unset = UNSET
    native_text: str | Unset = UNSET
    notes: str | Unset = UNSET
    status: InferenceReadResultStatus | Unset = UNSET
    origin: InferenceReadResultOrigin | Unset = UNSET
    coordinate_space: str | Unset = UNSET
    transform: InferenceReadResultTransform | Unset = UNSET
    raster_to_source: InferenceReadResultRasterToSourceType0 | None | Unset = UNSET
    tables: list[InferenceReadResultTablesItem] | Unset = UNSET
    findings: list[str] | Unset = UNSET
    fields: InferenceReadResultFieldsType0 | None | Unset = UNSET
    regions: list[InferenceTextRegion] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.inference_read_result_fields_type_0 import InferenceReadResultFieldsType0
        from ..models.inference_read_result_raster_to_source_type_0 import InferenceReadResultRasterToSourceType0

        text = self.text

        audio: dict[str, Any] | Unset = UNSET
        if not isinstance(self.audio, Unset):
            audio = self.audio.to_dict()

        source_id = self.source_id

        number = self.number

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        inherited_parts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.inherited_parts, Unset):
            inherited_parts = []
            for inherited_parts_item_data in self.inherited_parts:
                inherited_parts_item = inherited_parts_item_data.to_dict()
                inherited_parts.append(inherited_parts_item)

        native_text = self.native_text

        notes = self.notes

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        origin: str | Unset = UNSET
        if not isinstance(self.origin, Unset):
            origin = self.origin.value

        coordinate_space = self.coordinate_space

        transform: dict[str, Any] | Unset = UNSET
        if not isinstance(self.transform, Unset):
            transform = self.transform.to_dict()

        raster_to_source: dict[str, Any] | None | Unset
        if isinstance(self.raster_to_source, Unset):
            raster_to_source = UNSET
        elif isinstance(self.raster_to_source, InferenceReadResultRasterToSourceType0):
            raster_to_source = self.raster_to_source.to_dict()
        else:
            raster_to_source = self.raster_to_source

        tables: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tables, Unset):
            tables = []
            for tables_item_data in self.tables:
                tables_item = tables_item_data.to_dict()
                tables.append(tables_item)

        findings: list[str] | Unset = UNSET
        if not isinstance(self.findings, Unset):
            findings = self.findings

        fields: dict[str, Any] | None | Unset
        if isinstance(self.fields, Unset):
            fields = UNSET
        elif isinstance(self.fields, InferenceReadResultFieldsType0):
            fields = self.fields.to_dict()
        else:
            fields = self.fields

        regions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.regions, Unset):
            regions = []
            for regions_item_data in self.regions:
                regions_item = regions_item_data.to_dict()
                regions.append(regions_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
            }
        )
        if audio is not UNSET:
            field_dict["audio"] = audio
        if source_id is not UNSET:
            field_dict["source_id"] = source_id
        if number is not UNSET:
            field_dict["number"] = number
        if kind is not UNSET:
            field_dict["kind"] = kind
        if inherited_parts is not UNSET:
            field_dict["inherited_parts"] = inherited_parts
        if native_text is not UNSET:
            field_dict["native_text"] = native_text
        if notes is not UNSET:
            field_dict["notes"] = notes
        if status is not UNSET:
            field_dict["status"] = status
        if origin is not UNSET:
            field_dict["origin"] = origin
        if coordinate_space is not UNSET:
            field_dict["coordinate_space"] = coordinate_space
        if transform is not UNSET:
            field_dict["transform"] = transform
        if raster_to_source is not UNSET:
            field_dict["raster_to_source"] = raster_to_source
        if tables is not UNSET:
            field_dict["tables"] = tables
        if findings is not UNSET:
            field_dict["findings"] = findings
        if fields is not UNSET:
            field_dict["fields"] = fields
        if regions is not UNSET:
            field_dict["regions"] = regions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_audio_read_metadata import InferenceAudioReadMetadata
        from ..models.inference_read_result_fields_type_0 import InferenceReadResultFieldsType0
        from ..models.inference_read_result_inherited_parts_item import InferenceReadResultInheritedPartsItem
        from ..models.inference_read_result_raster_to_source_type_0 import InferenceReadResultRasterToSourceType0
        from ..models.inference_read_result_tables_item import InferenceReadResultTablesItem
        from ..models.inference_read_result_transform import InferenceReadResultTransform
        from ..models.inference_text_region import InferenceTextRegion

        d = dict(src_dict)
        text = d.pop("text")

        _audio = d.pop("audio", UNSET)
        audio: InferenceAudioReadMetadata | Unset
        if isinstance(_audio, Unset):
            audio = UNSET
        else:
            audio = InferenceAudioReadMetadata.from_dict(_audio)

        source_id = d.pop("source_id", UNSET)

        number = d.pop("number", UNSET)

        _kind = d.pop("kind", UNSET)
        kind: InferenceReadResultKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = InferenceReadResultKind(_kind)

        _inherited_parts = d.pop("inherited_parts", UNSET)
        inherited_parts: list[InferenceReadResultInheritedPartsItem] | Unset = UNSET
        if _inherited_parts is not UNSET:
            inherited_parts = []
            for inherited_parts_item_data in _inherited_parts:
                inherited_parts_item = InferenceReadResultInheritedPartsItem.from_dict(inherited_parts_item_data)

                inherited_parts.append(inherited_parts_item)

        native_text = d.pop("native_text", UNSET)

        notes = d.pop("notes", UNSET)

        _status = d.pop("status", UNSET)
        status: InferenceReadResultStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = InferenceReadResultStatus(_status)

        _origin = d.pop("origin", UNSET)
        origin: InferenceReadResultOrigin | Unset
        if isinstance(_origin, Unset):
            origin = UNSET
        else:
            origin = InferenceReadResultOrigin(_origin)

        coordinate_space = d.pop("coordinate_space", UNSET)

        _transform = d.pop("transform", UNSET)
        transform: InferenceReadResultTransform | Unset
        if isinstance(_transform, Unset):
            transform = UNSET
        else:
            transform = InferenceReadResultTransform.from_dict(_transform)

        def _parse_raster_to_source(data: object) -> InferenceReadResultRasterToSourceType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                raster_to_source_type_0 = InferenceReadResultRasterToSourceType0.from_dict(data)

                return raster_to_source_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(InferenceReadResultRasterToSourceType0 | None | Unset, data)

        raster_to_source = _parse_raster_to_source(d.pop("raster_to_source", UNSET))

        _tables = d.pop("tables", UNSET)
        tables: list[InferenceReadResultTablesItem] | Unset = UNSET
        if _tables is not UNSET:
            tables = []
            for tables_item_data in _tables:
                tables_item = InferenceReadResultTablesItem.from_dict(tables_item_data)

                tables.append(tables_item)

        findings = cast(list[str], d.pop("findings", UNSET))

        def _parse_fields(data: object) -> InferenceReadResultFieldsType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                fields_type_0 = InferenceReadResultFieldsType0.from_dict(data)

                return fields_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(InferenceReadResultFieldsType0 | None | Unset, data)

        fields = _parse_fields(d.pop("fields", UNSET))

        _regions = d.pop("regions", UNSET)
        regions: list[InferenceTextRegion] | Unset = UNSET
        if _regions is not UNSET:
            regions = []
            for regions_item_data in _regions:
                regions_item = InferenceTextRegion.from_dict(regions_item_data)

                regions.append(regions_item)

        inference_read_result = cls(
            text=text,
            audio=audio,
            source_id=source_id,
            number=number,
            kind=kind,
            inherited_parts=inherited_parts,
            native_text=native_text,
            notes=notes,
            status=status,
            origin=origin,
            coordinate_space=coordinate_space,
            transform=transform,
            raster_to_source=raster_to_source,
            tables=tables,
            findings=findings,
            fields=fields,
            regions=regions,
        )

        inference_read_result.additional_properties = d
        return inference_read_result

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
