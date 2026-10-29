from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_extraction_route_extractor_type import DocumentExtractionRouteExtractorType
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentExtractionRouteExtractor")


@_attrs_define
class DocumentExtractionRouteExtractor:
    """
    Attributes:
        type_ (DocumentExtractionRouteExtractorType): Extractor for matching sources. `image` is an alias of `ocr` and
            `transcript` of `audio`. An `ocr` route requires an `ocr` block and an `audio` route requires a `transcription`
            block; otherwise the configuration is rejected with `DocumentExtractionModelRequired`.
        unit (str | Unset): Unit type recorded for the extracted units, such as `page` or `transcript_segment`.
    """

    type_: DocumentExtractionRouteExtractorType
    unit: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        unit = self.unit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if unit is not UNSET:
            field_dict["unit"] = unit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = DocumentExtractionRouteExtractorType(d.pop("type"))

        unit = d.pop("unit", UNSET)

        document_extraction_route_extractor = cls(
            type_=type_,
            unit=unit,
        )

        document_extraction_route_extractor.additional_properties = d
        return document_extraction_route_extractor

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
