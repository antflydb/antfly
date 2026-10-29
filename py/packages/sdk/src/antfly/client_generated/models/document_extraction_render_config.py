from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_extraction_render_config_resolution import DocumentExtractionRenderConfigResolution
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentExtractionRenderConfig")


@_attrs_define
class DocumentExtractionRenderConfig:
    """How PDF pages are rasterized for OCR.

    Attributes:
        dpi (int | Unset): Render resolution in dots per inch. Default: 150.
        resolution (DocumentExtractionRenderConfigResolution | Unset): `requested_dpi` renders at `dpi` within the caps.
            `model_input` renders near the model's preferred input size. Default:
            DocumentExtractionRenderConfigResolution.REQUESTED_DPI.
        max_pixels (int | Unset): Maximum pixels per rendered page. Default: 40000000.
        max_dimension (int | Unset): Maximum width or height of a rendered page. Default: 4096.
    """

    dpi: int | Unset = 150
    resolution: DocumentExtractionRenderConfigResolution | Unset = (
        DocumentExtractionRenderConfigResolution.REQUESTED_DPI
    )
    max_pixels: int | Unset = 40000000
    max_dimension: int | Unset = 4096
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        dpi = self.dpi

        resolution: str | Unset = UNSET
        if not isinstance(self.resolution, Unset):
            resolution = self.resolution.value

        max_pixels = self.max_pixels

        max_dimension = self.max_dimension

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if dpi is not UNSET:
            field_dict["dpi"] = dpi
        if resolution is not UNSET:
            field_dict["resolution"] = resolution
        if max_pixels is not UNSET:
            field_dict["max_pixels"] = max_pixels
        if max_dimension is not UNSET:
            field_dict["max_dimension"] = max_dimension

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        dpi = d.pop("dpi", UNSET)

        _resolution = d.pop("resolution", UNSET)
        resolution: DocumentExtractionRenderConfigResolution | Unset
        if isinstance(_resolution, Unset):
            resolution = UNSET
        else:
            resolution = DocumentExtractionRenderConfigResolution(_resolution)

        max_pixels = d.pop("max_pixels", UNSET)

        max_dimension = d.pop("max_dimension", UNSET)

        document_extraction_render_config = cls(
            dpi=dpi,
            resolution=resolution,
            max_pixels=max_pixels,
            max_dimension=max_dimension,
        )

        document_extraction_render_config.additional_properties = d
        return document_extraction_render_config

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
