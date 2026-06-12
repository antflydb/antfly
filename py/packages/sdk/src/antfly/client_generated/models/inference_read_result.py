from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_read_result_fields import InferenceReadResultFields
    from ..models.inference_text_region import InferenceTextRegion


T = TypeVar("T", bound="InferenceReadResult")


@_attrs_define
class InferenceReadResult:
    """
    Attributes:
        text (str): Extracted text from the image Example: Invoice Total: $123.45.
        fields (InferenceReadResultFields | Unset): Structured fields extracted by document understanding models (Donut,
            Florence-2).
            Fields are flattened with dot notation for nested structures.
            Only present for models that output structured data.
             Example: {'menu.nm': 'Coffee', 'menu.price': '$3.50', 'total': '$123.45'}.
        regions (list[InferenceTextRegion] | Unset): Individual text regions with bounding boxes and recognized text.
            Populated by multi-stage OCR models (Surya, PaddleOCR).
    """

    text: str
    fields: InferenceReadResultFields | Unset = UNSET
    regions: list[InferenceTextRegion] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        fields: dict[str, Any] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields.to_dict()

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
        if fields is not UNSET:
            field_dict["fields"] = fields
        if regions is not UNSET:
            field_dict["regions"] = regions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_read_result_fields import InferenceReadResultFields
        from ..models.inference_text_region import InferenceTextRegion

        d = dict(src_dict)
        text = d.pop("text")

        _fields = d.pop("fields", UNSET)
        fields: InferenceReadResultFields | Unset
        if isinstance(_fields, Unset):
            fields = UNSET
        else:
            fields = InferenceReadResultFields.from_dict(_fields)

        _regions = d.pop("regions", UNSET)
        regions: list[InferenceTextRegion] | Unset = UNSET
        if _regions is not UNSET:
            regions = []
            for regions_item_data in _regions:
                regions_item = InferenceTextRegion.from_dict(regions_item_data)

                regions.append(regions_item)

        inference_read_result = cls(
            text=text,
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
