from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceTextRegion")


@_attrs_define
class InferenceTextRegion:
    """
    Attributes:
        text (str): Recognized text within the region
        bbox (list[float]): Bounding box [x1, y1, x2, y2] in pixel coordinates
        confidence (float | Unset): Recognition confidence score (0-1)
        label (str | Unset): Semantic label from layout analysis (e.g., text, title, table)
    """

    text: str
    bbox: list[float]
    confidence: float | Unset = UNSET
    label: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        bbox = self.bbox

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
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if label is not UNSET:
            field_dict["label"] = label

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        bbox = cast(list[float], d.pop("bbox"))

        confidence = d.pop("confidence", UNSET)

        label = d.pop("label", UNSET)

        inference_text_region = cls(
            text=text,
            bbox=bbox,
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
