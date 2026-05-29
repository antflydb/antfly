from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceDocumentTokenClassificationFeatures")


@_attrs_define
class InferenceDocumentTokenClassificationFeatures:
    """
    Attributes:
        text_length (int):
        bbox (list[int]):
        width (float):
        height (float):
        relative_position (float):
        bbox_phase_sin (float):
    """

    text_length: int
    bbox: list[int]
    width: float
    height: float
    relative_position: float
    bbox_phase_sin: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text_length = self.text_length

        bbox = self.bbox

        width = self.width

        height = self.height

        relative_position = self.relative_position

        bbox_phase_sin = self.bbox_phase_sin

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text_length": text_length,
                "bbox": bbox,
                "width": width,
                "height": height,
                "relative_position": relative_position,
                "bbox_phase_sin": bbox_phase_sin,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text_length = d.pop("text_length")

        bbox = cast(list[int], d.pop("bbox"))

        width = d.pop("width")

        height = d.pop("height")

        relative_position = d.pop("relative_position")

        bbox_phase_sin = d.pop("bbox_phase_sin")

        inference_document_token_classification_features = cls(
            text_length=text_length,
            bbox=bbox,
            width=width,
            height=height,
            relative_position=relative_position,
            bbox_phase_sin=bbox_phase_sin,
        )

        inference_document_token_classification_features.additional_properties = d
        return inference_document_token_classification_features

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
