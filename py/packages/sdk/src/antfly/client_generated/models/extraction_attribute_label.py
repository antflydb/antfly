from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="ExtractionAttributeLabel")


@_attrs_define
class ExtractionAttributeLabel:
    """
    Attributes:
        label (str):
        confidence (float):
    """

    label: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        label = self.label

        confidence = self.confidence

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "label": label,
                "confidence": confidence,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        label = d.pop("label")

        confidence = d.pop("confidence")

        extraction_attribute_label = cls(
            label=label,
            confidence=confidence,
        )

        return extraction_attribute_label
