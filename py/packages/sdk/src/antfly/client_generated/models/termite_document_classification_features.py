from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TermiteDocumentClassificationFeatures")


@_attrs_define
class TermiteDocumentClassificationFeatures:
    """
    Attributes:
        num_tokens (int):
        image_width (int):
        image_height (int):
        image_components (int):
        mean_darkness (float):
        std_darkness (float):
        top_darkness (float):
        bottom_darkness (float):
        left_darkness (float):
        right_darkness (float):
        center_darkness (float):
    """

    num_tokens: int
    image_width: int
    image_height: int
    image_components: int
    mean_darkness: float
    std_darkness: float
    top_darkness: float
    bottom_darkness: float
    left_darkness: float
    right_darkness: float
    center_darkness: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        num_tokens = self.num_tokens

        image_width = self.image_width

        image_height = self.image_height

        image_components = self.image_components

        mean_darkness = self.mean_darkness

        std_darkness = self.std_darkness

        top_darkness = self.top_darkness

        bottom_darkness = self.bottom_darkness

        left_darkness = self.left_darkness

        right_darkness = self.right_darkness

        center_darkness = self.center_darkness

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "num_tokens": num_tokens,
                "image_width": image_width,
                "image_height": image_height,
                "image_components": image_components,
                "mean_darkness": mean_darkness,
                "std_darkness": std_darkness,
                "top_darkness": top_darkness,
                "bottom_darkness": bottom_darkness,
                "left_darkness": left_darkness,
                "right_darkness": right_darkness,
                "center_darkness": center_darkness,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        num_tokens = d.pop("num_tokens")

        image_width = d.pop("image_width")

        image_height = d.pop("image_height")

        image_components = d.pop("image_components")

        mean_darkness = d.pop("mean_darkness")

        std_darkness = d.pop("std_darkness")

        top_darkness = d.pop("top_darkness")

        bottom_darkness = d.pop("bottom_darkness")

        left_darkness = d.pop("left_darkness")

        right_darkness = d.pop("right_darkness")

        center_darkness = d.pop("center_darkness")

        termite_document_classification_features = cls(
            num_tokens=num_tokens,
            image_width=image_width,
            image_height=image_height,
            image_components=image_components,
            mean_darkness=mean_darkness,
            std_darkness=std_darkness,
            top_darkness=top_darkness,
            bottom_darkness=bottom_darkness,
            left_darkness=left_darkness,
            right_darkness=right_darkness,
            center_darkness=center_darkness,
        )

        termite_document_classification_features.additional_properties = d
        return termite_document_classification_features

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
