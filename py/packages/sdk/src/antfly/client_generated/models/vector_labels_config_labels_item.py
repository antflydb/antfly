from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="VectorLabelsConfigLabelsItem")


@_attrs_define
class VectorLabelsConfigLabelsItem:
    """
    Attributes:
        label (str):
        prompt (str | Unset):
    """

    label: str
    prompt: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        label = self.label

        prompt = self.prompt

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "label": label,
            }
        )
        if prompt is not UNSET:
            field_dict["prompt"] = prompt

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        label = d.pop("label")

        prompt = d.pop("prompt", UNSET)

        vector_labels_config_labels_item = cls(
            label=label,
            prompt=prompt,
        )

        return vector_labels_config_labels_item
