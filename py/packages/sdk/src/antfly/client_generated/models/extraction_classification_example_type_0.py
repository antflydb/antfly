from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="ExtractionClassificationExampleType0")


@_attrs_define
class ExtractionClassificationExampleType0:
    """
    Attributes:
        input_ (str):
        label (str):
    """

    input_: str
    label: str

    def to_dict(self) -> dict[str, Any]:
        input_ = self.input_

        label = self.label

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "input": input_,
                "label": label,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        input_ = d.pop("input")

        label = d.pop("label")

        extraction_classification_example_type_0 = cls(
            input_=input_,
            label=label,
        )

        return extraction_classification_example_type_0
