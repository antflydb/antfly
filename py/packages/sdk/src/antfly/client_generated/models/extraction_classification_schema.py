from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionClassificationSchema")


@_attrs_define
class ExtractionClassificationSchema:
    """
    Attributes:
        name (str):
        labels (list[str]):
        multi_label (bool | Unset):  Default: False.
    """

    name: str
    labels: list[str]
    multi_label: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        labels = self.labels

        multi_label = self.multi_label

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "labels": labels,
            }
        )
        if multi_label is not UNSET:
            field_dict["multi_label"] = multi_label

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        labels = cast(list[str], d.pop("labels"))

        multi_label = d.pop("multi_label", UNSET)

        extraction_classification_schema = cls(
            name=name,
            labels=labels,
            multi_label=multi_label,
        )

        extraction_classification_schema.additional_properties = d
        return extraction_classification_schema

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
