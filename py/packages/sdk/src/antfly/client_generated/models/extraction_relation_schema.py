from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionRelationSchema")


@_attrs_define
class ExtractionRelationSchema:
    """Optional source and target labels constrain relation endpoints. A target requires a source.

    Attributes:
        type_ (str):
        source (str | Unset):
        target (str | Unset):
        description (str | Unset): Version 2 model-facing relation description.
        threshold (float | Unset):
    """

    type_: str
    source: str | Unset = UNSET
    target: str | Unset = UNSET
    description: str | Unset = UNSET
    threshold: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_

        source = self.source

        target = self.target

        description = self.description

        threshold = self.threshold

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if source is not UNSET:
            field_dict["source"] = source
        if target is not UNSET:
            field_dict["target"] = target
        if description is not UNSET:
            field_dict["description"] = description
        if threshold is not UNSET:
            field_dict["threshold"] = threshold

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = d.pop("type")

        source = d.pop("source", UNSET)

        target = d.pop("target", UNSET)

        description = d.pop("description", UNSET)

        threshold = d.pop("threshold", UNSET)

        extraction_relation_schema = cls(
            type_=type_,
            source=source,
            target=target,
            description=description,
            threshold=threshold,
        )

        extraction_relation_schema.additional_properties = d
        return extraction_relation_schema

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
