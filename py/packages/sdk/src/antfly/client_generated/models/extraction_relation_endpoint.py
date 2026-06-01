from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionRelationEndpoint")


@_attrs_define
class ExtractionRelationEndpoint:
    """
    Attributes:
        entity_index (int | Unset):
        id (str | Unset):
    """

    entity_index: int | Unset = UNSET
    id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        entity_index = self.entity_index

        id = self.id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if entity_index is not UNSET:
            field_dict["entity_index"] = entity_index
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        entity_index = d.pop("entity_index", UNSET)

        id = d.pop("id", UNSET)

        extraction_relation_endpoint = cls(
            entity_index=entity_index,
            id=id,
        )

        extraction_relation_endpoint.additional_properties = d
        return extraction_relation_endpoint

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
