from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.entity_namespace_outcome_absorbed_item_disposition import EntityNamespaceOutcomeAbsorbedItemDisposition

T = TypeVar("T", bound="EntityNamespaceOutcomeAbsorbedItem")


@_attrs_define
class EntityNamespaceOutcomeAbsorbedItem:
    """
    Attributes:
        key (str):
        disposition (EntityNamespaceOutcomeAbsorbedItemDisposition):
    """

    key: str
    disposition: EntityNamespaceOutcomeAbsorbedItemDisposition
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        disposition = self.disposition.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "disposition": disposition,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        disposition = EntityNamespaceOutcomeAbsorbedItemDisposition(d.pop("disposition"))

        entity_namespace_outcome_absorbed_item = cls(
            key=key,
            disposition=disposition,
        )

        entity_namespace_outcome_absorbed_item.additional_properties = d
        return entity_namespace_outcome_absorbed_item

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
