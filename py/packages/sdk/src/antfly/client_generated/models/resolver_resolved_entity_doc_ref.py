from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResolverResolvedEntityDocRef")


@_attrs_define
class ResolverResolvedEntityDocRef:
    """
    Attributes:
        table (str):
        key (str):
        storage_table (str | Unset):
    """

    table: str
    key: str
    storage_table: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key = self.key

        storage_table = self.storage_table

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "key": key,
            }
        )
        if storage_table is not UNSET:
            field_dict["storage_table"] = storage_table

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        key = d.pop("key")

        storage_table = d.pop("storage_table", UNSET)

        resolver_resolved_entity_doc_ref = cls(
            table=table,
            key=key,
            storage_table=storage_table,
        )

        resolver_resolved_entity_doc_ref.additional_properties = d
        return resolver_resolved_entity_doc_ref

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
