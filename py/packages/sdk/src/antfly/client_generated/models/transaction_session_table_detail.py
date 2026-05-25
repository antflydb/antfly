from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TransactionSessionTableDetail")


@_attrs_define
class TransactionSessionTableDetail:
    """
    Attributes:
        table (str | Unset):
        staged_read_count (int | Unset):
        staged_write_count (int | Unset):
        staged_delete_count (int | Unset):
        staged_predicate_count (int | Unset):
    """

    table: str | Unset = UNSET
    staged_read_count: int | Unset = UNSET
    staged_write_count: int | Unset = UNSET
    staged_delete_count: int | Unset = UNSET
    staged_predicate_count: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        staged_read_count = self.staged_read_count

        staged_write_count = self.staged_write_count

        staged_delete_count = self.staged_delete_count

        staged_predicate_count = self.staged_predicate_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if staged_read_count is not UNSET:
            field_dict["staged_read_count"] = staged_read_count
        if staged_write_count is not UNSET:
            field_dict["staged_write_count"] = staged_write_count
        if staged_delete_count is not UNSET:
            field_dict["staged_delete_count"] = staged_delete_count
        if staged_predicate_count is not UNSET:
            field_dict["staged_predicate_count"] = staged_predicate_count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table", UNSET)

        staged_read_count = d.pop("staged_read_count", UNSET)

        staged_write_count = d.pop("staged_write_count", UNSET)

        staged_delete_count = d.pop("staged_delete_count", UNSET)

        staged_predicate_count = d.pop("staged_predicate_count", UNSET)

        transaction_session_table_detail = cls(
            table=table,
            staged_read_count=staged_read_count,
            staged_write_count=staged_write_count,
            staged_delete_count=staged_delete_count,
            staged_predicate_count=staged_predicate_count,
        )

        transaction_session_table_detail.additional_properties = d
        return transaction_session_table_detail

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
