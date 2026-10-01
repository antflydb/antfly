from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="CreateTableStorageMigrationBodyBudget")


@_attrs_define
class CreateTableStorageMigrationBodyBudget:
    """
    Attributes:
        batch_bytes (int | Unset):  Default: 4194304.
        batch_rows (int | Unset):  Default: 1024.
        temporary_bytes (int | Unset):  Default: 68719476736.
        disk_reserve_bytes (int | Unset):  Default: 1073741824.
    """

    batch_bytes: int | Unset = 4194304
    batch_rows: int | Unset = 1024
    temporary_bytes: int | Unset = 68719476736
    disk_reserve_bytes: int | Unset = 1073741824

    def to_dict(self) -> dict[str, Any]:
        batch_bytes = self.batch_bytes

        batch_rows = self.batch_rows

        temporary_bytes = self.temporary_bytes

        disk_reserve_bytes = self.disk_reserve_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if batch_bytes is not UNSET:
            field_dict["batch_bytes"] = batch_bytes
        if batch_rows is not UNSET:
            field_dict["batch_rows"] = batch_rows
        if temporary_bytes is not UNSET:
            field_dict["temporary_bytes"] = temporary_bytes
        if disk_reserve_bytes is not UNSET:
            field_dict["disk_reserve_bytes"] = disk_reserve_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        batch_bytes = d.pop("batch_bytes", UNSET)

        batch_rows = d.pop("batch_rows", UNSET)

        temporary_bytes = d.pop("temporary_bytes", UNSET)

        disk_reserve_bytes = d.pop("disk_reserve_bytes", UNSET)

        create_table_storage_migration_body_budget = cls(
            batch_bytes=batch_bytes,
            batch_rows=batch_rows,
            temporary_bytes=temporary_bytes,
            disk_reserve_bytes=disk_reserve_bytes,
        )

        return create_table_storage_migration_body_budget
