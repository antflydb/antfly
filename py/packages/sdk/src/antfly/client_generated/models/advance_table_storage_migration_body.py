from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.advance_table_storage_migration_body_action import AdvanceTableStorageMigrationBodyAction

T = TypeVar("T", bound="AdvanceTableStorageMigrationBody")


@_attrs_define
class AdvanceTableStorageMigrationBody:
    """
    Attributes:
        action (AdvanceTableStorageMigrationBodyAction):
    """

    action: AdvanceTableStorageMigrationBodyAction

    def to_dict(self) -> dict[str, Any]:
        action = self.action.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "action": action,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        action = AdvanceTableStorageMigrationBodyAction(d.pop("action"))

        advance_table_storage_migration_body = cls(
            action=action,
        )

        return advance_table_storage_migration_body
