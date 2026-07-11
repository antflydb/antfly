from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_table_response_200_durability import RestoreTableResponse200Durability
from ..models.restore_table_response_200_restore import RestoreTableResponse200Restore

T = TypeVar("T", bound="RestoreTableResponse200")


@_attrs_define
class RestoreTableResponse200:
    """
    Attributes:
        restore (RestoreTableResponse200Restore):
        durability (RestoreTableResponse200Durability):
    """

    restore: RestoreTableResponse200Restore
    durability: RestoreTableResponse200Durability

    def to_dict(self) -> dict[str, Any]:
        restore = self.restore.value

        durability = self.durability.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "restore": restore,
                "durability": durability,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        restore = RestoreTableResponse200Restore(d.pop("restore"))

        durability = RestoreTableResponse200Durability(d.pop("durability"))

        restore_table_response_200 = cls(
            restore=restore,
            durability=durability,
        )

        return restore_table_response_200
