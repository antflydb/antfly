from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_table_response_202_type_1_durability import RestoreTableResponse202Type1Durability
from ..models.restore_table_response_202_type_1_restore import RestoreTableResponse202Type1Restore

T = TypeVar("T", bound="RestoreTableResponse202Type1")


@_attrs_define
class RestoreTableResponse202Type1:
    """
    Attributes:
        restore (RestoreTableResponse202Type1Restore):
        durability (RestoreTableResponse202Type1Durability):
    """

    restore: RestoreTableResponse202Type1Restore
    durability: RestoreTableResponse202Type1Durability

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
        restore = RestoreTableResponse202Type1Restore(d.pop("restore"))

        durability = RestoreTableResponse202Type1Durability(d.pop("durability"))

        restore_table_response_202_type_1 = cls(
            restore=restore,
            durability=durability,
        )

        return restore_table_response_202_type_1
