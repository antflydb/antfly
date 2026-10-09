from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="LakeReadReceipt")


@_attrs_define
class LakeReadReceipt:
    """
    Attributes:
        table_id (int):
        object_generation (int):
        wal_lsn (int):
    """

    table_id: int
    object_generation: int
    wal_lsn: int

    def to_dict(self) -> dict[str, Any]:
        table_id = self.table_id

        object_generation = self.object_generation

        wal_lsn = self.wal_lsn

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table_id": table_id,
                "object_generation": object_generation,
                "wal_lsn": wal_lsn,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table_id = d.pop("table_id")

        object_generation = d.pop("object_generation")

        wal_lsn = d.pop("wal_lsn")

        lake_read_receipt = cls(
            table_id=table_id,
            object_generation=object_generation,
            wal_lsn=wal_lsn,
        )

        return lake_read_receipt
