from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="ReadReceipt")


@_attrs_define
class ReadReceipt:
    """Opaque write receipt returned by ingestion. Pass it back unchanged to require a read including that write; it is a
    minimum freshness frontier, not an exact snapshot.

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

        read_receipt = cls(
            table_id=table_id,
            object_generation=object_generation,
            wal_lsn=wal_lsn,
        )

        return read_receipt
