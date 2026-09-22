from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.index_maintenance_owner_proof import IndexMaintenanceOwnerProof


T = TypeVar("T", bound="IndexMaintenanceRequest")


@_attrs_define
class IndexMaintenanceRequest:
    """Exact observations from index status. Each selected owner is admitted atomically through the replicated transaction
    journal; the selection is not one global transaction. Cancellation, conflicts, or a lost acknowledgement may leave
    some owners admitted. Resubmit the identical request to resume safely; do not replace its observations with newer
    progress unless starting a new maintenance attempt.

        Attributes:
            table_id (str):
            schema_version (int):
            owners (list[IndexMaintenanceOwnerProof]):
    """

    table_id: str
    schema_version: int
    owners: list[IndexMaintenanceOwnerProof]

    def to_dict(self) -> dict[str, Any]:
        table_id = self.table_id

        schema_version = self.schema_version

        owners = []
        for owners_item_data in self.owners:
            owners_item = owners_item_data.to_dict()
            owners.append(owners_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table_id": table_id,
                "schema_version": schema_version,
                "owners": owners,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.index_maintenance_owner_proof import IndexMaintenanceOwnerProof

        d = dict(src_dict)
        table_id = d.pop("table_id")

        schema_version = d.pop("schema_version")

        owners = []
        _owners = d.pop("owners")
        for owners_item_data in _owners:
            owners_item = IndexMaintenanceOwnerProof.from_dict(owners_item_data)

            owners.append(owners_item)

        index_maintenance_request = cls(
            table_id=table_id,
            schema_version=schema_version,
            owners=owners,
        )

        return index_maintenance_request
