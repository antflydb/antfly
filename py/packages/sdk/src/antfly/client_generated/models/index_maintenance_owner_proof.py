from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="IndexMaintenanceOwnerProof")


@_attrs_define
class IndexMaintenanceOwnerProof:
    """
    Attributes:
        group_id (str):
        generation (str):
        slot (int):
        owner (str):
        comparison (str):
        progress_digest (str):
        maintenance_epoch (str):
    """

    group_id: str
    generation: str
    slot: int
    owner: str
    comparison: str
    progress_digest: str
    maintenance_epoch: str

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        generation = self.generation

        slot = self.slot

        owner = self.owner

        comparison = self.comparison

        progress_digest = self.progress_digest

        maintenance_epoch = self.maintenance_epoch

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "group_id": group_id,
                "generation": generation,
                "slot": slot,
                "owner": owner,
                "comparison": comparison,
                "progress_digest": progress_digest,
                "maintenance_epoch": maintenance_epoch,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        group_id = d.pop("group_id")

        generation = d.pop("generation")

        slot = d.pop("slot")

        owner = d.pop("owner")

        comparison = d.pop("comparison")

        progress_digest = d.pop("progress_digest")

        maintenance_epoch = d.pop("maintenance_epoch")

        index_maintenance_owner_proof = cls(
            group_id=group_id,
            generation=generation,
            slot=slot,
            owner=owner,
            comparison=comparison,
            progress_digest=progress_digest,
            maintenance_epoch=maintenance_epoch,
        )

        return index_maintenance_owner_proof
