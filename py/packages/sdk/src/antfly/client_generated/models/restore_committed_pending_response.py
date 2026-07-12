from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_committed_pending_response_durability import RestoreCommittedPendingResponseDurability
from ..models.restore_committed_pending_response_restore import RestoreCommittedPendingResponseRestore

T = TypeVar("T", bound="RestoreCommittedPendingResponse")


@_attrs_define
class RestoreCommittedPendingResponse:
    """The restored generation is published but its durability barrier remains pending.

    Attributes:
        restore (RestoreCommittedPendingResponseRestore):
        durability (RestoreCommittedPendingResponseDurability):
    """

    restore: RestoreCommittedPendingResponseRestore
    durability: RestoreCommittedPendingResponseDurability

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
        restore = RestoreCommittedPendingResponseRestore(d.pop("restore"))

        durability = RestoreCommittedPendingResponseDurability(d.pop("durability"))

        restore_committed_pending_response = cls(
            restore=restore,
            durability=durability,
        )

        return restore_committed_pending_response
