from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_committed_durable_response_durability import RestoreCommittedDurableResponseDurability
from ..models.restore_committed_durable_response_restore import RestoreCommittedDurableResponseRestore

T = TypeVar("T", bound="RestoreCommittedDurableResponse")


@_attrs_define
class RestoreCommittedDurableResponse:
    """
    Attributes:
        restore (RestoreCommittedDurableResponseRestore):
        durability (RestoreCommittedDurableResponseDurability):
    """

    restore: RestoreCommittedDurableResponseRestore
    durability: RestoreCommittedDurableResponseDurability

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
        restore = RestoreCommittedDurableResponseRestore(d.pop("restore"))

        durability = RestoreCommittedDurableResponseDurability(d.pop("durability"))

        restore_committed_durable_response = cls(
            restore=restore,
            durability=durability,
        )

        return restore_committed_durable_response
