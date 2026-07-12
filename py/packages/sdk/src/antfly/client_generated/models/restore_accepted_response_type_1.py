from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_accepted_response_type_1_durability import RestoreAcceptedResponseType1Durability
from ..models.restore_accepted_response_type_1_restore import RestoreAcceptedResponseType1Restore

T = TypeVar("T", bound="RestoreAcceptedResponseType1")


@_attrs_define
class RestoreAcceptedResponseType1:
    """
    Attributes:
        restore (RestoreAcceptedResponseType1Restore):
        durability (RestoreAcceptedResponseType1Durability):
    """

    restore: RestoreAcceptedResponseType1Restore
    durability: RestoreAcceptedResponseType1Durability

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
        restore = RestoreAcceptedResponseType1Restore(d.pop("restore"))

        durability = RestoreAcceptedResponseType1Durability(d.pop("durability"))

        restore_accepted_response_type_1 = cls(
            restore=restore,
            durability=durability,
        )

        return restore_accepted_response_type_1
