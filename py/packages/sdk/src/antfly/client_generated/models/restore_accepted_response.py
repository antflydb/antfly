from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_accepted_response_durability import RestoreAcceptedResponseDurability
from ..models.restore_accepted_response_restore import RestoreAcceptedResponseRestore
from ..types import UNSET, Unset

T = TypeVar("T", bound="RestoreAcceptedResponse")


@_attrs_define
class RestoreAcceptedResponse:
    """An accepted restore. `triggered` means asynchronous restoration has
    started. `committed` means the new generation is published but its
    durability barrier has not yet been confirmed.

        Attributes:
            restore (RestoreAcceptedResponseRestore):
            durability (RestoreAcceptedResponseDurability | Unset): Present only when `restore` is `committed`.
    """

    restore: RestoreAcceptedResponseRestore
    durability: RestoreAcceptedResponseDurability | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        restore = self.restore.value

        durability: str | Unset = UNSET
        if not isinstance(self.durability, Unset):
            durability = self.durability.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "restore": restore,
            }
        )
        if durability is not UNSET:
            field_dict["durability"] = durability

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        restore = RestoreAcceptedResponseRestore(d.pop("restore"))

        _durability = d.pop("durability", UNSET)
        durability: RestoreAcceptedResponseDurability | Unset
        if isinstance(_durability, Unset):
            durability = UNSET
        else:
            durability = RestoreAcceptedResponseDurability(_durability)

        restore_accepted_response = cls(
            restore=restore,
            durability=durability,
        )

        return restore_accepted_response
