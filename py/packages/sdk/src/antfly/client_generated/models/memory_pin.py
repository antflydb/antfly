from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryPin")


@_attrs_define
class MemoryPin:
    """
    Attributes:
        id (str):
        pinned (bool | Unset):  Default: True.
        reason (str | Unset):
    """

    id: str
    pinned: bool | Unset = True
    reason: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        pinned = self.pinned

        reason = self.reason

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
            }
        )
        if pinned is not UNSET:
            field_dict["pinned"] = pinned
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        pinned = d.pop("pinned", UNSET)

        reason = d.pop("reason", UNSET)

        memory_pin = cls(
            id=id,
            pinned=pinned,
            reason=reason,
        )

        return memory_pin
