from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_space_change import MemorySpaceChange


T = TypeVar("T", bound="MemoryChangePage")


@_attrs_define
class MemoryChangePage:
    """
    Attributes:
        items (list[MemorySpaceChange]):
        next_cursor (str):
        caught_up (bool):
        revision (int):
        retained_from (str):
        retained_after (int):
    """

    items: list[MemorySpaceChange]
    next_cursor: str
    caught_up: bool
    revision: int
    retained_from: str
    retained_after: int

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        next_cursor = self.next_cursor

        caught_up = self.caught_up

        revision = self.revision

        retained_from = self.retained_from

        retained_after = self.retained_after

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "items": items,
                "next_cursor": next_cursor,
                "caught_up": caught_up,
                "revision": revision,
                "retained_from": retained_from,
                "retained_after": retained_after,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_space_change import MemorySpaceChange

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemorySpaceChange.from_dict(items_item_data)

            items.append(items_item)

        next_cursor = d.pop("next_cursor")

        caught_up = d.pop("caught_up")

        revision = d.pop("revision")

        retained_from = d.pop("retained_from")

        retained_after = d.pop("retained_after")

        memory_change_page = cls(
            items=items,
            next_cursor=next_cursor,
            caught_up=caught_up,
            revision=revision,
            retained_from=retained_from,
            retained_after=retained_after,
        )

        return memory_change_page
