from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_entity_page_items_item import MemoryEntityPageItemsItem


T = TypeVar("T", bound="MemoryEntityPage")


@_attrs_define
class MemoryEntityPage:
    """
    Attributes:
        items (list[MemoryEntityPageItemsItem]):
        next_cursor (None | str):
        complete (bool):
    """

    items: list[MemoryEntityPageItemsItem]
    next_cursor: None | str
    complete: bool

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        next_cursor: None | str
        next_cursor = self.next_cursor

        complete = self.complete

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "items": items,
                "next_cursor": next_cursor,
                "complete": complete,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_page_items_item import MemoryEntityPageItemsItem

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemoryEntityPageItemsItem.from_dict(items_item_data)

            items.append(items_item)

        def _parse_next_cursor(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor"))

        complete = d.pop("complete")

        memory_entity_page = cls(
            items=items,
            next_cursor=next_cursor,
            complete=complete,
        )

        return memory_entity_page
