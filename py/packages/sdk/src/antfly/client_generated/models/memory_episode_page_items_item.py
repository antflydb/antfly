from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_episode_page_items_item_turns_item import MemoryEpisodePageItemsItemTurnsItem


T = TypeVar("T", bound="MemoryEpisodePageItemsItem")


@_attrs_define
class MemoryEpisodePageItemsItem:
    """
    Attributes:
        thread_id (str):
        turns (list[MemoryEpisodePageItemsItemTurnsItem]):
        next_cursor (None | str):
        complete (bool):
        revision (int):
    """

    thread_id: str
    turns: list[MemoryEpisodePageItemsItemTurnsItem]
    next_cursor: None | str
    complete: bool
    revision: int

    def to_dict(self) -> dict[str, Any]:
        thread_id = self.thread_id

        turns = []
        for turns_item_data in self.turns:
            turns_item = turns_item_data.to_dict()
            turns.append(turns_item)

        next_cursor: None | str
        next_cursor = self.next_cursor

        complete = self.complete

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "thread_id": thread_id,
                "turns": turns,
                "next_cursor": next_cursor,
                "complete": complete,
                "revision": revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_episode_page_items_item_turns_item import MemoryEpisodePageItemsItemTurnsItem

        d = dict(src_dict)
        thread_id = d.pop("thread_id")

        turns = []
        _turns = d.pop("turns")
        for turns_item_data in _turns:
            turns_item = MemoryEpisodePageItemsItemTurnsItem.from_dict(turns_item_data)

            turns.append(turns_item)

        def _parse_next_cursor(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor"))

        complete = d.pop("complete")

        revision = d.pop("revision")

        memory_episode_page_items_item = cls(
            thread_id=thread_id,
            turns=turns,
            next_cursor=next_cursor,
            complete=complete,
            revision=revision,
        )

        return memory_episode_page_items_item
