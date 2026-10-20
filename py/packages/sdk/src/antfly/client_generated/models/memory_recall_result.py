from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_recall_result_retrieval import MemoryRecallResultRetrieval

if TYPE_CHECKING:
    from ..models.memory_recall_result_items_item import MemoryRecallResultItemsItem


T = TypeVar("T", bound="MemoryRecallResult")


@_attrs_define
class MemoryRecallResult:
    """
    Attributes:
        items (list[MemoryRecallResultItemsItem]):
        next_cursor (None | str):
        complete (bool):
        truncated (bool):
        scanned (int):
        omitted (int):
        token_upper_bound (int):
        revision (int):
        config_generation (int):
        retrieval (MemoryRecallResultRetrieval):
    """

    items: list[MemoryRecallResultItemsItem]
    next_cursor: None | str
    complete: bool
    truncated: bool
    scanned: int
    omitted: int
    token_upper_bound: int
    revision: int
    config_generation: int
    retrieval: MemoryRecallResultRetrieval

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        next_cursor: None | str
        next_cursor = self.next_cursor

        complete = self.complete

        truncated = self.truncated

        scanned = self.scanned

        omitted = self.omitted

        token_upper_bound = self.token_upper_bound

        revision = self.revision

        config_generation = self.config_generation

        retrieval = self.retrieval.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "items": items,
                "next_cursor": next_cursor,
                "complete": complete,
                "truncated": truncated,
                "scanned": scanned,
                "omitted": omitted,
                "token_upper_bound": token_upper_bound,
                "revision": revision,
                "config_generation": config_generation,
                "retrieval": retrieval,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_recall_result_items_item import MemoryRecallResultItemsItem

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemoryRecallResultItemsItem.from_dict(items_item_data)

            items.append(items_item)

        def _parse_next_cursor(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor"))

        complete = d.pop("complete")

        truncated = d.pop("truncated")

        scanned = d.pop("scanned")

        omitted = d.pop("omitted")

        token_upper_bound = d.pop("token_upper_bound")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        retrieval = MemoryRecallResultRetrieval(d.pop("retrieval"))

        memory_recall_result = cls(
            items=items,
            next_cursor=next_cursor,
            complete=complete,
            truncated=truncated,
            scanned=scanned,
            omitted=omitted,
            token_upper_bound=token_upper_bound,
            revision=revision,
            config_generation=config_generation,
            retrieval=retrieval,
        )

        return memory_recall_result
