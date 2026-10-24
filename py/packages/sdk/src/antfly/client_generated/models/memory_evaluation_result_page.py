from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_evaluation_result_entry import MemoryEvaluationResultEntry


T = TypeVar("T", bound="MemoryEvaluationResultPage")


@_attrs_define
class MemoryEvaluationResultPage:
    """
    Attributes:
        items (list[MemoryEvaluationResultEntry]):
        next_cursor (None | str):
        complete (bool):
        run_fingerprint (str):
    """

    items: list[MemoryEvaluationResultEntry]
    next_cursor: None | str
    complete: bool
    run_fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        next_cursor: None | str
        next_cursor = self.next_cursor

        complete = self.complete

        run_fingerprint = self.run_fingerprint

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "items": items,
                "next_cursor": next_cursor,
                "complete": complete,
                "run_fingerprint": run_fingerprint,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_result_entry import MemoryEvaluationResultEntry

        d = dict(src_dict)
        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemoryEvaluationResultEntry.from_dict(items_item_data)

            items.append(items_item)

        def _parse_next_cursor(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor"))

        complete = d.pop("complete")

        run_fingerprint = d.pop("run_fingerprint")

        memory_evaluation_result_page = cls(
            items=items,
            next_cursor=next_cursor,
            complete=complete,
            run_fingerprint=run_fingerprint,
        )

        return memory_evaluation_result_page
