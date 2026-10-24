from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_recall_result_items_item_citations_item import MemoryRecallResultItemsItemCitationsItem
    from ..models.memory_recall_result_items_item_memory import MemoryRecallResultItemsItemMemory


T = TypeVar("T", bound="MemoryRecallResultItemsItem")


@_attrs_define
class MemoryRecallResultItemsItem:
    """
    Attributes:
        memory (MemoryRecallResultItemsItemMemory):
        score (float):
        historical (bool):
        citations (list[MemoryRecallResultItemsItemCitationsItem]):
    """

    memory: MemoryRecallResultItemsItemMemory
    score: float
    historical: bool
    citations: list[MemoryRecallResultItemsItemCitationsItem]

    def to_dict(self) -> dict[str, Any]:
        memory = self.memory.to_dict()

        score = self.score

        historical = self.historical

        citations = []
        for citations_item_data in self.citations:
            citations_item = citations_item_data.to_dict()
            citations.append(citations_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "memory": memory,
                "score": score,
                "historical": historical,
                "citations": citations,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_recall_result_items_item_citations_item import MemoryRecallResultItemsItemCitationsItem
        from ..models.memory_recall_result_items_item_memory import MemoryRecallResultItemsItemMemory

        d = dict(src_dict)
        memory = MemoryRecallResultItemsItemMemory.from_dict(d.pop("memory"))

        score = d.pop("score")

        historical = d.pop("historical")

        citations = []
        _citations = d.pop("citations")
        for citations_item_data in _citations:
            citations_item = MemoryRecallResultItemsItemCitationsItem.from_dict(citations_item_data)

            citations.append(citations_item)

        memory_recall_result_items_item = cls(
            memory=memory,
            score=score,
            historical=historical,
            citations=citations,
        )

        return memory_recall_result_items_item
