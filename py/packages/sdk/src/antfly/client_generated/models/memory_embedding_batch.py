from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_embedding_batch_items_item import MemoryEmbeddingBatchItemsItem
    from ..models.memory_embedding_batch_model import MemoryEmbeddingBatchModel


T = TypeVar("T", bound="MemoryEmbeddingBatch")


@_attrs_define
class MemoryEmbeddingBatch:
    """
    Attributes:
        generation (int):
        model (MemoryEmbeddingBatchModel):
        items (list[MemoryEmbeddingBatchItemsItem]):
    """

    generation: int
    model: MemoryEmbeddingBatchModel
    items: list[MemoryEmbeddingBatchItemsItem]

    def to_dict(self) -> dict[str, Any]:
        generation = self.generation

        model = self.model.to_dict()

        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "generation": generation,
                "model": model,
                "items": items,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_embedding_batch_items_item import MemoryEmbeddingBatchItemsItem
        from ..models.memory_embedding_batch_model import MemoryEmbeddingBatchModel

        d = dict(src_dict)
        generation = d.pop("generation")

        model = MemoryEmbeddingBatchModel.from_dict(d.pop("model"))

        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemoryEmbeddingBatchItemsItem.from_dict(items_item_data)

            items.append(items_item)

        memory_embedding_batch = cls(
            generation=generation,
            model=model,
            items=items,
        )

        return memory_embedding_batch
