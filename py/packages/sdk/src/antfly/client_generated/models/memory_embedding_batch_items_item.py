from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryEmbeddingBatchItemsItem")


@_attrs_define
class MemoryEmbeddingBatchItemsItem:
    """
    Attributes:
        id (str):
        embedding (list[float]):
    """

    id: str
    embedding: list[float]

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        embedding = self.embedding

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "embedding": embedding,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        embedding = cast(list[float], d.pop("embedding"))

        memory_embedding_batch_items_item = cls(
            id=id,
            embedding=embedding,
        )

        return memory_embedding_batch_items_item
