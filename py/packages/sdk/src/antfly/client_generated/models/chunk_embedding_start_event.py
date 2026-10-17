from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_embedding_start_event_object import ChunkEmbeddingStartEventObject

if TYPE_CHECKING:
    from ..models.chunk_embedding_context import ChunkEmbeddingContext


T = TypeVar("T", bound="ChunkEmbeddingStartEvent")


@_attrs_define
class ChunkEmbeddingStartEvent:
    """
    Attributes:
        object_ (ChunkEmbeddingStartEventObject):
        context (ChunkEmbeddingContext):
    """

    object_: ChunkEmbeddingStartEventObject
    context: ChunkEmbeddingContext
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        context = self.context.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "context": context,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_embedding_context import ChunkEmbeddingContext

        d = dict(src_dict)
        object_ = ChunkEmbeddingStartEventObject(d.pop("object"))

        context = ChunkEmbeddingContext.from_dict(d.pop("context"))

        chunk_embedding_start_event = cls(
            object_=object_,
            context=context,
        )

        chunk_embedding_start_event.additional_properties = d
        return chunk_embedding_start_event

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
