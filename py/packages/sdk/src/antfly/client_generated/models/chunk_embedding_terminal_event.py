from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_embedding_terminal_event_object import ChunkEmbeddingTerminalEventObject

if TYPE_CHECKING:
    from ..models.chunk_embedding_response import ChunkEmbeddingResponse


T = TypeVar("T", bound="ChunkEmbeddingTerminalEvent")


@_attrs_define
class ChunkEmbeddingTerminalEvent:
    """
    Attributes:
        object_ (ChunkEmbeddingTerminalEventObject):
        response (ChunkEmbeddingResponse):
    """

    object_: ChunkEmbeddingTerminalEventObject
    response: ChunkEmbeddingResponse
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        response = self.response.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "response": response,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_embedding_response import ChunkEmbeddingResponse

        d = dict(src_dict)
        object_ = ChunkEmbeddingTerminalEventObject(d.pop("object"))

        response = ChunkEmbeddingResponse.from_dict(d.pop("response"))

        chunk_embedding_terminal_event = cls(
            object_=object_,
            response=response,
        )

        chunk_embedding_terminal_event.additional_properties = d
        return chunk_embedding_terminal_event

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
