from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_embedding_initialization_error_event_object import ChunkEmbeddingInitializationErrorEventObject

T = TypeVar("T", bound="ChunkEmbeddingInitializationErrorEvent")


@_attrs_define
class ChunkEmbeddingInitializationErrorEvent:
    """
    Attributes:
        object_ (ChunkEmbeddingInitializationErrorEventObject):
        code (str):
    """

    object_: ChunkEmbeddingInitializationErrorEventObject
    code: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        code = self.code

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "code": code,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        object_ = ChunkEmbeddingInitializationErrorEventObject(d.pop("object"))

        code = d.pop("code")

        chunk_embedding_initialization_error_event = cls(
            object_=object_,
            code=code,
        )

        chunk_embedding_initialization_error_event.additional_properties = d
        return chunk_embedding_initialization_error_event

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
