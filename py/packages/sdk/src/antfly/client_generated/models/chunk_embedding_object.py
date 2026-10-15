from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunk_descriptor import ChunkDescriptor
    from ..models.chunk_embedding_error import ChunkEmbeddingError


T = TypeVar("T", bound="ChunkEmbeddingObject")


@_attrs_define
class ChunkEmbeddingObject:
    """
    Attributes:
        descriptor (ChunkDescriptor):
        mime_type (str):
        embedding (list[float] | Unset):
        error (ChunkEmbeddingError | Unset):
        text (str | Unset):
        data (str | Unset): Optional base64 payload; source coordinates remain mandatory.
    """

    descriptor: ChunkDescriptor
    mime_type: str
    embedding: list[float] | Unset = UNSET
    error: ChunkEmbeddingError | Unset = UNSET
    text: str | Unset = UNSET
    data: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        descriptor = self.descriptor.to_dict()

        mime_type = self.mime_type

        embedding: list[float] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding

        error: dict[str, Any] | Unset = UNSET
        if not isinstance(self.error, Unset):
            error = self.error.to_dict()

        text = self.text

        data = self.data

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "descriptor": descriptor,
                "mime_type": mime_type,
            }
        )
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if error is not UNSET:
            field_dict["error"] = error
        if text is not UNSET:
            field_dict["text"] = text
        if data is not UNSET:
            field_dict["data"] = data

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_descriptor import ChunkDescriptor
        from ..models.chunk_embedding_error import ChunkEmbeddingError

        d = dict(src_dict)
        descriptor = ChunkDescriptor.from_dict(d.pop("descriptor"))

        mime_type = d.pop("mime_type")

        embedding = cast(list[float], d.pop("embedding", UNSET))

        _error = d.pop("error", UNSET)
        error: ChunkEmbeddingError | Unset
        if isinstance(_error, Unset):
            error = UNSET
        else:
            error = ChunkEmbeddingError.from_dict(_error)

        text = d.pop("text", UNSET)

        data = d.pop("data", UNSET)

        chunk_embedding_object = cls(
            descriptor=descriptor,
            mime_type=mime_type,
            embedding=embedding,
            error=error,
            text=text,
            data=data,
        )

        chunk_embedding_object.additional_properties = d
        return chunk_embedding_object

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
