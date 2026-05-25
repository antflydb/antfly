from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_chunk_object_object import TermiteChunkObjectObject

T = TypeVar("T", bound="TermiteChunkObject")


@_attrs_define
class TermiteChunkObject:
    """A chunk result object. Text chunks have mime_type text/plain.

    Attributes:
        id (int): Sequence number of the chunk (0, 1, 2, ...)
        mime_type (str): MIME type: text/plain, audio/wav, image/png, etc.
        object_ (TermiteChunkObjectObject):
        index (int): Position of this chunk object in the response data array.
    """

    id: int
    mime_type: str
    object_: TermiteChunkObjectObject
    index: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        mime_type = self.mime_type

        object_ = self.object_.value

        index = self.index

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "mime_type": mime_type,
                "object": object_,
                "index": index,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        mime_type = d.pop("mime_type")

        object_ = TermiteChunkObjectObject(d.pop("object"))

        index = d.pop("index")

        termite_chunk_object = cls(
            id=id,
            mime_type=mime_type,
            object_=object_,
            index=index,
        )

        termite_chunk_object.additional_properties = d
        return termite_chunk_object

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
