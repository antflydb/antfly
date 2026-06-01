from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceChunk")


@_attrs_define
class InferenceChunk:
    """A chunk of content. Text chunks have mime_type text/plain.

    Attributes:
        id (int): Sequence number of the chunk (0, 1, 2, ...)
        mime_type (str): MIME type: text/plain, audio/wav, image/png, etc.
    """

    id: int
    mime_type: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        mime_type = self.mime_type

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "mime_type": mime_type,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        mime_type = d.pop("mime_type")

        inference_chunk = cls(
            id=id,
            mime_type=mime_type,
        )

        inference_chunk.additional_properties = d
        return inference_chunk

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
