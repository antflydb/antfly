from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_generate_chunk_object import TermiteGenerateChunkObject

if TYPE_CHECKING:
    from ..models.termite_generate_chunk_choice import TermiteGenerateChunkChoice


T = TypeVar("T", bound="TermiteGenerateChunk")


@_attrs_define
class TermiteGenerateChunk:
    """Streaming generation chunk (SSE event data)

    Attributes:
        id (str):
        object_ (TermiteGenerateChunkObject):
        created (int):
        model (str):
        choices (list[TermiteGenerateChunkChoice]):
    """

    id: str
    object_: TermiteGenerateChunkObject
    created: int
    model: str
    choices: list[TermiteGenerateChunkChoice]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        object_ = self.object_.value

        created = self.created

        model = self.model

        choices = []
        for choices_item_data in self.choices:
            choices_item = choices_item_data.to_dict()
            choices.append(choices_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "object": object_,
                "created": created,
                "model": model,
                "choices": choices,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_generate_chunk_choice import TermiteGenerateChunkChoice

        d = dict(src_dict)
        id = d.pop("id")

        object_ = TermiteGenerateChunkObject(d.pop("object"))

        created = d.pop("created")

        model = d.pop("model")

        choices = []
        _choices = d.pop("choices")
        for choices_item_data in _choices:
            choices_item = TermiteGenerateChunkChoice.from_dict(choices_item_data)

            choices.append(choices_item)

        termite_generate_chunk = cls(
            id=id,
            object_=object_,
            created=created,
            model=model,
            choices=choices,
        )

        termite_generate_chunk.additional_properties = d
        return termite_generate_chunk

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
