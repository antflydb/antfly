from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.chunk_coordinates import ChunkCoordinates


T = TypeVar("T", bound="ChunkDescriptor")


@_attrs_define
class ChunkDescriptor:
    """
    Attributes:
        id (str): Opaque source-plan-bound inference identity; not a database artifact ID.
        ordinal (int):
        coordinates (ChunkCoordinates): Original-source coordinates; time intervals are half-open integer nanoseconds
            and text spans are UTF-8 byte offsets.
        input_tokens (int):
    """

    id: str
    ordinal: int
    coordinates: ChunkCoordinates
    input_tokens: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        ordinal = self.ordinal

        coordinates = self.coordinates.to_dict()

        input_tokens = self.input_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "ordinal": ordinal,
                "coordinates": coordinates,
                "input_tokens": input_tokens,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_coordinates import ChunkCoordinates

        d = dict(src_dict)
        id = d.pop("id")

        ordinal = d.pop("ordinal")

        coordinates = ChunkCoordinates.from_dict(d.pop("coordinates"))

        input_tokens = d.pop("input_tokens")

        chunk_descriptor = cls(
            id=id,
            ordinal=ordinal,
            coordinates=coordinates,
            input_tokens=input_tokens,
        )

        chunk_descriptor.additional_properties = d
        return chunk_descriptor

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
