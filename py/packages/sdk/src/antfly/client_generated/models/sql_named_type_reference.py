from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLNamedTypeReference")


@_attrs_define
class SQLNamedTypeReference:
    """Immutable named-base identity pinned by a domain definition.

    Attributes:
        id (int):
        origin_id (str):
        generation (int):
    """

    id: int
    origin_id: str
    generation: int

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        origin_id = self.origin_id

        generation = self.generation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "origin_id": origin_id,
                "generation": generation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        origin_id = d.pop("origin_id")

        generation = d.pop("generation")

        sql_named_type_reference = cls(
            id=id,
            origin_id=origin_id,
            generation=generation,
        )

        return sql_named_type_reference
