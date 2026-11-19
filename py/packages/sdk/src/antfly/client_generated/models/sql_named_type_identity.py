from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLNamedTypeIdentity")


@_attrs_define
class SQLNamedTypeIdentity:
    """Server-resolved immutable SQL type identity. Origin survives restore remapping of local IDs and wire OIDs.

    Attributes:
        id (int):
        namespace_id (int):
        generation (int):
        origin_id (str): Opaque durable 128-bit origin, encoded as lowercase hexadecimal.
        oid (int):
        array_oid (int):
    """

    id: int
    namespace_id: int
    generation: int
    origin_id: str
    oid: int
    array_oid: int

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        namespace_id = self.namespace_id

        generation = self.generation

        origin_id = self.origin_id

        oid = self.oid

        array_oid = self.array_oid

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "namespace_id": namespace_id,
                "generation": generation,
                "origin_id": origin_id,
                "oid": oid,
                "array_oid": array_oid,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        namespace_id = d.pop("namespace_id")

        generation = d.pop("generation")

        origin_id = d.pop("origin_id")

        oid = d.pop("oid")

        array_oid = d.pop("array_oid")

        sql_named_type_identity = cls(
            id=id,
            namespace_id=namespace_id,
            generation=generation,
            origin_id=origin_id,
            oid=oid,
            array_oid=array_oid,
        )

        return sql_named_type_identity
