from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SparseChunkVectorSeed")


@_attrs_define
class SparseChunkVectorSeed:
    """One seeded sparse vector of a chunk. Sparse chunk vectors are never
    windowed. Vectors of a document extraction's unit chunks name their
    unit with `parent_unit_id` (every entry) and are accepted with the
    extraction seed in the same write; plain chunk vectors must not name
    one. Every entry must name a chunk of this revision; chunks without a
    seeded vector are embedded as usual.

        Attributes:
            chunk_id (int):
            indices (list[int]):
            values (list[float]):
            parent_unit_id (str | Unset):
    """

    chunk_id: int
    indices: list[int]
    values: list[float]
    parent_unit_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        chunk_id = self.chunk_id

        indices = self.indices

        values = self.values

        parent_unit_id = self.parent_unit_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "chunk_id": chunk_id,
                "indices": indices,
                "values": values,
            }
        )
        if parent_unit_id is not UNSET:
            field_dict["parent_unit_id"] = parent_unit_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        chunk_id = d.pop("chunk_id")

        indices = cast(list[int], d.pop("indices"))

        values = cast(list[float], d.pop("values"))

        parent_unit_id = d.pop("parent_unit_id", UNSET)

        sparse_chunk_vector_seed = cls(
            chunk_id=chunk_id,
            indices=indices,
            values=values,
            parent_unit_id=parent_unit_id,
        )

        return sparse_chunk_vector_seed
