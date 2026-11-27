from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkVectorSeed")


@_attrs_define
class ChunkVectorSeed:
    """One seeded vector of a chunk-derived embedding. An entry without
    `window_index` is the chunk's only vector, stored unsplit (window 0).
    If any entry for a chunk has a `window_index`, that chunk's windows
    must be supplied contiguously from 0. Chunks without a seeded vector
    are embedded as usual.

    Vectors of a document extraction's unit chunks name their unit with
    `unit_id` or its alias `parent_unit_id` (every entry, no
    `window_index`: unit chunks are embedded whole) and are accepted with the extraction seed in the
    same write; each must name a text chunk of an extracted unit.

        Attributes:
            chunk_id (int):
            vector (list[float]):
            unit_id (str | Unset): The extraction unit of the chunk, e.g. `page:000003`.
            parent_unit_id (str | Unset): Alias of `unit_id`; both may appear only when equal.
            window_index (int | Unset):
    """

    chunk_id: int
    vector: list[float]
    unit_id: str | Unset = UNSET
    parent_unit_id: str | Unset = UNSET
    window_index: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        chunk_id = self.chunk_id

        vector = self.vector

        unit_id = self.unit_id

        parent_unit_id = self.parent_unit_id

        window_index = self.window_index

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "chunk_id": chunk_id,
                "vector": vector,
            }
        )
        if unit_id is not UNSET:
            field_dict["unit_id"] = unit_id
        if parent_unit_id is not UNSET:
            field_dict["parent_unit_id"] = parent_unit_id
        if window_index is not UNSET:
            field_dict["window_index"] = window_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        chunk_id = d.pop("chunk_id")

        vector = cast(list[float], d.pop("vector"))

        unit_id = d.pop("unit_id", UNSET)

        parent_unit_id = d.pop("parent_unit_id", UNSET)

        window_index = d.pop("window_index", UNSET)

        chunk_vector_seed = cls(
            chunk_id=chunk_id,
            vector=vector,
            unit_id=unit_id,
            parent_unit_id=parent_unit_id,
            window_index=window_index,
        )

        chunk_vector_seed.additional_properties = d
        return chunk_vector_seed

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
