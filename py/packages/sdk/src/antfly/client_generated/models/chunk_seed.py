from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkSeed")


@_attrs_define
class ChunkSeed:
    """One chunk of a seeded chunk collection (`_chunks.<name>`), the plain
    shape a `_chunks` read returns minus internal fields. Chunk ids must be
    unique and ascending; character spans must lie within the source text.

    Chunks of a document extraction's chunk consumer are unit-scoped: each
    names its unit with `_parent_unit_id` (ids ascend within a unit) and
    may carry its `_speaker`. They are accepted together with the
    extraction seed in the same write and must name one of its units.
    A collection is either entirely unit-scoped or not at all.

    The accepted collection is kept for its input: later edits and replays
    that leave the source text (or unit text) and the chunker
    configuration unchanged keep it; a changed input is chunked again.

        Attributes:
            field_content (str):
            field_chunk_id (int | Unset): Chunk ordinal. `_id` is accepted as an alias and must agree.
            field_start_char (int | Unset):
            field_end_char (int | Unset):
            field_start_time_ms (float | Unset):
            field_end_time_ms (float | Unset):
            field_parent_unit_id (str | Unset): The extraction unit this chunk belongs to, e.g. `page:000003`.
            field_speaker (int | str | Unset): Diarized speaker, `SPEAKER_NN` as reads label it, or its index.
    """

    field_content: str
    field_chunk_id: int | Unset = UNSET
    field_start_char: int | Unset = UNSET
    field_end_char: int | Unset = UNSET
    field_start_time_ms: float | Unset = UNSET
    field_end_time_ms: float | Unset = UNSET
    field_parent_unit_id: str | Unset = UNSET
    field_speaker: int | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_content = self.field_content

        field_chunk_id = self.field_chunk_id

        field_start_char = self.field_start_char

        field_end_char = self.field_end_char

        field_start_time_ms = self.field_start_time_ms

        field_end_time_ms = self.field_end_time_ms

        field_parent_unit_id = self.field_parent_unit_id

        field_speaker: int | str | Unset
        if isinstance(self.field_speaker, Unset):
            field_speaker = UNSET
        else:
            field_speaker = self.field_speaker

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "_content": field_content,
            }
        )
        if field_chunk_id is not UNSET:
            field_dict["_chunk_id"] = field_chunk_id
        if field_start_char is not UNSET:
            field_dict["_start_char"] = field_start_char
        if field_end_char is not UNSET:
            field_dict["_end_char"] = field_end_char
        if field_start_time_ms is not UNSET:
            field_dict["_start_time_ms"] = field_start_time_ms
        if field_end_time_ms is not UNSET:
            field_dict["_end_time_ms"] = field_end_time_ms
        if field_parent_unit_id is not UNSET:
            field_dict["_parent_unit_id"] = field_parent_unit_id
        if field_speaker is not UNSET:
            field_dict["_speaker"] = field_speaker

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field_content = d.pop("_content")

        field_chunk_id = d.pop("_chunk_id", UNSET)

        field_start_char = d.pop("_start_char", UNSET)

        field_end_char = d.pop("_end_char", UNSET)

        field_start_time_ms = d.pop("_start_time_ms", UNSET)

        field_end_time_ms = d.pop("_end_time_ms", UNSET)

        field_parent_unit_id = d.pop("_parent_unit_id", UNSET)

        def _parse_field_speaker(data: object) -> int | str | Unset:
            if isinstance(data, Unset):
                return data
            return cast(int | str | Unset, data)

        field_speaker = _parse_field_speaker(d.pop("_speaker", UNSET))

        chunk_seed = cls(
            field_content=field_content,
            field_chunk_id=field_chunk_id,
            field_start_char=field_start_char,
            field_end_char=field_end_char,
            field_start_time_ms=field_start_time_ms,
            field_end_time_ms=field_end_time_ms,
            field_parent_unit_id=field_parent_unit_id,
            field_speaker=field_speaker,
        )

        chunk_seed.additional_properties = d
        return chunk_seed

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
