from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ArtifactSeedSource")


@_attrs_define
class ArtifactSeedSource:
    """Optional identity of the remote content the result was computed from.

    Attributes:
        generation (str | Unset): Immutable object generation or version.
        sha256 (str | Unset): Hex SHA-256 of the fetched bytes.
        source_hash (str | Unset): The `source_hash` of an Antfly artifact read, `xxh64:<hex>`.
    """

    generation: str | Unset = UNSET
    sha256: str | Unset = UNSET
    source_hash: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        generation = self.generation

        sha256 = self.sha256

        source_hash = self.source_hash

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if generation is not UNSET:
            field_dict["generation"] = generation
        if sha256 is not UNSET:
            field_dict["sha256"] = sha256
        if source_hash is not UNSET:
            field_dict["source_hash"] = source_hash

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        generation = d.pop("generation", UNSET)

        sha256 = d.pop("sha256", UNSET)

        source_hash = d.pop("source_hash", UNSET)

        artifact_seed_source = cls(
            generation=generation,
            sha256=sha256,
            source_hash=source_hash,
        )

        return artifact_seed_source
