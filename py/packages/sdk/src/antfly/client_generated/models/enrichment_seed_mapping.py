from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.enrichment_seed_mapping_format import EnrichmentSeedMappingFormat
from ..models.enrichment_seed_mapping_on_missing import EnrichmentSeedMappingOnMissing
from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentSeedMapping")


@_attrs_define
class EnrichmentSeedMapping:
    """Accepts an existing result for this enrichment from an ordinary field
    of the written record instead of invoking its producer (artifact
    seeding). The mapped field stays an ordinary document field; its value
    is decoded like an explicit `_artifacts.<name>` or `_chunks.<name>`
    seed and bound to the same source version. An explicit seed in the
    write wins over the mapping. The mapping is not part of the
    enrichment's artifact identity: adding or changing it never rebuilds
    existing output.

        Attributes:
            field (str): Dotted path of the record field holding the existing result.
            format_ (EnrichmentSeedMappingFormat | Unset): How the field value is read. `text` and `json` are asset values,
                `vector` is a dense document embedding, `chunk_vectors` is an array
                of per-chunk vectors in chunk order, and `chunks` is an array of
                `ChunkSeed`. `auto` infers the format from the enrichment kind.
                 Default: EnrichmentSeedMappingFormat.AUTO.
            on_missing (EnrichmentSeedMappingOnMissing | Unset): When the field is absent: `generate` runs the producer as
                usual;
                `fail` rejects the write (`ArtifactSeedMissing`); `pending` runs no
                producer, now or on later edits and replays, and the output stays
                absent (reported `pending`) until a write supplies a seed,
                explicitly or through this field.
                 Default: EnrichmentSeedMappingOnMissing.GENERATE.
    """

    field: str
    format_: EnrichmentSeedMappingFormat | Unset = EnrichmentSeedMappingFormat.AUTO
    on_missing: EnrichmentSeedMappingOnMissing | Unset = EnrichmentSeedMappingOnMissing.GENERATE

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        on_missing: str | Unset = UNSET
        if not isinstance(self.on_missing, Unset):
            on_missing = self.on_missing.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if format_ is not UNSET:
            field_dict["format"] = format_
        if on_missing is not UNSET:
            field_dict["on_missing"] = on_missing

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        _format_ = d.pop("format", UNSET)
        format_: EnrichmentSeedMappingFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = EnrichmentSeedMappingFormat(_format_)

        _on_missing = d.pop("on_missing", UNSET)
        on_missing: EnrichmentSeedMappingOnMissing | Unset
        if isinstance(_on_missing, Unset):
            on_missing = UNSET
        else:
            on_missing = EnrichmentSeedMappingOnMissing(_on_missing)

        enrichment_seed_mapping = cls(
            field=field,
            format_=format_,
            on_missing=on_missing,
        )

        return enrichment_seed_mapping
