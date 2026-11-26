from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.artifact_seed_provenance import ArtifactSeedProvenance
    from ..models.artifact_seed_source import ArtifactSeedSource


T = TypeVar("T", bound="ArtifactSeed")


@_attrs_define
class ArtifactSeed:
    """An already computed result for a named enrichment, written as
    `_artifacts.<name>` in a document or transform. The enrichment defines
    the output contract (content type, dimensions, vector space); the seed
    does not repeat it. `"<name>": null` drops the accepted output so its
    producer regenerates it; omitting a name leaves it unchanged.

    `value` by enrichment kind:
    - Text asset: a string (`""` is a valid, complete empty result), or
      `{ "text": "..." }`.
    - JSON asset: any JSON value.
    - Document embedding: an array of finite numbers.
    - Chunk-derived embedding: an array of `ChunkVectorSeed`.

    Descriptive read fields (`artifact_id`, `artifact_ref`, `kind`,
    `status`, `dims`, `items`, `content_type`) and internal routing
    fields are rejected.

        Example:
            {'value': [0.12, 0.34, 0.56]}

        Attributes:
            value (Any): The seeded result; its shape follows the enrichment kind.
            source (ArtifactSeedSource | Unset): Optional identity of the remote content the result was computed from.
            provenance (ArtifactSeedProvenance | Unset): Optional caller assertion about the producer; never proof that
                Antfly ran it.
    """

    value: Any
    source: ArtifactSeedSource | Unset = UNSET
    provenance: ArtifactSeedProvenance | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

        provenance: dict[str, Any] | Unset = UNSET
        if not isinstance(self.provenance, Unset):
            provenance = self.provenance.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "value": value,
            }
        )
        if source is not UNSET:
            field_dict["source"] = source
        if provenance is not UNSET:
            field_dict["provenance"] = provenance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_seed_provenance import ArtifactSeedProvenance
        from ..models.artifact_seed_source import ArtifactSeedSource

        d = dict(src_dict)
        value = d.pop("value")

        _source = d.pop("source", UNSET)
        source: ArtifactSeedSource | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = ArtifactSeedSource.from_dict(_source)

        _provenance = d.pop("provenance", UNSET)
        provenance: ArtifactSeedProvenance | Unset
        if isinstance(_provenance, Unset):
            provenance = UNSET
        else:
            provenance = ArtifactSeedProvenance.from_dict(_provenance)

        artifact_seed = cls(
            value=value,
            source=source,
            provenance=provenance,
        )

        artifact_seed.additional_properties = d
        return artifact_seed

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
