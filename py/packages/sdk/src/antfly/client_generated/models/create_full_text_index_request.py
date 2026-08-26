from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.create_full_text_index_request_type import CreateFullTextIndexRequestType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.artifact_index_source import ArtifactIndexSource
    from ..models.enrichment_config import EnrichmentConfig


T = TypeVar("T", bound="CreateFullTextIndexRequest")


@_attrs_define
class CreateFullTextIndexRequest:
    """Create a full-text index.

    Attributes:
        type_ (CreateFullTextIndexRequestType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[EnrichmentConfig] | Unset): Inline managed enrichment definitions required by this index.
        sources (list[ArtifactIndexSource] | Unset): Chunk or textual asset streams indexed together; every artifact
            record is an independent full-text member. Requires index_capabilities.artifact_sources=true and is rejected by
            serverless deployments.
        mem_only (bool | Unset): Whether to use memory-only storage
        field (str | Unset): Content field indexed as text. With an artifact source, this selects the field within each
            artifact record; without one, it selects a document field. String values and arrays of strings are indexed;
            missing, null, and non-text values produce no posting. Omit to index the default text projection.
        artifact_name (str | Unset): Single-source convenience form. Mutually exclusive with sources; normalized
            responses use sources. Requires index_capabilities.artifact_sources=true and is rejected by serverless
            deployments.
    """

    type_: CreateFullTextIndexRequestType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[EnrichmentConfig] | Unset = UNSET
    sources: list[ArtifactIndexSource] | Unset = UNSET
    mem_only: bool | Unset = UNSET
    field: str | Unset = UNSET
    artifact_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        description = self.description

        version = self.version

        enrichments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.enrichments, Unset):
            enrichments = []
            for enrichments_item_data in self.enrichments:
                enrichments_item = enrichments_item_data.to_dict()
                enrichments.append(enrichments_item)

        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

        mem_only = self.mem_only

        field = self.field

        artifact_name = self.artifact_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if version is not UNSET:
            field_dict["version"] = version
        if enrichments is not UNSET:
            field_dict["enrichments"] = enrichments
        if sources is not UNSET:
            field_dict["sources"] = sources
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only
        if field is not UNSET:
            field_dict["field"] = field
        if artifact_name is not UNSET:
            field_dict["artifact_name"] = artifact_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_index_source import ArtifactIndexSource
        from ..models.enrichment_config import EnrichmentConfig

        d = dict(src_dict)
        type_ = CreateFullTextIndexRequestType(d.pop("type"))

        description = d.pop("description", UNSET)

        version = d.pop("version", UNSET)

        _enrichments = d.pop("enrichments", UNSET)
        enrichments: list[EnrichmentConfig] | Unset = UNSET
        if _enrichments is not UNSET:
            enrichments = []
            for enrichments_item_data in _enrichments:
                enrichments_item = EnrichmentConfig.from_dict(enrichments_item_data)

                enrichments.append(enrichments_item)

        _sources = d.pop("sources", UNSET)
        sources: list[ArtifactIndexSource] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = ArtifactIndexSource.from_dict(sources_item_data)

                sources.append(sources_item)

        mem_only = d.pop("mem_only", UNSET)

        field = d.pop("field", UNSET)

        artifact_name = d.pop("artifact_name", UNSET)

        create_full_text_index_request = cls(
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            sources=sources,
            mem_only=mem_only,
            field=field,
            artifact_name=artifact_name,
        )

        create_full_text_index_request.additional_properties = d
        return create_full_text_index_request

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
