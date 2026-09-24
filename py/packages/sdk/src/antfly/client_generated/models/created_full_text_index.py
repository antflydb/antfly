from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.created_full_text_index_type import CreatedFullTextIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.created_enrichment_config import CreatedEnrichmentConfig
    from ..models.full_text_artifact_index_source import FullTextArtifactIndexSource
    from ..models.text_analysis_config import TextAnalysisConfig


T = TypeVar("T", bound="CreatedFullTextIndex")


@_attrs_define
class CreatedFullTextIndex:
    """Normalized effective full-text index configuration returned after creation.

    Attributes:
        name (str): Name of the created index
        type_ (CreatedFullTextIndexType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[CreatedEnrichmentConfig] | Unset): Normalized inline managed enrichment definitions required
            by this index.
        sources (list[FullTextArtifactIndexSource] | Unset):
        mem_only (bool | Unset):
        field (str | Unset):
        analysis_config (TextAnalysisConfig | Unset): Custom text analysis for a full-text index. Component maps are
            keyed
            by the name that analyzers and `field_analyzers` reference. Built-in
            analyzers (`standard`, `simple`, `keyword`, `html`, `search_as_you_type`,
            `substring`, and the language analyzers such as `german`) are always
            available without declaring them.

            Example: split camelCase identifiers and match them as substrings.

            ```json
            {
              "analysis_config": {
                "field_analyzers": {"symbol": "code"},
                "token_filters": {
                  "tails": {"type": "suffix", "config": {"min": 3, "max": 24}}
                },
                "analyzers": {
                  "code": {
                    "type": "custom",
                    "config": {
                      "tokenizer": "whitespace",
                      "token_filters": ["camel_case", "unique", "tails"]
                    }
                  }
                }
              }
            }
            ```
    """

    name: str
    type_: CreatedFullTextIndexType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[CreatedEnrichmentConfig] | Unset = UNSET
    sources: list[FullTextArtifactIndexSource] | Unset = UNSET
    mem_only: bool | Unset = UNSET
    field: str | Unset = UNSET
    analysis_config: TextAnalysisConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

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

        analysis_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.analysis_config, Unset):
            analysis_config = self.analysis_config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
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
        if analysis_config is not UNSET:
            field_dict["analysis_config"] = analysis_config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.created_enrichment_config import CreatedEnrichmentConfig
        from ..models.full_text_artifact_index_source import FullTextArtifactIndexSource
        from ..models.text_analysis_config import TextAnalysisConfig

        d = dict(src_dict)
        name = d.pop("name")

        type_ = CreatedFullTextIndexType(d.pop("type"))

        description = d.pop("description", UNSET)

        version = d.pop("version", UNSET)

        _enrichments = d.pop("enrichments", UNSET)
        enrichments: list[CreatedEnrichmentConfig] | Unset = UNSET
        if _enrichments is not UNSET:
            enrichments = []
            for enrichments_item_data in _enrichments:
                enrichments_item = CreatedEnrichmentConfig.from_dict(enrichments_item_data)

                enrichments.append(enrichments_item)

        _sources = d.pop("sources", UNSET)
        sources: list[FullTextArtifactIndexSource] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = FullTextArtifactIndexSource.from_dict(sources_item_data)

                sources.append(sources_item)

        mem_only = d.pop("mem_only", UNSET)

        field = d.pop("field", UNSET)

        _analysis_config = d.pop("analysis_config", UNSET)
        analysis_config: TextAnalysisConfig | Unset
        if isinstance(_analysis_config, Unset):
            analysis_config = UNSET
        else:
            analysis_config = TextAnalysisConfig.from_dict(_analysis_config)

        created_full_text_index = cls(
            name=name,
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            sources=sources,
            mem_only=mem_only,
            field=field,
            analysis_config=analysis_config,
        )

        created_full_text_index.additional_properties = d
        return created_full_text_index

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
