from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.full_text_artifact_index_source import FullTextArtifactIndexSource
    from ..models.text_analysis_config import TextAnalysisConfig


T = TypeVar("T", bound="FullTextIndexConfig")


@_attrs_define
class FullTextIndexConfig:
    """
    Attributes:
        sources (list[FullTextArtifactIndexSource] | Unset): Chunk or textual asset streams indexed together; every
            artifact record is an independent full-text member. A source-local field overrides the shared index-level field
            for that stream. Artifact names must be unique. Requires index_capabilities.artifact_sources=true and is
            rejected by serverless deployments.
        mem_only (bool | Unset): Whether to use memory-only storage
        field (str | Unset): Content field indexed as text. With an artifact source, this selects the field within each
            artifact record; without one, it selects a document field. String values and arrays of strings are indexed;
            missing, null, and non-text values produce no posting. Omit to index the default text projection.
        artifact_name (str | Unset): Single-source convenience form. Mutually exclusive with sources; normalized
            responses use sources. Requires index_capabilities.artifact_sources=true and is rejected by serverless
            deployments.
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

    sources: list[FullTextArtifactIndexSource] | Unset = UNSET
    mem_only: bool | Unset = UNSET
    field: str | Unset = UNSET
    artifact_name: str | Unset = UNSET
    analysis_config: TextAnalysisConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

        mem_only = self.mem_only

        field = self.field

        artifact_name = self.artifact_name

        analysis_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.analysis_config, Unset):
            analysis_config = self.analysis_config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if sources is not UNSET:
            field_dict["sources"] = sources
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only
        if field is not UNSET:
            field_dict["field"] = field
        if artifact_name is not UNSET:
            field_dict["artifact_name"] = artifact_name
        if analysis_config is not UNSET:
            field_dict["analysis_config"] = analysis_config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.full_text_artifact_index_source import FullTextArtifactIndexSource
        from ..models.text_analysis_config import TextAnalysisConfig

        d = dict(src_dict)
        _sources = d.pop("sources", UNSET)
        sources: list[FullTextArtifactIndexSource] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = FullTextArtifactIndexSource.from_dict(sources_item_data)

                sources.append(sources_item)

        mem_only = d.pop("mem_only", UNSET)

        field = d.pop("field", UNSET)

        artifact_name = d.pop("artifact_name", UNSET)

        _analysis_config = d.pop("analysis_config", UNSET)
        analysis_config: TextAnalysisConfig | Unset
        if isinstance(_analysis_config, Unset):
            analysis_config = UNSET
        else:
            analysis_config = TextAnalysisConfig.from_dict(_analysis_config)

        full_text_index_config = cls(
            sources=sources,
            mem_only=mem_only,
            field=field,
            artifact_name=artifact_name,
            analysis_config=analysis_config,
        )

        full_text_index_config.additional_properties = d
        return full_text_index_config

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
