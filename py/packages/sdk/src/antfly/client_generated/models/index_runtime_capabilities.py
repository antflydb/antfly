from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.artifact_sources_capability_state import ArtifactSourcesCapabilityState

T = TypeVar("T", bound="IndexRuntimeCapabilities")


@_attrs_define
class IndexRuntimeCapabilities:
    """Deployment-level index capabilities clients can inspect before submitting index mutations.

    Attributes:
        artifact_sources (bool): Whether full-text, embedding, and graph indexes may currently consume generated
            artifact streams through either single-source or multi-source request forms. Equivalent to
            artifact_sources_state=available.
        artifact_sources_state (ArtifactSourcesCapabilityState): Whether artifact-backed index mutations are accepted
            now, temporarily fenced during a distributed rolling upgrade, or permanently unsupported by this deployment.
    """

    artifact_sources: bool
    artifact_sources_state: ArtifactSourcesCapabilityState

    def to_dict(self) -> dict[str, Any]:
        artifact_sources = self.artifact_sources

        artifact_sources_state = self.artifact_sources_state.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact_sources": artifact_sources,
                "artifact_sources_state": artifact_sources_state,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact_sources = d.pop("artifact_sources")

        artifact_sources_state = ArtifactSourcesCapabilityState(d.pop("artifact_sources_state"))

        index_runtime_capabilities = cls(
            artifact_sources=artifact_sources,
            artifact_sources_state=artifact_sources_state,
        )

        return index_runtime_capabilities
