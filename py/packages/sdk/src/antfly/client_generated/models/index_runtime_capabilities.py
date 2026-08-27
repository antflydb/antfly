from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="IndexRuntimeCapabilities")


@_attrs_define
class IndexRuntimeCapabilities:
    """Deployment-level index capabilities clients can inspect before submitting index mutations.

    Attributes:
        artifact_sources (bool): Whether full-text, embedding, and graph indexes may consume generated artifact streams
            through either single-source or multi-source request forms. False for serverless deployments and during
            distributed rolling upgrades until every live data store reports protocol support.
    """

    artifact_sources: bool

    def to_dict(self) -> dict[str, Any]:
        artifact_sources = self.artifact_sources

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact_sources": artifact_sources,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact_sources = d.pop("artifact_sources")

        index_runtime_capabilities = cls(
            artifact_sources=artifact_sources,
        )

        return index_runtime_capabilities
