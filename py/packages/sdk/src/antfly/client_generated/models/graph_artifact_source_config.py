from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_artifact_source_config_format import GraphArtifactSourceConfigFormat
from ..models.graph_artifact_source_config_kind import GraphArtifactSourceConfigKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphArtifactSourceConfig")


@_attrs_define
class GraphArtifactSourceConfig:
    """Artifact stream materialized into graph edges.

    Attributes:
        kind (GraphArtifactSourceConfigKind):
        artifact (str):
        path (str | Unset):
        format_ (GraphArtifactSourceConfigFormat | Unset):  Default:
            GraphArtifactSourceConfigFormat.EXTRACTION_RELATION.
        mention_edge_type (str | Unset):
    """

    kind: GraphArtifactSourceConfigKind
    artifact: str
    path: str | Unset = UNSET
    format_: GraphArtifactSourceConfigFormat | Unset = GraphArtifactSourceConfigFormat.EXTRACTION_RELATION
    mention_edge_type: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        artifact = self.artifact

        path = self.path

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        mention_edge_type = self.mention_edge_type

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "artifact": artifact,
            }
        )
        if path is not UNSET:
            field_dict["path"] = path
        if format_ is not UNSET:
            field_dict["format"] = format_
        if mention_edge_type is not UNSET:
            field_dict["mention_edge_type"] = mention_edge_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = GraphArtifactSourceConfigKind(d.pop("kind"))

        artifact = d.pop("artifact")

        path = d.pop("path", UNSET)

        _format_ = d.pop("format", UNSET)
        format_: GraphArtifactSourceConfigFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = GraphArtifactSourceConfigFormat(_format_)

        mention_edge_type = d.pop("mention_edge_type", UNSET)

        graph_artifact_source_config = cls(
            kind=kind,
            artifact=artifact,
            path=path,
            format_=format_,
            mention_edge_type=mention_edge_type,
        )

        return graph_artifact_source_config
