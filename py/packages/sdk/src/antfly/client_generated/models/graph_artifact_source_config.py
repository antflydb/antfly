from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_artifact_source_config_format import GraphArtifactSourceConfigFormat
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_artifact_context_config import GraphArtifactContextConfig
    from ..models.graph_artifact_edge_mapping_config import GraphArtifactEdgeMappingConfig
    from ..models.graph_artifact_node_mapping_config import GraphArtifactNodeMappingConfig


T = TypeVar("T", bound="GraphArtifactSourceConfig")


@_attrs_define
class GraphArtifactSourceConfig:
    """Artifact stream materialized into graph edges. Each source artifact is limited to 16 MiB and 1,000,000 relation
    items so live apply, repair, split, and restore share one bounded admission contract. Artifact-backed graph sources
    require index_capabilities.artifact_sources=true and are rejected by serverless deployments.

        Attributes:
            artifact (str):
            path (str | Unset): Optional root path selecting the graph payload. Supports `$`, dot-separated ASCII field
                names such as `$.relations`, and an optional terminal `[*]` such as `$.relations[*]`.
            format_ (GraphArtifactSourceConfigFormat | Unset):  Default:
                GraphArtifactSourceConfigFormat.EXTRACTION_RELATION.
            mention_edge_type (str | Unset): Durable graph edge type. Values must be valid UTF-8 and encode to at most 64
                KiB; `maxLength` is the standard-schema code-point ceiling and `x-antfly-max-utf8-bytes` carries the exact wire-
                byte limit.
            nodes (GraphArtifactNodeMappingConfig | Unset): Maps each artifact item to graph node identifiers.
            edge (GraphArtifactEdgeMappingConfig | Unset): Maps each artifact item to an edge type, weight, and public
                metadata.
            context (GraphArtifactContextConfig | Unset): Document fields made available to graph mapping templates through
                `_doc.value`.
    """

    artifact: str
    path: str | Unset = UNSET
    format_: GraphArtifactSourceConfigFormat | Unset = GraphArtifactSourceConfigFormat.EXTRACTION_RELATION
    mention_edge_type: str | Unset = UNSET
    nodes: GraphArtifactNodeMappingConfig | Unset = UNSET
    edge: GraphArtifactEdgeMappingConfig | Unset = UNSET
    context: GraphArtifactContextConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact

        path = self.path

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        mention_edge_type = self.mention_edge_type

        nodes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = self.nodes.to_dict()

        edge: dict[str, Any] | Unset = UNSET
        if not isinstance(self.edge, Unset):
            edge = self.edge.to_dict()

        context: dict[str, Any] | Unset = UNSET
        if not isinstance(self.context, Unset):
            context = self.context.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact": artifact,
            }
        )
        if path is not UNSET:
            field_dict["path"] = path
        if format_ is not UNSET:
            field_dict["format"] = format_
        if mention_edge_type is not UNSET:
            field_dict["mention_edge_type"] = mention_edge_type
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if edge is not UNSET:
            field_dict["edge"] = edge
        if context is not UNSET:
            field_dict["context"] = context

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_artifact_context_config import GraphArtifactContextConfig
        from ..models.graph_artifact_edge_mapping_config import GraphArtifactEdgeMappingConfig
        from ..models.graph_artifact_node_mapping_config import GraphArtifactNodeMappingConfig

        d = dict(src_dict)
        artifact = d.pop("artifact")

        path = d.pop("path", UNSET)

        _format_ = d.pop("format", UNSET)
        format_: GraphArtifactSourceConfigFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = GraphArtifactSourceConfigFormat(_format_)

        mention_edge_type = d.pop("mention_edge_type", UNSET)

        _nodes = d.pop("nodes", UNSET)
        nodes: GraphArtifactNodeMappingConfig | Unset
        if isinstance(_nodes, Unset):
            nodes = UNSET
        else:
            nodes = GraphArtifactNodeMappingConfig.from_dict(_nodes)

        _edge = d.pop("edge", UNSET)
        edge: GraphArtifactEdgeMappingConfig | Unset
        if isinstance(_edge, Unset):
            edge = UNSET
        else:
            edge = GraphArtifactEdgeMappingConfig.from_dict(_edge)

        _context = d.pop("context", UNSET)
        context: GraphArtifactContextConfig | Unset
        if isinstance(_context, Unset):
            context = UNSET
        else:
            context = GraphArtifactContextConfig.from_dict(_context)

        graph_artifact_source_config = cls(
            artifact=artifact,
            path=path,
            format_=format_,
            mention_edge_type=mention_edge_type,
            nodes=nodes,
            edge=edge,
            context=context,
        )

        return graph_artifact_source_config
