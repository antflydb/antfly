from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_index_source_format import GraphIndexSourceFormat
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_index_source_context import GraphIndexSourceContext
    from ..models.graph_index_source_edge import GraphIndexSourceEdge
    from ..models.graph_index_source_nodes import GraphIndexSourceNodes


T = TypeVar("T", bound="GraphIndexSource")


@_attrs_define
class GraphIndexSource:
    """Graph-specific artifact source. The source owns the payload path and format because different artifact streams in
    one graph index may require different interpretations.

        Attributes:
            artifact (str): Stable name of the generated graph artifact.
            path (str | Unset): Optional JSON path selecting edge records within this artifact payload. Example:
                $.relations[*].
            format_ (GraphIndexSourceFormat | Unset): Payload interpretation for this artifact source. Default:
                GraphIndexSourceFormat.EXTRACTION_RELATION.
            mention_edge_type (str | Unset): Optional provenance edge type emitted for resolver mention decisions from this
                source.
            nodes (GraphIndexSourceNodes | Unset):
            edge (GraphIndexSourceEdge | Unset):
            context (GraphIndexSourceContext | Unset):
    """

    artifact: str
    path: str | Unset = UNSET
    format_: GraphIndexSourceFormat | Unset = GraphIndexSourceFormat.EXTRACTION_RELATION
    mention_edge_type: str | Unset = UNSET
    nodes: GraphIndexSourceNodes | Unset = UNSET
    edge: GraphIndexSourceEdge | Unset = UNSET
    context: GraphIndexSourceContext | Unset = UNSET

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
        from ..models.graph_index_source_context import GraphIndexSourceContext
        from ..models.graph_index_source_edge import GraphIndexSourceEdge
        from ..models.graph_index_source_nodes import GraphIndexSourceNodes

        d = dict(src_dict)
        artifact = d.pop("artifact")

        path = d.pop("path", UNSET)

        _format_ = d.pop("format", UNSET)
        format_: GraphIndexSourceFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = GraphIndexSourceFormat(_format_)

        mention_edge_type = d.pop("mention_edge_type", UNSET)

        _nodes = d.pop("nodes", UNSET)
        nodes: GraphIndexSourceNodes | Unset
        if isinstance(_nodes, Unset):
            nodes = UNSET
        else:
            nodes = GraphIndexSourceNodes.from_dict(_nodes)

        _edge = d.pop("edge", UNSET)
        edge: GraphIndexSourceEdge | Unset
        if isinstance(_edge, Unset):
            edge = UNSET
        else:
            edge = GraphIndexSourceEdge.from_dict(_edge)

        _context = d.pop("context", UNSET)
        context: GraphIndexSourceContext | Unset
        if isinstance(_context, Unset):
            context = UNSET
        else:
            context = GraphIndexSourceContext.from_dict(_context)

        graph_index_source = cls(
            artifact=artifact,
            path=path,
            format_=format_,
            mention_edge_type=mention_edge_type,
            nodes=nodes,
            edge=edge,
            context=context,
        )

        return graph_index_source
