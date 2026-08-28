from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_path_edge import GraphPathEdge
    from ..models.graph_path_endpoint import GraphPathEndpoint
    from ..models.graph_result_node_document import GraphResultNodeDocument
    from ..models.graph_result_node_evidence import GraphResultNodeEvidence


T = TypeVar("T", bound="GraphResultNode")


@_attrs_define
class GraphResultNode:
    """A traversal result node or the terminal node paired with a pathfinding result. Traversal paths, when requested, are
    carried by path and path_edges. Shortest-path operations keep the complete path only in the enclosing
    GraphNodesResult.paths entry to avoid duplicate wire data.

        Attributes:
            key (str): Document key
            depth (int): Hop count from the start node; when path is present this equals path length minus one
            table (str | Unset): Owning table for a cross-table node; omitted for nodes in the queried table
            document (GraphResultNodeDocument | Unset): Full document (if include_documents=true)
            path (list[GraphPathEndpoint] | Unset): Exact ordered traversal identities from the start node, terminating at
                this node's fully qualified identity. Present only for traversal queries with include_paths=true; pathfinding
                uses GraphNodesResult.paths.
            path_edges (list[GraphPathEdge] | Unset): Ordered typed traversal edges from the start node. Present only with
                path for traversal queries; pathfinding uses GraphNodesResult.paths.
            provenance (list[str] | Unset): Algebraic provenance labels folded into this result, when requested by an
                algebraic graph executor
            evidence (GraphResultNodeEvidence | Unset): Parsed evidence envelope for provenance labels and edge metadata
    """

    key: str
    depth: int
    table: str | Unset = UNSET
    document: GraphResultNodeDocument | Unset = UNSET
    path: list[GraphPathEndpoint] | Unset = UNSET
    path_edges: list[GraphPathEdge] | Unset = UNSET
    provenance: list[str] | Unset = UNSET
    evidence: GraphResultNodeEvidence | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        depth = self.depth

        table = self.table

        document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        path: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.path, Unset):
            path = []
            for path_item_data in self.path:
                path_item = path_item_data.to_dict()
                path.append(path_item)

        path_edges: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.path_edges, Unset):
            path_edges = []
            for path_edges_item_data in self.path_edges:
                path_edges_item = path_edges_item_data.to_dict()
                path_edges.append(path_edges_item)

        provenance: list[str] | Unset = UNSET
        if not isinstance(self.provenance, Unset):
            provenance = self.provenance

        evidence: dict[str, Any] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = self.evidence.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key": key,
                "depth": depth,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if document is not UNSET:
            field_dict["document"] = document
        if path is not UNSET:
            field_dict["path"] = path
        if path_edges is not UNSET:
            field_dict["path_edges"] = path_edges
        if provenance is not UNSET:
            field_dict["provenance"] = provenance
        if evidence is not UNSET:
            field_dict["evidence"] = evidence

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_path_edge import GraphPathEdge
        from ..models.graph_path_endpoint import GraphPathEndpoint
        from ..models.graph_result_node_document import GraphResultNodeDocument
        from ..models.graph_result_node_evidence import GraphResultNodeEvidence

        d = dict(src_dict)
        key = d.pop("key")

        depth = d.pop("depth")

        table = d.pop("table", UNSET)

        _document = d.pop("document", UNSET)
        document: GraphResultNodeDocument | Unset
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = GraphResultNodeDocument.from_dict(_document)

        _path = d.pop("path", UNSET)
        path: list[GraphPathEndpoint] | Unset = UNSET
        if _path is not UNSET:
            path = []
            for path_item_data in _path:
                path_item = GraphPathEndpoint.from_dict(path_item_data)

                path.append(path_item)

        _path_edges = d.pop("path_edges", UNSET)
        path_edges: list[GraphPathEdge] | Unset = UNSET
        if _path_edges is not UNSET:
            path_edges = []
            for path_edges_item_data in _path_edges:
                path_edges_item = GraphPathEdge.from_dict(path_edges_item_data)

                path_edges.append(path_edges_item)

        provenance = cast(list[str], d.pop("provenance", UNSET))

        _evidence = d.pop("evidence", UNSET)
        evidence: GraphResultNodeEvidence | Unset
        if isinstance(_evidence, Unset):
            evidence = UNSET
        else:
            evidence = GraphResultNodeEvidence.from_dict(_evidence)

        graph_result_node = cls(
            key=key,
            depth=depth,
            table=table,
            document=document,
            path=path,
            path_edges=path_edges,
            provenance=provenance,
            evidence=evidence,
        )

        return graph_result_node
