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
    """A node in graph query results

    Attributes:
        key (str): Document key
        table (str | Unset): Owning table for a cross-table node; omitted for nodes in the queried table
        depth (int | Unset): Distance from start node
        distance (float | Unset): Weighted distance
        document (GraphResultNodeDocument | Unset): Full document (if include_documents=true)
        path (list[GraphPathEndpoint] | Unset): Exact ordered node identities in the path from the start node to this
            node
        path_edges (list[GraphPathEdge] | Unset): Ordered typed edges in path from start to this node
        provenance (list[str] | Unset): Algebraic provenance labels folded into this result, when requested by an
            algebraic graph executor
        evidence (GraphResultNodeEvidence | Unset): Parsed evidence envelope for provenance labels and edge metadata
    """

    key: str
    table: str | Unset = UNSET
    depth: int | Unset = UNSET
    distance: float | Unset = UNSET
    document: GraphResultNodeDocument | Unset = UNSET
    path: list[GraphPathEndpoint] | Unset = UNSET
    path_edges: list[GraphPathEdge] | Unset = UNSET
    provenance: list[str] | Unset = UNSET
    evidence: GraphResultNodeEvidence | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        table = self.table

        depth = self.depth

        distance = self.distance

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
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if depth is not UNSET:
            field_dict["depth"] = depth
        if distance is not UNSET:
            field_dict["distance"] = distance
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

        table = d.pop("table", UNSET)

        depth = d.pop("depth", UNSET)

        distance = d.pop("distance", UNSET)

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
            table=table,
            depth=depth,
            distance=distance,
            document=document,
            path=path,
            path_edges=path_edges,
            provenance=provenance,
            evidence=evidence,
        )

        return graph_result_node
