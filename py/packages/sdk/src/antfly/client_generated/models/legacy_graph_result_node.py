from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.edge import Edge
    from ..models.legacy_graph_result_node_document import LegacyGraphResultNodeDocument
    from ..models.legacy_graph_result_node_evidence import LegacyGraphResultNodeEvidence
    from ..models.path_edge import PathEdge


T = TypeVar("T", bound="LegacyGraphResultNode")


@_attrs_define
class LegacyGraphResultNode:
    """Deprecated graph_searches node response with an unqualified string path.

    Attributes:
        key (str): Document key
        table (str | Unset): Owning table for a cross-table node; omitted for nodes in the queried table
        depth (int | Unset): Distance from start node
        distance (float | Unset): Weighted distance
        document (LegacyGraphResultNodeDocument | Unset): Full document (if include_documents=true)
        path (list[str] | Unset): Deprecated unqualified keys in the path from the start node to this node
        path_edges (list[PathEdge] | Unset): Edges in path from start to this node
        provenance (list[str] | Unset): Algebraic provenance labels folded into this result, when requested by an
            algebraic graph executor
        evidence (LegacyGraphResultNodeEvidence | Unset): Parsed evidence envelope for provenance labels and edge
            metadata
        edges (list[Edge] | Unset): Connected edges when supplied by the graph executor.
    """

    key: str
    table: str | Unset = UNSET
    depth: int | Unset = UNSET
    distance: float | Unset = UNSET
    document: LegacyGraphResultNodeDocument | Unset = UNSET
    path: list[str] | Unset = UNSET
    path_edges: list[PathEdge] | Unset = UNSET
    provenance: list[str] | Unset = UNSET
    evidence: LegacyGraphResultNodeEvidence | Unset = UNSET
    edges: list[Edge] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        table = self.table

        depth = self.depth

        distance = self.distance

        document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        path: list[str] | Unset = UNSET
        if not isinstance(self.path, Unset):
            path = self.path

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

        edges: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.edges, Unset):
            edges = []
            for edges_item_data in self.edges:
                edges_item = edges_item_data.to_dict()
                edges.append(edges_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
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
        if edges is not UNSET:
            field_dict["edges"] = edges

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge import Edge
        from ..models.legacy_graph_result_node_document import LegacyGraphResultNodeDocument
        from ..models.legacy_graph_result_node_evidence import LegacyGraphResultNodeEvidence
        from ..models.path_edge import PathEdge

        d = dict(src_dict)
        key = d.pop("key")

        table = d.pop("table", UNSET)

        depth = d.pop("depth", UNSET)

        distance = d.pop("distance", UNSET)

        _document = d.pop("document", UNSET)
        document: LegacyGraphResultNodeDocument | Unset
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = LegacyGraphResultNodeDocument.from_dict(_document)

        path = cast(list[str], d.pop("path", UNSET))

        _path_edges = d.pop("path_edges", UNSET)
        path_edges: list[PathEdge] | Unset = UNSET
        if _path_edges is not UNSET:
            path_edges = []
            for path_edges_item_data in _path_edges:
                path_edges_item = PathEdge.from_dict(path_edges_item_data)

                path_edges.append(path_edges_item)

        provenance = cast(list[str], d.pop("provenance", UNSET))

        _evidence = d.pop("evidence", UNSET)
        evidence: LegacyGraphResultNodeEvidence | Unset
        if isinstance(_evidence, Unset):
            evidence = UNSET
        else:
            evidence = LegacyGraphResultNodeEvidence.from_dict(_evidence)

        _edges = d.pop("edges", UNSET)
        edges: list[Edge] | Unset = UNSET
        if _edges is not UNSET:
            edges = []
            for edges_item_data in _edges:
                edges_item = Edge.from_dict(edges_item_data)

                edges.append(edges_item)

        legacy_graph_result_node = cls(
            key=key,
            table=table,
            depth=depth,
            distance=distance,
            document=document,
            path=path,
            path_edges=path_edges,
            provenance=provenance,
            evidence=evidence,
            edges=edges,
        )

        legacy_graph_result_node.additional_properties = d
        return legacy_graph_result_node

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
