from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.edge import Edge
    from ..models.graph_result_node_document import GraphResultNodeDocument
    from ..models.path_edge import PathEdge


T = TypeVar("T", bound="GraphResultNode")


@_attrs_define
class GraphResultNode:
    """A node in graph query results

    Attributes:
        key (str): Document key
        depth (Union[Unset, int]): Distance from start node
        distance (Union[Unset, float]): Weighted distance
        document (Union[Unset, GraphResultNodeDocument]): Full document (if include_documents=true)
        path (Union[Unset, list[str]]): Keys in path from start to this node
        path_edges (Union[Unset, list['PathEdge']]): Edges in path from start to this node
        edges (Union[Unset, list['Edge']]): Connected edges (when include_edges=true)
    """

    key: str
    depth: Union[Unset, int] = UNSET
    distance: Union[Unset, float] = UNSET
    document: Union[Unset, "GraphResultNodeDocument"] = UNSET
    path: Union[Unset, list[str]] = UNSET
    path_edges: Union[Unset, list["PathEdge"]] = UNSET
    edges: Union[Unset, list["Edge"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        depth = self.depth

        distance = self.distance

        document: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        path: Union[Unset, list[str]] = UNSET
        if not isinstance(self.path, Unset):
            path = self.path

        path_edges: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.path_edges, Unset):
            path_edges = []
            for path_edges_item_data in self.path_edges:
                path_edges_item = path_edges_item_data.to_dict()
                path_edges.append(path_edges_item)

        edges: Union[Unset, list[dict[str, Any]]] = UNSET
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
        if edges is not UNSET:
            field_dict["edges"] = edges

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.edge import Edge
        from ..models.graph_result_node_document import GraphResultNodeDocument
        from ..models.path_edge import PathEdge

        d = dict(src_dict)
        key = d.pop("key")

        depth = d.pop("depth", UNSET)

        distance = d.pop("distance", UNSET)

        _document = d.pop("document", UNSET)
        document: Union[Unset, GraphResultNodeDocument]
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = GraphResultNodeDocument.from_dict(_document)

        path = cast(list[str], d.pop("path", UNSET))

        path_edges = []
        _path_edges = d.pop("path_edges", UNSET)
        for path_edges_item_data in _path_edges or []:
            path_edges_item = PathEdge.from_dict(path_edges_item_data)

            path_edges.append(path_edges_item)

        edges = []
        _edges = d.pop("edges", UNSET)
        for edges_item_data in _edges or []:
            edges_item = Edge.from_dict(edges_item_data)

            edges.append(edges_item)

        graph_result_node = cls(
            key=key,
            depth=depth,
            distance=distance,
            document=document,
            path=path,
            path_edges=path_edges,
            edges=edges,
        )

        graph_result_node.additional_properties = d
        return graph_result_node

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
