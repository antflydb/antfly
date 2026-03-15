from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_query_type import GraphQueryType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_result_node import GraphResultNode
    from ..models.path import Path
    from ..models.pattern_match import PatternMatch


T = TypeVar("T", bound="GraphQueryResult")


@_attrs_define
class GraphQueryResult:
    """Results of a graph query

    Attributes:
        type_ (GraphQueryType): Type of graph query to execute
        total (int): Total number of results
        nodes (Union[Unset, list['GraphResultNode']]): Result nodes
        paths (Union[Unset, list['Path']]): Result paths (for pathfinding queries)
        matches (Union[Unset, list['PatternMatch']]): Pattern matches (for pattern queries)
        took (Union[Unset, int]): Query execution time
    """

    type_: GraphQueryType
    total: int
    nodes: Union[Unset, list["GraphResultNode"]] = UNSET
    paths: Union[Unset, list["Path"]] = UNSET
    matches: Union[Unset, list["PatternMatch"]] = UNSET
    took: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        total = self.total

        nodes: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = []
            for nodes_item_data in self.nodes:
                nodes_item = nodes_item_data.to_dict()
                nodes.append(nodes_item)

        paths: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.paths, Unset):
            paths = []
            for paths_item_data in self.paths:
                paths_item = paths_item_data.to_dict()
                paths.append(paths_item)

        matches: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.matches, Unset):
            matches = []
            for matches_item_data in self.matches:
                matches_item = matches_item_data.to_dict()
                matches.append(matches_item)

        took = self.took

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "total": total,
            }
        )
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if paths is not UNSET:
            field_dict["paths"] = paths
        if matches is not UNSET:
            field_dict["matches"] = matches
        if took is not UNSET:
            field_dict["took"] = took

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_result_node import GraphResultNode
        from ..models.path import Path
        from ..models.pattern_match import PatternMatch

        d = dict(src_dict)
        type_ = GraphQueryType(d.pop("type"))

        total = d.pop("total")

        nodes = []
        _nodes = d.pop("nodes", UNSET)
        for nodes_item_data in _nodes or []:
            nodes_item = GraphResultNode.from_dict(nodes_item_data)

            nodes.append(nodes_item)

        paths = []
        _paths = d.pop("paths", UNSET)
        for paths_item_data in _paths or []:
            paths_item = Path.from_dict(paths_item_data)

            paths.append(paths_item)

        matches = []
        _matches = d.pop("matches", UNSET)
        for matches_item_data in _matches or []:
            matches_item = PatternMatch.from_dict(matches_item_data)

            matches.append(matches_item)

        took = d.pop("took", UNSET)

        graph_query_result = cls(
            type_=type_,
            total=total,
            nodes=nodes,
            paths=paths,
            matches=matches,
            took=took,
        )

        graph_query_result.additional_properties = d
        return graph_query_result

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
