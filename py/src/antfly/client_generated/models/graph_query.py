from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_query_type import GraphQueryType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_node_selector import GraphNodeSelector
    from ..models.graph_query_params import GraphQueryParams
    from ..models.pattern_step import PatternStep


T = TypeVar("T", bound="GraphQuery")


@_attrs_define
class GraphQuery:
    """Declarative graph query to execute after full-text/vector searches

    Attributes:
        type_ (GraphQueryType): Type of graph query to execute
        index_name (str): Graph index name (must be graph type)
        start_nodes (Union[Unset, GraphNodeSelector]): Defines how to select start/target nodes for graph queries
        target_nodes (Union[Unset, GraphNodeSelector]): Defines how to select start/target nodes for graph queries
        params (Union[Unset, GraphQueryParams]): Parameters for graph traversal and pathfinding
        pattern (Union[Unset, list['PatternStep']]): Pattern steps for pattern query type
        return_aliases (Union[Unset, list[str]]): Which aliases to return from pattern query (empty = all)
        include_documents (Union[Unset, bool]): Fetch full documents for graph results
        include_edges (Union[Unset, bool]): Include edge details for each node
        fields (Union[Unset, list[str]]): Which fields to return from documents
    """

    type_: GraphQueryType
    index_name: str
    start_nodes: Union[Unset, "GraphNodeSelector"] = UNSET
    target_nodes: Union[Unset, "GraphNodeSelector"] = UNSET
    params: Union[Unset, "GraphQueryParams"] = UNSET
    pattern: Union[Unset, list["PatternStep"]] = UNSET
    return_aliases: Union[Unset, list[str]] = UNSET
    include_documents: Union[Unset, bool] = UNSET
    include_edges: Union[Unset, bool] = UNSET
    fields: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        index_name = self.index_name

        start_nodes: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.start_nodes, Unset):
            start_nodes = self.start_nodes.to_dict()

        target_nodes: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.target_nodes, Unset):
            target_nodes = self.target_nodes.to_dict()

        params: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.params, Unset):
            params = self.params.to_dict()

        pattern: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.pattern, Unset):
            pattern = []
            for pattern_item_data in self.pattern:
                pattern_item = pattern_item_data.to_dict()
                pattern.append(pattern_item)

        return_aliases: Union[Unset, list[str]] = UNSET
        if not isinstance(self.return_aliases, Unset):
            return_aliases = self.return_aliases

        include_documents = self.include_documents

        include_edges = self.include_edges

        fields: Union[Unset, list[str]] = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "index_name": index_name,
            }
        )
        if start_nodes is not UNSET:
            field_dict["start_nodes"] = start_nodes
        if target_nodes is not UNSET:
            field_dict["target_nodes"] = target_nodes
        if params is not UNSET:
            field_dict["params"] = params
        if pattern is not UNSET:
            field_dict["pattern"] = pattern
        if return_aliases is not UNSET:
            field_dict["return_aliases"] = return_aliases
        if include_documents is not UNSET:
            field_dict["include_documents"] = include_documents
        if include_edges is not UNSET:
            field_dict["include_edges"] = include_edges
        if fields is not UNSET:
            field_dict["fields"] = fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_node_selector import GraphNodeSelector
        from ..models.graph_query_params import GraphQueryParams
        from ..models.pattern_step import PatternStep

        d = dict(src_dict)
        type_ = GraphQueryType(d.pop("type"))

        index_name = d.pop("index_name")

        _start_nodes = d.pop("start_nodes", UNSET)
        start_nodes: Union[Unset, GraphNodeSelector]
        if isinstance(_start_nodes, Unset):
            start_nodes = UNSET
        else:
            start_nodes = GraphNodeSelector.from_dict(_start_nodes)

        _target_nodes = d.pop("target_nodes", UNSET)
        target_nodes: Union[Unset, GraphNodeSelector]
        if isinstance(_target_nodes, Unset):
            target_nodes = UNSET
        else:
            target_nodes = GraphNodeSelector.from_dict(_target_nodes)

        _params = d.pop("params", UNSET)
        params: Union[Unset, GraphQueryParams]
        if isinstance(_params, Unset):
            params = UNSET
        else:
            params = GraphQueryParams.from_dict(_params)

        pattern = []
        _pattern = d.pop("pattern", UNSET)
        for pattern_item_data in _pattern or []:
            pattern_item = PatternStep.from_dict(pattern_item_data)

            pattern.append(pattern_item)

        return_aliases = cast(list[str], d.pop("return_aliases", UNSET))

        include_documents = d.pop("include_documents", UNSET)

        include_edges = d.pop("include_edges", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        graph_query = cls(
            type_=type_,
            index_name=index_name,
            start_nodes=start_nodes,
            target_nodes=target_nodes,
            params=params,
            pattern=pattern,
            return_aliases=return_aliases,
            include_documents=include_documents,
            include_edges=include_edges,
            fields=fields,
        )

        graph_query.additional_properties = d
        return graph_query

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
