from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.edge_direction import EdgeDirection
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_node_selector import GraphNodeSelector
    from ..models.graph_traversal_filter import GraphTraversalFilter


T = TypeVar("T", bound="GraphTraversal")


@_attrs_define
class GraphTraversal:
    """
    Attributes:
        start (GraphNodeSelector): Defines how to select start/target nodes for graph queries
        edge_types (list[str] | Unset):
        direction (EdgeDirection | Unset): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
        max_depth (int | Unset):  Default: 3.
        min_weight (float | Unset):
        max_weight (float | Unset):
        limit (int | Unset):  Default: 100.
        deduplicate_nodes (bool | Unset):  Default: True.
        include_paths (bool | Unset):  Default: False.
        include_documents (bool | Unset): Include each result node's stored document. Default: False.
        fields (list[str] | Unset): Document fields to include when include_documents is true. Omit to include all
            fields.
        filter_ (GraphTraversalFilter | Unset): Canonical Antfly document-query AST (the same shape accepted by
            QueryRequest.filter_query).
    """

    start: GraphNodeSelector
    edge_types: list[str] | Unset = UNSET
    direction: EdgeDirection | Unset = UNSET
    max_depth: int | Unset = 3
    min_weight: float | Unset = UNSET
    max_weight: float | Unset = UNSET
    limit: int | Unset = 100
    deduplicate_nodes: bool | Unset = True
    include_paths: bool | Unset = False
    include_documents: bool | Unset = False
    fields: list[str] | Unset = UNSET
    filter_: GraphTraversalFilter | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        start = self.start.to_dict()

        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        max_depth = self.max_depth

        min_weight = self.min_weight

        max_weight = self.max_weight

        limit = self.limit

        deduplicate_nodes = self.deduplicate_nodes

        include_paths = self.include_paths

        include_documents = self.include_documents

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        filter_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_, Unset):
            filter_ = self.filter_.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "start": start,
            }
        )
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if direction is not UNSET:
            field_dict["direction"] = direction
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if max_weight is not UNSET:
            field_dict["max_weight"] = max_weight
        if limit is not UNSET:
            field_dict["limit"] = limit
        if deduplicate_nodes is not UNSET:
            field_dict["deduplicate_nodes"] = deduplicate_nodes
        if include_paths is not UNSET:
            field_dict["include_paths"] = include_paths
        if include_documents is not UNSET:
            field_dict["include_documents"] = include_documents
        if fields is not UNSET:
            field_dict["fields"] = fields
        if filter_ is not UNSET:
            field_dict["filter"] = filter_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_node_selector import GraphNodeSelector
        from ..models.graph_traversal_filter import GraphTraversalFilter

        d = dict(src_dict)
        start = GraphNodeSelector.from_dict(d.pop("start"))

        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        _direction = d.pop("direction", UNSET)
        direction: EdgeDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        max_depth = d.pop("max_depth", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        max_weight = d.pop("max_weight", UNSET)

        limit = d.pop("limit", UNSET)

        deduplicate_nodes = d.pop("deduplicate_nodes", UNSET)

        include_paths = d.pop("include_paths", UNSET)

        include_documents = d.pop("include_documents", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        _filter_ = d.pop("filter", UNSET)
        filter_: GraphTraversalFilter | Unset
        if isinstance(_filter_, Unset):
            filter_ = UNSET
        else:
            filter_ = GraphTraversalFilter.from_dict(_filter_)

        graph_traversal = cls(
            start=start,
            edge_types=edge_types,
            direction=direction,
            max_depth=max_depth,
            min_weight=min_weight,
            max_weight=max_weight,
            limit=limit,
            deduplicate_nodes=deduplicate_nodes,
            include_paths=include_paths,
            include_documents=include_documents,
            fields=fields,
            filter_=filter_,
        )

        return graph_traversal
