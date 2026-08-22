from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.edge_direction import EdgeDirection
from ..models.path_weight_mode import PathWeightMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_document_query import GraphDocumentQuery
    from ..models.graph_path_endpoint import GraphPathEndpoint


T = TypeVar("T", bound="GraphKShortestPaths")


@_attrs_define
class GraphKShortestPaths:
    """
    Attributes:
        from_ (GraphPathEndpoint):
        to (GraphPathEndpoint):
        k (int):
        edge_types (list[str] | Unset):
        direction (EdgeDirection | Unset): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
        max_depth (int | Unset):  Default: 10.
        min_weight (float | Unset):
        max_weight (float | Unset):
        weight_mode (PathWeightMode | Unset): Path weighting algorithm for pathfinding:
            - min_hops: Minimize number of edges
            - min_weight: Minimize sum of edge weights
            - max_weight: Maximize product of edge weights
        filter_ (GraphDocumentQuery | Unset): A document-query expression in either public QueryRequest.filter_query
            syntax or canonical Antfly filter AST syntax. Graph queries embed this existing document query language; alias-
            to-alias predicates belong in GraphMatch.where.
        include_documents (bool | Unset): Include stored documents on nodes returned with each path. Default: False.
        fields (list[str] | Unset): Document fields to include when include_documents is true. Omit to include all
            fields.
    """

    from_: GraphPathEndpoint
    to: GraphPathEndpoint
    k: int
    edge_types: list[str] | Unset = UNSET
    direction: EdgeDirection | Unset = UNSET
    max_depth: int | Unset = 10
    min_weight: float | Unset = UNSET
    max_weight: float | Unset = UNSET
    weight_mode: PathWeightMode | Unset = UNSET
    filter_: GraphDocumentQuery | Unset = UNSET
    include_documents: bool | Unset = False
    fields: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from_ = self.from_.to_dict()

        to = self.to.to_dict()

        k = self.k

        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        max_depth = self.max_depth

        min_weight = self.min_weight

        max_weight = self.max_weight

        weight_mode: str | Unset = UNSET
        if not isinstance(self.weight_mode, Unset):
            weight_mode = self.weight_mode.value

        filter_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_, Unset):
            filter_ = self.filter_.to_dict()

        include_documents = self.include_documents

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "from": from_,
                "to": to,
                "k": k,
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
        if weight_mode is not UNSET:
            field_dict["weight_mode"] = weight_mode
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if include_documents is not UNSET:
            field_dict["include_documents"] = include_documents
        if fields is not UNSET:
            field_dict["fields"] = fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_document_query import GraphDocumentQuery
        from ..models.graph_path_endpoint import GraphPathEndpoint

        d = dict(src_dict)
        from_ = GraphPathEndpoint.from_dict(d.pop("from"))

        to = GraphPathEndpoint.from_dict(d.pop("to"))

        k = d.pop("k")

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

        _weight_mode = d.pop("weight_mode", UNSET)
        weight_mode: PathWeightMode | Unset
        if isinstance(_weight_mode, Unset):
            weight_mode = UNSET
        else:
            weight_mode = PathWeightMode(_weight_mode)

        _filter_ = d.pop("filter", UNSET)
        filter_: GraphDocumentQuery | Unset
        if isinstance(_filter_, Unset):
            filter_ = UNSET
        else:
            filter_ = GraphDocumentQuery.from_dict(_filter_)

        include_documents = d.pop("include_documents", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        graph_k_shortest_paths = cls(
            from_=from_,
            to=to,
            k=k,
            edge_types=edge_types,
            direction=direction,
            max_depth=max_depth,
            min_weight=min_weight,
            max_weight=max_weight,
            weight_mode=weight_mode,
            filter_=filter_,
            include_documents=include_documents,
            fields=fields,
        )

        return graph_k_shortest_paths
