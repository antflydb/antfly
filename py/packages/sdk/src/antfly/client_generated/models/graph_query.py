from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_query_metric_freshness import GraphQueryMetricFreshness
from ..models.graph_query_type import GraphQueryType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_metric_filter import GraphMetricFilter
    from ..models.graph_metric_order import GraphMetricOrder
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
        start_nodes (GraphNodeSelector | Unset): Defines how to select start/target nodes for graph queries
        target_nodes (GraphNodeSelector | Unset): Defines how to select start/target nodes for graph queries
        params (GraphQueryParams | Unset): Parameters for graph traversal and pathfinding
        pattern (list[PatternStep] | Unset): Pattern steps for pattern query type
        return_aliases (list[str] | Unset): Which aliases to return from pattern query (empty = all)
        include_documents (bool | Unset): Fetch full documents for graph results
        include_edges (bool | Unset): Include edge details for each node
        fields (list[str] | Unset): Which fields to return from documents
        metrics (list[str] | Unset): Graph metric names to project onto result nodes. At most 16 distinct metric
            dependencies may be used across metrics, order_by, and where_metric.
        order_by (list[GraphMetricOrder] | Unset): Sort graph result nodes by up to 8 distinct graph metric scores. At
            most 16 distinct metric dependencies may be used across metrics, order_by, and where_metric.
        where_metric (list[GraphMetricFilter] | Unset): Filter graph result nodes with up to 32 predicates. Multiple
            predicates may target one metric, and at most 16 distinct metric dependencies may be used across metrics,
            order_by, and where_metric.
        metric_freshness (GraphQueryMetricFreshness | Unset): Freshness mode for projected, ordered, and filtered graph
            metrics
        include_metric_status (bool | Unset): Include graph metric status metadata in the graph result
    """

    type_: GraphQueryType
    index_name: str
    start_nodes: GraphNodeSelector | Unset = UNSET
    target_nodes: GraphNodeSelector | Unset = UNSET
    params: GraphQueryParams | Unset = UNSET
    pattern: list[PatternStep] | Unset = UNSET
    return_aliases: list[str] | Unset = UNSET
    include_documents: bool | Unset = UNSET
    include_edges: bool | Unset = UNSET
    fields: list[str] | Unset = UNSET
    metrics: list[str] | Unset = UNSET
    order_by: list[GraphMetricOrder] | Unset = UNSET
    where_metric: list[GraphMetricFilter] | Unset = UNSET
    metric_freshness: GraphQueryMetricFreshness | Unset = UNSET
    include_metric_status: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        index_name = self.index_name

        start_nodes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.start_nodes, Unset):
            start_nodes = self.start_nodes.to_dict()

        target_nodes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.target_nodes, Unset):
            target_nodes = self.target_nodes.to_dict()

        params: dict[str, Any] | Unset = UNSET
        if not isinstance(self.params, Unset):
            params = self.params.to_dict()

        pattern: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.pattern, Unset):
            pattern = []
            for pattern_item_data in self.pattern:
                pattern_item = pattern_item_data.to_dict()
                pattern.append(pattern_item)

        return_aliases: list[str] | Unset = UNSET
        if not isinstance(self.return_aliases, Unset):
            return_aliases = self.return_aliases

        include_documents = self.include_documents

        include_edges = self.include_edges

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        metrics: list[str] | Unset = UNSET
        if not isinstance(self.metrics, Unset):
            metrics = self.metrics

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item = order_by_item_data.to_dict()
                order_by.append(order_by_item)

        where_metric: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where_metric, Unset):
            where_metric = []
            for where_metric_item_data in self.where_metric:
                where_metric_item = where_metric_item_data.to_dict()
                where_metric.append(where_metric_item)

        metric_freshness: str | Unset = UNSET
        if not isinstance(self.metric_freshness, Unset):
            metric_freshness = self.metric_freshness.value

        include_metric_status = self.include_metric_status

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
        if metrics is not UNSET:
            field_dict["metrics"] = metrics
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if where_metric is not UNSET:
            field_dict["where_metric"] = where_metric
        if metric_freshness is not UNSET:
            field_dict["metric_freshness"] = metric_freshness
        if include_metric_status is not UNSET:
            field_dict["include_metric_status"] = include_metric_status

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_metric_filter import GraphMetricFilter
        from ..models.graph_metric_order import GraphMetricOrder
        from ..models.graph_node_selector import GraphNodeSelector
        from ..models.graph_query_params import GraphQueryParams
        from ..models.pattern_step import PatternStep

        d = dict(src_dict)
        type_ = GraphQueryType(d.pop("type"))

        index_name = d.pop("index_name")

        _start_nodes = d.pop("start_nodes", UNSET)
        start_nodes: GraphNodeSelector | Unset
        if isinstance(_start_nodes, Unset):
            start_nodes = UNSET
        else:
            start_nodes = GraphNodeSelector.from_dict(_start_nodes)

        _target_nodes = d.pop("target_nodes", UNSET)
        target_nodes: GraphNodeSelector | Unset
        if isinstance(_target_nodes, Unset):
            target_nodes = UNSET
        else:
            target_nodes = GraphNodeSelector.from_dict(_target_nodes)

        _params = d.pop("params", UNSET)
        params: GraphQueryParams | Unset
        if isinstance(_params, Unset):
            params = UNSET
        else:
            params = GraphQueryParams.from_dict(_params)

        _pattern = d.pop("pattern", UNSET)
        pattern: list[PatternStep] | Unset = UNSET
        if _pattern is not UNSET:
            pattern = []
            for pattern_item_data in _pattern:
                pattern_item = PatternStep.from_dict(pattern_item_data)

                pattern.append(pattern_item)

        return_aliases = cast(list[str], d.pop("return_aliases", UNSET))

        include_documents = d.pop("include_documents", UNSET)

        include_edges = d.pop("include_edges", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        metrics = cast(list[str], d.pop("metrics", UNSET))

        _order_by = d.pop("order_by", UNSET)
        order_by: list[GraphMetricOrder] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = GraphMetricOrder.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        _where_metric = d.pop("where_metric", UNSET)
        where_metric: list[GraphMetricFilter] | Unset = UNSET
        if _where_metric is not UNSET:
            where_metric = []
            for where_metric_item_data in _where_metric:
                where_metric_item = GraphMetricFilter.from_dict(where_metric_item_data)

                where_metric.append(where_metric_item)

        _metric_freshness = d.pop("metric_freshness", UNSET)
        metric_freshness: GraphQueryMetricFreshness | Unset
        if isinstance(_metric_freshness, Unset):
            metric_freshness = UNSET
        else:
            metric_freshness = GraphQueryMetricFreshness(_metric_freshness)

        include_metric_status = d.pop("include_metric_status", UNSET)

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
            metrics=metrics,
            order_by=order_by,
            where_metric=where_metric,
            metric_freshness=metric_freshness,
            include_metric_status=include_metric_status,
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
