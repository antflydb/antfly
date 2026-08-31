from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.edge_direction import EdgeDirection
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_document_bool_field_filter import GraphDocumentBoolFieldFilter
    from ..models.graph_document_date_range_filter import GraphDocumentDateRangeFilter
    from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
    from ..models.graph_document_filter_conjunction import GraphDocumentFilterConjunction
    from ..models.graph_document_filter_disjunction import GraphDocumentFilterDisjunction
    from ..models.graph_document_fuzzy_filter import GraphDocumentFuzzyFilter
    from ..models.graph_document_ids_filter import GraphDocumentIdsFilter
    from ..models.graph_document_match_all_filter import GraphDocumentMatchAllFilter
    from ..models.graph_document_match_none_filter import GraphDocumentMatchNoneFilter
    from ..models.graph_document_numeric_range_filter import GraphDocumentNumericRangeFilter
    from ..models.graph_document_prefix_filter import GraphDocumentPrefixFilter
    from ..models.graph_document_regexp_filter import GraphDocumentRegexpFilter
    from ..models.graph_document_term_filter import GraphDocumentTermFilter
    from ..models.graph_document_term_range_filter import GraphDocumentTermRangeFilter
    from ..models.graph_document_wildcard_filter import GraphDocumentWildcardFilter
    from ..models.graph_edge_weight_range import GraphEdgeWeightRange
    from ..models.graph_identity_node_selector import GraphIdentityNodeSelector
    from ..models.graph_key_node_selector import GraphKeyNodeSelector
    from ..models.graph_result_ref_node_selector import GraphResultRefNodeSelector


T = TypeVar("T", bound="GraphTraversal")


@_attrs_define
class GraphTraversal:
    """Breadth-first traversal with request-wide deduplication by exact table-qualified node identity. Direction defaults
    to `out`; use `both` to traverse a relationship as undirected without storing a reciprocal edge.

        Attributes:
            start (GraphIdentityNodeSelector | GraphKeyNodeSelector | GraphResultRefNodeSelector): Select graph nodes using
                exactly one explicit, exact selector form.
            direction (EdgeDirection | Unset): Direction of edges to query:
                - out: Outgoing edges from the node
                - in: Incoming edges to the node
                - both: Both outgoing and incoming edges
            edge_types (list[str] | Unset): At most 64 unique edge types totaling at most 64 KiB.
            max_depth (int | Unset): Maximum traversal depth. Defaults to one hop to keep fan-out explicit. Default: 1.
            edge_weight (GraphEdgeWeightRange | Unset): Inclusive per-edge weight filter. At least one bound is required.
                Bounds must be finite and non-negative; when both are present, min must not exceed max. This filters individual
                stored edges and does not constrain the aggregate path objective.
            limit (int | Unset):  Default: 100.
            include_paths (bool | Unset):  Default: False.
            include_documents (bool | Unset): Include each result node's stored document when it exists at the pinned
                snapshot. A dangling graph identity omits document. When false, document is always omitted. Default: False.
            fields (list[str] | Unset): Requires include_documents=true. Omit to include all document fields.
            filter_ (GraphDocumentBoolFieldFilter | GraphDocumentDateRangeFilter | GraphDocumentFilterBoolean |
                GraphDocumentFilterConjunction | GraphDocumentFilterDisjunction | GraphDocumentFuzzyFilter |
                GraphDocumentIdsFilter | GraphDocumentMatchAllFilter | GraphDocumentMatchNoneFilter |
                GraphDocumentNumericRangeFilter | GraphDocumentPrefixFilter | GraphDocumentRegexpFilter |
                GraphDocumentTermFilter | GraphDocumentTermRangeFilter | GraphDocumentWildcardFilter | Unset): A non-scoring
                stored-document predicate embedded at a graph node. It uses structurally distinct stored-field predicates and
                deliberately excludes analyzer-backed full-text clauses such as match, phrase, multi_match, and query_string.
                Fuzzy predicates require an explicit fuzziness. Range predicates use numeric_range, term_range, or date_range
                wrappers, and every stored value is addressed by an RFC 6901 JSON Pointer in `path`. Alias-to-alias predicates
                belong in GraphMatch.where.
    """

    start: GraphIdentityNodeSelector | GraphKeyNodeSelector | GraphResultRefNodeSelector
    direction: EdgeDirection | Unset = UNSET
    edge_types: list[str] | Unset = UNSET
    max_depth: int | Unset = 1
    edge_weight: GraphEdgeWeightRange | Unset = UNSET
    limit: int | Unset = 100
    include_paths: bool | Unset = False
    include_documents: bool | Unset = False
    fields: list[str] | Unset = UNSET
    filter_: (
        GraphDocumentBoolFieldFilter
        | GraphDocumentDateRangeFilter
        | GraphDocumentFilterBoolean
        | GraphDocumentFilterConjunction
        | GraphDocumentFilterDisjunction
        | GraphDocumentFuzzyFilter
        | GraphDocumentIdsFilter
        | GraphDocumentMatchAllFilter
        | GraphDocumentMatchNoneFilter
        | GraphDocumentNumericRangeFilter
        | GraphDocumentPrefixFilter
        | GraphDocumentRegexpFilter
        | GraphDocumentTermFilter
        | GraphDocumentTermRangeFilter
        | GraphDocumentWildcardFilter
        | Unset
    ) = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_document_bool_field_filter import GraphDocumentBoolFieldFilter
        from ..models.graph_document_date_range_filter import GraphDocumentDateRangeFilter
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
        from ..models.graph_document_filter_conjunction import GraphDocumentFilterConjunction
        from ..models.graph_document_fuzzy_filter import GraphDocumentFuzzyFilter
        from ..models.graph_document_ids_filter import GraphDocumentIdsFilter
        from ..models.graph_document_match_all_filter import GraphDocumentMatchAllFilter
        from ..models.graph_document_match_none_filter import GraphDocumentMatchNoneFilter
        from ..models.graph_document_numeric_range_filter import GraphDocumentNumericRangeFilter
        from ..models.graph_document_prefix_filter import GraphDocumentPrefixFilter
        from ..models.graph_document_regexp_filter import GraphDocumentRegexpFilter
        from ..models.graph_document_term_filter import GraphDocumentTermFilter
        from ..models.graph_document_term_range_filter import GraphDocumentTermRangeFilter
        from ..models.graph_document_wildcard_filter import GraphDocumentWildcardFilter
        from ..models.graph_identity_node_selector import GraphIdentityNodeSelector
        from ..models.graph_key_node_selector import GraphKeyNodeSelector

        start: dict[str, Any]
        if isinstance(self.start, GraphKeyNodeSelector):
            start = self.start.to_dict()
        elif isinstance(self.start, GraphIdentityNodeSelector):
            start = self.start.to_dict()
        else:
            start = self.start.to_dict()

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        max_depth = self.max_depth

        edge_weight: dict[str, Any] | Unset = UNSET
        if not isinstance(self.edge_weight, Unset):
            edge_weight = self.edge_weight.to_dict()

        limit = self.limit

        include_paths = self.include_paths

        include_documents = self.include_documents

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        filter_: dict[str, Any] | Unset
        if isinstance(self.filter_, Unset):
            filter_ = UNSET
        elif isinstance(self.filter_, GraphDocumentFuzzyFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentTermFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentPrefixFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentRegexpFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentWildcardFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentNumericRangeFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentTermRangeFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentDateRangeFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentMatchAllFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentMatchNoneFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentIdsFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentBoolFieldFilter):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentFilterBoolean):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentFilterConjunction):
            filter_ = self.filter_.to_dict()
        else:
            filter_ = self.filter_.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "start": start,
            }
        )
        if direction is not UNSET:
            field_dict["direction"] = direction
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if edge_weight is not UNSET:
            field_dict["edge_weight"] = edge_weight
        if limit is not UNSET:
            field_dict["limit"] = limit
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
        from ..models.graph_document_bool_field_filter import GraphDocumentBoolFieldFilter
        from ..models.graph_document_date_range_filter import GraphDocumentDateRangeFilter
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
        from ..models.graph_document_filter_conjunction import GraphDocumentFilterConjunction
        from ..models.graph_document_filter_disjunction import GraphDocumentFilterDisjunction
        from ..models.graph_document_fuzzy_filter import GraphDocumentFuzzyFilter
        from ..models.graph_document_ids_filter import GraphDocumentIdsFilter
        from ..models.graph_document_match_all_filter import GraphDocumentMatchAllFilter
        from ..models.graph_document_match_none_filter import GraphDocumentMatchNoneFilter
        from ..models.graph_document_numeric_range_filter import GraphDocumentNumericRangeFilter
        from ..models.graph_document_prefix_filter import GraphDocumentPrefixFilter
        from ..models.graph_document_regexp_filter import GraphDocumentRegexpFilter
        from ..models.graph_document_term_filter import GraphDocumentTermFilter
        from ..models.graph_document_term_range_filter import GraphDocumentTermRangeFilter
        from ..models.graph_document_wildcard_filter import GraphDocumentWildcardFilter
        from ..models.graph_edge_weight_range import GraphEdgeWeightRange
        from ..models.graph_identity_node_selector import GraphIdentityNodeSelector
        from ..models.graph_key_node_selector import GraphKeyNodeSelector
        from ..models.graph_result_ref_node_selector import GraphResultRefNodeSelector

        d = dict(src_dict)

        def _parse_start(data: object) -> GraphIdentityNodeSelector | GraphKeyNodeSelector | GraphResultRefNodeSelector:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_node_selector_type_0 = GraphKeyNodeSelector.from_dict(data)

                return componentsschemas_graph_node_selector_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_node_selector_type_1 = GraphIdentityNodeSelector.from_dict(data)

                return componentsschemas_graph_node_selector_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_graph_node_selector_type_2 = GraphResultRefNodeSelector.from_dict(data)

            return componentsschemas_graph_node_selector_type_2

        start = _parse_start(d.pop("start"))

        _direction = d.pop("direction", UNSET)
        direction: EdgeDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        max_depth = d.pop("max_depth", UNSET)

        _edge_weight = d.pop("edge_weight", UNSET)
        edge_weight: GraphEdgeWeightRange | Unset
        if isinstance(_edge_weight, Unset):
            edge_weight = UNSET
        else:
            edge_weight = GraphEdgeWeightRange.from_dict(_edge_weight)

        limit = d.pop("limit", UNSET)

        include_paths = d.pop("include_paths", UNSET)

        include_documents = d.pop("include_documents", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        def _parse_filter_(
            data: object,
        ) -> (
            GraphDocumentBoolFieldFilter
            | GraphDocumentDateRangeFilter
            | GraphDocumentFilterBoolean
            | GraphDocumentFilterConjunction
            | GraphDocumentFilterDisjunction
            | GraphDocumentFuzzyFilter
            | GraphDocumentIdsFilter
            | GraphDocumentMatchAllFilter
            | GraphDocumentMatchNoneFilter
            | GraphDocumentNumericRangeFilter
            | GraphDocumentPrefixFilter
            | GraphDocumentRegexpFilter
            | GraphDocumentTermFilter
            | GraphDocumentTermRangeFilter
            | GraphDocumentWildcardFilter
            | Unset
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_0 = GraphDocumentFuzzyFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_1 = GraphDocumentTermFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_2 = GraphDocumentPrefixFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_3 = GraphDocumentRegexpFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_4 = GraphDocumentWildcardFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_5 = GraphDocumentNumericRangeFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_6 = GraphDocumentTermRangeFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_7 = GraphDocumentDateRangeFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_8 = GraphDocumentMatchAllFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_9 = GraphDocumentMatchNoneFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_10 = GraphDocumentIdsFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_11 = GraphDocumentBoolFieldFilter.from_dict(data)

                return componentsschemas_graph_document_filter_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_12 = GraphDocumentFilterBoolean.from_dict(data)

                return componentsschemas_graph_document_filter_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_13 = GraphDocumentFilterConjunction.from_dict(data)

                return componentsschemas_graph_document_filter_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_graph_document_filter_type_14 = GraphDocumentFilterDisjunction.from_dict(data)

            return componentsschemas_graph_document_filter_type_14

        filter_ = _parse_filter_(d.pop("filter", UNSET))

        graph_traversal = cls(
            start=start,
            direction=direction,
            edge_types=edge_types,
            max_depth=max_depth,
            edge_weight=edge_weight,
            limit=limit,
            include_paths=include_paths,
            include_documents=include_documents,
            fields=fields,
            filter_=filter_,
        )

        return graph_traversal
