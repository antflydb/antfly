from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.edge_direction import EdgeDirection
from ..models.path_weight_mode import PathWeightMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.doc_id_query import DocIdQuery
    from ..models.fuzzy_query import FuzzyQuery
    from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
    from ..models.graph_document_filter_conjunction import GraphDocumentFilterConjunction
    from ..models.graph_document_filter_disjunction import GraphDocumentFilterDisjunction
    from ..models.graph_path_endpoint import GraphPathEndpoint
    from ..models.match_all_query import MatchAllQuery
    from ..models.match_none_query import MatchNoneQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="GraphShortestPath")


@_attrs_define
class GraphShortestPath:
    """
    Attributes:
        from_ (GraphPathEndpoint):
        to (GraphPathEndpoint):
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
        filter_ (BoolFieldQuery | DateRangeStringQuery | DocIdQuery | FuzzyQuery | GraphDocumentFilterBoolean |
            GraphDocumentFilterConjunction | GraphDocumentFilterDisjunction | MatchAllQuery | MatchNoneQuery |
            NumericRangeQuery | PrefixQuery | RegexpQuery | TermQuery | TermRangeQuery | Unset | WildcardQuery): A non-
            scoring stored-document predicate embedded at a graph node. It reuses the structured field-filter shapes from
            QueryRequest.filter_query, but deliberately excludes analyzer-backed full-text clauses such as match, phrase,
            multi_match, and query_string. Alias-to-alias predicates belong in GraphMatch.where.
        include_documents (bool | Unset): Include stored documents on nodes returned with the path. Default: False.
        fields (list[str] | Unset): Document fields to include when include_documents is true. Omit to include all
            fields.
    """

    from_: GraphPathEndpoint
    to: GraphPathEndpoint
    edge_types: list[str] | Unset = UNSET
    direction: EdgeDirection | Unset = UNSET
    max_depth: int | Unset = 10
    min_weight: float | Unset = UNSET
    max_weight: float | Unset = UNSET
    weight_mode: PathWeightMode | Unset = UNSET
    filter_: (
        BoolFieldQuery
        | DateRangeStringQuery
        | DocIdQuery
        | FuzzyQuery
        | GraphDocumentFilterBoolean
        | GraphDocumentFilterConjunction
        | GraphDocumentFilterDisjunction
        | MatchAllQuery
        | MatchNoneQuery
        | NumericRangeQuery
        | PrefixQuery
        | RegexpQuery
        | TermQuery
        | TermRangeQuery
        | Unset
        | WildcardQuery
    ) = UNSET
    include_documents: bool | Unset = False
    fields: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
        from ..models.graph_document_filter_conjunction import GraphDocumentFilterConjunction
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        from_ = self.from_.to_dict()

        to = self.to.to_dict()

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

        filter_: dict[str, Any] | Unset
        if isinstance(self.filter_, Unset):
            filter_ = UNSET
        elif isinstance(self.filter_, TermQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, FuzzyQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, PrefixQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, RegexpQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, WildcardQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, NumericRangeQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, TermRangeQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, DateRangeStringQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MatchAllQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MatchNoneQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, DocIdQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, BoolFieldQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentFilterBoolean):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GraphDocumentFilterConjunction):
            filter_ = self.filter_.to_dict()
        else:
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
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
        from ..models.graph_document_filter_conjunction import GraphDocumentFilterConjunction
        from ..models.graph_document_filter_disjunction import GraphDocumentFilterDisjunction
        from ..models.graph_path_endpoint import GraphPathEndpoint
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        d = dict(src_dict)
        from_ = GraphPathEndpoint.from_dict(d.pop("from"))

        to = GraphPathEndpoint.from_dict(d.pop("to"))

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

        def _parse_filter_(
            data: object,
        ) -> (
            BoolFieldQuery
            | DateRangeStringQuery
            | DocIdQuery
            | FuzzyQuery
            | GraphDocumentFilterBoolean
            | GraphDocumentFilterConjunction
            | GraphDocumentFilterDisjunction
            | MatchAllQuery
            | MatchNoneQuery
            | NumericRangeQuery
            | PrefixQuery
            | RegexpQuery
            | TermQuery
            | TermRangeQuery
            | Unset
            | WildcardQuery
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_0 = TermQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_1 = FuzzyQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_2 = PrefixQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_3 = RegexpQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_4 = WildcardQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_5 = NumericRangeQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_6 = TermRangeQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_7 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_8 = MatchAllQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_9 = MatchNoneQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_10 = DocIdQuery.from_dict(data)

                return componentsschemas_graph_document_filter_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_document_filter_type_11 = BoolFieldQuery.from_dict(data)

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

        include_documents = d.pop("include_documents", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        graph_shortest_path = cls(
            from_=from_,
            to=to,
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

        return graph_shortest_path
