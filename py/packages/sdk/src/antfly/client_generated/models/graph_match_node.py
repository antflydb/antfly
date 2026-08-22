from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.doc_id_query import DocIdQuery
    from ..models.fuzzy_query import FuzzyQuery
    from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
    from ..models.graph_document_filter_conjunction import GraphDocumentFilterConjunction
    from ..models.graph_document_filter_disjunction import GraphDocumentFilterDisjunction
    from ..models.match_all_query import MatchAllQuery
    from ..models.match_none_query import MatchNoneQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="GraphMatchNode")


@_attrs_define
class GraphMatchNode:
    """
    Attributes:
        filter_ (BoolFieldQuery | DateRangeStringQuery | DocIdQuery | FuzzyQuery | GraphDocumentFilterBoolean |
            GraphDocumentFilterConjunction | GraphDocumentFilterDisjunction | MatchAllQuery | MatchNoneQuery |
            NumericRangeQuery | PrefixQuery | RegexpQuery | TermQuery | TermRangeQuery | Unset | WildcardQuery): A non-
            scoring stored-document predicate embedded at a graph node. It reuses the structured field-filter shapes from
            QueryRequest.filter_query, but deliberately excludes analyzer-backed full-text clauses such as match, phrase,
            multi_match, and query_string. Alias-to-alias predicates belong in GraphMatch.where.
    """

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

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if filter_ is not UNSET:
            field_dict["filter"] = filter_

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
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        d = dict(src_dict)

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

        graph_match_node = cls(
            filter_=filter_,
        )

        return graph_match_node
