from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.doc_id_query import DocIdQuery
    from ..models.fuzzy_query import FuzzyQuery
    from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
    from ..models.graph_document_filter_disjunction import GraphDocumentFilterDisjunction
    from ..models.match_all_query import MatchAllQuery
    from ..models.match_none_query import MatchNoneQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="GraphDocumentFilterConjunction")


@_attrs_define
class GraphDocumentFilterConjunction:
    """
    Attributes:
        conjuncts (list[BoolFieldQuery | DateRangeStringQuery | DocIdQuery | FuzzyQuery | GraphDocumentFilterBoolean |
            GraphDocumentFilterConjunction | GraphDocumentFilterDisjunction | MatchAllQuery | MatchNoneQuery |
            NumericRangeQuery | PrefixQuery | RegexpQuery | TermQuery | TermRangeQuery | WildcardQuery]):
    """

    conjuncts: list[
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
        | WildcardQuery
    ]

    def to_dict(self) -> dict[str, Any]:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        conjuncts = []
        for conjuncts_item_data in self.conjuncts:
            conjuncts_item: dict[str, Any]
            if isinstance(conjuncts_item_data, TermQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, FuzzyQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, PrefixQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, RegexpQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, WildcardQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, NumericRangeQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, TermRangeQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, DateRangeStringQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, MatchAllQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, MatchNoneQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, DocIdQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, BoolFieldQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentFilterBoolean):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentFilterConjunction):
                conjuncts_item = conjuncts_item_data.to_dict()
            else:
                conjuncts_item = conjuncts_item_data.to_dict()

            conjuncts.append(conjuncts_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "conjuncts": conjuncts,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
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
        conjuncts = []
        _conjuncts = d.pop("conjuncts")
        for conjuncts_item_data in _conjuncts:

            def _parse_conjuncts_item(
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
                | WildcardQuery
            ):
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

            conjuncts_item = _parse_conjuncts_item(conjuncts_item_data)

            conjuncts.append(conjuncts_item)

        graph_document_filter_conjunction = cls(
            conjuncts=conjuncts,
        )

        return graph_document_filter_conjunction
