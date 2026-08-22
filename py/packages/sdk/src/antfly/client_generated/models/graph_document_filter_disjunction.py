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
    from ..models.match_all_query import MatchAllQuery
    from ..models.match_none_query import MatchNoneQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="GraphDocumentFilterDisjunction")


@_attrs_define
class GraphDocumentFilterDisjunction:
    """
    Attributes:
        disjuncts (list[BoolFieldQuery | DateRangeStringQuery | DocIdQuery | FuzzyQuery | GraphDocumentFilterBoolean |
            GraphDocumentFilterConjunction | GraphDocumentFilterDisjunction | MatchAllQuery | MatchNoneQuery |
            NumericRangeQuery | PrefixQuery | RegexpQuery | TermQuery | TermRangeQuery | WildcardQuery]):
        min_ (float | Unset):
    """

    disjuncts: list[
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
    min_: float | Unset = UNSET

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

        disjuncts = []
        for disjuncts_item_data in self.disjuncts:
            disjuncts_item: dict[str, Any]
            if isinstance(disjuncts_item_data, TermQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, FuzzyQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, PrefixQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, RegexpQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, WildcardQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, NumericRangeQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, TermRangeQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, DateRangeStringQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, MatchAllQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, MatchNoneQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, DocIdQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, BoolFieldQuery):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentFilterBoolean):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentFilterConjunction):
                disjuncts_item = disjuncts_item_data.to_dict()
            else:
                disjuncts_item = disjuncts_item_data.to_dict()

            disjuncts.append(disjuncts_item)

        min_ = self.min_

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "disjuncts": disjuncts,
            }
        )
        if min_ is not UNSET:
            field_dict["min"] = min_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
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

        d = dict(src_dict)
        disjuncts = []
        _disjuncts = d.pop("disjuncts")
        for disjuncts_item_data in _disjuncts:

            def _parse_disjuncts_item(
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

            disjuncts_item = _parse_disjuncts_item(disjuncts_item_data)

            disjuncts.append(disjuncts_item)

        min_ = d.pop("min", UNSET)

        graph_document_filter_disjunction = cls(
            disjuncts=disjuncts,
            min_=min_,
        )

        return graph_document_filter_disjunction
