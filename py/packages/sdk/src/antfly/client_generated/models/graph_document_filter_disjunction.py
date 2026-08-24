from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
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


T = TypeVar("T", bound="GraphDocumentFilterDisjunction")


@_attrs_define
class GraphDocumentFilterDisjunction:
    """
    Attributes:
        disjuncts (list[GraphDocumentBoolFieldFilter | GraphDocumentDateRangeFilter | GraphDocumentFilterBoolean |
            GraphDocumentFilterConjunction | GraphDocumentFilterDisjunction | GraphDocumentFuzzyFilter |
            GraphDocumentIdsFilter | GraphDocumentMatchAllFilter | GraphDocumentMatchNoneFilter |
            GraphDocumentNumericRangeFilter | GraphDocumentPrefixFilter | GraphDocumentRegexpFilter |
            GraphDocumentTermFilter | GraphDocumentTermRangeFilter | GraphDocumentWildcardFilter]):
        min_ (int | Unset): Minimum number of disjuncts that must match. Omit for conventional disjunction semantics;
            set to 0 to make a pure disjunction optional.
    """

    disjuncts: list[
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
    ]
    min_: int | Unset = UNSET

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

        disjuncts = []
        for disjuncts_item_data in self.disjuncts:
            disjuncts_item: dict[str, Any]
            if isinstance(disjuncts_item_data, GraphDocumentFuzzyFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentTermFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentPrefixFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentRegexpFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentWildcardFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentNumericRangeFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentTermRangeFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentDateRangeFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentMatchAllFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentMatchNoneFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentIdsFilter):
                disjuncts_item = disjuncts_item_data.to_dict()
            elif isinstance(disjuncts_item_data, GraphDocumentBoolFieldFilter):
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

        d = dict(src_dict)
        disjuncts = []
        _disjuncts = d.pop("disjuncts")
        for disjuncts_item_data in _disjuncts:

            def _parse_disjuncts_item(
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
            ):
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

            disjuncts_item = _parse_disjuncts_item(disjuncts_item_data)

            disjuncts.append(disjuncts_item)

        min_ = d.pop("min", UNSET)

        graph_document_filter_disjunction = cls(
            disjuncts=disjuncts,
            min_=min_,
        )

        return graph_document_filter_disjunction
