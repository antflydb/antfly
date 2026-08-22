from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_document_bool_field_filter import GraphDocumentBoolFieldFilter
    from ..models.graph_document_date_range_filter import GraphDocumentDateRangeFilter
    from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
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


T = TypeVar("T", bound="GraphDocumentFilterConjunction")


@_attrs_define
class GraphDocumentFilterConjunction:
    """
    Attributes:
        conjuncts (list[GraphDocumentBoolFieldFilter | GraphDocumentDateRangeFilter | GraphDocumentFilterBoolean |
            GraphDocumentFilterConjunction | GraphDocumentFilterDisjunction | GraphDocumentFuzzyFilter |
            GraphDocumentIdsFilter | GraphDocumentMatchAllFilter | GraphDocumentMatchNoneFilter |
            GraphDocumentNumericRangeFilter | GraphDocumentPrefixFilter | GraphDocumentRegexpFilter |
            GraphDocumentTermFilter | GraphDocumentTermRangeFilter | GraphDocumentWildcardFilter]):
    """

    conjuncts: list[
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

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_document_bool_field_filter import GraphDocumentBoolFieldFilter
        from ..models.graph_document_date_range_filter import GraphDocumentDateRangeFilter
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
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

        conjuncts = []
        for conjuncts_item_data in self.conjuncts:
            conjuncts_item: dict[str, Any]
            if isinstance(conjuncts_item_data, GraphDocumentFuzzyFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentTermFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentPrefixFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentRegexpFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentWildcardFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentNumericRangeFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentTermRangeFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentDateRangeFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentMatchAllFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentMatchNoneFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentIdsFilter):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GraphDocumentBoolFieldFilter):
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
        from ..models.graph_document_bool_field_filter import GraphDocumentBoolFieldFilter
        from ..models.graph_document_date_range_filter import GraphDocumentDateRangeFilter
        from ..models.graph_document_filter_boolean import GraphDocumentFilterBoolean
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

        d = dict(src_dict)
        conjuncts = []
        _conjuncts = d.pop("conjuncts")
        for conjuncts_item_data in _conjuncts:

            def _parse_conjuncts_item(
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

            conjuncts_item = _parse_conjuncts_item(conjuncts_item_data)

            conjuncts.append(conjuncts_item)

        graph_document_filter_conjunction = cls(
            conjuncts=conjuncts,
        )

        return graph_document_filter_conjunction
