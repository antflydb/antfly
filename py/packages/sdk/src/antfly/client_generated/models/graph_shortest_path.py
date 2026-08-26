from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.path_weight_mode import PathWeightMode
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
    from ..models.graph_path_endpoint import GraphPathEndpoint


T = TypeVar("T", bound="GraphShortestPath")


@_attrs_define
class GraphShortestPath:
    """Find the best outgoing path from `from` to `to`.

    Attributes:
        from_ (GraphPathEndpoint):
        to (GraphPathEndpoint):
        edge_types (list[str] | Unset): At most 64 unique edge types totaling at most 64 KiB.
        max_depth (int | Unset):  Default: 10.
        min_weight (float | Unset):
        max_weight (float | Unset):
        weight_mode (PathWeightMode | Unset): Path weighting algorithm for pathfinding:
            - min_hops: Minimize number of edges
            - min_weight: Minimize sum of finite non-negative edge weights
            - max_weight: Maximize product of finite edge weights in [0,1]
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
        include_documents (bool | Unset): Include stored documents on terminal result nodes returned alongside the path.
            Default: False.
        fields (list[str] | Unset): Requires include_documents=true. Omit to include all document fields.
    """

    from_: GraphPathEndpoint
    to: GraphPathEndpoint
    edge_types: list[str] | Unset = UNSET
    max_depth: int | Unset = 10
    min_weight: float | Unset = UNSET
    max_weight: float | Unset = UNSET
    weight_mode: PathWeightMode | Unset = UNSET
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
    include_documents: bool | Unset = False
    fields: list[str] | Unset = UNSET

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

        from_ = self.from_.to_dict()

        to = self.to.to_dict()

        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        max_depth = self.max_depth

        min_weight = self.min_weight

        max_weight = self.max_weight

        weight_mode: str | Unset = UNSET
        if not isinstance(self.weight_mode, Unset):
            weight_mode = self.weight_mode.value

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
        from ..models.graph_path_endpoint import GraphPathEndpoint

        d = dict(src_dict)
        from_ = GraphPathEndpoint.from_dict(d.pop("from"))

        to = GraphPathEndpoint.from_dict(d.pop("to"))

        edge_types = cast(list[str], d.pop("edge_types", UNSET))

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

        include_documents = d.pop("include_documents", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        graph_shortest_path = cls(
            from_=from_,
            to=to,
            edge_types=edge_types,
            max_depth=max_depth,
            min_weight=min_weight,
            max_weight=max_weight,
            weight_mode=weight_mode,
            filter_=filter_,
            include_documents=include_documents,
            fields=fields,
        )

        return graph_shortest_path
