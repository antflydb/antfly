from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.boolean_query import BooleanQuery
    from ..models.conjunction_query import ConjunctionQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.disjunction_query import DisjunctionQuery
    from ..models.doc_id_query import DocIdQuery
    from ..models.fuzzy_query import FuzzyQuery
    from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
    from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
    from ..models.geo_distance_query import GeoDistanceQuery
    from ..models.geo_shape_query import GeoShapeQuery
    from ..models.ip_range_query import IPRangeQuery
    from ..models.match_all_query import MatchAllQuery
    from ..models.match_none_query import MatchNoneQuery
    from ..models.match_phrase_query import MatchPhraseQuery
    from ..models.match_query import MatchQuery
    from ..models.multi_match_query import MultiMatchQuery
    from ..models.multi_phrase_query import MultiPhraseQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.phrase_query import PhraseQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.query_string_query import QueryStringQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="ScanKeysRequest")


@_attrs_define
class ScanKeysRequest:
    """Request to scan keys in a table within a key range.
    If no range is specified, scans all keys in the table.

        Attributes:
            from_ (str | Unset): Start of the key range to scan (exclusive by default).
                Can be a full key or a prefix. If not specified, starts from
                the beginning of the table.
                 Example: user:100.
            to (str | Unset): End of the key range to scan (inclusive by default).
                Can be a full key or a prefix. If not specified, scans to
                the end of the table.
                 Example: user:200.
            inclusive_from (bool | Unset): If true, include keys matching 'from' in the results.
                Default: false (exclusive lower bound for pagination).
                 Default: False.
            exclusive_to (bool | Unset): If true, exclude keys matching 'to' from the results.
                Default: false (inclusive upper bound).
                 Default: False.
            fields (list[str] | Unset): List of fields to include in each result. If not specified,
                only returns the key. Supports:
                - Simple fields: "title", "author"
                - Nested paths: "user.address.city"
                - Wildcards: "_chunks.*"
                - Exclusions: "-_chunks.*._embedding"
                - Special fields: "_embeddings", "_summaries", "_chunks"
                 Example: ['title', 'author', 'metadata.tags'].
            filter_query (BooleanQuery | BoolFieldQuery | ConjunctionQuery | DateRangeStringQuery | DisjunctionQuery |
                DocIdQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery |
                IPRangeQuery | MatchAllQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery |
                MultiPhraseQuery | NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery |
                TermRangeQuery | Unset | WildcardQuery):
            limit (int | Unset): Maximum number of results to return. If not specified, returns all
                matching keys in the range. Useful for pagination or sampling.
                 Example: 100.
    """

    from_: str | Unset = UNSET
    to: str | Unset = UNSET
    inclusive_from: bool | Unset = False
    exclusive_to: bool | Unset = False
    fields: list[str] | Unset = UNSET
    filter_query: (
        BooleanQuery
        | BoolFieldQuery
        | ConjunctionQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | MatchAllQuery
        | MatchNoneQuery
        | MatchPhraseQuery
        | MatchQuery
        | MultiMatchQuery
        | MultiPhraseQuery
        | NumericRangeQuery
        | PhraseQuery
        | PrefixQuery
        | QueryStringQuery
        | RegexpQuery
        | TermQuery
        | TermRangeQuery
        | Unset
        | WildcardQuery
    ) = UNSET
    limit: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
        from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
        from ..models.geo_distance_query import GeoDistanceQuery
        from ..models.ip_range_query import IPRangeQuery
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.match_phrase_query import MatchPhraseQuery
        from ..models.match_query import MatchQuery
        from ..models.multi_match_query import MultiMatchQuery
        from ..models.multi_phrase_query import MultiPhraseQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.phrase_query import PhraseQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.query_string_query import QueryStringQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        from_ = self.from_

        to = self.to

        inclusive_from = self.inclusive_from

        exclusive_to = self.exclusive_to

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        filter_query: dict[str, Any] | Unset
        if isinstance(self.filter_query, Unset):
            filter_query = UNSET
        elif isinstance(self.filter_query, TermQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MultiMatchQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchPhraseQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, PhraseQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MultiPhraseQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, FuzzyQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, PrefixQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, RegexpQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, WildcardQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, QueryStringQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, NumericRangeQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, TermRangeQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, DateRangeStringQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, BooleanQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, ConjunctionQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, DisjunctionQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchAllQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchNoneQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, DocIdQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, BoolFieldQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, IPRangeQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, GeoBoundingBoxQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, GeoDistanceQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, GeoBoundingPolygonQuery):
            filter_query = self.filter_query.to_dict()
        else:
            filter_query = self.filter_query.to_dict()

        limit = self.limit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if from_ is not UNSET:
            field_dict["from"] = from_
        if to is not UNSET:
            field_dict["to"] = to
        if inclusive_from is not UNSET:
            field_dict["inclusive_from"] = inclusive_from
        if exclusive_to is not UNSET:
            field_dict["exclusive_to"] = exclusive_to
        if fields is not UNSET:
            field_dict["fields"] = fields
        if filter_query is not UNSET:
            field_dict["filter_query"] = filter_query
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
        from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
        from ..models.geo_distance_query import GeoDistanceQuery
        from ..models.geo_shape_query import GeoShapeQuery
        from ..models.ip_range_query import IPRangeQuery
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.match_phrase_query import MatchPhraseQuery
        from ..models.match_query import MatchQuery
        from ..models.multi_match_query import MultiMatchQuery
        from ..models.multi_phrase_query import MultiPhraseQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.phrase_query import PhraseQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.query_string_query import QueryStringQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        d = dict(src_dict)
        from_ = d.pop("from", UNSET)

        to = d.pop("to", UNSET)

        inclusive_from = d.pop("inclusive_from", UNSET)

        exclusive_to = d.pop("exclusive_to", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        def _parse_filter_query(
            data: object,
        ) -> (
            BooleanQuery
            | BoolFieldQuery
            | ConjunctionQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchNoneQuery
            | MatchPhraseQuery
            | MatchQuery
            | MultiMatchQuery
            | MultiPhraseQuery
            | NumericRangeQuery
            | PhraseQuery
            | PrefixQuery
            | QueryStringQuery
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
                componentsschemas_query_type_0 = TermQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_type_25 = GeoShapeQuery.from_dict(data)

            return componentsschemas_query_type_25

        filter_query = _parse_filter_query(d.pop("filter_query", UNSET))

        limit = d.pop("limit", UNSET)

        scan_keys_request = cls(
            from_=from_,
            to=to,
            inclusive_from=inclusive_from,
            exclusive_to=exclusive_to,
            fields=fields,
            filter_query=filter_query,
            limit=limit,
        )

        scan_keys_request.additional_properties = d
        return scan_keys_request

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
