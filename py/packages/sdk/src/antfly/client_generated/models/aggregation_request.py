from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.aggregation_type import AggregationType
from ..models.calendar_interval import CalendarInterval
from ..models.cardinality_mode import CardinalityMode
from ..models.distance_unit import DistanceUnit
from ..models.significance_algorithm import SignificanceAlgorithm
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.aggregation_date_range import AggregationDateRange
    from ..models.aggregation_range import AggregationRange
    from ..models.aggregation_request_sub_aggregations import AggregationRequestSubAggregations
    from ..models.algebraic_aggregation_join import AlgebraicAggregationJoin
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.boolean_query import BooleanQuery
    from ..models.conjunction_query import ConjunctionQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.disjunction_query import DisjunctionQuery
    from ..models.distance_range import DistanceRange
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


T = TypeVar("T", bound="AggregationRequest")


@_attrs_define
class AggregationRequest:
    """
    Attributes:
        type_ (AggregationType): Type of aggregation to compute:
            - Metrics: sum, avg, min, max, count, sumsquares, stats, cardinality
            - Bucketing: terms, range, date_range, histogram, date_histogram
            - Geo: geohash_grid, geo_distance
            - Analytics: significant_terms
        field (str | Unset): Field to aggregate on. Required unless `fields` is supplied for a multi-field terms
            aggregation.
        mode (CardinalityMode | Unset): Exact-vs-approximate selection for a cardinality aggregation:
            - auto: use a materialized HyperLogLog sketch when one applies and is
              current, else an exact distinct scan (default).
            - exact: always compute an exact distinct count.
            - approximate: require a matching sketch; error if none applies.
        fields (list[str] | Unset): Ordered field list for multi-field terms aggregations. Each bucket key is a JSON
            string containing the serialized value array in the same order; clients should parse bucket.key as JSON.
        size (int | Unset): Maximum number of buckets to return (for bucketing aggregations) Example: 10.
        ranges (list[AggregationRange] | Unset): Ranges for range aggregations
        date_ranges (list[AggregationDateRange] | Unset): Date ranges for date_range aggregations
        interval (float | Unset): Fixed interval for histogram aggregations
        calendar_interval (CalendarInterval | Unset): Calendar-aware interval for date_histogram aggregations
        origin (str | Unset): Origin for geohash_grid aggregation (format: "lat,lon")
            Example: "37.7749,-122.4194"
        precision (int | Unset): Geohash precision (1-12) for geohash_grid aggregations
        distance_ranges (list[DistanceRange] | Unset): Distance ranges for geo_distance aggregations
        unit (DistanceUnit | Unset): Distance unit for geo aggregations:
            - m: meters
            - km: kilometers
            - mi: miles
            - ft: feet
            - yd: yards
        min_doc_count (int | Unset): Minimum document count for a bucket to be included Example: 1.
        background_filter (BooleanQuery | BoolFieldQuery | ConjunctionQuery | DateRangeStringQuery | DisjunctionQuery |
            DocIdQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery |
            IPRangeQuery | MatchAllQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery |
            MultiPhraseQuery | NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery |
            TermRangeQuery | Unset | WildcardQuery):
        algorithm (SignificanceAlgorithm | Unset): Algorithm for computing term significance:
            - jlh: JLH algorithm (default)
            - mutual_information: Mutual Information
            - chi_squared: Chi-squared test
            - percentage: Simple percentage comparison
        algebraic_join (AlgebraicAggregationJoin | Unset):
        sub_aggregations (AggregationRequestSubAggregations | Unset): Nested sub-aggregations
    """

    type_: AggregationType
    field: str | Unset = UNSET
    mode: CardinalityMode | Unset = UNSET
    fields: list[str] | Unset = UNSET
    size: int | Unset = UNSET
    ranges: list[AggregationRange] | Unset = UNSET
    date_ranges: list[AggregationDateRange] | Unset = UNSET
    interval: float | Unset = UNSET
    calendar_interval: CalendarInterval | Unset = UNSET
    origin: str | Unset = UNSET
    precision: int | Unset = UNSET
    distance_ranges: list[DistanceRange] | Unset = UNSET
    unit: DistanceUnit | Unset = UNSET
    min_doc_count: int | Unset = UNSET
    background_filter: (
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
    algorithm: SignificanceAlgorithm | Unset = UNSET
    algebraic_join: AlgebraicAggregationJoin | Unset = UNSET
    sub_aggregations: AggregationRequestSubAggregations | Unset = UNSET
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

        type_ = self.type_.value

        field = self.field

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        size = self.size

        ranges: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ranges, Unset):
            ranges = []
            for ranges_item_data in self.ranges:
                ranges_item = ranges_item_data.to_dict()
                ranges.append(ranges_item)

        date_ranges: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.date_ranges, Unset):
            date_ranges = []
            for date_ranges_item_data in self.date_ranges:
                date_ranges_item = date_ranges_item_data.to_dict()
                date_ranges.append(date_ranges_item)

        interval = self.interval

        calendar_interval: str | Unset = UNSET
        if not isinstance(self.calendar_interval, Unset):
            calendar_interval = self.calendar_interval.value

        origin = self.origin

        precision = self.precision

        distance_ranges: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.distance_ranges, Unset):
            distance_ranges = []
            for distance_ranges_item_data in self.distance_ranges:
                distance_ranges_item = distance_ranges_item_data.to_dict()
                distance_ranges.append(distance_ranges_item)

        unit: str | Unset = UNSET
        if not isinstance(self.unit, Unset):
            unit = self.unit.value

        min_doc_count = self.min_doc_count

        background_filter: dict[str, Any] | Unset
        if isinstance(self.background_filter, Unset):
            background_filter = UNSET
        elif isinstance(self.background_filter, TermQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, MatchQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, MultiMatchQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, MatchPhraseQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, PhraseQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, MultiPhraseQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, FuzzyQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, PrefixQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, RegexpQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, WildcardQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, QueryStringQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, NumericRangeQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, TermRangeQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, DateRangeStringQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, BooleanQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, ConjunctionQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, DisjunctionQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, MatchAllQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, MatchNoneQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, DocIdQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, BoolFieldQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, IPRangeQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, GeoBoundingBoxQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, GeoDistanceQuery):
            background_filter = self.background_filter.to_dict()
        elif isinstance(self.background_filter, GeoBoundingPolygonQuery):
            background_filter = self.background_filter.to_dict()
        else:
            background_filter = self.background_filter.to_dict()

        algorithm: str | Unset = UNSET
        if not isinstance(self.algorithm, Unset):
            algorithm = self.algorithm.value

        algebraic_join: dict[str, Any] | Unset = UNSET
        if not isinstance(self.algebraic_join, Unset):
            algebraic_join = self.algebraic_join.to_dict()

        sub_aggregations: dict[str, Any] | Unset = UNSET
        if not isinstance(self.sub_aggregations, Unset):
            sub_aggregations = self.sub_aggregations.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if mode is not UNSET:
            field_dict["mode"] = mode
        if fields is not UNSET:
            field_dict["fields"] = fields
        if size is not UNSET:
            field_dict["size"] = size
        if ranges is not UNSET:
            field_dict["ranges"] = ranges
        if date_ranges is not UNSET:
            field_dict["date_ranges"] = date_ranges
        if interval is not UNSET:
            field_dict["interval"] = interval
        if calendar_interval is not UNSET:
            field_dict["calendar_interval"] = calendar_interval
        if origin is not UNSET:
            field_dict["origin"] = origin
        if precision is not UNSET:
            field_dict["precision"] = precision
        if distance_ranges is not UNSET:
            field_dict["distance_ranges"] = distance_ranges
        if unit is not UNSET:
            field_dict["unit"] = unit
        if min_doc_count is not UNSET:
            field_dict["min_doc_count"] = min_doc_count
        if background_filter is not UNSET:
            field_dict["background_filter"] = background_filter
        if algorithm is not UNSET:
            field_dict["algorithm"] = algorithm
        if algebraic_join is not UNSET:
            field_dict["algebraic_join"] = algebraic_join
        if sub_aggregations is not UNSET:
            field_dict["sub_aggregations"] = sub_aggregations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.aggregation_date_range import AggregationDateRange
        from ..models.aggregation_range import AggregationRange
        from ..models.aggregation_request_sub_aggregations import AggregationRequestSubAggregations
        from ..models.algebraic_aggregation_join import AlgebraicAggregationJoin
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.distance_range import DistanceRange
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
        type_ = AggregationType(d.pop("type"))

        field = d.pop("field", UNSET)

        _mode = d.pop("mode", UNSET)
        mode: CardinalityMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = CardinalityMode(_mode)

        fields = cast(list[str], d.pop("fields", UNSET))

        size = d.pop("size", UNSET)

        _ranges = d.pop("ranges", UNSET)
        ranges: list[AggregationRange] | Unset = UNSET
        if _ranges is not UNSET:
            ranges = []
            for ranges_item_data in _ranges:
                ranges_item = AggregationRange.from_dict(ranges_item_data)

                ranges.append(ranges_item)

        _date_ranges = d.pop("date_ranges", UNSET)
        date_ranges: list[AggregationDateRange] | Unset = UNSET
        if _date_ranges is not UNSET:
            date_ranges = []
            for date_ranges_item_data in _date_ranges:
                date_ranges_item = AggregationDateRange.from_dict(date_ranges_item_data)

                date_ranges.append(date_ranges_item)

        interval = d.pop("interval", UNSET)

        _calendar_interval = d.pop("calendar_interval", UNSET)
        calendar_interval: CalendarInterval | Unset
        if isinstance(_calendar_interval, Unset):
            calendar_interval = UNSET
        else:
            calendar_interval = CalendarInterval(_calendar_interval)

        origin = d.pop("origin", UNSET)

        precision = d.pop("precision", UNSET)

        _distance_ranges = d.pop("distance_ranges", UNSET)
        distance_ranges: list[DistanceRange] | Unset = UNSET
        if _distance_ranges is not UNSET:
            distance_ranges = []
            for distance_ranges_item_data in _distance_ranges:
                distance_ranges_item = DistanceRange.from_dict(distance_ranges_item_data)

                distance_ranges.append(distance_ranges_item)

        _unit = d.pop("unit", UNSET)
        unit: DistanceUnit | Unset
        if isinstance(_unit, Unset):
            unit = UNSET
        else:
            unit = DistanceUnit(_unit)

        min_doc_count = d.pop("min_doc_count", UNSET)

        def _parse_background_filter(
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

        background_filter = _parse_background_filter(d.pop("background_filter", UNSET))

        _algorithm = d.pop("algorithm", UNSET)
        algorithm: SignificanceAlgorithm | Unset
        if isinstance(_algorithm, Unset):
            algorithm = UNSET
        else:
            algorithm = SignificanceAlgorithm(_algorithm)

        _algebraic_join = d.pop("algebraic_join", UNSET)
        algebraic_join: AlgebraicAggregationJoin | Unset
        if isinstance(_algebraic_join, Unset):
            algebraic_join = UNSET
        else:
            algebraic_join = AlgebraicAggregationJoin.from_dict(_algebraic_join)

        _sub_aggregations = d.pop("sub_aggregations", UNSET)
        sub_aggregations: AggregationRequestSubAggregations | Unset
        if isinstance(_sub_aggregations, Unset):
            sub_aggregations = UNSET
        else:
            sub_aggregations = AggregationRequestSubAggregations.from_dict(_sub_aggregations)

        aggregation_request = cls(
            type_=type_,
            field=field,
            mode=mode,
            fields=fields,
            size=size,
            ranges=ranges,
            date_ranges=date_ranges,
            interval=interval,
            calendar_interval=calendar_interval,
            origin=origin,
            precision=precision,
            distance_ranges=distance_ranges,
            unit=unit,
            min_doc_count=min_doc_count,
            background_filter=background_filter,
            algorithm=algorithm,
            algebraic_join=algebraic_join,
            sub_aggregations=sub_aggregations,
        )

        aggregation_request.additional_properties = d
        return aggregation_request

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
