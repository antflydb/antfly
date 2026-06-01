from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.bool_field_query import BoolFieldQuery
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


T = TypeVar("T", bound="BooleanQuery")


@_attrs_define
class BooleanQuery:
    """
    Attributes:
        must (ConjunctionQuery | Unset):
        should (DisjunctionQuery | Unset):
        must_not (DisjunctionQuery | Unset):
        filter_ (BooleanQuery | BoolFieldQuery | ConjunctionQuery | DateRangeStringQuery | DisjunctionQuery | DocIdQuery
            | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery | IPRangeQuery |
            MatchAllQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery |
            NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery |
            Unset | WildcardQuery):
        boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
            query.
    """

    must: ConjunctionQuery | Unset = UNSET
    should: DisjunctionQuery | Unset = UNSET
    must_not: DisjunctionQuery | Unset = UNSET
    filter_: (
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
    boost: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.bool_field_query import BoolFieldQuery
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

        must: dict[str, Any] | Unset = UNSET
        if not isinstance(self.must, Unset):
            must = self.must.to_dict()

        should: dict[str, Any] | Unset = UNSET
        if not isinstance(self.should, Unset):
            should = self.should.to_dict()

        must_not: dict[str, Any] | Unset = UNSET
        if not isinstance(self.must_not, Unset):
            must_not = self.must_not.to_dict()

        filter_: dict[str, Any] | Unset
        if isinstance(self.filter_, Unset):
            filter_ = UNSET
        elif isinstance(self.filter_, TermQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MatchQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MultiMatchQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MatchPhraseQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, PhraseQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MultiPhraseQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, FuzzyQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, PrefixQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, RegexpQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, WildcardQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, QueryStringQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, NumericRangeQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, TermRangeQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, DateRangeStringQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, BooleanQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, ConjunctionQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, DisjunctionQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MatchAllQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MatchNoneQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, DocIdQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, BoolFieldQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, IPRangeQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GeoBoundingBoxQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GeoDistanceQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, GeoBoundingPolygonQuery):
            filter_ = self.filter_.to_dict()
        else:
            filter_ = self.filter_.to_dict()

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if must is not UNSET:
            field_dict["must"] = must
        if should is not UNSET:
            field_dict["should"] = should
        if must_not is not UNSET:
            field_dict["must_not"] = must_not
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bool_field_query import BoolFieldQuery
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
        _must = d.pop("must", UNSET)
        must: ConjunctionQuery | Unset
        if isinstance(_must, Unset):
            must = UNSET
        else:
            must = ConjunctionQuery.from_dict(_must)

        _should = d.pop("should", UNSET)
        should: DisjunctionQuery | Unset
        if isinstance(_should, Unset):
            should = UNSET
        else:
            should = DisjunctionQuery.from_dict(_should)

        _must_not = d.pop("must_not", UNSET)
        must_not: DisjunctionQuery | Unset
        if isinstance(_must_not, Unset):
            must_not = UNSET
        else:
            must_not = DisjunctionQuery.from_dict(_must_not)

        def _parse_filter_(
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

        filter_ = _parse_filter_(d.pop("filter", UNSET))

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        boolean_query = cls(
            must=must,
            should=should,
            must_not=must_not,
            filter_=filter_,
            boost=boost,
        )

        boolean_query.additional_properties = d
        return boolean_query

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
