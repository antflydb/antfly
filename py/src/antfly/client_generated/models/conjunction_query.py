from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.boolean_query import BooleanQuery
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
    from ..models.multi_phrase_query import MultiPhraseQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.phrase_query import PhraseQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.query_string_query import QueryStringQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="ConjunctionQuery")


@_attrs_define
class ConjunctionQuery:
    """
    Attributes:
        conjuncts (list[Union['BoolFieldQuery', 'BooleanQuery', 'ConjunctionQuery', 'DateRangeStringQuery',
            'DisjunctionQuery', 'DocIdQuery', 'FuzzyQuery', 'GeoBoundingBoxQuery', 'GeoBoundingPolygonQuery',
            'GeoDistanceQuery', 'GeoShapeQuery', 'IPRangeQuery', 'MatchAllQuery', 'MatchNoneQuery', 'MatchPhraseQuery',
            'MatchQuery', 'MultiPhraseQuery', 'NumericRangeQuery', 'PhraseQuery', 'PrefixQuery', 'QueryStringQuery',
            'RegexpQuery', 'TermQuery', 'TermRangeQuery', 'WildcardQuery']]):
        boost (Union[None, Unset, float]): A floating-point number used to decrease or increase the relevance scores of
            a query.
    """

    conjuncts: list[
        Union[
            "BoolFieldQuery",
            "BooleanQuery",
            "ConjunctionQuery",
            "DateRangeStringQuery",
            "DisjunctionQuery",
            "DocIdQuery",
            "FuzzyQuery",
            "GeoBoundingBoxQuery",
            "GeoBoundingPolygonQuery",
            "GeoDistanceQuery",
            "GeoShapeQuery",
            "IPRangeQuery",
            "MatchAllQuery",
            "MatchNoneQuery",
            "MatchPhraseQuery",
            "MatchQuery",
            "MultiPhraseQuery",
            "NumericRangeQuery",
            "PhraseQuery",
            "PrefixQuery",
            "QueryStringQuery",
            "RegexpQuery",
            "TermQuery",
            "TermRangeQuery",
            "WildcardQuery",
        ]
    ]
    boost: Union[None, Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
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
        from ..models.multi_phrase_query import MultiPhraseQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.phrase_query import PhraseQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.query_string_query import QueryStringQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        conjuncts = []
        for conjuncts_item_data in self.conjuncts:
            conjuncts_item: dict[str, Any]
            if isinstance(conjuncts_item_data, TermQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, MatchQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, MatchPhraseQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, PhraseQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, MultiPhraseQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, FuzzyQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, PrefixQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, RegexpQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, WildcardQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, QueryStringQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, NumericRangeQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, TermRangeQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, DateRangeStringQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, BooleanQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, ConjunctionQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, DisjunctionQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, MatchAllQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, MatchNoneQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, DocIdQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, BoolFieldQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, IPRangeQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GeoBoundingBoxQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GeoDistanceQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            elif isinstance(conjuncts_item_data, GeoBoundingPolygonQuery):
                conjuncts_item = conjuncts_item_data.to_dict()
            else:
                conjuncts_item = conjuncts_item_data.to_dict()

            conjuncts.append(conjuncts_item)

        boost: Union[None, Unset, float]
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "conjuncts": conjuncts,
            }
        )
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
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
        conjuncts = []
        _conjuncts = d.pop("conjuncts")
        for conjuncts_item_data in _conjuncts:

            def _parse_conjuncts_item(
                data: object,
            ) -> Union[
                "BoolFieldQuery",
                "BooleanQuery",
                "ConjunctionQuery",
                "DateRangeStringQuery",
                "DisjunctionQuery",
                "DocIdQuery",
                "FuzzyQuery",
                "GeoBoundingBoxQuery",
                "GeoBoundingPolygonQuery",
                "GeoDistanceQuery",
                "GeoShapeQuery",
                "IPRangeQuery",
                "MatchAllQuery",
                "MatchNoneQuery",
                "MatchPhraseQuery",
                "MatchQuery",
                "MultiPhraseQuery",
                "NumericRangeQuery",
                "PhraseQuery",
                "PrefixQuery",
                "QueryStringQuery",
                "RegexpQuery",
                "TermQuery",
                "TermRangeQuery",
                "WildcardQuery",
            ]:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_0 = TermQuery.from_dict(data)

                    return componentsschemas_query_type_0
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_1 = MatchQuery.from_dict(data)

                    return componentsschemas_query_type_1
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_2 = MatchPhraseQuery.from_dict(data)

                    return componentsschemas_query_type_2
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_3 = PhraseQuery.from_dict(data)

                    return componentsschemas_query_type_3
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_4 = MultiPhraseQuery.from_dict(data)

                    return componentsschemas_query_type_4
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_5 = FuzzyQuery.from_dict(data)

                    return componentsschemas_query_type_5
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_6 = PrefixQuery.from_dict(data)

                    return componentsschemas_query_type_6
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_7 = RegexpQuery.from_dict(data)

                    return componentsschemas_query_type_7
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_8 = WildcardQuery.from_dict(data)

                    return componentsschemas_query_type_8
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_9 = QueryStringQuery.from_dict(data)

                    return componentsschemas_query_type_9
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_10 = NumericRangeQuery.from_dict(data)

                    return componentsschemas_query_type_10
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_11 = TermRangeQuery.from_dict(data)

                    return componentsschemas_query_type_11
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_12 = DateRangeStringQuery.from_dict(data)

                    return componentsschemas_query_type_12
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_13 = BooleanQuery.from_dict(data)

                    return componentsschemas_query_type_13
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_14 = ConjunctionQuery.from_dict(data)

                    return componentsschemas_query_type_14
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_15 = DisjunctionQuery.from_dict(data)

                    return componentsschemas_query_type_15
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_16 = MatchAllQuery.from_dict(data)

                    return componentsschemas_query_type_16
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_17 = MatchNoneQuery.from_dict(data)

                    return componentsschemas_query_type_17
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_18 = DocIdQuery.from_dict(data)

                    return componentsschemas_query_type_18
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_19 = BoolFieldQuery.from_dict(data)

                    return componentsschemas_query_type_19
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_20 = IPRangeQuery.from_dict(data)

                    return componentsschemas_query_type_20
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_21 = GeoBoundingBoxQuery.from_dict(data)

                    return componentsschemas_query_type_21
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_22 = GeoDistanceQuery.from_dict(data)

                    return componentsschemas_query_type_22
                except:  # noqa: E722
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_23 = GeoBoundingPolygonQuery.from_dict(data)

                    return componentsschemas_query_type_23
                except:  # noqa: E722
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = GeoShapeQuery.from_dict(data)

                return componentsschemas_query_type_24

            conjuncts_item = _parse_conjuncts_item(conjuncts_item_data)

            conjuncts.append(conjuncts_item)

        def _parse_boost(data: object) -> Union[None, Unset, float]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, float], data)

        boost = _parse_boost(d.pop("boost", UNSET))

        conjunction_query = cls(
            conjuncts=conjuncts,
            boost=boost,
        )

        conjunction_query.additional_properties = d
        return conjunction_query

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
