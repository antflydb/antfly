from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.autocomplete_query import AutocompleteQuery
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.bool_query import BoolQuery
    from ..models.boolean_query import BooleanQuery
    from ..models.conjunction_query import ConjunctionQuery
    from ..models.contains_query import ContainsQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.disjunction_query import DisjunctionQuery
    from ..models.doc_id_query import DocIdQuery
    from ..models.equals_query import EqualsQuery
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
    from ..models.matches_query import MatchesQuery
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


T = TypeVar("T", bound="BoolQueryBody")


@_attrs_define
class BoolQueryBody:
    """
    Attributes:
        must (AutocompleteQuery | BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery |
            DateRangeStringQuery | DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery |
            GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | list[AutocompleteQuery |
            BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery | DateRangeStringQuery |
            DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery |
            GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | MatchAllQuery | MatchesQuery | MatchNoneQuery |
            MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery | NumericRangeQuery | PhraseQuery |
            PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery | WildcardQuery] | MatchAllQuery |
            MatchesQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery |
            NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery |
            Unset | WildcardQuery):
        should (AutocompleteQuery | BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery |
            DateRangeStringQuery | DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery |
            GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | list[AutocompleteQuery |
            BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery | DateRangeStringQuery |
            DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery |
            GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | MatchAllQuery | MatchesQuery | MatchNoneQuery |
            MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery | NumericRangeQuery | PhraseQuery |
            PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery | WildcardQuery] | MatchAllQuery |
            MatchesQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery |
            NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery |
            Unset | WildcardQuery):
        must_not (AutocompleteQuery | BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery |
            DateRangeStringQuery | DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery |
            GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | list[AutocompleteQuery |
            BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery | DateRangeStringQuery |
            DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery |
            GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | MatchAllQuery | MatchesQuery | MatchNoneQuery |
            MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery | NumericRangeQuery | PhraseQuery |
            PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery | WildcardQuery] | MatchAllQuery |
            MatchesQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery |
            NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery |
            Unset | WildcardQuery):
        filter_ (AutocompleteQuery | BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery |
            DateRangeStringQuery | DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery |
            GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | list[AutocompleteQuery |
            BooleanQuery | BoolFieldQuery | BoolQuery | ConjunctionQuery | ContainsQuery | DateRangeStringQuery |
            DisjunctionQuery | DocIdQuery | EqualsQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery |
            GeoDistanceQuery | GeoShapeQuery | IPRangeQuery | MatchAllQuery | MatchesQuery | MatchNoneQuery |
            MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery | NumericRangeQuery | PhraseQuery |
            PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery | WildcardQuery] | MatchAllQuery |
            MatchesQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery | MultiPhraseQuery |
            NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery | TermRangeQuery |
            Unset | WildcardQuery):
        minimum_should_match (int | Unset): Minimum optional clauses required; omission follows the runtime's required-
            clause defaults.
        boost (float | None | Unset): A floating-point number used to decrease or increase the relevance scores of a
            query.
    """

    must: (
        AutocompleteQuery
        | BooleanQuery
        | BoolFieldQuery
        | BoolQuery
        | ConjunctionQuery
        | ContainsQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | EqualsQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | list[
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchesQuery
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
            | WildcardQuery
        ]
        | MatchAllQuery
        | MatchesQuery
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
    should: (
        AutocompleteQuery
        | BooleanQuery
        | BoolFieldQuery
        | BoolQuery
        | ConjunctionQuery
        | ContainsQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | EqualsQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | list[
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchesQuery
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
            | WildcardQuery
        ]
        | MatchAllQuery
        | MatchesQuery
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
    must_not: (
        AutocompleteQuery
        | BooleanQuery
        | BoolFieldQuery
        | BoolQuery
        | ConjunctionQuery
        | ContainsQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | EqualsQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | list[
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchesQuery
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
            | WildcardQuery
        ]
        | MatchAllQuery
        | MatchesQuery
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
    filter_: (
        AutocompleteQuery
        | BooleanQuery
        | BoolFieldQuery
        | BoolQuery
        | ConjunctionQuery
        | ContainsQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | EqualsQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | list[
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchesQuery
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
            | WildcardQuery
        ]
        | MatchAllQuery
        | MatchesQuery
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
    minimum_should_match: int | Unset = UNSET
    boost: float | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.autocomplete_query import AutocompleteQuery
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.bool_query import BoolQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.contains_query import ContainsQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.equals_query import EqualsQuery
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
        from ..models.matches_query import MatchesQuery
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

        must: dict[str, Any] | list[dict[str, Any]] | Unset
        if isinstance(self.must, Unset):
            must = UNSET
        elif isinstance(self.must, EqualsQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, ContainsQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, MatchesQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, AutocompleteQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, TermQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, MatchQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, MultiMatchQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, MatchPhraseQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, PhraseQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, MultiPhraseQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, FuzzyQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, PrefixQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, RegexpQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, WildcardQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, QueryStringQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, NumericRangeQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, TermRangeQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, DateRangeStringQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, BoolQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, BooleanQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, ConjunctionQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, DisjunctionQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, MatchAllQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, MatchNoneQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, DocIdQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, BoolFieldQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, IPRangeQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, GeoBoundingBoxQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, GeoDistanceQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, GeoBoundingPolygonQuery):
            must = self.must.to_dict()
        elif isinstance(self.must, GeoShapeQuery):
            must = self.must.to_dict()
        else:
            must = []
            for componentsschemas_bool_query_clause_type_1_item_data in self.must:
                componentsschemas_bool_query_clause_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_bool_query_clause_type_1_item_data, EqualsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ContainsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchesQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, AutocompleteQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiMatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, FuzzyQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PrefixQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, RegexpQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, WildcardQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, QueryStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, NumericRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DateRangeStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BooleanQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ConjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DisjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchAllQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchNoneQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DocIdQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolFieldQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, IPRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingBoxQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoDistanceQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingPolygonQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )

                must.append(componentsschemas_bool_query_clause_type_1_item)

        should: dict[str, Any] | list[dict[str, Any]] | Unset
        if isinstance(self.should, Unset):
            should = UNSET
        elif isinstance(self.should, EqualsQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, ContainsQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, MatchesQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, AutocompleteQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, TermQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, MatchQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, MultiMatchQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, MatchPhraseQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, PhraseQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, MultiPhraseQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, FuzzyQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, PrefixQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, RegexpQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, WildcardQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, QueryStringQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, NumericRangeQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, TermRangeQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, DateRangeStringQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, BoolQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, BooleanQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, ConjunctionQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, DisjunctionQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, MatchAllQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, MatchNoneQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, DocIdQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, BoolFieldQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, IPRangeQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, GeoBoundingBoxQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, GeoDistanceQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, GeoBoundingPolygonQuery):
            should = self.should.to_dict()
        elif isinstance(self.should, GeoShapeQuery):
            should = self.should.to_dict()
        else:
            should = []
            for componentsschemas_bool_query_clause_type_1_item_data in self.should:
                componentsschemas_bool_query_clause_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_bool_query_clause_type_1_item_data, EqualsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ContainsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchesQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, AutocompleteQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiMatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, FuzzyQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PrefixQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, RegexpQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, WildcardQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, QueryStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, NumericRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DateRangeStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BooleanQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ConjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DisjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchAllQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchNoneQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DocIdQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolFieldQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, IPRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingBoxQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoDistanceQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingPolygonQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )

                should.append(componentsschemas_bool_query_clause_type_1_item)

        must_not: dict[str, Any] | list[dict[str, Any]] | Unset
        if isinstance(self.must_not, Unset):
            must_not = UNSET
        elif isinstance(self.must_not, EqualsQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, ContainsQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, MatchesQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, AutocompleteQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, TermQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, MatchQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, MultiMatchQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, MatchPhraseQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, PhraseQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, MultiPhraseQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, FuzzyQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, PrefixQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, RegexpQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, WildcardQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, QueryStringQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, NumericRangeQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, TermRangeQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, DateRangeStringQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, BoolQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, BooleanQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, ConjunctionQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, DisjunctionQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, MatchAllQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, MatchNoneQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, DocIdQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, BoolFieldQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, IPRangeQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, GeoBoundingBoxQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, GeoDistanceQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, GeoBoundingPolygonQuery):
            must_not = self.must_not.to_dict()
        elif isinstance(self.must_not, GeoShapeQuery):
            must_not = self.must_not.to_dict()
        else:
            must_not = []
            for componentsschemas_bool_query_clause_type_1_item_data in self.must_not:
                componentsschemas_bool_query_clause_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_bool_query_clause_type_1_item_data, EqualsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ContainsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchesQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, AutocompleteQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiMatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, FuzzyQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PrefixQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, RegexpQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, WildcardQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, QueryStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, NumericRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DateRangeStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BooleanQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ConjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DisjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchAllQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchNoneQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DocIdQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolFieldQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, IPRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingBoxQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoDistanceQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingPolygonQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )

                must_not.append(componentsschemas_bool_query_clause_type_1_item)

        filter_: dict[str, Any] | list[dict[str, Any]] | Unset
        if isinstance(self.filter_, Unset):
            filter_ = UNSET
        elif isinstance(self.filter_, EqualsQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, ContainsQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, MatchesQuery):
            filter_ = self.filter_.to_dict()
        elif isinstance(self.filter_, AutocompleteQuery):
            filter_ = self.filter_.to_dict()
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
        elif isinstance(self.filter_, BoolQuery):
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
        elif isinstance(self.filter_, GeoShapeQuery):
            filter_ = self.filter_.to_dict()
        else:
            filter_ = []
            for componentsschemas_bool_query_clause_type_1_item_data in self.filter_:
                componentsschemas_bool_query_clause_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_bool_query_clause_type_1_item_data, EqualsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ContainsQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchesQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, AutocompleteQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiMatchQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MultiPhraseQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, FuzzyQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, PrefixQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, RegexpQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, WildcardQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, QueryStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, NumericRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, TermRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DateRangeStringQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BooleanQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, ConjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DisjunctionQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchAllQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, MatchNoneQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, DocIdQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, BoolFieldQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, IPRangeQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingBoxQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoDistanceQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_bool_query_clause_type_1_item_data, GeoBoundingPolygonQuery):
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_bool_query_clause_type_1_item = (
                        componentsschemas_bool_query_clause_type_1_item_data.to_dict()
                    )

                filter_.append(componentsschemas_bool_query_clause_type_1_item)

        minimum_should_match = self.minimum_should_match

        boost: float | None | Unset
        if isinstance(self.boost, Unset):
            boost = UNSET
        else:
            boost = self.boost

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if must is not UNSET:
            field_dict["must"] = must
        if should is not UNSET:
            field_dict["should"] = should
        if must_not is not UNSET:
            field_dict["must_not"] = must_not
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if minimum_should_match is not UNSET:
            field_dict["minimum_should_match"] = minimum_should_match
        if boost is not UNSET:
            field_dict["boost"] = boost

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.autocomplete_query import AutocompleteQuery
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.bool_query import BoolQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.contains_query import ContainsQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.equals_query import EqualsQuery
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
        from ..models.matches_query import MatchesQuery
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

        if set(src_dict) - {
            "must",
            "should",
            "must_not",
            "filter",
            "minimum_should_match",
            "boost",
        }:
            raise ValueError("Unexpected properties for BoolQueryBody")
        d = dict(src_dict)

        def _parse_must(
            data: object,
        ) -> (
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | list[
                AutocompleteQuery
                | BooleanQuery
                | BoolFieldQuery
                | BoolQuery
                | ConjunctionQuery
                | ContainsQuery
                | DateRangeStringQuery
                | DisjunctionQuery
                | DocIdQuery
                | EqualsQuery
                | FuzzyQuery
                | GeoBoundingBoxQuery
                | GeoBoundingPolygonQuery
                | GeoDistanceQuery
                | GeoShapeQuery
                | IPRangeQuery
                | MatchAllQuery
                | MatchesQuery
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
                | WildcardQuery
            ]
            | MatchAllQuery
            | MatchesQuery
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
                componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = TermQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_25
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_26
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_27
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_28
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_29
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                return componentsschemas_query_type_30
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, list):
                raise TypeError()
            componentsschemas_bool_query_clause_type_1 = []
            _componentsschemas_bool_query_clause_type_1 = data
            for componentsschemas_bool_query_clause_type_1_item_data in _componentsschemas_bool_query_clause_type_1:

                def _parse_componentsschemas_bool_query_clause_type_1_item(
                    data: object,
                ) -> (
                    AutocompleteQuery
                    | BooleanQuery
                    | BoolFieldQuery
                    | BoolQuery
                    | ConjunctionQuery
                    | ContainsQuery
                    | DateRangeStringQuery
                    | DisjunctionQuery
                    | DocIdQuery
                    | EqualsQuery
                    | FuzzyQuery
                    | GeoBoundingBoxQuery
                    | GeoBoundingPolygonQuery
                    | GeoDistanceQuery
                    | GeoShapeQuery
                    | IPRangeQuery
                    | MatchAllQuery
                    | MatchesQuery
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
                    | WildcardQuery
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                        return componentsschemas_query_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                        return componentsschemas_query_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                        return componentsschemas_query_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                        return componentsschemas_query_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_4 = TermQuery.from_dict(data)

                        return componentsschemas_query_type_4
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                        return componentsschemas_query_type_5
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                        return componentsschemas_query_type_6
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_7
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                        return componentsschemas_query_type_8
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_9
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                        return componentsschemas_query_type_10
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                        return componentsschemas_query_type_11
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                        return componentsschemas_query_type_12
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                        return componentsschemas_query_type_13
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                        return componentsschemas_query_type_14
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                        return componentsschemas_query_type_15
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                        return componentsschemas_query_type_16
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                        return componentsschemas_query_type_17
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                        return componentsschemas_query_type_18
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                        return componentsschemas_query_type_19
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_20
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_21
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                        return componentsschemas_query_type_22
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                        return componentsschemas_query_type_23
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                        return componentsschemas_query_type_24
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                        return componentsschemas_query_type_25
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                        return componentsschemas_query_type_26
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                        return componentsschemas_query_type_27
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                        return componentsschemas_query_type_28
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                        return componentsschemas_query_type_29
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                    return componentsschemas_query_type_30

                componentsschemas_bool_query_clause_type_1_item = (
                    _parse_componentsschemas_bool_query_clause_type_1_item(
                        componentsschemas_bool_query_clause_type_1_item_data
                    )
                )

                componentsschemas_bool_query_clause_type_1.append(componentsschemas_bool_query_clause_type_1_item)

            return componentsschemas_bool_query_clause_type_1

        must = _parse_must(d.pop("must", UNSET))

        def _parse_should(
            data: object,
        ) -> (
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | list[
                AutocompleteQuery
                | BooleanQuery
                | BoolFieldQuery
                | BoolQuery
                | ConjunctionQuery
                | ContainsQuery
                | DateRangeStringQuery
                | DisjunctionQuery
                | DocIdQuery
                | EqualsQuery
                | FuzzyQuery
                | GeoBoundingBoxQuery
                | GeoBoundingPolygonQuery
                | GeoDistanceQuery
                | GeoShapeQuery
                | IPRangeQuery
                | MatchAllQuery
                | MatchesQuery
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
                | WildcardQuery
            ]
            | MatchAllQuery
            | MatchesQuery
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
                componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = TermQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_25
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_26
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_27
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_28
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_29
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                return componentsschemas_query_type_30
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, list):
                raise TypeError()
            componentsschemas_bool_query_clause_type_1 = []
            _componentsschemas_bool_query_clause_type_1 = data
            for componentsschemas_bool_query_clause_type_1_item_data in _componentsschemas_bool_query_clause_type_1:

                def _parse_componentsschemas_bool_query_clause_type_1_item(
                    data: object,
                ) -> (
                    AutocompleteQuery
                    | BooleanQuery
                    | BoolFieldQuery
                    | BoolQuery
                    | ConjunctionQuery
                    | ContainsQuery
                    | DateRangeStringQuery
                    | DisjunctionQuery
                    | DocIdQuery
                    | EqualsQuery
                    | FuzzyQuery
                    | GeoBoundingBoxQuery
                    | GeoBoundingPolygonQuery
                    | GeoDistanceQuery
                    | GeoShapeQuery
                    | IPRangeQuery
                    | MatchAllQuery
                    | MatchesQuery
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
                    | WildcardQuery
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                        return componentsschemas_query_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                        return componentsschemas_query_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                        return componentsschemas_query_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                        return componentsschemas_query_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_4 = TermQuery.from_dict(data)

                        return componentsschemas_query_type_4
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                        return componentsschemas_query_type_5
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                        return componentsschemas_query_type_6
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_7
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                        return componentsschemas_query_type_8
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_9
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                        return componentsschemas_query_type_10
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                        return componentsschemas_query_type_11
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                        return componentsschemas_query_type_12
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                        return componentsschemas_query_type_13
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                        return componentsschemas_query_type_14
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                        return componentsschemas_query_type_15
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                        return componentsschemas_query_type_16
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                        return componentsschemas_query_type_17
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                        return componentsschemas_query_type_18
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                        return componentsschemas_query_type_19
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_20
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_21
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                        return componentsschemas_query_type_22
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                        return componentsschemas_query_type_23
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                        return componentsschemas_query_type_24
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                        return componentsschemas_query_type_25
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                        return componentsschemas_query_type_26
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                        return componentsschemas_query_type_27
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                        return componentsschemas_query_type_28
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                        return componentsschemas_query_type_29
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                    return componentsschemas_query_type_30

                componentsschemas_bool_query_clause_type_1_item = (
                    _parse_componentsschemas_bool_query_clause_type_1_item(
                        componentsschemas_bool_query_clause_type_1_item_data
                    )
                )

                componentsschemas_bool_query_clause_type_1.append(componentsschemas_bool_query_clause_type_1_item)

            return componentsschemas_bool_query_clause_type_1

        should = _parse_should(d.pop("should", UNSET))

        def _parse_must_not(
            data: object,
        ) -> (
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | list[
                AutocompleteQuery
                | BooleanQuery
                | BoolFieldQuery
                | BoolQuery
                | ConjunctionQuery
                | ContainsQuery
                | DateRangeStringQuery
                | DisjunctionQuery
                | DocIdQuery
                | EqualsQuery
                | FuzzyQuery
                | GeoBoundingBoxQuery
                | GeoBoundingPolygonQuery
                | GeoDistanceQuery
                | GeoShapeQuery
                | IPRangeQuery
                | MatchAllQuery
                | MatchesQuery
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
                | WildcardQuery
            ]
            | MatchAllQuery
            | MatchesQuery
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
                componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = TermQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_25
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_26
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_27
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_28
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_29
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                return componentsschemas_query_type_30
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, list):
                raise TypeError()
            componentsschemas_bool_query_clause_type_1 = []
            _componentsschemas_bool_query_clause_type_1 = data
            for componentsschemas_bool_query_clause_type_1_item_data in _componentsschemas_bool_query_clause_type_1:

                def _parse_componentsschemas_bool_query_clause_type_1_item(
                    data: object,
                ) -> (
                    AutocompleteQuery
                    | BooleanQuery
                    | BoolFieldQuery
                    | BoolQuery
                    | ConjunctionQuery
                    | ContainsQuery
                    | DateRangeStringQuery
                    | DisjunctionQuery
                    | DocIdQuery
                    | EqualsQuery
                    | FuzzyQuery
                    | GeoBoundingBoxQuery
                    | GeoBoundingPolygonQuery
                    | GeoDistanceQuery
                    | GeoShapeQuery
                    | IPRangeQuery
                    | MatchAllQuery
                    | MatchesQuery
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
                    | WildcardQuery
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                        return componentsschemas_query_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                        return componentsschemas_query_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                        return componentsschemas_query_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                        return componentsschemas_query_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_4 = TermQuery.from_dict(data)

                        return componentsschemas_query_type_4
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                        return componentsschemas_query_type_5
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                        return componentsschemas_query_type_6
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_7
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                        return componentsschemas_query_type_8
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_9
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                        return componentsschemas_query_type_10
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                        return componentsschemas_query_type_11
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                        return componentsschemas_query_type_12
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                        return componentsschemas_query_type_13
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                        return componentsschemas_query_type_14
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                        return componentsschemas_query_type_15
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                        return componentsschemas_query_type_16
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                        return componentsschemas_query_type_17
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                        return componentsschemas_query_type_18
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                        return componentsschemas_query_type_19
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_20
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_21
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                        return componentsschemas_query_type_22
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                        return componentsschemas_query_type_23
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                        return componentsschemas_query_type_24
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                        return componentsschemas_query_type_25
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                        return componentsschemas_query_type_26
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                        return componentsschemas_query_type_27
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                        return componentsschemas_query_type_28
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                        return componentsschemas_query_type_29
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                    return componentsschemas_query_type_30

                componentsschemas_bool_query_clause_type_1_item = (
                    _parse_componentsschemas_bool_query_clause_type_1_item(
                        componentsschemas_bool_query_clause_type_1_item_data
                    )
                )

                componentsschemas_bool_query_clause_type_1.append(componentsschemas_bool_query_clause_type_1_item)

            return componentsschemas_bool_query_clause_type_1

        must_not = _parse_must_not(d.pop("must_not", UNSET))

        def _parse_filter_(
            data: object,
        ) -> (
            AutocompleteQuery
            | BooleanQuery
            | BoolFieldQuery
            | BoolQuery
            | ConjunctionQuery
            | ContainsQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | EqualsQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | list[
                AutocompleteQuery
                | BooleanQuery
                | BoolFieldQuery
                | BoolQuery
                | ConjunctionQuery
                | ContainsQuery
                | DateRangeStringQuery
                | DisjunctionQuery
                | DocIdQuery
                | EqualsQuery
                | FuzzyQuery
                | GeoBoundingBoxQuery
                | GeoBoundingPolygonQuery
                | GeoDistanceQuery
                | GeoShapeQuery
                | IPRangeQuery
                | MatchAllQuery
                | MatchesQuery
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
                | WildcardQuery
            ]
            | MatchAllQuery
            | MatchesQuery
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
                componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = TermQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_25
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_26
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_27
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_28
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_29
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                return componentsschemas_query_type_30
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, list):
                raise TypeError()
            componentsschemas_bool_query_clause_type_1 = []
            _componentsschemas_bool_query_clause_type_1 = data
            for componentsschemas_bool_query_clause_type_1_item_data in _componentsschemas_bool_query_clause_type_1:

                def _parse_componentsschemas_bool_query_clause_type_1_item(
                    data: object,
                ) -> (
                    AutocompleteQuery
                    | BooleanQuery
                    | BoolFieldQuery
                    | BoolQuery
                    | ConjunctionQuery
                    | ContainsQuery
                    | DateRangeStringQuery
                    | DisjunctionQuery
                    | DocIdQuery
                    | EqualsQuery
                    | FuzzyQuery
                    | GeoBoundingBoxQuery
                    | GeoBoundingPolygonQuery
                    | GeoDistanceQuery
                    | GeoShapeQuery
                    | IPRangeQuery
                    | MatchAllQuery
                    | MatchesQuery
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
                    | WildcardQuery
                ):
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_0 = EqualsQuery.from_dict(data)

                        return componentsschemas_query_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_1 = ContainsQuery.from_dict(data)

                        return componentsschemas_query_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_2 = MatchesQuery.from_dict(data)

                        return componentsschemas_query_type_2
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_3 = AutocompleteQuery.from_dict(data)

                        return componentsschemas_query_type_3
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_4 = TermQuery.from_dict(data)

                        return componentsschemas_query_type_4
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_5 = MatchQuery.from_dict(data)

                        return componentsschemas_query_type_5
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_6 = MultiMatchQuery.from_dict(data)

                        return componentsschemas_query_type_6
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_7 = MatchPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_7
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_8 = PhraseQuery.from_dict(data)

                        return componentsschemas_query_type_8
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_9 = MultiPhraseQuery.from_dict(data)

                        return componentsschemas_query_type_9
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_10 = FuzzyQuery.from_dict(data)

                        return componentsschemas_query_type_10
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_11 = PrefixQuery.from_dict(data)

                        return componentsschemas_query_type_11
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_12 = RegexpQuery.from_dict(data)

                        return componentsschemas_query_type_12
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_13 = WildcardQuery.from_dict(data)

                        return componentsschemas_query_type_13
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_14 = QueryStringQuery.from_dict(data)

                        return componentsschemas_query_type_14
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_15 = NumericRangeQuery.from_dict(data)

                        return componentsschemas_query_type_15
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_16 = TermRangeQuery.from_dict(data)

                        return componentsschemas_query_type_16
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_17 = DateRangeStringQuery.from_dict(data)

                        return componentsschemas_query_type_17
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_18 = BoolQuery.from_dict(data)

                        return componentsschemas_query_type_18
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_19 = BooleanQuery.from_dict(data)

                        return componentsschemas_query_type_19
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_20 = ConjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_20
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_21 = DisjunctionQuery.from_dict(data)

                        return componentsschemas_query_type_21
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_22 = MatchAllQuery.from_dict(data)

                        return componentsschemas_query_type_22
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_23 = MatchNoneQuery.from_dict(data)

                        return componentsschemas_query_type_23
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_24 = DocIdQuery.from_dict(data)

                        return componentsschemas_query_type_24
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_25 = BoolFieldQuery.from_dict(data)

                        return componentsschemas_query_type_25
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_26 = IPRangeQuery.from_dict(data)

                        return componentsschemas_query_type_26
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_27 = GeoBoundingBoxQuery.from_dict(data)

                        return componentsschemas_query_type_27
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_28 = GeoDistanceQuery.from_dict(data)

                        return componentsschemas_query_type_28
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_query_type_29 = GeoBoundingPolygonQuery.from_dict(data)

                        return componentsschemas_query_type_29
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_query_type_30 = GeoShapeQuery.from_dict(data)

                    return componentsschemas_query_type_30

                componentsschemas_bool_query_clause_type_1_item = (
                    _parse_componentsschemas_bool_query_clause_type_1_item(
                        componentsschemas_bool_query_clause_type_1_item_data
                    )
                )

                componentsschemas_bool_query_clause_type_1.append(componentsschemas_bool_query_clause_type_1_item)

            return componentsschemas_bool_query_clause_type_1

        filter_ = _parse_filter_(d.pop("filter", UNSET))

        minimum_should_match = d.pop("minimum_should_match", UNSET)

        def _parse_boost(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        boost = _parse_boost(d.pop("boost", UNSET))

        bool_query_body = cls(
            must=must,
            should=should,
            must_not=must_not,
            filter_=filter_,
            minimum_should_match=minimum_should_match,
            boost=boost,
        )

        return bool_query_body
