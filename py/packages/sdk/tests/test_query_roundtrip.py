"""Query union dispatch must preserve predicates, including nested filters."""

import pytest

from antfly.client_generated.models.bool_query import BoolQuery
from antfly.client_generated.models.query_request import QueryRequest

QUERIES = [
    {"equals": "needle", "field": "content"},
    {"contains": "needle", "field": "content"},
    {"matches": "n.*e", "field": "content"},
    {"autocomplete": "nee", "field": "content"},
    {"term": "needle", "field": "content"},
    {"match": "needle", "field": "content"},
    {"prefix": "nee", "field": "content"},
    {"regexp": "n.*e", "field": "content"},
    {"wildcard": "n*e", "field": "content"},
    {"query": "needle"},
    {"min": 1, "max": 4, "field": "number"},
    {"min": "a", "max": "z", "field": "name"},
    {"start": "2026-01-01T00:00:00", "end": "2026-12-31T00:00:00", "field": "date"},
    {"bool": {"filter": {"contains": "needle", "field": "content"}}},
    {"conjuncts": [{"term": "a"}, {"term": "b"}]},
    {"disjuncts": [{"term": "a"}, {"term": "b"}]},
    {"match_all": {}},
    {"match_none": {}},
    {"ids": ["a", "b"]},
    {"multi_match": {"query": "one two", "fields": ["title", "body"], "type": "bool_prefix"}},
    {"match_phrase": "one two", "field": "content"},
    {"terms": ["one", "two"], "field": "content"},
    {"terms": [["one", "two"], ["three"]], "field": "content"},
    {"term": "needle", "fuzziness": 1, "field": "content"},
    {"bool": True, "field": "enabled"},
    {"cidr": "10.0.0.0/8", "field": "ip"},
    {"field": "point", "min_lat": -1.0, "min_lon": -2.0, "max_lat": 1.0, "max_lon": 2.0},
    {"location": [0.0, 1.0], "distance": "10km", "field": "point"},
    {
        "polygon_points": [{"lat": 0.0, "lon": 0.0}, {"lat": 1.0, "lon": 0.0}, {"lat": 0.0, "lon": 1.0}],
        "field": "point",
    },
    {"geometry": {"shape": {"type": "Point", "coordinates": [0, 0]}, "relation": "intersects"}, "field": "shape"},
    {"must": {"conjuncts": [{"term": "a"}]}},
]


@pytest.mark.parametrize("query", QUERIES)
@pytest.mark.parametrize("field", ["query", "filter_query", "exclusion_query"])
def test_query_request_roundtrip(query, field):
    body = {field: query}
    assert QueryRequest.from_dict(body).to_dict() == body


@pytest.mark.parametrize("query", QUERIES)
@pytest.mark.parametrize("clause", ["must", "should", "must_not", "filter"])
@pytest.mark.parametrize("array", [False, True])
def test_nested_bool_roundtrip(query, clause, array):
    nested = {"bool": {clause: [query] if array else query}}
    body = {"bool": {"filter": nested}}
    assert BoolQuery.from_dict(body).to_dict() == body


def test_strict_models_reject_unknown_properties():
    from antfly.client_generated.models.numeric_range_query import NumericRangeQuery

    with pytest.raises(ValueError, match="Unexpected properties"):
        NumericRangeQuery.from_dict({"bool": {"filter": {"match_all": {}}}})
