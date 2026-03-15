//go:generate go tool oapi-codegen --config=cfg.yaml ../../../bleve-query-openapi.yaml
package query

import "time"

// Builder helpers for creating Bleve queries with convenience functions

// FuzzinessInt creates a Fuzziness from an int32. Panics on error.
func FuzzinessInt(v int32) Fuzziness {
	var f Fuzziness
	if err := f.FromFuzziness0(v); err != nil {
		panic(err)
	}
	return f
}

// FuzzinessStr creates a Fuzziness from a string. Panics on error.
func FuzzinessStr(v string) Fuzziness {
	var f Fuzziness
	if err := f.FromFuzziness1(Fuzziness1(v)); err != nil {
		panic(err)
	}
	return f
}

// ToQuery creates a Query from a TermQuery. Panics on error.
func (v TermQuery) ToQuery() Query {
	var q Query
	if err := q.FromTermQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a MatchQuery. Panics on error.
func (v MatchQuery) ToQuery() Query {
	var q Query
	if err := q.FromMatchQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a MatchPhraseQuery. Panics on error.
func (v MatchPhraseQuery) ToQuery() Query {
	var q Query
	if err := q.FromMatchPhraseQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a PhraseQuery. Panics on error.
func (v PhraseQuery) ToQuery() Query {
	var q Query
	if err := q.FromPhraseQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a MultiPhraseQuery. Panics on error.
func (v MultiPhraseQuery) ToQuery() Query {
	var q Query
	if err := q.FromMultiPhraseQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a FuzzyQuery. Panics on error.
func (v FuzzyQuery) ToQuery() Query {
	var q Query
	if err := q.FromFuzzyQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a PrefixQuery. Panics on error.
func (v PrefixQuery) ToQuery() Query {
	var q Query
	if err := q.FromPrefixQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a RegexpQuery. Panics on error.
func (v RegexpQuery) ToQuery() Query {
	var q Query
	if err := q.FromRegexpQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a WildcardQuery. Panics on error.
func (v WildcardQuery) ToQuery() Query {
	var q Query
	if err := q.FromWildcardQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a QueryStringQuery. Panics on error.
func (v QueryStringQuery) ToQuery() Query {
	var q Query
	if err := q.FromQueryStringQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a NumericRangeQuery. Panics on error.
func (v NumericRangeQuery) ToQuery() Query {
	var q Query
	if err := q.FromNumericRangeQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a TermRangeQuery. Panics on error.
func (v TermRangeQuery) ToQuery() Query {
	var q Query
	if err := q.FromTermRangeQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a DateRangeStringQuery. Panics on error.
func (v DateRangeStringQuery) ToQuery() Query {
	var q Query
	if err := q.FromDateRangeStringQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a BooleanQuery. Panics on error.
func (v BooleanQuery) ToQuery() Query {
	var q Query
	if err := q.FromBooleanQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a ConjunctionQuery. Panics on error.
func (v ConjunctionQuery) ToQuery() Query {
	var q Query
	if err := q.FromConjunctionQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a DisjunctionQuery. Panics on error.
func (v DisjunctionQuery) ToQuery() Query {
	var q Query
	if err := q.FromDisjunctionQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a MatchAllQuery. Panics on error.
func (v MatchAllQuery) ToQuery() Query {
	var q Query
	if err := q.FromMatchAllQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a MatchNoneQuery. Panics on error.
func (v MatchNoneQuery) ToQuery() Query {
	var q Query
	if err := q.FromMatchNoneQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a DocIdQuery. Panics on error.
func (v DocIdQuery) ToQuery() Query {
	var q Query
	if err := q.FromDocIdQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a BoolFieldQuery. Panics on error.
func (v BoolFieldQuery) ToQuery() Query {
	var q Query
	if err := q.FromBoolFieldQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a IPRangeQuery. Panics on error.
func (v IPRangeQuery) ToQuery() Query {
	var q Query
	if err := q.FromIPRangeQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a GeoBoundingBoxQuery. Panics on error.
func (v GeoBoundingBoxQuery) ToQuery() Query {
	var q Query
	if err := q.FromGeoBoundingBoxQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a GeoDistanceQuery. Panics on error.
func (v GeoDistanceQuery) ToQuery() Query {
	var q Query
	if err := q.FromGeoDistanceQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a GeoBoundingPolygonQuery. Panics on error.
func (v GeoBoundingPolygonQuery) ToQuery() Query {
	var q Query
	if err := q.FromGeoBoundingPolygonQuery(v); err != nil {
		panic(err)
	}
	return q
}

// ToQuery creates a Query from a GeoShapeQuery. Panics on error.
func (v GeoShapeQuery) ToQuery() Query {
	var q Query
	if err := q.FromGeoShapeQuery(v); err != nil {
		panic(err)
	}
	return q
}

// Convenience builder functions for common query types

// NewQueryString creates a QueryStringQuery.
//
// Example:
//
//	q := query.NewQueryString("body:computer AND category:technology")
func NewQueryString(queryStr string) Query {
	return QueryStringQuery{Query: queryStr}.ToQuery()
}

// NewQueryStringBoost creates a QueryStringQuery with boost.
//
// Example:
//
//	q := query.NewQueryStringBoost("body:computer", 1.5)
func NewQueryStringBoost(queryStr string, boost float64) Query {
	return QueryStringQuery{Query: queryStr, Boost: Boost(boost)}.ToQuery()
}

// NewTerm creates a TermQuery.
//
// Example:
//
//	q := query.NewTerm("published", "status")
func NewTerm(term string, field string) Query {
	return TermQuery{Term: term, Field: field}.ToQuery()
}

// NewMatch creates a MatchQuery.
//
// Example:
//
//	q := query.NewMatch("golang tutorial", "body")
func NewMatch(match string, field string) Query {
	return MatchQuery{Match: match, Field: field}.ToQuery()
}

// NewMatchPhrase creates a MatchPhraseQuery.
//
// Example:
//
//	q := query.NewMatchPhrase("distributed systems", "body")
func NewMatchPhrase(phrase string, field string) Query {
	return MatchPhraseQuery{MatchPhrase: phrase, Field: field}.ToQuery()
}

// NewPrefix creates a PrefixQuery.
//
// Example:
//
//	q := query.NewPrefix("comp", "title")
func NewPrefix(prefix string, field string) Query {
	return PrefixQuery{Prefix: prefix, Field: field}.ToQuery()
}

// NewNumericRange creates a NumericRangeQuery.
//
// Example:
//
//	q := query.NewNumericRange(0, 1000, "price")
func NewNumericRange(min float64, max float64, field string) Query {
	return NumericRangeQuery{Min: min, Max: max, Field: field}.ToQuery()
}

// NewDateRange creates a DateRangeStringQuery.
//
// Example:
//
//	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
//	end := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
//	q := query.NewDateRange(start, end, "created_at")
func NewDateRange(start time.Time, end time.Time, field string) Query {
	return DateRangeStringQuery{Start: start, End: end, Field: field}.ToQuery()
}

// NewMatchAll creates a MatchAllQuery.
//
// Example:
//
//	q := query.NewMatchAll()
func NewMatchAll() Query {
	return MatchAllQuery{}.ToQuery()
}

// NewMatchNone creates a MatchNoneQuery.
//
// Example:
//
//	q := query.NewMatchNone()
func NewMatchNone() Query {
	return MatchNoneQuery{}.ToQuery()
}

// NewBoolean creates a BooleanQuery.
//
// Example:
//
//	must := query.NewConjunction([]query.Query{query.NewTerm("published", "status")})
//	mustNot := query.NewDisjunction([]query.Query{query.NewTerm("archived", "status")}, 0)
//	q := query.NewBoolean(must, DisjunctionQuery{}, mustNot)
func NewBoolean(must ConjunctionQuery, should DisjunctionQuery, mustNot DisjunctionQuery) Query {
	return BooleanQuery{
		Must:    must,
		Should:  should,
		MustNot: mustNot,
	}.ToQuery()
}

// NewConjunction creates a ConjunctionQuery (AND).
//
// Example:
//
//	q := query.NewConjunction([]query.Query{
//	    query.NewTerm("published", "status"),
//	    query.NewMatch("golang", "title"),
//	})
func NewConjunction(queries []Query) ConjunctionQuery {
	return ConjunctionQuery{Conjuncts: queries}
}

// NewDisjunction creates a DisjunctionQuery (OR).
//
// Example:
//
//	q := query.NewDisjunction([]query.Query{
//	    query.NewTerm("draft", "status"),
//	    query.NewTerm("pending", "status"),
//	}, 0)
func NewDisjunction(queries []Query, min float64) DisjunctionQuery {
	return DisjunctionQuery{Disjuncts: queries, Min: min}
}

// NewDocIds creates a DocIdQuery.
//
// Example:
//
//	q := query.NewDocIds([]string{"doc1", "doc2", "doc3"})
func NewDocIds(ids []string) Query {
	return DocIdQuery{Ids: ids}.ToQuery()
}

// NewGeoDistance creates a GeoDistanceQuery.
//
// Example:
//
//	q := query.NewGeoDistance(-122.4, 37.8, "5km", "location")
func NewGeoDistance(lon float64, lat float64, distance string, field string) Query {
	return GeoDistanceQuery{
		Location: []float64{lon, lat},
		Distance: distance,
		Field:    field,
	}.ToQuery()
}

// NewGeoBoundingBox creates a GeoBoundingBoxQuery.
//
// Example:
//
//	q := query.NewGeoBoundingBox(
//	    -122.5, 37.9,  // top left lon, lat
//	    -122.3, 37.7,  // bottom right lon, lat
//	    "location",
//	)
func NewGeoBoundingBox(topLeftLon float64, topLeftLat float64, bottomRightLon float64, bottomRightLat float64, field string) Query {
	return GeoBoundingBoxQuery{
		TopLeft:     []float64{topLeftLon, topLeftLat},
		BottomRight: []float64{bottomRightLon, bottomRightLat},
		Field:       field,
	}.ToQuery()
}
