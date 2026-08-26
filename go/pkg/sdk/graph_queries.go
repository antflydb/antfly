// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sdk

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	querydsl "github.com/antflydb/antfly/go/pkg/sdk/query"
)

// GraphBindingsOptions controls row count and optional document projection.
// A zero Limit uses the server default. Fields require IncludeDocuments, and
// hydrated projections are capped at 10,000 binding documents per operation.
type GraphBindingsOptions struct {
	Limit            int
	IncludeDocuments bool
	Fields           []string
}

const (
	maxGraphMatchNodes          = 64
	maxGraphMatchEdges          = 64
	maxGraphOptionalPatterns    = 64
	maxGraphMatchPredicates     = 64
	maxGraphMatchPredicateDepth = 16
	maxGraphCountAggregates     = 64
	maxGraphEdgeTypes           = 64
	maxGraphEdgeTypeBytes       = 64 * 1024
	maxGraphHydratedBindings    = 10_000
	maxNamedGraphQueries        = 64
	defaultGraphBindingsLimit   = 100
)

func validateNamedGraphQueries(queries map[string]GraphQuery) error {
	if len(queries) > maxNamedGraphQueries {
		return fmt.Errorf("antfly: graph_queries accepts at most %d named operations", maxNamedGraphQueries)
	}
	for name, query := range queries {
		if !validGraphQueryName(name) {
			return invalidGraphIdentifier("graph_queries key")
		}
		if err := validateGraphQuery(query); err != nil {
			return fmt.Errorf("antfly: graph_queries[%q]: %w", name, err)
		}
	}
	return nil
}

func validateGraphQuery(query GraphQuery) error {
	encoded, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("invalid graph query: %w", err)
	}
	var shape struct {
		Match          json.RawMessage `json:"match"`
		Traverse       json.RawMessage `json:"traverse"`
		ShortestPath   json.RawMessage `json:"shortest_path"`
		KShortestPaths json.RawMessage `json:"k_shortest_paths"`
	}
	if err := json.Unmarshal(encoded, &shape); err != nil {
		return fmt.Errorf("invalid graph query: %w", err)
	}
	variants := 0
	for _, value := range []json.RawMessage{shape.Match, shape.Traverse, shape.ShortestPath, shape.KShortestPaths} {
		if value != nil {
			variants++
		}
	}
	if variants != 1 {
		return fmt.Errorf("graph query must contain exactly one of match, traverse, shortest_path, or k_shortest_paths")
	}
	switch {
	case shape.Match != nil:
		value, err := query.AsGraphMatchQuery()
		if err != nil {
			return err
		}
		return validateGraphMatchQuery(value)
	case shape.Traverse != nil:
		value, err := query.AsGraphTraverseQuery()
		if err != nil {
			return err
		}
		return validateGraphTraverseQuery(value)
	case shape.ShortestPath != nil:
		value, err := query.AsGraphShortestPathQuery()
		if err != nil {
			return err
		}
		return validateGraphPathQuery(value.Index, value.ShortestPath.From, value.ShortestPath.To, value.ShortestPath.EdgeTypes, value.ShortestPath.MaxDepth, value.ShortestPath.MinWeight, value.ShortestPath.MaxWeight, value.ShortestPath.IncludeDocuments, value.ShortestPath.Fields)
	default:
		value, err := query.AsGraphKShortestPathsQuery()
		if err != nil {
			return err
		}
		if value.KShortestPaths.K < 1 || value.KShortestPaths.K > 100 {
			return fmt.Errorf("graph k must be between 1 and 100")
		}
		return validateGraphPathQuery(value.Index, value.KShortestPaths.From, value.KShortestPaths.To, value.KShortestPaths.EdgeTypes, value.KShortestPaths.MaxDepth, value.KShortestPaths.MinWeight, value.KShortestPaths.MaxWeight, value.KShortestPaths.IncludeDocuments, value.KShortestPaths.Fields)
	}
}

// NewGraphDocumentFilter adapts the non-scoring stored-document subset of the
// query DSL to a graph node filter. Query DSL dotted fields are converted to
// canonical RFC 6901 JSON Pointers; an already pointer-shaped field is
// validated and preserved. Analyzer-backed full-text clauses are rejected
// locally because evaluating them against stored JSON would change both their
// semantics and cost model.
func NewGraphDocumentFilter(filter querydsl.Query) (GraphDocumentFilter, error) {
	visited := 0
	return convertGraphDocumentFilter(filter, 0, &visited)
}

// DecodeGraphResult returns the concrete response result selected by its
// stable discriminator. During the v0.2 compatibility window, a response
// without a discriminator is decoded as the only structural variant that
// permits one to be absent: LegacyGraphQueryResult.
func DecodeGraphResult(result GraphResult) (any, error) {
	// Probe only the small control fields. Decoding into a RawMessage map would
	// copy every top-level value, including result rows, paths, and hydrated
	// documents, before the selected variant decodes the payload a second time.
	var envelope graphQueryResultEnvelope
	if err := result.DecodeInto(&envelope); err != nil {
		return nil, err
	}
	if envelope.Kind != nil {
		var kind *string
		if err := json.Unmarshal(envelope.Kind, &kind); err != nil || kind == nil {
			return nil, fmt.Errorf("antfly: graph result has an invalid discriminator")
		}
		switch *kind {
		case string(GraphBindingsResultKindBindings):
			var value GraphBindingsResult
			if err := result.DecodeInto(&value); err != nil {
				return nil, err
			}
			return value, nil
		case string(GraphAggregatesResultKindAggregates):
			var value GraphAggregatesResult
			if err := result.DecodeInto(&value); err != nil {
				return nil, err
			}
			return value, nil
		case string(GraphNodesResultKindNodes):
			var value GraphNodesResult
			if err := result.DecodeInto(&value); err != nil {
				return nil, err
			}
			return value, nil
		case string(LegacyGraphQueryResultKindLegacy):
			return decodeLegacyGraphQueryResult(result, envelope)
		default:
			return nil, fmt.Errorf("antfly: unknown graph result discriminator %q", *kind)
		}
	}
	return decodeLegacyGraphQueryResult(result, envelope)
}

// DecodeGraphQueryResult decodes a canonical graph_queries result. Legacy
// compatibility is intentionally absent from this API.
func DecodeGraphQueryResult(result GraphQueryResult) (any, error) {
	var envelope graphQueryResultEnvelope
	if err := result.DecodeInto(&envelope); err != nil {
		return nil, err
	}
	if envelope.Kind == nil {
		return nil, fmt.Errorf("antfly: canonical graph result requires a discriminator")
	}
	var kind *string
	if err := json.Unmarshal(envelope.Kind, &kind); err != nil || kind == nil {
		return nil, fmt.Errorf("antfly: graph result has an invalid discriminator")
	}
	switch *kind {
	case string(GraphBindingsResultKindBindings):
		return result.AsGraphBindingsResult()
	case string(GraphAggregatesResultKindAggregates):
		return result.AsGraphAggregatesResult()
	case string(GraphNodesResultKindNodes):
		return result.AsGraphNodesResult()
	default:
		return nil, fmt.Errorf("antfly: unknown canonical graph result discriminator %q", *kind)
	}
}

type graphQueryResultEnvelope struct {
	Kind  json.RawMessage `json:"kind"`
	Type  json.RawMessage `json:"type"`
	Total json.RawMessage `json:"total"`
}

func decodeLegacyGraphQueryResult(result GraphResult, envelope graphQueryResultEnvelope) (LegacyGraphQueryResult, error) {
	if envelope.Type == nil || envelope.Total == nil {
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: legacy graph result requires type and total")
	}
	var legacyType GraphQueryType
	if err := json.Unmarshal(envelope.Type, &legacyType); err != nil {
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: invalid legacy graph result type: %w", err)
	}
	switch legacyType {
	case GraphQueryTypeNeighbors, GraphQueryTypeTraverse, GraphQueryTypeShortestPath,
		GraphQueryTypeKShortestPaths, GraphQueryTypePattern:
	default:
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: unknown legacy graph result type %q", legacyType)
	}
	var total *int
	if err := json.Unmarshal(envelope.Total, &total); err != nil {
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: invalid legacy graph result total: %w", err)
	}
	if total == nil {
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: legacy graph result total must be an integer")
	}
	legacy, err := result.AsLegacyGraphQueryResult()
	if err != nil {
		return LegacyGraphQueryResult{}, err
	}
	if legacy.Type != legacyType {
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: inconsistent legacy graph result type")
	}
	return legacy, nil
}

// convertGraphDocumentFilter is a closed-world adapter. It classifies only the
// explicitly supported query variants, converts them to graph-specific wire
// types, and recursively rejects every scoring or index-only option. This
// keeps future additions to the full-text DSL from silently changing graph
// semantics.
func convertGraphDocumentFilter(filter querydsl.Query, depth int, visited *int) (GraphDocumentFilter, error) {
	(*visited)++
	if depth > 64 || *visited > 16_384 {
		return GraphDocumentFilter{}, fmt.Errorf("antfly: graph document filter exceeds the query complexity budget")
	}
	encoded, err := json.Marshal(filter)
	if err != nil {
		return GraphDocumentFilter{}, err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil || len(members) == 0 {
		return GraphDocumentFilter{}, fmt.Errorf("antfly: graph document filter must be a query object")
	}

	switch {
	case graphQueryMemberPresent(members, "term"):
		if graphQueryMemberPresent(members, "fuzziness") {
			if err := requireGraphQueryMembers(members, "term", "field", "fuzziness", "prefix_length"); err != nil {
				return GraphDocumentFilter{}, err
			}
			value, err := filter.AsFuzzyQuery()
			if err != nil {
				return GraphDocumentFilter{}, err
			}
			path, err := graphDocumentPathFromQueryField(value.Field)
			if err != nil {
				return GraphDocumentFilter{}, err
			}
			fuzziness, err := graphDocumentFuzziness(value.Fuzziness)
			if err != nil {
				return GraphDocumentFilter{}, err
			}
			var out GraphDocumentFilter
			err = out.FromGraphDocumentFuzzyFilter(oapi.GraphDocumentFuzzyFilter{
				Term: value.Term, Path: path, Fuzziness: fuzziness, PrefixLength: value.PrefixLength,
			})
			return out, err
		}
		if err := requireGraphQueryMembers(members, "term", "field"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsTermQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentTermFilter(oapi.GraphDocumentTermFilter{Term: value.Term, Path: path})
		return out, err
	case graphQueryMemberPresent(members, "prefix"):
		if err := requireGraphQueryMembers(members, "prefix", "field"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsPrefixQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentPrefixFilter(oapi.GraphDocumentPrefixFilter{Prefix: value.Prefix, Path: path})
		return out, err
	case graphQueryMemberPresent(members, "regexp"):
		if err := requireGraphQueryMembers(members, "regexp", "field"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsRegexpQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentRegexpFilter(oapi.GraphDocumentRegexpFilter{Regexp: value.Regexp, Path: path})
		return out, err
	case graphQueryMemberPresent(members, "wildcard"):
		if err := requireGraphQueryMembers(members, "wildcard", "field"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsWildcardQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentWildcardFilter(oapi.GraphDocumentWildcardFilter{Wildcard: value.Wildcard, Path: path})
		return out, err
	case graphQueryMemberPresent(members, "min") || graphQueryMemberPresent(members, "max"):
		return convertGraphRangeFilter(filter, members)
	case graphQueryMemberPresent(members, "start") || graphQueryMemberPresent(members, "end"):
		if err := requireGraphQueryMembers(members, "start", "end", "inclusive_start", "inclusive_end", "field"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsDateRangeStringQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		if value.Start == nil && value.End == nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph date range requires start or end")
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentDateRangeFilter(oapi.GraphDocumentDateRangeFilter{
			Start: value.Start, End: value.End, InclusiveStart: value.InclusiveStart,
			InclusiveEnd: value.InclusiveEnd, Path: path,
		})
		return out, err
	case graphQueryMemberPresent(members, "ids"):
		if err := requireGraphQueryMembers(members, "ids"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsDocIdQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		if len(value.Ids) == 0 || len(value.Ids) > 10_000 {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph ids must contain between 1 and 10000 values")
		}
		if err := validateNonEmptyUnique("graph id", value.Ids); err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentIdsFilter(oapi.GraphDocumentIdsFilter{Ids: value.Ids})
		return out, err
	case graphQueryMemberPresent(members, "bool"):
		if err := requireGraphQueryMembers(members, "bool", "field"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsBoolFieldQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentBoolFieldFilter(oapi.GraphDocumentBoolFieldFilter{Bool: value.Bool, Path: path})
		return out, err
	case graphQueryMemberPresent(members, "match_all"):
		if err := requireGraphQueryMembers(members, "match_all"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsMatchAllQuery()
		if err != nil || len(value.MatchAll) != 0 {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph match_all body must be empty")
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentMatchAllFilter(oapi.GraphDocumentMatchAllFilter{MatchAll: value.MatchAll})
		return out, err
	case graphQueryMemberPresent(members, "match_none"):
		if err := requireGraphQueryMembers(members, "match_none"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsMatchNoneQuery()
		if err != nil || len(value.MatchNone) != 0 {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph match_none body must be empty")
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentMatchNoneFilter(oapi.GraphDocumentMatchNoneFilter{MatchNone: value.MatchNone})
		return out, err
	case graphQueryMemberPresent(members, "conjuncts"):
		if err := requireGraphQueryMembers(members, "conjuncts"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsConjunctionQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		items, err := convertGraphFilterItems(value.Conjuncts, depth, visited)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentFilterConjunction(oapi.GraphDocumentFilterConjunction{Conjuncts: items})
		return out, err
	case graphQueryMemberPresent(members, "disjuncts"):
		if err := requireGraphQueryMembers(members, "disjuncts", "min"); err != nil {
			return GraphDocumentFilter{}, err
		}
		value, err := filter.AsDisjunctionQuery()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		items, err := convertGraphFilterItems(value.Disjuncts, depth, visited)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		if value.Min != nil && *value.Min > uint32(len(items)) {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph disjunction min exceeds its number of clauses")
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentFilterDisjunction(oapi.GraphDocumentFilterDisjunction{Disjuncts: items, Min: value.Min})
		return out, err
	case graphQueryMemberPresent(members, "must") || graphQueryMemberPresent(members, "should") ||
		graphQueryMemberPresent(members, "must_not") || graphQueryMemberPresent(members, "filter"):
		return convertGraphBooleanFilter(members, depth, visited)
	default:
		return GraphDocumentFilter{}, fmt.Errorf("antfly: query variant is not supported by graph document filters")
	}
}

func graphQueryMemberPresent(members map[string]json.RawMessage, name string) bool {
	raw, ok := members[name]
	return ok && string(raw) != "null"
}

func requireGraphQueryMembers(members map[string]json.RawMessage, allowed ...string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}
	for name := range members {
		if _, ok := allowedSet[name]; !ok {
			return fmt.Errorf("antfly: graph document filters do not support query option %q", name)
		}
	}
	return nil
}

func graphDocumentFuzziness(value querydsl.Fuzziness) (oapi.Fuzziness, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return oapi.Fuzziness{}, err
	}
	var out oapi.Fuzziness
	if err := json.Unmarshal(encoded, &out); err != nil {
		return oapi.Fuzziness{}, err
	}
	return out, nil
}

func convertGraphRangeFilter(filter querydsl.Query, members map[string]json.RawMessage) (GraphDocumentFilter, error) {
	if err := requireGraphQueryMembers(members, "min", "max", "inclusive_min", "inclusive_max", "field"); err != nil {
		return GraphDocumentFilter{}, err
	}
	bound := members["min"]
	if !graphQueryMemberPresent(members, "min") {
		bound = members["max"]
	}
	if len(bound) == 0 {
		return GraphDocumentFilter{}, fmt.Errorf("antfly: graph range requires min or max")
	}
	if bound[0] == '"' {
		value, err := filter.AsTermRangeQuery()
		if err != nil || value.Min == nil && value.Max == nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph term range requires min or max")
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentTermRangeFilter(oapi.GraphDocumentTermRangeFilter{TermRange: oapi.GraphDocumentTermRangeBody{
			Min: value.Min, Max: value.Max, InclusiveMin: value.InclusiveMin,
			InclusiveMax: value.InclusiveMax, Path: path,
		}})
		return out, err
	}
	value, err := filter.AsNumericRangeQuery()
	if err != nil || value.Min == nil && value.Max == nil {
		return GraphDocumentFilter{}, fmt.Errorf("antfly: graph numeric range requires min or max")
	}
	path, err := graphDocumentPathFromQueryField(value.Field)
	if err != nil {
		return GraphDocumentFilter{}, err
	}
	var out GraphDocumentFilter
	err = out.FromGraphDocumentNumericRangeFilter(oapi.GraphDocumentNumericRangeFilter{NumericRange: oapi.GraphDocumentNumericRangeBody{
		Min: value.Min, Max: value.Max, InclusiveMin: value.InclusiveMin,
		InclusiveMax: value.InclusiveMax, Path: path,
	}})
	return out, err
}

func convertGraphFilterItems(items []querydsl.Query, depth int, visited *int) ([]GraphDocumentFilter, error) {
	if len(items) == 0 || len(items) > 64 {
		return nil, fmt.Errorf("antfly: graph filter clause arrays must contain between 1 and 64 entries")
	}
	out := make([]GraphDocumentFilter, len(items))
	for i, item := range items {
		converted, err := convertGraphDocumentFilter(item, depth+1, visited)
		if err != nil {
			return nil, fmt.Errorf("antfly: graph filter clause %d: %w", i, err)
		}
		out[i] = converted
	}
	return out, nil
}

func graphQueryFromRaw(raw json.RawMessage) (querydsl.Query, error) {
	var query querydsl.Query
	if err := json.Unmarshal(raw, &query); err != nil {
		return querydsl.Query{}, err
	}
	return query, nil
}

func convertGraphBooleanFilter(members map[string]json.RawMessage, depth int, visited *int) (GraphDocumentFilter, error) {
	if err := requireGraphQueryMembers(members, "must", "should", "must_not", "filter"); err != nil {
		return GraphDocumentFilter{}, err
	}
	var body oapi.GraphDocumentFilterBoolean
	if raw, ok := members["filter"]; ok && string(raw) != "null" {
		query, err := graphQueryFromRaw(raw)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		body.Filter, err = convertGraphDocumentFilter(query, depth+1, visited)
		if err != nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph bool filter: %w", err)
		}
	}
	for _, clause := range []struct {
		name        string
		destination *oapi.GraphDocumentFilterDisjunction
	}{
		{name: "should", destination: &body.Should},
		{name: "must_not", destination: &body.MustNot},
	} {
		raw, ok := members[clause.name]
		if !ok || string(raw) == "null" {
			continue
		}
		query, err := graphQueryFromRaw(raw)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		converted, err := convertGraphDocumentFilter(query, depth+1, visited)
		if err != nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph bool %s: %w", clause.name, err)
		}
		*clause.destination, err = converted.AsGraphDocumentFilterDisjunction()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
	}
	if raw, ok := members["must"]; ok && string(raw) != "null" {
		query, err := graphQueryFromRaw(raw)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		converted, err := convertGraphDocumentFilter(query, depth+1, visited)
		if err != nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph bool must: %w", err)
		}
		body.Must, err = converted.AsGraphDocumentFilterConjunction()
		if err != nil {
			return GraphDocumentFilter{}, err
		}
	}
	var out GraphDocumentFilter
	err := out.FromGraphDocumentFilterBoolean(body)
	return out, err
}

func graphDocumentPathFromQueryField(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("antfly: graph document filter field must not be empty")
	}
	if strings.HasPrefix(field, "/") {
		if !validGraphDocumentJSONPointer(field) {
			return "", fmt.Errorf("antfly: graph document filter field is not a valid RFC 6901 JSON Pointer")
		}
		return field, nil
	}
	segments := strings.Split(field, ".")
	for i, segment := range segments {
		if segment == "" {
			return "", fmt.Errorf("antfly: graph document filter field contains an empty path segment")
		}
		segments[i] = strings.ReplaceAll(strings.ReplaceAll(segment, "~", "~0"), "/", "~1")
	}
	return "/" + strings.Join(segments, "/"), nil
}

func validGraphDocumentJSONPointer(path string) bool {
	if !strings.HasPrefix(path, "/") {
		return false
	}
	for i := 0; i < len(path); i++ {
		if path[i] != '~' {
			continue
		}
		if i+1 >= len(path) || (path[i+1] != '0' && path[i+1] != '1') {
			return false
		}
		i++
	}
	return true
}

// NewGraphMatchQuery wraps a MATCH query in the canonical GraphQuery union.
func NewGraphMatchQuery(query GraphMatchQuery) (GraphQuery, error) {
	if err := validateGraphMatchQuery(query); err != nil {
		return GraphQuery{}, err
	}
	var result GraphQuery
	err := result.FromGraphMatchQuery(query)
	return result, err
}

// NewGraphTraverseQuery wraps a traversal query in the canonical GraphQuery union.
func NewGraphTraverseQuery(query GraphTraverseQuery) (GraphQuery, error) {
	if err := validateGraphTraverseQuery(query); err != nil {
		return GraphQuery{}, err
	}
	var result GraphQuery
	err := result.FromGraphTraverseQuery(query)
	return result, err
}

// NewGraphShortestPathQuery wraps a shortest-path query in the canonical GraphQuery union.
func NewGraphShortestPathQuery(query GraphShortestPathQuery) (GraphQuery, error) {
	if err := validateGraphPathQuery(query.Index, query.ShortestPath.From, query.ShortestPath.To, query.ShortestPath.EdgeTypes, query.ShortestPath.MaxDepth, query.ShortestPath.MinWeight, query.ShortestPath.MaxWeight, query.ShortestPath.IncludeDocuments, query.ShortestPath.Fields); err != nil {
		return GraphQuery{}, err
	}
	var result GraphQuery
	err := result.FromGraphShortestPathQuery(query)
	return result, err
}

// NewGraphKShortestPathsQuery wraps a k-shortest-paths query in the canonical GraphQuery union.
func NewGraphKShortestPathsQuery(query GraphKShortestPathsQuery) (GraphQuery, error) {
	if query.KShortestPaths.K < 1 || query.KShortestPaths.K > 100 {
		return GraphQuery{}, fmt.Errorf("antfly: graph k must be between 1 and 100")
	}
	if err := validateGraphPathQuery(query.Index, query.KShortestPaths.From, query.KShortestPaths.To, query.KShortestPaths.EdgeTypes, query.KShortestPaths.MaxDepth, query.KShortestPaths.MinWeight, query.KShortestPaths.MaxWeight, query.KShortestPaths.IncludeDocuments, query.KShortestPaths.Fields); err != nil {
		return GraphQuery{}, err
	}
	var result GraphQuery
	err := result.FromGraphKShortestPathsQuery(query)
	return result, err
}

// NewGraphKeySelector selects exact keys in the query table.
func NewGraphKeySelector(keys ...string) (GraphNodeSelector, error) {
	if len(keys) > 10_000 {
		return GraphNodeSelector{}, fmt.Errorf("antfly: graph keys must contain at most 10000 entries")
	}
	if err := validateNonEmptyUnique("graph key", keys); err != nil {
		return GraphNodeSelector{}, err
	}
	var result GraphNodeSelector
	err := result.FromGraphKeyNodeSelector(GraphKeyNodeSelector{Keys: keys})
	return result, err
}

// NewGraphIdentitySelector selects exact table-qualified node identities.
func NewGraphIdentitySelector(identities ...GraphPathEndpoint) (GraphNodeSelector, error) {
	if err := validateGraphIdentities(identities); err != nil {
		return GraphNodeSelector{}, err
	}
	var result GraphNodeSelector
	err := result.FromGraphIdentityNodeSelector(GraphIdentityNodeSelector{Identities: identities})
	return result, err
}

// NewGraphResultRefSelector selects a prior complete result set. A zero limit
// means unbounded and is accepted only when the referenced result is complete.
func NewGraphResultRefSelector(resultRef string, limit int) (GraphNodeSelector, error) {
	if !validGraphResultRef(resultRef) {
		return GraphNodeSelector{}, fmt.Errorf("antfly: unsupported graph result reference %q", resultRef)
	}
	if limit < 0 || limit > 10_000 {
		return GraphNodeSelector{}, fmt.Errorf("antfly: graph result reference limit must be 0 or between 1 and 10000")
	}
	var result GraphNodeSelector
	err := result.FromGraphResultRefNodeSelector(GraphResultRefNodeSelector{ResultRef: resultRef, Limit: limit})
	return result, err
}

// NewGraphResultBindingSelector selects one returned alias from a prior MATCH
// query. Selecting the alias explicitly avoids flattening unrelated bindings.
func NewGraphResultBindingSelector(queryName, binding string, limit int) (GraphNodeSelector, error) {
	if !validGraphQueryName(queryName) {
		return GraphNodeSelector{}, invalidGraphIdentifier("graph result query name")
	}
	if !validGraphIdentifier(binding) {
		return GraphNodeSelector{}, invalidGraphIdentifier("graph result binding")
	}
	if limit < 0 || limit > 10_000 {
		return GraphNodeSelector{}, fmt.Errorf("antfly: graph result reference limit must be 0 or between 1 and 10000")
	}
	var result GraphNodeSelector
	err := result.FromGraphResultRefNodeSelector(GraphResultRefNodeSelector{
		ResultRef: "$graph_results." + queryName,
		Binding:   binding,
		Limit:     limit,
	})
	return result, err
}

// NewGraphBindingsReturn selects projected aliases and optional stored fields.
func NewGraphBindingsReturn(bindings []string, options GraphBindingsOptions) (GraphReturn, error) {
	if err := validateGraphBindingsProjection(bindings, options.Limit, options.IncludeDocuments, options.Fields); err != nil {
		return GraphReturn{}, err
	}
	var result GraphReturn
	err := result.FromGraphBindingsReturn(GraphBindingsReturn{
		Bindings:         bindings,
		Limit:            options.Limit,
		IncludeDocuments: options.IncludeDocuments,
		Fields:           options.Fields,
	})
	return result, err
}

// NewGraphAggregatesReturn selects named exact graph aggregates.
func NewGraphAggregatesReturn(aggregates map[string]GraphCountAggregate) (GraphReturn, error) {
	if err := validateGraphAggregates(aggregates, nil); err != nil {
		return GraphReturn{}, err
	}
	var result GraphReturn
	err := result.FromGraphAggregatesReturn(GraphAggregatesReturn{Aggregates: aggregates})
	return result, err
}

// CountGraphRows returns count(*) for graph bindings.
func CountGraphRows() GraphCountAggregate {
	var aggregate GraphCountAggregate
	if err := aggregate.FromGraphRowCountAggregate(GraphRowCountAggregate{
		Count: GraphRowCountTarget("*"),
	}); err != nil {
		panic(fmt.Sprintf("antfly: construct count(*): %v", err))
	}
	return aggregate
}

// CountGraphAlias counts non-null bindings for alias. Set distinct to count
// unique (table, key) node identities.
func CountGraphAlias(alias string, distinct bool) (GraphCountAggregate, error) {
	if !validGraphIdentifier(alias) {
		return GraphCountAggregate{}, invalidGraphIdentifier("graph count alias")
	}
	var aggregate GraphCountAggregate
	if err := aggregate.FromGraphAliasCountAggregate(GraphAliasCountAggregate{
		Count:    alias,
		Distinct: distinct,
	}); err != nil {
		return GraphCountAggregate{}, fmt.Errorf("antfly: construct count(%s): %w", alias, err)
	}
	return aggregate, nil
}

// NewGraphNotEqual rejects rows where two aliases resolve to the same exact
// (table, key) node identity.
func NewGraphNotEqual(left, right string) (GraphWhereExpression, error) {
	if !validGraphIdentifier(left) || !validGraphIdentifier(right) {
		return GraphWhereExpression{}, invalidGraphIdentifier("graph inequality aliases")
	}
	if left == right {
		return GraphWhereExpression{}, fmt.Errorf("antfly: graph inequality aliases must differ")
	}
	var result GraphWhereExpression
	err := result.FromGraphWhereNotEqual(GraphWhereNotEqual{
		NotEqual: GraphNotEqualPredicate{
			Left:  GraphAliasOperand{Alias: left},
			Right: GraphAliasOperand{Alias: right},
		},
	})
	return result, err
}

// NewGraphNotExists creates a correlated negative-edge predicate.
func NewGraphNotExists(edges []GraphMatchEdge) (GraphWhereExpression, error) {
	if len(edges) == 0 || len(edges) > maxGraphMatchEdges {
		return GraphWhereExpression{}, fmt.Errorf("antfly: graph not-exists edges must contain between 1 and %d entries", maxGraphMatchEdges)
	}
	for _, edge := range edges {
		if err := validateGraphMatchEdgeShape(edge); err != nil {
			return GraphWhereExpression{}, err
		}
	}
	var result GraphWhereExpression
	err := result.FromGraphWhereNotExists(GraphWhereNotExists{
		NotExists: GraphNotExistsPattern{Edges: edges},
	})
	return result, err
}

// NewGraphWhereAnd combines graph predicates conjunctively.
func NewGraphWhereAnd(expressions ...GraphWhereExpression) (GraphWhereExpression, error) {
	if len(expressions) == 0 || len(expressions) > maxGraphMatchPredicates {
		return GraphWhereExpression{}, fmt.Errorf("antfly: graph where-and must contain between 1 and %d expressions", maxGraphMatchPredicates)
	}
	var result GraphWhereExpression
	err := result.FromGraphWhereAnd(GraphWhereAnd{And: expressions})
	return result, err
}

func validateGraphMatchQuery(query GraphMatchQuery) error {
	if strings.TrimSpace(query.Index) == "" {
		return fmt.Errorf("antfly: graph index must not be empty")
	}
	if len(query.Match.Nodes) == 0 || len(query.Match.Nodes) > maxGraphMatchNodes {
		return fmt.Errorf("antfly: graph match nodes must contain between 1 and %d aliases", maxGraphMatchNodes)
	}
	if len(query.Match.Edges) > maxGraphMatchEdges {
		return fmt.Errorf("antfly: graph match exceeds the %d-edge complexity budget", maxGraphMatchEdges)
	}
	if len(query.Match.Optional) > maxGraphOptionalPatterns {
		return fmt.Errorf("antfly: graph match exceeds the %d optional-pattern complexity budget", maxGraphOptionalPatterns)
	}
	complexity := graphMatchComplexity{nodes: len(query.Match.Nodes), edges: len(query.Match.Edges)}
	for alias, node := range query.Match.Nodes {
		if !validGraphIdentifier(alias) {
			return invalidGraphIdentifier("graph alias")
		}
		if node.Table != "" && strings.TrimSpace(node.Table) == "" {
			return fmt.Errorf("antfly: graph alias %q table must not be blank", alias)
		}
	}
	visible := make(map[string]struct{}, len(query.Match.Nodes))
	for alias := range query.Match.Nodes {
		visible[alias] = struct{}{}
	}
	if !validGraphIdentifier(query.Match.Anchor) {
		return invalidGraphIdentifier("graph match anchor")
	}
	if _, ok := visible[query.Match.Anchor]; !ok {
		return fmt.Errorf("antfly: graph match anchor %q is not declared in nodes", query.Match.Anchor)
	}
	for _, edge := range query.Match.Edges {
		if err := validateGraphMatchEdge(edge, visible); err != nil {
			return err
		}
	}
	if len(query.Match.Nodes) > 1 {
		adjacent := make(map[string][]string, len(query.Match.Nodes))
		for _, edge := range query.Match.Edges {
			adjacent[edge.From] = append(adjacent[edge.From], edge.To)
			adjacent[edge.To] = append(adjacent[edge.To], edge.From)
		}
		var first string
		for alias := range query.Match.Nodes {
			first = alias
			break
		}
		seen := map[string]struct{}{first: {}}
		queue := []string{first}
		for len(queue) > 0 {
			alias := queue[0]
			queue = queue[1:]
			for _, next := range adjacent[alias] {
				if _, ok := seen[next]; ok {
					continue
				}
				seen[next] = struct{}{}
				queue = append(queue, next)
			}
		}
		if len(seen) != len(query.Match.Nodes) {
			return fmt.Errorf("antfly: graph match nodes must form one connected pattern")
		}
	}
	if err := validateGraphWhereExpression(query.Match.Where, visible, &complexity, 0); err != nil {
		return err
	}
	for _, optional := range query.Match.Optional {
		if err := complexity.addNodes(len(optional.Nodes)); err != nil {
			return err
		}
		if len(optional.Edges) == 0 {
			return fmt.Errorf("antfly: optional graph pattern edges must not be empty")
		}
		if err := complexity.addEdges(len(optional.Edges)); err != nil {
			return err
		}
		introduced := make(map[string]struct{}, len(optional.Nodes))
		for alias := range optional.Nodes {
			if !validGraphIdentifier(alias) {
				return invalidGraphIdentifier("optional graph alias")
			}
			if _, exists := visible[alias]; exists {
				return fmt.Errorf("antfly: duplicate optional graph alias %q", alias)
			}
			introduced[alias] = struct{}{}
		}
		optionalVisible := make(map[string]struct{}, len(visible)+len(introduced))
		for alias := range visible {
			optionalVisible[alias] = struct{}{}
		}
		for alias := range introduced {
			optionalVisible[alias] = struct{}{}
		}
		for _, edge := range optional.Edges {
			if err := validateGraphMatchEdge(edge, optionalVisible); err != nil {
				return err
			}
		}
		connected := make(map[string]struct{}, len(introduced))
		changed := true
		for changed {
			changed = false
			for _, edge := range optional.Edges {
				_, fromPrior := visible[edge.From]
				_, toPrior := visible[edge.To]
				_, fromConnected := connected[edge.From]
				_, toConnected := connected[edge.To]
				if fromPrior || fromConnected {
					if _, isNew := introduced[edge.To]; isNew && !toConnected {
						connected[edge.To] = struct{}{}
						changed = true
					}
				}
				if toPrior || toConnected {
					if _, isNew := introduced[edge.From]; isNew && !fromConnected {
						connected[edge.From] = struct{}{}
						changed = true
					}
				}
			}
		}
		if len(connected) != len(introduced) {
			return fmt.Errorf("antfly: optional graph pattern must be correlated and connected")
		}
		if err := validateGraphWhereExpression(optional.Where, optionalVisible, &complexity, 0); err != nil {
			return err
		}
		for alias := range introduced {
			visible[alias] = struct{}{}
		}
	}
	return validateGraphReturn(query.Return, query.Match)
}

type graphMatchComplexity struct {
	nodes      int
	edges      int
	predicates int
}

func (c *graphMatchComplexity) addNodes(count int) error {
	c.nodes += count
	if c.nodes > maxGraphMatchNodes {
		return fmt.Errorf("antfly: graph match exceeds the %d-alias complexity budget", maxGraphMatchNodes)
	}
	return nil
}

func (c *graphMatchComplexity) addEdges(count int) error {
	c.edges += count
	if c.edges > maxGraphMatchEdges {
		return fmt.Errorf("antfly: graph match exceeds the %d-edge complexity budget", maxGraphMatchEdges)
	}
	return nil
}

func (c *graphMatchComplexity) addPredicate() error {
	c.predicates++
	if c.predicates > maxGraphMatchPredicates {
		return fmt.Errorf("antfly: graph match exceeds the %d-predicate complexity budget", maxGraphMatchPredicates)
	}
	return nil
}

func validateGraphMatchEdge(edge GraphMatchEdge, aliases map[string]struct{}) error {
	if err := validateGraphMatchEdgeShape(edge); err != nil {
		return err
	}
	if _, ok := aliases[edge.From]; !ok {
		return fmt.Errorf("antfly: graph edge references unknown alias %q", edge.From)
	}
	if _, ok := aliases[edge.To]; !ok {
		return fmt.Errorf("antfly: graph edge references unknown alias %q", edge.To)
	}
	return nil
}

func validateGraphMatchEdgeShape(edge GraphMatchEdge) error {
	if !validGraphIdentifier(edge.From) || !validGraphIdentifier(edge.To) {
		return invalidGraphIdentifier("graph edge aliases")
	}
	minHops, maxHops := edge.MinHops, edge.MaxHops
	if minHops == 0 {
		minHops = 1
	}
	if maxHops == 0 {
		maxHops = 1
	}
	if minHops < 1 || maxHops < 1 || minHops > 64 || maxHops > 64 || minHops > maxHops {
		return fmt.Errorf("antfly: invalid graph edge hop range")
	}
	if err := validateGraphEdgeTypes(edge.Types); err != nil {
		return err
	}
	if err := validateGraphWeightBounds(edge.MinWeight, edge.MaxWeight); err != nil {
		return err
	}
	return nil
}

func validateGraphWhereExpression(where GraphWhereExpression, aliases map[string]struct{}, complexity *graphMatchComplexity, depth int) error {
	if depth >= maxGraphMatchPredicateDepth {
		return fmt.Errorf("antfly: graph where expression exceeds the maximum depth of %d", maxGraphMatchPredicateDepth)
	}
	encoded, err := json.Marshal(where)
	if err != nil {
		return err
	}
	trimmed := strings.TrimSpace(string(encoded))
	if trimmed == "" || trimmed == "null" || trimmed == "{}" {
		return nil
	}
	type whereJSON struct {
		And       []json.RawMessage       `json:"and"`
		NotEqual  *GraphNotEqualPredicate `json:"not_equal"`
		NotExists *GraphNotExistsPattern  `json:"not_exists"`
	}
	var value whereJSON
	if err := json.Unmarshal(encoded, &value); err != nil {
		return err
	}
	forms := 0
	if len(value.And) > 0 {
		forms++
		if len(value.And) > maxGraphMatchPredicates {
			return fmt.Errorf("antfly: graph where-and exceeds %d expressions", maxGraphMatchPredicates)
		}
		for _, child := range value.And {
			var expression GraphWhereExpression
			if err := json.Unmarshal(child, &expression); err != nil {
				return err
			}
			if err := validateGraphWhereExpression(expression, aliases, complexity, depth+1); err != nil {
				return err
			}
		}
	}
	if value.NotEqual != nil {
		forms++
		if err := complexity.addPredicate(); err != nil {
			return err
		}
		if _, ok := aliases[value.NotEqual.Left.Alias]; !ok {
			return fmt.Errorf("antfly: graph predicate references unknown alias %q", value.NotEqual.Left.Alias)
		}
		if _, ok := aliases[value.NotEqual.Right.Alias]; !ok {
			return fmt.Errorf("antfly: graph predicate references unknown alias %q", value.NotEqual.Right.Alias)
		}
	}
	if value.NotExists != nil {
		forms++
		if err := complexity.addPredicate(); err != nil {
			return err
		}
		if len(value.NotExists.Edges) == 0 {
			return fmt.Errorf("antfly: graph not-exists edges must not be empty")
		}
		if err := complexity.addEdges(len(value.NotExists.Edges)); err != nil {
			return err
		}
		for _, edge := range value.NotExists.Edges {
			if err := validateGraphMatchEdge(edge, aliases); err != nil {
				return err
			}
		}
	}
	if forms != 1 {
		return fmt.Errorf("antfly: graph where expression must contain exactly one predicate form")
	}
	return nil
}

func validateGraphReturn(graphReturn GraphReturn, match GraphMatch) error {
	encoded, err := json.Marshal(graphReturn)
	if err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return err
	}
	_, hasBindings := members["bindings"]
	_, hasAggregates := members["aggregates"]
	if hasBindings == hasAggregates {
		return fmt.Errorf("antfly: graph return must contain exactly one of bindings or aggregates")
	}
	known := make(map[string]struct{}, len(match.Nodes))
	for alias := range match.Nodes {
		known[alias] = struct{}{}
	}
	for _, optional := range match.Optional {
		for alias := range optional.Nodes {
			known[alias] = struct{}{}
		}
	}
	if hasBindings {
		if err := rejectUnknownGraphFields("graph return", members, "bindings", "limit", "include_documents", "fields"); err != nil {
			return err
		}
		var value GraphBindingsReturn
		if err := json.Unmarshal(encoded, &value); err != nil {
			return err
		}
		if err := validateGraphBindingsProjection(
			value.Bindings,
			value.Limit,
			value.IncludeDocuments,
			value.Fields,
		); err != nil {
			return err
		}
		for _, binding := range value.Bindings {
			if _, ok := known[binding]; !ok {
				return fmt.Errorf("antfly: graph return references unknown alias %q", binding)
			}
		}
		return nil
	}
	if err := rejectUnknownGraphFields("graph return", members, "aggregates"); err != nil {
		return err
	}
	var value GraphAggregatesReturn
	if err := json.Unmarshal(encoded, &value); err != nil {
		return err
	}
	return validateGraphAggregates(value.Aggregates, known)
}

func rejectUnknownGraphFields(context string, members map[string]json.RawMessage, allowed ...string) error {
	known := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		known[field] = struct{}{}
	}
	unknown := make([]string, 0)
	for field := range members {
		if _, ok := known[field]; !ok {
			unknown = append(unknown, field)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("antfly: %s contains unknown field %q", context, unknown[0])
}

func validateGraphAggregates(aggregates map[string]GraphCountAggregate, aliases map[string]struct{}) error {
	if len(aggregates) == 0 {
		return fmt.Errorf("antfly: graph aggregates must not be empty")
	}
	if len(aggregates) > maxGraphCountAggregates {
		return fmt.Errorf("antfly: graph aggregates exceed the maximum of %d", maxGraphCountAggregates)
	}
	for name, aggregate := range aggregates {
		if !validGraphIdentifier(name) {
			return invalidGraphIdentifier("graph aggregate name")
		}
		count, distinct, err := decodeGraphCountAggregate(aggregate)
		if err != nil {
			return fmt.Errorf("antfly: graph aggregate %q: %w", name, err)
		}
		if strings.TrimSpace(count) == "" {
			return fmt.Errorf("antfly: graph aggregate %q must name an alias or *", name)
		}
		if count == "*" {
			if distinct {
				return fmt.Errorf("antfly: graph aggregate %q cannot use distinct count(*)", name)
			}
			continue
		}
		if !validGraphIdentifier(count) {
			return fmt.Errorf("antfly: graph aggregate %q references an invalid alias", name)
		}
		if aliases != nil {
			if _, ok := aliases[count]; !ok {
				return fmt.Errorf("antfly: graph aggregate %q references unknown alias %q", name, count)
			}
		}
	}
	return nil
}

func decodeGraphCountAggregate(aggregate GraphCountAggregate) (count string, distinct bool, err error) {
	encoded, err := json.Marshal(aggregate)
	if err != nil {
		return "", false, fmt.Errorf("encode count expression: %w", err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return "", false, fmt.Errorf("decode count expression: %w", err)
	}
	if err := rejectUnknownGraphFields("graph count aggregate", members, "count", "distinct"); err != nil {
		return "", false, err
	}
	rawCount, ok := members["count"]
	if !ok {
		return "", false, fmt.Errorf("count expression must contain count")
	}
	if err := json.Unmarshal(rawCount, &count); err != nil {
		return "", false, fmt.Errorf("count must be a string: %w", err)
	}
	if rawDistinct, ok := members["distinct"]; ok {
		if count == "*" {
			return "", false, fmt.Errorf("distinct is only valid for alias counts")
		}
		if err := json.Unmarshal(rawDistinct, &distinct); err != nil {
			return "", false, fmt.Errorf("distinct must be a boolean: %w", err)
		}
	}
	return count, distinct, nil
}

func validateGraphBindingsProjection(bindings []string, limit int, includeDocuments bool, fields []string) error {
	if len(bindings) > maxGraphMatchNodes {
		return fmt.Errorf("antfly: graph bindings exceed the maximum of %d aliases", maxGraphMatchNodes)
	}
	if err := validateNonEmptyUnique("graph binding", bindings); err != nil {
		return err
	}
	for _, binding := range bindings {
		if !validGraphIdentifier(binding) {
			return invalidGraphIdentifier(fmt.Sprintf("graph binding %q", binding))
		}
	}
	if err := validateGraphLimit(limit); err != nil {
		return err
	}
	if len(fields) > 0 && !includeDocuments {
		return fmt.Errorf("antfly: graph binding fields require IncludeDocuments")
	}
	if len(fields) > 0 {
		if err := validateNonEmptyUnique("graph field", fields); err != nil {
			return err
		}
	}
	if !includeDocuments {
		return nil
	}
	effectiveLimit := limit
	if effectiveLimit == 0 {
		effectiveLimit = defaultGraphBindingsLimit
	}
	// Use division rather than multiplication so this remains overflow-safe if
	// either public limit grows independently in a future contract revision.
	if len(bindings) > 0 && effectiveLimit > maxGraphHydratedBindings/len(bindings) {
		return fmt.Errorf(
			"antfly: graph binding document hydration requires limit times bindings to be at most %d (got %d times %d)",
			maxGraphHydratedBindings,
			effectiveLimit,
			len(bindings),
		)
	}
	return nil
}

func validateGraphTraverseQuery(query GraphTraverseQuery) error {
	if strings.TrimSpace(query.Index) == "" {
		return fmt.Errorf("antfly: graph index must not be empty")
	}
	if err := validateGraphSelector(query.Traverse.Start); err != nil {
		return err
	}
	if query.Traverse.MaxDepth < 0 || query.Traverse.MaxDepth > 64 {
		return fmt.Errorf("antfly: graph max depth must be 0 or between 1 and 64")
	}
	if err := validateGraphLimit(query.Traverse.Limit); err != nil {
		return err
	}
	if err := validateGraphEdgeTypes(query.Traverse.EdgeTypes); err != nil {
		return err
	}
	if err := validateGraphWeightBounds(query.Traverse.MinWeight, query.Traverse.MaxWeight); err != nil {
		return err
	}
	if len(query.Traverse.Fields) > 0 && !query.Traverse.IncludeDocuments {
		return fmt.Errorf("antfly: graph traversal fields require IncludeDocuments")
	}
	if len(query.Traverse.Fields) > 0 {
		if err := validateNonEmptyUnique("graph field", query.Traverse.Fields); err != nil {
			return err
		}
	}
	return nil
}

func validateGraphPathQuery(index string, from, to GraphPathEndpoint, edgeTypes []string, maxDepth int, minWeight, maxWeight *float64, includeDocuments bool, fields []string) error {
	if strings.TrimSpace(index) == "" {
		return fmt.Errorf("antfly: graph index must not be empty")
	}
	if from.Key == "" || to.Key == "" {
		return fmt.Errorf("antfly: graph path endpoints must not be empty")
	}
	if maxDepth < 0 || maxDepth > 64 {
		return fmt.Errorf("antfly: graph max depth must be 0 or between 1 and 64")
	}
	if err := validateGraphEdgeTypes(edgeTypes); err != nil {
		return err
	}
	if err := validateGraphWeightBounds(minWeight, maxWeight); err != nil {
		return err
	}
	if len(fields) > 0 && !includeDocuments {
		return fmt.Errorf("antfly: graph path fields require IncludeDocuments")
	}
	if len(fields) > 0 {
		if err := validateNonEmptyUnique("graph field", fields); err != nil {
			return err
		}
	}
	return nil
}

func validateGraphSelector(selector GraphNodeSelector) error {
	encoded, err := json.Marshal(selector)
	if err != nil {
		return err
	}
	var value struct {
		Keys       []string            `json:"keys"`
		Identities []GraphPathEndpoint `json:"identities"`
		ResultRef  string              `json:"result_ref"`
		Binding    string              `json:"binding"`
		Limit      int                 `json:"limit"`
	}
	if err := json.Unmarshal(encoded, &value); err != nil {
		return err
	}
	forms := 0
	if len(value.Keys) > 0 {
		forms++
		if len(value.Keys) > 10_000 {
			return fmt.Errorf("antfly: graph keys must contain at most 10000 entries")
		}
		if err := validateNonEmptyUnique("graph key", value.Keys); err != nil {
			return err
		}
	}
	if len(value.Identities) > 0 {
		forms++
		if err := validateGraphIdentities(value.Identities); err != nil {
			return err
		}
	}
	if value.ResultRef != "" {
		forms++
		if !validGraphResultRef(value.ResultRef) {
			return fmt.Errorf("antfly: unsupported graph result reference %q", value.ResultRef)
		}
		if value.Limit < 0 || value.Limit > 10_000 {
			return fmt.Errorf("antfly: graph result reference limit must be 0 or between 1 and 10000")
		}
		if value.Binding != "" && !strings.HasPrefix(value.ResultRef, "$graph_results.") {
			return fmt.Errorf("antfly: graph result binding requires a prior graph result reference")
		}
		if value.Binding != "" && !validGraphIdentifier(value.Binding) {
			return fmt.Errorf("antfly: graph result binding is invalid")
		}
	}
	if forms != 1 {
		return fmt.Errorf("antfly: graph selector must contain exactly one selector form")
	}
	return nil
}

func validateGraphIdentities(identities []GraphPathEndpoint) error {
	if len(identities) == 0 {
		return fmt.Errorf("antfly: graph identities must not be empty")
	}
	if len(identities) > 10_000 {
		return fmt.Errorf("antfly: graph identities must contain at most 10000 entries")
	}
	seen := make(map[string]struct{}, len(identities))
	for _, identity := range identities {
		if identity.Key == "" {
			return fmt.Errorf("antfly: graph identity key must not be empty")
		}
		identityKey := identity.Table + "\x00" + identity.Key
		if _, ok := seen[identityKey]; ok {
			return fmt.Errorf("antfly: duplicate graph identity %q", identity.Key)
		}
		seen[identityKey] = struct{}{}
	}
	return nil
}

func validGraphResultRef(resultRef string) bool {
	if resultRef == "$query_results" {
		return true
	}
	const prefix = "$graph_results."
	return strings.HasPrefix(resultRef, prefix) && validGraphQueryName(resultRef[len(prefix):])
}

func validGraphQueryName(value string) bool {
	return validGraphIdentifier(value) && value[0] != '$'
}

func validateGraphWeightBounds(minWeight, maxWeight *float64) error {
	if minWeight != nil && (math.IsNaN(*minWeight) || math.IsInf(*minWeight, 0)) {
		return fmt.Errorf("antfly: graph minimum weight must be finite")
	}
	if maxWeight != nil && (math.IsNaN(*maxWeight) || math.IsInf(*maxWeight, 0)) {
		return fmt.Errorf("antfly: graph maximum weight must be finite")
	}
	if minWeight != nil && maxWeight != nil && *minWeight > *maxWeight {
		return fmt.Errorf("antfly: graph minimum weight must not exceed maximum weight")
	}
	return nil
}

func validateGraphEdgeTypes(edgeTypes []string) error {
	if len(edgeTypes) > maxGraphEdgeTypes {
		return fmt.Errorf("antfly: graph edge types must contain at most %d entries", maxGraphEdgeTypes)
	}
	seen := make(map[string]struct{}, len(edgeTypes))
	totalBytes := 0
	for _, edgeType := range edgeTypes {
		if edgeType == "" {
			return fmt.Errorf("antfly: graph edge type must not be empty")
		}
		if _, exists := seen[edgeType]; exists {
			return fmt.Errorf("antfly: duplicate graph edge type %q", edgeType)
		}
		seen[edgeType] = struct{}{}
		totalBytes += len(edgeType)
		if totalBytes > maxGraphEdgeTypeBytes {
			return fmt.Errorf("antfly: graph edge types must total at most %d bytes", maxGraphEdgeTypeBytes)
		}
	}
	return nil
}

func validateGraphLimit(limit int) error {
	if limit < 0 || limit > 10_000 {
		return fmt.Errorf("antfly: graph limit must be 0 or between 1 and 10000")
	}
	return nil
}

func validateNonEmptyUnique(kind string, values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("antfly: %s list must not be empty", kind)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("antfly: %s must not be empty", kind)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("antfly: duplicate %s %q", kind, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func invalidGraphIdentifier(kind string) error {
	return fmt.Errorf("antfly: %s must be 1-%d Unicode code points, have no leading/trailing spaces, non-ASCII whitespace, or control/format characters, and must not begin with $ or equal *", kind, maxGraphIdentifierRunes)
}
