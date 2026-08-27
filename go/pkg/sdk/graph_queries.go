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
	"time"
	"unicode/utf8"

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
	maxAntflyUnixSeconds        = int64(18_446_744_073)
	maxAntflyUnixNanoseconds    = 709_551_615
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
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return fmt.Errorf("invalid graph query: %w", err)
	}
	match, hasMatch := members["match"]
	traverse, hasTraverse := members["traverse"]
	shortestPath, hasShortestPath := members["shortest_path"]
	kShortestPaths, hasKShortestPaths := members["k_shortest_paths"]
	variants := 0
	for _, present := range []bool{hasMatch, hasTraverse, hasShortestPath, hasKShortestPaths} {
		if present {
			variants++
		}
	}
	if variants != 1 {
		return fmt.Errorf("graph query must contain exactly one of match, traverse, shortest_path, or k_shortest_paths")
	}
	switch {
	case hasMatch:
		if isNullGraphJSON(match) {
			return fmt.Errorf("graph query match must not be null")
		}
		var value GraphMatchQuery
		if err := query.DecodeStrictInto(&value); err != nil {
			return fmt.Errorf("antfly: invalid graph match query: %w", err)
		}
		return validateGraphMatchQuery(value)
	case hasTraverse:
		if isNullGraphJSON(traverse) {
			return fmt.Errorf("graph query traverse must not be null")
		}
		var value GraphTraverseQuery
		if err := query.DecodeStrictInto(&value); err != nil {
			return fmt.Errorf("antfly: invalid graph traverse query: %w", err)
		}
		return validateGraphTraverseQuery(value)
	case hasShortestPath:
		if isNullGraphJSON(shortestPath) {
			return fmt.Errorf("graph query shortest_path must not be null")
		}
		var value GraphShortestPathQuery
		if err := query.DecodeStrictInto(&value); err != nil {
			return fmt.Errorf("antfly: invalid graph shortest-path query: %w", err)
		}
		return validateGraphPathQuery(value.Index, value.ShortestPath.From, value.ShortestPath.To, value.ShortestPath.Filter, value.ShortestPath.EdgeTypes, value.ShortestPath.MaxDepth, value.ShortestPath.MinWeight, value.ShortestPath.MaxWeight, value.ShortestPath.IncludeDocuments, value.ShortestPath.Fields)
	default:
		if isNullGraphJSON(kShortestPaths) {
			return fmt.Errorf("graph query k_shortest_paths must not be null")
		}
		var value GraphKShortestPathsQuery
		if err := query.DecodeStrictInto(&value); err != nil {
			return fmt.Errorf("antfly: invalid graph k-shortest-paths query: %w", err)
		}
		if value.KShortestPaths.K < 1 || value.KShortestPaths.K > 100 {
			return fmt.Errorf("graph k must be between 1 and 100")
		}
		return validateGraphPathQuery(value.Index, value.KShortestPaths.From, value.KShortestPaths.To, value.KShortestPaths.Filter, value.KShortestPaths.EdgeTypes, value.KShortestPaths.MaxDepth, value.KShortestPaths.MinWeight, value.KShortestPaths.MaxWeight, value.KShortestPaths.IncludeDocuments, value.KShortestPaths.Fields)
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
	// Probe control fields and identity presence without copying opaque hydrated
	// documents. A RawMessage map would copy every top-level result value before
	// the selected variant decodes the payload a second time.
	var envelope graphQueryResultEnvelope
	if err := result.DecodeInto(&envelope); err != nil {
		return nil, err
	}
	if envelope.Kind != nil {
		var kind *string
		if err := json.Unmarshal(envelope.Kind, &kind); err != nil || kind == nil {
			return nil, fmt.Errorf("antfly: graph result has an invalid discriminator")
		}
		if *kind == string(LegacyGraphQueryResultKindLegacy) {
			return decodeLegacyGraphQueryResult(result, envelope)
		}
		return decodeCanonicalGraphResult(result, *kind, envelope)
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
	return decodeCanonicalGraphResult(result, *kind, envelope)
}

type graphQueryResultEnvelope struct {
	Kind  json.RawMessage               `json:"kind"`
	Type  json.RawMessage               `json:"type"`
	Total json.RawMessage               `json:"total"`
	Stats *graphQueryStatsPresenceProbe `json:"stats"`
}

type graphQueryStatsPresenceProbe struct {
	ReturnedItems *uint64 `json:"returned_items"`
	Truncated     *bool   `json:"truncated"`
}

type strictGraphResultDecoder interface {
	DecodeStrictInto(any) error
}

func decodeCanonicalGraphResult(
	result strictGraphResultDecoder,
	kind string,
	envelope graphQueryResultEnvelope,
) (any, error) {
	switch kind {
	case string(GraphBindingsResultKindBindings):
		var value GraphBindingsResult
		if err := result.DecodeStrictInto(&value); err != nil {
			return nil, fmt.Errorf("antfly: invalid bindings graph result: %w", err)
		}
		if value.Kind != GraphBindingsResultKindBindings || value.Rows == nil {
			return nil, fmt.Errorf("antfly: bindings graph result requires kind and rows")
		}
		if len(value.Rows) > maxGraphHydratedBindings {
			return nil, fmt.Errorf("antfly: bindings graph result exceeds %d rows", maxGraphHydratedBindings)
		}
		for rowIndex, row := range value.Rows {
			if len(row) == 0 || len(row) > maxGraphMatchNodes {
				return nil, fmt.Errorf("antfly: bindings graph result row %d must contain between 1 and %d properties", rowIndex, maxGraphMatchNodes)
			}
			for alias, binding := range row {
				if !validGraphIdentifier(alias) {
					return nil, fmt.Errorf("antfly: bindings graph result row %d has an invalid alias", rowIndex)
				}
				if binding != nil {
					if err := validateDecodedGraphIdentity(binding.Key, binding.Table); err != nil {
						return nil, fmt.Errorf("antfly: bindings graph result row %d alias %q: %w", rowIndex, alias, err)
					}
				}
			}
		}
		if err := validateDecodedGraphStats(envelope, len(value.Rows), true); err != nil {
			return nil, err
		}
		return value, nil
	case string(GraphAggregatesResultKindAggregates):
		var value GraphAggregatesResult
		if err := result.DecodeStrictInto(&value); err != nil {
			return nil, fmt.Errorf("antfly: invalid aggregates graph result: %w", err)
		}
		if value.Kind != GraphAggregatesResultKindAggregates || len(value.Aggregates) == 0 || len(value.Aggregates) > maxGraphCountAggregates {
			return nil, fmt.Errorf("antfly: aggregates graph result requires between 1 and %d aggregates", maxGraphCountAggregates)
		}
		for name, aggregate := range value.Aggregates {
			if !validGraphIdentifier(name) {
				return nil, fmt.Errorf("antfly: aggregates graph result has an invalid name")
			}
			if !bool(aggregate.Exact) || !isUnsignedDecimal(aggregate.Value) {
				return nil, fmt.Errorf("antfly: aggregate %q must contain an exact decimal value", name)
			}
		}
		if err := validateDecodedGraphStats(envelope, len(value.Aggregates), false); err != nil {
			return nil, err
		}
		return value, nil
	case string(GraphNodesResultKindNodes):
		var value GraphNodesResult
		if err := result.DecodeStrictInto(&value); err != nil {
			return nil, fmt.Errorf("antfly: invalid nodes graph result: %w", err)
		}
		if value.Kind != GraphNodesResultKindNodes || value.Nodes == nil || value.Paths == nil {
			return nil, fmt.Errorf("antfly: nodes graph result requires kind, nodes, and paths")
		}
		if len(value.Nodes) > maxGraphHydratedBindings || len(value.Paths) > maxGraphHydratedBindings {
			return nil, fmt.Errorf("antfly: nodes graph result exceeds %d items", maxGraphHydratedBindings)
		}
		for i, node := range value.Nodes {
			if err := validateDecodedGraphResultNode(node); err != nil {
				return nil, fmt.Errorf("antfly: nodes graph result node %d: %w", i, err)
			}
		}
		for i, path := range value.Paths {
			if err := validateDecodedGraphPath(path); err != nil {
				return nil, fmt.Errorf("antfly: nodes graph result path %d: %w", i, err)
			}
		}
		if len(value.Paths) > 0 {
			if len(value.Nodes) != len(value.Paths) {
				return nil, fmt.Errorf("antfly: path graph result requires one terminal node per path")
			}
			for i, path := range value.Paths {
				terminal := path.Nodes[len(path.Nodes)-1]
				node := value.Nodes[i]
				if !sameDecodedGraphEndpoint(terminal, GraphPathEndpoint{Key: node.Key, Table: node.Table}) {
					return nil, fmt.Errorf("antfly: path graph result node %d does not match its path terminal", i)
				}
			}
		}
		primaryItems := len(value.Nodes)
		if len(value.Paths) > 0 {
			primaryItems = len(value.Paths)
		}
		if err := validateDecodedGraphStats(envelope, primaryItems, true); err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("antfly: unknown canonical graph result discriminator %q", kind)
	}
}

func validateDecodedGraphStats(envelope graphQueryResultEnvelope, itemCount int, allowTruncated bool) error {
	if envelope.Stats == nil || envelope.Stats.ReturnedItems == nil || envelope.Stats.Truncated == nil {
		return fmt.Errorf("antfly: canonical graph result requires complete stats")
	}
	if *envelope.Stats.ReturnedItems != uint64(itemCount) {
		return fmt.Errorf("antfly: graph result stats returned_items does not match the payload")
	}
	if !allowTruncated && *envelope.Stats.Truncated {
		return fmt.Errorf("antfly: exact aggregate graph results cannot be truncated")
	}
	return nil
}

func validateDecodedGraphIdentity(key string, table *string) error {
	if key == "" {
		return fmt.Errorf("graph node key must not be empty")
	}
	if table != nil && *table == "" {
		return fmt.Errorf("graph node table must be omitted or non-empty")
	}
	return nil
}

func validateDecodedGraphResultNode(node GraphResultNode) error {
	if err := validateDecodedGraphIdentity(node.Key, node.Table); err != nil {
		return err
	}
	if node.Depth < 0 || node.Depth > maxGraphMatchEdges {
		return fmt.Errorf("graph node depth must be between 0 and %d", maxGraphMatchEdges)
	}
	if node.Path != nil {
		if len(node.Path) == 0 || len(node.Path) > maxGraphMatchEdges+1 {
			return fmt.Errorf("graph node path must contain between 1 and %d nodes", maxGraphMatchEdges+1)
		}
		if node.Depth != len(node.Path)-1 {
			return fmt.Errorf("graph node depth must equal path length minus one")
		}
		for _, endpoint := range node.Path {
			if err := validateDecodedGraphIdentity(endpoint.Key, endpoint.Table); err != nil {
				return err
			}
		}
		last := node.Path[len(node.Path)-1]
		if !sameDecodedGraphEndpoint(last, GraphPathEndpoint{Key: node.Key, Table: node.Table}) {
			return fmt.Errorf("graph node path must terminate at the result node")
		}
	}
	if node.PathEdges != nil {
		if node.Path == nil || len(node.PathEdges)+1 != len(node.Path) {
			return fmt.Errorf("graph node path_edges must align with path")
		}
		for i, edge := range node.PathEdges {
			if err := validateDecodedGraphPathEdge(edge, node.Path[i], node.Path[i+1], false); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateDecodedGraphPath(path GraphPath) error {
	if path.Nodes == nil || path.Edges == nil || len(path.Nodes) == 0 || len(path.Nodes) > maxGraphMatchEdges+1 {
		return fmt.Errorf("graph path requires bounded nodes and edges")
	}
	if path.Length != len(path.Edges) || len(path.Nodes) != len(path.Edges)+1 {
		return fmt.Errorf("graph path length, nodes, and edges do not align")
	}
	for _, endpoint := range path.Nodes {
		if err := validateDecodedGraphIdentity(endpoint.Key, endpoint.Table); err != nil {
			return err
		}
	}
	var sum float64
	product := 1.0
	for i, edge := range path.Edges {
		maxWeightMode := path.WeightMode == PathWeightModeMaxWeight
		if err := validateDecodedGraphPathEdge(edge, path.Nodes[i], path.Nodes[i+1], maxWeightMode); err != nil {
			return err
		}
		sum += edge.Weight
		product *= edge.Weight
		if !math.IsInf(sum, 0) && !math.IsNaN(sum) && !math.IsInf(product, 0) && !math.IsNaN(product) {
			continue
		}
		return fmt.Errorf("graph path score overflow")
	}
	if !finiteNonNegative(path.WeightSum) || !finiteNonNegative(path.ObjectiveValue) || !graphFloatEqual(path.WeightSum, sum) {
		return fmt.Errorf("graph path has inconsistent weight_sum")
	}
	var objective float64
	switch path.WeightMode {
	case PathWeightModeMinHops:
		objective = float64(len(path.Edges))
	case PathWeightModeMinWeight:
		objective = sum
	case PathWeightModeMaxWeight:
		objective = product
	default:
		return fmt.Errorf("graph path has an unknown weight_mode")
	}
	if !graphFloatEqual(path.ObjectiveValue, objective) {
		return fmt.Errorf("graph path has an inconsistent objective_value")
	}
	return nil
}

func validateDecodedGraphPathEdge(edge GraphPathEdge, from, to GraphPathEndpoint, maxWeightMode bool) error {
	if edge.Type == "" || len(edge.Type) > maxGraphEdgeTypeBytes || !utf8.ValidString(edge.Type) {
		return fmt.Errorf("graph path edge type must encode to between 1 and %d UTF-8 bytes", maxGraphEdgeTypeBytes)
	}
	if !sameDecodedGraphEndpoint(edge.From, from) || !sameDecodedGraphEndpoint(edge.To, to) {
		return fmt.Errorf("graph path edge does not match adjacent nodes")
	}
	if !finiteNonNegative(edge.Weight) || maxWeightMode && edge.Weight > 1 {
		return fmt.Errorf("graph path edge has an invalid weight")
	}
	return nil
}

func sameDecodedGraphEndpoint(left, right GraphPathEndpoint) bool {
	if left.Key != right.Key || (left.Table == nil) != (right.Table == nil) {
		return false
	}
	return left.Table == nil || *left.Table == *right.Table
}

func finiteNonNegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func graphFloatEqual(left, right float64) bool {
	if !finiteNonNegative(left) || !finiteNonNegative(right) {
		return false
	}
	scale := math.Max(1, math.Max(math.Abs(left), math.Abs(right)))
	return math.Abs(left-right) <= 1e-12*scale
}

func isUnsignedDecimal(value string) bool {
	if value == "" {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
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
		start, err := normalizeGraphDateBound(value.Start, "start")
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		end, err := normalizeGraphDateBound(value.End, "end")
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentDateRangeFilter(oapi.GraphDocumentDateRangeFilter{
			DateRange: oapi.GraphDocumentDateRangeBody{
				Start: start, End: end, InclusiveStart: value.InclusiveStart,
				InclusiveEnd: value.InclusiveEnd, Path: path,
			},
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
		err = out.FromGraphDocumentBoolFieldFilter(oapi.GraphDocumentBoolFieldFilter{
			BoolField: oapi.GraphDocumentBoolFieldBody{Path: path, Value: value.Bool},
		})
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

func normalizeGraphDateBound(value *time.Time, name string) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	normalized := value.UTC()
	seconds := normalized.Unix()
	if seconds < 0 || seconds > maxAntflyUnixSeconds ||
		(seconds == maxAntflyUnixSeconds && normalized.Nanosecond() > maxAntflyUnixNanoseconds) {
		return nil, fmt.Errorf(
			"antfly: graph date range %s must fall within the supported Unix-nanosecond range (1970-01-01T00:00:00Z through 2554-07-21T23:34:33.709551615Z)",
			name,
		)
	}
	return &normalized, nil
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

// validateGraphDocumentFilter validates the graph-specific closed union at the
// public request boundary. Generated oneOf wrappers retain raw JSON, so their
// nested members need explicit variant selection before strict concrete-model
// decoding can enforce additionalProperties: false.
func validateGraphDocumentFilter(filter GraphDocumentFilter, depth int, visited *int) error {
	(*visited)++
	if depth > 64 || *visited > 16_384 {
		return fmt.Errorf("antfly: graph document filter exceeds the query complexity budget")
	}
	encoded, err := json.Marshal(filter)
	if err != nil {
		return err
	}
	trimmed := strings.TrimSpace(string(encoded))
	if trimmed == "" || trimmed == "null" {
		return nil // The containing filter property is optional.
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil || len(members) == 0 {
		return fmt.Errorf("antfly: graph document filter must be an object")
	}

	validatePath := func(path string) error {
		if !validGraphDocumentJSONPointer(path) {
			return fmt.Errorf("antfly: graph document filter path must be a valid RFC 6901 JSON Pointer")
		}
		return nil
	}
	present := func(name string) bool {
		raw, ok := members[name]
		return ok && !isNullGraphJSON(raw)
	}

	switch {
	case present("term") && present("fuzziness"):
		var value oapi.GraphDocumentFuzzyFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if err := validatePath(value.Path); err != nil {
			return err
		}
		if value.PrefixLength < 0 || value.PrefixLength > 255 {
			return fmt.Errorf("antfly: graph document filter prefix_length must be between 0 and 255")
		}
		fuzziness, err := json.Marshal(value.Fuzziness)
		if err != nil {
			return err
		}
		var scalar any
		if err := json.Unmarshal(fuzziness, &scalar); err != nil {
			return err
		}
		switch scalar := scalar.(type) {
		case float64:
			if scalar < 0 || scalar > 2 || scalar != math.Trunc(scalar) {
				return fmt.Errorf("antfly: graph document filter fuzziness must be 0, 1, 2, or auto")
			}
		case string:
			if scalar != "auto" {
				return fmt.Errorf("antfly: graph document filter fuzziness must be 0, 1, 2, or auto")
			}
		default:
			return fmt.Errorf("antfly: graph document filter fuzziness must be 0, 1, 2, or auto")
		}
		return nil
	case present("term"):
		var value oapi.GraphDocumentTermFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		return validatePath(value.Path)
	case present("prefix"):
		var value oapi.GraphDocumentPrefixFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		return validatePath(value.Path)
	case present("regexp"):
		var value oapi.GraphDocumentRegexpFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		return validatePath(value.Path)
	case present("wildcard"):
		var value oapi.GraphDocumentWildcardFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		return validatePath(value.Path)
	case present("numeric_range"):
		var value oapi.GraphDocumentNumericRangeFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		body := value.NumericRange
		if err := validatePath(body.Path); err != nil {
			return err
		}
		if body.Min == nil && body.Max == nil {
			return fmt.Errorf("antfly: graph numeric range requires min or max")
		}
		if body.Min != nil && body.Max != nil && *body.Min > *body.Max {
			return fmt.Errorf("antfly: graph numeric range min must not exceed max")
		}
		return nil
	case present("term_range"):
		var value oapi.GraphDocumentTermRangeFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if err := validatePath(value.TermRange.Path); err != nil {
			return err
		}
		if value.TermRange.Min == nil && value.TermRange.Max == nil {
			return fmt.Errorf("antfly: graph term range requires min or max")
		}
		return nil
	case present("date_range"):
		var value oapi.GraphDocumentDateRangeFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if err := validatePath(value.DateRange.Path); err != nil {
			return err
		}
		if value.DateRange.Start == nil && value.DateRange.End == nil {
			return fmt.Errorf("antfly: graph date range requires start or end")
		}
		start, err := normalizeGraphDateBound(value.DateRange.Start, "start")
		if err != nil {
			return err
		}
		end, err := normalizeGraphDateBound(value.DateRange.End, "end")
		if err != nil {
			return err
		}
		if start != nil && end != nil && start.After(*end) {
			return fmt.Errorf("antfly: graph date range start must not exceed end")
		}
		return nil
	case present("match_all"):
		var value oapi.GraphDocumentMatchAllFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if value.MatchAll == nil || len(value.MatchAll) != 0 {
			return fmt.Errorf("antfly: graph match_all body must be an empty object")
		}
		return nil
	case present("match_none"):
		var value oapi.GraphDocumentMatchNoneFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if value.MatchNone == nil || len(value.MatchNone) != 0 {
			return fmt.Errorf("antfly: graph match_none body must be an empty object")
		}
		return nil
	case present("ids"):
		var value oapi.GraphDocumentIdsFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if len(value.Ids) == 0 || len(value.Ids) > 10_000 {
			return fmt.Errorf("antfly: graph ids must contain between 1 and 10000 values")
		}
		return validateNonEmptyUnique("graph id", value.Ids)
	case present("bool_field"):
		var value oapi.GraphDocumentBoolFieldFilter
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		return validatePath(value.BoolField.Path)
	case present("conjuncts"):
		var value GraphDocumentFilterConjunction
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if len(value.Conjuncts) == 0 || len(value.Conjuncts) > 64 {
			return fmt.Errorf("antfly: graph filter conjuncts must contain between 1 and 64 entries")
		}
		for i, child := range value.Conjuncts {
			if err := validateGraphDocumentFilter(child, depth+1, visited); err != nil {
				return fmt.Errorf("antfly: graph filter conjunct %d: %w", i, err)
			}
		}
		return nil
	case present("disjuncts"):
		var value GraphDocumentFilterDisjunction
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if len(value.Disjuncts) == 0 || len(value.Disjuncts) > 64 {
			return fmt.Errorf("antfly: graph filter disjuncts must contain between 1 and 64 entries")
		}
		if value.Min != nil && *value.Min > uint32(len(value.Disjuncts)) {
			return fmt.Errorf("antfly: graph disjunction min exceeds its number of clauses")
		}
		for i, child := range value.Disjuncts {
			if err := validateGraphDocumentFilter(child, depth+1, visited); err != nil {
				return fmt.Errorf("antfly: graph filter disjunct %d: %w", i, err)
			}
		}
		return nil
	case present("filter") || present("must") || present("should") || present("must_not"):
		var value GraphDocumentFilterBoolean
		if err := filter.DecodeStrictInto(&value); err != nil {
			return err
		}
		if present("filter") {
			if err := validateGraphDocumentFilter(value.Filter, depth+1, visited); err != nil {
				return fmt.Errorf("antfly: graph bool filter: %w", err)
			}
		}
		for _, clauseGroup := range []struct {
			name    string
			clauses []GraphDocumentFilter
		}{
			{name: "must", clauses: value.Must.Conjuncts},
			{name: "should", clauses: value.Should.Disjuncts},
			{name: "must_not", clauses: value.MustNot.Disjuncts},
		} {
			name, clause := clauseGroup.name, clauseGroup.clauses
			if !present(name) {
				continue
			}
			if len(clause) == 0 || len(clause) > 64 {
				return fmt.Errorf("antfly: graph bool %s must contain between 1 and 64 entries", name)
			}
			for i, child := range clause {
				if err := validateGraphDocumentFilter(child, depth+1, visited); err != nil {
					return fmt.Errorf("antfly: graph bool %s clause %d: %w", name, i, err)
				}
			}
		}
		if present("should") && value.Should.Min != nil && *value.Should.Min > uint32(len(value.Should.Disjuncts)) {
			return fmt.Errorf("antfly: graph bool should min exceeds its number of clauses")
		}
		if present("must_not") && value.MustNot.Min != nil && *value.MustNot.Min > uint32(len(value.MustNot.Disjuncts)) {
			return fmt.Errorf("antfly: graph bool must_not min exceeds its number of clauses")
		}
		return nil
	default:
		return fmt.Errorf("antfly: unsupported graph document filter variant")
	}
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
	if err := validateGraphPathQuery(query.Index, query.ShortestPath.From, query.ShortestPath.To, query.ShortestPath.Filter, query.ShortestPath.EdgeTypes, query.ShortestPath.MaxDepth, query.ShortestPath.MinWeight, query.ShortestPath.MaxWeight, query.ShortestPath.IncludeDocuments, query.ShortestPath.Fields); err != nil {
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
	if err := validateGraphPathQuery(query.Index, query.KShortestPaths.From, query.KShortestPaths.To, query.KShortestPaths.Filter, query.KShortestPaths.EdgeTypes, query.KShortestPaths.MaxDepth, query.KShortestPaths.MinWeight, query.KShortestPaths.MaxWeight, query.KShortestPaths.IncludeDocuments, query.KShortestPaths.Fields); err != nil {
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

// NewGraphIdentity constructs an exact graph identity without requiring callers
// to manage the generated optional-table pointer. Omit table for the queried
// table; provide exactly one non-empty value for a cross-table identity.
func NewGraphIdentity(key string, table ...string) (GraphPathEndpoint, error) {
	if key == "" {
		return GraphPathEndpoint{}, fmt.Errorf("antfly: graph identity key must not be empty")
	}
	if len(table) > 1 {
		return GraphPathEndpoint{}, fmt.Errorf("antfly: graph identity accepts at most one table")
	}
	result := GraphPathEndpoint{Key: key}
	if len(table) == 1 {
		if strings.TrimSpace(table[0]) == "" {
			return GraphPathEndpoint{}, fmt.Errorf("antfly: graph identity table must not be empty")
		}
		result.Table = &table[0]
	}
	return result, nil
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
	filterVisited := 0
	for alias, node := range query.Match.Nodes {
		if !validGraphIdentifier(alias) {
			return invalidGraphIdentifier("graph alias")
		}
		if node.Table != "" && strings.TrimSpace(node.Table) == "" {
			return fmt.Errorf("antfly: graph alias %q table must not be blank", alias)
		}
		if err := validateGraphDocumentFilter(node.Filter, 0, &filterVisited); err != nil {
			return fmt.Errorf("antfly: graph alias %q filter: %w", alias, err)
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
		for alias, node := range optional.Nodes {
			if !validGraphIdentifier(alias) {
				return invalidGraphIdentifier("optional graph alias")
			}
			if _, exists := visible[alias]; exists {
				return fmt.Errorf("antfly: duplicate optional graph alias %q", alias)
			}
			introduced[alias] = struct{}{}
			if node.Table != "" && strings.TrimSpace(node.Table) == "" {
				return fmt.Errorf("antfly: optional graph alias %q table must not be blank", alias)
			}
			if err := validateGraphDocumentFilter(node.Filter, 0, &filterVisited); err != nil {
				return fmt.Errorf("antfly: optional graph alias %q filter: %w", alias, err)
			}
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
	if edge.Direction != "" && edge.Direction != EdgeDirectionOut &&
		edge.Direction != EdgeDirectionIn && edge.Direction != EdgeDirectionBoth {
		return fmt.Errorf("antfly: graph edge direction must be out, in, or both")
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
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return err
	}
	if err := rejectUnknownGraphFields("graph where expression", members, "and", "not_equal", "not_exists"); err != nil {
		return err
	}
	_, hasAnd := members["and"]
	_, hasNotEqual := members["not_equal"]
	_, hasNotExists := members["not_exists"]
	forms := 0
	for _, present := range []bool{hasAnd, hasNotEqual, hasNotExists} {
		if present {
			forms++
		}
	}
	if forms != 1 {
		return fmt.Errorf("antfly: graph where expression must contain exactly one predicate form")
	}
	if hasAnd {
		var value GraphWhereAnd
		if err := where.DecodeStrictInto(&value); err != nil {
			return err
		}
		if len(value.And) == 0 {
			return fmt.Errorf("antfly: graph where-and must not be empty")
		}
		if len(value.And) > maxGraphMatchPredicates {
			return fmt.Errorf("antfly: graph where-and exceeds %d expressions", maxGraphMatchPredicates)
		}
		for _, child := range value.And {
			if err := validateGraphWhereExpression(child, aliases, complexity, depth+1); err != nil {
				return err
			}
		}
	}
	if hasNotEqual {
		var value GraphWhereNotEqual
		if err := where.DecodeStrictInto(&value); err != nil {
			return err
		}
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
	if hasNotExists {
		var value GraphWhereNotExists
		if err := where.DecodeStrictInto(&value); err != nil {
			return err
		}
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
		var value GraphBindingsReturn
		if err := graphReturn.DecodeStrictInto(&value); err != nil {
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
	var value GraphAggregatesReturn
	if err := graphReturn.DecodeStrictInto(&value); err != nil {
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

func isNullGraphJSON(value json.RawMessage) bool {
	return strings.TrimSpace(string(value)) == "null"
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
	filterVisited := 0
	if err := validateGraphDocumentFilter(query.Traverse.Filter, 0, &filterVisited); err != nil {
		return fmt.Errorf("antfly: graph traversal filter: %w", err)
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

func validateGraphPathQuery(index string, from, to GraphPathEndpoint, filter GraphDocumentFilter, edgeTypes []string, maxDepth int, minWeight, maxWeight *float64, includeDocuments bool, fields []string) error {
	if strings.TrimSpace(index) == "" {
		return fmt.Errorf("antfly: graph index must not be empty")
	}
	if from.Key == "" || to.Key == "" {
		return fmt.Errorf("antfly: graph path endpoints must not be empty")
	}
	filterVisited := 0
	if err := validateGraphDocumentFilter(filter, 0, &filterVisited); err != nil {
		return fmt.Errorf("antfly: graph path filter: %w", err)
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
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return err
	}
	if err := rejectUnknownGraphFields("graph selector", members, "keys", "identities", "result_ref", "binding", "limit"); err != nil {
		return err
	}
	_, hasKeys := members["keys"]
	_, hasIdentities := members["identities"]
	_, hasResultRef := members["result_ref"]
	forms := 0
	for _, present := range []bool{hasKeys, hasIdentities, hasResultRef} {
		if present {
			forms++
		}
	}
	if forms != 1 {
		return fmt.Errorf("antfly: graph selector must contain exactly one selector form")
	}
	if hasKeys {
		var value GraphKeyNodeSelector
		if err := selector.DecodeStrictInto(&value); err != nil {
			return err
		}
		if len(value.Keys) > 10_000 {
			return fmt.Errorf("antfly: graph keys must contain at most 10000 entries")
		}
		if err := validateNonEmptyUnique("graph key", value.Keys); err != nil {
			return err
		}
	}
	if hasIdentities {
		var value GraphIdentityNodeSelector
		if err := selector.DecodeStrictInto(&value); err != nil {
			return err
		}
		if err := validateGraphIdentities(value.Identities); err != nil {
			return err
		}
	}
	if hasResultRef {
		var value GraphResultRefNodeSelector
		if err := selector.DecodeStrictInto(&value); err != nil {
			return err
		}
		if isNullGraphJSON(members["result_ref"]) {
			return fmt.Errorf("antfly: graph result reference must not be null")
		}
		if binding, present := members["binding"]; present && isNullGraphJSON(binding) {
			return fmt.Errorf("antfly: graph result binding must not be null")
		}
		if limit, present := members["limit"]; present && isNullGraphJSON(limit) {
			return fmt.Errorf("antfly: graph result reference limit must not be null")
		}
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
		if identity.Table != nil && strings.TrimSpace(*identity.Table) == "" {
			return fmt.Errorf("antfly: graph identity table must not be empty")
		}
		table := ""
		if identity.Table != nil {
			table = *identity.Table
		}
		identityKey := table + "\x00" + identity.Key
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
		if edgeType == "" || !utf8.ValidString(edgeType) {
			return fmt.Errorf("antfly: graph edge type must be non-empty valid UTF-8")
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
