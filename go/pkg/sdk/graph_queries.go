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
	"strings"
	"unicode/utf8"

	querydsl "github.com/antflydb/antfly/go/pkg/sdk/query"
)

// GraphBindingsOptions controls row count and optional document projection.
// A zero Limit uses the server default. Fields require IncludeDocuments.
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
	maxGraphIdentifierRunes     = 128
	maxGraphIdentifierBytes     = maxGraphIdentifierRunes * utf8.UTFMax
	maxGraphEdgeTypes           = 64
	maxGraphEdgeTypeBytes       = 64 * 1024
)

// NewGraphDocumentFilter adapts the non-scoring stored-document subset of the
// query DSL to a graph node filter. Query DSL dotted fields are converted to
// canonical RFC 6901 JSON Pointers; an already pointer-shaped field is
// validated and preserved. Analyzer-backed full-text clauses are rejected
// locally because evaluating them against stored JSON would change both their
// semantics and cost model.
func NewGraphDocumentFilter(filter querydsl.Query) (GraphDocumentFilter, error) {
	encoded, err := json.Marshal(filter)
	if err != nil {
		return GraphDocumentFilter{}, err
	}
	if err := validateGraphDocumentFilterJSON(encoded); err != nil {
		return GraphDocumentFilter{}, err
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return GraphDocumentFilter{}, err
	}
	decoded, err = normalizeGraphDocumentFilter(decoded)
	if err != nil {
		return GraphDocumentFilter{}, err
	}
	encoded, err = json.Marshal(decoded)
	if err != nil {
		return GraphDocumentFilter{}, err
	}
	var result GraphDocumentFilter
	if err := json.Unmarshal(encoded, &result); err != nil {
		return GraphDocumentFilter{}, err
	}
	return result, nil
}

// DecodeGraphQueryResult returns the concrete result selected by its stable
// discriminator. During the v0.2 compatibility window, a response without a
// discriminator is decoded as the only structural variant that permits one to
// be absent: LegacyGraphQueryResult.
func DecodeGraphQueryResult(result GraphQueryResult) (any, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return nil, err
	}
	if rawKind, present := envelope["kind"]; present {
		var kind *string
		if err := json.Unmarshal(rawKind, &kind); err != nil || kind == nil {
			return nil, fmt.Errorf("antfly: graph result has an invalid discriminator")
		}
		switch *kind {
		case string(GraphBindingsResultKindBindings):
			return result.AsGraphBindingsResult()
		case string(GraphAggregatesResultKindAggregates):
			return result.AsGraphAggregatesResult()
		case string(GraphNodesResultKindNodes):
			return result.AsGraphNodesResult()
		case string(LegacyGraphQueryResultKindLegacy):
			return decodeLegacyGraphQueryResult(result, envelope)
		default:
			return nil, fmt.Errorf("antfly: unknown graph result discriminator %q", *kind)
		}
	}
	return decodeLegacyGraphQueryResult(result, envelope)
}

func decodeLegacyGraphQueryResult(result GraphQueryResult, envelope map[string]json.RawMessage) (LegacyGraphQueryResult, error) {
	rawType, hasType := envelope["type"]
	rawTotal, hasTotal := envelope["total"]
	if !hasType || !hasTotal {
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: legacy graph result requires type and total")
	}
	var legacyType GraphQueryType
	if err := json.Unmarshal(rawType, &legacyType); err != nil {
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: invalid legacy graph result type: %w", err)
	}
	switch legacyType {
	case GraphQueryTypeNeighbors, GraphQueryTypeTraverse, GraphQueryTypeShortestPath,
		GraphQueryTypeKShortestPaths, GraphQueryTypePattern:
	default:
		return LegacyGraphQueryResult{}, fmt.Errorf("antfly: unknown legacy graph result type %q", legacyType)
	}
	var total *int
	if err := json.Unmarshal(rawTotal, &total); err != nil {
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

// Range query variants share the same flat full-text shape, which makes an
// OpenAPI oneOf impossible to discriminate reliably. Graph filters use
// explicit numeric_range and term_range operator wrappers on the wire and
// canonical JSON Pointers rather than full-text field names.
func normalizeGraphDocumentFilter(value any) (any, error) {
	switch current := value.(type) {
	case map[string]any:
		if fieldValue, ok := current["field"]; ok {
			if _, hasPath := current["path"]; hasPath {
				return nil, fmt.Errorf("antfly: graph document filter cannot contain both field and path")
			}
			field, ok := fieldValue.(string)
			if !ok {
				return nil, fmt.Errorf("antfly: graph document filter field must be a string")
			}
			path, err := graphDocumentPathFromQueryField(field)
			if err != nil {
				return nil, err
			}
			delete(current, "field")
			current["path"] = path
		}
		if pathValue, ok := current["path"]; ok {
			path, ok := pathValue.(string)
			if !ok || !validGraphDocumentJSONPointer(path) {
				return nil, fmt.Errorf("antfly: graph document filter path must be an RFC 6901 JSON Pointer")
			}
		}

		_, isNumericRange := current["numeric_range"]
		_, isTermRange := current["term_range"]
		_, hasPath := current["path"]
		min, hasMin := current["min"]
		max, hasMax := current["max"]
		if !isNumericRange && !isTermRange && hasPath && (hasMin || hasMax) {
			kind := ""
			for _, bound := range []struct {
				value any
				set   bool
			}{{min, hasMin}, {max, hasMax}} {
				if !bound.set {
					continue
				}
				switch bound.value.(type) {
				case string:
					if kind == "numeric_range" {
						return nil, fmt.Errorf("antfly: graph range bounds must have one scalar type")
					}
					kind = "term_range"
				case float64:
					if kind == "term_range" {
						return nil, fmt.Errorf("antfly: graph range bounds must have one scalar type")
					}
					kind = "numeric_range"
				default:
					return nil, fmt.Errorf("antfly: graph range bounds must be strings or numbers")
				}
			}
			if kind == "" {
				return nil, fmt.Errorf("antfly: graph range requires min or max")
			}
			return map[string]any{kind: current}, nil
		}
		for key, child := range current {
			normalized, err := normalizeGraphDocumentFilter(child)
			if err != nil {
				return nil, err
			}
			current[key] = normalized
		}
		return current, nil
	case []any:
		for i, child := range current {
			normalized, err := normalizeGraphDocumentFilter(child)
			if err != nil {
				return nil, err
			}
			current[i] = normalized
		}
		return current, nil
	default:
		return value, nil
	}
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

func validateGraphDocumentFilterJSON(encoded []byte) error {
	var value any
	if err := json.Unmarshal(encoded, &value); err != nil {
		return err
	}
	type entry struct {
		value any
		depth int
	}
	pending := []entry{{value: value}}
	visited := 0
	unsupported := map[string]struct{}{
		"match": {}, "multi_match": {}, "match_phrase": {}, "terms": {},
		"query": {}, "polygon_points": {}, "location": {}, "geometry": {}, "cidr": {},
		"min_lat": {}, "min_lon": {}, "max_lat": {}, "max_lon": {},
		"boost": {},
	}
	for len(pending) > 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		visited++
		if item.depth > 64 || visited > 16_384 {
			return fmt.Errorf("antfly: graph document filter exceeds the query complexity budget")
		}
		switch current := item.value.(type) {
		case map[string]any:
			_, hasField := current["field"]
			hasScalarOperator := false
			for _, key := range []string{"term", "prefix", "regexp", "wildcard", "bool"} {
				if _, ok := current[key]; ok {
					hasScalarOperator = true
					break
				}
			}
			if hasScalarOperator && !hasField {
				return fmt.Errorf("antfly: graph document scalar filters require a field")
			}
			_, hasMin := current["min"]
			_, hasMax := current["max"]
			_, hasStart := current["start"]
			_, hasEnd := current["end"]
			if hasField && !hasScalarOperator && !hasMin && !hasMax && !hasStart && !hasEnd {
				return fmt.Errorf("antfly: graph document range filters require a bound")
			}
			for key, child := range current {
				if _, blocked := unsupported[key]; blocked {
					return fmt.Errorf("antfly: graph document filters do not support analyzer-backed or index-only clause %q", key)
				}
				pending = append(pending, entry{value: child, depth: item.depth + 1})
			}
		case []any:
			for _, child := range current {
				pending = append(pending, entry{value: child, depth: item.depth + 1})
			}
		}
	}
	return nil
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
		return GraphNodeSelector{}, fmt.Errorf("antfly: graph result query name must be a non-$ identifier of at most %d Unicode code points", maxGraphIdentifierRunes)
	}
	if !validGraphIdentifier(binding) {
		return GraphNodeSelector{}, fmt.Errorf("antfly: graph result binding must contain at most %d Unicode code points", maxGraphIdentifierRunes)
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
	if len(bindings) > maxGraphMatchNodes {
		return GraphReturn{}, fmt.Errorf("antfly: graph bindings exceed the maximum of %d aliases", maxGraphMatchNodes)
	}
	if err := validateNonEmptyUnique("graph binding", bindings); err != nil {
		return GraphReturn{}, err
	}
	for _, binding := range bindings {
		if !validGraphIdentifier(binding) {
			return GraphReturn{}, fmt.Errorf("antfly: graph binding %q must contain at most %d Unicode code points", binding, maxGraphIdentifierRunes)
		}
	}
	if err := validateGraphLimit(options.Limit); err != nil {
		return GraphReturn{}, err
	}
	if len(options.Fields) > 0 && !options.IncludeDocuments {
		return GraphReturn{}, fmt.Errorf("antfly: graph binding fields require IncludeDocuments")
	}
	if len(options.Fields) > 0 {
		if err := validateNonEmptyUnique("graph field", options.Fields); err != nil {
			return GraphReturn{}, err
		}
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
	if len(aggregates) == 0 {
		return GraphReturn{}, fmt.Errorf("antfly: graph aggregates must not be empty")
	}
	if len(aggregates) > maxGraphCountAggregates {
		return GraphReturn{}, fmt.Errorf("antfly: graph aggregates exceed the maximum of %d", maxGraphCountAggregates)
	}
	for name, aggregate := range aggregates {
		if strings.TrimSpace(name) == "" || !validGraphIdentifier(name) {
			return GraphReturn{}, fmt.Errorf("antfly: graph aggregate name must be non-empty and contain at most %d Unicode code points", maxGraphIdentifierRunes)
		}
		if strings.TrimSpace(aggregate.Count) == "" {
			return GraphReturn{}, fmt.Errorf("antfly: graph aggregate %q must name an alias or *", name)
		}
		if aggregate.Count == "*" && aggregate.Distinct {
			return GraphReturn{}, fmt.Errorf("antfly: graph aggregate %q cannot use distinct count(*)", name)
		}
		if aggregate.Count != "*" && !validGraphIdentifier(aggregate.Count) {
			return GraphReturn{}, fmt.Errorf("antfly: graph aggregate %q references an invalid alias", name)
		}
	}
	var result GraphReturn
	err := result.FromGraphAggregatesReturn(GraphAggregatesReturn{Aggregates: aggregates})
	return result, err
}

// CountGraphRows returns count(*) for graph bindings.
func CountGraphRows() GraphCountAggregate {
	return GraphCountAggregate{Count: "*"}
}

// CountGraphAlias counts non-null bindings for alias. Set distinct to count
// unique (table, key) node identities.
func CountGraphAlias(alias string, distinct bool) GraphCountAggregate {
	return GraphCountAggregate{Count: alias, Distinct: distinct}
}

// NewGraphNotEqual rejects rows where two aliases resolve to the same exact
// (table, key) node identity.
func NewGraphNotEqual(left, right string) (GraphWhereExpression, error) {
	if !validGraphIdentifier(left) || !validGraphIdentifier(right) {
		return GraphWhereExpression{}, fmt.Errorf("antfly: graph inequality aliases must be non-empty and contain at most %d Unicode code points", maxGraphIdentifierRunes)
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
			return fmt.Errorf("antfly: graph alias must be non-empty and contain at most %d Unicode code points", maxGraphIdentifierRunes)
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
		return fmt.Errorf("antfly: graph match anchor must be non-empty and contain at most %d Unicode code points", maxGraphIdentifierRunes)
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
				return fmt.Errorf("antfly: optional graph alias must be non-empty and contain at most %d Unicode code points", maxGraphIdentifierRunes)
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
		return fmt.Errorf("antfly: graph edge aliases must contain at most %d Unicode code points", maxGraphIdentifierRunes)
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
	var value struct {
		Bindings   []string                       `json:"bindings"`
		Aggregates map[string]GraphCountAggregate `json:"aggregates"`
	}
	if err := json.Unmarshal(encoded, &value); err != nil {
		return err
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
	if len(value.Bindings) == 0 && len(value.Aggregates) == 0 {
		return fmt.Errorf("antfly: graph return must contain bindings or aggregates")
	}
	if len(value.Bindings) > maxGraphMatchNodes {
		return fmt.Errorf("antfly: graph bindings exceed the maximum of %d aliases", maxGraphMatchNodes)
	}
	if len(value.Bindings) > 0 {
		if err := validateNonEmptyUnique("graph binding", value.Bindings); err != nil {
			return err
		}
	}
	for _, binding := range value.Bindings {
		if _, ok := known[binding]; !ok {
			return fmt.Errorf("antfly: graph return references unknown alias %q", binding)
		}
	}
	for name, aggregate := range value.Aggregates {
		if !validGraphIdentifier(name) {
			return fmt.Errorf("antfly: graph aggregate name %q is invalid", name)
		}
		if aggregate.Count == "*" {
			continue
		}
		if _, ok := known[aggregate.Count]; !ok {
			return fmt.Errorf("antfly: graph aggregate %q references unknown alias %q", name, aggregate.Count)
		}
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

func validGraphIdentifier(value string) bool {
	if value == "" || len(value) > maxGraphIdentifierBytes || !utf8.ValidString(value) {
		return false
	}
	// Keep aliases disjoint from the count(*) sentinel and result/control refs.
	if value == "*" || value[0] == '$' || strings.TrimSpace(value) == "" {
		return false
	}
	count := utf8.RuneCountInString(value)
	return count >= 1 && count <= maxGraphIdentifierRunes
}
