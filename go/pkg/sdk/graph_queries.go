// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sdk

import (
	"fmt"
	"strings"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
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
)

// NewGraphDocumentFilter adapts the non-scoring stored-document subset of the
// query DSL to a graph node filter. Analyzer-backed full-text clauses are
// rejected locally because evaluating them against stored JSON would change
// both their semantics and cost model.
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
	decoded, err = normalizeGraphDocumentFilterRanges(decoded)
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

// Range query variants share the same flat full-text shape, which makes an
// OpenAPI oneOf impossible to discriminate reliably. Graph filters use
// explicit numeric_range and term_range operator wrappers on the wire.
func normalizeGraphDocumentFilterRanges(value any) (any, error) {
	switch current := value.(type) {
	case map[string]any:
		// Canonical graph ranges are already discriminated. Keeping this
		// normalization idempotent also prevents a canonical range body from
		// being wrapped a second time when it is nested in a boolean filter.
		if _, ok := current["numeric_range"]; ok {
			return current, nil
		}
		if _, ok := current["term_range"]; ok {
			return current, nil
		}
		_, hasField := current["field"]
		min, hasMin := current["min"]
		max, hasMax := current["max"]
		if hasField && (hasMin || hasMax) {
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
			normalized, err := normalizeGraphDocumentFilterRanges(child)
			if err != nil {
				return nil, err
			}
			current[key] = normalized
		}
		return current, nil
	case []any:
		for i, child := range current {
			normalized, err := normalizeGraphDocumentFilterRanges(child)
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

// NewGraphFilter is retained as a concise compatibility alias for
// NewGraphDocumentFilter.
func NewGraphFilter(filter querydsl.Query) (GraphDocumentFilter, error) {
	return NewGraphDocumentFilter(filter)
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
	if err := validateGraphPathQuery(query.Index, query.ShortestPath.From, query.ShortestPath.To, query.ShortestPath.MaxDepth, query.ShortestPath.IncludeDocuments, query.ShortestPath.Fields); err != nil {
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
	if err := validateGraphPathQuery(query.Index, query.KShortestPaths.From, query.KShortestPaths.To, query.KShortestPaths.MaxDepth, query.KShortestPaths.IncludeDocuments, query.KShortestPaths.Fields); err != nil {
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
	queryName = strings.TrimSpace(queryName)
	binding = strings.TrimSpace(binding)
	if queryName == "" || binding == "" {
		return GraphNodeSelector{}, fmt.Errorf("antfly: graph result query name and binding must not be empty")
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
	if err := validateNonEmptyUnique("graph binding", bindings); err != nil {
		return GraphReturn{}, err
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
		if strings.TrimSpace(name) == "" {
			return GraphReturn{}, fmt.Errorf("antfly: graph aggregate name must not be empty")
		}
		if strings.TrimSpace(aggregate.Count) == "" {
			return GraphReturn{}, fmt.Errorf("antfly: graph aggregate %q must name an alias or *", name)
		}
		if aggregate.Count == "*" && aggregate.Distinct {
			return GraphReturn{}, fmt.Errorf("antfly: graph aggregate %q cannot use distinct count(*)", name)
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
	if strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
		return GraphWhereExpression{}, fmt.Errorf("antfly: graph inequality aliases must not be empty")
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
		if strings.TrimSpace(edge.From) == "" || strings.TrimSpace(edge.To) == "" {
			return GraphWhereExpression{}, fmt.Errorf("antfly: graph not-exists edge aliases must not be empty")
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
	for alias := range query.Match.Nodes {
		if strings.TrimSpace(alias) == "" {
			return fmt.Errorf("antfly: graph alias must not be empty")
		}
	}
	visible := make(map[string]struct{}, len(query.Match.Nodes))
	for alias := range query.Match.Nodes {
		visible[alias] = struct{}{}
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
			if strings.TrimSpace(alias) == "" {
				return fmt.Errorf("antfly: optional graph alias must not be empty")
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
	if _, ok := aliases[edge.From]; !ok {
		return fmt.Errorf("antfly: graph edge references unknown alias %q", edge.From)
	}
	if _, ok := aliases[edge.To]; !ok {
		return fmt.Errorf("antfly: graph edge references unknown alias %q", edge.To)
	}
	if edge.MinHops < 0 || edge.MaxHops < 0 || edge.MinHops > 64 || edge.MaxHops > 64 ||
		(edge.MinHops > 0 && edge.MaxHops > 0 && edge.MinHops > edge.MaxHops) {
		return fmt.Errorf("antfly: invalid graph edge hop range")
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
	for _, binding := range value.Bindings {
		if _, ok := known[binding]; !ok {
			return fmt.Errorf("antfly: graph return references unknown alias %q", binding)
		}
	}
	for name, aggregate := range value.Aggregates {
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

func validateGraphPathQuery(index string, from, to GraphPathEndpoint, maxDepth int, includeDocuments bool, fields []string) error {
	if strings.TrimSpace(index) == "" {
		return fmt.Errorf("antfly: graph index must not be empty")
	}
	if strings.TrimSpace(from.Key) == "" || strings.TrimSpace(to.Key) == "" {
		return fmt.Errorf("antfly: graph path endpoints must not be empty")
	}
	if maxDepth < 0 || maxDepth > 64 {
		return fmt.Errorf("antfly: graph max depth must be 0 or between 1 and 64")
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
		if strings.TrimSpace(identity.Key) == "" {
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
	return strings.HasPrefix(resultRef, prefix) && len(resultRef) > len(prefix)
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
