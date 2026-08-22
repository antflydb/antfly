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

// NewGraphFilter adapts the same typed query value used by QueryRequest to a
// graph node filter. The wire representation is unchanged.
func NewGraphFilter(filter querydsl.Query) (GraphFilter, error) {
	encoded, err := json.Marshal(filter)
	if err != nil {
		return GraphFilter{}, err
	}
	var result GraphFilter
	if err := json.Unmarshal(encoded, &result); err != nil {
		return GraphFilter{}, err
	}
	return result, nil
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
	if len(edges) == 0 {
		return GraphWhereExpression{}, fmt.Errorf("antfly: graph not-exists edges must not be empty")
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
	if len(expressions) == 0 {
		return GraphWhereExpression{}, fmt.Errorf("antfly: graph where-and expressions must not be empty")
	}
	var result GraphWhereExpression
	err := result.FromGraphWhereAnd(GraphWhereAnd{And: expressions})
	return result, err
}

func validateGraphMatchQuery(query GraphMatchQuery) error {
	if strings.TrimSpace(query.Index) == "" {
		return fmt.Errorf("antfly: graph index must not be empty")
	}
	if len(query.Match.Nodes) == 0 {
		return fmt.Errorf("antfly: graph match nodes must not be empty")
	}
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
	if err := validateGraphWhereExpression(query.Match.Where, visible); err != nil {
		return err
	}
	for _, optional := range query.Match.Optional {
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
		if err := validateGraphWhereExpression(optional.Where, optionalVisible); err != nil {
			return err
		}
		for alias := range introduced {
			visible[alias] = struct{}{}
		}
	}
	return validateGraphReturn(query.Return, query.Match)
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

func validateGraphWhereExpression(where GraphWhereExpression, aliases map[string]struct{}) error {
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
		for _, child := range value.And {
			var expression GraphWhereExpression
			if err := json.Unmarshal(child, &expression); err != nil {
				return err
			}
			if err := validateGraphWhereExpression(expression, aliases); err != nil {
				return err
			}
		}
	}
	if value.NotEqual != nil {
		forms++
		if _, ok := aliases[value.NotEqual.Left.Alias]; !ok {
			return fmt.Errorf("antfly: graph predicate references unknown alias %q", value.NotEqual.Left.Alias)
		}
		if _, ok := aliases[value.NotEqual.Right.Alias]; !ok {
			return fmt.Errorf("antfly: graph predicate references unknown alias %q", value.NotEqual.Right.Alias)
		}
	}
	if value.NotExists != nil {
		forms++
		if len(value.NotExists.Edges) == 0 {
			return fmt.Errorf("antfly: graph not-exists edges must not be empty")
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
