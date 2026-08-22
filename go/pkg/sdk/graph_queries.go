// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sdk

// NewGraphMatchQuery wraps a MATCH query in the canonical GraphQuery union.
func NewGraphMatchQuery(query GraphMatchQuery) (GraphQuery, error) {
	var result GraphQuery
	err := result.FromGraphMatchQuery(query)
	return result, err
}

// NewGraphTraverseQuery wraps a traversal query in the canonical GraphQuery union.
func NewGraphTraverseQuery(query GraphTraverseQuery) (GraphQuery, error) {
	var result GraphQuery
	err := result.FromGraphTraverseQuery(query)
	return result, err
}

// NewGraphShortestPathQuery wraps a shortest-path query in the canonical GraphQuery union.
func NewGraphShortestPathQuery(query GraphShortestPathQuery) (GraphQuery, error) {
	var result GraphQuery
	err := result.FromGraphShortestPathQuery(query)
	return result, err
}

// NewGraphKShortestPathsQuery wraps a k-shortest-paths query in the canonical GraphQuery union.
func NewGraphKShortestPathsQuery(query GraphKShortestPathsQuery) (GraphQuery, error) {
	var result GraphQuery
	err := result.FromGraphKShortestPathsQuery(query)
	return result, err
}

// NewGraphBindingsReturn selects projected aliases with a query-wide row limit.
func NewGraphBindingsReturn(bindings []string, limit int) (GraphReturn, error) {
	var result GraphReturn
	err := result.FromGraphBindingsReturn(GraphBindingsReturn{Bindings: bindings, Limit: limit})
	return result, err
}

// NewGraphAggregatesReturn selects named exact graph aggregates.
func NewGraphAggregatesReturn(aggregates map[string]GraphCountAggregate) (GraphReturn, error) {
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
	var result GraphWhereExpression
	err := result.FromGraphWhereNotExists(GraphWhereNotExists{
		NotExists: GraphNotExistsPattern{Edges: edges},
	})
	return result, err
}

// NewGraphWhereAnd combines graph predicates conjunctively.
func NewGraphWhereAnd(expressions ...GraphWhereExpression) (GraphWhereExpression, error) {
	var result GraphWhereExpression
	err := result.FromGraphWhereAnd(GraphWhereAnd{And: expressions})
	return result, err
}
