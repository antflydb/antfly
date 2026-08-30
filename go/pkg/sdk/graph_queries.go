// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"slices"
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
	maxGraphPathResults         = 100
	maxNamedGraphQueries        = 64
	maxGraphMatchQueries        = 8
	defaultGraphBindingsLimit   = 100
	defaultGraphTraversalDepth  = 1
	defaultGraphPathDepth       = 10
	maxAntflyUnixSeconds        = int64(18_446_744_073)
	maxAntflyUnixNanoseconds    = 709_551_615
)

func validateNamedGraphQueries(queries map[string]GraphQuery) error {
	if len(queries) == 0 {
		return fmt.Errorf("antfly: graph_queries must contain at least one named operation")
	}
	if len(queries) > maxNamedGraphQueries {
		return fmt.Errorf("antfly: graph_queries accepts at most %d named operations", maxNamedGraphQueries)
	}
	matchQueries := 0
	for name, query := range queries {
		if !validGraphQueryName(name) {
			return invalidGraphIdentifier("graph_queries key")
		}
		isMatch, err := validateGraphQueryWithKind(query)
		if err != nil {
			return fmt.Errorf("antfly: graph_queries[%q]: %w", name, err)
		}
		if isMatch {
			matchQueries++
			if matchQueries > maxGraphMatchQueries {
				return fmt.Errorf("antfly: graph_queries accepts at most %d match operations", maxGraphMatchQueries)
			}
		}
	}
	return nil
}

func validateGraphQuery(query GraphQuery) error {
	_, err := validateGraphQueryWithKind(query)
	return err
}

// validateGraphQueryWithKind validates a query and reports whether it is a
// MATCH. Request-level admission uses that bit to enforce the independent
// complete-anchor scan cap. Structural union mechanics live beside the
// generated type so every SDK caller gets the same presence-safe strict decode.
func validateGraphQueryWithKind(query GraphQuery) (bool, error) {
	decoded, err := query.DecodeStrictVariant()
	if err != nil {
		return false, fmt.Errorf("invalid graph query: %w", err)
	}
	switch decoded.Kind {
	case oapi.GraphQueryVariantMatch:
		return true, validateGraphMatchQuery(*decoded.Match)
	case oapi.GraphQueryVariantTraverse:
		return false, validateGraphTraverseQuery(*decoded.Traverse)
	case oapi.GraphQueryVariantShortestPath:
		value := decoded.ShortestPath
		return false, validateGraphPathQuery(value.Index, value.ShortestPath.From, value.ShortestPath.To, value.ShortestPath.Direction, value.ShortestPath.Filter, value.ShortestPath.EdgeTypes, value.ShortestPath.MaxDepth, value.ShortestPath.EdgeWeight, value.ShortestPath.Objective, value.ShortestPath.IncludeDocuments, value.ShortestPath.Fields)
	case oapi.GraphQueryVariantKShortestPaths:
		value := decoded.KShortestPaths
		if value.KShortestPaths.K < 1 || value.KShortestPaths.K > 100 {
			return false, fmt.Errorf("graph k must be between 1 and 100")
		}
		return false, validateGraphPathQuery(value.Index, value.KShortestPaths.From, value.KShortestPaths.To, value.KShortestPaths.Direction, value.KShortestPaths.Filter, value.KShortestPaths.EdgeTypes, value.KShortestPaths.MaxDepth, value.KShortestPaths.EdgeWeight, value.KShortestPaths.Objective, value.KShortestPaths.IncludeDocuments, value.KShortestPaths.Fields)
	default:
		return false, fmt.Errorf("invalid graph query: unknown operation")
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

// DecodeGraphResult returns the concrete canonical response selected by its
// required stable discriminator.
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
		return decodeCanonicalGraphResult(result, *kind, envelope)
	}
	return nil, fmt.Errorf("antfly: canonical graph result requires a discriminator")
}

// validateQueryGraphResponses binds each graph result to the request that
// selected its wire dialect and result shape. Keep this automatic check
// allocation-light: callers that consume a result use DecodeGraphResultForQuery
// for the one full payload decode. The validation representation retains graph
// structure but discards opaque hydrated JSON objects instead of materializing
// up to 10,000 documents only to discard them before returning.
func validateQueryGraphResponses(requests []QueryRequest, result *QueryResponses) error {
	hasGraphRequest := false
	for _, request := range requests {
		if request.GraphQueries != nil {
			hasGraphRequest = true
			break
		}
	}
	if hasGraphRequest && len(result.Responses) != len(requests) {
		return fmt.Errorf("antfly: query response count %d does not match request count %d", len(result.Responses), len(requests))
	}

	for responseIndex, response := range result.Responses {
		if responseIndex >= len(requests) {
			if len(response.GraphResults) != 0 {
				return fmt.Errorf("antfly: response %d contains graph_results without a matching request", responseIndex)
			}
			continue
		}
		request := requests[responseIndex]
		switch {
		case request.GraphQueries != nil:
			if err := validateGraphResultNames(request.GraphQueries, response.GraphResults); err != nil {
				return fmt.Errorf("antfly: response %d: %w", responseIndex, err)
			}
			for name, raw := range response.GraphResults {
				if err := validateCanonicalGraphResultForQueryInTable(request.GraphQueries[name], request.Table, raw); err != nil {
					return fmt.Errorf("antfly: response %d graph result %q: %w", responseIndex, name, err)
				}
			}
		default:
			if len(response.GraphResults) != 0 {
				return fmt.Errorf("antfly: response %d contains graph_results for a request without graph operations", responseIndex)
			}
		}
	}
	return nil
}

type canonicalGraphResultContract struct {
	kind             string
	names            []string
	maxItems         int
	maxDepth         int
	nodeMode         canonicalGraphNodeMode
	includePaths     bool
	includeDocuments bool
	queryTable       string
	from             GraphPathEndpoint
	to               GraphPathEndpoint
	objective        GraphPathObjective
	direction        EdgeDirection
	edgeTypes        []oapi.GraphEdgeType
	edgeWeight       *GraphEdgeWeightRange
	starts           []GraphPathEndpoint
	startsKnown      bool
}

type canonicalGraphNodeMode uint8

const (
	canonicalGraphNodeModeNone canonicalGraphNodeMode = iota
	canonicalGraphNodeModeTraversal
	canonicalGraphNodeModeShortestPath
	canonicalGraphNodeModeKShortestPaths
)

func canonicalGraphResultContractForQuery(query GraphQuery) (canonicalGraphResultContract, error) {
	return canonicalGraphResultContractForQueryInTable(query, "")
}

func canonicalGraphResultContractForQueryInTable(query GraphQuery, queryTable string) (canonicalGraphResultContract, error) {
	decoded, err := query.DecodeStrictVariant()
	if err != nil {
		return canonicalGraphResultContract{}, fmt.Errorf("antfly: inspect graph query: %w", err)
	}
	switch decoded.Kind {
	case oapi.GraphQueryVariantMatch:
		graphReturn, err := decoded.Match.Return.DecodeStrictVariant()
		if err != nil {
			return canonicalGraphResultContract{}, fmt.Errorf("antfly: inspect graph return: %w", err)
		}
		if graphReturn.Kind == oapi.GraphReturnVariantBindings {
			value := graphReturn.Bindings
			limit := value.Limit
			if limit == 0 {
				limit = defaultGraphBindingsLimit
			}
			return canonicalGraphResultContract{
				kind:             string(GraphBindingsResultKindBindings),
				names:            append([]string(nil), value.Bindings...),
				maxItems:         limit,
				includeDocuments: value.IncludeDocuments,
			}, nil
		}
		if graphReturn.Kind == oapi.GraphReturnVariantAggregates {
			value := graphReturn.Aggregates
			names := make([]string, 0, len(value.Aggregates))
			for name := range value.Aggregates {
				names = append(names, name)
			}
			sort.Strings(names)
			return canonicalGraphResultContract{kind: string(GraphAggregatesResultKindAggregates), names: names}, nil
		}
		return canonicalGraphResultContract{}, fmt.Errorf("antfly: graph match return must select bindings or aggregates")
	case oapi.GraphQueryVariantTraverse:
		value := decoded.Traverse
		limit := value.Traverse.Limit
		if limit == 0 {
			limit = defaultGraphBindingsLimit
		}
		maxDepth := value.Traverse.MaxDepth
		if maxDepth == 0 {
			maxDepth = defaultGraphTraversalDepth
		}
		starts, startsKnown, err := canonicalDirectSelectorEndpoints(value.Traverse.Start, queryTable)
		if err != nil {
			return canonicalGraphResultContract{}, fmt.Errorf("antfly: inspect graph traversal start: %w", err)
		}
		return canonicalGraphResultContract{
			kind:             string(GraphNodesResultKindNodes),
			maxItems:         limit,
			maxDepth:         maxDepth,
			nodeMode:         canonicalGraphNodeModeTraversal,
			includePaths:     value.Traverse.IncludePaths,
			includeDocuments: value.Traverse.IncludeDocuments,
			queryTable:       queryTable,
			direction:        effectiveGraphDirection(value.Traverse.Direction),
			edgeTypes:        value.Traverse.EdgeTypes,
			edgeWeight:       value.Traverse.EdgeWeight,
			starts:           starts,
			startsKnown:      startsKnown,
		}, nil
	case oapi.GraphQueryVariantShortestPath:
		value := decoded.ShortestPath
		maxDepth := value.ShortestPath.MaxDepth
		if maxDepth == 0 {
			maxDepth = defaultGraphPathDepth
		}
		return canonicalGraphResultContract{
			kind:             string(GraphPathsResultKindPaths),
			maxItems:         1,
			maxDepth:         maxDepth,
			nodeMode:         canonicalGraphNodeModeShortestPath,
			includeDocuments: value.ShortestPath.IncludeDocuments,
			queryTable:       queryTable,
			from:             value.ShortestPath.From,
			to:               value.ShortestPath.To,
			objective:        effectiveGraphObjective(value.ShortestPath.Objective),
			direction:        effectiveGraphDirection(value.ShortestPath.Direction),
			edgeTypes:        value.ShortestPath.EdgeTypes,
			edgeWeight:       value.ShortestPath.EdgeWeight,
		}, nil
	case oapi.GraphQueryVariantKShortestPaths:
		value := decoded.KShortestPaths
		maxDepth := value.KShortestPaths.MaxDepth
		if maxDepth == 0 {
			maxDepth = defaultGraphPathDepth
		}
		return canonicalGraphResultContract{
			kind:             string(GraphPathsResultKindPaths),
			maxItems:         value.KShortestPaths.K,
			maxDepth:         maxDepth,
			nodeMode:         canonicalGraphNodeModeKShortestPaths,
			includeDocuments: value.KShortestPaths.IncludeDocuments,
			queryTable:       queryTable,
			from:             value.KShortestPaths.From,
			to:               value.KShortestPaths.To,
			objective:        effectiveGraphObjective(value.KShortestPaths.Objective),
			direction:        effectiveGraphDirection(value.KShortestPaths.Direction),
			edgeTypes:        value.KShortestPaths.EdgeTypes,
			edgeWeight:       value.KShortestPaths.EdgeWeight,
		}, nil
	default:
		return canonicalGraphResultContract{}, fmt.Errorf("antfly: graph query has no supported operation")
	}
}

func effectiveGraphDirection(direction EdgeDirection) EdgeDirection {
	if direction == "" {
		return EdgeDirectionOut
	}
	return direction
}

func effectiveGraphObjective(objective GraphPathObjective) GraphPathObjective {
	if objective == "" {
		return GraphPathObjectiveMinHops
	}
	return objective
}

func canonicalDirectSelectorEndpoints(selector GraphNodeSelector, queryTable string) ([]GraphPathEndpoint, bool, error) {
	decoded, err := selector.DecodeStrictVariant()
	if err != nil {
		return nil, false, err
	}
	switch decoded.Kind {
	case oapi.GraphNodeSelectorVariantKeys:
		endpoints := make([]GraphPathEndpoint, len(decoded.Keys.Keys))
		for i, key := range decoded.Keys.Keys {
			endpoints[i] = GraphPathEndpoint{Key: key}
		}
		return endpoints, true, nil
	case oapi.GraphNodeSelectorVariantIdentities:
		endpoints := make([]GraphPathEndpoint, len(decoded.Identities.Identities))
		for i, endpoint := range decoded.Identities.Identities {
			endpoints[i] = canonicalExpectedEndpoint(endpoint, queryTable)
		}
		return endpoints, true, nil
	case oapi.GraphNodeSelectorVariantResultRef:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("unsupported graph selector")
	}
}

type graphResultEnvelopeDecoder interface {
	DecodeInto(any) error
}

func canonicalGraphResultEnvelope(result graphResultEnvelopeDecoder) (graphQueryResultEnvelope, string, error) {
	var envelope graphQueryResultEnvelope
	if err := result.DecodeInto(&envelope); err != nil {
		return graphQueryResultEnvelope{}, "", err
	}
	if envelope.Kind == nil {
		return graphQueryResultEnvelope{}, "", fmt.Errorf("antfly: canonical graph result requires a discriminator")
	}
	var kind *string
	if err := json.Unmarshal(envelope.Kind, &kind); err != nil || kind == nil {
		return graphQueryResultEnvelope{}, "", fmt.Errorf("antfly: graph result has an invalid discriminator")
	}
	if envelope.Stats == nil || envelope.Stats.ReturnedItems == nil || envelope.Stats.Truncated == nil {
		return graphQueryResultEnvelope{}, "", fmt.Errorf("antfly: canonical graph result requires complete stats")
	}
	return envelope, *kind, nil
}

func validateCanonicalGraphResultForQuery(query GraphQuery, result GraphResult) error {
	return validateCanonicalGraphResultForQueryInTable(query, "", result)
}

func validateCanonicalGraphResultForQueryInTable(query GraphQuery, queryTable string, result GraphResult) error {
	contract, err := canonicalGraphResultContractForQueryInTable(query, queryTable)
	if err != nil {
		return err
	}
	envelope, kind, err := canonicalGraphResultEnvelope(result)
	if err != nil {
		return err
	}
	if kind != contract.kind {
		return fmt.Errorf("antfly: graph operation requires result kind %q, got %q", contract.kind, kind)
	}
	return validateCanonicalGraphResultPayload(contract, result, envelope)
}

// graphOpaqueJSONObject validates the outer JSON type while deliberately not
// retaining object contents. Hydrated documents, evidence, and edge metadata
// are schema-defined opaque JSON objects; their keys are not graph protocol
// fields and must not be subjected to DisallowUnknownFields.
type graphOpaqueJSONObject struct {
	present bool
}

func (object *graphOpaqueJSONObject) UnmarshalJSON(encoded []byte) error {
	trimmed := bytes.TrimSpace(encoded)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		// UnmarshalJSON is called only for a present member. Retain presence without
		// materializing opaque document contents a second time.
		object.present = true
		return nil
	}
	return fmt.Errorf("expected an object")
}

// graphRequiredJSONValue retains required-member presence without allocating a
// pointer for every scalar. Generated value fields cannot otherwise distinguish
// a missing member from a valid zero value (for example a zero-hop path).
type graphRequiredJSONValue[T any] struct {
	value   T
	present bool
}

func (value *graphRequiredJSONValue[T]) UnmarshalJSON(encoded []byte) error {
	if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
		return fmt.Errorf("required graph result member must be non-null")
	}
	if err := json.Unmarshal(encoded, &value.value); err != nil {
		return err
	}
	value.present = true
	return nil
}

// graphOptionalNonNullString preserves the distinction between an omitted
// table qualifier and explicit null without allocating a pointer per identity.
type graphOptionalNonNullString struct {
	value   string
	present bool
}

func (value *graphOptionalNonNullString) UnmarshalJSON(encoded []byte) error {
	if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
		return fmt.Errorf("graph node table must be omitted or non-null")
	}
	if err := json.Unmarshal(encoded, &value.value); err != nil {
		return err
	}
	value.present = true
	return nil
}

func (value *graphOptionalNonNullString) pointer() *string {
	if !value.present {
		return nil
	}
	return &value.value
}

type graphBindingNodeValidation struct {
	Document graphOpaqueJSONObject      `json:"document,omitempty"`
	Key      string                     `json:"key"`
	Table    graphOptionalNonNullString `json:"table,omitempty"`
}

type graphBindingsResultValidation struct {
	Kind  GraphBindingsResultKind                  `json:"kind"`
	Rows  []map[string]*graphBindingNodeValidation `json:"rows"`
	Stats GraphResultStats                         `json:"stats"`
}

type graphAggregatesResultValidation struct {
	Aggregates map[string]GraphAggregateValue `json:"aggregates"`
	Kind       GraphAggregatesResultKind      `json:"kind"`
	Stats      GraphExactResultStats          `json:"stats"`
}

type graphPathEdgeValidation struct {
	Direction graphRequiredJSONValue[GraphPathEdgeDirection] `json:"direction"`
	From      GraphPathEndpoint                              `json:"from"`
	Metadata  graphOpaqueJSONObject                          `json:"metadata,omitempty"`
	To        GraphPathEndpoint                              `json:"to"`
	Type      string                                         `json:"type"`
	Weight    graphRequiredJSONValue[float64]                `json:"weight"`
}

type graphPathValidation struct {
	Edges          []graphPathEdgeValidation                  `json:"edges"`
	Length         graphRequiredJSONValue[int]                `json:"length"`
	Nodes          []GraphPathEndpoint                        `json:"nodes"`
	Objective      graphRequiredJSONValue[GraphPathObjective] `json:"objective"`
	ObjectiveValue graphRequiredJSONValue[float64]            `json:"objective_value"`
	WeightSum      graphRequiredJSONValue[float64]            `json:"weight_sum"`
}

type graphResultNodeValidation struct {
	Depth      graphRequiredJSONValue[int] `json:"depth"`
	Document   graphOpaqueJSONObject       `json:"document,omitempty"`
	Evidence   graphOpaqueJSONObject       `json:"evidence,omitempty"`
	Key        string                      `json:"key"`
	Path       []GraphPathEndpoint         `json:"path,omitempty"`
	PathEdges  []graphPathEdgeValidation   `json:"path_edges,omitempty"`
	Provenance []string                    `json:"provenance,omitempty"`
	Table      graphOptionalNonNullString  `json:"table,omitempty"`
}

type graphNodesResultValidation struct {
	Kind  GraphNodesResultKind        `json:"kind"`
	Nodes []graphResultNodeValidation `json:"nodes"`
	Stats GraphResultStats            `json:"stats"`
}

type graphPathResultValidation struct {
	Path     graphPathValidation   `json:"path"`
	Document graphOpaqueJSONObject `json:"document,omitempty"`
}

type graphPathsResultValidation struct {
	Kind  GraphPathsResultKind        `json:"kind"`
	Paths []graphPathResultValidation `json:"paths"`
	Stats GraphExactResultStats       `json:"stats"`
}

func validateCanonicalGraphResultPayload(
	contract canonicalGraphResultContract,
	result strictGraphResultDecoder,
	envelope graphQueryResultEnvelope,
) error {
	switch contract.kind {
	case string(GraphBindingsResultKindBindings):
		return validateBindingsResultPayload(contract, result, envelope)
	case string(GraphAggregatesResultKindAggregates):
		return validateAggregatesResultPayload(contract, result, envelope)
	case string(GraphNodesResultKindNodes):
		return validateNodesResultPayload(contract, result, envelope)
	case string(GraphPathsResultKindPaths):
		return validatePathsResultPayload(contract, result, envelope)
	default:
		return fmt.Errorf("antfly: unknown canonical graph result discriminator %q", contract.kind)
	}
}

func validateBindingsResultPayload(contract canonicalGraphResultContract, result strictGraphResultDecoder, envelope graphQueryResultEnvelope) error {
	var value graphBindingsResultValidation
	if err := result.DecodeStrictInto(&value); err != nil {
		return fmt.Errorf("antfly: invalid bindings graph result: %w", err)
	}
	if value.Kind != GraphBindingsResultKindBindings || value.Rows == nil {
		return fmt.Errorf("antfly: bindings graph result requires kind and rows")
	}
	if len(value.Rows) > contract.maxItems {
		return fmt.Errorf("antfly: bindings graph result exceeds the requested limit of %d rows", contract.maxItems)
	}
	expected := make(map[string]struct{}, len(contract.names))
	for _, name := range contract.names {
		expected[name] = struct{}{}
	}
	for rowIndex, row := range value.Rows {
		if len(row) == 0 || len(row) > maxGraphMatchNodes {
			return fmt.Errorf("antfly: bindings graph result row %d must contain between 1 and %d properties", rowIndex, maxGraphMatchNodes)
		}
		if len(row) != len(expected) {
			return fmt.Errorf("antfly: bindings graph result row %d does not match the requested projection", rowIndex)
		}
		for alias, binding := range row {
			if !validGraphIdentifier(alias) {
				return fmt.Errorf("antfly: bindings graph result row %d has an invalid alias", rowIndex)
			}
			if _, ok := expected[alias]; !ok {
				return fmt.Errorf("antfly: bindings graph result row %d contains unrequested alias %q", rowIndex, alias)
			}
			if binding != nil {
				if err := validateDecodedGraphIdentity(binding.Key, binding.Table.pointer()); err != nil {
					return fmt.Errorf("antfly: bindings graph result row %d alias %q: %w", rowIndex, alias, err)
				}
				if !contract.includeDocuments && binding.Document.present {
					return fmt.Errorf("antfly: bindings graph result row %d alias %q contains a document that was not requested", rowIndex, alias)
				}
			}
		}
	}
	return validateDecodedGraphStats(envelope, len(value.Rows), true)
}

func validateAggregatesResultPayload(contract canonicalGraphResultContract, result strictGraphResultDecoder, envelope graphQueryResultEnvelope) error {
	var value graphAggregatesResultValidation
	if err := result.DecodeStrictInto(&value); err != nil {
		return fmt.Errorf("antfly: invalid aggregates graph result: %w", err)
	}
	if value.Kind != GraphAggregatesResultKindAggregates || len(value.Aggregates) == 0 || len(value.Aggregates) > maxGraphCountAggregates {
		return fmt.Errorf("antfly: aggregates graph result requires between 1 and %d aggregates", maxGraphCountAggregates)
	}
	actual := make([]string, 0, len(value.Aggregates))
	for name, aggregate := range value.Aggregates {
		if !validGraphIdentifier(name) {
			return fmt.Errorf("antfly: aggregates graph result has an invalid name")
		}
		if !bool(aggregate.Exact) || !isUnsignedDecimal(aggregate.Value) {
			return fmt.Errorf("antfly: aggregate %q must contain an exact decimal value", name)
		}
		actual = append(actual, name)
	}
	sort.Strings(actual)
	if !slices.Equal(actual, contract.names) {
		return fmt.Errorf("antfly: aggregate graph result names %v do not match requested names %v", actual, contract.names)
	}
	return validateDecodedGraphStats(envelope, len(value.Aggregates), false)
}

func validateNodesResultPayload(contract canonicalGraphResultContract, result strictGraphResultDecoder, envelope graphQueryResultEnvelope) error {
	var value graphNodesResultValidation
	if err := result.DecodeStrictInto(&value); err != nil {
		return fmt.Errorf("antfly: invalid nodes graph result: %w", err)
	}
	if value.Kind != GraphNodesResultKindNodes || value.Nodes == nil {
		return fmt.Errorf("antfly: nodes graph result requires kind and nodes")
	}
	if contract.nodeMode != canonicalGraphNodeModeTraversal {
		return fmt.Errorf("antfly: nodes graph result requires a traversal operation")
	}
	if len(value.Nodes) > contract.maxItems {
		return fmt.Errorf("antfly: nodes graph result exceeds the requested limit of %d items", contract.maxItems)
	}
	for i := range value.Nodes {
		if err := validateGraphResultNodePayload(&value.Nodes[i]); err != nil {
			return fmt.Errorf("antfly: nodes graph result node %d: %w", i, err)
		}
		if !contract.includeDocuments && value.Nodes[i].Document.present {
			return fmt.Errorf("antfly: nodes graph result node %d contains a document that was not requested", i)
		}
		if err := validateTraversalNodeForContract(
			contract,
			GraphPathEndpoint{Key: value.Nodes[i].Key, Table: value.Nodes[i].Table.pointer()},
			value.Nodes[i].Depth.value,
			value.Nodes[i].Path,
			value.Nodes[i].PathEdges,
		); err != nil {
			return fmt.Errorf("antfly: nodes graph result node %d: %w", i, err)
		}
	}
	for i := range value.Nodes {
		if contract.includePaths {
			if value.Nodes[i].Path == nil {
				return fmt.Errorf("antfly: traversal graph result node %d is missing its requested path", i)
			}
		} else if value.Nodes[i].Path != nil || value.Nodes[i].PathEdges != nil {
			return fmt.Errorf("antfly: traversal graph result node %d contains a path that was not requested", i)
		}
	}
	return validateDecodedGraphStats(envelope, len(value.Nodes), true)
}

func validatePathsResultPayload(contract canonicalGraphResultContract, result strictGraphResultDecoder, envelope graphQueryResultEnvelope) error {
	var value graphPathsResultValidation
	if err := result.DecodeStrictInto(&value); err != nil {
		return fmt.Errorf("antfly: invalid paths graph result: %w", err)
	}
	if value.Kind != GraphPathsResultKindPaths || value.Paths == nil {
		return fmt.Errorf("antfly: paths graph result requires kind and paths")
	}
	if contract.nodeMode != canonicalGraphNodeModeShortestPath && contract.nodeMode != canonicalGraphNodeModeKShortestPaths {
		return fmt.Errorf("antfly: paths graph result requires a path operation")
	}
	if len(value.Paths) > contract.maxItems {
		return fmt.Errorf("antfly: paths graph result exceeds the requested limit of %d items", contract.maxItems)
	}
	for i := range value.Paths {
		if err := validateGraphPathPayload(&value.Paths[i].Path); err != nil {
			return fmt.Errorf("antfly: paths graph result path %d: %w", i, err)
		}
		if !contract.includeDocuments && value.Paths[i].Document.present {
			return fmt.Errorf("antfly: paths graph result path %d contains a document that was not requested", i)
		}
		if err := validateGraphPathForContract(contract, &value.Paths[i].Path); err != nil {
			return fmt.Errorf("antfly: paths graph result path %d: %w", i, err)
		}
	}
	if err := validateGraphPathCollectionForContract(contract, value.Paths); err != nil {
		return fmt.Errorf("antfly: paths graph result: %w", err)
	}
	return validateDecodedGraphStats(envelope, len(value.Paths), false)
}

func validateTraversalNodeForContract(
	contract canonicalGraphResultContract,
	node GraphPathEndpoint,
	depth int,
	path []GraphPathEndpoint,
	edges []graphPathEdgeValidation,
) error {
	if depth > contract.maxDepth {
		return fmt.Errorf("graph node depth %d exceeds the requested max_depth of %d", depth, contract.maxDepth)
	}
	if path != nil {
		if contract.startsKnown && !endpointInContractSet(path[0], contract.starts) {
			return fmt.Errorf("graph node path does not start at a requested traversal identity")
		}
		for i := range edges {
			if err := validateGraphEdgeForContract(contract, edges[i].Direction.value, edges[i].Type, edges[i].Weight.value); err != nil {
				return err
			}
		}
	} else if depth == 0 && contract.startsKnown && !endpointInContractSet(node, contract.starts) {
		return fmt.Errorf("depth-zero graph node is not a requested traversal identity")
	}
	return nil
}

func validateGraphPathForContract(contract canonicalGraphResultContract, path *graphPathValidation) error {
	if path.Length.value > contract.maxDepth {
		return fmt.Errorf("length %d exceeds the requested max_depth of %d", path.Length.value, contract.maxDepth)
	}
	if path.Objective.value != contract.objective {
		return fmt.Errorf("objective %q does not match the requested objective %q", path.Objective.value, contract.objective)
	}
	if !endpointMatchesContract(path.Nodes[0], contract.from, contract.queryTable) {
		return fmt.Errorf("start endpoint does not match the requested from identity")
	}
	if !endpointMatchesContract(path.Nodes[len(path.Nodes)-1], contract.to, contract.queryTable) {
		return fmt.Errorf("terminal endpoint does not match the requested to identity")
	}
	for i := range path.Edges {
		if err := validateGraphEdgeForContract(contract, path.Edges[i].Direction.value, path.Edges[i].Type, path.Edges[i].Weight.value); err != nil {
			return err
		}
	}
	if contract.nodeMode == canonicalGraphNodeModeKShortestPaths && !graphValidationPathIsLoopless(path.Nodes) {
		return fmt.Errorf("k_shortest_paths result must be loopless")
	}
	return nil
}

func validateGraphEdgeForContract(contract canonicalGraphResultContract, direction GraphPathEdgeDirection, edgeType string, weight float64) error {
	if contract.direction != EdgeDirectionBoth && string(direction) != string(contract.direction) {
		return fmt.Errorf("edge direction %q does not match the requested direction %q", direction, contract.direction)
	}
	if len(contract.edgeTypes) > 0 {
		matched := false
		for _, allowed := range contract.edgeTypes {
			if edgeType == string(allowed) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("edge type %q was not requested", edgeType)
		}
	}
	if contract.edgeWeight != nil {
		if contract.edgeWeight.Min != nil && weight < *contract.edgeWeight.Min {
			return fmt.Errorf("edge weight is below the requested minimum")
		}
		if contract.edgeWeight.Max != nil && weight > *contract.edgeWeight.Max {
			return fmt.Errorf("edge weight is above the requested maximum")
		}
	}
	return nil
}

func validateGraphPathCollectionForContract(contract canonicalGraphResultContract, paths []graphPathResultValidation) error {
	if contract.nodeMode != canonicalGraphNodeModeKShortestPaths {
		return nil
	}
	for i := range paths {
		for prior := 0; prior < i; prior++ {
			if sameValidationGraphPath(&paths[prior].Path, &paths[i].Path) {
				return fmt.Errorf("contains duplicate paths at indexes %d and %d", prior, i)
			}
		}
		if i > 0 && graphObjectiveOutOfOrder(
			contract.objective,
			paths[i-1].Path.ObjectiveValue.value,
			paths[i].Path.ObjectiveValue.value,
		) {
			return fmt.Errorf("paths are not ordered by requested objective %q", contract.objective)
		}
	}
	return nil
}

func validateGraphResultNodePayload(node *graphResultNodeValidation) error {
	if err := validateDecodedGraphIdentity(node.Key, node.Table.pointer()); err != nil {
		return err
	}
	if !node.Depth.present {
		return fmt.Errorf("graph node requires depth")
	}
	if node.Depth.value < 0 || node.Depth.value > maxGraphMatchEdges {
		return fmt.Errorf("graph node depth must be between 0 and %d", maxGraphMatchEdges)
	}
	if node.Path != nil {
		if len(node.Path) == 0 || len(node.Path) > maxGraphMatchEdges+1 {
			return fmt.Errorf("graph node path must contain between 1 and %d nodes", maxGraphMatchEdges+1)
		}
		if node.Depth.value != len(node.Path)-1 {
			return fmt.Errorf("graph node depth must equal path length minus one")
		}
		for _, endpoint := range node.Path {
			if err := validateDecodedGraphIdentity(endpoint.Key, endpoint.Table); err != nil {
				return err
			}
		}
		if !sameDecodedGraphEndpoint(node.Path[len(node.Path)-1], GraphPathEndpoint{Key: node.Key, Table: node.Table.pointer()}) {
			return fmt.Errorf("graph node path must terminate at the result node")
		}
	}
	if node.PathEdges != nil {
		if node.Path == nil || len(node.PathEdges)+1 != len(node.Path) {
			return fmt.Errorf("graph node path_edges must align with path")
		}
		for i := range node.PathEdges {
			if err := validateGraphPathEdgePayload(&node.PathEdges[i], node.Path[i], node.Path[i+1], false); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateGraphPathPayload(path *graphPathValidation) error {
	if path.Nodes == nil || path.Edges == nil || len(path.Nodes) == 0 || len(path.Nodes) > maxGraphMatchEdges+1 {
		return fmt.Errorf("graph path requires bounded nodes and edges")
	}
	if !path.Length.present || !path.Objective.present || !path.WeightSum.present || !path.ObjectiveValue.present {
		return fmt.Errorf("graph path requires length, objective, weight_sum, and objective_value")
	}
	if path.Length.value != len(path.Edges) || len(path.Nodes) != len(path.Edges)+1 {
		return fmt.Errorf("graph path length, nodes, and edges do not align")
	}
	for _, endpoint := range path.Nodes {
		if err := validateDecodedGraphIdentity(endpoint.Key, endpoint.Table); err != nil {
			return err
		}
	}
	var sum float64
	product := 1.0
	maxWeightProduct := path.Objective.value == GraphPathObjectiveMaxWeightProduct
	for i := range path.Edges {
		if err := validateGraphPathEdgePayload(&path.Edges[i], path.Nodes[i], path.Nodes[i+1], maxWeightProduct); err != nil {
			return err
		}
		sum += path.Edges[i].Weight.value
		if !finiteNonNegative(sum) {
			return fmt.Errorf("graph path score overflow")
		}
		if maxWeightProduct {
			product *= path.Edges[i].Weight.value
			if !finiteNonNegative(product) {
				return fmt.Errorf("graph path score overflow")
			}
		}
	}
	return validateGraphPathScore(path.Objective.value, len(path.Edges), path.WeightSum.value, path.ObjectiveValue.value, sum, product)
}

func validateGraphPathEdgePayload(edge *graphPathEdgeValidation, from, to GraphPathEndpoint, maxWeightProduct bool) error {
	if !sameDecodedGraphEndpoint(edge.From, from) || !sameDecodedGraphEndpoint(edge.To, to) {
		return fmt.Errorf("graph path edge does not match adjacent nodes")
	}
	if !edge.Direction.present || !edge.Weight.present {
		return fmt.Errorf("graph path edge requires direction and weight")
	}
	return validateGraphPathEdgeFields(edge.Direction.value, edge.Type, edge.Weight.value, maxWeightProduct)
}

func validateGraphResultNames[T any](requested map[string]T, received map[string]GraphResult) error {
	if len(requested) == len(received) {
		matches := true
		for name := range requested {
			if _, ok := received[name]; !ok {
				matches = false
				break
			}
		}
		if matches {
			return nil
		}
	}

	missing := make([]string, 0)
	unexpected := make([]string, 0)
	for name := range requested {
		if _, ok := received[name]; !ok {
			missing = append(missing, name)
		}
	}
	for name := range received {
		if _, ok := requested[name]; !ok {
			unexpected = append(unexpected, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return fmt.Errorf("graph_results operation names do not match the request (missing=%v unexpected=%v)", missing, unexpected)
}

// DecodeCanonicalGraphResult decodes the GraphResult stored in
// QueryResult.graph_results and enforces its canonical discriminator and
// structural invariants.
func DecodeCanonicalGraphResult(result GraphResult) (any, error) {
	envelope, kind, err := canonicalGraphResultEnvelope(result)
	if err != nil {
		return nil, err
	}
	return decodeCanonicalGraphResult(result, kind, envelope)
}

// DecodeGraphResultForQuery performs the full canonical payload decode and
// verifies that its variant and projected names match the operation that
// requested it. This is the preferred decoder for values returned by a known
// graph_queries entry.
func DecodeGraphResultForQuery(query GraphQuery, result GraphResult) (any, error) {
	return DecodeGraphResultForQueryInTable("", query, result)
}

// DecodeGraphResultForQueryInTable additionally knows the queried table, so it
// can enforce canonical omission of that table qualifier and distinguish it
// from a cross-table endpoint. Prefer this form for table-scoped transports.
func DecodeGraphResultForQueryInTable(queryTable string, query GraphQuery, result GraphResult) (any, error) {
	contract, err := canonicalGraphResultContractForQueryInTable(query, queryTable)
	if err != nil {
		return nil, err
	}
	decoded, err := DecodeCanonicalGraphResult(result)
	if err != nil {
		return nil, err
	}
	if err := validateDecodedGraphResultContract(contract, decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func validateDecodedGraphResultContract(contract canonicalGraphResultContract, decoded any) error {
	switch value := decoded.(type) {
	case GraphBindingsResult:
		if contract.kind != string(GraphBindingsResultKindBindings) {
			return fmt.Errorf("antfly: graph operation requires result kind %q, got %q", contract.kind, GraphBindingsResultKindBindings)
		}
		if len(value.Rows) > contract.maxItems {
			return fmt.Errorf("antfly: bindings graph result exceeds the requested limit of %d rows", contract.maxItems)
		}
		expected := make(map[string]struct{}, len(contract.names))
		for _, name := range contract.names {
			expected[name] = struct{}{}
		}
		for rowIndex, row := range value.Rows {
			if len(row) != len(expected) {
				return fmt.Errorf("antfly: bindings graph result row %d does not match the requested projection", rowIndex)
			}
			for name := range row {
				if _, ok := expected[name]; !ok {
					return fmt.Errorf("antfly: bindings graph result row %d contains unrequested alias %q", rowIndex, name)
				}
			}
			if !contract.includeDocuments {
				for name, binding := range row {
					if binding != nil && binding.Document != nil {
						return fmt.Errorf("antfly: bindings graph result row %d alias %q contains a document that was not requested", rowIndex, name)
					}
				}
			}
		}
		return nil
	case GraphAggregatesResult:
		if contract.kind != string(GraphAggregatesResultKindAggregates) {
			return fmt.Errorf("antfly: graph operation requires result kind %q, got %q", contract.kind, GraphAggregatesResultKindAggregates)
		}
		actual := make([]string, 0, len(value.Aggregates))
		for name := range value.Aggregates {
			actual = append(actual, name)
		}
		sort.Strings(actual)
		if !slices.Equal(actual, contract.names) {
			return fmt.Errorf("antfly: aggregate graph result names %v do not match requested names %v", actual, contract.names)
		}
		return nil
	case GraphNodesResult:
		if contract.kind != string(GraphNodesResultKindNodes) {
			return fmt.Errorf("antfly: graph operation requires result kind %q, got %q", contract.kind, GraphNodesResultKindNodes)
		}
		if contract.nodeMode != canonicalGraphNodeModeTraversal || len(value.Nodes) > contract.maxItems {
			return fmt.Errorf("antfly: nodes graph result exceeds the requested limit of %d items", contract.maxItems)
		}
		if !contract.includeDocuments {
			for i := range value.Nodes {
				if value.Nodes[i].Document != nil {
					return fmt.Errorf("antfly: nodes graph result node %d contains a document that was not requested", i)
				}
			}
		}
		for i := range value.Nodes {
			if contract.includePaths {
				if value.Nodes[i].Path == nil {
					return fmt.Errorf("antfly: traversal graph result node %d is missing its requested path", i)
				}
			} else {
				if value.Nodes[i].Path != nil || value.Nodes[i].PathEdges != nil {
					return fmt.Errorf("antfly: traversal graph result node %d contains a path that was not requested", i)
				}
			}
			if err := validateDecodedTraversalNodeForContract(contract, value.Nodes[i]); err != nil {
				return fmt.Errorf("antfly: nodes graph result node %d: %w", i, err)
			}
		}
		return nil
	case GraphPathsResult:
		if contract.kind != string(GraphPathsResultKindPaths) || (contract.nodeMode != canonicalGraphNodeModeShortestPath && contract.nodeMode != canonicalGraphNodeModeKShortestPaths) {
			return fmt.Errorf("antfly: graph operation requires result kind %q, got %q", contract.kind, GraphPathsResultKindPaths)
		}
		if len(value.Paths) > contract.maxItems {
			return fmt.Errorf("antfly: paths graph result exceeds the requested limit of %d items", contract.maxItems)
		}
		if !contract.includeDocuments {
			for i := range value.Paths {
				if value.Paths[i].Document != nil {
					return fmt.Errorf("antfly: paths graph result path %d contains a document that was not requested", i)
				}
			}
		}
		for i := range value.Paths {
			if err := validateDecodedGraphPathForContract(contract, value.Paths[i].Path); err != nil {
				return fmt.Errorf("antfly: paths graph result path %d: %w", i, err)
			}
		}
		if err := validateDecodedGraphPathCollectionForContract(contract, value.Paths); err != nil {
			return fmt.Errorf("antfly: paths graph result: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("antfly: unsupported decoded graph result type %T", decoded)
	}
}

type graphQueryResultEnvelope struct {
	Kind  json.RawMessage               `json:"kind"`
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
		// Generated scalar value fields erase required-member presence. Probe the
		// strict wire shape first; opaque documents are type-checked but not copied.
		var wire graphNodesResultValidation
		if err := result.DecodeStrictInto(&wire); err != nil {
			return nil, fmt.Errorf("antfly: invalid nodes graph result: %w", err)
		}
		if wire.Kind != GraphNodesResultKindNodes || wire.Nodes == nil {
			return nil, fmt.Errorf("antfly: nodes graph result requires kind and nodes")
		}
		if len(wire.Nodes) > maxGraphHydratedBindings {
			return nil, fmt.Errorf("antfly: nodes graph result exceeds %d items", maxGraphHydratedBindings)
		}
		for i := range wire.Nodes {
			if err := validateGraphResultNodePayload(&wire.Nodes[i]); err != nil {
				return nil, fmt.Errorf("antfly: nodes graph result node %d: %w", i, err)
			}
		}
		var value GraphNodesResult
		if err := result.DecodeStrictInto(&value); err != nil {
			return nil, fmt.Errorf("antfly: invalid nodes graph result: %w", err)
		}
		if value.Kind != GraphNodesResultKindNodes || value.Nodes == nil {
			return nil, fmt.Errorf("antfly: nodes graph result requires kind and nodes")
		}
		if len(value.Nodes) > maxGraphHydratedBindings {
			return nil, fmt.Errorf("antfly: nodes graph result exceeds %d items", maxGraphHydratedBindings)
		}
		if err := validateDecodedGraphStats(envelope, len(value.Nodes), true); err != nil {
			return nil, err
		}
		return value, nil
	case string(GraphPathsResultKindPaths):
		var wire graphPathsResultValidation
		if err := result.DecodeStrictInto(&wire); err != nil {
			return nil, fmt.Errorf("antfly: invalid paths graph result: %w", err)
		}
		if wire.Kind != GraphPathsResultKindPaths || wire.Paths == nil {
			return nil, fmt.Errorf("antfly: paths graph result requires kind and paths")
		}
		if len(wire.Paths) > maxGraphPathResults {
			return nil, fmt.Errorf("antfly: paths graph result exceeds %d items", maxGraphPathResults)
		}
		for i := range wire.Paths {
			if err := validateGraphPathPayload(&wire.Paths[i].Path); err != nil {
				return nil, fmt.Errorf("antfly: paths graph result path %d: %w", i, err)
			}
		}
		var value GraphPathsResult
		if err := result.DecodeStrictInto(&value); err != nil {
			return nil, fmt.Errorf("antfly: invalid paths graph result: %w", err)
		}
		if value.Kind != GraphPathsResultKindPaths || value.Paths == nil {
			return nil, fmt.Errorf("antfly: paths graph result requires kind and paths")
		}
		if len(value.Paths) > maxGraphPathResults {
			return nil, fmt.Errorf("antfly: paths graph result exceeds %d items", maxGraphPathResults)
		}
		if err := validateDecodedGraphStats(envelope, len(value.Paths), false); err != nil {
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
		return fmt.Errorf("antfly: exact graph results cannot be truncated")
	}
	return nil
}

func validateDecodedGraphIdentity(key string, table *string) error {
	if key == "" {
		return fmt.Errorf("graph node key must not be empty")
	}
	if table != nil && !validGraphTableQualifier(*table) {
		return fmt.Errorf("graph node table must be omitted or nonblank")
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
	maxWeightProduct := path.Objective == GraphPathObjectiveMaxWeightProduct
	for i, edge := range path.Edges {
		if err := validateDecodedGraphPathEdge(edge, path.Nodes[i], path.Nodes[i+1], maxWeightProduct); err != nil {
			return err
		}
		sum += edge.Weight
		if !finiteNonNegative(sum) {
			return fmt.Errorf("graph path score overflow")
		}
		if maxWeightProduct {
			product *= edge.Weight
			if !finiteNonNegative(product) {
				return fmt.Errorf("graph path score overflow")
			}
		}
	}
	return validateGraphPathScore(path.Objective, len(path.Edges), path.WeightSum, path.ObjectiveValue, sum, product)
}

func validateDecodedGraphPathEdge(edge GraphPathEdge, from, to GraphPathEndpoint, maxWeightProduct bool) error {
	if !sameDecodedGraphEndpoint(edge.From, from) || !sameDecodedGraphEndpoint(edge.To, to) {
		return fmt.Errorf("graph path edge does not match adjacent nodes")
	}
	return validateGraphPathEdgeFields(edge.Direction, string(edge.Type), edge.Weight, maxWeightProduct)
}

func validateGraphPathEdgeFields(direction GraphPathEdgeDirection, edgeType string, weight float64, maxWeightProduct bool) error {
	if !direction.Valid() {
		return fmt.Errorf("graph path edge direction must be out or in")
	}
	if edgeType == "" || len(edgeType) > maxGraphEdgeTypeBytes || !utf8.ValidString(edgeType) {
		return fmt.Errorf("graph path edge type must encode to between 1 and %d UTF-8 bytes", maxGraphEdgeTypeBytes)
	}
	if !finiteNonNegative(weight) || maxWeightProduct && weight > 1 {
		return fmt.Errorf("graph path edge has an invalid weight")
	}
	return nil
}

func validateGraphPathScore(objectiveMode GraphPathObjective, edgeCount int, weightSum, objectiveValue, sum, product float64) error {
	if !finiteNonNegative(weightSum) || !finiteNonNegative(objectiveValue) || !graphFloatEqual(weightSum, sum) {
		return fmt.Errorf("graph path has inconsistent weight_sum")
	}
	var objective float64
	switch objectiveMode {
	case GraphPathObjectiveMinHops:
		objective = float64(edgeCount)
	case GraphPathObjectiveMinWeightSum:
		objective = sum
	case GraphPathObjectiveMaxWeightProduct:
		objective = product
	default:
		return fmt.Errorf("graph path has an unknown objective")
	}
	if !graphFloatEqual(objectiveValue, objective) {
		return fmt.Errorf("graph path has an inconsistent objective_value")
	}
	return nil
}

func sameDecodedGraphEndpoint(left, right GraphPathEndpoint) bool {
	if left.Key != right.Key || (left.Table == nil) != (right.Table == nil) {
		return false
	}
	return left.Table == nil || *left.Table == *right.Table
}

func canonicalExpectedEndpoint(endpoint GraphPathEndpoint, queryTable string) GraphPathEndpoint {
	if endpoint.Table != nil && queryTable != "" && *endpoint.Table == queryTable {
		return GraphPathEndpoint{Key: endpoint.Key}
	}
	return endpoint
}

func endpointMatchesContract(actual, requested GraphPathEndpoint, queryTable string) bool {
	expected := canonicalExpectedEndpoint(requested, queryTable)
	if sameDecodedGraphEndpoint(actual, expected) {
		return true
	}
	// The standalone decoder does not know the table selected by its transport.
	// In that form an omitted result qualifier may represent an explicitly named
	// query table. Automatic client validation and DecodeGraphResultForQueryInTable
	// always carry the table and take the strict branch above.
	return queryTable == "" && requested.Table != nil && actual.Table == nil && actual.Key == requested.Key
}

func endpointInContractSet(endpoint GraphPathEndpoint, expected []GraphPathEndpoint) bool {
	for _, candidate := range expected {
		if sameDecodedGraphEndpoint(endpoint, candidate) {
			return true
		}
	}
	return false
}

func graphValidationPathIsLoopless(nodes []GraphPathEndpoint) bool {
	for i := range nodes {
		for prior := 0; prior < i; prior++ {
			if sameDecodedGraphEndpoint(nodes[prior], nodes[i]) {
				return false
			}
		}
	}
	return true
}

func sameValidationGraphPath(left, right *graphPathValidation) bool {
	if len(left.Nodes) != len(right.Nodes) || len(left.Edges) != len(right.Edges) {
		return false
	}
	for i := range left.Nodes {
		if !sameDecodedGraphEndpoint(left.Nodes[i], right.Nodes[i]) {
			return false
		}
	}
	for i := range left.Edges {
		if left.Edges[i].Direction.value != right.Edges[i].Direction.value ||
			left.Edges[i].Type != right.Edges[i].Type {
			return false
		}
	}
	return true
}

func graphObjectiveOutOfOrder(objective GraphPathObjective, previous, current float64) bool {
	if graphFloatEqual(previous, current) {
		return false
	}
	if objective == GraphPathObjectiveMaxWeightProduct {
		return current > previous
	}
	return current < previous
}

func validateDecodedTraversalNodeForContract(contract canonicalGraphResultContract, node GraphResultNode) error {
	if node.Depth > contract.maxDepth {
		return fmt.Errorf("graph node depth %d exceeds the requested max_depth of %d", node.Depth, contract.maxDepth)
	}
	if node.Path != nil {
		if contract.startsKnown && !endpointInContractSet(node.Path[0], contract.starts) {
			return fmt.Errorf("graph node path does not start at a requested traversal identity")
		}
		for _, edge := range node.PathEdges {
			if err := validateGraphEdgeForContract(contract, edge.Direction, string(edge.Type), edge.Weight); err != nil {
				return err
			}
		}
	} else if node.Depth == 0 && contract.startsKnown && !endpointInContractSet(
		GraphPathEndpoint{Key: node.Key, Table: node.Table},
		contract.starts,
	) {
		return fmt.Errorf("depth-zero graph node is not a requested traversal identity")
	}
	return nil
}

func validateDecodedGraphPathForContract(contract canonicalGraphResultContract, path GraphPath) error {
	if path.Length > contract.maxDepth {
		return fmt.Errorf("length %d exceeds the requested max_depth of %d", path.Length, contract.maxDepth)
	}
	if path.Objective != contract.objective {
		return fmt.Errorf("objective %q does not match the requested objective %q", path.Objective, contract.objective)
	}
	if !endpointMatchesContract(path.Nodes[0], contract.from, contract.queryTable) {
		return fmt.Errorf("start endpoint does not match the requested from identity")
	}
	if !endpointMatchesContract(path.Nodes[len(path.Nodes)-1], contract.to, contract.queryTable) {
		return fmt.Errorf("terminal endpoint does not match the requested to identity")
	}
	for _, edge := range path.Edges {
		if err := validateGraphEdgeForContract(contract, edge.Direction, string(edge.Type), edge.Weight); err != nil {
			return err
		}
	}
	if contract.nodeMode == canonicalGraphNodeModeKShortestPaths && !graphValidationPathIsLoopless(path.Nodes) {
		return fmt.Errorf("k_shortest_paths result must be loopless")
	}
	return nil
}

func sameDecodedGraphPath(left, right GraphPath) bool {
	if len(left.Nodes) != len(right.Nodes) || len(left.Edges) != len(right.Edges) {
		return false
	}
	for i := range left.Nodes {
		if !sameDecodedGraphEndpoint(left.Nodes[i], right.Nodes[i]) {
			return false
		}
	}
	for i := range left.Edges {
		if left.Edges[i].Direction != right.Edges[i].Direction || left.Edges[i].Type != right.Edges[i].Type {
			return false
		}
	}
	return true
}

func validateDecodedGraphPathCollectionForContract(contract canonicalGraphResultContract, paths []GraphPathResult) error {
	if contract.nodeMode != canonicalGraphNodeModeKShortestPaths {
		return nil
	}
	for i := range paths {
		for prior := 0; prior < i; prior++ {
			if sameDecodedGraphPath(paths[prior].Path, paths[i].Path) {
				return fmt.Errorf("contains duplicate paths at indexes %d and %d", prior, i)
			}
		}
		if i > 0 && graphObjectiveOutOfOrder(contract.objective, paths[i-1].Path.ObjectiveValue, paths[i].Path.ObjectiveValue) {
			return fmt.Errorf("paths are not ordered by requested objective %q", contract.objective)
		}
	}
	return nil
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

type graphQueryMemberProbe struct {
	present bool
	null    bool
	kind    byte
}

func (p *graphQueryMemberProbe) UnmarshalJSON(encoded []byte) error {
	p.present = true
	trimmed := bytes.TrimSpace(encoded)
	p.null = bytes.Equal(trimmed, []byte("null"))
	if len(trimmed) != 0 {
		p.kind = trimmed[0]
	}
	return nil
}

type graphQueryProbe struct {
	Term           graphQueryMemberProbe `json:"term"`
	Fuzziness      graphQueryMemberProbe `json:"fuzziness"`
	Prefix         graphQueryMemberProbe `json:"prefix"`
	Regexp         graphQueryMemberProbe `json:"regexp"`
	Wildcard       graphQueryMemberProbe `json:"wildcard"`
	Min            graphQueryMemberProbe `json:"min"`
	Max            graphQueryMemberProbe `json:"max"`
	Start          graphQueryMemberProbe `json:"start"`
	End            graphQueryMemberProbe `json:"end"`
	IDs            graphQueryMemberProbe `json:"ids"`
	Bool           graphQueryMemberProbe `json:"bool"`
	MatchAll       graphQueryMemberProbe `json:"match_all"`
	MatchNone      graphQueryMemberProbe `json:"match_none"`
	Conjuncts      graphQueryMemberProbe `json:"conjuncts"`
	Disjuncts      graphQueryMemberProbe `json:"disjuncts"`
	Must           graphQueryMemberProbe `json:"must"`
	Should         graphQueryMemberProbe `json:"should"`
	MustNot        graphQueryMemberProbe `json:"must_not"`
	Filter         graphQueryMemberProbe `json:"filter"`
	Field          graphQueryMemberProbe `json:"field"`
	PrefixLength   graphQueryMemberProbe `json:"prefix_length"`
	InclusiveMin   graphQueryMemberProbe `json:"inclusive_min"`
	InclusiveMax   graphQueryMemberProbe `json:"inclusive_max"`
	InclusiveStart graphQueryMemberProbe `json:"inclusive_start"`
	InclusiveEnd   graphQueryMemberProbe `json:"inclusive_end"`
	Boost          graphQueryMemberProbe `json:"boost"`
	DatetimeParser graphQueryMemberProbe `json:"datetime_parser"`
}

func inspectGraphQuery(filter querydsl.Query) (graphQueryProbe, error) {
	var probe graphQueryProbe
	if err := filter.DecodeInto(&probe); err != nil {
		return graphQueryProbe{}, err
	}
	for _, candidate := range [...]struct {
		name   string
		member graphQueryMemberProbe
	}{
		{name: "term", member: probe.Term}, {name: "fuzziness", member: probe.Fuzziness},
		{name: "prefix", member: probe.Prefix}, {name: "regexp", member: probe.Regexp},
		{name: "wildcard", member: probe.Wildcard}, {name: "min", member: probe.Min},
		{name: "max", member: probe.Max}, {name: "start", member: probe.Start},
		{name: "end", member: probe.End}, {name: "ids", member: probe.IDs},
		{name: "bool", member: probe.Bool}, {name: "match_all", member: probe.MatchAll},
		{name: "match_none", member: probe.MatchNone}, {name: "conjuncts", member: probe.Conjuncts},
		{name: "disjuncts", member: probe.Disjuncts}, {name: "must", member: probe.Must},
		{name: "should", member: probe.Should}, {name: "must_not", member: probe.MustNot},
		{name: "filter", member: probe.Filter}, {name: "field", member: probe.Field},
		{name: "prefix_length", member: probe.PrefixLength},
		{name: "inclusive_min", member: probe.InclusiveMin},
		{name: "inclusive_max", member: probe.InclusiveMax},
		{name: "inclusive_start", member: probe.InclusiveStart},
		{name: "inclusive_end", member: probe.InclusiveEnd},
	} {
		name, member := candidate.name, candidate.member
		if member.present && member.null {
			return graphQueryProbe{}, fmt.Errorf("antfly: graph document filter member %q must not be null", name)
		}
	}
	if probe.Boost.present {
		return graphQueryProbe{}, fmt.Errorf("antfly: graph document filters do not support query option %q", "boost")
	}
	if probe.DatetimeParser.present {
		return graphQueryProbe{}, fmt.Errorf("antfly: graph document filters do not support query option %q", "datetime_parser")
	}
	return probe, nil
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
	probe, err := inspectGraphQuery(filter)
	if err != nil {
		return GraphDocumentFilter{}, err
	}
	if probe == (graphQueryProbe{}) {
		return GraphDocumentFilter{}, fmt.Errorf("antfly: graph document filter must be a query object")
	}

	switch {
	case probe.Term.present:
		if probe.Fuzziness.present {
			var value querydsl.FuzzyQuery
			if err := filter.DecodeStrictInto(&value); err != nil {
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
		var value querydsl.TermQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentTermFilter(oapi.GraphDocumentTermFilter{Term: value.Term, Path: path})
		return out, err
	case probe.Prefix.present:
		var value querydsl.PrefixQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentPrefixFilter(oapi.GraphDocumentPrefixFilter{Prefix: value.Prefix, Path: path})
		return out, err
	case probe.Regexp.present:
		var value querydsl.RegexpQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentRegexpFilter(oapi.GraphDocumentRegexpFilter{Regexp: value.Regexp, Path: path})
		return out, err
	case probe.Wildcard.present:
		var value querydsl.WildcardQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
			return GraphDocumentFilter{}, err
		}
		path, err := graphDocumentPathFromQueryField(value.Field)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentWildcardFilter(oapi.GraphDocumentWildcardFilter{Wildcard: value.Wildcard, Path: path})
		return out, err
	case probe.Min.present || probe.Max.present:
		return convertGraphRangeFilter(filter, probe)
	case probe.Start.present || probe.End.present:
		var value querydsl.DateRangeStringQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
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
	case probe.IDs.present:
		var value querydsl.DocIdQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
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
	case probe.Bool.present:
		var value querydsl.BoolFieldQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
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
	case probe.MatchAll.present:
		var value querydsl.MatchAllQuery
		if err := filter.DecodeStrictInto(&value); err != nil || len(value.MatchAll) != 0 {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph match_all body must be empty")
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentMatchAllFilter(oapi.GraphDocumentMatchAllFilter{MatchAll: value.MatchAll})
		return out, err
	case probe.MatchNone.present:
		var value querydsl.MatchNoneQuery
		if err := filter.DecodeStrictInto(&value); err != nil || len(value.MatchNone) != 0 {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph match_none body must be empty")
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentMatchNoneFilter(oapi.GraphDocumentMatchNoneFilter{MatchNone: value.MatchNone})
		return out, err
	case probe.Conjuncts.present:
		var value querydsl.ConjunctionQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
			return GraphDocumentFilter{}, err
		}
		items, err := convertGraphFilterItems(value.Conjuncts, depth, visited)
		if err != nil {
			return GraphDocumentFilter{}, err
		}
		var out GraphDocumentFilter
		err = out.FromGraphDocumentFilterConjunction(oapi.GraphDocumentFilterConjunction{Conjuncts: items})
		return out, err
	case probe.Disjuncts.present:
		var value querydsl.DisjunctionQuery
		if err := filter.DecodeStrictInto(&value); err != nil {
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
	case probe.Must.present || probe.Should.present || probe.MustNot.present || probe.Filter.present:
		return convertGraphBooleanFilter(filter, probe, depth, visited)
	default:
		return GraphDocumentFilter{}, fmt.Errorf("antfly: query variant is not supported by graph document filters")
	}
}

func graphDocumentFuzziness(value querydsl.Fuzziness) (oapi.Fuzziness, error) {
	var out oapi.Fuzziness
	if numeric, err := value.AsFuzziness0(); err == nil {
		if numeric < 0 || numeric > 2 {
			return oapi.Fuzziness{}, fmt.Errorf("antfly: graph fuzziness must be between 0 and 2")
		}
		return out, out.FromFuzziness0(numeric)
	}
	text, err := value.AsFuzziness1()
	if err != nil || text != querydsl.Fuzziness1("auto") {
		return oapi.Fuzziness{}, fmt.Errorf("antfly: graph fuzziness must be 0, 1, 2, or auto")
	}
	return out, out.FromFuzziness1(oapi.Fuzziness1(text))
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

func convertGraphRangeFilter(filter querydsl.Query, probe graphQueryProbe) (GraphDocumentFilter, error) {
	bound := probe.Min
	if !bound.present {
		bound = probe.Max
	}
	if !bound.present {
		return GraphDocumentFilter{}, fmt.Errorf("antfly: graph range requires min or max")
	}
	if bound.kind == '"' {
		var value querydsl.TermRangeQuery
		if err := filter.DecodeStrictInto(&value); err != nil || value.Min == nil && value.Max == nil {
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
	var value querydsl.NumericRangeQuery
	if err := filter.DecodeStrictInto(&value); err != nil || value.Min == nil && value.Max == nil {
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

func convertGraphBooleanFilter(filter querydsl.Query, probe graphQueryProbe, depth int, visited *int) (GraphDocumentFilter, error) {
	var value querydsl.BooleanQuery
	if err := filter.DecodeStrictInto(&value); err != nil {
		return GraphDocumentFilter{}, err
	}
	var body oapi.GraphDocumentFilterBoolean
	if probe.Filter.present {
		converted, err := convertGraphDocumentFilter(value.Filter, depth+1, visited)
		if err != nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph bool filter: %w", err)
		}
		body.Filter = converted
	}
	for _, clause := range []struct {
		name        string
		present     bool
		value       querydsl.DisjunctionQuery
		destination *oapi.GraphDocumentFilterDisjunction
	}{
		{name: "should", present: probe.Should.present, value: value.Should, destination: &body.Should},
		{name: "must_not", present: probe.MustNot.present, value: value.MustNot, destination: &body.MustNot},
	} {
		if !clause.present {
			continue
		}
		items, err := convertGraphFilterItems(clause.value.Disjuncts, depth, visited)
		if err != nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph bool %s: %w", clause.name, err)
		}
		if clause.value.Min != nil && *clause.value.Min > uint32(len(items)) {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph boolean threshold exceeds its number of clauses")
		}
		*clause.destination = oapi.GraphDocumentFilterDisjunction{Disjuncts: items, Min: clause.value.Min}
	}
	if probe.Must.present {
		items, err := convertGraphFilterItems(value.Must.Conjuncts, depth, visited)
		if err != nil {
			return GraphDocumentFilter{}, fmt.Errorf("antfly: graph bool must: %w", err)
		}
		body.Must = oapi.GraphDocumentFilterConjunction{Conjuncts: items}
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
	decoded, err := filter.DecodeStrictVariant()
	if err != nil {
		return err
	}
	if decoded.Kind == 0 {
		if depth == 0 {
			return nil // The containing filter property is optional.
		}
		return fmt.Errorf("antfly: nested graph document filters must not be null")
	}

	validatePath := func(path string) error {
		if !validGraphDocumentJSONPointer(path) {
			return fmt.Errorf("antfly: graph document filter path must be a valid RFC 6901 JSON Pointer")
		}
		return nil
	}
	switch decoded.Kind {
	case oapi.GraphDocumentFilterVariantFuzzy:
		value := *decoded.Fuzzy
		if err := validatePath(value.Path); err != nil {
			return err
		}
		if value.PrefixLength < 0 || value.PrefixLength > 255 {
			return fmt.Errorf("antfly: graph document filter prefix_length must be between 0 and 255")
		}
		if numeric, numericErr := value.Fuzziness.AsFuzziness0(); numericErr == nil {
			if numeric < 0 || numeric > 2 {
				return fmt.Errorf("antfly: graph document filter fuzziness must be 0, 1, 2, or auto")
			}
		} else if text, textErr := value.Fuzziness.AsFuzziness1(); textErr != nil || text != "auto" {
			return fmt.Errorf("antfly: graph document filter fuzziness must be 0, 1, 2, or auto")
		}
		return nil
	case oapi.GraphDocumentFilterVariantTerm:
		value := *decoded.Term
		return validatePath(value.Path)
	case oapi.GraphDocumentFilterVariantPrefix:
		value := *decoded.Prefix
		return validatePath(value.Path)
	case oapi.GraphDocumentFilterVariantRegexp:
		value := *decoded.Regexp
		return validatePath(value.Path)
	case oapi.GraphDocumentFilterVariantWildcard:
		value := *decoded.Wildcard
		return validatePath(value.Path)
	case oapi.GraphDocumentFilterVariantNumericRange:
		value := *decoded.Numeric
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
	case oapi.GraphDocumentFilterVariantTermRange:
		value := *decoded.TermRange
		if err := validatePath(value.TermRange.Path); err != nil {
			return err
		}
		if value.TermRange.Min == nil && value.TermRange.Max == nil {
			return fmt.Errorf("antfly: graph term range requires min or max")
		}
		return nil
	case oapi.GraphDocumentFilterVariantDateRange:
		value := *decoded.DateRange
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
	case oapi.GraphDocumentFilterVariantMatchAll:
		value := *decoded.MatchAll
		if value.MatchAll == nil || len(value.MatchAll) != 0 {
			return fmt.Errorf("antfly: graph match_all body must be an empty object")
		}
		return nil
	case oapi.GraphDocumentFilterVariantMatchNone:
		value := *decoded.MatchNone
		if value.MatchNone == nil || len(value.MatchNone) != 0 {
			return fmt.Errorf("antfly: graph match_none body must be an empty object")
		}
		return nil
	case oapi.GraphDocumentFilterVariantIDs:
		value := *decoded.IDs
		if len(value.Ids) == 0 || len(value.Ids) > 10_000 {
			return fmt.Errorf("antfly: graph ids must contain between 1 and 10000 values")
		}
		return validateNonEmptyUnique("graph id", value.Ids)
	case oapi.GraphDocumentFilterVariantBoolField:
		value := *decoded.BoolField
		return validatePath(value.BoolField.Path)
	case oapi.GraphDocumentFilterVariantConjunction:
		value := *decoded.Conjunction
		if len(value.Conjuncts) == 0 || len(value.Conjuncts) > 64 {
			return fmt.Errorf("antfly: graph filter conjuncts must contain between 1 and 64 entries")
		}
		for i, child := range value.Conjuncts {
			if err := validateGraphDocumentFilter(child, depth+1, visited); err != nil {
				return fmt.Errorf("antfly: graph filter conjunct %d: %w", i, err)
			}
		}
		return nil
	case oapi.GraphDocumentFilterVariantDisjunction:
		value := *decoded.Disjunction
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
	case oapi.GraphDocumentFilterVariantBoolean:
		value := *decoded.Boolean
		if decoded.BooleanFilterPresent {
			if err := validateGraphDocumentFilter(value.Filter, depth+1, visited); err != nil {
				return fmt.Errorf("antfly: graph bool filter: %w", err)
			}
		}
		for _, clauseGroup := range []struct {
			name    string
			clauses []GraphDocumentFilter
			present bool
		}{
			{name: "must", clauses: value.Must.Conjuncts, present: decoded.BooleanMustPresent},
			{name: "should", clauses: value.Should.Disjuncts, present: decoded.BooleanShouldPresent},
			{name: "must_not", clauses: value.MustNot.Disjuncts, present: decoded.BooleanMustNotPresent},
		} {
			name, clause := clauseGroup.name, clauseGroup.clauses
			if !clauseGroup.present {
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
		if decoded.BooleanShouldPresent && value.Should.Min != nil && *value.Should.Min > uint32(len(value.Should.Disjuncts)) {
			return fmt.Errorf("antfly: graph bool should min exceeds its number of clauses")
		}
		if decoded.BooleanMustNotPresent && value.MustNot.Min != nil && *value.MustNot.Min > uint32(len(value.MustNot.Disjuncts)) {
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
	if err := validateGraphPathQuery(query.Index, query.ShortestPath.From, query.ShortestPath.To, query.ShortestPath.Direction, query.ShortestPath.Filter, query.ShortestPath.EdgeTypes, query.ShortestPath.MaxDepth, query.ShortestPath.EdgeWeight, query.ShortestPath.Objective, query.ShortestPath.IncludeDocuments, query.ShortestPath.Fields); err != nil {
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
	if err := validateGraphPathQuery(query.Index, query.KShortestPaths.From, query.KShortestPaths.To, query.KShortestPaths.Direction, query.KShortestPaths.Filter, query.KShortestPaths.EdgeTypes, query.KShortestPaths.MaxDepth, query.KShortestPaths.EdgeWeight, query.KShortestPaths.Objective, query.KShortestPaths.IncludeDocuments, query.KShortestPaths.Fields); err != nil {
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
		if !validGraphTableQualifier(table[0]) {
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
		if node.Table != "" && !validGraphTableQualifier(node.Table) {
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
			if node.Table != "" && !validGraphTableQualifier(node.Table) {
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
	if err := validateGraphEdgeWeightRange(edge.EdgeWeight); err != nil {
		return err
	}
	return nil
}

func validateGraphWhereExpression(where GraphWhereExpression, aliases map[string]struct{}, complexity *graphMatchComplexity, depth int) error {
	if depth >= maxGraphMatchPredicateDepth {
		return fmt.Errorf("antfly: graph where expression exceeds the maximum depth of %d", maxGraphMatchPredicateDepth)
	}
	decoded, err := where.DecodeStrictVariant()
	if err != nil {
		return err
	}
	if decoded.Kind == 0 {
		if depth == 0 {
			return nil // The containing where property is optional.
		}
		return fmt.Errorf("antfly: nested graph where expressions must not be null")
	}
	switch decoded.Kind {
	case oapi.GraphWhereVariantAnd:
		value := *decoded.And
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
	case oapi.GraphWhereVariantNotEqual:
		value := *decoded.NotEqual
		if err := complexity.addPredicate(); err != nil {
			return err
		}
		if _, ok := aliases[value.NotEqual.Left.Alias]; !ok {
			return fmt.Errorf("antfly: graph predicate references unknown alias %q", value.NotEqual.Left.Alias)
		}
		if _, ok := aliases[value.NotEqual.Right.Alias]; !ok {
			return fmt.Errorf("antfly: graph predicate references unknown alias %q", value.NotEqual.Right.Alias)
		}
	case oapi.GraphWhereVariantNotExists:
		value := *decoded.NotExists
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
	default:
		return fmt.Errorf("antfly: unsupported graph where expression variant")
	}
	return nil
}

func validateGraphReturn(graphReturn GraphReturn, match GraphMatch) error {
	decoded, err := graphReturn.DecodeStrictVariant()
	if err != nil {
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
	if decoded.Kind == oapi.GraphReturnVariantBindings {
		value := *decoded.Bindings
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
	return validateGraphAggregates(decoded.Aggregates.Aggregates, known)
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
	decoded, err := aggregate.DecodeStrictVariant()
	if err != nil {
		return "", false, err
	}
	return decoded.Count, decoded.Distinct, nil
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
	if err := validateGraphDirection(query.Traverse.Direction); err != nil {
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
	if err := validateGraphEdgeWeightRange(query.Traverse.EdgeWeight); err != nil {
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

func validateGraphPathQuery(index string, from, to GraphPathEndpoint, direction EdgeDirection, filter GraphDocumentFilter, edgeTypes []string, maxDepth int, edgeWeight *GraphEdgeWeightRange, objective GraphPathObjective, includeDocuments bool, fields []string) error {
	if strings.TrimSpace(index) == "" {
		return fmt.Errorf("antfly: graph index must not be empty")
	}
	if from.Key == "" || to.Key == "" {
		return fmt.Errorf("antfly: graph path endpoints must not be empty")
	}
	if from.Table != nil && !validGraphTableQualifier(*from.Table) {
		return fmt.Errorf("antfly: graph path from table must be omitted or nonblank")
	}
	if to.Table != nil && !validGraphTableQualifier(*to.Table) {
		return fmt.Errorf("antfly: graph path to table must be omitted or nonblank")
	}
	if err := validateGraphDirection(direction); err != nil {
		return err
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
	if err := validateGraphEdgeWeightRange(edgeWeight); err != nil {
		return err
	}
	if objective != "" && objective != GraphPathObjectiveMinHops && objective != GraphPathObjectiveMinWeightSum && objective != GraphPathObjectiveMaxWeightProduct {
		return fmt.Errorf("antfly: graph path objective must be min_hops, min_weight_sum, or max_weight_product")
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

func validateGraphDirection(direction EdgeDirection) error {
	if direction != "" && direction != EdgeDirectionOut && direction != EdgeDirectionIn && direction != EdgeDirectionBoth {
		return fmt.Errorf("antfly: graph direction must be out, in, or both")
	}
	return nil
}

func validateGraphSelector(selector GraphNodeSelector) error {
	decoded, err := selector.DecodeStrictVariant()
	if err != nil {
		return err
	}
	switch decoded.Kind {
	case oapi.GraphNodeSelectorVariantKeys:
		value := *decoded.Keys
		if len(value.Keys) > 10_000 {
			return fmt.Errorf("antfly: graph keys must contain at most 10000 entries")
		}
		if err := validateNonEmptyUnique("graph key", value.Keys); err != nil {
			return err
		}
	case oapi.GraphNodeSelectorVariantIdentities:
		value := *decoded.Identities
		if err := validateGraphIdentities(value.Identities); err != nil {
			return err
		}
	case oapi.GraphNodeSelectorVariantResultRef:
		value := *decoded.ResultRef
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
	default:
		return fmt.Errorf("antfly: unsupported graph selector variant")
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
		if identity.Table != nil && !validGraphTableQualifier(*identity.Table) {
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

// validGraphTableQualifier is the graph wire policy for an explicitly present
// table identity. Exact bytes are preserved, but a qualifier containing only
// JSON/HTTP ASCII whitespace is invalid; omission selects the query table.
func validGraphTableQualifier(value string) bool {
	return strings.Trim(value, " \t\r\n") != ""
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
	if minWeight != nil && (math.IsNaN(*minWeight) || math.IsInf(*minWeight, 0) || *minWeight < 0) {
		return fmt.Errorf("antfly: graph minimum weight must be finite and non-negative")
	}
	if maxWeight != nil && (math.IsNaN(*maxWeight) || math.IsInf(*maxWeight, 0) || *maxWeight < 0) {
		return fmt.Errorf("antfly: graph maximum weight must be finite and non-negative")
	}
	if minWeight != nil && maxWeight != nil && *minWeight > *maxWeight {
		return fmt.Errorf("antfly: graph minimum weight must not exceed maximum weight")
	}
	return nil
}

func validateGraphEdgeWeightRange(weight *GraphEdgeWeightRange) error {
	if weight == nil {
		return nil
	}
	if weight.Min == nil && weight.Max == nil {
		return fmt.Errorf("antfly: graph edge_weight must contain min or max")
	}
	return validateGraphWeightBounds(weight.Min, weight.Max)
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
