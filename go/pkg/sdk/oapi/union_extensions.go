// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// DecodeInto decodes the retained GraphResult JSON directly into value.
// It lets SDK adapters inspect a small discriminator probe without first
// marshaling and copying a potentially large result payload.
func (t GraphResult) DecodeInto(value any) error {
	return json.Unmarshal(t.union, value)
}

// DecodeStrictInto decodes a GraphResult with the generated model while
// rejecting fields that are not part of that model. Semantic graph invariants
// are validated by the SDK after the concrete result variant is selected.
func (t GraphResult) DecodeStrictInto(value any) error {
	return decodeStrictJSON(t.union, value)
}

// DecodeStrictInto decodes an opaque graph request union into its selected
// concrete generated type without an intermediate JSON copy.
func (t GraphQuery) DecodeStrictInto(value any) error {
	return decodeStrictJSON(t.union, value)
}

// DecodeStrictInto decodes a GraphDocumentFilter into one concrete filter.
func (t GraphDocumentFilter) DecodeStrictInto(value any) error {
	return decodeStrictJSON(t.union, value)
}

// DecodeStrictInto decodes a GraphNodeSelector into one concrete selector.
func (t GraphNodeSelector) DecodeStrictInto(value any) error {
	return decodeStrictJSON(t.union, value)
}

// DecodeStrictInto decodes a GraphReturn into one concrete return shape.
func (t GraphReturn) DecodeStrictInto(value any) error {
	return decodeStrictJSON(t.union, value)
}

// DecodeStrictInto decodes a GraphWhereExpression into one concrete predicate.
func (t GraphWhereExpression) DecodeStrictInto(value any) error {
	return decodeStrictJSON(t.union, value)
}

func decodeStrictJSON(encoded []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

// strictPresent preserves the distinction between an omitted member, an
// explicit null, and a concrete value while retaining strict decoding for the
// selected value. Generated optional pointers cannot represent all three wire
// states, which matters for operation-keyed structural unions.
type strictPresent[T any] struct {
	present bool
	null    bool
	value   T
}

// memberPresence records only member presence and nullability. It is used for
// structural-union selection without retaining or copying the member payload.
type memberPresence struct {
	present bool
	null    bool
}

func (p *memberPresence) UnmarshalJSON(encoded []byte) error {
	p.present = true
	p.null = bytes.Equal(bytes.TrimSpace(encoded), []byte("null"))
	return nil
}

func retainedUnionIsNull(encoded []byte) bool {
	trimmed := bytes.TrimSpace(encoded)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

// rejectCanonicalGraphNulls enforces the canonical graph request invariant at
// its retained root union. Canonical graph request schemas are deliberately
// non-nullable throughout: optional members are omitted and every present
// member has a concrete value. Scanning once here prevents generated Go value
// fields from collapsing an explicit nested null into the same zero value as
// omission, without materializing a second recursive JSON tree.
func rejectCanonicalGraphNulls(encoded []byte) error {
	inString := false
	escaped := false
	for index, value := range encoded {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch value {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		if value == '"' {
			inString = true
			continue
		}
		if value == 'n' && len(encoded)-index >= len("null") &&
			bytes.Equal(encoded[index:index+len("null")], []byte("null")) {
			return errors.New("canonical graph queries do not accept explicit null; omit optional members")
		}
	}
	return nil
}

func (p *strictPresent[T]) UnmarshalJSON(encoded []byte) error {
	p.present = true
	if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
		p.null = true
		return nil
	}
	return decodeStrictJSON(encoded, &p.value)
}

// GraphQueryVariantKind identifies the concrete arm selected by GraphQuery.
type GraphQueryVariantKind uint8

const (
	GraphQueryVariantMatch GraphQueryVariantKind = iota + 1
	GraphQueryVariantTraverse
	GraphQueryVariantShortestPath
	GraphQueryVariantKShortestPaths
)

// DecodedGraphQuery is a presence-safe, strictly decoded GraphQuery arm.
// Exactly one pointer is non-nil.
type DecodedGraphQuery struct {
	Kind           GraphQueryVariantKind
	Match          *GraphMatchQuery
	Traverse       *GraphTraverseQuery
	ShortestPath   *GraphShortestPathQuery
	KShortestPaths *GraphKShortestPathsQuery
}

type graphQueryStrictEnvelope struct {
	Index          strictPresent[string]              `json:"index"`
	Return         strictPresent[GraphReturn]         `json:"return"`
	Match          strictPresent[GraphMatch]          `json:"match"`
	Traverse       strictPresent[GraphTraversal]      `json:"traverse"`
	ShortestPath   strictPresent[GraphShortestPath]   `json:"shortest_path"`
	KShortestPaths strictPresent[GraphKShortestPaths] `json:"k_shortest_paths"`
}

// DecodeStrictVariant selects and decodes one operation-keyed GraphQuery arm
// without copying the retained union through a RawMessage map. Null operation
// members remain present and are rejected instead of being mistaken for
// omission.
func (t GraphQuery) DecodeStrictVariant() (DecodedGraphQuery, error) {
	if err := rejectCanonicalGraphNulls(t.union); err != nil {
		return DecodedGraphQuery{}, err
	}
	var envelope graphQueryStrictEnvelope
	if err := decodeStrictJSON(t.union, &envelope); err != nil {
		return DecodedGraphQuery{}, err
	}
	if !envelope.Index.present || envelope.Index.null {
		return DecodedGraphQuery{}, errors.New("graph query requires a non-null index")
	}
	operations := []bool{
		envelope.Match.present,
		envelope.Traverse.present,
		envelope.ShortestPath.present,
		envelope.KShortestPaths.present,
	}
	count := 0
	for _, present := range operations {
		if present {
			count++
		}
	}
	if count != 1 {
		return DecodedGraphQuery{}, errors.New("graph query must contain exactly one operation")
	}
	if envelope.Match.present {
		if envelope.Match.null {
			return DecodedGraphQuery{}, errors.New("graph query match must not be null")
		}
		if !envelope.Return.present || envelope.Return.null {
			return DecodedGraphQuery{}, errors.New("graph match query requires a non-null return")
		}
		value := &GraphMatchQuery{Index: envelope.Index.value, Match: envelope.Match.value, Return: envelope.Return.value}
		return DecodedGraphQuery{Kind: GraphQueryVariantMatch, Match: value}, nil
	}
	if envelope.Return.present {
		return DecodedGraphQuery{}, errors.New("graph return is only valid for match queries")
	}
	if envelope.Traverse.present {
		if envelope.Traverse.null {
			return DecodedGraphQuery{}, errors.New("graph query traverse must not be null")
		}
		value := &GraphTraverseQuery{Index: envelope.Index.value, Traverse: envelope.Traverse.value}
		return DecodedGraphQuery{Kind: GraphQueryVariantTraverse, Traverse: value}, nil
	}
	if envelope.ShortestPath.present {
		if envelope.ShortestPath.null {
			return DecodedGraphQuery{}, errors.New("graph query shortest_path must not be null")
		}
		value := &GraphShortestPathQuery{Index: envelope.Index.value, ShortestPath: envelope.ShortestPath.value}
		return DecodedGraphQuery{Kind: GraphQueryVariantShortestPath, ShortestPath: value}, nil
	}
	if envelope.KShortestPaths.null {
		return DecodedGraphQuery{}, errors.New("graph query k_shortest_paths must not be null")
	}
	value := &GraphKShortestPathsQuery{Index: envelope.Index.value, KShortestPaths: envelope.KShortestPaths.value}
	return DecodedGraphQuery{Kind: GraphQueryVariantKShortestPaths, KShortestPaths: value}, nil
}

// GraphReturnVariantKind identifies the concrete canonical MATCH return arm.
type GraphReturnVariantKind uint8

const (
	GraphReturnVariantBindings GraphReturnVariantKind = iota + 1
	GraphReturnVariantAggregates
)

// DecodedGraphReturn is a strictly decoded canonical MATCH return arm.
type DecodedGraphReturn struct {
	Kind       GraphReturnVariantKind
	Bindings   *GraphBindingsReturn
	Aggregates *GraphAggregatesReturn
}

type graphReturnStrictEnvelope struct {
	Bindings         strictPresent[[]GraphIdentifier]              `json:"bindings"`
	Aggregates       strictPresent[map[string]GraphCountAggregate] `json:"aggregates"`
	Limit            strictPresent[int]                            `json:"limit"`
	IncludeDocuments strictPresent[bool]                           `json:"include_documents"`
	Fields           strictPresent[[]string]                       `json:"fields"`
}

// DecodeStrictVariant selects one GraphReturn arm without remarshal/probe
// cycles and rejects null or cross-arm members.
func (t GraphReturn) DecodeStrictVariant() (DecodedGraphReturn, error) {
	var envelope graphReturnStrictEnvelope
	if err := decodeStrictJSON(t.union, &envelope); err != nil {
		return DecodedGraphReturn{}, err
	}
	if envelope.Bindings.present == envelope.Aggregates.present {
		return DecodedGraphReturn{}, errors.New("graph return must contain exactly one arm")
	}
	if envelope.Bindings.present {
		if envelope.Bindings.null {
			return DecodedGraphReturn{}, errors.New("graph return bindings must not be null")
		}
		if envelope.Limit.null || envelope.IncludeDocuments.null || envelope.Fields.null {
			return DecodedGraphReturn{}, errors.New("graph binding return optional fields must not be null")
		}
		value := &GraphBindingsReturn{
			Bindings:         envelope.Bindings.value,
			Limit:            envelope.Limit.value,
			IncludeDocuments: envelope.IncludeDocuments.value,
			Fields:           envelope.Fields.value,
		}
		return DecodedGraphReturn{Kind: GraphReturnVariantBindings, Bindings: value}, nil
	}
	if envelope.Aggregates.null {
		return DecodedGraphReturn{}, errors.New("graph return aggregates must not be null")
	}
	if envelope.Limit.present || envelope.IncludeDocuments.present || envelope.Fields.present {
		return DecodedGraphReturn{}, errors.New("binding projection fields are not valid for aggregate returns")
	}
	value := &GraphAggregatesReturn{Aggregates: envelope.Aggregates.value}
	return DecodedGraphReturn{Kind: GraphReturnVariantAggregates, Aggregates: value}, nil
}

// GraphNodeSelectorVariantKind identifies the selected exact selector arm.
type GraphNodeSelectorVariantKind uint8

const (
	GraphNodeSelectorVariantKeys GraphNodeSelectorVariantKind = iota + 1
	GraphNodeSelectorVariantIdentities
	GraphNodeSelectorVariantResultRef
)

// DecodedGraphNodeSelector is a presence-safe selector arm. Exactly one
// pointer is non-nil.
type DecodedGraphNodeSelector struct {
	Kind       GraphNodeSelectorVariantKind
	Keys       *GraphKeyNodeSelector
	Identities *GraphIdentityNodeSelector
	ResultRef  *GraphResultRefNodeSelector
}

type graphNodeSelectorStrictEnvelope struct {
	Keys       strictPresent[[]string]            `json:"keys"`
	Identities strictPresent[[]GraphPathEndpoint] `json:"identities"`
	ResultRef  strictPresent[string]              `json:"result_ref"`
	Binding    strictPresent[GraphIdentifier]     `json:"binding"`
	Limit      strictPresent[int]                 `json:"limit"`
}

// DecodeStrictVariant selects and decodes one GraphNodeSelector arm in one
// pass over the retained union. Result-reference-only options are rejected on
// key and identity selectors rather than being silently ignored.
func (t GraphNodeSelector) DecodeStrictVariant() (DecodedGraphNodeSelector, error) {
	var envelope graphNodeSelectorStrictEnvelope
	if err := decodeStrictJSON(t.union, &envelope); err != nil {
		return DecodedGraphNodeSelector{}, err
	}
	forms := 0
	for _, present := range []bool{envelope.Keys.present, envelope.Identities.present, envelope.ResultRef.present} {
		if present {
			forms++
		}
	}
	if forms != 1 {
		return DecodedGraphNodeSelector{}, errors.New("graph selector must contain exactly one selector form")
	}
	if envelope.Keys.present {
		if envelope.Keys.null {
			return DecodedGraphNodeSelector{}, errors.New("graph selector keys must not be null")
		}
		if envelope.Binding.present || envelope.Limit.present {
			return DecodedGraphNodeSelector{}, errors.New("graph result-reference options require result_ref")
		}
		value := &GraphKeyNodeSelector{Keys: envelope.Keys.value}
		return DecodedGraphNodeSelector{Kind: GraphNodeSelectorVariantKeys, Keys: value}, nil
	}
	if envelope.Identities.present {
		if envelope.Identities.null {
			return DecodedGraphNodeSelector{}, errors.New("graph selector identities must not be null")
		}
		if envelope.Binding.present || envelope.Limit.present {
			return DecodedGraphNodeSelector{}, errors.New("graph result-reference options require result_ref")
		}
		value := &GraphIdentityNodeSelector{Identities: envelope.Identities.value}
		return DecodedGraphNodeSelector{Kind: GraphNodeSelectorVariantIdentities, Identities: value}, nil
	}
	if envelope.ResultRef.null || envelope.Binding.null || envelope.Limit.null {
		return DecodedGraphNodeSelector{}, errors.New("graph result-reference members must be omitted or non-null")
	}
	value := &GraphResultRefNodeSelector{
		ResultRef: envelope.ResultRef.value,
		Binding:   envelope.Binding.value,
		Limit:     envelope.Limit.value,
	}
	return DecodedGraphNodeSelector{Kind: GraphNodeSelectorVariantResultRef, ResultRef: value}, nil
}

// GraphWhereVariantKind identifies one structural MATCH predicate arm.
type GraphWhereVariantKind uint8

const (
	GraphWhereVariantAnd GraphWhereVariantKind = iota + 1
	GraphWhereVariantNotEqual
	GraphWhereVariantNotExists
)

// DecodedGraphWhereExpression contains exactly one strictly decoded arm.
type DecodedGraphWhereExpression struct {
	Kind      GraphWhereVariantKind
	And       *GraphWhereAnd
	NotEqual  *GraphWhereNotEqual
	NotExists *GraphWhereNotExists
}

type graphWhereStrictEnvelope struct {
	And       strictPresent[[]GraphWhereExpression] `json:"and"`
	NotEqual  strictPresent[GraphNotEqualPredicate] `json:"not_equal"`
	NotExists strictPresent[GraphNotExistsPattern]  `json:"not_exists"`
}

// DecodeStrictVariant preserves the distinction between an omitted optional
// where expression and an invalid empty, null-member, or multi-arm object.
func (t GraphWhereExpression) DecodeStrictVariant() (DecodedGraphWhereExpression, error) {
	if retainedUnionIsNull(t.union) {
		return DecodedGraphWhereExpression{}, nil
	}
	var envelope graphWhereStrictEnvelope
	if err := decodeStrictJSON(t.union, &envelope); err != nil {
		return DecodedGraphWhereExpression{}, err
	}
	forms := 0
	for _, present := range []bool{envelope.And.present, envelope.NotEqual.present, envelope.NotExists.present} {
		if present {
			forms++
		}
	}
	if forms != 1 {
		return DecodedGraphWhereExpression{}, errors.New("graph where expression must contain exactly one predicate form")
	}
	if envelope.And.present {
		if envelope.And.null {
			return DecodedGraphWhereExpression{}, errors.New("graph where-and must not be null")
		}
		value := &GraphWhereAnd{And: envelope.And.value}
		return DecodedGraphWhereExpression{Kind: GraphWhereVariantAnd, And: value}, nil
	}
	if envelope.NotEqual.present {
		if envelope.NotEqual.null {
			return DecodedGraphWhereExpression{}, errors.New("graph not_equal must not be null")
		}
		value := &GraphWhereNotEqual{NotEqual: envelope.NotEqual.value}
		return DecodedGraphWhereExpression{Kind: GraphWhereVariantNotEqual, NotEqual: value}, nil
	}
	if envelope.NotExists.null {
		return DecodedGraphWhereExpression{}, errors.New("graph not_exists must not be null")
	}
	value := &GraphWhereNotExists{NotExists: envelope.NotExists.value}
	return DecodedGraphWhereExpression{Kind: GraphWhereVariantNotExists, NotExists: value}, nil
}

// DecodedGraphCountAggregate is the canonical count expression. Distinct is
// represented only for alias counts; count(*) with a distinct member is
// rejected even when the member is false.
type DecodedGraphCountAggregate struct {
	Count    string
	Distinct bool
}

type graphCountAggregateStrictEnvelope struct {
	Count    strictPresent[string] `json:"count"`
	Distinct strictPresent[bool]   `json:"distinct"`
}

func (t GraphCountAggregate) DecodeStrictVariant() (DecodedGraphCountAggregate, error) {
	var envelope graphCountAggregateStrictEnvelope
	if err := decodeStrictJSON(t.union, &envelope); err != nil {
		return DecodedGraphCountAggregate{}, err
	}
	if !envelope.Count.present || envelope.Count.null {
		return DecodedGraphCountAggregate{}, errors.New("count expression requires a non-null count")
	}
	if envelope.Distinct.null {
		return DecodedGraphCountAggregate{}, errors.New("count distinct must be omitted or non-null")
	}
	if envelope.Count.value == "*" && envelope.Distinct.present {
		return DecodedGraphCountAggregate{}, errors.New("distinct is only valid for alias counts")
	}
	return DecodedGraphCountAggregate{Count: envelope.Count.value, Distinct: envelope.Distinct.value}, nil
}

// GraphDocumentFilterVariantKind identifies the selected closed, non-scoring
// document-filter arm.
type GraphDocumentFilterVariantKind uint8

const (
	GraphDocumentFilterVariantFuzzy GraphDocumentFilterVariantKind = iota + 1
	GraphDocumentFilterVariantTerm
	GraphDocumentFilterVariantPrefix
	GraphDocumentFilterVariantRegexp
	GraphDocumentFilterVariantWildcard
	GraphDocumentFilterVariantNumericRange
	GraphDocumentFilterVariantTermRange
	GraphDocumentFilterVariantDateRange
	GraphDocumentFilterVariantMatchAll
	GraphDocumentFilterVariantMatchNone
	GraphDocumentFilterVariantIDs
	GraphDocumentFilterVariantBoolField
	GraphDocumentFilterVariantBoolean
	GraphDocumentFilterVariantConjunction
	GraphDocumentFilterVariantDisjunction
)

// DecodedGraphDocumentFilter contains one strictly decoded concrete arm. The
// boolean presence flags retain omission separately from generated zero values.
type DecodedGraphDocumentFilter struct {
	Kind        GraphDocumentFilterVariantKind
	Fuzzy       *GraphDocumentFuzzyFilter
	Term        *GraphDocumentTermFilter
	Prefix      *GraphDocumentPrefixFilter
	Regexp      *GraphDocumentRegexpFilter
	Wildcard    *GraphDocumentWildcardFilter
	Numeric     *GraphDocumentNumericRangeFilter
	TermRange   *GraphDocumentTermRangeFilter
	DateRange   *GraphDocumentDateRangeFilter
	MatchAll    *GraphDocumentMatchAllFilter
	MatchNone   *GraphDocumentMatchNoneFilter
	IDs         *GraphDocumentIdsFilter
	BoolField   *GraphDocumentBoolFieldFilter
	Boolean     *GraphDocumentFilterBoolean
	Conjunction *GraphDocumentFilterConjunction
	Disjunction *GraphDocumentFilterDisjunction

	BooleanFilterPresent  bool
	BooleanMustPresent    bool
	BooleanShouldPresent  bool
	BooleanMustNotPresent bool
}

type graphDocumentFilterProbe struct {
	Term         memberPresence `json:"term"`
	Fuzziness    memberPresence `json:"fuzziness"`
	Prefix       memberPresence `json:"prefix"`
	Regexp       memberPresence `json:"regexp"`
	Wildcard     memberPresence `json:"wildcard"`
	NumericRange memberPresence `json:"numeric_range"`
	TermRange    memberPresence `json:"term_range"`
	DateRange    memberPresence `json:"date_range"`
	MatchAll     memberPresence `json:"match_all"`
	MatchNone    memberPresence `json:"match_none"`
	IDs          memberPresence `json:"ids"`
	BoolField    memberPresence `json:"bool_field"`
	Filter       memberPresence `json:"filter"`
	Must         memberPresence `json:"must"`
	Should       memberPresence `json:"should"`
	MustNot      memberPresence `json:"must_not"`
	Conjuncts    memberPresence `json:"conjuncts"`
	Disjuncts    memberPresence `json:"disjuncts"`
}

// DecodeStrictVariant uses a presence-only probe followed by one strict typed
// decode. It never remarshal-copies the retained union or materializes a
// RawMessage map containing potentially large recursive filter payloads.
func (t GraphDocumentFilter) DecodeStrictVariant() (DecodedGraphDocumentFilter, error) {
	if retainedUnionIsNull(t.union) {
		return DecodedGraphDocumentFilter{}, nil
	}
	var probe graphDocumentFilterProbe
	if err := json.Unmarshal(t.union, &probe); err != nil {
		return DecodedGraphDocumentFilter{}, err
	}
	for _, member := range [...]struct {
		name     string
		presence memberPresence
	}{
		{name: "term", presence: probe.Term},
		{name: "fuzziness", presence: probe.Fuzziness},
		{name: "prefix", presence: probe.Prefix},
		{name: "regexp", presence: probe.Regexp},
		{name: "wildcard", presence: probe.Wildcard},
		{name: "numeric_range", presence: probe.NumericRange},
		{name: "term_range", presence: probe.TermRange},
		{name: "date_range", presence: probe.DateRange},
		{name: "match_all", presence: probe.MatchAll},
		{name: "match_none", presence: probe.MatchNone},
		{name: "ids", presence: probe.IDs},
		{name: "bool_field", presence: probe.BoolField},
		{name: "filter", presence: probe.Filter},
		{name: "must", presence: probe.Must},
		{name: "should", presence: probe.Should},
		{name: "must_not", presence: probe.MustNot},
		{name: "conjuncts", presence: probe.Conjuncts},
		{name: "disjuncts", presence: probe.Disjuncts},
	} {
		if member.presence.present && member.presence.null {
			return DecodedGraphDocumentFilter{}, fmt.Errorf("graph document filter %s must not be null", member.name)
		}
	}
	decode := func(value any) error { return decodeStrictJSON(t.union, value) }
	switch {
	case probe.Term.present && probe.Fuzziness.present:
		value := &GraphDocumentFuzzyFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantFuzzy, Fuzzy: value}, decode(value)
	case probe.Term.present:
		value := &GraphDocumentTermFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantTerm, Term: value}, decode(value)
	case probe.Prefix.present:
		value := &GraphDocumentPrefixFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantPrefix, Prefix: value}, decode(value)
	case probe.Regexp.present:
		value := &GraphDocumentRegexpFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantRegexp, Regexp: value}, decode(value)
	case probe.Wildcard.present:
		value := &GraphDocumentWildcardFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantWildcard, Wildcard: value}, decode(value)
	case probe.NumericRange.present:
		value := &GraphDocumentNumericRangeFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantNumericRange, Numeric: value}, decode(value)
	case probe.TermRange.present:
		value := &GraphDocumentTermRangeFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantTermRange, TermRange: value}, decode(value)
	case probe.DateRange.present:
		value := &GraphDocumentDateRangeFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantDateRange, DateRange: value}, decode(value)
	case probe.MatchAll.present:
		value := &GraphDocumentMatchAllFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantMatchAll, MatchAll: value}, decode(value)
	case probe.MatchNone.present:
		value := &GraphDocumentMatchNoneFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantMatchNone, MatchNone: value}, decode(value)
	case probe.IDs.present:
		value := &GraphDocumentIdsFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantIDs, IDs: value}, decode(value)
	case probe.BoolField.present:
		value := &GraphDocumentBoolFieldFilter{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantBoolField, BoolField: value}, decode(value)
	case probe.Conjuncts.present:
		value := &GraphDocumentFilterConjunction{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantConjunction, Conjunction: value}, decode(value)
	case probe.Disjuncts.present:
		value := &GraphDocumentFilterDisjunction{}
		return DecodedGraphDocumentFilter{Kind: GraphDocumentFilterVariantDisjunction, Disjunction: value}, decode(value)
	case probe.Filter.present || probe.Must.present || probe.Should.present || probe.MustNot.present:
		value := &GraphDocumentFilterBoolean{}
		return DecodedGraphDocumentFilter{
			Kind:                  GraphDocumentFilterVariantBoolean,
			Boolean:               value,
			BooleanFilterPresent:  probe.Filter.present,
			BooleanMustPresent:    probe.Must.present,
			BooleanShouldPresent:  probe.Should.present,
			BooleanMustNotPresent: probe.MustNot.present,
		}, decode(value)
	default:
		return DecodedGraphDocumentFilter{}, errors.New("unsupported graph document filter variant")
	}
}

type graphPathEndpointWire GraphPathEndpoint

func (t *GraphEdgeWeightRange) UnmarshalJSON(encoded []byte) error {
	var decoded struct {
		Max strictPresent[float64] `json:"max"`
		Min strictPresent[float64] `json:"min"`
	}
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	if decoded.Min.null || decoded.Max.null {
		return errors.New("graph edge_weight bounds must be omitted or non-null")
	}
	*t = GraphEdgeWeightRange{}
	if decoded.Min.present {
		value := decoded.Min.value
		t.Min = &value
	}
	if decoded.Max.present {
		value := decoded.Max.value
		t.Max = &value
	}
	return nil
}

type graphTraversalWire GraphTraversal

func (t *GraphTraversal) UnmarshalJSON(encoded []byte) error {
	if err := rejectNullGraphPathOptions(encoded, false); err != nil {
		return err
	}
	var decoded graphTraversalWire
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphTraversal(decoded)
	return nil
}

type graphShortestPathWire GraphShortestPath

func (t *GraphShortestPath) UnmarshalJSON(encoded []byte) error {
	if err := rejectNullGraphPathOptions(encoded, true); err != nil {
		return err
	}
	var decoded graphShortestPathWire
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphShortestPath(decoded)
	return nil
}

type graphKShortestPathsWire GraphKShortestPaths

func (t *GraphKShortestPaths) UnmarshalJSON(encoded []byte) error {
	if err := rejectNullGraphPathOptions(encoded, true); err != nil {
		return err
	}
	var decoded graphKShortestPathsWire
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphKShortestPaths(decoded)
	return nil
}

type graphMatchEdgeWire GraphMatchEdge

func (t *GraphMatchEdge) UnmarshalJSON(encoded []byte) error {
	if err := rejectNullGraphPathOptions(encoded, false); err != nil {
		return err
	}
	var decoded graphMatchEdgeWire
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphMatchEdge(decoded)
	return nil
}

func rejectNullGraphPathOptions(encoded []byte, hasObjective bool) error {
	var presence struct {
		EdgeWeight json.RawMessage `json:"edge_weight"`
		Objective  json.RawMessage `json:"objective"`
	}
	if err := json.Unmarshal(encoded, &presence); err != nil {
		return err
	}
	if err := rejectExplicitJSONNull(presence.EdgeWeight, "graph edge_weight must be omitted or non-null"); err != nil {
		return err
	}
	if hasObjective {
		return rejectExplicitJSONNull(presence.Objective, "graph path objective must be omitted or non-null")
	}
	return nil
}

// UnmarshalJSON preserves the OpenAPI distinction between an omitted optional
// table qualifier and explicit null without requiring callers to retain a
// second shadow copy of a graph result.
func (t *GraphPathEndpoint) UnmarshalJSON(encoded []byte) error {
	var decoded graphPathEndpointWire
	if err := decodeGraphIdentityJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphPathEndpoint(decoded)
	return nil
}

type graphBindingNodeWire GraphBindingNode

func (t *GraphBindingNode) UnmarshalJSON(encoded []byte) error {
	presence := struct {
		Document graphJSONObjectPresence `json:"document"`
		Table    json.RawMessage         `json:"table"`
	}{
		Document: graphJSONObjectPresence{name: "graph binding document"},
	}
	if err := json.Unmarshal(encoded, &presence); err != nil {
		return err
	}
	if err := rejectExplicitJSONNull(presence.Table, "graph node table must be omitted or non-null"); err != nil {
		return err
	}

	var decoded graphBindingNodeWire
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphBindingNode(decoded)
	return nil
}

type graphResultNodeWire GraphResultNode

func (t *GraphResultNode) UnmarshalJSON(encoded []byte) error {
	presence := struct {
		Document graphJSONObjectPresence `json:"document"`
		Evidence graphJSONObjectPresence `json:"evidence"`
		Table    json.RawMessage         `json:"table"`
	}{
		Document: graphJSONObjectPresence{name: "graph result document"},
		Evidence: graphJSONObjectPresence{name: "graph result evidence"},
	}
	if err := json.Unmarshal(encoded, &presence); err != nil {
		return err
	}
	if err := rejectExplicitJSONNull(presence.Table, "graph node table must be omitted or non-null"); err != nil {
		return err
	}

	var decoded graphResultNodeWire
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphResultNode(decoded)
	return nil
}

type graphPathEdgeWire GraphPathEdge

func (t *GraphPathEdge) UnmarshalJSON(encoded []byte) error {
	presence := struct {
		Metadata graphJSONObjectPresence `json:"metadata"`
	}{
		Metadata: graphJSONObjectPresence{name: "graph path edge metadata"},
	}
	if err := json.Unmarshal(encoded, &presence); err != nil {
		return err
	}

	var decoded graphPathEdgeWire
	if err := decodeStrictJSON(encoded, &decoded); err != nil {
		return err
	}
	*t = GraphPathEdge(decoded)
	return nil
}

func decodeGraphIdentityJSON(encoded []byte, value any) error {
	var presence struct {
		Table json.RawMessage `json:"table"`
	}
	if err := json.Unmarshal(encoded, &presence); err != nil {
		return err
	}
	if err := rejectExplicitJSONNull(presence.Table, "graph node table must be omitted or non-null"); err != nil {
		return err
	}
	return decodeStrictJSON(encoded, value)
}

func rejectExplicitJSONNull(encoded json.RawMessage, message string) error {
	if len(encoded) != 0 && bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
		return errors.New(message)
	}
	return nil
}

// graphJSONObjectPresence validates an opaque JSON object's outer type without
// retaining or copying its contents. The generated model performs the one
// materializing decode after this presence check succeeds.
type graphJSONObjectPresence struct {
	name string
}

func (presence *graphJSONObjectPresence) UnmarshalJSON(encoded []byte) error {
	trimmed := bytes.TrimSpace(encoded)
	if len(trimmed) != 0 && trimmed[0] == '{' {
		return nil
	}
	return errors.New(presence.name + " must be omitted or an object")
}
