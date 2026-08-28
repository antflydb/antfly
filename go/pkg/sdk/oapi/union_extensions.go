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
	"io"
)

// DecodeInto decodes the retained GraphResult JSON directly into value.
// It lets SDK adapters inspect a small discriminator probe without first
// marshaling and copying a potentially large result payload.
func (t GraphResult) DecodeInto(value any) error {
	return json.Unmarshal(t.union, value)
}

// DecodeInto decodes a canonical GraphQueryResult without copying it through
// an intermediate JSON map.
func (t GraphQueryResult) DecodeInto(value any) error {
	return json.Unmarshal(t.union, value)
}

// DecodeStrictInto decodes a GraphResult with the generated model while
// rejecting fields that are not part of that model. Semantic graph invariants
// are validated by the SDK after the concrete result variant is selected.
func (t GraphResult) DecodeStrictInto(value any) error {
	return decodeStrictJSON(t.union, value)
}

// DecodeStrictInto is the canonical GraphQueryResult equivalent.
func (t GraphQueryResult) DecodeStrictInto(value any) error {
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
