// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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

// DecodeInto decodes a canonical GraphQueryResult without copying it through
// an intermediate JSON map.
func (t GraphQueryResult) DecodeInto(value any) error {
	return json.Unmarshal(t.union, value)
}

// DecodeStrictInto decodes the selected canonical GraphResult shape while
// enforcing additionalProperties: false throughout typed objects. It reads
// directly from the retained union bytes and therefore does not copy large
// result arrays before decoding them.
func (t GraphResult) DecodeStrictInto(value any) error {
	return decodeStrict(t.union, value)
}

// DecodeStrictInto is the canonical GraphQueryResult equivalent.
func (t GraphQueryResult) DecodeStrictInto(value any) error {
	return decodeStrict(t.union, value)
}

func decodeStrict(encoded []byte, value any) error {
	if shape := canonicalGraphResultShape(value); shape != nil {
		if err := validateRequiredJSONShape(encoded, shape); err != nil {
			return err
		}
	}
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

// requiredJSONShape is a compact, streaming presence schema for canonical
// graph results. Generated Go structs intentionally expose required scalar
// fields as values rather than pointers, so encoding/json alone cannot tell an
// omitted zero-valued field from a present zero. Keeping this validator beside
// the generated union extensions preserves the ergonomic public structs while
// avoiding a RawMessage copy of large rows, paths, and hydrated documents.
type requiredJSONShape struct {
	name                 string
	object               bool
	array                bool
	nullable             bool
	nonEmptyString       bool
	required             []string
	properties           map[string]*requiredJSONShape
	additionalProperties *requiredJSONShape
	items                *requiredJSONShape
}

var (
	nonEmptyGraphStringShape = &requiredJSONShape{name: "non-empty string", nonEmptyString: true}
	graphPathEndpointShape   = &requiredJSONShape{
		name:     "GraphPathEndpoint",
		object:   true,
		required: []string{"key"},
		properties: map[string]*requiredJSONShape{
			"key":   nonEmptyGraphStringShape,
			"table": nonEmptyGraphStringShape,
		},
	}
	graphPathEdgeShape = &requiredJSONShape{
		name:     "GraphPathEdge",
		object:   true,
		required: []string{"from", "to", "type", "weight"},
		properties: map[string]*requiredJSONShape{
			"from": graphPathEndpointShape,
			"to":   graphPathEndpointShape,
			"type": nonEmptyGraphStringShape,
		},
	}
	graphResultNodeShape = &requiredJSONShape{
		name:     "GraphResultNode",
		object:   true,
		required: []string{"key", "depth"},
		properties: map[string]*requiredJSONShape{
			"key":        nonEmptyGraphStringShape,
			"table":      nonEmptyGraphStringShape,
			"path":       {name: "GraphResultNode.path", array: true, items: graphPathEndpointShape},
			"path_edges": {name: "GraphResultNode.path_edges", array: true, items: graphPathEdgeShape},
		},
	}
	graphPathShape = &requiredJSONShape{
		name:     "GraphPath",
		object:   true,
		required: []string{"nodes", "edges", "length", "weight_mode", "weight_sum", "objective_value"},
		properties: map[string]*requiredJSONShape{
			"nodes": {name: "GraphPath.nodes", array: true, items: graphPathEndpointShape},
			"edges": {name: "GraphPath.edges", array: true, items: graphPathEdgeShape},
		},
	}
	graphQueryStatsShape = &requiredJSONShape{
		name:     "GraphQueryStats",
		object:   true,
		required: []string{"returned_items", "truncated"},
	}
	graphBindingNodeShape = &requiredJSONShape{
		name:     "GraphBindingNode",
		object:   true,
		nullable: true,
		required: []string{"key"},
		properties: map[string]*requiredJSONShape{
			"key":   nonEmptyGraphStringShape,
			"table": nonEmptyGraphStringShape,
		},
	}
	graphResultRowShape = &requiredJSONShape{
		name:                 "GraphResultRow",
		object:               true,
		additionalProperties: graphBindingNodeShape,
	}
	graphAggregateValueShape = &requiredJSONShape{
		name:     "GraphAggregateValue",
		object:   true,
		required: []string{"value", "exact"},
	}
	graphBindingsResultShape = &requiredJSONShape{
		name:     "GraphBindingsResult",
		object:   true,
		required: []string{"kind", "rows", "stats"},
		properties: map[string]*requiredJSONShape{
			"rows":  {name: "GraphBindingsResult.rows", array: true, items: graphResultRowShape},
			"stats": graphQueryStatsShape,
		},
	}
	graphAggregatesResultShape = &requiredJSONShape{
		name:     "GraphAggregatesResult",
		object:   true,
		required: []string{"kind", "aggregates", "stats"},
		properties: map[string]*requiredJSONShape{
			"aggregates": {name: "GraphAggregatesResult.aggregates", object: true, additionalProperties: graphAggregateValueShape},
			"stats":      graphQueryStatsShape,
		},
	}
	graphNodesResultShape = &requiredJSONShape{
		name:     "GraphNodesResult",
		object:   true,
		required: []string{"kind", "nodes", "paths", "stats"},
		properties: map[string]*requiredJSONShape{
			"nodes": {name: "GraphNodesResult.nodes", array: true, items: graphResultNodeShape},
			"paths": {name: "GraphNodesResult.paths", array: true, items: graphPathShape},
			"stats": graphQueryStatsShape,
		},
	}
)

func canonicalGraphResultShape(value any) *requiredJSONShape {
	switch value.(type) {
	case *GraphBindingsResult:
		return graphBindingsResultShape
	case *GraphAggregatesResult:
		return graphAggregatesResultShape
	case *GraphNodesResult:
		return graphNodesResultShape
	default:
		return nil
	}
}

func validateRequiredJSONShape(encoded []byte, shape *requiredJSONShape) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := validateJSONShapeValue(decoder, shape); err != nil {
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

func validateJSONShapeValue(decoder *json.Decoder, shape *requiredJSONShape) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		if shape == nil || shape.nullable {
			return nil
		}
		return fmt.Errorf("%s must not be null", shape.name)
	}
	if shape == nil {
		return skipJSONTokenValue(decoder, token)
	}
	if shape.nonEmptyString {
		value, ok := token.(string)
		if !ok || value == "" {
			return fmt.Errorf("%s must be a non-empty string", shape.name)
		}
		return nil
	}

	delim, isDelim := token.(json.Delim)
	if shape.object {
		if !isDelim || delim != '{' {
			return fmt.Errorf("%s must be an object", shape.name)
		}
		var seen uint64
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%s contains a non-string property name", shape.name)
			}
			for i, required := range shape.required {
				if key == required {
					seen |= uint64(1) << i
					break
				}
			}
			child := shape.properties[key]
			if child == nil {
				child = shape.additionalProperties
			}
			if err := validateJSONShapeValue(decoder, child); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
		for i, required := range shape.required {
			if seen&(uint64(1)<<i) == 0 {
				return fmt.Errorf("%s requires field %q", shape.name, required)
			}
		}
		return nil
	}
	if shape.array {
		if !isDelim || delim != '[' {
			return fmt.Errorf("%s must be an array", shape.name)
		}
		for decoder.More() {
			if err := validateJSONShapeValue(decoder, shape.items); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	}
	return skipJSONTokenValue(decoder, token)
}

func skipJSONTokenValue(decoder *json.Decoder, token json.Token) error {
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		for decoder.More() {
			if _, err := decoder.Token(); err != nil {
				return err
			}
			value, err := decoder.Token()
			if err != nil {
				return err
			}
			if err := skipJSONTokenValue(decoder, value); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	case '[':
		for decoder.More() {
			value, err := decoder.Token()
			if err != nil {
				return err
			}
			if err := skipJSONTokenValue(decoder, value); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}
