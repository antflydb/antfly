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

type graphPathEndpointWire GraphPathEndpoint

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
