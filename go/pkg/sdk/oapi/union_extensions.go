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
