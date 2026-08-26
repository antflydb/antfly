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
