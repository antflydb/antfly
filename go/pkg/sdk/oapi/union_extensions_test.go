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
	"reflect"
	"testing"
)

func TestDecodeNullableScalarShape(t *testing.T) {
	shape := &requiredJSONShape{
		name:     "OptionalLabel",
		nullable: true,
		reference: &requiredJSONShape{
			name:      "OptionalLabel",
			minLength: 1,
		},
	}
	wantLabel := "label"

	for _, tc := range []struct {
		name    string
		json    string
		want    *string
		wantErr bool
	}{
		{name: "value", json: `"label"`, want: &wantLabel},
		{name: "null", json: `null`},
		{name: "constraint failure", json: `""`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *string
			decoder := json.NewDecoder(bytes.NewReader([]byte(tc.json)))
			err := decodeJSONShapeValue(decoder, shape, reflect.ValueOf(&got).Elem())
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected nullable scalar constraint failure")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == nil {
				if got != nil {
					t.Fatalf("decoded %q, want nil", *got)
				}
				return
			}
			if got == nil || *got != *tc.want {
				t.Fatalf("decoded %v, want %q", got, *tc.want)
			}
		})
	}
}

func TestDecodeNullableShapeRequiresNullableDestination(t *testing.T) {
	shape := &requiredJSONShape{
		name:      "OptionalLabel",
		nullable:  true,
		reference: &requiredJSONShape{name: "OptionalLabel"},
	}
	var got string
	decoder := json.NewDecoder(bytes.NewReader([]byte(`null`)))
	if err := decodeJSONShapeValue(decoder, shape, reflect.ValueOf(&got).Elem()); err == nil {
		t.Fatal("expected non-nullable Go destination to reject JSON null")
	}
}
