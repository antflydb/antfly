// Copyright 2025 Antfly, Inc.
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

package ner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSchemaString(t *testing.T) {
	tests := []struct {
		name    string
		schema  map[string][]string
		want    []ExtractionSchema
		wantErr bool
	}{
		{
			name: "basic str fields",
			schema: map[string][]string{
				"person": {"name::str", "age::str"},
			},
			want: []ExtractionSchema{
				{
					Name: "person",
					Fields: []SchemaField{
						{Name: "name", Type: FieldTypeStr},
						{Name: "age", Type: FieldTypeStr},
					},
				},
			},
		},
		{
			name: "mixed str and list",
			schema: map[string][]string{
				"person": {"name::str", "skills::list"},
			},
			want: []ExtractionSchema{
				{
					Name: "person",
					Fields: []SchemaField{
						{Name: "name", Type: FieldTypeStr},
						{Name: "skills", Type: FieldTypeList},
					},
				},
			},
		},
		{
			name: "default type is str",
			schema: map[string][]string{
				"item": {"name"},
			},
			want: []ExtractionSchema{
				{
					Name: "item",
					Fields: []SchemaField{
						{Name: "name", Type: FieldTypeStr},
					},
				},
			},
		},
		{
			name: "choice field",
			schema: map[string][]string{
				"person": {"role::[engineer|manager]"},
			},
			want: []ExtractionSchema{
				{
					Name: "person",
					Fields: []SchemaField{
						{Name: "role", Type: FieldTypeStr, Choices: []string{"engineer", "manager"}},
					},
				},
			},
		},
		{
			name: "choice field with explicit type",
			schema: map[string][]string{
				"person": {"role::[engineer|manager]::str"},
			},
			want: []ExtractionSchema{
				{
					Name: "person",
					Fields: []SchemaField{
						{Name: "role", Type: FieldTypeStr, Choices: []string{"engineer", "manager"}},
					},
				},
			},
		},
		{
			name:    "empty structure name",
			schema:  map[string][]string{"": {"name::str"}},
			wantErr: true,
		},
		{
			name:    "empty fields",
			schema:  map[string][]string{"person": {}},
			wantErr: true,
		},
		{
			name: "unknown suffix becomes part of field name",
			schema: map[string][]string{
				"person": {"name::invalid"},
			},
			want: []ExtractionSchema{
				{
					Name: "person",
					Fields: []SchemaField{
						{Name: "name::invalid", Type: FieldTypeStr},
					},
				},
			},
		},
		{
			name:    "empty field def",
			schema:  map[string][]string{"person": {""}},
			wantErr: true,
		},
		{
			name:    "empty choice option",
			schema:  map[string][]string{"person": {"role::[a|]"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSchemaString(tt.schema)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))

			// Since map iteration order is non-deterministic, match by name
			for _, wantSchema := range tt.want {
				found := false
				for _, gotSchema := range got {
					if gotSchema.Name == wantSchema.Name {
						found = true
						assert.Equal(t, wantSchema.Fields, gotSchema.Fields, "fields mismatch for schema %q", wantSchema.Name)
						break
					}
				}
				assert.True(t, found, "schema %q not found in result", wantSchema.Name)
			}
		})
	}
}

func TestParseFieldDef(t *testing.T) {
	tests := []struct {
		def     string
		want    SchemaField
		wantErr bool
	}{
		{def: "name::str", want: SchemaField{Name: "name", Type: FieldTypeStr}},
		{def: "skills::list", want: SchemaField{Name: "skills", Type: FieldTypeList}},
		{def: "name", want: SchemaField{Name: "name", Type: FieldTypeStr}},
		{def: "role::[a|b|c]", want: SchemaField{Name: "role", Type: FieldTypeStr, Choices: []string{"a", "b", "c"}}},
		{def: "role::[a|]", wantErr: true},
		{def: "role::[|b]", wantErr: true},
		{def: "", wantErr: true},
		{def: "::str", wantErr: true},
		// Right-to-left parsing: field names containing "::"
		{def: "person::name::str", want: SchemaField{Name: "person::name", Type: FieldTypeStr}},
		{def: "person::name", want: SchemaField{Name: "person::name", Type: FieldTypeStr}},
		{def: "a::b::[x|y]", want: SchemaField{Name: "a::b", Type: FieldTypeStr, Choices: []string{"x", "y"}}},
	}

	for _, tt := range tests {
		t.Run(tt.def, func(t *testing.T) {
			got, err := parseFieldDef(tt.def)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
