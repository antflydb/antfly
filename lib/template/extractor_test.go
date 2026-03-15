// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package template

import (
	"reflect"
	"strings"
	"testing"
)

func TestExtractFieldPaths(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     [][]string
		wantErr  bool
	}{
		{
			name:     "simple field",
			template: "{{title}}",
			want:     [][]string{{"title"}},
			wantErr:  false,
		},
		{
			name:     "nested field",
			template: "{{user.profile.avatar}}",
			want:     [][]string{{"user", "profile", "avatar"}},
			wantErr:  false,
		},
		{
			name:     "multiple fields",
			template: "{{title}} {{author.name}}",
			want:     [][]string{{"title"}, {"author", "name"}},
			wantErr:  false,
		},
		{
			name:     "duplicate fields",
			template: "{{title}} and {{title}} again",
			want:     [][]string{{"title"}},
			wantErr:  false,
		},
		{
			name:     "each loop",
			template: "{{#each items}}{{name}} {{price}}{{/each}}",
			want:     [][]string{{"items"}, {"name"}, {"price"}},
			wantErr:  false,
		},
		{
			name:     "nested each loops",
			template: "{{#each users}}{{name}} {{#each posts}}{{title}}{{/each}}{{/each}}",
			want:     [][]string{{"users"}, {"name"}, {"posts"}, {"title"}},
			wantErr:  false,
		},
		{
			name:     "if conditional",
			template: "{{#if user.active}}{{user.name}}{{/if}}",
			want:     [][]string{{"user", "active"}, {"user", "name"}},
			wantErr:  false,
		},
		{
			name:     "if-else conditional",
			template: "{{#if isActive}}{{activeText}}{{else}}{{inactiveText}}{{/if}}",
			want:     [][]string{{"isActive"}, {"activeText"}, {"inactiveText"}},
			wantErr:  false,
		},
		{
			name:     "media helper",
			template: "{{media url=author.avatar}}",
			want:     [][]string{{"author", "avatar"}},
			wantErr:  false,
		},
		{
			name:     "media helper with this",
			template: "{{media url=this}}",
			want:     [][]string{},
			wantErr:  false,
		},
		{
			name:     "multiple media helpers",
			template: "{{media url=thumbnail}} and {{media url=author.photo}}",
			want:     [][]string{{"thumbnail"}, {"author", "photo"}},
			wantErr:  false,
		},
		{
			name:     "with helper",
			template: "{{#with author}}{{name}} {{email}}{{/with}}",
			want:     [][]string{{"author"}, {"name"}, {"email"}},
			wantErr:  false,
		},
		{
			name:     "data variables (should be skipped)",
			template: "{{@root.title}} {{@index}} {{@key}}",
			want:     [][]string{},
			wantErr:  false,
		},
		{
			name:     "mixed data and regular variables",
			template: "{{title}} {{@root.foo}} {{author.name}}",
			want:     [][]string{{"title"}, {"author", "name"}},
			wantErr:  false,
		},
		{
			name:     "parent paths (..)",
			template: "{{#each items}}{{../title}}{{/each}}",
			want:     [][]string{{"items"}, {"title"}},
			wantErr:  false,
		},
		{
			name:     "literals should be ignored",
			template: `{{helper "string literal" 123 true}}`,
			want:     [][]string{{"helper"}},
			wantErr:  false,
		},
		{
			name:     "subexpressions",
			template: "{{outer (inner value)}}",
			want:     [][]string{{"outer"}, {"inner"}, {"value"}},
			wantErr:  false,
		},
		{
			name: "complex real-world template",
			template: `
				<h1>{{title}}</h1>
				<div>
					{{#if author.avatar}}
						{{media url=author.avatar}}
					{{/if}}
					<p>By {{author.name}}</p>
				</div>
				<div>
					{{#each tags}}
						<span>{{this}}</span>
					{{/each}}
				</div>
			`,
			want:    [][]string{{"title"}, {"author", "avatar"}, {"author", "name"}, {"tags"}},
			wantErr: false,
		},
		{
			name:     "empty template",
			template: "",
			want:     [][]string{},
			wantErr:  false,
		},
		{
			name:     "only literals",
			template: "Hello, World!",
			want:     [][]string{},
			wantErr:  false,
		},
		{
			name:     "comments should be ignored",
			template: "{{! this is a comment with {{fake}} }} {{realField}}",
			want:     [][]string{{"realField"}},
			wantErr:  false,
		},
		{
			name:     "invalid template",
			template: "{{#if unclosed",
			want:     nil,
			wantErr:  true,
		},
		{
			name:     "hash parameters",
			template: "{{helper key1=value1 key2=value2.nested}}",
			want:     [][]string{{"helper"}, {"value1"}, {"value2", "nested"}},
			wantErr:  false,
		},
		{
			name:     "partial statement with literal name",
			template: "{{> myPartial param=value}}",
			want:     [][]string{{"value"}},
			wantErr:  false,
		},
		{
			name:     "partial with string literal name",
			template: `{{> "foo"}} baz`,
			want:     [][]string{},
			wantErr:  false,
		},
		{
			name:     "partial with dynamic name from field",
			template: "{{> (lookup . partialName)}}",
			want:     [][]string{{"partialName"}},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractFieldPaths(tt.template)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractFieldPaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Sort both slices for consistent comparison
				if !equalFieldPaths(got, tt.want) {
					t.Errorf("ExtractFieldPaths() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestIsFieldReferenced(t *testing.T) {
	tests := []struct {
		name      string
		template  string
		fieldPath []string
		want      bool
		wantErr   bool
	}{
		{
			name:      "simple field present",
			template:  "{{title}}",
			fieldPath: []string{"title"},
			want:      true,
			wantErr:   false,
		},
		{
			name:      "simple field absent",
			template:  "{{title}}",
			fieldPath: []string{"description"},
			want:      false,
			wantErr:   false,
		},
		{
			name:      "nested field present",
			template:  "{{author.name}} {{author.email}}",
			fieldPath: []string{"author", "name"},
			want:      true,
			wantErr:   false,
		},
		{
			name:      "nested field absent",
			template:  "{{author.name}}",
			fieldPath: []string{"author", "email"},
			want:      false,
			wantErr:   false,
		},
		{
			name:      "partial match should be false",
			template:  "{{author.name}}",
			fieldPath: []string{"author"},
			want:      false,
			wantErr:   false,
		},
		{
			name:      "field in conditional",
			template:  "{{#if user.active}}Active{{/if}}",
			fieldPath: []string{"user", "active"},
			want:      true,
			wantErr:   false,
		},
		{
			name:      "field in loop",
			template:  "{{#each items}}{{name}}{{/each}}",
			fieldPath: []string{"items"},
			want:      true,
			wantErr:   false,
		},
		{
			name:      "field in media helper",
			template:  "{{media url=thumbnail}}",
			fieldPath: []string{"thumbnail"},
			want:      true,
			wantErr:   false,
		},
		{
			name:      "invalid template",
			template:  "{{#if unclosed",
			fieldPath: []string{"any"},
			want:      false,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IsFieldReferenced(tt.template, tt.fieldPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsFieldReferenced() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IsFieldReferenced() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeFieldPath(t *testing.T) {
	tests := []struct {
		name string
		path []string
		want []string
	}{
		{
			name: "remove this prefix",
			path: []string{"this", "author", "name"},
			want: []string{"author", "name"},
		},
		{
			name: "remove fields prefix",
			path: []string{"fields", "title"},
			want: []string{"title"},
		},
		{
			name: "remove both this and fields",
			path: []string{"this", "fields", "value"},
			want: []string{"value"},
		},
		{
			name: "no prefix to remove",
			path: []string{"author", "name"},
			want: []string{"author", "name"},
		},
		{
			name: "only this",
			path: []string{"this"},
			want: []string{},
		},
		{
			name: "only fields",
			path: []string{"fields"},
			want: []string{},
		},
		{
			name: "empty path",
			path: []string{},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeFieldPath(tt.path)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NormalizeFieldPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Helper function to compare field paths (order-independent)
func equalFieldPaths(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}

	// Create maps for comparison
	aMap := make(map[string]bool)
	bMap := make(map[string]bool)

	for _, path := range a {
		key := ""
		var keySb356 strings.Builder
		for i, part := range path {
			if i > 0 {
				keySb356.WriteString(".")
			}
			keySb356.WriteString(part)
		}
		key += keySb356.String()
		aMap[key] = true
	}

	for _, path := range b {
		key := ""
		var keySb367 strings.Builder
		for i, part := range path {
			if i > 0 {
				keySb367.WriteString(".")
			}
			keySb367.WriteString(part)
		}
		key += keySb367.String()
		bMap[key] = true
	}

	// Check if maps are equal
	if len(aMap) != len(bMap) {
		return false
	}

	for key := range aMap {
		if !bMap[key] {
			return false
		}
	}

	return true
}
