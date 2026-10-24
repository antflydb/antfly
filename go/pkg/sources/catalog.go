// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import "encoding/json"

// Catalog describes search bindings, not credentials or authority. An endpoint
// is an ordinary Antfly API base URL. Hosted adapters may use an origin-pinned
// relative reference; standalone catalogs use an absolute endpoint.
type Catalog struct {
	Version    int       `json:"version"`
	Endpoint   string    `json:"endpoint"`
	Sources    []Binding `json:"sources"`
	Principals []string  `json:"principals"`
}
type Binding struct {
	PublishedRevision string          `json:"published_revision,omitempty"`
	ID                string          `json:"id"`
	Alias             string          `json:"alias"`
	Table             string          `json:"table"`
	DocumentField     string          `json:"document_field"`
	Filter            json.RawMessage `json:"filter"`
	Status            string          `json:"status"`
	AccessID          string          `json:"access_id,omitempty"`
	ValidUntil        *int64          `json:"valid_until,omitempty"`
}

func SourceFilter(id string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"conjuncts": []any{map[string]any{"equals": id, "field": "source_id"}, map[string]any{"equals": "source_item", "field": "doc_type"}}})
	return raw
}
