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

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

func (b localManagement) QueryKnowledge(ctx context.Context, id string, input KnowledgeRequest) (json.RawMessage, error) {
	source, err := b.configuredSource(id)
	if err != nil {
		return nil, err
	}
	if err = ValidateKnowledge(source.Knowledge, b.s.knowledgeAudience); err != nil || source.Knowledge == nil {
		return nil, management.Failure(403, "knowledge_deployment_boundary_unavailable")
	}
	var catalog Catalog
	if err = loadJSON(filepath.Join(b.s.runnerPath(), "catalog.json"), &catalog, 32<<20); err != nil {
		return nil, management.Failure(409, "source_not_published")
	}
	for _, binding := range catalog.Sources {
		if binding.ID == id && binding.Table == source.Table && reflect.DeepEqual(binding.Knowledge, source.Knowledge) {
			if _, err := BuildKnowledgeQuery(binding, input, b.s.knowledgeAudience); err != nil {
				return nil, management.Failure(400, "invalid_knowledge_query")
			}
			raw, err := b.searchPublisher().QueryKnowledge(ctx, binding, input, b.s.knowledgeAudience)
			if err != nil {
				return nil, err
			}
			current, err := b.configuredSource(id)
			if err != nil {
				return nil, err
			}
			if !reflect.DeepEqual(current.Knowledge, source.Knowledge) || !reflect.DeepEqual(current.Permissions, source.Permissions) || current.Connection != source.Connection || current.Table != source.Table || current.Publication != source.Publication || !reflect.DeepEqual(current.Config, source.Config) {
				return nil, management.Failure(409, "knowledge_changed_during_query")
			}
			// Publication can retire a source independently of its desired config.
			var published Catalog
			if err = loadJSON(filepath.Join(b.s.runnerPath(), "catalog.json"), &published, 32<<20); err != nil {
				return nil, management.Failure(409, "source_not_published")
			}
			for _, currentBinding := range published.Sources {
				if currentBinding.ID == id && reflect.DeepEqual(currentBinding, binding) {
					return raw, nil
				}
			}
			return nil, management.Failure(409, "knowledge_changed_during_query")
		}
	}
	return nil, management.Failure(409, "knowledge_not_published")
}
