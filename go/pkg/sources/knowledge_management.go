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
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

func (b localManagement) QueryKnowledge(ctx context.Context, id string, input KnowledgeRequest) (json.RawMessage, error) {
	snapshot, err := b.knowledgeSnapshot(id)
	if err != nil {
		return nil, err
	}
	source := snapshot.source
	if err = ValidateKnowledge(source.Knowledge, b.s.knowledgeAudience); err != nil || source.Knowledge == nil {
		return nil, management.Failure(403, "knowledge_deployment_boundary_unavailable")
	}
	var catalog Catalog
	if err = loadJSON(filepath.Join(b.s.runnerPath(), "catalog.json"), &catalog, 32<<20); err != nil {
		return nil, management.Failure(409, "source_not_published")
	}
	for _, binding := range catalog.Sources {
		if binding.ID == id && binding.Table == source.Table && reflect.DeepEqual(binding.Knowledge, source.Knowledge) {
			if binding.AcquisitionID == "" || binding.AcquisitionID != snapshot.acquisition || strings.TrimRight(catalog.Endpoint, "/") != snapshot.endpoint {
				return nil, management.Failure(409, "knowledge_acquisition_not_current")
			}
			if _, err := BuildKnowledgeQuery(binding, input, b.s.knowledgeAudience); err != nil {
				return nil, management.Failure(400, "invalid_knowledge_query")
			}
			publisher := &Publisher{Endpoint: snapshot.endpoint, Token: os.Getenv(snapshot.tokenEnv)}
			raw, err := publisher.QueryKnowledge(ctx, binding, input, b.s.knowledgeAudience)
			if err != nil {
				return nil, err
			}
			current, err := b.knowledgeSnapshot(id)
			if err != nil {
				return nil, err
			}
			if current.acquisition != snapshot.acquisition || current.endpoint != snapshot.endpoint || current.tokenEnv != snapshot.tokenEnv || !reflect.DeepEqual(current.source.Knowledge, source.Knowledge) {
				return nil, management.Failure(409, "knowledge_changed_during_query")
			}
			// Publication can retire a source independently of its desired config.
			var published Catalog
			if err = loadJSON(filepath.Join(b.s.runnerPath(), "catalog.json"), &published, 32<<20); err != nil {
				return nil, management.Failure(409, "source_not_published")
			}
			for _, currentBinding := range published.Sources {
				if strings.TrimRight(published.Endpoint, "/") == snapshot.endpoint && currentBinding.ID == id && reflect.DeepEqual(currentBinding, binding) {
					return raw, nil
				}
			}
			return nil, management.Failure(409, "knowledge_changed_during_query")
		}
	}
	return nil, management.Failure(409, "knowledge_not_published")
}

type knowledgeSnapshot struct {
	source                          Source
	endpoint, tokenEnv, acquisition string
}

// Capture source and connection together without loading the potentially large
// acquisition checkpoint or cloning unrelated sources on every graph query.
func (b localManagement) knowledgeSnapshot(id string) (knowledgeSnapshot, error) {
	b.s.mu.Lock()
	var profile Profile
	for _, source := range b.s.profile.Sources {
		if source.ID == id {
			connection, ok := b.s.profile.Connections[source.Connection]
			if !ok {
				b.s.mu.Unlock()
				return knowledgeSnapshot{}, management.Failure(409, "knowledge_acquisition_not_current")
			}
			profile = cloneProfile(Profile{Endpoint: b.s.profile.Endpoint, DataPlaneTokenEnv: b.s.profile.DataPlaneTokenEnv, Sources: []Source{source}, Connections: map[string]Connection{source.Connection: connection}})
			break
		}
	}
	b.s.mu.Unlock()
	if len(profile.Sources) == 0 {
		return knowledgeSnapshot{}, management.Failure(404, "source_not_found")
	}
	source := profile.Sources[0]
	identity, err := sourceAcquisitionIdentity(source, profile.Connections[source.Connection])
	return knowledgeSnapshot{source: source, endpoint: strings.TrimRight(profile.Endpoint, "/"), tokenEnv: profile.DataPlaneTokenEnv, acquisition: identity}, err
}
