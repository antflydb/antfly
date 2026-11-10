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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources"
)

func TestLoadGeneratedKnowledgeProfilePreservesNativeNumbers(t *testing.T) {
	plan, knowledge, _, err := sources.BuildKnowledgeProcessing(sources.KnowledgePreset{
		Name: "facts", SourceID: "11111111-1111-4111-8111-111111111111", Audience: "research",
		Field: "facts", EntityTable: "entities",
		Resolver: map[string]any{"config_generation": json.Number("9007199254740993")},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := sources.Profile{Version: 1, Sources: []sources.Source{{Processing: &plan, Knowledge: &knowledge}}}
	data, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadProfile(path)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(data) {
		t.Fatalf("profile changed native values during round trip:\n%s\n%s", data, actual)
	}
	if err := sources.ValidateProcessing(loaded.Sources[0].Processing); err != nil {
		t.Fatalf("generated native recipe no longer validates: %v", err)
	}
}
