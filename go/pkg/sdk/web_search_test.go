// Copyright 2026 Antfly, Inc.
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

package sdk

import (
	"encoding/json"
	"testing"
)

func TestRetrievalExaProviderOptionsRoundTrip(t *testing.T) {
	raw := []byte(`{"enabled_tools":["web_search"],"web_search_config":{"provider":"exa","num_results":3,"include_domains":["antfly.io"],"start_published_date":"2026-01-01T00:00:00Z"}}`)
	var tools ChatToolsConfig
	if err := json.Unmarshal(raw, &tools); err != nil {
		t.Fatal(err)
	}
	exa, err := tools.WebSearchConfig.AsExaSearchConfig()
	if err != nil {
		t.Fatal(err)
	}
	if exa.NumResults != 3 || len(exa.IncludeDomains) != 1 || exa.IncludeDomains[0] != "antfly.io" {
		t.Fatalf("lost Exa options: %+v", exa)
	}
	var provider WebSearchProviderConfig
	if err := provider.FromExaSearchConfig(exa); err != nil {
		t.Fatal(err)
	}
	tools.WebSearchConfig = provider
	encoded, err := json.Marshal(tools)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip map[string]any
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	config := roundTrip["web_search_config"].(map[string]any)
	if config["provider"] != "exa" || config["num_results"] != float64(3) || config["start_published_date"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("unexpected wire config: %s", encoded)
	}
}
