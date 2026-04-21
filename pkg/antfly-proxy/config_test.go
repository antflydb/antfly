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

package proxy

import "testing"

func TestParseRoutesJSON(t *testing.T) {
	routes, err := ParseRoutesJSON(`[{"tenant":"t1","table":"docs","serving_namespace":"docs-serving","preferred_backend":"serverless","allow_stateful":true,"allow_serverless":true,"stateful_url":"http://stateful","serverless_query_url":"http://serverless-query","serverless_api_url":"http://serverless-api"}]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("got %d routes", len(routes))
	}
	if routes[0].PreferredBackend != BackendServerless {
		t.Fatalf("got backend %q", routes[0].PreferredBackend)
	}
	if routes[0].Table != "docs" || routes[0].Namespace != "docs-serving" {
		t.Fatalf("unexpected route mapping: %+v", routes[0])
	}
	if routes[0].ServerlessQueryURL != "http://serverless-query" || routes[0].ServerlessAPIURL != "http://serverless-api" {
		t.Fatalf("unexpected serverless URLs: %+v", routes[0])
	}
}
