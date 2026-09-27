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

package proxy

import (
	"context"
	"testing"
)

func TestChainedCatalogFallsBackToStaticRoutes(t *testing.T) {
	catalog := NewChainedCatalog(
		NewStaticCatalog(nil),
		NewStaticCatalog([]NamespaceRoute{
			{
				Tenant:             "t1",
				Table:              "docs",
				Namespace:          "docs-serving",
				AllowServerless:    true,
				ServerlessQueryURL: "http://serverless-query",
				ServerlessAPIURL:   "http://serverless-api",
			},
		}),
	)

	route, err := catalog.ResolveRoute(context.Background(), "t1", "docs")
	if err != nil {
		t.Fatalf("unexpected resolve error: %v", err)
	}
	if route.ServerlessQueryURL != "http://serverless-query" || route.ServerlessAPIURL != "http://serverless-api" {
		t.Fatalf("got route %+v", route)
	}
}
