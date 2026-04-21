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
