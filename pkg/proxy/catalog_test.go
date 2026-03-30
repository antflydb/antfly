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
