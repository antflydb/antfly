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
	"fmt"
)

type Catalog interface {
	ResolveRoute(ctx context.Context, tenant, resource string) (NamespaceRoute, error)
}

type StaticCatalog struct {
	routes map[string]NamespaceRoute
}

type ChainedCatalog struct {
	catalogs []Catalog
}

func NewStaticCatalog(routes []NamespaceRoute) *StaticCatalog {
	indexed := make(map[string]NamespaceRoute, len(routes)*2)
	for _, route := range routes {
		if table := route.TableName(); table != "" {
			indexed[routeKey(route.Tenant, table)] = route
		}
		if namespace := route.ServingNamespace(); namespace != "" {
			indexed[routeKey(route.Tenant, namespace)] = route
		}
	}
	return &StaticCatalog{routes: indexed}
}

func (c *StaticCatalog) ResolveRoute(ctx context.Context, tenant, resource string) (NamespaceRoute, error) {
	_ = ctx
	route, ok := c.routes[routeKey(tenant, resource)]
	if !ok {
		return NamespaceRoute{}, fmt.Errorf("no route configured for tenant=%q resource=%q", tenant, resource)
	}
	return route, nil
}

func NewChainedCatalog(catalogs ...Catalog) *ChainedCatalog {
	filtered := make([]Catalog, 0, len(catalogs))
	for _, catalog := range catalogs {
		if catalog != nil {
			filtered = append(filtered, catalog)
		}
	}
	return &ChainedCatalog{catalogs: filtered}
}

func (c *ChainedCatalog) ResolveRoute(ctx context.Context, tenant, resource string) (NamespaceRoute, error) {
	if len(c.catalogs) == 0 {
		return NamespaceRoute{}, fmt.Errorf("no route configured for tenant=%q resource=%q", tenant, resource)
	}

	var lastErr error
	for _, catalog := range c.catalogs {
		route, err := catalog.ResolveRoute(ctx, tenant, resource)
		if err == nil {
			return route, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return NamespaceRoute{}, lastErr
	}
	return NamespaceRoute{}, fmt.Errorf("no route configured for tenant=%q resource=%q", tenant, resource)
}
