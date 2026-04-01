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
	"strings"
)

type OperationKind string

const (
	OperationRead  OperationKind = "read"
	OperationWrite OperationKind = "write"
	OperationAdmin OperationKind = "admin"
)

type RequestContext struct {
	Tenant           string
	Table            string
	Namespace        string
	Operation        OperationKind
	Policy           RequestPolicy
	BackendPath      string
	RequireGraph     bool
	AllowLatest      bool
	PreferredBackend BackendKind
}

type Router struct {
	catalog Catalog
}

func NewRouter(routes []NamespaceRoute) *Router {
	return NewRouterWithCatalog(NewStaticCatalog(routes))
}

func NewRouterWithCatalog(catalog Catalog) *Router {
	return &Router{catalog: catalog}
}

func (r *Router) Resolve(ctx context.Context, req RequestContext) (BackendKind, error) {
	kind, _, _, err := r.ResolveBackend(ctx, req)
	return kind, err
}

func (r *Router) ResolveBackend(ctx context.Context, req RequestContext) (BackendKind, BackendAdapter, NamespaceRoute, error) {
	if err := ValidatePolicy(req); err != nil {
		return "", nil, NamespaceRoute{}, err
	}
	req.Policy = NormalizePolicy(req.Policy)
	if req.Policy.View == ViewLatest {
		req.AllowLatest = true
	}

	resource := req.ResourceName()
	route, err := r.catalog.ResolveRoute(ctx, req.Tenant, resource)
	if err != nil {
		return "", nil, NamespaceRoute{}, err
	}

	resolve := func(kind BackendKind) (BackendKind, BackendAdapter, NamespaceRoute, error) {
		adapter := adapterFor(kind)
		if adapter == nil {
			return "", nil, NamespaceRoute{}, fmt.Errorf("no adapter configured for backend=%q", kind)
		}
		if _, err := adapter.BaseURL(req, route); err != nil {
			return "", nil, NamespaceRoute{}, err
		}
		if !adapter.CanServe(req, route) {
			return "", nil, NamespaceRoute{}, fmt.Errorf("backend %q cannot serve tenant=%q table=%q request", kind, req.Tenant, route.TableName())
		}
		return kind, adapter, route, nil
	}

	if req.Operation == OperationWrite {
		if route.AllowStateful {
			return resolve(BackendStateful)
		}
		if route.AllowServerless {
			return resolve(BackendServerless)
		}
		return "", nil, NamespaceRoute{}, fmt.Errorf("table %q has no writable backend", route.TableName())
	}

	if req.PreferredBackend != "" {
		if req.PreferredBackend == BackendServerless && route.AllowServerless {
			return resolve(BackendServerless)
		}
		if req.PreferredBackend == BackendStateful && route.AllowStateful {
			return resolve(BackendStateful)
		}
	}

	if req.RequireGraph || req.AllowLatest {
		if route.AllowServerless {
			return resolve(BackendServerless)
		}
	}

	if route.PreferredBackend != "" {
		if route.PreferredBackend == BackendServerless && route.AllowServerless {
			return resolve(BackendServerless)
		}
		if route.PreferredBackend == BackendStateful && route.AllowStateful {
			return resolve(BackendStateful)
		}
	}

	if route.AllowServerless {
		return resolve(BackendServerless)
	}
	if route.AllowStateful {
		return resolve(BackendStateful)
	}
	return "", nil, NamespaceRoute{}, fmt.Errorf("table %q has no usable backend", route.TableName())
}

func (req RequestContext) ResourceName() string {
	return firstNonEmpty(req.Table, req.Namespace)
}

func (route NamespaceRoute) TableName() string {
	return firstNonEmpty(route.Table, route.Namespace)
}

func (route NamespaceRoute) ServingNamespace() string {
	return firstNonEmpty(route.Namespace, route.Table)
}

func routeKey(tenant, resource string) string {
	return strings.TrimSpace(tenant) + "/" + strings.TrimSpace(resource)
}
