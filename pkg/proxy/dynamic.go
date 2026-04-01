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
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type FileCatalog struct {
	path    string
	mu      sync.RWMutex
	modTime time.Time
	routes  map[string]NamespaceRoute
}

func NewFileCatalog(path string) *FileCatalog {
	return &FileCatalog{
		path:   path,
		routes: map[string]NamespaceRoute{},
	}
}

func (c *FileCatalog) ResolveRoute(ctx context.Context, tenant, resource string) (NamespaceRoute, error) {
	_ = ctx
	if err := c.reloadIfNeeded(); err != nil {
		return NamespaceRoute{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	route, ok := c.routes[routeKey(tenant, resource)]
	if !ok {
		return NamespaceRoute{}, fmt.Errorf("no route configured for tenant=%q resource=%q", tenant, resource)
	}
	return route, nil
}

func (c *FileCatalog) reloadIfNeeded() error {
	if strings.TrimSpace(c.path) == "" {
		return nil
	}
	info, err := os.Stat(c.path)
	if err != nil {
		return err
	}

	c.mu.RLock()
	unchanged := !info.ModTime().After(c.modTime)
	c.mu.RUnlock()
	if unchanged {
		return nil
	}

	data, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}
	routes, err := ParseRoutesJSON(string(data))
	if err != nil {
		return err
	}
	indexed := make(map[string]NamespaceRoute, len(routes)*2)
	for _, route := range routes {
		if table := route.TableName(); table != "" {
			indexed[routeKey(route.Tenant, table)] = route
		}
		if namespace := route.ServingNamespace(); namespace != "" {
			indexed[routeKey(route.Tenant, namespace)] = route
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.routes = indexed
	c.modTime = info.ModTime()
	return nil
}

type ReloadingBearerAuthenticator struct {
	Required     bool
	Path         string
	StaticTokens map[string]Principal

	mu      sync.RWMutex
	modTime time.Time
	tokens  map[string]Principal
}

func (a *ReloadingBearerAuthenticator) Authenticate(r *http.Request) (*Principal, error) {
	if err := a.reloadIfNeeded(); err != nil {
		return nil, err
	}

	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		if a.Required {
			return nil, fmt.Errorf("missing authorization header")
		}
		return &Principal{Subject: "anonymous"}, nil
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return nil, fmt.Errorf("unsupported authorization scheme")
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))

	a.mu.RLock()
	defer a.mu.RUnlock()
	principal, ok := a.tokens[token]
	if !ok {
		return nil, fmt.Errorf("invalid bearer token")
	}
	return &principal, nil
}

func (a *ReloadingBearerAuthenticator) reloadIfNeeded() error {
	if strings.TrimSpace(a.Path) == "" {
		a.mu.Lock()
		if a.tokens == nil {
			a.tokens = clonePrincipals(a.StaticTokens)
		}
		a.mu.Unlock()
		return nil
	}

	info, err := os.Stat(a.Path)
	if err != nil {
		return err
	}

	a.mu.RLock()
	unchanged := !info.ModTime().After(a.modTime)
	a.mu.RUnlock()
	if unchanged {
		return nil
	}

	data, err := os.ReadFile(a.Path)
	if err != nil {
		return err
	}
	tokens, err := parseBearerTokensJSON(string(data))
	if err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokens = clonePrincipals(tokens)
	a.modTime = info.ModTime()
	return nil
}

func clonePrincipals(in map[string]Principal) map[string]Principal {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]Principal, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
