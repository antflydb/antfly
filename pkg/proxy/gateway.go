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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type Gateway struct {
	router        *Router
	authenticator Authenticator
	authorizer    Authorizer
	forwarder     BackendForwarder
}

type ResolvedTarget struct {
	Tenant           string      `json:"tenant"`
	Table            string      `json:"table"`
	ServingNamespace string      `json:"serving_namespace,omitempty"`
	Backend          BackendKind `json:"backend"`
	TargetURL        string      `json:"target_url"`
	View             string      `json:"view"`
	RequireGraph     bool        `json:"require_graph"`
}

type GatewayConfig struct {
	Router        *Router
	Authenticator Authenticator
	Authorizer    Authorizer
	Forwarder     BackendForwarder
}

func NewGateway(router *Router) *Gateway {
	return NewGatewayFromConfig(GatewayConfig{Router: router})
}

func NewGatewayFromConfig(cfg GatewayConfig) *Gateway {
	g := &Gateway{
		router:        cfg.Router,
		authenticator: cfg.Authenticator,
		authorizer:    cfg.Authorizer,
		forwarder:     cfg.Forwarder,
	}
	if g.authenticator == nil {
		g.authenticator = StaticBearerAuthenticator{}
	}
	if g.authorizer == nil {
		g.authorizer = TenantAuthorizer{}
	}
	if g.forwarder == nil {
		g.forwarder = HTTPBackendForwarder{}
	}
	return g
}

func (g *Gateway) Resolve(ctx context.Context, req RequestContext) (*ResolvedTarget, error) {
	kind, adapter, route, err := g.router.ResolveBackend(ctx, req)
	if err != nil {
		return nil, err
	}
	baseURL, err := adapter.BaseURL(req, route)
	if err != nil {
		return nil, err
	}
	return &ResolvedTarget{
		Tenant:           req.Tenant,
		Table:            route.TableName(),
		ServingNamespace: route.ServingNamespace(),
		Backend:          kind,
		TargetURL:        baseURL,
		View:             NormalizePolicy(req.Policy).View,
		RequireGraph:     req.RequireGraph,
	}, nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/resolve":
		g.handleResolve(w, r)
	case strings.HasPrefix(r.URL.Path, "/proxy"), strings.HasPrefix(r.URL.Path, "/v1/tenants/"):
		g.handleProxy(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (g *Gateway) handleResolve(w http.ResponseWriter, r *http.Request) {
	req, err := requestContextFromHTTP(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resolved, err := g.Resolve(r.Context(), req)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "no route configured") {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resolved); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (g *Gateway) handleProxy(w http.ResponseWriter, r *http.Request) {
	req, err := requestContextFromHTTP(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	principal, err := g.authenticator.Authenticate(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	kind, adapter, route, err := g.router.ResolveBackend(r.Context(), req)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "no route configured") {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	req.PreferredBackend = kind

	if err := g.authorizer.Authorize(principal, req, route); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	if req.Operation == OperationRead && len(principal.RowFilter) > 0 {
		table := firstNonEmpty(req.Table, route.TableName())
		if secFilter := resolveRowFilter(principal.RowFilter, table); secFilter != nil {
			var injectErr error
			if isAgentPath(req.BackendPath) {
				body, readErr := io.ReadAll(r.Body)
				if readErr != nil {
					http.Error(w, "failed to read request body", http.StatusInternalServerError)
					return
				}
				r.Body.Close()
				modified, modErr := injectFilterIntoAgentBody(body, secFilter)
				if modErr != nil {
					injectErr = modErr
				} else {
					r.Body = io.NopCloser(bytes.NewReader(modified))
					r.ContentLength = int64(len(modified))
				}
			} else {
				injectErr = injectRowFilterIntoRequest(r, secFilter)
			}
			if injectErr != nil {
				http.Error(w, "failed to inject row filter", http.StatusInternalServerError)
				return
			}
		}
	}

	targetBaseURL, err := adapter.BaseURL(req, route)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetBaseURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	outReq.Header = r.Header.Clone()
	outReq.URL.RawQuery = r.URL.RawQuery
	if err := adapter.RewriteRequest(outReq, r, req, route); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	if err := g.forwarder.Forward(w, outReq, targetBaseURL, adapter, req, route); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
}

func requestContextFromHTTP(r *http.Request) (RequestContext, error) {
	requiredVersion, err := parseOptionalUint64(firstNonEmpty(r.Header.Get("X-Antfly-Required-Version"), r.URL.Query().Get("required_version")))
	if err != nil {
		return RequestContext{}, fmt.Errorf("invalid required_version: %w", err)
	}
	maxLagRecords, err := parseOptionalUint64Value(firstNonEmpty(r.Header.Get("X-Antfly-Max-Lag-Records"), r.URL.Query().Get("max_lag_records")))
	if err != nil {
		return RequestContext{}, fmt.Errorf("invalid max_lag_records: %w", err)
	}

	tenantFromPath, tableFromPath, backendPath := parsePublicAPIPath(r.URL.Path)
	req := RequestContext{
		Tenant:       firstNonEmpty(tenantFromPath, r.Header.Get("X-Antfly-Tenant"), r.URL.Query().Get("tenant")),
		Table:        firstNonEmpty(tableFromPath, r.Header.Get("X-Antfly-Table"), r.URL.Query().Get("table")),
		Namespace:    firstNonEmpty(r.Header.Get("X-Antfly-Namespace"), r.URL.Query().Get("namespace")),
		RequireGraph: r.URL.Query().Get("graph") == "1" || strings.EqualFold(r.URL.Query().Get("graph"), "true"),
		BackendPath:  backendPath,
		Policy: RequestPolicy{
			View:            firstNonEmpty(r.Header.Get("X-Antfly-View"), r.URL.Query().Get("view")),
			MaxLagRecords:   maxLagRecords,
			RequiredVersion: requiredVersion,
		},
	}

	if req.BackendPath == "" && strings.HasPrefix(r.URL.Path, "/proxy") {
		req.BackendPath = normalizeProxySuffix(r.URL.Path)
	}
	if classifyResponseKind(req.BackendPath) == responseKindGraphNeighbors ||
		classifyResponseKind(req.BackendPath) == responseKindGraphTraverse ||
		classifyResponseKind(req.BackendPath) == responseKindGraphShortestPath {
		req.RequireGraph = true
	}

	operationHint := firstNonEmpty(r.Header.Get("X-Antfly-Operation"), r.URL.Query().Get("operation"))
	switch strings.ToLower(operationHint) {
	case "", "read":
		if inferred := inferOperationFromBackendPath(req.BackendPath); inferred != "" {
			req.Operation = inferred
		} else if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch || r.Method == http.MethodDelete {
			req.Operation = OperationWrite
		} else {
			req.Operation = OperationRead
		}
	case "write":
		req.Operation = OperationWrite
	case "admin":
		req.Operation = OperationAdmin
	default:
		return RequestContext{}, fmt.Errorf("unsupported operation %q", firstNonEmpty(r.Header.Get("X-Antfly-Operation"), r.URL.Query().Get("operation")))
	}

	if req.Tenant == "" || req.ResourceName() == "" {
		return RequestContext{}, fmt.Errorf("tenant and table are required")
	}
	return req, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parsePublicAPIPath(path string) (tenant string, table string, backendPath string) {
	if !strings.HasPrefix(path, "/v1/tenants/") {
		return "", "", ""
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) < 5 || parts[0] != "v1" || parts[1] != "tenants" || parts[3] != "tables" {
		return "", "", ""
	}
	tenant = strings.TrimSpace(parts[2])
	table = strings.TrimSpace(parts[4])
	suffix := "/"
	if len(parts) > 5 {
		suffix = "/" + strings.Join(parts[5:], "/")
	}
	return tenant, table, canonicalPublicBackendPath(suffix)
}

func inferOperationFromBackendPath(path string) OperationKind {
	switch {
	case strings.HasPrefix(path, "/query"), strings.HasPrefix(path, "/graph"), strings.HasPrefix(path, "/versions/"):
		return OperationRead
	case strings.HasPrefix(path, "/admin"):
		return OperationAdmin
	default:
		return ""
	}
}

func parseOptionalUint64(raw string) (*uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func parseOptionalUint64Value(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseUint(raw, 10, 64)
}

func canonicalPublicBackendPath(path string) string {
	path = strings.TrimSpace(path)
	switch {
	case path == "", path == "/":
		return "/"
	case strings.HasPrefix(path, "/query/"), path == "/query":
		return path
	case strings.HasPrefix(path, "/search"), path == "/search":
		return "/query" + path
	case strings.HasPrefix(path, "/graph/"), path == "/graph":
		return path
	case strings.HasPrefix(path, "/versions/"):
		parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
		if len(parts) >= 2 && parts[0] == "versions" {
			versionSuffix := "/"
			if len(parts) > 2 {
				versionSuffix = "/" + strings.Join(parts[2:], "/")
			}
			switch {
			case strings.HasPrefix(versionSuffix, "/query/"), versionSuffix == "/query":
				return path
			case strings.HasPrefix(versionSuffix, "/search"), versionSuffix == "/search":
				return "/versions/" + parts[1] + "/query" + versionSuffix
			case strings.HasPrefix(versionSuffix, "/graph/"), versionSuffix == "/graph":
				return path
			}
		}
	}
	return path
}
