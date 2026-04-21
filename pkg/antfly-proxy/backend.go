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
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type BackendAdapter interface {
	Kind() BackendKind
	BaseURL(req RequestContext, route NamespaceRoute) (string, error)
	CanServe(req RequestContext, route NamespaceRoute) bool
	RewriteRequest(outReq *http.Request, inbound *http.Request, req RequestContext, route NamespaceRoute) error
	NormalizeResponse(resp *http.Response, req RequestContext, route NamespaceRoute)
}

type StatefulBackendAdapter struct{}

func (StatefulBackendAdapter) Kind() BackendKind {
	return BackendStateful
}

func (StatefulBackendAdapter) BaseURL(req RequestContext, route NamespaceRoute) (string, error) {
	_ = req
	if strings.TrimSpace(route.StatefulURL) == "" {
		return "", fmt.Errorf("table %q has no stateful backend URL", route.TableName())
	}
	return route.StatefulURL, nil
}

func (StatefulBackendAdapter) CanServe(req RequestContext, route NamespaceRoute) bool {
	if !route.AllowStateful {
		return false
	}
	if req.RequireGraph || req.AllowLatest {
		return false
	}
	return true
}

func (StatefulBackendAdapter) RewriteRequest(outReq *http.Request, inbound *http.Request, req RequestContext, route NamespaceRoute) error {
	_ = route
	path := req.BackendPath
	if path == "" {
		path = normalizeProxySuffix(inbound.URL.Path)
	}
	outReq.URL.Path = path
	applyNormalizedPolicy(outReq, req, normalizedBackendQuery(inbound.URL.Query()))
	return nil
}

func (StatefulBackendAdapter) NormalizeResponse(resp *http.Response, req RequestContext, route NamespaceRoute) {
	_ = route
	resp.Header.Del("X-Antfly-Namespace")
	resp.Header.Set("X-Antfly-Backend", string(BackendStateful))
	resp.Header.Set("X-Antfly-Tenant", req.Tenant)
	resp.Header.Set("X-Antfly-Table", route.TableName())
	resp.Header.Set("X-Antfly-View", NormalizePolicy(req.Policy).View)
	if req.Policy.MaxLagRecords > 0 {
		resp.Header.Set("X-Antfly-Max-Lag-Records", strconv.FormatUint(req.Policy.MaxLagRecords, 10))
	}
	if req.Policy.RequiredVersion != nil {
		resp.Header.Set("X-Antfly-Required-Version", strconv.FormatUint(*req.Policy.RequiredVersion, 10))
	}
}

type ServerlessBackendAdapter struct{}

func (ServerlessBackendAdapter) Kind() BackendKind {
	return BackendServerless
}

func (ServerlessBackendAdapter) BaseURL(req RequestContext, route NamespaceRoute) (string, error) {
	if req.Operation == OperationWrite || req.Operation == OperationAdmin {
		if strings.TrimSpace(route.ServerlessAPIURL) == "" {
			return "", fmt.Errorf("table %q has no serverless api URL", route.TableName())
		}
		return route.ServerlessAPIURL, nil
	}
	if strings.TrimSpace(route.ServerlessQueryURL) == "" {
		return "", fmt.Errorf("table %q has no serverless query URL", route.TableName())
	}
	return route.ServerlessQueryURL, nil
}

func (ServerlessBackendAdapter) CanServe(req RequestContext, route NamespaceRoute) bool {
	return route.AllowServerless
}

func (ServerlessBackendAdapter) RewriteRequest(outReq *http.Request, inbound *http.Request, req RequestContext, route NamespaceRoute) error {
	table := url.PathEscape(route.TableName())
	ns := url.PathEscape(route.ServingNamespace())
	path := req.BackendPath
	if path == "" {
		path = normalizeProxySuffix(inbound.URL.Path)
	}
	path = canonicalPublicBackendPath(path)
	switch {
	case strings.HasPrefix(path, "/_internal/namespaces/"):
	case strings.HasPrefix(path, "/internal/namespaces/"):
		path = "/_internal" + strings.TrimPrefix(path, "/internal")
	case strings.HasPrefix(path, "/tables/"):
	case strings.HasPrefix(path, "/query/"), path == "/query":
		path = "/tables/" + table + path
	case strings.HasPrefix(path, "/graph/"), path == "/graph":
		path = "/tables/" + table + "/query" + path
	case strings.HasPrefix(path, "/versions/"):
		path = "/_internal/namespaces/" + ns + "/query" + path
	default:
		path = "/tables/" + table + path
	}
	outReq.URL.Path = path
	applyNormalizedPolicy(outReq, req, normalizedBackendQuery(inbound.URL.Query()))
	return nil
}

func (ServerlessBackendAdapter) NormalizeResponse(resp *http.Response, req RequestContext, route NamespaceRoute) {
	_ = route
	resp.Header.Del("X-Antfly-Namespace")
	resp.Header.Set("X-Antfly-Backend", string(BackendServerless))
	resp.Header.Set("X-Antfly-Tenant", req.Tenant)
	resp.Header.Set("X-Antfly-Table", route.TableName())
	resp.Header.Set("X-Antfly-View", NormalizePolicy(req.Policy).View)
	if req.Policy.MaxLagRecords > 0 {
		resp.Header.Set("X-Antfly-Max-Lag-Records", strconv.FormatUint(req.Policy.MaxLagRecords, 10))
	}
	if req.Policy.RequiredVersion != nil {
		resp.Header.Set("X-Antfly-Required-Version", strconv.FormatUint(*req.Policy.RequiredVersion, 10))
	}
}

func adapterFor(kind BackendKind) BackendAdapter {
	switch kind {
	case BackendStateful:
		return StatefulBackendAdapter{}
	case BackendServerless:
		return ServerlessBackendAdapter{}
	default:
		return nil
	}
}

func normalizeProxySuffix(path string) string {
	path = strings.TrimPrefix(path, "/proxy")
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func normalizedBackendQuery(values url.Values) url.Values {
	if values == nil {
		return nil
	}
	cloned := url.Values{}
	for key, vals := range values {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "tenant", "table", "namespace", "graph", "view", "operation", "max_lag_records", "required_version":
			continue
		default:
			cloned[key] = append([]string(nil), vals...)
		}
	}
	return cloned
}

func applyNormalizedPolicy(outReq *http.Request, req RequestContext, query url.Values) {
	policy := NormalizePolicy(req.Policy)
	if query == nil {
		query = url.Values{}
	}
	query.Set("view", policy.View)
	if policy.MaxLagRecords > 0 {
		query.Set("max_lag_records", strconv.FormatUint(policy.MaxLagRecords, 10))
	}
	if policy.RequiredVersion != nil {
		query.Set("required_version", strconv.FormatUint(*policy.RequiredVersion, 10))
	}
	outReq.URL.RawQuery = query.Encode()
	outReq.Header.Set("X-Antfly-Tenant", req.Tenant)
	if req.Table != "" {
		outReq.Header.Set("X-Antfly-Table", req.Table)
	}
	if req.Namespace != "" {
		outReq.Header.Set("X-Antfly-Namespace", req.Namespace)
	}
	outReq.Header.Set("X-Antfly-View", policy.View)
	if policy.MaxLagRecords > 0 {
		outReq.Header.Set("X-Antfly-Max-Lag-Records", strconv.FormatUint(policy.MaxLagRecords, 10))
	}
	if policy.RequiredVersion != nil {
		outReq.Header.Set("X-Antfly-Required-Version", strconv.FormatUint(*policy.RequiredVersion, 10))
	}
}
