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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type BackendForwarder interface {
	Forward(w http.ResponseWriter, outReq *http.Request, targetBaseURL string, adapter BackendAdapter, req RequestContext, route NamespaceRoute) error
}

type HTTPBackendForwarder struct {
	Client *http.Client
}

func (f HTTPBackendForwarder) Forward(w http.ResponseWriter, outReq *http.Request, targetBaseURL string, adapter BackendAdapter, req RequestContext, route NamespaceRoute) error {
	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}

	baseURL, err := url.Parse(targetBaseURL)
	if err != nil {
		return err
	}

	target := *baseURL
	target.Path = joinPaths(baseURL.Path, outReq.URL.Path)
	target.RawQuery = outReq.URL.RawQuery
	outReq.URL = &target

	resp, err := client.Do(outReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if adapter != nil {
		adapter.NormalizeResponse(resp, req, route)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return writeNormalizedError(w, resp, req)
	}
	if normalizedBody, normalized, err := normalizeSuccessfulResponse(resp, req); err != nil {
		return err
	} else if normalized {
		copyHeader(w.Header(), resp.Header)
		w.Header().Del("Content-Length")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, err = w.Write(normalizedBody)
		return err
	}
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	return err
}

func joinPaths(base, suffix string) string {
	if base == "" {
		base = "/"
	}
	if suffix == "" {
		return base
	}
	if strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}
	if !strings.HasPrefix(suffix, "/") {
		suffix = "/" + suffix
	}
	return base + suffix
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		dst[k] = append([]string(nil), vv...)
	}
}

type normalizedBackendError struct {
	Error   string `json:"error"`
	Status  int    `json:"status"`
	Backend string `json:"backend,omitempty"`
	Tenant  string `json:"tenant,omitempty"`
	Table   string `json:"table,omitempty"`
}

func writeNormalizedError(w http.ResponseWriter, resp *http.Response, req RequestContext) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	message := strings.TrimSpace(string(body))
	if message == "" {
		message = fmt.Sprintf("backend returned status %d", resp.StatusCode)
	}

	copyHeader(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)

	backend := resp.Header.Get("X-Antfly-Backend")
	payload := normalizedBackendError{
		Error:   message,
		Status:  resp.StatusCode,
		Backend: backend,
		Tenant:  req.Tenant,
		Table:   req.ResourceName(),
	}
	return json.NewEncoder(w).Encode(payload)
}
