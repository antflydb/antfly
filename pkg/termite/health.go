// Copyright 2025 Antfly, Inc.
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

package termite

import (
	"net/http"

	json "github.com/antflydb/antfly/pkg/libaf/json"
)

// Version information - set at build time via ldflags
var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

// HealthResponse is the response for /healthz endpoint
type HealthResponse struct {
	Status string `json:"status"`
}

// ReadyResponse is the response for /readyz endpoint
type ReadyResponse struct {
	Status   string         `json:"status"`
	Models   ReadyModels    `json:"models"`
	Detailed map[string]any `json:"detailed,omitempty"`
}

// ReadyModels shows model availability
type ReadyModels struct {
	Embedders  int `json:"embedders"`
	Chunkers   int `json:"chunkers"`
	Rerankers  int `json:"rerankers"`
	Generators int `json:"generators"`
}

// handleHealthz returns 200 if the service is running (liveness check)
func (ln *TermiteNode) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(HealthResponse{Status: "ok"})
}

// handleReadyz returns 200 if the service is ready to accept requests (readiness check)
func (ln *TermiteNode) handleReadyz(w http.ResponseWriter, r *http.Request) {
	resp := ReadyResponse{
		Status: "ready",
		Models: ReadyModels{},
	}

	// Count available models (discovered, not necessarily loaded)
	if ln.embedderRegistry != nil {
		resp.Models.Embedders = len(ln.embedderRegistry.List())
	}
	if ln.chunker != nil {
		resp.Models.Chunkers = len(ln.chunker.ListModels())
	}
	if ln.rerankerRegistry != nil {
		resp.Models.Rerankers = len(ln.rerankerRegistry.List())
	}
	if ln.generatorRegistry != nil {
		resp.Models.Generators = len(ln.generatorRegistry.List())
	}

	// Service is ready if at least one model type is available
	// (chunker always has "fixed" built-in, so we're always ready)
	totalModels := resp.Models.Embedders + resp.Models.Chunkers + resp.Models.Rerankers + resp.Models.Generators
	if totalModels == 0 {
		resp.Status = "not_ready"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
