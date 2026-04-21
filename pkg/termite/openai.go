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
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"go.uber.org/zap"
)

// OpenAI-compatible API at /openai/v1/*
//
// This provides a compatibility layer that allows standard OpenAI SDKs to work
// with Termite. The endpoints mirror OpenAI's API structure:
//
//   - POST /openai/v1/chat/completions - Chat completion (routes to handleApiGenerate)
//   - GET  /openai/v1/models           - List available models
//
// Usage with OpenAI SDK:
//
//	client := openai.NewClient(
//	    option.WithBaseURL("http://localhost:8080/openai/v1"),
//	    option.WithAPIKey("unused"), // Termite doesn't require auth
//	)

// OpenAI API response types
type openAIModel struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type openAIModelList struct {
	Object string        `json:"object"`
	Data   []openAIModel `json:"data"`
}

// RegisterOpenAIRoutes adds OpenAI-compatible endpoints to the given mux.
// These routes allow standard OpenAI SDKs to work with Termite by providing
// endpoints at /openai/v1/* that mirror OpenAI's API structure.
func (ln *TermiteNode) RegisterOpenAIRoutes(mux *http.ServeMux) {
	// POST /openai/v1/chat/completions - reuses the existing generate handler
	// which already accepts OpenAI-format requests (including content parts)
	mux.HandleFunc("POST /openai/v1/chat/completions", ln.handleOpenAIChatCompletions)

	// GET /openai/v1/models - returns models in OpenAI format
	mux.HandleFunc("GET /openai/v1/models", ln.handleOpenAIModels)
}

// handleOpenAIChatCompletions handles OpenAI-compatible chat completion requests.
// It delegates to handleApiGenerate which handles both content formats.
func (ln *TermiteNode) handleOpenAIChatCompletions(w http.ResponseWriter, r *http.Request) {
	ln.logger.Info("OpenAI chat completions request received",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("content-type", r.Header.Get("Content-Type")))

	ln.handleApiGenerate(w, r)
}

// handleOpenAIModels returns models in OpenAI-compatible format.
// This enables standard OpenAI SDKs to discover available models.
func (ln *TermiteNode) handleOpenAIModels(w http.ResponseWriter, r *http.Request) {
	resp := openAIModelList{
		Object: "list",
		Data:   []openAIModel{},
	}

	now := time.Now().Unix()

	// Add generators (primary use case for chat completions)
	if ln.generatorRegistry != nil {
		for _, name := range ln.generatorRegistry.List() {
			resp.Data = append(resp.Data, openAIModel{
				ID:      name,
				Object:  "model",
				Created: now,
				OwnedBy: "termite",
			})
		}
	}

	// Add embedders (for potential /v1/embeddings compatibility)
	if ln.embedderRegistry != nil {
		for _, name := range ln.embedderRegistry.List() {
			resp.Data = append(resp.Data, openAIModel{
				ID:      name,
				Object:  "model",
				Created: now,
				OwnedBy: "termite",
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		ln.logger.Error("encoding openai models response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
