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

package reranking

import (
	"context"
)

// Model represents a reranking model that can score text relevance.
// This interface allows different model implementations (ONNX, API-based, etc.)
// to be used interchangeably in the reranking pipeline.
type Model interface {
	// Rerank scores pre-rendered document texts based on their relevance to the query.
	// Returns a slice of scores with the same length as prompts.
	// Higher scores indicate higher relevance.
	Rerank(ctx context.Context, query string, prompts []string) ([]float32, error)

	// Close releases any resources held by the model (sessions, connections, etc.)
	Close() error
}

// ModelType identifies different reranking model implementations
type ModelType string

const (
	ModelTypeHugot ModelType = "hugot" // ONNX-based cross-encoder via Hugot
	// Future model types can be added here:
	// ModelTypeCohere   ModelType = "cohere"   // Cohere Rerank API
	// ModelTypeOpenAI   ModelType = "openai"   // OpenAI semantic search
	// ModelTypeVertexAI ModelType = "vertexai" // Google Vertex AI
)
