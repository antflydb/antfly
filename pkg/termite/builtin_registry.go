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
	"sync"

	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/libaf/reranking"
)

// BuiltinEmbedderFactory creates a built-in embedder on demand.
type BuiltinEmbedderFactory func() (name string, embedder embeddings.Embedder, err error)

// BuiltinRerankerFactory creates a built-in reranker on demand.
type BuiltinRerankerFactory func() (name string, model reranking.Model, err error)

var (
	builtinEmbeddersMu       sync.RWMutex
	builtinEmbedderFactories []BuiltinEmbedderFactory

	builtinRerankersMu       sync.RWMutex
	builtinRerankerFactories []BuiltinRerankerFactory
)

// RegisterBuiltinEmbedder registers a factory for a built-in embedder.
// Called from init() in builtin embedder packages.
func RegisterBuiltinEmbedder(factory BuiltinEmbedderFactory) {
	builtinEmbeddersMu.Lock()
	defer builtinEmbeddersMu.Unlock()
	builtinEmbedderFactories = append(builtinEmbedderFactories, factory)
}

// RegisterBuiltinReranker registers a factory for a built-in reranker.
// Called from init() in builtin reranker packages.
func RegisterBuiltinReranker(factory BuiltinRerankerFactory) {
	builtinRerankersMu.Lock()
	defer builtinRerankersMu.Unlock()
	builtinRerankerFactories = append(builtinRerankerFactories, factory)
}

func getBuiltinEmbedderFactories() []BuiltinEmbedderFactory {
	builtinEmbeddersMu.RLock()
	defer builtinEmbeddersMu.RUnlock()
	result := make([]BuiltinEmbedderFactory, len(builtinEmbedderFactories))
	copy(result, builtinEmbedderFactories)
	return result
}

func getBuiltinRerankerFactories() []BuiltinRerankerFactory {
	builtinRerankersMu.RLock()
	defer builtinRerankersMu.RUnlock()
	result := make([]BuiltinRerankerFactory, len(builtinRerankerFactories))
	copy(result, builtinRerankerFactories)
	return result
}
