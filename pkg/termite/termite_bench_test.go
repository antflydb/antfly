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
	"context"
	"testing"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/embeddings"
)

// BenchmarkMockEmbedder benchmarks direct embedding calls without caching
func BenchmarkMockEmbedder(b *testing.B) {
	mockEmbedder := &MockEmbedder{
		embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
			time.Sleep(10 * time.Millisecond) // Simulate API call
			result := make([][]float32, len(values))
			for i := range values {
				result[i] = make([]float32, 768) // Typical embedding size
				for j := range result[i] {
					result[i][j] = float32(i + j)
				}
			}
			return result, nil
		},
	}

	ctx := context.Background()
	prompts := make([]string, 10)
	for i := range 10 {
		prompts[i] = "benchmark-prompt"
	}

	b.ReportAllocs()

	for b.Loop() {
		_, err := embeddings.EmbedText(ctx, mockEmbedder, prompts)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Logf("Embedder calls: %d", mockEmbedder.GetCallCount())
}
