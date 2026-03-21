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

package metadata

import (
	"errors"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/pkg/generating"
	"github.com/stretchr/testify/assert"
)

func TestResolveEffectiveGeneratorChain(t *testing.T) {
	originalDefault := generating.GetDefaultChain()
	t.Cleanup(func() {
		generating.SetDefaultChain(originalDefault)
	})

	defaultChain := []ai.ChainLink{
		{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderGemini}},
	}
	generating.SetDefaultChain(defaultChain)

	t.Run("prefers explicit chain over single generator", func(t *testing.T) {
		req := &RetrievalAgentRequest{
			Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOpenai},
			Chain: []ai.ChainLink{
				{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderAnthropic}},
			},
		}

		chain := resolveEffectiveGeneratorChain(req)
		if assert.Len(t, chain, 1) {
			assert.Equal(t, ai.GeneratorProviderAnthropic, chain[0].Generator.Provider)
		}
	})

	t.Run("wraps explicit generator when chain is absent", func(t *testing.T) {
		req := &RetrievalAgentRequest{
			Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOpenai},
		}

		chain := resolveEffectiveGeneratorChain(req)
		if assert.Len(t, chain, 1) {
			assert.Equal(t, ai.GeneratorProviderOpenai, chain[0].Generator.Provider)
		}
	})

	t.Run("falls back to default chain", func(t *testing.T) {
		req := &RetrievalAgentRequest{}

		chain := resolveEffectiveGeneratorChain(req)
		if assert.Len(t, chain, 1) {
			assert.Equal(t, ai.GeneratorProviderGemini, chain[0].Generator.Provider)
		}
	})

	t.Run("returns nil when no explicit or default chain exists", func(t *testing.T) {
		generating.SetDefaultChain(nil)

		req := &RetrievalAgentRequest{}
		assert.Nil(t, resolveEffectiveGeneratorChain(req))
	})
}

func TestResolveProviderName(t *testing.T) {
	originalDefault := generating.GetDefaultChain()
	t.Cleanup(func() {
		generating.SetDefaultChain(originalDefault)
	})

	t.Run("uses first provider from explicit chain", func(t *testing.T) {
		generating.SetDefaultChain([]ai.ChainLink{
			{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderGemini}},
		})
		req := &RetrievalAgentRequest{
			Chain: []ai.ChainLink{
				{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderAnthropic}},
				{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOpenai}},
			},
		}

		assert.Equal(t, "anthropic", resolveProviderName(req))
		assert.Equal(t, ai.GeneratorProviderAnthropic, resolveProvider(req))
	})

	t.Run("uses default chain provider when request does not specify one", func(t *testing.T) {
		generating.SetDefaultChain([]ai.ChainLink{
			{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOllama}},
		})

		req := &RetrievalAgentRequest{}
		assert.Equal(t, "ollama", resolveProviderName(req))
		assert.Equal(t, ai.GeneratorProviderOllama, resolveProvider(req))
	})

	t.Run("returns unknown when no provider can be resolved", func(t *testing.T) {
		generating.SetDefaultChain(nil)

		req := &RetrievalAgentRequest{}
		assert.Equal(t, "unknown", resolveProviderName(req))
		assert.Equal(t, ai.GeneratorProvider(""), resolveProvider(req))
	})
}

func TestAsGenerationErrorResponse(t *testing.T) {
	t.Run("generic query failures are not classified as generation errors", func(t *testing.T) {
		message, statusCode, ok := asGenerationErrorResponse(errors.New("query failed (table=test): bad filter"))
		assert.False(t, ok)
		assert.Empty(t, message)
		assert.Zero(t, statusCode)
	})

	t.Run("preserves typed generation errors", func(t *testing.T) {
		message, statusCode, ok := asGenerationErrorResponse(&ai.GenerationError{
			Kind:        ai.GenerationErrorRateLimit,
			UserMessage: "Rate limit reached for provider 'openrouter'. Please wait and try again.",
		})
		assert.True(t, ok)
		assert.Equal(t, "Rate limit reached for provider 'openrouter'. Please wait and try again.", message)
		assert.Equal(t, 429, statusCode)
	})
}
