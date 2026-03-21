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

package common

import (
	"testing"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/reranking"
	generating "github.com/antflydb/antfly/pkg/generating"
)

func TestRegistry_EmbedderConfig(t *testing.T) {
	reg := &Registry{
		embedderConfigs: make(map[string]embeddings.EmbedderConfig),
	}

	// Register first embedder - should become default
	config1 := embeddings.EmbedderConfig{Provider: "openai"}
	reg.RegisterEmbedderConfig("openai-small", config1)

	if reg.DefaultEmbedderName() != "openai-small" {
		t.Errorf("expected default embedder to be 'openai-small', got %q", reg.DefaultEmbedderName())
	}

	// Register second embedder - should not change default
	config2 := embeddings.EmbedderConfig{Provider: "termite"}
	reg.RegisterEmbedderConfig("termite-local", config2)

	if reg.DefaultEmbedderName() != "openai-small" {
		t.Errorf("expected default embedder to still be 'openai-small', got %q", reg.DefaultEmbedderName())
	}

	// Get by name
	got, err := reg.GetEmbedderConfig("termite-local")
	if err != nil {
		t.Fatalf("GetEmbedderConfig failed: %v", err)
	}
	if got.Provider != "termite" {
		t.Errorf("expected provider 'termite', got %q", got.Provider)
	}

	// Get default (empty name)
	got, err = reg.GetEmbedderConfig("")
	if err != nil {
		t.Fatalf("GetEmbedderConfig (default) failed: %v", err)
	}
	if got.Provider != "openai" {
		t.Errorf("expected default provider 'openai', got %q", got.Provider)
	}

	// Get non-existent
	_, err = reg.GetEmbedderConfig("non-existent")
	if err == nil {
		t.Error("expected error for non-existent embedder")
	}

	// List names
	names := reg.EmbedderNames()
	if len(names) != 2 {
		t.Errorf("expected 2 embedder names, got %d", len(names))
	}
}

func TestRegistry_GeneratorConfig(t *testing.T) {
	reg := &Registry{
		generatorConfigs: make(map[string]generating.GeneratorConfig),
	}

	// Register generators
	config1 := generating.GeneratorConfig{Provider: "gemini"}
	reg.RegisterGeneratorConfig("gemini-flash", config1)

	config2 := generating.GeneratorConfig{Provider: "openai"}
	reg.RegisterGeneratorConfig("openai-gpt4", config2)

	if reg.DefaultGeneratorName() != "gemini-flash" {
		t.Errorf("expected default generator to be 'gemini-flash', got %q", reg.DefaultGeneratorName())
	}

	got, err := reg.GetGeneratorConfig("openai-gpt4")
	if err != nil {
		t.Fatalf("GetGeneratorConfig failed: %v", err)
	}
	if got.Provider != "openai" {
		t.Errorf("expected provider 'openai', got %q", got.Provider)
	}
}

func TestRegistry_RerankerConfig(t *testing.T) {
	reg := &Registry{
		rerankerConfigs: make(map[string]reranking.RerankerConfig),
	}

	config := reranking.RerankerConfig{Provider: "cohere"}
	reg.RegisterRerankerConfig("cohere-english", config)

	if reg.DefaultRerankerName() != "cohere-english" {
		t.Errorf("expected default reranker to be 'cohere-english', got %q", reg.DefaultRerankerName())
	}

	got, err := reg.GetRerankerConfig("")
	if err != nil {
		t.Fatalf("GetRerankerConfig failed: %v", err)
	}
	if got.Provider != "cohere" {
		t.Errorf("expected provider 'cohere', got %q", got.Provider)
	}
}

func TestRegistry_ChunkerConfig(t *testing.T) {
	reg := &Registry{
		chunkerConfigs: make(map[string]chunking.ChunkerConfig),
	}

	config := chunking.ChunkerConfig{Provider: "termite"}
	reg.RegisterChunkerConfig("fixed-500", config)

	if reg.DefaultChunkerName() != "fixed-500" {
		t.Errorf("expected default chunker to be 'fixed-500', got %q", reg.DefaultChunkerName())
	}

	got, err := reg.GetChunkerConfig("fixed-500")
	if err != nil {
		t.Fatalf("GetChunkerConfig failed: %v", err)
	}
	if got.Provider != "termite" {
		t.Errorf("expected provider 'termite', got %q", got.Provider)
	}
}

func TestRegistry_Chain(t *testing.T) {
	reg := &Registry{
		generatorConfigs: make(map[string]generating.GeneratorConfig),
		chains:           make(map[string][]ChainLinkConfig),
	}

	// Register generators first
	reg.RegisterGeneratorConfig("gemini-flash", generating.GeneratorConfig{Provider: "gemini"})
	reg.RegisterGeneratorConfig("openai-gpt4", generating.GeneratorConfig{Provider: "openai"})

	// Register chain with named references
	condition := generating.ChainConditionOnRateLimit
	chainLinks := []ChainLinkConfig{
		{GeneratorName: "gemini-flash"},
		{GeneratorName: "openai-gpt4", Condition: &condition},
	}
	reg.RegisterChain("default", chainLinks)

	if reg.DefaultChainName() != "default" {
		t.Errorf("expected default chain to be 'default', got %q", reg.DefaultChainName())
	}

	// Get and resolve chain
	resolved, err := reg.GetChain("default")
	if err != nil {
		t.Fatalf("GetChain failed: %v", err)
	}

	if len(resolved) != 2 {
		t.Fatalf("expected 2 chain links, got %d", len(resolved))
	}

	if resolved[0].Generator.Provider != "gemini" {
		t.Errorf("expected first link provider 'gemini', got %q", resolved[0].Generator.Provider)
	}

	if resolved[1].Generator.Provider != "openai" {
		t.Errorf("expected second link provider 'openai', got %q", resolved[1].Generator.Provider)
	}

	if resolved[1].Condition == nil || *resolved[1].Condition != generating.ChainConditionOnRateLimit {
		t.Error("expected second link condition to be 'on_rate_limit'")
	}
}

func TestRegistry_ChainWithInlineConfig(t *testing.T) {
	reg := &Registry{
		generatorConfigs: make(map[string]generating.GeneratorConfig),
		chains:           make(map[string][]ChainLinkConfig),
	}

	// Register one named generator
	reg.RegisterGeneratorConfig("gemini-flash", generating.GeneratorConfig{Provider: "gemini"})

	// Register chain with mix of named and inline
	inlineConfig := generating.GeneratorConfig{Provider: "anthropic"}
	chainLinks := []ChainLinkConfig{
		{GeneratorName: "gemini-flash"},
		{GeneratorConfig: &inlineConfig},
	}
	reg.RegisterChain("mixed", chainLinks)

	resolved, err := reg.GetChain("mixed")
	if err != nil {
		t.Fatalf("GetChain failed: %v", err)
	}

	if len(resolved) != 2 {
		t.Fatalf("expected 2 chain links, got %d", len(resolved))
	}

	if resolved[0].Generator.Provider != "gemini" {
		t.Errorf("expected first link provider 'gemini', got %q", resolved[0].Generator.Provider)
	}

	if resolved[1].Generator.Provider != "anthropic" {
		t.Errorf("expected second link provider 'anthropic', got %q", resolved[1].Generator.Provider)
	}
}

func TestRegistry_ChainMissingGenerator(t *testing.T) {
	reg := &Registry{
		generatorConfigs: make(map[string]generating.GeneratorConfig),
		chains:           make(map[string][]ChainLinkConfig),
	}

	// Register chain referencing non-existent generator
	chainLinks := []ChainLinkConfig{
		{GeneratorName: "non-existent"},
	}
	reg.RegisterChain("bad-chain", chainLinks)

	_, err := reg.GetChain("bad-chain")
	if err == nil {
		t.Error("expected error for chain with missing generator reference")
	}
}

func TestRegistry_Clear(t *testing.T) {
	reg := &Registry{
		embedderConfigs:  make(map[string]embeddings.EmbedderConfig),
		generatorConfigs: make(map[string]generating.GeneratorConfig),
		rerankerConfigs:  make(map[string]reranking.RerankerConfig),
		chunkerConfigs:   make(map[string]chunking.ChunkerConfig),
		chains:           make(map[string][]ChainLinkConfig),
	}

	// Add some data
	reg.RegisterEmbedderConfig("test", embeddings.EmbedderConfig{Provider: "test"})
	reg.RegisterGeneratorConfig("test", generating.GeneratorConfig{Provider: "test"})
	reg.RegisterRerankerConfig("test", reranking.RerankerConfig{Provider: "cohere"})
	reg.RegisterChunkerConfig("test", chunking.ChunkerConfig{Provider: "termite"})
	reg.RegisterChain("test", []ChainLinkConfig{})

	// Clear
	reg.Clear()

	// Verify everything is cleared
	if len(reg.EmbedderNames()) != 0 {
		t.Error("expected embedders to be cleared")
	}
	if len(reg.GeneratorNames()) != 0 {
		t.Error("expected generators to be cleared")
	}
	if len(reg.RerankerNames()) != 0 {
		t.Error("expected rerankers to be cleared")
	}
	if len(reg.ChunkerNames()) != 0 {
		t.Error("expected chunkers to be cleared")
	}
	if len(reg.ChainNames()) != 0 {
		t.Error("expected chains to be cleared")
	}
	if reg.DefaultEmbedderName() != "" {
		t.Error("expected default embedder to be cleared")
	}
}

func TestRegistry_EmptyRegistryErrors(t *testing.T) {
	reg := &Registry{
		embedderConfigs:  make(map[string]embeddings.EmbedderConfig),
		generatorConfigs: make(map[string]generating.GeneratorConfig),
		rerankerConfigs:  make(map[string]reranking.RerankerConfig),
		chunkerConfigs:   make(map[string]chunking.ChunkerConfig),
		chains:           make(map[string][]ChainLinkConfig),
	}

	// All should error when empty and requesting default
	_, err := reg.GetEmbedderConfig("")
	if err == nil {
		t.Error("expected error for empty embedder registry")
	}

	_, err = reg.GetGeneratorConfig("")
	if err == nil {
		t.Error("expected error for empty generator registry")
	}

	_, err = reg.GetRerankerConfig("")
	if err == nil {
		t.Error("expected error for empty reranker registry")
	}

	_, err = reg.GetChunkerConfig("")
	if err == nil {
		t.Error("expected error for empty chunker registry")
	}

	_, err = reg.GetChain("")
	if err == nil {
		t.Error("expected error for empty chain registry")
	}
}
