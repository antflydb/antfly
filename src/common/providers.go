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

// Package common provides a centralized registry for named AI/ML provider configs.
// It manages named configurations for STT, TTS, embedders, and generators that can be
// referenced by name from templates, API calls, and chain configurations.
package common

import (
	"fmt"
	"sync"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/audio"
	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/reranking"
)

// Registry holds named provider configurations.
type Registry struct {
	mu sync.RWMutex

	// Named STT providers (initialized instances)
	sttProviders map[string]audio.STT
	defaultSTT   string

	// Named TTS providers (initialized instances)
	ttsProviders map[string]audio.TTS
	defaultTTS   string

	// Named embedder configs
	embedderConfigs map[string]embeddings.EmbedderConfig
	defaultEmbedder string

	// Named generator configs
	generatorConfigs map[string]ai.GeneratorConfig
	defaultGenerator string

	// Named chains (arrays of chain links that can reference generators by name)
	chains       map[string][]ChainLinkConfig
	defaultChain string

	// Named reranker configs
	rerankerConfigs map[string]reranking.RerankerConfig
	defaultReranker string

	// Named chunker configs
	chunkerConfigs map[string]chunking.ChunkerConfig
	defaultChunker string
}

// ChainLinkConfig represents a chain link that can reference a generator by name
// or contain an inline generator config.
type ChainLinkConfig struct {
	// GeneratorName references a named generator (mutually exclusive with GeneratorConfig)
	GeneratorName string
	// GeneratorConfig is an inline generator config (mutually exclusive with GeneratorName)
	GeneratorConfig *ai.GeneratorConfig
	// Retry configuration
	Retry *ai.RetryConfig
	// Condition for trying the next generator
	Condition *ai.ChainCondition
}

// Global registry instance
var globalRegistry = &Registry{
	sttProviders:     make(map[string]audio.STT),
	ttsProviders:     make(map[string]audio.TTS),
	embedderConfigs:  make(map[string]embeddings.EmbedderConfig),
	generatorConfigs: make(map[string]ai.GeneratorConfig),
	chains:           make(map[string][]ChainLinkConfig),
	rerankerConfigs:  make(map[string]reranking.RerankerConfig),
	chunkerConfigs:   make(map[string]chunking.ChunkerConfig),
}

// GlobalRegistry returns the global provider registry.
func GlobalRegistry() *Registry {
	return globalRegistry
}

// RegisterSTT registers a named STT provider instance.
// The first registered provider becomes the default.
func (r *Registry) RegisterSTT(name string, stt audio.STT) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.defaultSTT == "" {
		r.defaultSTT = name
	}
	r.sttProviders[name] = stt
}

// GetSTT returns the STT provider by name.
// If name is empty, returns the default provider.
func (r *Registry) GetSTT(name string) (audio.STT, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultSTT
	}
	if name == "" {
		return nil, fmt.Errorf("no STT providers registered")
	}

	stt, ok := r.sttProviders[name]
	if !ok {
		return nil, fmt.Errorf("STT provider %q not found", name)
	}
	return stt, nil
}

// DefaultSTTName returns the default STT provider name.
func (r *Registry) DefaultSTTName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultSTT
}

// STTNames returns all registered STT provider names.
func (r *Registry) STTNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.sttProviders))
	for name := range r.sttProviders {
		names = append(names, name)
	}
	return names
}

// RegisterTTS registers a named TTS provider instance.
// The first registered provider becomes the default.
func (r *Registry) RegisterTTS(name string, tts audio.TTS) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.defaultTTS == "" {
		r.defaultTTS = name
	}
	r.ttsProviders[name] = tts
}

// GetTTS returns the TTS provider by name.
// If name is empty, returns the default provider.
func (r *Registry) GetTTS(name string) (audio.TTS, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultTTS
	}
	if name == "" {
		return nil, fmt.Errorf("no TTS providers registered")
	}

	tts, ok := r.ttsProviders[name]
	if !ok {
		return nil, fmt.Errorf("TTS provider %q not found", name)
	}
	return tts, nil
}

// RegisterEmbedderConfig registers a named embedder configuration.
// The first registered embedder becomes the default.
func (r *Registry) RegisterEmbedderConfig(name string, config embeddings.EmbedderConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.defaultEmbedder == "" {
		r.defaultEmbedder = name
	}
	r.embedderConfigs[name] = config
}

// GetEmbedderConfig returns the embedder config by name.
// If name is empty, returns the default embedder config.
func (r *Registry) GetEmbedderConfig(name string) (embeddings.EmbedderConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultEmbedder
	}
	if name == "" {
		return embeddings.EmbedderConfig{}, fmt.Errorf("no embedders registered")
	}

	config, ok := r.embedderConfigs[name]
	if !ok {
		return embeddings.EmbedderConfig{}, fmt.Errorf("embedder %q not found", name)
	}
	return config, nil
}

// DefaultEmbedderName returns the default embedder name.
func (r *Registry) DefaultEmbedderName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultEmbedder
}

// EmbedderNames returns all registered embedder names.
func (r *Registry) EmbedderNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.embedderConfigs))
	for name := range r.embedderConfigs {
		names = append(names, name)
	}
	return names
}

// RegisterGeneratorConfig registers a named generator configuration.
// The first registered generator becomes the default.
func (r *Registry) RegisterGeneratorConfig(name string, config ai.GeneratorConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.defaultGenerator == "" {
		r.defaultGenerator = name
	}
	r.generatorConfigs[name] = config
}

// GetGeneratorConfig returns the generator config by name.
// If name is empty, returns the default generator config.
func (r *Registry) GetGeneratorConfig(name string) (ai.GeneratorConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultGenerator
	}
	if name == "" {
		return ai.GeneratorConfig{}, fmt.Errorf("no generators registered")
	}

	config, ok := r.generatorConfigs[name]
	if !ok {
		return ai.GeneratorConfig{}, fmt.Errorf("generator %q not found", name)
	}
	return config, nil
}

// DefaultGeneratorName returns the default generator name.
func (r *Registry) DefaultGeneratorName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultGenerator
}

// GeneratorNames returns all registered generator names.
func (r *Registry) GeneratorNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.generatorConfigs))
	for name := range r.generatorConfigs {
		names = append(names, name)
	}
	return names
}

// RegisterChain registers a named chain configuration.
// The first registered chain becomes the default.
func (r *Registry) RegisterChain(name string, links []ChainLinkConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.defaultChain == "" {
		r.defaultChain = name
	}
	r.chains[name] = links
}

// GetChain resolves and returns a chain by name as []ai.ChainLink.
// If name is empty, returns the default chain.
// This resolves generator name references to actual GeneratorConfig values.
func (r *Registry) GetChain(name string) ([]ai.ChainLink, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultChain
	}
	if name == "" {
		return nil, fmt.Errorf("no chains registered")
	}

	linkConfigs, ok := r.chains[name]
	if !ok {
		return nil, fmt.Errorf("chain %q not found", name)
	}

	// Resolve generator references
	links := make([]ai.ChainLink, 0, len(linkConfigs))
	for i, cfg := range linkConfigs {
		var genConfig ai.GeneratorConfig

		if cfg.GeneratorName != "" {
			// Lookup by name
			var found bool
			genConfig, found = r.generatorConfigs[cfg.GeneratorName]
			if !found {
				return nil, fmt.Errorf("chain %q link %d: generator %q not found", name, i, cfg.GeneratorName)
			}
		} else if cfg.GeneratorConfig != nil {
			// Use inline config
			genConfig = *cfg.GeneratorConfig
		} else {
			return nil, fmt.Errorf("chain %q link %d: no generator specified", name, i)
		}

		links = append(links, ai.ChainLink{
			Generator: genConfig,
			Retry:     cfg.Retry,
			Condition: cfg.Condition,
		})
	}

	return links, nil
}

// DefaultChainName returns the default chain name.
func (r *Registry) DefaultChainName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultChain
}

// ChainNames returns all registered chain names.
func (r *Registry) ChainNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.chains))
	for name := range r.chains {
		names = append(names, name)
	}
	return names
}

// RegisterRerankerConfig registers a named reranker configuration.
// The first registered reranker becomes the default.
func (r *Registry) RegisterRerankerConfig(name string, config reranking.RerankerConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.defaultReranker == "" {
		r.defaultReranker = name
	}
	r.rerankerConfigs[name] = config
}

// GetRerankerConfig returns the reranker config by name.
// If name is empty, returns the default reranker config.
func (r *Registry) GetRerankerConfig(name string) (reranking.RerankerConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultReranker
	}
	if name == "" {
		return reranking.RerankerConfig{}, fmt.Errorf("no rerankers registered")
	}

	config, ok := r.rerankerConfigs[name]
	if !ok {
		return reranking.RerankerConfig{}, fmt.Errorf("reranker %q not found", name)
	}
	return config, nil
}

// DefaultRerankerName returns the default reranker name.
func (r *Registry) DefaultRerankerName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultReranker
}

// RerankerNames returns all registered reranker names.
func (r *Registry) RerankerNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.rerankerConfigs))
	for name := range r.rerankerConfigs {
		names = append(names, name)
	}
	return names
}

// RegisterChunkerConfig registers a named chunker configuration.
// The first registered chunker becomes the default.
func (r *Registry) RegisterChunkerConfig(name string, config chunking.ChunkerConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.defaultChunker == "" {
		r.defaultChunker = name
	}
	r.chunkerConfigs[name] = config
}

// GetChunkerConfig returns the chunker config by name.
// If name is empty, returns the default chunker config.
func (r *Registry) GetChunkerConfig(name string) (chunking.ChunkerConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultChunker
	}
	if name == "" {
		return chunking.ChunkerConfig{}, fmt.Errorf("no chunkers registered")
	}

	config, ok := r.chunkerConfigs[name]
	if !ok {
		return chunking.ChunkerConfig{}, fmt.Errorf("chunker %q not found", name)
	}
	return config, nil
}

// DefaultChunkerName returns the default chunker name.
func (r *Registry) DefaultChunkerName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultChunker
}

// ChunkerNames returns all registered chunker names.
func (r *Registry) ChunkerNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.chunkerConfigs))
	for name := range r.chunkerConfigs {
		names = append(names, name)
	}
	return names
}

// Clear resets the registry (mainly useful for testing).
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sttProviders = make(map[string]audio.STT)
	r.defaultSTT = ""
	r.ttsProviders = make(map[string]audio.TTS)
	r.defaultTTS = ""
	r.embedderConfigs = make(map[string]embeddings.EmbedderConfig)
	r.defaultEmbedder = ""
	r.generatorConfigs = make(map[string]ai.GeneratorConfig)
	r.defaultGenerator = ""
	r.chains = make(map[string][]ChainLinkConfig)
	r.defaultChain = ""
	r.rerankerConfigs = make(map[string]reranking.RerankerConfig)
	r.defaultReranker = ""
	r.chunkerConfigs = make(map[string]chunking.ChunkerConfig)
	r.defaultChunker = ""
}
