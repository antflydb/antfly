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
	"log"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/audio"
	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/reranking"
)

// InitRegistryFromConfig initializes the global provider registry from configuration.
// It registers all named STT, TTS, embedder, generator, reranker, chunker, and chain configurations.
func InitRegistryFromConfig(config *Config) {
	reg := GlobalRegistry()

	// Register STT providers
	for name, sttConfig := range config.SpeechToText {
		stt, err := audio.NewSTT(sttConfig)
		if err != nil {
			log.Printf("Warning: failed to initialize STT provider %q: %v", name, err)
			continue
		}
		reg.RegisterSTT(name, stt)
		log.Printf("Registered STT provider: %s", name)
	}

	// Set the first registered STT as the default for backwards compatibility
	if defaultSTT := reg.DefaultSTTName(); defaultSTT != "" {
		stt, _ := reg.GetSTT(defaultSTT)
		audio.SetDefaultSTT(stt)
	}

	// Register embedder configs
	for name, embedderConfig := range config.Embedders {
		reg.RegisterEmbedderConfig(name, embedderConfig)
		log.Printf("Registered embedder: %s", name)
	}

	// Register generator configs
	for name, generatorConfig := range config.Generators {
		reg.RegisterGeneratorConfig(name, generatorConfig)
		log.Printf("Registered generator: %s", name)
	}

	// Register chains
	for name, chainLinks := range config.Chains {
		links := make([]ChainLinkConfig, 0, len(chainLinks))
		for _, link := range chainLinks {
			linkConfig := ChainLinkConfig{
				GeneratorName: link.Generator,
			}
			// Convert value types to pointers for optional fields
			if link.Retry.MaxAttempts != nil && *link.Retry.MaxAttempts > 0 {
				retry := link.Retry
				linkConfig.Retry = &retry
			}
			if link.Condition != "" {
				condition := link.Condition
				linkConfig.Condition = &condition
			}
			// Check if inline GeneratorConfig is provided (Provider is required)
			if link.GeneratorConfig.Provider != "" {
				genConfig := link.GeneratorConfig
				linkConfig.GeneratorConfig = &genConfig
			}
			links = append(links, linkConfig)
		}
		reg.RegisterChain(name, links)
		log.Printf("Registered chain: %s (%d links)", name, len(links))
	}

	// Set default chain for backwards compatibility with ai package
	if defaultChain := reg.DefaultChainName(); defaultChain != "" {
		chain, err := reg.GetChain(defaultChain)
		if err == nil {
			ai.SetDefaultChain(chain)
		}
	}

	// Register reranker configs
	for name, rerankerConfig := range config.Rerankers {
		reg.RegisterRerankerConfig(name, rerankerConfig)
		log.Printf("Registered reranker: %s", name)
	}

	// Set the first registered reranker as the default
	if defaultReranker := reg.DefaultRerankerName(); defaultReranker != "" {
		cfg, _ := reg.GetRerankerConfig(defaultReranker)
		reranking.SetDefaultRerankerConfig(&cfg)
	}

	// Register chunker configs
	for name, chunkerConfig := range config.Chunkers {
		reg.RegisterChunkerConfig(name, chunkerConfig)
		log.Printf("Registered chunker: %s", name)
	}

	// Set the first registered chunker as the default
	if defaultChunker := reg.DefaultChunkerName(); defaultChunker != "" {
		cfg, _ := reg.GetChunkerConfig(defaultChunker)
		chunking.SetDefaultChunkerConfig(&cfg)
	}

	// Set the first registered embedder as the default
	if defaultEmbedder := reg.DefaultEmbedderName(); defaultEmbedder != "" {
		cfg, _ := reg.GetEmbedderConfig(defaultEmbedder)
		embeddings.SetDefaultEmbedderConfig(&cfg)
	}
}
