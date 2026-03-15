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

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml

package eval

import (
	"github.com/antflydb/antfly/lib/ai"
)

// DefaultOptions returns the default evaluation options.
func DefaultOptions() EvalOptions {
	return EvalOptions{
		K:              10,
		PassThreshold:  0.5,
		TimeoutSeconds: 30,
	}
}

// DefaultJudge returns the default judge configuration for LLM evaluators.
func DefaultJudge() ai.GeneratorConfig {
	model := "gemma3:4b"
	temp := float32(0.0)
	cfg := ai.GeneratorConfig{
		Provider: ai.GeneratorProviderOllama,
	}
	_ = cfg.FromOllamaGeneratorConfig(ai.OllamaGeneratorConfig{
		Model:       model,
		Temperature: &temp,
	})
	return cfg
}

// ApplyDefaults fills in missing config values with defaults.
func ApplyDefaults(cfg *EvalConfig) {
	if cfg == nil {
		return
	}

	defaults := DefaultOptions()

	// Apply default K if not set
	if cfg.Options.K == 0 {
		cfg.Options.K = defaults.K
	}

	// Apply default pass threshold if not set
	if cfg.Options.PassThreshold == 0 {
		cfg.Options.PassThreshold = defaults.PassThreshold
	}

	// Apply default timeout if not set
	if cfg.Options.TimeoutSeconds == 0 {
		cfg.Options.TimeoutSeconds = defaults.TimeoutSeconds
	}
}
