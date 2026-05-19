// Copyright 2026 Antfly, Inc.
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

package ai

import generating "github.com/antflydb/antfly/pkg/generating"

type GoogleGeneratorConfig = generating.GoogleGeneratorConfig
type VertexGeneratorConfig = generating.VertexGeneratorConfig
type OllamaGeneratorConfig = generating.OllamaGeneratorConfig
type TermiteGeneratorConfig = generating.TermiteGeneratorConfig
type OpenAIGeneratorConfig = generating.OpenAIGeneratorConfig
type OpenRouterGeneratorConfig = generating.OpenRouterGeneratorConfig
type BedrockGeneratorConfig = generating.BedrockGeneratorConfig
type AnthropicGeneratorConfig = generating.AnthropicGeneratorConfig
type CohereGeneratorConfig = generating.CohereGeneratorConfig

const (
	ChainConditionAlways        = generating.ChainConditionAlways
	ChainConditionOnError       = generating.ChainConditionOnError
	ChainConditionOnRateLimit   = generating.ChainConditionOnRateLimit
	ChainConditionOnTimeout     = generating.ChainConditionOnTimeout
	GeneratorProviderAntfly     = generating.GeneratorProviderAntfly
	GeneratorProviderAnthropic  = generating.GeneratorProviderAnthropic
	GeneratorProviderBedrock    = generating.GeneratorProviderBedrock
	GeneratorProviderCohere     = generating.GeneratorProviderCohere
	GeneratorProviderGemini     = generating.GeneratorProviderGemini
	GeneratorProviderMock       = generating.GeneratorProviderMock
	GeneratorProviderOllama     = generating.GeneratorProviderOllama
	GeneratorProviderOpenai     = generating.GeneratorProviderOpenai
	GeneratorProviderOpenrouter = generating.GeneratorProviderOpenrouter
	GeneratorProviderTermite    = generating.GeneratorProviderTermite
	GeneratorProviderVertex     = generating.GeneratorProviderVertex
)
