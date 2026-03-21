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
