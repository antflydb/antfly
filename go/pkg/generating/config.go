package generating

import "fmt"

// NewGeneratorConfig constructs a unified GeneratorConfig from a provider-specific config.
func NewGeneratorConfig(config any) (*GeneratorConfig, error) {
	var provider GeneratorProvider
	modelConfig := &GeneratorConfig{}

	switch v := config.(type) {
	case GeneratorConfig:
		copy := v
		return &copy, nil
	case *GeneratorConfig:
		if v == nil {
			return nil, fmt.Errorf("generator config cannot be nil")
		}
		copy := *v
		return &copy, nil
	case OllamaGeneratorConfig:
		provider = GeneratorProviderOllama
		if err := modelConfig.FromOllamaGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from ollama generator config: %w", err)
		}
	case OpenAIGeneratorConfig:
		provider = GeneratorProviderOpenai
		if err := modelConfig.FromOpenAIGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from openai generator config: %w", err)
		}
	case GoogleGeneratorConfig:
		provider = GeneratorProviderGemini
		if err := modelConfig.FromGoogleGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from google generator config: %w", err)
		}
	case BedrockGeneratorConfig:
		provider = GeneratorProviderBedrock
		if err := modelConfig.FromBedrockGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from bedrock generator config: %w", err)
		}
	case VertexGeneratorConfig:
		provider = GeneratorProviderVertex
		if err := modelConfig.FromVertexGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from vertex generator config: %w", err)
		}
	case AnthropicGeneratorConfig:
		provider = GeneratorProviderAnthropic
		if err := modelConfig.FromAnthropicGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from anthropic generator config: %w", err)
		}
	case CohereGeneratorConfig:
		provider = GeneratorProviderCohere
		if err := modelConfig.FromCohereGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from cohere generator config: %w", err)
		}
	case OpenRouterGeneratorConfig:
		provider = GeneratorProviderOpenrouter
		if err := modelConfig.FromOpenRouterGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from openrouter generator config: %w", err)
		}
	case TermiteGeneratorConfig:
		provider = GeneratorProviderTermite
		if err := modelConfig.FromTermiteGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("from termite generator config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown generator config type: %T", v)
	}

	modelConfig.Provider = provider
	return modelConfig, nil
}

// NewGeneratorConfigFromJSON creates a GeneratorConfig from provider name and raw JSON.
// Mostly useful for tests and migration shims.
func NewGeneratorConfigFromJSON(provider string, data []byte) *GeneratorConfig {
	return &GeneratorConfig{
		Provider: GeneratorProvider(provider),
		union:    data,
	}
}

// GetModel extracts the configured model name from a GeneratorConfig.
func (gc *GeneratorConfig) GetModel() (string, error) {
	switch gc.Provider {
	case GeneratorProviderOllama:
		c, err := gc.AsOllamaGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderGemini:
		c, err := gc.AsGoogleGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderOpenai:
		c, err := gc.AsOpenAIGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderOpenrouter:
		c, err := gc.AsOpenRouterGeneratorConfig()
		if err != nil {
			return "", err
		}
		if c.Model != nil && *c.Model != "" {
			return *c.Model, nil
		}
		if c.Models != nil && len(*c.Models) > 0 {
			return (*c.Models)[0], nil
		}
		return "", nil
	case GeneratorProviderBedrock:
		c, err := gc.AsBedrockGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderVertex:
		c, err := gc.AsVertexGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderAnthropic:
		c, err := gc.AsAnthropicGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderCohere:
		c, err := gc.AsCohereGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderTermite:
		c, err := gc.AsTermiteGeneratorConfig()
		if err != nil {
			return "", err
		}
		return c.Model, nil
	case GeneratorProviderMock:
		return "", nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", gc.Provider)
	}
}
