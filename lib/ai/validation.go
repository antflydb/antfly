package ai

import generating "github.com/antflydb/antfly/pkg/generating"

// ValidGeneratorProviders returns all valid generator provider values.
func ValidGeneratorProviders() []GeneratorProvider {
	providers := generating.ValidGeneratorProviders()
	out := make([]GeneratorProvider, len(providers))
	for i, provider := range providers {
		out[i] = GeneratorProvider(provider)
	}
	return out
}
