package generating

import "slices"

// ResolveGeneratorOrChain returns an effective chain for execution.
// If a generator is provided, it is wrapped in a single-link chain.
func ResolveGeneratorOrChain(generator GeneratorConfig, chain []ChainLink) []ChainLink {
	if len(chain) > 0 {
		return slices.Clone(chain)
	}
	if generator.Provider == "" {
		return nil
	}
	return []ChainLink{{Generator: generator}}
}

var defaultChain []ChainLink

// SetDefaultChain sets the default generator chain.
func SetDefaultChain(chain []ChainLink) {
	defaultChain = slices.Clone(chain)
}

// GetDefaultChain returns the current default generator chain.
func GetDefaultChain() []ChainLink {
	return slices.Clone(defaultChain)
}
