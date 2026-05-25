package generating

import "testing"

func TestResolveGeneratorOrChain(t *testing.T) {
	t.Run("with chain provided", func(t *testing.T) {
		chain := []ChainLink{
			{Generator: GeneratorConfig{Provider: GeneratorProviderOllama}},
			{Generator: GeneratorConfig{Provider: GeneratorProviderOpenai}},
		}
		result := ResolveGeneratorOrChain(GeneratorConfig{}, chain)
		if len(result) != 2 {
			t.Fatalf("len(result) = %d, want 2", len(result))
		}
		if result[0].Generator.Provider != GeneratorProviderOllama {
			t.Fatalf("first provider = %q, want %q", result[0].Generator.Provider, GeneratorProviderOllama)
		}
		chain[0].Generator.Provider = GeneratorProviderAnthropic
		if result[0].Generator.Provider != GeneratorProviderOllama {
			t.Fatal("ResolveGeneratorOrChain should return a cloned chain")
		}
	})

	t.Run("with generator provided", func(t *testing.T) {
		gen := GeneratorConfig{Provider: GeneratorProviderAnthropic}
		result := ResolveGeneratorOrChain(gen, nil)
		if len(result) != 1 {
			t.Fatalf("len(result) = %d, want 1", len(result))
		}
		if result[0].Generator.Provider != GeneratorProviderAnthropic {
			t.Fatalf("provider = %q, want %q", result[0].Generator.Provider, GeneratorProviderAnthropic)
		}
	})
}

func TestDefaultChainClones(t *testing.T) {
	original := []ChainLink{{Generator: GeneratorConfig{Provider: GeneratorProviderGemini}}}
	SetDefaultChain(original)

	original[0].Generator.Provider = GeneratorProviderOllama
	got := GetDefaultChain()
	if len(got) != 1 {
		t.Fatalf("len(GetDefaultChain()) = %d, want 1", len(got))
	}
	if got[0].Generator.Provider != GeneratorProviderGemini {
		t.Fatal("SetDefaultChain should clone the incoming slice")
	}

	got[0].Generator.Provider = GeneratorProviderOpenai
	again := GetDefaultChain()
	if again[0].Generator.Provider != GeneratorProviderGemini {
		t.Fatal("GetDefaultChain should return a cloned slice")
	}
}
