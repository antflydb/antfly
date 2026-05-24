package client

import "testing"

func TestNewEmbedderConfigSupportsAntfly(t *testing.T) {
	cfg, err := NewEmbedderConfig(AntflyEmbedderConfig{
		"model": "antflydb/clipclap",
	})
	if err != nil {
		t.Fatalf("NewEmbedderConfig failed: %v", err)
	}
	if cfg.Provider != EmbedderProviderAntfly {
		t.Fatalf("provider = %q, want %q", cfg.Provider, EmbedderProviderAntfly)
	}

	embedder, err := cfg.AsAntflyEmbedderConfig()
	if err != nil {
		t.Fatalf("AsAntflyEmbedderConfig failed: %v", err)
	}
	if embedder["model"] != "antflydb/clipclap" {
		t.Fatalf("model = %q, want %q", embedder["model"], "antflydb/clipclap")
	}
}
