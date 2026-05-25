package eval

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the evaluation configuration.
type Config struct {
	// Version of the config format
	Version int `yaml:"version" json:"version"`

	// Evaluators contains evaluator configurations keyed by name
	Evaluators map[string]EvaluatorConfig `yaml:"evaluators" json:"evaluators"`

	// Datasets contains dataset configurations
	Datasets []DatasetConfig `yaml:"datasets" json:"datasets"`

	// Output configures output format and destination
	Output OutputConfig `yaml:"output" json:"output"`

	// Execution configures execution behavior
	Execution ExecutionConfig `yaml:"execution" json:"execution"`
}

// EvaluatorConfig represents the configuration for a single evaluator.
type EvaluatorConfig struct {
	// Type is the evaluator type (e.g., "exact_match", "regex", "genkit_llm_judge")
	Type string `yaml:"type" json:"type"`

	// Model specifies which model to use (for LLM-based evaluators)
	Model string `yaml:"model,omitempty" json:"model,omitempty"`

	// Temperature for LLM-based evaluators
	Temperature float64 `yaml:"temperature,omitempty" json:"temperature,omitempty"`

	// Prompt template for LLM-based evaluators
	Prompt string `yaml:"prompt,omitempty" json:"prompt,omitempty"`

	// Pattern for regex-based evaluators
	Pattern string `yaml:"pattern,omitempty" json:"pattern,omitempty"`

	// K for metrics like precision@k, recall@k, NDCG@k
	K int `yaml:"k,omitempty" json:"k,omitempty"`

	// Metric specifies which metric to use (e.g., "ndcg", "precision", "recall", "mrr")
	Metric string `yaml:"metric,omitempty" json:"metric,omitempty"`

	// Classes for classification evaluators
	Classes []string `yaml:"classes,omitempty" json:"classes,omitempty"`

	// Custom parameters for extensibility
	Custom map[string]any `yaml:"custom,omitempty" json:"custom,omitempty"`
}

// DatasetConfig represents the configuration for a dataset.
type DatasetConfig struct {
	// Name is the dataset name
	Name string `yaml:"name" json:"name"`

	// Path is the path to the dataset file
	Path string `yaml:"path" json:"path"`

	// Type is the dataset type (e.g., "json", "yaml", "csv")
	Type string `yaml:"type" json:"type"`

	// Custom parameters for extensibility
	Custom map[string]any `yaml:"custom,omitempty" json:"custom,omitempty"`
}

// OutputConfig configures output format and destination.
type OutputConfig struct {
	// Format is the output format (e.g., "json", "yaml", "markdown", "html")
	Format string `yaml:"format" json:"format"`

	// Path is the output directory or file path
	Path string `yaml:"path" json:"path"`

	// Pretty enables pretty-printing for JSON/YAML output
	Pretty bool `yaml:"pretty" json:"pretty"`
}

// ExecutionConfig configures execution behavior.
type ExecutionConfig struct {
	// Parallel enables parallel evaluation of examples
	Parallel bool `yaml:"parallel" json:"parallel"`

	// MaxConcurrency limits the number of concurrent evaluations
	MaxConcurrency int `yaml:"max_concurrency" json:"max_concurrency"`

	// Timeout is the overall timeout for evaluation (in Go duration format, e.g., "5m")
	Timeout string `yaml:"timeout" json:"timeout"`

	// RateLimitPerMinute limits the number of evaluator calls per minute (0 = unlimited)
	// This is useful for LLM-based evaluators with API rate limits (e.g., Gemini 3 Pro has 25/min)
	RateLimitPerMinute int `yaml:"rate_limit_per_minute" json:"rate_limit_per_minute"`
}

// LoadConfig loads configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from trusted caller config
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if config.Version == 0 {
		config.Version = 1
	}
	if config.Output.Format == "" {
		config.Output.Format = "json"
	}
	if config.Execution.MaxConcurrency == 0 {
		config.Execution.MaxConcurrency = 10
	}

	return &config, nil
}

// LoadConfigFromBytes loads configuration from YAML bytes.
func LoadConfigFromBytes(data []byte) (*Config, error) {
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if config.Version == 0 {
		config.Version = 1
	}
	if config.Output.Format == "" {
		config.Output.Format = "json"
	}
	if config.Execution.MaxConcurrency == 0 {
		config.Execution.MaxConcurrency = 10
	}

	return &config, nil
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		Version:    1,
		Evaluators: make(map[string]EvaluatorConfig),
		Datasets:   []DatasetConfig{},
		Output: OutputConfig{
			Format: "json",
			Pretty: true,
		},
		Execution: ExecutionConfig{
			Parallel:       true,
			MaxConcurrency: 10,
			Timeout:        "5m",
		},
	}
}
