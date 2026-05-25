package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// JSONDataset implements Dataset for JSON files.
type JSONDataset struct {
	name     string
	examples []Example
}

// NewJSONDataset creates a new JSON dataset from a file.
func NewJSONDataset(name, path string) (*JSONDataset, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from trusted caller config
	if err != nil {
		return nil, fmt.Errorf("failed to read dataset file: %w", err)
	}

	var examples []Example
	if err := json.Unmarshal(data, &examples); err != nil {
		return nil, fmt.Errorf("failed to parse dataset file: %w", err)
	}

	return &JSONDataset{
		name:     name,
		examples: examples,
	}, nil
}

// NewJSONDatasetFromBytes creates a new JSON dataset from bytes.
func NewJSONDatasetFromBytes(name string, data []byte) (*JSONDataset, error) {
	var examples []Example
	if err := json.Unmarshal(data, &examples); err != nil {
		return nil, fmt.Errorf("failed to parse dataset: %w", err)
	}

	return &JSONDataset{
		name:     name,
		examples: examples,
	}, nil
}

// NewJSONDatasetFromExamples creates a new JSON dataset from examples.
func NewJSONDatasetFromExamples(name string, examples []Example) *JSONDataset {
	return &JSONDataset{
		name:     name,
		examples: examples,
	}
}

// Name returns the dataset name.
func (d *JSONDataset) Name() string {
	return d.name
}

// Load returns all examples in the dataset.
func (d *JSONDataset) Load(ctx context.Context) ([]Example, error) {
	return d.examples, nil
}

// Len returns the number of examples.
func (d *JSONDataset) Len() int {
	return len(d.examples)
}

// Get returns a specific example by index.
func (d *JSONDataset) Get(idx int) (*Example, error) {
	if idx < 0 || idx >= len(d.examples) {
		return nil, fmt.Errorf("index %d out of range [0, %d)", idx, len(d.examples))
	}
	return &d.examples[idx], nil
}

// LoadDataset loads a dataset based on the config.
func LoadDataset(config DatasetConfig) (Dataset, error) {
	switch config.Type {
	case "json", "":
		return NewJSONDataset(config.Name, config.Path)
	default:
		return nil, fmt.Errorf("unsupported dataset type: %s", config.Type)
	}
}

// LoadDatasets loads all datasets from the config.
func LoadDatasets(config *Config) ([]Dataset, error) {
	datasets := make([]Dataset, 0, len(config.Datasets))
	for _, datasetConfig := range config.Datasets {
		dataset, err := LoadDataset(datasetConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to load dataset %s: %w", datasetConfig.Name, err)
		}
		datasets = append(datasets, dataset)
	}
	return datasets, nil
}
