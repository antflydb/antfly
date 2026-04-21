// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// NERConfig contains model configuration for NER models.
type NERConfig struct {
	// Labels is the list of entity labels (e.g., ["O", "B-PER", "I-PER", ...])
	Labels []string `json:"labels"`

	// ID2Label maps label indices to label strings
	ID2Label map[string]string `json:"id2label"`

	// Label2ID maps label strings to indices
	Label2ID map[string]int `json:"label2id"`
}

// LoadNERConfig loads NER configuration from the model directory.
// It first tries to load ner_config.json, then falls back to config.json.
func LoadNERConfig(modelPath string) (*NERConfig, error) {
	// Try ner_config.json first (custom Termite format)
	nerConfigPath := filepath.Join(modelPath, "ner_config.json")
	if _, err := os.Stat(nerConfigPath); err == nil {
		return loadNERConfigFromFile(nerConfigPath)
	}

	// Fall back to HuggingFace config.json
	configPath := filepath.Join(modelPath, "config.json")
	if _, err := os.Stat(configPath); err == nil {
		return loadHFConfig(configPath)
	}

	return nil, fmt.Errorf("no config.json or ner_config.json found in %s", modelPath)
}

// loadNERConfigFromFile loads ner_config.json format.
func loadNERConfigFromFile(path string) (*NERConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading ner config: %w", err)
	}

	var config NERConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing ner config: %w", err)
	}

	// Build ID2Label from Labels if not present
	if len(config.ID2Label) == 0 && len(config.Labels) > 0 {
		config.ID2Label = make(map[string]string, len(config.Labels))
		config.Label2ID = make(map[string]int, len(config.Labels))
		for i, label := range config.Labels {
			config.ID2Label[strconv.Itoa(i)] = label
			config.Label2ID[label] = i
		}
	}

	return &config, nil
}

// loadHFConfig loads labels from a HuggingFace config.json.
func loadHFConfig(path string) (*NERConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading HF config: %w", err)
	}

	// HuggingFace config.json structure
	var hfConfig struct {
		ID2Label map[string]string `json:"id2label"`
		Label2ID map[string]int    `json:"label2id"`
	}
	if err := json.Unmarshal(data, &hfConfig); err != nil {
		return nil, fmt.Errorf("parsing HF config: %w", err)
	}

	if len(hfConfig.ID2Label) == 0 {
		return nil, fmt.Errorf("no id2label found in config.json")
	}

	// Build labels list from id2label (sorted by ID)
	labels := make([]string, len(hfConfig.ID2Label))
	for idStr, label := range hfConfig.ID2Label {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		if id >= 0 && id < len(labels) {
			labels[id] = label
		}
	}

	return &NERConfig{
		Labels:   labels,
		ID2Label: hfConfig.ID2Label,
		Label2ID: hfConfig.Label2ID,
	}, nil
}

// NormalizeLabel normalizes BIO/BIOES labels to a standard form.
// Examples:
//   - "B-PER" -> "PER"
//   - "I-ORG" -> "ORG"
//   - "B-LOCATION" -> "LOC"
//   - "I-MISC" -> "MISC"
//   - "O" -> "" (outside)
func NormalizeLabel(label string) string {
	if label == "O" || label == "" {
		return ""
	}

	// Remove BIO prefix if present (B-, I-, E-, S-)
	if len(label) >= 2 && label[1] == '-' {
		label = label[2:]
	}

	// Normalize common label variations
	label = strings.ToUpper(label)
	switch label {
	case "PERSON", "PEOPLE":
		return "PER"
	case "ORGANIZATION", "ORGANISATIONS", "COMPANY":
		return "ORG"
	case "LOCATION", "PLACE", "GPE":
		return "LOC"
	case "MISCELLANEOUS":
		return "MISC"
	default:
		return label
	}
}

// IsBIOBegin checks if a label is a beginning token (B-).
func IsBIOBegin(label string) bool {
	return len(label) >= 2 && label[0] == 'B' && label[1] == '-'
}

// IsBIOInside checks if a label is an inside token (I-).
func IsBIOInside(label string) bool {
	return len(label) >= 2 && label[0] == 'I' && label[1] == '-'
}

// IsBIOOutside checks if a label is an outside token (O).
func IsBIOOutside(label string) bool {
	return label == "O" || label == ""
}

// GetLabelType extracts the entity type from a BIO label.
// Returns empty string for O labels.
func GetLabelType(label string) string {
	if IsBIOOutside(label) {
		return ""
	}
	if len(label) >= 2 && label[1] == '-' {
		return label[2:]
	}
	return label
}
