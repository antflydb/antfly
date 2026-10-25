// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func corpusEmbedderConfig(model, filename string) (map[string]any, error) {
	if filename == "" {
		return map[string]any{"provider": "antfly", "model": model, "dimension": DefaultEmbeddingDims}, nil
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if len(data) > 64<<10 {
		return nil, fmt.Errorf("embedder config exceeds 64 KiB")
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	if err := validateCorpusEmbedder(config); err != nil {
		return nil, err
	}
	return config, nil
}

func validateCorpusEmbedder(config map[string]any) error {
	if config == nil {
		return fmt.Errorf("embedder config must be an object")
	}
	for _, key := range []string{"api_key", "bearer_token", "access_token", "credentials", "credentials_json", "credentials_path", "headers"} {
		if _, present := config[key]; present {
			return fmt.Errorf("embedder config must use server-managed credentials or ADC, not %s", key)
		}
	}
	provider, _ := config["provider"].(string)
	model, _ := config["model"].(string)
	if provider != "vertex" && provider != "antfly" {
		return fmt.Errorf("corpus embedder must use vertex or antfly")
	}
	if strings.TrimSpace(model) == "" {
		return fmt.Errorf("embedding model is required")
	}
	if model == "gemini-embedding-002" {
		return fmt.Errorf("the Vertex model ID is gemini-embedding-2, not gemini-embedding-002")
	}
	if _, present := config["dimensions"]; present {
		return fmt.Errorf("use dimension consistently for the native embedder and index")
	}
	dims := corpusEmbeddingDimensions(config)
	if dims < 1 || dims > 65536 {
		return fmt.Errorf("embedder dimension must be an integer between 1 and 65536")
	}
	if provider == "vertex" {
		project, _ := config["project_id"].(string)
		location, _ := config["location"].(string)
		if strings.TrimSpace(project) == "" || strings.TrimSpace(location) == "" {
			return fmt.Errorf("Vertex embedder requires explicit project_id and location")
		}
		if model == "gemini-embedding-2" && location != "global" && location != "us" && location != "eu" {
			return fmt.Errorf("gemini-embedding-2 requires global, us, or eu location")
		}
	}
	return nil
}

func corpusEmbeddingDimensions(config map[string]any) int {
	switch value := config["dimension"].(type) {
	case int:
		return value
	case float64:
		if value >= 1 && value <= 65536 && float64(int(value)) == value {
			return int(value)
		}
	}
	return 0
}
