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
	"fmt"
	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"strings"
)

const (
	DefaultAntflyURL        = "http://localhost:8080/db/v1"
	DefaultInferenceURL     = "http://localhost:8080"
	DefaultFullTextIndex    = "full_text_index_v0"
	DefaultEmbeddingIndex   = "embeddings"
	DefaultEmbeddingModel   = "antflydb/clipclap"
	DefaultEmbeddingPull    = "antflydb/clipclap:gguf:Q4_K"
	DefaultEmbeddingDims    = 512
	DefaultChunkerModel     = "fixed-bert-tokenizer"
	DefaultOCRModel         = "microsoft/Florence-2-base-ft"
	DefaultRecognizerModel  = "antflydb/gliner2-base-v1"
	DefaultAutographIndex   = "autograph_relations"
	DefaultAutographAsset   = "relations_v1"
	DefaultAutographModel   = DefaultRecognizerModel
	DefaultGeneratorModel   = "ggml-org/gemma-4-E4B-it-GGUF"
	DefaultArtifactProducer = "extractor"
	DefaultEntityLabels     = "person,organization,location,date,case,document,facility,aircraft"
	DefaultRelationLabels   = "associated with,communicated with,traveled to,visited,worked for,represented by,mentioned in,located in"
	DefaultEntityMaxChars   = 3000
	DefaultEntityOverlap    = 300
)

type StringSliceFlag []string

func (s *StringSliceFlag) String() string     { return strings.Join(*s, ", ") }
func (s *StringSliceFlag) Set(v string) error { *s = append(*s, v); return nil }
func inferenceMLBaseURL(raw string) (string, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return "", fmt.Errorf("inference URL is required")
	}
	if strings.HasSuffix(trimmed, "/ai/v1") {
		return trimmed, nil
	}
	if strings.HasSuffix(trimmed, "/api") {
		return "", fmt.Errorf("legacy Inference /api URLs are unsupported; use the Antfly root URL or a /ai/v1 URL")
	}
	root := strings.TrimRight(antfly.NormalizeBaseURL(trimmed), "/")
	if root == "" {
		return "", fmt.Errorf("inference URL is required")
	}
	if strings.HasSuffix(root, "/ai/v1") {
		return root, nil
	}
	return root + "/ai/v1", nil
}

func createArtifactGraphIndex(indexName, artifactName, producerType, model, inferenceURL string, labels, relationLabels []string) (*antfly.IndexConfig, error) {
	if strings.TrimSpace(indexName) == "" {
		return nil, fmt.Errorf("artifact graph index name is required")
	}
	if strings.TrimSpace(artifactName) == "" {
		return nil, fmt.Errorf("artifact name is required")
	}
	producerType = strings.TrimSpace(strings.ToLower(producerType))
	if producerType == "" {
		producerType = DefaultArtifactProducer
	}
	if producerType != "extractor" && producerType != "generator" {
		return nil, fmt.Errorf("artifact producer must be extractor or generator")
	}
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("artifact extractor model is required")
	}
	inferenceAPIURL := ""
	var err error
	if strings.TrimSpace(inferenceURL) != "" {
		inferenceAPIURL, err = inferenceMLBaseURL(inferenceURL)
		if err != nil {
			return nil, fmt.Errorf("invalid artifact inference URL: %w", err)
		}
	}
	if len(labels) == 0 {
		return nil, fmt.Errorf("at least one artifact entity label is required")
	}
	if len(relationLabels) == 0 {
		return nil, fmt.Errorf("at least one artifact relation label is required")
	}

	relationEnum := append([]string(nil), relationLabels...)
	labelEnum := append([]string(nil), labels...)
	relationSchemas := make([]map[string]any, 0, len(relationLabels))
	for _, label := range relationLabels {
		relationSchemas = append(relationSchemas, map[string]any{"type": label})
	}

	producerJSON := artifactProducerConfig(producerType, model, inferenceAPIURL, labelEnum, relationEnum, relationSchemas)
	if inferenceAPIURL == "" {
		delete(producerJSON["config"].(map[string]any), "api_url")
	}

	graphSource := antfly.GraphArtifactSourceConfig{
		Artifact: artifactName,
		Path:     "$.relations[*]",
		Format:   antfly.GraphArtifactSourceConfigFormatExtractionRelation,
	}
	if producerType == "extractor" {
		target, err := antfly.NewGraphTemplateValue("{{ _item.target.text }}")
		if err != nil {
			return nil, fmt.Errorf("build graph target mapping: %w", err)
		}
		weight, err := antfly.NewGraphTemplateValue("{{ _item.score }}")
		if err != nil {
			return nil, fmt.Errorf("build graph weight mapping: %w", err)
		}
		graphSource.Nodes = antfly.GraphArtifactNodeMappingConfig{
			Model:  antfly.GraphArtifactNodeMappingConfigModelDocument,
			Target: target,
		}
		graphSource.Edge = antfly.GraphArtifactEdgeMappingConfig{
			Weight: weight,
			Metadata: map[string]any{
				"type":          "{{ _item.type }}",
				"source_entity": "{{ _item.source.text }}",
				"target_entity": "{{ _item.target.text }}",
				"score":         "{{ _item.score }}",
			},
		}
	}
	graphSources, err := antfly.NewGraphIndexSources(graphSource)
	if err != nil {
		return nil, fmt.Errorf("build graph artifact source: %w", err)
	}
	return antfly.NewIndexConfig(indexName, antfly.GraphIndexConfig{
		Source: graphSources[0],
		Artifact: antfly.GraphArtifactProducerConfig{
			Name: artifactName,
			Kind: antfly.GraphArtifactProducerConfigKindAsset,
			Source: antfly.GraphArtifactProducerSourceConfig{
				Type:  antfly.GraphArtifactProducerSourceConfigTypeField,
				Value: "content",
			},
			ContentType:  "application/json",
			ProducerJson: producerJSON,
		},
		AlgebraicPlanning: antfly.GraphAlgebraicPlanningConfig{
			BoundedTraversal: antfly.GraphBoundedTraversalConfig{
				Law: antfly.GraphBoundedTraversalConfigLawProvenanceSemiring,
			},
		},
	})
}

func artifactProducerConfig(producerType, model, inferenceAPIURL string, labels, relationLabels []string, relationSchemas []map[string]any) map[string]any {
	if producerType == "extractor" {
		return map[string]any{
			"type": "extractor",
			"config": map[string]any{
				"provider": "antfly",
				"model":    model,
				"api_url":  inferenceAPIURL,
				"schema": map[string]any{
					"entities":  labels,
					"relations": relationSchemas,
				},
				"options": map[string]any{
					"include_confidence": true,
					"include_spans":      true,
					"long_document":      map[string]any{"mode": "window"},
				},
			},
		}
	}
	return map[string]any{
		"type": "generator",
		"config": map[string]any{
			"provider":    "antfly",
			"model":       model,
			"api_url":     inferenceAPIURL,
			"tool_output": "arguments",
			"tool_name":   "emit_relations",
			"tool_choice": map[string]any{
				"type": "function",
				"function": map[string]any{
					"name": "emit_relations",
				},
			},
			"tools": []map[string]any{
				{
					"type": "function",
					"function": map[string]any{
						"name":        "emit_relations",
						"description": "Extract autograph, signature, sender, recipient, and related entity relationships from the document text. Return only relations directly supported by the text.",
						"parameters": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []string{"relations"},
							"properties": map[string]any{
								"relations": map[string]any{
									"type": "array",
									"items": map[string]any{
										"type":                 "object",
										"additionalProperties": false,
										"required":             []string{"type", "target"},
										"properties": map[string]any{
											"type": map[string]any{
												"type": "string",
												"enum": relationLabels,
											},
											"source": map[string]any{
												"type":        "object",
												"description": "Optional source endpoint. Omit this to use the current document as the source.",
												"properties": map[string]any{
													"id":    map[string]any{"type": "string"},
													"label": map[string]any{"type": "string", "enum": labels},
													"text":  map[string]any{"type": "string"},
												},
											},
											"target": map[string]any{
												"type":        "object",
												"description": "Target endpoint for the relation. Use id for the normalized entity name.",
												"required":    []string{"id"},
												"properties": map[string]any{
													"id":    map[string]any{"type": "string"},
													"label": map[string]any{"type": "string", "enum": labels},
													"text":  map[string]any{"type": "string"},
												},
											},
											"confidence": map[string]any{
												"type":    "number",
												"minimum": 0,
												"maximum": 1,
											},
											"evidence": map[string]any{
												"type":        "string",
												"description": "Short text span or explanation supporting the relation.",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
