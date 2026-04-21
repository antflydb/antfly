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
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"go.uber.org/zap"
)

// extractFromText processes a single text against all schemas using the pipeline.
func extractFromText(
	ctx context.Context,
	pipeline *pipelines.GLiNERPipeline,
	text string,
	schemas []ExtractionSchema,
	config ExtractionConfig,
	logger *zap.Logger,
) (ExtractionResult, error) {
	result := make(ExtractionResult, len(schemas))

	for _, schema := range schemas {
		instances, err := extractStructure(ctx, pipeline, text, schema, config, logger)
		if err != nil {
			return nil, err
		}
		result[schema.Name] = instances
	}

	return result, nil
}

// extractStructure extracts instances of a single structure from text.
func extractStructure(
	ctx context.Context,
	pipeline *pipelines.GLiNERPipeline,
	text string,
	schema ExtractionSchema,
	config ExtractionConfig,
	logger *zap.Logger,
) ([]ExtractedInstance, error) {
	// Collect field names as NER labels
	labels := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		labels[i] = field.Name
	}

	// Extract spans using field names as labels
	spans, err := pipeline.ExtractSpansForLabels(ctx, text, labels, config.Threshold, config.FlatNER)
	if err != nil {
		return nil, err
	}

	if len(spans) == 0 {
		return []ExtractedInstance{}, nil
	}

	// Group spans by field label
	spansByField := make(map[string][]pipelines.NERExtractedSpan)
	for _, span := range spans {
		spansByField[span.Label] = append(spansByField[span.Label], span)
	}

	// Handle choice fields: classify extracted text against choices
	for _, field := range schema.Fields {
		if len(field.Choices) == 0 {
			continue
		}
		fieldSpans, ok := spansByField[field.Name]
		if !ok || len(fieldSpans) == 0 {
			continue
		}
		// For choice fields, classify the top span text against choices
		bestSpan := fieldSpans[0]
		for _, s := range fieldSpans[1:] {
			if s.Score > bestSpan.Score {
				bestSpan = s
			}
		}

		// Short-circuit: if the span text exactly matches a choice (case-insensitive),
		// use the canonical choice spelling directly and skip the inference call.
		exactMatch := false
		for _, c := range field.Choices {
			if strings.EqualFold(bestSpan.Text, c) {
				spansByField[field.Name] = []pipelines.NERExtractedSpan{{
					Text:  c,
					Label: field.Name,
					Start: bestSpan.Start,
					End:   bestSpan.End,
					Score: bestSpan.Score,
				}}
				exactMatch = true
				break
			}
		}
		if exactMatch {
			continue
		}

		choice, score, err := pipeline.ClassifySpanText(ctx, bestSpan.Text, field.Choices)
		if err != nil {
			logger.Warn("ClassifySpanText failed, using raw span text",
				zap.String("field", field.Name),
				zap.String("spanText", bestSpan.Text),
				zap.Error(err))
			continue
		}
		// Replace span text with the classified choice
		spansByField[field.Name] = []pipelines.NERExtractedSpan{
			{
				Text:  choice,
				Label: field.Name,
				Start: bestSpan.Start,
				End:   bestSpan.End,
				Score: score,
			},
		}
	}

	// Assemble into instances using positional clustering
	instances := assembleInstances(schema, spansByField, config, len(text))

	return instances, nil
}

// assembleInstances groups extracted spans into structured instances.
// Uses positional clustering: spans close together form one instance.
func assembleInstances(
	schema ExtractionSchema,
	spansByField map[string][]pipelines.NERExtractedSpan,
	config ExtractionConfig,
	textLength int,
) []ExtractedInstance {
	// Sort spans by position for each field
	for field := range spansByField {
		slices.SortFunc(spansByField[field], func(a, b pipelines.NERExtractedSpan) int {
			return cmp.Compare(a.Start, b.Start)
		})
	}

	// Collect all spans to determine instance boundaries
	var allSpans []pipelines.NERExtractedSpan
	for _, fieldSpans := range spansByField {
		allSpans = append(allSpans, fieldSpans...)
	}

	if len(allSpans) == 0 {
		return []ExtractedInstance{}
	}

	slices.SortFunc(allSpans, func(a, b pipelines.NERExtractedSpan) int {
		return cmp.Compare(a.Start, b.Start)
	})

	// Cluster spans into instances based on positional gaps.
	// A gap larger than the median span distance starts a new instance.
	clusters := clusterSpans(allSpans, config.ClusterGap, textLength)

	// Build one instance per cluster
	instances := make([]ExtractedInstance, 0, len(clusters))
	for _, cluster := range clusters {
		instance := buildInstance(schema, cluster, config)
		if len(instance) > 0 {
			instances = append(instances, instance)
		}
	}

	// If no clusters produced instances, try building a single instance from all spans
	if len(instances) == 0 {
		instance := buildInstance(schema, allSpans, config)
		if len(instance) > 0 {
			instances = append(instances, instance)
		}
	}

	return instances
}

// clusterSpans groups spans into clusters based on positional proximity.
// clusterGap overrides the adaptive threshold when > 0.
// textLength is used for the adaptive floor calculation.
func clusterSpans(spans []pipelines.NERExtractedSpan, clusterGap int, textLength int) [][]pipelines.NERExtractedSpan {
	if len(spans) <= 1 {
		return [][]pipelines.NERExtractedSpan{spans}
	}

	// Calculate gaps between consecutive spans
	gaps := make([]int, len(spans)-1)
	for i := 0; i < len(spans)-1; i++ {
		gap := max(spans[i+1].Start-spans[i].End, 0)
		gaps[i] = gap
	}

	// Use a threshold: gaps significantly larger than the median suggest a new instance.
	// If clusterGap is explicitly set, use it; otherwise adapt to the text length.
	medianGap := medianInt(gaps)
	var threshold int
	if clusterGap > 0 {
		threshold = clusterGap
	} else {
		minGap := min(100, textLength/10)
		threshold = max(medianGap*3, minGap)
	}

	// Split into clusters at large gaps
	var clusters [][]pipelines.NERExtractedSpan
	currentCluster := []pipelines.NERExtractedSpan{spans[0]}

	for i := range gaps {
		if gaps[i] > threshold {
			clusters = append(clusters, currentCluster)
			currentCluster = []pipelines.NERExtractedSpan{spans[i+1]}
		} else {
			currentCluster = append(currentCluster, spans[i+1])
		}
	}
	clusters = append(clusters, currentCluster)

	return clusters
}

// buildInstance creates a single ExtractedInstance from a cluster of spans.
func buildInstance(
	schema ExtractionSchema,
	spans []pipelines.NERExtractedSpan,
	config ExtractionConfig,
) ExtractedInstance {
	// Index spans by label
	spansByLabel := make(map[string][]pipelines.NERExtractedSpan)
	for _, span := range spans {
		spansByLabel[span.Label] = append(spansByLabel[span.Label], span)
	}

	instance := make(ExtractedInstance)

	for _, field := range schema.Fields {
		fieldSpans, ok := spansByLabel[field.Name]
		if !ok || len(fieldSpans) == 0 {
			continue
		}

		switch field.Type {
		case FieldTypeStr:
			// Keep only the top-scoring span
			best := fieldSpans[0]
			for _, s := range fieldSpans[1:] {
				if s.Score > best.Score {
					best = s
				}
			}
			instance[field.Name] = spanToFieldValue(best, config)

		case FieldTypeList:
			// Keep all spans, sorted by position
			slices.SortFunc(fieldSpans, func(a, b pipelines.NERExtractedSpan) int {
				return cmp.Compare(a.Start, b.Start)
			})
			values := make([]ExtractedFieldValue, len(fieldSpans))
			for i, s := range fieldSpans {
				values[i] = spanToFieldValue(s, config)
			}
			instance[field.Name] = values
		}
	}

	return instance
}

// spanToFieldValue converts a span to an ExtractedFieldValue.
func spanToFieldValue(span pipelines.NERExtractedSpan, config ExtractionConfig) ExtractedFieldValue {
	v := ExtractedFieldValue{
		Value: span.Text,
	}
	if config.IncludeConfidence {
		v.Score = span.Score
	}
	if config.IncludeSpans {
		start := span.Start
		end := span.End
		v.Start = &start
		v.End = &end
	}
	return v
}

// medianInt returns the median of an int slice.
func medianInt(values []int) int {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]int, len(values))
	copy(sorted, values)
	slices.Sort(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}
