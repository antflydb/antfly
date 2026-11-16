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

package sources

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// PreviewProcessing constructs native declarations without writes or provider
// calls. Provider-specific model/prompt settings remain ordinary native config.
func PreviewProcessing(input management.ProcessingPreset) (management.ProcessingPreview, error) {
	var out management.ProcessingPreview
	preset := TextProcessingPreset{Name: input.Name, Purpose: string(input.Kind), Field: "content.text"}
	switch input.Kind {
	case "content":
		if len(input.ProducerConfig) > 0 {
			return out, fmt.Errorf("content copy has no producer configuration")
		}
	case "extract":
		preset.Field = ""
		preset.ResourceField = "content.resource"
		preset.Producer = map[string]any{"type": "document_extraction", "config": input.ProducerConfig}
	case "transcription":
		preset.Field = ""
		preset.ResourceField = "content.resource"
		preset.Producer = map[string]any{"type": "transcriber", "config": input.ProducerConfig}
	case "caption":
		preset.Field = ""
		preset.ResourceField = "content.resource"
		preset.Producer = map[string]any{"type": "generator", "config": input.ProducerConfig}
	case "summary":
		preset.Producer = map[string]any{"type": "generator", "config": input.ProducerConfig}
	default:
		return out, fmt.Errorf("unsupported processing preset")
	}
	if len(input.Embedder) > 0 {
		metric := string(input.Metric)
		if metric == "" {
			metric = "cosine"
		}
		space, err := NativeVectorSpace(input.Name, input.Embedder, input.Dimension, metric, "text")
		if err != nil {
			return out, err
		}
		preset.VectorSpace = &space
		preset.Embedder = input.Embedder
	}
	plan, stream, artifact, err := BuildTextProcessing(preset)
	return management.ProcessingPreview{Processing: plan, Stream: stream, Artifact: artifact}, err
}

// ProcessingJobPath validates ownership against the source's declared artifact
// set. Native repair is table-wide: callers must authorize table administration,
// including status reads, rather than imply a source-row filter is supported.
func ProcessingJobPath(source management.Source, input management.ProcessingJobRequest) (string, string, any, error) {
	if !validBindingName(input.Artifact) {
		return "", "", nil, fmt.Errorf("invalid artifact")
	}
	declared := false
	if source.Knowledge != nil {
		for _, g := range source.Knowledge.Graphs {
			for _, a := range g.Artifacts {
				if a == input.Artifact {
					declared = true
				}
			}
		}
		for _, e := range source.Knowledge.Entities {
			if e.SourceArtifact == input.Artifact {
				declared = true
			}
		}
	}
	for _, artifact := range source.Artifacts {
		if artifact.Artifact == input.Artifact {
			declared = true
		}
	}
	for _, stream := range source.Streams {
		if stream.Artifact == input.Artifact {
			declared = true
		}
		if stream.Retrieval != nil && stream.Retrieval.Semantic != nil {
			for _, member := range stream.Retrieval.Semantic.MemberArtifacts {
				if member == input.Artifact {
					declared = true
				}
			}
		}
	}
	if !declared {
		return "", "", nil, fmt.Errorf("artifact is not declared by this source")
	}
	if input.Limit == 0 {
		input.Limit = 64
	}
	if input.Limit < 1 || input.Limit > 256 {
		return "", "", nil, fmt.Errorf("invalid job page limit")
	}
	path := tablePath(source.Table) + "/artifacts/" + url.PathEscape(input.Artifact) + "/reprocess-jobs"
	if input.Action == "start" {
		if input.JobId != "" {
			return "", "", nil, fmt.Errorf("new jobs cannot name an existing job")
		}
		return http.MethodPost, path, map[string]any{"advance": false, "limit": input.Limit}, nil
	}
	if _, err := strconv.ParseUint(input.JobId, 10, 64); err != nil {
		return "", "", nil, fmt.Errorf("invalid processing job identity")
	}
	path += "/" + input.JobId
	switch input.Action {
	case "status":
		return http.MethodGet, path, nil, nil
	case "advance":
		return http.MethodPost, path + "/advance", nil, nil
	case "cancel":
		return http.MethodPost, path + "/cancel", map[string]any{}, nil
	default:
		return "", "", nil, fmt.Errorf("invalid processing action")
	}
}
func (p *Publisher) ProcessingJob(ctx context.Context, source management.Source, input management.ProcessingJobRequest) (map[string]any, error) {
	method, path, body, err := ProcessingJobPath(source, input)
	if err != nil {
		return nil, management.Failure(400, "invalid_processing_job")
	}
	raw, err := p.call(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	return DecodeProcessingJob(raw)
}
func (b localManagement) PreviewProcessing(ctx context.Context, id string, input management.ProcessingPreset) (management.ProcessingPreview, error) {
	if _, err := b.GetSource(ctx, id); err != nil {
		return management.ProcessingPreview{}, err
	}
	result, err := PreviewProcessing(input)
	if err != nil {
		return result, management.Failure(400, "invalid_processing_preset")
	}
	return result, nil
}
func (b localManagement) ProcessingJob(ctx context.Context, id string, input management.ProcessingJobRequest) (map[string]any, error) {
	source, err := b.GetSource(ctx, id)
	if err != nil {
		return nil, err
	}
	return b.searchPublisher().ProcessingJob(ctx, source, input)
}

// Native job IDs are u64. Expose them as decimal strings across source clients
// so JavaScript never rounds the identity before a continuation or cancellation.
func DecodeProcessingJob(raw []byte) (map[string]any, error) {
	if len(raw) > management.MaxResponseBytes {
		return nil, fmt.Errorf("processing response exceeds budget")
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if id, ok := result["job_id"].(json.Number); ok {
		result["job_id"] = id.String()
	}
	return result, nil
}
