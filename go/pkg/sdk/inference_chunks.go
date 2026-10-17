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

package sdk

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// ChunkAndEmbed plans model-sized chunks and preserves typed source provenance.
// Inspect Status and NextContinuation before declaring a source fully covered.
func (c *InferenceClient) ChunkAndEmbed(ctx context.Context, req oapi.InferenceChunkRequest) (*oapi.ChunkEmbeddingResponse, error) {
	if req.Embedding.Model == "" {
		return nil, errors.New("chunk embedding target is required")
	}
	req.Stream = false
	response, err := c.client.ChunkText(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, readInferenceAPIError(response)
	}
	body, truncated, err := readLimitedBody(response.Body, 16<<20)
	if err != nil {
		return nil, err
	}
	if truncated {
		return nil, errors.New("chunk embedding response exceeded 16 MiB; use streaming")
	}
	var result oapi.ChunkEmbeddingResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if result.Object != "chunk.embedding.list" {
		return nil, errors.New("unexpected chunk embedding response")
	}
	return &result, nil
}

// ChunkAndEmbedStream delivers each descriptor/vector pair with backpressure.
// Only the returned terminal record confirms coverage; EOF is an error. A
// callback error cancels delivery without acknowledging later chunks. The optional
// onStart consumer receives source/model context before any chunk callback.
func (c *InferenceClient) ChunkAndEmbedStream(ctx context.Context, req oapi.InferenceChunkRequest, accept func(oapi.ChunkEmbeddingObject) error, onStart ...func(oapi.ChunkEmbeddingContext) error) (*oapi.ChunkEmbeddingResponse, error) {
	if req.Embedding.Model == "" || accept == nil || len(onStart) > 1 {
		return nil, errors.New("chunk target and consumer are required")
	}
	req.Stream = true
	response, err := c.client.ChunkText(ctx, req, func(_ context.Context, r *http.Request) error {
		r.Header.Set("Accept", "application/x-ndjson")
		return nil
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, readInferenceAPIError(response)
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || contentType != "application/x-ndjson" {
		return nil, errors.New("expected chunk embedding NDJSON stream")
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	var header *oapi.ChunkEmbeddingContext
	var plan string
	var next int64
	if req.Continuation != "" {
		prefix, ordinal, ok := strings.Cut(req.Continuation, ":")
		if !ok {
			return nil, errors.New("invalid chunk continuation")
		}
		plan = prefix
		next, err = strconv.ParseInt(ordinal, 10, 64)
		if err != nil || next < 0 {
			return nil, errors.New("invalid chunk continuation")
		}
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var event struct {
			Context  *oapi.ChunkEmbeddingContext  `json:"context"`
			Object   string                       `json:"object"`
			Data     *oapi.ChunkEmbeddingObject   `json:"data"`
			Response *oapi.ChunkEmbeddingResponse `json:"response"`
			Code     string                       `json:"code"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		switch event.Object {
		case "chunk.embedding.start":
			h := event.Context
			if header != nil || h == nil || len(h.PlanId) != 64 || len(h.ModelIdentity) != 64 || len(h.ModelRevision) != 64 || len(h.SourceFingerprint) != 64 || h.Model != req.Embedding.Model || h.StartOrdinal != next || (plan != "" && h.PlanId != plan) || (req.Embedding.ModelIdentity != "" && req.Embedding.ModelIdentity != h.ModelIdentity) {
				return nil, errors.New("invalid chunk stream context")
			}
			for _, digest := range []string{h.PlanId, h.ModelIdentity, h.ModelRevision, h.SourceFingerprint} {
				if _, err := hex.DecodeString(digest); err != nil || strings.ToLower(digest) != digest {
					return nil, errors.New("invalid chunk stream context identity")
				}
			}
			header = h
			plan = h.PlanId
			if len(onStart) == 1 && onStart[0] != nil {
				if err := onStart[0](*h); err != nil {
					return nil, err
				}
			}
		case "chunk.embedding":
			if header == nil {
				return nil, errors.New("missing chunk stream context")
			}
			if event.Data == nil {
				return nil, errors.New("missing chunk event payload")
			}
			descriptor := event.Data.Descriptor
			prefix, ordinal, ok := strings.Cut(descriptor.Id, ":")
			if !ok || len(prefix) != 64 || ordinal != strconv.FormatInt(next, 10) || descriptor.Ordinal != next || (plan != "" && prefix != plan) {
				return nil, errors.New("invalid chunk identity or order")
			}
			plan = prefix
			if err := accept(*event.Data); err != nil {
				return nil, err
			}
			next++
		case "chunk.embedding.complete":
			terminal := event.Response
			if header == nil || terminal == nil || terminal.Model != header.Model || terminal.ModelIdentity != header.ModelIdentity || terminal.ModelRevision != header.ModelRevision || terminal.SourceFingerprint != header.SourceFingerprint || terminal.SourceVersion != header.SourceVersion || terminal.TaskType != header.TaskType || terminal.StartOrdinal != header.StartOrdinal || (header.Dimensions != 0 && terminal.Dimensions != header.Dimensions) || terminal.Object != "chunk.embedding.list" || terminal.CoveredThroughOrdinal != next || (plan != "" && terminal.PlanId != plan) {
				return nil, errors.New("chunk terminal coverage mismatch")
			}
			return terminal, nil
		case "chunk.embedding.error":
			return nil, fmt.Errorf("chunk stream initialization failed: %s", event.Code)
		default:
			return nil, errors.New("unknown chunk stream event")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("chunk stream ended before terminal coverage")
}
