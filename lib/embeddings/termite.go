// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//go:generate protoc --go_out=. --go_opt=paths=source_relative termite.proto

package embeddings

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/termite/pkg/client"
	"github.com/antflydb/termite/pkg/client/oapi"
	"golang.org/x/time/rate"
)

type TermiteClient struct {
	client *client.TermiteClient
	model  string
	caps   EmbedderCapabilities
}

func NewTermiteClient(url string, model string, configCaps *EmbedderCapabilities) (Embedder, error) {
	if model == "" {
		return nil, fmt.Errorf("model is required for Termite embeddings")
	}

	httpClient := &http.Client{Timeout: time.Second * 540}
	termiteClient, err := client.NewTermiteClient(url, httpClient)
	if err != nil {
		return nil, fmt.Errorf("creating termite client: %w", err)
	}

	return &TermiteClient{
		client: termiteClient,
		model:  model,
		caps:   ResolveCapabilities(model, configCaps),
	}, nil
}

func (p *TermiteClient) Capabilities() EmbedderCapabilities {
	return p.caps
}

func (p *TermiteClient) RateLimiter() *rate.Limiter {
	return nil // local inference, no rate limit
}

func (p *TermiteClient) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	// If the model is text-only or all inputs are text, use the efficient text path
	if p.caps.IsTextOnly() || allText(contents) {
		values := ExtractText(contents)
		return p.client.Embed(ctx, p.model, values)
	}

	// Multimodal path: convert ai.ContentPart to oapi.ContentPart
	parts, err := convertToOAPIContentParts(contents)
	if err != nil {
		return nil, fmt.Errorf("converting content parts: %w", err)
	}

	return p.client.EmbedMultimodal(ctx, p.model, parts)
}

// SparseEmbed generates sparse (SPLADE-style) embeddings via the Termite service.
func (p *TermiteClient) SparseEmbed(ctx context.Context, texts []string) ([]SparseVector, error) {
	if len(texts) == 0 {
		return []SparseVector{}, nil
	}

	clientVecs, err := p.client.SparseEmbed(ctx, p.model, texts)
	if err != nil {
		return nil, fmt.Errorf("sparse embed via termite: %w", err)
	}

	// Convert client SparseVector to embeddings.SparseVector
	result := make([]SparseVector, len(clientVecs))
	for i, cv := range clientVecs {
		indices := make([]uint32, len(cv.Indices))
		for j, idx := range cv.Indices {
			indices[j] = uint32(idx) //nolint:gosec // G115: int32 to uint32, indices are non-negative
		}
		result[i] = SparseVector{
			Indices: indices,
			Values:  cv.Values,
		}
	}
	return result, nil
}

// allText returns true if every input item contains only text content.
func allText(contents [][]ai.ContentPart) bool {
	for _, parts := range contents {
		for _, part := range parts {
			switch part.(type) {
			case ai.TextContent:
				// ok
			default:
				return false
			}
		}
	}
	return true
}

// convertToOAPIContentParts converts internal ai.ContentPart slices to oapi.ContentPart slices.
// Each input item ([]ai.ContentPart) becomes one oapi.ContentPart. For items with multiple parts,
// the first non-text part is used (text is used as fallback).
func convertToOAPIContentParts(contents [][]ai.ContentPart) ([]oapi.ContentPart, error) {
	result := make([]oapi.ContentPart, 0, len(contents))
	for _, parts := range contents {
		converted, err := convertSingleInput(parts)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, nil
}

// convertSingleInput converts a single input's content parts to one oapi.ContentPart.
func convertSingleInput(parts []ai.ContentPart) (oapi.ContentPart, error) {
	var out oapi.ContentPart

	for _, part := range parts {
		switch p := part.(type) {
		case ai.BinaryContent:
			dataURI := "data:" + p.MIMEType + ";base64," + base64.StdEncoding.EncodeToString(p.Data)
			if err := out.FromImageURLContentPart(oapi.ImageURLContentPart{
				Type: oapi.ImageURLContentPartTypeImageUrl,
				ImageUrl: oapi.ImageURL{
					Url: dataURI,
				},
			}); err != nil {
				return out, fmt.Errorf("creating image URL content part: %w", err)
			}
			return out, nil

		case ai.ImageURLContent:
			if err := out.FromImageURLContentPart(oapi.ImageURLContentPart{
				Type: oapi.ImageURLContentPartTypeImageUrl,
				ImageUrl: oapi.ImageURL{
					Url: p.URL,
				},
			}); err != nil {
				return out, fmt.Errorf("creating image URL content part: %w", err)
			}
			return out, nil

		case ai.TextContent:
			// Continue looking for non-text parts; use text as fallback
			if err := out.FromTextContentPart(oapi.TextContentPart{
				Type: oapi.TextContentPartTypeText,
				Text: p.Text,
			}); err != nil {
				return out, fmt.Errorf("creating text content part: %w", err)
			}
		}
	}

	return out, nil
}
