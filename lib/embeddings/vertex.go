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

package embeddings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	aiplatform "cloud.google.com/go/aiplatform/apiv1beta1"
	aiplatformpb "cloud.google.com/go/aiplatform/apiv1beta1/aiplatformpb"
	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vertex"
	"golang.org/x/time/rate"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// VertexAIEmbedderImpl implements embeddings using Google Cloud Vertex AI
type VertexAIEmbedderImpl struct {
	client         *aiplatform.PredictionClient
	embeddingModel string
	dimension      uint64
	location       string
	project        string
	caps           EmbedderCapabilities
	limiter        *rate.Limiter
}

// getConfigOrEnv returns the config value if set, otherwise checks environment variable
func getConfigOrEnv(configVal *string, envVar string) string {
	if configVal != nil && *configVal != "" {
		return *configVal
	}
	return os.Getenv(envVar)
}

// NewVertexAIEmbedder creates a new Vertex AI embedder with proper authentication
func NewVertexAIEmbedder(config EmbedderConfig) (Embedder, error) {
	// Validate config
	c, err := config.AsVertexEmbedderConfig()
	if err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	// Get project and location
	project := getConfigOrEnv(c.ProjectId, "GOOGLE_CLOUD_PROJECT")
	if project == "" {
		return nil, errors.New(
			"project_id is required (set in config or GOOGLE_CLOUD_PROJECT env var)",
		)
	}

	location := getConfigOrEnv(c.Location, "GOOGLE_CLOUD_LOCATION")
	if location == "" {
		location = "us-central1" // Default region
	}

	// Set up client options for authentication
	creds, err := vertex.LoadCredentials(c.CredentialsPath, []string{vertex.CloudPlatformScope})
	if err != nil {
		return nil, fmt.Errorf("resolving credentials: %w", err)
	}
	opts := []option.ClientOption{vertex.AuthClientOption(creds)}

	// Set API endpoint for the location
	apiEndpoint := location + "-aiplatform.googleapis.com:443"
	opts = append(opts, option.WithEndpoint(apiEndpoint))

	// Create Vertex AI prediction client
	ctx := context.Background()
	client, err := aiplatform.NewPredictionClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Vertex AI client: %w", err)
	}

	var dimension uint64
	if c.Dimension != nil {
		dimension = uint64(*c.Dimension) //nolint:gosec // G115: bounded value, cannot overflow in practice
	}

	return &VertexAIEmbedderImpl{
		client:         client,
		embeddingModel: c.Model,
		dimension:      dimension,
		project:        project,
		location:       location,
		caps:           ResolveCapabilities(c.Model, config.GetConfigCapabilities()),
		limiter:        rate.NewLimiter(VertexDefaultRPS, VertexDefaultRPS),
	}, nil
}

// Capabilities returns the capabilities of this embedder
func (v *VertexAIEmbedderImpl) Capabilities() EmbedderCapabilities {
	return v.caps
}

func (v *VertexAIEmbedderImpl) RateLimiter() *rate.Limiter {
	return v.limiter
}

// Embed generates embeddings for content using Vertex AI.
// Supports both text and multimodal content (images, audio, video).
func (v *VertexAIEmbedderImpl) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	// Route to appropriate embedding method
	if !allText(contents) {
		if v.caps.IsTextOnly() {
			return nil, fmt.Errorf(
				"binary content (images, audio, video) requires a multimodal model (set multimodal: true in config), got %s",
				v.embeddingModel,
			)
		}
		// The legacy Vertex Prediction API only supports multimodalembedding@001
		// for binary content. For gemini-embedding-2-preview on Vertex, use
		// provider: "gemini" with GOOGLE_GENAI_USE_VERTEXAI=1 instead.
		if !strings.HasPrefix(v.embeddingModel, "multimodalembedding") {
			return nil, fmt.Errorf(
				"the vertex provider only supports multimodalembedding models for binary content; "+
					"for %s on Vertex AI, use provider: \"gemini\" with GOOGLE_GENAI_USE_VERTEXAI=1",
				v.embeddingModel,
			)
		}
		return v.embedMultimodal(ctx, contents)
	}

	// Text-only embedding
	return v.embedText(ctx, contents)
}

// embedText generates embeddings for text-only content
func (v *VertexAIEmbedderImpl) embedText(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	values := ExtractText(contents)
	embeddings := make([][]float32, len(values))

	for i, text := range values {
		// Create prediction request for text embedding
		instance, err := structpb.NewValue(map[string]any{
			"content": text,
		})
		if err != nil {
			return nil, fmt.Errorf("constructing request payload: %w", err)
		}

		var params *structpb.Value
		if v.dimension > 0 {
			params, err = structpb.NewValue(map[string]any{
				"outputDimensionality": v.dimension,
			})
			if err != nil {
				return nil, fmt.Errorf("constructing request params: %w", err)
			}
		}

		endpoint := fmt.Sprintf(
			"projects/%s/locations/%s/publishers/google/models/%s",
			v.project,
			v.location,
			v.embeddingModel,
		)

		req := &aiplatformpb.PredictRequest{
			Endpoint:   endpoint,
			Instances:  []*structpb.Value{instance},
			Parameters: params,
		}

		resp, err := v.client.Predict(ctx, req)
		if err != nil {
			if strings.Contains(err.Error(), "resource exhausted") {
				return nil, &RateLimitError{
					Err:        fmt.Errorf("predicting embedding for text %d: %w", i, err),
					RetryAfter: 30 * time.Second,
				}
			}
			return nil, fmt.Errorf("predicting embedding for text %d: %w", i, err)
		}

		// Parse response
		if len(resp.GetPredictions()) == 0 {
			return nil, fmt.Errorf("no predictions returned for text %d", i)
		}

		predictionJSON, err := protojson.Marshal(resp.GetPredictions()[0])
		if err != nil {
			return nil, fmt.Errorf("marshalling prediction: %w", err)
		}

		var prediction struct {
			Embeddings struct {
				Values []float32 `json:"values"`
			} `json:"embeddings"`
		}

		if err := json.Unmarshal(predictionJSON, &prediction); err != nil {
			return nil, fmt.Errorf("unmarshalling prediction: %w", err)
		}

		embeddings[i] = prediction.Embeddings.Values
	}

	return embeddings, nil
}

// embedMultimodal generates embeddings for multimodal content (images, audio, video)
func (v *VertexAIEmbedderImpl) embedMultimodal(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	emb := make([][]float32, 0, len(contents))
	for i, content := range contents {
		if i%10 == 0 {
			log.Printf("Generating multimodal embedding for content %d", i)
		}

		// Extract the raw bytes from the content parts
		var rawBytes []byte
		for _, part := range content {
			if binaryPart, ok := part.(ai.BinaryContent); ok {
				rawBytes = binaryPart.Data
				break
			}
		}

		if len(rawBytes) == 0 {
			return nil, errors.New(
				"multimodal embedding requires binary content (images, audio, video)",
			)
		}

		// Generate embedding for this content
		e, err := v.generateMultiModal(ctx, rawBytes)
		if err != nil {
			if strings.Contains(err.Error(), "resource exhausted") {
				return nil, &RateLimitError{
					Err:        fmt.Errorf("generating multimodal embedding for content %d: %w", i, err),
					RetryAfter: 30 * time.Second,
				}
			}
			return nil, fmt.Errorf("generating multimodal embedding for content %d: %w", i, err)
		}
		emb = append(emb, e)
	}

	return emb, nil
}

// generateMultiModal generates a multimodal embedding using Vertex AI
// Supports dimensions: 128, 256, 512, 1408
func (v *VertexAIEmbedderImpl) generateMultiModal(
	ctx context.Context,
	rawBytes []byte,
) (vector.T, error) {
	dimension := v.dimension
	if dimension == 0 {
		dimension = 128 // Default to 128 if not specified
	}

	model := "multimodalembedding@001"
	endpoint := fmt.Sprintf(
		"projects/%s/locations/%s/publishers/google/models/%s",
		v.project,
		v.location,
		model,
	)

	// Build request payload
	// Schema: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/multimodal-embeddings-api#request_body
	instance, err := structpb.NewValue(map[string]any{
		"image": map[string]any{
			"bytesBase64Encoded": rawBytes,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("constructing request payload: %w", err)
	}

	params, err := structpb.NewValue(map[string]any{
		"dimension": dimension,
	})
	if err != nil {
		return nil, fmt.Errorf("constructing request params: %w", err)
	}

	req := &aiplatformpb.PredictRequest{
		Endpoint:   endpoint,
		Instances:  []*structpb.Value{instance},
		Parameters: params,
	}

	resp, err := v.client.Predict(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("generating embeddings: %w", err)
	}

	// Parse response
	// Schema: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/multimodal-embeddings-api#response-body
	instanceEmbeddingsJSON, err := protojson.Marshal(resp.GetPredictions()[0])
	if err != nil {
		return nil, fmt.Errorf("converting protobuf value to JSON: %w", err)
	}

	var instanceEmbeddings struct {
		ImageEmbeddings []float32 `json:"imageEmbedding"`
	}
	if err := json.Unmarshal(instanceEmbeddingsJSON, &instanceEmbeddings); err != nil {
		return nil, fmt.Errorf("unmarshalling JSON: %w", err)
	}

	return instanceEmbeddings.ImageEmbeddings, nil
}
