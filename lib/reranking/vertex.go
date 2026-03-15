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

package reranking

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/vertex"
	"go.uber.org/multierr"
)

const (
	// defaultVertexModel is the default ranking model to use
	defaultVertexModel = "semantic-ranker-default@latest"
)

// VertexReranker implements the Reranker interface using Google Vertex AI Ranking API
type VertexReranker struct {
	client    *http.Client
	projectID string
	model     string
	topN      *int
	field     string
	template  string
}

func init() {
	RegisterReranker(RerankerProviderVertex, NewVertexReranker)
}

// NewVertexReranker creates a new Vertex AI reranker from configuration
func NewVertexReranker(config RerankerConfig) (Reranker, error) {
	c, err := config.AsVertexRerankerConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing vertex config: %w", err)
	}

	// Get project ID from config or environment
	projectID := ""
	if c.ProjectId != nil && *c.ProjectId != "" {
		projectID = *c.ProjectId
	} else {
		projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	if projectID == "" {
		return nil, fmt.Errorf("project_id is required (set in config or GOOGLE_CLOUD_PROJECT env var)")
	}

	// Set up credentials (file path → env var → ADC)
	creds, err := vertex.LoadCredentials(c.CredentialsPath, []string{vertex.CloudPlatformScope})
	if err != nil {
		return nil, fmt.Errorf("resolving credentials: %w", err)
	}
	client := vertex.AuthHTTPClient(creds)

	// Default model if not specified
	model := c.Model
	if model == "" {
		model = defaultVertexModel
	}

	// Extract field and template from config
	field := ""
	if config.Field != nil {
		field = *config.Field
	}

	template := ""
	if config.Template != nil {
		template = *config.Template
	}

	return &VertexReranker{
		client:    client,
		projectID: projectID,
		model:     model,
		topN:      c.TopN,
		field:     field,
		template:  template,
	}, nil
}

// vertexRankRequest is the request body for the Vertex AI Ranking API
type vertexRankRequest struct {
	Model   string             `json:"model"`
	Query   string             `json:"query"`
	Records []vertexRankRecord `json:"records"`
	TopN    *int               `json:"topN,omitempty"`
}

// vertexRankRecord represents a document record in the ranking request
type vertexRankRecord struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

// vertexRankResponse is the response from the Vertex AI Ranking API
type vertexRankResponse struct {
	Records []vertexRankedRecord `json:"records"`
}

// vertexRankedRecord represents a ranked document in the response
type vertexRankedRecord struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
}

func (r *VertexReranker) Rerank(
	ctx context.Context,
	query string,
	documents []schema.Document,
) ([]float32, error) {
	if len(documents) == 0 {
		return []float32{}, nil
	}

	// Extract text from documents using field or template
	documentTexts, err := ExtractDocumentTexts(documents, r.field, r.template)
	if err != nil {
		return nil, fmt.Errorf("extracting document texts: %w", err)
	}

	// Build records with IDs
	records := make([]vertexRankRecord, len(documentTexts))
	for i, text := range documentTexts {
		records[i] = vertexRankRecord{
			ID:      strconv.Itoa(i),
			Content: text,
		}
	}

	// Build request
	req := vertexRankRequest{
		Model:   r.model,
		Query:   query,
		Records: records,
		TopN:    r.topN,
	}

	// Make API call
	resp, err := r.callRankingAPI(ctx, req)
	if err != nil {
		return nil, err
	}

	// Map results back to original document positions
	// Initialize with zeros for documents not in response (e.g., if topN was used)
	scores := make([]float32, len(documents))
	var errs error
	mappedCount := 0
	for _, record := range resp.Records {
		idx, err := strconv.Atoi(record.ID)
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("parsing record ID %q: %w", record.ID, err))
			continue
		}
		if idx < 0 || idx >= len(scores) {
			errs = multierr.Append(errs, fmt.Errorf("record index %d out of bounds (0-%d)", idx, len(scores)-1))
			continue
		}
		scores[idx] = float32(record.Score)
		mappedCount++
	}

	// Add warning if we didn't map all expected results (unless topN was used)
	if r.topN == nil && mappedCount != len(documents) {
		errs = multierr.Append(errs, fmt.Errorf(
			"score count mismatch: expected %d, mapped %d, response had %d records",
			len(documents), mappedCount, len(resp.Records)))
	}

	// Return scores with accumulated errors (graceful degradation)
	return scores, errs
}

// callRankingAPI makes the HTTP request to the Discovery Engine Ranking API
func (r *VertexReranker) callRankingAPI(ctx context.Context, req vertexRankRequest) (*vertexRankResponse, error) {
	// Build API URL
	// https://discoveryengine.googleapis.com/v1/projects/{PROJECT_ID}/locations/global/rankingConfigs/default_ranking_config:rank
	url := fmt.Sprintf(
		"https://discoveryengine.googleapis.com/v1/projects/%s/locations/global/rankingConfigs/default_ranking_config:rank",
		r.projectID,
	)

	// Serialize request body
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Goog-User-Project", r.projectID)

	// Execute request
	httpResp, err := r.client.Do(httpReq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer func() { _ = httpResp.Body.Close() }()

	// Read response body
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	// Check for errors
	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ranking API error (status %d): %s", httpResp.StatusCode, string(respBody))
	}

	// Parse response
	var resp vertexRankResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &resp, nil
}
