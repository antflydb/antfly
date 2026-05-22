// Copyright 2026 Antfly, Inc.
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
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/template"
	libtermite "github.com/antflydb/antfly/lib/termite"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/cespare/xxhash/v2"
	"github.com/google/dotprompt/go/dotprompt"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"golang.org/x/time/rate"
)

func NewEmbedder(conf EmbedderConfig) (Embedder, error) {
	if conf.Provider == "" {
		return nil, errors.New("provider not specified")
	}
	e, ok := EmbedderRegistry[conf.Provider]
	if !ok {
		return nil, fmt.Errorf("no embedder registered for type %s", conf.Provider)
	}
	emb, err := e(conf)
	if err != nil {
		return nil, fmt.Errorf("creating embedder from conf: %w", err)
	}
	return emb, nil
}

func RegisterEmbedder(
	typ EmbedderProvider,
	constructor func(config EmbedderConfig) (Embedder, error),
) {
	if _, exists := EmbedderRegistry[typ]; exists {
		panic(fmt.Sprintf("embedder provider %s already registered", typ))
	}
	EmbedderRegistry[typ] = constructor
}

func DeregisterEmbedder(typ EmbedderProvider) {
	delete(EmbedderRegistry, typ)
}

// RateLimitError indicates the provider returned a rate limit (HTTP 429 or
// equivalent). RetryAfter, when non-zero, carries the server's requested
// backoff duration (from Retry-After header or gRPC RetryInfo).
type RateLimitError struct {
	Err        error
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("rate limited (retry after %s): %v", e.RetryAfter, e.Err)
	}
	return fmt.Sprintf("rate limited: %v", e.Err)
}

func (e *RateLimitError) Unwrap() error { return e.Err }

// Embedder is the unified interface for all embedding providers.
// It supports both text-only and multimodal embedding through a single interface.
type Embedder interface {
	// Capabilities returns what this embedder supports (MIME types, dimensions, etc.)
	Capabilities() EmbedderCapabilities

	// Embed generates embeddings for content.
	// Each []ContentPart represents one document (can be text, image, mixed, etc.)
	// Returns one embedding vector per input document.
	Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error)

	// RateLimiter returns a rate limiter for pre-request throttling, or nil
	// if the provider has no rate limit (e.g. local inference like Termite,
	// Ollama, or the built-in Antfly embedder). The enricher calls WaitN
	// before each batch; remote providers return a limiter matching their
	// API quota (e.g. OpenAI ~3000 RPM, Vertex ~600 RPM).
	RateLimiter() *rate.Limiter
}

// SparseEmbedder generates sparse (SPLADE-style) embeddings from text.
// Unlike dense Embedder which returns fixed-dimension float vectors,
// SparseEmbedder returns variable-length sparse vectors with vocab-space indices.
type SparseEmbedder interface {
	// SparseEmbed generates sparse embeddings for the given texts.
	// Returns one SparseVector per input text.
	SparseEmbed(ctx context.Context, texts []string) ([]SparseVector, error)
}

// SparseVector represents a sparse embedding as parallel arrays of indices and values.
// Indices are token IDs from the model's vocabulary, sorted ascending.
// Values are the corresponding weights (always positive after SPLADE activation).
type SparseVector struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

// EmbedText is a convenience function for text-only embedding.
func EmbedText(ctx context.Context, e Embedder, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	contents := make([][]ai.ContentPart, len(texts))
	for i, t := range texts {
		contents[i] = []ai.ContentPart{ai.TextContent{Text: t}}
	}
	return e.Embed(ctx, contents)
}

// ExtractText extracts text from ContentPart slices for text-only embedders.
// It prefers TextContent but falls back to ImageURLContent URL as text if no text found.
func ExtractText(contents [][]ai.ContentPart) []string {
	texts := make([]string, len(contents))
	for i, parts := range contents {
		for _, part := range parts {
			if tc, ok := part.(ai.TextContent); ok {
				texts[i] = tc.Text
				break
			}
		}
		// If no text content found, fall back to URL content as text
		if texts[i] == "" {
			for _, part := range parts {
				if uc, ok := part.(ai.ImageURLContent); ok {
					texts[i] = uc.URL
					break
				}
			}
		}
	}
	return texts
}

// EmbedDocuments generates embeddings for documents using a template.
// It uses DocumentToParts to convert each document to content parts via the template,
// then embeds the resulting parts using the provided embedder.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - e: The embedder to use for generating embeddings
//   - dp: A configured Dotprompt instance with any custom helpers
//   - templateStr: A Handlebars template string for rendering document content
//   - docs: The documents to embed
//
// Returns one embedding vector per document.
//
// Example template:
//
//	{{#if photoUrl}}
//	{{remoteMedia url=photoUrl}}
//	{{/if}}
//	Title: {{title}}
//	Content: {{body}}
func EmbedDocuments(
	ctx context.Context,
	e Embedder,
	dp *dotprompt.Dotprompt,
	templateStr string,
	docs []schema.Document,
) ([][]float32, error) {
	if len(docs) == 0 {
		return [][]float32{}, nil
	}

	// Convert each document to content parts
	contents := make([][]ai.ContentPart, len(docs))
	for i, doc := range docs {
		// Render the document through the template
		rendered, err := template.DocumentToParts(ctx, dp, doc, templateStr)
		if err != nil {
			return nil, fmt.Errorf("rendering document %d: %w", i, err)
		}

		// Convert rendered prompt to ContentParts
		msgParts, err := ai.RenderedPromptToContentParts(rendered)
		if err != nil {
			return nil, fmt.Errorf("converting document %d to parts: %w", i, err)
		}

		// Flatten all message parts into a single []ContentPart for this document
		var docParts []ai.ContentPart
		for _, parts := range msgParts {
			docParts = append(docParts, parts...)
		}
		contents[i] = docParts
	}

	return e.Embed(ctx, contents)
}

var (
	EmbedderRegistry = map[EmbedderProvider]func(config EmbedderConfig) (Embedder, error){}
)

func init() {
	RegisterEmbedder(EmbedderProviderGemini, NewGenaiGoogleImpl)
	RegisterEmbedder(EmbedderProviderVertex, NewVertexAIEmbedder)

	RegisterEmbedder(EmbedderProviderOpenai, NewOpenAIImpl)

	RegisterEmbedder(EmbedderProviderOllama, NewOllamaEmbedderImpl)

	RegisterEmbedder(EmbedderProviderBedrock, NewBedrockImpl)

	RegisterEmbedder(EmbedderProviderTermite, NewTermiteEmbedderFromConfig)
}

// Default rate limits for remote providers (requests per second).
// These are conservative defaults; providers may allow higher rates
// depending on tier. The enricher env var ANTFLY_ENRICHER_RATE_LIMIT
// can override these.
const (
	OpenAIDefaultRPS          = 50  // ~3000 RPM
	VertexDefaultRPS          = 10  // ~600 RPM
	GeminiDefaultRPS          = 25  // ~1500 RPM
	CohereDefaultRPS          = 100 // ~6000 RPM (production key)
	OpenRouterDefaultRPS      = 50
	BedrockDefaultRPS         = 50
	BedrockCohereMaxBatchSize = 96
)

type OpenAIImpl struct {
	client  *openai.Client
	model   string
	caps    EmbedderCapabilities
	limiter *rate.Limiter
}
type BedrockImpl struct {
	client        bedrockRuntimeClient
	model         string
	stripNewLines bool
	batchSize     int
	dimension     int
	inputType     string
	truncate      string
	caps          EmbedderCapabilities
	limiter       *rate.Limiter
}

type bedrockRuntimeClient interface {
	InvokeModel(context.Context, *bedrockruntime.InvokeModelInput, ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

func NewOpenAIImpl(config EmbedderConfig) (Embedder, error) {
	// Validate config
	c, err := config.AsOpenAIEmbedderConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	opts := []option.RequestOption{}

	// Set base URL if provided
	var baseURL string
	if c.Url != nil && *c.Url != "" {
		baseURL = *c.Url
	} else if envURL := os.Getenv("OPENAI_BASE_URL"); envURL != "" {
		baseURL = envURL
	}
	if baseURL != "" {
		opts = append(opts, option.WithBaseURL(baseURL))
	}

	// Set API key
	var apiKey string
	if c.ApiKey != nil && *c.ApiKey != "" {
		apiKey = *c.ApiKey
	} else {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if apiKey != "" {
		opts = append(opts, option.WithAPIKey(apiKey))
	}

	client := openai.NewClient(opts...)
	return &OpenAIImpl{
		client:  &client,
		model:   c.Model,
		caps:    ResolveCapabilities(c.Model, config.GetConfigCapabilities()),
		limiter: rate.NewLimiter(OpenAIDefaultRPS, OpenAIDefaultRPS),
	}, nil
}

func (l *OpenAIImpl) Capabilities() EmbedderCapabilities {
	return l.caps
}

func (l *OpenAIImpl) RateLimiter() *rate.Limiter {
	return l.limiter
}

func (l *OpenAIImpl) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	// OpenAI only supports text embeddings, extract text from content parts
	values := ExtractText(contents)

	resp, err := l.client.Embeddings.New(ctx, openai.EmbeddingNewParams{
		Input: openai.EmbeddingNewParamsInputUnion{
			OfArrayOfStrings: values,
		},
		Model: openai.EmbeddingModel(l.model),
	})
	if err != nil {
		return nil, fmt.Errorf("creating embeddings: %w", err)
	}

	embeddings := make([][]float32, len(resp.Data))
	for i, data := range resp.Data {
		// Convert float64 to float32
		emb := make([]float32, len(data.Embedding))
		for j, v := range data.Embedding {
			emb[j] = float32(v)
		}
		embeddings[i] = emb
	}
	return embeddings, nil
}

func (mc *EmbedderConfig) HashID() uint64 {
	return xxhash.Sum64(mc.union)
	// return xxhash.Sum64(append([]byte(mc.Provider), mc.union...))
}

func NewBedrockImpl(cfg EmbedderConfig) (Embedder, error) {
	// Validate config
	c, err := cfg.AsBedrockEmbedderConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Load AWS config with region
	region := "us-east-1" // default
	if c.Region != nil && *c.Region != "" {
		region = *c.Region
	}
	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	client := bedrockruntime.NewFromConfig(awsCfg)

	stripNewLines := false
	if c.StripNewLines != nil {
		stripNewLines = *c.StripNewLines
	}

	batchSize := 100 // default
	if c.BatchSize != nil && *c.BatchSize > 0 {
		batchSize = *c.BatchSize
	}
	dimension := 0
	if c.Dimension != nil {
		dimension = *c.Dimension
	} else if c.Dimensions != nil {
		dimension = *c.Dimensions
	}
	inputType := ""
	if c.InputType != nil {
		inputType = *c.InputType
	}
	if strings.HasPrefix(c.Model, "cohere.embed-") && inputType == "" {
		inputType = "search_document"
	}
	truncate := ""
	if c.Truncate != nil {
		truncate = *c.Truncate
	}

	return &BedrockImpl{
		client:        client,
		model:         string(c.Model),
		stripNewLines: stripNewLines,
		batchSize:     batchSize,
		dimension:     dimension,
		inputType:     inputType,
		truncate:      truncate,
		caps:          ResolveCapabilities(c.Model, cfg.GetConfigCapabilities()),
		limiter:       rate.NewLimiter(BedrockDefaultRPS, BedrockDefaultRPS),
	}, nil
}

func (l *BedrockImpl) Capabilities() EmbedderCapabilities {
	return l.caps
}

func (l *BedrockImpl) RateLimiter() *rate.Limiter {
	return l.limiter
}

func (l *BedrockImpl) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	if strings.HasPrefix(l.model, "amazon.titan-embed-image") {
		return l.embedTitanMultimodal(ctx, contents)
	}
	if strings.HasPrefix(l.model, "cohere.embed-") {
		return l.embedCohere(ctx, contents)
	}

	// Bedrock text embeddings: extract text from content parts
	values := ExtractText(contents)

	// Process texts in batches
	results := make([][]float32, 0, len(values))
	for i := 0; i < len(values); i += l.batchSize {
		end := min(i+l.batchSize, len(values))
		batch := values[i:end]

		// Strip newlines if configured
		if l.stripNewLines {
			for j := range batch {
				batch[j] = strings.ReplaceAll(batch[j], "\n", " ")
			}
		}

		// Call Bedrock API for each text in batch
		for _, text := range batch {
			// Format depends on model - Titan models use this format
			bodyBytes, err := json.Marshal(l.titanTextBody(text))
			if err != nil {
				return nil, fmt.Errorf("marshaling request body: %w", err)
			}

			resp, err := l.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
				ModelId:     &l.model,
				Body:        bodyBytes,
				ContentType: aws.String("application/json"),
			})
			if err != nil {
				return nil, fmt.Errorf("invoking bedrock model: %w", err)
			}

			// Parse response
			var result struct {
				Embedding []float32 `json:"embedding"`
			}
			if err := json.Unmarshal(resp.Body, &result); err != nil {
				return nil, fmt.Errorf("unmarshaling response: %w", err)
			}
			results = append(results, result.Embedding)
		}
	}

	return results, nil
}

func (l *BedrockImpl) titanTextBody(text string) map[string]any {
	body := map[string]any{"inputText": text}
	if l.dimension > 0 {
		body["dimensions"] = l.dimension
	}
	return body
}

func (l *BedrockImpl) embedTitanMultimodal(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	results := make([][]float32, 0, len(contents))
	for i, parts := range contents {
		body, err := l.titanMultimodalBody(parts)
		if err != nil {
			return nil, fmt.Errorf("building titan multimodal request for input %d: %w", i, err)
		}
		embedding, err := l.invokeEmbedding(ctx, body)
		if err != nil {
			return nil, fmt.Errorf("embedding titan multimodal input %d: %w", i, err)
		}
		results = append(results, embedding)
	}
	return results, nil
}

func (l *BedrockImpl) titanMultimodalBody(parts []ai.ContentPart) (map[string]any, error) {
	var text string
	var image []byte
	for _, part := range parts {
		switch p := part.(type) {
		case ai.TextContent:
			if text == "" {
				text = p.Text
			} else if p.Text != "" {
				text += " " + p.Text
			}
		case ai.BinaryContent:
			if !strings.HasPrefix(p.MIMEType, "image/") {
				return nil, fmt.Errorf("titan multimodal only supports image binary content, got %s", p.MIMEType)
			}
			if len(image) > 0 {
				return nil, errors.New("titan multimodal supports one image per embedding request")
			}
			image = p.Data
		case ai.ImageURLContent:
			if text == "" {
				text = p.URL
			}
		}
	}
	body := map[string]any{}
	if strings.TrimSpace(text) != "" {
		body["inputText"] = text
	}
	if len(image) > 0 {
		body["inputImage"] = base64.StdEncoding.EncodeToString(image)
	}
	if len(body) == 0 {
		return nil, errors.New("titan multimodal requires non-empty text or image content")
	}
	if l.dimension > 0 {
		body["embeddingConfig"] = map[string]any{
			"outputEmbeddingLength": l.dimension,
		}
	}
	return body, nil
}

func (l *BedrockImpl) embedCohere(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if l.cohereSupportsMixedInputs(contents) {
		return l.embedCohereV4(ctx, contents)
	}

	texts := ExtractText(contents)
	for i, text := range texts {
		if l.stripNewLines {
			text = strings.ReplaceAll(text, "\n", " ")
			texts[i] = text
		}
		if strings.TrimSpace(text) == "" {
			return nil, fmt.Errorf("cohere bedrock text embedding input %d is empty; use a Cohere v4 multimodal model for image content", i)
		}
	}

	results := make([][]float32, 0, len(texts))
	batchSize := l.cohereBatchSize()
	for start := 0; start < len(texts); start += batchSize {
		end := min(start+batchSize, len(texts))
		body := l.cohereTextBody(texts[start:end])
		embeddings, err := l.invokeEmbeddings(ctx, body)
		if err != nil {
			return nil, fmt.Errorf("embedding cohere text batch %d:%d: %w", start, end, err)
		}
		results = append(results, embeddings...)
	}
	return results, nil
}

func (l *BedrockImpl) cohereSupportsMixedInputs(contents [][]ai.ContentPart) bool {
	if !l.isCohereV4() {
		return false
	}
	for _, parts := range contents {
		for _, part := range parts {
			if _, ok := part.(ai.BinaryContent); ok {
				return true
			}
		}
	}
	return false
}

func (l *BedrockImpl) isCohereV4() bool {
	return strings.HasPrefix(l.model, "cohere.embed-v4")
}

func (l *BedrockImpl) cohereBatchSize() int {
	batchSize := l.batchSize
	if batchSize <= 0 || batchSize > BedrockCohereMaxBatchSize {
		batchSize = BedrockCohereMaxBatchSize
	}
	return batchSize
}

func (l *BedrockImpl) cohereTextBody(texts []string) map[string]any {
	body := map[string]any{
		"texts": texts,
	}
	if l.inputType != "" {
		body["input_type"] = l.inputType
	}
	if l.dimension > 0 && l.isCohereV4() {
		body["output_dimension"] = l.dimension
	}
	if l.truncate != "" {
		body["truncate"] = l.truncate
	}
	return body
}

func (l *BedrockImpl) embedCohereV4(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	results := make([][]float32, 0, len(contents))
	batchSize := l.cohereBatchSize()
	for start := 0; start < len(contents); start += batchSize {
		end := min(start+batchSize, len(contents))
		inputs := make([]any, 0, end-start)
		for i, parts := range contents[start:end] {
			input, err := cohereV4Input(parts, l.stripNewLines)
			if err != nil {
				return nil, fmt.Errorf("building cohere v4 request for input %d: %w", start+i, err)
			}
			inputs = append(inputs, input)
		}
		body := l.cohereV4Body(inputs)
		embeddings, err := l.invokeEmbeddings(ctx, body)
		if err != nil {
			return nil, fmt.Errorf("embedding cohere v4 batch %d:%d: %w", start, end, err)
		}
		results = append(results, embeddings...)
	}
	return results, nil
}

func (l *BedrockImpl) cohereV4Body(inputs []any) map[string]any {
	body := map[string]any{
		"inputs":          inputs,
		"embedding_types": []string{"float"},
	}
	if l.inputType != "" {
		body["input_type"] = l.inputType
	}
	if l.dimension > 0 {
		body["output_dimension"] = l.dimension
	}
	if l.truncate != "" {
		body["truncate"] = l.truncate
	}
	return body
}

func cohereV4Input(parts []ai.ContentPart, stripNewLines bool) (map[string]any, error) {
	content := make([]any, 0, len(parts))
	for _, part := range parts {
		switch p := part.(type) {
		case ai.TextContent:
			text := p.Text
			if stripNewLines {
				text = strings.ReplaceAll(text, "\n", " ")
			}
			if text != "" {
				content = append(content, map[string]any{"type": "text", "text": text})
			}
		case ai.BinaryContent:
			if !strings.HasPrefix(p.MIMEType, "image/") {
				return nil, fmt.Errorf("cohere v4 bedrock only supports image binary content, got %s", p.MIMEType)
			}
			imageURL := "data:" + p.MIMEType + ";base64," + base64.StdEncoding.EncodeToString(p.Data)
			content = append(content, map[string]any{
				"type":      "image_url",
				"image_url": map[string]any{"url": imageURL},
			})
		case ai.ImageURLContent:
			if p.URL != "" {
				content = append(content, map[string]any{"type": "text", "text": p.URL})
			}
		}
	}
	if len(content) == 0 {
		return nil, errors.New("cohere v4 embedding requires non-empty text or image content")
	}
	return map[string]any{"content": content}, nil
}

func (l *BedrockImpl) invokeEmbedding(ctx context.Context, body map[string]any) ([]float32, error) {
	embeddings, err := l.invokeEmbeddings(ctx, body)
	if err != nil {
		return nil, err
	}
	if len(embeddings) == 0 {
		return nil, errors.New("bedrock returned no embeddings")
	}
	return embeddings[0], nil
}

func (l *BedrockImpl) invokeEmbeddings(ctx context.Context, body map[string]any) ([][]float32, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}
	resp, err := l.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     &l.model,
		Body:        bodyBytes,
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
	})
	if err != nil {
		return nil, fmt.Errorf("invoking bedrock model: %w", err)
	}
	return parseBedrockEmbeddings(resp.Body)
}

func parseBedrockEmbeddings(body []byte) ([][]float32, error) {
	var raw struct {
		Embedding  []float32       `json:"embedding"`
		Embeddings json.RawMessage `json:"embeddings"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling response: %w", err)
	}
	if len(raw.Embedding) > 0 {
		return [][]float32{raw.Embedding}, nil
	}
	if len(raw.Embeddings) == 0 {
		return nil, errors.New("bedrock response did not include embeddings")
	}
	var vectors [][]float32
	if err := json.Unmarshal(raw.Embeddings, &vectors); err == nil {
		return vectors, nil
	}
	var typed struct {
		Float [][]float32 `json:"float"`
	}
	if err := json.Unmarshal(raw.Embeddings, &typed); err != nil {
		return nil, fmt.Errorf("unmarshaling embeddings response: %w", err)
	}
	if len(typed.Float) == 0 {
		return nil, errors.New("bedrock response did not include float embeddings")
	}
	return typed.Float, nil
}

// NewEmbedderConfigFromJSON creates an EmbedderConfig from raw JSON. Mostly useful for testing.
func NewEmbedderConfigFromJSON(provider string, data []byte) *EmbedderConfig {
	cfg := &EmbedderConfig{union: data}
	// Populate generated fields (e.g. Multimodal) from the JSON payload.
	_ = json.Unmarshal(data, cfg)
	cfg.Provider = EmbedderProvider(provider)
	return cfg
}

// defaultEmbedderConfig is the default embedder configuration, set from config at startup.
var defaultEmbedderConfig *EmbedderConfig

// SetDefaultEmbedderConfig sets the default embedder configuration.
// This should be called during config initialization.
func SetDefaultEmbedderConfig(config *EmbedderConfig) {
	defaultEmbedderConfig = config
}

// GetDefaultEmbedderConfig returns the current default embedder configuration.
func GetDefaultEmbedderConfig() *EmbedderConfig {
	return defaultEmbedderConfig
}

// NewTermiteEmbedderFromConfig creates a Termite embedder from the unified config.
func NewTermiteEmbedderFromConfig(config EmbedderConfig) (Embedder, error) {
	c, err := config.AsTermiteEmbedderConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Get URL from config, environment variable, or default
	configURL := ""
	if c.ApiUrl != nil {
		configURL = *c.ApiUrl
	}
	url := libtermite.ResolveURL(configURL)
	if url == "" {
		return nil, fmt.Errorf("termite URL is required: set api_url in config or ANTFLY_TERMITE_URL environment variable")
	}

	return NewTermiteClient(url, c.Model, config.GetConfigCapabilities())
}
