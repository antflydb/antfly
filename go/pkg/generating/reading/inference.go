package reading

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	libai "github.com/antflydb/antfly/go/pkg/libaf/ai"
	libreading "github.com/antflydb/antfly/go/pkg/libaf/reading"
	inferenceclient "github.com/antflydb/antfly/go/pkg/sdk"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

const DefaultMaxTokens = 256

// Result includes extracted text and the model that produced it.
type Result struct {
	Text  string
	Model string
}

// ModelReader extends reading.Reader with model-aware results.
type ModelReader interface {
	libreading.Reader
	ReadDetailed(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]Result, error)
}

// AntflyConfig configures Antfly-backed reading adapters.
type AntflyConfig struct {
	BaseURL          string
	HTTPClient       *http.Client
	Client           *inferenceclient.InferenceClient
	Models           []string
	DefaultMaxTokens int
	RenderDPI        float64
}

type inferenceBase struct {
	client           *inferenceclient.InferenceClient
	models           []string
	defaultMaxTokens int
	renderDPI        float64
}

// AntflyReadReader uses the Antfly read endpoint for OCR-style extraction.
type AntflyReadReader struct {
	base inferenceBase
}

// AntflyGenerateReader uses the Antfly generate endpoint for prompt-driven vision extraction.
type AntflyGenerateReader struct {
	base inferenceBase
}

// NewAntflyReadReader creates an Antfly OCR-style reader.
func NewAntflyReadReader(cfg AntflyConfig) (*AntflyReadReader, error) {
	base, err := newInferenceBase(cfg)
	if err != nil {
		return nil, err
	}
	return &AntflyReadReader{base: base}, nil
}

// NewAntflyGenerateReader creates an Antfly prompt-driven vision reader.
func NewAntflyGenerateReader(cfg AntflyConfig) (*AntflyGenerateReader, error) {
	base, err := newInferenceBase(cfg)
	if err != nil {
		return nil, err
	}
	return &AntflyGenerateReader{base: base}, nil
}

func newInferenceBase(cfg AntflyConfig) (inferenceBase, error) {
	if len(cfg.Models) == 0 {
		return inferenceBase{}, fmt.Errorf("at least one model is required")
	}

	client := cfg.Client
	if client == nil {
		httpClient := cfg.HTTPClient
		if httpClient == nil {
			httpClient = http.DefaultClient
		}
		var err error
		client, err = inferenceclient.NewInferenceClient(cfg.BaseURL, httpClient)
		if err != nil {
			return inferenceBase{}, fmt.Errorf("create inference client: %w", err)
		}
	}

	maxTokens := cfg.DefaultMaxTokens
	if maxTokens <= 0 {
		maxTokens = DefaultMaxTokens
	}

	renderDPI := cfg.RenderDPI
	if renderDPI <= 0 {
		renderDPI = DefaultRenderDPI
	}

	return inferenceBase{
		client:           client,
		models:           append([]string(nil), cfg.Models...),
		defaultMaxTokens: maxTokens,
		renderDPI:        renderDPI,
	}, nil
}

func (r *AntflyReadReader) Read(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]string, error) {
	results, err := r.ReadDetailed(ctx, pages, opts)
	if err != nil {
		return nil, err
	}
	return resultTexts(results), nil
}

func (r *AntflyReadReader) ReadDetailed(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]Result, error) {
	results := make([]Result, len(pages))
	for i, page := range pages {
		dataURI, err := r.base.dataURI(page)
		if err != nil {
			return nil, fmt.Errorf("prepare page %d: %w", i+1, err)
		}

		result, err := r.base.readSingle(ctx, dataURI, opts)
		if err != nil {
			return nil, fmt.Errorf("read page %d: %w", i+1, err)
		}
		results[i] = result
	}
	return results, nil
}

func (r *AntflyGenerateReader) Read(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]string, error) {
	results, err := r.ReadDetailed(ctx, pages, opts)
	if err != nil {
		return nil, err
	}
	return resultTexts(results), nil
}

func (r *AntflyGenerateReader) ReadDetailed(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]Result, error) {
	results := make([]Result, len(pages))
	for i, page := range pages {
		dataURI, err := r.base.dataURI(page)
		if err != nil {
			return nil, fmt.Errorf("prepare page %d: %w", i+1, err)
		}

		result, err := r.base.generateSingle(ctx, dataURI, opts)
		if err != nil {
			return nil, fmt.Errorf("generate page %d: %w", i+1, err)
		}
		results[i] = result
	}
	return results, nil
}

func (r *AntflyReadReader) Close() error {
	return nil
}

func (r *AntflyGenerateReader) Close() error {
	return nil
}

func resultTexts(results []Result) []string {
	texts := make([]string, len(results))
	for i, result := range results {
		texts[i] = result.Text
	}
	return texts
}

func (b inferenceBase) dataURI(page libai.BinaryContent) (string, error) {
	content := page
	switch strings.TrimSpace(page.MIMEType) {
	case "application/pdf":
		rendered, err := RenderPDFPage(page.Data, 1, b.renderDPI)
		if err != nil {
			return "", err
		}
		content = rendered
	default:
		if !strings.HasPrefix(strings.TrimSpace(page.MIMEType), "image/") {
			return "", fmt.Errorf("unsupported MIME type %q", page.MIMEType)
		}
	}

	return EncodeDataURI(content)
}

func (b inferenceBase) readSingle(ctx context.Context, dataURI string, opts *libreading.ReadOptions) (Result, error) {
	prompt, maxTokens := resolveReadOptions(opts, b.defaultMaxTokens)
	var lastErr error

	for _, model := range b.models {
		resp, err := b.client.Client().ReadImagesWithResponse(ctx, oapi.InferenceReadRequest{
			Model:     model,
			Prompt:    prompt,
			Images:    []oapi.InferenceImageURL{{Url: dataURI}},
			MaxTokens: maxTokens,
		})
		if err != nil {
			lastErr = err
			continue
		}
		if resp.JSON400 != nil {
			lastErr = fmt.Errorf("bad request: %s", resp.JSON400.Error)
			continue
		}
		if resp.JSON404 != nil {
			lastErr = fmt.Errorf("model not found: %s", resp.JSON404.Error)
			continue
		}
		if resp.JSON500 != nil {
			lastErr = fmt.Errorf("server error: %s", resp.JSON500.Error)
			continue
		}
		if resp.JSON503 != nil {
			lastErr = fmt.Errorf("service unavailable: %s", resp.JSON503.Error)
			continue
		}
		if resp.JSON200 == nil || len(resp.JSON200.Data) == 0 {
			continue
		}

		text := strings.TrimSpace(resp.JSON200.Data[0].Text)
		if text == "" {
			continue
		}
		return Result{Text: text, Model: model}, nil
	}

	if lastErr != nil {
		return Result{}, lastErr
	}
	return Result{}, nil
}

func (b inferenceBase) generateSingle(ctx context.Context, dataURI string, opts *libreading.ReadOptions) (Result, error) {
	prompt, maxTokens := resolveReadOptions(opts, b.defaultMaxTokens)
	message, err := inferenceclient.NewMultimodalUserMessage(prompt, dataURI)
	if err != nil {
		return Result{}, fmt.Errorf("build multimodal message: %w", err)
	}

	var lastErr error
	for _, model := range b.models {
		resp, err := b.client.Generate(ctx, model, []oapi.InferenceChatMessage{message}, &inferenceclient.GenerateConfig{
			MaxTokens: maxTokens,
		})
		if err != nil {
			lastErr = err
			continue
		}
		if len(resp.Choices) == 0 {
			continue
		}

		text := strings.TrimSpace(resp.Choices[0].Message.Content)
		if text == "" {
			continue
		}
		return Result{Text: text, Model: model}, nil
	}

	if lastErr != nil {
		return Result{}, lastErr
	}
	return Result{}, nil
}

func resolveReadOptions(opts *libreading.ReadOptions, defaultMaxTokens int) (string, int) {
	if opts == nil {
		return "", defaultMaxTokens
	}

	maxTokens := opts.MaxTokens
	if maxTokens <= 0 {
		maxTokens = defaultMaxTokens
	}
	return opts.Prompt, maxTokens
}
