package reading

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	libai "github.com/antflydb/antfly/pkg/libaf/ai"
	libreading "github.com/antflydb/antfly/pkg/libaf/reading"
	termiteclient "github.com/antflydb/antfly/pkg/termite-client"
	"github.com/antflydb/antfly/pkg/termite-client/oapi"
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

// TermiteConfig configures Termite-backed reading adapters.
type TermiteConfig struct {
	BaseURL          string
	HTTPClient       *http.Client
	Client           *termiteclient.TermiteClient
	Models           []string
	DefaultMaxTokens int
	RenderDPI        float64
}

type termiteBase struct {
	client           *termiteclient.TermiteClient
	models           []string
	defaultMaxTokens int
	renderDPI        float64
}

// TermiteReadReader uses Termite's read endpoint for OCR-style extraction.
type TermiteReadReader struct {
	base termiteBase
}

// TermiteGenerateReader uses Termite's generate endpoint for prompt-driven vision extraction.
type TermiteGenerateReader struct {
	base termiteBase
}

// NewTermiteReadReader creates a Termite OCR-style reader.
func NewTermiteReadReader(cfg TermiteConfig) (*TermiteReadReader, error) {
	base, err := newTermiteBase(cfg)
	if err != nil {
		return nil, err
	}
	return &TermiteReadReader{base: base}, nil
}

// NewTermiteGenerateReader creates a Termite prompt-driven vision reader.
func NewTermiteGenerateReader(cfg TermiteConfig) (*TermiteGenerateReader, error) {
	base, err := newTermiteBase(cfg)
	if err != nil {
		return nil, err
	}
	return &TermiteGenerateReader{base: base}, nil
}

func newTermiteBase(cfg TermiteConfig) (termiteBase, error) {
	if len(cfg.Models) == 0 {
		return termiteBase{}, fmt.Errorf("at least one model is required")
	}

	client := cfg.Client
	if client == nil {
		httpClient := cfg.HTTPClient
		if httpClient == nil {
			httpClient = http.DefaultClient
		}
		var err error
		client, err = termiteclient.NewTermiteClient(cfg.BaseURL, httpClient)
		if err != nil {
			return termiteBase{}, fmt.Errorf("create termite client: %w", err)
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

	return termiteBase{
		client:           client,
		models:           append([]string(nil), cfg.Models...),
		defaultMaxTokens: maxTokens,
		renderDPI:        renderDPI,
	}, nil
}

func (r *TermiteReadReader) Read(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]string, error) {
	results, err := r.ReadDetailed(ctx, pages, opts)
	if err != nil {
		return nil, err
	}
	return resultTexts(results), nil
}

func (r *TermiteReadReader) ReadDetailed(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]Result, error) {
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

func (r *TermiteGenerateReader) Read(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]string, error) {
	results, err := r.ReadDetailed(ctx, pages, opts)
	if err != nil {
		return nil, err
	}
	return resultTexts(results), nil
}

func (r *TermiteGenerateReader) ReadDetailed(ctx context.Context, pages []libai.BinaryContent, opts *libreading.ReadOptions) ([]Result, error) {
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

func (r *TermiteReadReader) Close() error {
	return nil
}

func (r *TermiteGenerateReader) Close() error {
	return nil
}

func resultTexts(results []Result) []string {
	texts := make([]string, len(results))
	for i, result := range results {
		texts[i] = result.Text
	}
	return texts
}

func (b termiteBase) dataURI(page libai.BinaryContent) (string, error) {
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

func (b termiteBase) readSingle(ctx context.Context, dataURI string, opts *libreading.ReadOptions) (Result, error) {
	prompt, maxTokens := resolveReadOptions(opts, b.defaultMaxTokens)
	var lastErr error

	for _, model := range b.models {
		resp, err := b.client.Client().ReadImagesWithResponse(ctx, oapi.ReadRequest{
			Model:     model,
			Prompt:    prompt,
			Images:    []oapi.ImageURL{{Url: dataURI}},
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
		if resp.JSON200 == nil || len(resp.JSON200.Results) == 0 {
			continue
		}

		text := strings.TrimSpace(resp.JSON200.Results[0].Text)
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

func (b termiteBase) generateSingle(ctx context.Context, dataURI string, opts *libreading.ReadOptions) (Result, error) {
	prompt, maxTokens := resolveReadOptions(opts, b.defaultMaxTokens)
	message, err := termiteclient.NewMultimodalUserMessage(prompt, dataURI)
	if err != nil {
		return Result{}, fmt.Errorf("build multimodal message: %w", err)
	}

	var lastErr error
	for _, model := range b.models {
		resp, err := b.client.Generate(ctx, model, []oapi.ChatMessage{message}, &termiteclient.GenerateConfig{
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
