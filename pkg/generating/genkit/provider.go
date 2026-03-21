package genkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"time"

	generating "github.com/antflydb/antfly/pkg/generating"
	"github.com/antflydb/antfly/pkg/genkit/openrouter"
	fgenai "github.com/firebase/genkit/go/ai"
	fgenkit "github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/compat_oai/anthropic"
	"github.com/firebase/genkit/go/plugins/compat_oai/openai"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/firebase/genkit/go/plugins/ollama"
	"github.com/openai/openai-go/option"
)

var mediaSupportedModels = []string{"gemma3:4b", "gemma3:12b", "gemma3:27b"}

// Model holds the initialized Genkit runtime, selected model, and default output limits.
type Model struct {
	Genkit          *fgenkit.Genkit
	Model           fgenai.Model
	MaxOutputTokens int
}

// NewModel initializes a Genkit model from a generator config.
func NewModel(ctx context.Context, config generating.GeneratorConfig) (*Model, error) {
	var (
		g               *fgenkit.Genkit
		model           fgenai.Model
		maxOutputTokens int
	)

	switch config.Provider {
	case generating.GeneratorProviderOllama:
		c, err := config.AsOllamaGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing ollama config: %w", err)
		}

		if c.Url == nil || *c.Url == "" {
			defaultURL := "http://localhost:11434"
			c.Url = &defaultURL
		}
		timeout := 540
		if c.Timeout != nil && *c.Timeout > 0 {
			timeout = *c.Timeout
		}

		ollamaPlugin := &ollama.Ollama{ServerAddress: *c.Url, Timeout: timeout}
		g = fgenkit.Init(ctx, fgenkit.WithPlugins(ollamaPlugin))

		var modelOpts *fgenai.ModelOptions
		if slices.Contains(mediaSupportedModels, c.Model) {
			modelOpts = &fgenai.ModelOptions{
				Label: c.Model,
				Supports: &fgenai.ModelSupports{
					Multiturn:  true,
					SystemRole: true,
					Media:      true,
					Tools:      false,
				},
				Versions: []string{},
			}
		}

		model = ollamaPlugin.DefineModel(
			g,
			ollama.ModelDefinition{
				Name: c.Model,
				Type: "chat",
			},
			modelOpts,
		)

	case generating.GeneratorProviderGemini:
		c, err := config.AsGoogleGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing google config: %w", err)
		}
		if c.Model == "" {
			c.Model = "gemini-2.5-flash"
		}

		googlePlugin := &googlegenai.GoogleAI{}
		if c.ApiKey != nil && *c.ApiKey != "" {
			googlePlugin.APIKey = *c.ApiKey
		}

		g = fgenkit.Init(ctx, fgenkit.WithPlugins(googlePlugin))
		model = googlegenai.GoogleAIModel(g, c.Model)
		if model == nil {
			return nil, fmt.Errorf("google model not found: %s", c.Model)
		}

	case generating.GeneratorProviderVertex:
		c, err := config.AsVertexGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing vertex config: %w", err)
		}
		if c.Model == "" {
			c.Model = "gemini-2.5-flash"
		}

		project := getConfigOrEnv(c.ProjectId, "GOOGLE_CLOUD_PROJECT")
		if project == "" {
			return nil, errors.New("project_id is required for Vertex AI (set in config or GOOGLE_CLOUD_PROJECT env var)")
		}

		location := getConfigOrEnv(c.Location, "GOOGLE_CLOUD_LOCATION")
		if location == "" {
			location = "us-central1"
		}

		if c.CredentialsPath != nil && *c.CredentialsPath != "" {
			if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", *c.CredentialsPath); err != nil {
				return nil, fmt.Errorf("failed to set GOOGLE_APPLICATION_CREDENTIALS: %w", err)
			}
		}

		vertexPlugin := &googlegenai.VertexAI{
			ProjectID: project,
			Location:  location,
		}
		g = fgenkit.Init(ctx, fgenkit.WithPlugins(vertexPlugin))

		model = googlegenai.VertexAIModel(g, c.Model)
		if model == nil {
			return nil, fmt.Errorf("vertex model not found: %s", c.Model)
		}

	case generating.GeneratorProviderOpenai:
		c, err := config.AsOpenAIGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing openai config: %w", err)
		}

		openaiPlugin := &openai.OpenAI{}
		if c.ApiKey != nil && *c.ApiKey != "" {
			openaiPlugin.APIKey = *c.ApiKey
		}

		var opts []option.RequestOption
		if c.Url != nil && *c.Url != "" {
			opts = append(opts, option.WithBaseURL(*c.Url))
		}
		if len(opts) > 0 {
			openaiPlugin.Opts = opts
		}

		g = fgenkit.Init(ctx, fgenkit.WithPlugins(openaiPlugin))
		model = openaiPlugin.DefineModel(c.Model, fgenai.ModelOptions{})

	case generating.GeneratorProviderAnthropic:
		c, err := config.AsAnthropicGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing anthropic config: %w", err)
		}
		if c.Model == "" {
			c.Model = "claude-sonnet-4-5-20250929"
		}

		var opts []option.RequestOption
		apiKey := ""
		if c.ApiKey != nil && *c.ApiKey != "" {
			apiKey = *c.ApiKey
		} else if envKey := os.Getenv("ANTHROPIC_API_KEY"); envKey != "" {
			apiKey = envKey
		}
		if apiKey != "" {
			opts = append(opts, option.WithAPIKey(apiKey))
		}
		if c.Url != nil && *c.Url != "" {
			opts = append(opts, option.WithBaseURL(*c.Url))
		}

		anthropicPlugin := &anthropic.Anthropic{Opts: opts}
		g = fgenkit.Init(ctx, fgenkit.WithPlugins(anthropicPlugin))
		model = anthropicPlugin.DefineModel(c.Model, fgenai.ModelOptions{})

	case generating.GeneratorProviderOpenrouter:
		c, err := config.AsOpenRouterGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing openrouter config: %w", err)
		}

		var models []string
		if c.Model != nil && *c.Model != "" {
			models = append(models, *c.Model)
		}
		if c.Models != nil && len(*c.Models) > 0 {
			models = append(models, (*c.Models)...)
		}
		if len(models) == 0 {
			return nil, errors.New("openrouter: either model or models must be provided")
		}

		apiKey := ""
		if c.ApiKey != nil && *c.ApiKey != "" {
			apiKey = *c.ApiKey
		}

		openrouterPlugin := &openrouter.OpenRouter{APIKey: apiKey}
		g = fgenkit.Init(ctx, fgenkit.WithPlugins(openrouterPlugin))
		model = openrouterPlugin.DefineModel(g, openrouter.ModelDefinition{Models: models}, nil)

	case generating.GeneratorProviderTermite:
		c, err := config.AsTermiteGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing termite config: %w", err)
		}

		configURL := ""
		if c.ApiUrl != nil {
			configURL = *c.ApiUrl
		}
		apiURL := resolveTermiteURL(configURL)
		if apiURL == "" {
			return nil, errors.New("termite: api_url is required (set via config or ANTFLY_TERMITE_URL env var)")
		}

		termiteURL := apiURL + "/openai/v1"
		timeout := 540
		if c.Timeout != nil && *c.Timeout > 0 {
			timeout = *c.Timeout
		}

		openaiPlugin := &openai.OpenAI{
			APIKey: "termite-local",
			Opts: []option.RequestOption{
				option.WithBaseURL(termiteURL),
				option.WithHTTPClient(&http.Client{Timeout: time.Duration(timeout) * time.Second}),
			},
		}

		g = fgenkit.Init(ctx, fgenkit.WithPlugins(openaiPlugin))
		model = openaiPlugin.DefineModel(c.Model, fgenai.ModelOptions{})
		if c.MaxTokens != nil && *c.MaxTokens > 0 {
			maxOutputTokens = *c.MaxTokens
		}

	default:
		return nil, fmt.Errorf("unsupported provider: %s", config.Provider)
	}

	return &Model{
		Genkit:          g,
		Model:           model,
		MaxOutputTokens: maxOutputTokens,
	}, nil
}

func getConfigOrEnv(configVal *string, envVar string) string {
	if configVal != nil && *configVal != "" {
		return *configVal
	}
	return os.Getenv(envVar)
}

func resolveTermiteURL(configURL string) string {
	if configURL != "" {
		return configURL
	}
	return os.Getenv("ANTFLY_TERMITE_URL")
}
