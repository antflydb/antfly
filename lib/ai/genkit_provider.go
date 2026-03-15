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

package ai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"time"

	libtermite "github.com/antflydb/antfly/lib/termite"
	"github.com/antflydb/antfly/pkg/genkit/openrouter"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/compat_oai/anthropic"
	"github.com/firebase/genkit/go/plugins/compat_oai/openai"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/firebase/genkit/go/plugins/ollama"
	"github.com/openai/openai-go/option"
)

var mediaSupportedModels = []string{"gemma3:4b", "gemma3:12b", "gemma3:27b"}

// getConfigOrEnv returns the config value if set, otherwise checks environment variable
func getConfigOrEnv(configVal *string, envVar string) string {
	if configVal != nil && *configVal != "" {
		return *configVal
	}
	return os.Getenv(envVar)
}

func NewGenKitGenerator(
	ctx context.Context,
	config GeneratorConfig,
) (*GenKitModelImpl, error) {
	var g *genkit.Genkit
	var model ai.Model
	var genkitOpts []GenKitOption

	switch config.Provider {
	case GeneratorProviderOllama:
		c, err := config.AsOllamaGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing ollama config: %w", err)
		}

		if c.Url == nil || *c.Url == "" {
			defaultUrl := "http://localhost:11434"
			c.Url = &defaultUrl
		}
		timeout := 540 // 9 minutes, matches Termite default
		if c.Timeout != nil && *c.Timeout > 0 {
			timeout = *c.Timeout
		}
		ollamaPlugin := &ollama.Ollama{ServerAddress: *c.Url, Timeout: timeout}

		g = genkit.Init(ctx,
			genkit.WithPlugins(ollamaPlugin),
		)

		var modelOpts *ai.ModelOptions
		if slices.Contains(mediaSupportedModels, c.Model) {
			modelOpts = &ai.ModelOptions{
				Label: c.Model,
				Supports: &ai.ModelSupports{
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

	case GeneratorProviderGemini:
		c, err := config.AsGoogleGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing google config: %w", err)
		}

		// Default to gemini-2.5-flash if no model specified
		if c.Model == "" {
			c.Model = "gemini-2.5-flash"
		}

		googlePlugin := &googlegenai.GoogleAI{}
		if c.ApiKey != nil && *c.ApiKey != "" {
			googlePlugin.APIKey = *c.ApiKey
		}

		g = genkit.Init(ctx,
			genkit.WithPlugins(googlePlugin),
		)

		if model = googlegenai.GoogleAIModel(g, c.Model); model == nil {
			return nil, fmt.Errorf("google model not found: %s", c.Model)
		}
		// Alternatively, you may create a ModelRef which pairs the model name with its config:
		//
		// modelRef := googlegenai.GoogleAIModelRef("gemini-2.5-flash", &genai.GenerateContentConfig{
		//     Temperature: genai.Ptr[float32](0.5),
		//     MaxOutputTokens: genai.Ptr[int32](500),
		//     // Other configuration...
		// })

		// Use the model name from config (e.g., "gemini-2.0-flash-exp")
		// var modelErr error
		// model, modelErr = googlePlugin.DefineModel(g, c.Model, nil)
		// if modelErr != nil {
		// 	return nil, fmt.Errorf("defining google model: %w", modelErr)
		// }

	case GeneratorProviderVertex:
		c, err := config.AsVertexGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing vertex config: %w", err)
		}

		// Default to gemini-2.5-flash if no model specified
		if c.Model == "" {
			c.Model = "gemini-2.5-flash"
		}

		// Get project and location from config or environment
		project := getConfigOrEnv(c.ProjectId, "GOOGLE_CLOUD_PROJECT")
		if project == "" {
			return nil, errors.New(
				"project_id is required for Vertex AI (set in config or GOOGLE_CLOUD_PROJECT env var)",
			)
		}

		location := getConfigOrEnv(c.Location, "GOOGLE_CLOUD_LOCATION")
		if location == "" {
			location = "us-central1" // Default region
		}

		// Set up authentication via environment variables
		// The genkit VertexAI plugin uses Application Default Credentials (ADC)
		// We need to set GOOGLE_APPLICATION_CREDENTIALS if credentials are provided
		if c.CredentialsPath != nil && *c.CredentialsPath != "" {
			if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", *c.CredentialsPath); err != nil {
				return nil, fmt.Errorf("failed to set GOOGLE_APPLICATION_CREDENTIALS: %w", err)
			}
		}
		// Note: credentials_json is not directly supported by genkit's VertexAI plugin
		// Users should use credentials_path or rely on ADC

		// Initialize Vertex AI plugin
		vertexPlugin := &googlegenai.VertexAI{
			ProjectID: project,
			Location:  location,
		}

		g = genkit.Init(ctx,
			genkit.WithPlugins(vertexPlugin),
		)

		// Get model reference
		model = googlegenai.VertexAIModel(g, c.Model)
		if model == nil {
			return nil, fmt.Errorf("vertex model not found: %s", c.Model)
		}

	case GeneratorProviderOpenai:
		c, err := config.AsOpenAIGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing openai config: %w", err)
		}

		openaiPlugin := &openai.OpenAI{}
		if c.ApiKey != nil && *c.ApiKey != "" {
			openaiPlugin.APIKey = *c.ApiKey
		}

		// Configure base URL using request options
		var opts []option.RequestOption
		if c.Url != nil && *c.Url != "" {
			opts = append(opts, option.WithBaseURL(*c.Url))
		}
		if len(opts) > 0 {
			openaiPlugin.Opts = opts
		}

		g = genkit.Init(ctx,
			genkit.WithPlugins(openaiPlugin),
		)

		// Use the model name from config (e.g., "gpt-4o")
		// OpenAI.DefineModel doesn't take genkit instance and returns only ai.Model (no error)
		model = openaiPlugin.DefineModel(c.Model, ai.ModelOptions{})

	case GeneratorProviderAnthropic:
		c, err := config.AsAnthropicGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing anthropic config: %w", err)
		}

		// Default to claude-3-7-sonnet-20250219 if no model specified
		if c.Model == "" {
			c.Model = "claude-3-7-sonnet-20250219"
		}

		// Configure request options
		var opts []option.RequestOption

		// API key from config or environment variable
		apiKey := ""
		if c.ApiKey != nil && *c.ApiKey != "" {
			apiKey = *c.ApiKey
		} else if envKey := os.Getenv("ANTHROPIC_API_KEY"); envKey != "" {
			apiKey = envKey
		}
		if apiKey != "" {
			opts = append(opts, option.WithAPIKey(apiKey))
		}

		// Configure base URL if provided
		if c.Url != nil && *c.Url != "" {
			opts = append(opts, option.WithBaseURL(*c.Url))
		}

		anthropicPlugin := &anthropic.Anthropic{
			Opts: opts,
		}

		g = genkit.Init(ctx,
			genkit.WithPlugins(anthropicPlugin),
		)

		// Use the model name from config (e.g., "claude-3-7-sonnet-20250219")
		model = anthropicPlugin.DefineModel(c.Model, ai.ModelOptions{})

	case GeneratorProviderOpenrouter:
		c, err := config.AsOpenRouterGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing openrouter config: %w", err)
		}

		// Build the list of models - support both single `model` and `models` array
		var models []string
		if c.Model != nil && *c.Model != "" {
			models = append(models, *c.Model)
		}
		if c.Models != nil && len(*c.Models) > 0 {
			models = append(models, *c.Models...)
		}
		if len(models) == 0 {
			return nil, errors.New("openrouter: either model or models must be provided")
		}

		// API key from config or environment variable (plugin handles fallback to env var)
		apiKey := ""
		if c.ApiKey != nil && *c.ApiKey != "" {
			apiKey = *c.ApiKey
		}

		openrouterPlugin := &openrouter.OpenRouter{
			APIKey: apiKey,
		}

		g = genkit.Init(ctx,
			genkit.WithPlugins(openrouterPlugin),
		)

		// Define model with the OpenRouter plugin
		// The plugin supports fallback routing when multiple models are provided
		model = openrouterPlugin.DefineModel(g, openrouter.ModelDefinition{
			Models: models,
		}, nil)

	case GeneratorProviderTermite:
		c, err := config.AsTermiteGeneratorConfig()
		if err != nil {
			return nil, fmt.Errorf("parsing termite config: %w", err)
		}

		// Resolve Termite URL: explicit config → env var → global default
		configURL := ""
		if c.ApiUrl != nil {
			configURL = *c.ApiUrl
		}
		apiURL := libtermite.ResolveURL(configURL)
		if apiURL == "" {
			return nil, errors.New("termite: api_url is required (set via config, ANTFLY_TERMITE_URL env var, or termite.api_url in config file)")
		}

		// Build request options with Termite's OpenAI-compatible API URL
		// Termite provides OpenAI-compatible endpoints at /openai/v1/*
		termiteURL := apiURL + "/openai/v1"
		termiteTimeout := 540 // 9 minutes default, matches Termite embedding/reranking clients
		if c.Timeout != nil && *c.Timeout > 0 {
			termiteTimeout = *c.Timeout
		}
		opts := []option.RequestOption{
			option.WithBaseURL(termiteURL),
			option.WithHTTPClient(&http.Client{Timeout: time.Duration(termiteTimeout) * time.Second}),
		}

		openaiPlugin := &openai.OpenAI{
			// Termite doesn't require an API key, but the OpenAI plugin requires one
			APIKey: "termite-local",
			Opts:   opts,
		}

		g = genkit.Init(ctx,
			genkit.WithPlugins(openaiPlugin),
		)

		// Use the model name from config (e.g., "onnxruntime/Gemma-3-ONNX")
		model = openaiPlugin.DefineModel(c.Model, ai.ModelOptions{})

		if c.MaxTokens != nil && *c.MaxTokens > 0 {
			genkitOpts = append(genkitOpts, WithMaxOutputTokens(*c.MaxTokens))
		}

	default:
		return nil, fmt.Errorf("unsupported provider: %s", config.Provider)
	}

	return NewGenKitSummarizer(g, model, genkitOpts...), nil
}
