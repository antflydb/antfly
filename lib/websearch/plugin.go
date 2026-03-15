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

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package websearch

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

// SearchProvider is the interface that all web search providers must implement
type SearchProvider interface {
	Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error)
	Name() string
}

// SearchOptions contains options for search queries
type SearchOptions struct {
	MaxResults int
	Language   string
	Region     string
	SafeSearch bool
	TimeoutMS  int
}

// FetchOptions contains options for URL fetching
type FetchOptions struct {
	MaxContentLength int
	ExtractMode      string
	TimeoutMS        int
	UserAgent        string
}

// getConfigOrEnv returns the config value if set, otherwise checks environment variable
func getConfigOrEnv(configVal *string, envVar string) string {
	if configVal != nil && *configVal != "" {
		return *configVal
	}
	return os.Getenv(envVar)
}

// NewSearchProvider creates a search provider based on the config
func NewSearchProvider(config WebSearchConfig) (SearchProvider, error) {
	switch config.Provider {
	case WebSearchProviderGoogle:
		return newGoogleProvider(config)
	case WebSearchProviderBing:
		return newBingProvider(config)
	case WebSearchProviderSerper:
		return newSerperProvider(config)
	case WebSearchProviderTavily:
		return newTavilyProvider(config)
	case WebSearchProviderBrave:
		return newBraveProvider(config)
	case WebSearchProviderDuckduckgo:
		return newDuckDuckGoProvider(config)
	default:
		return nil, fmt.Errorf("unsupported search provider: %s", config.Provider)
	}
}

// BaseProvider contains common functionality for all providers
type BaseProvider struct {
	client     *http.Client
	maxResults int
	language   string
	region     string
	safeSearch bool
}

func newBaseProvider(config WebSearchConfig) BaseProvider {
	timeout := 10 * time.Second
	if config.TimeoutMs != 0 {
		timeout = time.Duration(config.TimeoutMs) * time.Millisecond
	}

	maxResults := 5
	if config.MaxResults != 0 {
		maxResults = config.MaxResults
	}

	safeSearch := true
	if config.SafeSearch != nil {
		safeSearch = *config.SafeSearch
	}

	return BaseProvider{
		client: &http.Client{
			Timeout: timeout,
		},
		maxResults: maxResults,
		language:   config.Language,
		region:     config.Region,
		safeSearch: safeSearch,
	}
}
