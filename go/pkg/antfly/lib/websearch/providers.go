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

package websearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/vertex"
)

type apiKeyProvider struct {
	BaseProvider
	name     string
	apiKey   string
	endpoint string
}

func newExaProvider(config WebSearchConfig) (*apiKeyProvider, error) {
	apiKey, err := getResolvedConfigOrEnv(&config.ApiKey, "EXA_API_KEY")
	if err != nil {
		return nil, fmt.Errorf("resolving exa API key: %w", err)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("exa API key required (set api_key or EXA_API_KEY)")
	}
	endpoint := firstNonEmpty(config.Endpoint, "https://api.exa.ai/search")
	if endpoint == "" {
		endpoint = "https://api.exa.ai/search"
	}
	return &apiKeyProvider{
		BaseProvider: newBaseProvider(config),
		name:         "exa",
		apiKey:       apiKey,
		endpoint:     endpoint,
	}, nil
}

func newSerperProvider(config WebSearchConfig) (*apiKeyProvider, error) {
	apiKey, err := getResolvedConfigOrEnv(&config.ApiKey, "SERPER_API_KEY")
	if err != nil {
		return nil, fmt.Errorf("resolving serper API key: %w", err)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("serper API key required (set api_key or SERPER_API_KEY)")
	}
	endpoint := firstNonEmpty(config.Endpoint, "https://google.serper.dev/search")
	if endpoint == "" {
		endpoint = "https://google.serper.dev/search"
	}
	return &apiKeyProvider{
		BaseProvider: newBaseProvider(config),
		name:         "serper",
		apiKey:       apiKey,
		endpoint:     endpoint,
	}, nil
}

func newTavilyProvider(config WebSearchConfig) (*apiKeyProvider, error) {
	apiKey, err := getResolvedConfigOrEnv(&config.ApiKey, "TAVILY_API_KEY")
	if err != nil {
		return nil, fmt.Errorf("resolving tavily API key: %w", err)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("tavily API key required (set api_key or TAVILY_API_KEY)")
	}
	endpoint := firstNonEmpty(config.Endpoint, "https://api.tavily.com/search")
	if endpoint == "" {
		endpoint = "https://api.tavily.com/search"
	}
	return &apiKeyProvider{
		BaseProvider: newBaseProvider(config),
		name:         "tavily",
		apiKey:       apiKey,
		endpoint:     endpoint,
	}, nil
}

func newBraveProvider(config WebSearchConfig) (*apiKeyProvider, error) {
	apiKey, err := getResolvedConfigOrEnv(&config.ApiKey, "BRAVE_API_KEY")
	if err != nil {
		return nil, fmt.Errorf("resolving brave API key: %w", err)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("brave search API key required (set api_key or BRAVE_API_KEY)")
	}
	endpoint := firstNonEmpty(config.Endpoint, "https://api.search.brave.com/res/v1/web/search")
	if endpoint == "" {
		endpoint = "https://api.search.brave.com/res/v1/web/search"
	}
	return &apiKeyProvider{
		BaseProvider: newBaseProvider(config),
		name:         "brave",
		apiKey:       apiKey,
		endpoint:     endpoint,
	}, nil
}

func newYouProvider(config WebSearchConfig) (*apiKeyProvider, error) {
	apiKey, err := getResolvedConfigOrEnv(&config.ApiKey, "YOU_API_KEY")
	if err != nil {
		return nil, fmt.Errorf("resolving you.com API key: %w", err)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("you.com API key required (set api_key or YOU_API_KEY)")
	}
	endpoint := getConfigOrEnv(&config.Endpoint, "YOU_SEARCH_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://api.ydc-index.io/search"
	}
	return &apiKeyProvider{
		BaseProvider: newBaseProvider(config),
		name:         "you",
		apiKey:       apiKey,
		endpoint:     endpoint,
	}, nil
}

func newLinkupProvider(config WebSearchConfig) (*apiKeyProvider, error) {
	apiKey, err := getResolvedConfigOrEnv(&config.ApiKey, "LINKUP_API_KEY")
	if err != nil {
		return nil, fmt.Errorf("resolving linkup API key: %w", err)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("linkup API key required (set api_key or LINKUP_API_KEY)")
	}
	endpoint := getConfigOrEnv(&config.Endpoint, "LINKUP_SEARCH_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://api.linkup.so/v1/search"
	}
	return &apiKeyProvider{
		BaseProvider: newBaseProvider(config),
		name:         "linkup",
		apiKey:       apiKey,
		endpoint:     endpoint,
	}, nil
}

type vertexProvider struct {
	BaseProvider
	client        *http.Client
	projectID     string
	location      string
	dataStore     string
	servingConfig string
	endpoint      string
}

type vertexSearchResponse struct {
	Results []struct {
		Document vertexSearchDocument `json:"document"`
	} `json:"results"`
	Summary struct {
		SummaryText string `json:"summaryText"`
	} `json:"summary"`
}

type vertexSearchDocument struct {
	ID                string         `json:"id"`
	Name              string         `json:"name"`
	StructData        map[string]any `json:"structData"`
	DerivedStructData map[string]any `json:"derivedStructData"`
}

func newVertexProvider(config WebSearchConfig) (*vertexProvider, error) {
	projectID, err := getResolvedConfigOrEnv(&config.ProjectId, "GOOGLE_CLOUD_PROJECT")
	if err != nil {
		return nil, fmt.Errorf("resolving vertex project_id: %w", err)
	}
	if projectID == "" {
		return nil, fmt.Errorf("vertex project_id required (set project_id or GOOGLE_CLOUD_PROJECT)")
	}

	location, err := getResolvedConfigOrEnv(&config.Location, "GOOGLE_CLOUD_LOCATION")
	if err != nil {
		return nil, fmt.Errorf("resolving vertex location: %w", err)
	}
	if location == "" {
		location = "global"
	}

	dataStore, err := resolveConfigString(config.DataStore)
	if err != nil {
		return nil, fmt.Errorf("resolving vertex data_store: %w", err)
	}
	if dataStore == "" {
		return nil, fmt.Errorf("vertex data_store required")
	}

	servingConfig, err := resolveConfigString(config.ServingConfig)
	if err != nil {
		return nil, fmt.Errorf("resolving vertex serving_config: %w", err)
	}
	if servingConfig == "" {
		servingConfig = "default_config"
	}

	credentialsPath, err := resolveConfigString(config.CredentialsPath)
	if err != nil {
		return nil, fmt.Errorf("resolving vertex credentials_path: %w", err)
	}
	var credentialsPathPtr *string
	if credentialsPath != "" {
		credentialsPathPtr = &credentialsPath
	}
	creds, err := vertex.LoadCredentials(credentialsPathPtr, []string{vertex.CloudPlatformScope})
	if err != nil {
		return nil, fmt.Errorf("resolving vertex credentials: %w", err)
	}

	endpoint, err := resolveConfigString(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("resolving vertex endpoint: %w", err)
	}
	if endpoint == "" {
		endpoint = vertexSearchEndpoint(projectID, location, dataStore, servingConfig)
	}

	return &vertexProvider{
		BaseProvider:  newBaseProvider(config),
		client:        vertex.AuthHTTPClient(creds),
		projectID:     projectID,
		location:      location,
		dataStore:     dataStore,
		servingConfig: servingConfig,
		endpoint:      endpoint,
	}, nil
}

func resolveConfigString(value string) (string, error) {
	return getResolvedConfigOrEnv(&value, "")
}

func vertexSearchEndpoint(projectID, location, dataStore, servingConfig string) string {
	return fmt.Sprintf(
		"https://discoveryengine.googleapis.com/v1/projects/%s/locations/%s/collections/default_collection/dataStores/%s/servingConfigs/%s:search",
		url.PathEscape(projectID),
		url.PathEscape(location),
		url.PathEscape(dataStore),
		url.PathEscape(servingConfig),
	)
}

func (p *apiKeyProvider) Name() string { return p.name }

func (p *vertexProvider) Name() string { return "vertex" }

func (p *vertexProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	payload := map[string]any{
		"query":    query,
		"pageSize": p.max(opts),
	}
	if opts.SafeSearch || p.safeSearch {
		payload["safeSearch"] = true
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Goog-User-Project", p.projectID)

	started := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, providerStatusError("vertex", resp)
	}

	var result vertexSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Results)),
		Answer:       result.Summary.SummaryText,
		SearchTimeMs: int(time.Since(started).Milliseconds()),
	}
	for _, item := range result.Results {
		title, rawURL, snippet := vertexResultFields(item.Document)
		if rawURL == "" {
			continue
		}
		response.Results = append(response.Results, resultItem(title, rawURL, snippet, 0, ""))
	}
	return response, nil
}

func vertexResultFields(document vertexSearchDocument) (string, string, string) {
	title := firstNonEmpty(
		mapString(document.DerivedStructData, "title", "htmlTitle", "name"),
		mapString(document.StructData, "title", "name"),
		document.ID,
	)
	rawURL := firstNonEmpty(
		mapString(document.DerivedStructData, "link", "url", "uri"),
		mapString(document.StructData, "link", "url", "uri"),
	)
	snippet := firstNonEmpty(
		mapString(document.DerivedStructData, "snippet", "snippets", "extractive_answers", "extractiveAnswers", "description"),
		mapString(document.StructData, "snippet", "description", "body", "content", "text"),
	)
	return title, rawURL, snippet
}

func mapString(values map[string]any, keys ...string) string {
	if values == nil {
		return ""
	}
	for _, key := range keys {
		if value := stringValue(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := stringValue(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		return firstNonEmpty(
			mapString(v, "snippet", "content", "answer", "text", "title", "url", "link"),
		)
	default:
		return ""
	}
}

func (p *apiKeyProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	switch p.name {
	case "exa":
		return p.searchExa(ctx, query, opts)
	case "serper":
		return p.searchSerper(ctx, query, opts)
	case "tavily":
		return p.searchTavily(ctx, query, opts)
	case "brave":
		return p.searchBrave(ctx, query, opts)
	case "you":
		return p.searchYou(ctx, query, opts)
	case "linkup":
		return p.searchLinkup(ctx, query, opts)
	default:
		return nil, fmt.Errorf("unsupported search provider: %s", p.name)
	}
}

func (p *apiKeyProvider) max(opts SearchOptions) int {
	if opts.MaxResults > 0 {
		return opts.MaxResults
	}
	return p.maxResults
}

func (p *vertexProvider) max(opts SearchOptions) int {
	if opts.MaxResults > 0 {
		return opts.MaxResults
	}
	return p.maxResults
}

func (p *apiKeyProvider) searchExa(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	payload := map[string]any{
		"query":      query,
		"numResults": p.max(opts),
		"type":       "auto",
	}
	if p.includeContent || p.includeHighlights {
		contents := map[string]any{}
		if p.includeContent {
			contents["text"] = true
		}
		if p.includeHighlights {
			contents["highlights"] = true
		}
		payload["contents"] = contents
	}

	resp, started, err := p.postJSON(ctx, p.endpoint, payload, func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, providerStatusError("exa", resp)
	}

	var result struct {
		Results []struct {
			Title         string   `json:"title"`
			URL           string   `json:"url"`
			Text          string   `json:"text"`
			Highlights    []string `json:"highlights"`
			Score         float64  `json:"score"`
			PublishedDate string   `json:"publishedDate"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Results)),
		SearchTimeMs: int(time.Since(started).Milliseconds()),
	}
	for _, item := range result.Results {
		response.Results = append(response.Results, resultItem(item.Title, item.URL, firstNonEmpty(item.Text, strings.Join(item.Highlights, "\n")), float32(item.Score), item.PublishedDate))
	}
	return response, nil
}

func (p *apiKeyProvider) searchSerper(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	payload := map[string]any{
		"q":   query,
		"num": p.max(opts),
	}
	resp, started, err := p.postJSON(ctx, p.endpoint, payload, func(req *http.Request) {
		req.Header.Set("X-API-KEY", p.apiKey)
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, providerStatusError("serper", resp)
	}

	var result struct {
		Organic []struct {
			Title   string `json:"title"`
			Link    string `json:"link"`
			Snippet string `json:"snippet"`
		} `json:"organic"`
		AnswerBox struct {
			Answer string `json:"answer"`
		} `json:"answerBox"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Organic)),
		Answer:       result.AnswerBox.Answer,
		SearchTimeMs: int(time.Since(started).Milliseconds()),
	}
	for _, item := range result.Organic {
		response.Results = append(response.Results, resultItem(item.Title, item.Link, item.Snippet, 0, ""))
	}
	return response, nil
}

func (p *apiKeyProvider) searchTavily(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	payload := map[string]any{
		"api_key":             p.apiKey,
		"query":               query,
		"max_results":         p.max(opts),
		"search_depth":        "basic",
		"include_answer":      true,
		"include_raw_content": p.includeContent,
	}
	resp, started, err := p.postJSON(ctx, p.endpoint, payload, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, providerStatusError("tavily", resp)
	}

	var result struct {
		Answer  string `json:"answer"`
		Results []struct {
			Title   string  `json:"title"`
			URL     string  `json:"url"`
			Content string  `json:"content"`
			Score   float64 `json:"score"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Results)),
		Answer:       result.Answer,
		SearchTimeMs: int(time.Since(started).Milliseconds()),
	}
	for _, item := range result.Results {
		response.Results = append(response.Results, resultItem(item.Title, item.URL, item.Content, float32(item.Score), ""))
	}
	return response, nil
}

func (p *apiKeyProvider) searchBrave(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	u, _ := url.Parse(p.endpoint)
	q := u.Query()
	q.Set("q", query)
	q.Set("count", strconv.Itoa(p.max(opts)))
	if opts.SafeSearch || p.safeSearch {
		q.Set("safesearch", "strict")
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Subscription-Token", p.apiKey)
	req.Header.Set("Accept", "application/json")

	started := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, providerStatusError("brave", resp)
	}

	var result struct {
		Web struct {
			Results []struct {
				Title       string `json:"title"`
				URL         string `json:"url"`
				Description string `json:"description"`
			} `json:"results"`
		} `json:"web"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Web.Results)),
		SearchTimeMs: int(time.Since(started).Milliseconds()),
	}
	for _, item := range result.Web.Results {
		response.Results = append(response.Results, resultItem(item.Title, item.URL, item.Description, 0, ""))
	}
	return response, nil
}

func (p *apiKeyProvider) searchYou(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	u, _ := url.Parse(p.endpoint)
	q := u.Query()
	q.Set("query", query)
	q.Set("num_web_results", strconv.Itoa(p.max(opts)))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", p.apiKey)
	req.Header.Set("Accept", "application/json")

	started := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, providerStatusError("you", resp)
	}

	var result struct {
		Hits []struct {
			Title       string   `json:"title"`
			URL         string   `json:"url"`
			Description string   `json:"description"`
			Snippet     string   `json:"snippet"`
			Snippets    []string `json:"snippets"`
		} `json:"hits"`
		WebResults []struct {
			Title       string `json:"title"`
			URL         string `json:"url"`
			Description string `json:"description"`
			Snippet     string `json:"snippet"`
		} `json:"web_results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Hits)+len(result.WebResults)),
		SearchTimeMs: int(time.Since(started).Milliseconds()),
	}
	for _, item := range result.Hits {
		response.Results = append(response.Results, resultItem(item.Title, item.URL, firstNonEmpty(item.Snippet, item.Description, strings.Join(item.Snippets, "\n")), 0, ""))
	}
	for _, item := range result.WebResults {
		response.Results = append(response.Results, resultItem(item.Title, item.URL, firstNonEmpty(item.Snippet, item.Description), 0, ""))
	}
	return response, nil
}

func (p *apiKeyProvider) searchLinkup(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	payload := map[string]any{
		"q":          query,
		"depth":      "standard",
		"outputType": "searchResults",
	}
	if maxResults := p.max(opts); maxResults > 0 {
		payload["maxResults"] = maxResults
	}
	resp, started, err := p.postJSON(ctx, p.endpoint, payload, func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, providerStatusError("linkup", resp)
	}

	var result struct {
		Answer  string `json:"answer"`
		Results []struct {
			Name    string  `json:"name"`
			Title   string  `json:"title"`
			URL     string  `json:"url"`
			Content string  `json:"content"`
			Snippet string  `json:"snippet"`
			Score   float64 `json:"score"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Results)),
		Answer:       result.Answer,
		SearchTimeMs: int(time.Since(started).Milliseconds()),
	}
	for _, item := range result.Results {
		response.Results = append(response.Results, resultItem(firstNonEmpty(item.Title, item.Name), item.URL, firstNonEmpty(item.Snippet, item.Content), float32(item.Score), ""))
	}
	return response, nil
}

func (p *apiKeyProvider) postJSON(ctx context.Context, endpoint string, payload map[string]any, decorate func(*http.Request)) (*http.Response, time.Time, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, time.Time{}, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if decorate != nil {
		decorate(req)
	}
	started := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	return resp, started, err
}

func providerStatusError(provider string, resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("%s search failed: %s - %s", provider, resp.Status, string(body))
}

func resultItem(title, rawURL, snippet string, score float32, publishedDate string) WebSearchResult {
	parsed, _ := url.Parse(rawURL)
	item := WebSearchResult{
		Title:   title,
		Url:     rawURL,
		Snippet: snippet,
		Score:   score,
	}
	if parsed != nil {
		item.Source = parsed.Host
	}
	if publishedDate != "" {
		if t, err := time.Parse(time.RFC3339, publishedDate); err == nil {
			item.PublishedDate = t
		}
	}
	return item
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
