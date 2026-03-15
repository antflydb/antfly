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

package websearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Google Custom Search provider
type googleProvider struct {
	BaseProvider
	apiKey string
	cseID  string
}

func newGoogleProvider(config WebSearchConfig) (*googleProvider, error) {
	apiKey := getConfigOrEnv(nil, "GOOGLE_CSE_API_KEY")
	cseID := getConfigOrEnv(nil, "GOOGLE_CSE_ID")

	if apiKey == "" {
		return nil, fmt.Errorf("google CSE API key required (set GOOGLE_CSE_API_KEY)")
	}
	if cseID == "" {
		return nil, fmt.Errorf("google CSE ID required (set GOOGLE_CSE_ID)")
	}

	return &googleProvider{
		BaseProvider: newBaseProvider(config),
		apiKey:       apiKey,
		cseID:        cseID,
	}, nil
}

func (p *googleProvider) Name() string { return "google" }

func (p *googleProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	maxResults := opts.MaxResults
	if maxResults == 0 {
		maxResults = p.maxResults
	}

	u, _ := url.Parse("https://www.googleapis.com/customsearch/v1")
	q := u.Query()
	q.Set("key", p.apiKey)
	q.Set("cx", p.cseID)
	q.Set("q", query)
	q.Set("num", strconv.Itoa(min(maxResults, 10))) // Google max is 10
	if opts.SafeSearch || p.safeSearch {
		q.Set("safe", "active")
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("google search failed: %s - %s", resp.Status, string(body))
	}

	var result struct {
		Items []struct {
			Title   string `json:"title"`
			Link    string `json:"link"`
			Snippet string `json:"snippet"`
		} `json:"items"`
		SearchInformation struct {
			TotalResults string `json:"totalResults"`
		} `json:"searchInformation"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.Items)),
		SearchTimeMs: int(time.Since(start).Milliseconds()),
	}

	if total, err := strconv.Atoi(result.SearchInformation.TotalResults); err == nil {
		response.TotalResults = total
	}

	for _, item := range result.Items {
		u, _ := url.Parse(item.Link)
		response.Results = append(response.Results, WebSearchResult{
			Title:   item.Title,
			Url:     item.Link,
			Snippet: item.Snippet,
			Source:  u.Host,
		})
	}

	return response, nil
}

// Bing Search provider
type bingProvider struct {
	BaseProvider
	apiKey   string
	endpoint string
}

func newBingProvider(config WebSearchConfig) (*bingProvider, error) {
	apiKey := getConfigOrEnv(nil, "BING_SEARCH_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("bing search API key required (set BING_SEARCH_API_KEY)")
	}

	endpoint := "https://api.bing.microsoft.com/v7.0/search"

	return &bingProvider{
		BaseProvider: newBaseProvider(config),
		apiKey:       apiKey,
		endpoint:     endpoint,
	}, nil
}

func (p *bingProvider) Name() string { return "bing" }

func (p *bingProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	maxResults := opts.MaxResults
	if maxResults == 0 {
		maxResults = p.maxResults
	}

	u, _ := url.Parse(p.endpoint)
	q := u.Query()
	q.Set("q", query)
	q.Set("count", strconv.Itoa(maxResults))
	if opts.SafeSearch || p.safeSearch {
		q.Set("safeSearch", "Strict")
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Ocp-Apim-Subscription-Key", p.apiKey)

	start := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bing search failed: %s - %s", resp.Status, string(body))
	}

	var result struct {
		WebPages struct {
			TotalEstimatedMatches int `json:"totalEstimatedMatches"`
			Value                 []struct {
				Name            string `json:"name"`
				URL             string `json:"url"`
				Snippet         string `json:"snippet"`
				DisplayURL      string `json:"displayUrl"`
				DateLastCrawled string `json:"dateLastCrawled"`
			} `json:"value"`
		} `json:"webPages"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0, len(result.WebPages.Value)),
		TotalResults: result.WebPages.TotalEstimatedMatches,
		SearchTimeMs: int(time.Since(start).Milliseconds()),
	}

	for _, item := range result.WebPages.Value {
		u, _ := url.Parse(item.URL)
		response.Results = append(response.Results, WebSearchResult{
			Title:   item.Name,
			Url:     item.URL,
			Snippet: item.Snippet,
			Source:  u.Host,
		})
	}

	return response, nil
}

// Serper provider
type serperProvider struct {
	BaseProvider
	apiKey string
}

func newSerperProvider(config WebSearchConfig) (*serperProvider, error) {
	apiKey := getConfigOrEnv(nil, "SERPER_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("serper API key required (set SERPER_API_KEY)")
	}

	return &serperProvider{
		BaseProvider: newBaseProvider(config),
		apiKey:       apiKey,
	}, nil
}

func (p *serperProvider) Name() string { return "serper" }

func (p *serperProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	maxResults := opts.MaxResults
	if maxResults == 0 {
		maxResults = p.maxResults
	}

	payload := map[string]any{
		"q":   query,
		"num": maxResults,
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", "https://google.serper.dev/search",
		http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(&readCloser{data: body})
	req.Header.Set("X-API-KEY", p.apiKey)
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("serper search failed: %s - %s", resp.Status, string(respBody))
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
		SearchTimeMs: int(time.Since(start).Milliseconds()),
	}

	if result.AnswerBox.Answer != "" {
		response.Answer = result.AnswerBox.Answer
	}

	for _, item := range result.Organic {
		u, _ := url.Parse(item.Link)
		response.Results = append(response.Results, WebSearchResult{
			Title:   item.Title,
			Url:     item.Link,
			Snippet: item.Snippet,
			Source:  u.Host,
		})
	}

	return response, nil
}

// Tavily provider
type tavilyProvider struct {
	BaseProvider
	apiKey        string
	searchDepth   string
	includeAnswer bool
}

func newTavilyProvider(config WebSearchConfig) (*tavilyProvider, error) {
	apiKey := getConfigOrEnv(nil, "TAVILY_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("tavily API key required (set TAVILY_API_KEY)")
	}

	return &tavilyProvider{
		BaseProvider:  newBaseProvider(config),
		apiKey:        apiKey,
		searchDepth:   "basic",
		includeAnswer: true,
	}, nil
}

func (p *tavilyProvider) Name() string { return "tavily" }

func (p *tavilyProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	maxResults := opts.MaxResults
	if maxResults == 0 {
		maxResults = p.maxResults
	}

	payload := map[string]any{
		"api_key":        p.apiKey,
		"query":          query,
		"max_results":    maxResults,
		"search_depth":   p.searchDepth,
		"include_answer": p.includeAnswer,
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.tavily.com/search",
		http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(&readCloser{data: body})
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("tavily search failed: %s - %s", resp.Status, string(respBody))
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
		SearchTimeMs: int(time.Since(start).Milliseconds()),
	}

	for _, item := range result.Results {
		u, _ := url.Parse(item.URL)
		score := float32(item.Score)
		response.Results = append(response.Results, WebSearchResult{
			Title:   item.Title,
			Url:     item.URL,
			Snippet: item.Content,
			Source:  u.Host,
			Score:   score,
		})
	}

	return response, nil
}

// Brave Search provider
type braveProvider struct {
	BaseProvider
	apiKey string
}

func newBraveProvider(config WebSearchConfig) (*braveProvider, error) {
	apiKey := getConfigOrEnv(nil, "BRAVE_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("brave search API key required (set BRAVE_API_KEY)")
	}

	return &braveProvider{
		BaseProvider: newBaseProvider(config),
		apiKey:       apiKey,
	}, nil
}

func (p *braveProvider) Name() string { return "brave" }

func (p *braveProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	maxResults := opts.MaxResults
	if maxResults == 0 {
		maxResults = p.maxResults
	}

	u, _ := url.Parse("https://api.search.brave.com/res/v1/web/search")
	q := u.Query()
	q.Set("q", query)
	q.Set("count", strconv.Itoa(maxResults))
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

	start := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("brave search failed: %s - %s", resp.Status, string(respBody))
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
		SearchTimeMs: int(time.Since(start).Milliseconds()),
	}

	for _, item := range result.Web.Results {
		u, _ := url.Parse(item.URL)
		response.Results = append(response.Results, WebSearchResult{
			Title:   item.Title,
			Url:     item.URL,
			Snippet: item.Description,
			Source:  u.Host,
		})
	}

	return response, nil
}

// DuckDuckGo provider (limited, free)
type duckduckgoProvider struct {
	BaseProvider
}

func newDuckDuckGoProvider(config WebSearchConfig) (*duckduckgoProvider, error) {
	return &duckduckgoProvider{
		BaseProvider: newBaseProvider(config),
	}, nil
}

func (p *duckduckgoProvider) Name() string { return "duckduckgo" }

func (p *duckduckgoProvider) Search(ctx context.Context, query string, opts SearchOptions) (*WebSearchResponse, error) {
	// DuckDuckGo Instant Answer API is very limited
	// For production, recommend using other providers
	u, _ := url.Parse("https://api.duckduckgo.com/")
	q := u.Query()
	q.Set("q", query)
	q.Set("format", "json")
	q.Set("no_redirect", "1")
	q.Set("no_html", "1")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := p.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		AbstractText   string `json:"AbstractText"`
		AbstractURL    string `json:"AbstractURL"`
		AbstractSource string `json:"AbstractSource"`
		RelatedTopics  []struct {
			Text     string `json:"Text"`
			FirstURL string `json:"FirstURL"`
		} `json:"RelatedTopics"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	response := &WebSearchResponse{
		Query:        query,
		Results:      make([]WebSearchResult, 0),
		SearchTimeMs: int(time.Since(start).Milliseconds()),
	}

	// Add abstract if available
	if result.AbstractText != "" && result.AbstractURL != "" {
		response.Results = append(response.Results, WebSearchResult{
			Title:   result.AbstractSource,
			Url:     result.AbstractURL,
			Snippet: result.AbstractText,
			Source:  result.AbstractSource,
		})
		response.Answer = result.AbstractText
	}

	// Add related topics
	for _, topic := range result.RelatedTopics {
		if topic.FirstURL != "" {
			u, _ := url.Parse(topic.FirstURL)
			response.Results = append(response.Results, WebSearchResult{
				Title:   topic.Text,
				Url:     topic.FirstURL,
				Snippet: topic.Text,
				Source:  u.Host,
			})
		}
	}

	return response, nil
}

// Helper for reading request body
type readCloser struct {
	data []byte
	pos  int
}

func (r *readCloser) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *readCloser) Close() error {
	return nil
}
