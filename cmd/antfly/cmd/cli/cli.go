/*
Copyright 2025 The Antfly Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cli

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/spf13/cobra"
)

var antflyClient *AntflyClient

// defaultHTTPClient returns an HTTP client with standard timeouts for most CLI operations.
func defaultHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 90 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     time.Minute,
			DisableKeepAlives:   false,
		},
	}
}

// longTimeoutHTTPClient returns an HTTP client with extended timeouts for
// long-running operations like backup and restore.
func longTimeoutHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return &http.Client{
		Timeout: 540 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			DisableKeepAlives:     false,
			ResponseHeaderTimeout: 5 * time.Minute,
			IdleConnTimeout:       5 * time.Minute,
			DialContext:           dialer.DialContext,
		},
	}
}

// splitCSV splits a comma-separated string into trimmed, non-empty values.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for p := range strings.SplitSeq(s, ",") {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, t)
		}
	}
	return result
}

// parseJSONFlag unmarshals a JSON flag string into the target, returning a
// descriptive error that includes the flag name on failure.
func parseJSONFlag[T any](flagValue, flagName string) (T, error) {
	var result T
	if err := json.Unmarshal([]byte(flagValue), &result); err != nil {
		return result, fmt.Errorf("parsing --%s JSON: %w", flagName, err)
	}
	return result, nil
}

// parseFullTextSearch parses the mutually-exclusive --full-text-search and
// --full-text-search-json flags into a bleve Query.
func parseFullTextSearch(fts, ftsJSON string) (query.Query, error) {
	if fts != "" && ftsJSON != "" {
		return nil, fmt.Errorf("cannot specify both --full-text-search and --full-text-search-json")
	}
	if ftsJSON != "" {
		return query.ParseQuery([]byte(ftsJSON))
	}
	if fts != "" {
		return query.NewQueryStringQuery(fts), nil
	}
	return nil, nil
}

// parseOptionalReranker parses an optional --reranker JSON flag.
func parseOptionalReranker(rerankerStr string) (*antfly.RerankerConfig, error) {
	if rerankerStr == "" {
		return nil, nil
	}
	reranker, err := parseJSONFlag[antfly.RerankerConfig](rerankerStr, "reranker")
	if err != nil {
		return nil, err
	}
	return &reranker, nil
}

// parseOptionalPruner parses an optional --pruner JSON flag.
func parseOptionalPruner(prunerStr string) (antfly.Pruner, error) {
	if prunerStr == "" {
		return antfly.Pruner{}, nil
	}
	return parseJSONFlag[antfly.Pruner](prunerStr, "pruner")
}

// resolveURL returns the server URL from command flags.
// The default is set on the persistent flag definition.
func resolveURL(cmd *cobra.Command) string {
	url, _ := cmd.Flags().GetString("url")
	return url
}

// initClient initializes the global antflyClient with the given HTTP client.
func initClient(cmd *cobra.Command, httpClient *http.Client) error {
	var err error
	antflyClient, err = NewAntflyClient(resolveURL(cmd), httpClient)
	return err
}

// cliCommandNames is the set of CLI subcommand names, used to detect
// whether a root-level invocation needs client initialization.
// Keep in sync with the commands registered in RegisterCommands below.
var cliCommandNames = map[string]bool{
	"table": true, "index": true, "query": true, "lookup": true,
	"load": true, "insert": true, "delete": true, "agents": true,
	"backup": true, "restore": true, "internal": true,
}

// RegisterCommands registers CLI subcommands directly on a parent command.
func RegisterCommands(parent *cobra.Command) {
	if parent.PersistentFlags().Lookup("url") == nil {
		parent.PersistentFlags().String("url", "http://localhost:8080", "Antfly server URL")
	}

	// Chain client init onto parent's PersistentPreRunE, but only
	// trigger for CLI subcommands (not swarm/metadata/store/termite).
	origPreRunE := parent.PersistentPreRunE
	parent.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if origPreRunE != nil {
			if err := origPreRunE(cmd, args); err != nil {
				return err
			}
		}
		// Walk up to find the immediate child of root
		for p := cmd; p != nil; p = p.Parent() {
			if p.Parent() == parent && cliCommandNames[p.Name()] {
				return initClient(cmd, defaultHTTPClient())
			}
		}
		return nil
	}

	addTableCommands(parent)
	addIndexCommands(parent)
	addQueryCommands(parent)
	addLoadCommands(parent)
	addAgentCommands(parent)
	addInternalCommands(parent)
}

// SearchParams holds the common search/query parameters used by both
// the query command and the retrieval agent command.
type SearchParams struct {
	Table          string
	FullTextSearch query.Query
	Fields         []string
	Limit          int
	Offset         int
	OrderBy        []antfly.SortField
	SearchAfter    []string
	SearchBefore   []string
	SemanticSearch string
	Indexes        []string
	FilterPrefix   []byte
	FilterQuery    query.Query
	ExclusionQuery query.Query
	Aggregations   map[string]antfly.AggregationRequest
	Reranker       *antfly.RerankerConfig
	Pruner         antfly.Pruner
}

// addSearchFlags registers the common search/query flags on a command.
func addSearchFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("table", "t", "", "Name of the table to query")
	cmd.Flags().String("full-text-search", "", `Bleve query string (e.g. 'age:>16 body:"computer skills"')`)
	cmd.Flags().String("full-text-search-json", "", `Bleve JSON query (e.g. '{"must": {"term": "active", "field":"status"}, "should": {"query": "developer", "field":"role"}}')`)
	cmd.Flags().String("fields", "", "Comma-separated list of fields to return (e.g. 'id,name,age')")
	cmd.Flags().Int("limit", 5, "Maximum number of results to return")
	cmd.Flags().String("semantic-search", "", "End user natural language search query")
	cmd.Flags().StringP("indexes", "i", "", "Comma-separated list of indexes to use (e.g., desc_idx,desc_idx_mini)")
	cmd.Flags().String("filter-prefix", "", "Filter results by key prefix (e.g., 'user:' to only return keys starting with 'user:')")
	cmd.Flags().String("filter-query", "", "Bleve query string for filtering results (applied as an AND condition)")
	cmd.Flags().String("exclusion-query", "", "Bleve query string for excluding results (applied as a NOT condition)")
	cmd.Flags().String("reranker", "", `JSON string defining reranker configuration (e.g. '{"provider":"termite","model":"mxbai-rerank-base-v1","field":"body"}')`)
	cmd.Flags().String("pruner", "", `JSON string defining pruner configuration for filtering low-relevance results (e.g. '{"min_score_ratio":0.5}' or '{"max_score_gap_percent":30}')`)
	_ = cmd.MarkFlagRequired("table")
}

// parseSearchParams reads the common search flags from a command and returns
// a populated SearchParams. Callers may set additional fields (Offset, OrderBy,
// Aggregations) before passing the struct to a client method.
func parseSearchParams(cmd *cobra.Command) (SearchParams, error) {
	tableName, _ := cmd.Flags().GetString("table")
	fullTextSearch, _ := cmd.Flags().GetString("full-text-search")
	fullTextSearchJSON, _ := cmd.Flags().GetString("full-text-search-json")
	fieldsStr, _ := cmd.Flags().GetString("fields")
	limit, _ := cmd.Flags().GetInt("limit")
	semanticSearch, _ := cmd.Flags().GetString("semantic-search")
	modelsStr, _ := cmd.Flags().GetString("indexes")
	filterPrefix, _ := cmd.Flags().GetString("filter-prefix")
	filterQueryStr, _ := cmd.Flags().GetString("filter-query")
	exclusionQueryStr, _ := cmd.Flags().GetString("exclusion-query")
	rerankerStr, _ := cmd.Flags().GetString("reranker")
	prunerStr, _ := cmd.Flags().GetString("pruner")

	q, err := parseFullTextSearch(fullTextSearch, fullTextSearchJSON)
	if err != nil {
		return SearchParams{}, err
	}

	reranker, err := parseOptionalReranker(rerankerStr)
	if err != nil {
		return SearchParams{}, err
	}

	pruner, err := parseOptionalPruner(prunerStr)
	if err != nil {
		return SearchParams{}, err
	}

	var fq, eq query.Query
	if filterQueryStr != "" {
		fq = query.NewQueryStringQuery(filterQueryStr)
	}
	if exclusionQueryStr != "" {
		eq = query.NewQueryStringQuery(exclusionQueryStr)
	}

	var fp []byte
	if filterPrefix != "" {
		fp = []byte(filterPrefix)
	}

	return SearchParams{
		Table:          tableName,
		FullTextSearch: q,
		Fields:         splitCSV(fieldsStr),
		Limit:          limit,
		SemanticSearch: semanticSearch,
		Indexes:        splitCSV(modelsStr),
		FilterPrefix:   fp,
		FilterQuery:    fq,
		ExclusionQuery: eq,
		Reranker:       reranker,
		Pruner:         pruner,
	}, nil
}
