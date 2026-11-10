// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
)

//go:embed explorer.html
var explorerHTML string

//go:embed explorer.js
var explorerJS string

func exploreCmd(args []string) error {
	f := flag.NewFlagSet("explore", flag.ContinueOnError)
	endpoint := f.String("url", DefaultAntflyURL, "Antfly API")
	table := f.String("table", "grepstein_corpus", "Corpus table")
	listen := f.String("listen", ":"+envDefault("PORT", "3000"), "Listen address")
	if err := f.Parse(args); err != nil {
		return err
	}
	// The hosted process uses a table-scoped read key. It never exposes that key
	// or accepts arbitrary API paths, query bodies, or mutation operations.
	handler := explorerHandler(strings.TrimRight(*endpoint, "/")+"/tables/"+url.PathEscape(*table), &http.Client{Timeout: 45 * time.Second})
	fmt.Printf("Grepstein explorer listening on %s\n", *listen)
	return (&http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}).ListenAndServe()
}

func envDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func explorerHandler(tableURL string, client *http.Client) http.Handler {
	return explorerHandlerWithStorage(tableURL, client, os.Getenv("GREPSTEIN_BUCKET"), gcsReadClient)
}
func explorerHandlerWithStorage(tableURL string, client *http.Client, bucket string, storageClient func(context.Context) (*http.Client, error)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /explorer.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		fmt.Fprint(w, explorerJS)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; frame-src 'self'")
		fmt.Fprint(w, explorerHTML)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })
	mux.HandleFunc("GET /api/search", func(w http.ResponseWriter, r *http.Request) {
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		mode := r.URL.Query().Get("mode")
		if q == "" || len(q) > 2048 {
			http.Error(w, "Enter a query of at most 2048 bytes", 400)
			return
		}
		payload, err := explorerQuery(q, mode)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		raw, err := corpusRequestWithContext(r.Context(), client, "POST", tableURL+"/query", payload)
		if err != nil {
			http.Error(w, "Search unavailable; check index coverage and server logs", 502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		raw, err := corpusRequestWithContext(r.Context(), client, "GET", tableURL, nil)
		if err != nil {
			http.Error(w, "Status unavailable", 502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)
	})
	mux.HandleFunc("GET /pdf", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" || len(id) > 1024 {
			http.Error(w, "Invalid document", 400)
			return
		}
		raw, err := corpusRequestWithContext(r.Context(), client, "GET", tableURL+"/documents/"+url.PathEscape(id), nil)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		var doc map[string]any
		if json.Unmarshal(raw, &doc) != nil {
			http.Error(w, "Invalid source", 502)
			return
		}
		if nested, ok := doc["_source"].(map[string]any); ok {
			doc = nested
		}
		original, _ := doc["original_url"].(string)
		u, err := url.Parse(original)
		if err != nil || !validOriginalLocator(u, bucket) {
			http.Error(w, "Source unavailable", 404)
			return
		}
		storage, err := storageClient(r.Context())
		if err != nil {
			http.Error(w, "Storage unavailable", 502)
			return
		}
		storage.Timeout = time.Minute
		query := url.Values{"alt": {"media"}}
		if u.Scheme == "gs" {
			query.Set("generation", u.Query().Get("generation"))
		}
		req, err := http.NewRequestWithContext(r.Context(), "GET", "https://storage.googleapis.com/storage/v1/b/"+url.PathEscape(bucket)+"/o/"+url.PathEscape(strings.TrimPrefix(u.Path, "/"))+"?"+query.Encode(), nil)
		if err != nil {
			http.Error(w, "Source unavailable", 502)
			return
		}
		if value := r.Header.Get("Range"); value != "" {
			req.Header.Set("Range", value)
		}
		response, err := storage.Do(req)
		if err != nil {
			http.Error(w, "Source unavailable", 502)
			return
		}
		defer response.Body.Close()
		if response.StatusCode != 200 && response.StatusCode != 206 {
			http.Error(w, "Source unavailable", 502)
			return
		}
		for _, key := range []string{"Content-Length", "Content-Range", "Accept-Ranges", "ETag"} {
			if value := response.Header.Get(key); value != "" {
				w.Header().Set(key, value)
			}
		}
		w.Header().Set("Content-Type", "application/pdf")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "private, max-age=300")
		w.WriteHeader(response.StatusCode)
		io.Copy(w, response.Body)
	})
	return boundExplorerRequests(mux, 4)
}

// Bound work at the exposed HTTP boundary, including direct API and PDF calls.
// A full queue is explicit backpressure, never an empty successful result.
func boundExplorerRequests(next http.Handler, limit int) http.Handler {
	active := make(chan struct{}, limit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") && r.URL.Path != "/pdf" {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case active <- struct{}{}:
			defer func() { <-active }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Explorer is busy; retry shortly", http.StatusTooManyRequests)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func explorerQuery(q, mode string) (map[string]any, error) {
	payload := map[string]any{"limit": 20, "fields": []string{"title", "original_url", "metadata"}}
	switch mode {
	case "", "text":
		payload["full_text_index"] = corpusTextIndex
		payload["full_text_search"] = map[string]any{"match": q, "field": "text"}
	case "semantic":
		payload["semantic_search"] = q
		payload["indexes"] = []string{DefaultEmbeddingIndex}
	case "graph":
		payload["full_text_index"] = DefaultFullTextIndex
		payload["full_text_search"] = map[string]any{"match": q, "field": "content"}
		payload["limit"] = 8
		payload["graph_queries"] = map[string]any{"relations": map[string]any{"index": DefaultAutographIndex, "traverse": map[string]any{"start": map[string]any{"result_ref": "$query_results", "limit": 8}, "direction": "both", "max_depth": 1, "limit": 80, "include_paths": true, "include_documents": true, "fields": []string{"title", "original_url", "metadata"}}}}
		return payload, nil
	default:
		return nil, fmt.Errorf("Unknown search mode")
	}
	payload["hierarchy"] = map[string]any{"group_by": map[string]any{"level": "source", "matches": map[string]any{"limit": 3, "fields": []string{"text"}}}, "ancestors": map[string]any{"unit": map[string]any{"fields": []string{"provenance.page_number"}}}}
	return payload, nil
}

// Keep Google's read client separate from the Antfly client: an API key must
// never be forwarded to storage, even when a result contains an external URL.
func gcsReadClient(ctx context.Context) (*http.Client, error) {
	return google.DefaultClient(ctx, "https://www.googleapis.com/auth/devstorage.read_only")
}

// Only immutable original PDFs in the configured private bucket can be served.
// Page fragments are citations and never become part of the storage request.
func validOriginalLocator(u *url.URL, bucket string) bool {
	if u == nil || bucket == "" || u.Host != bucket || u.User != nil || !regexp.MustCompile(`^/objects/sha256/[a-f0-9]{64}\.pdf$`).MatchString(u.Path) {
		return false
	}
	if u.Scheme == "s3" {
		return u.RawQuery == "" && !u.ForceQuery
	}
	if u.Scheme != "gs" {
		return false
	}
	q, err := url.ParseQuery(u.RawQuery)
	return err == nil && len(q) == 1 && len(q["generation"]) == 1 && validGCSGeneration(q.Get("generation")) && u.RawQuery == "generation="+q.Get("generation")
}
func validGCSGeneration(value string) bool {
	if value == "" || value[0] == '0' {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}
