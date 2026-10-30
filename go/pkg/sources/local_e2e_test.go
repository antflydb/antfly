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

package sources

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Opt in with a dedicated loopback Antfly, never a shared or Cloud instance.
// The destination, CLI, query snapshots and merges are real; only upstream S3
// and acknowledgement loss are local fixtures. Run with -timeout 20m.
func TestLocalAntflySourcePipeline(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCES_E2E_URL")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCES_E2E_URL to a dedicated local Antfly /db/v1")
	}
	target, err := url.Parse(endpoint)
	require.NoError(t, err)
	ip := net.ParseIP(target.Hostname())
	require.True(t, target.Scheme == "http" && ip != nil && ip.IsLoopback(), "native E2E refuses non-loopback endpoints")
	require.Equal(t, "/db/v1", strings.TrimRight(target.Path, "/"))
	count := 475
	if value := os.Getenv("ANTFLY_SOURCES_E2E_RECORDS"); value != "" {
		count, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	require.True(t, count >= 4 && count <= 1000)
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	root := t.TempDir()
	sourceCLI := os.Getenv("ANTFLY_SOURCE_TEST_BINARY")
	if sourceCLI == "" {
		sourceCLI = filepath.Join(root, "antfly-source")
		output, err := exec.CommandContext(ctx, "go", "build", "-o", sourceCLI, "./cmd/antfly-source").CombinedOutput()
		require.NoError(t, err, "build source CLI: %s", output)
	}
	upstream := &localSourceObjects{objects: map[string]string{}}
	for i := 0; i < count; i++ {
		upstream.objects[fmt.Sprintf("docs/event-%04d.txt", i)] = fmt.Sprintf("native_e2e_budget event %04d\n", i)
	}
	upstream.objects["other/guard.txt"] = "other_source_guard\n"
	provider := httptest.NewServer(upstream)
	defer provider.Close()
	proxy := &localAntflyFaultProxy{target: target, client: &http.Client{Timeout: time.Minute}, queryDrops: 1}
	gateway := httptest.NewServer(proxy)
	defer gateway.Close()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		proxy.mu.Lock()
		defer proxy.mu.Unlock()
		for _, message := range proxy.failures {
			t.Log(message)
		}
	})
	providerURL, _ := url.Parse(provider.URL)
	sourceID, otherID := localSourceUUID(t), localSourceUUID(t)
	suffix := strings.ReplaceAll(sourceID[:13], "-", "")
	table, alias := "source_e2e_"+suffix, "e2e_"+suffix
	profile := Profile{Version: 1, Endpoint: gateway.URL + "/db/v1", DataPlaneTokenEnv: "SOURCE_E2E_DATABASE_TOKEN", Principals: []string{"user:e2e"}, Connections: map[string]Connection{"local": {Provider: ProviderS3, S3Endpoint: providerURL.Host, S3Insecure: true, AccessKeyEnv: "SOURCE_E2E_ACCESS", SecretKeyEnv: "SOURCE_E2E_SECRET"}}, Sources: []Source{
		{ID: sourceID, Alias: alias, Connection: "local", Table: table, Config: json.RawMessage(`{"bucket":"docs","prefix":"docs/"}`), Permissions: &Permissions{Public: true}},
		{ID: otherID, Alias: alias + "_other", Connection: "local", Table: table, Config: json.RawMessage(`{"bucket":"docs","prefix":"other/"}`), Permissions: &Permissions{Public: true}},
	}}
	config, catalog := filepath.Join(root, "profile.json"), filepath.Join(root, "catalog.json")
	require.NoError(t, writeJSON(config, profile, 1<<20))
	cliEnv := append(os.Environ(), "SOURCE_E2E_DATABASE_TOKEN=local-e2e-token", "SOURCE_E2E_ACCESS=local-access", "SOURCE_E2E_SECRET=local-secret", "ANTFLY_GREP_TABLE_TOKEN=local-e2e-token", "ANTFLY_GREP_CONFIG_PATH=", "RIPGREP_CONFIG_PATH=")
	run := func(expectSuccess bool) {
		t.Helper()
		command := exec.CommandContext(ctx, sourceCLI, "sync", "--config", config, "--state-dir", filepath.Join(root, "state"), "--catalog-out", catalog)
		command.Env = cliEnv
		output, err := command.CombinedOutput()
		if expectSuccess {
			require.NoError(t, err, "source CLI: %s", output)
		} else {
			require.Error(t, err, "fault injection unexpectedly succeeded: %s", output)
		}
	}
	publisher := &Publisher{Endpoint: profile.Endpoint, Token: "local-e2e-token", Client: gateway.Client()}
	t.Logf("real native source pipeline: %d records, table %s", count, table)
	run(true)
	initial := proxy.counts()
	require.Equal(t, count+1, initial.content)
	require.Zero(t, initial.cleanup)
	require.Equal(t, 1, initial.queryDrops)
	raw, err := os.ReadFile(catalog)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "local-secret")
	var published Catalog
	require.NoError(t, json.Unmarshal(raw, &published))
	require.Len(t, published.Sources, 2)
	for _, binding := range published.Sources {
		require.Equal(t, "searchable", binding.Status)
	}
	// Foreign application rows and another source share the destination table.
	require.NoError(t, publisher.Batch(ctx, table, map[string]any{"foreign-application": map[string]any{"content": "application_guard"}}, nil))
	run(true)
	unchanged := proxy.counts()
	require.Equal(t, initial.content, unchanged.content)
	require.Equal(t, initial.cleanup, unchanged.cleanup)
	t.Log("unchanged synchronization issued no content or cleanup merges")
	if grep := os.Getenv("ANTFLY_GREP_TEST_BINARY"); grep != "" {
		for _, selector := range [][]string{{"--source-catalog", catalog}, {"--source-endpoint", profile.Endpoint}} {
			args := append(append([]string{}, selector...), "--source", alias, "native_e2e_budget")
			command := exec.CommandContext(ctx, grep, args...)
			command.Env = cliEnv
			output, err := command.CombinedOutput()
			require.NoError(t, err, "native grep: %s", output)
			require.Equal(t, count, bytes.Count(output, []byte("native_e2e_budget")), "grep must return every source record across pages: %s", output)
			require.NotContains(t, string(output), "other_source_guard")
		}
		t.Log("native antfly-grep searched both file and ordinary-table catalogs")
	} else {
		t.Log("ANTFLY_GREP_TEST_BINARY unset: native grep executable not exercised")
	}
	upstream.mu.Lock()
	upstream.objects["docs/event-0000.txt"] = "native_e2e_budget revised_content\n"
	upstream.mu.Unlock()
	proxy.arm(sourceID, false)
	run(false)
	failedContent := proxy.counts()
	require.Equal(t, unchanged.content+1, failedContent.content)
	require.Equal(t, 1, failedContent.contentDrops)
	run(true)
	recovered := proxy.counts()
	require.Equal(t, failedContent.content, recovered.content)
	require.Equal(t, failedContent.cleanup, recovered.cleanup)
	t.Log("committed content with lost acknowledgement recovered without a replay")
	removedKey, err := RecordKey(sourceID, localPublishedItem(t, root, sourceID, fmt.Sprintf("docs/event-%04d.txt", count-1)).ID)
	require.NoError(t, err)
	upstream.mu.Lock()
	delete(upstream.objects, fmt.Sprintf("docs/event-%04d.txt", count-1))
	upstream.mu.Unlock()
	proxy.arm(sourceID, true)
	run(false)
	failedCleanup := proxy.counts()
	require.Equal(t, 1, failedCleanup.cleanupDrops)
	run(true)
	cleaned := proxy.counts()
	require.Equal(t, failedCleanup.content, cleaned.content)
	require.Equal(t, failedCleanup.cleanup, cleaned.cleanup)
	_, err = publisher.call(ctx, "GET", tablePath(table)+"/documents/"+url.PathEscape(removedKey), nil)
	var absent *PublishHTTPError
	require.ErrorAs(t, err, &absent)
	require.Equal(t, http.StatusNotFound, absent.Status)
	t.Log("committed cleanup with lost acknowledgement recovered without a replay")
	// The source reader must not reconcile anything when upstream access fails.
	upstream.mu.Lock()
	upstream.denied = "docs/event-0000.txt"
	upstream.mu.Unlock()
	run(false)
	denied := proxy.counts()
	require.Equal(t, cleaned.content, denied.content)
	require.Equal(t, cleaned.cleanup, denied.cleanup)
	upstream.mu.Lock()
	upstream.denied = ""
	upstream.mu.Unlock()
	run(true)
	// Verify mixed pages against the real destination as well as the one-row CLI.
	docs := map[string]Document{}
	providers := []string{}
	keys := []string{}
	for i := 0; i < 3; i++ {
		providerID := localPublishedItem(t, root, sourceID, fmt.Sprintf("docs/event-%04d.txt", i)).ID
		key, err := RecordKey(sourceID, providerID)
		require.NoError(t, err)
		keys = append(keys, key)
		providers = append(providers, providerID)
	}
	query, _ := json.Marshal(map[string]any{"query": documentIDsQuery(keys), "fields": append(sourceRowFields(), "grep.content"), "limit": len(keys), "order_by": []any{map[string]any{"field": "_id", "desc": false}}})
	response, err := publisher.Query(ctx, table, query)
	require.NoError(t, err)
	hits, err := decodeSyncHits(response)
	require.NoError(t, err)
	require.Len(t, hits, 3)
	for _, hit := range hits {
		doc := hit.Source.Grep
		doc.IndexedAt += 100
		docs[hit.ID] = doc
	}
	require.Equal(t, "native_e2e_budget revised_content\n", docs[keys[0]].Content)
	changed := docs[keys[1]]
	changed.Metadata = cloneSourceMetadata(changed.Metadata)
	changed.Metadata["e2e_mixed_page"] = "changed"
	docs[keys[1]] = changed
	beforeMixed := proxy.counts()
	writer, err := NewSyncWriter(publisher, table, sourceID, providers, false)
	require.NoError(t, err)
	result, err := writer.Upsert(ctx, docs)
	require.NoError(t, err)
	require.Equal(t, 1, result.Upserted)
	require.Equal(t, 2, result.Skipped)
	_, err = writer.Finish(ctx)
	require.NoError(t, err)
	afterMixed := proxy.counts()
	require.Equal(t, beforeMixed.content+1, afterMixed.content)
	require.Equal(t, beforeMixed.cleanup, afterMixed.cleanup)
	otherKey, err := RecordKey(otherID, localPublishedItem(t, root, otherID, "other/guard.txt").ID)
	require.NoError(t, err)
	for _, key := range []string{otherKey, "foreign-application"} {
		_, err := publisher.call(ctx, "GET", tablePath(table)+"/documents/"+url.PathEscape(key), nil)
		require.NoError(t, err, "foreign row must survive source cleanup")
	}
	t.Log("mixed page changed one record; foreign source and application records survived")
}

func localSourceUUID(t *testing.T) string {
	t.Helper()
	var value [16]byte
	_, err := rand.Read(value[:])
	require.NoError(t, err)
	value[6] = value[6]&15 | 64
	value[8] = value[8]&63 | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:])
}

type localSourceObjects struct {
	mu      sync.Mutex
	objects map[string]string
	denied  string
}

func (s *localSourceObjects) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	if r.URL.Query().Has("location") {
		fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
		return
	}
	if r.URL.Query().Get("list-type") == "2" {
		keys := []string{}
		for key := range s.objects {
			if strings.HasPrefix(key, r.URL.Query().Get("prefix")) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>docs</Name><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`, len(keys))
		for _, key := range keys {
			var escaped bytes.Buffer
			xml.EscapeText(&escaped, []byte(key))
			text := s.objects[key]
			fmt.Fprintf(w, `<Contents><Key>%s</Key><LastModified>2026-10-23T00:00:00Z</LastModified><ETag>"%x"</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`, escaped.String(), md5.Sum([]byte(text)), len(text))
		}
		fmt.Fprint(w, `</ListBucketResult>`)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/docs/")
	text, ok := s.objects[key]
	if !ok {
		http.Error(w, `<Error><Code>NoSuchKey</Code></Error>`, 404)
		return
	}
	if key == s.denied {
		http.Error(w, `<Error><Code>AccessDenied</Code></Error>`, 403)
		return
	}
	etag := fmt.Sprintf("%x", md5.Sum([]byte(text)))
	if match := strings.Trim(r.Header.Get("If-Match"), `"`); match != "" && match != etag {
		w.WriteHeader(412)
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("Last-Modified", "Fri, 23 Oct 2026 00:00:00 GMT")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(len(text)))
	if r.Method != "HEAD" {
		fmt.Fprint(w, text)
	}
}

type localMergeCounts struct{ content, cleanup, queryDrops, contentDrops, cleanupDrops int }
type localAntflyFaultProxy struct {
	mu          sync.Mutex
	target      *url.URL
	client      *http.Client
	queryDrops  int
	dropSource  string
	dropCleanup bool
	stats       localMergeCounts
	failures    []string
}

func (p *localAntflyFaultProxy) counts() localMergeCounts {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}
func (p *localAntflyFaultProxy) arm(sourceID string, cleanup bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropSource = sourceID
	p.dropCleanup = cleanup
}
func (p *localAntflyFaultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, "request read failed", 502)
		return
	}
	p.mu.Lock()
	if strings.HasSuffix(r.URL.Path, "/query") && p.queryDrops > 0 {
		p.queryDrops--
		p.stats.queryDrops++
		p.mu.Unlock()
		localDropResponse(w)
		return
	}
	drop := false
	cleanup := false
	if strings.HasSuffix(r.URL.Path, "/merge") {
		var request ScopedMergeRequest
		if json.Unmarshal(body, &request) == nil {
			cleanup = request.ScopedMerge.DeleteMissing
			if cleanup {
				p.stats.cleanup++
			} else {
				p.stats.content++
			}
			drop = p.dropSource != "" && request.ScopedMerge.KeyRange == recordRange(p.dropSource) && cleanup == p.dropCleanup
		}
	}
	p.mu.Unlock()
	forward := r.Clone(r.Context())
	forward.URL.Scheme = p.target.Scheme
	forward.URL.Host = p.target.Host
	forward.Host = p.target.Host
	forward.RequestURI = ""
	forward.Body = io.NopCloser(bytes.NewReader(body))
	forward.ContentLength = int64(len(body))
	response, err := p.client.Do(forward)
	if err != nil {
		http.Error(w, "local destination unavailable", 502)
		return
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		http.Error(w, "destination response read failed", 502)
		return
	}
	p.mu.Lock()
	if response.StatusCode >= 400 {
		p.failures = append(p.failures, fmt.Sprintf("%s %s HTTP %d: %.2048s", r.Method, r.URL.Path, response.StatusCode, data))
	}
	if drop && response.StatusCode >= 200 && response.StatusCode < 300 {
		p.dropSource = ""
		if cleanup {
			p.stats.cleanupDrops++
		} else {
			p.stats.contentDrops++
		}
		p.mu.Unlock()
		localDropResponse(w)
		return
	}
	p.mu.Unlock()
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	w.Write(data)
}
func localDropResponse(w http.ResponseWriter) {
	connection, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		connection.Close()
	}
}

func localPublishedItem(t *testing.T, root, sourceID, path string) Item {
	t.Helper()
	var state runState
	require.NoError(t, loadJSON(filepath.Join(root, "state", sourceID+".json"), &state, 256<<20))
	require.NotNil(t, state.Published)
	for _, item := range state.Published.Objects {
		if item.Path == path {
			return item
		}
	}
	t.Fatalf("published source has no item %s", path)
	return Item{}
}
