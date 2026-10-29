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
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublisherRetriesOnlyReadOperations(t *testing.T) {
	for _, operation := range []string{"query", "get", "merge", "batch", "create"} {
		t.Run(operation, func(t *testing.T) {
			calls := 0
			p := &Publisher{Endpoint: "https://instance.example/db/v1", Token: "private-test-token", Client: &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, "Bearer private-test-token", r.Header.Get("Authorization"))
				if operation == "get" || operation == "query" {
					deadline, ok := r.Context().Deadline()
					require.True(t, ok)
					require.LessOrEqual(t, time.Until(deadline), 30*time.Second)
				}
				if operation == "query" {
					b, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					require.JSONEq(t, `{"query":{"doc_id":{"ids":["one"]}},"limit":1}`, string(b))
				}
				if calls == 1 {
					return nil, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"status":"success"}`)), Request: r}, nil
			})}}
			var err error
			switch operation {
			case "query":
				_, err = p.Query(t.Context(), "docs", []byte(`{"query":{"doc_id":{"ids":["one"]}},"limit":1}`))
			case "get":
				_, err = p.call(t.Context(), "GET", tablePath("docs"), nil)
			case "merge":
				_, err = p.Merge(t.Context(), "docs", []byte(`{"scoped_merge":{}}`))
			case "batch":
				err = p.Batch(t.Context(), "docs", nil, nil)
			case "create":
				_, err = p.call(t.Context(), "POST", tablePath("docs"), map[string]any{"num_shards": 1})
			}
			if operation == "query" || operation == "get" {
				require.NoError(t, err)
				require.Equal(t, 2, calls)
			} else {
				require.ErrorIs(t, err, syscall.ECONNRESET)
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestPublisherReadRetriesAreBoundedAndRespectCancellationAndCooldown(t *testing.T) {
	for _, scenario := range []string{"persistent_reset", "bad_query", "conflict", "retry_after", "canceled", "canceled_response"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := &Publisher{Endpoint: "https://instance.example", Client: &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if scenario == "persistent_reset" {
					return nil, syscall.ECONNRESET
				}
				if scenario == "canceled" {
					cancel()
					return nil, syscall.ECONNRESET
				}
				if scenario == "canceled_response" {
					cancel()
					return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
				}
				status := 400
				header := http.Header{}
				if scenario == "conflict" {
					status = 409
				}
				if scenario == "retry_after" {
					status = 429
					header.Set("Retry-After", "60")
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader("provider-secret")), Request: r}, nil
			})}}
			started := time.Now()
			_, err := p.Query(ctx, "docs", []byte(`{}`))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "provider-secret")
			if scenario == "persistent_reset" {
				require.Equal(t, 3, calls)
			} else {
				require.Equal(t, 1, calls)
			}
			if scenario == "canceled" || scenario == "canceled_response" {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Less(t, time.Since(started), 5*time.Second)
		})
	}
}

type sourceBrokenBody struct{}

func (sourceBrokenBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (sourceBrokenBody) Close() error             { return nil }
func TestPublisherRetriesTruncatedReadResponseAndTransientHTTP(t *testing.T) {
	for _, scenario := range []string{"truncated", "unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			p := &Publisher{Endpoint: "https://instance.example", Client: &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				response := &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"responses":[]}`)), Request: r}
				if calls == 1 {
					if scenario == "truncated" {
						response.Body = sourceBrokenBody{}
					} else {
						response.StatusCode = 503
					}
				}
				return response, nil
			})}}
			raw, err := p.Query(t.Context(), "docs", []byte(`{}`))
			require.NoError(t, err)
			require.JSONEq(t, `{"responses":[]}`, string(raw))
			require.Equal(t, 2, calls)
		})
	}
}

func TestRunnerRecoversLostContentAcknowledgementWithoutReplay(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	backend.dropContentAck = true
	require.Error(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	require.Equal(t, 1, backend.contentMerges)
	require.Len(t, backend.records, 1)
	require.Zero(t, backend.deletes)
	backend.mu.Unlock()
	var state runState
	require.NoError(t, loadJSON(filepath.Join(runner.StateDir, testSourceID+".json"), &state, 256<<20))
	require.NotNil(t, state.Pending)
	ownership := map[string]string{}
	require.NoError(t, replayOwnership(filepath.Join(runner.StateDir, testSourceID+".json.owned"), "job/"+testSourceID, ownership))
	require.Contains(t, ownership, connector.inventory.Objects[0].ID)
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Equal(t, 1, backend.contentMerges, "committed content must be proven by a fresh query, not replayed")
	require.Zero(t, backend.cleanupMerges)
	require.Equal(t, 1, connector.enumerations, "resume must use its pinned inventory")
}

func TestRunnerRecoversLostCleanupAcknowledgementWithoutReplay(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	first := connector.inventory.Objects[0]
	second := first
	second.ID += "-second"
	second.Path = "second.txt"
	connector.inventory.Objects = append(connector.inventory.Objects, second)
	connector.data[second.ID] = []byte("second text")
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	backend.records["foreign"] = map[string]any{"source_id": "another-source"}
	before := backend.contentMerges
	backend.dropCleanupAck = true
	backend.mu.Unlock()
	connector.inventory.Objects = connector.inventory.Objects[:1]
	require.Error(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	require.Equal(t, 1, backend.cleanupMerges)
	require.Equal(t, before, backend.contentMerges)
	require.Equal(t, 1, backend.deletes)
	backend.mu.Unlock()
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Equal(t, 1, backend.cleanupMerges, "already visible deletion must not replay the cleanup merge")
	require.Equal(t, before, backend.contentMerges)
	require.Contains(t, backend.records, "foreign")
	require.Len(t, backend.records, 2)
	require.Equal(t, 2, connector.enumerations)
}

func TestSyncWriterCannotReuseAnAmbiguousMergeOrReconcileItsRange(t *testing.T) {
	runner, connector, _ := runnerFixture(t)
	item := connector.inventory.Objects[0]
	doc, err := Normalize(testSourceID, item, connector.data[item.ID], *runner.Profile.Sources[0].Permissions, time.Now())
	require.NoError(t, err)
	key, err := RecordKey(testSourceID, item.ID)
	require.NoError(t, err)
	calls := 0
	client := &interceptedSyncClient{SyncClient: runner.Publisher, merge: func(context.Context, string, []byte) ([]byte, error) { calls++; return nil, syscall.ECONNRESET }}
	writer, err := NewSyncWriter(client, "sources", testSourceID, []string{item.ID}, true)
	require.NoError(t, err)
	_, err = writer.Upsert(t.Context(), map[string]Document{key: doc})
	require.ErrorIs(t, err, syscall.ECONNRESET)
	_, err = writer.Upsert(t.Context(), map[string]Document{key: doc})
	require.Error(t, err)
	_, err = writer.Finish(t.Context())
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestCanonicalQueriesUseTaggedIDsAndNeverUnboundedAbsence(t *testing.T) {
	doc := readinessDocument(t)
	calls := 0
	query := func(_ context.Context, _ string, body []byte) ([]byte, error) {
		calls++
		var request struct {
			Query struct {
				IDs   []string `json:"ids"`
				DocID struct {
					IDs []string `json:"ids"`
				} `json:"doc_id"`
			} `json:"query"`
			Limit int `json:"limit"`
		}
		require.NoError(t, json.Unmarshal(body, &request))
		require.Nil(t, request.Query.IDs)
		require.Equal(t, []string{"item"}, request.Query.DocID.IDs)
		require.Equal(t, 1, request.Limit)
		if calls == 1 {
			return readinessResponse(doc), nil
		}
		return []byte(`{"responses":[{"status":200,"remote_snapshot":"fresh","hits":{"hits":[]}}]}`), nil
	}
	require.NoError(t, WaitForDocuments(t.Context(), query, "docs", map[string]Document{"item": doc}))
	require.NoError(t, WaitForAbsent(t.Context(), query, "docs", []string{"item"}))
	require.NoError(t, WaitForAbsent(t.Context(), query, "docs", nil))
	require.Equal(t, 2, calls)
	require.Error(t, WaitForAbsent(t.Context(), query, "docs", make([]string, 1001)))
}

func TestSecondUnchanged475RecordSyncPerformsNoMerges(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	prototype := connector.inventory.Objects[0]
	connector.inventory.Objects = nil
	for i := 0; i < 475; i++ {
		item := prototype
		item.ID += fmt.Sprintf("/event-%03d", i)
		item.Path = fmt.Sprintf("calendar/event-%03d.txt", i)
		item.Metadata = map[string]any{"calendar_id": "private-calendar", "event_id": fmt.Sprint(i)}
		connector.inventory.Objects = append(connector.inventory.Objects, item)
		connector.data[item.ID] = []byte(fmt.Sprintf("event %d\n", i))
	}
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	content, cleanup := backend.contentMerges, backend.cleanupMerges
	require.Equal(t, 475, content)
	require.Len(t, backend.records, 475)
	backend.mu.Unlock()
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Equal(t, content, backend.contentMerges)
	require.Equal(t, cleanup, backend.cleanupMerges)
	require.Len(t, backend.records, 475)
}

func TestMixedPageMergesOnlyChangedProjection(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	prototype := connector.inventory.Objects[0]
	ids := []string{}
	documents := map[string]Document{}
	for i := 0; i < 3; i++ {
		item := prototype
		item.ID += fmt.Sprint(i)
		ids = append(ids, item.ID)
		doc, err := Normalize(testSourceID, item, []byte("same text"), *runner.Profile.Sources[0].Permissions, time.Unix(123, 0))
		require.NoError(t, err)
		key, err := RecordKey(testSourceID, item.ID)
		require.NoError(t, err)
		documents[key] = doc
	}
	writer, err := NewSyncWriter(runner.Publisher, "sources", testSourceID, ids, true)
	require.NoError(t, err)
	_, err = writer.Upsert(t.Context(), documents)
	require.NoError(t, err)
	_, err = writer.Finish(t.Context())
	require.NoError(t, err)
	changed, _ := RecordKey(testSourceID, ids[1])
	expected := map[string]Document{}
	for key, doc := range documents {
		doc.IndexedAt = 456
		if key == changed {
			doc.Metadata = cloneSourceMetadata(doc.Metadata)
			doc.Metadata["connection_id"] = "different-connection"
		}
		expected[key] = doc
	}
	calls := 0
	client := &interceptedSyncClient{SyncClient: runner.Publisher, merge: func(ctx context.Context, table string, body []byte) ([]byte, error) {
		calls++
		var page ScopedMergeRequest
		require.NoError(t, json.Unmarshal(body, &page))
		require.False(t, page.ScopedMerge.DeleteMissing)
		require.Len(t, page.ScopedMerge.Records, 1)
		require.Contains(t, page.ScopedMerge.Records, changed)
		return runner.Publisher.Merge(ctx, table, body)
	}}
	writer, err = NewSyncWriter(client, "sources", testSourceID, ids, true)
	require.NoError(t, err)
	result, err := writer.Upsert(t.Context(), expected)
	require.NoError(t, err)
	require.Equal(t, 1, result.Upserted)
	require.Equal(t, 2, result.Skipped)
	_, err = writer.Finish(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	for key, doc := range expected {
		if key != changed {
			require.Equal(t, int64(123), doc.IndexedAt)
		} else {
			require.Equal(t, int64(456), doc.IndexedAt)
		}
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Len(t, backend.records, 3)
	require.Zero(t, backend.deletes)
}

func TestUnchangedVerificationRepairsOuterOwnershipAndRejectsForgedDigest(t *testing.T) {
	for _, field := range []string{"id", "source_id", "doc_type", "forged_digest", "invalid_utf8"} {
		t.Run(field, func(t *testing.T) {
			runner, connector, backend := runnerFixture(t)
			require.NoError(t, runner.Sync(t.Context()))
			item := connector.inventory.Objects[0]
			key, err := RecordKey(testSourceID, item.ID)
			require.NoError(t, err)
			doc, err := Normalize(testSourceID, item, connector.data[item.ID], *runner.Profile.Sources[0].Permissions, time.Now())
			require.NoError(t, err)
			doc.Metadata["connection_id"] = runner.Profile.Sources[0].Connection
			badContent := field == "forged_digest" || field == "invalid_utf8"
			backend.mu.Lock()
			before := backend.contentMerges
			if !badContent {
				delete(backend.records[key], field)
			}
			backend.mu.Unlock()
			writer, err := NewSyncWriter(runner.Publisher, "sources", testSourceID, []string{item.ID}, true)
			require.NoError(t, err)
			if field == "forged_digest" {
				doc.Content = "changed bytes with old checksum"
			}
			if field == "invalid_utf8" {
				doc.Content = string([]byte{0xff})
				doc.Metadata["sha256"] = fmt.Sprintf("%x", sha256.Sum256([]byte(doc.Content)))
			}
			_, err = writer.Upsert(t.Context(), map[string]Document{key: doc})
			if badContent {
				require.Error(t, err)
				_, err = writer.Finish(t.Context())
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			backend.mu.Lock()
			defer backend.mu.Unlock()
			if badContent {
				require.Equal(t, before, backend.contentMerges)
			} else {
				require.Equal(t, before+1, backend.contentMerges)
				require.Equal(t, key, backend.records[key]["id"])
				require.Equal(t, testSourceID, backend.records[key]["source_id"])
				require.Equal(t, "source_item", backend.records[key]["doc_type"])
			}
		})
	}
}

func TestNoopCleanupStillRequiresEveryRetainedRecord(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	item := connector.inventory.Objects[0]
	key, err := RecordKey(testSourceID, item.ID)
	require.NoError(t, err)
	doc, err := Normalize(testSourceID, item, connector.data[item.ID], *runner.Profile.Sources[0].Permissions, time.Now())
	require.NoError(t, err)
	writer, err := NewSyncWriter(runner.Publisher, "sources", testSourceID, []string{item.ID}, true)
	require.NoError(t, err)
	_, err = writer.Upsert(t.Context(), map[string]Document{key: doc})
	require.NoError(t, err)
	backend.mu.Lock()
	delete(backend.records, key)
	backend.mu.Unlock()
	_, err = writer.Finish(t.Context())
	require.ErrorContains(t, err, "no longer query-visible")
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Zero(t, backend.cleanupMerges)
}

func TestReadinessRejectsForeignAndDuplicateSnapshotHits(t *testing.T) {
	doc := readinessDocument(t)
	for _, kind := range []string{"foreign", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			id := "item"
			if kind == "foreign" {
				id = "another-source"
			}
			hits := []any{map[string]any{"_id": "item", "_source": map[string]any{"grep": doc}}, map[string]any{"_id": id, "_source": map[string]any{"grep": doc}}}
			response, _ := json.Marshal(map[string]any{"responses": []any{map[string]any{"status": 200, "remote_snapshot": "fresh", "hits": map[string]any{"hits": hits}}}})
			err := WaitForDocuments(t.Context(), func(context.Context, string, []byte) ([]byte, error) { return response, nil }, "docs", map[string]Document{"item": doc})
			require.ErrorContains(t, err, "unexpected canonical readiness identity")
		})
	}
}

type failingSourceQuery struct {
	SyncClient
	fail bool
}

func (c *failingSourceQuery) Query(ctx context.Context, table string, body []byte) ([]byte, error) {
	if c.fail {
		return nil, syscall.ECONNRESET
	}
	return c.SyncClient.Query(ctx, table, body)
}
func TestNewUpsertInvalidatesAnEarlierVerificationBeforeQueryFailure(t *testing.T) {
	runner, connector, _ := runnerFixture(t)
	item := connector.inventory.Objects[0]
	key, err := RecordKey(testSourceID, item.ID)
	require.NoError(t, err)
	doc, err := Normalize(testSourceID, item, connector.data[item.ID], *runner.Profile.Sources[0].Permissions, time.Now())
	require.NoError(t, err)
	client := &failingSourceQuery{SyncClient: runner.Publisher}
	writer, err := NewSyncWriter(client, "sources", testSourceID, []string{item.ID}, true)
	require.NoError(t, err)
	_, err = writer.Upsert(t.Context(), map[string]Document{key: doc})
	require.NoError(t, err)
	doc.Permissions = Permissions{Readers: []string{"user:updated"}}
	client.fail = true
	_, err = writer.Upsert(t.Context(), map[string]Document{key: doc})
	require.ErrorIs(t, err, syscall.ECONNRESET)
	client.fail = false
	_, err = writer.Finish(t.Context())
	require.ErrorContains(t, err, "not completely verified")
}
