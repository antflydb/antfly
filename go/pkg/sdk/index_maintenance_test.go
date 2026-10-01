// Copyright 2026 The Antfly Contributors
// SPDX-License-Identifier: Apache-2.0

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func maintenanceFixture() IndexMaintenanceRequest {
	proof := IndexMaintenanceOwnerProof{GroupId: "9007199254740993", Generation: "9007199254740995", MaintenanceEpoch: "0", Owner: strings.Repeat("a", 64), Comparison: strings.Repeat("b", 64), ProgressDigest: strings.Repeat("c", 64)}
	second := proof
	second.GroupId = "9007199254740994"
	return IndexMaintenanceRequest{TableId: "9007199254740999", SchemaVersion: 7, Owners: []IndexMaintenanceOwnerProof{proof, second}}
}

func TestIndexMaintenancePreservesProofsEscapedPathsAndCancellation(t *testing.T) {
	request := maintenanceFixture()
	request.SchemaVersion = 0 // Initial immutable schema versions are valid.
	original, _ := json.Marshal(request)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		action := "retry"
		if calls == 2 {
			action = "repair"
		}
		if r.Method != http.MethodPost || r.URL.EscapedPath() != "/db/v1/tables/wiki%2Fmedia/indexes/by%20id%2F%23/"+action {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
		}
		var received IndexMaintenanceRequest
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(request, received) {
			t.Errorf("changed proofs: %#v", received)
		}
		_, _ = io.WriteString(w, `{"acknowledged_groups":["9007199254740994","9007199254740993"]}`)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RetryIndex(context.Background(), "wiki/media", "by id/#", request); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RepairIndex(context.Background(), "wiki/media", "by id/#", request); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.RetryIndex(canceled, "wiki/media", "by id/#", request); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("unexpected retry/refresh: %d requests", calls)
	}
	current, _ := json.Marshal(request)
	if string(current) != string(original) {
		t.Fatal("caller request mutated")
	}
}

func TestIndexMaintenanceRejectsPartialMalformedAndForeignAcknowledgements(t *testing.T) {
	for _, body := range []string{
		`{}`, `{"acknowledged_groups":["9007199254740993"]}`,
		`{"acknowledged_groups":["9007199254740993","9007199254740993"]}`,
		`{"acknowledged_groups":["9007199254740993","12"]}`,
		`{"acknowledged_groups":[9007199254740993,"9007199254740994"]}`,
		`{"acknowledged_groups":null}`, `not json`, strings.Repeat(" ", 33<<10),
	} {
		t.Run(body[:min(len(body), 80)], func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; _, _ = io.WriteString(w, body) }))
			defer server.Close()
			client, err := NewAntflyClient(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.RetryIndex(context.Background(), "rows", "by_id", maintenanceFixture()); err == nil {
				t.Fatal("accepted invalid acknowledgement")
			}
			if calls != 1 {
				t.Fatalf("unexpected retry: %d calls", calls)
			}
		})
	}
}

func TestIndexMaintenanceBoundsAndCanonicalProofs(t *testing.T) {
	for _, mutate := range []func(*IndexMaintenanceRequest){
		func(r *IndexMaintenanceRequest) { r.Owners = nil },
		func(r *IndexMaintenanceRequest) { r.Owners = make([]IndexMaintenanceOwnerProof, 129) },
		func(r *IndexMaintenanceRequest) { r.Owners[1].GroupId = r.Owners[0].GroupId },
		func(r *IndexMaintenanceRequest) { r.TableId = "01" },
		func(r *IndexMaintenanceRequest) { r.Owners[0].MaintenanceEpoch = "18446744073709551616" },
		func(r *IndexMaintenanceRequest) { r.Owners[0].ProgressDigest = strings.Repeat("A", 64) },
	} {
		request := maintenanceFixture()
		mutate(&request)
		if _, err := validateIndexMaintenanceRequest(request); err == nil {
			t.Fatal("accepted invalid proof")
		}
	}
}
