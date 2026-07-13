/*
Copyright 2026 The Antfly Contributors

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

package sdk

import (
	"context"
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestClusterRestoreCarriesConnectionAndExposesDurableJobLifecycle(t *testing.T) {
	const jobID = "9223372036854775807"
	const job = `{"job_id":"` + jobID + `","attempt_id":0,"scope":"cluster","backup_id":"daily","phase":"queued","cancel_requested":false,"published_table_count":0,"completed_table_count":0,"created_at_ms":1,"updated_at_ms":1}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/db/v1/restore":
			if got := r.Header.Get("Idempotency-Key"); got != "restore-daily" {
				t.Errorf("Idempotency-Key = %q, want restore-daily", got)
			}
			var body oapi.ClusterRestoreRequest
			if err := stdjson.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode restore body: %v", err)
			} else if body.Connection != "archive-reader" {
				t.Errorf("connection = %q, want archive-reader", body.Connection)
			}
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodGet && r.URL.Path == "/db/v1/restore/jobs/"+jobID:
		case r.Method == http.MethodDelete && r.URL.Path == "/db/v1/restore/jobs/"+jobID:
		default:
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(job))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	started, err := client.ClusterRestore(context.Background(), "daily", "s3://archive/daily", "archive-reader", nil, "fail_if_exists", "restore-daily")
	if err != nil {
		t.Fatalf("ClusterRestore: %v", err)
	}
	if started.JobId != jobID {
		t.Fatalf("started job ID = %q, want %q", started.JobId, jobID)
	}
	if _, err := client.GetRestoreJob(context.Background(), jobID); err != nil {
		t.Fatalf("GetRestoreJob: %v", err)
	}
	if _, err := client.CancelRestoreJob(context.Background(), jobID); err != nil {
		t.Fatalf("CancelRestoreJob: %v", err)
	}
}

func TestListRestoreJobsFollowsEmptyFilteredPages(t *testing.T) {
	const job = `{"job_id":"42","attempt_id":1,"scope":"table","table_name":"docs","backup_id":"daily","phase":"running","cancel_requested":false,"published_table_count":0,"completed_table_count":0,"created_at_ms":1,"updated_at_ms":2}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet || r.URL.Path != "/db/v1/restore/jobs" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("phase") != "running" || r.URL.Query().Get("scope") != "table" {
			t.Errorf("unexpected restore filters: %s", r.URL.RawQuery)
		}
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"jobs":[],"next_cursor":"100"}`))
			return
		}
		_, _ = w.Write([]byte(`{"jobs":[` + job + `]}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	jobs, err := client.ListRestoreJobs(context.Background(), RestoreJobListOptions{Phase: RestoreJobPhaseRunning, Scope: RestoreJobScopeTable})
	if err != nil {
		t.Fatalf("ListRestoreJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].JobId != "42" {
		t.Fatalf("jobs = %#v, want job 42", jobs)
	}
}

func TestListBackupsPreservesAllResultsAcrossPages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet || r.URL.Path != "/db/v1/backups" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"backups":[{"backup_id":"a","timestamp":"2026-01-01T00:00:00Z","antfly_version":"1","tables":["docs"]}],"next_cursor":"page-2"}`))
			return
		}
		_, _ = w.Write([]byte(`{"backups":[{"backup_id":"b","timestamp":"2026-01-02T00:00:00Z","antfly_version":"1","tables":["docs"]}]}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	backups, err := client.ListBackups(context.Background(), "file:///backups")
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(backups) != 2 || backups[0].BackupID != "a" || backups[1].BackupID != "b" {
		t.Fatalf("backups = %#v, want a and b", backups)
	}
}
