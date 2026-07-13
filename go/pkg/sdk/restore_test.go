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
