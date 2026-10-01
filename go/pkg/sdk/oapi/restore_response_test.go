package oapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParseRestoreTableAcceptedResponse(t *testing.T) {
	const body = `{"job_id":"9223372036854775807","attempt_id":0,"scope":"table","table_name":"products","backup_id":"nightly","phase":"queued","cancel_requested":false,"published_table_count":0,"completed_table_count":0,"total_table_count":1,"created_at_ms":1,"updated_at_ms":1}`
	response, err := ParseRestoreTableResponse(&http.Response{
		StatusCode: http.StatusAccepted,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	})
	if err != nil {
		t.Fatalf("parse restore response: %v", err)
	}
	if response.JSON202 == nil {
		t.Fatal("accepted restore job was not decoded")
	}
	if response.JSON202.JobId != "9223372036854775807" {
		t.Fatalf("job ID = %q, want opaque int64-width identifier", response.JSON202.JobId)
	}
	if response.JSON202.Scope != RestoreJobScopeTable {
		t.Fatalf("scope = %q, want table", response.JSON202.Scope)
	}
	if response.JSON202.Phase != RestoreJobPhaseQueued {
		t.Fatalf("phase = %q, want queued", response.JSON202.Phase)
	}
	if response.JSON202.TableName != "products" {
		t.Fatalf("table name = %q, want products", response.JSON202.TableName)
	}
}

func TestParseSchemaRewriteAcceptedResponse(t *testing.T) {
	const body = `{"job_id":"9223372036854775807","attempt_id":1,"scope":"table","table_name":"products","backup_id":"","phase":"queued","cancel_requested":false,"published_table_count":0,"completed_table_count":0,"total_table_count":2,"created_at_ms":1,"updated_at_ms":1}`
	response := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Header: http.Header{
				"Content-Type": []string{"application/json"},
				"Location":     []string{"/db/v1/restore/jobs/9223372036854775807"},
			},
			Body: io.NopCloser(strings.NewReader(body)),
		}
	}
	patched, err := ParsePatchSchemaResponse(response())
	if err != nil || patched.JSON202 == nil {
		t.Fatalf("parse PATCH rewrite receipt: %v", err)
	}
	patchJob, err := patched.JSON202.AsRestoreJob()
	if err != nil || patchJob.JobId != "9223372036854775807" || patchJob.TotalTableCount != 2 {
		t.Fatalf("PATCH rewrite job lost identity or cohort: %+v, %v", patchJob, err)
	}
	replaced, err := ParseUpdateSchemaResponse(response())
	if err != nil || replaced.JSON202 == nil {
		t.Fatalf("parse PUT rewrite receipt: %v", err)
	}
	replaceJob, err := replaced.JSON202.AsRestoreJob()
	if err != nil || replaceJob.JobId != patchJob.JobId || replaceJob.Phase != RestoreJobPhaseQueued {
		t.Fatalf("PUT rewrite job lost identity or phase: %+v, %v", replaceJob, err)
	}
}
