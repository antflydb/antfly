package oapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParseRestoreTableAcceptedResponse(t *testing.T) {
	const body = `{"job_id":42,"attempt_id":0,"scope":"table","table_name":"products","backup_id":"nightly","phase":"queued","cancel_requested":false,"published_table_count":0,"completed_table_count":0,"total_table_count":1,"created_at_ms":1,"updated_at_ms":1,"expires_at_ms":9223372036854775807}`
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
	if response.JSON202.JobId != 42 {
		t.Fatalf("job ID = %d, want 42", response.JSON202.JobId)
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
