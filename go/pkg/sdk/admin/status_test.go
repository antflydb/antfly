package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStandbyStatusParserAcceptsLegacyPrimaryEnvelope(t *testing.T) {
	t.Parallel()

	parsed, err := ParseStandbyPrimaryStatus([]byte(standbyLegacyPrimaryStatusJSON()))
	if err != nil {
		t.Fatalf("ParseStandbyPrimaryStatus returned error: %v", err)
	}
	snapshot := parsed.Response.Snapshot
	if parsed.Response.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", parsed.Response.SchemaVersion)
	}
	if !parsed.HasDurability {
		t.Fatalf("HasDurability = false, want true")
	}
	if snapshot.CurrentLsn != 12 {
		t.Fatalf("CurrentLsn = %d, want 12", snapshot.CurrentLsn)
	}
	if snapshot.Identity.ClusterId != 11 || snapshot.Identity.TimelineId != 44 {
		t.Fatalf("Identity = %+v, want cluster_id=11 timeline_id=44", snapshot.Identity)
	}
	if snapshot.Retention.ActiveSlots != 1 || snapshot.Retention.ReseedRecommended != 0 {
		t.Fatalf("Retention = %+v, want active_slots=1 reseed_recommended=0", snapshot.Retention)
	}
	if snapshot.Retention.RetainedByteCount != 512 {
		t.Fatalf("RetainedByteCount = %d, want 512", snapshot.Retention.RetainedByteCount)
	}
	if got := len(snapshot.Slots); got != 1 {
		t.Fatalf("len(Slots) = %d, want 1", got)
	}
	slot := snapshot.Slots[0]
	if slot.Name != "standby-a" || slot.Status != StandbySlotSnapshotStatusHealthy || !slot.Active || slot.LastError != "" {
		t.Fatalf("Slot = %+v, want healthy active standby-a", slot)
	}
	if snapshot.Durability.Mode != StandbyDurabilityModeRemoteWrite ||
		snapshot.Durability.Status != StandbyDurabilityStatusSatisfied ||
		snapshot.Durability.Selection != StandbyDurabilitySelectionAny {
		t.Fatalf("Durability = %+v, want satisfied remote_write any", snapshot.Durability)
	}
}

func TestStandbyStatusParserAcceptsFreshPrimaryRetentionSentinel(t *testing.T) {
	t.Parallel()

	body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":5588500719990866000,"shard_id":0,"table_id":0,"timeline_id":1,"epoch":1},"current_lsn":0,"slots":[{"name":"standby-a","timeline_id":1,"active":true,"reseed_required":false,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"write_lag_lsn":0,"apply_lag_lsn":0,"safe_read_lag_lsn":0,"retention_lag_lsn":0,"status":"healthy","last_error":null}],"retention":{"primary_lsn":0,"oldest_restart_lsn":0,"retained_lsn_count":1,"retained_byte_count":0,"retained_age_ns":0,"active_slots":1,"reseed_recommended":0},"durability":null}}`

	parsed, err := ParseStandbyPrimaryStatus([]byte(body))
	if err != nil {
		t.Fatalf("ParseStandbyPrimaryStatus returned error: %v", err)
	}
	if parsed.Response.Snapshot.Retention.RetainedLsnCount != 1 {
		t.Fatalf("RetainedLsnCount = %d, want 1", parsed.Response.Snapshot.Retention.RetainedLsnCount)
	}
	if err := ValidateStandbyPrimaryStatusResponseEvidence([]byte(body)); err != nil {
		t.Fatalf("ValidateStandbyPrimaryStatusResponseEvidence returned error: %v", err)
	}
	var response StandbyPrimaryStatusResponse
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatalf("json.Unmarshal returned error: %v", err)
	}
	if err := ValidateStandbyPrimaryStatusResponse(response); err != nil {
		t.Fatalf("ValidateStandbyPrimaryStatusResponse returned error: %v", err)
	}
}

func TestStandbyStatusParserRejectsInvalidPrimaryFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "missing schema version",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"schema_version":1,`, `"schema_version":0,`, 1),
			wantErr: "schema_version",
		},
		{
			name:    "invalid slot status",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"status":"healthy"`, `"status":"catching_up"`, 1),
			wantErr: "slot",
		},
		{
			name:    "invalid slot name",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"name":"standby-a"`, `"name":"standby a"`, 1),
			wantErr: "slot",
		},
		{
			name:    "padded slot name",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"name":"standby-a"`, `"name":" standby-a"`, 1),
			wantErr: "slot",
		},
		{
			name:    "invalid durability mode",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"mode":"remote_write"`, `"mode":"remote-write"`, 1),
			wantErr: "durability",
		},
		{
			name:    "inconsistent retention count",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"retained_lsn_count":5`, `"retained_lsn_count":4`, 1),
			wantErr: "retention",
		},
		{
			name:    "missing retained age evidence",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"retained_age_ns":400,`, ``, 1),
			wantErr: "retention",
		},
		{
			name:    "slot applied beyond received",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"applied_lsn":12`, `"applied_lsn":13`, 1),
			wantErr: "slot",
		},
		{
			name:    "inconsistent durability missing count",
			body:    strings.Replace(standbyLegacyPrimaryStatusJSON(), `"missing_lsn_count":0`, `"missing_lsn_count":1`, 1),
			wantErr: "durability",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseStandbyPrimaryStatus([]byte(tt.body))
			if err == nil {
				t.Fatalf("ParseStandbyPrimaryStatus returned nil error, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestStandbyStatusParserAcceptsLegacyStandbyEnvelope(t *testing.T) {
	t.Parallel()

	response, err := ParseStandbyStatus([]byte(standbyLegacyStandbyStatusJSON()))
	if err != nil {
		t.Fatalf("ParseStandbyStatus returned error: %v", err)
	}
	snapshot := response.Snapshot
	if response.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", response.SchemaVersion)
	}
	if snapshot.Role != StandbySnapshotRoleStandby {
		t.Fatalf("Role = %q, want standby", snapshot.Role)
	}
	if snapshot.Identity.ClusterId != 11 || snapshot.Identity.TimelineId != 44 {
		t.Fatalf("Identity = %+v, want cluster_id=11 timeline_id=44", snapshot.Identity)
	}
	if snapshot.ReceivedLsn != 12 || snapshot.AppliedLsn != 11 || !snapshot.CanServeSafeReads {
		t.Fatalf("Snapshot = %+v, want received=12 applied=11 safe reads", snapshot)
	}
	if snapshot.LastError != "ConnectionRefused" {
		t.Fatalf("LastError = %q, want ConnectionRefused", snapshot.LastError)
	}
	if snapshot.LastAttemptNs != 1000 || snapshot.LastSuccessNs != 900 || snapshot.ReplicationFailuresTotal != 3 {
		t.Fatalf("standby replication watermarks = attempt %d success %d failures %d, want 1000/900/3", snapshot.LastAttemptNs, snapshot.LastSuccessNs, snapshot.ReplicationFailuresTotal)
	}
}

func TestStandbyStatusParserRejectsMissingStandbySafeReadFlag(t *testing.T) {
	t.Parallel()

	body := strings.Replace(standbyLegacyStandbyStatusJSON(), `"can_serve_safe_reads":true`, `"can_serve_safe_reads":null`, 1)
	_, err := ParseStandbyStatus([]byte(body))
	if err == nil {
		t.Fatalf("ParseStandbyStatus returned nil error, want missing standby fields error")
	}
	if !strings.Contains(err.Error(), "standby status fields") {
		t.Fatalf("error = %q, want standby status fields", err.Error())
	}
}

func TestStandbyStatusParserRejectsInconsistentStandbyProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "applied beyond received",
			body:    strings.Replace(standbyLegacyStandbyStatusJSON(), `"applied_lsn":11`, `"applied_lsn":13`, 1),
			wantErr: "applied_lsn",
		},
		{
			name:    "caught up flag lies",
			body:    strings.Replace(standbyLegacyStandbyStatusJSON(), `"caught_up_to_received":false`, `"caught_up_to_received":true`, 1),
			wantErr: "caught_up_to_received",
		},
		{
			name:    "upstream apply lag lies",
			body:    strings.Replace(standbyLegacyStandbyStatusJSON(), `"apply_lag_lsn":1`, `"apply_lag_lsn":0`, 1),
			wantErr: "apply_lag_lsn",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseStandbyStatus([]byte(tt.body))
			if err == nil {
				t.Fatalf("ParseStandbyStatus returned nil error, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestStandbyClientPrimaryStatusParsedResponseValidatesRawBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAPrimaryStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPrimaryStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyLegacyPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	response, err := client.PrimaryStatusParsedResponse(context.Background(), nil)
	if err != nil {
		t.Fatalf("PrimaryStatusParsedResponse returned error: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if len(response.Body) == 0 {
		t.Fatalf("Body is empty, want raw response body")
	}
	if response.Value.Response.Snapshot.CurrentLsn != 12 || !response.Value.HasDurability {
		t.Fatalf("parsed response = %+v, want current_lsn=12 with durability", response.Value)
	}
}

func TestStandbyClientWatchdogProofUsesDedicatedAuthenticatedRoute(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != HAWatchdogProofPath {
			t.Fatalf("request = %s %s, want GET %s", r.Method, r.URL.Path, HAWatchdogProofPath)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"proof":{"capability_version":1,"active":true,"authority_granted":true,"authority_remaining_ms":5000,"lease_name":"lease-a","lease_namespace":"default","stable_topology_id":"topology-a","local_node_id":"primary-a","observed_holder_node_id":"primary-a","pod_uid":"pod-a","process_boot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","observed_lease_transitions":1,"max_fence_latency_ms":10000}}`)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	response, err := client.WithToken("test-token").WatchdogProofResponse(context.Background())
	if err != nil {
		t.Fatalf("WatchdogProofResponse returned error: %v", err)
	}
	if response.Value.Proof.LocalNodeId != "primary-a" || !response.Value.Proof.AuthorityGranted {
		t.Fatalf("watchdog proof = %+v, want authoritative primary-a proof", response.Value.Proof)
	}
}

func TestStandbyClientPrimaryStatusParsedResponseSanitizesGeneratedQuery(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAPrimaryStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPrimaryStatusPath)
		}
		query := r.URL.Query()
		if _, ok := query["max_lag_lsn"]; ok {
			t.Fatalf("query includes max_lag_lsn = %q, want omitted zero value", query["max_lag_lsn"])
		}
		if _, ok := query["max_retained_bytes"]; ok {
			t.Fatalf("query includes max_retained_bytes = %q, want omitted zero value", query["max_retained_bytes"])
		}
		if _, ok := query["max_retained_age_ns"]; ok {
			t.Fatalf("query includes max_retained_age_ns = %q, want omitted zero value", query["max_retained_age_ns"])
		}
		if got := query.Get("sync_selection"); got != "all" {
			t.Fatalf("sync_selection = %q, want all", got)
		}
		if _, ok := query["sync_required"]; ok {
			t.Fatalf("query includes sync_required = %q, want omitted for ALL policy", query["sync_required"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyLegacyPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.PrimaryStatusParsedResponse(context.Background(), &StandbyPrimaryStatusParams{
		SyncMode:      StandbyPrimaryStatusSyncModeRemoteApply,
		SyncSelection: StandbyPrimaryStatusSyncSelectionAll,
		SyncRequired:  2,
		SyncStandby:   []string{"standby-a", "standby-b"},
	})
	if err != nil {
		t.Fatalf("PrimaryStatusParsedResponse returned error: %v", err)
	}
}

func TestStandbyClientPrimaryStatusParsedResponseOmitsEmptyAsyncSyncQuery(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAPrimaryStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPrimaryStatusPath)
		}
		query := r.URL.Query()
		if got := query.Get("max_lag_lsn"); got != "1000000" {
			t.Fatalf("max_lag_lsn = %q, want 1000000", got)
		}
		for _, key := range []string{"sync_mode", "sync_selection", "sync_required", "sync_standby", "sync_failure"} {
			if values, ok := query[key]; ok {
				t.Fatalf("query includes %s = %q, want omitted for async policy", key, values)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyLegacyPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.PrimaryStatusParsedResponse(context.Background(), &StandbyPrimaryStatusParams{
		MaxLagLsn: 1000000,
	})
	if err != nil {
		t.Fatalf("PrimaryStatusParsedResponse returned error: %v", err)
	}
}

func TestStandbyClientPrimaryStatusResponseRejectsInvalidGeneratedBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAPrimaryStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPrimaryStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(standbyGeneratedPrimaryStatusJSON(), `"retained_lsn_count":5`, `"retained_lsn_count":4`, 1))
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.PrimaryStatusResponse(context.Background(), nil)
	if err == nil {
		t.Fatalf("PrimaryStatusResponse returned nil error, want validation error")
	}
	if !strings.Contains(err.Error(), "get HA primary status response invalid") {
		t.Fatalf("error = %q, want typed response validation error", err.Error())
	}
	if !strings.Contains(err.Error(), "retention") {
		t.Fatalf("error = %q, want retention invariant", err.Error())
	}
}

func TestStandbyClientPrimaryStatusResponseRejectsMissingRequiredGeneratedField(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAPrimaryStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPrimaryStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(standbyGeneratedPrimaryStatusJSON(), `"retained_age_ns":400,`, ``, 1))
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.PrimaryStatusResponse(context.Background(), nil)
	if err == nil {
		t.Fatalf("PrimaryStatusResponse returned nil error, want missing field evidence error")
	}
	if !strings.Contains(err.Error(), "missing primary status retention field evidence") {
		t.Fatalf("error = %q, want missing retention evidence", err.Error())
	}
}

func TestValidateStandbyPrimaryStatusResponseEvidenceRejectsPaddedSlotName(t *testing.T) {
	t.Parallel()

	body := strings.Replace(standbyGeneratedPrimaryStatusJSON(), `"name":"standby-a"`, `"name":"standby-a "`, 1)
	err := ValidateStandbyPrimaryStatusResponseEvidence([]byte(body))
	if err == nil {
		t.Fatalf("ValidateStandbyPrimaryStatusResponseEvidence returned nil error, want slot field evidence error")
	}
	if !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("error = %q, want slot field evidence", err.Error())
	}
}

func TestStandbyClientStandbyStatusResponseRejectsInvalidGeneratedBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAStandbyStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAStandbyStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(standbyGeneratedStandbyStatusJSON(), `"caught_up_to_received":false`, `"caught_up_to_received":true`, 1))
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.StandbyStatusResponse(context.Background(), nil)
	if err == nil {
		t.Fatalf("StandbyStatusResponse returned nil error, want validation error")
	}
	if !strings.Contains(err.Error(), "get HA standby status response invalid") {
		t.Fatalf("error = %q, want typed response validation error", err.Error())
	}
	if !strings.Contains(err.Error(), "caught_up_to_received") {
		t.Fatalf("error = %q, want caught_up_to_received invariant", err.Error())
	}
}

func TestValidateStandbyStatusResponsesRejectInvalidNodeIDs(t *testing.T) {
	t.Parallel()

	var primary StandbyPrimaryStatusResponse
	if err := json.Unmarshal([]byte(standbyGeneratedPrimaryStatusJSON()), &primary); err != nil {
		t.Fatalf("unmarshal primary status: %v", err)
	}
	primary.Snapshot.NodeId = "primary a"
	if err := ValidateStandbyPrimaryStatusResponse(primary); err == nil || !strings.Contains(err.Error(), "invalid primary status node_id") {
		t.Fatalf("primary status node_id error = %v, want invalid node_id", err)
	}

	var standby StandbyStatusResponse
	if err := json.Unmarshal([]byte(standbyGeneratedStandbyStatusJSON()), &standby); err != nil {
		t.Fatalf("unmarshal standby status: %v", err)
	}
	standby.Snapshot.NodeId = "standby/a"
	if err := ValidateStandbyStatusResponse(standby); err == nil || !strings.Contains(err.Error(), "invalid standby status node_id") {
		t.Fatalf("standby status node_id error = %v, want invalid node_id", err)
	}
}

func TestStandbyClientStandbyStatusResponseRejectsMissingRequiredGeneratedField(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAStandbyStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAStandbyStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(standbyGeneratedStandbyStatusJSON(), `"safe_read_lsn":11,`, ``, 1))
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.StandbyStatusResponse(context.Background(), nil)
	if err == nil {
		t.Fatalf("StandbyStatusResponse returned nil error, want missing field evidence error")
	}
	if !strings.Contains(err.Error(), "missing standby status progress field evidence") {
		t.Fatalf("error = %q, want missing progress evidence", err.Error())
	}
}

func TestStandbyClientStandbyStatusParsedResponseValidatesRawBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAStandbyStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAStandbyStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyLegacyStandbyStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	response, err := client.StandbyStatusParsedResponse(context.Background(), nil)
	if err != nil {
		t.Fatalf("StandbyStatusParsedResponse returned error: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if len(response.Body) == 0 {
		t.Fatalf("Body is empty, want raw response body")
	}
	var parsed = response.Value
	if parsed.Snapshot.ReceivedLsn != 12 || parsed.Snapshot.AppliedLsn != 11 || !parsed.Snapshot.CanServeSafeReads || parsed.Snapshot.LastError != "ConnectionRefused" {
		t.Fatalf("parsed response = %+v, want received=12 applied=11 safe reads", parsed)
	}
}

func TestStandbyClientStandbyStatusParsedResponseSanitizesGeneratedQuery(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAStandbyStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAStandbyStatusPath)
		}
		query := r.URL.Query()
		if _, ok := query["upstream_lsn"]; ok {
			t.Fatalf("query includes upstream_lsn = %q, want omitted zero value", query["upstream_lsn"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyLegacyStandbyStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.StandbyStatusParsedResponse(context.Background(), &StandbyStatusParams{})
	if err != nil {
		t.Fatalf("StandbyStatusParsedResponse returned error: %v", err)
	}
}

func TestStandbyClientStandbyStatusParsedResponseRejectsInvalidRawBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(standbyLegacyStandbyStatusJSON(), `"can_serve_safe_reads":true`, `"can_serve_safe_reads":null`, 1))
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.StandbyStatusParsedResponse(context.Background(), nil)
	if err == nil {
		t.Fatalf("StandbyStatusParsedResponse returned nil error, want validation error")
	}
	if !strings.Contains(err.Error(), "standby status fields") {
		t.Fatalf("error = %q, want standby status fields", err.Error())
	}
}

func standbyGeneratedPrimaryStatusJSON() string {
	return `{
		"schema_version":1,
		"snapshot":{
			"role":"primary",
			"node_id":"primary-a",
			"identity":{
				"cluster_id":11,
				"shard_id":22,
				"table_id":33,
				"timeline_id":44,
				"epoch":55
			},
			"current_lsn":12,
			"retention":{
				"primary_lsn":12,
				"oldest_restart_lsn":7,
				"retained_lsn_count":5,
				"retained_byte_count":512,
				"retained_age_ns":400,
				"active_slots":1,
				"reseed_recommended":0
			},
			"durability":{
				"status":"satisfied",
				"mode":"remote_write",
				"selection":"any",
				"target_lsn":12,
				"progress_lsn":12,
				"missing_lsn_count":0,
				"satisfied_count":1,
				"required_count":1,
				"candidate_count":1
			},
			"slots":[{
				"name":"standby-a",
				"timeline_id":44,
				"active":true,
				"reseed_required":false,
				"restart_lsn":7,
				"received_lsn":12,
				"applied_lsn":12,
				"safe_read_lsn":12,
				"write_lag_lsn":0,
				"apply_lag_lsn":0,
				"safe_read_lag_lsn":0,
				"retention_lag_lsn":5,
				"status":"healthy",
				"last_error":""
			}]
		}
	}`
}

func standbyGeneratedStandbyStatusJSON() string {
	return `{
		"schema_version":1,
		"snapshot":{
			"role":"standby",
			"node_id":"standby-a",
			"identity":{
				"cluster_id":11,
				"shard_id":22,
				"table_id":33,
				"timeline_id":44,
				"epoch":55
			},
			"received_lsn":12,
			"applied_lsn":11,
			"safe_read_lsn":11,
			"upstream_lsn":12,
			"write_lag_lsn":0,
			"receive_lag_lsn":0,
			"apply_lag_lsn":1,
			"last_error":"ConnectionRefused",
			"last_attempt_ns":1000,
			"last_success_ns":900,
			"replication_failures_total":3,
			"unapplied_lsn_count":1,
			"caught_up_to_received":false,
			"can_serve_safe_reads":true
		}
	}`
}

func standbyLegacyPrimaryStatusJSON() string {
	return `{
		"schema_version":1,
		"result":{
			"primary_status":{
				"role":"primary",
				"node_id":"primary-a",
				"identity":{
					"cluster_id":11,
					"shard_id":22,
					"table_id":33,
					"timeline_id":44,
					"epoch":55
				},
				"current_lsn":12,
				"retention":{
					"primary_lsn":12,
					"oldest_restart_lsn":7,
					"retained_lsn_count":5,
					"retained_byte_count":512,
					"retained_age_ns":400,
					"active_slots":1,
					"reseed_recommended":0
				},
				"durability":{
					"status":"satisfied",
					"mode":"remote_write",
					"selection":"any",
					"target_lsn":12,
					"progress_lsn":12,
					"missing_lsn_count":0,
					"satisfied_count":1,
					"required_count":1,
					"candidate_count":1
				},
				"slots":[{
					"name":"standby-a",
					"timeline_id":44,
					"active":true,
					"reseed_required":false,
					"restart_lsn":7,
					"received_lsn":12,
					"applied_lsn":12,
					"safe_read_lsn":12,
					"write_lag_lsn":0,
					"apply_lag_lsn":0,
					"safe_read_lag_lsn":0,
					"retention_lag_lsn":5,
					"status":"healthy",
					"last_error":""
				}]
			}
		}
	}`
}

func standbyLegacyStandbyStatusJSON() string {
	return `{
		"schema_version":1,
		"result":{
			"standby_status":{
				"role":"standby",
				"node_id":"standby-a",
				"identity":{
					"cluster_id":11,
					"shard_id":22,
					"table_id":33,
					"timeline_id":44,
					"epoch":55
				},
				"received_lsn":12,
				"applied_lsn":11,
				"safe_read_lsn":11,
				"upstream_lsn":12,
				"write_lag_lsn":0,
				"receive_lag_lsn":0,
			"apply_lag_lsn":1,
			"last_error":"ConnectionRefused",
			"last_attempt_ns":1000,
			"last_success_ns":900,
			"replication_failures_total":3,
			"unapplied_lsn_count":1,
			"caught_up_to_received":false,
			"can_serve_safe_reads":true
			}
		}
	}`
}
