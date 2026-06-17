package admin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHAStatusParserAcceptsLegacyPrimaryEnvelope(t *testing.T) {
	t.Parallel()

	parsed, err := ParseHAPrimaryStatus([]byte(haLegacyPrimaryStatusJSON()))
	if err != nil {
		t.Fatalf("ParseHAPrimaryStatus returned error: %v", err)
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
	if got := len(snapshot.Slots); got != 1 {
		t.Fatalf("len(Slots) = %d, want 1", got)
	}
	slot := snapshot.Slots[0]
	if slot.Name != "standby-a" || slot.Status != HASlotSnapshotStatusHealthy || !slot.Active || slot.LastError != "" {
		t.Fatalf("Slot = %+v, want healthy active standby-a", slot)
	}
	if snapshot.Durability.Mode != HADurabilityModeRemoteWrite ||
		snapshot.Durability.Status != HADurabilityStatusSatisfied ||
		snapshot.Durability.Selection != HADurabilitySelectionAny {
		t.Fatalf("Durability = %+v, want satisfied remote_write any", snapshot.Durability)
	}
}

func TestHAStatusParserRejectsInvalidPrimaryFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "missing schema version",
			body:    strings.Replace(haLegacyPrimaryStatusJSON(), `"schema_version":1,`, `"schema_version":0,`, 1),
			wantErr: "schema_version",
		},
		{
			name:    "invalid slot status",
			body:    strings.Replace(haLegacyPrimaryStatusJSON(), `"status":"healthy"`, `"status":"catching_up"`, 1),
			wantErr: "slot",
		},
		{
			name:    "invalid durability mode",
			body:    strings.Replace(haLegacyPrimaryStatusJSON(), `"mode":"remote_write"`, `"mode":"remote-write"`, 1),
			wantErr: "durability",
		},
		{
			name:    "inconsistent retention count",
			body:    strings.Replace(haLegacyPrimaryStatusJSON(), `"retained_lsn_count":5`, `"retained_lsn_count":4`, 1),
			wantErr: "retention",
		},
		{
			name:    "slot applied beyond received",
			body:    strings.Replace(haLegacyPrimaryStatusJSON(), `"applied_lsn":12`, `"applied_lsn":13`, 1),
			wantErr: "slot",
		},
		{
			name:    "inconsistent durability missing count",
			body:    strings.Replace(haLegacyPrimaryStatusJSON(), `"missing_lsn_count":0`, `"missing_lsn_count":1`, 1),
			wantErr: "durability",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseHAPrimaryStatus([]byte(tt.body))
			if err == nil {
				t.Fatalf("ParseHAPrimaryStatus returned nil error, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestHAStatusParserAcceptsLegacyStandbyEnvelope(t *testing.T) {
	t.Parallel()

	response, err := ParseHAStandbyStatus([]byte(haLegacyStandbyStatusJSON()))
	if err != nil {
		t.Fatalf("ParseHAStandbyStatus returned error: %v", err)
	}
	snapshot := response.Snapshot
	if response.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", response.SchemaVersion)
	}
	if snapshot.Role != HAStandbySnapshotRoleStandby {
		t.Fatalf("Role = %q, want standby", snapshot.Role)
	}
	if snapshot.Identity.ClusterId != 11 || snapshot.Identity.TimelineId != 44 {
		t.Fatalf("Identity = %+v, want cluster_id=11 timeline_id=44", snapshot.Identity)
	}
	if snapshot.ReceivedLsn != 12 || snapshot.AppliedLsn != 11 || !snapshot.CanServeSafeReads {
		t.Fatalf("Snapshot = %+v, want received=12 applied=11 safe reads", snapshot)
	}
}

func TestHAStatusParserRejectsMissingStandbySafeReadFlag(t *testing.T) {
	t.Parallel()

	body := strings.Replace(haLegacyStandbyStatusJSON(), `"can_serve_safe_reads":true`, `"can_serve_safe_reads":null`, 1)
	_, err := ParseHAStandbyStatus([]byte(body))
	if err == nil {
		t.Fatalf("ParseHAStandbyStatus returned nil error, want missing standby fields error")
	}
	if !strings.Contains(err.Error(), "standby status fields") {
		t.Fatalf("error = %q, want standby status fields", err.Error())
	}
}

func TestHAStatusParserRejectsInconsistentStandbyProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "applied beyond received",
			body:    strings.Replace(haLegacyStandbyStatusJSON(), `"applied_lsn":11`, `"applied_lsn":13`, 1),
			wantErr: "applied_lsn",
		},
		{
			name:    "caught up flag lies",
			body:    strings.Replace(haLegacyStandbyStatusJSON(), `"caught_up_to_received":false`, `"caught_up_to_received":true`, 1),
			wantErr: "caught_up_to_received",
		},
		{
			name:    "upstream apply lag lies",
			body:    strings.Replace(haLegacyStandbyStatusJSON(), `"apply_lag_lsn":1`, `"apply_lag_lsn":0`, 1),
			wantErr: "apply_lag_lsn",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseHAStandbyStatus([]byte(tt.body))
			if err == nil {
				t.Fatalf("ParseHAStandbyStatus returned nil error, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestHAClientPrimaryStatusParsedResponseValidatesRawBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAPrimaryStatusPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPrimaryStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, haLegacyPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
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

func TestHAClientStandbyStatusParsedResponseRejectsInvalidRawBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(haLegacyStandbyStatusJSON(), `"can_serve_safe_reads":true`, `"can_serve_safe_reads":null`, 1))
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	_, err = client.StandbyStatusParsedResponse(context.Background(), nil)
	if err == nil {
		t.Fatalf("StandbyStatusParsedResponse returned nil error, want validation error")
	}
	if !strings.Contains(err.Error(), "standby status fields") {
		t.Fatalf("error = %q, want standby status fields", err.Error())
	}
}

func haLegacyPrimaryStatusJSON() string {
	return `{
		"schema_version":1,
		"result":{
			"primary_status":{
				"role":"primary",
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

func haLegacyStandbyStatusJSON() string {
	return `{
		"schema_version":1,
		"result":{
			"standby_status":{
				"role":"standby",
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
				"unapplied_lsn_count":1,
				"caught_up_to_received":false,
				"can_serve_safe_reads":true
			}
		}
	}`
}
