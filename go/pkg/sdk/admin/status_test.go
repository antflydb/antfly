package admin

import (
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
					"retention_lag_lsn":0,
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
