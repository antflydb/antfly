package admin

import (
	"encoding/json"
	"fmt"
	"strings"
)

type haAdminStatusJSON struct {
	SchemaVersion uint32 `json:"schema_version"`
	Result        struct {
		PrimaryStatus *haPrimaryStatusJSON `json:"primary_status,omitempty"`
		StandbyStatus *haStandbyStatusJSON `json:"standby_status,omitempty"`
	} `json:"result"`
}

type haPrimaryStatusEnvelopeJSON struct {
	SchemaVersion uint32               `json:"schema_version"`
	Snapshot      *haPrimaryStatusJSON `json:"snapshot,omitempty"`
}

type haStandbyStatusEnvelopeJSON struct {
	SchemaVersion uint32               `json:"schema_version"`
	Snapshot      *haStandbyStatusJSON `json:"snapshot,omitempty"`
}

type haAdminIdentityJSON struct {
	ClusterID  *uint64 `json:"cluster_id"`
	ShardID    *uint64 `json:"shard_id"`
	TableID    *uint64 `json:"table_id"`
	TimelineID *uint64 `json:"timeline_id"`
	Epoch      *uint64 `json:"epoch"`
}

type haPrimaryStatusJSON struct {
	Role       string                  `json:"role"`
	Identity   haAdminIdentityJSON     `json:"identity"`
	CurrentLSN *uint64                 `json:"current_lsn"`
	Retention  *haRetentionStatusJSON  `json:"retention"`
	Durability *haDurabilityStatusJSON `json:"durability,omitempty"`
	Slots      *[]haSlotStatusJSON     `json:"slots"`
}

type haRetentionStatusJSON struct {
	PrimaryLSN        *uint64 `json:"primary_lsn"`
	OldestRestartLSN  *uint64 `json:"oldest_restart_lsn"`
	RetainedLSNCount  *uint64 `json:"retained_lsn_count"`
	ActiveSlots       *uint64 `json:"active_slots"`
	ReseedRecommended *uint64 `json:"reseed_recommended"`
}

type haSlotStatusJSON struct {
	Name            string  `json:"name"`
	TimelineID      *uint64 `json:"timeline_id"`
	Active          *bool   `json:"active"`
	ReseedRequired  *bool   `json:"reseed_required"`
	RestartLSN      *uint64 `json:"restart_lsn"`
	ReceivedLSN     *uint64 `json:"received_lsn"`
	AppliedLSN      *uint64 `json:"applied_lsn"`
	SafeReadLSN     *uint64 `json:"safe_read_lsn"`
	WriteLagLSN     *uint64 `json:"write_lag_lsn"`
	ApplyLagLSN     *uint64 `json:"apply_lag_lsn"`
	SafeReadLagLSN  *uint64 `json:"safe_read_lag_lsn"`
	RetentionLagLSN *uint64 `json:"retention_lag_lsn"`
	Status          string  `json:"status"`
	LastError       *string `json:"last_error"`
}

type haDurabilityStatusJSON struct {
	Status          string  `json:"status"`
	Mode            string  `json:"mode"`
	Selection       string  `json:"selection"`
	TargetLSN       *uint64 `json:"target_lsn"`
	ProgressLSN     *uint64 `json:"progress_lsn"`
	MissingLSNCount *uint64 `json:"missing_lsn_count"`
	SatisfiedCount  *uint64 `json:"satisfied_count"`
	RequiredCount   *uint64 `json:"required_count"`
	CandidateCount  *uint64 `json:"candidate_count"`
}

type haStandbyStatusJSON struct {
	Role               string              `json:"role"`
	Identity           haAdminIdentityJSON `json:"identity"`
	ReceivedLSN        *uint64             `json:"received_lsn"`
	AppliedLSN         *uint64             `json:"applied_lsn"`
	SafeReadLSN        *uint64             `json:"safe_read_lsn"`
	UpstreamLSN        *uint64             `json:"upstream_lsn"`
	WriteLagLSN        *uint64             `json:"write_lag_lsn"`
	ReceiveLagLSN      *uint64             `json:"receive_lag_lsn"`
	ApplyLagLSN        *uint64             `json:"apply_lag_lsn"`
	UnappliedLSNCount  *uint64             `json:"unapplied_lsn_count"`
	CaughtUpToReceived *bool               `json:"caught_up_to_received"`
	CanServeSafeReads  *bool               `json:"can_serve_safe_reads"`
}

type ParsedHAPrimaryStatus struct {
	Response      HAPrimaryStatusResponse
	HasDurability bool
}

// ParseHAPrimaryStatus validates a primary status body and returns the
// generated OpenAPI response model. It accepts the current /admin/v1 shape and
// the older CLI compatibility envelope used by existing operator tests.
func ParseHAPrimaryStatus(raw []byte) (*ParsedHAPrimaryStatus, error) {
	var direct haPrimaryStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return nil, err
	}
	snapshot := direct.Snapshot
	schemaVersion := direct.SchemaVersion
	if snapshot == nil {
		var doc haAdminStatusJSON
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, err
		}
		snapshot = doc.Result.PrimaryStatus
		schemaVersion = doc.SchemaVersion
	}
	if schemaVersion == 0 {
		return nil, fmt.Errorf("missing primary status schema_version")
	}
	if snapshot == nil {
		return nil, fmt.Errorf("missing primary status snapshot")
	}
	if strings.TrimSpace(snapshot.Role) != string(HAPrimarySnapshotRolePrimary) {
		return nil, fmt.Errorf("invalid primary status role")
	}
	if !haAdminIdentityJSONComplete(snapshot.Identity) {
		return nil, fmt.Errorf("missing primary status identity")
	}
	if snapshot.CurrentLSN == nil {
		return nil, fmt.Errorf("missing current_lsn")
	}
	if !haRetentionStatusJSONComplete(snapshot.Retention) {
		return nil, fmt.Errorf("missing retention snapshot fields")
	}
	if snapshot.Slots == nil {
		return nil, fmt.Errorf("missing slot snapshots")
	}
	parsed := &ParsedHAPrimaryStatus{
		HasDurability: snapshot.Durability != nil,
		Response: HAPrimaryStatusResponse{
			SchemaVersion: schemaVersion,
			Snapshot: HAPrimarySnapshot{
				CurrentLsn: *snapshot.CurrentLSN,
				Identity:   haIdentityFromStatusJSON(snapshot.Identity),
				Retention: HARetentionSnapshot{
					PrimaryLsn:        haUint64StatusValue(snapshot.Retention.PrimaryLSN),
					OldestRestartLsn:  haUint64StatusValue(snapshot.Retention.OldestRestartLSN),
					RetainedLsnCount:  haUint64StatusValue(snapshot.Retention.RetainedLSNCount),
					ActiveSlots:       haUint64StatusValue(snapshot.Retention.ActiveSlots),
					ReseedRecommended: haUint64StatusValue(snapshot.Retention.ReseedRecommended),
				},
				Role: HAPrimarySnapshotRolePrimary,
			},
		},
	}
	for _, slot := range *snapshot.Slots {
		if !haSlotStatusJSONComplete(slot) {
			return nil, fmt.Errorf("missing slot snapshot fields")
		}
		lastError := ""
		if slot.LastError != nil {
			lastError = strings.TrimSpace(*slot.LastError)
		}
		parsed.Response.Snapshot.Slots = append(parsed.Response.Snapshot.Slots, HASlotSnapshot{
			Name:            strings.TrimSpace(slot.Name),
			TimelineId:      haUint64StatusValue(slot.TimelineID),
			Active:          haBoolStatusValue(slot.Active),
			ReseedRequired:  haBoolStatusValue(slot.ReseedRequired),
			RestartLsn:      haUint64StatusValue(slot.RestartLSN),
			ReceivedLsn:     haUint64StatusValue(slot.ReceivedLSN),
			AppliedLsn:      haUint64StatusValue(slot.AppliedLSN),
			SafeReadLsn:     haUint64StatusValue(slot.SafeReadLSN),
			WriteLagLsn:     haUint64StatusValue(slot.WriteLagLSN),
			ApplyLagLsn:     haUint64StatusValue(slot.ApplyLagLSN),
			SafeReadLagLsn:  haUint64StatusValue(slot.SafeReadLagLSN),
			RetentionLagLsn: haUint64StatusValue(slot.RetentionLagLSN),
			Status:          HASlotSnapshotStatus(strings.TrimSpace(slot.Status)),
			LastError:       lastError,
		})
	}
	if snapshot.Durability != nil {
		if !haDurabilityStatusJSONComplete(*snapshot.Durability) {
			return nil, fmt.Errorf("missing durability status fields")
		}
		parsed.Response.Snapshot.Durability = HADurabilityDecision{
			Status:          HADurabilityDecisionStatus(strings.TrimSpace(snapshot.Durability.Status)),
			Mode:            HADurabilityDecisionMode(strings.TrimSpace(snapshot.Durability.Mode)),
			Selection:       HADurabilityDecisionSelection(strings.TrimSpace(snapshot.Durability.Selection)),
			TargetLsn:       haUint64StatusValue(snapshot.Durability.TargetLSN),
			ProgressLsn:     haUint64StatusValue(snapshot.Durability.ProgressLSN),
			MissingLsnCount: haUint64StatusValue(snapshot.Durability.MissingLSNCount),
			SatisfiedCount:  haUint64StatusValue(snapshot.Durability.SatisfiedCount),
			RequiredCount:   haUint64StatusValue(snapshot.Durability.RequiredCount),
			CandidateCount:  haUint64StatusValue(snapshot.Durability.CandidateCount),
		}
	}
	return parsed, nil
}

// ParseHAStandbyStatus validates a standby status body and returns the
// generated OpenAPI response model. It accepts the current /admin/v1 shape and
// the older CLI compatibility envelope used by existing operator tests.
func ParseHAStandbyStatus(raw []byte) (*HAStandbyStatusResponse, error) {
	var direct haStandbyStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return nil, err
	}
	snapshot := direct.Snapshot
	schemaVersion := direct.SchemaVersion
	if snapshot == nil {
		var doc haAdminStatusJSON
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, err
		}
		snapshot = doc.Result.StandbyStatus
		schemaVersion = doc.SchemaVersion
	}
	if schemaVersion == 0 {
		return nil, fmt.Errorf("missing standby status schema_version")
	}
	if snapshot == nil {
		return nil, fmt.Errorf("missing standby status snapshot")
	}
	if strings.TrimSpace(snapshot.Role) != string(HAStandbySnapshotRoleStandby) {
		return nil, fmt.Errorf("invalid standby status role")
	}
	if !haAdminIdentityJSONComplete(snapshot.Identity) {
		return nil, fmt.Errorf("missing standby status identity")
	}
	if !haStandbyStatusJSONComplete(snapshot) {
		return nil, fmt.Errorf("missing standby status fields")
	}
	return &HAStandbyStatusResponse{
		SchemaVersion: schemaVersion,
		Snapshot: HAStandbySnapshot{
			Role:               HAStandbySnapshotRoleStandby,
			Identity:           haIdentityFromStatusJSON(snapshot.Identity),
			ReceivedLsn:        haUint64StatusValue(snapshot.ReceivedLSN),
			AppliedLsn:         haUint64StatusValue(snapshot.AppliedLSN),
			SafeReadLsn:        haUint64StatusValue(snapshot.SafeReadLSN),
			UpstreamLsn:        haUint64StatusValue(snapshot.UpstreamLSN),
			WriteLagLsn:        haUint64StatusValue(snapshot.WriteLagLSN),
			ReceiveLagLsn:      haUint64StatusValue(snapshot.ReceiveLagLSN),
			ApplyLagLsn:        haUint64StatusValue(snapshot.ApplyLagLSN),
			UnappliedLsnCount:  haUint64StatusValue(snapshot.UnappliedLSNCount),
			CaughtUpToReceived: haBoolStatusValue(snapshot.CaughtUpToReceived),
			CanServeSafeReads:  haBoolStatusValue(snapshot.CanServeSafeReads),
		},
	}, nil
}

func haIdentityFromStatusJSON(identity haAdminIdentityJSON) HAIdentity {
	return HAIdentity{
		ClusterId:  haUint64StatusValue(identity.ClusterID),
		ShardId:    haUint64StatusValue(identity.ShardID),
		TableId:    haUint64StatusValue(identity.TableID),
		TimelineId: haUint64StatusValue(identity.TimelineID),
		Epoch:      haUint64StatusValue(identity.Epoch),
	}
}

func haAdminIdentityJSONComplete(identity haAdminIdentityJSON) bool {
	return identity.ClusterID != nil &&
		haUint64StatusValue(identity.ClusterID) > 0 &&
		identity.ShardID != nil &&
		identity.TableID != nil &&
		identity.TimelineID != nil &&
		haUint64StatusValue(identity.TimelineID) > 0 &&
		identity.Epoch != nil &&
		haUint64StatusValue(identity.Epoch) > 0
}

func haRetentionStatusJSONComplete(retention *haRetentionStatusJSON) bool {
	return retention != nil &&
		retention.PrimaryLSN != nil &&
		retention.OldestRestartLSN != nil &&
		retention.RetainedLSNCount != nil &&
		retention.ActiveSlots != nil &&
		retention.ReseedRecommended != nil
}

func haSlotStatusJSONComplete(slot haSlotStatusJSON) bool {
	return strings.TrimSpace(slot.Name) != "" &&
		slot.TimelineID != nil &&
		haUint64StatusValue(slot.TimelineID) > 0 &&
		slot.Active != nil &&
		slot.ReseedRequired != nil &&
		slot.RestartLSN != nil &&
		slot.ReceivedLSN != nil &&
		slot.AppliedLSN != nil &&
		slot.SafeReadLSN != nil &&
		slot.WriteLagLSN != nil &&
		slot.ApplyLagLSN != nil &&
		slot.SafeReadLagLSN != nil &&
		slot.RetentionLagLSN != nil &&
		haSlotStatusJSONValid(slot.Status)
}

func haDurabilityStatusJSONComplete(durability haDurabilityStatusJSON) bool {
	return haDurabilityDecisionStatusJSONValid(durability.Status) &&
		haDurabilityModeJSONValid(durability.Mode) &&
		haStandbySelectionJSONValid(durability.Selection) &&
		durability.TargetLSN != nil &&
		durability.ProgressLSN != nil &&
		durability.MissingLSNCount != nil &&
		durability.SatisfiedCount != nil &&
		durability.RequiredCount != nil &&
		durability.CandidateCount != nil
}

func haSlotStatusJSONValid(status string) bool {
	switch HASlotSnapshotStatus(strings.TrimSpace(status)) {
	case HASlotSnapshotStatusHealthy, HASlotSnapshotStatusLagging, HASlotSnapshotStatusReseedRequired:
		return true
	default:
		return false
	}
}

func haDurabilityDecisionStatusJSONValid(status string) bool {
	switch HADurabilityDecisionStatus(strings.TrimSpace(status)) {
	case HADurabilityStatusSatisfied, HADurabilityStatusWouldBlock, HADurabilityStatusFailClosed, HADurabilityStatusDegradedToAsync:
		return true
	default:
		return false
	}
}

func haDurabilityModeJSONValid(mode string) bool {
	switch HADurabilityDecisionMode(strings.TrimSpace(mode)) {
	case HADurabilityModeAsync, HADurabilityModeRemoteWrite, HADurabilityModeRemoteApply:
		return true
	default:
		return false
	}
}

func haStandbySelectionJSONValid(selection string) bool {
	switch HADurabilityDecisionSelection(strings.TrimSpace(selection)) {
	case HADurabilitySelectionAny, HADurabilitySelectionFirst, HADurabilitySelectionAll:
		return true
	default:
		return false
	}
}

func haStandbyStatusJSONComplete(snapshot *haStandbyStatusJSON) bool {
	return snapshot != nil &&
		snapshot.ReceivedLSN != nil &&
		snapshot.AppliedLSN != nil &&
		snapshot.SafeReadLSN != nil &&
		snapshot.UnappliedLSNCount != nil &&
		snapshot.CaughtUpToReceived != nil &&
		snapshot.CanServeSafeReads != nil
}

func haUint64StatusValue(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func haBoolStatusValue(value *bool) bool {
	return value != nil && *value
}
