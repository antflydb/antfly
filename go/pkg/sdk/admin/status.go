package admin

import (
	"encoding/json"
	"fmt"
	"strings"
)

type standbyAdminStatusJSON struct {
	SchemaVersion uint32 `json:"schema_version"`
	Result        struct {
		PrimaryStatus *standbyPrimaryStatusJSON `json:"primary_status,omitempty"`
		StandbyStatus *standbyStatusJSON        `json:"standby_status,omitempty"`
	} `json:"result"`
}

type standbyPrimaryStatusEnvelopeJSON struct {
	SchemaVersion uint32                    `json:"schema_version"`
	Snapshot      *standbyPrimaryStatusJSON `json:"snapshot,omitempty"`
}

type standbyStatusEnvelopeJSON struct {
	SchemaVersion uint32             `json:"schema_version"`
	Snapshot      *standbyStatusJSON `json:"snapshot,omitempty"`
}

type standbyAdminIdentityJSON struct {
	ClusterID  *uint64 `json:"cluster_id"`
	ShardID    *uint64 `json:"shard_id"`
	TableID    *uint64 `json:"table_id"`
	TimelineID *uint64 `json:"timeline_id"`
	Epoch      *uint64 `json:"epoch"`
}

type standbyPrimaryStatusJSON struct {
	WaitingForTables *bool                        `json:"waiting_for_tables,omitempty"`
	Role             string                       `json:"role"`
	NodeID           string                       `json:"node_id"`
	Identity         standbyAdminIdentityJSON     `json:"identity"`
	CurrentLSN       *uint64                      `json:"current_lsn"`
	Retention        *standbyRetentionStatusJSON  `json:"retention"`
	Durability       *standbyDurabilityStatusJSON `json:"durability,omitempty"`
	Slots            *[]standbySlotStatusJSON     `json:"slots"`
	LeaseWatchdog    *StandbyLeaseWatchdogProof   `json:"lease_watchdog,omitempty"`
}

type standbyRetentionStatusJSON struct {
	PrimaryLSN        *uint64 `json:"primary_lsn"`
	OldestRestartLSN  *uint64 `json:"oldest_restart_lsn"`
	RetainedLSNCount  *uint64 `json:"retained_lsn_count"`
	RetainedByteCount *uint64 `json:"retained_byte_count"`
	RetainedAgeNS     *uint64 `json:"retained_age_ns"`
	ActiveSlots       *uint64 `json:"active_slots"`
	ReseedRecommended *uint64 `json:"reseed_recommended"`
}

type standbySlotStatusJSON struct {
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

type standbyDurabilityStatusJSON struct {
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

type standbyStatusJSON struct {
	Role                     string                     `json:"role"`
	NodeID                   string                     `json:"node_id"`
	Identity                 standbyAdminIdentityJSON   `json:"identity"`
	ReceivedLSN              *uint64                    `json:"received_lsn"`
	AppliedLSN               *uint64                    `json:"applied_lsn"`
	SafeReadLSN              *uint64                    `json:"safe_read_lsn"`
	UpstreamLSN              *uint64                    `json:"upstream_lsn"`
	WriteLagLSN              *uint64                    `json:"write_lag_lsn"`
	ReceiveLagLSN            *uint64                    `json:"receive_lag_lsn"`
	ApplyLagLSN              *uint64                    `json:"apply_lag_lsn"`
	LastError                *string                    `json:"last_error"`
	LastAttemptNs            *uint64                    `json:"last_attempt_ns"`
	LastSuccessNs            *uint64                    `json:"last_success_ns"`
	ReplicationFailuresTotal *uint64                    `json:"replication_failures_total"`
	UnappliedLSNCount        *uint64                    `json:"unapplied_lsn_count"`
	CaughtUpToReceived       *bool                      `json:"caught_up_to_received"`
	CanServeSafeReads        *bool                      `json:"can_serve_safe_reads"`
	LeaseWatchdog            *StandbyLeaseWatchdogProof `json:"lease_watchdog,omitempty"`
}

type ParsedStandbyPrimaryStatus struct {
	Response      StandbyPrimaryStatusResponse
	HasDurability bool
}

type ParsedStandbyStatus = StandbyStatusResponse

// ParseStandbyPrimaryStatus validates a primary status body and returns the
// generated OpenAPI response model. It accepts the current /admin/v1 shape and
// the older CLI compatibility envelope used by existing operator tests.
func ParseStandbyPrimaryStatus(raw []byte) (*ParsedStandbyPrimaryStatus, error) {
	var direct standbyPrimaryStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return nil, err
	}
	snapshot := direct.Snapshot
	schemaVersion := direct.SchemaVersion
	if snapshot == nil {
		var doc standbyAdminStatusJSON
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
	if strings.TrimSpace(snapshot.Role) != string(StandbyPrimarySnapshotRolePrimary) {
		return nil, fmt.Errorf("invalid primary status role")
	}
	nodeID := snapshot.NodeID
	if !validStandbyIdentifier(nodeID) {
		return nil, fmt.Errorf("invalid primary status node_id %q", snapshot.NodeID)
	}
	if !standbyAdminIdentityJSONComplete(snapshot.Identity) {
		return nil, fmt.Errorf("missing primary status identity")
	}
	if snapshot.CurrentLSN == nil {
		return nil, fmt.Errorf("missing current_lsn")
	}
	if !standbyRetentionStatusJSONComplete(snapshot.Retention) {
		return nil, fmt.Errorf("missing retention snapshot fields")
	}
	if snapshot.Slots == nil {
		return nil, fmt.Errorf("missing slot snapshots")
	}
	if err := standbyRetentionStatusJSONConsistent(*snapshot.CurrentLSN, snapshot.Retention, len(*snapshot.Slots)); err != nil {
		return nil, err
	}
	parsed := &ParsedStandbyPrimaryStatus{
		HasDurability: snapshot.Durability != nil,
		Response: StandbyPrimaryStatusResponse{
			SchemaVersion: schemaVersion,
			Snapshot: StandbyPrimarySnapshot{
				WaitingForTables: snapshot.WaitingForTables,
				CurrentLsn:       *snapshot.CurrentLSN,
				Identity:         standbyIdentityFromStatusJSON(snapshot.Identity),
				NodeId:           nodeID,
				Retention: StandbyRetentionSnapshot{
					PrimaryLsn:        standbyUint64StatusValue(snapshot.Retention.PrimaryLSN),
					OldestRestartLsn:  standbyUint64StatusValue(snapshot.Retention.OldestRestartLSN),
					RetainedLsnCount:  standbyUint64StatusValue(snapshot.Retention.RetainedLSNCount),
					RetainedByteCount: standbyUint64StatusValue(snapshot.Retention.RetainedByteCount),
					RetainedAgeNs:     standbyUint64StatusValue(snapshot.Retention.RetainedAgeNS),
					ActiveSlots:       standbyUint64StatusValue(snapshot.Retention.ActiveSlots),
					ReseedRecommended: standbyUint64StatusValue(snapshot.Retention.ReseedRecommended),
				},
				Role: StandbyPrimarySnapshotRolePrimary,
			},
		},
	}
	if snapshot.LeaseWatchdog != nil {
		parsed.Response.Snapshot.LeaseWatchdog = *snapshot.LeaseWatchdog
	}
	for _, slot := range *snapshot.Slots {
		if !standbySlotStatusJSONComplete(slot) {
			return nil, fmt.Errorf("missing slot snapshot fields")
		}
		if err := standbySlotStatusJSONConsistent(*snapshot.CurrentLSN, slot); err != nil {
			return nil, err
		}
		lastError := ""
		if slot.LastError != nil {
			lastError = strings.TrimSpace(*slot.LastError)
		}
		parsed.Response.Snapshot.Slots = append(parsed.Response.Snapshot.Slots, StandbySlotSnapshot{
			Name:            slot.Name,
			TimelineId:      standbyUint64StatusValue(slot.TimelineID),
			Active:          standbyBoolStatusValue(slot.Active),
			ReseedRequired:  standbyBoolStatusValue(slot.ReseedRequired),
			RestartLsn:      standbyUint64StatusValue(slot.RestartLSN),
			ReceivedLsn:     standbyUint64StatusValue(slot.ReceivedLSN),
			AppliedLsn:      standbyUint64StatusValue(slot.AppliedLSN),
			SafeReadLsn:     standbyUint64StatusValue(slot.SafeReadLSN),
			WriteLagLsn:     standbyUint64StatusValue(slot.WriteLagLSN),
			ApplyLagLsn:     standbyUint64StatusValue(slot.ApplyLagLSN),
			SafeReadLagLsn:  standbyUint64StatusValue(slot.SafeReadLagLSN),
			RetentionLagLsn: standbyUint64StatusValue(slot.RetentionLagLSN),
			Status:          StandbySlotSnapshotStatus(strings.TrimSpace(slot.Status)),
			LastError:       lastError,
		})
	}
	if snapshot.Durability != nil {
		if !standbyDurabilityStatusJSONComplete(*snapshot.Durability) {
			return nil, fmt.Errorf("missing durability status fields")
		}
		if err := standbyDurabilityStatusJSONConsistent(*snapshot.Durability); err != nil {
			return nil, err
		}
		parsed.Response.Snapshot.Durability = StandbyDurabilityDecision{
			Status:          StandbyDurabilityDecisionStatus(strings.TrimSpace(snapshot.Durability.Status)),
			Mode:            StandbyDurabilityDecisionMode(strings.TrimSpace(snapshot.Durability.Mode)),
			Selection:       StandbyDurabilityDecisionSelection(strings.TrimSpace(snapshot.Durability.Selection)),
			TargetLsn:       standbyUint64StatusValue(snapshot.Durability.TargetLSN),
			ProgressLsn:     standbyUint64StatusValue(snapshot.Durability.ProgressLSN),
			MissingLsnCount: standbyUint64StatusValue(snapshot.Durability.MissingLSNCount),
			SatisfiedCount:  standbyUint64StatusValue(snapshot.Durability.SatisfiedCount),
			RequiredCount:   standbyUint64StatusValue(snapshot.Durability.RequiredCount),
			CandidateCount:  standbyUint64StatusValue(snapshot.Durability.CandidateCount),
		}
	}
	if err := ValidateStandbyPrimaryStatusResponse(parsed.Response); err != nil {
		return nil, err
	}
	return parsed, nil
}

// ParseStandbyStatus validates a standby status body and returns the
// generated OpenAPI response model. It accepts the current /admin/v1 shape and
// the older CLI compatibility envelope used by existing operator tests.
func ParseStandbyStatus(raw []byte) (*ParsedStandbyStatus, error) {
	var direct standbyStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return nil, err
	}
	snapshot := direct.Snapshot
	schemaVersion := direct.SchemaVersion
	if snapshot == nil {
		var doc standbyAdminStatusJSON
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
	if strings.TrimSpace(snapshot.Role) != string(StandbySnapshotRoleStandby) {
		return nil, fmt.Errorf("invalid standby status role")
	}
	nodeID := snapshot.NodeID
	if !validStandbyIdentifier(nodeID) {
		return nil, fmt.Errorf("invalid standby status node_id %q", snapshot.NodeID)
	}
	if !standbyAdminIdentityJSONComplete(snapshot.Identity) {
		return nil, fmt.Errorf("missing standby status identity")
	}
	if !standbyStatusJSONComplete(snapshot) {
		return nil, fmt.Errorf("missing standby status fields")
	}
	if err := standbyStatusJSONConsistent(snapshot); err != nil {
		return nil, err
	}
	response := &StandbyStatusResponse{
		SchemaVersion: schemaVersion,
		Snapshot: StandbySnapshot{
			Role:                     StandbySnapshotRoleStandby,
			NodeId:                   nodeID,
			Identity:                 standbyIdentityFromStatusJSON(snapshot.Identity),
			ReceivedLsn:              standbyUint64StatusValue(snapshot.ReceivedLSN),
			AppliedLsn:               standbyUint64StatusValue(snapshot.AppliedLSN),
			SafeReadLsn:              standbyUint64StatusValue(snapshot.SafeReadLSN),
			UpstreamLsn:              standbyUint64StatusValue(snapshot.UpstreamLSN),
			WriteLagLsn:              standbyUint64StatusValue(snapshot.WriteLagLSN),
			ReceiveLagLsn:            standbyUint64StatusValue(snapshot.ReceiveLagLSN),
			ApplyLagLsn:              standbyUint64StatusValue(snapshot.ApplyLagLSN),
			LastError:                standbyStringStatusValue(snapshot.LastError),
			LastAttemptNs:            standbyUint64StatusValue(snapshot.LastAttemptNs),
			LastSuccessNs:            standbyUint64StatusValue(snapshot.LastSuccessNs),
			ReplicationFailuresTotal: standbyUint64StatusValue(snapshot.ReplicationFailuresTotal),
			UnappliedLsnCount:        standbyUint64StatusValue(snapshot.UnappliedLSNCount),
			CaughtUpToReceived:       standbyBoolStatusValue(snapshot.CaughtUpToReceived),
			CanServeSafeReads:        standbyBoolStatusValue(snapshot.CanServeSafeReads),
		},
	}
	if snapshot.LeaseWatchdog != nil {
		response.Snapshot.LeaseWatchdog = *snapshot.LeaseWatchdog
	}
	if err := ValidateStandbyStatusResponse(*response); err != nil {
		return nil, err
	}
	return response, nil
}

func standbyIdentityFromStatusJSON(identity standbyAdminIdentityJSON) StandbyIdentity {
	return StandbyIdentity{
		ClusterId:  standbyUint64StatusValue(identity.ClusterID),
		ShardId:    standbyUint64StatusValue(identity.ShardID),
		TableId:    standbyUint64StatusValue(identity.TableID),
		TimelineId: standbyUint64StatusValue(identity.TimelineID),
		Epoch:      standbyUint64StatusValue(identity.Epoch),
	}
}

func standbyAdminIdentityJSONComplete(identity standbyAdminIdentityJSON) bool {
	return identity.ClusterID != nil &&
		standbyUint64StatusValue(identity.ClusterID) > 0 &&
		identity.ShardID != nil &&
		identity.TableID != nil &&
		identity.TimelineID != nil &&
		standbyUint64StatusValue(identity.TimelineID) > 0 &&
		identity.Epoch != nil &&
		standbyUint64StatusValue(identity.Epoch) > 0
}

func standbyRetentionStatusJSONComplete(retention *standbyRetentionStatusJSON) bool {
	return retention != nil &&
		retention.PrimaryLSN != nil &&
		retention.OldestRestartLSN != nil &&
		retention.RetainedLSNCount != nil &&
		retention.RetainedByteCount != nil &&
		retention.RetainedAgeNS != nil &&
		retention.ActiveSlots != nil &&
		retention.ReseedRecommended != nil
}

func ValidateStandbyPrimaryStatusResponseEvidence(raw []byte) error {
	var direct standbyPrimaryStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return fmt.Errorf("missing primary status snapshot field evidence")
	}
	if !standbyAdminIdentityJSONComplete(direct.Snapshot.Identity) {
		return fmt.Errorf("missing primary status identity field evidence")
	}
	if direct.Snapshot.CurrentLSN == nil {
		return fmt.Errorf("missing primary status current_lsn field evidence")
	}
	if !standbyRetentionStatusJSONComplete(direct.Snapshot.Retention) {
		return fmt.Errorf("missing primary status retention field evidence")
	}
	if direct.Snapshot.Slots == nil {
		return fmt.Errorf("missing primary status slots field evidence")
	}
	for i, slot := range *direct.Snapshot.Slots {
		if !standbySlotStatusJSONComplete(slot) {
			return fmt.Errorf("missing primary status slot field evidence at index %d", i)
		}
	}
	if direct.Snapshot.Durability != nil && !standbyDurabilityStatusJSONComplete(*direct.Snapshot.Durability) {
		return fmt.Errorf("missing primary status durability field evidence")
	}
	return nil
}

func validateDirectStandbyPrimaryStatusEvidence(raw []byte) error {
	var direct standbyPrimaryStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return nil
	}
	return ValidateStandbyPrimaryStatusResponseEvidence(raw)
}

func standbyRetentionStatusJSONConsistent(currentLSN uint64, retention *standbyRetentionStatusJSON, slotCount int) error {
	primaryLSN := standbyUint64StatusValue(retention.PrimaryLSN)
	oldestRestartLSN := standbyUint64StatusValue(retention.OldestRestartLSN)
	retainedLSNCount := standbyUint64StatusValue(retention.RetainedLSNCount)
	activeSlots := standbyUint64StatusValue(retention.ActiveSlots)
	reseedRecommended := standbyUint64StatusValue(retention.ReseedRecommended)

	if primaryLSN != currentLSN {
		return fmt.Errorf("primary retention snapshot inconsistent: primary_lsn=%d current_lsn=%d", primaryLSN, currentLSN)
	}
	if oldestRestartLSN > primaryLSN {
		return fmt.Errorf("primary retention snapshot inconsistent: oldest_restart_lsn=%d primary_lsn=%d", oldestRestartLSN, primaryLSN)
	}
	if !standbyRetainedLSNCountConsistent(primaryLSN, oldestRestartLSN, retainedLSNCount, slotCount) {
		return fmt.Errorf("primary retention snapshot inconsistent: retained_lsn_count=%d expected=%d", retainedLSNCount, primaryLSN-oldestRestartLSN)
	}
	if activeSlots > uint64(slotCount) {
		return fmt.Errorf("primary retention snapshot inconsistent: active_slots=%d slots=%d", activeSlots, slotCount)
	}
	if reseedRecommended > uint64(slotCount) {
		return fmt.Errorf("primary retention snapshot inconsistent: reseed_recommended=%d slots=%d", reseedRecommended, slotCount)
	}
	return nil
}

func standbySlotStatusJSONComplete(slot standbySlotStatusJSON) bool {
	return validStandbyIdentifier(slot.Name) &&
		slot.TimelineID != nil &&
		standbyUint64StatusValue(slot.TimelineID) > 0 &&
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
		standbySlotStatusJSONValid(slot.Status)
}

func standbySlotStatusJSONConsistent(currentLSN uint64, slot standbySlotStatusJSON) error {
	name := slot.Name
	restartLSN := standbyUint64StatusValue(slot.RestartLSN)
	receivedLSN := standbyUint64StatusValue(slot.ReceivedLSN)
	appliedLSN := standbyUint64StatusValue(slot.AppliedLSN)
	safeReadLSN := standbyUint64StatusValue(slot.SafeReadLSN)
	writeLagLSN := standbyUint64StatusValue(slot.WriteLagLSN)
	applyLagLSN := standbyUint64StatusValue(slot.ApplyLagLSN)
	safeReadLagLSN := standbyUint64StatusValue(slot.SafeReadLagLSN)
	retentionLagLSN := standbyUint64StatusValue(slot.RetentionLagLSN)

	if restartLSN > currentLSN || receivedLSN > currentLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: progress exceeds primary_lsn", name)
	}
	if appliedLSN > receivedLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: applied_lsn=%d received_lsn=%d", name, appliedLSN, receivedLSN)
	}
	if safeReadLSN > appliedLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: safe_read_lsn=%d applied_lsn=%d", name, safeReadLSN, appliedLSN)
	}
	if writeLagLSN != standbySaturatingSub(currentLSN, receivedLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: write_lag_lsn=%d expected=%d", name, writeLagLSN, standbySaturatingSub(currentLSN, receivedLSN))
	}
	if applyLagLSN != standbySaturatingSub(currentLSN, appliedLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: apply_lag_lsn=%d expected=%d", name, applyLagLSN, standbySaturatingSub(currentLSN, appliedLSN))
	}
	if safeReadLagLSN != standbySaturatingSub(currentLSN, safeReadLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: safe_read_lag_lsn=%d expected=%d", name, safeReadLagLSN, standbySaturatingSub(currentLSN, safeReadLSN))
	}
	if retentionLagLSN != standbySaturatingSub(currentLSN, restartLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: retention_lag_lsn=%d expected=%d", name, retentionLagLSN, standbySaturatingSub(currentLSN, restartLSN))
	}
	return nil
}

func standbyDurabilityStatusJSONComplete(durability standbyDurabilityStatusJSON) bool {
	return standbyDurabilityDecisionStatusJSONValid(durability.Status) &&
		standbyDurabilityModeJSONValid(durability.Mode) &&
		standbySelectionJSONValid(durability.Selection) &&
		durability.TargetLSN != nil &&
		durability.ProgressLSN != nil &&
		durability.MissingLSNCount != nil &&
		durability.SatisfiedCount != nil &&
		durability.RequiredCount != nil &&
		durability.CandidateCount != nil
}

func standbyDurabilityStatusJSONConsistent(durability standbyDurabilityStatusJSON) error {
	targetLSN := standbyUint64StatusValue(durability.TargetLSN)
	progressLSN := standbyUint64StatusValue(durability.ProgressLSN)
	missingLSNCount := standbyUint64StatusValue(durability.MissingLSNCount)
	satisfiedCount := standbyUint64StatusValue(durability.SatisfiedCount)
	requiredCount := standbyUint64StatusValue(durability.RequiredCount)
	candidateCount := standbyUint64StatusValue(durability.CandidateCount)

	if progressLSN > targetLSN {
		return fmt.Errorf("durability status inconsistent: progress_lsn=%d target_lsn=%d", progressLSN, targetLSN)
	}
	if missingLSNCount != targetLSN-progressLSN {
		return fmt.Errorf("durability status inconsistent: missing_lsn_count=%d expected=%d", missingLSNCount, targetLSN-progressLSN)
	}
	if satisfiedCount > candidateCount {
		return fmt.Errorf("durability status inconsistent: satisfied_count=%d candidate_count=%d", satisfiedCount, candidateCount)
	}
	if StandbyDurabilityDecisionStatus(strings.TrimSpace(durability.Status)) == StandbyDurabilityStatusSatisfied && satisfiedCount < requiredCount {
		return fmt.Errorf("durability status inconsistent: satisfied_count=%d required_count=%d", satisfiedCount, requiredCount)
	}
	return nil
}

func standbySlotStatusJSONValid(status string) bool {
	switch StandbySlotSnapshotStatus(strings.TrimSpace(status)) {
	case StandbySlotSnapshotStatusHealthy, StandbySlotSnapshotStatusLagging, StandbySlotSnapshotStatusReseedRequired:
		return true
	default:
		return false
	}
}

func standbyDurabilityDecisionStatusJSONValid(status string) bool {
	switch StandbyDurabilityDecisionStatus(strings.TrimSpace(status)) {
	case StandbyDurabilityStatusSatisfied, StandbyDurabilityStatusWouldBlock, StandbyDurabilityStatusFailClosed, StandbyDurabilityStatusDegradedToAsync:
		return true
	default:
		return false
	}
}

func standbyDurabilityModeJSONValid(mode string) bool {
	switch StandbyDurabilityDecisionMode(strings.TrimSpace(mode)) {
	case StandbyDurabilityModeAsync, StandbyDurabilityModeRemoteWrite, StandbyDurabilityModeRemoteApply:
		return true
	default:
		return false
	}
}

func standbySelectionJSONValid(selection string) bool {
	switch StandbyDurabilityDecisionSelection(strings.TrimSpace(selection)) {
	case StandbyDurabilitySelectionAny, StandbyDurabilitySelectionFirst, StandbyDurabilitySelectionAll:
		return true
	default:
		return false
	}
}

func standbyStatusJSONComplete(snapshot *standbyStatusJSON) bool {
	return snapshot != nil &&
		snapshot.ReceivedLSN != nil &&
		snapshot.AppliedLSN != nil &&
		snapshot.SafeReadLSN != nil &&
		snapshot.UnappliedLSNCount != nil &&
		snapshot.CaughtUpToReceived != nil &&
		snapshot.CanServeSafeReads != nil
}

func ValidateStandbyStatusResponseEvidence(raw []byte) error {
	var direct standbyStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return fmt.Errorf("missing standby status snapshot field evidence")
	}
	if !standbyAdminIdentityJSONComplete(direct.Snapshot.Identity) {
		return fmt.Errorf("missing standby status identity field evidence")
	}
	if !standbyStatusJSONComplete(direct.Snapshot) {
		return fmt.Errorf("missing standby status progress field evidence")
	}
	return nil
}

func validateDirectStandbyStatusEvidence(raw []byte) error {
	var direct standbyStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return nil
	}
	return ValidateStandbyStatusResponseEvidence(raw)
}

func standbyStatusJSONConsistent(snapshot *standbyStatusJSON) error {
	receivedLSN := standbyUint64StatusValue(snapshot.ReceivedLSN)
	appliedLSN := standbyUint64StatusValue(snapshot.AppliedLSN)
	safeReadLSN := standbyUint64StatusValue(snapshot.SafeReadLSN)
	unappliedLSNCount := standbyUint64StatusValue(snapshot.UnappliedLSNCount)
	caughtUpToReceived := standbyBoolStatusValue(snapshot.CaughtUpToReceived)
	canServeSafeReads := standbyBoolStatusValue(snapshot.CanServeSafeReads)

	if appliedLSN > receivedLSN {
		return fmt.Errorf("standby status inconsistent: applied_lsn=%d received_lsn=%d", appliedLSN, receivedLSN)
	}
	if safeReadLSN > appliedLSN {
		return fmt.Errorf("standby status inconsistent: safe_read_lsn=%d applied_lsn=%d", safeReadLSN, appliedLSN)
	}
	if unappliedLSNCount != receivedLSN-appliedLSN {
		return fmt.Errorf("standby status inconsistent: unapplied_lsn_count=%d expected=%d", unappliedLSNCount, receivedLSN-appliedLSN)
	}
	if caughtUpToReceived != (appliedLSN >= receivedLSN) {
		return fmt.Errorf("standby status inconsistent: caught_up_to_received=%t expected=%t", caughtUpToReceived, appliedLSN >= receivedLSN)
	}
	if canServeSafeReads != (safeReadLSN <= appliedLSN) {
		return fmt.Errorf("standby status inconsistent: can_serve_safe_reads=%t expected=%t", canServeSafeReads, safeReadLSN <= appliedLSN)
	}
	if snapshot.UpstreamLSN != nil {
		upstreamLSN := standbyUint64StatusValue(snapshot.UpstreamLSN)
		if snapshot.WriteLagLSN != nil && standbyUint64StatusValue(snapshot.WriteLagLSN) != standbySaturatingSub(upstreamLSN, receivedLSN) {
			return fmt.Errorf("standby status inconsistent: write_lag_lsn=%d expected=%d", standbyUint64StatusValue(snapshot.WriteLagLSN), standbySaturatingSub(upstreamLSN, receivedLSN))
		}
		if snapshot.ReceiveLagLSN != nil && standbyUint64StatusValue(snapshot.ReceiveLagLSN) != standbySaturatingSub(upstreamLSN, receivedLSN) {
			return fmt.Errorf("standby status inconsistent: receive_lag_lsn=%d expected=%d", standbyUint64StatusValue(snapshot.ReceiveLagLSN), standbySaturatingSub(upstreamLSN, receivedLSN))
		}
		if snapshot.ApplyLagLSN != nil && standbyUint64StatusValue(snapshot.ApplyLagLSN) != standbySaturatingSub(upstreamLSN, appliedLSN) {
			return fmt.Errorf("standby status inconsistent: apply_lag_lsn=%d expected=%d", standbyUint64StatusValue(snapshot.ApplyLagLSN), standbySaturatingSub(upstreamLSN, appliedLSN))
		}
	}
	return nil
}

func standbyUint64StatusValue(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func standbyBoolStatusValue(value *bool) bool {
	return value != nil && *value
}

func standbyStringStatusValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func standbySaturatingSub(a, b uint64) uint64 {
	if b >= a {
		return 0
	}
	return a - b
}
