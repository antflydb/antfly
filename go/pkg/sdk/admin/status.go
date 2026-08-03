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
	NodeID     string                  `json:"node_id"`
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
	RetainedByteCount *uint64 `json:"retained_byte_count"`
	RetainedAgeNS     *uint64 `json:"retained_age_ns"`
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
	Role                     string              `json:"role"`
	NodeID                   string              `json:"node_id"`
	Identity                 haAdminIdentityJSON `json:"identity"`
	ReceivedLSN              *uint64             `json:"received_lsn"`
	AppliedLSN               *uint64             `json:"applied_lsn"`
	SafeReadLSN              *uint64             `json:"safe_read_lsn"`
	UpstreamLSN              *uint64             `json:"upstream_lsn"`
	WriteLagLSN              *uint64             `json:"write_lag_lsn"`
	ReceiveLagLSN            *uint64             `json:"receive_lag_lsn"`
	ApplyLagLSN              *uint64             `json:"apply_lag_lsn"`
	LastError                *string             `json:"last_error"`
	LastAttemptNs            *uint64             `json:"last_attempt_ns"`
	LastSuccessNs            *uint64             `json:"last_success_ns"`
	ReplicationFailuresTotal *uint64             `json:"replication_failures_total"`
	UnappliedLSNCount        *uint64             `json:"unapplied_lsn_count"`
	CaughtUpToReceived       *bool               `json:"caught_up_to_received"`
	CanServeSafeReads        *bool               `json:"can_serve_safe_reads"`
}

type ParsedHAPrimaryStatus struct {
	Response      HAPrimaryStatusResponse
	HasDurability bool
}

type ParsedHAStandbyStatus = HAStandbyStatusResponse

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
	nodeID := snapshot.NodeID
	if !validHAIdentifier(nodeID) {
		return nil, fmt.Errorf("invalid primary status node_id %q", snapshot.NodeID)
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
	if err := haRetentionStatusJSONConsistent(*snapshot.CurrentLSN, snapshot.Retention, len(*snapshot.Slots)); err != nil {
		return nil, err
	}
	parsed := &ParsedHAPrimaryStatus{
		HasDurability: snapshot.Durability != nil,
		Response: HAPrimaryStatusResponse{
			SchemaVersion: schemaVersion,
			Snapshot: HAPrimarySnapshot{
				CurrentLsn: *snapshot.CurrentLSN,
				Identity:   haIdentityFromStatusJSON(snapshot.Identity),
				NodeId:     nodeID,
				Retention: HARetentionSnapshot{
					PrimaryLsn:        haUint64StatusValue(snapshot.Retention.PrimaryLSN),
					OldestRestartLsn:  haUint64StatusValue(snapshot.Retention.OldestRestartLSN),
					RetainedLsnCount:  haUint64StatusValue(snapshot.Retention.RetainedLSNCount),
					RetainedByteCount: haUint64StatusValue(snapshot.Retention.RetainedByteCount),
					RetainedAgeNs:     haUint64StatusValue(snapshot.Retention.RetainedAgeNS),
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
		if err := haSlotStatusJSONConsistent(*snapshot.CurrentLSN, slot); err != nil {
			return nil, err
		}
		lastError := ""
		if slot.LastError != nil {
			lastError = strings.TrimSpace(*slot.LastError)
		}
		parsed.Response.Snapshot.Slots = append(parsed.Response.Snapshot.Slots, HASlotSnapshot{
			Name:            slot.Name,
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
		if err := haDurabilityStatusJSONConsistent(*snapshot.Durability); err != nil {
			return nil, err
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
	if err := ValidateHAPrimaryStatusResponse(parsed.Response); err != nil {
		return nil, err
	}
	return parsed, nil
}

// ParseHAStandbyStatus validates a standby status body and returns the
// generated OpenAPI response model. It accepts the current /admin/v1 shape and
// the older CLI compatibility envelope used by existing operator tests.
func ParseHAStandbyStatus(raw []byte) (*ParsedHAStandbyStatus, error) {
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
	nodeID := snapshot.NodeID
	if !validHAIdentifier(nodeID) {
		return nil, fmt.Errorf("invalid standby status node_id %q", snapshot.NodeID)
	}
	if !haAdminIdentityJSONComplete(snapshot.Identity) {
		return nil, fmt.Errorf("missing standby status identity")
	}
	if !haStandbyStatusJSONComplete(snapshot) {
		return nil, fmt.Errorf("missing standby status fields")
	}
	if err := haStandbyStatusJSONConsistent(snapshot); err != nil {
		return nil, err
	}
	response := &HAStandbyStatusResponse{
		SchemaVersion: schemaVersion,
		Snapshot: HAStandbySnapshot{
			Role:                     HAStandbySnapshotRoleStandby,
			NodeId:                   nodeID,
			Identity:                 haIdentityFromStatusJSON(snapshot.Identity),
			ReceivedLsn:              haUint64StatusValue(snapshot.ReceivedLSN),
			AppliedLsn:               haUint64StatusValue(snapshot.AppliedLSN),
			SafeReadLsn:              haUint64StatusValue(snapshot.SafeReadLSN),
			UpstreamLsn:              haUint64StatusValue(snapshot.UpstreamLSN),
			WriteLagLsn:              haUint64StatusValue(snapshot.WriteLagLSN),
			ReceiveLagLsn:            haUint64StatusValue(snapshot.ReceiveLagLSN),
			ApplyLagLsn:              haUint64StatusValue(snapshot.ApplyLagLSN),
			LastError:                haStringStatusValue(snapshot.LastError),
			LastAttemptNs:            haUint64StatusValue(snapshot.LastAttemptNs),
			LastSuccessNs:            haUint64StatusValue(snapshot.LastSuccessNs),
			ReplicationFailuresTotal: haUint64StatusValue(snapshot.ReplicationFailuresTotal),
			UnappliedLsnCount:        haUint64StatusValue(snapshot.UnappliedLSNCount),
			CaughtUpToReceived:       haBoolStatusValue(snapshot.CaughtUpToReceived),
			CanServeSafeReads:        haBoolStatusValue(snapshot.CanServeSafeReads),
		},
	}
	if err := ValidateHAStandbyStatusResponse(*response); err != nil {
		return nil, err
	}
	return response, nil
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
		retention.RetainedByteCount != nil &&
		retention.RetainedAgeNS != nil &&
		retention.ActiveSlots != nil &&
		retention.ReseedRecommended != nil
}

func ValidateHAPrimaryStatusResponseEvidence(raw []byte) error {
	var direct haPrimaryStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return fmt.Errorf("missing primary status snapshot field evidence")
	}
	if !haAdminIdentityJSONComplete(direct.Snapshot.Identity) {
		return fmt.Errorf("missing primary status identity field evidence")
	}
	if direct.Snapshot.CurrentLSN == nil {
		return fmt.Errorf("missing primary status current_lsn field evidence")
	}
	if !haRetentionStatusJSONComplete(direct.Snapshot.Retention) {
		return fmt.Errorf("missing primary status retention field evidence")
	}
	if direct.Snapshot.Slots == nil {
		return fmt.Errorf("missing primary status slots field evidence")
	}
	for i, slot := range *direct.Snapshot.Slots {
		if !haSlotStatusJSONComplete(slot) {
			return fmt.Errorf("missing primary status slot field evidence at index %d", i)
		}
	}
	if direct.Snapshot.Durability != nil && !haDurabilityStatusJSONComplete(*direct.Snapshot.Durability) {
		return fmt.Errorf("missing primary status durability field evidence")
	}
	return nil
}

func validateDirectHAPrimaryStatusEvidence(raw []byte) error {
	var direct haPrimaryStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return nil
	}
	return ValidateHAPrimaryStatusResponseEvidence(raw)
}

func haRetentionStatusJSONConsistent(currentLSN uint64, retention *haRetentionStatusJSON, slotCount int) error {
	primaryLSN := haUint64StatusValue(retention.PrimaryLSN)
	oldestRestartLSN := haUint64StatusValue(retention.OldestRestartLSN)
	retainedLSNCount := haUint64StatusValue(retention.RetainedLSNCount)
	activeSlots := haUint64StatusValue(retention.ActiveSlots)
	reseedRecommended := haUint64StatusValue(retention.ReseedRecommended)

	if primaryLSN != currentLSN {
		return fmt.Errorf("primary retention snapshot inconsistent: primary_lsn=%d current_lsn=%d", primaryLSN, currentLSN)
	}
	if oldestRestartLSN > primaryLSN {
		return fmt.Errorf("primary retention snapshot inconsistent: oldest_restart_lsn=%d primary_lsn=%d", oldestRestartLSN, primaryLSN)
	}
	if !haRetainedLSNCountConsistent(primaryLSN, oldestRestartLSN, retainedLSNCount, slotCount) {
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

func haSlotStatusJSONComplete(slot haSlotStatusJSON) bool {
	return validHAIdentifier(slot.Name) &&
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

func haSlotStatusJSONConsistent(currentLSN uint64, slot haSlotStatusJSON) error {
	name := slot.Name
	restartLSN := haUint64StatusValue(slot.RestartLSN)
	receivedLSN := haUint64StatusValue(slot.ReceivedLSN)
	appliedLSN := haUint64StatusValue(slot.AppliedLSN)
	safeReadLSN := haUint64StatusValue(slot.SafeReadLSN)
	writeLagLSN := haUint64StatusValue(slot.WriteLagLSN)
	applyLagLSN := haUint64StatusValue(slot.ApplyLagLSN)
	safeReadLagLSN := haUint64StatusValue(slot.SafeReadLagLSN)
	retentionLagLSN := haUint64StatusValue(slot.RetentionLagLSN)

	if restartLSN > currentLSN || receivedLSN > currentLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: progress exceeds primary_lsn", name)
	}
	if appliedLSN > receivedLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: applied_lsn=%d received_lsn=%d", name, appliedLSN, receivedLSN)
	}
	if safeReadLSN > appliedLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: safe_read_lsn=%d applied_lsn=%d", name, safeReadLSN, appliedLSN)
	}
	if writeLagLSN != haSaturatingSub(currentLSN, receivedLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: write_lag_lsn=%d expected=%d", name, writeLagLSN, haSaturatingSub(currentLSN, receivedLSN))
	}
	if applyLagLSN != haSaturatingSub(currentLSN, appliedLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: apply_lag_lsn=%d expected=%d", name, applyLagLSN, haSaturatingSub(currentLSN, appliedLSN))
	}
	if safeReadLagLSN != haSaturatingSub(currentLSN, safeReadLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: safe_read_lag_lsn=%d expected=%d", name, safeReadLagLSN, haSaturatingSub(currentLSN, safeReadLSN))
	}
	if retentionLagLSN != haSaturatingSub(currentLSN, restartLSN) {
		return fmt.Errorf("slot %s snapshot inconsistent: retention_lag_lsn=%d expected=%d", name, retentionLagLSN, haSaturatingSub(currentLSN, restartLSN))
	}
	return nil
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

func haDurabilityStatusJSONConsistent(durability haDurabilityStatusJSON) error {
	targetLSN := haUint64StatusValue(durability.TargetLSN)
	progressLSN := haUint64StatusValue(durability.ProgressLSN)
	missingLSNCount := haUint64StatusValue(durability.MissingLSNCount)
	satisfiedCount := haUint64StatusValue(durability.SatisfiedCount)
	requiredCount := haUint64StatusValue(durability.RequiredCount)
	candidateCount := haUint64StatusValue(durability.CandidateCount)

	if progressLSN > targetLSN {
		return fmt.Errorf("durability status inconsistent: progress_lsn=%d target_lsn=%d", progressLSN, targetLSN)
	}
	if missingLSNCount != targetLSN-progressLSN {
		return fmt.Errorf("durability status inconsistent: missing_lsn_count=%d expected=%d", missingLSNCount, targetLSN-progressLSN)
	}
	if satisfiedCount > candidateCount {
		return fmt.Errorf("durability status inconsistent: satisfied_count=%d candidate_count=%d", satisfiedCount, candidateCount)
	}
	if HADurabilityDecisionStatus(strings.TrimSpace(durability.Status)) == HADurabilityStatusSatisfied && satisfiedCount < requiredCount {
		return fmt.Errorf("durability status inconsistent: satisfied_count=%d required_count=%d", satisfiedCount, requiredCount)
	}
	return nil
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

func ValidateHAStandbyStatusResponseEvidence(raw []byte) error {
	var direct haStandbyStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return fmt.Errorf("missing standby status snapshot field evidence")
	}
	if !haAdminIdentityJSONComplete(direct.Snapshot.Identity) {
		return fmt.Errorf("missing standby status identity field evidence")
	}
	if !haStandbyStatusJSONComplete(direct.Snapshot) {
		return fmt.Errorf("missing standby status progress field evidence")
	}
	return nil
}

func validateDirectHAStandbyStatusEvidence(raw []byte) error {
	var direct haStandbyStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return err
	}
	if direct.Snapshot == nil {
		return nil
	}
	return ValidateHAStandbyStatusResponseEvidence(raw)
}

func haStandbyStatusJSONConsistent(snapshot *haStandbyStatusJSON) error {
	receivedLSN := haUint64StatusValue(snapshot.ReceivedLSN)
	appliedLSN := haUint64StatusValue(snapshot.AppliedLSN)
	safeReadLSN := haUint64StatusValue(snapshot.SafeReadLSN)
	unappliedLSNCount := haUint64StatusValue(snapshot.UnappliedLSNCount)
	caughtUpToReceived := haBoolStatusValue(snapshot.CaughtUpToReceived)
	canServeSafeReads := haBoolStatusValue(snapshot.CanServeSafeReads)

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
		upstreamLSN := haUint64StatusValue(snapshot.UpstreamLSN)
		if snapshot.WriteLagLSN != nil && haUint64StatusValue(snapshot.WriteLagLSN) != haSaturatingSub(upstreamLSN, receivedLSN) {
			return fmt.Errorf("standby status inconsistent: write_lag_lsn=%d expected=%d", haUint64StatusValue(snapshot.WriteLagLSN), haSaturatingSub(upstreamLSN, receivedLSN))
		}
		if snapshot.ReceiveLagLSN != nil && haUint64StatusValue(snapshot.ReceiveLagLSN) != haSaturatingSub(upstreamLSN, receivedLSN) {
			return fmt.Errorf("standby status inconsistent: receive_lag_lsn=%d expected=%d", haUint64StatusValue(snapshot.ReceiveLagLSN), haSaturatingSub(upstreamLSN, receivedLSN))
		}
		if snapshot.ApplyLagLSN != nil && haUint64StatusValue(snapshot.ApplyLagLSN) != haSaturatingSub(upstreamLSN, appliedLSN) {
			return fmt.Errorf("standby status inconsistent: apply_lag_lsn=%d expected=%d", haUint64StatusValue(snapshot.ApplyLagLSN), haSaturatingSub(upstreamLSN, appliedLSN))
		}
	}
	return nil
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

func haStringStatusValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func haSaturatingSub(a, b uint64) uint64 {
	if b >= a {
		return 0
	}
	return a - b
}
