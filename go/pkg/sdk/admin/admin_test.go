package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInternalClientGetMetadataStatusSendsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != "/_internal/v1/status" {
			t.Fatalf("path = %s, want /_internal/v1/status", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		_, _ = fmt.Fprint(w, `{"raft_status":{"leader_id":1,"voters":{"1":"raft://node-1"}}}`)
	}))
	defer server.Close()

	status, err := NewInternalClient(server.URL, server.Client()).WithToken("test-token").GetMetadataStatus()
	if err != nil {
		t.Fatalf("GetMetadataStatus returned error: %v", err)
	}
	if status.Leader != 1 {
		t.Fatalf("Leader = %d, want 1", status.Leader)
	}
	if got := status.Members[1]; got != "raft://node-1" {
		t.Fatalf("Members[1] = %q, want raft://node-1", got)
	}
}

func TestInternalClientAddMetadataPeerSendsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/_internal/v1/peer/2" {
			t.Fatalf("path = %s, want /_internal/v1/peer/2", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/octet-stream" {
			t.Fatalf("Content-Type = %q, want application/octet-stream", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll returned error: %v", err)
		}
		if got := string(body); got != "raft://node-2" {
			t.Fatalf("body = %q, want raft://node-2", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := NewInternalClient(server.URL, server.Client()).WithToken("test-token").AddMetadataPeer(2, "raft://node-2"); err != nil {
		t.Fatalf("AddMetadataPeer returned error: %v", err)
	}
}

func TestInternalClientRemoveMetadataPeerSendsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodDelete)
		}
		if r.URL.Path != "/_internal/v1/peer/2" {
			t.Fatalf("path = %s, want /_internal/v1/peer/2", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := NewInternalClient(server.URL, server.Client()).WithToken("test-token").RemoveMetadataPeer(2); err != nil {
		t.Fatalf("RemoveMetadataPeer returned error: %v", err)
	}
}

func TestHAClientCreateReplicationSlotUsesAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/admin/v1/ha/replication-slots" {
			t.Fatalf("path = %s, want /admin/v1/ha/replication-slots", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q, want application/json", got)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll returned error: %v", err)
		}
		if got := string(body); !strings.Contains(got, `"slot_name":"standby-a"`) || !strings.Contains(got, `"initial_lsn":7`) {
			t.Fatalf("body = %s, want slot_name and initial_lsn", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"schema_version":1,
			"slot_action":"create",
			"action":{
				"action_id":"replication-slot-create:standby-a",
				"action_kind":"replication_slot_create",
				"target":"standby-a",
				"state":"applied",
				"node_id":"primary-a"
			},
			"slot":{
				"slot_name":"standby-a",
				"timeline_id":1,
				"restart_lsn":7,
				"received_lsn":7,
				"applied_lsn":7,
				"safe_read_lsn":7,
				"active":true,
				"reseed_required":false,
				"current_lsn":7
			}
		}`)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").CreateReplicationSlot(context.Background(), ReplicationSlotCreateRequest{
		SlotName:   "standby-a",
		InitialLsn: 7,
	})
	if err != nil {
		t.Fatalf("CreateReplicationSlot returned error: %v", err)
	}
	if resp.Slot.SlotName != "standby-a" {
		t.Fatalf("SlotName = %q, want standby-a", resp.Slot.SlotName)
	}
	if resp.Action.NodeId != "primary-a" {
		t.Fatalf("Action.NodeId = %q, want primary-a", resp.Action.NodeId)
	}
}

func TestHAClientGateOperationsUseAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q, want application/json", got)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll returned error: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/v1/ha/commit/append":
			if got := string(body); !strings.Contains(got, `"kind":"batch_mutation"`) ||
				!strings.Contains(got, `"payload_codec":"json"`) ||
				!strings.Contains(got, `"mode":"remote_write"`) {
				t.Fatalf("commit append body = %s, want kind, payload_codec, and sync policy", got)
			}
			_, _ = fmt.Fprint(w, haCommitAppendResponseJSON())
		case "/admin/v1/ha/commit/check":
			if got := string(body); !strings.Contains(got, `"target_lsn":9`) ||
				!strings.Contains(got, `"failure_policy":"fail_closed"`) {
				t.Fatalf("commit check body = %s, want target_lsn and sync policy", got)
			}
			_, _ = fmt.Fprint(w, haCommitCheckResponseJSON())
		case "/admin/v1/ha/read/check":
			if got := string(body); !strings.Contains(got, `"consistency":"at_least_lsn"`) {
				t.Fatalf("read check body = %s, want consistency", got)
			}
			_, _ = fmt.Fprint(w, `{
				"schema_version":1,
				"decision":{
					"action":"serve_standby",
					"applied_lsn":9,
					"consistency":"at_least_lsn",
					"metadata_missing_lsn_count":0,
					"missing_lsn_count":0,
					"received_lsn":9,
					"safe_read_lsn":9
				}
			}`)
		case "/admin/v1/ha/write/check":
			if got := string(body); !strings.Contains(got, `"role":"standby"`) {
				t.Fatalf("write check body = %s, want standby role", got)
			}
			_, _ = fmt.Fprint(w, haWriteDecisionResponseJSON())
		case "/admin/v1/ha/owner-jobs/check":
			if got := string(body); !strings.Contains(got, `"kind":"compaction_publish"`) ||
				!strings.Contains(got, `"role":"primary"`) {
				t.Fatalf("owner job check body = %s, want kind and primary role", got)
			}
			_, _ = fmt.Fprint(w, haOwnerJobDecisionResponseJSON())
		default:
			t.Fatalf("path = %s, want HA gate endpoint", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	client.WithToken("test-token")
	syncPolicy := HASyncPolicy{
		Mode:          HASyncPolicyModeRemoteWrite,
		Selection:     HASyncPolicySelectionAny,
		Required:      1,
		FailurePolicy: HASyncPolicyFailureFailClosed,
		StandbyNames:  []string{"standby-a"},
	}

	appendResp, err := client.AppendCommit(context.Background(), CommitAppendRequest{
		Kind:         CommitAppendKindBatchMutation,
		Payload:      `{"op":"put"}`,
		PayloadCodec: CommitAppendCodecJSON,
		SyncPolicy:   syncPolicy,
		TableId:      3,
		ShardId:      4,
	})
	if err != nil {
		t.Fatalf("AppendCommit returned error: %v", err)
	}
	if appendResp.Lsn != 9 || appendResp.Gate.Action != "acknowledge" {
		t.Fatalf("AppendCommit response = %+v, want lsn 9 acknowledged", appendResp)
	}

	commitResp, err := client.CheckCommit(context.Background(), CommitCheckRequest{
		TargetLsn:  9,
		SyncPolicy: syncPolicy,
	})
	if err != nil {
		t.Fatalf("CheckCommit returned error: %v", err)
	}
	if commitResp.Gate.Durability.Status != "satisfied" {
		t.Fatalf("CheckCommit durability status = %s, want satisfied", commitResp.Gate.Durability.Status)
	}

	readResp, err := client.CheckRead(context.Background(), ReadCheckRequest{
		Consistency: ReadCheckConsistencyAtLeastLSN,
		RequiredLsn: 9,
	})
	if err != nil {
		t.Fatalf("CheckRead returned error: %v", err)
	}
	if readResp.Decision.Action != "serve_standby" {
		t.Fatalf("CheckRead action = %s, want serve_standby", readResp.Decision.Action)
	}

	writeResp, err := client.CheckWrite(context.Background(), WriteCheckRequest{Role: WriteCheckRoleStandby})
	if err != nil {
		t.Fatalf("CheckWrite returned error: %v", err)
	}
	if writeResp.Decision.Action != "reject_read_only_standby" {
		t.Fatalf("CheckWrite action = %s, want reject_read_only_standby", writeResp.Decision.Action)
	}

	ownerJobResp, err := client.CheckOwnerJob(context.Background(), OwnerJobCheckRequest{
		Kind: OwnerJobCheckKindCompactionPublish,
		Role: OwnerJobCheckRolePrimary,
	})
	if err != nil {
		t.Fatalf("CheckOwnerJob returned error: %v", err)
	}
	if ownerJobResp.Decision.Action != "run" {
		t.Fatalf("CheckOwnerJob action = %s, want run", ownerJobResp.Decision.Action)
	}
}

func TestHAClientAcceptsAdminRootURL(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":false}`)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL+"/admin/v1", server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	resp, err := client.CurrentFence(context.Background())
	if err != nil {
		t.Fatalf("CurrentFence returned error: %v", err)
	}
	if resp.Held {
		t.Fatalf("Held = true, want false")
	}
}

func TestHAOperationMetadataUsesAdminAPIPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  HAOperation
		want HAOperation
	}{
		{
			name: "create replication slot",
			got:  HACreateReplicationSlotOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/replication-slots"},
		},
		{
			name: "begin base backup",
			got:  HABeginBaseBackupOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/base-backups"},
		},
		{
			name: "finish base backup",
			got:  HAFinishBaseBackupOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/base-backups/finish"},
		},
		{
			name: "bootstrap standby",
			got:  HABootstrapStandbyOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/standby/bootstrap"},
		},
		{
			name: "acquire fence",
			got:  HAAcquireFenceOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/fence"},
		},
		{
			name: "assess promotion",
			got:  HAAssessPromotionOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/promotion/assess"},
		},
		{
			name: "promote with current fence",
			got:  HAPromoteWithCurrentFenceOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/promotion/current-fence"},
		},
		{
			name: "assess rejoin",
			got:  HAAssessRejoinOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/rejoin/assess"},
		},
		{
			name: "rewind rejoin",
			got:  HARewindRejoinOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/rejoin/rewind"},
		},
		{
			name: "reseed rejoin",
			got:  HAReseedRejoinOperation(),
			want: HAOperation{Method: http.MethodPost, Path: "/admin/v1/ha/rejoin/reseed"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.got != tt.want {
				t.Fatalf("operation = %#v, want %#v", tt.got, tt.want)
			}
		})
	}

	slotPath, ok := HAReplicationSlotPath("standby a/%")
	if !ok {
		t.Fatal("HAReplicationSlotPath returned ok=false for non-empty slot")
	}
	if slotPath != "/admin/v1/ha/replication-slots/standby%20a%2F%25" {
		t.Fatalf("slot path = %q", slotPath)
	}
	if _, ok := HAReplicationSlotPath(" "); ok {
		t.Fatal("HAReplicationSlotPath returned ok=true for empty slot")
	}
	resume, ok := HAResumeReplicationSlotOperation("standby a/%")
	if !ok {
		t.Fatal("HAResumeReplicationSlotOperation returned ok=false")
	}
	if want := (HAOperation{Method: http.MethodPut, Path: "/admin/v1/ha/replication-slots/standby%20a%2F%25/resume"}); resume != want {
		t.Fatalf("resume operation = %#v, want %#v", resume, want)
	}
	pause, ok := HAPauseReplicationSlotOperation("standby a/%")
	if !ok {
		t.Fatal("HAPauseReplicationSlotOperation returned ok=false")
	}
	if want := (HAOperation{Method: http.MethodPut, Path: "/admin/v1/ha/replication-slots/standby%20a%2F%25/pause"}); pause != want {
		t.Fatalf("pause operation = %#v, want %#v", pause, want)
	}
	drop, ok := HADropReplicationSlotOperation("standby a/%")
	if !ok {
		t.Fatal("HADropReplicationSlotOperation returned ok=false")
	}
	if want := (HAOperation{Method: http.MethodDelete, Path: "/admin/v1/ha/replication-slots/standby%20a%2F%25"}); drop != want {
		t.Fatalf("drop operation = %#v, want %#v", drop, want)
	}
}

func TestHAReceiptExpectationsUseAdminAPIEnums(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		got       HAReceiptExpectation
		wantKind  string
		wantState string
	}{
		{
			name:      "create replication slot",
			got:       HAReplicationSlotCreateReceiptExpectation(),
			wantKind:  "replication_slot_create",
			wantState: "applied",
		},
		{
			name:      "resume replication slot",
			got:       HAReplicationSlotResumeReceiptExpectation(),
			wantKind:  "replication_slot_resume",
			wantState: "applied",
		},
		{
			name:      "pause replication slot",
			got:       HAReplicationSlotPauseReceiptExpectation(),
			wantKind:  "replication_slot_pause",
			wantState: "applied",
		},
		{
			name:      "drop replication slot",
			got:       HAReplicationSlotDropReceiptExpectation(),
			wantKind:  "replication_slot_drop",
			wantState: "applied",
		},
		{
			name:      "begin base backup",
			got:       HABaseBackupBeginReceiptExpectation(),
			wantKind:  "base_backup_begin",
			wantState: "applied",
		},
		{
			name:      "finish base backup",
			got:       HABaseBackupFinishReceiptExpectation(),
			wantKind:  "base_backup_finish",
			wantState: "applied",
		},
		{
			name:      "bootstrap standby",
			got:       HAStandbyBootstrapReceiptExpectation(),
			wantKind:  "standby_bootstrap",
			wantState: "applied",
		},
		{
			name:      "acquire fence",
			got:       HAFenceAcquireReceiptExpectation(),
			wantKind:  "fence_acquire",
			wantState: "applied",
		},
		{
			name:      "assess promotion",
			got:       HAPromotionAssessReceiptExpectation(),
			wantKind:  "promotion_assess",
			wantState: "assessed",
		},
		{
			name:      "promote",
			got:       HAPromotionReceiptExpectation(),
			wantKind:  "promotion",
			wantState: "applied",
		},
		{
			name:      "assess rejoin",
			got:       HARejoinAssessReceiptExpectation(),
			wantKind:  "rejoin_assess",
			wantState: "assessed",
		},
		{
			name:      "rewind rejoin",
			got:       HARejoinRewindReceiptExpectation(),
			wantKind:  "rejoin_rewind",
			wantState: "applied",
		},
		{
			name:      "reseed rejoin",
			got:       HARejoinReseedReceiptExpectation(),
			wantKind:  "rejoin_reseed",
			wantState: "applied",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotKind, gotState := tt.got.Strings()
			if gotKind != tt.wantKind || gotState != tt.wantState {
				t.Fatalf("receipt expectation = (%q, %q), want (%q, %q)", gotKind, gotState, tt.wantKind, tt.wantState)
			}
		})
	}
}

func TestHAReceiptMatchesExpectedOperationAndTarget(t *testing.T) {
	t.Parallel()

	expectation := HAReplicationSlotCreateReceiptExpectation()
	receipt := HAActionReceipt{
		ActionId:   "replication_slot_create:standby-a",
		ActionKind: HAActionKindReplicationSlotCreate,
		Target:     "standby-a",
		State:      HAActionStateApplied,
		NodeId:     "primary-a",
	}
	if !HAReceiptMatches(receipt, expectation, "standby-a") {
		t.Fatalf("HAReceiptMatches returned false for exact matching receipt")
	}
	receipt.State = HAActionStateAlreadyApplied
	if !HAReceiptMatches(receipt, expectation, "standby-a") {
		t.Fatalf("HAReceiptMatches returned false for already-applied idempotent receipt")
	}
	receipt.State = HAActionStateApplied
	receipt.Target = "standby-b"
	if HAReceiptMatches(receipt, expectation, "standby-a") {
		t.Fatalf("HAReceiptMatches returned true for mismatched target")
	}
	receipt.Target = "standby-a"
	if HAReceiptMatches(receipt, expectation, "") {
		t.Fatalf("HAReceiptMatches returned true with empty expected target")
	}
}

func TestHAReceiptMatchesNode(t *testing.T) {
	t.Parallel()

	expectation := HAReplicationSlotResumeReceiptExpectation()
	receipt := HAActionReceipt{
		ActionId:   "replication_slot_resume:standby-a",
		ActionKind: HAActionKindReplicationSlotResume,
		Target:     "standby-a",
		State:      HAActionStateApplied,
		NodeId:     "primary-a",
	}
	if !HAReceiptMatchesNode(receipt, expectation, "standby-a", "primary-a", true) {
		t.Fatalf("HAReceiptMatchesNode returned false for exact matching node")
	}
	if HAReceiptMatchesNode(receipt, expectation, "standby-a", "primary-b", true) {
		t.Fatalf("HAReceiptMatchesNode returned true for mismatched node")
	}
	if HAReceiptMatchesNode(receipt, expectation, "standby-a", "", true) {
		t.Fatalf("HAReceiptMatchesNode returned true without required expected node")
	}
	if !HAReceiptMatchesNode(receipt, expectation, "standby-a", "", false) {
		t.Fatalf("HAReceiptMatchesNode returned false for optional expected node")
	}
	receipt.NodeId = ""
	if HAReceiptMatchesNode(receipt, expectation, "standby-a", "", false) {
		t.Fatalf("HAReceiptMatchesNode returned true without receipt node id")
	}
}

func TestValidateHAReplicationSlotActionResponse(t *testing.T) {
	t.Parallel()

	response := HAReplicationSlotActionResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "replication_slot_create:standby-a",
			ActionKind: HAActionKindReplicationSlotCreate,
			Target:     "standby-a",
			State:      HAActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotAction: HAReplicationSlotActionCreate,
		Slot: HAReplicationSlot{
			SlotName:       "standby-a",
			TimelineId:     1,
			RestartLsn:     7,
			ReceivedLsn:    7,
			AppliedLsn:     7,
			SafeReadLsn:    7,
			CurrentLsn:     7,
			Active:         true,
			ReseedRequired: false,
		},
	}
	if err := ValidateHAReplicationSlotActionResponse(response); err != nil {
		t.Fatalf("ValidateHAReplicationSlotActionResponse returned error: %v", err)
	}

	response.Action.NodeId = ""
	if err := ValidateHAReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("missing node id error = %v, want receipt error", err)
	}
	response.Action.NodeId = "primary-a"

	response.SlotAction = HAReplicationSlotAction("invalid")
	if err := ValidateHAReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "invalid replication slot action") {
		t.Fatalf("invalid slot action error = %v, want invalid action error", err)
	}
	response.SlotAction = HAReplicationSlotActionCreate

	response.Slot.TimelineId = 0
	if err := ValidateHAReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("missing slot fields error = %v, want slot fields error", err)
	}
}

func TestValidateHASeedActionResponses(t *testing.T) {
	t.Parallel()

	begin := HABaseBackupBeginResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "base_backup_begin:manifest-a",
			ActionKind: HAActionKindBaseBackupBegin,
			Target:     "manifest-a",
			State:      HAActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotName:       "standby-a",
		ManifestId:     "manifest-a",
		BackupLsn:      7,
		StartRecordLsn: 8,
	}
	if err := ValidateHABaseBackupBeginResponse(begin); err != nil {
		t.Fatalf("ValidateHABaseBackupBeginResponse returned error: %v", err)
	}
	begin.StartRecordLsn = 0
	if err := ValidateHABaseBackupBeginResponse(begin); err == nil || !strings.Contains(err.Error(), "start_record_lsn") {
		t.Fatalf("missing start_record_lsn error = %v, want start_record_lsn error", err)
	}

	finish := HABaseBackupFinishResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "base_backup_finish:manifest-a",
			ActionKind: HAActionKindBaseBackupFinish,
			Target:     "manifest-a",
			State:      HAActionStateApplied,
			NodeId:     "primary-a",
		},
		ManifestId:   "manifest-a",
		BackupLsn:    7,
		EndRecordLsn: 9,
	}
	if err := ValidateHABaseBackupFinishResponse(finish); err != nil {
		t.Fatalf("ValidateHABaseBackupFinishResponse returned error: %v", err)
	}
	finish.EndRecordLsn = 0
	if err := ValidateHABaseBackupFinishResponse(finish); err == nil || !strings.Contains(err.Error(), "end_record_lsn") {
		t.Fatalf("missing end_record_lsn error = %v, want end_record_lsn error", err)
	}

	bootstrap := HAStandbyBootstrapResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "standby_bootstrap:manifest-a",
			ActionKind: HAActionKindStandbyBootstrap,
			Target:     "manifest-a",
			State:      HAActionStateApplied,
			NodeId:     "standby-a",
		},
		ManifestId:    "manifest-a",
		BackupLsn:     7,
		CheckpointLsn: 10,
	}
	if err := ValidateHAStandbyBootstrapResponse(bootstrap); err != nil {
		t.Fatalf("ValidateHAStandbyBootstrapResponse returned error: %v", err)
	}
	bootstrap.CheckpointLsn = 0
	if err := ValidateHAStandbyBootstrapResponse(bootstrap); err == nil || !strings.Contains(err.Error(), "checkpoint_lsn") {
		t.Fatalf("missing checkpoint_lsn error = %v, want checkpoint_lsn error", err)
	}
}

func TestHAClientReturnsStatusError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not primary", http.StatusConflict)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	_, err = client.CurrentFence(context.Background())
	var apiErr *HAAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("CurrentFence error = %T %v, want *HAAPIError", err, err)
	}
	if apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("StatusCode = %d, want %d", apiErr.StatusCode, http.StatusConflict)
	}
	if !strings.Contains(apiErr.Body, "not primary") {
		t.Fatalf("Body = %q, want not primary", apiErr.Body)
	}
}

func haCommitAppendResponseJSON() string {
	return `{
		"schema_version":1,
		"lsn":9,
		"gate":{
			"action":"acknowledge",
			"target_lsn":9,
			"durability":{
				"status":"satisfied",
				"mode":"remote_write",
				"selection":"any",
				"target_lsn":9,
				"progress_lsn":9,
				"missing_lsn_count":0,
				"satisfied_count":1,
				"required_count":1,
				"candidate_count":1
			}
		}
	}`
}

func haCommitCheckResponseJSON() string {
	return `{
		"schema_version":1,
		"gate":{
			"action":"acknowledge",
			"target_lsn":9,
			"durability":{
				"status":"satisfied",
				"mode":"remote_write",
				"selection":"any",
				"target_lsn":9,
				"progress_lsn":9,
				"missing_lsn_count":0,
				"satisfied_count":1,
				"required_count":1,
				"candidate_count":1
			}
		}
	}`
}

func haWriteDecisionResponseJSON() string {
	return `{
		"schema_version":1,
		"decision":{
			"action":"reject_read_only_standby",
			"durable_lsn":9,
			"identity":{"cluster_id":1,"timeline_id":1,"epoch":1,"table_id":0,"shard_id":0},
			"next_lsn":10,
			"role":"standby"
		}
	}`
}

func haOwnerJobDecisionResponseJSON() string {
	return `{
		"schema_version":1,
		"decision":{
			"action":"run",
			"durable_lsn":9,
			"identity":{"cluster_id":1,"timeline_id":1,"epoch":1,"table_id":0,"shard_id":0},
			"kind":"compaction_publish",
			"next_lsn":10,
			"role":"primary"
		}
	}`
}
