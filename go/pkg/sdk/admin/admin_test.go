package admin

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
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
				"action_id":"replication_slot_create:standby-a",
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

func TestHAClientPublicAPIDoesNotExposeGeneratedClient(t *testing.T) {
	t.Parallel()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate test file")
	}
	sourcePath := filepath.Join(filepath.Dir(file), "ha.go")
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, sourcePath, nil, 0)
	if err != nil {
		t.Fatalf("parse ha.go: %v", err)
	}

	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !fn.Name.IsExported() {
			continue
		}
		if fn.Recv != nil && fn.Name.Name == "Client" {
			t.Fatalf("HAClient must not expose the generated oapi client through an exported Client method")
		}
		if fn.Type.Params != nil && containsOAPISelector(fn.Type.Params) {
			t.Fatalf("%s exposes generated oapi types in public HA wrapper parameters", fn.Name.Name)
		}
		if fn.Type.Results != nil && containsOAPISelector(fn.Type.Results) {
			t.Fatalf("%s exposes generated oapi types in public HA wrapper results", fn.Name.Name)
		}
	}
}

func containsOAPISelector(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		if found || n == nil {
			return false
		}
		selector, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := selector.X.(*ast.Ident)
		if ok && ident.Name == "oapi" {
			found = true
			return false
		}
		return true
	})
	return found
}

func TestHAClientCreateReplicationSlotRejectsMissingEvidence(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"schema_version":1,
			"slot_action":"create",
			"action":{
				"action_id":"replication_slot_create:standby-a",
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
				"current_lsn":7
			}
		}`)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	_, err = client.CreateReplicationSlot(context.Background(), ReplicationSlotCreateRequest{SlotName: "standby-a"})
	if err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("CreateReplicationSlot error = %v, want slot field evidence error", err)
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

func TestHAClientGateOperationsRejectInvalidTypedResponses(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/write/check" {
			t.Fatalf("path = %s, want /admin/v1/ha/write/check", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(haWriteDecisionResponseJSON(), `"action":"reject_read_only_standby"`, `"action":"unknown"`, 1))
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	_, err = client.CheckWrite(context.Background(), WriteCheckRequest{Role: WriteCheckRoleStandby})
	if err == nil || !strings.Contains(err.Error(), "write decision fields") {
		t.Fatalf("CheckWrite error = %v, want write decision fields", err)
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

func TestHAClientCurrentFenceRejectsInvalidTypedResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":true}`)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	_, err = client.CurrentFence(context.Background())
	if err == nil || !strings.Contains(err.Error(), "current fence receipt fields") {
		t.Fatalf("CurrentFence error = %v, want current fence receipt fields", err)
	}
}

func TestHAClientCurrentFenceAcceptsEmptyReceiptReason(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":true,"receipt":{"identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":4,"epoch":5},"old_primary_id":"primary-a","promoted_node_id":"standby-a","parent_timeline_id":2,"parent_epoch":3,"new_timeline_id":4,"new_epoch":5,"required_lsn":8,"observed_lsn":8,"generation":9,"forced":false,"token":"fence-token","reason":""}}`)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	resp, err := client.CurrentFence(context.Background())
	if err != nil {
		t.Fatalf("CurrentFence returned error: %v", err)
	}
	if !resp.Held || resp.Receipt.Reason != "" {
		t.Fatalf("CurrentFence response = %#v, want held receipt with empty reason", resp)
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

	slot := HAReplicationSlot{
		SlotName:       "standby-a",
		TimelineId:     1,
		RestartLsn:     7,
		ReceivedLsn:    7,
		AppliedLsn:     7,
		SafeReadLsn:    7,
		CurrentLsn:     7,
		Active:         true,
		ReseedRequired: false,
	}
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
		Slot:       slot,
	}
	if err := ValidateHAReplicationSlotActionResponse(response); err != nil {
		t.Fatalf("ValidateHAReplicationSlotActionResponse returned error: %v", err)
	}
	wrongSlotTarget := response
	wrongSlotTarget.Action.Target = "standby-b"
	if err := ValidateHAReplicationSlotActionResponse(wrongSlotTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong slot target error = %v, want receipt mismatch", err)
	}
	wrongSlotKind := response
	wrongSlotKind.Action.ActionKind = HAActionKindReplicationSlotPause
	wrongSlotKind.Action.ActionId = "replication_slot_pause:standby-a"
	if err := ValidateHAReplicationSlotActionResponse(wrongSlotKind); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong slot action kind error = %v, want receipt mismatch", err)
	}
	if err := ValidateHAReplicationSlotListResponse(HAReplicationSlotListResponse{
		SchemaVersion: 1,
		Slots:         []HAReplicationSlot{slot},
	}); err != nil {
		t.Fatalf("ValidateHAReplicationSlotListResponse returned error: %v", err)
	}
	badListSlot := slot
	badListSlot.SlotName = ""
	if err := ValidateHAReplicationSlotListResponse(HAReplicationSlotListResponse{
		SchemaVersion: 1,
		Slots:         []HAReplicationSlot{badListSlot},
	}); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("invalid slot list error = %v, want slot fields error", err)
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
	wrongBeginTarget := begin
	wrongBeginTarget.Action.Target = "manifest-b"
	if err := ValidateHABaseBackupBeginResponse(wrongBeginTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong begin target error = %v, want receipt mismatch", err)
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
	wrongFinishKind := finish
	wrongFinishKind.Action.ActionKind = HAActionKindBaseBackupBegin
	wrongFinishKind.Action.ActionId = "base_backup_begin:manifest-a"
	if err := ValidateHABaseBackupFinishResponse(wrongFinishKind); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong finish kind error = %v, want receipt mismatch", err)
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
	wrongBootstrapTarget := bootstrap
	wrongBootstrapTarget.Action.Target = "manifest-b"
	if err := ValidateHAStandbyBootstrapResponse(wrongBootstrapTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong bootstrap target error = %v, want receipt mismatch", err)
	}
	bootstrap.CheckpointLsn = 0
	if err := ValidateHAStandbyBootstrapResponse(bootstrap); err == nil || !strings.Contains(err.Error(), "checkpoint_lsn") {
		t.Fatalf("missing checkpoint_lsn error = %v, want checkpoint_lsn error", err)
	}
}

func TestValidateHAFenceResponse(t *testing.T) {
	t.Parallel()

	receipt := HAFenceReceipt{
		Identity: HAIdentity{
			ClusterId:  1,
			ShardId:    2,
			TableId:    3,
			TimelineId: 6,
			Epoch:      7,
		},
		OldPrimaryId:     "primary-a",
		PromotedNodeId:   "standby-a",
		ParentTimelineId: 4,
		ParentEpoch:      5,
		NewTimelineId:    6,
		NewEpoch:         7,
		RequiredLsn:      8,
		ObservedLsn:      8,
		Generation:       9,
		Forced:           false,
		Token:            "fence-token",
		Reason:           "manual",
	}
	response := HAFenceResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "fence_acquire:standby-a",
			ActionKind: HAActionKindFenceAcquire,
			Target:     "standby-a",
			State:      HAActionStateApplied,
			NodeId:     "standby-a",
		},
		Receipt: receipt,
	}
	if err := ValidateHAFenceResponse(response); err != nil {
		t.Fatalf("ValidateHAFenceResponse returned error: %v", err)
	}
	emptyReason := response
	emptyReason.Receipt.Reason = ""
	if err := ValidateHAFenceResponse(emptyReason); err != nil {
		t.Fatalf("ValidateHAFenceResponse with empty reason returned error: %v", err)
	}
	wrongActionNode := response
	wrongActionNode.Action.NodeId = "standby-b"
	if err := ValidateHAFenceResponse(wrongActionNode); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("wrong fence action node error = %v, want action node mismatch", err)
	}
	wrongIdentity := response
	wrongIdentity.Receipt.Identity.TimelineId = 5
	if err := ValidateHAFenceResponse(wrongIdentity); err == nil || !strings.Contains(err.Error(), "promoted timeline") {
		t.Fatalf("wrong fence identity error = %v, want promoted timeline mismatch", err)
	}
	staleObserved := response
	staleObserved.Receipt.ObservedLsn = 7
	if err := ValidateHAFenceResponse(staleObserved); err == nil || !strings.Contains(err.Error(), "observed_lsn") {
		t.Fatalf("stale fence observed_lsn error = %v, want observed_lsn mismatch", err)
	}
	if err := ValidateHACurrentFenceResponse(HACurrentFenceResponse{
		SchemaVersion: 1,
		Held:          false,
	}); err != nil {
		t.Fatalf("ValidateHACurrentFenceResponse empty returned error: %v", err)
	}
	if err := ValidateHACurrentFenceResponse(HACurrentFenceResponse{
		SchemaVersion: 1,
		Held:          true,
		Receipt:       receipt,
	}); err != nil {
		t.Fatalf("ValidateHACurrentFenceResponse held returned error: %v", err)
	}
	if err := ValidateHACurrentFenceResponse(HACurrentFenceResponse{
		SchemaVersion: 1,
		Held:          true,
	}); err == nil || !strings.Contains(err.Error(), "receipt fields") {
		t.Fatalf("missing current fence receipt error = %v, want receipt fields error", err)
	}
	if err := ValidateHACurrentFenceResponse(HACurrentFenceResponse{
		SchemaVersion: 1,
		Held:          false,
		Receipt:       receipt,
	}); err == nil || !strings.Contains(err.Error(), "not held") {
		t.Fatalf("unexpected current fence receipt error = %v, want not held error", err)
	}
	currentWithBadReceipt := receipt
	currentWithBadReceipt.NewEpoch = currentWithBadReceipt.ParentEpoch
	currentWithBadReceipt.Identity.Epoch = currentWithBadReceipt.NewEpoch
	if err := ValidateHACurrentFenceResponse(HACurrentFenceResponse{
		SchemaVersion: 1,
		Held:          true,
		Receipt:       currentWithBadReceipt,
	}); err == nil || !strings.Contains(err.Error(), "does not advance") {
		t.Fatalf("bad current fence receipt error = %v, want advance error", err)
	}
	response.Receipt.Token = ""
	if err := ValidateHAFenceResponse(response); err == nil || !strings.Contains(err.Error(), "receipt fields") {
		t.Fatalf("missing token error = %v, want receipt fields error", err)
	}
	response.Receipt.Token = "fence-token"
	response.Action.NodeId = ""
	if err := ValidateHAFenceResponse(response); err == nil || !strings.Contains(err.Error(), "action receipt") {
		t.Fatalf("missing action receipt error = %v, want action receipt error", err)
	}
}

func TestValidateHAPromotionResponses(t *testing.T) {
	t.Parallel()

	assessment := HAPromotionAssessment{
		RequiredLsn:        8,
		ReceivedLsn:        8,
		AppliedLsn:         8,
		HasRequiredLsn:     true,
		CaughtUpToReceived: true,
		FencingConfirmed:   true,
		CanPromote:         true,
		Safe:               true,
	}
	assess := HAPromotionAssessResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "promotion_assess:standby-a",
			ActionKind: HAActionKindPromotionAssess,
			Target:     "standby-a",
			State:      HAActionStateAssessed,
			NodeId:     "standby-a",
		},
		Assessment: assessment,
	}
	if err := ValidateHAPromotionAssessResponse(assess); err != nil {
		t.Fatalf("ValidateHAPromotionAssessResponse returned error: %v", err)
	}
	wrongAssessNode := assess
	wrongAssessNode.Action.NodeId = "standby-b"
	if err := ValidateHAPromotionAssessResponse(wrongAssessNode); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("wrong promotion assess executor error = %v, want executor node mismatch", err)
	}
	inconsistentAssess := assess
	inconsistentAssess.Assessment.HasRequiredLsn = false
	if err := ValidateHAPromotionAssessResponse(inconsistentAssess); err == nil || !strings.Contains(err.Error(), "has_required_lsn") {
		t.Fatalf("inconsistent promotion assessment error = %v, want has_required_lsn mismatch", err)
	}
	assess.Assessment.RequiredLsn = 0
	if err := ValidateHAPromotionAssessResponse(assess); err == nil || !strings.Contains(err.Error(), "assessment fields") {
		t.Fatalf("missing assessment error = %v, want assessment fields error", err)
	}

	identity := HAIdentity{ClusterId: 1, ShardId: 2, TableId: 3, TimelineId: 4, Epoch: 5}
	promotion := HAPromotionResponse{
		SchemaVersion:   1,
		Action:          HAActionReceipt{ActionId: "promotion:standby-a", ActionKind: HAActionKindPromotion, Target: "standby-a", State: HAActionStateApplied, NodeId: "standby-a"},
		Assessment:      assessment,
		FenceGeneration: 9,
		FenceToken:      "fence-token",
		Promotion: HAPromotionResult{
			NodeId:      "standby-a",
			SwitchLsn:   9,
			OldIdentity: identity,
			NewIdentity: HAIdentity{ClusterId: 1, ShardId: 2, TableId: 3, TimelineId: 6, Epoch: 7},
		},
	}
	if err := ValidateHAPromotionResponse(promotion); err != nil {
		t.Fatalf("ValidateHAPromotionResponse returned error: %v", err)
	}
	wrongPromotionNode := promotion
	wrongPromotionNode.Action.NodeId = "standby-b"
	if err := ValidateHAPromotionResponse(wrongPromotionNode); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("wrong promotion node error = %v, want action node mismatch", err)
	}
	wrongSwitchLSN := promotion
	wrongSwitchLSN.Promotion.SwitchLsn = 10
	if err := ValidateHAPromotionResponse(wrongSwitchLSN); err == nil || !strings.Contains(err.Error(), "switch_lsn") {
		t.Fatalf("wrong promotion switch_lsn error = %v, want switch_lsn mismatch", err)
	}
	wrongIdentity := promotion
	wrongIdentity.Promotion.NewIdentity.ClusterId = 99
	if err := ValidateHAPromotionResponse(wrongIdentity); err == nil || !strings.Contains(err.Error(), "identity scope") {
		t.Fatalf("wrong promotion identity error = %v, want identity scope mismatch", err)
	}
	promotion.FenceToken = ""
	if err := ValidateHAPromotionResponse(promotion); err == nil || !strings.Contains(err.Error(), "fence_token") {
		t.Fatalf("missing fence_token error = %v, want fence_token error", err)
	}
	promotion.FenceToken = "fence-token"
	promotion.Promotion.SwitchLsn = 0
	if err := ValidateHAPromotionResponse(promotion); err == nil || !strings.Contains(err.Error(), "promotion result") {
		t.Fatalf("missing promotion result error = %v, want promotion result error", err)
	}
}

func TestValidateHAResponseEvidence(t *testing.T) {
	t.Parallel()

	slot := `{"schema_version":1,"action":{"action_id":"replication_slot_create:standby-a","action_kind":"replication_slot_create","target":"standby-a","state":"applied","node_id":"primary-a"},"slot_action":"create","slot":{"slot_name":"standby-a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":false,"reseed_required":false,"current_lsn":0}}`
	if err := ValidateHAReplicationSlotActionResponseEvidence([]byte(slot)); err != nil {
		t.Fatalf("ValidateHAReplicationSlotActionResponseEvidence returned error: %v", err)
	}
	if err := ValidateHAReplicationSlotActionResponseEvidence([]byte(strings.Replace(slot, `,"active":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot active evidence error = %v, want slot field evidence error", err)
	}
	slotList := `{"schema_version":1,"slots":[{"slot_name":"standby-a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":false,"reseed_required":false,"current_lsn":0}]}`
	if err := ValidateHAReplicationSlotListResponseEvidence([]byte(slotList)); err != nil {
		t.Fatalf("ValidateHAReplicationSlotListResponseEvidence returned error: %v", err)
	}
	if err := ValidateHAReplicationSlotListResponseEvidence([]byte(`{"schema_version":1}`)); err == nil || !strings.Contains(err.Error(), "slots field evidence") {
		t.Fatalf("missing slot list evidence error = %v, want slots field evidence error", err)
	}
	if err := ValidateHAReplicationSlotListResponseEvidence([]byte(strings.Replace(slotList, `,"current_lsn":0`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot current_lsn evidence error = %v, want slot field evidence error", err)
	}

	begin := `{"schema_version":1,"action":{"action_id":"base_backup_begin:manifest-a","action_kind":"base_backup_begin","target":"manifest-a","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"manifest-a","backup_lsn":7,"start_record_lsn":8}`
	if err := ValidateHABaseBackupBeginResponseEvidence([]byte(begin)); err != nil {
		t.Fatalf("ValidateHABaseBackupBeginResponseEvidence returned error: %v", err)
	}
	if err := ValidateHABaseBackupBeginResponseEvidence([]byte(strings.Replace(begin, `,"start_record_lsn":8`, "", 1))); err == nil || !strings.Contains(err.Error(), "base backup begin field evidence") {
		t.Fatalf("missing base backup begin evidence error = %v, want field evidence error", err)
	}
	finish := `{"schema_version":1,"action":{"action_id":"base_backup_finish:manifest-a","action_kind":"base_backup_finish","target":"manifest-a","state":"applied","node_id":"primary-a"},"manifest_id":"manifest-a","backup_lsn":7,"end_record_lsn":9}`
	if err := ValidateHABaseBackupFinishResponseEvidence([]byte(finish)); err != nil {
		t.Fatalf("ValidateHABaseBackupFinishResponseEvidence returned error: %v", err)
	}
	if err := ValidateHABaseBackupFinishResponseEvidence([]byte(strings.Replace(finish, `,"end_record_lsn":9`, "", 1))); err == nil || !strings.Contains(err.Error(), "base backup finish field evidence") {
		t.Fatalf("missing base backup finish evidence error = %v, want field evidence error", err)
	}
	bootstrap := `{"schema_version":1,"action":{"action_id":"standby_bootstrap:manifest-a","action_kind":"standby_bootstrap","target":"manifest-a","state":"applied","node_id":"standby-a"},"manifest_id":"manifest-a","backup_lsn":7,"checkpoint_lsn":10}`
	if err := ValidateHAStandbyBootstrapResponseEvidence([]byte(bootstrap)); err != nil {
		t.Fatalf("ValidateHAStandbyBootstrapResponseEvidence returned error: %v", err)
	}
	if err := ValidateHAStandbyBootstrapResponseEvidence([]byte(strings.Replace(bootstrap, `,"checkpoint_lsn":10`, "", 1))); err == nil || !strings.Contains(err.Error(), "standby bootstrap field evidence") {
		t.Fatalf("missing standby bootstrap evidence error = %v, want field evidence error", err)
	}

	fence := `{"schema_version":1,"action":{"action_id":"fence_acquire:standby-a","action_kind":"fence_acquire","target":"standby-a","state":"applied","node_id":"standby-a"},"receipt":{"identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":2,"epoch":3},"old_primary_id":"primary-a","promoted_node_id":"standby-a","parent_timeline_id":2,"parent_epoch":3,"new_timeline_id":4,"new_epoch":5,"required_lsn":8,"observed_lsn":8,"generation":9,"forced":false,"token":"fence-token","reason":""}}`
	if err := ValidateHAFenceResponseEvidence([]byte(fence)); err != nil {
		t.Fatalf("ValidateHAFenceResponseEvidence returned error: %v", err)
	}
	if err := ValidateHAFenceResponseEvidence([]byte(strings.Replace(fence, `,"forced":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "receipt field evidence") {
		t.Fatalf("missing fence forced evidence error = %v, want receipt evidence error", err)
	}
	if err := ValidateHACurrentFenceResponseEvidence([]byte(`{"schema_version":1}`)); err == nil || !strings.Contains(err.Error(), "held field evidence") {
		t.Fatalf("missing held evidence error = %v, want held evidence error", err)
	}

	assessment := `"assessment":{"required_lsn":8,"received_lsn":8,"applied_lsn":8,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}`
	promotionAssess := `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},` + assessment + `}`
	if err := ValidateHAPromotionAssessResponseEvidence([]byte(promotionAssess)); err != nil {
		t.Fatalf("ValidateHAPromotionAssessResponseEvidence returned error: %v", err)
	}
	if err := ValidateHAPromotionAssessResponseEvidence([]byte(strings.Replace(promotionAssess, `,"force":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "assessment field evidence") {
		t.Fatalf("missing promotion force evidence error = %v, want assessment evidence error", err)
	}

	promotion := `{"schema_version":1,"action":{"action_id":"promotion:standby-a","action_kind":"promotion","target":"standby-a","state":"applied","node_id":"standby-a"},` + assessment + `,"fence_generation":9,"fence_token":"fence-token","forced":false,"promotion":{"node_id":"standby-a","switch_lsn":9,"old_identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":2,"epoch":3},"new_identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":4,"epoch":5},"data_loss_possible":false,"forced":false}}`
	if err := ValidateHAPromotionResponseEvidence([]byte(promotion)); err != nil {
		t.Fatalf("ValidateHAPromotionResponseEvidence returned error: %v", err)
	}
	if err := ValidateHAPromotionResponseEvidence([]byte(strings.Replace(promotion, `,"data_loss_possible":false,"forced":false}}`, `,"forced":false}}`, 1))); err == nil || !strings.Contains(err.Error(), "promotion result field evidence") {
		t.Fatalf("missing promotion result evidence error = %v, want promotion result evidence error", err)
	}

	rejoin := `{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"assessed","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":4,"target_epoch":5,"parent_cluster_id":1,"parent_shard_id":0,"parent_table_id":0,"parent_timeline_id":2,"parent_epoch":3,"fork_lsn":8,"former_last_lsn":9,"retained_from_lsn":7,"data_loss_discarded":false},"rewind":{"node_id":"primary-a","target_timeline_id":4,"target_epoch":5,"next_lsn":9,"current_last_lsn":9,"previous_last_lsn":10,"fork_lsn":8,"discarded_lsn_count":1,"data_loss_discarded":false}}`
	if err := ValidateHARejoinAssessResponseEvidence([]byte(rejoin)); err != nil {
		t.Fatalf("ValidateHARejoinAssessResponseEvidence returned error: %v", err)
	}
	if err := ValidateHARejoinAssessResponseEvidence([]byte(strings.Replace(rejoin, `,"data_loss_discarded":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "rejoin assessment field evidence") {
		t.Fatalf("missing rejoin assessment evidence error = %v, want assessment evidence error", err)
	}
}

func TestValidateHAGateResponses(t *testing.T) {
	t.Parallel()

	durability := HADurabilityDecision{
		Status:          HADurabilityStatusSatisfied,
		Mode:            HADurabilityModeRemoteWrite,
		Selection:       HADurabilitySelectionAny,
		TargetLsn:       9,
		ProgressLsn:     9,
		RequiredCount:   1,
		SatisfiedCount:  1,
		CandidateCount:  1,
		MissingLsnCount: 0,
	}
	gate := HACommitGate{
		Action:     HACommitGateActionAcknowledge,
		TargetLsn:  9,
		Durability: durability,
	}
	if err := ValidateHACommitCheckResponse(HACommitCheckResponse{SchemaVersion: 1, Gate: gate}); err != nil {
		t.Fatalf("ValidateHACommitCheckResponse returned error: %v", err)
	}
	if err := ValidateHACommitAppendResponse(HACommitAppendResponse{SchemaVersion: 1, Lsn: 9, Gate: gate}); err != nil {
		t.Fatalf("ValidateHACommitAppendResponse returned error: %v", err)
	}
	mismatchedGate := gate
	mismatchedGate.Durability.TargetLsn = 8
	if err := ValidateHACommitCheckResponse(HACommitCheckResponse{SchemaVersion: 1, Gate: mismatchedGate}); err == nil || !strings.Contains(err.Error(), "target_lsn") {
		t.Fatalf("mismatched gate target error = %v, want target_lsn mismatch", err)
	}
	impossibleProgress := gate
	impossibleProgress.Durability.ProgressLsn = 10
	if err := ValidateHACommitCheckResponse(HACommitCheckResponse{SchemaVersion: 1, Gate: impossibleProgress}); err == nil || !strings.Contains(err.Error(), "progress_lsn") {
		t.Fatalf("impossible durability progress error = %v, want progress_lsn mismatch", err)
	}
	if err := ValidateHACommitAppendResponse(HACommitAppendResponse{SchemaVersion: 1, Lsn: 8, Gate: gate}); err == nil || !strings.Contains(err.Error(), "does not match gate") {
		t.Fatalf("mismatched append lsn error = %v, want gate lsn mismatch", err)
	}
	gate.Action = HACommitGateAction("unknown")
	if err := ValidateHACommitCheckResponse(HACommitCheckResponse{SchemaVersion: 1, Gate: gate}); err == nil || !strings.Contains(err.Error(), "invalid commit gate action") {
		t.Fatalf("invalid gate error = %v, want invalid action error", err)
	}

	read := HAReadCheckResponse{
		SchemaVersion: 1,
		Decision: HAReadDecision{
			Action:                  HAReadDecisionActionServeStandby,
			Consistency:             HAReadDecisionConsistencyAtLeastLSN,
			ReceivedLsn:             9,
			AppliedLsn:              9,
			SafeReadLsn:             9,
			MissingLsnCount:         0,
			MetadataMissingLsnCount: 0,
		},
	}
	if err := ValidateHAReadCheckResponse(read); err != nil {
		t.Fatalf("ValidateHAReadCheckResponse returned error: %v", err)
	}
	badReadProgress := read
	badReadProgress.Decision.AppliedLsn = 10
	if err := ValidateHAReadCheckResponse(badReadProgress); err == nil || !strings.Contains(err.Error(), "applied_lsn") {
		t.Fatalf("invalid read progress error = %v, want applied_lsn error", err)
	}
	badReadMissing := read
	badReadMissing.Decision.RequiredLsn = 11
	if err := ValidateHAReadCheckResponse(badReadMissing); err == nil || !strings.Contains(err.Error(), "missing_lsn_count") {
		t.Fatalf("invalid read missing count error = %v, want missing_lsn_count error", err)
	}
	badReadServe := read
	badReadServe.Decision.ServeLsn = 10
	if err := ValidateHAReadCheckResponse(badReadServe); err == nil || !strings.Contains(err.Error(), "serve_lsn") {
		t.Fatalf("invalid read serve lsn error = %v, want serve_lsn error", err)
	}
	badReadPrimary := read
	badReadPrimary.Decision.Consistency = HAReadDecisionConsistencyPrimary
	if err := ValidateHAReadCheckResponse(badReadPrimary); err == nil || !strings.Contains(err.Error(), "primary consistency") {
		t.Fatalf("invalid read primary action error = %v, want primary consistency error", err)
	}
	badReadFields := read
	badReadFields.Decision.Consistency = HAReadDecisionConsistency("unknown")
	if err := ValidateHAReadCheckResponse(badReadFields); err == nil || !strings.Contains(err.Error(), "read decision fields") {
		t.Fatalf("invalid read decision error = %v, want read decision fields error", err)
	}

	identity := HAIdentity{ClusterId: 1, TimelineId: 2, Epoch: 3}
	write := HAWriteCheckResponse{
		SchemaVersion: 1,
		Decision: HAWriteDecision{
			Action:     HAWriteDecisionActionRejectReadOnly,
			Role:       HAWriteDecisionRoleStandby,
			Identity:   identity,
			DurableLsn: 9,
			NextLsn:    10,
		},
	}
	if err := ValidateHAWriteCheckResponse(write); err != nil {
		t.Fatalf("ValidateHAWriteCheckResponse returned error: %v", err)
	}
	badWriteNext := write
	badWriteNext.Decision.NextLsn = 12
	if err := ValidateHAWriteCheckResponse(badWriteNext); err == nil || !strings.Contains(err.Error(), "next_lsn") {
		t.Fatalf("invalid write next lsn error = %v, want next_lsn error", err)
	}
	badWriteAction := write
	badWriteAction.Decision.Action = HAWriteDecisionActionAllowWrite
	if err := ValidateHAWriteCheckResponse(badWriteAction); err == nil || !strings.Contains(err.Error(), "standby role action") {
		t.Fatalf("invalid write role action error = %v, want standby role action error", err)
	}
	promotedIdentity := HAIdentity{ClusterId: 1, TimelineId: 4, Epoch: 5}
	promotedWrite := HAWriteCheckResponse{
		SchemaVersion: 1,
		Decision: HAWriteDecision{
			Action:     HAWriteDecisionActionOpenPromotedPrimary,
			Role:       HAWriteDecisionRolePromotedStandby,
			Identity:   promotedIdentity,
			DurableLsn: 12,
			NextLsn:    13,
			PromotionHandoff: HAPromotionHandoff{
				Identity:  promotedIdentity,
				SwitchLsn: 12,
				NextLsn:   13,
			},
		},
	}
	if err := ValidateHAWriteCheckResponse(promotedWrite); err != nil {
		t.Fatalf("ValidateHAWriteCheckResponse promoted returned error: %v", err)
	}
	badWriteHandoff := promotedWrite
	badWriteHandoff.Decision.PromotionHandoff.Identity.Epoch = 6
	if err := ValidateHAWriteCheckResponse(badWriteHandoff); err == nil || !strings.Contains(err.Error(), "promotion_handoff identity") {
		t.Fatalf("invalid write handoff error = %v, want promotion_handoff identity error", err)
	}
	badWriteFields := write
	badWriteFields.Decision.Identity = HAIdentity{}
	if err := ValidateHAWriteCheckResponse(badWriteFields); err == nil || !strings.Contains(err.Error(), "write decision fields") {
		t.Fatalf("invalid write decision error = %v, want write decision fields error", err)
	}

	owner := HAOwnerJobCheckResponse{
		SchemaVersion: 1,
		Decision: HAOwnerJobDecision{
			Action:     HAOwnerJobDecisionActionRun,
			Kind:       HAOwnerJobDecisionKindCompactionPublish,
			Role:       HAOwnerJobDecisionRolePrimary,
			Identity:   identity,
			DurableLsn: 9,
			NextLsn:    10,
		},
	}
	if err := ValidateHAOwnerJobCheckResponse(owner); err != nil {
		t.Fatalf("ValidateHAOwnerJobCheckResponse returned error: %v", err)
	}
	badOwnerNext := owner
	badOwnerNext.Decision.NextLsn = 12
	if err := ValidateHAOwnerJobCheckResponse(badOwnerNext); err == nil || !strings.Contains(err.Error(), "next_lsn") {
		t.Fatalf("invalid owner job next lsn error = %v, want next_lsn error", err)
	}
	badOwnerAction := owner
	badOwnerAction.Decision.Role = HAOwnerJobDecisionRoleStandby
	if err := ValidateHAOwnerJobCheckResponse(badOwnerAction); err == nil || !strings.Contains(err.Error(), "standby role action") {
		t.Fatalf("invalid owner job role action error = %v, want standby role action error", err)
	}
	promotedOwner := HAOwnerJobCheckResponse{
		SchemaVersion: 1,
		Decision: HAOwnerJobDecision{
			Action:     HAOwnerJobDecisionActionOpenPromotedPrimary,
			Kind:       HAOwnerJobDecisionKindCompactionPublish,
			Role:       HAOwnerJobDecisionRolePromotedStandby,
			Identity:   promotedIdentity,
			DurableLsn: 12,
			NextLsn:    13,
			PromotionHandoff: HAPromotionHandoff{
				Identity:  promotedIdentity,
				SwitchLsn: 12,
				NextLsn:   13,
			},
		},
	}
	if err := ValidateHAOwnerJobCheckResponse(promotedOwner); err != nil {
		t.Fatalf("ValidateHAOwnerJobCheckResponse promoted returned error: %v", err)
	}
	badOwnerHandoff := promotedOwner
	badOwnerHandoff.Decision.PromotionHandoff.NextLsn = 14
	if err := ValidateHAOwnerJobCheckResponse(badOwnerHandoff); err == nil || !strings.Contains(err.Error(), "promotion_handoff next_lsn") {
		t.Fatalf("invalid owner job handoff error = %v, want promotion_handoff next_lsn error", err)
	}
	badOwnerFields := owner
	badOwnerFields.Decision.Kind = HAOwnerJobDecisionKind("unknown")
	if err := ValidateHAOwnerJobCheckResponse(badOwnerFields); err == nil || !strings.Contains(err.Error(), "owner job decision fields") {
		t.Fatalf("invalid owner job decision error = %v, want owner job decision fields error", err)
	}
}

func TestValidateHARejoinAssessResponse(t *testing.T) {
	t.Parallel()

	base := HARejoinAssessResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "rejoin_assess:primary-a",
			ActionKind: HAActionKindRejoinAssess,
			Target:     "primary-a",
			State:      HAActionStateAssessed,
			NodeId:     "primary-a",
		},
		Assessment: HARejoinAssessment{
			Action:           HARejoinActionAlreadyCurrent,
			Reason:           HARejoinReasonCurrentTimeline,
			FormerNodeId:     "primary-a",
			TargetTimelineId: 6,
			TargetEpoch:      7,
			ParentClusterId:  1,
			ParentShardId:    2,
			ParentTableId:    3,
			ParentTimelineId: 4,
			ParentEpoch:      5,
			ForkLsn:          8,
			FormerLastLsn:    8,
			RetainedFromLsn:  1,
		},
	}
	if err := ValidateHARejoinAssessResponse(base); err != nil {
		t.Fatalf("ValidateHARejoinAssessResponse returned error: %v", err)
	}
	wrongTarget := base
	wrongTarget.Action.Target = "primary-b"
	if err := ValidateHARejoinAssessResponse(wrongTarget); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("wrong target error = %v, want target mismatch error", err)
	}
	wrongAssessNode := base
	wrongAssessNode.Action.NodeId = "primary-b"
	if err := ValidateHARejoinAssessResponse(wrongAssessNode); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("wrong assess executor error = %v, want executor node mismatch error", err)
	}
	base.Assessment.Reason = HARejoinAssessmentReason("unknown")
	if err := ValidateHARejoinAssessResponse(base); err == nil || !strings.Contains(err.Error(), "assessment fields") {
		t.Fatalf("invalid reason error = %v, want assessment fields error", err)
	}
	base.Assessment.Reason = HARejoinReasonCurrentTimeline

	rewind := base
	rewind.Action.ActionId = "rejoin_rewind:primary-a"
	rewind.Action.ActionKind = HAActionKindRejoinRewind
	rewind.Action.State = HAActionStateApplied
	rewind.Assessment.Action = HARejoinActionRewind
	rewind.Assessment.Reason = HARejoinReasonParentTimelineRetained
	rewind.Rewind = HARejoinRewindResult{
		NodeId:           "primary-a",
		TargetTimelineId: 6,
		TargetEpoch:      7,
		CurrentLastLsn:   8,
		PreviousLastLsn:  10,
		NextLsn:          9,
		ForkLsn:          8,
	}
	if err := ValidateHARejoinAssessResponse(rewind); err != nil {
		t.Fatalf("ValidateHARejoinAssessResponse rewind returned error: %v", err)
	}
	wrongRewindNode := rewind
	wrongRewindNode.Action.NodeId = "primary-b"
	if err := ValidateHARejoinAssessResponse(wrongRewindNode); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("wrong rewind executor error = %v, want executor node mismatch error", err)
	}
	rewind.Rewind.NextLsn = 0
	if err := ValidateHARejoinAssessResponse(rewind); err == nil || !strings.Contains(err.Error(), "rewind fields") {
		t.Fatalf("missing rewind error = %v, want rewind fields error", err)
	}

	reseed := base
	reseed.Action.ActionId = "rejoin_reseed:primary-a"
	reseed.Action.ActionKind = HAActionKindRejoinReseed
	reseed.Action.State = HAActionStateApplied
	reseed.Action.NodeId = "primary-current"
	reseed.Assessment.Action = HARejoinActionReseed
	reseed.Assessment.Reason = HARejoinReasonParentTimelineWALExpired
	reseed.Reseed = HARejoinReseedResult{
		NodeId:             "primary-a",
		SlotName:           "primary-a",
		TargetTimelineId:   6,
		TargetEpoch:        7,
		ForkLsn:            8,
		FormerLastLsn:      10,
		ReseedRequired:     true,
		BaseBackupRequired: true,
	}
	if err := ValidateHARejoinAssessResponse(reseed); err != nil {
		t.Fatalf("ValidateHARejoinAssessResponse reseed returned error: %v", err)
	}
	reseed.Reseed.SlotName = ""
	if err := ValidateHARejoinAssessResponse(reseed); err == nil || !strings.Contains(err.Error(), "reseed fields") {
		t.Fatalf("missing reseed error = %v, want reseed fields error", err)
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

func TestHAErrorRetryability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{{
		name: "nil",
		err:  nil,
		want: false,
	}, {
		name: "service unavailable",
		err:  &HAAPIError{Operation: "get HA primary status", StatusCode: http.StatusServiceUnavailable},
		want: true,
	}, {
		name: "too many requests",
		err:  &HAAPIError{Operation: "get HA primary status", StatusCode: http.StatusTooManyRequests},
		want: true,
	}, {
		name: "conflict",
		err:  &HAAPIError{Operation: "get current HA fence", StatusCode: http.StatusConflict},
		want: false,
	}, {
		name: "bad request",
		err:  &HAAPIError{Operation: "create HA replication slot", StatusCode: http.StatusBadRequest},
		want: false,
	}, {
		name: "validation",
		err:  &HAResponseValidationError{Operation: "create HA replication slot", Err: errors.New("missing action receipt")},
		want: false,
	}, {
		name: "deadline",
		err:  context.DeadlineExceeded,
		want: true,
	}, {
		name: "canceled",
		err:  context.Canceled,
		want: false,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := HAIsRetryable(tt.err); got != tt.want {
				t.Fatalf("HAIsRetryable(%T) = %v, want %v", tt.err, got, tt.want)
			}
		})
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
