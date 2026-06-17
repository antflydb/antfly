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
