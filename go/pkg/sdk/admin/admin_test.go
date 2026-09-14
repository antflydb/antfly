package admin

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/admin/oapi"
	"github.com/getkin/kin-openapi/openapi3"
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

func TestStandbyClientCreateReplicationSlotUsesAdminAPI(t *testing.T) {
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

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
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

func TestStandbyClientRejectsInvalidStandbyInputsLocally(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	identity := StandbyIdentity{
		ClusterId:  1,
		ShardId:    0,
		TableId:    0,
		TimelineId: 1,
		Epoch:      1,
	}
	validFence := FenceAcquireRequest{
		Identity:       identity,
		OldPrimaryId:   "primary-a",
		PromotedNodeId: "standby-a",
		NewTimelineId:  2,
		NewEpoch:       2,
		Generation:     1,
		RequiredLsn:    9,
		ObservedLsn:    9,
		Force:          false,
	}
	validRejoin := RejoinAssessRequest{
		NodeId:                          "primary-a",
		Identity:                        identity,
		LastLsn:                         9,
		RetainedFromLsn:                 1,
		AllowRewindAfterForcedPromotion: false,
		Receipt:                         StandbyFenceReceipt{},
	}
	validSyncPolicy := StandbySyncPolicy{
		Mode:          StandbySyncPolicyModeRemoteWrite,
		Selection:     StandbySyncPolicySelectionAny,
		Required:      1,
		FailurePolicy: StandbySyncPolicyFailureFailClosed,
		StandbyNames:  []string{"standby-a"},
	}
	validStandbyUpstream := StandbyUpstreamRequest{
		Identity:    identity,
		UpstreamUrl: "https://primary-b.example:5433",
		SlotName:    "standby-a",
		Reason:      "switchover",
	}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "create whitespace",
			call: func() error {
				_, err := client.CreateReplicationSlot(context.Background(), ReplicationSlotCreateRequest{
					SlotName:   " standby-a",
					InitialLsn: 7,
				})
				return err
			},
		},
		{
			name: "pause hidden path separator",
			call: func() error {
				_, err := client.PauseReplicationSlot(context.Background(), "standby/a")
				return err
			},
		},
		{
			name: "resume hidden whitespace",
			call: func() error {
				_, err := client.ResumeReplicationSlot(context.Background(), "standby a")
				return err
			},
		},
		{
			name: "drop too long",
			call: func() error {
				_, err := client.DropReplicationSlot(context.Background(), strings.Repeat("a", 129))
				return err
			},
		},
		{
			name: "primary status sync standby padded",
			call: func() error {
				_, err := client.PrimaryStatus(context.Background(), &StandbyPrimaryStatusParams{
					SyncStandby: []string{"standby-a "},
				})
				return err
			},
		},
		{
			name: "append commit sync standby hidden whitespace",
			call: func() error {
				policy := validSyncPolicy
				policy.StandbyNames = []string{"standby a"}
				_, err := client.AppendCommit(context.Background(), CommitAppendRequest{
					Payload:      "{}",
					SyncPolicy:   policy,
					Kind:         CommitAppendKindBatchMutation,
					PayloadCodec: CommitAppendRequestCodec("json"),
				})
				return err
			},
		},
		{
			name: "check commit sync standby path separator",
			call: func() error {
				policy := validSyncPolicy
				policy.StandbyNames = []string{"standby/a"}
				_, err := client.CheckCommit(context.Background(), CommitCheckRequest{
					TargetLsn:  9,
					SyncPolicy: policy,
				})
				return err
			},
		},
		{
			name: "begin base backup slot hidden whitespace",
			call: func() error {
				_, err := client.BeginBaseBackup(context.Background(), BaseBackupStartRequest{
					SlotName:   "standby a",
					ManifestId: "manifest-a",
				})
				return err
			},
		},
		{
			name: "finish base backup relative manifest path",
			call: func() error {
				_, err := client.FinishBaseBackup(context.Background(), BaseBackupManifestPathRequest{
					ManifestPath: "backup/manifest-a.json",
				})
				return err
			},
		},
		{
			name: "finish base backup padded manifest path",
			call: func() error {
				_, err := client.FinishBaseBackup(context.Background(), BaseBackupManifestPathRequest{
					ManifestPath: " /backup/manifest-a.json",
				})
				return err
			},
		},
		{
			name: "bootstrap manifest path not normalized",
			call: func() error {
				_, err := client.BootstrapStandby(context.Background(), StandbyBootstrapRequest{
					ManifestPath: "/backup/../manifest-a.json",
				})
				return err
			},
		},
		{
			name: "bootstrap content root not normalized",
			call: func() error {
				_, err := client.BootstrapStandby(context.Background(), StandbyBootstrapRequest{
					ManifestPath: "/backup/manifest-a.json",
					ContentRoot:  "/backup/standby-a/..",
				})
				return err
			},
		},
		{
			name: "acquire fence padded old primary id",
			call: func() error {
				body := validFence
				body.OldPrimaryId = " primary-a"
				_, err := client.AcquireFence(context.Background(), body)
				return err
			},
		},
		{
			name: "set standby upstream incomplete identity",
			call: func() error {
				body := validStandbyUpstream
				body.Identity = StandbyIdentity{}
				_, err := client.SetStandbyUpstream(context.Background(), body)
				return err
			},
		},
		{
			name: "set standby upstream missing scheme",
			call: func() error {
				body := validStandbyUpstream
				body.UpstreamUrl = "primary-b.example:5433"
				_, err := client.SetStandbyUpstream(context.Background(), body)
				return err
			},
		},
		{
			name: "set standby upstream unsupported scheme",
			call: func() error {
				body := validStandbyUpstream
				body.UpstreamUrl = "ftp://primary-b.example:5433"
				_, err := client.SetStandbyUpstream(context.Background(), body)
				return err
			},
		},
		{
			name: "set standby upstream padded url",
			call: func() error {
				body := validStandbyUpstream
				body.UpstreamUrl = " https://primary-b.example:5433"
				_, err := client.SetStandbyUpstream(context.Background(), body)
				return err
			},
		},
		{
			name: "set standby upstream invalid slot name",
			call: func() error {
				body := validStandbyUpstream
				body.SlotName = "standby/a"
				_, err := client.SetStandbyUpstream(context.Background(), body)
				return err
			},
		},
		{
			name: "promote padded promoted node id",
			call: func() error {
				body := validFence
				body.PromotedNodeId = "standby-a "
				_, err := client.Promote(context.Background(), body)
				return err
			},
		},
		{
			name: "assess rejoin invalid node id",
			call: func() error {
				body := validRejoin
				body.NodeId = "primary/a"
				_, err := client.AssessRejoin(context.Background(), body)
				return err
			},
		},
		{
			name: "rewind rejoin invalid receipt old primary id",
			call: func() error {
				body := validRejoin
				body.Receipt = StandbyFenceReceipt{
					Identity:         identity,
					OldPrimaryId:     "primary a",
					PromotedNodeId:   "standby-a",
					ParentTimelineId: 1,
					ParentEpoch:      1,
					NewTimelineId:    2,
					NewEpoch:         2,
					RequiredLsn:      9,
					ObservedLsn:      9,
					Generation:       1,
					Forced:           false,
					Token:            "token-a",
					Reason:           "test",
				}
				_, err := client.RewindRejoin(context.Background(), body)
				return err
			},
		},
		{
			name: "reseed rejoin invalid receipt promoted node id",
			call: func() error {
				body := validRejoin
				body.Receipt = StandbyFenceReceipt{
					Identity:         identity,
					OldPrimaryId:     "primary-a",
					PromotedNodeId:   strings.Repeat("a", 129),
					ParentTimelineId: 1,
					ParentEpoch:      1,
					NewTimelineId:    2,
					NewEpoch:         2,
					RequiredLsn:      9,
					ObservedLsn:      9,
					Generation:       1,
					Forced:           false,
					Token:            "token-a",
					Reason:           "test",
				}
				_, err := client.ReseedRejoin(context.Background(), body)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil || !strings.Contains(err.Error(), "invalid HA") {
				t.Fatalf("error = %v, want local invalid HA input error", err)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("server received %d requests for locally invalid slot names", got)
	}
}

func TestStandbyClientSeedWorkflowUsesAdminAPI(t *testing.T) {
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
		case HABaseBackupsPath:
			got := string(body)
			if !strings.Contains(got, `"slot_name":"standby-a"`) ||
				!strings.Contains(got, `"manifest_id":"manifest-a"`) {
				t.Fatalf("base backup begin body = %s, want slot and manifest", got)
			}
			_, _ = fmt.Fprint(w, standbyBaseBackupBeginResponseJSON())
		case HABaseBackupsFinishPath:
			got := string(body)
			if !strings.Contains(got, `"manifest_path":"/backup/manifest-a.json"`) {
				t.Fatalf("base backup finish body = %s, want manifest path", got)
			}
			_, _ = fmt.Fprint(w, standbyBaseBackupFinishResponseJSON())
		case HAStandbyBootstrapPath:
			got := string(body)
			if !strings.Contains(got, `"manifest_path":"/backup/manifest-a.json"`) ||
				!strings.Contains(got, `"content_root":"/backup/files"`) {
				t.Fatalf("standby bootstrap body = %s, want manifest path and content root", got)
			}
			_, _ = fmt.Fprint(w, standbyBootstrapResponseJSON())
		default:
			t.Fatalf("path = %s, want HA seed workflow endpoint", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithToken("test-token")

	begin, err := client.BeginBaseBackup(context.Background(), BaseBackupStartRequest{
		SlotName:   "standby-a",
		ManifestId: "manifest-a",
	})
	if err != nil {
		t.Fatalf("BeginBaseBackup returned error: %v", err)
	}
	if begin.Action.ActionKind != StandbyActionKindBaseBackupBegin ||
		begin.Action.NodeId != "primary-a" ||
		begin.BackupLsn != 7 ||
		begin.StartRecordLsn != 8 {
		t.Fatalf("begin base backup response = %#v, want primary begin evidence", begin)
	}

	finish, err := client.FinishBaseBackup(context.Background(), BaseBackupManifestPathRequest{
		ManifestPath: "/backup/manifest-a.json",
	})
	if err != nil {
		t.Fatalf("FinishBaseBackup returned error: %v", err)
	}
	if finish.Action.ActionKind != StandbyActionKindBaseBackupFinish ||
		finish.Action.NodeId != "primary-a" ||
		finish.BackupLsn != 7 ||
		finish.EndRecordLsn != 9 {
		t.Fatalf("finish base backup response = %#v, want primary finish evidence", finish)
	}

	bootstrap, err := client.BootstrapStandby(context.Background(), StandbyBootstrapRequest{
		ManifestPath: "/backup/manifest-a.json",
		ContentRoot:  "/backup/files",
	})
	if err != nil {
		t.Fatalf("BootstrapStandby returned error: %v", err)
	}
	if bootstrap.Action.ActionKind != StandbyActionKindStandbyBootstrap ||
		bootstrap.Action.NodeId != "standby-a" ||
		bootstrap.BackupLsn != 7 ||
		bootstrap.CheckpointLsn != 10 {
		t.Fatalf("standby bootstrap response = %#v, want standby bootstrap evidence", bootstrap)
	}
}

func TestStandbyClientSetStandbyUpstreamUsesAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != HAStandbyUpstreamPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAStandbyUpstreamPath)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll returned error: %v", err)
		}
		got := string(body)
		if !strings.Contains(got, `"upstream_url":"https://primary-b.example:5433"`) ||
			!strings.Contains(got, `"slot_name":"standby-a"`) ||
			!strings.Contains(got, `"reason":"switchover"`) {
			t.Fatalf("set standby upstream body = %s, want upstream url, slot, and reason", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyUpstreamResponseJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").SetStandbyUpstream(context.Background(), StandbyUpstreamRequest{
		Identity: StandbyIdentity{
			ClusterId:  100,
			ShardId:    10,
			TableId:    20,
			TimelineId: 4,
			Epoch:      6,
		},
		UpstreamUrl: "https://primary-b.example:5433",
		SlotName:    "standby-a",
		Reason:      "switchover",
	})
	if err != nil {
		t.Fatalf("SetStandbyUpstream returned error: %v", err)
	}
	if resp.Action.ActionKind != StandbyActionKindStandbyUpstream || resp.Action.NodeId != "standby-a" {
		t.Fatalf("standby upstream action = %#v, want standby upstream receipt", resp.Action)
	}
	if !resp.Changed || resp.Upstream.UpstreamUrl != "https://primary-b.example:5433" || resp.Upstream.SlotName != "standby-a" {
		t.Fatalf("standby upstream response = %#v, want changed upstream evidence", resp)
	}
	if resp.Previous.UpstreamUrl != "https://primary-a.example:5433" || resp.Previous.SlotName != "standby-a" {
		t.Fatalf("standby upstream previous = %#v, want prior upstream evidence", resp.Previous)
	}
}

func TestStandbyClientAcquireFenceUsesAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != HAFencePath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAFencePath)
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
		got := string(body)
		if !strings.Contains(got, `"old_primary_id":"primary-a"`) ||
			!strings.Contains(got, `"promoted_node_id":"standby-a"`) ||
			!strings.Contains(got, `"required_lsn":12`) ||
			!strings.Contains(got, `"observed_lsn":12`) ||
			!strings.Contains(got, `"new_timeline_id":5`) ||
			!strings.Contains(got, `"reason":"LeaseAcquired"`) {
			t.Fatalf("fence acquire body = %s, want primary, standby, LSN, timeline, and reason", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyFenceAcquireResponseJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").AcquireFence(context.Background(), FenceAcquireRequest{
		Identity: StandbyIdentity{
			ClusterId:  100,
			ShardId:    10,
			TableId:    20,
			TimelineId: 4,
			Epoch:      6,
		},
		OldPrimaryId:   "primary-a",
		PromotedNodeId: "standby-a",
		NewTimelineId:  5,
		NewEpoch:       7,
		Generation:     3,
		RequiredLsn:    12,
		ObservedLsn:    12,
		Reason:         "LeaseAcquired",
	})
	if err != nil {
		t.Fatalf("AcquireFence returned error: %v", err)
	}
	if resp.Action.ActionKind != StandbyActionKindFenceAcquire || resp.Action.NodeId != "standby-a" {
		t.Fatalf("fence action = %#v, want standby fence acquisition receipt", resp.Action)
	}
	if resp.Receipt.Generation != 3 ||
		resp.Receipt.Token != "ha-fence-token" ||
		resp.Receipt.Identity.TimelineId != 5 ||
		resp.Receipt.ParentTimelineId != 4 ||
		resp.Receipt.ObservedLsn != 12 {
		t.Fatalf("fence receipt = %#v, want promoted timeline fence evidence", resp.Receipt)
	}
}

func TestStandbyClientAcquireFenceAllowsOmittedGeneration(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll returned error: %v", err)
		}
		got := string(body)
		if strings.Contains(got, `"generation"`) {
			t.Fatalf("fence acquire body = %s, want generation omitted when unset", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyFenceAcquireResponseJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	// Generation is intentionally left at its zero value: the server
	// allocates the next generation when it is omitted from the request.
	_, err = client.AcquireFence(context.Background(), FenceAcquireRequest{
		Identity: StandbyIdentity{
			ClusterId:  100,
			ShardId:    10,
			TableId:    20,
			TimelineId: 4,
			Epoch:      6,
		},
		OldPrimaryId:   "primary-a",
		PromotedNodeId: "standby-a",
		NewTimelineId:  5,
		NewEpoch:       7,
		RequiredLsn:    12,
		ObservedLsn:    12,
	})
	if err != nil {
		t.Fatalf("AcquireFence with omitted generation returned error: %v", err)
	}
}

func TestStandbyClientPromoteWithCurrentFenceUsesAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != HAPromotionCurrentFencePath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPromotionCurrentFencePath)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q, want application/json", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyPromotionResponseJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").PromoteWithCurrentFence(context.Background())
	if err != nil {
		t.Fatalf("PromoteWithCurrentFence returned error: %v", err)
	}
	if resp.Action.ActionKind != StandbyActionKindPromotion || resp.Action.NodeId != "standby-a" {
		t.Fatalf("promotion action = %#v, want standby promotion receipt", resp.Action)
	}
	if resp.Promotion.NewIdentity.TimelineId != 5 || resp.Promotion.SwitchLsn != 13 {
		t.Fatalf("promotion result = %#v, want new timeline 5 and switch LSN 13", resp.Promotion)
	}
	if resp.FenceGeneration != 3 || resp.FenceToken != "ha-fence-token" {
		t.Fatalf("fence evidence = generation %d token %q, want generation 3 token ha-fence-token", resp.FenceGeneration, resp.FenceToken)
	}
}

func TestStandbyClientAssessPromotionUsesAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != HAPromotionAssessPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAPromotionAssessPath)
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
		got := string(body)
		if !strings.Contains(got, `"required_lsn":12`) ||
			!strings.Contains(got, `"fencing_confirmed":true`) ||
			!strings.Contains(got, `"force":false`) ||
			!strings.Contains(got, `"use_current_fence":true`) {
			t.Fatalf("promotion assess body = %s, want required LSN and fence mode fields", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyPromotionAssessResponseJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").AssessPromotion(context.Background(), PromotionAssessRequest{
		RequiredLsn:      12,
		FencingConfirmed: true,
		Force:            false,
		UseCurrentFence:  true,
	})
	if err != nil {
		t.Fatalf("AssessPromotion returned error: %v", err)
	}
	if resp.Action.ActionKind != StandbyActionKindPromotionAssess || resp.Action.State != StandbyActionStateAssessed {
		t.Fatalf("promotion assess action = %#v, want assessed promotion receipt", resp.Action)
	}
	if !resp.Assessment.CanPromote || !resp.Assessment.Safe || resp.Assessment.Mode != StandbyPromotionModeSafe {
		t.Fatalf("promotion assessment = %#v, want safe promotable assessment", resp.Assessment)
	}
	if resp.Assessment.RequiredLsn != 12 || resp.Assessment.ReceivedLsn != 12 || resp.Assessment.AppliedLsn != 12 {
		t.Fatalf("promotion assessment LSNs = %#v, want all at 12", resp.Assessment)
	}
}

func TestStandbyClientRewindRejoinUsesAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != HARejoinRewindPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HARejoinRewindPath)
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
		got := string(body)
		if !strings.Contains(got, `"node_id":"primary-a"`) ||
			!strings.Contains(got, `"last_lsn":13`) ||
			!strings.Contains(got, `"retained_from_lsn":8`) ||
			!strings.Contains(got, `"allow_rewind_after_forced_promotion":true`) {
			t.Fatalf("rejoin rewind body = %s, want node, LSN, retention, and force-rewind fields", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyRejoinRewindResponseJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").RewindRejoin(context.Background(), RejoinAssessRequest{
		NodeId:          "primary-a",
		LastLsn:         13,
		RetainedFromLsn: 8,
		Identity: StandbyIdentity{
			ClusterId:  100,
			ShardId:    10,
			TableId:    20,
			TimelineId: 4,
			Epoch:      6,
		},
		AllowRewindAfterForcedPromotion: true,
	})
	if err != nil {
		t.Fatalf("RewindRejoin returned error: %v", err)
	}
	if resp.Action.ActionKind != StandbyActionKindRejoinRewind || resp.Action.NodeId != "primary-a" {
		t.Fatalf("rejoin action = %#v, want former-primary rewind receipt", resp.Action)
	}
	if resp.Assessment.Action != StandbyRejoinActionRewind || resp.Assessment.Reason != StandbyRejoinReasonParentTimelineRetained {
		t.Fatalf("rejoin assessment = %#v, want rewind on retained parent timeline", resp.Assessment)
	}
	if resp.Rewind.NodeId != "primary-a" ||
		resp.Rewind.TargetTimelineId != 5 ||
		resp.Rewind.NextLsn != 13 ||
		!resp.Rewind.DataLossDiscarded {
		t.Fatalf("rejoin rewind result = %#v, want rewind evidence for primary-a", resp.Rewind)
	}
}

func TestStandbyClientReseedRejoinUsesAdminAPI(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != HARejoinReseedPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HARejoinReseedPath)
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
		got := string(body)
		if !strings.Contains(got, `"node_id":"primary-a"`) ||
			!strings.Contains(got, `"last_lsn":13`) ||
			!strings.Contains(got, `"retained_from_lsn":14`) {
			t.Fatalf("rejoin reseed body = %s, want former node, LSN, and expired retention boundary", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyRejoinReseedResponseJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").ReseedRejoin(context.Background(), RejoinAssessRequest{
		NodeId:          "primary-a",
		LastLsn:         13,
		RetainedFromLsn: 14,
		Identity: StandbyIdentity{
			ClusterId:  100,
			ShardId:    10,
			TableId:    20,
			TimelineId: 4,
			Epoch:      6,
		},
	})
	if err != nil {
		t.Fatalf("ReseedRejoin returned error: %v", err)
	}
	if resp.Action.ActionKind != StandbyActionKindRejoinReseed || resp.Action.NodeId != "primary-current" {
		t.Fatalf("rejoin action = %#v, want current-primary reseed receipt", resp.Action)
	}
	if resp.Assessment.Action != StandbyRejoinActionReseed || resp.Assessment.Reason != StandbyRejoinReasonParentTimelineWALExpired {
		t.Fatalf("rejoin assessment = %#v, want reseed after expired parent timeline WAL", resp.Assessment)
	}
	if resp.Reseed.NodeId != "primary-a" ||
		resp.Reseed.SlotName != "primary-a" ||
		resp.Reseed.TargetTimelineId != 5 ||
		!resp.Reseed.BaseBackupRequired ||
		!resp.Reseed.ReseedRequired {
		t.Fatalf("rejoin reseed result = %#v, want reseed evidence for primary-a", resp.Reseed)
	}
}

func TestStandbyClientWithTokenCanChangeAndClearBearerAuth(t *testing.T) {
	t.Parallel()

	expectedAuth := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != HAReplicationSlotsPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, HAReplicationSlotsPath)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q, want application/json", got)
		}
		if got, want := r.Header.Get("Authorization"), <-expectedAuth; got != want {
			t.Fatalf("Authorization = %q, want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"slots":[]}`)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}

	expectedAuth <- "Bearer first-token"
	if _, err := client.WithToken(" first-token ").ListReplicationSlots(context.Background()); err != nil {
		t.Fatalf("ListReplicationSlots with first token returned error: %v", err)
	}

	expectedAuth <- "Bearer second-token"
	if _, err := client.WithToken("second-token").ListReplicationSlots(context.Background()); err != nil {
		t.Fatalf("ListReplicationSlots with second token returned error: %v", err)
	}

	expectedAuth <- ""
	if _, err := client.WithToken("  ").ListReplicationSlots(context.Background()); err != nil {
		t.Fatalf("ListReplicationSlots with cleared token returned error: %v", err)
	}
}

func TestStandbyClientStatusWrappersExposeLagAndRetention(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q, want application/json", got)
		}

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case HAPrimaryStatusPath:
			query := r.URL.Query()
			syncStandbys := query["sync_standby"]
			if query.Get("max_lag_lsn") != "8" ||
				query.Get("max_retained_bytes") != "1024" ||
				query.Get("max_retained_age_ns") != "5000" ||
				query.Get("sync_mode") != "remote-write" ||
				query.Get("sync_selection") != "any" ||
				query.Get("sync_required") != "1" ||
				query.Get("sync_failure") != "fail-closed" ||
				len(syncStandbys) != 1 ||
				syncStandbys[0] != "standby-a" {
				t.Fatalf("primary status query = %s, want retention and sync policy params", r.URL.RawQuery)
			}
			_, _ = fmt.Fprint(w, standbyPrimaryStatusResponseJSON())
		case HAStandbyStatusPath:
			if got := r.URL.Query().Get("upstream_lsn"); got != "20" {
				t.Fatalf("standby upstream_lsn = %q, want 20", got)
			}
			_, _ = fmt.Fprint(w, standbyStatusResponseJSON())
		default:
			t.Fatalf("path = %s, want HA status endpoint", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithToken("test-token")

	primary, err := client.PrimaryStatus(context.Background(), &StandbyPrimaryStatusParams{
		MaxLagLsn:        8,
		MaxRetainedBytes: 1024,
		MaxRetainedAgeNs: 5000,
		SyncMode:         StandbyPrimaryStatusSyncModeRemoteWrite,
		SyncSelection:    StandbyPrimaryStatusSyncSelectionAny,
		SyncRequired:     1,
		SyncStandby:      []string{"standby-a"},
		SyncFailure:      StandbyPrimaryStatusSyncFailureFailClosed,
	})
	if err != nil {
		t.Fatalf("PrimaryStatus returned error: %v", err)
	}
	if primary.Snapshot.Retention.RetainedLsnCount != 8 ||
		primary.Snapshot.Retention.RetainedByteCount != 1024 ||
		primary.Snapshot.Slots[0].RetentionLagLsn != 8 ||
		primary.Snapshot.Slots[0].WriteLagLsn != 2 ||
		primary.Snapshot.Durability.Mode != StandbyDurabilityModeRemoteWrite {
		t.Fatalf("primary status = %#v, want retention, lag, and remote_write durability evidence", primary.Snapshot)
	}

	standby, err := client.StandbyStatus(context.Background(), &StandbyStatusParams{UpstreamLsn: 20})
	if err != nil {
		t.Fatalf("StandbyStatus returned error: %v", err)
	}
	if standby.Snapshot.UpstreamLsn != 20 ||
		standby.Snapshot.WriteLagLsn != 2 ||
		standby.Snapshot.ReceiveLagLsn != 2 ||
		standby.Snapshot.ApplyLagLsn != 3 ||
		standby.Snapshot.UnappliedLsnCount != 1 ||
		!standby.Snapshot.CanServeSafeReads ||
		standby.Snapshot.CaughtUpToReceived {
		t.Fatalf("standby status = %#v, want lag and freshness evidence", standby.Snapshot)
	}
}

func TestStandbyClientPublicAPIDoesNotExposeGeneratedClient(t *testing.T) {
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
			t.Fatalf("StandbyClient must not expose the generated oapi client through an exported Client method")
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

func TestStandbyClientCreateReplicationSlotRejectsMissingEvidence(t *testing.T) {
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

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.CreateReplicationSlot(context.Background(), ReplicationSlotCreateRequest{SlotName: "standby-a"})
	if err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("CreateReplicationSlot error = %v, want slot field evidence error", err)
	}
}

func TestStandbyClientGateOperationsUseAdminAPI(t *testing.T) {
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
			_, _ = fmt.Fprint(w, standbyCommitAppendResponseJSON())
		case "/admin/v1/ha/commit/check":
			if got := string(body); !strings.Contains(got, `"target_lsn":9`) ||
				!strings.Contains(got, `"failure_policy":"fail_closed"`) {
				t.Fatalf("commit check body = %s, want target_lsn and sync policy", got)
			}
			_, _ = fmt.Fprint(w, standbyCommitCheckResponseJSON())
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
			_, _ = fmt.Fprint(w, standbyWriteDecisionResponseJSON())
		case "/admin/v1/ha/owner-jobs/check":
			if got := string(body); !strings.Contains(got, `"kind":"compaction_publish"`) ||
				!strings.Contains(got, `"role":"primary"`) {
				t.Fatalf("owner job check body = %s, want kind and primary role", got)
			}
			_, _ = fmt.Fprint(w, standbyOwnerJobDecisionResponseJSON())
		default:
			t.Fatalf("path = %s, want HA gate endpoint", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithToken("test-token")
	syncPolicy := StandbySyncPolicy{
		Mode:          StandbySyncPolicyModeRemoteWrite,
		Selection:     StandbySyncPolicySelectionAny,
		Required:      1,
		FailurePolicy: StandbySyncPolicyFailureFailClosed,
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

func TestStandbyClientGateOperationsRejectInvalidTypedResponses(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/write/check" {
			t.Fatalf("path = %s, want /admin/v1/ha/write/check", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, strings.Replace(standbyWriteDecisionResponseJSON(), `"action":"reject_read_only_standby"`, `"action":"unknown"`, 1))
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.CheckWrite(context.Background(), WriteCheckRequest{Role: WriteCheckRoleStandby})
	if err == nil || !strings.Contains(err.Error(), "write decision fields") {
		t.Fatalf("CheckWrite error = %v, want write decision fields", err)
	}
}

func TestStandbyClientAcceptsAdminRootURL(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":false}`)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL+"/admin/v1", server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.CurrentFence(context.Background())
	if err != nil {
		t.Fatalf("CurrentFence returned error: %v", err)
	}
	if resp.Held {
		t.Fatalf("Held = true, want false")
	}
}

func TestStandbyClientAcceptsStandbyRootURL(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":false}`)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL+"/admin/v1/ha", server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.CurrentFence(context.Background())
	if err != nil {
		t.Fatalf("CurrentFence returned error: %v", err)
	}
	if resp.Held {
		t.Fatalf("Held = true, want false")
	}
}

func TestStandbyClientRejectsInvalidBaseURLs(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"  ",
		" http://ha-admin.test ",
		"http://ha admin.test",
		"http://ha-admin.test/\tadmin",
		"ha-admin.test",
		"file:///tmp/ha-admin",
	}

	for _, baseURL := range tests {
		t.Run(baseURL, func(t *testing.T) {
			t.Parallel()
			if _, err := NewStandbyClient(baseURL, nil); err == nil || !strings.Contains(err.Error(), "invalid HA admin base URL") {
				t.Fatalf("NewStandbyClient(%q) error = %v, want invalid HA admin base URL", baseURL, err)
			}
		})
	}
}

func TestStandbyClientCurrentFenceRejectsInvalidTypedResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":true}`)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.CurrentFence(context.Background())
	if err == nil || !strings.Contains(err.Error(), "current fence receipt fields") {
		t.Fatalf("CurrentFence error = %v, want current fence receipt fields", err)
	}
}

func TestStandbyClientCurrentFenceAcceptsEmptyReceiptReason(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":true,"receipt":{"identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":4,"epoch":5},"old_primary_id":"primary-a","promoted_node_id":"standby-a","parent_timeline_id":2,"parent_epoch":3,"new_timeline_id":4,"new_epoch":5,"required_lsn":8,"observed_lsn":8,"generation":9,"forced":false,"token":"fence-token","reason":""}}`)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	resp, err := client.CurrentFence(context.Background())
	if err != nil {
		t.Fatalf("CurrentFence returned error: %v", err)
	}
	if !resp.Held || resp.Receipt.Reason != "" {
		t.Fatalf("CurrentFence response = %#v, want held receipt with empty reason", resp)
	}
}

func TestStandbyClientGeneratedSpecIsDedicatedAdminAPI(t *testing.T) {
	t.Parallel()

	spec, err := oapi.GetSwagger()
	if err != nil {
		t.Fatalf("GetSwagger returned error: %v", err)
	}
	if spec.Info == nil || spec.Info.Title != "Antfly Admin API" {
		t.Fatalf("spec title = %#v, want Antfly Admin API", spec.Info)
	}
	if len(spec.Servers) != 1 || spec.Servers[0].URL != AdminV1Path {
		t.Fatalf("servers = %#v, want single %s server", spec.Servers, AdminV1Path)
	}
	if len(spec.Security) != 1 {
		t.Fatalf("security requirements = %#v, want one BearerAuth requirement", spec.Security)
	}
	if _, ok := spec.Security[0]["BearerAuth"]; !ok {
		t.Fatalf("security requirements = %#v, want BearerAuth", spec.Security)
	}
	bearer := spec.Components.SecuritySchemes["BearerAuth"]
	if bearer == nil || bearer.Value == nil ||
		bearer.Value.Type != "http" ||
		bearer.Value.Scheme != "bearer" {
		t.Fatalf("BearerAuth security scheme = %#v, want http bearer", bearer)
	}
	pathItem := spec.Paths.Find("/standby/primary/status")
	if pathItem == nil || pathItem.Get == nil {
		t.Fatalf("/standby/primary/status operation = %#v, want GET operation", pathItem)
	}
	req, err := oapi.NewGetHAPrimaryStatusRequest("http://admin.test"+AdminV1Path+"/", nil)
	if err != nil {
		t.Fatalf("NewGetHAPrimaryStatusRequest returned error: %v", err)
	}
	if req.Method != http.MethodGet || req.URL.Path != StandbyPrimaryStatusPath {
		t.Fatalf("generated primary status request = %s %s, want GET %s", req.Method, req.URL.Path, StandbyPrimaryStatusPath)
	}

	sourceSpec := loadSourceAdminOpenAPISpec(t)
	sourceOperations := standbyOpenAPIOperations(sourceSpec)
	generatedOperations := standbyOpenAPIOperations(spec)
	for key, sourceOperationID := range sourceOperations {
		generatedOperationID, ok := generatedOperations[key]
		if !ok {
			t.Fatalf("source admin OpenAPI operation %s is missing from generated Go admin spec", key)
		}
		if !strings.EqualFold(generatedOperationID, sourceOperationID) {
			t.Fatalf("generated Go admin operation %s has operationId %q, want generated form of %q", key, generatedOperationID, sourceOperationID)
		}
	}
	for key := range generatedOperations {
		if _, ok := sourceOperations[key]; !ok {
			t.Fatalf("generated Go admin spec contains operation %s that is missing from source admin OpenAPI spec", key)
		}
	}
}

func loadSourceAdminOpenAPISpec(t *testing.T) *openapi3.T {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate test file")
	}
	specPath := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../../specs/openapi/antfly/admin.yaml"))
	if _, err := os.Stat(specPath); err != nil {
		t.Fatalf("stat source admin OpenAPI spec %s: %v", specPath, err)
	}
	loader := openapi3.NewLoader()
	spec, err := loader.LoadFromFile(specPath)
	if err != nil {
		t.Fatalf("load source admin OpenAPI spec %s: %v", specPath, err)
	}
	return spec
}

func standbyOpenAPIOperations(spec *openapi3.T) map[string]string {
	operations := map[string]string{}
	if spec == nil || spec.Paths == nil {
		return operations
	}
	for path, pathItem := range spec.Paths.Map() {
		if pathItem == nil {
			continue
		}
		for method, operation := range pathItem.Operations() {
			if operation == nil {
				continue
			}
			operations[strings.ToUpper(method)+" "+path] = operation.OperationID
		}
	}
	return operations
}

func TestStandbyOperationMetadataUsesAdminAPIPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		got       StandbyOperation
		generated func(*testing.T) StandbyOperation
	}{
		{
			name: "primary status",
			got:  StandbyPrimaryStatusOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewGetHAPrimaryStatusRequest(server, nil)
			}),
		},
		{
			name: "standby status",
			got:  StandbyStatusOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewGetHAStandbyStatusRequest(server, nil)
			}),
		},
		{
			name: "check commit",
			got:  StandbyCheckCommitOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHACommitRequest(server, oapi.CheckHACommitJSONRequestBody{})
			}),
		},
		{
			name: "append commit",
			got:  StandbyAppendCommitOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewAppendHACommitRequest(server, oapi.AppendHACommitJSONRequestBody{})
			}),
		},
		{
			name: "check read",
			got:  StandbyCheckReadOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHAReadRequest(server, oapi.CheckHAReadJSONRequestBody{})
			}),
		},
		{
			name: "check write",
			got:  StandbyCheckWriteOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHAWriteRequest(server, oapi.CheckHAWriteJSONRequestBody{})
			}),
		},
		{
			name: "check owner job",
			got:  StandbyCheckOwnerJobOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHAOwnerJobRequest(server, oapi.CheckHAOwnerJobJSONRequestBody{})
			}),
		},
		{
			name: "list replication slots",
			got:  StandbyListReplicationSlotsOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewListHAReplicationSlotsRequest(server)
			}),
		},
		{
			name: "create replication slot",
			got:  StandbyCreateReplicationSlotOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewCreateHAReplicationSlotRequest(server, oapi.CreateHAReplicationSlotJSONRequestBody{})
			}),
		},
		{
			name: "begin base backup",
			got:  StandbyBeginBaseBackupOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewBeginHABaseBackupRequest(server, oapi.BeginHABaseBackupJSONRequestBody{})
			}),
		},
		{
			name: "finish base backup",
			got:  StandbyFinishBaseBackupOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewFinishHABaseBackupRequest(server, oapi.FinishHABaseBackupJSONRequestBody{})
			}),
		},
		{
			name: "capture seed artifact",
			got:  StandbySeedCaptureOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewCaptureHASeedArtifactRequest(server, oapi.CaptureHASeedArtifactJSONRequestBody{})
			}),
		},
		{
			name: "activate seeded slot",
			got:  StandbyActivateSeededSlotOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewActivateHASeededSlotRequest(server, oapi.ActivateHASeededSlotJSONRequestBody{})
			}),
		},
		{
			name: "bootstrap standby",
			got:  StandbyBootstrapStandbyOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewBootstrapHAStandbyRequest(server, oapi.BootstrapHAStandbyJSONRequestBody{})
			}),
		},
		{
			name: "set standby upstream",
			got:  StandbySetStandbyUpstreamOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewSetHAStandbyUpstreamRequest(server, oapi.SetHAStandbyUpstreamJSONRequestBody{})
			}),
		},
		{
			name: "acquire fence",
			got:  StandbyAcquireFenceOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewAcquireHAFenceRequest(server, oapi.AcquireHAFenceJSONRequestBody{})
			}),
		},
		{
			name: "current fence",
			got:  StandbyCurrentFenceOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewGetHACurrentFenceRequest(server)
			}),
		},
		{
			name: "assess promotion",
			got:  StandbyAssessPromotionOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewAssessHAPromotionRequest(server, oapi.AssessHAPromotionJSONRequestBody{})
			}),
		},
		{
			name: "promote with current fence",
			got:  StandbyPromoteWithCurrentFenceOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewPromoteHAWithCurrentFenceRequest(server)
			}),
		},
		{
			name: "promote",
			got:  StandbyPromoteOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewPromoteHARequest(server, oapi.PromoteHAJSONRequestBody{})
			}),
		},
		{
			name: "assess rejoin",
			got:  StandbyAssessRejoinOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewAssessHARejoinRequest(server, oapi.AssessHARejoinJSONRequestBody{})
			}),
		},
		{
			name: "rewind rejoin",
			got:  StandbyRewindRejoinOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewRewindHARejoinRequest(server, oapi.RewindHARejoinJSONRequestBody{})
			}),
		},
		{
			name: "reseed rejoin",
			got:  StandbyReseedRejoinOperation(),
			generated: generatedStandbyOperation(func(server string) (*http.Request, error) {
				return oapi.NewReseedHARejoinRequest(server, oapi.ReseedHARejoinJSONRequestBody{})
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			want := tt.generated(t)
			if tt.got != want {
				t.Fatalf("operation = %#v, want generated OpenAPI operation %#v", tt.got, want)
			}
		})
	}

	// The deprecated HA*Operation wrappers must keep returning the legacy
	// /admin/v1/ha paths verbatim, since they describe what PathStyleLegacy
	// (the default) actually sends and existing callers depend on the exact
	// value.
	legacyTests := []struct {
		name string
		got  StandbyOperation
		want StandbyOperation
	}{
		{"primary status", HAPrimaryStatusOperation(), StandbyOperation{Method: http.MethodGet, Path: HAPrimaryStatusPath}},
		{"standby status", HAStandbyStatusOperation(), StandbyOperation{Method: http.MethodGet, Path: HAStandbyStatusPath}},
		{"check commit", HACheckCommitOperation(), StandbyOperation{Method: http.MethodPost, Path: HACommitCheckPath}},
		{"append commit", HAAppendCommitOperation(), StandbyOperation{Method: http.MethodPost, Path: HACommitAppendPath}},
		{"check read", HACheckReadOperation(), StandbyOperation{Method: http.MethodPost, Path: HAReadCheckPath}},
		{"check write", HACheckWriteOperation(), StandbyOperation{Method: http.MethodPost, Path: HAWriteCheckPath}},
		{"check owner job", HACheckOwnerJobOperation(), StandbyOperation{Method: http.MethodPost, Path: HAOwnerJobCheckPath}},
		{"list replication slots", HAListReplicationSlotsOperation(), StandbyOperation{Method: http.MethodGet, Path: HAReplicationSlotsPath}},
		{"create replication slot", HACreateReplicationSlotOperation(), StandbyOperation{Method: http.MethodPost, Path: HAReplicationSlotsPath}},
		{"begin base backup", HABeginBaseBackupOperation(), StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsPath}},
		{"finish base backup", HAFinishBaseBackupOperation(), StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsFinishPath}},
		{"capture seed artifact", HASeedCaptureOperation(), StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsCapturePath}},
		{"activate seeded slot", HAActivateSeededSlotOperation(), StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsActivatePath}},
		{"bootstrap standby", HABootstrapStandbyOperation(), StandbyOperation{Method: http.MethodPost, Path: HAStandbyBootstrapPath}},
		{"set standby upstream", HASetStandbyUpstreamOperation(), StandbyOperation{Method: http.MethodPost, Path: HAStandbyUpstreamPath}},
		{"acquire fence", HAAcquireFenceOperation(), StandbyOperation{Method: http.MethodPost, Path: HAFencePath}},
		{"current fence", HACurrentFenceOperation(), StandbyOperation{Method: http.MethodGet, Path: HAFenceCurrentPath}},
		{"assess promotion", HAAssessPromotionOperation(), StandbyOperation{Method: http.MethodPost, Path: HAPromotionAssessPath}},
		{"promote with current fence", HAPromoteWithCurrentFenceOperation(), StandbyOperation{Method: http.MethodPost, Path: HAPromotionCurrentFencePath}},
		{"promote", HAPromoteOperation(), StandbyOperation{Method: http.MethodPost, Path: HAPromotionPath}},
		{"assess rejoin", HAAssessRejoinOperation(), StandbyOperation{Method: http.MethodPost, Path: HARejoinAssessPath}},
		{"rewind rejoin", HARewindRejoinOperation(), StandbyOperation{Method: http.MethodPost, Path: HARejoinRewindPath}},
		{"reseed rejoin", HAReseedRejoinOperation(), StandbyOperation{Method: http.MethodPost, Path: HARejoinReseedPath}},
	}
	for _, tt := range legacyTests {
		t.Run("legacy alias/"+tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.got != tt.want {
				t.Fatalf("legacy operation = %#v, want %#v", tt.got, tt.want)
			}
		})
	}

	const slotName = "standby-a.1:zone_9"

	slotPath, ok := StandbyReplicationSlotPath(slotName)
	if !ok {
		t.Fatal("StandbyReplicationSlotPath returned ok=false for valid slot")
	}
	if slotPath != StandbyReplicationSlotPathPrefix+url.PathEscape(slotName) {
		t.Fatalf("slot path = %q, want escaped valid slot path", slotPath)
	}
	generatedDrop := generatedStandbyOperation(func(server string) (*http.Request, error) {
		return oapi.NewDropHAReplicationSlotRequest(server, slotName)
	})(t)
	if dropPath := (StandbyOperation{Method: http.MethodDelete, Path: slotPath}); dropPath != generatedDrop {
		t.Fatalf("drop slot path operation = %#v, want generated OpenAPI operation %#v", dropPath, generatedDrop)
	}
	resume, ok := StandbyResumeReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("StandbyResumeReplicationSlotOperation returned ok=false")
	}
	if want := generatedStandbyOperation(func(server string) (*http.Request, error) {
		return oapi.NewResumeHAReplicationSlotRequest(server, slotName)
	})(t); resume != want {
		t.Fatalf("resume operation = %#v, want generated OpenAPI operation %#v", resume, want)
	}
	pause, ok := StandbyPauseReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("StandbyPauseReplicationSlotOperation returned ok=false")
	}
	if want := generatedStandbyOperation(func(server string) (*http.Request, error) {
		return oapi.NewPauseHAReplicationSlotRequest(server, slotName)
	})(t); pause != want {
		t.Fatalf("pause operation = %#v, want generated OpenAPI operation %#v", pause, want)
	}
	drop, ok := StandbyDropReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("StandbyDropReplicationSlotOperation returned ok=false")
	}
	if drop != generatedDrop {
		t.Fatalf("drop operation = %#v, want generated OpenAPI operation %#v", drop, generatedDrop)
	}

	// Legacy replication slot path/operation helpers must keep returning the
	// legacy /admin/v1/ha/replication-slots/... paths verbatim.
	legacySlotPath, ok := HAReplicationSlotPath(slotName)
	if !ok {
		t.Fatal("HAReplicationSlotPath returned ok=false for valid slot")
	}
	if legacySlotPath != HAReplicationSlotPathPrefix+url.PathEscape(slotName) {
		t.Fatalf("legacy slot path = %q, want escaped legacy slot path", legacySlotPath)
	}
	legacyResume, ok := HAResumeReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("HAResumeReplicationSlotOperation returned ok=false")
	}
	if want := (StandbyOperation{Method: http.MethodPut, Path: legacySlotPath + HAReplicationSlotResumePathSuffix}); legacyResume != want {
		t.Fatalf("legacy resume operation = %#v, want %#v", legacyResume, want)
	}
	legacyPause, ok := HAPauseReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("HAPauseReplicationSlotOperation returned ok=false")
	}
	if want := (StandbyOperation{Method: http.MethodPut, Path: legacySlotPath + HAReplicationSlotPausePathSuffix}); legacyPause != want {
		t.Fatalf("legacy pause operation = %#v, want %#v", legacyPause, want)
	}
	legacyDrop, ok := HADropReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("HADropReplicationSlotOperation returned ok=false")
	}
	if want := (StandbyOperation{Method: http.MethodDelete, Path: legacySlotPath}); legacyDrop != want {
		t.Fatalf("legacy drop operation = %#v, want %#v", legacyDrop, want)
	}

	invalidSlots := []string{
		"",
		" ",
		" standby-a",
		"standby-a ",
		"standby a",
		"standby/a",
		"standby%",
		strings.Repeat("a", 129),
	}
	for _, invalid := range invalidSlots {
		if path, ok := StandbyReplicationSlotPath(invalid); ok {
			t.Fatalf("StandbyReplicationSlotPath(%q) = %q, true; want false", invalid, path)
		}
		if operation, ok := StandbyResumeReplicationSlotOperation(invalid); ok {
			t.Fatalf("StandbyResumeReplicationSlotOperation(%q) = %#v, true; want false", invalid, operation)
		}
		if operation, ok := StandbyPauseReplicationSlotOperation(invalid); ok {
			t.Fatalf("StandbyPauseReplicationSlotOperation(%q) = %#v, true; want false", invalid, operation)
		}
		if operation, ok := StandbyDropReplicationSlotOperation(invalid); ok {
			t.Fatalf("StandbyDropReplicationSlotOperation(%q) = %#v, true; want false", invalid, operation)
		}
		if path, ok := HAReplicationSlotPath(invalid); ok {
			t.Fatalf("HAReplicationSlotPath(%q) = %q, true; want false", invalid, path)
		}
		if operation, ok := HAResumeReplicationSlotOperation(invalid); ok {
			t.Fatalf("HAResumeReplicationSlotOperation(%q) = %#v, true; want false", invalid, operation)
		}
		if operation, ok := HAPauseReplicationSlotOperation(invalid); ok {
			t.Fatalf("HAPauseReplicationSlotOperation(%q) = %#v, true; want false", invalid, operation)
		}
		if operation, ok := HADropReplicationSlotOperation(invalid); ok {
			t.Fatalf("HADropReplicationSlotOperation(%q) = %#v, true; want false", invalid, operation)
		}
	}
}

func generatedStandbyOperation(build func(string) (*http.Request, error)) func(*testing.T) StandbyOperation {
	return func(t *testing.T) StandbyOperation {
		t.Helper()
		req, err := build("http://admin.test" + AdminV1Path + "/")
		if err != nil {
			t.Fatalf("generated OpenAPI request builder returned error: %v", err)
		}
		return StandbyOperation{Method: req.Method, Path: req.URL.EscapedPath()}
	}
}

func TestStandbyReceiptExpectationsUseAdminAPIEnums(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		got       StandbyReceiptExpectation
		wantKind  string
		wantState string
	}{
		{
			name:      "create replication slot",
			got:       StandbyReplicationSlotCreateReceiptExpectation(),
			wantKind:  "replication_slot_create",
			wantState: "applied",
		},
		{
			name:      "resume replication slot",
			got:       StandbyReplicationSlotResumeReceiptExpectation(),
			wantKind:  "replication_slot_resume",
			wantState: "applied",
		},
		{
			name:      "pause replication slot",
			got:       StandbyReplicationSlotPauseReceiptExpectation(),
			wantKind:  "replication_slot_pause",
			wantState: "applied",
		},
		{
			name:      "drop replication slot",
			got:       StandbyReplicationSlotDropReceiptExpectation(),
			wantKind:  "replication_slot_drop",
			wantState: "applied",
		},
		{
			name:      "begin base backup",
			got:       StandbyBaseBackupBeginReceiptExpectation(),
			wantKind:  "base_backup_begin",
			wantState: "applied",
		},
		{
			name:      "finish base backup",
			got:       StandbyBaseBackupFinishReceiptExpectation(),
			wantKind:  "base_backup_finish",
			wantState: "applied",
		},
		{
			name:      "capture seed artifact",
			got:       StandbySeedCaptureReceiptExpectation(),
			wantKind:  "seed_capture",
			wantState: "applied",
		},
		{
			name:      "bootstrap standby",
			got:       StandbyBootstrapReceiptExpectation(),
			wantKind:  "standby_bootstrap",
			wantState: "applied",
		},
		{
			name:      "set standby upstream",
			got:       StandbyUpstreamReceiptExpectation(),
			wantKind:  "standby_upstream",
			wantState: "applied",
		},
		{
			name:      "acquire fence",
			got:       StandbyFenceAcquireReceiptExpectation(),
			wantKind:  "fence_acquire",
			wantState: "applied",
		},
		{
			name:      "assess promotion",
			got:       StandbyPromotionAssessReceiptExpectation(),
			wantKind:  "promotion_assess",
			wantState: "assessed",
		},
		{
			name:      "promote",
			got:       StandbyPromotionReceiptExpectation(),
			wantKind:  "promotion",
			wantState: "applied",
		},
		{
			name:      "assess rejoin",
			got:       StandbyRejoinAssessReceiptExpectation(),
			wantKind:  "rejoin_assess",
			wantState: "assessed",
		},
		{
			name:      "rewind rejoin",
			got:       StandbyRejoinRewindReceiptExpectation(),
			wantKind:  "rejoin_rewind",
			wantState: "applied",
		},
		{
			name:      "reseed rejoin",
			got:       StandbyRejoinReseedReceiptExpectation(),
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

func TestStandbyReceiptMatchesExpectedOperationAndTarget(t *testing.T) {
	t.Parallel()

	expectation := StandbyReplicationSlotCreateReceiptExpectation()
	receipt := StandbyActionReceipt{
		ActionId:   "replication_slot_create:standby-a",
		ActionKind: StandbyActionKindReplicationSlotCreate,
		Target:     "standby-a",
		State:      StandbyActionStateApplied,
		NodeId:     "primary-a",
	}
	if !StandbyReceiptMatches(receipt, expectation, "standby-a") {
		t.Fatalf("StandbyReceiptMatches returned false for exact matching receipt")
	}
	receipt.State = StandbyActionStateAlreadyApplied
	if !StandbyReceiptMatches(receipt, expectation, "standby-a") {
		t.Fatalf("StandbyReceiptMatches returned false for already-applied idempotent receipt")
	}
	receipt.State = StandbyActionStateApplied
	receipt.Target = "standby-b"
	if StandbyReceiptMatches(receipt, expectation, "standby-a") {
		t.Fatalf("StandbyReceiptMatches returned true for mismatched target")
	}
	receipt.Target = "standby-a"
	if StandbyReceiptMatches(receipt, expectation, "") {
		t.Fatalf("StandbyReceiptMatches returned true with empty expected target")
	}
}

func TestStandbyReceiptMatchesNode(t *testing.T) {
	t.Parallel()

	expectation := StandbyReplicationSlotResumeReceiptExpectation()
	receipt := StandbyActionReceipt{
		ActionId:   "replication_slot_resume:standby-a",
		ActionKind: StandbyActionKindReplicationSlotResume,
		Target:     "standby-a",
		State:      StandbyActionStateApplied,
		NodeId:     "primary-a",
	}
	if !StandbyReceiptMatchesNode(receipt, expectation, "standby-a", "primary-a", true) {
		t.Fatalf("StandbyReceiptMatchesNode returned false for exact matching node")
	}
	if StandbyReceiptMatchesNode(receipt, expectation, "standby-a", "primary-b", true) {
		t.Fatalf("StandbyReceiptMatchesNode returned true for mismatched node")
	}
	if StandbyReceiptMatchesNode(receipt, expectation, "standby-a", "", true) {
		t.Fatalf("StandbyReceiptMatchesNode returned true without required expected node")
	}
	if !StandbyReceiptMatchesNode(receipt, expectation, "standby-a", "", false) {
		t.Fatalf("StandbyReceiptMatchesNode returned false for optional expected node")
	}
	receipt.NodeId = ""
	if StandbyReceiptMatchesNode(receipt, expectation, "standby-a", "", false) {
		t.Fatalf("StandbyReceiptMatchesNode returned true without receipt node id")
	}
}

func TestValidateStandbyReplicationSlotActionResponse(t *testing.T) {
	t.Parallel()

	slot := StandbyReplicationSlot{
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
	response := StandbyReplicationSlotActionResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "replication_slot_create:standby-a",
			ActionKind: StandbyActionKindReplicationSlotCreate,
			Target:     "standby-a",
			State:      StandbyActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotAction: StandbyReplicationSlotActionCreate,
		Slot:       slot,
	}
	if err := ValidateStandbyReplicationSlotActionResponse(response); err != nil {
		t.Fatalf("ValidateStandbyReplicationSlotActionResponse returned error: %v", err)
	}
	wrongSlotTarget := response
	wrongSlotTarget.Action.Target = "standby-b"
	if err := ValidateStandbyReplicationSlotActionResponse(wrongSlotTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong slot target error = %v, want receipt mismatch", err)
	}
	paddedSlotTarget := response
	paddedSlotTarget.Action.Target = " standby-a"
	if err := ValidateStandbyReplicationSlotActionResponse(paddedSlotTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("padded slot target error = %v, want receipt mismatch", err)
	}
	paddedActionID := response
	paddedActionID.Action.ActionId = "replication_slot_create:standby-a "
	if err := ValidateStandbyReplicationSlotActionResponse(paddedActionID); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("padded action id error = %v, want receipt mismatch", err)
	}
	wrongSlotKind := response
	wrongSlotKind.Action.ActionKind = StandbyActionKindReplicationSlotPause
	wrongSlotKind.Action.ActionId = "replication_slot_pause:standby-a"
	if err := ValidateStandbyReplicationSlotActionResponse(wrongSlotKind); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong slot action kind error = %v, want receipt mismatch", err)
	}
	if err := ValidateStandbyReplicationSlotListResponse(StandbyReplicationSlotListResponse{
		SchemaVersion: 1,
		Slots:         []StandbyReplicationSlot{slot},
	}); err != nil {
		t.Fatalf("ValidateStandbyReplicationSlotListResponse returned error: %v", err)
	}
	badListSlot := slot
	badListSlot.SlotName = ""
	if err := ValidateStandbyReplicationSlotListResponse(StandbyReplicationSlotListResponse{
		SchemaVersion: 1,
		Slots:         []StandbyReplicationSlot{badListSlot},
	}); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("invalid slot list error = %v, want slot fields error", err)
	}
	badListSlot.SlotName = "standby a"
	if err := ValidateStandbyReplicationSlotListResponse(StandbyReplicationSlotListResponse{
		SchemaVersion: 1,
		Slots:         []StandbyReplicationSlot{badListSlot},
	}); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("invalid slot name error = %v, want slot fields error", err)
	}

	response.Action.NodeId = ""
	if err := ValidateStandbyReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("missing node id error = %v, want receipt error", err)
	}
	response.Action.NodeId = "primary a"
	if err := ValidateStandbyReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("invalid node id error = %v, want receipt error", err)
	}
	response.Action.NodeId = "primary-a"

	response.SlotAction = StandbyReplicationSlotAction("invalid")
	if err := ValidateStandbyReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "invalid replication slot action") {
		t.Fatalf("invalid slot action error = %v, want invalid action error", err)
	}
	response.SlotAction = StandbyReplicationSlotActionCreate

	response.Slot.SlotName = "standby a"
	if err := ValidateStandbyReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("invalid slot name error = %v, want slot fields error", err)
	}
	response.Slot.SlotName = "standby-a"

	response.Slot.TimelineId = 0
	if err := ValidateStandbyReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("missing slot fields error = %v, want slot fields error", err)
	}
}

func TestValidateStandbySeedActionResponses(t *testing.T) {
	t.Parallel()

	begin := StandbyBaseBackupBeginResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "base_backup_begin:manifest-a",
			ActionKind: StandbyActionKindBaseBackupBegin,
			Target:     "manifest-a",
			State:      StandbyActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotName:       "standby-a",
		ManifestId:     "manifest-a",
		BackupLsn:      7,
		StartRecordLsn: 8,
	}
	if err := ValidateStandbyBaseBackupBeginResponse(begin); err != nil {
		t.Fatalf("ValidateStandbyBaseBackupBeginResponse returned error: %v", err)
	}
	wrongBeginTarget := begin
	wrongBeginTarget.Action.Target = "manifest-b"
	if err := ValidateStandbyBaseBackupBeginResponse(wrongBeginTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong begin target error = %v, want receipt mismatch", err)
	}
	begin.StartRecordLsn = 0
	if err := ValidateStandbyBaseBackupBeginResponse(begin); err == nil || !strings.Contains(err.Error(), "start_record_lsn") {
		t.Fatalf("missing start_record_lsn error = %v, want start_record_lsn error", err)
	}

	finish := StandbyBaseBackupFinishResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "base_backup_finish:manifest-a",
			ActionKind: StandbyActionKindBaseBackupFinish,
			Target:     "manifest-a",
			State:      StandbyActionStateApplied,
			NodeId:     "primary-a",
		},
		ManifestId:   "manifest-a",
		BackupLsn:    7,
		EndRecordLsn: 9,
	}
	if err := ValidateStandbyBaseBackupFinishResponse(finish); err != nil {
		t.Fatalf("ValidateStandbyBaseBackupFinishResponse returned error: %v", err)
	}
	wrongFinishKind := finish
	wrongFinishKind.Action.ActionKind = StandbyActionKindBaseBackupBegin
	wrongFinishKind.Action.ActionId = "base_backup_begin:manifest-a"
	if err := ValidateStandbyBaseBackupFinishResponse(wrongFinishKind); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong finish kind error = %v, want receipt mismatch", err)
	}
	finish.EndRecordLsn = 0
	if err := ValidateStandbyBaseBackupFinishResponse(finish); err == nil || !strings.Contains(err.Error(), "end_record_lsn") {
		t.Fatalf("missing end_record_lsn error = %v, want end_record_lsn error", err)
	}

	digest := strings.Repeat("a", 64)
	capture := StandbySeedArtifactCaptureResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "seed_capture:seed-standby-a-7",
			ActionKind: StandbyActionKindSeedCapture,
			Target:     "seed-standby-a-7",
			State:      StandbyActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotName:             "standby-a",
		Generation:           "seed-standby-a-7",
		TopologyId:           "topology-a",
		TopologyGeneration:   7,
		NodeId:               "standby-a",
		TargetPvcName:        "standby-a-data",
		TargetPvcUid:         "pvc-uid-7",
		ClusterId:            1,
		TimelineId:           4,
		Epoch:                2,
		ManifestId:           "seed-standby-a-7",
		SourcePlanSha256:     digest,
		BackupLsn:            7,
		CheckpointLsn:        7,
		EndRecordLsn:         8,
		ManifestSha256:       digest,
		CaptureReceiptSha256: digest,
		FileCount:            2,
		TotalBytes:           20,
		GenerationRoot:       "/antflydb/ha/seed-captures/generations/seed-standby-a-7",
		ContentRoot:          "/antflydb/ha/seed-captures/generations/seed-standby-a-7/content",
		ManifestPath:         "/antflydb/ha/seed-captures/generations/seed-standby-a-7/manifest.afha",
	}
	if err := ValidateStandbySeedArtifactCaptureResponse(capture); err != nil {
		t.Fatalf("ValidateStandbySeedArtifactCaptureResponse returned error: %v", err)
	}
	badCapture := capture
	badCapture.SourcePlanSha256 = strings.ToUpper(digest)
	if err := ValidateStandbySeedArtifactCaptureResponse(badCapture); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("invalid capture digest error = %v, want digest error", err)
	}

	activation := StandbySeededSlotActivateResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "seeded_slot_activate:seed-standby-a-7",
			ActionKind: StandbyActionKindSeededSlotActivate,
			Target:     "seed-standby-a-7",
			State:      StandbyActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotName:             "standby-a",
		Generation:           "seed-standby-a-7",
		ManifestId:           "manifest-a",
		TimelineId:           4,
		CheckpointLsn:        10,
		SeedReceiptSha256:    digest,
		CaptureReceiptSha256: digest,
		ManifestSha256:       digest,
		AggregateSha256:      digest,
	}
	if err := ValidateStandbySeededSlotActivateResponse(activation); err != nil {
		t.Fatalf("ValidateStandbySeededSlotActivateResponse returned error: %v", err)
	}
	wrongActivationTarget := activation
	wrongActivationTarget.Action.Target = "seed-standby-a-8"
	if err := ValidateStandbySeededSlotActivateResponse(wrongActivationTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong activation target error = %v, want receipt mismatch", err)
	}
	activation.SeedReceiptSha256 = strings.Repeat("A", 64)
	if err := ValidateStandbySeededSlotActivateResponse(activation); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("invalid activation digest error = %v, want digest error", err)
	}

	bootstrap := StandbyBootstrapResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "standby_bootstrap:manifest-a",
			ActionKind: StandbyActionKindStandbyBootstrap,
			Target:     "manifest-a",
			State:      StandbyActionStateApplied,
			NodeId:     "standby-a",
		},
		ManifestId:    "manifest-a",
		BackupLsn:     7,
		CheckpointLsn: 10,
	}
	if err := ValidateStandbyBootstrapResponse(bootstrap); err != nil {
		t.Fatalf("ValidateStandbyBootstrapResponse returned error: %v", err)
	}
	wrongBootstrapTarget := bootstrap
	wrongBootstrapTarget.Action.Target = "manifest-b"
	if err := ValidateStandbyBootstrapResponse(wrongBootstrapTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong bootstrap target error = %v, want receipt mismatch", err)
	}
	bootstrap.CheckpointLsn = 0
	if err := ValidateStandbyBootstrapResponse(bootstrap); err == nil || !strings.Contains(err.Error(), "checkpoint_lsn") {
		t.Fatalf("missing checkpoint_lsn error = %v, want checkpoint_lsn error", err)
	}
}

func TestValidateStandbyFenceResponse(t *testing.T) {
	t.Parallel()

	receipt := StandbyFenceReceipt{
		Identity: StandbyIdentity{
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
	response := StandbyFenceResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "fence_acquire:standby-a",
			ActionKind: StandbyActionKindFenceAcquire,
			Target:     "standby-a",
			State:      StandbyActionStateApplied,
			NodeId:     "standby-a",
		},
		Receipt: receipt,
	}
	if err := ValidateStandbyFenceResponse(response); err != nil {
		t.Fatalf("ValidateStandbyFenceResponse returned error: %v", err)
	}
	formerPrimaryCopy := response
	formerPrimaryCopy.Action.NodeId = "primary-a"
	if err := ValidateStandbyFenceResponse(formerPrimaryCopy); err != nil {
		t.Fatalf("ValidateStandbyFenceResponse rejected former-primary receipt copy: %v", err)
	}
	emptyReason := response
	emptyReason.Receipt.Reason = ""
	if err := ValidateStandbyFenceResponse(emptyReason); err != nil {
		t.Fatalf("ValidateStandbyFenceResponse with empty reason returned error: %v", err)
	}
	wrongActionNode := response
	wrongActionNode.Action.NodeId = "standby-b"
	if err := ValidateStandbyFenceResponse(wrongActionNode); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("wrong fence action node error = %v, want action node mismatch", err)
	}
	paddedActionTarget := response
	paddedActionTarget.Action.Target = "standby-a "
	if err := ValidateStandbyFenceResponse(paddedActionTarget); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("padded fence action target error = %v, want action node mismatch", err)
	}
	paddedActionID := response
	paddedActionID.Action.ActionId = "fence_acquire:standby-a "
	if err := ValidateStandbyFenceResponse(paddedActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded fence action id error = %v, want action id mismatch", err)
	}
	invalidReceiptNode := response
	invalidReceiptNode.Receipt.PromotedNodeId = "standby a"
	if err := ValidateStandbyFenceResponse(invalidReceiptNode); err == nil || !strings.Contains(err.Error(), "receipt fields") {
		t.Fatalf("invalid fence receipt node error = %v, want receipt fields", err)
	}
	wrongIdentity := response
	wrongIdentity.Receipt.Identity.TimelineId = 5
	if err := ValidateStandbyFenceResponse(wrongIdentity); err == nil || !strings.Contains(err.Error(), "promoted timeline") {
		t.Fatalf("wrong fence identity error = %v, want promoted timeline mismatch", err)
	}
	staleObserved := response
	staleObserved.Receipt.ObservedLsn = 7
	if err := ValidateStandbyFenceResponse(staleObserved); err == nil || !strings.Contains(err.Error(), "observed_lsn") {
		t.Fatalf("stale fence observed_lsn error = %v, want observed_lsn mismatch", err)
	}
	if err := ValidateStandbyCurrentFenceResponse(StandbyCurrentFenceResponse{
		SchemaVersion: 1,
		Held:          false,
	}); err != nil {
		t.Fatalf("ValidateStandbyCurrentFenceResponse empty returned error: %v", err)
	}
	if err := ValidateStandbyCurrentFenceResponse(StandbyCurrentFenceResponse{
		SchemaVersion: 1,
		Held:          true,
		Receipt:       receipt,
	}); err != nil {
		t.Fatalf("ValidateStandbyCurrentFenceResponse held returned error: %v", err)
	}
	if err := ValidateStandbyCurrentFenceResponse(StandbyCurrentFenceResponse{
		SchemaVersion: 1,
		Held:          true,
	}); err == nil || !strings.Contains(err.Error(), "receipt fields") {
		t.Fatalf("missing current fence receipt error = %v, want receipt fields error", err)
	}
	if err := ValidateStandbyCurrentFenceResponse(StandbyCurrentFenceResponse{
		SchemaVersion: 1,
		Held:          false,
		Receipt:       receipt,
	}); err == nil || !strings.Contains(err.Error(), "not held") {
		t.Fatalf("unexpected current fence receipt error = %v, want not held error", err)
	}
	currentWithBadReceipt := receipt
	currentWithBadReceipt.NewEpoch = currentWithBadReceipt.ParentEpoch
	currentWithBadReceipt.Identity.Epoch = currentWithBadReceipt.NewEpoch
	if err := ValidateStandbyCurrentFenceResponse(StandbyCurrentFenceResponse{
		SchemaVersion: 1,
		Held:          true,
		Receipt:       currentWithBadReceipt,
	}); err == nil || !strings.Contains(err.Error(), "does not advance") {
		t.Fatalf("bad current fence receipt error = %v, want advance error", err)
	}
	response.Receipt.Token = ""
	if err := ValidateStandbyFenceResponse(response); err == nil || !strings.Contains(err.Error(), "receipt fields") {
		t.Fatalf("missing token error = %v, want receipt fields error", err)
	}
	response.Receipt.Token = "fence-token"
	response.Action.NodeId = ""
	if err := ValidateStandbyFenceResponse(response); err == nil || !strings.Contains(err.Error(), "action receipt") {
		t.Fatalf("missing action receipt error = %v, want action receipt error", err)
	}
}

func TestValidateStandbyUpstreamResponse(t *testing.T) {
	t.Parallel()

	identity := StandbyIdentity{ClusterId: 100, ShardId: 10, TableId: 20, TimelineId: 4, Epoch: 6}
	newUpstream := StandbyUpstream{UpstreamUrl: "https://primary-b.example:5433", SlotName: "standby-a"}
	oldUpstream := StandbyUpstream{UpstreamUrl: "https://primary-a.example:5433", SlotName: "standby-a"}

	changed := StandbyUpstreamResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "standby_upstream:standby-a",
			ActionKind: StandbyActionKindStandbyUpstream,
			Target:     "standby-a",
			State:      StandbyActionStateApplied,
			NodeId:     "standby-a",
		},
		Identity: identity,
		Upstream: newUpstream,
		Previous: oldUpstream,
		Changed:  true,
	}
	if err := ValidateStandbyUpstreamResponse(changed); err != nil {
		t.Fatalf("ValidateStandbyUpstreamResponse changed returned error: %v", err)
	}

	changedWithoutPrevious := changed
	changedWithoutPrevious.Previous = StandbyUpstream{}
	if err := ValidateStandbyUpstreamResponse(changedWithoutPrevious); err != nil {
		t.Fatalf("ValidateStandbyUpstreamResponse changed without previous returned error: %v", err)
	}

	alreadyApplied := changed
	alreadyApplied.Action.State = StandbyActionStateAlreadyApplied
	alreadyApplied.Changed = false
	alreadyApplied.Previous = newUpstream
	if err := ValidateStandbyUpstreamResponse(alreadyApplied); err != nil {
		t.Fatalf("ValidateStandbyUpstreamResponse unchanged returned error: %v", err)
	}

	unchangedWithoutPrevious := alreadyApplied
	unchangedWithoutPrevious.Previous = StandbyUpstream{}
	if err := ValidateStandbyUpstreamResponse(unchangedWithoutPrevious); err == nil || !strings.Contains(err.Error(), "without a previous upstream") {
		t.Fatalf("unchanged without previous error = %v, want missing previous error", err)
	}

	unchangedWithMismatchedPrevious := alreadyApplied
	unchangedWithMismatchedPrevious.Previous = oldUpstream
	if err := ValidateStandbyUpstreamResponse(unchangedWithMismatchedPrevious); err == nil || !strings.Contains(err.Error(), "mismatched previous upstream") {
		t.Fatalf("unchanged with mismatched previous error = %v, want mismatched previous error", err)
	}

	changedWithIdenticalPrevious := changed
	changedWithIdenticalPrevious.Previous = newUpstream
	if err := ValidateStandbyUpstreamResponse(changedWithIdenticalPrevious); err == nil || !strings.Contains(err.Error(), "unchanged previous upstream") {
		t.Fatalf("changed with identical previous error = %v, want unchanged previous error", err)
	}

	missingIdentity := changed
	missingIdentity.Identity = StandbyIdentity{}
	if err := ValidateStandbyUpstreamResponse(missingIdentity); err == nil || !strings.Contains(err.Error(), "identity fields") {
		t.Fatalf("missing identity error = %v, want identity fields error", err)
	}

	invalidUpstream := changed
	invalidUpstream.Upstream.UpstreamUrl = ""
	if err := ValidateStandbyUpstreamResponse(invalidUpstream); err == nil || !strings.Contains(err.Error(), "standby upstream fields") {
		t.Fatalf("invalid upstream error = %v, want standby upstream fields error", err)
	}

	mismatchedTarget := changed
	mismatchedTarget.Action.Target = "standby-b"
	if err := ValidateStandbyUpstreamResponse(mismatchedTarget); err == nil || !strings.Contains(err.Error(), "does not match action target") {
		t.Fatalf("mismatched action target error = %v, want action target mismatch error", err)
	}

	missingAction := changed
	missingAction.Action.NodeId = ""
	if err := ValidateStandbyUpstreamResponse(missingAction); err == nil || !strings.Contains(err.Error(), "action receipt") {
		t.Fatalf("missing action receipt error = %v, want action receipt error", err)
	}
}

func TestValidateStandbyPromotionResponses(t *testing.T) {
	t.Parallel()

	assessment := StandbyPromotionAssessment{
		RequiredLsn:        8,
		ReceivedLsn:        8,
		AppliedLsn:         8,
		HasRequiredLsn:     true,
		CaughtUpToReceived: true,
		FencingConfirmed:   true,
		Force:              false,
		Mode:               StandbyPromotionModeSafe,
		CanPromote:         true,
		Safe:               true,
	}
	assess := StandbyPromotionAssessResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "promotion_assess:standby-a",
			ActionKind: StandbyActionKindPromotionAssess,
			Target:     "standby-a",
			State:      StandbyActionStateAssessed,
			NodeId:     "standby-a",
		},
		Assessment: assessment,
	}
	if err := ValidateStandbyPromotionAssessResponse(assess); err != nil {
		t.Fatalf("ValidateStandbyPromotionAssessResponse returned error: %v", err)
	}
	emptyStandbyAssess := assess
	emptyStandbyAssess.Assessment.RequiredLsn = 0
	emptyStandbyAssess.Assessment.ReceivedLsn = 0
	emptyStandbyAssess.Assessment.AppliedLsn = 0
	emptyStandbyAssess.Assessment.HasRequiredLsn = true
	if err := ValidateStandbyPromotionAssessResponse(emptyStandbyAssess); err != nil {
		t.Fatalf("ValidateStandbyPromotionAssessResponse with zero required_lsn returned error: %v", err)
	}
	wrongAssessNode := assess
	wrongAssessNode.Action.NodeId = "standby-b"
	if err := ValidateStandbyPromotionAssessResponse(wrongAssessNode); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("wrong promotion assess executor error = %v, want executor node mismatch", err)
	}
	paddedAssessTarget := assess
	paddedAssessTarget.Action.Target = "standby-a "
	if err := ValidateStandbyPromotionAssessResponse(paddedAssessTarget); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("padded promotion assess target error = %v, want executor node mismatch", err)
	}
	paddedAssessActionID := assess
	paddedAssessActionID.Action.ActionId = "promotion_assess:standby-a "
	if err := ValidateStandbyPromotionAssessResponse(paddedAssessActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded promotion assess action id error = %v, want action id mismatch", err)
	}
	inconsistentAssess := assess
	inconsistentAssess.Assessment.HasRequiredLsn = false
	if err := ValidateStandbyPromotionAssessResponse(inconsistentAssess); err == nil || !strings.Contains(err.Error(), "has_required_lsn") {
		t.Fatalf("inconsistent promotion assessment error = %v, want has_required_lsn mismatch", err)
	}
	wrongMode := assess
	wrongMode.Assessment.Mode = StandbyPromotionModeForced
	if err := ValidateStandbyPromotionAssessResponse(wrongMode); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("wrong promotion assessment mode error = %v, want mode mismatch", err)
	}
	assess.Assessment.RequiredLsn = 9
	if err := ValidateStandbyPromotionAssessResponse(assess); err == nil || !strings.Contains(err.Error(), "assessment fields") {
		t.Fatalf("missing assessment error = %v, want assessment fields error", err)
	}

	identity := StandbyIdentity{ClusterId: 1, ShardId: 2, TableId: 3, TimelineId: 4, Epoch: 5}
	promotion := StandbyPromotionResponse{
		SchemaVersion:   1,
		Action:          StandbyActionReceipt{ActionId: "promotion:standby-a", ActionKind: StandbyActionKindPromotion, Target: "standby-a", State: StandbyActionStateApplied, NodeId: "standby-a"},
		Assessment:      assessment,
		FenceGeneration: 9,
		FenceToken:      "fence-token",
		Promotion: StandbyPromotionResult{
			NodeId:      "standby-a",
			SwitchLsn:   9,
			OldIdentity: identity,
			NewIdentity: StandbyIdentity{ClusterId: 1, ShardId: 2, TableId: 3, TimelineId: 6, Epoch: 7},
		},
	}
	if err := ValidateStandbyPromotionResponse(promotion); err != nil {
		t.Fatalf("ValidateStandbyPromotionResponse returned error: %v", err)
	}
	wrongPromotionNode := promotion
	wrongPromotionNode.Action.NodeId = "standby-b"
	if err := ValidateStandbyPromotionResponse(wrongPromotionNode); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("wrong promotion node error = %v, want action node mismatch", err)
	}
	paddedPromotionTarget := promotion
	paddedPromotionTarget.Action.Target = "standby-a "
	if err := ValidateStandbyPromotionResponse(paddedPromotionTarget); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("padded promotion target error = %v, want action node mismatch", err)
	}
	paddedPromotionActionID := promotion
	paddedPromotionActionID.Action.ActionId = "promotion:standby-a "
	if err := ValidateStandbyPromotionResponse(paddedPromotionActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded promotion action id error = %v, want action id mismatch", err)
	}
	wrongSwitchLSN := promotion
	wrongSwitchLSN.Promotion.SwitchLsn = 10
	if err := ValidateStandbyPromotionResponse(wrongSwitchLSN); err == nil || !strings.Contains(err.Error(), "switch_lsn") {
		t.Fatalf("wrong promotion switch_lsn error = %v, want switch_lsn mismatch", err)
	}
	wrongIdentity := promotion
	wrongIdentity.Promotion.NewIdentity.ClusterId = 99
	if err := ValidateStandbyPromotionResponse(wrongIdentity); err == nil || !strings.Contains(err.Error(), "identity scope") {
		t.Fatalf("wrong promotion identity error = %v, want identity scope mismatch", err)
	}
	promotion.FenceToken = ""
	if err := ValidateStandbyPromotionResponse(promotion); err == nil || !strings.Contains(err.Error(), "fence_token") {
		t.Fatalf("missing fence_token error = %v, want fence_token error", err)
	}
	promotion.FenceToken = "fence-token"
	promotion.Promotion.SwitchLsn = 0
	if err := ValidateStandbyPromotionResponse(promotion); err == nil || !strings.Contains(err.Error(), "promotion result") {
		t.Fatalf("missing promotion result error = %v, want promotion result error", err)
	}
}

func TestValidateStandbySeedLifecycleReceiptInventory(t *testing.T) {
	t.Parallel()
	receipt := `{"format_version":2,"generation":"seed-a-7","slot_name":"standby-a","topology_id":"topology-a","topology_generation":7,"node_id":"standby-a","target_pvc_name":"standby-a-data","target_pvc_uid":"pvc-uid-7"}`
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(receipt)))
	response := StandbySeedLifecycleReceiptInventory{
		SchemaVersion:    1,
		FirstCursor:      4,
		EndCursor:        4,
		NextCursor:       4,
		HistoryTruncated: true,
		Entries: []StandbySeedLifecycleReceiptEvent{{
			Cursor: 4, Kind: oapi.StandbySeedLifecycleReceiptEventKindCapture,
			Generation: "seed-a-7", SlotName: "standby-a", TopologyId: "topology-a", TopologyGeneration: 7,
			NodeId: "standby-a", TargetPvcName: "standby-a-data", TargetPvcUid: "pvc-uid-7",
			ReceiptSha256: digest, ReceiptJson: receipt, RecordedAtUnixNs: 99,
			AuthoritativeState: oapi.StandbySeedLifecycleReceiptEventAuthoritativeStateRetained,
		}},
		Runtime: StandbyRuntimeLifecycleObservation{
			NodeId: "primary-a", Role: oapi.StandbyRuntimeLifecycleObservationRolePrimary,
			PodUid: "pod-primary-a", Fenced: false, ObservedAtUnixNs: 100,
		},
	}
	if err := ValidateStandbySeedLifecycleReceiptInventory(response); err != nil {
		t.Fatalf("ValidateStandbySeedLifecycleReceiptInventory returned error: %v", err)
	}
	badDigest := response
	badDigest.Entries = append([]StandbySeedLifecycleReceiptEvent(nil), response.Entries...)
	badDigest.Entries[0].ReceiptJson += " "
	if err := ValidateStandbySeedLifecycleReceiptInventory(badDigest); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("receipt digest mismatch error = %v, want digest error", err)
	}
	badCursor := response
	badCursor.NextCursor = 3
	if err := ValidateStandbySeedLifecycleReceiptInventory(badCursor); err == nil || !strings.Contains(err.Error(), "next_cursor") {
		t.Fatalf("cursor mismatch error = %v, want next_cursor error", err)
	}
}

func TestValidateStandbyResponseEvidence(t *testing.T) {
	t.Parallel()

	slot := `{"schema_version":1,"action":{"action_id":"replication_slot_create:standby-a","action_kind":"replication_slot_create","target":"standby-a","state":"applied","node_id":"primary-a"},"slot_action":"create","slot":{"slot_name":"standby-a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":false,"reseed_required":false,"current_lsn":0}}`
	if err := ValidateStandbyReplicationSlotActionResponseEvidence([]byte(slot)); err != nil {
		t.Fatalf("ValidateStandbyReplicationSlotActionResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyReplicationSlotActionResponseEvidence([]byte(strings.Replace(slot, `"slot_name":"standby-a",`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot name evidence error = %v, want slot field evidence error", err)
	}
	if err := ValidateStandbyReplicationSlotActionResponseEvidence([]byte(strings.Replace(slot, `,"active":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot active evidence error = %v, want slot field evidence error", err)
	}
	slotList := `{"schema_version":1,"slots":[{"slot_name":"standby-a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":false,"reseed_required":false,"current_lsn":0}]}`
	if err := ValidateStandbyReplicationSlotListResponseEvidence([]byte(slotList)); err != nil {
		t.Fatalf("ValidateStandbyReplicationSlotListResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyReplicationSlotListResponseEvidence([]byte(`{"schema_version":1}`)); err == nil || !strings.Contains(err.Error(), "slots field evidence") {
		t.Fatalf("missing slot list evidence error = %v, want slots field evidence error", err)
	}
	if err := ValidateStandbyReplicationSlotListResponseEvidence([]byte(strings.Replace(slotList, `,"timeline_id":1`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot timeline evidence error = %v, want slot field evidence error", err)
	}
	if err := ValidateStandbyReplicationSlotListResponseEvidence([]byte(strings.Replace(slotList, `,"current_lsn":0`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot current_lsn evidence error = %v, want slot field evidence error", err)
	}

	begin := `{"schema_version":1,"action":{"action_id":"base_backup_begin:manifest-a","action_kind":"base_backup_begin","target":"manifest-a","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"manifest-a","backup_lsn":7,"start_record_lsn":8}`
	if err := ValidateStandbyBaseBackupBeginResponseEvidence([]byte(begin)); err != nil {
		t.Fatalf("ValidateStandbyBaseBackupBeginResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyBaseBackupBeginResponseEvidence([]byte(strings.Replace(begin, `,"start_record_lsn":8`, "", 1))); err == nil || !strings.Contains(err.Error(), "base backup begin field evidence") {
		t.Fatalf("missing base backup begin evidence error = %v, want field evidence error", err)
	}
	finish := `{"schema_version":1,"action":{"action_id":"base_backup_finish:manifest-a","action_kind":"base_backup_finish","target":"manifest-a","state":"applied","node_id":"primary-a"},"manifest_id":"manifest-a","backup_lsn":7,"end_record_lsn":9}`
	if err := ValidateStandbyBaseBackupFinishResponseEvidence([]byte(finish)); err != nil {
		t.Fatalf("ValidateStandbyBaseBackupFinishResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyBaseBackupFinishResponseEvidence([]byte(strings.Replace(finish, `,"end_record_lsn":9`, "", 1))); err == nil || !strings.Contains(err.Error(), "base backup finish field evidence") {
		t.Fatalf("missing base backup finish evidence error = %v, want field evidence error", err)
	}
	bootstrap := `{"schema_version":1,"action":{"action_id":"standby_bootstrap:manifest-a","action_kind":"standby_bootstrap","target":"manifest-a","state":"applied","node_id":"standby-a"},"manifest_id":"manifest-a","backup_lsn":7,"checkpoint_lsn":10}`
	if err := ValidateStandbyBootstrapResponseEvidence([]byte(bootstrap)); err != nil {
		t.Fatalf("ValidateStandbyBootstrapResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyBootstrapResponseEvidence([]byte(strings.Replace(bootstrap, `,"checkpoint_lsn":10`, "", 1))); err == nil || !strings.Contains(err.Error(), "standby bootstrap field evidence") {
		t.Fatalf("missing standby bootstrap evidence error = %v, want field evidence error", err)
	}

	standbyUpstream := `{"schema_version":1,"action":{"action_id":"standby_upstream:standby-a","action_kind":"standby_upstream","target":"standby-a","state":"applied","node_id":"standby-a"},"identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":4,"epoch":6},"upstream":{"upstream_url":"https://primary-b.example:5433","slot_name":"standby-a"},"previous":{"upstream_url":"https://primary-a.example:5433","slot_name":"standby-a"},"changed":true}`
	if err := ValidateStandbyUpstreamResponseEvidence([]byte(standbyUpstream)); err != nil {
		t.Fatalf("ValidateStandbyUpstreamResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyUpstreamResponseEvidence([]byte(strings.Replace(standbyUpstream, `,"epoch":6`, "", 1))); err == nil || !strings.Contains(err.Error(), "identity field evidence") {
		t.Fatalf("missing standby upstream identity evidence error = %v, want identity field evidence error", err)
	}
	if err := ValidateStandbyUpstreamResponseEvidence([]byte(strings.Replace(standbyUpstream, `"upstream":{"upstream_url":"https://primary-b.example:5433","slot_name":"standby-a"}`, `"upstream":{"upstream_url":"https://primary-b.example:5433"}`, 1))); err == nil || !strings.Contains(err.Error(), "standby upstream field evidence") {
		t.Fatalf("missing standby upstream field evidence error = %v, want field evidence error", err)
	}
	if err := ValidateStandbyUpstreamResponseEvidence([]byte(strings.Replace(standbyUpstream, `,"changed":true`, "", 1))); err == nil || !strings.Contains(err.Error(), "changed field evidence") {
		t.Fatalf("missing standby upstream changed evidence error = %v, want changed field evidence error", err)
	}
	unchangedWithoutPreviousEvidence := strings.Replace(standbyUpstream, `,"previous":{"upstream_url":"https://primary-a.example:5433","slot_name":"standby-a"}`, "", 1)
	unchangedWithoutPreviousEvidence = strings.Replace(unchangedWithoutPreviousEvidence, `"changed":true`, `"changed":false`, 1)
	if err := ValidateStandbyUpstreamResponseEvidence([]byte(unchangedWithoutPreviousEvidence)); err == nil || !strings.Contains(err.Error(), "previous field evidence") {
		t.Fatalf("unchanged without previous evidence error = %v, want previous field evidence error", err)
	}
	partialPreviousEvidence := strings.Replace(standbyUpstream, `"previous":{"upstream_url":"https://primary-a.example:5433","slot_name":"standby-a"}`, `"previous":{"upstream_url":"https://primary-a.example:5433"}`, 1)
	if err := ValidateStandbyUpstreamResponseEvidence([]byte(partialPreviousEvidence)); err == nil || !strings.Contains(err.Error(), "previous field evidence") {
		t.Fatalf("partial previous evidence error = %v, want previous field evidence error", err)
	}

	fence := `{"schema_version":1,"action":{"action_id":"fence_acquire:standby-a","action_kind":"fence_acquire","target":"standby-a","state":"applied","node_id":"standby-a"},"receipt":{"identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":2,"epoch":3},"old_primary_id":"primary-a","promoted_node_id":"standby-a","parent_timeline_id":2,"parent_epoch":3,"new_timeline_id":4,"new_epoch":5,"required_lsn":8,"observed_lsn":8,"generation":9,"forced":false,"token":"fence-token","reason":""}}`
	if err := ValidateStandbyFenceResponseEvidence([]byte(fence)); err != nil {
		t.Fatalf("ValidateStandbyFenceResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyFenceResponseEvidence([]byte(strings.Replace(fence, `,"forced":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "receipt field evidence") {
		t.Fatalf("missing fence forced evidence error = %v, want receipt evidence error", err)
	}
	if err := ValidateStandbyCurrentFenceResponseEvidence([]byte(`{"schema_version":1}`)); err == nil || !strings.Contains(err.Error(), "held field evidence") {
		t.Fatalf("missing held evidence error = %v, want held evidence error", err)
	}

	assessment := `"assessment":{"required_lsn":8,"received_lsn":8,"applied_lsn":8,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}`
	promotionAssess := `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},` + assessment + `}`
	if err := ValidateStandbyPromotionAssessResponseEvidence([]byte(promotionAssess)); err != nil {
		t.Fatalf("ValidateStandbyPromotionAssessResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyPromotionAssessResponseEvidence([]byte(strings.Replace(promotionAssess, `,"force":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "assessment field evidence") {
		t.Fatalf("missing promotion force evidence error = %v, want assessment evidence error", err)
	}

	promotion := `{"schema_version":1,"action":{"action_id":"promotion:standby-a","action_kind":"promotion","target":"standby-a","state":"applied","node_id":"standby-a"},` + assessment + `,"fence_generation":9,"fence_token":"fence-token","forced":false,"promotion":{"node_id":"standby-a","switch_lsn":9,"old_identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":2,"epoch":3},"new_identity":{"cluster_id":1,"shard_id":0,"table_id":0,"timeline_id":4,"epoch":5},"data_loss_possible":false,"forced":false}}`
	if err := ValidateStandbyPromotionResponseEvidence([]byte(promotion)); err != nil {
		t.Fatalf("ValidateStandbyPromotionResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyPromotionResponseEvidence([]byte(strings.Replace(promotion, `,"data_loss_possible":false,"forced":false}}`, `,"forced":false}}`, 1))); err == nil || !strings.Contains(err.Error(), "promotion result field evidence") {
		t.Fatalf("missing promotion result evidence error = %v, want promotion result evidence error", err)
	}

	rejoin := `{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"assessed","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":4,"target_epoch":5,"parent_cluster_id":1,"parent_shard_id":0,"parent_table_id":0,"parent_timeline_id":2,"parent_epoch":3,"fork_lsn":8,"former_last_lsn":9,"retained_from_lsn":7,"data_loss_discarded":false},"rewind":{"node_id":"primary-a","target_timeline_id":4,"target_epoch":5,"next_lsn":9,"current_last_lsn":9,"previous_last_lsn":10,"fork_lsn":8,"discarded_lsn_count":1,"data_loss_discarded":false}}`
	if err := ValidateStandbyRejoinAssessResponseEvidence([]byte(rejoin)); err != nil {
		t.Fatalf("ValidateStandbyRejoinAssessResponseEvidence returned error: %v", err)
	}
	if err := ValidateStandbyRejoinAssessResponseEvidence([]byte(strings.Replace(rejoin, `,"data_loss_discarded":false`, "", 1))); err == nil || !strings.Contains(err.Error(), "rejoin assessment field evidence") {
		t.Fatalf("missing rejoin assessment evidence error = %v, want assessment evidence error", err)
	}
}

func TestValidateStandbyGateResponses(t *testing.T) {
	t.Parallel()

	durability := StandbyDurabilityDecision{
		Status:          StandbyDurabilityStatusSatisfied,
		Mode:            StandbyDurabilityModeRemoteWrite,
		Selection:       StandbyDurabilitySelectionAny,
		TargetLsn:       9,
		ProgressLsn:     9,
		RequiredCount:   1,
		SatisfiedCount:  1,
		CandidateCount:  1,
		MissingLsnCount: 0,
	}
	gate := StandbyCommitGate{
		Action:     StandbyCommitGateActionAcknowledge,
		TargetLsn:  9,
		Durability: durability,
	}
	if err := ValidateStandbyCommitCheckResponse(StandbyCommitCheckResponse{SchemaVersion: 1, Gate: gate}); err != nil {
		t.Fatalf("ValidateStandbyCommitCheckResponse returned error: %v", err)
	}
	if err := ValidateStandbyCommitAppendResponse(StandbyCommitAppendResponse{SchemaVersion: 1, Lsn: 9, Gate: gate}); err != nil {
		t.Fatalf("ValidateStandbyCommitAppendResponse returned error: %v", err)
	}
	mismatchedGate := gate
	mismatchedGate.Durability.TargetLsn = 8
	if err := ValidateStandbyCommitCheckResponse(StandbyCommitCheckResponse{SchemaVersion: 1, Gate: mismatchedGate}); err == nil || !strings.Contains(err.Error(), "target_lsn") {
		t.Fatalf("mismatched gate target error = %v, want target_lsn mismatch", err)
	}
	impossibleProgress := gate
	impossibleProgress.Durability.ProgressLsn = 10
	if err := ValidateStandbyCommitCheckResponse(StandbyCommitCheckResponse{SchemaVersion: 1, Gate: impossibleProgress}); err == nil || !strings.Contains(err.Error(), "progress_lsn") {
		t.Fatalf("impossible durability progress error = %v, want progress_lsn mismatch", err)
	}
	if err := ValidateStandbyCommitAppendResponse(StandbyCommitAppendResponse{SchemaVersion: 1, Lsn: 8, Gate: gate}); err == nil || !strings.Contains(err.Error(), "does not match gate") {
		t.Fatalf("mismatched append lsn error = %v, want gate lsn mismatch", err)
	}
	gate.Action = StandbyCommitGateAction("unknown")
	if err := ValidateStandbyCommitCheckResponse(StandbyCommitCheckResponse{SchemaVersion: 1, Gate: gate}); err == nil || !strings.Contains(err.Error(), "invalid commit gate action") {
		t.Fatalf("invalid gate error = %v, want invalid action error", err)
	}

	read := StandbyReadCheckResponse{
		SchemaVersion: 1,
		Decision: StandbyReadDecision{
			Action:                  StandbyReadDecisionActionServeStandby,
			Consistency:             StandbyReadDecisionConsistencyAtLeastLSN,
			ReceivedLsn:             9,
			AppliedLsn:              9,
			SafeReadLsn:             9,
			MissingLsnCount:         0,
			MetadataMissingLsnCount: 0,
		},
	}
	if err := ValidateStandbyReadCheckResponse(read); err != nil {
		t.Fatalf("ValidateStandbyReadCheckResponse returned error: %v", err)
	}
	badReadProgress := read
	badReadProgress.Decision.AppliedLsn = 10
	if err := ValidateStandbyReadCheckResponse(badReadProgress); err == nil || !strings.Contains(err.Error(), "applied_lsn") {
		t.Fatalf("invalid read progress error = %v, want applied_lsn error", err)
	}
	badReadMissing := read
	badReadMissing.Decision.RequiredLsn = 11
	if err := ValidateStandbyReadCheckResponse(badReadMissing); err == nil || !strings.Contains(err.Error(), "missing_lsn_count") {
		t.Fatalf("invalid read missing count error = %v, want missing_lsn_count error", err)
	}
	badReadServe := read
	badReadServe.Decision.ServeLsn = 10
	if err := ValidateStandbyReadCheckResponse(badReadServe); err == nil || !strings.Contains(err.Error(), "serve_lsn") {
		t.Fatalf("invalid read serve lsn error = %v, want serve_lsn error", err)
	}
	badReadPrimary := read
	badReadPrimary.Decision.Consistency = StandbyReadDecisionConsistencyPrimary
	if err := ValidateStandbyReadCheckResponse(badReadPrimary); err == nil || !strings.Contains(err.Error(), "primary consistency") {
		t.Fatalf("invalid read primary action error = %v, want primary consistency error", err)
	}
	badReadFields := read
	badReadFields.Decision.Consistency = StandbyReadDecisionConsistency("unknown")
	if err := ValidateStandbyReadCheckResponse(badReadFields); err == nil || !strings.Contains(err.Error(), "read decision fields") {
		t.Fatalf("invalid read decision error = %v, want read decision fields error", err)
	}

	identity := StandbyIdentity{ClusterId: 1, TimelineId: 2, Epoch: 3}
	write := StandbyWriteCheckResponse{
		SchemaVersion: 1,
		Decision: StandbyWriteDecision{
			Action:     StandbyWriteDecisionActionRejectReadOnly,
			Role:       StandbyWriteDecisionRoleStandby,
			Identity:   identity,
			DurableLsn: 9,
			NextLsn:    10,
		},
	}
	if err := ValidateStandbyWriteCheckResponse(write); err != nil {
		t.Fatalf("ValidateStandbyWriteCheckResponse returned error: %v", err)
	}
	badWriteNext := write
	badWriteNext.Decision.NextLsn = 12
	if err := ValidateStandbyWriteCheckResponse(badWriteNext); err == nil || !strings.Contains(err.Error(), "next_lsn") {
		t.Fatalf("invalid write next lsn error = %v, want next_lsn error", err)
	}
	badWriteAction := write
	badWriteAction.Decision.Action = StandbyWriteDecisionActionAllowWrite
	if err := ValidateStandbyWriteCheckResponse(badWriteAction); err == nil || !strings.Contains(err.Error(), "standby role action") {
		t.Fatalf("invalid write role action error = %v, want standby role action error", err)
	}
	promotedIdentity := StandbyIdentity{ClusterId: 1, TimelineId: 4, Epoch: 5}
	promotedWrite := StandbyWriteCheckResponse{
		SchemaVersion: 1,
		Decision: StandbyWriteDecision{
			Action:     StandbyWriteDecisionActionOpenPromotedPrimary,
			Role:       StandbyWriteDecisionRolePromotedStandby,
			Identity:   promotedIdentity,
			DurableLsn: 12,
			NextLsn:    13,
			PromotionHandoff: StandbyPromotionHandoff{
				Identity:  promotedIdentity,
				SwitchLsn: 12,
				NextLsn:   13,
			},
		},
	}
	if err := ValidateStandbyWriteCheckResponse(promotedWrite); err != nil {
		t.Fatalf("ValidateStandbyWriteCheckResponse promoted returned error: %v", err)
	}
	fencedWrite := StandbyWriteCheckResponse{
		SchemaVersion: 1,
		Decision: StandbyWriteDecision{
			Action:     StandbyWriteDecisionActionRejectFencedPrimary,
			Role:       StandbyWriteDecisionRoleFencedPrimary,
			Identity:   identity,
			DurableLsn: 9,
			NextLsn:    10,
		},
	}
	if err := ValidateStandbyWriteCheckResponse(fencedWrite); err != nil {
		t.Fatalf("ValidateStandbyWriteCheckResponse fenced primary returned error: %v", err)
	}
	badFencedWrite := fencedWrite
	badFencedWrite.Decision.Action = StandbyWriteDecisionActionAllowWrite
	if err := ValidateStandbyWriteCheckResponse(badFencedWrite); err == nil || !strings.Contains(err.Error(), "fenced_primary role action") {
		t.Fatalf("invalid fenced write action error = %v, want fenced_primary role action error", err)
	}
	badWriteHandoff := promotedWrite
	badWriteHandoff.Decision.PromotionHandoff.Identity.Epoch = 6
	if err := ValidateStandbyWriteCheckResponse(badWriteHandoff); err == nil || !strings.Contains(err.Error(), "promotion_handoff identity") {
		t.Fatalf("invalid write handoff error = %v, want promotion_handoff identity error", err)
	}
	badWriteFields := write
	badWriteFields.Decision.Identity = StandbyIdentity{}
	if err := ValidateStandbyWriteCheckResponse(badWriteFields); err == nil || !strings.Contains(err.Error(), "write decision fields") {
		t.Fatalf("invalid write decision error = %v, want write decision fields error", err)
	}

	owner := StandbyOwnerJobCheckResponse{
		SchemaVersion: 1,
		Decision: StandbyOwnerJobDecision{
			Action:     StandbyOwnerJobDecisionActionRun,
			Kind:       StandbyOwnerJobDecisionKindCompactionPublish,
			Role:       StandbyOwnerJobDecisionRolePrimary,
			Identity:   identity,
			DurableLsn: 9,
			NextLsn:    10,
		},
	}
	if err := ValidateStandbyOwnerJobCheckResponse(owner); err != nil {
		t.Fatalf("ValidateStandbyOwnerJobCheckResponse returned error: %v", err)
	}
	badOwnerNext := owner
	badOwnerNext.Decision.NextLsn = 12
	if err := ValidateStandbyOwnerJobCheckResponse(badOwnerNext); err == nil || !strings.Contains(err.Error(), "next_lsn") {
		t.Fatalf("invalid owner job next lsn error = %v, want next_lsn error", err)
	}
	badOwnerAction := owner
	badOwnerAction.Decision.Role = StandbyOwnerJobDecisionRoleStandby
	if err := ValidateStandbyOwnerJobCheckResponse(badOwnerAction); err == nil || !strings.Contains(err.Error(), "standby role action") {
		t.Fatalf("invalid owner job role action error = %v, want standby role action error", err)
	}
	promotedOwner := StandbyOwnerJobCheckResponse{
		SchemaVersion: 1,
		Decision: StandbyOwnerJobDecision{
			Action:     StandbyOwnerJobDecisionActionOpenPromotedPrimary,
			Kind:       StandbyOwnerJobDecisionKindCompactionPublish,
			Role:       StandbyOwnerJobDecisionRolePromotedStandby,
			Identity:   promotedIdentity,
			DurableLsn: 12,
			NextLsn:    13,
			PromotionHandoff: StandbyPromotionHandoff{
				Identity:  promotedIdentity,
				SwitchLsn: 12,
				NextLsn:   13,
			},
		},
	}
	if err := ValidateStandbyOwnerJobCheckResponse(promotedOwner); err != nil {
		t.Fatalf("ValidateStandbyOwnerJobCheckResponse promoted returned error: %v", err)
	}
	badOwnerHandoff := promotedOwner
	badOwnerHandoff.Decision.PromotionHandoff.NextLsn = 14
	if err := ValidateStandbyOwnerJobCheckResponse(badOwnerHandoff); err == nil || !strings.Contains(err.Error(), "promotion_handoff next_lsn") {
		t.Fatalf("invalid owner job handoff error = %v, want promotion_handoff next_lsn error", err)
	}
	badOwnerFields := owner
	badOwnerFields.Decision.Kind = StandbyOwnerJobDecisionKind("unknown")
	if err := ValidateStandbyOwnerJobCheckResponse(badOwnerFields); err == nil || !strings.Contains(err.Error(), "owner job decision fields") {
		t.Fatalf("invalid owner job decision error = %v, want owner job decision fields error", err)
	}
}

func TestValidateStandbyRejoinAssessResponse(t *testing.T) {
	t.Parallel()

	base := StandbyRejoinAssessResponse{
		SchemaVersion: 1,
		Action: StandbyActionReceipt{
			ActionId:   "rejoin_assess:primary-a",
			ActionKind: StandbyActionKindRejoinAssess,
			Target:     "primary-a",
			State:      StandbyActionStateAssessed,
			NodeId:     "primary-a",
		},
		Assessment: StandbyRejoinAssessment{
			Action:           StandbyRejoinActionAlreadyCurrent,
			Reason:           StandbyRejoinReasonCurrentTimeline,
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
	if err := ValidateStandbyRejoinAssessResponse(base); err != nil {
		t.Fatalf("ValidateStandbyRejoinAssessResponse returned error: %v", err)
	}
	assessRewind := base
	assessRewind.Assessment.Action = StandbyRejoinActionRewind
	assessRewind.Assessment.Reason = StandbyRejoinReasonParentTimelineRetained
	if err := ValidateStandbyRejoinAssessResponse(assessRewind); err != nil {
		t.Fatalf("ValidateStandbyRejoinAssessResponse assess rewind returned error: %v", err)
	}
	wrongTarget := base
	wrongTarget.Action.Target = "primary-b"
	if err := ValidateStandbyRejoinAssessResponse(wrongTarget); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("wrong target error = %v, want target mismatch error", err)
	}
	paddedTarget := base
	paddedTarget.Action.Target = " primary-a"
	if err := ValidateStandbyRejoinAssessResponse(paddedTarget); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("padded target error = %v, want target mismatch error", err)
	}
	paddedActionID := base
	paddedActionID.Action.ActionId = "rejoin_assess:primary-a "
	if err := ValidateStandbyRejoinAssessResponse(paddedActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded action id error = %v, want action id mismatch error", err)
	}
	wrongAssessNode := base
	wrongAssessNode.Action.NodeId = "primary-b"
	if err := ValidateStandbyRejoinAssessResponse(wrongAssessNode); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("wrong assess executor error = %v, want executor node mismatch error", err)
	}
	base.Assessment.Reason = StandbyRejoinAssessmentReason("unknown")
	if err := ValidateStandbyRejoinAssessResponse(base); err == nil || !strings.Contains(err.Error(), "assessment fields") {
		t.Fatalf("invalid reason error = %v, want assessment fields error", err)
	}
	base.Assessment.Reason = StandbyRejoinReasonCurrentTimeline

	rewind := base
	rewind.Action.ActionId = "rejoin_rewind:primary-a"
	rewind.Action.ActionKind = StandbyActionKindRejoinRewind
	rewind.Action.State = StandbyActionStateApplied
	rewind.Assessment.Action = StandbyRejoinActionRewind
	rewind.Assessment.Reason = StandbyRejoinReasonParentTimelineRetained
	rewind.Rewind = StandbyRejoinRewindResult{
		NodeId:           "primary-a",
		TargetTimelineId: 6,
		TargetEpoch:      7,
		CurrentLastLsn:   8,
		PreviousLastLsn:  10,
		NextLsn:          9,
		ForkLsn:          8,
	}
	if err := ValidateStandbyRejoinAssessResponse(rewind); err != nil {
		t.Fatalf("ValidateStandbyRejoinAssessResponse rewind returned error: %v", err)
	}
	wrongRewindNode := rewind
	wrongRewindNode.Action.NodeId = "primary-b"
	if err := ValidateStandbyRejoinAssessResponse(wrongRewindNode); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("wrong rewind executor error = %v, want executor node mismatch error", err)
	}
	rewind.Rewind.NextLsn = 0
	if err := ValidateStandbyRejoinAssessResponse(rewind); err == nil || !strings.Contains(err.Error(), "rewind fields") {
		t.Fatalf("missing rewind error = %v, want rewind fields error", err)
	}

	reseed := base
	reseed.Action.ActionId = "rejoin_reseed:primary-a"
	reseed.Action.ActionKind = StandbyActionKindRejoinReseed
	reseed.Action.State = StandbyActionStateApplied
	reseed.Action.NodeId = "primary-current"
	reseed.Assessment.Action = StandbyRejoinActionReseed
	reseed.Assessment.Reason = StandbyRejoinReasonParentTimelineWALExpired
	reseed.Reseed = StandbyRejoinReseedResult{
		NodeId:             "primary-a",
		SlotName:           "primary-a",
		TargetTimelineId:   6,
		TargetEpoch:        7,
		ForkLsn:            8,
		FormerLastLsn:      10,
		ReseedRequired:     true,
		BaseBackupRequired: true,
	}
	if err := ValidateStandbyRejoinAssessResponse(reseed); err != nil {
		t.Fatalf("ValidateStandbyRejoinAssessResponse reseed returned error: %v", err)
	}
	reseed.Reseed.SlotName = ""
	if err := ValidateStandbyRejoinAssessResponse(reseed); err == nil || !strings.Contains(err.Error(), "reseed fields") {
		t.Fatalf("missing reseed error = %v, want reseed fields error", err)
	}
}

func TestStandbyClientReturnsStatusError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not primary", http.StatusConflict)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	_, err = client.CurrentFence(context.Background())
	var apiErr *StandbyAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("CurrentFence error = %T %v, want *StandbyAPIError", err, err)
	}
	if apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("StatusCode = %d, want %d", apiErr.StatusCode, http.StatusConflict)
	}
	if !strings.Contains(apiErr.Body, "not primary") {
		t.Fatalf("Body = %q, want not primary", apiErr.Body)
	}
	wrapped := fmt.Errorf("operator context: %w", err)
	status, ok := StandbyStatusCode(wrapped)
	if !ok || status != http.StatusConflict {
		t.Fatalf("StandbyStatusCode(wrapped) = %d, %t, want %d, true", status, ok, http.StatusConflict)
	}
	if !StandbyIsConflict(wrapped) {
		t.Fatalf("StandbyIsConflict(wrapped) = false, want true")
	}
	if StandbyIsUnauthorized(wrapped) {
		t.Fatalf("StandbyIsUnauthorized(wrapped) = true, want false")
	}
}

func TestStandbyErrorRetryability(t *testing.T) {
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
		err:  &StandbyAPIError{Operation: "get HA primary status", StatusCode: http.StatusServiceUnavailable},
		want: true,
	}, {
		name: "too many requests",
		err:  &StandbyAPIError{Operation: "get HA primary status", StatusCode: http.StatusTooManyRequests},
		want: true,
	}, {
		name: "conflict",
		err:  &StandbyAPIError{Operation: "get current HA fence", StatusCode: http.StatusConflict},
		want: false,
	}, {
		name: "bad request",
		err:  &StandbyAPIError{Operation: "create HA replication slot", StatusCode: http.StatusBadRequest},
		want: false,
	}, {
		name: "validation",
		err:  &StandbyResponseValidationError{Operation: "create HA replication slot", Err: errors.New("missing action receipt")},
		want: false,
	}, {
		name: "deadline",
		err:  context.DeadlineExceeded,
		want: true,
	}, {
		name: "transport url error",
		err:  &url.Error{Op: "Post", URL: "http://standby-a/admin/v1/ha/primary/status", Err: errors.New("connection refused")},
		want: true,
	}, {
		name: "canceled",
		err:  context.Canceled,
		want: false,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := StandbyIsRetryable(tt.err); got != tt.want {
				t.Fatalf("StandbyIsRetryable(%T) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestStandbyStatusHelpersClassifyWrappedErrors(t *testing.T) {
	t.Parallel()

	unauthorized := fmt.Errorf("direct admin call failed: %w", &StandbyAPIError{
		Operation:  "get HA primary status",
		StatusCode: http.StatusUnauthorized,
		Body:       "missing bearer token",
	})
	if !StandbyIsUnauthorized(unauthorized) {
		t.Fatal("StandbyIsUnauthorized(wrapped unauthorized) = false, want true")
	}
	if StandbyIsConflict(unauthorized) {
		t.Fatal("StandbyIsConflict(wrapped unauthorized) = true, want false")
	}
	status, ok := StandbyStatusCode(unauthorized)
	if !ok || status != http.StatusUnauthorized {
		t.Fatalf("StandbyStatusCode(wrapped unauthorized) = %d, %t, want %d, true", status, ok, http.StatusUnauthorized)
	}

	validation := fmt.Errorf("missing evidence: %w", &StandbyResponseValidationError{
		Operation: "create HA replication slot",
		Err:       errors.New("missing action receipt"),
	})
	if _, ok := StandbyStatusCode(validation); ok {
		t.Fatal("StandbyStatusCode(validation) returned true, want false")
	}
	if StandbyIsUnauthorized(validation) || StandbyIsConflict(validation) {
		t.Fatal("status helpers classified validation error as HTTP API error")
	}
}

func standbyBaseBackupBeginResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"base_backup_begin:manifest-a",
			"action_kind":"base_backup_begin",
			"target":"manifest-a",
			"state":"applied",
			"node_id":"primary-a"
		},
		"slot_name":"standby-a",
		"manifest_id":"manifest-a",
		"backup_lsn":7,
		"start_record_lsn":8
	}`
}

func standbyBaseBackupFinishResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"base_backup_finish:manifest-a",
			"action_kind":"base_backup_finish",
			"target":"manifest-a",
			"state":"applied",
			"node_id":"primary-a"
		},
		"manifest_id":"manifest-a",
		"backup_lsn":7,
		"end_record_lsn":9
	}`
}

func standbyBootstrapResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"standby_bootstrap:manifest-a",
			"action_kind":"standby_bootstrap",
			"target":"manifest-a",
			"state":"applied",
			"node_id":"standby-a"
		},
		"manifest_id":"manifest-a",
		"backup_lsn":7,
		"checkpoint_lsn":10
	}`
}

func standbyUpstreamResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"standby_upstream:standby-a",
			"action_kind":"standby_upstream",
			"target":"standby-a",
			"state":"applied",
			"node_id":"standby-a"
		},
		"identity":{
			"cluster_id":100,
			"shard_id":10,
			"table_id":20,
			"timeline_id":4,
			"epoch":6
		},
		"upstream":{
			"upstream_url":"https://primary-b.example:5433",
			"slot_name":"standby-a"
		},
		"previous":{
			"upstream_url":"https://primary-a.example:5433",
			"slot_name":"standby-a"
		},
		"changed":true
	}`
}

func standbyFenceAcquireResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"fence_acquire:standby-a",
			"action_kind":"fence_acquire",
			"target":"standby-a",
			"state":"applied",
			"node_id":"standby-a"
		},
		"receipt":{
			"identity":{
				"cluster_id":100,
				"shard_id":10,
				"table_id":20,
				"timeline_id":5,
				"epoch":7
			},
			"old_primary_id":"primary-a",
			"promoted_node_id":"standby-a",
			"parent_timeline_id":4,
			"parent_epoch":6,
			"new_timeline_id":5,
			"new_epoch":7,
			"required_lsn":12,
			"observed_lsn":12,
			"generation":3,
			"forced":false,
			"token":"ha-fence-token",
			"reason":"LeaseAcquired"
		}
	}`
}

func standbyPrimaryStatusResponseJSON() string {
	return `{
		"schema_version":1,
		"snapshot":{
			"role":"primary",
			"node_id":"primary-a",
			"identity":{
				"cluster_id":100,
				"shard_id":10,
				"table_id":20,
				"timeline_id":4,
				"epoch":6
			},
			"current_lsn":20,
			"retention":{
				"primary_lsn":20,
				"oldest_restart_lsn":12,
				"retained_lsn_count":8,
				"retained_byte_count":1024,
				"retained_age_ns":5000,
				"active_slots":1,
				"reseed_recommended":0
			},
			"durability":{
				"status":"satisfied",
				"mode":"remote_write",
				"selection":"any",
				"target_lsn":20,
				"progress_lsn":20,
				"missing_lsn_count":0,
				"satisfied_count":1,
				"required_count":1,
				"candidate_count":1
			},
			"slots":[{
				"name":"standby-a",
				"timeline_id":4,
				"active":true,
				"reseed_required":false,
				"restart_lsn":12,
				"received_lsn":18,
				"applied_lsn":17,
				"safe_read_lsn":17,
				"write_lag_lsn":2,
				"apply_lag_lsn":3,
				"safe_read_lag_lsn":3,
				"retention_lag_lsn":8,
				"status":"healthy"
			}]
		}
	}`
}

func standbyStatusResponseJSON() string {
	return `{
		"schema_version":1,
		"snapshot":{
			"role":"standby",
			"node_id":"standby-a",
			"identity":{
				"cluster_id":100,
				"shard_id":10,
				"table_id":20,
				"timeline_id":4,
				"epoch":6
			},
			"received_lsn":18,
			"applied_lsn":17,
			"safe_read_lsn":17,
			"upstream_lsn":20,
			"write_lag_lsn":2,
			"receive_lag_lsn":2,
			"apply_lag_lsn":3,
			"unapplied_lsn_count":1,
			"caught_up_to_received":false,
			"can_serve_safe_reads":true,
			"last_attempt_ns":1000,
			"last_success_ns":900,
			"replication_failures_total":1,
			"last_error":"transient pull timeout"
		}
	}`
}

func standbyPromotionResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"promotion:standby-a",
			"action_kind":"promotion",
			"target":"standby-a",
			"state":"applied",
			"node_id":"standby-a"
		},
		"assessment":{
			"required_lsn":12,
			"received_lsn":12,
			"applied_lsn":12,
			"has_required_lsn":true,
			"caught_up_to_received":true,
			"fencing_confirmed":true,
			"force":false,
			"mode":"safe",
			"data_loss_possible":false,
			"safe":true,
			"requires_fencing":false,
			"requires_force":false,
			"can_promote":true
		},
		"promotion":{
			"node_id":"standby-a",
			"old_identity":{
				"cluster_id":100,
				"shard_id":10,
				"table_id":20,
				"timeline_id":4,
				"epoch":6
			},
			"new_identity":{
				"cluster_id":100,
				"shard_id":10,
				"table_id":20,
				"timeline_id":5,
				"epoch":7
			},
			"switch_lsn":13,
			"forced":false,
			"data_loss_possible":false
		},
		"fence_generation":3,
		"fence_token":"ha-fence-token",
		"forced":false
	}`
}

func standbyPromotionAssessResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"promotion_assess:standby-a",
			"action_kind":"promotion_assess",
			"target":"standby-a",
			"state":"assessed",
			"node_id":"standby-a"
		},
		"assessment":{
			"required_lsn":12,
			"received_lsn":12,
			"applied_lsn":12,
			"has_required_lsn":true,
			"caught_up_to_received":true,
			"fencing_confirmed":true,
			"force":false,
			"mode":"safe",
			"data_loss_possible":false,
			"safe":true,
			"requires_fencing":false,
			"requires_force":false,
			"can_promote":true
		}
	}`
}

func standbyRejoinRewindResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"rejoin_rewind:primary-a",
			"action_kind":"rejoin_rewind",
			"target":"primary-a",
			"state":"applied",
			"node_id":"primary-a"
		},
		"assessment":{
			"action":"rewind",
			"reason":"parent_timeline_retained",
			"former_node_id":"primary-a",
			"target_timeline_id":5,
			"target_epoch":7,
			"parent_cluster_id":100,
			"parent_shard_id":10,
			"parent_table_id":20,
			"parent_timeline_id":4,
			"parent_epoch":6,
			"fork_lsn":12,
			"former_last_lsn":13,
			"retained_from_lsn":8,
			"data_loss_discarded":true
		},
		"rewind":{
			"node_id":"primary-a",
			"fork_lsn":12,
			"previous_last_lsn":13,
			"current_last_lsn":12,
			"next_lsn":13,
			"discarded_lsn_count":1,
			"target_timeline_id":5,
			"target_epoch":7,
			"data_loss_discarded":true
		}
	}`
}

func standbyRejoinReseedResponseJSON() string {
	return `{
		"schema_version":1,
		"action":{
			"action_id":"rejoin_reseed:primary-a",
			"action_kind":"rejoin_reseed",
			"target":"primary-a",
			"state":"applied",
			"node_id":"primary-current"
		},
		"assessment":{
			"action":"reseed",
			"reason":"parent_timeline_wal_expired",
			"former_node_id":"primary-a",
			"target_timeline_id":5,
			"target_epoch":7,
			"parent_cluster_id":100,
			"parent_shard_id":10,
			"parent_table_id":20,
			"parent_timeline_id":4,
			"parent_epoch":6,
			"fork_lsn":12,
			"former_last_lsn":13,
			"retained_from_lsn":14,
			"data_loss_discarded":false
		},
		"reseed":{
			"node_id":"primary-a",
			"slot_name":"primary-a",
			"target_timeline_id":5,
			"target_epoch":7,
			"fork_lsn":12,
			"former_last_lsn":13,
			"reseed_required":true,
			"base_backup_required":true
		}
	}`
}

func standbyCommitAppendResponseJSON() string {
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

func standbyCommitCheckResponseJSON() string {
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

func standbyWriteDecisionResponseJSON() string {
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

func standbyOwnerJobDecisionResponseJSON() string {
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

// --- PathStyle tests ---
//
// These cover the rename of the hot-standby admin surface from /admin/v1/ha
// to /admin/v1/standby: the default and explicit-legacy styles must keep
// sending the pre-rename paths (which is what the OpenAPI spec's generated
// client no longer builds on its own, now that the spec paths are
// canonical), the explicit-canonical style must send the new paths, and the
// deprecated HA* aliases introduced by the rename must still resolve and
// behave identically to their Standby* counterparts.

func TestStandbyClientPathStyleDefaultsToLegacy(t *testing.T) {
	t.Parallel()

	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if gotPath != HAPrimaryStatusPath {
		t.Fatalf("default path = %s, want legacy %s", gotPath, HAPrimaryStatusPath)
	}
}

func TestStandbyClientPathStyleLegacySendsLegacyPaths(t *testing.T) {
	t.Parallel()

	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleLegacy)
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if gotPath != HAPrimaryStatusPath {
		t.Fatalf("legacy path = %s, want %s", gotPath, HAPrimaryStatusPath)
	}
}

func TestStandbyClientPathStyleCanonicalSendsCanonicalPaths(t *testing.T) {
	t.Parallel()

	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleCanonical)
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if gotPath != StandbyPrimaryStatusPath {
		t.Fatalf("canonical path = %s, want %s", gotPath, StandbyPrimaryStatusPath)
	}
	if gotPath == HAPrimaryStatusPath {
		t.Fatalf("canonical path unexpectedly matched legacy path %s", HAPrimaryStatusPath)
	}
}

func TestStandbyClientPathStyleAndTokenAreIndependent(t *testing.T) {
	t.Parallel()

	var gotPath, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}

	client.WithPathStyle(PathStyleCanonical)
	client.WithToken("s3cr3t")
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if gotPath != StandbyPrimaryStatusPath {
		t.Fatalf("path after WithToken = %s, want canonical %s to survive", gotPath, StandbyPrimaryStatusPath)
	}
	if gotAuth != "Bearer s3cr3t" {
		t.Fatalf("Authorization = %q, want Bearer token", gotAuth)
	}

	client.WithPathStyle(PathStyleLegacy)
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if gotPath != HAPrimaryStatusPath {
		t.Fatalf("path after switching to legacy = %s, want %s", gotPath, HAPrimaryStatusPath)
	}
	if gotAuth != "Bearer s3cr3t" {
		t.Fatalf("Authorization after WithPathStyle = %q, want bearer token to survive", gotAuth)
	}

	client.WithToken("")
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization after clearing token = %q, want empty", gotAuth)
	}
	if gotPath != HAPrimaryStatusPath {
		t.Fatalf("path after clearing token = %s, want path style %s to survive", gotPath, HAPrimaryStatusPath)
	}
}

func TestDeprecatedHAAliasesResolveToStandbyTypes(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != HAPrimaryStatusPath {
			t.Fatalf("path = %s, want legacy %s", r.URL.Path, HAPrimaryStatusPath)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	// HAClient is a true type alias for StandbyClient, so the deprecated
	// constructor returns a value directly usable wherever *StandbyClient
	// is expected, and it defaults to PathStyleLegacy exactly like
	// NewStandbyClient.
	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	var _ = client

	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}

	// Deprecated func aliases behave identically to their canonical
	// counterparts.
	apiErr := &HAAPIError{Operation: "op", StatusCode: http.StatusConflict}
	if !HAIsConflict(apiErr) {
		t.Fatal("HAIsConflict(apiErr) = false, want true")
	}
	if HAIsConflict(apiErr) != StandbyIsConflict(apiErr) {
		t.Fatal("HAIsConflict and StandbyIsConflict disagree")
	}
	if code, ok := HAStatusCode(apiErr); !ok || code != http.StatusConflict {
		t.Fatalf("HAStatusCode(apiErr) = (%d, %v), want (%d, true)", code, ok, http.StatusConflict)
	}

	// Deprecated const aliases hold the same values as their canonical
	// counterparts.
	if HAActionStateApplied != StandbyActionStateApplied {
		t.Fatalf("HAActionStateApplied = %v, want %v", HAActionStateApplied, StandbyActionStateApplied)
	}
	if HAReadDecisionConsistencyStaleOK != StandbyReadDecisionConsistencyStaleOK {
		t.Fatalf("HAReadDecisionConsistencyStaleOK = %v, want %v", HAReadDecisionConsistencyStaleOK, StandbyReadDecisionConsistencyStaleOK)
	}

	// The generic HAResponse[T] alias is identical to StandbyResponse[T].
	value := 42
	resp := HAResponse[int]{Value: &value}
	var canonical = resp
	if canonical.Value != resp.Value || *canonical.Value != 42 {
		t.Fatalf("HAResponse[int] alias round-trip = %#v, want Value pointing at 42", canonical)
	}
}

const standbyAutoSlotCreateJSON = `{
	"schema_version":1,
	"slot_action":"create",
	"action":{"action_id":"replication_slot_create:standby-a","action_kind":"replication_slot_create","target":"standby-a","state":"applied","node_id":"primary-a"},
	"slot":{"slot_name":"standby-a","timeline_id":1,"restart_lsn":7,"received_lsn":7,"applied_lsn":7,"safe_read_lsn":7,"active":true,"reseed_required":false,"current_lsn":7}
}`

// legacyOnlyStandbyServer emulates a 0.2 server: canonical paths are unrouted
// and only the /admin/v1/ha spelling answers.
func legacyOnlyStandbyServer(t *testing.T, counts map[string]int) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()
		if strings.HasPrefix(r.URL.Path, StandbyPath) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case HAPrimaryStatusPath:
			_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
		case HAReplicationSlotsPath:
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), `"slot_name":"standby-a"`) {
				http.Error(w, "body was not replayed: "+string(body), http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprint(w, standbyAutoSlotCreateJSON)
		default:
			http.Error(w, "SlotNotFound", http.StatusNotFound)
		}
	}))
}

func TestStandbyClientPathStyleAutoStaysCanonicalWhenServed(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if !strings.HasPrefix(r.URL.Path, StandbyPath) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleAuto)
	for i := 0; i < 2; i++ {
		if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
			t.Fatalf("PrimaryStatusResponse returned error: %v", err)
		}
	}
	if len(paths) != 2 || paths[0] != StandbyPrimaryStatusPath || paths[1] != StandbyPrimaryStatusPath {
		t.Fatalf("paths = %v, want two canonical requests and no legacy probe", paths)
	}
	if style, pinned := client.negotiator.current(); style != PathStyleCanonical || !pinned {
		t.Fatalf("negotiated style = %v pinned=%v, want canonical pinned", style, pinned)
	}
}

func TestStandbyClientPathStyleAutoFallsBackToLegacyOnce(t *testing.T) {
	t.Parallel()

	counts := map[string]int{}
	server := legacyOnlyStandbyServer(t, counts)
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleAuto).WithToken("test-token")
	for i := 0; i < 3; i++ {
		if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
			t.Fatalf("PrimaryStatusResponse %d returned error: %v", i, err)
		}
	}
	if counts[StandbyPrimaryStatusPath] != 1 {
		t.Fatalf("canonical probes = %d, want exactly one before pinning legacy", counts[StandbyPrimaryStatusPath])
	}
	if counts[HAPrimaryStatusPath] != 3 {
		t.Fatalf("legacy requests = %d, want 3", counts[HAPrimaryStatusPath])
	}
	if style, pinned := client.negotiator.current(); style != PathStyleLegacy || !pinned {
		t.Fatalf("negotiated style = %v pinned=%v, want legacy pinned", style, pinned)
	}

	// A later client for the same base URL starts on the remembered spelling.
	second, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	second.WithPathStyle(PathStyleAuto)
	if _, err := second.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("second PrimaryStatusResponse returned error: %v", err)
	}
	if counts[StandbyPrimaryStatusPath] != 1 {
		t.Fatalf("canonical probes after cached client = %d, want still 1", counts[StandbyPrimaryStatusPath])
	}
}

func TestStandbyClientPathStyleAutoReplaysRequestBodyOnFallback(t *testing.T) {
	t.Parallel()

	counts := map[string]int{}
	server := legacyOnlyStandbyServer(t, counts)
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleAuto)
	resp, err := client.CreateReplicationSlot(context.Background(), ReplicationSlotCreateRequest{SlotName: "standby-a", InitialLsn: 7})
	if err != nil {
		t.Fatalf("CreateReplicationSlot returned error: %v", err)
	}
	if resp.Slot.SlotName != "standby-a" {
		t.Fatalf("SlotName = %q, want standby-a", resp.Slot.SlotName)
	}
	if counts[StandbyReplicationSlotsPath] != 1 || counts[HAReplicationSlotsPath] != 1 {
		t.Fatalf("counts = %v, want one canonical probe and one legacy replay", counts)
	}
}

func TestStandbyClientPathStyleAutoDoesNotReprobeTypedNotFound(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if !strings.HasPrefix(r.URL.Path, StandbyPath) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.URL.Path == StandbyPrimaryStatusPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
			return
		}
		http.Error(w, "SlotNotFound", http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleAuto)
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	before := len(paths)
	if _, err := client.PauseReplicationSlot(context.Background(), "missing"); err == nil {
		t.Fatalf("PauseReplicationSlot for a missing slot succeeded")
	}
	if got := paths[before:]; len(got) != 1 || !strings.HasPrefix(got[0], StandbyPath) {
		t.Fatalf("requests for typed 404 = %v, want a single canonical request and no legacy retry", got)
	}
}

func TestStandbyClientNegotiatedPathStyleReportsPinnedCanonical(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, StandbyPath) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleAuto)
	if style, pinned := client.NegotiatedPathStyle(); pinned {
		t.Fatalf("NegotiatedPathStyle before any request = %v pinned=%v, want unpinned", style, pinned)
	}
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if style, pinned := client.NegotiatedPathStyle(); style != PathStyleCanonical || !pinned {
		t.Fatalf("NegotiatedPathStyle = %v pinned=%v, want canonical pinned", style, pinned)
	}
}

func TestStandbyClientNegotiatedPathStyleReportsPinnedLegacyAfterFallback(t *testing.T) {
	t.Parallel()

	counts := map[string]int{}
	server := legacyOnlyStandbyServer(t, counts)
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleAuto)
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if style, pinned := client.NegotiatedPathStyle(); style != PathStyleLegacy || !pinned {
		t.Fatalf("NegotiatedPathStyle = %v pinned=%v, want legacy pinned", style, pinned)
	}
}

func TestStandbyClientNegotiatedPathStyleIgnoredForExplicitStyles(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, standbyGeneratedPrimaryStatusJSON())
	}))
	defer server.Close()

	client, err := NewStandbyClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewStandbyClient returned error: %v", err)
	}
	client.WithPathStyle(PathStyleCanonical)
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse returned error: %v", err)
	}
	if style, pinned := client.NegotiatedPathStyle(); pinned {
		t.Fatalf("NegotiatedPathStyle under explicit style = %v pinned=%v, want unpinned (no negotiation occurs)", style, pinned)
	}
}
