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
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestHAClientRejectsInvalidHAInputsLocally(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	identity := HAIdentity{
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
		Receipt:                         HAFenceReceipt{},
	}
	validSyncPolicy := HASyncPolicy{
		Mode:          HASyncPolicyModeRemoteWrite,
		Selection:     HASyncPolicySelectionAny,
		Required:      1,
		FailurePolicy: HASyncPolicyFailureFailClosed,
		StandbyNames:  []string{"standby-a"},
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
				_, err := client.PrimaryStatus(context.Background(), &HAPrimaryStatusParams{
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
				body.Receipt = HAFenceReceipt{
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
				body.Receipt = HAFenceReceipt{
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

func TestHAClientSeedWorkflowUsesAdminAPI(t *testing.T) {
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
			_, _ = fmt.Fprint(w, haBaseBackupBeginResponseJSON())
		case HABaseBackupsFinishPath:
			got := string(body)
			if !strings.Contains(got, `"manifest_path":"/backup/manifest-a.json"`) {
				t.Fatalf("base backup finish body = %s, want manifest path", got)
			}
			_, _ = fmt.Fprint(w, haBaseBackupFinishResponseJSON())
		case HAStandbyBootstrapPath:
			got := string(body)
			if !strings.Contains(got, `"manifest_path":"/backup/manifest-a.json"`) ||
				!strings.Contains(got, `"content_root":"/backup/files"`) {
				t.Fatalf("standby bootstrap body = %s, want manifest path and content root", got)
			}
			_, _ = fmt.Fprint(w, haStandbyBootstrapResponseJSON())
		default:
			t.Fatalf("path = %s, want HA seed workflow endpoint", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	client.WithToken("test-token")

	begin, err := client.BeginBaseBackup(context.Background(), BaseBackupStartRequest{
		SlotName:   "standby-a",
		ManifestId: "manifest-a",
	})
	if err != nil {
		t.Fatalf("BeginBaseBackup returned error: %v", err)
	}
	if begin.Action.ActionKind != HAActionKindBaseBackupBegin ||
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
	if finish.Action.ActionKind != HAActionKindBaseBackupFinish ||
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
	if bootstrap.Action.ActionKind != HAActionKindStandbyBootstrap ||
		bootstrap.Action.NodeId != "standby-a" ||
		bootstrap.BackupLsn != 7 ||
		bootstrap.CheckpointLsn != 10 {
		t.Fatalf("standby bootstrap response = %#v, want standby bootstrap evidence", bootstrap)
	}
}

func TestHAClientAcquireFenceUsesAdminAPI(t *testing.T) {
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
		_, _ = fmt.Fprint(w, haFenceAcquireResponseJSON())
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").AcquireFence(context.Background(), FenceAcquireRequest{
		Identity: HAIdentity{
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
		Reason:         "LeaseAcquired",
	})
	if err != nil {
		t.Fatalf("AcquireFence returned error: %v", err)
	}
	if resp.Action.ActionKind != HAActionKindFenceAcquire || resp.Action.NodeId != "standby-a" {
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

func TestHAClientPromoteWithCurrentFenceUsesAdminAPI(t *testing.T) {
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
		_, _ = fmt.Fprint(w, haPromotionResponseJSON())
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").PromoteWithCurrentFence(context.Background())
	if err != nil {
		t.Fatalf("PromoteWithCurrentFence returned error: %v", err)
	}
	if resp.Action.ActionKind != HAActionKindPromotion || resp.Action.NodeId != "standby-a" {
		t.Fatalf("promotion action = %#v, want standby promotion receipt", resp.Action)
	}
	if resp.Promotion.NewIdentity.TimelineId != 5 || resp.Promotion.SwitchLsn != 13 {
		t.Fatalf("promotion result = %#v, want new timeline 5 and switch LSN 13", resp.Promotion)
	}
	if resp.FenceGeneration != 3 || resp.FenceToken != "ha-fence-token" {
		t.Fatalf("fence evidence = generation %d token %q, want generation 3 token ha-fence-token", resp.FenceGeneration, resp.FenceToken)
	}
}

func TestHAClientAssessPromotionUsesAdminAPI(t *testing.T) {
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
		_, _ = fmt.Fprint(w, haPromotionAssessResponseJSON())
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
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
	if resp.Action.ActionKind != HAActionKindPromotionAssess || resp.Action.State != HAActionStateAssessed {
		t.Fatalf("promotion assess action = %#v, want assessed promotion receipt", resp.Action)
	}
	if !resp.Assessment.CanPromote || !resp.Assessment.Safe || resp.Assessment.Mode != HAPromotionModeSafe {
		t.Fatalf("promotion assessment = %#v, want safe promotable assessment", resp.Assessment)
	}
	if resp.Assessment.RequiredLsn != 12 || resp.Assessment.ReceivedLsn != 12 || resp.Assessment.AppliedLsn != 12 {
		t.Fatalf("promotion assessment LSNs = %#v, want all at 12", resp.Assessment)
	}
}

func TestHAClientRewindRejoinUsesAdminAPI(t *testing.T) {
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
		_, _ = fmt.Fprint(w, haRejoinRewindResponseJSON())
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").RewindRejoin(context.Background(), RejoinAssessRequest{
		NodeId:          "primary-a",
		LastLsn:         13,
		RetainedFromLsn: 8,
		Identity: HAIdentity{
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
	if resp.Action.ActionKind != HAActionKindRejoinRewind || resp.Action.NodeId != "primary-a" {
		t.Fatalf("rejoin action = %#v, want former-primary rewind receipt", resp.Action)
	}
	if resp.Assessment.Action != HARejoinActionRewind || resp.Assessment.Reason != HARejoinReasonParentTimelineRetained {
		t.Fatalf("rejoin assessment = %#v, want rewind on retained parent timeline", resp.Assessment)
	}
	if resp.Rewind.NodeId != "primary-a" ||
		resp.Rewind.TargetTimelineId != 5 ||
		resp.Rewind.NextLsn != 13 ||
		!resp.Rewind.DataLossDiscarded {
		t.Fatalf("rejoin rewind result = %#v, want rewind evidence for primary-a", resp.Rewind)
	}
}

func TestHAClientReseedRejoinUsesAdminAPI(t *testing.T) {
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
		_, _ = fmt.Fprint(w, haRejoinReseedResponseJSON())
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	resp, err := client.WithToken("test-token").ReseedRejoin(context.Background(), RejoinAssessRequest{
		NodeId:          "primary-a",
		LastLsn:         13,
		RetainedFromLsn: 14,
		Identity: HAIdentity{
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
	if resp.Action.ActionKind != HAActionKindRejoinReseed || resp.Action.NodeId != "primary-current" {
		t.Fatalf("rejoin action = %#v, want current-primary reseed receipt", resp.Action)
	}
	if resp.Assessment.Action != HARejoinActionReseed || resp.Assessment.Reason != HARejoinReasonParentTimelineWALExpired {
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

func TestHAClientWithTokenCanChangeAndClearBearerAuth(t *testing.T) {
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

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
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

func TestHAClientStatusWrappersExposeLagAndRetention(t *testing.T) {
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
			_, _ = fmt.Fprint(w, haPrimaryStatusResponseJSON())
		case HAStandbyStatusPath:
			if got := r.URL.Query().Get("upstream_lsn"); got != "20" {
				t.Fatalf("standby upstream_lsn = %q, want 20", got)
			}
			_, _ = fmt.Fprint(w, haStandbyStatusResponseJSON())
		default:
			t.Fatalf("path = %s, want HA status endpoint", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewHAClient returned error: %v", err)
	}
	client.WithToken("test-token")

	primary, err := client.PrimaryStatus(context.Background(), &HAPrimaryStatusParams{
		MaxLagLsn:        8,
		MaxRetainedBytes: 1024,
		MaxRetainedAgeNs: 5000,
		SyncMode:         HAPrimaryStatusSyncModeRemoteWrite,
		SyncSelection:    HAPrimaryStatusSyncSelectionAny,
		SyncRequired:     1,
		SyncStandby:      []string{"standby-a"},
		SyncFailure:      HAPrimaryStatusSyncFailureFailClosed,
	})
	if err != nil {
		t.Fatalf("PrimaryStatus returned error: %v", err)
	}
	if primary.Snapshot.Retention.RetainedLsnCount != 8 ||
		primary.Snapshot.Retention.RetainedByteCount != 1024 ||
		primary.Snapshot.Slots[0].RetentionLagLsn != 8 ||
		primary.Snapshot.Slots[0].WriteLagLsn != 2 ||
		primary.Snapshot.Durability.Mode != HADurabilityModeRemoteWrite {
		t.Fatalf("primary status = %#v, want retention, lag, and remote_write durability evidence", primary.Snapshot)
	}

	standby, err := client.StandbyStatus(context.Background(), &HAStandbyStatusParams{UpstreamLsn: 20})
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

func TestHAClientAcceptsHARootURL(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/ha/fence/current" {
			t.Fatalf("path = %s, want /admin/v1/ha/fence/current", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"schema_version":1,"held":false}`)
	}))
	defer server.Close()

	client, err := NewHAClient(server.URL+"/admin/v1/ha", server.Client())
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

func TestHAClientRejectsInvalidBaseURLs(t *testing.T) {
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
			if _, err := NewHAClient(baseURL, nil); err == nil || !strings.Contains(err.Error(), "invalid HA admin base URL") {
				t.Fatalf("NewHAClient(%q) error = %v, want invalid HA admin base URL", baseURL, err)
			}
		})
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

func TestHAClientGeneratedSpecIsDedicatedAdminAPI(t *testing.T) {
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
	pathItem := spec.Paths.Find("/ha/primary/status")
	if pathItem == nil || pathItem.Get == nil {
		t.Fatalf("/ha/primary/status operation = %#v, want GET operation", pathItem)
	}
	req, err := oapi.NewGetHAPrimaryStatusRequest("http://admin.test"+AdminV1Path+"/", nil)
	if err != nil {
		t.Fatalf("NewGetHAPrimaryStatusRequest returned error: %v", err)
	}
	if req.Method != http.MethodGet || req.URL.Path != HAPrimaryStatusPath {
		t.Fatalf("generated primary status request = %s %s, want GET %s", req.Method, req.URL.Path, HAPrimaryStatusPath)
	}

	sourceSpec := loadSourceAdminOpenAPISpec(t)
	sourceOperations := haOpenAPIOperations(sourceSpec)
	generatedOperations := haOpenAPIOperations(spec)
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

func haOpenAPIOperations(spec *openapi3.T) map[string]string {
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

func TestHAOperationMetadataUsesAdminAPIPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		got       HAOperation
		generated func(*testing.T) HAOperation
	}{
		{
			name: "primary status",
			got:  HAPrimaryStatusOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewGetHAPrimaryStatusRequest(server, nil)
			}),
		},
		{
			name: "standby status",
			got:  HAStandbyStatusOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewGetHAStandbyStatusRequest(server, nil)
			}),
		},
		{
			name: "check commit",
			got:  HACheckCommitOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHACommitRequest(server, oapi.CheckHACommitJSONRequestBody{})
			}),
		},
		{
			name: "append commit",
			got:  HAAppendCommitOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewAppendHACommitRequest(server, oapi.AppendHACommitJSONRequestBody{})
			}),
		},
		{
			name: "check read",
			got:  HACheckReadOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHAReadRequest(server, oapi.CheckHAReadJSONRequestBody{})
			}),
		},
		{
			name: "check write",
			got:  HACheckWriteOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHAWriteRequest(server, oapi.CheckHAWriteJSONRequestBody{})
			}),
		},
		{
			name: "check owner job",
			got:  HACheckOwnerJobOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewCheckHAOwnerJobRequest(server, oapi.CheckHAOwnerJobJSONRequestBody{})
			}),
		},
		{
			name: "list replication slots",
			got:  HAListReplicationSlotsOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewListHAReplicationSlotsRequest(server)
			}),
		},
		{
			name: "create replication slot",
			got:  HACreateReplicationSlotOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewCreateHAReplicationSlotRequest(server, oapi.CreateHAReplicationSlotJSONRequestBody{})
			}),
		},
		{
			name: "begin base backup",
			got:  HABeginBaseBackupOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewBeginHABaseBackupRequest(server, oapi.BeginHABaseBackupJSONRequestBody{})
			}),
		},
		{
			name: "finish base backup",
			got:  HAFinishBaseBackupOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewFinishHABaseBackupRequest(server, oapi.FinishHABaseBackupJSONRequestBody{})
			}),
		},
		{
			name: "capture seed artifact",
			got:  HASeedCaptureOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewCaptureHASeedArtifactRequest(server, oapi.CaptureHASeedArtifactJSONRequestBody{})
			}),
		},
		{
			name: "activate seeded slot",
			got:  HAActivateSeededSlotOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewActivateHASeededSlotRequest(server, oapi.ActivateHASeededSlotJSONRequestBody{})
			}),
		},
		{
			name: "bootstrap standby",
			got:  HABootstrapStandbyOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewBootstrapHAStandbyRequest(server, oapi.BootstrapHAStandbyJSONRequestBody{})
			}),
		},
		{
			name: "acquire fence",
			got:  HAAcquireFenceOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewAcquireHAFenceRequest(server, oapi.AcquireHAFenceJSONRequestBody{})
			}),
		},
		{
			name: "current fence",
			got:  HACurrentFenceOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewGetHACurrentFenceRequest(server)
			}),
		},
		{
			name: "assess promotion",
			got:  HAAssessPromotionOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewAssessHAPromotionRequest(server, oapi.AssessHAPromotionJSONRequestBody{})
			}),
		},
		{
			name: "promote with current fence",
			got:  HAPromoteWithCurrentFenceOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewPromoteHAWithCurrentFenceRequest(server)
			}),
		},
		{
			name: "promote",
			got:  HAPromoteOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewPromoteHARequest(server, oapi.PromoteHAJSONRequestBody{})
			}),
		},
		{
			name: "assess rejoin",
			got:  HAAssessRejoinOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewAssessHARejoinRequest(server, oapi.AssessHARejoinJSONRequestBody{})
			}),
		},
		{
			name: "rewind rejoin",
			got:  HARewindRejoinOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
				return oapi.NewRewindHARejoinRequest(server, oapi.RewindHARejoinJSONRequestBody{})
			}),
		},
		{
			name: "reseed rejoin",
			got:  HAReseedRejoinOperation(),
			generated: generatedHAOperation(func(server string) (*http.Request, error) {
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

	const slotName = "standby-a.1:zone_9"

	slotPath, ok := HAReplicationSlotPath(slotName)
	if !ok {
		t.Fatal("HAReplicationSlotPath returned ok=false for valid slot")
	}
	if slotPath != HAReplicationSlotPathPrefix+url.PathEscape(slotName) {
		t.Fatalf("slot path = %q, want escaped valid slot path", slotPath)
	}
	generatedDrop := generatedHAOperation(func(server string) (*http.Request, error) {
		return oapi.NewDropHAReplicationSlotRequest(server, slotName)
	})(t)
	if dropPath := (HAOperation{Method: http.MethodDelete, Path: slotPath}); dropPath != generatedDrop {
		t.Fatalf("drop slot path operation = %#v, want generated OpenAPI operation %#v", dropPath, generatedDrop)
	}
	resume, ok := HAResumeReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("HAResumeReplicationSlotOperation returned ok=false")
	}
	if want := generatedHAOperation(func(server string) (*http.Request, error) {
		return oapi.NewResumeHAReplicationSlotRequest(server, slotName)
	})(t); resume != want {
		t.Fatalf("resume operation = %#v, want generated OpenAPI operation %#v", resume, want)
	}
	pause, ok := HAPauseReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("HAPauseReplicationSlotOperation returned ok=false")
	}
	if want := generatedHAOperation(func(server string) (*http.Request, error) {
		return oapi.NewPauseHAReplicationSlotRequest(server, slotName)
	})(t); pause != want {
		t.Fatalf("pause operation = %#v, want generated OpenAPI operation %#v", pause, want)
	}
	drop, ok := HADropReplicationSlotOperation(slotName)
	if !ok {
		t.Fatal("HADropReplicationSlotOperation returned ok=false")
	}
	if drop != generatedDrop {
		t.Fatalf("drop operation = %#v, want generated OpenAPI operation %#v", drop, generatedDrop)
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

func generatedHAOperation(build func(string) (*http.Request, error)) func(*testing.T) HAOperation {
	return func(t *testing.T) HAOperation {
		t.Helper()
		req, err := build("http://admin.test" + AdminV1Path + "/")
		if err != nil {
			t.Fatalf("generated OpenAPI request builder returned error: %v", err)
		}
		return HAOperation{Method: req.Method, Path: req.URL.EscapedPath()}
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
			name:      "capture seed artifact",
			got:       HASeedCaptureReceiptExpectation(),
			wantKind:  "seed_capture",
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
	paddedSlotTarget := response
	paddedSlotTarget.Action.Target = " standby-a"
	if err := ValidateHAReplicationSlotActionResponse(paddedSlotTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("padded slot target error = %v, want receipt mismatch", err)
	}
	paddedActionID := response
	paddedActionID.Action.ActionId = "replication_slot_create:standby-a "
	if err := ValidateHAReplicationSlotActionResponse(paddedActionID); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("padded action id error = %v, want receipt mismatch", err)
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
	badListSlot.SlotName = "standby a"
	if err := ValidateHAReplicationSlotListResponse(HAReplicationSlotListResponse{
		SchemaVersion: 1,
		Slots:         []HAReplicationSlot{badListSlot},
	}); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("invalid slot name error = %v, want slot fields error", err)
	}

	response.Action.NodeId = ""
	if err := ValidateHAReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("missing node id error = %v, want receipt error", err)
	}
	response.Action.NodeId = "primary a"
	if err := ValidateHAReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("invalid node id error = %v, want receipt error", err)
	}
	response.Action.NodeId = "primary-a"

	response.SlotAction = HAReplicationSlotAction("invalid")
	if err := ValidateHAReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "invalid replication slot action") {
		t.Fatalf("invalid slot action error = %v, want invalid action error", err)
	}
	response.SlotAction = HAReplicationSlotActionCreate

	response.Slot.SlotName = "standby a"
	if err := ValidateHAReplicationSlotActionResponse(response); err == nil || !strings.Contains(err.Error(), "slot fields") {
		t.Fatalf("invalid slot name error = %v, want slot fields error", err)
	}
	response.Slot.SlotName = "standby-a"

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

	digest := strings.Repeat("a", 64)
	capture := HASeedArtifactCaptureResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "seed_capture:seed-standby-a-7",
			ActionKind: HAActionKindSeedCapture,
			Target:     "seed-standby-a-7",
			State:      HAActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotName:         "standby-a",
		Generation:       "seed-standby-a-7",
		ClusterId:        1,
		TimelineId:       4,
		Epoch:            2,
		ManifestId:       "seed-standby-a-7",
		SourcePlanSha256: digest,
		BackupLsn:        7,
		CheckpointLsn:    7,
		EndRecordLsn:     8,
		ManifestSha256:   digest,
		FileCount:        2,
		TotalBytes:       20,
		GenerationRoot:   "/antflydb/ha/seed-captures/generations/seed-standby-a-7",
		ContentRoot:      "/antflydb/ha/seed-captures/generations/seed-standby-a-7/content",
		ManifestPath:     "/antflydb/ha/seed-captures/generations/seed-standby-a-7/manifest.afha",
	}
	if err := ValidateHASeedArtifactCaptureResponse(capture); err != nil {
		t.Fatalf("ValidateHASeedArtifactCaptureResponse returned error: %v", err)
	}
	badCapture := capture
	badCapture.SourcePlanSha256 = strings.ToUpper(digest)
	if err := ValidateHASeedArtifactCaptureResponse(badCapture); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("invalid capture digest error = %v, want digest error", err)
	}

	activation := HASeededSlotActivateResponse{
		SchemaVersion: 1,
		Action: HAActionReceipt{
			ActionId:   "seeded_slot_activate:seed-standby-a-7",
			ActionKind: HAActionKindSeededSlotActivate,
			Target:     "seed-standby-a-7",
			State:      HAActionStateApplied,
			NodeId:     "primary-a",
		},
		SlotName:          "standby-a",
		Generation:        "seed-standby-a-7",
		ManifestId:        "manifest-a",
		TimelineId:        4,
		CheckpointLsn:     10,
		SeedReceiptSha256: digest,
		ManifestSha256:    digest,
		AggregateSha256:   digest,
	}
	if err := ValidateHASeededSlotActivateResponse(activation); err != nil {
		t.Fatalf("ValidateHASeededSlotActivateResponse returned error: %v", err)
	}
	wrongActivationTarget := activation
	wrongActivationTarget.Action.Target = "seed-standby-a-8"
	if err := ValidateHASeededSlotActivateResponse(wrongActivationTarget); err == nil || !strings.Contains(err.Error(), "receipt") {
		t.Fatalf("wrong activation target error = %v, want receipt mismatch", err)
	}
	activation.SeedReceiptSha256 = strings.Repeat("A", 64)
	if err := ValidateHASeededSlotActivateResponse(activation); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("invalid activation digest error = %v, want digest error", err)
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
	formerPrimaryCopy := response
	formerPrimaryCopy.Action.NodeId = "primary-a"
	if err := ValidateHAFenceResponse(formerPrimaryCopy); err != nil {
		t.Fatalf("ValidateHAFenceResponse rejected former-primary receipt copy: %v", err)
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
	paddedActionTarget := response
	paddedActionTarget.Action.Target = "standby-a "
	if err := ValidateHAFenceResponse(paddedActionTarget); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("padded fence action target error = %v, want action node mismatch", err)
	}
	paddedActionID := response
	paddedActionID.Action.ActionId = "fence_acquire:standby-a "
	if err := ValidateHAFenceResponse(paddedActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded fence action id error = %v, want action id mismatch", err)
	}
	invalidReceiptNode := response
	invalidReceiptNode.Receipt.PromotedNodeId = "standby a"
	if err := ValidateHAFenceResponse(invalidReceiptNode); err == nil || !strings.Contains(err.Error(), "receipt fields") {
		t.Fatalf("invalid fence receipt node error = %v, want receipt fields", err)
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
		Force:              false,
		Mode:               HAPromotionModeSafe,
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
	emptyStandbyAssess := assess
	emptyStandbyAssess.Assessment.RequiredLsn = 0
	emptyStandbyAssess.Assessment.ReceivedLsn = 0
	emptyStandbyAssess.Assessment.AppliedLsn = 0
	emptyStandbyAssess.Assessment.HasRequiredLsn = true
	if err := ValidateHAPromotionAssessResponse(emptyStandbyAssess); err != nil {
		t.Fatalf("ValidateHAPromotionAssessResponse with zero required_lsn returned error: %v", err)
	}
	wrongAssessNode := assess
	wrongAssessNode.Action.NodeId = "standby-b"
	if err := ValidateHAPromotionAssessResponse(wrongAssessNode); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("wrong promotion assess executor error = %v, want executor node mismatch", err)
	}
	paddedAssessTarget := assess
	paddedAssessTarget.Action.Target = "standby-a "
	if err := ValidateHAPromotionAssessResponse(paddedAssessTarget); err == nil || !strings.Contains(err.Error(), "executor node mismatch") {
		t.Fatalf("padded promotion assess target error = %v, want executor node mismatch", err)
	}
	paddedAssessActionID := assess
	paddedAssessActionID.Action.ActionId = "promotion_assess:standby-a "
	if err := ValidateHAPromotionAssessResponse(paddedAssessActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded promotion assess action id error = %v, want action id mismatch", err)
	}
	inconsistentAssess := assess
	inconsistentAssess.Assessment.HasRequiredLsn = false
	if err := ValidateHAPromotionAssessResponse(inconsistentAssess); err == nil || !strings.Contains(err.Error(), "has_required_lsn") {
		t.Fatalf("inconsistent promotion assessment error = %v, want has_required_lsn mismatch", err)
	}
	wrongMode := assess
	wrongMode.Assessment.Mode = HAPromotionModeForced
	if err := ValidateHAPromotionAssessResponse(wrongMode); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("wrong promotion assessment mode error = %v, want mode mismatch", err)
	}
	assess.Assessment.RequiredLsn = 9
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
	paddedPromotionTarget := promotion
	paddedPromotionTarget.Action.Target = "standby-a "
	if err := ValidateHAPromotionResponse(paddedPromotionTarget); err == nil || !strings.Contains(err.Error(), "action node mismatch") {
		t.Fatalf("padded promotion target error = %v, want action node mismatch", err)
	}
	paddedPromotionActionID := promotion
	paddedPromotionActionID.Action.ActionId = "promotion:standby-a "
	if err := ValidateHAPromotionResponse(paddedPromotionActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded promotion action id error = %v, want action id mismatch", err)
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
	if err := ValidateHAReplicationSlotActionResponseEvidence([]byte(strings.Replace(slot, `"slot_name":"standby-a",`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot name evidence error = %v, want slot field evidence error", err)
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
	if err := ValidateHAReplicationSlotListResponseEvidence([]byte(strings.Replace(slotList, `,"timeline_id":1`, "", 1))); err == nil || !strings.Contains(err.Error(), "slot field evidence") {
		t.Fatalf("missing slot timeline evidence error = %v, want slot field evidence error", err)
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

	assessment := `"assessment":{"required_lsn":8,"received_lsn":8,"applied_lsn":8,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}`
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
	fencedWrite := HAWriteCheckResponse{
		SchemaVersion: 1,
		Decision: HAWriteDecision{
			Action:     HAWriteDecisionActionRejectFencedPrimary,
			Role:       HAWriteDecisionRoleFencedPrimary,
			Identity:   identity,
			DurableLsn: 9,
			NextLsn:    10,
		},
	}
	if err := ValidateHAWriteCheckResponse(fencedWrite); err != nil {
		t.Fatalf("ValidateHAWriteCheckResponse fenced primary returned error: %v", err)
	}
	badFencedWrite := fencedWrite
	badFencedWrite.Decision.Action = HAWriteDecisionActionAllowWrite
	if err := ValidateHAWriteCheckResponse(badFencedWrite); err == nil || !strings.Contains(err.Error(), "fenced_primary role action") {
		t.Fatalf("invalid fenced write action error = %v, want fenced_primary role action error", err)
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
	assessRewind := base
	assessRewind.Assessment.Action = HARejoinActionRewind
	assessRewind.Assessment.Reason = HARejoinReasonParentTimelineRetained
	if err := ValidateHARejoinAssessResponse(assessRewind); err != nil {
		t.Fatalf("ValidateHARejoinAssessResponse assess rewind returned error: %v", err)
	}
	wrongTarget := base
	wrongTarget.Action.Target = "primary-b"
	if err := ValidateHARejoinAssessResponse(wrongTarget); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("wrong target error = %v, want target mismatch error", err)
	}
	paddedTarget := base
	paddedTarget.Action.Target = " primary-a"
	if err := ValidateHARejoinAssessResponse(paddedTarget); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("padded target error = %v, want target mismatch error", err)
	}
	paddedActionID := base
	paddedActionID.Action.ActionId = "rejoin_assess:primary-a "
	if err := ValidateHARejoinAssessResponse(paddedActionID); err == nil || !strings.Contains(err.Error(), "action id") {
		t.Fatalf("padded action id error = %v, want action id mismatch error", err)
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
	wrapped := fmt.Errorf("operator context: %w", err)
	status, ok := HAStatusCode(wrapped)
	if !ok || status != http.StatusConflict {
		t.Fatalf("HAStatusCode(wrapped) = %d, %t, want %d, true", status, ok, http.StatusConflict)
	}
	if !HAIsConflict(wrapped) {
		t.Fatalf("HAIsConflict(wrapped) = false, want true")
	}
	if HAIsUnauthorized(wrapped) {
		t.Fatalf("HAIsUnauthorized(wrapped) = true, want false")
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
			if got := HAIsRetryable(tt.err); got != tt.want {
				t.Fatalf("HAIsRetryable(%T) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestHAStatusHelpersClassifyWrappedErrors(t *testing.T) {
	t.Parallel()

	unauthorized := fmt.Errorf("direct admin call failed: %w", &HAAPIError{
		Operation:  "get HA primary status",
		StatusCode: http.StatusUnauthorized,
		Body:       "missing bearer token",
	})
	if !HAIsUnauthorized(unauthorized) {
		t.Fatal("HAIsUnauthorized(wrapped unauthorized) = false, want true")
	}
	if HAIsConflict(unauthorized) {
		t.Fatal("HAIsConflict(wrapped unauthorized) = true, want false")
	}
	status, ok := HAStatusCode(unauthorized)
	if !ok || status != http.StatusUnauthorized {
		t.Fatalf("HAStatusCode(wrapped unauthorized) = %d, %t, want %d, true", status, ok, http.StatusUnauthorized)
	}

	validation := fmt.Errorf("missing evidence: %w", &HAResponseValidationError{
		Operation: "create HA replication slot",
		Err:       errors.New("missing action receipt"),
	})
	if _, ok := HAStatusCode(validation); ok {
		t.Fatal("HAStatusCode(validation) returned true, want false")
	}
	if HAIsUnauthorized(validation) || HAIsConflict(validation) {
		t.Fatal("status helpers classified validation error as HTTP API error")
	}
}

func haBaseBackupBeginResponseJSON() string {
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

func haBaseBackupFinishResponseJSON() string {
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

func haStandbyBootstrapResponseJSON() string {
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

func haFenceAcquireResponseJSON() string {
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

func haPrimaryStatusResponseJSON() string {
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

func haStandbyStatusResponseJSON() string {
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

func haPromotionResponseJSON() string {
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

func haPromotionAssessResponseJSON() string {
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

func haRejoinRewindResponseJSON() string {
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

func haRejoinReseedResponseJSON() string {
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
