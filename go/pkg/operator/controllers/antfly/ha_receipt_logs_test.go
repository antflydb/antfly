// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func activationLogFixture(t *testing.T) (antflyv1.HAPlannedActionStatus, string) {
	t.Helper()
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionActivateSeedArtifact), SlotName: "standby-a", StandbyName: "standby-a", TargetLSN: 10,
		SeedArtifactGeneration: "seed-standby-a-10", SeedCaptureReceiptSHA256: strings.Repeat("d", 64),
		TargetLocalNodeID: 7, TargetReplicaID: 1, TopologyID: "test-standalone", TopologyGeneration: 3,
		TopologyNodeID: "standby-a", TargetPVCName: "standby-a-data", TargetPVCUID: "pvc-uid-1",
		AdminJobName: "activation-job", AdminJobPhase: haAdminJobPhaseSucceeded,
	}
	receipt := map[string]any{
		"format_version": 2, "generation": action.SeedArtifactGeneration, "slot_name": action.SlotName,
		"cluster_id": 100, "shard_id": 0, "table_id": 0, "timeline_id": 1, "epoch": 1,
		"manifest_id": "base-standby-a-10", "backup_lsn": 10, "checkpoint_lsn": 12,
		"seed_receipt_sha256": strings.Repeat("c", 64), "capture_receipt_sha256": action.SeedCaptureReceiptSHA256,
		"manifest_sha256": strings.Repeat("a", 64), "aggregate_sha256": strings.Repeat("b", 64),
		"materialized_receipt_sha256": strings.Repeat("e", 64), "materialized_aggregate_sha256": strings.Repeat("f", 64),
		"generation_path": "live-generations/" + action.SeedArtifactGeneration, "raw_generation_path": "generations/" + action.SeedArtifactGeneration,
		"target_local_node_id": 7, "target_replica_id": 1, "topology_id": action.TopologyID, "topology_generation": 3,
		"node_id": action.TopologyNodeID, "target_pvc_name": action.TargetPVCName, "target_pvc_uid": action.TargetPVCUID,
	}
	body, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return action, string(body)
}

const activationWarning = "warning: dense replay target advance deferred to artifact maintenance index=document_vectors indexed=0 expected_docs=2\n"

func TestHAReceiptLogContract(t *testing.T) {
	action, body := activationLogFixture(t)
	for _, tt := range []struct {
		name, body string
		valid      bool
	}{
		{"clean", body, true}, {"warning prefixed", activationWarning + activationWarning + body, true},
		{"warning with braces", "warning: diagnostic {context}\n" + body, true},
		{"truncated", activationWarning + body[:len(body)-1], false}, {"malformed", activationWarning + "{broken}", false},
		{"unknown field", activationWarning + strings.TrimSuffix(body, "}") + `,"unexpected":true}`, false},
		{"two receipts", activationWarning + body + "\n" + body, false}, {"object before", "{}\n" + body, false},
		{"object after", activationWarning + body + "\n{}", false}, {"trailing warning", activationWarning + body + "\nwarning: done", false},
		{"unrecognized prefix", "info: completed\n" + body, false}, {"only warning", activationWarning, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseHASeedArtifactReceipt(tt.body, action); (got != nil) != tt.valid {
				t.Fatalf("valid=%v want %v", got != nil, tt.valid)
			}
		})
	}
	for _, field := range []string{"topology_id", "topology_generation", "generation", "slot_name", "target_pvc_uid", "capture_receipt_sha256", "manifest_sha256", "node_id"} {
		t.Run("wrong "+field, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal([]byte(body), &value); err != nil {
				t.Fatal(err)
			}
			if field == "topology_generation" {
				value[field] = 4
			} else {
				value[field] = "wrong"
			}
			altered, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if parseHASeedArtifactReceipt(activationWarning+string(altered), action) != nil {
				t.Fatal("invalid binding accepted")
			}
		})
	}
}

func TestHAReceiptLogRecoveryDoesNotRerunActivation(t *testing.T) {
	for _, tt := range []struct {
		valid bool
		ttl   *int32
	}{
		{false, nil}, {true, nil}, {false, new(int32)}, {true, new(int32)},
	} {
		valid := tt.valid
		t.Run(fmt.Sprintf("valid=%v/ttl=%v", valid, tt.ttl != nil), func(t *testing.T) {
			ctx := context.Background()
			action, body := activationLogFixture(t)
			if !valid {
				body = body[:len(body)-1]
			}
			cluster := startupGatedStandaloneControllerCluster(true)
			cluster.Spec.HighAvailability.Runtime.StartupGate.RequiredReceipt.Generation = action.SeedArtifactGeneration
			artifact := cluster.Spec.HighAvailability.Standbys[0].SeedArtifact
			artifact.TopologyID = action.TopologyID
			artifact.TopologyGeneration = action.TopologyGeneration
			artifact.NodeID = action.TopologyNodeID
			artifact.TargetPVCUID = action.TargetPVCUID
			artifact.Generation = action.SeedArtifactGeneration
			cluster.Spec.HighAvailability.Runtime.StartupGate.RequiredReceipt.TargetPVCUID = action.TargetPVCUID
			cluster.Spec.HighAvailability.Standbys[0].SeedArtifact.SourcePVC = &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"}
			cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{ExecutePlannedActions: true}
			action.SourcePVCName = "primary-data"
			action.SourcePVCUID = "primary-pvc-uid"
			action.OperationID = haPlannedActionOperationID(action)
			cluster.Status.HAStatus = &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{action}}
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: action.AdminJobName, Namespace: cluster.Namespace}, Spec: batchv1.JobSpec{TTLSecondsAfterFinished: tt.ttl}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "activation-pod", Namespace: cluster.Namespace, Labels: map[string]string{"job-name": action.AdminJobName}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: action.TargetPVCName, Namespace: cluster.Namespace, UID: types.UID(action.TargetPVCUID)}}
			sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "primary-data", Namespace: cluster.Namespace, UID: "primary-pvc-uid"}}
			api := newHAControllerTestClient(t, scheme, cluster, job, pod, pvc, sourcePVC)
			reads := 0
			kube, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "https://synthetic.invalid"}, &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/api/v1/namespaces/"+cluster.Namespace+"/pods/activation-pod/log" {
					t.Fatalf("unexpected request %s", req.URL.Path)
				}
				reads++
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(activationWarning + body))}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			r := &AntflyClusterReconciler{Client: api, Scheme: scheme, KubeClient: kube}
			if valid {
				if err := r.reconcileHAAdminJobs(ctx, cluster); !errors.Is(err, errHAPlanNeedsPersistence) {
					t.Fatalf("recovery must checkpoint before proceeding: %v", err)
				}
				before := &batchv1.Job{}
				if err := api.Get(ctx, client.ObjectKeyFromObject(job), before); err != nil {
					t.Fatal(err)
				}
				if before.Spec.TTLSecondsAfterFinished != nil {
					t.Fatal("TTL armed before recovered evidence was persisted")
				}
				persisted := &antflyv1.AntflyCluster{}
				if err := api.Get(ctx, client.ObjectKeyFromObject(cluster), persisted); err != nil {
					t.Fatal(err)
				}
				if persisted.Status.HAStatus.PlannedActions[0].SeedArtifactReceipt != nil {
					t.Fatal("unexpected persisted receipt before checkpoint")
				}
				if err := r.persistHAActionPlanBarrier(ctx, cluster); err != nil {
					t.Fatal(err)
				}
			}
			if err := reconcileHAAdminJobsUntilIdle(ctx, r, cluster); err != nil {
				t.Fatal(err)
			}
			recovered := cluster.Status.HAStatus.PlannedActions[0]
			if reads == 0 || recovered.AdminJobName != action.AdminJobName || recovered.AdminJobPhase != haAdminJobPhaseSucceeded {
				t.Fatalf("attempt lost: %#v reads=%d", recovered, reads)
			}
			if haAdminActionSucceededWithEvidence(recovered) != valid {
				t.Fatalf("evidence valid=%v want %v", haAdminActionSucceededWithEvidence(recovered), valid)
			}
			if err := r.ensureHAAdminJobTTLAfterCheckpoint(ctx, cluster, cluster.Spec.HighAvailability.Admin, &recovered); err != nil {
				t.Fatal(err)
			}
			observed := &batchv1.Job{}
			if err := api.Get(ctx, client.ObjectKeyFromObject(job), observed); err != nil {
				t.Fatal(err)
			}
			if (observed.Spec.TTLSecondsAfterFinished != nil) != valid {
				t.Fatal("TTL must wait for validated evidence")
			}
			jobs := &batchv1.JobList{}
			if err := api.List(ctx, jobs); err != nil {
				t.Fatal(err)
			}
			if len(jobs.Items) != 1 {
				t.Fatal("activation rerun")
			}
			if valid {
				r.updateHAStartupGateStatus(ctx, cluster)
				if cluster.Status.HAStatus.StartupGate == nil || cluster.Status.HAStatus.StartupGate.RuntimeEligible || cluster.Status.HAStatus.StartupGate.Reason != "TargetGenerationGCNotObserved" {
					t.Fatalf("startup bypassed cleanup: %#v", cluster.Status.HAStatus.StartupGate)
				}
				// Normal downstream cleanup must still checkpoint its own exact
				// evidence before the recovered activation can open startup.
				cluster.Status.HAStatus.PlannedActions = append(cluster.Status.HAStatus.PlannedActions, antflyv1.HAPlannedActionStatus{
					Kind: string(haActionGCTargetSeedGenerations), SlotName: action.SlotName, SeedArtifactGeneration: action.SeedArtifactGeneration,
					AdminJobPhase: haAdminJobPhaseSucceeded,
					SeedArtifactReceipt: &antflyv1.HASeedArtifactReceiptStatus{
						FormatVersion: 1, ActionKind: "gc_local_seed_generations", Scope: "target_activation",
						SlotName: action.SlotName, Generation: action.SeedArtifactGeneration,
						CheckpointSHA256: strings.Repeat("a", 64), RetainedCount: 1,
					},
				})
				r.updateHAStartupGateStatus(ctx, cluster)
				if !cluster.Status.HAStatus.StartupGate.RuntimeEligible {
					t.Fatalf("gate did not recover after cleanup: %#v", cluster.Status.HAStatus.StartupGate)
				}
			}
		})
	}
}

// Exercise the planner that normal reconciliation runs before Job recovery.
func TestHAReceiptRecoverySurvivesNormalReplanning(t *testing.T) {
	cluster := startupGatedStandaloneControllerCluster(true)
	cluster.Spec.HighAvailability.Standbys[0].Desired = nil
	artifact := cluster.Spec.HighAvailability.Standbys[0].SeedArtifact
	artifact.TopologyID = "test-standalone"
	artifact.TopologyGeneration = 3
	artifact.NodeID = "standby-a"
	artifact.TargetPVCUID = "pvc-uid-1"
	artifact.SourcePVC = &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"}
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{ExecutePlannedActions: true}
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 10}
	r := &AntflyClusterReconciler{}
	r.updateHAStatusAndConditions(cluster)
	pending := 0
	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if !haActionRequiresSeedArtifactReceipt(haActionKind(action.Kind)) {
			continue
		}
		action.AdminJobName = fmt.Sprintf("completed-seed-%d", i)
		action.AdminJobPhase = haAdminJobPhaseSucceeded
		action.AttemptCount = 1
		pending++
	}
	if pending == 0 {
		t.Fatal("fixture did not plan portable seed actions")
	}
	previous := cluster.Status.HAStatus.DeepCopy()
	cluster.Status.HAStatus.PrimaryLSN = 20
	r.updateHAStatusAndConditions(cluster)
	for _, before := range previous.PlannedActions {
		if before.AdminJobName == "" {
			continue
		}
		found := false
		for _, after := range cluster.Status.HAStatus.PlannedActions {
			if after.OperationID != before.OperationID {
				continue
			}
			found = true
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("replanning lost or retargeted completed attempt: before=%#v after=%#v", before, after)
			}
			if haAdminActionSucceededWithEvidence(after) {
				t.Fatal("missing receipt became success evidence")
			}
		}
		if !found {
			t.Fatalf("replanning discarded completed operation %s", before.Kind)
		}
	}
}
