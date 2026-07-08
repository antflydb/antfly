#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-kind-colony-cloud-test}"
KIND_CLUSTER="${KIND_CLUSTER:-colony-cloud-test}"
NAMESPACE="${NAMESPACE:-antfly-ha-failover-fixture}"
CLUSTER_NAME="${CLUSTER_NAME:-ha-fixture}"
IMAGE="${IMAGE:-antfly-ha-admin-mock:local}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ha-failover-kind-fixture.XXXXXX")"

cleanup_tmp() {
  rm -rf "$TMP_DIR"
}
trap cleanup_tmp EXIT

log() {
  printf '[ha-fixture] %s\n' "$*" >&2
}

kubectl_cmd() {
  kubectl --context "$KUBE_CONTEXT" "$@"
}

wait_until() {
  local label="$1"
  shift
  local start now
  start="$(date +%s)"
  while true; do
    if "$@"; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - start >= TIMEOUT_SECONDS )); then
      log "timed out waiting for ${label}"
      return 1
    fi
    sleep 2
  done
}

node_arch() {
  local machine
  machine="$(docker exec "${KIND_CLUSTER}-control-plane" uname -m)"
  case "$machine" in
    aarch64|arm64) printf 'arm64\n' ;;
    x86_64|amd64) printf 'amd64\n' ;;
    *) log "unsupported KinD node architecture: $machine"; return 1 ;;
  esac
}

build_mock_image() {
  local goarch
  goarch="$(node_arch)"
  log "building mock HA admin image ${IMAGE} for linux/${goarch}"
  cat > "$TMP_DIR/main.go" <<'GO'
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

const (
	clusterID  = 100
	shardID    = 10
	tableID    = 20
	timelineID = 4
	epoch      = 6
	primaryID  = "primary-a"
	standbyID  = "standby-a"
	lsn        = 12
)

func main() {
	role := os.Getenv("ROLE")
	mode := os.Getenv("PRIMARY_MODE")
	if mode == "" {
		mode = "healthy"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/admin/v1/ha/primary/status", func(w http.ResponseWriter, r *http.Request) {
		if role != "primary" && role != "standby" {
			http.NotFound(w, r)
			return
		}
		if role == "primary" && mode == "down" {
			http.Error(w, "primary unavailable", http.StatusServiceUnavailable)
			return
		}
		if role == "standby" {
			writeJSON(w, promotedPrimaryStatus())
			return
		}
		writeJSON(w, primaryStatus())
	})
	mux.HandleFunc("/admin/v1/ha/standby/status", func(w http.ResponseWriter, r *http.Request) {
		if role != "standby" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, standbyStatus())
	})
	mux.HandleFunc("/admin/v1/ha/fence", func(w http.ResponseWriter, r *http.Request) {
		if role != "standby" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, fenceResponse())
	})
	mux.HandleFunc("/admin/v1/ha/promotion/assess", func(w http.ResponseWriter, r *http.Request) {
		if role != "standby" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, promotionAssessResponse())
	})
	mux.HandleFunc("/admin/v1/ha/promotion/current-fence", func(w http.ResponseWriter, r *http.Request) {
		if role != "standby" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, promotionResponse())
	})
	log.Printf("mock HA admin server role=%s mode=%s listening on :8080", role, mode)
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func identity(timeline, ep uint64) map[string]any {
	return map[string]any{
		"cluster_id":  clusterID,
		"shard_id":    shardID,
		"table_id":    tableID,
		"timeline_id": timeline,
		"epoch":       ep,
	}
}

func primaryStatus() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"snapshot": map[string]any{
			"role":        "primary",
			"node_id":     primaryID,
			"identity":    identity(timelineID, epoch),
			"current_lsn": lsn,
			"slots": []map[string]any{{
				"name":              standbyID,
				"timeline_id":       timelineID,
				"active":            true,
				"reseed_required":   false,
				"restart_lsn":       7,
				"received_lsn":      lsn,
				"applied_lsn":       lsn,
				"safe_read_lsn":     lsn,
				"write_lag_lsn":     0,
				"apply_lag_lsn":     0,
				"safe_read_lag_lsn": 0,
				"retention_lag_lsn": 5,
				"status":            "healthy",
				"last_error":        nil,
			}},
			"retention": map[string]any{
				"primary_lsn":        lsn,
				"oldest_restart_lsn": 7,
				"retained_lsn_count": 5,
				"retained_byte_count": 512,
				"retained_age_ns":    1,
				"active_slots":       1,
				"reseed_recommended": 0,
			},
			"durability": map[string]any{
				"status":            "satisfied",
				"mode":              "remote_apply",
				"selection":         "any",
				"target_lsn":        lsn,
				"progress_lsn":      lsn,
				"missing_lsn_count": 0,
				"satisfied_count":   1,
				"required_count":    1,
				"candidate_count":   1,
			},
		},
	}
}

func promotedPrimaryStatus() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"snapshot": map[string]any{
			"role":        "primary",
			"node_id":     standbyID,
			"identity":    identity(timelineID+1, epoch+1),
			"current_lsn": lsn + 1,
			"slots":       []map[string]any{},
			"retention": map[string]any{
				"primary_lsn":        lsn + 1,
				"oldest_restart_lsn": lsn + 1,
				"retained_lsn_count": 0,
				"retained_byte_count": 0,
				"retained_age_ns":    1,
				"active_slots":       0,
				"reseed_recommended": 0,
			},
			"durability": map[string]any{
				"status":            "satisfied",
				"mode":              "remote_apply",
				"selection":         "any",
				"target_lsn":        lsn + 1,
				"progress_lsn":      lsn + 1,
				"missing_lsn_count": 0,
				"satisfied_count":   0,
				"required_count":    0,
				"candidate_count":   0,
			},
		},
	}
}

func standbyStatus() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"snapshot": map[string]any{
			"role":                       "standby",
			"node_id":                    standbyID,
			"identity":                   identity(timelineID, epoch),
			"received_lsn":               lsn,
			"applied_lsn":                lsn,
			"safe_read_lsn":              lsn,
			"upstream_lsn":               lsn,
			"write_lag_lsn":              0,
			"receive_lag_lsn":            0,
			"apply_lag_lsn":              0,
			"last_error":                 "",
			"last_attempt_ns":            2,
			"last_success_ns":            2,
			"replication_failures_total": 0,
			"unapplied_lsn_count":        0,
			"caught_up_to_received":      true,
			"can_serve_safe_reads":       true,
		},
	}
}

func fenceResponse() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"action": map[string]any{
			"action_id":   "fence_acquire:" + standbyID,
			"action_kind": "fence_acquire",
			"target":      standbyID,
			"state":       "applied",
			"node_id":     standbyID,
		},
		"receipt": map[string]any{
			"identity":           identity(timelineID+1, epoch+1),
			"old_primary_id":     primaryID,
			"promoted_node_id":   standbyID,
			"parent_timeline_id": timelineID,
			"parent_epoch":       epoch,
			"new_timeline_id":    timelineID + 1,
			"new_epoch":          epoch + 1,
			"required_lsn":       lsn,
			"observed_lsn":       lsn,
			"generation":         1,
			"forced":             false,
			"token":              "kind-fixture-fence-token",
			"reason":             "LeaseAcquired",
		},
	}
}

func promotionAssessResponse() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"action": map[string]any{
			"action_id":   "promotion_assess:" + standbyID,
			"action_kind": "promotion_assess",
			"target":      standbyID,
			"state":       "assessed",
			"node_id":     standbyID,
		},
		"assessment": promotionAssessment(),
	}
}

func promotionResponse() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"action": map[string]any{
			"action_id":   "promotion:" + standbyID,
			"action_kind": "promotion",
			"target":      standbyID,
			"state":       "applied",
			"node_id":     standbyID,
		},
		"assessment":       promotionAssessment(),
		"promotion":        promotionResult(),
		"fence_generation": 1,
		"fence_token":      "kind-fixture-fence-token",
		"forced":           false,
	}
}

func promotionAssessment() map[string]any {
	return map[string]any{
		"required_lsn":          lsn,
		"received_lsn":          lsn,
		"applied_lsn":           lsn,
		"has_required_lsn":      true,
		"caught_up_to_received": true,
		"fencing_confirmed":     true,
		"force":                 false,
		"mode":                  "safe",
		"data_loss_possible":    false,
		"safe":                  true,
		"requires_fencing":      false,
		"requires_force":        false,
		"can_promote":           true,
	}
}

func promotionResult() map[string]any {
	return map[string]any{
		"node_id":            standbyID,
		"switch_lsn":         lsn + 1,
		"old_identity":       identity(timelineID, epoch),
		"new_identity":       identity(timelineID+1, epoch+1),
		"forced":             false,
		"data_loss_possible": false,
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
GO
  cat > "$TMP_DIR/Dockerfile" <<'DOCKER'
FROM scratch
COPY ha-admin-mock /ha-admin-mock
ENTRYPOINT ["/ha-admin-mock"]
DOCKER
  (cd "$TMP_DIR" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -o ha-admin-mock main.go)
  docker build -t "$IMAGE" "$TMP_DIR" >/dev/null
  kind load docker-image "$IMAGE" --name "$KIND_CLUSTER" >/dev/null
}

apply_fixture() {
  log "applying fixture namespace ${NAMESPACE}"
  kubectl_cmd delete namespace "$NAMESPACE" --ignore-not-found >/dev/null
  kubectl_cmd create namespace "$NAMESPACE" >/dev/null
  kubectl_cmd -n "$NAMESPACE" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${CLUSTER_NAME}-primary-admin
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ha-admin-mock
      app.kubernetes.io/instance: ${CLUSTER_NAME}
      app.kubernetes.io/component: primary-admin
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ha-admin-mock
        app.kubernetes.io/instance: ${CLUSTER_NAME}
        app.kubernetes.io/component: primary-admin
    spec:
      containers:
      - name: mock
        image: ${IMAGE}
        imagePullPolicy: IfNotPresent
        env:
        - name: ROLE
          value: primary
        - name: PRIMARY_MODE
          value: healthy
        ports:
        - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: ${CLUSTER_NAME}-primary-admin
spec:
  selector:
    app.kubernetes.io/name: ha-admin-mock
    app.kubernetes.io/instance: ${CLUSTER_NAME}
    app.kubernetes.io/component: primary-admin
  ports:
  - name: http
    port: 8080
    targetPort: 8080
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${CLUSTER_NAME}-standby-admin
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: antfly-database
      app.kubernetes.io/instance: ${CLUSTER_NAME}
      app.kubernetes.io/component: standby-a
  template:
    metadata:
      labels:
        app.kubernetes.io/name: antfly-database
        app.kubernetes.io/instance: ${CLUSTER_NAME}
        app.kubernetes.io/component: standby-a
    spec:
      containers:
      - name: mock
        image: ${IMAGE}
        imagePullPolicy: IfNotPresent
        env:
        - name: ROLE
          value: standby
        ports:
        - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: ${CLUSTER_NAME}-standby-admin
spec:
  selector:
    app.kubernetes.io/name: antfly-database
    app.kubernetes.io/instance: ${CLUSTER_NAME}
    app.kubernetes.io/component: standby-a
  ports:
  - name: http
    port: 8080
    targetPort: 8080
---
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: ${CLUSTER_NAME}
spec:
  mode: Swarm
  image: ghcr.io/antflydb/antfly:v0.2.0-rc.17
  swarm:
    replicas: 0
    nodeID: 1
    resources:
      cpu: 10m
      memory: 64Mi
      limits:
        cpu: 10m
        memory: 64Mi
    metadataAPI:
      port: 8080
    metadataRaft:
      port: 9017
    storeAPI:
      port: 12380
    storeRaft:
      port: 9021
    health:
      port: 4200
    inference:
      enabled: false
  storage:
    storageClass: standard
    swarmStorage: 1Gi
  publicAPI:
    enabled: true
    serviceType: ClusterIP
    port: 80
  config: "{}"
  highAvailability:
    mode: HotStandby
    identity:
      clusterID: 100
      shardID: 10
      tableID: 20
      timelineID: 4
      epoch: 6
      currentPrimaryID: primary-a
    admin:
      primaryURL: http://${CLUSTER_NAME}-primary-admin.${NAMESPACE}.svc.cluster.local:8080
      executePlannedActions: true
    standbys:
    - name: standby-a
      adminURL: http://${CLUSTER_NAME}-standby-admin.${NAMESPACE}.svc.cluster.local:8080
      routeSelector:
        app.kubernetes.io/name: antfly-database
        app.kubernetes.io/instance: ${CLUSTER_NAME}
        app.kubernetes.io/component: standby-a
    syncPolicy:
      mode: RemoteApply
      selection: Any
      required: 1
      standbyNames:
      - standby-a
      failurePolicy: Block
    retention:
      maxLagLSN: 1000000
    automaticFailover:
      enabled: true
      fencingAuthority: KubernetesLease
      requireRemoteApply: true
YAML
}

jsonpath_equals() {
  local expr="$1"
  local want="$2"
  local got
  got="$(kubectl_cmd -n "$NAMESPACE" get antflycluster "$CLUSTER_NAME" -o "jsonpath=${expr}" 2>/dev/null || true)"
  [[ "$got" == "$want" ]]
}

service_selector_component_is() {
  local want="$1"
  local got
  got="$(kubectl_cmd -n "$NAMESPACE" get svc "${CLUSTER_NAME}-public-api" -o jsonpath='{.spec.selector.app\.kubernetes\.io/component}' 2>/dev/null || true)"
  [[ "$got" == "$want" ]]
}

lease_held_by_standby() {
  local holder
  holder="$(kubectl_cmd -n "$NAMESPACE" get lease "${CLUSTER_NAME}-ha-fence" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)"
  [[ "$holder" == "standby-a" ]]
}

trigger_reconcile() {
  kubectl_cmd -n "$NAMESPACE" annotate antflycluster "$CLUSTER_NAME" "antfly.io/ha-fixture-tick=$(date +%s%N)" --overwrite >/dev/null
}

run_fixture() {
  kubectl_cmd -n "$NAMESPACE" rollout status "deployment/${CLUSTER_NAME}-primary-admin" --timeout="${TIMEOUT_SECONDS}s" >/dev/null
  kubectl_cmd -n "$NAMESPACE" rollout status "deployment/${CLUSTER_NAME}-standby-admin" --timeout="${TIMEOUT_SECONDS}s" >/dev/null

  log "waiting for initial healthy HA observation"
  wait_until "public API service creation" service_selector_component_is swarm
  wait_until "primary LSN observation" jsonpath_equals '{.status.haStatus.primaryLSN}' '12'
  wait_until "healthy standby observation" jsonpath_equals '{.status.haStatus.standbys[0].status}' 'healthy'

  log "simulating primary admin outage"
  kubectl_cmd -n "$NAMESPACE" set env "deployment/${CLUSTER_NAME}-primary-admin" PRIMARY_MODE=down >/dev/null
  kubectl_cmd -n "$NAMESPACE" rollout status "deployment/${CLUSTER_NAME}-primary-admin" --timeout="${TIMEOUT_SECONDS}s" >/dev/null
  trigger_reconcile

  log "waiting for Kubernetes Lease fencing"
  wait_until "Lease held by standby-a" lease_held_by_standby
  trigger_reconcile

  log "waiting for fenced promotion and public route switch"
  wait_until "last promotion recorded" jsonpath_equals '{.status.haStatus.lastPromotion.promotedStandbyID}' 'standby-a'
  wait_until "public route switched to standby-a" service_selector_component_is standby-a

  printf 'Lease holder: '
  kubectl_cmd -n "$NAMESPACE" get lease "${CLUSTER_NAME}-ha-fence" -o jsonpath='{.spec.holderIdentity}{"\n"}'
  printf 'Lease scope: '
  kubectl_cmd -n "$NAMESPACE" get lease "${CLUSTER_NAME}-ha-fence" -o jsonpath='{.metadata.annotations}{"\n"}'
  printf 'Promotion: '
  kubectl_cmd -n "$NAMESPACE" get antflycluster "$CLUSTER_NAME" -o jsonpath='{.status.haStatus.lastPromotion.promotedStandbyID}{" fence="}{.status.haStatus.lastPromotion.fenceAuthority}{"/"}{.status.haStatus.lastPromotion.fenceGeneration}{" required_lsn="}{.status.haStatus.lastPromotion.requiredLSN}{" observed_lsn="}{.status.haStatus.lastPromotion.observedLSN}{" switch_lsn="}{.status.haStatus.lastPromotion.switchLSN}{"\n"}'
  printf 'Public route selector: '
  kubectl_cmd -n "$NAMESPACE" get svc "${CLUSTER_NAME}-public-api" -o jsonpath='{.spec.selector}{"\n"}'
  printf 'Public route annotations: '
  kubectl_cmd -n "$NAMESPACE" get svc "${CLUSTER_NAME}-public-api" -o jsonpath='{.metadata.annotations}{"\n"}'
}

main() {
  command -v docker >/dev/null
  command -v kind >/dev/null
  command -v kubectl >/dev/null
  command -v go >/dev/null
  cd "$ROOT_DIR"
  build_mock_image
  apply_fixture
  run_fixture
}

main "$@"
