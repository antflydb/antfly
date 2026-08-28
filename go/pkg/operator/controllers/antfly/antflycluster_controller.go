package controllers

//go:generate go tool controller-gen rbac:roleName=antfly-operator-cluster-role paths="../..." output:rbac:artifacts:config=../../manifests/rbac

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"os"
	"path"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	inferencev1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	"github.com/antflydb/antfly/go/pkg/operator/controllers/internal/poddiagnostics"
	adminsdk "github.com/antflydb/antfly/go/pkg/sdk/admin"
)

// AntflyClusterReconciler reconciles an AntflyCluster object
type AntflyClusterReconciler struct {
	client.Client
	// BoundaryReader bypasses the controller cache for fencing decisions whose
	// correctness depends on a current Lease/workload/Pod snapshot.
	BoundaryReader        client.Reader
	Scheme                *runtime.Scheme
	AutoScaler            *AutoScaler
	KubeClient            kubernetes.Interface
	NodeStatsFetcher      func(context.Context, string) (*kubeletStatsSummary, error)
	HTTPClient            *http.Client
	Recorder              events.EventRecorder
	ManageInferencePools  bool
	DefaultInferenceImage string
	ClusterDomain         string
	Now                   func() time.Time
	// MonotonicNow is a test hook for process-local HA watchdog barriers. In
	// production it is nil and time.Now retains its monotonic clock reading.
	MonotonicNow func() time.Time

	// validationAttempts tracks consecutive validation failure counts per cluster
	// (namespace/name -> int). Reset on successful validation. Used for
	// exponential backoff on repeated validation failures.
	validationAttempts sync.Map
	// metadataTopologyObservations tracks retryable runtime-topology observations
	// so short grace periods can span reconciliations without occupying a
	// controller worker while Raft settles or a status probe recovers.
	metadataTopologyObservations sync.Map
	// haIsolationGraceStarts is intentionally process-local and unpersisted. A
	// controller or leader restart must observe the exact Lease transfer again
	// and wait a fresh full runtime maximum fence latency.
	haIsolationGraceStarts sync.Map
	// haProcessGraceStarts prevents a replacement process from inheriting a
	// still-live process incarnation's Lease before that incarnation's maximum
	// local authority window has elapsed without a renewal.
	haProcessGraceStarts sync.Map
}

var defaultOperatorHTTPClient = &http.Client{Timeout: 10 * time.Second}

const maxHASeededSlotActivationReceiptBytes = 64 * 1024

// A checkpoint is successful durable progress, so it should restart
// reconciliation promptly without consuming the error rate limiter.
const haStatusCheckpointRequeueAfter = 5 * time.Millisecond

const haStartupGateObservationRequeueAfter = 5 * time.Second

// Runtime LSN, replication health, and failure-detector evidence do not emit
// Kubernetes events. Keep their observation clock independent from the
// higher-frequency Lease-renewal controller so writes and failover decisions
// never depend on unrelated object churn.
const haRuntimeStatusObservationRequeueAfter = 5 * time.Second

const (
	antflyRuntimeUID int64 = 10001
	antflyRuntimeGID int64 = 10001

	antflySecretStoreVolumeName                   = "secret-store"
	antflySecretStoreDefaultKey                   = "secrets.json"
	antflySecretStoreDefaultPath                  = "/run/antfly/secrets/secrets.json" // #nosec G101 -- file path, not a credential
	antflySecretStoreEnvVar                       = "ANTFLY_SECRET_STORE_PATH"         // #nosec G101 -- environment variable name, not a credential
	antflyExtensionPackageStoreEnvVar             = "ANTFLY_EXTENSION_PACKAGE_STORE"
	antflyStandaloneExtensionPackageStore         = "/antflydb/extensions"
	antflyInternalServiceSecretEnvVar             = "ANTFLY_INTERNAL_SERVICE_SECRET"              // #nosec G101 -- environment variable name, not a credential
	antflyInternalServiceVerificationSecretEnvVar = "ANTFLY_INTERNAL_SERVICE_VERIFICATION_SECRET" // #nosec G101 -- environment variable name, not a credential
	antflyInternalServiceIssuerEnvVar             = "ANTFLY_INTERNAL_SERVICE_ISSUER"              // #nosec G101 -- environment variable name, not a credential
	antflyInternalServiceRolloutEnvVar            = "ANTFLY_INTERNAL_SERVICE_ROLLOUT_MODE"
	internalServiceAuthRolloutAnnotation          = "antfly.io/internal-service-auth-rollout-mode"
	internalServiceAuthKeyRolloutAnnotation       = "antfly.io/internal-service-auth-key-rollout"
	internalServiceAuthKeyTargetAnnotation        = "antfly.io/internal-service-auth-key-target"
	internalServiceAuthCapabilityHeader           = "X-Antfly-Internal-Service-Auth"
	internalServiceAuthCapabilityVersion          = "v1"
	internalServiceAuthRolloutInterval            = 10 * time.Second

	haPrimaryRouteTargetAnnotation          = "antfly.io/ha-primary-route-target"
	haPrimaryRouteFenceAuthorityAnnotation  = "antfly.io/ha-primary-route-fence-authority"
	haPrimaryRouteFenceGenerationAnnotation = "antfly.io/ha-primary-route-fence-generation"
	haPrimaryRouteSelectorAnnotation        = "antfly.io/ha-primary-route-selector-applied"
	haAdminTokenDefaultEnvVar               = "ANTFLY_HA_ADMIN_TOKEN" // #nosec G101 -- environment variable name, not a credential
	defaultHAPrimaryLogPath                 = "/antflydb/ha/primary.wal"
	defaultHAPrimarySlotsPath               = "/antflydb/ha/slots"
	defaultHASeedCaptureRoot                = "/antflydb/ha/seed-captures"
	defaultHAFencePath                      = "/antflydb/ha/fence.wal"
	defaultHAStandbyLogPath                 = "/antflydb/ha/standby.wal"
	defaultHAStandbyProgressPath            = "/antflydb/ha/standby-progress.wal"
	defaultHADirectAdminRetryLimit          = int32(8)
	defaultHADirectAdminRetryBase           = 5 * time.Second
	defaultHADirectAdminRetryMaximum        = 2 * time.Minute
	defaultHADirectAdminReservation         = 30 * time.Second
	defaultHADirectPrerequisiteTimeout      = 10 * time.Minute
	haStartupGateReceiptHashAnnotation      = "antfly.io/ha-startup-receipt-hash"
	haSeedRoleAnnotation                    = "antfly.io/ha-seed-role"
	haTopologyIDAnnotation                  = "antfly.io/ha-topology-id"
	haTopologyGenerationAnnotation          = "antfly.io/ha-topology-generation"
	haNodeIDAnnotation                      = "antfly.io/ha-node-id"
	haSlotNameAnnotation                    = "antfly.io/ha-slot-name"
	haSeedGenerationAnnotation              = "antfly.io/ha-seed-generation"
	haSeedManifestIDAnnotation              = "antfly.io/ha-seed-manifest-id"
	haSeedManifestSHA256Annotation          = "antfly.io/ha-seed-manifest-sha256"
	haSeedSourcePVCNameAnnotation           = "antfly.io/ha-seed-source-pvc-name"
	haSeedSourcePVCUIDAnnotation            = "antfly.io/ha-seed-source-pvc-uid"
	haSeedTargetPVCNameAnnotation           = "antfly.io/ha-seed-target-pvc-name"
	haSeedTargetPVCUIDAnnotation            = "antfly.io/ha-seed-target-pvc-uid"
	haSeedCheckpointLSNAnnotation           = "antfly.io/ha-seed-checkpoint-lsn"
	haSeedLiveDataPath                      = "/antflydb/data"
	haSeedLiveMetadataPath                  = "/antflydb/metadata"
	haSeedLiveExtensionsPath                = "/antflydb/extensions"
	haSeedActivationRelativeRoot            = ".antfly-ha/active"
	cloudHAPromotionReceiptAnnotation       = "cloud.antfly.io/ha-promotion-receipt"
	cloudHATopologyGenerationAnnotation     = "cloud.antfly.io/ha-topology-generation"
	haPromotedProcessBindingAnnotation      = "antfly.io/ha-promoted-process-binding"
	cloudHARoleLabel                        = "cloud.antfly.io/ha-role"
	cloudHAStandbyRole                      = "standby"

	haAdminJobPhaseWaitingDependency   = "WaitingDependency"
	haAdminJobPhaseWaitingPrerequisite = "WaitingPrerequisite"
	haAdminJobPhaseWaitingJobFallback  = "WaitingJobFallback"
	haAdminJobPhasePending             = "Pending"
	haAdminJobPhaseRunning             = "Running"
	haAdminJobPhaseSucceeded           = "Succeeded"
	haAdminJobPhaseFailed              = "Failed"
	haAdminJobPhaseMissingAdminURL     = "MissingAdminURL"

	defaultManagedInferenceAPIPort = 8080
)

//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups="",resources=pods/log,verbs=get
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
//+kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=inferencepools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=inferencepools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=inferencepools/finalizers,verbs=update
// No delete on CRDs: startup bootstrap applies CRDs but never removes them.
//+kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch;create;update;patch

var reservedPodLabelPrefixes = []string{"app.kubernetes.io/"}

func int64Ptr(v int64) *int64 {
	return &v
}

func secretStoreKey(store *antflyv1.SecretStoreSpec) string {
	if store == nil || store.Key == "" {
		return antflySecretStoreDefaultKey
	}
	return store.Key
}

func secretStorePath(store *antflyv1.SecretStoreSpec) string {
	if store == nil || store.Path == "" {
		return antflySecretStoreDefaultPath
	}
	return store.Path
}

func secretStoreEnv(store *antflyv1.SecretStoreSpec) []corev1.EnvVar {
	if store == nil {
		return nil
	}
	return []corev1.EnvVar{{
		Name:  antflySecretStoreEnvVar,
		Value: secretStorePath(store),
	}}
}

type internalServiceAuthRolloutMode string

type internalServiceAuthKeyRolloutMode string

const (
	internalServiceAuthRolloutEnforce           internalServiceAuthRolloutMode    = "enforce"
	internalServiceAuthRolloutMigration         internalServiceAuthRolloutMode    = "migration"
	internalServiceAuthKeyRolloutSteady         internalServiceAuthKeyRolloutMode = "steady"
	internalServiceAuthKeyRolloutPrepare        internalServiceAuthKeyRolloutMode = "prepare"
	internalServiceAuthKeyRolloutSwitch         internalServiceAuthKeyRolloutMode = "switch"
	internalServiceAuthPublicBoundaryLabel                                        = "antfly.io/internal-service-auth-boundary"
	internalServiceAuthPublicBoundaryAnnotation                                   = "antfly.io/internal-service-auth-public-boundary"
)

func internalServiceAuthIssuer(cluster *antflyv1.AntflyCluster) string {
	// UID prevents a deleted/recreated cluster with the same namespace/name from
	// sharing an issuer. The fallback is used only by unit tests and pre-create
	// rendering; reconciled Kubernetes objects always have a UID.
	identity := strings.TrimSpace(string(cluster.UID))
	if identity == "" {
		identity = cluster.Namespace + ":" + cluster.Name
	}
	return "antfly-cluster:" + identity
}

func requiredSecretKeyEnv(name string, selector corev1.SecretKeySelector) corev1.EnvVar {
	secretRef := selector.DeepCopy()
	optional := false
	secretRef.Optional = &optional
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: secretRef}}
}

func internalServiceAuthEnv(cluster *antflyv1.AntflyCluster, mode internalServiceAuthRolloutMode, keyMode internalServiceAuthKeyRolloutMode) []corev1.EnvVar {
	if cluster == nil || cluster.Spec.InternalServiceAuth == nil {
		return nil
	}
	auth := cluster.Spec.InternalServiceAuth
	signing := auth.SecretKeyRef
	var verification *corev1.SecretKeySelector
	if auth.NextSecretKeyRef != nil {
		if keyMode == internalServiceAuthKeyRolloutSwitch {
			signing = *auth.NextSecretKeyRef
			old := auth.SecretKeyRef
			verification = &old
		} else {
			next := *auth.NextSecretKeyRef
			verification = &next
		}
	}
	env := []corev1.EnvVar{
		requiredSecretKeyEnv(antflyInternalServiceSecretEnvVar, signing),
		{Name: antflyInternalServiceIssuerEnvVar, Value: internalServiceAuthIssuer(cluster)},
		{Name: antflyInternalServiceRolloutEnvVar, Value: string(mode)},
	}
	if verification != nil {
		env = append(env, requiredSecretKeyEnv(antflyInternalServiceVerificationSecretEnvVar, *verification))
	}
	return env
}

func internalServiceAuthKeyTarget(auth *antflyv1.InternalServiceAuthSpec) string {
	if auth == nil || auth.NextSecretKeyRef == nil {
		return ""
	}
	sum := sha256.Sum256([]byte(auth.NextSecretKeyRef.Name + "\x00" + auth.NextSecretKeyRef.Key))
	return hex.EncodeToString(sum[:8])
}

func haRuntimeAdminTokenEnv(ha *antflyv1.HighAvailabilitySpec) []corev1.EnvVar {
	if ha == nil || ha.Runtime == nil || ha.Runtime.AdminTokenSecretRef == nil {
		return nil
	}
	envVar := strings.TrimSpace(ha.Runtime.AdminTokenEnvVar)
	if envVar == "" {
		return nil
	}
	secretRef := ha.Runtime.AdminTokenSecretRef.DeepCopy()
	optional := false
	secretRef.Optional = &optional
	return []corev1.EnvVar{{
		Name: envVar,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: secretRef,
		},
	}}
}

func secretStoreArg(store *antflyv1.SecretStoreSpec) string {
	if store == nil {
		return ""
	}
	return fmt.Sprintf(" \\\n  --secret-store-path %s", shellQuoteArg(secretStorePath(store)))
}

func haRuntimeStartupGate(cluster *antflyv1.AntflyCluster) *antflyv1.HAStartupGateSpec {
	if cluster == nil || cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil {
		return nil
	}
	return cluster.Spec.HighAvailability.Runtime.StartupGate
}

func (r *AntflyClusterReconciler) reconcileHAStartupTargetPVC(ctx context.Context, cluster *antflyv1.AntflyCluster, storageSize string) (*corev1.PersistentVolumeClaim, error) {
	gate := haRuntimeStartupGate(cluster)
	if gate == nil || gate.Policy != antflyv1.HAStartupGatePolicyRequireActivatedSeed || gate.RequiredReceipt == nil {
		return nil, nil
	}
	name := strings.TrimSpace(gate.RequiredReceipt.TargetPVCName)
	if name == "" {
		return nil, fmt.Errorf("HA startup gate target PVC name is empty")
	}
	key := types.NamespacedName{Name: name, Namespace: cluster.Namespace}
	existing := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, key, existing); err != nil {
		if !errors.IsNotFound(err) {
			return nil, err
		}
		var storageClassName *string
		if cluster.Spec.Storage.StorageClass != "" {
			storageClassName = &cluster.Spec.Storage.StorageClass
		}
		created := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: persistentVolumeClaimLabels(cluster, standaloneComponent(cluster))},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: storageClassName,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(storageSize),
				}},
			},
		}
		if err := r.Create(ctx, created); err != nil {
			return nil, err
		}
		return created, nil
	}
	if existing.DeletionTimestamp != nil {
		return nil, fmt.Errorf("HA startup target PVC %s is terminating", name)
	}
	if owner := metav1.GetControllerOf(existing); owner != nil {
		return nil, fmt.Errorf("HA startup target PVC %s must be independently retained, but is controlled by %s %s", name, owner.Kind, owner.Name)
	}
	if expectedUID := strings.TrimSpace(gate.RequiredReceipt.TargetPVCUID); expectedUID != "" && string(existing.UID) != expectedUID {
		return nil, fmt.Errorf("HA startup target PVC %s UID %s does not match required activation receipt UID %s", name, existing.UID, expectedUID)
	}
	changed := false
	desiredLabels := persistentVolumeClaimLabels(cluster, standaloneComponent(cluster))
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	for key, value := range desiredLabels {
		if existing.Labels[key] != value {
			existing.Labels[key] = value
			changed = true
		}
	}
	if changed {
		if err := r.Update(ctx, existing); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

// Existing ungated StatefulSets own immutable volumeClaimTemplates. Migrate
// safely by first suspending and retaining all claims, waiting for every pod to
// stop, and only then recreating the controller around the deterministic claim.
func (r *AntflyClusterReconciler) reconcileLegacyStandaloneStatefulSetStartupGate(ctx context.Context, cluster *antflyv1.AntflyCluster) (bool, error) {
	existing := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, existing); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if len(existing.Spec.VolumeClaimTemplates) == 0 {
		return false, nil
	}
	retain := &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	if existing.Spec.Replicas == nil || *existing.Spec.Replicas != 0 || existing.Spec.PersistentVolumeClaimRetentionPolicy == nil ||
		existing.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != retain.WhenDeleted ||
		existing.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != retain.WhenScaled {
		zero := int32(0)
		existing.Spec.Replicas = &zero
		existing.Spec.PersistentVolumeClaimRetentionPolicy = retain
		return true, r.Update(ctx, existing)
	}
	if existing.Status.Replicas != 0 || existing.Status.CurrentReplicas != 0 || existing.Status.ReadyReplicas != 0 {
		return true, nil
	}
	return true, r.Delete(ctx, existing)
}

// Suspend is a pure availability hold. Preserve the existing storage and pod
// template verbatim so a former primary cannot encounter an immutable
// StatefulSet/PVC handoff while it is being fenced and repaired.
func (r *AntflyClusterReconciler) reconcileSuspendedStandaloneStatefulSet(ctx context.Context, cluster *antflyv1.AntflyCluster) (bool, error) {
	existing := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, existing); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if existing.Spec.Replicas == nil || *existing.Spec.Replicas != 0 {
		zero := int32(0)
		existing.Spec.Replicas = &zero
		return true, r.Update(ctx, existing)
	}
	return true, nil
}

func haStartupGateRuntimeEligible(cluster *antflyv1.AntflyCluster, pvc *corev1.PersistentVolumeClaim) (bool, string) {
	gate := haRuntimeStartupGate(cluster)
	if gate == nil {
		return true, "NotConfigured"
	}
	if gate.Policy == antflyv1.HAStartupGatePolicySuspend {
		return false, "PolicySuspended"
	}
	if !gate.RuntimeEligible {
		return false, "DeclarativelySuspended"
	}
	if cluster.Status.HAStatus == nil || cluster.Status.HAStatus.StartupGate == nil ||
		cluster.Status.HAStatus.StartupGate.ActivationReceipt == nil {
		return false, "ActivationReceiptNotObserved"
	}
	return haStartupGateActivationReceiptMatches(cluster, pvc, cluster.Status.HAStatus.StartupGate.ActivationReceipt)
}

// Physical isolation is an irreversible writer fence, not a permanent process
// tombstone. Release its availability hold only after Colony has rewritten the
// former primary as a standby and both the declarative and observed startup
// gates prove the exact activated generation on the exact retained PVC.
func haFormerPrimaryIsolationReleasedByActivatedStandby(cluster *antflyv1.AntflyCluster, pvc *corev1.PersistentVolumeClaim) bool {
	if cluster == nil || cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil ||
		cluster.Spec.HighAvailability.Runtime.Role != antflyv1.HARuntimeRoleStandby {
		return false
	}
	gate := haRuntimeStartupGate(cluster)
	if gate == nil || gate.Policy != antflyv1.HAStartupGatePolicyRequireActivatedSeed ||
		gate.ReceiptMatchPolicy != antflyv1.HAReceiptMatchPolicyExact || gate.RequiredReceipt == nil {
		return false
	}
	eligible, _ := haStartupGateRuntimeEligible(cluster, pvc)
	return eligible
}

func haStartupGateActivationReceiptMatches(
	cluster *antflyv1.AntflyCluster,
	pvc *corev1.PersistentVolumeClaim,
	receipt *antflyv1.HASeedActivationReceiptStatus,
) (bool, string) {
	gate := haRuntimeStartupGate(cluster)
	if gate == nil {
		return false, "NotConfigured"
	}
	if gate.Policy != antflyv1.HAStartupGatePolicyRequireActivatedSeed || gate.ReceiptMatchPolicy != antflyv1.HAReceiptMatchPolicyExact || gate.RequiredReceipt == nil {
		return false, "UnsupportedPolicy"
	}
	if pvc == nil || strings.TrimSpace(string(pvc.UID)) == "" {
		return false, "TargetPVCNotObserved"
	}
	if receipt == nil {
		return false, "ActivationReceiptNotObserved"
	}
	required := *gate.RequiredReceipt
	if receipt.TopologyID != required.TopologyID || receipt.NodeID != required.NodeID ||
		receipt.SlotName != required.SlotName || receipt.Generation != required.Generation ||
		receipt.TargetPVCName != required.TargetPVCName || receipt.TargetPVCUID != string(pvc.UID) ||
		receipt.GenerationPath != path.Join("live-generations", required.Generation) ||
		receipt.RawGenerationPath != path.Join("generations", required.Generation) ||
		receipt.TargetLocalNodeID == 0 || receipt.TargetReplicaID == 0 ||
		!isLowerHexDigest(receipt.CaptureReceiptSHA256) ||
		!isLowerHexDigest(receipt.MaterializedReceiptSHA256) ||
		!isLowerHexDigest(receipt.MaterializedAggregateSHA256) {
		return false, "ActivationReceiptIdentityMismatch"
	}
	if required.TopologyGeneration != 0 && receipt.TopologyGeneration != required.TopologyGeneration {
		return false, "ActivationReceiptTopologyGenerationMismatch"
	}
	if required.TargetPVCUID != "" && receipt.TargetPVCUID != required.TargetPVCUID {
		return false, "ActivationReceiptPVCUIDMismatch"
	}
	if (required.ManifestSHA256 != "" && receipt.ManifestSHA256 != required.ManifestSHA256) ||
		(required.AggregateSHA256 != "" && receipt.AggregateSHA256 != required.AggregateSHA256) ||
		(required.SeedReceiptSHA256 != "" && receipt.SeedReceiptSHA256 != required.SeedReceiptSHA256) ||
		(required.CaptureReceiptSHA256 != "" && receipt.CaptureReceiptSHA256 != required.CaptureReceiptSHA256) ||
		(required.MaterializedReceiptSHA256 != "" && receipt.MaterializedReceiptSHA256 != required.MaterializedReceiptSHA256) ||
		(required.MaterializedAggregateSHA256 != "" && receipt.MaterializedAggregateSHA256 != required.MaterializedAggregateSHA256) ||
		(required.TargetLocalNodeID != 0 && receipt.TargetLocalNodeID != required.TargetLocalNodeID) ||
		(required.TargetReplicaID != 0 && receipt.TargetReplicaID != required.TargetReplicaID) {
		return false, "ActivationReceiptDigestMismatch"
	}
	if !haSeedReceiptMatchesRuntimeLineage(cluster, receipt.ClusterID, receipt.ShardID, receipt.TableID, receipt.TimelineID, receipt.Epoch) {
		return false, "ActivationReceiptReplicationIdentityMismatch"
	}
	return true, "ExactActivationReceiptMatched"
}

// haSeedReceiptMatchesRuntimeLineage distinguishes standby startup authority
// from promoted-primary storage provenance. A standby may only start from a
// receipt on its exact current replication boundary. Once that same runtime is
// promoted, its materialized receipt necessarily belongs to the predecessor
// boundary; accept it only when the database identity is unchanged and the new
// authority boundary is monotonically later (never equal, incomparable, or in
// the future).
func haSeedReceiptMatchesRuntimeLineage(
	cluster *antflyv1.AntflyCluster,
	clusterID, shardID, tableID, timelineID, epoch uint64,
) bool {
	if cluster == nil || cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil {
		return false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || clusterID != identity.ClusterID || shardID != identity.ShardID || tableID != identity.TableID {
		return false
	}
	if cluster.Spec.HighAvailability.Runtime.Role != antflyv1.HARuntimeRolePrimary {
		return timelineID == identity.TimelineID && epoch == identity.Epoch
	}
	return timelineID <= identity.TimelineID && epoch <= identity.Epoch &&
		(timelineID < identity.TimelineID || epoch < identity.Epoch)
}

func haStartupGateReceiptHash(cluster *antflyv1.AntflyCluster, pvc *corev1.PersistentVolumeClaim) string {
	eligible, _ := haStartupGateRuntimeEligible(cluster, pvc)
	if !eligible || cluster.Status.HAStatus == nil || cluster.Status.HAStatus.StartupGate == nil {
		return ""
	}
	raw, err := json.Marshal(cluster.Status.HAStatus.StartupGate.ActivationReceipt)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

func haStandaloneRuntimeSeedIdentityAnnotations(cluster *antflyv1.AntflyCluster) map[string]string {
	if cluster == nil || cluster.Status.HAStatus == nil || cluster.Status.HAStatus.StartupGate == nil ||
		cluster.Status.HAStatus.StartupGate.ActivationReceipt == nil {
		return nil
	}
	receipt := cluster.Status.HAStatus.StartupGate.ActivationReceipt
	annotations := map[string]string{haSeedRoleAnnotation: "standby-runtime"}
	setHASeedIdentityAnnotation(annotations, haTopologyIDAnnotation, receipt.TopologyID)
	if receipt.TopologyGeneration > 0 {
		annotations[haTopologyGenerationAnnotation] = strconv.FormatInt(receipt.TopologyGeneration, 10)
	}
	setHASeedIdentityAnnotation(annotations, haNodeIDAnnotation, receipt.NodeID)
	setHASeedIdentityAnnotation(annotations, haSlotNameAnnotation, receipt.SlotName)
	setHASeedIdentityAnnotation(annotations, haSeedGenerationAnnotation, receipt.Generation)
	setHASeedIdentityAnnotation(annotations, haSeedManifestIDAnnotation, receipt.ManifestID)
	setHASeedIdentityAnnotation(annotations, haSeedManifestSHA256Annotation, receipt.ManifestSHA256)
	setHASeedIdentityAnnotation(annotations, haSeedTargetPVCNameAnnotation, receipt.TargetPVCName)
	setHASeedIdentityAnnotation(annotations, haSeedTargetPVCUIDAnnotation, receipt.TargetPVCUID)
	if receipt.CheckpointLSN > 0 {
		annotations[haSeedCheckpointLSNAnnotation] = strconv.FormatUint(receipt.CheckpointLSN, 10)
	}
	return annotations
}

func standaloneHAArgs(ha *antflyv1.HighAvailabilitySpec, startupGeneration string) string {
	if ha == nil || ha.Mode == antflyv1.HAModeDisabled || ha.Runtime == nil || ha.Identity == nil {
		return ""
	}
	runtime := ha.Runtime
	identity := ha.Identity
	var args strings.Builder
	seedCaptureRoot := defaultHASeedCaptureRoot
	if value := strings.TrimSpace(runtime.SeedCaptureRoot); value != "" {
		seedCaptureRoot = value
	}
	fencePath := defaultHAFencePath
	if value := strings.TrimSpace(runtime.FencePath); value != "" {
		fencePath = value
	}
	formerPrimaryLogPath := strings.TrimSpace(runtime.FormerPrimaryLogPath)
	adminTokenEnvVar := strings.TrimSpace(runtime.AdminTokenEnvVar)
	appendHAArg := func(name, value string) {
		args.WriteString(" \\\n  ")
		args.WriteString(name)
		args.WriteByte(' ')
		args.WriteString(shellQuoteArg(strings.TrimSpace(value)))
	}
	appendHAUint := func(name string, value uint64) {
		args.WriteString(" \\\n  ")
		args.WriteString(name)
		args.WriteByte(' ')
		args.WriteString(strconv.FormatUint(value, 10))
	}

	switch runtime.Role {
	case antflyv1.HARuntimeRolePrimary:
		primary := runtime.Primary
		logPath := defaultHAPrimaryLogPath
		slotsPath := defaultHAPrimarySlotsPath
		if primary != nil {
			if value := strings.TrimSpace(primary.LogPath); value != "" {
				logPath = value
			}
			if value := strings.TrimSpace(primary.SlotsPath); value != "" {
				slotsPath = value
			}
		}
		appendHAArg("--ha-primary-log", logPath)
		appendHAArg("--ha-primary-slots", slotsPath)
		appendHAArg("--ha-primary-node-id", runtime.NodeID)
		appendHAArg("--ha-fence-wal", fencePath)
		if formerPrimaryLogPath == "" {
			formerPrimaryLogPath = logPath
		}
		if formerPrimaryLogPath != "" {
			appendHAArg("--ha-former-primary-log", formerPrimaryLogPath)
		}
		if adminTokenEnvVar != "" {
			appendHAArg("--admin-token-env", adminTokenEnvVar)
		}
		if ha.Retention != nil && ha.Retention.MaxLagLSN > 0 {
			appendHAUint("--ha-retention-max-lag-lsn", ha.Retention.MaxLagLSN)
		}
		if ha.Retention != nil && ha.Retention.MaxRetainedBytes > 0 {
			appendHAUint("--ha-retention-max-retained-bytes", ha.Retention.MaxRetainedBytes)
		}
		if ha.Retention != nil && ha.Retention.MaxRetainedAgeNS > 0 {
			appendHAUint("--ha-retention-max-retained-age-ns", ha.Retention.MaxRetainedAgeNS)
		}
		appendStandaloneHASyncPolicyArgs(&args, ha.SyncPolicy)
	case antflyv1.HARuntimeRoleStandby:
		standby := runtime.Standby
		logPath := defaultHAStandbyLogPath
		progressPath := defaultHAStandbyProgressPath
		// A materialized seed snapshot supersedes all receive/apply state from
		// prior topologies. Keep operator-default standby WALs generation-scoped
		// so an exact reseed starts from its validated checkpoint while ordinary
		// restarts of that same generation retain their progress.
		if generation := strings.TrimSpace(startupGeneration); generation != "" {
			generationRoot := path.Join("/antflydb/ha/standby-generations", generation)
			logPath = path.Join(generationRoot, "receive.wal")
			progressPath = path.Join(generationRoot, "progress.wal")
		}
		if standby != nil {
			if value := strings.TrimSpace(standby.LogPath); value != "" {
				logPath = value
			}
			if value := strings.TrimSpace(standby.ProgressPath); value != "" {
				progressPath = value
			}
		}
		appendHAArg("--ha-standby-log", logPath)
		appendHAArg("--ha-standby-progress", progressPath)
		appendHAArg("--ha-standby-node-id", runtime.NodeID)
		appendHAArg("--ha-fence-wal", fencePath)
		if formerPrimaryLogPath != "" {
			appendHAArg("--ha-former-primary-log", formerPrimaryLogPath)
		}
		if adminTokenEnvVar != "" {
			appendHAArg("--admin-token-env", adminTokenEnvVar)
		}
		if standby != nil && strings.TrimSpace(standby.UpstreamURL) != "" && strings.TrimSpace(standby.SlotName) != "" {
			appendHAArg("--ha-standby-upstream-url", standby.UpstreamURL)
			appendHAArg("--ha-standby-slot", standby.SlotName)
		}
		// The standby opens the same runtime that may later be promoted in
		// place. Pass the future primary durability policy at startup so a
		// promotion cannot silently inherit Async defaults during the topology
		// repair gap.
		appendStandaloneHASyncPolicyArgs(&args, ha.SyncPolicy)
	default:
		return ""
	}
	appendHAArg("--ha-seed-capture-root", seedCaptureRoot)
	appendHAUint("--ha-cluster-id", identity.ClusterID)
	if identity.ShardID != 0 {
		appendHAUint("--ha-shard-id", identity.ShardID)
	}
	if identity.TableID != 0 {
		appendHAUint("--ha-table-id", identity.TableID)
	}
	appendHAUint("--ha-timeline-id", identity.TimelineID)
	appendHAUint("--ha-epoch", identity.Epoch)
	return args.String()
}

func standaloneHAStartupGeneration(cluster *antflyv1.AntflyCluster) string {
	if cluster == nil {
		return ""
	}
	gate := haRuntimeStartupGate(cluster)
	if gate == nil || gate.Policy != antflyv1.HAStartupGatePolicyRequireActivatedSeed ||
		cluster.Status.HAStatus == nil || cluster.Status.HAStatus.StartupGate == nil ||
		!cluster.Status.HAStatus.StartupGate.RuntimeEligible || cluster.Status.HAStatus.StartupGate.ActivationReceipt == nil {
		return ""
	}
	return strings.TrimSpace(cluster.Status.HAStatus.StartupGate.ActivationReceipt.Generation)
}

func standaloneHAStartupArgs(cluster *antflyv1.AntflyCluster) string {
	gate := haRuntimeStartupGate(cluster)
	if gate == nil || gate.Policy != antflyv1.HAStartupGatePolicyRequireActivatedSeed ||
		cluster.Status.HAStatus == nil || cluster.Status.HAStatus.StartupGate == nil ||
		!cluster.Status.HAStatus.StartupGate.RuntimeEligible || cluster.Status.HAStatus.StartupGate.ActivationReceipt == nil {
		return ""
	}
	receipt := cluster.Status.HAStatus.StartupGate.ActivationReceipt
	var args strings.Builder
	appendArg := func(name, value string) {
		args.WriteString(" \\\n  ")
		args.WriteString(name)
		args.WriteByte(' ')
		args.WriteString(shellQuoteArg(strings.TrimSpace(value)))
	}
	appendArg("--ha-startup-target-root", path.Join("/antflydb", haSeedActivationRelativeRoot))
	appendArg("--ha-startup-topology-id", receipt.TopologyID)
	appendArg("--ha-startup-topology-generation", strconv.FormatInt(receipt.TopologyGeneration, 10))
	appendArg("--ha-startup-generation", receipt.Generation)
	appendArg("--ha-startup-slot-name", receipt.SlotName)
	appendArg("--ha-startup-timeline-id", strconv.FormatUint(receipt.TimelineID, 10))
	appendArg("--ha-startup-epoch", strconv.FormatUint(receipt.Epoch, 10))
	appendArg("--ha-startup-target-pvc-name", receipt.TargetPVCName)
	appendArg("--ha-startup-target-pvc-uid", receipt.TargetPVCUID)
	if receipt.ManifestSHA256 != "" {
		appendArg("--ha-startup-manifest-sha256", receipt.ManifestSHA256)
	}
	if receipt.AggregateSHA256 != "" {
		appendArg("--ha-startup-aggregate-sha256", receipt.AggregateSHA256)
	}
	if receipt.SeedReceiptSHA256 != "" {
		appendArg("--ha-startup-seed-receipt-sha256", receipt.SeedReceiptSHA256)
	}
	appendArg("--ha-startup-capture-receipt-sha256", receipt.CaptureReceiptSHA256)
	appendArg("--ha-startup-materialized-receipt-sha256", receipt.MaterializedReceiptSHA256)
	appendArg("--ha-startup-materialized-aggregate-sha256", receipt.MaterializedAggregateSHA256)
	appendArg("--ha-startup-target-local-node-id", strconv.FormatUint(receipt.TargetLocalNodeID, 10))
	appendArg("--ha-startup-target-replica-id", strconv.FormatUint(receipt.TargetReplicaID, 10))
	return args.String()
}

func appendStandaloneHASyncPolicyArgs(args *strings.Builder, policy *antflyv1.HASyncPolicy) {
	if policy == nil || policy.Mode == "" || policy.Mode == antflyv1.HADurabilityModeAsync {
		return
	}
	appendArg := func(name, value string) {
		args.WriteString(" \\\n  ")
		args.WriteString(name)
		args.WriteByte(' ')
		args.WriteString(shellQuoteArg(strings.TrimSpace(value)))
	}
	appendUint := func(name string, value int32) {
		args.WriteString(" \\\n  ")
		args.WriteString(name)
		args.WriteByte(' ')
		args.WriteString(strconv.FormatInt(int64(value), 10))
	}

	appendArg("--ha-sync-mode", standaloneHASyncMode(policy.Mode))
	if policy.Selection != "" {
		appendArg("--ha-sync-selection", standaloneHAStandbySelection(policy.Selection))
	}
	if policy.Required > 0 {
		appendUint("--ha-sync-required", policy.Required)
	}
	for _, name := range policy.StandbyNames {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			appendArg("--ha-sync-standby", trimmed)
		}
	}
	if policy.FailurePolicy != "" {
		appendArg("--ha-sync-failure", standaloneHAFailurePolicy(policy.FailurePolicy))
	}
}

func shellQuoteArg(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func standaloneHASyncMode(mode antflyv1.HADurabilityMode) string {
	switch mode {
	case antflyv1.HADurabilityModeRemoteApply:
		return "remote-apply"
	case antflyv1.HADurabilityModeRemoteWrite:
		return "remote-write"
	default:
		return "async"
	}
}

func standaloneHAStandbySelection(selection antflyv1.HAStandbySelection) string {
	switch selection {
	case antflyv1.HAStandbySelectionFirst:
		return "first"
	case antflyv1.HAStandbySelectionAll:
		return "all"
	default:
		return "any"
	}
}

func standaloneHAFailurePolicy(policy antflyv1.HAFailurePolicy) string {
	switch policy {
	case antflyv1.HAFailurePolicyFailClosed:
		return "fail-closed"
	case antflyv1.HAFailurePolicyDegradeToAsync:
		return "degrade-to-async"
	default:
		return "block"
	}
}

func secretStoreVolumeMounts(store *antflyv1.SecretStoreSpec) []corev1.VolumeMount {
	if store == nil {
		return nil
	}
	return []corev1.VolumeMount{{
		Name:      antflySecretStoreVolumeName,
		MountPath: path.Dir(secretStorePath(store)),
		ReadOnly:  true,
	}}
}

func secretStoreVolumes(store *antflyv1.SecretStoreSpec) []corev1.Volume {
	if store == nil {
		return nil
	}
	return []corev1.Volume{{
		Name: antflySecretStoreVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: store.SecretName,
				Items: []corev1.KeyToPath{{
					Key:  secretStoreKey(store),
					Path: path.Base(secretStorePath(store)),
				}},
			},
		},
	}}
}

func podFSGroupChangePolicyPtr(v corev1.PodFSGroupChangePolicy) *corev1.PodFSGroupChangePolicy {
	return &v
}

func antflyPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		FSGroup:             int64Ptr(antflyRuntimeGID),
		FSGroupChangePolicy: podFSGroupChangePolicyPtr(corev1.FSGroupChangeOnRootMismatch),
	}
}

// podLabels returns the standard labels for pod templates including the instance identifier.
// These are a superset of serviceSelectorLabels — they include managed-by labels
// that MUST NOT be added to StatefulSet spec.selector.matchLabels (immutable after creation).
func podLabels(cluster *antflyv1.AntflyCluster, component string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name":       "antfly-database",
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "antfly-operator",
	}

	for k, v := range cluster.Labels {
		reserved := false
		for _, prefix := range reservedPodLabelPrefixes {
			if strings.HasPrefix(k, prefix) {
				reserved = true
				break
			}
		}
		if reserved {
			continue
		}
		labels[k] = v
	}

	return labels
}

// serviceSelectorLabels returns the labels used for Service and StatefulSet selectors.
// These are immutable after StatefulSet creation and must remain stable.
// Includes instance to prevent two AntflyClusters in the same namespace from
// adopting each other's pods. Existing StatefulSets will require recreation
// (PVCs are preserved) when upgrading to this version.
func serviceSelectorLabels(clusterName, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "antfly-database",
		"app.kubernetes.io/component": component,
		"app.kubernetes.io/instance":  clusterName,
	}
}

const labelClusterUID = "antfly.io/cluster-uid"

// persistentVolumeClaimLabels bind a claim to one immutable AntflyCluster
// incarnation. Cluster names can be reused, so the instance label and the
// StatefulSet-generated claim name are discovery aids rather than sufficient
// authorization for destructive cleanup.
func persistentVolumeClaimLabels(cluster *antflyv1.AntflyCluster, component string) map[string]string {
	labels := serviceSelectorLabels(cluster.Name, component)
	if cluster.UID != "" {
		labels[labelClusterUID] = string(cluster.UID)
	}
	return labels
}

func haCurrentPrimaryRouteTarget(cluster *antflyv1.AntflyCluster) string {
	if cluster.Status.HAStatus != nil && cluster.Status.HAStatus.PrimaryRoute.CurrentTarget != "" {
		return cluster.Status.HAStatus.PrimaryRoute.CurrentTarget
	}
	return "primary"
}

func haPrimaryRouteManaged(cluster *antflyv1.AntflyCluster) bool {
	return cluster.Spec.HighAvailability != nil &&
		cluster.Spec.HighAvailability.Mode != "" &&
		cluster.Spec.HighAvailability.Mode != antflyv1.HAModeDisabled
}

func haPublicAPISelector(cluster *antflyv1.AntflyCluster, standaloneMode bool, target string) (map[string]string, bool) {
	if target == "" || target == "primary" {
		component := "metadata"
		if standaloneMode {
			component = standaloneComponent(cluster)
		}
		return serviceSelectorLabels(cluster.Name, component), true
	}
	ha := cluster.Spec.HighAvailability
	if ha == nil {
		return nil, false
	}
	for _, standby := range ha.Standbys {
		if standby.Name == target && len(standby.RouteSelector) > 0 {
			return maps.Clone(standby.RouteSelector), true
		}
	}
	return nil, false
}

func haPrimaryRouteServiceAnnotations(cluster *antflyv1.AntflyCluster, target string, selectorApplied bool) map[string]string {
	annotations := map[string]string{
		haPrimaryRouteTargetAnnotation:   target,
		haPrimaryRouteSelectorAnnotation: strconv.FormatBool(selectorApplied),
	}
	if cluster.Status.HAStatus == nil || target == "" || target == "primary" {
		return annotations
	}
	route := cluster.Status.HAStatus.PrimaryRoute
	if route.FenceAuthority != "" {
		annotations[haPrimaryRouteFenceAuthorityAnnotation] = string(route.FenceAuthority)
	}
	if route.FenceGeneration > 0 {
		annotations[haPrimaryRouteFenceGenerationAnnotation] = strconv.FormatUint(route.FenceGeneration, 10)
	}
	return annotations
}

func syncHAPrimaryRouteServiceAnnotations(service *corev1.Service, desired map[string]string) {
	if service.Annotations == nil {
		service.Annotations = map[string]string{}
	}
	managedKeys := [...]string{
		haPrimaryRouteTargetAnnotation,
		haPrimaryRouteFenceAuthorityAnnotation,
		haPrimaryRouteFenceGenerationAnnotation,
		haPrimaryRouteSelectorAnnotation,
	}
	for _, key := range managedKeys {
		if value, ok := desired[key]; ok {
			service.Annotations[key] = value
		} else {
			delete(service.Annotations, key)
		}
	}
}

// buildPVCRetentionPolicy maps CRD PVCRetentionPolicy to the Kubernetes StatefulSet retention policy.
// Returns nil if no retention policy is configured (Kubernetes defaults to Retain/Retain).
func buildPVCRetentionPolicy(policy *antflyv1.PVCRetentionPolicy) *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy {
	if policy == nil {
		return nil
	}

	mapPolicy := func(val antflyv1.PVCRetentionPolicyType) appsv1.PersistentVolumeClaimRetentionPolicyType {
		if val == antflyv1.PVCRetentionDelete {
			return appsv1.DeletePersistentVolumeClaimRetentionPolicyType
		}
		return appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}

	return &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: mapPolicy(policy.WhenDeleted),
		WhenScaled:  mapPolicy(policy.WhenScaled),
	}
}

// cleanupStorageResources handles ordered deletion of StatefulSets, pods, and PVCs
// on cluster deletion. Returns a non-nil Result if the caller should requeue.
// Deletion order: validate all PVCs → record PVC deletion intent → delete
// StatefulSets → wait for pods → remove finalizer. Kubernetes PVC protection
// keeps claims alive until their last pod exits without losing cleanup intent
// when a historical StatefulSet disappears between reconciles.
func (r *AntflyClusterReconciler) cleanupStorageResources(ctx context.Context, cluster *antflyv1.AntflyCluster) (*ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Step 1: Discover StatefulSets controlled by this exact AntflyCluster UID.
	// app.kubernetes.io/instance is a discovery label, not an ownership proof:
	// another controller or Helm release can legitimately use the same value.
	var namespaceStatefulSets appsv1.StatefulSetList
	if err := r.List(ctx, &namespaceStatefulSets, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list StatefulSets for PVC cleanup: %w", err)
	}
	discoveredClaimPrefixes := make([]string, 0)
	ownedStatefulSetNames := make(map[string]struct{})
	ownedStatefulSetUIDs := make(map[types.UID]struct{})
	for i := range namespaceStatefulSets.Items {
		sts := &namespaceStatefulSets.Items[i]
		if !metav1.IsControlledBy(sts, cluster) {
			continue
		}
		ownedStatefulSetNames[sts.Name] = struct{}{}
		if sts.UID != "" {
			ownedStatefulSetUIDs[sts.UID] = struct{}{}
		}
		for claimIndex := range sts.Spec.VolumeClaimTemplates {
			discoveredClaimPrefixes = append(discoveredClaimPrefixes,
				sts.Spec.VolumeClaimTemplates[claimIndex].Name+"-"+sts.Name+"-")
		}
	}

	// Include canonical owned workloads in the deletion set. Do not mutate them
	// yet: every matching PVC must pass ownership validation first so an error or
	// controller restart cannot erase the only source of a historical prefix.
	for _, suffix := range []string{"-metadata", "-data", "-standalone", "-swarm"} {
		sts := &appsv1.StatefulSet{}
		stsName := cluster.Name + suffix
		err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: cluster.Namespace}, sts)
		if err == nil {
			if !metav1.IsControlledBy(sts, cluster) {
				log.Info("Skipping canonical-name StatefulSet not owned by cluster", "statefulset", stsName)
				continue
			}
			ownedStatefulSetNames[sts.Name] = struct{}{}
			if sts.UID != "" {
				ownedStatefulSetUIDs[sts.UID] = struct{}{}
			}
		} else if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get StatefulSet %s: %w", stsName, err)
		}
	}

	canonicalClaimPrefixes := []string{
		"metadata-storage-" + cluster.Name + "-metadata-",
		"data-storage-" + cluster.Name + "-data-",
		"standalone-storage-" + cluster.Name + "-standalone-",
		"swarm-storage-" + cluster.Name + "-swarm-",
	}
	startupTargetPVCName := ""
	startupTargetPVCUID := ""
	if gate := haRuntimeStartupGate(cluster); gate != nil && gate.RequiredReceipt != nil {
		startupTargetPVCName = strings.TrimSpace(gate.RequiredReceipt.TargetPVCName)
		startupTargetPVCUID = strings.TrimSpace(gate.RequiredReceipt.TargetPVCUID)
	}
	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list PVCs: %w", err)
	}
	cleanupPVCs := make([]*corev1.PersistentVolumeClaim, 0)
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if pvc.Name != startupTargetPVCName && !hasAnyPrefix(pvc.Name, discoveredClaimPrefixes) && !hasAnyPrefix(pvc.Name, canonicalClaimPrefixes) {
			continue
		}
		if err := validatePVCOwnership(cluster, pvc); err != nil {
			return nil, err
		}
		if pvc.Name == startupTargetPVCName && startupTargetPVCUID != "" && string(pvc.UID) != startupTargetPVCUID {
			return nil, fmt.Errorf("refusing HA startup target PVC cleanup for %s/%s: expected UID %q, got %q", pvc.Namespace, pvc.Name, startupTargetPVCUID, pvc.UID)
		}
		cleanupPVCs = append(cleanupPVCs, pvc)
	}

	// Record claim deletion intent before deleting StatefulSets. PVC protection
	// keeps claims mounted by live pods until those pods terminate; issuing the
	// delete first makes cleanup retry-safe even if reconciliation stops between
	// the workload and claim operations.
	for _, pvc := range cleanupPVCs {
		log.Info("Deleting PVC", "pvc", pvc.Name)
		if err := r.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to delete PVC %s: %w", pvc.Name, err)
		}
	}
	for i := range namespaceStatefulSets.Items {
		sts := &namespaceStatefulSets.Items[i]
		if _, owned := ownedStatefulSetNames[sts.Name]; !owned || !metav1.IsControlledBy(sts, cluster) {
			continue
		}
		log.Info("Deleting StatefulSet for PVC cleanup", "statefulset", sts.Name)
		if err := r.Delete(ctx, sts); err != nil && !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to delete StatefulSet %s: %w", sts.Name, err)
		}
	}

	// Step 2: Check if pods still exist — requeue if they do
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list pods for PVC cleanup: %w", err)
	}
	remainingOwnedPods := 0
	for i := range podList.Items {
		owner := metav1.GetControllerOf(&podList.Items[i])
		if owner == nil || owner.Kind != "StatefulSet" {
			continue
		}
		_, uidMatch := ownedStatefulSetUIDs[owner.UID]
		_, nameMatch := ownedStatefulSetNames[owner.Name]
		if uidMatch || (owner.UID == "" && nameMatch) {
			remainingOwnedPods++
		}
	}
	if remainingOwnedPods > 0 {
		log.Info("Waiting for pods to terminate", "remaining", remainingOwnedPods)
		result := ctrl.Result{RequeueAfter: 5 * time.Second}
		return &result, nil
	}

	return nil, nil
}

// validatePVCOwnership requires the immutable cluster UID before destructive
// cleanup. Names and app.kubernetes.io/instance labels are intentionally not
// ownership proofs because both can be reused by another cluster incarnation.
func validatePVCOwnership(cluster *antflyv1.AntflyCluster, pvc *corev1.PersistentVolumeClaim) error {
	if instance, ok := pvc.Labels["app.kubernetes.io/instance"]; ok && instance != cluster.Name {
		return fmt.Errorf(
			"refusing PVC cleanup for %s/%s: canonical claim name matches AntflyCluster %q but app.kubernetes.io/instance identifies %q",
			pvc.Namespace, pvc.Name, cluster.Name, instance,
		)
	}
	uid, ok := pvc.Labels[labelClusterUID]
	if !ok || uid != string(cluster.UID) {
		return fmt.Errorf(
			"refusing PVC cleanup for %s/%s: expected %s=%q, got %q",
			pvc.Namespace, pvc.Name, labelClusterUID, cluster.UID, uid,
		)
	}
	return nil
}

type dataNodeShutdownStatus struct {
	NodeID          int64  `json:"node_id"`
	Phase           string `json:"phase"`
	SafeToTerminate bool   `json:"safe_to_terminate"`
	Blocked         bool   `json:"blocked,omitempty"`
	BlockedReason   string `json:"blocked_reason,omitempty"`
	Message         string `json:"message,omitempty"`
}

func (r *AntflyClusterReconciler) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return defaultOperatorHTTPClient
}

// requestDataNodeShutdown asks Antfly metadata to drain a data node and returns
// the current runtime safety status. Node ID is deterministic:
// node_id = pod_ordinal + 1.
func (r *AntflyClusterReconciler) requestDataNodeShutdown(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeIDString string) (*dataNodeShutdownStatus, error) {
	log := log.FromContext(ctx)
	metadataAddr := fmt.Sprintf("http://%s-metadata.%s.svc:%d",
		cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)
	url := fmt.Sprintf("%s/internal/v1/nodes/%s/shutdown", metadataAddr, nodeIDString)

	log.Info("Requesting data node shutdown before scale-down", "nodeID", nodeIDString)
	body := strings.NewReader(`{"type":"remove","reason":"operator scale-down"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown request for node %s: %w", nodeIDString, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient().Do(req) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		log.Error(err, "Failed to request data node shutdown, will retry", "nodeID", nodeIDString)
		return nil, fmt.Errorf("failed to request shutdown for node %s: %w", nodeIDString, err)
	}
	statusCode := resp.StatusCode
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("shutdown request for node %s returned status %d", nodeIDString, statusCode)
	}

	statusReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown status request for node %s: %w", nodeIDString, err)
	}
	statusResp, err := r.httpClient().Do(statusReq) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return nil, fmt.Errorf("failed to get shutdown status for node %s: %w", nodeIDString, err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode < 200 || statusResp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, statusResp.Body)
		return nil, fmt.Errorf("shutdown status for node %s returned status %d", nodeIDString, statusResp.StatusCode)
	}

	var status dataNodeShutdownStatus
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode shutdown status for node %s: %w", nodeIDString, err)
	}
	return &status, nil
}

func (r *AntflyClusterReconciler) cancelDataNodeShutdown(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeIDString string) (*dataNodeShutdownStatus, error) {
	log := log.FromContext(ctx)
	metadataAddr := fmt.Sprintf("http://%s-metadata.%s.svc:%d",
		cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)
	url := fmt.Sprintf("%s/internal/v1/nodes/%s/shutdown", metadataAddr, nodeIDString)

	log.Info("Canceling data node shutdown", "nodeID", nodeIDString)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown cancellation request for node %s: %w", nodeIDString, err)
	}
	resp, err := r.httpClient().Do(req) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return nil, fmt.Errorf("failed to cancel shutdown for node %s: %w", nodeIDString, err)
	}
	statusCode := resp.StatusCode
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("shutdown cancellation for node %s returned status %d", nodeIDString, statusCode)
	}

	statusReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown status request after cancellation for node %s: %w", nodeIDString, err)
	}
	statusResp, err := r.httpClient().Do(statusReq) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return nil, fmt.Errorf("failed to get shutdown status after cancellation for node %s: %w", nodeIDString, err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, statusResp.Body)
		return &dataNodeShutdownStatus{Phase: "not_found"}, nil
	}
	if statusResp.StatusCode < 200 || statusResp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, statusResp.Body)
		return nil, fmt.Errorf("shutdown status after cancellation for node %s returned status %d", nodeIDString, statusResp.StatusCode)
	}

	var status dataNodeShutdownStatus
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode shutdown status after cancellation for node %s: %w", nodeIDString, err)
	}
	return &status, nil
}

func (r *AntflyClusterReconciler) finalizeDataNodeShutdown(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeIDString string) error {
	metadataAddr := fmt.Sprintf("http://%s-metadata.%s.svc:%d",
		cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)
	url := fmt.Sprintf("%s/internal/v1/nodes/%s", metadataAddr, nodeIDString)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create node shutdown finalization request for node %s: %w", nodeIDString, err)
	}
	resp, err := r.httpClient().Do(req) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return fmt.Errorf("failed to finalize shutdown for node %s: %w", nodeIDString, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("shutdown finalization for node %s returned status %d", nodeIDString, resp.StatusCode)
	}
	return nil
}

const (
	// annotationDefaultTopologySpread tracks whether the operator applied default topology spread
	annotationDefaultTopologySpread = "antfly.io/default-topology-spread"
	// These annotations are a durable guard against changing the on-disk format
	// when admission webhooks are unavailable or bypassed.
	annotationStorageEngine = "antfly.io/storage-engine"
	annotationLiteFileName  = "antfly.io/lite-file-name"
)

type topologyMode string

const (
	topologyModeDistributed topologyMode = "distributed"
	topologyModeStandalone  topologyMode = "standalone"
	topologyModeInvalid     topologyMode = "invalid"
)

func effectiveTopologyMode(cluster *antflyv1.AntflyCluster) topologyMode {
	switch cluster.Spec.Mode {
	case antflyv1.ClusterModeStandalone:
		return topologyModeStandalone
	case antflyv1.ClusterModeDistributed, "":
		return topologyModeDistributed
	default:
		return topologyModeInvalid
	}
}

func isStandaloneMode(cluster *antflyv1.AntflyCluster) bool {
	return effectiveTopologyMode(cluster) == topologyModeStandalone
}

func standaloneUsesLegacySwarmIdentity(cluster *antflyv1.AntflyCluster) bool {
	return cluster.Spec.Standalone != nil &&
		cluster.Spec.Standalone.ResourceIdentity == antflyv1.StandaloneResourceIdentityLegacySwarm
}

func standaloneComponent(cluster *antflyv1.AntflyCluster) string {
	if standaloneUsesLegacySwarmIdentity(cluster) {
		return "swarm"
	}
	return "standalone"
}

func standaloneStatefulSetName(cluster *antflyv1.AntflyCluster) string {
	return cluster.Name + "-" + standaloneComponent(cluster)
}

func standaloneStorageVolumeName(cluster *antflyv1.AntflyCluster) string {
	return standaloneComponent(cluster) + "-storage"
}

func standaloneStorageIdentity(cluster *antflyv1.AntflyCluster) (engine, liteFileName string) {
	engine = cluster.Spec.Storage.Engine
	if engine == "" {
		engine = "local"
	}
	if engine == "lite" {
		liteFileName = cluster.Spec.Storage.LiteFileName
		if liteFileName == "" {
			liteFileName = "antfly.aflite"
		}
	}
	return engine, liteFileName
}

func validateAndSetStandaloneStorageIdentity(statefulSet *appsv1.StatefulSet, cluster *antflyv1.AntflyCluster) error {
	engine, liteFileName := standaloneStorageIdentity(cluster)
	if statefulSet.ResourceVersion != "" {
		persistedEngine, ok := statefulSet.Annotations[annotationStorageEngine]
		if !ok {
			if standaloneUsesLegacySwarmIdentity(cluster) && engine == "local" && statefulSet.Name == cluster.Name+"-swarm" && metav1.IsControlledBy(statefulSet, cluster) {
				persistedEngine = "local"
			} else {
				return fmt.Errorf("existing StatefulSet %s is missing storage identity annotation %q; refusing to guess its on-disk format", statefulSet.Name, annotationStorageEngine)
			}
		}
		if persistedEngine != engine {
			return fmt.Errorf("storage engine migration requires backup and restore: existing StatefulSet %s uses %q, requested %q", statefulSet.Name, persistedEngine, engine)
		}
		if engine == "lite" {
			persistedFileName, ok := statefulSet.Annotations[annotationLiteFileName]
			if !ok {
				return fmt.Errorf("existing Lite StatefulSet %s is missing storage identity annotation %q; refusing to guess its database file", statefulSet.Name, annotationLiteFileName)
			}
			if persistedFileName != liteFileName {
				return fmt.Errorf("lite filename migration requires backup and restore: existing StatefulSet %s uses %q, requested %q", statefulSet.Name, persistedFileName, liteFileName)
			}
		}
	}
	if statefulSet.Annotations == nil {
		statefulSet.Annotations = make(map[string]string)
	}
	statefulSet.Annotations[annotationStorageEngine] = engine
	if engine == "lite" {
		statefulSet.Annotations[annotationLiteFileName] = liteFileName
	} else {
		delete(statefulSet.Annotations, annotationLiteFileName)
	}
	return nil
}

func shouldCancelDataScaleDown(status *antflyv1.DataScaleDownStatus, currentReplicas, desiredReplicas int32) bool {
	if status == nil || status.DrainingNodeID == "" {
		return false
	}
	switch status.Phase {
	case "Draining", "Blocked", "Failed", "Canceling", "Scaling":
	default:
		return false
	}
	return currentReplicas > status.DrainingOrdinal && desiredReplicas > status.DrainingOrdinal
}

func shouldCancelDataScaleDownForSuspend(status *antflyv1.DataScaleDownStatus, currentReplicas int32) bool {
	if status == nil || status.DrainingNodeID == "" {
		return false
	}
	switch status.Phase {
	case "Draining", "Blocked", "Failed", "Canceling", "Scaling":
	default:
		return false
	}
	return currentReplicas > status.DrainingOrdinal
}

func shouldFinalizeDataScaleDown(status *antflyv1.DataScaleDownStatus, currentReplicas, desiredReplicas int32, dataScaleDownRequested bool) bool {
	if status == nil || status.DrainingNodeID == "" || shouldCancelDataScaleDown(status, currentReplicas, desiredReplicas) {
		return false
	}
	switch status.Phase {
	case "Scaling":
		return true
	case "Failed":
		return !dataScaleDownRequested && status.AppliedReplicas < status.FromReplicas && currentReplicas <= status.AppliedReplicas
	default:
		return false
	}
}

func (r *AntflyClusterReconciler) ensureTopologyResourcesMatchMode(ctx context.Context, cluster *antflyv1.AntflyCluster, mode topologyMode) error {
	if mode == topologyModeInvalid {
		return fmt.Errorf("unsupported spec.mode %q; reconciliation is blocked until the resource uses Distributed or Standalone", cluster.Spec.Mode)
	}

	expected := map[string]struct{}{}
	if mode == topologyModeStandalone {
		expected[standaloneStatefulSetName(cluster)] = struct{}{}
	} else {
		expected[cluster.Name+"-metadata"] = struct{}{}
		expected[cluster.Name+"-data"] = struct{}{}
	}

	// Discover by the stable instance label instead of guessing historical names.
	// Any operator-owned workload outside the selected topology requires an
	// explicit data migration; silently adopting or redirecting Services could
	// expose an empty database.
	var statefulSets appsv1.StatefulSetList
	if err := r.List(ctx, &statefulSets, client.InNamespace(cluster.Namespace)); err != nil {
		return fmt.Errorf("failed to discover existing StatefulSets for topology safety check: %w", err)
	}
	for i := range statefulSets.Items {
		sts := &statefulSets.Items[i]
		_, hasExpectedName := expected[sts.Name]
		controlled := metav1.IsControlledBy(sts, cluster)
		instanceLabeled := sts.Labels["app.kubernetes.io/instance"] == cluster.Name
		if hasExpectedName && !controlled {
			return fmt.Errorf("cannot reconcile %s topology because canonical StatefulSet %q is not controlled by AntflyCluster UID %s; rename or remove the conflicting workload explicitly", mode, sts.Name, cluster.UID)
		}
		if !instanceLabeled && !controlled {
			continue
		}
		if hasExpectedName {
			continue
		}
		return fmt.Errorf("cannot reconcile %s topology while existing Antfly StatefulSet %q is not part of that topology; migrate or remove the old workload explicitly", mode, sts.Name)
	}
	return nil
}

// applyDefaultZoneTopologySpread adds a soft zone topology spread constraint when:
// - User has not specified explicit topology constraints in the CRD
// - GKE Autopilot is not enabled (Autopilot manages topology internally)
// - The StatefulSet is new OR already has the operator annotation (supports existing clusters opting in)
func applyDefaultZoneTopologySpread(statefulSet *appsv1.StatefulSet, podTemplate *corev1.PodTemplateSpec, component string, clusterName string, userConstraints []corev1.TopologySpreadConstraint, isGKEAutopilot bool) {
	// User has explicit constraints — respect them, remove our annotation if present
	if len(userConstraints) > 0 {
		delete(statefulSet.Annotations, annotationDefaultTopologySpread)
		return
	}

	// GKE Autopilot manages topology internally
	if isGKEAutopilot {
		return
	}

	// Only apply to new StatefulSets or those already tracked by our annotation
	if statefulSet.CreationTimestamp.IsZero() || statefulSet.Annotations[annotationDefaultTopologySpread] == "true" {
		// Ensure StatefulSet has annotations map
		if statefulSet.Annotations == nil {
			statefulSet.Annotations = make(map[string]string)
		}
		statefulSet.Annotations[annotationDefaultTopologySpread] = "true"

		podTemplate.Spec.TopologySpreadConstraints = append(podTemplate.Spec.TopologySpreadConstraints,
			corev1.TopologySpreadConstraint{
				MaxSkew:           1,
				TopologyKey:       "topology.kubernetes.io/zone",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: serviceSelectorLabels(clusterName, component),
				},
			},
		)
	}
}

// applyDefaults sets default port values if not specified
func (r *AntflyClusterReconciler) applyDefaults(cluster *antflyv1.AntflyCluster) {
	standaloneMode := isStandaloneMode(cluster)

	if cluster.Spec.Mode == "" {
		cluster.Spec.Mode = antflyv1.ClusterModeDistributed
	}

	if standaloneMode && cluster.Spec.Standalone != nil {
		if cluster.Spec.Standalone.ResourceIdentity == "" {
			cluster.Spec.Standalone.ResourceIdentity = antflyv1.StandaloneResourceIdentityV1
		}
		if cluster.Spec.Standalone.Replicas == 0 {
			cluster.Spec.Standalone.Replicas = 1
		}
		if cluster.Spec.Standalone.NodeID == 0 {
			cluster.Spec.Standalone.NodeID = 1
		}
		if cluster.Spec.Standalone.MetadataAPI.Port == 0 {
			cluster.Spec.Standalone.MetadataAPI.Port = 8080
		}
		if cluster.Spec.Standalone.MetadataRaft.Port == 0 {
			cluster.Spec.Standalone.MetadataRaft.Port = 9017
		}
		if cluster.Spec.Standalone.StoreAPI.Port == 0 {
			cluster.Spec.Standalone.StoreAPI.Port = 12380
		}
		if cluster.Spec.Standalone.StoreRaft.Port == 0 {
			cluster.Spec.Standalone.StoreRaft.Port = 9021
		}
		if cluster.Spec.Standalone.Health.Port == 0 {
			cluster.Spec.Standalone.Health.Port = 4200
		}
		if cluster.Spec.Standalone.Inference == nil {
			cluster.Spec.Standalone.Inference = &antflyv1.StandaloneInferenceSpec{
				Enabled: true,
				APIURL:  "http://0.0.0.0:11433",
			}
		} else if cluster.Spec.Standalone.Inference.APIURL == "" {
			cluster.Spec.Standalone.Inference.APIURL = "http://0.0.0.0:11433"
		}
	}

	if !standaloneMode {
		// Default ports for metadata nodes
		if cluster.Spec.MetadataNodes.MetadataAPI.Port == 0 {
			cluster.Spec.MetadataNodes.MetadataAPI.Port = 12377
		}
		if cluster.Spec.MetadataNodes.MetadataRaft.Port == 0 {
			cluster.Spec.MetadataNodes.MetadataRaft.Port = 9017
		}
		if cluster.Spec.MetadataNodes.Health.Port == 0 {
			cluster.Spec.MetadataNodes.Health.Port = 4200
		}

		// Default ports for data nodes
		if cluster.Spec.DataNodes.API.Port == 0 {
			cluster.Spec.DataNodes.API.Port = 12380
		}
		if cluster.Spec.DataNodes.Raft.Port == 0 {
			cluster.Spec.DataNodes.Raft.Port = 9021
		}
		if cluster.Spec.DataNodes.Health.Port == 0 {
			cluster.Spec.DataNodes.Health.Port = 4200
		}
	}

	// Default service mesh configuration
	if cluster.Spec.ServiceMesh == nil {
		cluster.Spec.ServiceMesh = &antflyv1.ServiceMeshSpec{
			Enabled: false,
		}
	}

	if cluster.Spec.Storage.StorageAutoGrow != nil && cluster.Spec.Storage.StorageAutoGrow.Enabled {
		if cluster.Spec.Storage.StorageAutoGrow.GrowThresholdPercent == 0 {
			cluster.Spec.Storage.StorageAutoGrow.GrowThresholdPercent = 85
		}
		if cluster.Spec.Storage.StorageAutoGrow.GrowIncrement == "" {
			cluster.Spec.Storage.StorageAutoGrow.GrowIncrement = "10Gi"
		}
	}

	// Default PublicAPI configuration
	if cluster.Spec.PublicAPI == nil {
		enabled := false
		serviceType := corev1.ServiceTypeLoadBalancer
		cluster.Spec.PublicAPI = &antflyv1.PublicAPIConfig{
			Enabled:     &enabled,
			ServiceType: &serviceType,
			Port:        80,
		}
	} else {
		if cluster.Spec.PublicAPI.Enabled == nil {
			enabled := false
			cluster.Spec.PublicAPI.Enabled = &enabled
		}
		if cluster.Spec.PublicAPI.ServiceType == nil {
			serviceType := corev1.ServiceTypeLoadBalancer
			cluster.Spec.PublicAPI.ServiceType = &serviceType
		}
		if cluster.Spec.PublicAPI.Port == 0 {
			cluster.Spec.PublicAPI.Port = 80
		}
	}

	// GKE Autopilot defaults (T024)
	if cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot {
		if cluster.Spec.GKE.AutopilotComputeClass == "" {
			cluster.Spec.GKE.AutopilotComputeClass = "Balanced"
		}
	}

	// EKS defaults
	if cluster.Spec.EKS != nil && cluster.Spec.EKS.Enabled {
		// Default EBS volume type to gp3 (best price/performance)
		if cluster.Spec.EKS.EBSVolumeType == "" {
			cluster.Spec.EKS.EBSVolumeType = "gp3"
		}
	}
}

// validateClusterConfiguration validates the cluster configuration (T025)
// This is a fallback validation for cases where webhook validation is disabled
func (r *AntflyClusterReconciler) validateClusterConfiguration(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	// Call the webhook validation methods as fallback
	if err := cluster.ValidateCreate(); err != nil {
		return err
	}

	if err := r.validateMetadataReplicaTopology(ctx, cluster); err != nil {
		return &metadataTopologyValidationError{cause: err}
	}
	return nil
}

// validateMetadataReplicaTopology closes the update-validation gap on clusters
// where admission webhooks or CRD CEL transition rules are not available. It
// uses controller-owned status and retained-PVC annotations as durable records;
// the mutable StatefulSet replica field is used only to migrate legacy clusters.
func (r *AntflyClusterReconciler) validateMetadataReplicaTopology(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if cluster.Spec.Mode != antflyv1.ClusterModeDistributed {
		r.metadataTopologyObservations.Delete(metadataTopologyObservationKey(cluster))
		return nil
	}

	desiredReplicas := effectiveMetadataNodeReplicas(cluster)
	// Retained storage is a safety boundary: a stale cache miss could otherwise
	// approve and create a new StatefulSet before an incompatible retained claim
	// becomes visible. Production wires BoundaryReader to the uncached API reader.
	topologyReader := r.haBoundaryReader()
	type topologyRecord struct {
		source   string
		replicas int32
	}
	records := make([]topologyRecord, 0, 3)
	if cluster.Status.MetadataTopologyReplicas > 0 {
		records = append(records, topologyRecord{
			source:   "AntflyCluster status",
			replicas: cluster.Status.MetadataTopologyReplicas,
		})
	}

	metadataStatefulSet := &appsv1.StatefulSet{}
	err := topologyReader.Get(ctx, types.NamespacedName{
		Name:      cluster.Name + "-metadata",
		Namespace: cluster.Namespace,
	}, metadataStatefulSet)
	statefulSetExists := err == nil
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("read existing metadata StatefulSet: %w", err)
	}
	if statefulSetExists {
		if raw, ok := metadataStatefulSet.Annotations[metadataTopologyReplicasAnnotation]; ok {
			replicas, err := parseMetadataTopologyReplicas(raw, "metadata StatefulSet annotation")
			if err != nil {
				return err
			}
			records = append(records, topologyRecord{source: "metadata StatefulSet annotation", replicas: replicas})
		}
	}

	pvcPrefix := metadataPVCPrefix(cluster)
	var pvcList corev1.PersistentVolumeClaimList
	if err := topologyReader.List(ctx, &pvcList, client.InNamespace(cluster.Namespace)); err != nil {
		return fmt.Errorf("list metadata PVCs for topology validation: %w", err)
	}
	metadataPVCsExist := false
	pvcOrdinals := make(map[int32]string)
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		ordinal, matches := metadataPVCOrdinal(cluster, pvc)
		if !matches {
			continue
		}
		metadataPVCsExist = true
		pvcOrdinals[ordinal] = pvc.Name
		if raw, ok := pvc.Annotations[metadataTopologyReplicasAnnotation]; ok {
			replicas, err := parseMetadataTopologyReplicas(raw, fmt.Sprintf("metadata PVC %s annotation", pvc.Name))
			if err != nil {
				return err
			}
			records = append(records, topologyRecord{source: fmt.Sprintf("metadata PVC %s", pvc.Name), replicas: replicas})
		}
	}

	// Compare durable records before checking retained claim completeness so a
	// replica-count change receives the direct immutability diagnostic.
	for _, record := range records {
		if record.replicas == desiredReplicas {
			continue
		}
		return metadataTopologyChangeError(record.source, record.replicas, desiredReplicas)
	}

	// Any retained set must contain exactly the ordinals belonging to the
	// recorded topology. In particular, do not let a same-name recreation fill
	// missing ordinals with fresh claims and combine different Raft histories.
	if metadataPVCsExist {
		if err := validateMetadataPVCOrdinals(pvcOrdinals, desiredReplicas); err != nil {
			return err
		}
	}

	// Bootstrap pre-record clusters from their existing controller-owned
	// StatefulSet only when no durable status/PVC record exists. The mutable
	// replica field is not enough by itself: every retained claim and every
	// running member must prove one stable Raft incarnation before migration.
	if len(records) == 0 && statefulSetExists {
		legacyReplicas := int32(1)
		if metadataStatefulSet.Spec.Replicas != nil {
			legacyReplicas = *metadataStatefulSet.Spec.Replicas
		}
		if legacyReplicas != desiredReplicas {
			return metadataTopologyChangeError("legacy metadata StatefulSet", legacyReplicas, desiredReplicas)
		}
		if err := validateMetadataPVCOrdinals(pvcOrdinals, legacyReplicas); err != nil {
			return fmt.Errorf("cannot safely migrate the legacy metadata topology: %w", err)
		}
		if err := r.validateLegacyMetadataRuntimeTopology(ctx, cluster, legacyReplicas); err != nil {
			return err
		}
		return nil
	}
	if len(records) == 0 {
		if metadataPVCsExist {
			return fmt.Errorf(`cannot establish the immutable metadata topology for retained PVCs with prefix %q

Problem: The metadata StatefulSet is absent and the retained PVCs predate topology recording. Reusing them with an unverified replica count can start divergent Raft incarnations.

Solution: Keep the existing cluster topology if it can be recovered, or back up and restore into a differently named AntflyCluster with fresh metadata PVCs`, pvcPrefix)
		}
		return nil
	}

	return nil
}

func metadataTopologyChangeError(source string, recordedReplicas, desiredReplicas int32) error {
	return fmt.Errorf(`field 'spec.metadataNodes.replicas' is immutable after cluster creation (recorded by %s: %d, attempted: %d)

Problem: Metadata nodes are quorum-bearing. The operator does not yet have a quorum-aware metadata membership-change workflow. Changing the StatefulSet topology with retained PVCs can start divergent Raft incarnations.

Solution: Keep the existing metadata replica count. To change topology, back up the cluster, create a differently named AntflyCluster at the target replica count so it receives fresh metadata PVCs, restore the backup, and cut over. Do not reuse retained metadata PVCs across replica-count changes`, source, recordedReplicas, desiredReplicas)
}

func validateMetadataPVCOrdinals(pvcOrdinals map[int32]string, replicas int32) error {
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		if _, ok := pvcOrdinals[ordinal]; !ok {
			return fmt.Errorf("retained metadata PVC set is incomplete: missing ordinal %d of expected ordinals 0 through %d", ordinal, replicas-1)
		}
	}
	for ordinal, name := range pvcOrdinals {
		if ordinal >= replicas {
			return fmt.Errorf("retained metadata PVC %s has unexpected ordinal %d outside expected ordinals 0 through %d", name, ordinal, replicas-1)
		}
	}
	return nil
}

type metadataRuntimeTopologyStatus struct {
	MetadataGroupID                 uint64  `json:"metadata_group_id"`
	MetadataIncarnation             string  `json:"metadata_incarnation"`
	MetadataRaftLocalNodeID         uint64  `json:"metadata_raft_local_node_id"`
	MetadataRaftRole                string  `json:"metadata_raft_role"`
	MetadataRaftLeaderID            *uint64 `json:"metadata_raft_leader_id"`
	MetadataRaftTerm                uint64  `json:"metadata_raft_term"`
	MetadataRaftLocalVoter          bool    `json:"metadata_raft_local_voter"`
	MetadataRaftVoterCount          int32   `json:"metadata_raft_voter_count"`
	MetadataRaftVoterSetFingerprint *string `json:"metadata_raft_voter_set_fingerprint"`
	MetadataRaftJointConsensus      *bool   `json:"metadata_raft_joint_consensus"`
	MetadataRaftLearnerCount        *int32  `json:"metadata_raft_learner_count"`
	InternalServiceAuthCapability   string  `json:"-"`
}

const (
	maxMetadataRuntimeStatusBytes = 64 * 1024
	metadataRuntimeTopologyPath   = "/metadata/v1/runtime-topology"
	metadataRuntimeStatusPath     = "/metadata/v1/status"
)

var errMetadataRuntimeMembershipStatusUnavailable = stderrors.New("metadata runtime does not expose exact Raft membership status")

type metadataTopologyValidationError struct {
	cause error
}

func (e *metadataTopologyValidationError) Error() string {
	return e.cause.Error()
}

func (e *metadataTopologyValidationError) Unwrap() error {
	return e.cause
}

type metadataLeadershipObservationError struct {
	cause error
}

func (e *metadataLeadershipObservationError) Error() string {
	return e.cause.Error()
}

func (e *metadataLeadershipObservationError) Unwrap() error {
	return e.cause
}

type metadataRuntimeTopologyProbeError struct {
	cause error
}

func (e *metadataRuntimeTopologyProbeError) Error() string {
	return e.cause.Error()
}

func (e *metadataRuntimeTopologyProbeError) Unwrap() error {
	return e.cause
}

type metadataTopologyObservationPendingError struct {
	cause           error
	retryAfter      time.Duration
	conditionReason string
	waitMessage     string
}

func (e *metadataTopologyObservationPendingError) Error() string {
	return e.cause.Error()
}

func (e *metadataTopologyObservationPendingError) Unwrap() error {
	return e.cause
}

type metadataTopologyObservationState struct {
	uid           types.UID
	deadline      time.Time
	retryInterval time.Duration
	expired       bool
}

const (
	metadataLeadershipObservationRetryInterval = 250 * time.Millisecond
	// Metadata Raft uses a 30-tick election timeout with up to another 29
	// ticks of jitter. At the runtime's default 100ms tick, an election can
	// therefore take almost six seconds to begin; allow another second for
	// votes and the resulting leader observation to propagate.
	metadataLeadershipObservationGracePeriod = 7 * time.Second
	// Runtime status probes should fail quickly enough not to monopolize the
	// cluster controller, while tolerating a brief DNS, connection, or server
	// interruption without changing an otherwise healthy cluster's status.
	metadataRuntimeTopologyProbeTimeout       = 2 * time.Second
	metadataRuntimeTopologyProbeRetryInterval = 250 * time.Millisecond
	metadataRuntimeTopologyProbeGracePeriod   = 3 * time.Second
)

func (r *AntflyClusterReconciler) validateLegacyMetadataRuntimeTopology(ctx context.Context, cluster *antflyv1.AntflyCluster, replicas int32) error {
	if err := r.validateMetadataRuntimeTopology(ctx, cluster, replicas); err != nil {
		return fmt.Errorf("cannot safely migrate legacy metadata topology: %w", err)
	}
	return nil
}

// validateMetadataRuntimeTopology proves that every expected member belongs to
// one stable metadata Raft incarnation and the exact configured voter set. It
// is used both while migrating pre-record clusters and when publishing
// steady-state health.
func (r *AntflyClusterReconciler) validateMetadataRuntimeTopology(ctx context.Context, cluster *antflyv1.AntflyCluster, replicas int32) error {
	return r.validateMetadataRuntimeTopologyWithClock(
		ctx,
		cluster,
		replicas,
		time.Now,
		metadataLeadershipObservationRetryInterval,
		metadataLeadershipObservationGracePeriod,
	)
}

func (r *AntflyClusterReconciler) validateMetadataRuntimeTopologyAt(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	replicas int32,
	now time.Time,
	retryInterval time.Duration,
	gracePeriod time.Duration,
) error {
	return r.validateMetadataRuntimeTopologyWithClock(
		ctx,
		cluster,
		replicas,
		func() time.Time { return now },
		retryInterval,
		gracePeriod,
	)
}

func (r *AntflyClusterReconciler) validateMetadataRuntimeTopologyWithClock(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	replicas int32,
	now func() time.Time,
	retryInterval time.Duration,
	gracePeriod time.Duration,
) error {
	startedAt := now()
	err := r.validateMetadataRuntimeTopologyOnce(ctx, cluster, replicas)
	observedAt := now()
	key := metadataTopologyObservationKey(cluster)
	var leadershipErr *metadataLeadershipObservationError
	var probeErr *metadataRuntimeTopologyProbeError
	retry := retryInterval
	grace := gracePeriod
	conditionReason := antflyv1.ReasonMetadataLeadershipObservationPending
	waitMessage := "Waiting for metadata Raft leadership to settle"
	if stderrors.As(err, &probeErr) {
		retry = metadataRuntimeTopologyProbeRetryInterval
		grace = metadataRuntimeTopologyProbeGracePeriod
		conditionReason = antflyv1.ReasonMetadataTopologyObservationPending
		waitMessage = "Waiting for metadata runtime status probes to recover"
	} else if !stderrors.As(err, &leadershipErr) {
		r.metadataTopologyObservations.Delete(key)
		return err
	}

	// Member status is observed over multiple requests rather than through a
	// linearizable cluster-wide snapshot. Carry leadership failures for a full
	// election window and retryable probe failures for a much shorter window.
	// Durable identity, membership, and payload failures still return immediately.
	state := metadataTopologyObservationState{
		uid:           cluster.UID,
		deadline:      startedAt.Add(grace),
		retryInterval: retry,
	}
	if observed, ok := r.metadataTopologyObservations.Load(key); ok {
		candidate := observed.(metadataTopologyObservationState)
		if candidate.uid == cluster.UID {
			state = candidate
			// Alternating leadership and probe failures must not restart or extend
			// an observation window. Use the earliest applicable deadline.
			if deadline := startedAt.Add(grace); deadline.Before(state.deadline) {
				state.deadline = deadline
			}
			state.retryInterval = min(state.retryInterval, retry)
		}
	}
	r.metadataTopologyObservations.Store(key, state)
	remaining := state.deadline.Sub(observedAt)
	if remaining <= 0 {
		state.expired = true
		r.metadataTopologyObservations.Store(key, state)
		return err
	}
	return &metadataTopologyObservationPendingError{
		cause:           err,
		retryAfter:      min(retry, remaining),
		conditionReason: conditionReason,
		waitMessage:     waitMessage,
	}
}

func metadataTopologyObservationKey(cluster *antflyv1.AntflyCluster) string {
	return types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}.String()
}

func (r *AntflyClusterReconciler) metadataTopologyObservationRequeueAfter(cluster *antflyv1.AntflyCluster) time.Duration {
	observed, ok := r.metadataTopologyObservations.Load(metadataTopologyObservationKey(cluster))
	if !ok {
		return 0
	}
	state := observed.(metadataTopologyObservationState)
	if state.uid != cluster.UID {
		return 0
	}
	if state.expired {
		return 0
	}
	remaining := time.Until(state.deadline)
	if remaining <= 0 {
		return time.Millisecond
	}
	return min(state.retryInterval, remaining)
}

func (r *AntflyClusterReconciler) validateMetadataRuntimeTopologyOnce(ctx context.Context, cluster *antflyv1.AntflyCluster, replicas int32) error {
	if replicas < 1 {
		return fmt.Errorf("invalid replica count %d", replicas)
	}
	maxNodeID := uint64(replicas) // #nosec G115 -- replicas is explicitly checked positive above.
	expectedVoterSetFingerprint := metadataRaftVoterSetFingerprint(replicas)
	statuses := make([]*metadataRuntimeTopologyStatus, replicas)
	fetchErrors := make([]error, replicas)
	var waitGroup sync.WaitGroup
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		waitGroup.Add(1)
		go func(ordinal int32) {
			defer waitGroup.Done()
			status, err := r.fetchMetadataRuntimeTopology(ctx, cluster, ordinal)
			statuses[ordinal] = status
			fetchErrors[ordinal] = err
		}(ordinal)
	}
	waitGroup.Wait()
	for ordinal, err := range fetchErrors {
		if err != nil {
			return fmt.Errorf("member ordinal %d status is unavailable: %w", ordinal, err)
		}
	}

	var baseline *metadataRuntimeTopologyStatus
	membershipStatusUnavailable := false
	for ordinal, status := range statuses {
		expectedNodeID := uint64(ordinal + 1) // #nosec G115 -- ordinal is bounded by the positive int32 replica count.
		if status.MetadataGroupID == 0 || !validMetadataIncarnation(status.MetadataIncarnation) {
			return fmt.Errorf("member ordinal %d returned an invalid group or incarnation", ordinal)
		}
		if status.MetadataRaftLocalNodeID != expectedNodeID {
			return fmt.Errorf("member ordinal %d reports local node %d, expected %d", ordinal, status.MetadataRaftLocalNodeID, expectedNodeID)
		}
		if !status.MetadataRaftLocalVoter || status.MetadataRaftVoterCount != replicas {
			return fmt.Errorf("member ordinal %d reports local_voter=%t and voter_count=%d, expected local_voter=true and voter_count=%d", ordinal, status.MetadataRaftLocalVoter, status.MetadataRaftVoterCount, replicas)
		}
		if status.MetadataRaftJointConsensus != nil && *status.MetadataRaftJointConsensus {
			return fmt.Errorf("member ordinal %d reports an active joint-consensus membership transition", ordinal)
		}
		if status.MetadataRaftLearnerCount != nil && *status.MetadataRaftLearnerCount != 0 {
			return fmt.Errorf("member ordinal %d reports learner_count=%d, expected 0", ordinal, *status.MetadataRaftLearnerCount)
		}
		if status.MetadataRaftVoterSetFingerprint == nil || status.MetadataRaftJointConsensus == nil || status.MetadataRaftLearnerCount == nil {
			membershipStatusUnavailable = true
		} else if *status.MetadataRaftVoterSetFingerprint != expectedVoterSetFingerprint {
			return fmt.Errorf("member ordinal %d reports voter set fingerprint %q, expected %q", ordinal, *status.MetadataRaftVoterSetFingerprint, expectedVoterSetFingerprint)
		}

		if baseline == nil {
			baseline = status
			continue
		}
		if status.MetadataGroupID != baseline.MetadataGroupID || status.MetadataIncarnation != baseline.MetadataIncarnation {
			return fmt.Errorf("member ordinal %d disagrees on metadata group or incarnation", ordinal)
		}
	}

	leaderReports := 0
	for ordinal, status := range statuses {
		if status.MetadataRaftLeaderID == nil || *status.MetadataRaftLeaderID < 1 || *status.MetadataRaftLeaderID > maxNodeID {
			return &metadataLeadershipObservationError{cause: fmt.Errorf("member ordinal %d does not report a valid leader", ordinal)}
		}
		if status.MetadataRaftTerm != baseline.MetadataRaftTerm || *status.MetadataRaftLeaderID != *baseline.MetadataRaftLeaderID {
			return &metadataLeadershipObservationError{cause: fmt.Errorf("member ordinal %d disagrees on Raft term or leader", ordinal)}
		}
		switch status.MetadataRaftRole {
		case "leader":
			if *status.MetadataRaftLeaderID != status.MetadataRaftLocalNodeID {
				return &metadataLeadershipObservationError{cause: fmt.Errorf("member ordinal %d reports leader role for node %d but leader %d", ordinal, status.MetadataRaftLocalNodeID, *status.MetadataRaftLeaderID)}
			}
			leaderReports++
		case "follower":
			if *status.MetadataRaftLeaderID == status.MetadataRaftLocalNodeID {
				return &metadataLeadershipObservationError{cause: fmt.Errorf("member ordinal %d reports follower role but identifies itself as leader", ordinal)}
			}
		default:
			return &metadataLeadershipObservationError{cause: fmt.Errorf("member ordinal %d is not in a stable Raft role: %q", ordinal, status.MetadataRaftRole)}
		}
	}
	if leaderReports != 1 {
		return &metadataLeadershipObservationError{cause: fmt.Errorf("expected exactly one member to report leader role, got %d", leaderReports)}
	}
	if membershipStatusUnavailable {
		return fmt.Errorf("%w; update spec.image to a runtime that reports metadata_raft_voter_set_fingerprint, metadata_raft_joint_consensus, and metadata_raft_learner_count", errMetadataRuntimeMembershipStatusUnavailable)
	}
	return nil
}

func (r *AntflyClusterReconciler) fetchMetadataRuntimeTopology(ctx context.Context, cluster *antflyv1.AntflyCluster, ordinal int32) (*metadataRuntimeTopologyStatus, error) {
	probeCtx, cancel := context.WithTimeout(ctx, metadataRuntimeTopologyProbeTimeout)
	defer cancel()

	// New runtimes expose an O(1) topology payload. Fall back once to the
	// historical aggregate status route only while rolling an older runtime;
	// steady-state monitoring must not clone the projected catalog.
	paths := [...]string{metadataRuntimeTopologyPath, metadataRuntimeStatusPath}
	for pathIndex, requestPath := range paths {
		podName := fmt.Sprintf("%s-metadata-%d", cluster.Name, ordinal)
		host := statefulPodDNSName(podName, cluster.Name+"-metadata", cluster.Namespace, r.ClusterDomain)
		url := fmt.Sprintf("http://%s:%d%s", host, cluster.Spec.MetadataNodes.MetadataAPI.Port, requestPath)
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create metadata topology request: %w", err)
		}
		resp, err := r.httpClient().Do(req) //nolint:gosec // URL is a deterministic cluster-internal pod address.
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("request metadata topology: %w", err)
			}
			return nil, &metadataRuntimeTopologyProbeError{cause: fmt.Errorf("request metadata topology: %w", err)}
		}
		if pathIndex == 0 && resp.StatusCode == http.StatusNotFound {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			continue
		}
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			err := fmt.Errorf("metadata topology returned HTTP %d", resp.StatusCode)
			if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
				return nil, &metadataRuntimeTopologyProbeError{cause: err}
			}
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxMetadataRuntimeStatusBytes+1))
		_ = resp.Body.Close()
		if readErr != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("read metadata topology: %w", readErr)
			}
			return nil, &metadataRuntimeTopologyProbeError{cause: fmt.Errorf("read metadata topology: %w", readErr)}
		}
		if len(body) > maxMetadataRuntimeStatusBytes {
			return nil, fmt.Errorf("metadata topology exceeds %d bytes", maxMetadataRuntimeStatusBytes)
		}
		var status metadataRuntimeTopologyStatus
		if err := json.Unmarshal(body, &status); err != nil {
			return nil, fmt.Errorf("decode metadata topology: %w", err)
		}
		status.InternalServiceAuthCapability = strings.TrimSpace(resp.Header.Get(internalServiceAuthCapabilityHeader))
		return &status, nil
	}
	return nil, fmt.Errorf("metadata runtime topology route is unavailable")
}

func (r *AntflyClusterReconciler) fetchDataRuntimeAuthCapability(ctx context.Context, cluster *antflyv1.AntflyCluster, ordinal int32) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, metadataRuntimeTopologyProbeTimeout)
	defer cancel()
	url := fmt.Sprintf("http://%s-data-%d.%s-data.%s.svc.cluster.local:%d/healthz",
		cluster.Name, ordinal, cluster.Name, cluster.Namespace, cluster.Spec.DataNodes.API.Port)
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("create data capability request: %w", err)
	}
	resp, err := r.httpClient().Do(req) //nolint:gosec // URL is a deterministic cluster-internal pod address.
	if err != nil {
		return "", fmt.Errorf("request data capability: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("data capability returned HTTP %d", resp.StatusCode)
	}
	return strings.TrimSpace(resp.Header.Get(internalServiceAuthCapabilityHeader)), nil
}

func internalServiceAuthCapability(mode internalServiceAuthRolloutMode, additionalVerifier bool) string {
	value := internalServiceAuthCapabilityVersion + "; mode=" + string(mode)
	if additionalVerifier {
		value += "; verification=additional"
	}
	return value
}

func (r *AntflyClusterReconciler) everyInternalServiceRuntimeAcknowledges(ctx context.Context, cluster *antflyv1.AntflyCluster, mode internalServiceAuthRolloutMode, additionalVerifier bool) bool {
	want := internalServiceAuthCapability(mode, additionalVerifier)
	for ordinal := int32(0); ordinal < effectiveMetadataNodeReplicas(cluster); ordinal++ {
		status, err := r.fetchMetadataRuntimeTopology(ctx, cluster, ordinal)
		if err != nil || status.InternalServiceAuthCapability != want {
			return false
		}
	}
	dataReplicas := effectiveDataNodeReplicas(cluster)
	if cluster.Spec.DataNodes.Suspend {
		dataReplicas = 0
	}
	for ordinal := int32(0); ordinal < dataReplicas; ordinal++ {
		capability, err := r.fetchDataRuntimeAuthCapability(ctx, cluster, ordinal)
		if err != nil || capability != want {
			return false
		}
	}
	return true
}

func validMetadataIncarnation(value string) bool {
	if len(value) != 32 || value != strings.ToLower(value) || strings.Trim(value, "0") == "" {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func metadataRaftVoterSetFingerprint(replicas int32) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("antfly-raft-voter-set-v1\x00"))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(replicas)) // #nosec G115 -- callers validate replicas as positive.
	_, _ = hasher.Write(encoded[:])
	for nodeID := int32(1); nodeID <= replicas; nodeID++ {
		binary.BigEndian.PutUint64(encoded[:], uint64(nodeID)) // #nosec G115 -- loop starts at one and is bounded by positive replicas.
		_, _ = hasher.Write(encoded[:])
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func parseMetadataTopologyReplicas(raw, source string) (int32, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s %q is not a valid positive replica count", source, raw)
	}
	return int32(parsed), nil
}

func effectiveMetadataNodeReplicas(cluster *antflyv1.AntflyCluster) int32 {
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		return cluster.Spec.MetadataNodes.Replicas
	}
	return 3
}

func metadataPVCPrefix(cluster *antflyv1.AntflyCluster) string {
	return "metadata-storage-" + cluster.Name + "-metadata-"
}

// metadataPVCOrdinal matches the exact claim-name shape that this cluster's
// metadata StatefulSet will mount. Labels are deliberately not an ownership
// gate: Kubernetes attaches an existing claim by name even when its labels are
// stale or incorrect, so validation must account for every canonical name.
func metadataPVCOrdinal(cluster *antflyv1.AntflyCluster, pvc *corev1.PersistentVolumeClaim) (int32, bool) {
	ordinalText, ok := strings.CutPrefix(pvc.Name, metadataPVCPrefix(cluster))
	if !ok {
		return 0, false
	}
	ordinal, err := strconv.ParseInt(ordinalText, 10, 32)
	if err != nil || ordinal < 0 || strconv.FormatInt(ordinal, 10) != ordinalText {
		return 0, false
	}
	return int32(ordinal), true
}

const (
	metadataMembershipStatusCapabilityAnnotation = "antfly.io/metadata-membership-status-capability"
	metadataMembershipStatusCapabilityVersion    = "v2"
	metadataMembershipCapabilityRolloutInterval  = 10 * time.Second
)

type metadataMembershipCapabilityRolloutState int

const (
	metadataMembershipCapabilityRolloutStarted metadataMembershipCapabilityRolloutState = iota
	metadataMembershipCapabilityRolloutPending
	metadataMembershipCapabilityUnavailable
)

func metadataMembershipCapabilityRolloutComplete(statefulSet *appsv1.StatefulSet, expectedReplicas int32) bool {
	return statefulSet.Status.ObservedGeneration >= statefulSet.Generation &&
		statefulSet.Status.UpdatedReplicas >= expectedReplicas &&
		statefulSet.Status.ReadyReplicas >= expectedReplicas &&
		statefulSet.Status.CurrentRevision != "" &&
		statefulSet.Status.CurrentRevision == statefulSet.Status.UpdateRevision
}

func statefulSetRolloutComplete(statefulSet *appsv1.StatefulSet, expectedReplicas int32) bool {
	if statefulSet == nil || statefulSet.Name == "" || statefulSet.Status.ObservedGeneration < statefulSet.Generation {
		return false
	}
	if statefulSet.Status.UpdatedReplicas < expectedReplicas || statefulSet.Status.ReadyReplicas < expectedReplicas {
		return false
	}
	return statefulSet.Status.CurrentRevision != "" && statefulSet.Status.CurrentRevision == statefulSet.Status.UpdateRevision
}

func statefulSetInternalServiceAuthMode(statefulSet *appsv1.StatefulSet) internalServiceAuthRolloutMode {
	if statefulSet == nil {
		return ""
	}
	return internalServiceAuthRolloutMode(statefulSet.Spec.Template.Annotations[internalServiceAuthRolloutAnnotation])
}

// desiredInternalServiceAuthRolloutMode makes upgrades from runtimes that sent
// unsigned internal RPC a durable two-rollout transition. It never reads the
// signing key: capability is proven by a non-secret response header emitted by
// every upgraded metadata process after it has started in migration mode.
func (r *AntflyClusterReconciler) desiredInternalServiceAuthRolloutMode(ctx context.Context, cluster *antflyv1.AntflyCluster) (internalServiceAuthRolloutMode, bool, error) {
	metadataStatefulSet := &appsv1.StatefulSet{}
	metadataKey := types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}
	metadataErr := r.Get(ctx, metadataKey, metadataStatefulSet)
	if metadataErr != nil && !errors.IsNotFound(metadataErr) {
		return "", false, fmt.Errorf("read metadata StatefulSet for internal-service auth rollout: %w", metadataErr)
	}
	dataStatefulSet := &appsv1.StatefulSet{}
	dataKey := types.NamespacedName{Name: cluster.Name + "-data", Namespace: cluster.Namespace}
	dataErr := r.Get(ctx, dataKey, dataStatefulSet)
	if dataErr != nil && !errors.IsNotFound(dataErr) {
		return "", false, fmt.Errorf("read data StatefulSet for internal-service auth rollout: %w", dataErr)
	}

	metadataExists := metadataErr == nil
	dataExists := dataErr == nil
	if !metadataExists && !dataExists {
		// A brand-new cluster has no legacy peers and starts fail-closed.
		return internalServiceAuthRolloutEnforce, false, nil
	}
	metadataMode := statefulSetInternalServiceAuthMode(metadataStatefulSet)
	dataMode := statefulSetInternalServiceAuthMode(dataStatefulSet)
	if metadataMode == internalServiceAuthRolloutEnforce || dataMode == internalServiceAuthRolloutEnforce {
		// Once enforcement begins, never regress to accepting unsigned traffic.
		return internalServiceAuthRolloutEnforce, false, nil
	}
	if !metadataExists || !dataExists || metadataMode != internalServiceAuthRolloutMigration || dataMode != internalServiceAuthRolloutMigration {
		return internalServiceAuthRolloutMigration, true, nil
	}

	metadataReplicas := effectiveMetadataNodeReplicas(cluster)
	dataReplicas := effectiveDataNodeReplicas(cluster)
	if cluster.Spec.DataNodes.Suspend {
		dataReplicas = 0
	}
	if !statefulSetRolloutComplete(metadataStatefulSet, metadataReplicas) || !statefulSetRolloutComplete(dataStatefulSet, dataReplicas) {
		return internalServiceAuthRolloutMigration, true, nil
	}

	additionalVerifier := cluster.Spec.InternalServiceAuth != nil && cluster.Spec.InternalServiceAuth.NextSecretKeyRef != nil
	if !r.everyInternalServiceRuntimeAcknowledges(ctx, cluster, internalServiceAuthRolloutMigration, additionalVerifier) {
		// An old runtime can be Ready while ignoring the injected environment.
		// Keep migration active until every metadata and data member proves support.
		return internalServiceAuthRolloutMigration, true, nil
	}
	return internalServiceAuthRolloutEnforce, false, nil
}

func statefulSetInternalServiceAuthKeyRollout(statefulSet *appsv1.StatefulSet) (internalServiceAuthKeyRolloutMode, string) {
	if statefulSet == nil {
		return "", ""
	}
	return internalServiceAuthKeyRolloutMode(statefulSet.Spec.Template.Annotations[internalServiceAuthKeyRolloutAnnotation]),
		statefulSet.Spec.Template.Annotations[internalServiceAuthKeyTargetAnnotation]
}

func (r *AntflyClusterReconciler) desiredInternalServiceAuthKeyRollout(ctx context.Context, cluster *antflyv1.AntflyCluster, authMode internalServiceAuthRolloutMode) (internalServiceAuthKeyRolloutMode, bool, antflyv1.InternalServiceAuthRotationPhase, error) {
	if cluster.Spec.InternalServiceAuth == nil || cluster.Spec.InternalServiceAuth.NextSecretKeyRef == nil {
		return internalServiceAuthKeyRolloutSteady, false, "", nil
	}
	target := internalServiceAuthKeyTarget(cluster.Spec.InternalServiceAuth)
	metadata := &appsv1.StatefulSet{}
	metadataErr := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}, metadata)
	if metadataErr != nil && !errors.IsNotFound(metadataErr) {
		return "", false, "", metadataErr
	}
	data := &appsv1.StatefulSet{}
	dataErr := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-data", Namespace: cluster.Namespace}, data)
	if dataErr != nil && !errors.IsNotFound(dataErr) {
		return "", false, "", dataErr
	}
	if errors.IsNotFound(metadataErr) && errors.IsNotFound(dataErr) {
		return internalServiceAuthKeyRolloutSwitch, false, antflyv1.InternalServiceAuthRotationSwitched, nil
	}
	metadataMode, metadataTarget := statefulSetInternalServiceAuthKeyRollout(metadata)
	dataMode, dataTarget := statefulSetInternalServiceAuthKeyRollout(data)
	if (metadataMode == internalServiceAuthKeyRolloutSwitch && metadataTarget == target) ||
		(dataMode == internalServiceAuthKeyRolloutSwitch && dataTarget == target) {
		metadataComplete := metadataErr == nil && statefulSetRolloutComplete(metadata, effectiveMetadataNodeReplicas(cluster))
		dataReplicas := effectiveDataNodeReplicas(cluster)
		if cluster.Spec.DataNodes.Suspend {
			dataReplicas = 0
		}
		dataComplete := (dataReplicas == 0 && errors.IsNotFound(dataErr)) || (dataErr == nil && statefulSetRolloutComplete(data, dataReplicas))
		ready := metadataComplete && dataComplete && r.everyInternalServiceRuntimeAcknowledges(ctx, cluster, authMode, true)
		if ready {
			return internalServiceAuthKeyRolloutSwitch, false, antflyv1.InternalServiceAuthRotationSwitched, nil
		}
		return internalServiceAuthKeyRolloutSwitch, true, antflyv1.InternalServiceAuthRotationSwitching, nil
	}
	metadataPrepared := metadataErr == nil && metadataMode == internalServiceAuthKeyRolloutPrepare && metadataTarget == target &&
		statefulSetRolloutComplete(metadata, effectiveMetadataNodeReplicas(cluster))
	dataReplicas := effectiveDataNodeReplicas(cluster)
	if cluster.Spec.DataNodes.Suspend {
		dataReplicas = 0
	}
	dataPrepared := (dataReplicas == 0 && errors.IsNotFound(dataErr)) || (dataErr == nil && dataMode == internalServiceAuthKeyRolloutPrepare && dataTarget == target && statefulSetRolloutComplete(data, dataReplicas))
	if authMode == internalServiceAuthRolloutEnforce && metadataPrepared && dataPrepared &&
		r.everyInternalServiceRuntimeAcknowledges(ctx, cluster, authMode, true) {
		return internalServiceAuthKeyRolloutSwitch, true, antflyv1.InternalServiceAuthRotationSwitching, nil
	}
	return internalServiceAuthKeyRolloutPrepare, true, antflyv1.InternalServiceAuthRotationPreparing, nil
}

// internalServiceAuthPublicBoundaryReady keeps the externally addressable
// Service drained until every workload normally selected by it is running in
// enforce mode. Migration necessarily accepts unsigned legacy peers on the
// shared listener; withdrawing its endpoints turns that compatibility window
// into an explicit maintenance boundary instead of reopening /internal/v1 to
// the Internet. The headless node Services remain available for the rollout.
func (r *AntflyClusterReconciler) internalServiceAuthPublicBoundaryReady(ctx context.Context, cluster *antflyv1.AntflyCluster) (bool, error) {
	boundaryEstablished := false
	publicService := &corev1.Service{}
	publicKey := types.NamespacedName{Name: cluster.Name + "-public-api", Namespace: cluster.Namespace}
	if err := r.Get(ctx, publicKey, publicService); err == nil {
		boundaryEstablished = publicService.Annotations[internalServiceAuthPublicBoundaryAnnotation] == "enforced"
	} else if !errors.IsNotFound(err) {
		return false, fmt.Errorf("read public Service internal-service boundary: %w", err)
	}

	metadataStatefulSet := &appsv1.StatefulSet{}
	metadataKey := types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}
	if err := r.Get(ctx, metadataKey, metadataStatefulSet); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read metadata StatefulSet for public internal-service boundary: %w", err)
	}
	metadataReplicas := effectiveMetadataNodeReplicas(cluster)
	if statefulSetInternalServiceAuthMode(metadataStatefulSet) != internalServiceAuthRolloutEnforce ||
		(!boundaryEstablished && !statefulSetRolloutComplete(metadataStatefulSet, metadataReplicas)) {
		return false, nil
	}

	dataReplicas := effectiveDataNodeReplicas(cluster)
	if cluster.Spec.DataNodes.Suspend {
		dataReplicas = 0
	}
	dataStatefulSet := &appsv1.StatefulSet{}
	dataKey := types.NamespacedName{Name: cluster.Name + "-data", Namespace: cluster.Namespace}
	if err := r.Get(ctx, dataKey, dataStatefulSet); err != nil {
		if errors.IsNotFound(err) && dataReplicas == 0 {
			return true, nil
		}
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read data StatefulSet for public internal-service boundary: %w", err)
	}
	return statefulSetInternalServiceAuthMode(dataStatefulSet) == internalServiceAuthRolloutEnforce &&
		(boundaryEstablished || statefulSetRolloutComplete(dataStatefulSet, dataReplicas)), nil
}

func (r *AntflyClusterReconciler) internalServiceAuthPublicBoundaryDrained(ctx context.Context, cluster *antflyv1.AntflyCluster) (bool, error) {
	if cluster.Spec.PublicAPI == nil || cluster.Spec.PublicAPI.Enabled == nil || !*cluster.Spec.PublicAPI.Enabled {
		return true, nil
	}
	var slices discoveryv1.EndpointSliceList
	if err := r.haBoundaryReader().List(
		ctx,
		&slices,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: cluster.Name + "-public-api"},
	); err != nil {
		return false, fmt.Errorf("list public API EndpointSlices for internal-service boundary: %w", err)
	}
	for i := range slices.Items {
		for _, endpoint := range slices.Items[i].Endpoints {
			if len(endpoint.Addresses) > 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func (state metadataMembershipCapabilityRolloutState) requeueAfter() time.Duration {
	if state == metadataMembershipCapabilityUnavailable {
		return 0
	}
	return metadataMembershipCapabilityRolloutInterval
}

// reconcileLegacyMetadataRuntimeMembershipStatus rolls only the runtime image
// and its capability marker while exact membership status is unavailable. The
// legacy topology validator has already proved that the StatefulSet replica
// count, retained ordinals, local node IDs, incarnation, leader, and voter
// cardinality agree. Avoiding every other StatefulSet mutation here prevents
// an unverified topology from being recorded or changed before the upgraded
// runtime exposes the exact voter set, joint-consensus state, and learners.
func (r *AntflyClusterReconciler) reconcileLegacyMetadataRuntimeMembershipStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) (metadataMembershipCapabilityRolloutState, error) {
	statefulSet := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, statefulSet); err != nil {
		return metadataMembershipCapabilityRolloutPending, fmt.Errorf("read legacy metadata StatefulSet for runtime upgrade: %w", err)
	}
	if !metav1.IsControlledBy(statefulSet, cluster) {
		return metadataMembershipCapabilityRolloutPending, fmt.Errorf("legacy metadata StatefulSet %s is not controlled by the current AntflyCluster", statefulSet.Name)
	}
	expectedReplicas := effectiveMetadataNodeReplicas(cluster)
	if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != expectedReplicas {
		return metadataMembershipCapabilityRolloutPending, fmt.Errorf("legacy metadata StatefulSet changed during runtime upgrade: replicas must remain %d", expectedReplicas)
	}

	before := statefulSet.DeepCopy()
	for i := range statefulSet.Spec.Template.Spec.Containers {
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		if container.Name != "antfly" {
			continue
		}
		changed := container.Image != cluster.Spec.Image
		container.Image = cluster.Spec.Image
		if cluster.Spec.ImagePullPolicy != "" && container.ImagePullPolicy != corev1.PullPolicy(cluster.Spec.ImagePullPolicy) {
			container.ImagePullPolicy = corev1.PullPolicy(cluster.Spec.ImagePullPolicy)
			changed = true
		}
		if statefulSet.Spec.Template.Annotations == nil {
			statefulSet.Spec.Template.Annotations = make(map[string]string)
		}
		if statefulSet.Spec.Template.Annotations[metadataMembershipStatusCapabilityAnnotation] != metadataMembershipStatusCapabilityVersion {
			statefulSet.Spec.Template.Annotations[metadataMembershipStatusCapabilityAnnotation] = metadataMembershipStatusCapabilityVersion
			changed = true
		}
		if !changed {
			if metadataMembershipCapabilityRolloutComplete(statefulSet, expectedReplicas) {
				return metadataMembershipCapabilityUnavailable, nil
			}
			return metadataMembershipCapabilityRolloutPending, nil
		}
		if err := r.Patch(ctx, statefulSet, client.MergeFrom(before)); err != nil {
			return metadataMembershipCapabilityRolloutPending, fmt.Errorf("roll metadata runtime for exact membership status: %w", err)
		}
		return metadataMembershipCapabilityRolloutStarted, nil
	}
	return metadataMembershipCapabilityRolloutPending, fmt.Errorf("legacy metadata StatefulSet %s has no antfly container", statefulSet.Name)
}

// calculateBackoff calculates exponential backoff duration for validation failures (T027)
// Schedule: 1s, 2s, 4s, 8s, 16s, 32s, 60s (max)
func calculateBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// Cap to avoid int overflow (1<<63 wraps negative).
	if attempt > 6 {
		return 60 * time.Second
	}
	delay := time.Duration(1<<attempt) * time.Second
	if delay > 60*time.Second {
		return 60 * time.Second
	}
	return delay
}

func (r *AntflyClusterReconciler) getValidationAttempts(key string) int {
	if val, ok := r.validationAttempts.Load(key); ok {
		return val.(int)
	}
	return 0
}

func (r *AntflyClusterReconciler) incrementValidationAttempts(key string) int {
	count := r.getValidationAttempts(key) + 1
	r.validationAttempts.Store(key, count)
	return count
}

func (r *AntflyClusterReconciler) resetValidationAttempts(key string) {
	r.validationAttempts.Delete(key)
}

// updateStatusWithValidationError updates the cluster status with validation error (T026).
// Skips the API call if the complete failure status already reflects the error.
func (r *AntflyClusterReconciler) updateStatusWithValidationError(ctx context.Context, cluster *antflyv1.AntflyCluster, validationErr error) error {
	log := log.FromContext(ctx)

	errMsg := validationErr.Error()
	before := cluster.Status.DeepCopy()

	condition := metav1.Condition{
		Type:               antflyv1.TypeConfigurationValid,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             antflyv1.ReasonValidationFailed,
		Message:            errMsg,
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, condition)

	var topologyErr *metadataTopologyValidationError
	if stderrors.As(validationErr, &topologyErr) {
		cluster.Status.Phase = "Degraded"
		for _, conditionType := range []string{antflyv1.TypeMetadataReady, antflyv1.TypeAvailable} {
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
				Type:               conditionType,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: cluster.Generation,
				Reason:             antflyv1.ReasonValidationFailed,
				Message:            errMsg,
			})
		}
	}

	if reflect.DeepEqual(before, &cluster.Status) {
		return nil
	}

	if err := r.Status().Update(ctx, cluster); err != nil {
		log.Error(err, "Failed to update status with validation error")
		return err
	}

	if r.Recorder != nil {
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, antflyv1.ReasonValidationFailed, antflyv1.ReasonValidationFailed, "%s", errMsg)
	}

	return nil
}

// updateStatusWithValidationSuccess updates the cluster status with successful validation (T026).
// Skips the API call if the condition is already True and ObservedGeneration is current.
func (r *AntflyClusterReconciler) updateStatusWithValidationSuccess(ctx context.Context, cluster *antflyv1.AntflyCluster, metadataTopologyReplicas int32) error {
	log := log.FromContext(ctx)
	topologyRecordChanged := metadataTopologyReplicas > 0 && cluster.Status.MetadataTopologyReplicas != metadataTopologyReplicas
	if topologyRecordChanged {
		cluster.Status.MetadataTopologyReplicas = metadataTopologyReplicas
	}

	// Skip update if already valid for this generation
	if !topologyRecordChanged && cluster.Status.ObservedGeneration == cluster.Generation {
		for _, existing := range cluster.Status.Conditions {
			if existing.Type == antflyv1.TypeConfigurationValid &&
				existing.Status == metav1.ConditionTrue {
				return nil
			}
		}
	}

	condition := metav1.Condition{
		Type:               antflyv1.TypeConfigurationValid,
		Status:             metav1.ConditionTrue,
		Reason:             antflyv1.ReasonValidationPassed,
		Message:            "All validation rules passed",
		LastTransitionTime: metav1.Now(),
	}

	found := false
	for i, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeConfigurationValid {
			if existing.Status != metav1.ConditionTrue {
				cluster.Status.Conditions[i] = condition
			}
			found = true
			break
		}
	}
	if !found {
		cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
	}

	cluster.Status.ObservedGeneration = cluster.Generation

	if err := r.Status().Update(ctx, cluster); err != nil {
		log.Error(err, "Failed to update status with validation success")
		return err
	}

	return nil
}

// hasSidecarInjected checks if a pod has sidecar containers injected
// by comparing the actual container count to the expected container count
func (r *AntflyClusterReconciler) hasSidecarInjected(pod *corev1.Pod, expectedContainers int) bool {
	return len(pod.Status.ContainerStatuses) > expectedContainers
}

// detectSidecarInjectionStatus scans all pods for a cluster and counts how many have sidecars
func (r *AntflyClusterReconciler) detectSidecarInjectionStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) (int32, int32, error) {
	expectedContainers := 1 // Each pod should have 1 application container (antfly)

	// List all pods for this cluster
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/name":     "antfly-database",
		"app.kubernetes.io/instance": cluster.Name,
	}); err != nil {
		return 0, 0, fmt.Errorf("failed to list pods: %w", err)
	}

	var totalPods int32
	var podsWithSidecars int32

	for _, pod := range podList.Items {
		// Only count running or pending pods (ignore terminated/failed)
		if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
			continue
		}

		totalPods++
		if r.hasSidecarInjected(&pod, expectedContainers) {
			podsWithSidecars++
		}
	}

	return podsWithSidecars, totalPods, nil
}

// envFromCache caches configmap fetches for a single reconcile cycle,
// avoiding duplicate API calls when envFrom hashing references the same resources.
type envFromCache struct {
	client     client.Reader
	configMaps map[types.NamespacedName]*corev1.ConfigMap
	// notFound tracks keys that returned NotFound so we don't retry them.
	notFound map[types.NamespacedName]bool
}

func newEnvFromCache(c client.Reader) *envFromCache {
	return &envFromCache{
		client:     c,
		configMaps: make(map[types.NamespacedName]*corev1.ConfigMap),
		notFound:   make(map[types.NamespacedName]bool),
	}
}

func (c *envFromCache) getConfigMap(ctx context.Context, key types.NamespacedName) (*corev1.ConfigMap, error) {
	if cm, ok := c.configMaps[key]; ok {
		return cm, nil
	}
	if c.notFound[key] {
		return nil, errors.NewNotFound(corev1.Resource("configmaps"), key.Name)
	}
	cm := &corev1.ConfigMap{}
	if err := c.client.Get(ctx, key, cm); err != nil {
		if errors.IsNotFound(err) {
			c.notFound[key] = true
		}
		return nil, err
	}
	c.configMaps[key] = cm
	return cm, nil
}

// computeEnvFromHash computes a hash of envFrom references plus referenced ConfigMap data.
// Secret data is intentionally not read by the operator, so this ClusterRole does not need
// the dangerous secrets permission flagged by Snyk. Secret reference names are still hashed
// so changing which Secret is referenced rolls pods; rotating Secret contents should be
// handled by the workload or by updating an annotation/spec field.
func (r *AntflyClusterReconciler) computeEnvFromHash(ctx context.Context, cache *envFromCache, namespace string, envFrom []corev1.EnvFromSource) string {
	if len(envFrom) == 0 {
		return ""
	}

	h := sha256.New()

	for _, source := range envFrom {
		if source.SecretRef != nil {
			h.Write([]byte("secret:"))
			h.Write([]byte(namespace))
			h.Write([]byte("/"))
			h.Write([]byte(source.SecretRef.Name))
		}
		if source.ConfigMapRef != nil {
			key := types.NamespacedName{Name: source.ConfigMapRef.Name, Namespace: namespace}
			configMap, err := cache.getConfigMap(ctx, key)
			if err == nil {
				// Sort keys for deterministic hash
				keys := make([]string, 0, len(configMap.Data))
				for k := range configMap.Data {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					h.Write([]byte(k))
					h.Write([]byte(configMap.Data[k]))
				}
			}
		}
	}

	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// markEnvFromSecretsUnchecked records that Secret references are intentionally not
// read by the operator. Kubernetes will validate envFrom Secret existence when it
// creates pods, and avoiding direct Secret reads keeps the operator ClusterRole from
// carrying dangerous Secret permissions.
func (r *AntflyClusterReconciler) markEnvFromSecretsUnchecked(cluster *antflyv1.AntflyCluster) {
	r.setSecretsReadyCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonAllSecretsFound, "Secret references are delegated to Kubernetes and are not read by the operator")
}

// setSecretsReadyCondition updates the SecretsReady condition on the cluster status
func (r *AntflyClusterReconciler) setSecretsReadyCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               antflyv1.TypeSecretsReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	// Find and update or append the condition
	found := false
	for i, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeSecretsReady {
			// Only update if status changed
			if existing.Status != status || existing.Reason != reason {
				cluster.Status.Conditions[i] = condition
			}
			found = true
			break
		}
	}
	if !found {
		cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
	}
}

func (r *AntflyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling AntflyCluster", "name", req.Name, "namespace", req.Namespace)

	// Fetch the AntflyCluster instance
	var antflyCluster antflyv1.AntflyCluster
	if err := r.Get(ctx, req.NamespacedName, &antflyCluster); err != nil {
		if errors.IsNotFound(err) {
			r.validationAttempts.Delete(req.String())
			r.metadataTopologyObservations.Delete(req.String())
			log.Info("AntflyCluster resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get AntflyCluster")
		return ctrl.Result{}, err
	}

	// Handle deletion: if the cluster is being deleted and has our finalizer, clean up storage
	if !antflyCluster.DeletionTimestamp.IsZero() {
		r.validationAttempts.Delete(req.String())
		r.metadataTopologyObservations.Delete(req.String())
		if controllerutil.ContainsFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup) {
			// Only run PVC cleanup if the policy still requests deletion.
			// The finalizer is kept even when the policy changes back to Retain
			// (to avoid a race), so we check the current policy here.
			stillWantsDelete := antflyCluster.Spec.Storage.PVCRetentionPolicy != nil &&
				antflyCluster.Spec.Storage.PVCRetentionPolicy.WhenDeleted == antflyv1.PVCRetentionDelete
			if stillWantsDelete {
				result, err := r.cleanupStorageResources(ctx, &antflyCluster)
				if err != nil {
					return ctrl.Result{}, err
				}
				if result != nil {
					return *result, nil
				}
			}
			// Cleanup complete (or skipped) — remove the finalizer
			controllerutil.RemoveFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup)
			if err := r.Update(ctx, &antflyCluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Colony's deprovisioner persists an immutable, digest-bound request on the
	// current primary before Namespace deletion. Reconcile it ahead of ordinary
	// cluster validation so cleanup cannot deadlock behind unrelated runtime
	// readiness or a transient topology handoff.
	if result, handled, err := r.reconcileHASeedPrefixCleanup(ctx, &antflyCluster); handled {
		return result, err
	}

	// Ensure finalizer is present when WhenDeleted=Delete.
	// The finalizer is only removed inside the deletion handler (above) after
	// cleanup completes. Removing it here on policy change would race with
	// concurrent kubectl delete, defeating the cleanup guarantee.
	wantsDelete := antflyCluster.Spec.Storage.PVCRetentionPolicy != nil &&
		antflyCluster.Spec.Storage.PVCRetentionPolicy.WhenDeleted == antflyv1.PVCRetentionDelete
	if wantsDelete && !controllerutil.ContainsFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup) {
		controllerutil.AddFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup)
		if err := r.Update(ctx, &antflyCluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	// Apply defaults to a working copy, not the original
	// this avoids an error from our caller `reconcileHandler` because of a version missmatch.
	workingCluster := antflyCluster.DeepCopy()
	workingCluster.NormalizeLegacySwarm()
	r.applyDefaults(workingCluster) // Use workingCluster for all processing, keep original cluster for status updates
	topologyMode := effectiveTopologyMode(workingCluster)
	standaloneMode := topologyMode == topologyModeStandalone
	if err := r.ensureTopologyResourcesMatchMode(ctx, workingCluster, topologyMode); err != nil {
		return ctrl.Result{}, err
	}

	// Per-reconcile cache for ConfigMap lookups used by buildPodAnnotations.
	efCache := newEnvFromCache(r.Client)

	// Validate cluster configuration (T026)
	// Generation guard: skip validation if spec hasn't changed since last
	// successful validation (ObservedGeneration matches current Generation
	// and the ConfigurationValid condition is already True).
	needsValidation := antflyCluster.Status.ObservedGeneration != antflyCluster.Generation
	if !needsValidation {
		for _, c := range antflyCluster.Status.Conditions {
			if c.Type == antflyv1.TypeConfigurationValid && c.Status != metav1.ConditionTrue {
				needsValidation = true
				break
			}
		}
	}
	// A pre-authentication HA object may have been valid under an older
	// operator. Revalidate it on every upgrade reconcile before touching its
	// StatefulSet so a new fail-closed runtime image cannot roll out without a
	// bearer-token source and silently stop replication/admin control.
	if !needsValidation && haRuntimeNeedsAdminTokenMigration(&antflyCluster) {
		needsValidation = true
	}
	// Existing distributed clusters may have a successful validation checkpoint
	// from an operator version that predated internal-service authentication.
	// Revalidate before touching either StatefulSet so the new fail-closed image
	// cannot roll until the deployment controller has provisioned and referenced
	// the dedicated credential.
	if !needsValidation && distributedRuntimeNeedsInternalServiceAuthMigration(&antflyCluster) {
		needsValidation = true
	}
	// Seed the durable metadata topology record for clusters created by older
	// operator versions before relying on it for future update validation.
	if !needsValidation && !standaloneMode && antflyCluster.Status.MetadataTopologyReplicas == 0 {
		needsValidation = true
	}

	clusterKey := req.String()

	if needsValidation {
		if err := r.validateClusterConfiguration(ctx, workingCluster); err != nil {
			var topologyPending *metadataTopologyObservationPendingError
			if stderrors.As(err, &topologyPending) {
				r.resetValidationAttempts(clusterKey)
				return ctrl.Result{RequeueAfter: topologyPending.retryAfter}, nil
			}
			log.Error(err, "Cluster configuration validation failed")
			if statusErr := r.updateStatusWithValidationError(ctx, &antflyCluster, err); statusErr != nil {
				log.Error(statusErr, "Failed to update status with validation error")
			}
			if stderrors.Is(err, errMetadataRuntimeMembershipStatusUnavailable) {
				r.resetValidationAttempts(clusterKey)
				rolloutState, rolloutErr := r.reconcileLegacyMetadataRuntimeMembershipStatus(ctx, workingCluster)
				if rolloutErr != nil {
					return ctrl.Result{}, rolloutErr
				}
				if rolloutState == metadataMembershipCapabilityRolloutStarted {
					log.Info("Rolling legacy metadata pods to expose exact Raft membership status", "image", workingCluster.Spec.Image)
				}
				if requeueAfter := rolloutState.requeueAfter(); requeueAfter > 0 {
					return ctrl.Result{RequeueAfter: requeueAfter}, nil
				}
				log.Error(err, "Metadata runtime image lacks exact Raft membership status after its rollout completed; waiting for spec.image to change", "image", workingCluster.Spec.Image)
				return ctrl.Result{}, nil
			}
			attempt := r.incrementValidationAttempts(clusterKey)
			backoff := calculateBackoff(attempt - 1)
			return ctrl.Result{RequeueAfter: backoff}, nil
		}

		r.resetValidationAttempts(clusterKey)
		metadataTopologyReplicas := int32(0)
		if !standaloneMode {
			metadataTopologyReplicas = effectiveMetadataNodeReplicas(workingCluster)
		}
		if err := r.updateStatusWithValidationSuccess(ctx, &antflyCluster, metadataTopologyReplicas); err != nil {
			log.Error(err, "Failed to update status with validation success")
			// The status checkpoint is the first durable topology record for a new
			// cluster. Continuing after a conflict could reconcile an older spec and
			// permanently pin that stale replica count into the StatefulSet/PVC
			// annotations, so re-read the cluster before touching resources.
			return ctrl.Result{}, fmt.Errorf("persist validation checkpoint before reconciling resources: %w", err)
		} else {
			// Continue with the resource version and durable status record returned
			// by the status update instead of overwriting it later in reconciliation.
			workingCluster.ResourceVersion = antflyCluster.ResourceVersion
			workingCluster.Status = antflyCluster.Status
		}
	}

	// Do not read Secrets from the operator. Kubernetes validates referenced Secrets
	// during pod admission/startup, which avoids granting dangerous Secret RBAC here.
	r.markEnvFromSecretsUnchecked(workingCluster)

	if err := r.reconcileInferencePool(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	if standaloneMode {
		// Standalone mode is a single topology and does not support distributed autoscaling.
		if workingCluster.Spec.DataNodes.AutoScaling != nil && workingCluster.Spec.DataNodes.AutoScaling.Enabled {
			log.Info("Ignoring data node autoscaling because standalone mode is enabled")
		}

		if err := r.reconcileConfigMap(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileServices(ctx, workingCluster, false); err != nil {
			return ctrl.Result{}, err
		}
		component := standaloneComponent(workingCluster)
		storageVolume := standaloneStorageVolumeName(workingCluster)
		statefulSetName := standaloneStatefulSetName(workingCluster)
		workingCluster.Spec.Storage.StandaloneStorage = r.reconcileStorageAutoGrow(ctx, workingCluster, component, storageVolume, statefulSetName, chooseStandaloneStorageSize(workingCluster), maxStandaloneAutoGrowSize(workingCluster))
		if err := r.reconcileStandaloneStatefulSet(ctx, efCache, workingCluster); err != nil {
			return ctrl.Result{}, err
		}
		if repaired, err := r.repairBlockedStatefulSetRollouts(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		} else if repaired {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		r.setPVCExpansionCondition(workingCluster, []pvcExpansionResult{
			r.reconcilePVCExpansion(ctx, workingCluster, component, storageVolume, statefulSetName, chooseStandaloneStorageSize(workingCluster)),
		})

		if err := r.reconcilePodDisruptionBudget(ctx, workingCluster, statefulSetName+"-pdb", component); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.reconcileServiceMeshStatus(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		}

		r.checkPVCTopologyHealth(ctx, workingCluster)

		if err := r.updateStatus(ctx, workingCluster); err != nil {
			if stderrors.Is(err, errHAStatusCheckpointed) {
				return ctrl.Result{RequeueAfter: haStatusCheckpointRequeueAfter}, nil
			}
			return ctrl.Result{}, err
		}

		if requeueAfter := periodicRequeueAfter(workingCluster); requeueAfter > 0 {
			return ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
		return ctrl.Result{}, nil
	}

	// Create ConfigMap for Antfly configuration
	if err := r.reconcileConfigMap(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	internalServiceAuthMode, internalServiceAuthRolloutPending, err := r.desiredInternalServiceAuthRolloutMode(ctx, workingCluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	internalServiceAuthKeyMode, internalServiceAuthKeyRolloutPending, rotationPhase, err := r.desiredInternalServiceAuthKeyRollout(ctx, workingCluster, internalServiceAuthMode)
	if err != nil {
		return ctrl.Result{}, err
	}
	if workingCluster.Spec.InternalServiceAuth != nil && workingCluster.Spec.InternalServiceAuth.NextSecretKeyRef != nil {
		workingCluster.Status.InternalServiceAuthRotation = &antflyv1.InternalServiceAuthRotationStatus{
			Phase:            rotationPhase,
			TargetSecretName: workingCluster.Spec.InternalServiceAuth.NextSecretKeyRef.Name,
			TargetSecretKey:  workingCluster.Spec.InternalServiceAuth.NextSecretKeyRef.Key,
		}
	} else {
		workingCluster.Status.InternalServiceAuthRotation = nil
	}
	publicBoundaryReady, err := r.internalServiceAuthPublicBoundaryReady(ctx, workingCluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	// The internal headless Services stay present throughout the two-phase
	// rollout, while the optional externally addressable Service is restored
	// only after every selected pod proves enforcement.
	if err := r.reconcileServices(ctx, workingCluster, !publicBoundaryReady); err != nil {
		return ctrl.Result{}, err
	}
	if !publicBoundaryReady {
		drained, err := r.internalServiceAuthPublicBoundaryDrained(ctx, workingCluster)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !drained {
			log.Info("Waiting for public API endpoints to drain before starting internal-service migration")
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}
	if internalServiceAuthRolloutPending || internalServiceAuthKeyRolloutPending {
		log.Info("Internal-service authentication rollout is in progress", "mode", internalServiceAuthMode, "keyMode", internalServiceAuthKeyMode)
	}

	// Create Metadata StatefulSet
	if err := r.reconcileMetadataStatefulSet(ctx, efCache, workingCluster, internalServiceAuthMode, internalServiceAuthKeyMode); err != nil {
		return ctrl.Result{}, err
	}

	// Decide the data-node replica target before creating/updating the StatefulSet.
	// In manual mode, spec.dataNodes.replicas is authoritative. In autoscaling
	// mode, the operator autoscaler is authoritative and computes from observed
	// StatefulSet replicas, not the manual-mode spec field.
	dataSts := &appsv1.StatefulSet{}
	dataStsKey := types.NamespacedName{Name: workingCluster.Name + "-data", Namespace: workingCluster.Namespace}
	dataStsExists := true
	if err := r.Get(ctx, dataStsKey, dataSts); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		dataStsExists = false
	}

	currentDataReplicas := effectiveDataReplicas(dataSts, dataStsExists, effectiveDataNodeReplicas(workingCluster))
	currentDataTarget := effectiveDataReplicaTarget(dataSts, dataStsExists, effectiveDataNodeReplicas(workingCluster))
	scaleSafetyReplicas := max(currentDataReplicas, currentDataTarget)
	desiredDataReplicas := effectiveDataNodeReplicas(workingCluster)
	requestedDataReplicas := desiredDataReplicas
	dataScaleDownSource := antflyv1.DataScaleDownSourceManual
	dataScaleDownRequested := false
	autoscalingEnabled := r.AutoScaler != nil && workingCluster.Spec.DataNodes.AutoScaling != nil && workingCluster.Spec.DataNodes.AutoScaling.Enabled
	dataNodesSuspended := workingCluster.Spec.DataNodes.Suspend
	if dataNodesSuspended {
		workingCluster.Status.AutoScalingStatus = nil
		requestedDataReplicas = 0
		desiredDataReplicas = 0
		workingCluster.Spec.DataNodes.Replicas = 0
		r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Data nodes are suspended with PVCs retained")
	} else if autoscalingEnabled {
		dataScaleDownSource = antflyv1.DataScaleDownSourceAutoscaler
		recommendationReplicas, err := r.AutoScaler.EvaluateScaling(ctx, workingCluster, currentDataTarget)
		if err != nil {
			log.Error(err, "Failed to evaluate autoscaling")
			recommendationReplicas = currentDataTarget
		}

		requestedDataReplicas = recommendationReplicas
		desiredDataReplicas = recommendationReplicas
		blockedReason := ""
		blockedMessage := ""
		if recommendationReplicas < scaleSafetyReplicas {
			desiredDataReplicas = scaleSafetyReplicas
			dataScaleDownRequested = true
			blockedReason = antflyv1.ReasonDataScaleDownInProgress
			blockedMessage = fmt.Sprintf("Autoscaler recommended data-node scale-down from %d to %d; waiting for runtime drain before removing the next ordinal", scaleSafetyReplicas, recommendationReplicas)
		} else {
			r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		}

		workingCluster.Spec.DataNodes.Replicas = desiredDataReplicas
		r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, desiredDataReplicas, recommendationReplicas, blockedReason, blockedMessage)
		if recommendationReplicas != currentDataReplicas {
			log.Info("Autoscaling data nodes", "currentReplicas", currentDataReplicas, "desiredReplicas", desiredDataReplicas, "recommendationReplicas", recommendationReplicas, "blockedReason", blockedReason)
		}
	} else {
		workingCluster.Status.AutoScalingStatus = nil
		if desiredDataReplicas < scaleSafetyReplicas {
			message := fmt.Sprintf("Data-node scale-down from %d to %d requested; waiting for runtime drain before removing the next ordinal", scaleSafetyReplicas, desiredDataReplicas)
			requestedDataReplicas = desiredDataReplicas
			desiredDataReplicas = scaleSafetyReplicas
			dataScaleDownRequested = true
			workingCluster.Spec.DataNodes.Replicas = desiredDataReplicas
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
		} else {
			r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		}
	}

	if dataStsExists && shouldFinalizeDataScaleDown(workingCluster.Status.DataScaleDownStatus, currentDataReplicas, desiredDataReplicas, dataScaleDownRequested) {
		scalingStatus := workingCluster.Status.DataScaleDownStatus
		if currentDataReplicas > scalingStatus.AppliedReplicas {
			message := fmt.Sprintf("Waiting for StatefulSet to remove data ordinal %d/node %s; current replicas=%d target replicas=%d", scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, currentDataReplicas, scalingStatus.AppliedReplicas)
			r.setDataScaleDownStatus(workingCluster, scalingStatus.Source, scalingStatus.FromReplicas, scalingStatus.TargetReplicas, scalingStatus.AppliedReplicas, scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, "Scaling", message)
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status while waiting for data StatefulSet scale-down")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		if err := r.finalizeDataNodeShutdown(ctx, workingCluster, scalingStatus.DrainingNodeID); err != nil {
			failureMessage := fmt.Sprintf("Failed to finalize data-node shutdown for ordinal %d/node %s after StatefulSet scale-down: %v", scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, err)
			r.setDataScaleDownStatus(workingCluster, scalingStatus.Source, scalingStatus.FromReplicas, scalingStatus.TargetReplicas, scalingStatus.AppliedReplicas, scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, "Failed", failureMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownFailed, failureMessage)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down finalization failure")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		message := fmt.Sprintf("Finalized data-node shutdown for ordinal %d/node %s", scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID)
		r.setDataScaleDownStatus(workingCluster, scalingStatus.Source, scalingStatus.FromReplicas, scalingStatus.TargetReplicas, scalingStatus.AppliedReplicas, scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, "Complete", message)
		if requestedDataReplicas < scaleSafetyReplicas {
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, "Continuing data-node scale-down after finalizing the previous ordinal")
		} else {
			r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		}
		if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
			log.Error(statusErr, "Failed to update status after data scale-down finalization")
		}
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	if dataStsExists && !dataScaleDownRequested && (shouldCancelDataScaleDown(workingCluster.Status.DataScaleDownStatus, currentDataReplicas, desiredDataReplicas) || (dataNodesSuspended && shouldCancelDataScaleDownForSuspend(workingCluster.Status.DataScaleDownStatus, currentDataReplicas))) {
		drainingStatus := workingCluster.Status.DataScaleDownStatus
		status, err := r.cancelDataNodeShutdown(ctx, workingCluster, drainingStatus.DrainingNodeID)
		if err != nil {
			failureMessage := fmt.Sprintf("Failed to cancel data-node shutdown for ordinal %d/node %s: %v", drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, err)
			r.setDataScaleDownStatus(workingCluster, drainingStatus.Source, drainingStatus.FromReplicas, desiredDataReplicas, scaleSafetyReplicas, drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, "Failed", failureMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownFailed, failureMessage)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down cancellation failure")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if status.Phase != "active" && status.Phase != "not_found" {
			message := fmt.Sprintf("Canceling data-node shutdown for ordinal %d/node %s; runtime phase is %q", drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, status.Phase)
			r.setDataScaleDownStatus(workingCluster, drainingStatus.Source, drainingStatus.FromReplicas, desiredDataReplicas, scaleSafetyReplicas, drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, "Canceling", message)
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down cancellation progress")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		message := fmt.Sprintf("Canceled data-node shutdown for ordinal %d/node %s; runtime phase is %q", drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, status.Phase)
		r.setDataScaleDownStatus(workingCluster, drainingStatus.Source, drainingStatus.FromReplicas, desiredDataReplicas, scaleSafetyReplicas, drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, "Canceled", message)
		r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		if r.AutoScaler != nil && workingCluster.Status.AutoScalingStatus != nil {
			r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, desiredDataReplicas, requestedDataReplicas, "", "")
		}
		if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
			log.Error(statusErr, "Failed to update status with data scale-down cancellation")
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if dataNodesSuspended {
		workingCluster.Status.DataScaleDownStatus = nil
	} else if dataStsExists && dataScaleDownRequested {
		drainingOrdinal := scaleSafetyReplicas - 1
		drainingNodeID := nodeIDForDataOrdinal(drainingOrdinal)
		nextReplicas := scaleSafetyReplicas - 1
		message := fmt.Sprintf("Draining data ordinal %d/node %s before scaling StatefulSet from %d to %d", drainingOrdinal, drainingNodeID, scaleSafetyReplicas, nextReplicas)
		r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Draining", message)
		r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
		shutdownStatus, err := r.requestDataNodeShutdown(ctx, workingCluster, drainingNodeID)
		if err != nil {
			failureMessage := fmt.Sprintf("Failed to drain data ordinal %d/node %s: %v", drainingOrdinal, drainingNodeID, err)
			r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Failed", failureMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownFailed, failureMessage)
			if statusErr := r.Status().Update(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down failure")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if shutdownStatus.Blocked || shutdownStatus.Phase == "blocked" {
			blockedMessage := shutdownStatus.Message
			if blockedMessage == "" {
				blockedMessage = fmt.Sprintf("Data ordinal %d/node %s cannot be drained safely; runtime reports phase %q", drainingOrdinal, drainingNodeID, shutdownStatus.Phase)
			}
			r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Blocked", blockedMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownBlocked, blockedMessage)
			if r.AutoScaler != nil && workingCluster.Status.AutoScalingStatus != nil {
				r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, scaleSafetyReplicas, requestedDataReplicas, antflyv1.ReasonDataScaleDownBlocked, blockedMessage)
			}
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with blocked data scale-down")
			}
			return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
		}
		if !shutdownStatus.SafeToTerminate {
			message = fmt.Sprintf("Data ordinal %d/node %s is draining in runtime phase %q; StatefulSet remains at %d replicas", drainingOrdinal, drainingNodeID, shutdownStatus.Phase, scaleSafetyReplicas)
			r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Draining", message)
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down drain progress")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		desiredDataReplicas = nextReplicas
		workingCluster.Spec.DataNodes.Replicas = desiredDataReplicas
		message = fmt.Sprintf("Runtime reports data ordinal %d/node %s is safe to terminate; scaling StatefulSet from %d to %d", drainingOrdinal, drainingNodeID, scaleSafetyReplicas, desiredDataReplicas)
		r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, desiredDataReplicas, drainingOrdinal, drainingNodeID, "Scaling", message)
		r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
		if r.AutoScaler != nil && workingCluster.Status.AutoScalingStatus != nil {
			r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, desiredDataReplicas, requestedDataReplicas, antflyv1.ReasonDataScaleDownInProgress, message)
		}
	} else if dataStsExists {
		if workingCluster.Status.DataScaleDownStatus != nil {
			switch workingCluster.Status.DataScaleDownStatus.Phase {
			case "Complete", "Canceled":
				completedSource := workingCluster.Status.DataScaleDownStatus.Source
				if completedSource == "" {
					completedSource = antflyv1.DataScaleDownSourceManual
				}
				r.setDataScaleDownStatus(workingCluster, completedSource, scaleSafetyReplicas, scaleSafetyReplicas, desiredDataReplicas, 0, "", "Complete", "Data-node scale-down is complete")
			}
		}
	}

	workingCluster.Spec.Storage.DataStorage = r.reconcileStorageAutoGrow(ctx, workingCluster, "data", "data-storage", workingCluster.Name+"-data", effectiveDataStorageSize(workingCluster), maxDataAutoGrowSize(workingCluster))

	// Create Data StatefulSet
	if err := r.reconcileDataStatefulSet(ctx, efCache, workingCluster, internalServiceAuthMode, internalServiceAuthKeyMode); err != nil {
		return ctrl.Result{}, err
	}

	if repaired, err := r.repairBlockedStatefulSetRollouts(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	} else if repaired {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Reconcile PVC expansion (metadata and data)
	r.setPVCExpansionCondition(workingCluster, []pvcExpansionResult{
		r.reconcilePVCExpansion(ctx, workingCluster, "metadata", "metadata-storage", workingCluster.Name+"-metadata", workingCluster.Spec.Storage.MetadataStorage),
		r.reconcilePVCExpansion(ctx, workingCluster, "data", "data-storage", workingCluster.Name+"-data", workingCluster.Spec.Storage.DataStorage),
	})

	// Reconcile PodDisruptionBudgets for GKE
	if err := r.reconcilePodDisruptionBudget(ctx, workingCluster, workingCluster.Name+"-metadata-pdb", "metadata"); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePodDisruptionBudget(ctx, workingCluster, workingCluster.Name+"-data-pdb", "data"); err != nil {
		return ctrl.Result{}, err
	}

	// Detect service mesh sidecar injection status
	if err := r.reconcileServiceMeshStatus(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Check PVC/AZ topology health and set StorageHealthy condition
	r.checkPVCTopologyHealth(ctx, workingCluster)

	// Update status
	if err := r.updateStatusIfChanged(ctx, workingCluster, &antflyCluster.Status); err != nil {
		if stderrors.Is(err, errHAStatusCheckpointed) {
			return ctrl.Result{RequeueAfter: haStatusCheckpointRequeueAfter}, nil
		}
		return ctrl.Result{}, err
	}

	if requeueAfter := minPositiveDuration(
		minPositiveDuration(periodicRequeueAfter(workingCluster), r.metadataTopologyObservationRequeueAfter(workingCluster)),
		func() time.Duration {
			if internalServiceAuthRolloutPending || internalServiceAuthKeyRolloutPending {
				return internalServiceAuthRolloutInterval
			}
			return 0
		}(),
	); requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	return ctrl.Result{}, nil
}

func haRuntimeNeedsAdminTokenMigration(cluster *antflyv1.AntflyCluster) bool {
	if cluster == nil || cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil {
		return false
	}
	ha := cluster.Spec.HighAvailability
	return ha.Mode != "" && ha.Mode != antflyv1.HAModeDisabled && strings.TrimSpace(ha.Runtime.AdminTokenEnvVar) == ""
}

func distributedRuntimeNeedsInternalServiceAuthMigration(cluster *antflyv1.AntflyCluster) bool {
	return cluster != nil && effectiveTopologyMode(cluster) == topologyModeDistributed && cluster.Spec.InternalServiceAuth == nil
}

func periodicRequeueAfter(cluster *antflyv1.AntflyCluster) time.Duration {
	return periodicRequeueAfterAt(cluster, time.Now())
}

func periodicRequeueAfterAt(cluster *antflyv1.AntflyCluster, now time.Time) time.Duration {
	var requeueAfter time.Duration
	// Metadata agreement is runtime health, not just configuration validation.
	// Re-observe it even when the AntflyCluster generation has not changed so a
	// split incarnation cannot remain green indefinitely without another watch
	// event.
	if effectiveTopologyMode(cluster) == topologyModeDistributed && cluster.Status.MetadataTopologyReplicas > 0 {
		requeueAfter = minPositiveDuration(requeueAfter, 30*time.Second)
	}
	// Kubernetes Lease renewal has its own narrow controller and fixed cadence.
	// Putting that clock on the full reconciler turns every renewal into an
	// expensive status/seed observation cycle and can starve the very watchdog
	// proof that keeps the primary authoritative.
	// Runtime status itself is not event-driven: WAL and replication progress,
	// admin reachability, and the automatic-failover detector must still be
	// sampled at a bounded cadence. This clock is intentionally fixed rather
	// than derived from Lease duration or watchdog grace.
	if ha := cluster.Spec.HighAvailability; ha != nil && ha.Mode != "" && ha.Mode != antflyv1.HAModeDisabled {
		requeueAfter = minPositiveDuration(requeueAfter, haRuntimeStatusObservationRequeueAfter)
	}
	// A suspended seeded runtime has no Pod, Job, or Lease event of its own
	// after the primary checkpoints the final receipt. Re-observe peer status
	// until Colony accepts that exact receipt and opens the declarative gate;
	// otherwise a missed cross-CR event can leave a correctly seeded standby
	// suspended forever. This bounded cadence stops as soon as eligibility is
	// declared and avoids enqueueing every HA peer on every status write.
	if gate := haRuntimeStartupGate(cluster); gate != nil &&
		gate.Policy == antflyv1.HAStartupGatePolicyRequireActivatedSeed && !gate.RuntimeEligible {
		requeueAfter = minPositiveDuration(requeueAfter, haStartupGateObservationRequeueAfter)
	}
	if retryAfter := haDirectAdminRetryRequeueAfter(cluster, now); retryAfter > 0 {
		requeueAfter = minPositiveDuration(requeueAfter, retryAfter)
	}
	if cluster.Spec.DataNodes.AutoScaling != nil && cluster.Spec.DataNodes.AutoScaling.Enabled {
		requeueAfter = minPositiveDuration(requeueAfter, 30*time.Second)
	}
	if storageAutoGrowEnabled(cluster) {
		requeueAfter = minPositiveDuration(requeueAfter, 60*time.Second)
	}
	return requeueAfter
}

func haDirectAdminRetryRequeueAfter(cluster *antflyv1.AntflyCluster, now time.Time) time.Duration {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return 0
	}
	var retryAfter time.Duration
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.AdminJobName != haAdminDirectAPIName {
			continue
		}
		var due *metav1.Time
		switch {
		case action.InFlightAttempt > 0:
			due = action.ReservationExpiresAt
		case action.AdminJobPhase == haAdminJobPhaseWaitingPrerequisite:
			due = action.NextRetryAt
			if action.PrerequisiteDeadlineAt != nil && (due == nil || action.PrerequisiteDeadlineAt.Before(due)) {
				due = action.PrerequisiteDeadlineAt
			}
		case action.AdminJobPhase == haAdminJobPhasePending && action.Retryable && strings.TrimSpace(action.AdminError) != "":
			due = action.NextRetryAt
		default:
			continue
		}
		candidate := defaultHADirectAdminRetryBase
		if due != nil {
			candidate = due.Sub(now)
			if candidate <= 0 {
				candidate = time.Millisecond
			}
		}
		retryAfter = minPositiveDuration(retryAfter, candidate)
	}
	return retryAfter
}

func minPositiveDuration(current time.Duration, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

func (r *AntflyClusterReconciler) haNow() time.Time {
	if r != nil && r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func haDirectAdminRetryLimit(admin *antflyv1.HAAdminSpec) int32 {
	if admin != nil && admin.DirectRetryLimit != nil && *admin.DirectRetryLimit > 0 {
		return *admin.DirectRetryLimit
	}
	return defaultHADirectAdminRetryLimit
}

func haDirectAdminRetryBase(admin *antflyv1.HAAdminSpec) time.Duration {
	if admin != nil && admin.DirectRetryBaseSeconds != nil && *admin.DirectRetryBaseSeconds > 0 {
		return time.Duration(*admin.DirectRetryBaseSeconds) * time.Second
	}
	return defaultHADirectAdminRetryBase
}

func haDirectAdminRetryMaximum(admin *antflyv1.HAAdminSpec) time.Duration {
	if admin != nil && admin.DirectRetryMaxSeconds != nil && *admin.DirectRetryMaxSeconds > 0 {
		return time.Duration(*admin.DirectRetryMaxSeconds) * time.Second
	}
	return defaultHADirectAdminRetryMaximum
}

func haDirectAdminReservation(admin *antflyv1.HAAdminSpec) time.Duration {
	if admin != nil && admin.DirectReservationSeconds != nil && *admin.DirectReservationSeconds > 0 {
		return time.Duration(*admin.DirectReservationSeconds) * time.Second
	}
	return defaultHADirectAdminReservation
}

func haDirectPrerequisiteTimeout(admin *antflyv1.HAAdminSpec) time.Duration {
	if admin != nil && admin.DirectPrerequisiteTimeoutSeconds != nil && *admin.DirectPrerequisiteTimeoutSeconds > 0 {
		return time.Duration(*admin.DirectPrerequisiteTimeoutSeconds) * time.Second
	}
	return defaultHADirectPrerequisiteTimeout
}

func haDirectAdminRetryDelay(admin *antflyv1.HAAdminSpec, attempt int32) time.Duration {
	delay := haDirectAdminRetryBase(admin)
	maximum := haDirectAdminRetryMaximum(admin)
	if maximum < delay {
		maximum = delay
	}
	for i := int32(1); i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func haDirectAdminErrorClass(err error) string {
	if stderrors.Is(err, errHAPromotionBoundaryNotApplied) {
		return "PromotionBoundaryNotApplied"
	}
	if statusCode, ok := adminsdk.HAStatusCode(err); ok {
		return fmt.Sprintf("HTTP%d", statusCode)
	}
	if adminsdk.HAIsRetryable(err) {
		return "RetryableAdminError"
	}
	return "PermanentAdminError"
}

func haActionTime(t time.Time) *metav1.Time {
	value := metav1.NewTime(t.UTC())
	return &value
}

func resetHAActionAttemptStatus(action *antflyv1.HAPlannedActionStatus) {
	if action == nil {
		return
	}
	action.AttemptCount = 0
	action.RetryBudgetUsed = 0
	action.ExecutionStateVersion = 0
	action.Retryable = false
	action.ErrorClass = ""
	action.FirstAttemptAt = nil
	action.LastAttemptAt = nil
	action.NextRetryAt = nil
	action.InFlightAttempt = 0
	action.AttemptID = ""
	action.ReservationExpiresAt = nil
	action.PrerequisiteDeadlineAt = nil
	action.CompletedAt = nil
}

func (r *AntflyClusterReconciler) reconcileInferencePool(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	mode := antflyInferenceMode(cluster.Spec.Inference)
	if !r.ManageInferencePools {
		if mode == antflyv1.AntflyInferenceModeManaged {
			r.setInferencePoolReadyCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonInferencePoolManagementDisabled,
				"InferencePool management is disabled by --enable-inference-controllers=false; existing owned pools are left unchanged")
		}
		return nil
	}

	logger := log.FromContext(ctx)

	desired := map[string]struct{}{}
	if mode == antflyv1.AntflyInferenceModeManaged && cluster.Spec.Inference != nil {
		for i, managed := range cluster.Spec.Inference.ManagedPools {
			name := managedInferencePoolName(cluster, managed, len(cluster.Spec.Inference.ManagedPools), i)
			key := types.NamespacedName{Name: name, Namespace: cluster.Namespace}
			desired[name] = struct{}{}
			if err := r.reconcileManagedInferencePool(ctx, cluster, managed, name); err != nil {
				var conflictErr *inferencePoolNameConflictError
				if stderrors.As(err, &conflictErr) {
					logger.Info("Refusing to adopt same-name InferencePool because it is not controlled by this AntflyCluster", "inferencePool", key.String())
					r.setInferencePoolReadyCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonInferencePoolNameConflict, conflictErr.Error())
					return nil
				}
				return err
			}
			if i == len(cluster.Spec.Inference.ManagedPools)-1 {
				r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady,
					fmt.Sprintf("%d managed InferencePool(s) reconciled", len(cluster.Spec.Inference.ManagedPools)))
			}
		}
	}

	if err := r.deleteStaleOwnedInferencePools(ctx, cluster, desired); err != nil {
		return err
	}

	switch mode {
	case antflyv1.AntflyInferenceModeDisabled:
		r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady, "Inference integration is disabled")
	case antflyv1.AntflyInferenceModeSharedRef:
		r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady,
			fmt.Sprintf("%d shared InferencePool reference(s) configured", len(cluster.Spec.Inference.SharedPools)))
	case antflyv1.AntflyInferenceModePlatformShared:
		r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady,
			fmt.Sprintf("%d platform InferencePool reference(s) configured", len(cluster.Spec.Inference.PlatformPools)))
	}
	return nil
}

func antflyInferenceMode(spec *antflyv1.AntflyInferenceSpec) antflyv1.AntflyInferenceMode {
	if spec == nil {
		return antflyv1.AntflyInferenceModeDisabled
	}
	if spec.Mode != "" {
		return spec.Mode
	}
	if len(spec.SharedPools) > 0 {
		return antflyv1.AntflyInferenceModeSharedRef
	}
	if len(spec.PlatformPools) > 0 {
		return antflyv1.AntflyInferenceModePlatformShared
	}
	return antflyv1.AntflyInferenceModeManaged
}

func managedInferencePoolName(cluster *antflyv1.AntflyCluster, managed antflyv1.ManagedInferencePoolSpec, total, index int) string {
	if managed.Name != "" {
		return managed.Name
	}
	if total == 1 {
		return cluster.Name + "-inference"
	}
	return fmt.Sprintf("%s-inference-%d", cluster.Name, index)
}

func (r *AntflyClusterReconciler) configuredInferenceAPIURL(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Inference == nil {
		return ""
	}
	inference := cluster.Spec.Inference
	switch antflyInferenceMode(inference) {
	case antflyv1.AntflyInferenceModeManaged:
		for i, managed := range inference.ManagedPools {
			name := managedInferencePoolName(cluster, managed, len(inference.ManagedPools), i)
			if strings.TrimSpace(name) != "" {
				return fmt.Sprintf("http://%s:%d", serviceDNSName(name, cluster.Namespace, r.ClusterDomain), defaultManagedInferenceAPIPort)
			}
		}
	case antflyv1.AntflyInferenceModeSharedRef:
		return firstInferencePoolReferenceAPIURL(inference.SharedPools)
	case antflyv1.AntflyInferenceModePlatformShared:
		return firstInferencePoolReferenceAPIURL(inference.PlatformPools)
	}
	return ""
}

func firstInferencePoolReferenceAPIURL(refs []antflyv1.InferencePoolReference) string {
	for _, ref := range refs {
		if apiURL := strings.TrimRight(strings.TrimSpace(ref.APIURL), "/"); apiURL != "" {
			return apiURL
		}
	}
	return ""
}

func (r *AntflyClusterReconciler) reconcileManagedInferencePool(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	managed antflyv1.ManagedInferencePoolSpec,
	name string,
) error {
	key := types.NamespacedName{Name: name, Namespace: cluster.Namespace}
	pool := &inferencev1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pool, func() error {
		if pool.UID != "" && !metav1.IsControlledBy(pool, cluster) {
			return &inferencePoolNameConflictError{
				namespacedName: key.String(),
				cluster:        types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}.String(),
			}
		}

		if pool.Labels == nil {
			pool.Labels = map[string]string{}
		}
		pool.Labels["app.kubernetes.io/name"] = "inference"
		pool.Labels["app.kubernetes.io/component"] = "inferencepool"
		pool.Labels["app.kubernetes.io/instance"] = cluster.Name
		pool.Labels["app.kubernetes.io/managed-by"] = "antfly-operator"

		// AntflyCluster.spec.inference.managedPools is authoritative for managed pools.
		// Do not add InferencePool mutating-webhook defaults for spec fields
		// unless the same defaults are applied before this assignment.
		pool.Spec = *managed.Spec.DeepCopy()
		if pool.Spec.Image == "" {
			pool.Spec.Image = r.DefaultInferenceImage
		}
		if err := controllerutil.SetControllerReference(cluster, pool, r.Scheme); err != nil {
			return fmt.Errorf("failed to set owner reference on InferencePool %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile managed InferencePool %s: %w", name, err)
	}
	return nil
}

func (r *AntflyClusterReconciler) deleteStaleOwnedInferencePools(ctx context.Context, cluster *antflyv1.AntflyCluster, desired map[string]struct{}) error {
	defaultName := cluster.Name + "-inference"
	if _, ok := desired[defaultName]; !ok {
		pool := &inferencev1alpha1.InferencePool{}
		key := types.NamespacedName{Name: defaultName, Namespace: cluster.Namespace}
		if err := r.Get(ctx, key, pool); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to get managed InferencePool %s: %w", key.String(), err)
			}
		} else if metav1.IsControlledBy(pool, cluster) {
			if err := client.IgnoreNotFound(r.Delete(ctx, pool)); err != nil {
				return fmt.Errorf("failed to delete stale managed InferencePool %s: %w", key.String(), err)
			}
		}
	}

	var pools inferencev1alpha1.InferencePoolList
	if err := r.List(ctx, &pools, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "antfly-operator",
	}); err != nil {
		return fmt.Errorf("failed to list managed InferencePools for %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}
	for i := range pools.Items {
		pool := &pools.Items[i]
		if !metav1.IsControlledBy(pool, cluster) {
			continue
		}
		if _, ok := desired[pool.Name]; ok {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, pool)); err != nil {
			return fmt.Errorf("failed to delete stale managed InferencePool %s/%s: %w", pool.Namespace, pool.Name, err)
		}
	}
	return nil
}

type inferencePoolNameConflictError struct {
	namespacedName string
	cluster        string
}

func (e *inferencePoolNameConflictError) Error() string {
	return fmt.Sprintf("InferencePool %s already exists and is not controlled by AntflyCluster %s", e.namespacedName, e.cluster)
}

func (r *AntflyClusterReconciler) setInferencePoolReadyCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeInferencePoolReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
}

func (r *AntflyClusterReconciler) setScalingCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeScaling,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
}

func (r *AntflyClusterReconciler) setDataScaleDownStatus(cluster *antflyv1.AntflyCluster, source string, fromReplicas, targetReplicas, appliedReplicas, drainingOrdinal int32, drainingNodeID, phase, message string) {
	now := metav1.Now()
	if cluster.Status.DataScaleDownStatus != nil &&
		cluster.Status.DataScaleDownStatus.LastTransitionTime != nil &&
		cluster.Status.DataScaleDownStatus.Source == source &&
		cluster.Status.DataScaleDownStatus.Phase == phase &&
		cluster.Status.DataScaleDownStatus.DrainingOrdinal == drainingOrdinal &&
		cluster.Status.DataScaleDownStatus.TargetReplicas == targetReplicas {
		now = *cluster.Status.DataScaleDownStatus.LastTransitionTime
	}
	cluster.Status.DataScaleDownStatus = &antflyv1.DataScaleDownStatus{
		Source:             source,
		FromReplicas:       fromReplicas,
		TargetReplicas:     targetReplicas,
		AppliedReplicas:    appliedReplicas,
		DrainingOrdinal:    drainingOrdinal,
		DrainingNodeID:     drainingNodeID,
		Phase:              phase,
		Message:            message,
		LastTransitionTime: &now,
	}
}

func (r *AntflyClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	// Generate complete configuration with metadata section
	completeConfig, err := r.generateCompleteConfig(cluster)
	if err != nil {
		return fmt.Errorf("failed to generate complete config: %w", err)
	}
	configSum := sha256.Sum256([]byte(completeConfig))
	configHash := fmt.Sprintf("%x", configSum)[:16]

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-config",
			Namespace: cluster.Namespace,
		},
	}

	// Use CreateOrUpdate to ensure ConfigMap is updated with latest configuration
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, configMap, r.Scheme); err != nil {
			return err
		}

		// Update ConfigMap data
		configMap.Data = map[string]string{
			"config.json": completeConfig,
		}
		if configMap.Annotations == nil {
			configMap.Annotations = make(map[string]string)
		}
		configMap.Annotations[generatedConfigHashAnnotation] = configHash

		return nil
	})

	if err != nil {
		return err
	}
	cluster.Status.ConfigPublication = &antflyv1.ConfigPublicationStatus{
		ObservedGeneration: cluster.Generation,
		SHA256:             fmt.Sprintf("%x", configSum),
	}
	return nil
}

// generateCompleteConfig creates a complete Antfly configuration by merging user config with generated metadata network config
func (r *AntflyClusterReconciler) generateCompleteConfig(cluster *antflyv1.AntflyCluster) (string, error) {
	if err := antflyv1.ValidateOperatorManagedStorageSpec(cluster.Spec.Mode, cluster.Spec.Storage); err != nil {
		return "", err
	}
	if effectiveTopologyMode(cluster) == topologyModeStandalone {
		return r.generateStandaloneConfig(cluster)
	}

	return r.generateDistributedConfig(cluster)
}

func (r *AntflyClusterReconciler) generateDistributedConfig(cluster *antflyv1.AntflyCluster) (string, error) {
	// Parse user-provided configuration
	var userConfig map[string]any
	if err := json.Unmarshal([]byte(cluster.Spec.Config), &userConfig); err != nil {
		return "", fmt.Errorf("failed to parse user config: %w", err)
	}
	if _, exists := userConfig["storage"]; exists {
		return "", fmt.Errorf("spec.config.storage is operator-managed; configure PVC storage under spec.storage")
	}
	// Generate metadata orchestration URLs
	metadataReplicas := int32(3)
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		metadataReplicas = cluster.Spec.MetadataNodes.Replicas
	}

	orchestrationURLs := make(map[string]string, metadataReplicas)
	for i := uint64(1); i <= uint64(max(metadataReplicas, 0)); i++ { //nolint:gosec // G115: metadataReplicas is a small positive Kubernetes replica count
		podName := fmt.Sprintf("%s-metadata-%d", cluster.Name, i-1)
		host := statefulPodDNSName(podName, cluster.Name+"-metadata", cluster.Namespace, r.ClusterDomain)
		url := fmt.Sprintf("http://%s:%d", host, cluster.Spec.MetadataNodes.MetadataAPI.Port)
		s := strconv.FormatUint(i, 16)
		orchestrationURLs[s] = url
	}

	// Build complete configuration structure
	completeConfig := map[string]any{
		"storage": map[string]any{
			"engine": "local",
			"local": map[string]any{
				"base_dir": "/antflydb", // Must match PVC mount path
			},
		},
		"metadata": map[string]any{
			"orchestration_urls": orchestrationURLs,
		},
		"max_shard_size_bytes":     67108864, // Default 64MB
		"replication_factor":       3,        // Default
		"enable_auth":              false,    // Default
		"disable_shard_alloc":      true,     // Default
		"default_shards_per_table": uint64(1),
		"max_shards_per_table":     uint64(0),
	}

	// Merge user configuration on top of defaults
	maps.Copy(completeConfig, userConfig)

	// Ensure we don't override the generated network configuration
	completeConfig["metadata"] = map[string]any{
		"orchestration_urls": orchestrationURLs,
	}

	// Storage placement is operator-owned and must match the PVC mount path.
	storageConfig := map[string]any{
		"engine": "local",
		"local": map[string]any{
			"base_dir": "/antflydb",
		},
	}
	completeConfig["storage"] = storageConfig
	if apiURL := r.configuredInferenceAPIURL(cluster); apiURL != "" {
		ensureInferenceAPIURL(completeConfig, apiURL)
	}

	// Convert back to JSON
	configBytes, err := json.MarshalIndent(completeConfig, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal complete config: %w", err)
	}

	return string(configBytes), nil
}

func ensureInferenceAPIURL(config map[string]any, apiURL string) {
	inferenceConfig, _ := config["inference"].(map[string]any)
	if inferenceConfig == nil {
		inferenceConfig = map[string]any{}
	}
	if existing, _ := inferenceConfig["api_url"].(string); strings.TrimSpace(existing) != "" {
		return
	}
	inferenceConfig["api_url"] = apiURL
	config["inference"] = inferenceConfig
}

func (r *AntflyClusterReconciler) generateStandaloneConfig(cluster *antflyv1.AntflyCluster) (string, error) {
	standalone := cluster.Spec.Standalone
	if standalone == nil {
		return "", fmt.Errorf("spec.standalone is required when spec.mode=Standalone")
	}
	inferenceEnabled := standalone.Inference == nil || standalone.Inference.Enabled
	inferenceAPIURL := "http://0.0.0.0:11433"
	if standalone.Inference != nil && standalone.Inference.APIURL != "" {
		inferenceAPIURL = standalone.Inference.APIURL
	}

	// Parse user-provided configuration
	var userConfig map[string]any
	if err := json.Unmarshal([]byte(cluster.Spec.Config), &userConfig); err != nil {
		return "", fmt.Errorf("failed to parse user config: %w", err)
	}
	if err := antflyv1.ValidateOperatorManagedStorageConfig(cluster.Spec.Config); err != nil {
		return "", err
	}

	orchestrationURLs := map[string]string{
		strconv.FormatInt(int64(standalone.NodeID), 16): fmt.Sprintf("http://%s:%d", serviceDNSName(standaloneStatefulSetName(cluster), cluster.Namespace, r.ClusterDomain), standalone.MetadataAPI.Port),
	}

	completeConfig := map[string]any{
		"storage": standaloneRuntimeStorageConfig(cluster),
		"metadata": map[string]any{
			"orchestration_urls": orchestrationURLs,
		},
		"max_shard_size_bytes":     67108864, // Default 64MB
		"replication_factor":       uint64(1),
		"enable_auth":              false,
		"disable_shard_alloc":      true,
		"default_shards_per_table": uint64(1),
		"deployment_mode":          "standalone",
	}

	maps.Copy(completeConfig, userConfig)

	completeConfig["metadata"] = map[string]any{
		"orchestration_urls": orchestrationURLs,
	}
	completeConfig["replication_factor"] = uint64(1)
	completeConfig["default_shards_per_table"] = uint64(1)
	completeConfig["disable_shard_alloc"] = true
	completeConfig["deployment_mode"] = "standalone"

	if inferenceEnabled {
		inferenceConfig := map[string]any{}
		if userInference, ok := userConfig["inference"].(map[string]any); ok {
			maps.Copy(inferenceConfig, userInference)
		}
		inferenceConfig["api_url"] = inferenceAPIURL
		completeConfig["inference"] = inferenceConfig
	}

	completeConfig["storage"] = standaloneRuntimeStorageConfig(cluster)

	configBytes, err := json.MarshalIndent(completeConfig, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal complete config: %w", err)
	}

	return string(configBytes), nil
}

func standaloneRuntimeStorageConfig(cluster *antflyv1.AntflyCluster) map[string]any {
	if cluster.Spec.Storage.Engine == "lite" {
		fileName := cluster.Spec.Storage.LiteFileName
		if fileName == "" {
			fileName = "antfly.aflite"
		}
		return map[string]any{
			"engine": "lite",
			"lite": map[string]any{
				"path":  "/antflydb/" + fileName,
				"fsync": true,
			},
		}
	}
	return map[string]any{
		"engine": "local",
		"local": map[string]any{
			"base_dir": "/antflydb",
		},
	}
}

func (r *AntflyClusterReconciler) reconcileServices(ctx context.Context, cluster *antflyv1.AntflyCluster, suspendPublicAPI bool) error {
	mode := effectiveTopologyMode(cluster)

	// Build list of services to reconcile
	serviceDefs := []*corev1.Service{}

	// Only add public API service if enabled
	publicAPIService := r.createPublicAPIService(cluster, mode == topologyModeStandalone)
	if publicAPIService != nil {
		if mode != topologyModeStandalone && cluster.Spec.InternalServiceAuth != nil {
			if publicAPIService.Annotations == nil {
				publicAPIService.Annotations = make(map[string]string)
			}
			publicAPIService.Annotations[internalServiceAuthPublicBoundaryAnnotation] = "enforced"
		}
		if suspendPublicAPI {
			// Preserve the Service object, ClusterIP, and cloud load-balancer address
			// while publishing zero endpoints during the legacy compatibility
			// window. No operator-managed pod carries this quarantine label.
			publicAPIService.Spec.Selector[internalServiceAuthPublicBoundaryLabel] = "enforce-ready"
			if publicAPIService.Annotations == nil {
				publicAPIService.Annotations = make(map[string]string)
			}
			publicAPIService.Annotations[internalServiceAuthPublicBoundaryAnnotation] = "suspended"
		}
		serviceDefs = append(serviceDefs, publicAPIService)
	}

	if mode == topologyModeStandalone {
		serviceDefs = append(serviceDefs, r.createStandaloneService(cluster))
	} else {
		serviceDefs = append(serviceDefs,
			r.createMetadataService(cluster),
			r.createDataService(cluster),
		)
	}

	// If public API is disabled, delete any existing public-api service
	if publicAPIService == nil {
		existingSvc := &corev1.Service{}
		publicAPISvcName := fmt.Sprintf("%s-public-api", cluster.Name)
		err := r.Get(ctx, types.NamespacedName{
			Name:      publicAPISvcName,
			Namespace: cluster.Namespace,
		}, existingSvc)
		if err == nil {
			log.FromContext(ctx).Info("Deleting public API service because publicAPI is disabled", "service", publicAPISvcName)
			if err := r.Delete(ctx, existingSvc); err != nil {
				return fmt.Errorf("failed to delete public API service %s: %w", publicAPISvcName, err)
			}
		} else if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to check for existing public API service %s: %w", publicAPISvcName, err)
		}
	}

	for _, serviceDef := range serviceDefs {
		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceDef.Name,
				Namespace: serviceDef.Namespace,
			},
		}

		// Use CreateOrUpdate to ensure services are updated with latest configuration
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
			// Set controller reference
			if err := controllerutil.SetControllerReference(cluster, service, r.Scheme); err != nil {
				return err
			}

			// Update mutable service spec fields from the desired definition
			// Note: ClusterIP cannot be changed after creation (immutable)
			if service.Spec.ClusterIP == "" {
				// Only set ClusterIP on creation
				service.Spec.ClusterIP = serviceDef.Spec.ClusterIP
			}

			// Always sync Type to match desired state
			service.Spec.Type = serviceDef.Spec.Type

			service.Spec.PublishNotReadyAddresses = serviceDef.Spec.PublishNotReadyAddresses
			service.Spec.Selector = serviceDef.Spec.Selector
			if serviceDef.Name == cluster.Name+"-public-api" {
				syncHAPrimaryRouteServiceAnnotations(service, serviceDef.Annotations)
				if value, ok := serviceDef.Annotations[internalServiceAuthPublicBoundaryAnnotation]; ok {
					service.Annotations[internalServiceAuthPublicBoundaryAnnotation] = value
				} else {
					delete(service.Annotations, internalServiceAuthPublicBoundaryAnnotation)
				}
			}

			// Copy ports, handling NodePort specially
			service.Spec.Ports = make([]corev1.ServicePort, len(serviceDef.Spec.Ports))
			for i, port := range serviceDef.Spec.Ports {
				service.Spec.Ports[i] = port

				// Only preserve NodePort for NodePort and LoadBalancer service types
				// For ClusterIP, explicitly clear NodePort field
				if serviceDef.Spec.Type != corev1.ServiceTypeNodePort &&
					serviceDef.Spec.Type != corev1.ServiceTypeLoadBalancer {
					service.Spec.Ports[i].NodePort = 0
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *AntflyClusterReconciler) createPublicAPIService(cluster *antflyv1.AntflyCluster, standaloneMode bool) *corev1.Service {
	// Return nil if public API service is disabled
	if cluster.Spec.PublicAPI != nil && cluster.Spec.PublicAPI.Enabled != nil && !*cluster.Spec.PublicAPI.Enabled {
		return nil
	}

	// Get configuration with defaults already applied
	serviceType := corev1.ServiceTypeLoadBalancer
	if cluster.Spec.PublicAPI != nil && cluster.Spec.PublicAPI.ServiceType != nil {
		serviceType = *cluster.Spec.PublicAPI.ServiceType
	}

	port := int32(80)
	if cluster.Spec.PublicAPI != nil && cluster.Spec.PublicAPI.Port != 0 {
		port = cluster.Spec.PublicAPI.Port
	}

	targetPort := cluster.Spec.MetadataNodes.MetadataAPI.Port
	component := "metadata"
	if standaloneMode && cluster.Spec.Standalone != nil {
		targetPort = cluster.Spec.Standalone.MetadataAPI.Port
		component = standaloneComponent(cluster)
	}

	servicePort := corev1.ServicePort{
		Protocol:   corev1.ProtocolTCP,
		Port:       port,
		TargetPort: intstr.FromInt(int(targetPort)),
	}

	// Only set NodePort if service type is NodePort and a specific port is configured
	if serviceType == corev1.ServiceTypeNodePort &&
		cluster.Spec.PublicAPI != nil &&
		cluster.Spec.PublicAPI.NodePort != nil {
		servicePort.NodePort = *cluster.Spec.PublicAPI.NodePort
	}

	selector := serviceSelectorLabels(cluster.Name, component)
	var annotations map[string]string
	// Keep the single, explicitly selected HA route reachable while its Pod is
	// degraded. The runtime remains the authority boundary: it serves safe reads
	// and typed fencing responses, while rejecting mutations without authority.
	publishNotReadyAddresses := cluster.Spec.HighAvailability != nil &&
		cluster.Spec.HighAvailability.Mode == antflyv1.HAModeHotStandby
	if haPrimaryRouteManaged(cluster) {
		target := haCurrentPrimaryRouteTarget(cluster)
		routeSelectorApplied := false
		if routeSelector, ok := haPublicAPISelector(cluster, standaloneMode, target); ok {
			selector = routeSelector
			routeSelectorApplied = true
		}
		annotations = haPrimaryRouteServiceAnnotations(cluster, target, routeSelectorApplied)
	} else if routeSelector, ok := haPublicAPISelector(cluster, standaloneMode, "primary"); ok {
		selector = routeSelector
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name + "-public-api",
			Namespace:   cluster.Namespace,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:                     serviceType,
			Selector:                 selector,
			Ports:                    []corev1.ServicePort{servicePort},
			PublishNotReadyAddresses: publishNotReadyAddresses,
		},
	}
}

func (r *AntflyClusterReconciler) createMetadataService(cluster *antflyv1.AntflyCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-metadata",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 serviceSelectorLabels(cluster.Name, "metadata"),
			Ports: []corev1.ServicePort{
				{
					Name:       "metadata-api",
					Port:       cluster.Spec.MetadataNodes.MetadataAPI.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.MetadataNodes.MetadataAPI.Port)),
				},
				{
					Name:       "metadata-raft",
					Port:       cluster.Spec.MetadataNodes.MetadataRaft.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.MetadataNodes.MetadataRaft.Port)),
				},
			},
		},
	}
}

func (r *AntflyClusterReconciler) createDataService(cluster *antflyv1.AntflyCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-data",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 serviceSelectorLabels(cluster.Name, "data"),
			Ports: []corev1.ServicePort{
				{
					Name:       "data-api",
					Port:       cluster.Spec.DataNodes.API.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.DataNodes.API.Port)),
				},
				{
					Name:       "data-raft",
					Port:       cluster.Spec.DataNodes.Raft.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.DataNodes.Raft.Port)),
				},
			},
		},
	}
}

func (r *AntflyClusterReconciler) createStandaloneService(cluster *antflyv1.AntflyCluster) *corev1.Service {
	standalone := cluster.Spec.Standalone
	if standalone == nil {
		standalone = &antflyv1.StandaloneSpec{}
	}

	healthPort := standalone.Health.Port
	if healthPort == 0 {
		healthPort = 4200
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      standaloneStatefulSetName(cluster),
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 serviceSelectorLabels(cluster.Name, standaloneComponent(cluster)),
			Ports: []corev1.ServicePort{
				{
					Name:       "metadata-api",
					Port:       standalone.MetadataAPI.Port,
					TargetPort: intstr.FromInt(int(standalone.MetadataAPI.Port)),
				},
				{
					Name:       "metadata-raft",
					Port:       standalone.MetadataRaft.Port,
					TargetPort: intstr.FromInt(int(standalone.MetadataRaft.Port)),
				},
				{
					Name:       "store-api",
					Port:       standalone.StoreAPI.Port,
					TargetPort: intstr.FromInt(int(standalone.StoreAPI.Port)),
				},
				{
					Name:       "store-raft",
					Port:       standalone.StoreRaft.Port,
					TargetPort: intstr.FromInt(int(standalone.StoreRaft.Port)),
				},
				{
					Name:       "health",
					Port:       healthPort,
					TargetPort: intstr.FromInt(int(healthPort)),
				},
			},
		},
	}
}

func (r *AntflyClusterReconciler) reconcileStandaloneStatefulSet(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster) error {
	standalone := cluster.Spec.Standalone
	if standalone == nil {
		return fmt.Errorf("spec.standalone is required when spec.mode=Standalone")
	}
	runtimeServiceAccountName, err := r.reconcileHARuntimeLeaseRBAC(ctx, cluster)
	if err != nil {
		return err
	}
	replicas := standalone.Replicas
	if replicas == 0 {
		replicas = 1
	}
	storageSize := chooseStandaloneStorageSize(cluster)
	startupGate := haRuntimeStartupGate(cluster)
	activatedSeedGate := startupGate != nil && startupGate.Policy == antflyv1.HAStartupGatePolicyRequireActivatedSeed && startupGate.RequiredReceipt != nil
	var startupPVC *corev1.PersistentVolumeClaim
	if startupGate != nil {
		if activatedSeedGate {
			if migrated, err := r.reconcileLegacyStandaloneStatefulSetStartupGate(ctx, cluster); err != nil {
				return err
			} else if migrated {
				return nil
			}
			var err error
			startupPVC, err = r.reconcileHAStartupTargetPVC(ctx, cluster, storageSize)
			if err != nil {
				return err
			}
		}
		eligible, _ := haStartupGateRuntimeEligible(cluster, startupPVC)
		if !eligible {
			replicas = 0
		}
		if startupGate.Policy == antflyv1.HAStartupGatePolicySuspend {
			if held, err := r.reconcileSuspendedStandaloneStatefulSet(ctx, cluster); err != nil {
				return err
			} else if held {
				return nil
			}
		}
	}
	if haFormerPrimaryIsolationActive(cluster) && !haFormerPrimaryIsolationReleasedByActivatedStandby(cluster, startupPVC) {
		// Never let ordinary StatefulSet reconciliation resurrect the old writer
		// after ownership has moved. The only release path is the exact, PVC-bound
		// standby activation proof above.
		replicas = 0
	}

	var storageClassName *string
	if cluster.Spec.Storage.StorageClass != "" {
		storageClassName = &cluster.Spec.Storage.StorageClass
	}
	component := standaloneComponent(cluster)
	statefulSetName := standaloneStatefulSetName(cluster)
	storageVolumeName := standaloneStorageVolumeName(cluster)

	envFromSources := append([]corev1.EnvFromSource{}, standalone.EnvFrom...)

	volumeClaimTemplates := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   storageVolumeName,
				Labels: persistentVolumeClaimLabels(cluster, component),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				StorageClassName: storageClassName,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(storageSize),
					},
				},
			},
		},
	}
	if activatedSeedGate {
		volumeClaimTemplates = nil
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         statefulSetName,
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: serviceSelectorLabels(cluster.Name, component),
			},
			VolumeClaimTemplates: volumeClaimTemplates,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		if err := validateAndSetStandaloneStorageIdentity(statefulSet, cluster); err != nil {
			return err
		}
		if err := controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme); err != nil {
			return err
		}

		statefulSet.Spec.Replicas = &replicas
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = buildPVCRetentionPolicy(cluster.Spec.Storage.PVCRetentionPolicy)
		deferPromotionRollout, err := r.shouldDeferPromotedStandaloneRollout(ctx, cluster, statefulSet)
		if err != nil {
			return err
		}
		if deferPromotionRollout {
			// The runtime promotion API has already converted this exact live
			// standby process into the Lease-authorized primary. Keep its open
			// connections and in-memory promotion handoff while publishing the
			// restart-safe primary template. StatefulSet OnDelete creates any
			// replacement from that new template without killing the live writer.
			statefulSet.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.OnDeleteStatefulSetStrategyType,
			}
		} else {
			zero := int32(0)
			statefulSet.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: &zero,
				},
			}
		}
		podAnnotations := r.buildPodAnnotations(ctx, cache, cluster, envFromSources)
		if activatedSeedGate {
			podAnnotations = maps.Clone(podAnnotations)
			if podAnnotations == nil {
				podAnnotations = map[string]string{}
			}
			if hash := haStartupGateReceiptHash(cluster, startupPVC); hash != "" {
				podAnnotations[haStartupGateReceiptHashAnnotation] = hash
				maps.Copy(podAnnotations, haStandaloneRuntimeSeedIdentityAnnotations(cluster))
			}
		}
		volumeMounts := []corev1.VolumeMount{
			{Name: storageVolumeName, MountPath: "/antflydb"},
			{Name: "config", MountPath: "/config"},
		}
		volumes := []corev1.Volume{
			{
				Name: "config",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + "-config"},
				}},
			},
		}
		if activatedSeedGate {
			required := *startupGate.RequiredReceipt
			volumes = append(volumes,
				corev1.Volume{Name: storageVolumeName, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: required.TargetPVCName}}},
			)
			generationRoot := path.Join(haSeedActivationRelativeRoot, "live-generations", required.Generation)
			volumeMounts = append(volumeMounts,
				corev1.VolumeMount{
					Name: storageVolumeName, MountPath: haSeedLiveDataPath,
					SubPath: path.Join(generationRoot, "data"),
				},
				corev1.VolumeMount{
					Name: storageVolumeName, MountPath: haSeedLiveMetadataPath,
					SubPath: path.Join(generationRoot, "metadata"),
				},
				corev1.VolumeMount{
					Name: storageVolumeName, MountPath: haSeedLiveExtensionsPath,
					SubPath: path.Join(generationRoot, "extensions"),
				},
			)
		}
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels(cluster, component),
				Annotations: podAnnotations,
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: runtimeServiceAccountName,
				SecurityContext:    antflyPodSecurityContext(),
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer(storageVolumeName),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         envFromSources,
						Env: append(
							append(append(append(secretStoreEnv(cluster.Spec.SecretStore), haRuntimeAdminTokenEnv(cluster.Spec.HighAvailability)...), haPodUIDEnv()...), haRuntimeLeaseEnv(cluster)...),
							corev1.EnvVar{
								Name:  antflyExtensionPackageStoreEnvVar,
								Value: antflyStandaloneExtensionPackageStore,
							},
						),
						Ports: []corev1.ContainerPort{
							{
								Name:          "metadata-api",
								ContainerPort: standalone.MetadataAPI.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "metadata-raft",
								ContainerPort: standalone.MetadataRaft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "store-api",
								ContainerPort: standalone.StoreAPI.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "store-raft",
								ContainerPort: standalone.StoreRaft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "health",
								ContainerPort: standalone.Health.Port,
								Protocol:      corev1.ProtocolTCP,
							},
						},
						VolumeMounts: append(volumeMounts, secretStoreVolumeMounts(cluster.Spec.SecretStore)...),
						Command:      []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
exec /antfly standalone --id %d --config /config/config.json \
  --host 0.0.0.0 \
  --port %d \
  --health-port %d%s%s%s
							`,
								standalone.NodeID,
								standalone.MetadataAPI.Port,
								standalone.Health.Port,
								secretStoreArg(cluster.Spec.SecretStore),
								standaloneHAArgs(cluster.Spec.HighAvailability, standaloneHAStartupGeneration(cluster)),
								standaloneHAStartupArgs(cluster),
							),
						},
						Resources:    r.buildResourceRequirements(standalone.Resources),
						StartupProbe: buildHTTPStartupProbe(standalone.Health.Port, standalone.StartupProbe),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(standalone.Health.Port)),
								},
							},
							PeriodSeconds:    15,
							TimeoutSeconds:   10,
							FailureThreshold: 3,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(int(standalone.Health.Port)),
								},
							},
							PeriodSeconds:    5,
							TimeoutSeconds:   10,
							FailureThreshold: 5,
						},
					},
				},
				Volumes: append(volumes, secretStoreVolumes(cluster.Spec.SecretStore)...),
			},
		}

		applySchedulingConstraints(&statefulSet.Spec.Template,
			standalone.Tolerations,
			standalone.NodeSelector,
			standalone.Affinity,
			standalone.TopologySpreadConstraints)

		r.applyGKEPodSpec(&statefulSet.Spec.Template, cluster, false)
		r.applyEKSPodSpec(&statefulSet.Spec.Template, cluster, false)

		isGKEAutopilot := cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot
		applyDefaultZoneTopologySpread(statefulSet, &statefulSet.Spec.Template, component, cluster.Name,
			standalone.TopologySpreadConstraints, isGKEAutopilot)

		return nil
	})

	return err
}

func (r *AntflyClusterReconciler) shouldDeferPromotedStandaloneRollout(ctx context.Context, cluster *antflyv1.AntflyCluster, statefulSet *appsv1.StatefulSet) (bool, error) {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Runtime == nil || ha.Runtime.Role != antflyv1.HARuntimeRolePrimary {
		return false, nil
	}

	pod := &corev1.Pod{}
	key := types.NamespacedName{Name: statefulSet.Name + "-0", Namespace: statefulSet.Namespace}
	if err := r.Get(ctx, key, pod); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("observe promoted standalone Pod before rollout: %w", err)
	}
	if pod.UID == "" || pod.DeletionTimestamp != nil ||
		!isPodControlledByExactStatefulSet(pod, statefulSet) ||
		pod.Annotations[haNodeIDAnnotation] != strings.TrimSpace(ha.Runtime.NodeID) {
		return false, nil
	}

	// Colony publishes the promoted primary spec and its exact promotion receipt
	// in separate control-plane reconciliations. A StatefulSet controller can act
	// on the new primary template before the receipt reaches this CR. Recognize
	// the immutable command shape of the still-running standby process and switch
	// to OnDelete immediately; otherwise a rollout already queued in that window
	// cannot be cancelled when the receipt/Pod-UID binding arrives. This is only
	// a rollout hold, never promotion authority: the route and Lease paths still
	// require the exact receipt and generation below.
	runningPromotedProcess := podRunsHAStandbyCommand(pod)
	promotionReceipt := strings.TrimSpace(cluster.Annotations[cloudHAPromotionReceiptAnnotation])
	topologyGeneration, generationErr := strconv.ParseUint(strings.TrimSpace(cluster.Annotations[cloudHATopologyGenerationAnnotation]), 10, 64)
	receiptReady := isLowerHexDigest(promotionReceipt) && generationErr == nil && topologyGeneration >= 2
	if !receiptReady {
		return runningPromotedProcess, nil
	}
	binding := promotionReceipt + ":" + string(pod.UID)
	if hasExactPromotedProcessBinding(cluster, statefulSet, pod) {
		return true, nil
	}
	if !runningPromotedProcess {
		return false, nil
	}
	if statefulSet.Annotations == nil {
		statefulSet.Annotations = map[string]string{}
	}
	statefulSet.Annotations[haPromotedProcessBindingAnnotation] = binding
	return true, nil
}

func podRunsHAStandbyCommand(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		if container.Name != "antfly" {
			continue
		}
		args := strings.Join(container.Args, "\n")
		return strings.Contains(args, "--ha-standby-log") && !strings.Contains(args, "--ha-primary-log")
	}
	return false
}

// hasExactPromotedProcessBinding identifies the one live process that adopted
// the current promotion receipt. The binding deliberately combines the
// receipt with Kubernetes' immutable Pod UID so a replacement Pod can never
// inherit authority from mutable labels, names, or StatefulSet annotations.
func hasExactPromotedProcessBinding(cluster *antflyv1.AntflyCluster, statefulSet *appsv1.StatefulSet, pod *corev1.Pod) bool {
	if cluster == nil || statefulSet == nil || pod == nil || pod.UID == "" || pod.DeletionTimestamp != nil {
		return false
	}
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Runtime == nil || ha.Runtime.Role != antflyv1.HARuntimeRolePrimary {
		return false
	}
	promotionReceipt := strings.TrimSpace(cluster.Annotations[cloudHAPromotionReceiptAnnotation])
	if !isLowerHexDigest(promotionReceipt) {
		return false
	}
	topologyGeneration, err := strconv.ParseUint(strings.TrimSpace(cluster.Annotations[cloudHATopologyGenerationAnnotation]), 10, 64)
	if err != nil || topologyGeneration < 2 {
		return false
	}
	if !isPodControlledByExactStatefulSet(pod, statefulSet) ||
		strings.TrimSpace(pod.Annotations[haNodeIDAnnotation]) != strings.TrimSpace(ha.Runtime.NodeID) {
		return false
	}
	want := promotionReceipt + ":" + string(pod.UID)
	return strings.TrimSpace(statefulSet.Annotations[haPromotedProcessBindingAnnotation]) == want
}

const generatedConfigHashAnnotation = "antfly.io/config-hash"
const compatibilityRolloutGenerationAnnotation = "cloud.antfly.io/compatibility-rollout-generation"
const metadataTopologyReplicasAnnotation = "antfly.io/metadata-topology-replicas"

// buildPodAnnotations returns the complete annotations for pod templates including:
// - Service mesh annotations
// - Generated non-secret Antfly config hash
// - EnvFrom hash annotation (ConfigMap contents and Secret reference names only)
func (r *AntflyClusterReconciler) buildPodAnnotations(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster, envFrom []corev1.EnvFromSource) map[string]string {
	annotations := make(map[string]string)

	// Add service mesh annotations
	if cluster.Spec.ServiceMesh != nil && cluster.Spec.ServiceMesh.Enabled {
		maps.Copy(annotations, cluster.Spec.ServiceMesh.Annotations)
	}
	// Colony uses this non-secret generation only when the running Antfly
	// version does not expose live-reload acknowledgements. Copying it to the
	// template provides a compatibility rollout without granting the operator
	// permission to read Secret contents.
	if generation := strings.TrimSpace(cluster.Annotations[compatibilityRolloutGenerationAnnotation]); generation != "" {
		annotations[compatibilityRolloutGenerationAnnotation] = generation
	}

	// ConfigMap volumes are updated in place, but older Antfly versions only
	// parse config.json during startup. Hash the generated non-secret config so
	// those versions roll when routing or credential references change. Secret
	// values are deliberately absent from this hash and remain live-reloadable.
	if completeConfig, err := r.generateCompleteConfig(cluster); err == nil {
		sum := sha256.Sum256([]byte(completeConfig))
		annotations[generatedConfigHashAnnotation] = fmt.Sprintf("%x", sum)[:16]
	}

	// Add envFrom hash annotation if there are envFrom sources
	if len(envFrom) > 0 {
		hash := r.computeEnvFromHash(ctx, cache, cluster.Namespace, envFrom)
		if hash != "" {
			annotations["antfly.io/envfrom-hash"] = hash
		}
	}

	// Return nil if no annotations to avoid creating empty map
	if len(annotations) == 0 {
		return nil
	}

	return annotations
}

func (r *AntflyClusterReconciler) reconcileMetadataStatefulSet(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster, authMode internalServiceAuthRolloutMode, keyMode internalServiceAuthKeyRolloutMode) error {
	replicas := int32(3)
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		replicas = cluster.Spec.MetadataNodes.Replicas
	}

	storageSize := "500Mi"
	if cluster.Spec.Storage.MetadataStorage != "" {
		storageSize = cluster.Spec.Storage.MetadataStorage
	}

	// Get storage class pointer - nil means use cluster default
	var storageClassName *string
	if cluster.Spec.Storage.StorageClass != "" {
		storageClassName = &cluster.Spec.Storage.StorageClass
	}

	// Build metadata cluster configuration
	metadataCluster := r.buildMetadataClusterConfig(cluster, replicas)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-metadata",
			Namespace: cluster.Namespace,
			Annotations: map[string]string{
				metadataTopologyReplicasAnnotation: strconv.FormatInt(int64(replicas), 10),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         cluster.Name + "-metadata",
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: serviceSelectorLabels(cluster.Name, "metadata"),
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "metadata-storage",
						Labels:      persistentVolumeClaimLabels(cluster, "metadata"),
						Annotations: map[string]string{metadataTopologyReplicasAnnotation: strconv.FormatInt(int64(replicas), 10)},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: storageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(storageSize),
							},
						},
					},
				},
			},
			// Template is populated in the CreateOrUpdate callback below
		},
	}

	// Use CreateOrUpdate to properly handle all spec changes (image, resources, etc.)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme); err != nil {
			return err
		}
		if statefulSet.Annotations == nil {
			statefulSet.Annotations = make(map[string]string)
		}
		statefulSet.Annotations[metadataTopologyReplicasAnnotation] = strconv.FormatInt(int64(replicas), 10)

		// Update mutable fields
		// Note: VolumeClaimTemplates cannot be updated after creation
		statefulSet.Spec.Replicas = &replicas
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = buildPVCRetentionPolicy(cluster.Spec.Storage.PVCRetentionPolicy)
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels(cluster, "metadata"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, cluster.Spec.MetadataNodes.EnvFrom),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
				SecurityContext:    antflyPodSecurityContext(),
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer("metadata-storage"),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         cluster.Spec.MetadataNodes.EnvFrom,
						Env:             append(secretStoreEnv(cluster.Spec.SecretStore), internalServiceAuthEnv(cluster, authMode, keyMode)...),
						Ports: []corev1.ContainerPort{
							{
								Name:          "metadata-api",
								ContainerPort: cluster.Spec.MetadataNodes.MetadataAPI.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "metadata-raft",
								ContainerPort: cluster.Spec.MetadataNodes.MetadataRaft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "health",
								ContainerPort: cluster.Spec.MetadataNodes.Health.Port,
								Protocol:      corev1.ProtocolTCP,
							},
						},
						VolumeMounts: append([]corev1.VolumeMount{
							{
								Name:      "metadata-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						}, secretStoreVolumeMounts(cluster.Spec.SecretStore)...),
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
ORDINAL=${HOSTNAME##*-}
ID=$((ORDINAL + 1))
exec /antfly metadata --id $ID --config /config/config.json \
  --api-host 0.0.0.0 \
  --api-port %d \
  --raft-host 0.0.0.0 \
  --raft-port %d \
  --health-port %d \
  --cluster '%s'%s
								`,
								cluster.Spec.MetadataNodes.MetadataAPI.Port,
								cluster.Spec.MetadataNodes.MetadataRaft.Port,
								cluster.Spec.MetadataNodes.Health.Port,
								metadataCluster,
								secretStoreArg(cluster.Spec.SecretStore),
							),
						},
						Resources:    r.buildResourceRequirements(cluster.Spec.MetadataNodes.Resources),
						StartupProbe: buildHTTPStartupProbe(cluster.Spec.MetadataNodes.Health.Port, cluster.Spec.MetadataNodes.StartupProbe),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(cluster.Spec.MetadataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    15,
							TimeoutSeconds:   10,
							FailureThreshold: 3,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(int(cluster.Spec.MetadataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    5,
							TimeoutSeconds:   10,
							FailureThreshold: 5,
						},
					},
				},
				Volumes: append([]corev1.Volume{
					{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: cluster.Name + "-config",
								},
							},
						},
					},
				}, secretStoreVolumes(cluster.Spec.SecretStore)...),
			},
		}
		if statefulSet.Spec.Template.Annotations == nil {
			statefulSet.Spec.Template.Annotations = make(map[string]string)
		}
		statefulSet.Spec.Template.Annotations[metadataMembershipStatusCapabilityAnnotation] = metadataMembershipStatusCapabilityVersion
		statefulSet.Spec.Template.Annotations[internalServiceAuthRolloutAnnotation] = string(authMode)
		statefulSet.Spec.Template.Annotations[internalServiceAuthKeyRolloutAnnotation] = string(keyMode)
		statefulSet.Spec.Template.Annotations[internalServiceAuthKeyTargetAnnotation] = internalServiceAuthKeyTarget(cluster.Spec.InternalServiceAuth)

		// Apply user-specified scheduling constraints first
		applySchedulingConstraints(&statefulSet.Spec.Template,
			cluster.Spec.MetadataNodes.Tolerations,
			cluster.Spec.MetadataNodes.NodeSelector,
			cluster.Spec.MetadataNodes.Affinity,
			cluster.Spec.MetadataNodes.TopologySpreadConstraints)

		// Apply GKE-specific configurations
		r.applyGKEPodSpec(&statefulSet.Spec.Template, cluster, cluster.Spec.MetadataNodes.UseSpotPods)

		// Apply EKS-specific configurations
		r.applyEKSPodSpec(&statefulSet.Spec.Template, cluster, false) // Spot not recommended for metadata nodes

		// Apply default zone topology spread if user hasn't specified explicit constraints
		isGKEAutopilot := cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot
		applyDefaultZoneTopologySpread(statefulSet, &statefulSet.Spec.Template, "metadata", cluster.Name,
			cluster.Spec.MetadataNodes.TopologySpreadConstraints, isGKEAutopilot)

		return nil
	})

	if err != nil {
		return err
	}
	return r.recordMetadataTopologyOnPVCs(ctx, cluster, replicas)
}

func (r *AntflyClusterReconciler) recordMetadataTopologyOnPVCs(ctx context.Context, cluster *antflyv1.AntflyCluster, replicas int32) error {
	// This runs on every steady-state reconciliation, so address only the bounded
	// expected claim set. The validator retains the namespace-wide scan needed to
	// detect unexpected retained ordinals before any topology is accepted.
	topologyReader := r.haBoundaryReader()
	pvcPrefix := metadataPVCPrefix(cluster)
	want := strconv.FormatInt(int64(replicas), 10)
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		pvc := &corev1.PersistentVolumeClaim{}
		key := types.NamespacedName{
			Name:      pvcPrefix + strconv.FormatInt(int64(ordinal), 10),
			Namespace: cluster.Namespace,
		}
		if err := topologyReader.Get(ctx, key, pvc); err != nil {
			// StatefulSet PVC creation is asynchronous. A later pod/StatefulSet event
			// or the periodic topology observation will retry the exact claim.
			if errors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("read metadata PVC %s for topology recording: %w", key.Name, err)
		}
		if got, ok := pvc.Annotations[metadataTopologyReplicasAnnotation]; ok {
			if got != want {
				return fmt.Errorf("metadata PVC %s records topology %q, expected %q", pvc.Name, got, want)
			}
			continue
		}

		before := pvc.DeepCopy()
		if pvc.Annotations == nil {
			pvc.Annotations = make(map[string]string)
		}
		pvc.Annotations[metadataTopologyReplicasAnnotation] = want
		if err := r.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
			return fmt.Errorf("record metadata topology on PVC %s: %w", pvc.Name, err)
		}
	}
	return nil
}

func (r *AntflyClusterReconciler) buildMetadataClusterConfig(cluster *antflyv1.AntflyCluster, replicas int32) string {
	var config strings.Builder
	config.WriteString("{ ")
	for i := int32(1); i <= replicas; i++ {
		if i > 1 {
			config.WriteString(", ")
		}
		podName := fmt.Sprintf("%s-metadata-%d", cluster.Name, i-1)
		host := statefulPodDNSName(podName, cluster.Name+"-metadata", cluster.Namespace, r.ClusterDomain)
		fmt.Fprintf(
			&config,
			`"%d": {"raft_url":"http://%s:%d","orchestration_url":"http://%s:%d"}`,
			i,
			host, cluster.Spec.MetadataNodes.MetadataRaft.Port,
			host, cluster.Spec.MetadataNodes.MetadataAPI.Port,
		)
	}
	config.WriteString(" }")
	return config.String()
}

func (r *AntflyClusterReconciler) reconcileDataStatefulSet(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster, authMode internalServiceAuthRolloutMode, keyMode internalServiceAuthKeyRolloutMode) error {
	replicas := effectiveDataNodeReplicas(cluster)
	dataAdvertiseHost := "${HOSTNAME}." + serviceDNSName(cluster.Name+"-data", cluster.Namespace, r.ClusterDomain)

	storageSize := "1Gi"
	if cluster.Spec.Storage.DataStorage != "" {
		storageSize = cluster.Spec.Storage.DataStorage
	}

	// Get storage class pointer - nil means use cluster default
	var storageClassName *string
	if cluster.Spec.Storage.StorageClass != "" {
		storageClassName = &cluster.Spec.Storage.StorageClass
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-data",
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         cluster.Name + "-data",
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: serviceSelectorLabels(cluster.Name, "data"),
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "data-storage",
						Labels: persistentVolumeClaimLabels(cluster, "data"),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: storageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(storageSize),
							},
						},
					},
				},
			},
			// Template is populated in the CreateOrUpdate callback below
		},
	}

	// Determine if EKS Spot should be used for data nodes (safe with 3+ replicas)
	useEKSSpot := cluster.Spec.EKS != nil && cluster.Spec.EKS.UseSpotInstances

	// Use CreateOrUpdate to properly handle all spec changes (image, resources, etc.)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme); err != nil {
			return err
		}

		// Update mutable fields
		// Note: VolumeClaimTemplates cannot be updated after creation
		statefulSet.Spec.Replicas = &replicas
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = buildPVCRetentionPolicy(cluster.Spec.Storage.PVCRetentionPolicy)
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels(cluster, "data"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, cluster.Spec.DataNodes.EnvFrom),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
				SecurityContext:    antflyPodSecurityContext(),
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer("data-storage"),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         cluster.Spec.DataNodes.EnvFrom,
						Ports: []corev1.ContainerPort{
							{
								Name:          "data-api",
								ContainerPort: cluster.Spec.DataNodes.API.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "data-raft",
								ContainerPort: cluster.Spec.DataNodes.Raft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "health",
								ContainerPort: cluster.Spec.DataNodes.Health.Port,
								Protocol:      corev1.ProtocolTCP,
							},
						},
						VolumeMounts: append([]corev1.VolumeMount{
							{
								Name:      "data-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						}, secretStoreVolumeMounts(cluster.Spec.SecretStore)...),
						Env: append(append(append([]corev1.EnvVar{
							{
								Name: "POD_IP",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "status.podIP",
									},
								},
							},
						}, secretStoreEnv(cluster.Spec.SecretStore)...), haPodUIDEnv()...), internalServiceAuthEnv(cluster, authMode, keyMode)...),
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
ORDINAL=${HOSTNAME##*-}
ID=$((ORDINAL + 1))
exec /antfly data --node-id $ID --store-id $ID --config /config/config.json \
  --api-host ${POD_IP} \
  --api-port %d \
  --api-advertise-url http://%s:%d \
  --raft-host ${POD_IP} \
  --raft-port %d \
  --raft-advertise-url http://%s:%d \
  --health-port %d%s
								`,
								cluster.Spec.DataNodes.API.Port,
								dataAdvertiseHost,
								cluster.Spec.DataNodes.API.Port,
								cluster.Spec.DataNodes.Raft.Port,
								dataAdvertiseHost,
								cluster.Spec.DataNodes.Raft.Port,
								cluster.Spec.DataNodes.Health.Port,
								secretStoreArg(cluster.Spec.SecretStore),
							),
						},
						Resources:    r.buildResourceRequirements(cluster.Spec.DataNodes.Resources),
						StartupProbe: buildHTTPStartupProbe(cluster.Spec.DataNodes.Health.Port, cluster.Spec.DataNodes.StartupProbe),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(cluster.Spec.DataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    15,
							TimeoutSeconds:   10,
							FailureThreshold: 3,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(int(cluster.Spec.DataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    5,
							TimeoutSeconds:   10,
							FailureThreshold: 5,
						},
					},
				},
				Volumes: append([]corev1.Volume{
					{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: cluster.Name + "-config",
								},
							},
						},
					},
				}, secretStoreVolumes(cluster.Spec.SecretStore)...),
			},
		}
		if statefulSet.Spec.Template.Annotations == nil {
			statefulSet.Spec.Template.Annotations = make(map[string]string)
		}
		statefulSet.Spec.Template.Annotations[internalServiceAuthRolloutAnnotation] = string(authMode)
		statefulSet.Spec.Template.Annotations[internalServiceAuthKeyRolloutAnnotation] = string(keyMode)
		statefulSet.Spec.Template.Annotations[internalServiceAuthKeyTargetAnnotation] = internalServiceAuthKeyTarget(cluster.Spec.InternalServiceAuth)

		// Apply user-specified scheduling constraints first
		applySchedulingConstraints(&statefulSet.Spec.Template,
			cluster.Spec.DataNodes.Tolerations,
			cluster.Spec.DataNodes.NodeSelector,
			cluster.Spec.DataNodes.Affinity,
			cluster.Spec.DataNodes.TopologySpreadConstraints)

		// Apply GKE-specific configurations
		r.applyGKEPodSpec(&statefulSet.Spec.Template, cluster, cluster.Spec.DataNodes.UseSpotPods)

		// Apply EKS-specific configurations
		r.applyEKSPodSpec(&statefulSet.Spec.Template, cluster, useEKSSpot)

		// Apply default zone topology spread if user hasn't specified explicit constraints
		isGKEAutopilot := cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot
		applyDefaultZoneTopologySpread(statefulSet, &statefulSet.Spec.Template, "data", cluster.Name,
			cluster.Spec.DataNodes.TopologySpreadConstraints, isGKEAutopilot)

		return nil
	})

	return err
}

func (r *AntflyClusterReconciler) buildResourceRequirements(resourceSpec antflyv1.ResourceSpec) corev1.ResourceRequirements {
	requirements := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	if resourceSpec.CPU != "" {
		requirements.Requests[corev1.ResourceCPU] = resource.MustParse(resourceSpec.CPU)
	}
	if resourceSpec.Memory != "" {
		requirements.Requests[corev1.ResourceMemory] = resource.MustParse(resourceSpec.Memory)
	}
	if resourceSpec.Limits.CPU != "" {
		requirements.Limits[corev1.ResourceCPU] = resource.MustParse(resourceSpec.Limits.CPU)
	}
	if resourceSpec.Limits.Memory != "" {
		requirements.Limits[corev1.ResourceMemory] = resource.MustParse(resourceSpec.Limits.Memory)
	}
	if resourceSpec.Limits.GPU != "" {
		requirements.Limits[corev1.ResourceName("nvidia.com/gpu")] = resource.MustParse(resourceSpec.Limits.GPU)
	}

	return requirements
}

func buildHTTPStartupProbe(port int32, cfg *antflyv1.ProbeConfig) *corev1.Probe {
	failureThreshold := int32(30)
	periodSeconds := int32(10)
	timeoutSeconds := int32(10)
	if cfg != nil {
		if cfg.FailureThreshold != nil {
			failureThreshold = *cfg.FailureThreshold
		}
		if cfg.PeriodSeconds != nil {
			periodSeconds = *cfg.PeriodSeconds
		}
		if cfg.TimeoutSeconds != nil {
			timeoutSeconds = *cfg.TimeoutSeconds
		}
	}
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/healthz",
				Port: intstr.FromInt(int(port)),
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       periodSeconds,
		TimeoutSeconds:      timeoutSeconds,
		FailureThreshold:    failureThreshold,
	}
}

func chooseStandaloneStorageSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.StandaloneStorage != "" {
		return cluster.Spec.Storage.StandaloneStorage
	}
	return "1Gi"
}

func effectiveDataStorageSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.DataStorage != "" {
		return cluster.Spec.Storage.DataStorage
	}
	return "1Gi"
}

func maxDataAutoGrowSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.StorageAutoGrow == nil {
		return ""
	}
	return cluster.Spec.Storage.StorageAutoGrow.MaxDataStorage
}

func maxStandaloneAutoGrowSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.StorageAutoGrow == nil {
		return ""
	}
	if cluster.Spec.Storage.StorageAutoGrow.MaxStandaloneStorage != "" {
		return cluster.Spec.Storage.StorageAutoGrow.MaxStandaloneStorage
	}
	return cluster.Spec.Storage.StorageAutoGrow.MaxDataStorage
}

func storageAutoGrowEnabled(cluster *antflyv1.AntflyCluster) bool {
	return cluster.Spec.Storage.StorageAutoGrow != nil && cluster.Spec.Storage.StorageAutoGrow.Enabled
}

func effectiveDataNodeReplicas(cluster *antflyv1.AntflyCluster) int32 {
	if cluster.Spec.DataNodes.Suspend {
		return 0
	}
	if cluster.Spec.DataNodes.Replicas > 0 {
		return cluster.Spec.DataNodes.Replicas
	}
	return 3
}

func effectiveDataReplicas(sts *appsv1.StatefulSet, exists bool, fallback int32) int32 {
	if !exists || sts == nil {
		return fallback
	}
	if sts.Status.Replicas > 0 {
		return sts.Status.Replicas
	}
	if sts.Spec.Replicas != nil {
		return *sts.Spec.Replicas
	}
	return fallback
}

func effectiveDataReplicaTarget(sts *appsv1.StatefulSet, exists bool, fallback int32) int32 {
	if !exists || sts == nil {
		return fallback
	}
	if sts.Spec.Replicas != nil {
		return *sts.Spec.Replicas
	}
	if sts.Status.Replicas > 0 {
		return sts.Status.Replicas
	}
	return fallback
}

func nodeIDForDataOrdinal(ordinal int32) string {
	return strconv.FormatInt(int64(ordinal+1), 10)
}

// buildStorageInitContainer creates an init container that waits for the PVC to
// be mounted and prepares ownership for the non-root Antfly runtime user. This
// prevents a race where the main container starts before the PVC is attached,
// and recovers existing PVCs that were created with root-owned directories.
func (r *AntflyClusterReconciler) buildStorageInitContainer(volumeName string) corev1.Container {
	return corev1.Container{
		Name:    "wait-for-storage",
		Image:   "busybox:1.36",
		Command: []string{"/bin/sh", "-c"},
		Args: []string{fmt.Sprintf(`
set -eu

echo "Checking PVC mount status..."
timeout=120
while [ $timeout -gt 0 ]; do
    if mountpoint -q /antflydb; then
        echo "PVC mounted successfully at /antflydb"
        if [ -d "/antflydb/metadata" ] || [ -d "/antflydb/store" ]; then
            echo "Found existing data directories - recovery mode"
            ls -la /antflydb/
        else
            echo "No existing data - fresh cluster"
        fi
        marker=/antflydb/.antflydb-storage-prepared
        echo "Preparing PVC root for antfly runtime user %d:%d"
        if [ ! -f "$marker" ]; then
            chown -R %d:%d /antflydb
            chmod -R ug+rwX /antflydb
            touch "$marker"
            chown %d:%d "$marker"
            chmod ug+rw "$marker"
        else
            chown %d:%d /antflydb
            chmod ug+rwX /antflydb
        fi
        extension_store=%s
        echo "Preparing durable extension package store at $extension_store"
        mkdir -p "$extension_store"
        chown -R %d:%d "$extension_store"
        chmod -R ug+rwX "$extension_store"
        exit 0
    fi
    echo "Waiting for PVC mount... ($timeout seconds remaining)"
    sleep 2
    timeout=$((timeout - 2))
done
echo "ERROR: PVC mount timeout after 120 seconds"
exit 1
`,
			antflyRuntimeUID,
			antflyRuntimeGID,
			antflyRuntimeUID,
			antflyRuntimeGID,
			antflyRuntimeUID,
			antflyRuntimeGID,
			antflyRuntimeUID,
			antflyRuntimeGID,
			antflyStandaloneExtensionPackageStore,
			antflyRuntimeUID,
			antflyRuntimeGID,
		)},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: int64Ptr(0),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volumeName,
				MountPath: "/antflydb",
			},
		},
		// Minimal resources for the init container
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("32Mi"),
			},
		},
	}
}

// reconcileServiceMeshStatus detects sidecar injection status and updates cluster status accordingly
func (r *AntflyClusterReconciler) reconcileServiceMeshStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	log := log.FromContext(ctx)

	// Initialize ServiceMeshStatus if not present
	if cluster.Status.ServiceMeshStatus == nil {
		cluster.Status.ServiceMeshStatus = &antflyv1.ServiceMeshStatus{}
	}

	// Update enabled status from spec
	cluster.Status.ServiceMeshStatus.Enabled = cluster.Spec.ServiceMesh != nil && cluster.Spec.ServiceMesh.Enabled

	// If service mesh is disabled, set status to None and return
	if !cluster.Status.ServiceMeshStatus.Enabled {
		if cluster.Status.ServiceMeshStatus.SidecarInjectionStatus != "None" {
			log.Info("Service mesh disabled", "cluster", cluster.Name)
			cluster.Status.ServiceMeshStatus.SidecarInjectionStatus = "None"
			cluster.Status.ServiceMeshStatus.PodsWithSidecars = 0
			cluster.Status.ServiceMeshStatus.TotalPods = 0
			cluster.Status.ServiceMeshStatus.LastTransitionTime = &metav1.Time{Time: time.Now()}
		}
		return nil
	}

	// Detect sidecar injection status
	podsWithSidecars, totalPods, err := r.detectSidecarInjectionStatus(ctx, cluster)
	if err != nil {
		log.Error(err, "Failed to detect sidecar injection status")
		return err
	}

	// Calculate injection status
	var newStatus string
	if totalPods == 0 {
		newStatus = "Unknown"
	} else if podsWithSidecars == totalPods {
		newStatus = "Complete"
	} else if podsWithSidecars == 0 {
		newStatus = "None"
	} else {
		newStatus = "Partial"
	}

	// Check if status changed
	statusChanged := cluster.Status.ServiceMeshStatus.SidecarInjectionStatus != newStatus

	// Update status fields
	cluster.Status.ServiceMeshStatus.PodsWithSidecars = podsWithSidecars
	cluster.Status.ServiceMeshStatus.TotalPods = totalPods
	cluster.Status.ServiceMeshStatus.SidecarInjectionStatus = newStatus

	if statusChanged {
		cluster.Status.ServiceMeshStatus.LastTransitionTime = &metav1.Time{Time: time.Now()}
		log.Info("Service mesh status changed",
			"cluster", cluster.Name,
			"status", newStatus,
			"podsWithSidecars", podsWithSidecars,
			"totalPods", totalPods)
	}

	// Handle partial injection - emit event and block reconciliation
	if newStatus == "Partial" {
		// Create warning event
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "PartialSidecarInjection", "PartialSidecarInjection",
			"Partial sidecar injection detected: %d/%d pods have sidecars", podsWithSidecars, totalPods)

		log.Error(fmt.Errorf("partial sidecar injection"), "Blocking reconciliation",
			"podsWithSidecars", podsWithSidecars,
			"totalPods", totalPods)

		return fmt.Errorf("partial sidecar injection detected: %d/%d pods have sidecars", podsWithSidecars, totalPods)
	}

	// Log successful complete injection
	if newStatus == "Complete" && statusChanged {
		log.Info("Service mesh sidecar injection complete",
			"cluster", cluster.Name,
			"totalPods", totalPods)
	}

	return nil
}

func (r *AntflyClusterReconciler) updateStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	return r.updateStatusFrom(ctx, cluster, nil)
}

func (r *AntflyClusterReconciler) updateStatusIfChanged(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	previous *antflyv1.AntflyClusterStatus,
) error {
	return r.updateStatusFrom(ctx, cluster, previous)
}

func (r *AntflyClusterReconciler) updateStatusFrom(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	previous *antflyv1.AntflyClusterStatus,
) error {
	persist := func() error {
		if previous != nil && reflect.DeepEqual(previous, &cluster.Status) {
			return nil
		}
		return r.Status().Update(ctx, cluster)
	}
	originalConditions := append([]metav1.Condition(nil), cluster.Status.Conditions...)
	originalPhase := cluster.Status.Phase
	mode := effectiveTopologyMode(cluster)

	if mode == topologyModeStandalone {
		r.metadataTopologyObservations.Delete(metadataTopologyObservationKey(cluster))
		standalone := cluster.Spec.Standalone
		if standalone == nil {
			return fmt.Errorf("spec.standalone is required when spec.mode=Standalone")
		}

		standaloneSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace}, standaloneSts); err != nil && !errors.IsNotFound(err) {
			return err
		}

		readyReplicas := standaloneSts.Status.ReadyReplicas
		cluster.Status.Mode = antflyv1.ClusterModeStandalone
		cluster.Status.ReadyReplicas = readyReplicas
		cluster.Status.StandaloneNodesReady = readyReplicas
		cluster.Status.MetadataNodesReady = 0
		cluster.Status.DataNodesReady = 0
		if readyReplicas >= standalone.Replicas && standalone.Replicas > 0 {
			cluster.Status.Phase = "Running"
		} else {
			cluster.Status.Phase = "Pending"
		}

		if cluster.Status.StandaloneStatus == nil {
			cluster.Status.StandaloneStatus = &antflyv1.StandaloneStatus{}
		}
		oldStatus := *cluster.Status.StandaloneStatus
		inferenceEnabled := standalone.Inference == nil || standalone.Inference.Enabled
		cluster.Status.StandaloneStatus.Ready = readyReplicas >= standalone.Replicas && standalone.Replicas > 0
		cluster.Status.StandaloneStatus.MetadataReady = cluster.Status.StandaloneStatus.Ready
		cluster.Status.StandaloneStatus.StoreReady = cluster.Status.StandaloneStatus.Ready
		cluster.Status.StandaloneStatus.InferenceReady = !inferenceEnabled || cluster.Status.StandaloneStatus.Ready
		cluster.Status.StandaloneStatus.NodeID = standalone.NodeID

		if completeConfig, err := r.generateStandaloneConfig(cluster); err == nil {
			sum := sha256.Sum256([]byte(completeConfig))
			cluster.Status.StandaloneStatus.ObservedConfigHash = fmt.Sprintf("%x", sum)[:16]
		}

		var podList corev1.PodList
		if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, standaloneComponent(cluster)))); err != nil {
			return err
		}
		for _, pod := range podList.Items {
			if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
				cluster.Status.StandaloneStatus.PodName = pod.Name
				cluster.Status.StandaloneStatus.PodIP = pod.Status.PodIP
				break
			}
		}
		standaloneFindings := poddiagnostics.DiagnosePods(podList.Items)
		if len(standaloneFindings) > 0 {
			cluster.Status.Phase = "Degraded"
			cluster.Status.StandaloneStatus.Ready = false
			cluster.Status.StandaloneStatus.MetadataReady = false
			cluster.Status.StandaloneStatus.StoreReady = false
			cluster.Status.StandaloneStatus.InferenceReady = !inferenceEnabled
		}

		if oldStatus.LastTransitionTime == nil ||
			cluster.Status.StandaloneStatus.Ready != oldStatus.Ready ||
			cluster.Status.StandaloneStatus.MetadataReady != oldStatus.MetadataReady ||
			cluster.Status.StandaloneStatus.StoreReady != oldStatus.StoreReady ||
			cluster.Status.StandaloneStatus.InferenceReady != oldStatus.InferenceReady ||
			cluster.Status.StandaloneStatus.PodName != oldStatus.PodName ||
			cluster.Status.StandaloneStatus.PodIP != oldStatus.PodIP ||
			cluster.Status.StandaloneStatus.NodeID != oldStatus.NodeID ||
			cluster.Status.StandaloneStatus.ObservedConfigHash != oldStatus.ObservedConfigHash {
			now := metav1.Now()
			cluster.Status.StandaloneStatus.LastTransitionTime = &now
		}

		r.updateRolloutCondition(cluster, standaloneSts)
		r.setComponentCondition(cluster, antflyv1.TypeStandaloneReady, readyReplicas, standalone.Replicas, standaloneFindings, "standalone")
		r.setComponentCondition(cluster, antflyv1.TypeMetadataReady, readyReplicas, standalone.Replicas, standaloneFindings, "standalone metadata")
		r.setComponentCondition(cluster, antflyv1.TypeDataReady, readyReplicas, standalone.Replicas, standaloneFindings, "standalone store")
		if inferenceEnabled {
			r.setComponentCondition(cluster, antflyv1.TypeInferenceReady, readyReplicas, standalone.Replicas, standaloneFindings, "standalone inference")
		} else {
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
				Type:               antflyv1.TypeInferenceReady,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: cluster.Generation,
				Reason:             antflyv1.ReasonComponentReady,
				Message:            "Inference is disabled",
			})
		}
		r.setAvailableCondition(cluster, standaloneFindings, readyReplicas >= standalone.Replicas && standalone.Replicas > 0)
		r.recordClusterRuntimeFailureEvents(cluster, originalConditions)
		r.updateProductTierStatus(cluster)
		if err := r.observeHAPrimaryRouteStatus(ctx, cluster); err != nil {
			return err
		}
		haAdminStatusErr := r.observeHAPrimaryAdminStatusForReconcile(ctx, cluster)
		if standbyErr := r.observeHAStandbyAdminStatuses(ctx, cluster); standbyErr != nil && haAdminStatusErr == nil {
			haAdminStatusErr = standbyErr
		}
		if err := r.observeHAFencingStatus(ctx, cluster); err != nil {
			return err
		}
		if err := r.reconcileHAFencingLease(ctx, cluster); err != nil {
			return err
		}
		r.observeHAFormerPrimaryFenceStatus(ctx, cluster)
		// Runtime observation cannot carry operator-owned typed action results.
		// Restore a completed assessment before planning so a promoted runtime's
		// honest FormerPrimaryNotObserved view cannot repeatedly replan Demote
		// and strand the subsequent rewind/reseed disposition.
		r.updateHAFormerPrimaryFromAdminJobs(ctx, cluster)
		r.updateHAStatusAndConditions(cluster)
		if haAdminStatusErr != nil {
			setHACondition(
				cluster,
				antflyv1.TypeHADegraded,
				metav1.ConditionTrue,
				haAdminStatusUnavailableReason(cluster, antflyv1.ReasonHAAdminStatusUnavailable),
				fmt.Sprintf("Unable to observe HA admin status: %v", haAdminStatusErr),
			)
		}
		// Revalidate a previously succeeded physical-isolation receipt against
		// current uncached Kubernetes objects before any dependent admin action
		// can observe it as satisfied in this reconciliation.
		if err := r.reconcileHAFormerPrimaryIsolation(ctx, cluster); err != nil {
			return err
		}
		if err := r.reconcileHAAdminJobs(ctx, cluster); err != nil {
			if stderrors.Is(err, errHAPlanNeedsPersistence) {
				if persistErr := r.persistHAActionPlanBarrier(ctx, cluster); persistErr != nil {
					return persistErr
				}
				return errHAStatusCheckpointed
			}
			return err
		}
		r.updateHAStartupGateStatus(ctx, cluster)
		r.updateHALastPromotionFromAdminJobs(ctx, cluster)
		r.updateHAFormerPrimaryFromAdminJobs(ctx, cluster)
		if err := r.reconcileHAPrimaryRoute(ctx, cluster); err != nil {
			return err
		}
		r.updateHAAdminJobExecutionCondition(cluster)
		r.updateServiceMeshReadyCondition(cluster)
		return persist()
	}

	// Get current status of StatefulSets and Deployment
	metadataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}, metadataSts); err != nil && !errors.IsNotFound(err) {
		return err
	}

	dataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-data", Namespace: cluster.Namespace}, dataSts); err != nil && !errors.IsNotFound(err) {
		return err
	}

	cluster.Status.MetadataNodesReady = metadataSts.Status.ReadyReplicas
	cluster.Status.DataNodesReady = dataSts.Status.ReadyReplicas
	cluster.Status.Mode = antflyv1.ClusterModeDistributed
	cluster.Status.ReadyReplicas = metadataSts.Status.ReadyReplicas + dataSts.Status.ReadyReplicas
	cluster.Status.StandaloneNodesReady = 0
	cluster.Status.StandaloneStatus = nil

	// Update autoscaling status if enabled
	if cluster.Spec.DataNodes.AutoScaling != nil && cluster.Spec.DataNodes.AutoScaling.Enabled {
		if cluster.Status.AutoScalingStatus != nil {
			cluster.Status.AutoScalingStatus.CurrentReplicas = effectiveDataReplicas(dataSts, dataSts.Name != "", effectiveDataNodeReplicas(cluster))
		}
	}

	// Determine phase based on the configured replica counts rather than a
	// hardcoded production-sized cluster. Local dev intentionally runs 1+1.
	metadataReplicas := int32(3)
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		metadataReplicas = cluster.Spec.MetadataNodes.Replicas
	}
	dataReplicas := effectiveDataNodeReplicas(cluster)

	metadataPods, err := r.listComponentPods(ctx, cluster, "metadata")
	if err != nil {
		return err
	}
	dataPods, err := r.listComponentPods(ctx, cluster, "data")
	if err != nil {
		return err
	}
	metadataFindings := poddiagnostics.DiagnosePods(metadataPods)
	dataFindings := poddiagnostics.DiagnosePods(dataPods)
	var metadataTopologyHealthErr error
	var metadataTopologyPending *metadataTopologyObservationPendingError
	if cluster.Status.MetadataNodesReady >= metadataReplicas && metadataReplicas > 0 && len(metadataFindings) == 0 {
		if err := r.validateMetadataRuntimeTopology(ctx, cluster, metadataReplicas); err != nil {
			if !stderrors.As(err, &metadataTopologyPending) {
				metadataTopologyHealthErr = fmt.Errorf("metadata runtime topology validation failed: %w", err)
			}
		}
	} else {
		r.metadataTopologyObservations.Delete(metadataTopologyObservationKey(cluster))
	}
	r.setComponentCondition(cluster, antflyv1.TypeMetadataReady, cluster.Status.MetadataNodesReady, metadataReplicas, metadataFindings, "metadata")
	if metadataTopologyPending != nil {
		preserveConditionDuringMetadataTopologyObservation(cluster, originalConditions, antflyv1.TypeMetadataReady, metadataTopologyPending)
	} else if metadataTopologyHealthErr != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               antflyv1.TypeMetadataReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             antflyv1.ReasonValidationFailed,
			Message:            metadataTopologyHealthErr.Error(),
		})
	}
	r.setComponentCondition(cluster, antflyv1.TypeDataReady, cluster.Status.DataNodesReady, dataReplicas, dataFindings, "data")
	allRuntimeFindings := append(append([]poddiagnostics.Finding{}, metadataFindings...), dataFindings...)
	componentsReady := cluster.Status.MetadataNodesReady >= metadataReplicas && cluster.Status.DataNodesReady >= dataReplicas

	if len(allRuntimeFindings) > 0 || metadataTopologyHealthErr != nil {
		cluster.Status.Phase = "Degraded"
	} else if !componentsReady {
		cluster.Status.Phase = "Pending"
	} else if metadataTopologyPending != nil {
		if originalPhase == "" {
			cluster.Status.Phase = "Pending"
		} else {
			cluster.Status.Phase = originalPhase
		}
	} else {
		cluster.Status.Phase = "Running"
	}

	r.updateRolloutCondition(cluster, metadataSts, dataSts)
	r.setAvailableCondition(cluster, allRuntimeFindings, cluster.Status.Phase == "Running")
	if metadataTopologyPending != nil && len(allRuntimeFindings) == 0 && componentsReady {
		preserveConditionDuringMetadataTopologyObservation(cluster, originalConditions, antflyv1.TypeAvailable, metadataTopologyPending)
	} else if metadataTopologyHealthErr != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               antflyv1.TypeAvailable,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             antflyv1.ReasonValidationFailed,
			Message:            metadataTopologyHealthErr.Error(),
		})
	}
	r.recordClusterRuntimeFailureEvents(cluster, originalConditions)
	r.updateProductTierStatus(cluster)
	if err := r.observeHAPrimaryRouteStatus(ctx, cluster); err != nil {
		return err
	}
	haAdminStatusErr := r.observeHAPrimaryAdminStatusForReconcile(ctx, cluster)
	if standbyErr := r.observeHAStandbyAdminStatuses(ctx, cluster); standbyErr != nil && haAdminStatusErr == nil {
		haAdminStatusErr = standbyErr
	}
	if err := r.observeHAFencingStatus(ctx, cluster); err != nil {
		return err
	}
	if err := r.reconcileHAFencingLease(ctx, cluster); err != nil {
		return err
	}
	r.observeHAFormerPrimaryFenceStatus(ctx, cluster)
	// Preserve operator-owned rejoin assessment authority across the primary
	// runtime observation merge before deriving the next action plan.
	r.updateHAFormerPrimaryFromAdminJobs(ctx, cluster)
	r.updateHAStatusAndConditions(cluster)
	if haAdminStatusErr != nil {
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			haAdminStatusUnavailableReason(cluster, antflyv1.ReasonHAAdminStatusUnavailable),
			fmt.Sprintf("Unable to observe HA admin status: %v", haAdminStatusErr),
		)
	}
	// Revalidate physical isolation before releasing any dependent action.
	if err := r.reconcileHAFormerPrimaryIsolation(ctx, cluster); err != nil {
		return err
	}
	if err := r.reconcileHAAdminJobs(ctx, cluster); err != nil {
		if stderrors.Is(err, errHAPlanNeedsPersistence) {
			if persistErr := r.persistHAActionPlanBarrier(ctx, cluster); persistErr != nil {
				return persistErr
			}
			return errHAStatusCheckpointed
		}
		return err
	}
	r.updateHAStartupGateStatus(ctx, cluster)
	r.updateHALastPromotionFromAdminJobs(ctx, cluster)
	r.updateHAFormerPrimaryFromAdminJobs(ctx, cluster)
	if err := r.reconcileHAPrimaryRoute(ctx, cluster); err != nil {
		return err
	}
	r.updateHAAdminJobExecutionCondition(cluster)

	// Update ServiceMeshReady condition
	r.updateServiceMeshReadyCondition(cluster)

	return persist()
}

func preserveConditionDuringMetadataTopologyObservation(
	cluster *antflyv1.AntflyCluster,
	originalConditions []metav1.Condition,
	conditionType string,
	pending *metadataTopologyObservationPendingError,
) {
	if previous := meta.FindStatusCondition(originalConditions, conditionType); previous != nil {
		for i := range cluster.Status.Conditions {
			if cluster.Status.Conditions[i].Type == conditionType {
				cluster.Status.Conditions[i] = *previous
				return
			}
		}
		cluster.Status.Conditions = append(cluster.Status.Conditions, *previous)
		return
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionUnknown,
		ObservedGeneration: cluster.Generation,
		Reason:             pending.conditionReason,
		Message:            fmt.Sprintf("%s: %v", pending.waitMessage, pending),
	})
}

func (r *AntflyClusterReconciler) persistHAActionPlanBarrier(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if r == nil || r.Client == nil || cluster == nil {
		return fmt.Errorf("persist HA action plan barrier: cluster client is unavailable")
	}
	latest := &antflyv1.AntflyCluster{}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, latest); err != nil {
		return fmt.Errorf("persist HA action plan barrier: get latest cluster: %w", err)
	}
	if cluster.ResourceVersion != "" && latest.ResourceVersion != cluster.ResourceVersion {
		return errors.NewConflict(
			schema.GroupResource{Group: antflyv1.GroupVersion.Group, Resource: "antflyclusters"},
			cluster.Name,
			fmt.Errorf("resource changed before HA plan barrier"),
		)
	}
	latest.Status = cluster.DeepCopy().Status
	if err := r.Status().Update(ctx, latest); err != nil {
		return fmt.Errorf("persist HA action plan barrier status: %w", err)
	}
	cluster.ResourceVersion = latest.ResourceVersion
	return nil
}

func (r *AntflyClusterReconciler) reconcileHAAdminJobs(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Admin == nil || !ha.Admin.ExecutePlannedActions ||
		ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled ||
		cluster.Status.HAStatus == nil {
		return nil
	}
	// Dependency evidence can freeze payload fields (notably the former-primary
	// tail LSN) after an earlier action checkpoints. Apply those transformations
	// before the persistence preflight so a changed payload creates a plan-only
	// barrier instead of being overwritten by the older persisted action during
	// reservation.
	for i := range cluster.Status.HAStatus.PlannedActions {
		haBindPlannedActionToFrozenPrimaryBoundary(cluster.Status.HAStatus, &cluster.Status.HAStatus.PlannedActions[i])
	}
	sourceAuthorityChanged, err := r.bindHASeedSourcePVCAuthority(ctx, cluster)
	if err != nil {
		return err
	}
	if sourceAuthorityChanged {
		return errHAPlanNeedsPersistence
	}
	if err := r.requirePersistedHADirectActionPlan(ctx, cluster); err != nil {
		return err
	}

	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if strings.TrimSpace(action.AdminURL) == "" {
			if haPlannedActionRequiresAdminURL(*action) {
				action.AdminJobPhase = haAdminJobPhaseMissingAdminURL
				continue
			}
			// Controller-only actions are reconciled elsewhere. Portable artifact
			// actions are the only Job-backed operations that deliberately have no
			// HA admin URL.
			if !haPlannedActionKindIsPortableArtifact(haActionKind(action.Kind)) {
				continue
			}
		}
		if action.AdminJobPhase == haAdminJobPhaseFailed &&
			action.AdminJobName == haAdminDirectAPIName &&
			haAdminActionMissingTokenCanFallbackFromStatus(cluster, ha.Admin, *action) {
			if err := r.resetPersistedHADirectFailureForJobFallback(ctx, cluster, action); err != nil {
				return err
			}
			return errHAStatusCheckpointed
		}
		if action.AdminJobPhase == haAdminJobPhaseWaitingJobFallback {
			if err := r.reconcileHAAdminJob(ctx, cluster, ha.Admin, action); err != nil {
				return err
			}
			return nil
		}
		if haActionKind(action.Kind) == haActionActivateSeedArtifact && action.AdminJobPhase == haAdminJobPhaseSucceeded {
			current, err := r.haActivationReceiptMatchesCurrentTarget(ctx, cluster, *action)
			if err != nil {
				return err
			}
			if !current {
				action.AdminJobName = ""
				action.AdminJobPhase = ""
				action.AdminError = ""
				action.AdminStatusCode = 0
				action.SeedArtifactReceipt = nil
				resetHAActionAttemptStatus(action)
			}
		}
		if action.AdminJobPhase == haAdminJobPhaseSucceeded ||
			(action.AdminJobPhase == haAdminJobPhaseFailed && action.AdminJobName != haAdminDirectAPIName) {
			if err := r.ensureHAAdminJobTTLAfterCheckpoint(ctx, cluster, ha.Admin, action); err != nil {
				return err
			}
			if action.AdminJobPhase == haAdminJobPhaseSucceeded && action.AdminResult == nil {
				r.updateHAAdminActionResultFromJobLogs(ctx, cluster, action)
			}
			continue
		}
		if !haPlannedActionDependenciesSucceededForStatus(cluster.Status.HAStatus, cluster.Status.HAStatus.PlannedActions, i, cluster) {
			if action.AdminJobName == "" && action.AdminJobPhase == "" {
				action.AdminJobPhase = haAdminJobPhaseWaitingDependency
			}
			continue
		}
		if haActionKind(action.Kind) == haActionPromoteStandby &&
			action.FenceAuthority == antflyv1.HAFencingAuthorityKubernetesLease &&
			haRuntimeLeaseWatchdogEnabled(cluster) {
			ready, err := r.haCurrentLeaseAuthorizesPromotionBoundary(ctx, cluster, *action)
			if err != nil {
				return err
			}
			if !ready {
				action.AdminJobPhase = haAdminJobPhaseWaitingDependency
				continue
			}
		}
		now := r.haNow()
		if action.Executor != string(haActionExecutorCLIJob) && haPlannedActionSupportsDirectAdminAPI(haActionKind(action.Kind)) {
			reserved, checkpointed, err := r.reserveHADirectAdminAttempt(ctx, cluster, ha.Admin, action, now)
			if err != nil {
				return fmt.Errorf("reserve HA direct action %s: %w", action.Kind, err)
			}
			if !reserved {
				if checkpointed {
					return errHAStatusCheckpointed
				}
				continue
			}
			attemptID := action.AttemptID
			handled, executionErr := r.executeHAPlannedActionTyped(ctx, cluster, action)
			if !handled {
				executionErr = fmt.Errorf("HA action %s has no typed /admin/v1 request implementation", action.Kind)
			}
			if executionErr != nil && haAdminActionCanRunAsFallbackJob(cluster, ha.Admin, *action, executionErr) {
				if err := r.releaseHADirectReservationForJobFallback(ctx, cluster, action, attemptID); err != nil {
					return err
				}
				return errHAStatusCheckpointed
			}
			if err := r.checkpointHADirectAdminResult(ctx, cluster, ha.Admin, action, attemptID, now, executionErr); err != nil {
				return fmt.Errorf("checkpoint HA direct action %s attempt %s: %w", action.Kind, attemptID, err)
			}
			return errHAStatusCheckpointed
		}
		if action.Executor == string(haActionExecutorAdminAPI) {
			action.AdminJobName = haAdminDirectAPIName
			action.AdminJobPhase = haAdminJobPhaseFailed
			action.AdminError = fmt.Sprintf("HA action %s is marked AdminAPI but no typed /admin/v1 request could be executed", action.Kind)
			action.Retryable = false
			action.ErrorClass = "UnsupportedAdminAction"
			action.CompletedAt = haActionTime(now)
			haObserveActionFailure(action, haMetricExecutorDirect, true)
			continue
		}
		if action.Executor != string(haActionExecutorCLIJob) {
			continue
		}
		if haPlannedActionSupportsDirectAdminAPI(haActionKind(action.Kind)) {
			action.AdminJobPhase = haAdminJobPhaseFailed
			action.AdminError = fmt.Sprintf("HA action %s is marked CLIJob but has a typed /admin/v1 request; use AdminAPI execution or a pod-local action without a typed admin operation", action.Kind)
			continue
		}
		if len(action.AdminCommand) == 0 {
			continue
		}

		if err := r.reconcileHAAdminJob(ctx, cluster, ha.Admin, action); err != nil {
			return err
		}
	}
	return nil
}

func (r *AntflyClusterReconciler) bindHASeedSourcePVCAuthority(ctx context.Context, cluster *antflyv1.AntflyCluster) (bool, error) {
	if r == nil || r.Client == nil || cluster == nil || cluster.Status.HAStatus == nil {
		return false, nil
	}
	resolvedUIDs := make(map[string]string)
	changed := false
	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if !haPlannedActionKindUsesSeedSourceAuthority(haActionKind(action.Kind)) ||
			strings.TrimSpace(action.TopologyID) == "" {
			continue
		}
		artifact := haSeedArtifactForAction(cluster, *action)
		if artifact == nil || artifact.SourcePVC == nil || artifact.TargetPVC == nil {
			return false, fmt.Errorf("HA %s requires distinct configured source and target PVCs", action.Kind)
		}
		sourceName := strings.TrimSpace(artifact.SourcePVC.ClaimName)
		if sourceName == "" || sourceName == strings.TrimSpace(artifact.TargetPVC.ClaimName) {
			return false, fmt.Errorf("HA %s requires one distinct configured source PVC", action.Kind)
		}
		if action.TopologyGeneration <= 0 || strings.TrimSpace(action.TopologyNodeID) == "" ||
			strings.TrimSpace(action.TargetPVCName) == "" || strings.TrimSpace(action.TargetPVCUID) == "" ||
			action.TopologyID != strings.TrimSpace(artifact.TopologyID) ||
			action.TopologyGeneration != artifact.TopologyGeneration ||
			action.TopologyNodeID != strings.TrimSpace(artifact.NodeID) ||
			action.TargetPVCName != strings.TrimSpace(artifact.TargetPVC.ClaimName) ||
			action.TargetPVCUID != strings.TrimSpace(artifact.TargetPVCUID) {
			return false, fmt.Errorf("HA %s source PVC authority belongs to a stale topology or target incarnation", action.Kind)
		}
		if existing := strings.TrimSpace(action.SourcePVCName); existing != "" && existing != sourceName {
			return false, fmt.Errorf("HA %s source PVC identity is stale: planned name %s, live name %s", action.Kind, existing, sourceName)
		}
		uid, ok := resolvedUIDs[sourceName]
		if !ok {
			pvc := &corev1.PersistentVolumeClaim{}
			key := types.NamespacedName{Name: sourceName, Namespace: cluster.Namespace}
			if err := r.Get(ctx, key, pvc); err != nil {
				return false, fmt.Errorf("read source PVC %s for HA seed authority: %w", sourceName, err)
			}
			uid = strings.TrimSpace(string(pvc.UID))
			if uid == "" {
				return false, fmt.Errorf("HA source PVC %s has no durable UID", sourceName)
			}
			resolvedUIDs[sourceName] = uid
		}
		if existing := strings.TrimSpace(action.SourcePVCUID); existing != "" && existing != uid {
			return false, fmt.Errorf("HA %s source PVC identity is stale: planned UID %s, live UID %s", action.Kind, existing, uid)
		}
		if action.SourcePVCName != sourceName || action.SourcePVCUID != uid {
			action.SourcePVCName = sourceName
			action.SourcePVCUID = uid
			changed = true
		}
	}
	return changed, nil
}

func (r *AntflyClusterReconciler) requirePersistedHADirectActionPlan(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if r == nil || r.Client == nil || cluster == nil || cluster.Status.HAStatus == nil {
		return nil
	}
	latest := &antflyv1.AntflyCluster{}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	// Direct-action execution is a read-after-write protocol. The controller
	// cache is allowed to lag a just-persisted reservation, so use the uncached
	// boundary reader before deciding which exact attempt may execute.
	if err := r.haBoundaryReader().Get(ctx, key, latest); err != nil {
		return fmt.Errorf("read persisted HA action plan: %w", err)
	}
	if latest.Generation != cluster.Generation {
		return fmt.Errorf("%w: planned generation %d, latest generation %d", errHAPlanNeedsPersistence, cluster.Generation, latest.Generation)
	}
	for i := range cluster.Status.HAStatus.PlannedActions {
		desired := &cluster.Status.HAStatus.PlannedActions[i]
		if desired.Executor == string(haActionExecutorCLIJob) || !haPlannedActionSupportsDirectAdminAPI(haActionKind(desired.Kind)) {
			continue
		}
		if strings.TrimSpace(desired.OperationID) == "" {
			desired.OperationID = haPlannedActionOperationID(*desired)
		}
		if latest.Status.HAStatus == nil {
			return fmt.Errorf("%w: operation %s is not in status", errHAPlanNeedsPersistence, desired.OperationID)
		}
		match := -1
		for j := range latest.Status.HAStatus.PlannedActions {
			candidate := &latest.Status.HAStatus.PlannedActions[j]
			if !haSamePlannedActionIdentity(*desired, *candidate) {
				continue
			}
			if match >= 0 {
				return fmt.Errorf("persisted HA operation identity %s is ambiguous", desired.OperationID)
			}
			match = j
		}
		if match < 0 {
			return fmt.Errorf("%w: operation %s is absent or was concurrently replaced", errHAPlanNeedsPersistence, desired.OperationID)
		}
		persisted := &latest.Status.HAStatus.PlannedActions[match]
		if !haSamePlannedActionOperation(*desired, *persisted) {
			if haPlannedActionExecutionStarted(*persisted) {
				*desired = *persisted.DeepCopy()
				continue
			}
			return fmt.Errorf("%w: operation %s payload changed before execution", errHAPlanNeedsPersistence, desired.OperationID)
		}
		// Narrow status checkpoints may have landed after this reconcile read the
		// object. Always execute from the latest exact persisted action state.
		*desired = *persisted.DeepCopy()
		if strings.TrimSpace(desired.OperationID) == "" {
			desired.OperationID = haPlannedActionOperationID(*desired)
		}
	}
	return nil
}

func (r *AntflyClusterReconciler) updateHAStartupGateStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	gate := haRuntimeStartupGate(cluster)
	if gate == nil {
		if cluster != nil && cluster.Status.HAStatus != nil {
			cluster.Status.HAStatus.StartupGate = nil
		}
		return
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{}
	}
	if gate.Policy == antflyv1.HAStartupGatePolicySuspend {
		cluster.Status.HAStatus.StartupGate = &antflyv1.HAStartupGateStatus{
			RuntimeEligible: false,
			Reason:          "PolicySuspended",
		}
		return
	}
	var previousReceipt *antflyv1.HASeedActivationReceiptStatus
	if cluster.Status.HAStatus.StartupGate != nil && cluster.Status.HAStatus.StartupGate.ActivationReceipt != nil {
		previousReceipt = cluster.Status.HAStatus.StartupGate.ActivationReceipt.DeepCopy()
	}
	status := &antflyv1.HAStartupGateStatus{Reason: "ActivationReceiptNotObserved"}
	cluster.Status.HAStatus.StartupGate = status
	if gate.Policy != antflyv1.HAStartupGatePolicyRequireActivatedSeed || gate.RequiredReceipt == nil {
		status.Reason = "UnsupportedPolicy"
		return
	}
	required := *gate.RequiredReceipt

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Name: required.TargetPVCName, Namespace: cluster.Namespace}, pvc); err != nil {
		status.Reason = "TargetPVCNotObserved"
		return
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil {
		status.Reason = "ReplicationIdentityMissing"
		return
	}
	actionSets := [][]antflyv1.HAPlannedActionStatus{cluster.Status.HAStatus.PlannedActions}
	peers := &antflyv1.AntflyClusterList{}
	if err := r.List(ctx, peers, client.InNamespace(cluster.Namespace)); err != nil {
		status.Reason = "ActivationReceiptSourceLookupFailed"
		return
	}
	for i := range peers.Items {
		peer := &peers.Items[i]
		if peer.Name == cluster.Name || peer.Status.HAStatus == nil {
			continue
		}
		actionSets = append(actionSets, peer.Status.HAStatus.PlannedActions)
	}
	for _, actions := range actionSets {
		for i := range actions {
			action := actions[i]
			if haActionKind(action.Kind) != haActionActivateSeedArtifact ||
				strings.TrimSpace(action.SlotName) != strings.TrimSpace(required.SlotName) ||
				strings.TrimSpace(action.SeedArtifactGeneration) != strings.TrimSpace(required.Generation) ||
				!haAdminActionSucceededWithEvidence(action) {
				continue
			}
			matchesCurrentTarget, err := r.haActivationReceiptMatchesCurrentTarget(ctx, cluster, action)
			if err != nil {
				status.Reason = "ActivationReceiptSourceLookupFailed"
				return
			}
			if !matchesCurrentTarget {
				continue
			}
			receipt := action.SeedArtifactReceipt
			if !haSeedReceiptMatchesRuntimeLineage(cluster, receipt.ClusterID, receipt.ShardID, receipt.TableID, receipt.TimelineID, receipt.Epoch) {
				continue
			}
			activationReceipt := &antflyv1.HASeedActivationReceiptStatus{
				TopologyID:                  receipt.TopologyID,
				TopologyGeneration:          receipt.TopologyGeneration,
				NodeID:                      receipt.NodeID,
				SlotName:                    receipt.SlotName,
				Generation:                  receipt.Generation,
				TargetPVCName:               receipt.TargetPVCName,
				TargetPVCUID:                receipt.TargetPVCUID,
				ClusterID:                   receipt.ClusterID,
				ShardID:                     receipt.ShardID,
				TableID:                     receipt.TableID,
				TimelineID:                  receipt.TimelineID,
				Epoch:                       receipt.Epoch,
				BackupLSN:                   receipt.BackupLSN,
				CheckpointLSN:               receipt.CheckpointLSN,
				ManifestID:                  receipt.ManifestID,
				ManifestSHA256:              receipt.ManifestSHA256,
				AggregateSHA256:             receipt.AggregateSHA256,
				SeedReceiptSHA256:           receipt.SeedReceiptSHA256,
				CaptureReceiptSHA256:        receipt.CaptureReceiptSHA256,
				MaterializedReceiptSHA256:   receipt.MaterializedReceiptSHA256,
				MaterializedAggregateSHA256: receipt.MaterializedAggregateSHA256,
				TargetLocalNodeID:           receipt.TargetLocalNodeID,
				TargetReplicaID:             receipt.TargetReplicaID,
				GenerationPath:              receipt.GenerationPath,
				RawGenerationPath:           receipt.RawGenerationPath,
			}
			targetGenerationGCObserved := false
			for j := range actions {
				gc := actions[j]
				if haActionKind(gc.Kind) != haActionGCTargetSeedGenerations ||
					strings.TrimSpace(gc.SlotName) != strings.TrimSpace(required.SlotName) ||
					strings.TrimSpace(gc.SeedArtifactGeneration) != strings.TrimSpace(required.Generation) {
					continue
				}
				targetGenerationGCObserved = true
				if !haAdminActionSucceededWithEvidence(gc) {
					status.Reason = "TargetGenerationGCNotObserved"
					return
				}
				break
			}
			if !targetGenerationGCObserved {
				// The action chain is planned incrementally. Publishing the activation
				// receipt before target GC exists lets Colony open the runtime gate;
				// the resulting live PVC consumer then prevents the mandatory GC Job
				// from ever starting. Withhold handoff authority until that exact
				// cleanup action is both present and durably successful.
				status.Reason = "TargetGenerationGCNotObserved"
				return
			}
			status.ActivationReceipt = activationReceipt
			// Seed the status as eligible only for the exact matcher invocation;
			// declarative suspension and every configured evidence mismatch still win.
			status.RuntimeEligible = true
			eligible, reason := haStartupGateRuntimeEligible(cluster, pvc)
			status.RuntimeEligible = eligible
			status.Reason = reason
			return
		}
	}
	// Once the operator has validated a materialized receipt, keep that exact
	// receipt available after the primary's bounded action history advances.
	// Revalidate it against the current declarative identity and live PVC on
	// every pass; a topology, digest, or PVC-incarnation change still closes the
	// gate. Without this monotonic handoff, the standby can start, lose its peer
	// receipt on the next observation, and be suspended again indefinitely.
	if eligible, reason := haStartupGateActivationReceiptMatches(cluster, pvc, previousReceipt); eligible {
		status.ActivationReceipt = previousReceipt
		status.RuntimeEligible = gate.RuntimeEligible
		if gate.RuntimeEligible {
			status.Reason = reason
		} else {
			status.Reason = "DeclarativelySuspended"
		}
		return
	}
	if !gate.RuntimeEligible {
		status.Reason = "DeclarativelySuspended"
	}
}

func haBindPlannedActionToFrozenPrimaryBoundary(status *antflyv1.HAStatus, action *antflyv1.HAPlannedActionStatus) {
	if status == nil || action == nil {
		return
	}
	switch haActionKind(action.Kind) {
	case haActionAcquireFence, haActionAssessPromotion, haActionPromoteStandby, haActionUpdatePrimaryRoute, haActionDemoteFormerPrimary:
	default:
		return
	}
	promotedNodeID := strings.TrimSpace(action.FenceHolder)
	if promotedNodeID == "" && haActionKind(action.Kind) != haActionDemoteFormerPrimary {
		promotedNodeID = strings.TrimSpace(action.StandbyName)
	}
	frozen := haSucceededFormerPrimaryFenceResult(status, promotedNodeID, action.FenceGeneration)
	if frozen == nil {
		return
	}
	action.TargetLSN = frozen.FenceRequiredLSN
	if haActionKind(action.Kind) == haActionDemoteFormerPrimary {
		action.ObservedLSN = frozen.FenceRequiredLSN
	}
}

const haAdminDirectAPIName = "direct-admin-api"

var errHAAdminTokenEnvMissing = stderrors.New("configured HA admin token env var is empty or unset")
var errHAPromotionBoundaryNotApplied = stderrors.New("standby has not applied the frozen former-primary boundary")
var errHAStatusCheckpointed = stderrors.New("HA status checkpointed; requeue before further side effects")
var errHAPlanNeedsPersistence = stderrors.New("HA action plan must be persisted before execution")
var errHADirectOperationNotPersisted = stderrors.New("HA direct operation is absent from latest persisted plan")

type haDirectActionMutation func(*antflyv1.HAStatus, *antflyv1.HAPlannedActionStatus) (bool, error)

// mutatePersistedHADirectAction applies a narrow status mutation against the
// latest resource version. It is the durability boundary around external typed
// admin requests: neither an outer status conflict nor a later route error can
// erase a reserved attempt or its result.
func (r *AntflyClusterReconciler) mutatePersistedHADirectAction(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	action *antflyv1.HAPlannedActionStatus,
	mutate haDirectActionMutation,
) (bool, error) {
	if r == nil || r.Client == nil || cluster == nil || action == nil || strings.TrimSpace(cluster.Name) == "" {
		return false, fmt.Errorf("durable HA direct action checkpoint requires a persisted AntflyCluster")
	}
	desired := action.DeepCopy()
	if strings.TrimSpace(desired.OperationID) == "" {
		desired.OperationID = haPlannedActionOperationID(*desired)
	}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	var persisted antflyv1.HAPlannedActionStatus
	var resourceVersion string
	checkpointed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &antflyv1.AntflyCluster{}
		// Reservations and results are consecutive durable checkpoints around an
		// external side effect. Bypass the informer cache to guarantee read-your-
		// writes semantics; otherwise a fast Admin API response can arrive before
		// the reservation is visible through the cache and be replayed after the
		// reservation expires.
		if err := r.haBoundaryReader().Get(ctx, key, latest); err != nil {
			return fmt.Errorf("get latest AntflyCluster: %w", err)
		}
		statusBase := latest.DeepCopy()
		if latest.Generation != cluster.Generation {
			return fmt.Errorf("HA direct action %s was planned for generation %d, latest generation is %d", desired.OperationID, cluster.Generation, latest.Generation)
		}
		if latest.Status.HAStatus == nil {
			latest.Status.HAStatus = &antflyv1.HAStatus{}
		}
		match := -1
		for i := range latest.Status.HAStatus.PlannedActions {
			candidate := &latest.Status.HAStatus.PlannedActions[i]
			if !haSamePlannedActionIdentity(*desired, *candidate) {
				continue
			}
			if match >= 0 {
				return fmt.Errorf("HA operation identity %s is ambiguous in persisted status", desired.OperationID)
			}
			match = i
		}
		if match < 0 {
			return fmt.Errorf("%w: %s", errHADirectOperationNotPersisted, desired.OperationID)
		}
		persistedAction := &latest.Status.HAStatus.PlannedActions[match]
		if strings.TrimSpace(persistedAction.OperationID) == "" {
			persistedAction.OperationID = desired.OperationID
		}
		if !haSamePlannedActionOperation(*desired, *persistedAction) {
			return fmt.Errorf("%w: operation %s request payload changed before checkpoint", errHADirectOperationNotPersisted, desired.OperationID)
		}
		changed, err := mutate(latest.Status.HAStatus, persistedAction)
		if err != nil {
			return err
		}
		if changed {
			patch := client.MergeFromWithOptions(statusBase, client.MergeFromWithOptimisticLock{})
			if err := r.Status().Patch(ctx, latest, patch); err != nil {
				return fmt.Errorf("update AntflyCluster status checkpoint: %w", err)
			}
			checkpointed = true
		}
		persisted = *persistedAction.DeepCopy()
		resourceVersion = latest.ResourceVersion
		return nil
	})
	if err != nil {
		return false, err
	}
	cluster.ResourceVersion = resourceVersion
	*action = persisted
	return checkpointed, nil
}

func migrateLegacyHADirectActionStatus(action *antflyv1.HAPlannedActionStatus, admin *antflyv1.HAAdminSpec, now time.Time) bool {
	if action == nil || action.AdminJobName != haAdminDirectAPIName || action.ExecutionStateVersion != 0 {
		return false
	}
	switch action.AdminJobPhase {
	case haAdminJobPhasePending, haAdminJobPhaseRunning, haAdminJobPhaseFailed:
	default:
		return false
	}
	action.AttemptCount = 1
	action.ExecutionStateVersion = 1
	if action.FirstAttemptAt == nil {
		action.FirstAttemptAt = haActionTime(now)
	}
	if action.LastAttemptAt == nil {
		action.LastAttemptAt = haActionTime(now)
	}
	if action.AdminJobPhase == haAdminJobPhasePending && strings.TrimSpace(action.AdminError) == "" ||
		action.AdminJobPhase == haAdminJobPhaseRunning {
		action.AdminJobPhase = haAdminJobPhaseRunning
		action.InFlightAttempt = 1
		action.AttemptID = fmt.Sprintf("%s/attempt-%d", action.OperationID, action.AttemptCount)
		action.ReservationExpiresAt = haActionTime(now.Add(haDirectAdminReservation(admin)))
		action.Retryable = false
		action.ErrorClass = "LegacyInFlight"
		return true
	}
	if action.AdminJobPhase == haAdminJobPhasePending || action.AdminJobPhase == haAdminJobPhaseFailed {
		action.RetryBudgetUsed = 1
		action.AdminJobPhase = haAdminJobPhasePending
		action.Retryable = true
		action.ErrorClass = "LegacyBoundedRetry"
		action.NextRetryAt = haActionTime(now)
		action.CompletedAt = nil
		return true
	}
	return true
}

func (r *AntflyClusterReconciler) reserveHADirectAdminAttempt(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	admin *antflyv1.HAAdminSpec,
	action *antflyv1.HAPlannedActionStatus,
	now time.Time,
) (bool, bool, error) {
	reservationNonce := ""
	ensureReservationNonce := func() error {
		if reservationNonce != "" {
			return nil
		}
		nonceBytes := make([]byte, 16)
		if _, err := rand.Read(nonceBytes); err != nil {
			return fmt.Errorf("generate HA direct reservation nonce: %w", err)
		}
		reservationNonce = hex.EncodeToString(nonceBytes)
		return nil
	}
	reserved := false
	expiredReservation := false
	checkpointed, err := r.mutatePersistedHADirectAction(ctx, cluster, action, func(_ *antflyv1.HAStatus, current *antflyv1.HAPlannedActionStatus) (bool, error) {
		// RetryOnConflict may invoke this closure more than once. These outcomes
		// must describe only the final successful/current resource version, never
		// a mutation that lost a conflict to another reservation owner.
		reserved = false
		expiredReservation = false
		migrated := migrateLegacyHADirectActionStatus(current, admin, now)
		if migrated {
			return true, nil
		}
		changed := false
		if current.AdminJobPhase == haAdminJobPhaseSucceeded || current.AdminJobPhase == haAdminJobPhaseFailed {
			return changed, nil
		}
		if current.InFlightAttempt > 0 {
			if current.ReservationExpiresAt != nil && current.ReservationExpiresAt.After(now) {
				return changed, nil
			}
			// The operator may have crashed after sending the request. Consume that
			// uncertain attempt, then replay only after expiry with the exact frozen
			// payload; runtime receipts make the replay idempotent.
			current.InFlightAttempt = 0
			current.AttemptID = ""
			current.ReservationExpiresAt = nil
			current.RetryBudgetUsed++
			current.AdminError = "in-flight HA admin reservation expired without a durable result; replay is charged to the retry budget"
			current.ErrorClass = "ReservationExpired"
			expiredReservation = true
			changed = true
		}
		if current.AdminJobPhase == haAdminJobPhaseWaitingPrerequisite &&
			current.PrerequisiteDeadlineAt != nil && !current.PrerequisiteDeadlineAt.After(now) {
			current.AdminJobPhase = haAdminJobPhaseFailed
			current.ErrorClass = "PromotionPrerequisiteTimeout"
			current.Retryable = false
			current.CompletedAt = haActionTime(now)
			current.NextRetryAt = nil
			return true, nil
		}
		if current.NextRetryAt != nil && current.NextRetryAt.After(now) {
			return changed, nil
		}
		if current.RetryBudgetUsed >= haDirectAdminRetryLimit(admin) {
			current.AdminJobPhase = haAdminJobPhaseFailed
			current.ErrorClass = "RetryBudgetExhausted"
			current.Retryable = false
			current.CompletedAt = haActionTime(now)
			current.NextRetryAt = nil
			return true, nil
		}
		if err := ensureReservationNonce(); err != nil {
			return false, err
		}
		current.AttemptCount++
		current.ExecutionStateVersion = 1
		current.InFlightAttempt = current.AttemptCount
		current.AttemptID = fmt.Sprintf("%s/attempt-%d/%s", current.OperationID, current.AttemptCount, reservationNonce)
		current.ReservationExpiresAt = haActionTime(now.Add(haDirectAdminReservation(admin)))
		if current.FirstAttemptAt == nil {
			current.FirstAttemptAt = haActionTime(now)
		}
		current.LastAttemptAt = haActionTime(now)
		current.AdminJobName = haAdminDirectAPIName
		current.AdminJobPhase = haAdminJobPhaseRunning
		current.AdminError = ""
		current.AdminStatusCode = 0
		current.Retryable = false
		current.ErrorClass = ""
		current.NextRetryAt = nil
		current.CompletedAt = nil
		reserved = true
		return true, nil
	})
	if err != nil {
		return false, false, err
	}
	if reserved {
		haObserveActionAttempts(action, haMetricExecutorDirect, 1)
	}
	if expiredReservation {
		metricAction := action.DeepCopy()
		terminal := metricAction.AdminJobPhase == haAdminJobPhaseFailed
		if !terminal {
			metricAction.ErrorClass = "ReservationExpired"
		}
		haObserveActionFailure(metricAction, haMetricExecutorDirect, terminal)
	}
	return reserved, checkpointed, nil
}

func (r *AntflyClusterReconciler) checkpointHADirectAdminResult(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	admin *antflyv1.HAAdminSpec,
	action *antflyv1.HAPlannedActionStatus,
	attemptID string,
	now time.Time,
	executionErr error,
) error {
	result := action.DeepCopy()
	var promotion *antflyv1.HAPromotionStatus
	if executionErr == nil && action.Kind == string(haActionPromoteStandby) && cluster.Status.HAStatus != nil {
		if receipt := haPromotionReceipt(cluster.Status.HAStatus); receipt != nil {
			promotion = receipt.DeepCopy()
		}
	}
	_, err := r.mutatePersistedHADirectAction(ctx, cluster, action, func(status *antflyv1.HAStatus, current *antflyv1.HAPlannedActionStatus) (bool, error) {
		if current.InFlightAttempt == 0 || current.AttemptID != attemptID {
			return false, fmt.Errorf("HA direct action result attempt %q does not match persisted reservation %q", attemptID, current.AttemptID)
		}
		current.AdminResult = nil
		if result.AdminResult != nil {
			current.AdminResult = result.AdminResult.DeepCopy()
		}
		current.SeedArtifactReceipt = nil
		if result.SeedArtifactReceipt != nil {
			current.SeedArtifactReceipt = result.SeedArtifactReceipt.DeepCopy()
		}
		current.InFlightAttempt = 0
		current.AttemptID = ""
		current.ReservationExpiresAt = nil
		current.AdminJobName = haAdminDirectAPIName
		current.NextRetryAt = nil
		current.CompletedAt = nil
		if stderrors.Is(executionErr, errHAPromotionBoundaryNotApplied) {
			// A valid typed assessment reported replication progress, not a request
			// failure. It therefore does not consume the request-failure budget.
			current.AdminJobPhase = haAdminJobPhaseWaitingPrerequisite
			current.AdminError = executionErr.Error()
			current.AdminStatusCode = 0
			current.Retryable = false
			current.ErrorClass = "PromotionBoundaryNotApplied"
			current.NextRetryAt = haActionTime(now.Add(haDirectAdminRetryBase(admin)))
			if current.PrerequisiteDeadlineAt == nil {
				current.PrerequisiteDeadlineAt = haActionTime(now.Add(haDirectPrerequisiteTimeout(admin)))
			}
			if !current.PrerequisiteDeadlineAt.After(now) {
				current.AdminJobPhase = haAdminJobPhaseFailed
				current.ErrorClass = "PromotionPrerequisiteTimeout"
				current.NextRetryAt = nil
				current.CompletedAt = haActionTime(now)
			}
			return true, nil
		}
		current.PrerequisiteDeadlineAt = nil
		if executionErr == nil {
			if promotion != nil {
				status.LastPromotion = promotion.DeepCopy()
			}
			current.AdminJobPhase = haAdminJobPhaseSucceeded
			current.AdminError = ""
			current.AdminStatusCode = 0
			current.Retryable = false
			current.ErrorClass = ""
			current.CompletedAt = haActionTime(now)
			return true, nil
		}
		retryable := adminsdk.HAIsRetryable(executionErr)
		current.AdminError = executionErr.Error()
		current.ErrorClass = haDirectAdminErrorClass(executionErr)
		if statusCode, ok := adminsdk.HAStatusCode(executionErr); ok {
			current.AdminStatusCode = statusCode
		} else {
			current.AdminStatusCode = 0
		}
		if retryable {
			current.RetryBudgetUsed++
		}
		if retryable && current.RetryBudgetUsed < haDirectAdminRetryLimit(admin) {
			current.AdminJobPhase = haAdminJobPhasePending
			current.Retryable = true
			current.NextRetryAt = haActionTime(now.Add(haDirectAdminRetryDelay(admin, current.RetryBudgetUsed)))
			return true, nil
		}
		current.AdminJobPhase = haAdminJobPhaseFailed
		current.Retryable = false
		if retryable {
			current.ErrorClass = "RetryBudgetExhausted"
		}
		current.CompletedAt = haActionTime(now)
		return true, nil
	})
	if err != nil {
		return err
	}
	if executionErr == nil {
		haObserveActionSuccess(action, haMetricExecutorDirect)
	} else if stderrors.Is(executionErr, errHAPromotionBoundaryNotApplied) {
		haObserveActionWait(action, "promotion_boundary")
	} else {
		haObserveActionFailure(action, haMetricExecutorDirect, action.AdminJobPhase == haAdminJobPhaseFailed)
	}
	return nil
}

func (r *AntflyClusterReconciler) releaseHADirectReservationForJobFallback(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	action *antflyv1.HAPlannedActionStatus,
	attemptID string,
) error {
	_, err := r.mutatePersistedHADirectAction(ctx, cluster, action, func(_ *antflyv1.HAStatus, current *antflyv1.HAPlannedActionStatus) (bool, error) {
		if current.InFlightAttempt == 0 || current.AttemptID != attemptID {
			return false, fmt.Errorf("HA direct fallback attempt %q does not match persisted reservation %q", attemptID, current.AttemptID)
		}
		if current.AttemptCount > 0 {
			current.AttemptCount--
		}
		current.InFlightAttempt = 0
		current.AttemptID = ""
		current.ReservationExpiresAt = nil
		current.AdminJobName = ""
		current.AdminJobPhase = haAdminJobPhaseWaitingJobFallback
		current.AdminError = ""
		current.AdminStatusCode = 0
		current.Retryable = false
		current.ErrorClass = "JobFallbackReady"
		current.NextRetryAt = nil
		current.CompletedAt = nil
		if current.AttemptCount == 0 {
			current.FirstAttemptAt = nil
			current.LastAttemptAt = nil
		}
		return true, nil
	})
	return err
}

func (r *AntflyClusterReconciler) resetPersistedHADirectFailureForJobFallback(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	action *antflyv1.HAPlannedActionStatus,
) error {
	_, err := r.mutatePersistedHADirectAction(ctx, cluster, action, func(_ *antflyv1.HAStatus, current *antflyv1.HAPlannedActionStatus) (bool, error) {
		current.AdminJobName = ""
		current.AdminJobPhase = haAdminJobPhaseWaitingJobFallback
		current.AdminError = ""
		current.AdminStatusCode = 0
		resetHAActionAttemptStatus(current)
		current.AdminJobPhase = haAdminJobPhaseWaitingJobFallback
		current.ErrorClass = "JobFallbackReady"
		return true, nil
	})
	return err
}

func (r *AntflyClusterReconciler) reconcileHAAdminJob(ctx context.Context, cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action *antflyv1.HAPlannedActionStatus) error {
	previousPhase := action.AdminJobPhase
	previousAttemptCount := action.AttemptCount
	jobAction, ready, err := r.haActivationJobAction(ctx, cluster, *action)
	if err != nil {
		return err
	}
	if !ready {
		action.AdminJobName = ""
		action.AdminJobPhase = haAdminJobPhasePending
		return nil
	}
	jobAction, ready, err = r.haPortableArtifactJobAction(ctx, cluster, jobAction)
	if err != nil {
		return err
	}
	if !ready {
		action.AdminJobName = ""
		action.AdminJobPhase = haAdminJobPhasePending
		return nil
	}
	// Portable source jobs discover the exact source PVC incarnation immediately
	// before their first side effect. Freeze that identity into status before the
	// deterministic Job hash is rendered so retries cannot silently retarget a
	// replacement claim.
	action.SourcePVCName = jobAction.SourcePVCName
	action.SourcePVCUID = jobAction.SourcePVCUID
	if haActionKind(jobAction.Kind) == haActionGCTargetSeedGenerations {
		if err := r.ensureHASeededSlotActivationReceiptConfigMap(ctx, cluster, jobAction); err != nil {
			return err
		}
	}
	job := buildHAAdminJob(cluster, admin, jobAction)
	action.AdminJobName = job.Name
	if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
		return err
	}

	existing := &batchv1.Job{}
	err = r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, existing)
	if errors.IsNotFound(err) {
		if err := r.bindHAAdminJobToPVCConsumer(ctx, job); err != nil {
			return err
		}
		action.Retryable = true
		action.ErrorClass = ""
		action.CompletedAt = nil
		action.AdminJobPhase = haAdminJobPhasePending
		if err := r.Create(ctx, job); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	if haPlannedActionKindIsPortableArtifact(haActionKind(jobAction.Kind)) &&
		(!haSeedIdentityAnnotationsEqual(existing.Annotations, job.Annotations) ||
			!haSeedIdentityAnnotationsEqual(existing.Spec.Template.Annotations, job.Spec.Template.Annotations)) {
		return fmt.Errorf("HA admin Job %s immutable seed identity annotations do not match the exact planned action", existing.Name)
	}
	action.AdminJobPhase = haAdminJobPhase(existing)
	observedAttempts, firstAttemptAt, lastAttemptAt, err := r.observeHAAdminJobPodAttempts(ctx, existing)
	if err != nil {
		return err
	}
	// Terminal Job counters are retained as a conservative fallback if a pod was
	// manually deleted before observation. RestartPolicyNever makes each retained
	// pod correspond to exactly one process attempt.
	terminalCounterAttempts := existing.Status.Failed + existing.Status.Succeeded
	if terminalCounterAttempts > observedAttempts {
		observedAttempts = terminalCounterAttempts
	}
	if observedAttempts == 0 && (action.AdminJobPhase == haAdminJobPhaseSucceeded || action.AdminJobPhase == haAdminJobPhaseFailed) {
		observedAttempts = 1
	}
	if observedAttempts > action.AttemptCount {
		action.AttemptCount = observedAttempts
	}
	if firstAttemptAt != nil {
		if action.FirstAttemptAt == nil || firstAttemptAt.Before(action.FirstAttemptAt) {
			action.FirstAttemptAt = firstAttemptAt.DeepCopy()
		}
	}
	if lastAttemptAt != nil {
		action.LastAttemptAt = lastAttemptAt.DeepCopy()
	} else if existing.Status.StartTime != nil {
		if action.FirstAttemptAt == nil {
			action.FirstAttemptAt = existing.Status.StartTime.DeepCopy()
		}
		action.LastAttemptAt = existing.Status.StartTime.DeepCopy()
	}
	switch action.AdminJobPhase {
	case haAdminJobPhaseSucceeded:
		action.Retryable = false
		action.ErrorClass = ""
		if existing.Status.CompletionTime != nil {
			action.CompletedAt = existing.Status.CompletionTime.DeepCopy()
		} else if action.CompletedAt == nil {
			action.CompletedAt = haActionTime(r.haNow())
		}
	case haAdminJobPhaseFailed:
		action.Retryable = false
		action.ErrorClass = haAdminJobFailureClass(existing)
		if existing.Status.CompletionTime != nil {
			action.CompletedAt = existing.Status.CompletionTime.DeepCopy()
		} else if action.CompletedAt == nil {
			action.CompletedAt = haActionTime(r.haNow())
		}
	default:
		action.Retryable = true
		action.CompletedAt = nil
	}
	if action.AdminJobPhase == haAdminJobPhaseSucceeded && action.AdminResult == nil {
		r.updateHAAdminActionResultFromJobLogs(ctx, cluster, action)
	}
	if additionalAttempts := action.AttemptCount - previousAttemptCount; additionalAttempts > 0 {
		haObserveActionAttempts(action, haMetricExecutorJob, additionalAttempts)
	}
	if previousPhase != action.AdminJobPhase {
		switch action.AdminJobPhase {
		case haAdminJobPhaseSucceeded:
			haObserveActionSuccess(action, haMetricExecutorJob)
		case haAdminJobPhaseFailed:
			haObserveActionFailure(action, haMetricExecutorJob, true)
		}
	}
	return nil
}

func (r *AntflyClusterReconciler) haPortableArtifactJobAction(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (antflyv1.HAPlannedActionStatus, bool, error) {
	kind := haActionKind(action.Kind)
	if !haPlannedActionKindIsPortableArtifact(kind) {
		return action, true, nil
	}
	artifact := haSeedArtifactForAction(cluster, action)
	if artifact == nil {
		return action, false, fmt.Errorf("HA %s requires a matching seedArtifact spec", action.Kind)
	}
	var configured *antflyv1.HASeedArtifactPVCSpec
	if kind == haActionPublishSeedArtifact || kind == haActionGCSourceSeedGenerations {
		configured = artifact.SourcePVC
	} else if kind != haActionPruneSeedArtifacts {
		configured = artifact.TargetPVC
	}
	if kind != haActionPruneSeedArtifacts && (configured == nil || strings.TrimSpace(configured.ClaimName) == "") {
		return action, false, fmt.Errorf("HA %s requires one explicitly configured PVC", action.Kind)
	}
	if strings.TrimSpace(action.TopologyID) == "" || action.TopologyGeneration <= 0 ||
		strings.TrimSpace(action.TopologyNodeID) == "" || strings.TrimSpace(action.TargetPVCName) == "" ||
		strings.TrimSpace(action.TargetPVCUID) == "" {
		return action, false, fmt.Errorf("HA %s requires an exact persisted topology generation and target identity", action.Kind)
	}
	if action.TopologyID != strings.TrimSpace(artifact.TopologyID) ||
		action.TopologyGeneration != artifact.TopologyGeneration ||
		action.TopologyNodeID != strings.TrimSpace(artifact.NodeID) ||
		action.TargetPVCUID != strings.TrimSpace(artifact.TargetPVCUID) || artifact.TargetPVC == nil ||
		action.TargetPVCName != strings.TrimSpace(artifact.TargetPVC.ClaimName) {
		return action, false, fmt.Errorf("HA %s persisted topology binding no longer matches seedArtifact spec", action.Kind)
	}
	gate := haRuntimeStartupGate(cluster)
	if gate != nil {
		if gate.RequiredReceipt == nil {
			return action, false, fmt.Errorf("HA %s startup gate omits its exact receipt contract", action.Kind)
		}
		required := gate.RequiredReceipt
		gateTargetPVC := strings.TrimSpace(required.TargetPVCName)
		if gateTargetPVC == "" {
			return action, false, fmt.Errorf("HA %s startup gate omits its target PVC identity", action.Kind)
		}
		// A promoted primary retains the exact gate that binds its own pre-seeded
		// PVC across restarts. Portable repair for a different target PVC must not
		// compare against that local boot contract. If the action does target the
		// gated PVC, retain the full exact-match barrier below.
		if action.TargetPVCName == gateTargetPVC {
			if action.TopologyID != strings.TrimSpace(required.TopologyID) ||
				action.TopologyGeneration != required.TopologyGeneration ||
				action.TopologyNodeID != strings.TrimSpace(required.NodeID) ||
				action.TargetPVCUID != strings.TrimSpace(required.TargetPVCUID) ||
				action.SlotName != strings.TrimSpace(required.SlotName) ||
				action.SeedArtifactGeneration != strings.TrimSpace(required.Generation) {
				return action, false, fmt.Errorf("HA %s topology binding is stale relative to the desired startup gate", action.Kind)
			}
		}
	}
	targetPVC := &corev1.PersistentVolumeClaim{}
	targetKey := types.NamespacedName{Name: action.TargetPVCName, Namespace: cluster.Namespace}
	if err := r.Get(ctx, targetKey, targetPVC); err != nil {
		if errors.IsNotFound(err) {
			return action, false, nil
		}
		return action, false, fmt.Errorf("read target PVC %s for HA %s: %w", targetKey.Name, action.Kind, err)
	}
	if string(targetPVC.UID) != action.TargetPVCUID {
		return action, false, fmt.Errorf("HA %s target PVC UID is stale", action.Kind)
	}
	if kind == haActionPruneSeedArtifacts {
		return action, true, nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Name: strings.TrimSpace(configured.ClaimName), Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, pvc); err != nil {
		if errors.IsNotFound(err) {
			return action, false, nil
		}
		return action, false, fmt.Errorf("read PVC %s for HA %s: %w", key.Name, action.Kind, err)
	}
	pvcUID := strings.TrimSpace(string(pvc.UID))
	if pvcUID == "" {
		return action, false, nil
	}
	if kind != haActionPublishSeedArtifact && kind != haActionGCSourceSeedGenerations {
		if key.Name != action.TargetPVCName || action.TargetPVCUID != pvcUID {
			return action, false, fmt.Errorf("HA %s target PVC identity is stale", action.Kind)
		}
		action.TargetPVCUID = pvcUID
	} else {
		if (strings.TrimSpace(action.SourcePVCName) != "" && strings.TrimSpace(action.SourcePVCName) != key.Name) ||
			(strings.TrimSpace(action.SourcePVCUID) != "" && strings.TrimSpace(action.SourcePVCUID) != pvcUID) {
			return action, false, fmt.Errorf("HA %s source PVC identity is stale", action.Kind)
		}
		action.SourcePVCName = key.Name
		action.SourcePVCUID = pvcUID
	}
	return action, true, nil
}

func (r *AntflyClusterReconciler) ensureHASeededSlotActivationReceiptConfigMap(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) error {
	raw, err := haSeededSlotActivationReceiptForGC(cluster, action)
	if err != nil {
		return err
	}
	immutable := true
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: haSeededSlotActivationConfigMapName(cluster, action), Namespace: cluster.Namespace,
			Labels: haAdminJobLabels(cluster, action),
			Annotations: map[string]string{
				"antfly.io/ha-operation-id":        action.OperationID,
				"antfly.io/ha-topology-generation": strconv.FormatInt(action.TopologyGeneration, 10),
				"antfly.io/ha-target-pvc-uid":      action.TargetPVCUID,
			},
		},
		Immutable: &immutable,
		Data:      map[string]string{"seeded-slot-activation.json": raw},
	}
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	existing := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}
	if err := r.Get(ctx, key, existing); err != nil {
		if errors.IsNotFound(err) {
			if err := r.Create(ctx, desired); err != nil {
				return fmt.Errorf("create immutable seeded-slot activation receipt %s: %w", desired.Name, err)
			}
			return nil
		}
		return err
	}
	if existing.Immutable == nil || !*existing.Immutable ||
		!metav1.IsControlledBy(existing, cluster) ||
		existing.Data["seeded-slot-activation.json"] != raw ||
		existing.Annotations["antfly.io/ha-operation-id"] != action.OperationID ||
		existing.Annotations["antfly.io/ha-target-pvc-uid"] != action.TargetPVCUID {
		return fmt.Errorf("immutable seeded-slot activation receipt ConfigMap %s does not match the exact planned operation", existing.Name)
	}
	return nil
}

func haSeededSlotActivationReceiptForGC(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (string, error) {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return "", fmt.Errorf("HA target GC requires persisted seeded-slot activation evidence")
	}
	for i := range cluster.Status.HAStatus.PlannedActions {
		activation := cluster.Status.HAStatus.PlannedActions[i]
		if haActionKind(activation.Kind) != haActionActivateSeededSlot ||
			activation.StandbyName != action.StandbyName || activation.SlotName != action.SlotName ||
			activation.SeedArtifactGeneration != action.SeedArtifactGeneration ||
			!haAdminActionSucceededWithEvidence(activation) || activation.AdminResult == nil {
			continue
		}
		result := activation.AdminResult
		request := adminsdk.SeededSlotActivateRequest{
			SlotName: result.SlotName, Generation: result.SeedArtifactGeneration,
			ManifestId: result.ManifestID, TimelineId: result.SeedTimelineID,
			CheckpointLsn: result.CheckpointLSN, SeedReceiptSha256: result.SeedReceiptSHA256,
			CaptureReceiptSha256: result.CaptureReceiptSHA256,
			ManifestSha256:       result.ManifestSHA256, AggregateSha256: result.AggregateSHA256,
		}
		raw, err := haExactSeededSlotActivationReceipt([]byte(result.RawReceiptJSON), request)
		if err != nil {
			return "", fmt.Errorf("validate exact seeded-slot activation receipt for target GC: %w", err)
		}
		return raw, nil
	}
	return "", fmt.Errorf("HA target GC requires one matching successful seeded-slot activation receipt")
}

func haSeededSlotActivationConfigMapName(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) string {
	hash := haAdminActionHash(action)[:10]
	base := strings.Trim(strings.ToLower(cluster.Name)+"-ha-slot-receipt", "-")
	maxBaseLen := 63 - len(hash) - 1
	if len(base) > maxBaseLen {
		base = strings.TrimRight(base[:maxBaseLen], "-")
	}
	return fmt.Sprintf("%s-%s", base, hash)
}

func (r *AntflyClusterReconciler) observeHAAdminJobPodAttempts(ctx context.Context, job *batchv1.Job) (int32, *metav1.Time, *metav1.Time, error) {
	if r == nil || job == nil {
		return 0, nil, nil, nil
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace)); err != nil {
		return 0, nil, nil, fmt.Errorf("list attempt pods for HA admin Job %s: %w", job.Name, err)
	}
	var count int32
	var first *metav1.Time
	var last *metav1.Time
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !haPodControlledByJob(pod, job) {
			continue
		}
		var observedAt *metav1.Time
		started := false
		for j := range pod.Status.ContainerStatuses {
			container := &pod.Status.ContainerStatuses[j]
			switch {
			case container.State.Running != nil:
				started = true
				candidate := metav1.NewTime(container.State.Running.StartedAt.Time)
				observedAt = &candidate
			case container.State.Terminated != nil:
				started = true
				candidate := metav1.NewTime(container.State.Terminated.StartedAt.Time)
				observedAt = &candidate
			case container.LastTerminationState.Terminated != nil:
				started = true
				candidate := metav1.NewTime(container.LastTerminationState.Terminated.StartedAt.Time)
				observedAt = &candidate
			}
		}
		if !started {
			continue
		}
		count++
		if observedAt == nil || observedAt.IsZero() {
			observedAt = pod.Status.StartTime
		}
		if observedAt == nil || observedAt.IsZero() {
			continue
		}
		if first == nil || observedAt.Before(first) {
			first = observedAt.DeepCopy()
		}
		if last == nil || last.Before(observedAt) {
			last = observedAt.DeepCopy()
		}
	}
	return count, first, last, nil
}

// haActivationJobAction binds the activation receipt to the PVC instance that
// the operator actually observed. The bound copy is hashed into the Job name,
// while the declarative planned action remains stable across status replans.
func (r *AntflyClusterReconciler) haActivationJobAction(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (antflyv1.HAPlannedActionStatus, bool, error) {
	if haActionKind(action.Kind) != haActionActivateSeedArtifact {
		return action, true, nil
	}
	if strings.TrimSpace(action.TopologyID) == "" || action.TopologyGeneration <= 0 ||
		strings.TrimSpace(action.TopologyNodeID) == "" || strings.TrimSpace(action.TargetPVCName) == "" ||
		strings.TrimSpace(action.TargetPVCUID) == "" || action.TargetLocalNodeID == 0 || action.TargetReplicaID == 0 {
		return action, false, fmt.Errorf("HA target activation requires an exact persisted topology and PVC identity")
	}
	if cluster.Spec.Standalone == nil || cluster.Spec.Standalone.NodeID <= 0 || cluster.Spec.Standalone.Replicas != 1 ||
		action.TargetLocalNodeID != uint64(cluster.Spec.Standalone.NodeID) || action.TargetReplicaID != 1 {
		return action, false, fmt.Errorf("HA target activation runtime materialization identity does not match standalone topology")
	}
	if gate := haRuntimeStartupGate(cluster); gate != nil {
		if gate.Policy != antflyv1.HAStartupGatePolicyRequireActivatedSeed ||
			gate.ReceiptMatchPolicy != antflyv1.HAReceiptMatchPolicyExact || gate.RequiredReceipt == nil {
			return action, false, fmt.Errorf("HA target activation conflicts with the configured startup gate")
		}
		required := gate.RequiredReceipt
		gateTargetPVC := strings.TrimSpace(required.TargetPVCName)
		if gateTargetPVC == "" {
			return action, false, fmt.Errorf("HA target activation startup gate omits its target PVC identity")
		}
		// The runtime gate protects the PVC from which this cluster boots. A
		// promoted primary can also activate a portable seed on a different,
		// physically isolated PVC. Only compare that action to the boot contract
		// when both contracts address the same PVC instance.
		if action.TargetPVCName == gateTargetPVC {
			if action.TopologyID != strings.TrimSpace(required.TopologyID) || action.TopologyGeneration != required.TopologyGeneration ||
				action.TopologyNodeID != strings.TrimSpace(required.NodeID) ||
				action.TargetPVCUID != strings.TrimSpace(required.TargetPVCUID) ||
				strings.TrimSpace(action.SlotName) != strings.TrimSpace(required.SlotName) ||
				strings.TrimSpace(action.SeedArtifactGeneration) != strings.TrimSpace(required.Generation) {
				return action, false, fmt.Errorf("HA target activation topology binding is stale relative to the startup gate")
			}
		}
	}
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: action.TargetPVCName, Namespace: cluster.Namespace}, pvc)
	if errors.IsNotFound(err) {
		return action, false, nil
	}
	if err != nil {
		return action, false, err
	}
	pvcUID := strings.TrimSpace(string(pvc.UID))
	if pvcUID == "" || action.TargetPVCUID != pvcUID {
		return action, false, nil
	}
	return action, true, nil
}

func (r *AntflyClusterReconciler) haActivationReceiptMatchesCurrentTarget(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (bool, error) {
	receipt := action.SeedArtifactReceipt
	gate := haRuntimeStartupGate(cluster)
	if gate != nil && gate.Policy == antflyv1.HAStartupGatePolicyRequireActivatedSeed && gate.RequiredReceipt != nil {
		required := *gate.RequiredReceipt
		gateTargetPVC := strings.TrimSpace(required.TargetPVCName)
		actionTargetPVC := strings.TrimSpace(action.TargetPVCName)
		// Completed actions written before target identity became a required
		// action field can still authorize their exact local startup gate because
		// the immutable receipt carries that identity. New portable actions always
		// carry the target on both the action and receipt.
		gateApplies := gateTargetPVC != "" &&
			(actionTargetPVC == gateTargetPVC ||
				(actionTargetPVC == "" && receipt != nil && strings.TrimSpace(receipt.TargetPVCName) == gateTargetPVC))
		if gateApplies {
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.Get(ctx, types.NamespacedName{Name: gateTargetPVC, Namespace: cluster.Namespace}, pvc)
			if errors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if receipt == nil || strings.TrimSpace(string(pvc.UID)) == "" ||
				receipt.TopologyID != required.TopologyID || receipt.NodeID != required.NodeID ||
				receipt.SlotName != required.SlotName || receipt.Generation != required.Generation ||
				receipt.TargetPVCName != required.TargetPVCName || receipt.TargetPVCUID != string(pvc.UID) ||
				receipt.GenerationPath != path.Join("live-generations", required.Generation) ||
				receipt.RawGenerationPath != path.Join("generations", required.Generation) ||
				receipt.TargetLocalNodeID == 0 || receipt.TargetReplicaID == 0 ||
				!isLowerHexDigest(receipt.CaptureReceiptSHA256) ||
				!isLowerHexDigest(receipt.MaterializedReceiptSHA256) ||
				!isLowerHexDigest(receipt.MaterializedAggregateSHA256) {
				return false, nil
			}
			if required.TopologyGeneration != 0 && receipt.TopologyGeneration != required.TopologyGeneration {
				return false, nil
			}
			if required.TargetPVCUID != "" && receipt.TargetPVCUID != required.TargetPVCUID {
				return false, nil
			}
			if (required.ManifestSHA256 != "" && receipt.ManifestSHA256 != required.ManifestSHA256) ||
				(required.AggregateSHA256 != "" && receipt.AggregateSHA256 != required.AggregateSHA256) ||
				(required.SeedReceiptSHA256 != "" && receipt.SeedReceiptSHA256 != required.SeedReceiptSHA256) ||
				(required.CaptureReceiptSHA256 != "" && receipt.CaptureReceiptSHA256 != required.CaptureReceiptSHA256) ||
				(required.MaterializedReceiptSHA256 != "" && receipt.MaterializedReceiptSHA256 != required.MaterializedReceiptSHA256) ||
				(required.MaterializedAggregateSHA256 != "" && receipt.MaterializedAggregateSHA256 != required.MaterializedAggregateSHA256) ||
				(required.TargetLocalNodeID != 0 && receipt.TargetLocalNodeID != required.TargetLocalNodeID) ||
				(required.TargetReplicaID != 0 && receipt.TargetReplicaID != required.TargetReplicaID) {
				return false, nil
			}
			return true, nil
		}
	}

	if strings.TrimSpace(action.TopologyID) == "" || action.TopologyGeneration <= 0 ||
		strings.TrimSpace(action.TopologyNodeID) == "" || strings.TrimSpace(action.SlotName) == "" ||
		strings.TrimSpace(action.SeedArtifactGeneration) == "" || strings.TrimSpace(action.TargetPVCName) == "" ||
		strings.TrimSpace(action.TargetPVCUID) == "" || action.TargetLocalNodeID == 0 || action.TargetReplicaID == 0 {
		return false, nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: action.TargetPVCName, Namespace: cluster.Namespace}, pvc)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if receipt == nil || strings.TrimSpace(string(pvc.UID)) == "" ||
		receipt.TopologyID != action.TopologyID || receipt.TopologyGeneration != action.TopologyGeneration ||
		receipt.NodeID != action.TopologyNodeID || receipt.SlotName != action.SlotName ||
		receipt.Generation != action.SeedArtifactGeneration || receipt.TargetPVCName != action.TargetPVCName ||
		receipt.TargetPVCUID != action.TargetPVCUID || receipt.TargetPVCUID != string(pvc.UID) ||
		receipt.GenerationPath != path.Join("live-generations", action.SeedArtifactGeneration) ||
		receipt.RawGenerationPath != path.Join("generations", action.SeedArtifactGeneration) ||
		receipt.TargetLocalNodeID != action.TargetLocalNodeID || receipt.TargetReplicaID != action.TargetReplicaID ||
		!isLowerHexDigest(receipt.CaptureReceiptSHA256) ||
		!isLowerHexDigest(receipt.MaterializedReceiptSHA256) ||
		!isLowerHexDigest(receipt.MaterializedAggregateSHA256) {
		return false, nil
	}
	if expected := strings.TrimSpace(action.SeedCaptureReceiptSHA256); expected != "" && receipt.CaptureReceiptSHA256 != expected {
		return false, nil
	}
	return true, nil
}

func haAdminActionCanRunAsFallbackJob(cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action antflyv1.HAPlannedActionStatus, err error) bool {
	if !stderrors.Is(err, errHAAdminTokenEnvMissing) {
		return false
	}
	return haAdminActionHasJobTokenFallback(cluster, admin, action)
}

func haAdminActionMissingTokenCanFallbackFromStatus(cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action antflyv1.HAPlannedActionStatus) bool {
	if !strings.Contains(action.AdminError, "configured HA admin token env var") ||
		!strings.Contains(action.AdminError, "is empty or unset") {
		return false
	}
	return haAdminActionHasJobTokenFallback(cluster, admin, action)
}

func haAdminActionHasJobTokenFallback(cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action antflyv1.HAPlannedActionStatus) bool {
	if admin == nil || haAdminConfiguredTokenEnvVar(admin) == "" || len(action.AdminCommand) == 0 {
		return false
	}
	return len(admin.EnvFrom) > 0 || len(haAdminJobTokenEnv(cluster, admin)) > 0
}

func haPlannedActionRequiresAdminTarget(action antflyv1.HAPlannedActionStatus) bool {
	if action.Executor == string(haActionExecutorAdminAPI) &&
		haPlannedActionSupportsDirectAdminAPI(haActionKind(action.Kind)) {
		return true
	}
	return len(action.AdminCommand) > 0 ||
		strings.TrimSpace(action.AdminMethod) != "" ||
		strings.TrimSpace(action.AdminPath) != ""
}

func haPlannedActionRequiresAdminURL(action antflyv1.HAPlannedActionStatus) bool {
	if haPlannedActionKindIsPortableArtifact(haActionKind(action.Kind)) {
		return false
	}
	return haPlannedActionRequiresAdminTarget(action)
}

func (r *AntflyClusterReconciler) executeHAPlannedActionTyped(ctx context.Context, cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) (bool, error) {
	if action == nil {
		return false, nil
	}
	if action.Executor == string(haActionExecutorCLIJob) {
		return false, nil
	}
	if !haPlannedActionSupportsDirectAdminAPI(haActionKind(action.Kind)) {
		return false, nil
	}
	if err := haValidatePlannedActionAdminOperation(*action); err != nil {
		return true, err
	}
	adminClient, err := r.haAdminSDKClient(cluster, action.AdminURL)
	if err != nil {
		return true, err
	}
	switch action.Kind {
	case string(haActionCreateSlot):
		slotName := haActionRequestSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		body := adminsdk.ReplicationSlotCreateRequest{SlotName: slotName}
		if action.TargetLSN > 0 {
			body.InitialLsn = action.TargetLSN
		}
		result, err := adminClient.CreateReplicationSlotResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromReplicationSlotSDK(*value))
		}
		return true, err
	case string(haActionPauseSlot):
		slotName := haActionRequestSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		result, err := adminClient.PauseReplicationSlotResponse(ctx, slotName)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromReplicationSlotSDK(*value))
		}
		return true, err
	case string(haActionResumeSlot):
		slotName := haActionRequestSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		result, err := adminClient.ResumeReplicationSlotResponse(ctx, slotName)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromReplicationSlotSDK(*value))
		}
		return true, err
	case string(haActionDropSlot):
		slotName := haActionRequestSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		result, err := adminClient.DropReplicationSlotResponse(ctx, slotName)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromReplicationSlotSDK(*value))
		}
		return true, err
	case string(haActionSeedStandby), string(haActionMarkReseed):
		slotName := haActionRequestSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		manifestID := haSeedBeginManifestID(*action, slotName)
		if manifestID == "" {
			return true, fmt.Errorf("HA admin action %s requires nonzero target LSN to create seed manifest", action.Kind)
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		body := adminsdk.BaseBackupStartRequest{
			SlotName:   slotName,
			ManifestId: manifestID,
		}
		result, err := adminClient.BeginBaseBackupResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromBaseBackupBeginSDK(*value))
		}
		return true, err
	case string(haActionFinishStandbySeed):
		if strings.TrimSpace(action.SeedManifestPath) == "" {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		body := adminsdk.BaseBackupManifestPathRequest{ManifestPath: action.SeedManifestPath}
		result, err := adminClient.FinishBaseBackupResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromBaseBackupFinishSDK(*value))
		}
		return true, err
	case string(haActionCaptureSeedArtifact):
		if err := r.validateHASeedCaptureBinding(ctx, cluster, *action); err != nil {
			return true, err
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		// validateHASeedCaptureBinding rejects non-positive generations, and every
		// positive int64 value is exactly representable as uint64.
		topologyGeneration := uint64(action.TopologyGeneration) //nolint:gosec // G115: validated positive int64 widens without overflow
		body := adminsdk.SeedArtifactCaptureRequest{
			SlotName:           action.SlotName,
			Generation:         action.SeedArtifactGeneration,
			TopologyId:         action.TopologyID,
			TopologyGeneration: topologyGeneration,
			NodeId:             action.TopologyNodeID,
			TargetPvcName:      action.TargetPVCName,
			TargetPvcUid:       action.TargetPVCUID,
		}
		result, err := adminClient.CaptureSeedArtifactResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			if !haSeedCaptureResponseMatchesAction(cluster, *action, *value) {
				err = fmt.Errorf("HA seed capture response does not match the planned runtime-owned generation and identity")
			} else {
				err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromSeedCaptureSDK(*value))
			}
		}
		return true, err
	case string(haActionActivateSeededSlot):
		receipt := haSeedActivationReceiptForAction(cluster, *action)
		if receipt == nil {
			return true, fmt.Errorf("HA seeded slot activation requires matching durable target activation evidence")
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		body := adminsdk.SeededSlotActivateRequest{
			SlotName:             action.SlotName,
			Generation:           receipt.Generation,
			ManifestId:           receipt.ManifestID,
			TimelineId:           receipt.TimelineID,
			CheckpointLsn:        receipt.CheckpointLSN,
			SeedReceiptSha256:    receipt.SeedReceiptSHA256,
			CaptureReceiptSha256: receipt.CaptureReceiptSHA256,
			ManifestSha256:       receipt.ManifestSHA256,
			AggregateSha256:      receipt.AggregateSHA256,
		}
		result, err := adminClient.ActivateSeededSlotResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			if !haSeededSlotActivationResponseMatchesRequest(*value, body) {
				err = fmt.Errorf("HA seeded slot activation response does not match the durable target activation receipt")
			} else {
				raw, rawErr := haExactSeededSlotActivationReceipt(result.Body, body)
				if rawErr != nil {
					err = rawErr
				} else {
					adminResult := haAdminActionResultFromSeededSlotActivateSDK(*value)
					adminResult.RawReceiptJSON = raw
					err = requireHADirectAdminActionResultStatus(action, adminResult)
				}
			}
		}
		return true, err
	case string(haActionBootstrapStandbySeed):
		if strings.TrimSpace(action.SeedManifestPath) == "" {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		body := adminsdk.StandbyBootstrapRequest{ManifestPath: action.SeedManifestPath}
		if strings.TrimSpace(action.SeedContentRoot) != "" {
			body.ContentRoot = action.SeedContentRoot
		}
		result, err := adminClient.BootstrapStandbyResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectAdminActionResultStatus(action, haAdminActionResultFromStandbyBootstrapSDK(*value))
		}
		return true, err
	case string(haActionAcquireFence), string(haActionFenceFormerPrimary):
		var body adminsdk.FenceAcquireRequest
		var ok bool
		if haActionKind(action.Kind) == haActionFenceFormerPrimary {
			body, ok = haFormerPrimaryFenceAcquireBody(cluster, *action)
		} else {
			body, ok = haFenceAcquireBody(cluster, *action)
		}
		if !ok {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		result, err := adminClient.AcquireFenceResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectFenceAcquireResultStatus(cluster, action, haAdminActionResultFromFenceSDK(*value))
		}
		return true, err
	case string(haActionAssessPromotion):
		body := haPromotionAssessBody(*action)
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		result, err := adminClient.AssessPromotionResponse(ctx, body)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil {
			err = requireHADirectPromotionAssessmentResultStatus(action, haAdminActionResultFromPromotionAssessSDK(*value))
		}
		return true, err
	case string(haActionPromoteStandby):
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		result, err := adminClient.PromoteWithCurrentFenceResponse(ctx)
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil && !r.applyHADirectPromotionResultFromSDK(cluster, action, *value) {
			err = fmt.Errorf("HA admin action %s succeeded without typed promotion receipt", action.Kind)
		}
		return true, err
	case string(haActionDemoteFormerPrimary), string(haActionRewindFormerPrimary), string(haActionReseedFormerPrimary):
		body, ok := haRejoinAssessBody(cluster, *action)
		if !ok {
			return false, nil
		}
		if !haPlannedActionHasDirectAdminOperation(*action) {
			return false, nil
		}
		if err := haValidateDirectAdminNodeTarget(*action); err != nil {
			return true, err
		}
		var result *adminsdk.HAResponse[adminsdk.HARejoinAssessResponse]
		switch haActionKind(action.Kind) {
		case haActionRewindFormerPrimary:
			result, err = adminClient.RewindRejoinResponse(ctx, body)
		case haActionReseedFormerPrimary:
			result, err = adminClient.ReseedRejoinResponse(ctx, body)
		default:
			result, err = adminClient.AssessRejoinResponse(ctx, body)
		}
		value, err := haAdminSDKResponseValue(result, err)
		if err == nil && !r.applyHADirectRejoinAssessResultFromSDK(cluster, action, *value) {
			err = fmt.Errorf("HA admin action %s succeeded without typed rejoin assessment", action.Kind)
		}
		return true, err
	default:
		return false, nil
	}
}

func (r *AntflyClusterReconciler) haAdminSDKClient(cluster *antflyv1.AntflyCluster, baseURL string) (*adminsdk.HAClient, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("HA admin API execution requires adminURL")
	}
	client, err := adminsdk.NewHAClient(baseURL, r.httpClient())
	if err != nil {
		return nil, err
	}
	token, err := r.haAdminBearerToken(cluster)
	if err != nil {
		return nil, err
	}
	if token != "" {
		client.WithToken(token)
	}
	return client, nil
}

func (r *AntflyClusterReconciler) haAdminBearerToken(cluster *antflyv1.AntflyCluster) (string, error) {
	var admin *antflyv1.HAAdminSpec
	if cluster != nil && cluster.Spec.HighAvailability != nil && cluster.Spec.HighAvailability.Admin != nil {
		admin = cluster.Spec.HighAvailability.Admin
	}
	envVar := haAdminTokenEnvVar(admin)
	token := strings.TrimSpace(os.Getenv(envVar))
	if haAdminConfiguredTokenEnvVar(admin) != "" && token == "" {
		return "", fmt.Errorf("configured HA admin token env var %s is empty or unset: %w", envVar, errHAAdminTokenEnvMissing)
	}
	return token, nil
}

func haPlannedActionHasDirectAdminOperation(action antflyv1.HAPlannedActionStatus) bool {
	_, _, ok := haPlannedActionDirectAdminOperation(action)
	return ok
}

func (r *AntflyClusterReconciler) validateHASeedCaptureBinding(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) error {
	if r == nil || r.Client == nil {
		return fmt.Errorf("HA seed capture requires Kubernetes PVC observation")
	}
	artifact := haSeedArtifactForAction(cluster, action)
	if artifact == nil || artifact.SourcePVC == nil || artifact.TargetPVC == nil {
		return fmt.Errorf("HA seed capture requires distinct configured source and target PVCs")
	}
	if strings.TrimSpace(action.TopologyID) == "" || action.TopologyGeneration <= 0 ||
		strings.TrimSpace(action.TopologyNodeID) == "" || strings.TrimSpace(action.TargetPVCName) == "" ||
		strings.TrimSpace(action.TargetPVCUID) == "" ||
		action.TopologyID != strings.TrimSpace(artifact.TopologyID) ||
		action.TopologyGeneration != artifact.TopologyGeneration ||
		action.TopologyNodeID != strings.TrimSpace(artifact.NodeID) ||
		action.TargetPVCName != strings.TrimSpace(artifact.TargetPVC.ClaimName) ||
		action.TargetPVCUID != strings.TrimSpace(artifact.TargetPVCUID) {
		return fmt.Errorf("HA seed capture requires one exact persisted topology and target PVC binding")
	}
	for _, expected := range []struct {
		name string
		uid  string
	}{
		{name: strings.TrimSpace(artifact.SourcePVC.ClaimName)},
		{name: action.TargetPVCName, uid: action.TargetPVCUID},
	} {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: expected.name, Namespace: cluster.Namespace}, pvc); err != nil {
			return fmt.Errorf("verify HA seed capture PVC %s: %w", expected.name, err)
		}
		if strings.TrimSpace(string(pvc.UID)) == "" || (expected.uid != "" && expected.uid != string(pvc.UID)) {
			return fmt.Errorf("HA seed capture PVC %s identity is stale", expected.name)
		}
	}
	return nil
}

func haAdminSDKResponseValue[T any](value *adminsdk.HAResponse[T], err error) (*T, error) {
	if err != nil {
		var apiErr *adminsdk.HAAPIError
		if stderrors.As(err, &apiErr) {
			return nil, fmt.Errorf("HA admin API returned status %d: %s: %w", apiErr.StatusCode, strings.TrimSpace(apiErr.Body), err)
		}
		var validationErr *adminsdk.HAResponseValidationError
		if stderrors.As(err, &validationErr) {
			return nil, fmt.Errorf("HA admin action response missing typed result evidence: %w", err)
		}
		return nil, err
	}
	if value == nil || value.Value == nil {
		return nil, fmt.Errorf("HA admin SDK response is nil")
	}
	return value.Value, nil
}

func haAdminStatusUnavailableReason(cluster *antflyv1.AntflyCluster, defaultReason string) string {
	if cluster != nil &&
		cluster.Status.HAStatus != nil &&
		cluster.Status.HAStatus.PrimaryAdminStatusCode == http.StatusUnauthorized {
		return antflyv1.ReasonHAAdminUnauthorized
	}
	return defaultReason
}

func haAdminActionResultFromReplicationSlotSDK(response adminsdk.HAReplicationSlotActionResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	result.SlotAction = strings.TrimSpace(string(response.SlotAction))
	result.SlotName = strings.TrimSpace(response.Slot.SlotName)
	return result
}

func haAdminActionResultFromBaseBackupBeginSDK(response adminsdk.HABaseBackupBeginResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	result.SlotName = strings.TrimSpace(response.SlotName)
	result.ManifestID = strings.TrimSpace(response.ManifestId)
	result.BackupLSN = response.BackupLsn
	result.StartRecordLSN = response.StartRecordLsn
	return result
}

func haAdminActionResultFromBaseBackupFinishSDK(response adminsdk.HABaseBackupFinishResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	result.ManifestID = strings.TrimSpace(response.ManifestId)
	result.BackupLSN = response.BackupLsn
	result.EndRecordLSN = response.EndRecordLsn
	return result
}

func haAdminActionResultFromSeedCaptureSDK(response adminsdk.HASeedArtifactCaptureResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	result.SlotName = strings.TrimSpace(response.SlotName)
	result.ManifestID = strings.TrimSpace(response.ManifestId)
	result.BackupLSN = response.BackupLsn
	result.CheckpointLSN = response.CheckpointLsn
	result.EndRecordLSN = response.EndRecordLsn
	result.SeedArtifactGeneration = strings.TrimSpace(response.Generation)
	result.ManifestSHA256 = strings.TrimSpace(response.ManifestSha256)
	result.CaptureReceiptSHA256 = strings.TrimSpace(response.CaptureReceiptSha256)
	result.SeedClusterID = response.ClusterId
	result.SeedShardID = response.ShardId
	result.SeedTableID = response.TableId
	result.SeedTimelineID = response.TimelineId
	result.SeedEpoch = response.Epoch
	result.SeedSourcePlanSHA256 = strings.TrimSpace(response.SourcePlanSha256)
	result.SeedFileCount = response.FileCount
	result.SeedTotalBytes = response.TotalBytes
	result.SeedGenerationRoot = strings.TrimSpace(response.GenerationRoot)
	result.SeedContentRoot = strings.TrimSpace(response.ContentRoot)
	result.SeedManifestPath = strings.TrimSpace(response.ManifestPath)
	result.SeedAlreadyCaptured = response.AlreadyCaptured
	return result
}

func haSeedCaptureResponseMatchesAction(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus, response adminsdk.HASeedArtifactCaptureResponse) bool {
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	return identity != nil &&
		strings.TrimSpace(response.SlotName) == strings.TrimSpace(action.SlotName) &&
		strings.TrimSpace(response.Generation) == strings.TrimSpace(action.SeedArtifactGeneration) &&
		strings.TrimSpace(response.ManifestId) == strings.TrimSpace(action.SeedArtifactGeneration) &&
		response.ClusterId == identity.ClusterID &&
		response.ShardId == identity.ShardID &&
		response.TableId == identity.TableID &&
		response.TimelineId == identity.TimelineID &&
		response.Epoch == identity.Epoch &&
		isLowerHexDigest(response.CaptureReceiptSha256) &&
		response.BackupLsn >= action.TargetLSN &&
		response.CheckpointLsn >= response.BackupLsn &&
		response.EndRecordLsn >= response.CheckpointLsn
}

func haAdminActionResultFromSeededSlotActivateSDK(response adminsdk.HASeededSlotActivateResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	result.SlotName = strings.TrimSpace(response.SlotName)
	result.ManifestID = strings.TrimSpace(response.ManifestId)
	result.CheckpointLSN = response.CheckpointLsn
	result.SeedArtifactGeneration = strings.TrimSpace(response.Generation)
	result.SeedReceiptSHA256 = strings.TrimSpace(response.SeedReceiptSha256)
	result.CaptureReceiptSHA256 = strings.TrimSpace(response.CaptureReceiptSha256)
	result.ManifestSHA256 = strings.TrimSpace(response.ManifestSha256)
	result.AggregateSHA256 = strings.TrimSpace(response.AggregateSha256)
	result.SeedTimelineID = response.TimelineId
	result.TimelineID = response.TimelineId
	return result
}

func haExactSeededSlotActivationReceipt(body []byte, request adminsdk.SeededSlotActivateRequest) (string, error) {
	if len(body) == 0 || len(body) > maxHASeededSlotActivationReceiptBytes {
		return "", fmt.Errorf("HA seeded slot activation response body must contain at most %d bytes", maxHASeededSlotActivationReceiptBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	var response adminsdk.HASeededSlotActivateResponse
	if err := decoder.Decode(&response); err != nil {
		return "", fmt.Errorf("decode exact HA seeded slot activation response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", fmt.Errorf("HA seeded slot activation response contains trailing JSON")
	}
	if err := adminsdk.ValidateHASeededSlotActivateResponse(response); err != nil {
		return "", fmt.Errorf("validate exact HA seeded slot activation response: %w", err)
	}
	if !haSeededSlotActivationResponseMatchesRequest(response, request) {
		return "", fmt.Errorf("exact HA seeded slot activation response does not match the activation request")
	}
	return string(body), nil
}

func haSeededSlotActivationResponseMatchesRequest(response adminsdk.HASeededSlotActivateResponse, request adminsdk.SeededSlotActivateRequest) bool {
	return strings.TrimSpace(response.SlotName) == strings.TrimSpace(request.SlotName) &&
		strings.TrimSpace(response.Generation) == strings.TrimSpace(request.Generation) &&
		strings.TrimSpace(response.ManifestId) == strings.TrimSpace(request.ManifestId) &&
		response.TimelineId == request.TimelineId &&
		response.CheckpointLsn == request.CheckpointLsn &&
		strings.TrimSpace(response.SeedReceiptSha256) == strings.TrimSpace(request.SeedReceiptSha256) &&
		strings.TrimSpace(response.CaptureReceiptSha256) == strings.TrimSpace(request.CaptureReceiptSha256) &&
		strings.TrimSpace(response.ManifestSha256) == strings.TrimSpace(request.ManifestSha256) &&
		strings.TrimSpace(response.AggregateSha256) == strings.TrimSpace(request.AggregateSha256)
}

func haAdminActionResultFromStandbyBootstrapSDK(response adminsdk.HAStandbyBootstrapResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	result.ManifestID = strings.TrimSpace(response.ManifestId)
	result.BackupLSN = response.BackupLsn
	result.CheckpointLSN = response.CheckpointLsn
	return result
}

func haAdminActionResultFromFenceSDK(response adminsdk.HAFenceResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	applyHAFenceReceiptToAdminActionResult(result, response.Receipt)
	return result
}

func haAdminActionResultFromPromotionAssessSDK(response adminsdk.HAPromotionAssessResponse) *antflyv1.HAAdminActionResultStatus {
	result := haAdminActionResultFromReceipt(response.SchemaVersion, response.Action)
	applyHAPromotionAssessmentToAdminActionResult(result, response.Assessment)
	return result
}

func applyHAFenceReceiptToAdminActionResult(result *antflyv1.HAAdminActionResultStatus, receipt adminsdk.HAFenceReceipt) {
	if result == nil {
		return
	}
	result.FenceGeneration = receipt.Generation
	result.FenceToken = strings.TrimSpace(receipt.Token)
	result.FenceClusterID = receipt.Identity.ClusterId
	result.FenceShardID = receipt.Identity.ShardId
	result.FenceTableID = receipt.Identity.TableId
	result.FenceOldPrimaryID = strings.TrimSpace(receipt.OldPrimaryId)
	result.FencePromotedNodeID = strings.TrimSpace(receipt.PromotedNodeId)
	result.FenceParentTimelineID = receipt.ParentTimelineId
	result.FenceParentEpoch = receipt.ParentEpoch
	result.FenceNewTimelineID = receipt.NewTimelineId
	result.FenceNewEpoch = receipt.NewEpoch
	result.FenceRequiredLSN = receipt.RequiredLsn
	result.FenceObservedLSN = receipt.ObservedLsn
	result.FenceForced = receipt.Forced
	result.FenceReason = strings.TrimSpace(receipt.Reason)
}

func applyHAPromotionAssessmentToAdminActionResult(result *antflyv1.HAAdminActionResultStatus, assessment adminsdk.HAPromotionAssessment) {
	if result == nil {
		return
	}
	result.PromotionRequiredLSN = assessment.RequiredLsn
	result.PromotionReceivedLSN = assessment.ReceivedLsn
	result.PromotionAppliedLSN = assessment.AppliedLsn
	result.PromotionCanPromote = assessment.CanPromote
	result.PromotionFenced = assessment.FencingConfirmed
	result.PromotionSafe = assessment.Safe
	result.PromotionForce = assessment.Force
	result.PromotionMode = strings.TrimSpace(string(assessment.Mode))
	result.PromotionDataLossPossible = assessment.DataLossPossible
	result.PromotionRequiresFencing = assessment.RequiresFencing
	result.PromotionRequiresForce = assessment.RequiresForce
}

func haAdminActionResultFromReceipt(schemaVersion uint32, receipt adminsdk.HAActionReceipt) *antflyv1.HAAdminActionResultStatus {
	return &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: schemaVersion,
		ActionID:      strings.TrimSpace(receipt.ActionId),
		ActionKind:    strings.TrimSpace(string(receipt.ActionKind)),
		ActionTarget:  strings.TrimSpace(receipt.Target),
		ActionState:   strings.TrimSpace(string(receipt.State)),
		ActionNodeID:  strings.TrimSpace(receipt.NodeId),
	}
}

func haValidateDirectAdminNodeTarget(action antflyv1.HAPlannedActionStatus) error {
	if strings.TrimSpace(action.AdminNodeID) == "" {
		return fmt.Errorf("HA action %s requires adminNodeID before typed /admin/v1 execution", action.Kind)
	}
	return nil
}

func haPlannedActionDirectAdminOperation(action antflyv1.HAPlannedActionStatus) (string, string, bool) {
	method := strings.TrimSpace(action.AdminMethod)
	apiPath := strings.TrimSpace(action.AdminPath)
	if method != "" && apiPath != "" {
		return method, apiPath, true
	}
	if action.Executor == string(haActionExecutorAdminAPI) {
		return method, apiPath, false
	}
	method, apiPath = haAdminOperation(haPlannedAction{
		Kind:             haActionKind(action.Kind),
		StandbyName:      action.StandbyName,
		SlotName:         action.SlotName,
		SeedManifestPath: action.SeedManifestPath,
	})
	return method, apiPath, method != "" && apiPath != ""
}

func haPlannedActionSupportsDirectAdminAPI(kind haActionKind) bool {
	_, _, ok := haDirectAdminActionReceiptSpec(kind)
	return ok
}

func haDirectAdminActionReceiptSpec(kind haActionKind) (string, string, bool) {
	expectation := adminsdk.HAReceiptExpectation{}
	ok := true
	switch kind {
	case haActionCreateSlot:
		expectation = adminsdk.HAReplicationSlotCreateReceiptExpectation()
	case haActionResumeSlot:
		expectation = adminsdk.HAReplicationSlotResumeReceiptExpectation()
	case haActionPauseSlot:
		expectation = adminsdk.HAReplicationSlotPauseReceiptExpectation()
	case haActionDropSlot:
		expectation = adminsdk.HAReplicationSlotDropReceiptExpectation()
	case haActionSeedStandby, haActionMarkReseed:
		expectation = adminsdk.HABaseBackupBeginReceiptExpectation()
	case haActionFinishStandbySeed:
		expectation = adminsdk.HABaseBackupFinishReceiptExpectation()
	case haActionCaptureSeedArtifact:
		expectation = adminsdk.HASeedCaptureReceiptExpectation()
	case haActionActivateSeededSlot:
		expectation = adminsdk.HASeededSlotActivateReceiptExpectation()
	case haActionBootstrapStandbySeed:
		expectation = adminsdk.HAStandbyBootstrapReceiptExpectation()
	case haActionAcquireFence, haActionFenceFormerPrimary:
		expectation = adminsdk.HAFenceAcquireReceiptExpectation()
	case haActionAssessPromotion:
		expectation = adminsdk.HAPromotionAssessReceiptExpectation()
	case haActionPromoteStandby:
		expectation = adminsdk.HAPromotionReceiptExpectation()
	case haActionDemoteFormerPrimary:
		expectation = adminsdk.HARejoinAssessReceiptExpectation()
	case haActionRewindFormerPrimary:
		expectation = adminsdk.HARejoinRewindReceiptExpectation()
	case haActionReseedFormerPrimary:
		expectation = adminsdk.HARejoinReseedReceiptExpectation()
	default:
		ok = false
	}
	if !ok {
		return "", "", false
	}
	expectedKind, expectedState := expectation.Strings()
	return expectedKind, expectedState, true
}

func haValidatePlannedActionAdminOperation(action antflyv1.HAPlannedActionStatus) error {
	method := strings.TrimSpace(action.AdminMethod)
	apiPath := strings.TrimSpace(action.AdminPath)
	if method == "" && apiPath == "" {
		if action.Executor == string(haActionExecutorAdminAPI) &&
			haPlannedActionSupportsDirectAdminAPI(haActionKind(action.Kind)) {
			return fmt.Errorf("planned HA action %q is marked AdminAPI but status does not publish typed admin method/path", action.Kind)
		}
		return nil
	}
	expectedMethod, expectedPath := haAdminOperation(haPlannedAction{
		Kind:        haActionKind(action.Kind),
		StandbyName: action.StandbyName,
		SlotName:    action.SlotName,
	})
	if expectedMethod == "" || expectedPath == "" {
		return fmt.Errorf("planned HA action %q has typed admin operation %s %s but no matching direct admin API operation", action.Kind, action.AdminMethod, action.AdminPath)
	}
	if method != expectedMethod || apiPath != expectedPath {
		return fmt.Errorf("planned HA action %q typed admin operation mismatch: status has %s %s, expected %s %s", action.Kind, action.AdminMethod, action.AdminPath, expectedMethod, expectedPath)
	}
	return nil
}

func haAdminIdentityRequestFromSpec(identity *antflyv1.HAReplicationIdentitySpec) adminsdk.HAIdentity {
	if identity == nil {
		return adminsdk.HAIdentity{}
	}
	return adminsdk.HAIdentity{
		ClusterId:  identity.ClusterID,
		ShardId:    identity.ShardID,
		TableId:    identity.TableID,
		TimelineId: identity.TimelineID,
		Epoch:      identity.Epoch,
	}
}

func haFenceAcquireBody(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (adminsdk.FenceAcquireRequest, bool) {
	if cluster == nil {
		return adminsdk.FenceAcquireRequest{}, false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || identity.CurrentPrimaryID == "" || strings.TrimSpace(action.StandbyName) == "" {
		return adminsdk.FenceAcquireRequest{}, false
	}
	if frozen := haSucceededFormerPrimaryFenceResult(cluster.Status.HAStatus, action.StandbyName, action.FenceGeneration); frozen != nil {
		return adminsdk.FenceAcquireRequest{
			Identity: adminsdk.HAIdentity{
				ClusterId:  frozen.FenceClusterID,
				ShardId:    frozen.FenceShardID,
				TableId:    frozen.FenceTableID,
				TimelineId: frozen.FenceParentTimelineID,
				Epoch:      frozen.FenceParentEpoch,
			},
			OldPrimaryId:   frozen.FenceOldPrimaryID,
			PromotedNodeId: frozen.FencePromotedNodeID,
			NewTimelineId:  frozen.FenceNewTimelineID,
			NewEpoch:       frozen.FenceNewEpoch,
			Generation:     frozen.FenceGeneration,
			RequiredLsn:    frozen.FenceRequiredLSN,
			ObservedLsn:    frozen.FenceObservedLSN,
			Force:          frozen.FenceForced,
			Reason:         frozen.FenceReason,
		}, true
	}
	reason := action.FenceReason
	if strings.TrimSpace(reason) == "" {
		reason = action.Reason
	}
	return adminsdk.FenceAcquireRequest{
		Identity:       haAdminIdentityRequestFromSpec(identity),
		OldPrimaryId:   identity.CurrentPrimaryID,
		PromotedNodeId: action.StandbyName,
		NewTimelineId:  identity.TimelineID + 1,
		NewEpoch:       identity.Epoch + 1,
		Generation:     action.FenceGeneration,
		RequiredLsn:    action.TargetLSN,
		ObservedLsn:    action.TargetLSN,
		Reason:         reason,
	}, true
}

func haFormerPrimaryFenceAcquireBody(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (adminsdk.FenceAcquireRequest, bool) {
	if cluster == nil {
		return adminsdk.FenceAcquireRequest{}, false
	}
	promotion := haPromotionReceipt(cluster.Status.HAStatus)
	if promotion == nil {
		identity := haReplicationIdentity(cluster.Spec.HighAvailability)
		oldPrimaryID := strings.TrimSpace(action.StandbyName)
		promotedNodeID := strings.TrimSpace(action.RouteTo)
		if identity == nil ||
			oldPrimaryID == "" ||
			promotedNodeID == "" ||
			oldPrimaryID != strings.TrimSpace(identity.CurrentPrimaryID) ||
			promotedNodeID != strings.TrimSpace(action.FenceHolder) ||
			action.TargetLSN == 0 {
			return adminsdk.FenceAcquireRequest{}, false
		}
		reason := action.FenceReason
		if strings.TrimSpace(reason) == "" {
			reason = action.Reason
		}
		return adminsdk.FenceAcquireRequest{
			Identity:       haAdminIdentityRequestFromSpec(identity),
			OldPrimaryId:   oldPrimaryID,
			PromotedNodeId: promotedNodeID,
			NewTimelineId:  identity.TimelineID + 1,
			NewEpoch:       identity.Epoch + 1,
			Generation:     action.FenceGeneration,
			RequiredLsn:    action.TargetLSN,
			ObservedLsn:    action.TargetLSN,
			Reason:         reason,
		}, true
	}
	if strings.TrimSpace(promotion.OldPrimaryID) == "" ||
		strings.TrimSpace(promotion.PromotedStandbyID) == "" ||
		promotion.ParentTimelineID == 0 ||
		promotion.ParentEpoch == 0 ||
		promotion.NewTimelineID == 0 ||
		promotion.NewEpoch == 0 ||
		haPromotionRequiredLSN(promotion) == 0 ||
		haPromotionObservedLSN(promotion) < haPromotionRequiredLSN(promotion) {
		return adminsdk.FenceAcquireRequest{}, false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil ||
		promotion.OldPrimaryID != identity.CurrentPrimaryID ||
		promotion.ParentTimelineID != identity.TimelineID ||
		promotion.ParentEpoch != identity.Epoch ||
		strings.TrimSpace(action.StandbyName) != strings.TrimSpace(promotion.OldPrimaryID) ||
		strings.TrimSpace(action.RouteTo) != strings.TrimSpace(promotion.PromotedStandbyID) {
		return adminsdk.FenceAcquireRequest{}, false
	}
	return adminsdk.FenceAcquireRequest{
		Identity: adminsdk.HAIdentity{
			ClusterId:  identity.ClusterID,
			ShardId:    identity.ShardID,
			TableId:    identity.TableID,
			TimelineId: promotion.ParentTimelineID,
			Epoch:      promotion.ParentEpoch,
		},
		OldPrimaryId:   promotion.OldPrimaryID,
		PromotedNodeId: promotion.PromotedStandbyID,
		NewTimelineId:  promotion.NewTimelineID,
		NewEpoch:       promotion.NewEpoch,
		Generation:     promotion.FenceGeneration,
		RequiredLsn:    haPromotionRequiredLSN(promotion),
		ObservedLsn:    haPromotionObservedLSN(promotion),
		Force:          promotion.Forced,
		Reason:         promotion.FenceReason,
	}, true
}

func haSucceededFormerPrimaryFenceResult(status *antflyv1.HAStatus, promotedNodeID string, generation uint64) *antflyv1.HAAdminActionResultStatus {
	if status == nil {
		return nil
	}
	for i := range status.PlannedActions {
		action := &status.PlannedActions[i]
		if haActionKind(action.Kind) != haActionFenceFormerPrimary ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			strings.TrimSpace(action.RouteTo) != strings.TrimSpace(promotedNodeID) ||
			(generation != 0 && action.FenceGeneration != generation) ||
			action.AdminResult == nil {
			continue
		}
		result := action.AdminResult
		if result.FenceClusterID == 0 ||
			strings.TrimSpace(result.FenceOldPrimaryID) == "" ||
			strings.TrimSpace(result.FencePromotedNodeID) != strings.TrimSpace(promotedNodeID) ||
			result.FenceParentTimelineID == 0 ||
			result.FenceParentEpoch == 0 ||
			result.FenceNewTimelineID == 0 ||
			result.FenceNewEpoch == 0 ||
			result.FenceRequiredLSN == 0 ||
			result.FenceObservedLSN != result.FenceRequiredLSN ||
			result.FenceGeneration == 0 ||
			(generation != 0 && result.FenceGeneration != generation) ||
			strings.TrimSpace(result.FenceToken) == "" {
			continue
		}
		return result
	}
	return nil
}

func haPromotionAssessBody(action antflyv1.HAPlannedActionStatus) adminsdk.PromotionAssessRequest {
	return adminsdk.PromotionAssessRequest{
		RequiredLsn:      action.TargetLSN,
		FencingConfirmed: false,
		Force:            false,
		UseCurrentFence:  true,
	}
}

func haRejoinAssessBody(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (adminsdk.RejoinAssessRequest, bool) {
	if cluster == nil {
		return adminsdk.RejoinAssessRequest{}, false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || strings.TrimSpace(action.StandbyName) == "" {
		return adminsdk.RejoinAssessRequest{}, false
	}
	requestIdentity := haAdminIdentityRequestFromSpec(identity)
	promotion := haPromotionReceipt(cluster.Status.HAStatus)
	if promotion != nil {
		if !haIdentityMatchesPromotionParentOrChild(identity, promotion) {
			return adminsdk.RejoinAssessRequest{}, false
		}
		// The former-primary assessment must describe the immutable parent
		// branch. The cluster spec may already describe the promoted child.
		requestIdentity.TimelineId = promotion.ParentTimelineID
		requestIdentity.Epoch = promotion.ParentEpoch
	}
	lastLSN := action.ObservedLSN
	if lastLSN == 0 {
		lastLSN = action.TargetLSN
	}
	body := adminsdk.RejoinAssessRequest{
		NodeId:                          action.StandbyName,
		Identity:                        requestIdentity,
		LastLsn:                         lastLSN,
		RetainedFromLsn:                 action.RetainedFromLSN,
		AllowRewindAfterForcedPromotion: false,
	}
	if receipt, ok := haRejoinFenceReceipt(cluster.Status.HAStatus, identity); ok {
		body.Receipt = receipt
	} else if haActionKind(action.Kind) == haActionRewindFormerPrimary ||
		haActionKind(action.Kind) == haActionReseedFormerPrimary {
		return adminsdk.RejoinAssessRequest{}, false
	}
	return body, true
}

func haRejoinFenceReceipt(status *antflyv1.HAStatus, identity *antflyv1.HAReplicationIdentitySpec) (adminsdk.HAFenceReceipt, bool) {
	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return adminsdk.HAFenceReceipt{}, false
	}
	if identity == nil {
		return adminsdk.HAFenceReceipt{}, false
	}
	// Colony adopts the promoted topology before former-primary repair. Accept
	// either exact side of this one promotion receipt so the already-completed
	// fence remains usable after that declarative identity advance. Unrelated,
	// stale, and cross-topology identities still fail closed.
	if !haIdentityMatchesPromotionParentOrChild(identity, promotion) {
		return adminsdk.HAFenceReceipt{}, false
	}
	receiptIdentity := haAdminIdentityRequestFromSpec(identity)
	receiptIdentity.TimelineId = promotion.NewTimelineID
	receiptIdentity.Epoch = promotion.NewEpoch
	return adminsdk.HAFenceReceipt{
		Identity:         receiptIdentity,
		OldPrimaryId:     promotion.OldPrimaryID,
		PromotedNodeId:   promotion.PromotedStandbyID,
		ParentTimelineId: promotion.ParentTimelineID,
		ParentEpoch:      promotion.ParentEpoch,
		NewTimelineId:    promotion.NewTimelineID,
		NewEpoch:         promotion.NewEpoch,
		RequiredLsn:      haPromotionRequiredLSN(promotion),
		ObservedLsn:      haPromotionObservedLSN(promotion),
		Generation:       promotion.FenceGeneration,
		Forced:           promotion.Forced,
		Token:            strings.TrimSpace(promotion.FenceToken),
		Reason:           promotion.FenceReason,
	}, true
}

func haActionRequestSlotName(action antflyv1.HAPlannedActionStatus) string {
	if action.SlotName != "" {
		return action.SlotName
	}
	return action.StandbyName
}

func haActionSlotName(action antflyv1.HAPlannedActionStatus) string {
	if strings.TrimSpace(action.SlotName) != "" {
		return strings.TrimSpace(action.SlotName)
	}
	return strings.TrimSpace(action.StandbyName)
}

func haSeedBeginManifestID(action antflyv1.HAPlannedActionStatus, slotName string) string {
	for i := 0; i+1 < len(action.AdminCommand); i++ {
		if action.AdminCommand[i] == "--manifest-id" {
			if manifestID := strings.TrimSpace(action.AdminCommand[i+1]); manifestID != "" {
				return manifestID
			}
		}
	}
	slotName = strings.TrimSpace(slotName)
	if slotName == "" || action.TargetLSN == 0 {
		return ""
	}
	return fmt.Sprintf("base-%s-%d", slotName, action.TargetLSN)
}

type haAdminActionReceiptJSON struct {
	ActionID   string `json:"action_id"`
	ActionKind string `json:"action_kind"`
	Target     string `json:"target"`
	State      string `json:"state"`
	NodeID     string `json:"node_id"`
}

type haPromotionAssessmentJSON struct {
	RequiredLSN        *uint64 `json:"required_lsn"`
	ReceivedLSN        *uint64 `json:"received_lsn"`
	AppliedLSN         *uint64 `json:"applied_lsn"`
	HasRequiredLSN     *bool   `json:"has_required_lsn"`
	CaughtUpToReceived *bool   `json:"caught_up_to_received"`
	FencingConfirmed   *bool   `json:"fencing_confirmed"`
	Force              *bool   `json:"force"`
	Mode               string  `json:"mode"`
	DataLossPossible   *bool   `json:"data_loss_possible"`
	Safe               *bool   `json:"safe"`
	RequiresFencing    *bool   `json:"requires_fencing"`
	RequiresForce      *bool   `json:"requires_force"`
	CanPromote         *bool   `json:"can_promote"`
}

type haFenceReceiptIdentityJSON struct {
	ClusterID  *uint64 `json:"cluster_id"`
	ShardID    *uint64 `json:"shard_id"`
	TableID    *uint64 `json:"table_id"`
	TimelineID *uint64 `json:"timeline_id"`
	Epoch      *uint64 `json:"epoch"`
}

type haFenceReceiptJSON struct {
	Identity         haFenceReceiptIdentityJSON `json:"identity"`
	OldPrimaryID     string                     `json:"old_primary_id"`
	PromotedNodeID   string                     `json:"promoted_node_id"`
	ParentTimelineID *uint64                    `json:"parent_timeline_id"`
	ParentEpoch      *uint64                    `json:"parent_epoch"`
	NewTimelineID    *uint64                    `json:"new_timeline_id"`
	NewEpoch         *uint64                    `json:"new_epoch"`
	RequiredLSN      *uint64                    `json:"required_lsn"`
	ObservedLSN      *uint64                    `json:"observed_lsn"`
	Generation       *uint64                    `json:"generation"`
	Forced           *bool                      `json:"forced"`
	Token            string                     `json:"token"`
	Reason           *string                    `json:"reason"`
}

type haReplicationSlotJSON struct {
	SlotName       string  `json:"slot_name"`
	TimelineID     *uint64 `json:"timeline_id"`
	RestartLSN     *uint64 `json:"restart_lsn"`
	ReceivedLSN    *uint64 `json:"received_lsn"`
	AppliedLSN     *uint64 `json:"applied_lsn"`
	SafeReadLSN    *uint64 `json:"safe_read_lsn"`
	Active         *bool   `json:"active"`
	ReseedRequired *bool   `json:"reseed_required"`
	CurrentLSN     *uint64 `json:"current_lsn"`
}

type haDirectAdminActionResultJSON struct {
	SchemaVersion  uint32                    `json:"schema_version"`
	Action         haAdminActionReceiptJSON  `json:"action"`
	SlotAction     string                    `json:"slot_action"`
	Slot           haReplicationSlotJSON     `json:"slot"`
	SlotName       string                    `json:"slot_name"`
	ManifestID     string                    `json:"manifest_id"`
	BackupLSN      uint64                    `json:"backup_lsn"`
	StartRecordLSN uint64                    `json:"start_record_lsn"`
	EndRecordLSN   uint64                    `json:"end_record_lsn"`
	CheckpointLSN  uint64                    `json:"checkpoint_lsn"`
	Assessment     haPromotionAssessmentJSON `json:"assessment"`
	Receipt        haFenceReceiptJSON        `json:"receipt"`
}

type haDirectAdminActionResultEnvelope struct {
	Result struct {
		Slot          *haDirectAdminActionResultJSON `json:"slot,omitempty"`
		SeedBegin     *haDirectAdminActionResultJSON `json:"seed_begin,omitempty"`
		SeedFinish    *haDirectAdminActionResultJSON `json:"seed_finish,omitempty"`
		SeedBootstrap *haDirectAdminActionResultJSON `json:"seed_bootstrap,omitempty"`
		FenceAcquire  *haDirectAdminActionResultJSON `json:"fence_acquire,omitempty"`
	} `json:"result"`
}

func requireHADirectAdminActionResultStatus(action *antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) error {
	if action == nil || !haActionRequiresAdminResult(haActionKind(action.Kind)) {
		return nil
	}
	action.AdminResult = result
	if haActionHasRequiredAdminResult(*action) && haDirectAdminActionReceiptMatches(*action) {
		return nil
	}
	action.AdminResult = nil
	return fmt.Errorf("HA admin action %s succeeded without typed result evidence", action.Kind)
}

func parseHADirectAdminActionResult(raw []byte) (*antflyv1.HAAdminActionResultStatus, bool) {
	var direct haDirectAdminActionResultJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return nil, false
	}
	topLevel := haDirectAdminActionResultHasCorrelationFields(direct)
	result := &direct
	if !topLevel {
		var envelope haDirectAdminActionResultEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, false
		}
		switch {
		case envelope.Result.Slot != nil:
			result = envelope.Result.Slot
		case envelope.Result.SeedBegin != nil:
			result = envelope.Result.SeedBegin
		case envelope.Result.SeedFinish != nil:
			result = envelope.Result.SeedFinish
		case envelope.Result.SeedBootstrap != nil:
			result = envelope.Result.SeedBootstrap
		case envelope.Result.FenceAcquire != nil:
			result = envelope.Result.FenceAcquire
		default:
			return nil, false
		}
		if result.SchemaVersion == 0 {
			result.SchemaVersion = direct.SchemaVersion
		}
	}
	if result.SchemaVersion == 0 {
		return nil, false
	}
	if topLevel && !haAdminActionReceiptPresent(result.Action) {
		return nil, false
	}
	if topLevel &&
		strings.HasPrefix(strings.TrimSpace(result.Action.ActionKind), "replication_slot_") &&
		!haReplicationSlotJSONComplete(result.Slot) {
		return nil, false
	}
	if topLevel && !haDirectAdminActionPayloadComplete(*result) {
		return nil, false
	}
	if strings.TrimSpace(result.Action.ActionKind) == "promotion_assess" &&
		(!haPromotionAssessmentJSONComplete(result.Assessment) ||
			!haPromotionAssessmentJSONConsistent(result.Assessment)) {
		return nil, false
	}
	status := &antflyv1.HAAdminActionResultStatus{
		SchemaVersion:             result.SchemaVersion,
		ActionID:                  strings.TrimSpace(result.Action.ActionID),
		ActionKind:                strings.TrimSpace(result.Action.ActionKind),
		ActionTarget:              strings.TrimSpace(result.Action.Target),
		ActionState:               strings.TrimSpace(result.Action.State),
		ActionNodeID:              strings.TrimSpace(result.Action.NodeID),
		SlotAction:                strings.TrimSpace(result.SlotAction),
		SlotName:                  strings.TrimSpace(result.SlotName),
		ManifestID:                strings.TrimSpace(result.ManifestID),
		BackupLSN:                 result.BackupLSN,
		StartRecordLSN:            result.StartRecordLSN,
		EndRecordLSN:              result.EndRecordLSN,
		CheckpointLSN:             result.CheckpointLSN,
		PromotionRequiredLSN:      haUint64JSONValue(result.Assessment.RequiredLSN),
		PromotionReceivedLSN:      haUint64JSONValue(result.Assessment.ReceivedLSN),
		PromotionAppliedLSN:       haUint64JSONValue(result.Assessment.AppliedLSN),
		PromotionCanPromote:       haBoolJSONValue(result.Assessment.CanPromote),
		PromotionFenced:           haBoolJSONValue(result.Assessment.FencingConfirmed),
		PromotionSafe:             haBoolJSONValue(result.Assessment.Safe),
		PromotionForce:            haBoolJSONValue(result.Assessment.Force),
		PromotionMode:             strings.TrimSpace(result.Assessment.Mode),
		PromotionDataLossPossible: haBoolJSONValue(result.Assessment.DataLossPossible),
		PromotionRequiresFencing:  haBoolJSONValue(result.Assessment.RequiresFencing),
		PromotionRequiresForce:    haBoolJSONValue(result.Assessment.RequiresForce),
		FenceGeneration:           haUint64JSONValue(result.Receipt.Generation),
		FenceToken:                strings.TrimSpace(result.Receipt.Token),
		FenceClusterID:            haUint64JSONValue(result.Receipt.Identity.ClusterID),
		FenceShardID:              haUint64JSONValue(result.Receipt.Identity.ShardID),
		FenceTableID:              haUint64JSONValue(result.Receipt.Identity.TableID),
		FenceOldPrimaryID:         strings.TrimSpace(result.Receipt.OldPrimaryID),
		FencePromotedNodeID:       strings.TrimSpace(result.Receipt.PromotedNodeID),
		FenceParentTimelineID:     haUint64JSONValue(result.Receipt.ParentTimelineID),
		FenceParentEpoch:          haUint64JSONValue(result.Receipt.ParentEpoch),
		FenceNewTimelineID:        haUint64JSONValue(result.Receipt.NewTimelineID),
		FenceNewEpoch:             haUint64JSONValue(result.Receipt.NewEpoch),
		FenceRequiredLSN:          haUint64JSONValue(result.Receipt.RequiredLSN),
		FenceObservedLSN:          haUint64JSONValue(result.Receipt.ObservedLSN),
		FenceForced:               haBoolJSONValue(result.Receipt.Forced),
		FenceReason:               haStringJSONValue(result.Receipt.Reason),
	}
	if status.SlotName == "" {
		status.SlotName = strings.TrimSpace(result.Slot.SlotName)
	}
	if status.SlotAction == "" &&
		status.ActionID == "" &&
		status.ActionKind == "" &&
		status.ActionTarget == "" &&
		status.ActionState == "" &&
		status.ActionNodeID == "" &&
		status.SlotName == "" &&
		status.ManifestID == "" &&
		status.BackupLSN == 0 &&
		status.StartRecordLSN == 0 &&
		status.EndRecordLSN == 0 &&
		status.CheckpointLSN == 0 &&
		status.FenceGeneration == 0 &&
		status.FenceToken == "" {
		return nil, false
	}
	return status, true
}

func haPromotionAssessmentJSONComplete(assessment haPromotionAssessmentJSON) bool {
	return assessment.RequiredLSN != nil &&
		assessment.ReceivedLSN != nil &&
		assessment.AppliedLSN != nil &&
		assessment.HasRequiredLSN != nil &&
		assessment.CaughtUpToReceived != nil &&
		assessment.FencingConfirmed != nil &&
		assessment.Force != nil &&
		strings.TrimSpace(assessment.Mode) != "" &&
		assessment.DataLossPossible != nil &&
		assessment.Safe != nil &&
		assessment.RequiresFencing != nil &&
		assessment.RequiresForce != nil &&
		assessment.CanPromote != nil
}

func haReplicationSlotJSONComplete(slot haReplicationSlotJSON) bool {
	return strings.TrimSpace(slot.SlotName) != "" &&
		slot.TimelineID != nil &&
		haUint64JSONValue(slot.TimelineID) > 0 &&
		slot.RestartLSN != nil &&
		slot.ReceivedLSN != nil &&
		slot.AppliedLSN != nil &&
		slot.SafeReadLSN != nil &&
		slot.Active != nil &&
		slot.ReseedRequired != nil &&
		slot.CurrentLSN != nil
}

func haFenceReceiptJSONComplete(receipt haFenceReceiptJSON) bool {
	return receipt.Identity.ClusterID != nil &&
		haUint64JSONValue(receipt.Identity.ClusterID) > 0 &&
		receipt.Identity.ShardID != nil &&
		receipt.Identity.TableID != nil &&
		receipt.Identity.TimelineID != nil &&
		haUint64JSONValue(receipt.Identity.TimelineID) > 0 &&
		receipt.Identity.Epoch != nil &&
		haUint64JSONValue(receipt.Identity.Epoch) > 0 &&
		strings.TrimSpace(receipt.OldPrimaryID) != "" &&
		strings.TrimSpace(receipt.PromotedNodeID) != "" &&
		receipt.ParentTimelineID != nil &&
		haUint64JSONValue(receipt.ParentTimelineID) > 0 &&
		receipt.ParentEpoch != nil &&
		haUint64JSONValue(receipt.ParentEpoch) > 0 &&
		receipt.NewTimelineID != nil &&
		haUint64JSONValue(receipt.NewTimelineID) > 0 &&
		receipt.NewEpoch != nil &&
		haUint64JSONValue(receipt.NewEpoch) > 0 &&
		receipt.RequiredLSN != nil &&
		haUint64JSONValue(receipt.RequiredLSN) > 0 &&
		receipt.ObservedLSN != nil &&
		receipt.Generation != nil &&
		haUint64JSONValue(receipt.Generation) > 0 &&
		receipt.Forced != nil &&
		strings.TrimSpace(receipt.Token) != "" &&
		receipt.Reason != nil
}

func haDirectAdminActionPayloadComplete(result haDirectAdminActionResultJSON) bool {
	switch strings.TrimSpace(result.Action.ActionKind) {
	case "base_backup_begin":
		return strings.TrimSpace(result.SlotName) != "" &&
			strings.TrimSpace(result.ManifestID) != "" &&
			result.BackupLSN > 0 &&
			result.StartRecordLSN > 0
	case "base_backup_finish":
		return strings.TrimSpace(result.ManifestID) != "" &&
			result.BackupLSN > 0 &&
			result.EndRecordLSN > 0
	case "standby_bootstrap":
		return strings.TrimSpace(result.ManifestID) != "" &&
			result.BackupLSN > 0 &&
			result.CheckpointLSN > 0
	case "fence_acquire":
		return haFenceReceiptJSONComplete(result.Receipt)
	default:
		return true
	}
}

func haUint64JSONValue(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func haBoolJSONValue(value *bool) bool {
	return value != nil && *value
}

func haStringJSONValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func haDirectAdminActionResultHasCorrelationFields(result haDirectAdminActionResultJSON) bool {
	return strings.TrimSpace(result.SlotAction) != "" ||
		strings.TrimSpace(result.Slot.SlotName) != "" ||
		strings.TrimSpace(result.SlotName) != "" ||
		strings.TrimSpace(result.ManifestID) != "" ||
		result.BackupLSN != 0 ||
		result.StartRecordLSN != 0 ||
		result.EndRecordLSN != 0 ||
		result.CheckpointLSN != 0 ||
		strings.TrimSpace(result.Action.ActionID) != "" ||
		strings.TrimSpace(result.Action.ActionKind) != "" ||
		strings.TrimSpace(result.Action.Target) != "" ||
		strings.TrimSpace(result.Action.State) != "" ||
		strings.TrimSpace(result.Action.NodeID) != "" ||
		result.Receipt.Generation != nil ||
		strings.TrimSpace(result.Receipt.Token) != "" ||
		result.Receipt.Identity.ClusterID != nil ||
		result.Receipt.Identity.ShardID != nil ||
		result.Receipt.Identity.TableID != nil ||
		result.Receipt.Identity.TimelineID != nil ||
		result.Receipt.Identity.Epoch != nil ||
		strings.TrimSpace(result.Receipt.OldPrimaryID) != "" ||
		strings.TrimSpace(result.Receipt.PromotedNodeID) != "" ||
		result.Receipt.ParentTimelineID != nil ||
		result.Receipt.ParentEpoch != nil ||
		result.Receipt.NewTimelineID != nil ||
		result.Receipt.NewEpoch != nil ||
		result.Receipt.RequiredLSN != nil ||
		result.Receipt.ObservedLSN != nil ||
		result.Receipt.Forced != nil ||
		result.Receipt.Reason != nil
}

func requireHADirectFenceAcquireResultStatus(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) error {
	if action == nil {
		return nil
	}
	action.AdminResult = result
	if haActionHasRequiredAdminResult(*action) &&
		haDirectAdminActionReceiptMatches(*action) &&
		haFenceAcquireResultMatchesAction(cluster, action, action.AdminResult) {
		return nil
	}
	action.AdminResult = nil
	return fmt.Errorf("HA admin action %s succeeded without matching typed fence receipt", action.Kind)
}

func requireHADirectPromotionAssessmentResultStatus(action *antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) error {
	if action == nil {
		return nil
	}
	action.AdminResult = result
	if haDirectAdminActionReceiptMatches(*action) &&
		haPromotionAssessmentResultMatchesAction(*action, action.AdminResult) {
		return nil
	}
	if haPromotionAssessmentWaitingForBoundary(*action, result) {
		return fmt.Errorf("%w: required_lsn=%d received_lsn=%d applied_lsn=%d", errHAPromotionBoundaryNotApplied, result.PromotionRequiredLSN, result.PromotionReceivedLSN, result.PromotionAppliedLSN)
	}
	action.AdminResult = nil
	return fmt.Errorf("HA admin action %s succeeded without safe typed promotion assessment", action.Kind)
}

func haPromotionAssessmentWaitingForBoundary(action antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if result == nil || result.SchemaVersion == 0 || action.TargetLSN == 0 ||
		result.ActionKind != "promotion_assess" ||
		result.ActionTarget != action.StandbyName ||
		result.ActionState != "assessed" ||
		(strings.TrimSpace(action.AdminNodeID) != "" && result.ActionNodeID != action.AdminNodeID) ||
		result.PromotionRequiredLSN != action.TargetLSN ||
		!result.PromotionFenced ||
		result.PromotionForce ||
		result.PromotionRequiresFencing {
		return false
	}
	return !result.PromotionCanPromote &&
		!result.PromotionSafe &&
		result.PromotionDataLossPossible &&
		result.PromotionRequiresForce &&
		(result.PromotionReceivedLSN < action.TargetLSN ||
			result.PromotionAppliedLSN < action.TargetLSN)
}

func haFenceAcquireResultMatchesAction(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if cluster == nil || action == nil || result == nil {
		return false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || strings.TrimSpace(action.StandbyName) == "" {
		return false
	}
	if haActionKind(action.Kind) == haActionFenceFormerPrimary {
		promotion := haPromotionReceipt(cluster.Status.HAStatus)
		if promotion != nil {
			return result.FenceClusterID != 0 &&
				(promotion.ClusterID == 0 || result.FenceClusterID == promotion.ClusterID) &&
				(promotion.ClusterID == 0 || result.FenceShardID == promotion.ShardID) &&
				(promotion.ClusterID == 0 || result.FenceTableID == promotion.TableID) &&
				result.FenceOldPrimaryID == promotion.OldPrimaryID &&
				result.FencePromotedNodeID == promotion.PromotedStandbyID &&
				result.FenceParentTimelineID == promotion.ParentTimelineID &&
				result.FenceParentEpoch == promotion.ParentEpoch &&
				result.FenceNewTimelineID == promotion.NewTimelineID &&
				result.FenceNewEpoch == promotion.NewEpoch &&
				result.FenceRequiredLSN == haPromotionRequiredLSN(promotion) &&
				result.FenceObservedLSN == haPromotionObservedLSN(promotion) &&
				result.FenceGeneration == promotion.FenceGeneration &&
				result.FenceToken == promotion.FenceToken &&
				result.FenceForced == promotion.Forced &&
				result.FenceReason == promotion.FenceReason
		}
		return result.FenceClusterID == identity.ClusterID &&
			result.FenceShardID == identity.ShardID &&
			result.FenceTableID == identity.TableID &&
			result.FenceOldPrimaryID == strings.TrimSpace(action.StandbyName) &&
			result.FencePromotedNodeID == strings.TrimSpace(action.RouteTo) &&
			result.FenceParentTimelineID == identity.TimelineID &&
			result.FenceParentEpoch == identity.Epoch &&
			result.FenceNewTimelineID == identity.TimelineID+1 &&
			result.FenceNewEpoch == identity.Epoch+1 &&
			result.FenceRequiredLSN >= action.TargetLSN &&
			result.FenceObservedLSN == result.FenceRequiredLSN &&
			result.FenceGeneration == action.FenceGeneration &&
			strings.TrimSpace(result.FenceToken) != "" &&
			!result.FenceForced &&
			result.FenceReason == action.FenceReason
	}
	if frozen := haSucceededFormerPrimaryFenceResult(cluster.Status.HAStatus, action.StandbyName, action.FenceGeneration); frozen != nil {
		return result.FenceClusterID == frozen.FenceClusterID &&
			result.FenceShardID == frozen.FenceShardID &&
			result.FenceTableID == frozen.FenceTableID &&
			result.FenceOldPrimaryID == frozen.FenceOldPrimaryID &&
			result.FencePromotedNodeID == frozen.FencePromotedNodeID &&
			result.FenceParentTimelineID == frozen.FenceParentTimelineID &&
			result.FenceParentEpoch == frozen.FenceParentEpoch &&
			result.FenceNewTimelineID == frozen.FenceNewTimelineID &&
			result.FenceNewEpoch == frozen.FenceNewEpoch &&
			result.FenceRequiredLSN == frozen.FenceRequiredLSN &&
			result.FenceObservedLSN == frozen.FenceObservedLSN &&
			result.FenceGeneration == frozen.FenceGeneration &&
			result.FenceToken == frozen.FenceToken &&
			result.FenceForced == frozen.FenceForced &&
			result.FenceReason == frozen.FenceReason
	}
	if result.FenceClusterID == 0 ||
		result.FenceClusterID != identity.ClusterID ||
		result.FenceShardID != identity.ShardID ||
		result.FenceTableID != identity.TableID {
		return false
	}
	if result.FenceOldPrimaryID != identity.CurrentPrimaryID ||
		result.FencePromotedNodeID != strings.TrimSpace(action.StandbyName) {
		return false
	}
	if result.FenceParentTimelineID != identity.TimelineID ||
		result.FenceParentEpoch != identity.Epoch ||
		result.FenceNewTimelineID != identity.TimelineID+1 ||
		result.FenceNewEpoch != identity.Epoch+1 {
		return false
	}
	if action.TargetLSN > 0 && result.FenceRequiredLSN != action.TargetLSN {
		return false
	}
	if !result.FenceForced && result.FenceObservedLSN < result.FenceRequiredLSN {
		return false
	}
	if action.FenceGeneration > 0 && result.FenceGeneration != action.FenceGeneration {
		return false
	}
	return true
}

func (r *AntflyClusterReconciler) updateHAAdminActionResultFromJobLogs(ctx context.Context, cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) {
	if r.KubeClient == nil || action == nil || action.AdminJobName == "" || action.AdminJobName == haAdminDirectAPIName {
		return
	}
	body, ok := r.haAdminJobLogBody(ctx, cluster, action.AdminJobName)
	if !ok {
		action.AdminResult = haCompletedSlotAdminJobResult(*action)
		return
	}
	if haActionRequiresSeedArtifactReceipt(haActionKind(action.Kind)) {
		action.SeedArtifactReceipt = parseHASeedArtifactReceipt(body, *action)
		return
	}
	result, ok := parseHAAdminActionResultTable(body)
	if !ok {
		action.AdminResult = haCompletedSlotAdminJobResult(*action)
		return
	}
	if haActionKind(action.Kind) == haActionPromoteStandby {
		enrichHAPromotionAdminActionResult(result, haReplicationIdentity(cluster.Spec.HighAvailability), *action)
	}
	action.AdminResult = result
}

func parseHASeedArtifactReceipt(body string, action antflyv1.HAPlannedActionStatus) *antflyv1.HASeedArtifactReceiptStatus {
	type chunkReceipt struct {
		Index     uint64 `json:"index"`
		SizeBytes uint64 `json:"size_bytes"`
		SHA256    string `json:"sha256"`
	}
	type fileReceipt struct {
		Path      string         `json:"path"`
		SizeBytes uint64         `json:"size_bytes"`
		CRC32     uint32         `json:"crc32"`
		SHA256    string         `json:"sha256"`
		Chunks    []chunkReceipt `json:"chunks"`
	}
	type artifactReceipt struct {
		FormatVersion               int32         `json:"format_version"`
		Generation                  string        `json:"generation"`
		SlotName                    string        `json:"slot_name"`
		ClusterID                   uint64        `json:"cluster_id"`
		ShardID                     uint64        `json:"shard_id"`
		TableID                     uint64        `json:"table_id"`
		TimelineID                  uint64        `json:"timeline_id"`
		Epoch                       uint64        `json:"epoch"`
		ManifestID                  string        `json:"manifest_id"`
		BackupLSN                   uint64        `json:"backup_lsn"`
		CheckpointLSN               uint64        `json:"checkpoint_lsn"`
		ManifestSHA256              string        `json:"manifest_sha256"`
		AggregateSHA256             string        `json:"aggregate_sha256"`
		SeedReceiptSHA256           string        `json:"seed_receipt_sha256"`
		CaptureReceiptSHA256        string        `json:"capture_receipt_sha256"`
		GenerationPath              string        `json:"generation_path"`
		RawGenerationPath           string        `json:"raw_generation_path"`
		MaterializedReceiptSHA256   string        `json:"materialized_receipt_sha256"`
		MaterializedAggregateSHA256 string        `json:"materialized_aggregate_sha256"`
		TargetLocalNodeID           uint64        `json:"target_local_node_id"`
		TargetReplicaID             uint64        `json:"target_replica_id"`
		TopologyID                  string        `json:"topology_id"`
		TopologyGeneration          int64         `json:"topology_generation"`
		NodeID                      string        `json:"node_id"`
		TargetPVCName               string        `json:"target_pvc_name"`
		TargetPVCUID                string        `json:"target_pvc_uid"`
		TotalBytes                  uint64        `json:"total_bytes"`
		Files                       []fileReceipt `json:"files"`
	}
	type pruneReceipt struct {
		FormatVersion      int32  `json:"format_version"`
		SlotName           string `json:"slot_name"`
		CurrentGeneration  string `json:"current_generation"`
		RetainedGeneration int32  `json:"retained_generations"`
		DeletedGeneration  int32  `json:"deleted_generations"`
	}
	type localGCReceipt struct {
		SchemaVersion        int32  `json:"schema_version"`
		ActionKind           string `json:"action_kind"`
		Scope                string `json:"scope"`
		SlotName             string `json:"slot_name"`
		CurrentGeneration    string `json:"current_generation"`
		CheckpointSHA256     string `json:"checkpoint_sha256"`
		RetainedGenerations  int32  `json:"retained_generations"`
		ProtectedGenerations int32  `json:"protected_generations"`
		DeletedGenerations   int32  `json:"deleted_generations"`
		ResumedTombstones    int32  `json:"resumed_tombstones"`
		SkippedIneligible    int32  `json:"skipped_ineligible"`
	}

	kind := haActionKind(action.Kind)
	if kind == haActionGCSourceSeedGenerations || kind == haActionGCTargetSeedGenerations {
		var receipt localGCReceipt
		decoder := json.NewDecoder(strings.NewReader(strings.TrimSpace(body)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&receipt); err != nil {
			return nil
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return nil
		}
		result := &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion: receipt.SchemaVersion, ActionKind: strings.TrimSpace(receipt.ActionKind),
			Scope: strings.TrimSpace(receipt.Scope), Generation: strings.TrimSpace(receipt.CurrentGeneration),
			SlotName: strings.TrimSpace(receipt.SlotName), CheckpointSHA256: strings.TrimSpace(receipt.CheckpointSHA256),
			RetainedCount: receipt.RetainedGenerations, ProtectedCount: receipt.ProtectedGenerations,
			DeletedCount: receipt.DeletedGenerations, ResumedTombstoneCount: receipt.ResumedTombstones,
			SkippedIneligibleCount: receipt.SkippedIneligible,
		}
		if !haSeedArtifactReceiptMatchesStatus(action, result) {
			return nil
		}
		return result
	}

	if kind == haActionPruneSeedArtifacts {
		var receipt pruneReceipt
		if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &receipt); err != nil {
			return nil
		}
		result := &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion: receipt.FormatVersion,
			Generation:    strings.TrimSpace(receipt.CurrentGeneration),
			SlotName:      strings.TrimSpace(receipt.SlotName),
			RetainedCount: receipt.RetainedGeneration,
			DeletedCount:  receipt.DeletedGeneration,
		}
		if !haSeedArtifactReceiptMatchesStatus(action, result) {
			return nil
		}
		return result
	}

	var receipt artifactReceipt
	decoder := json.NewDecoder(strings.NewReader(strings.TrimSpace(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return nil
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil
	}
	if len(receipt.Files) > math.MaxInt32 {
		return nil
	}
	result := &antflyv1.HASeedArtifactReceiptStatus{
		FormatVersion:               receipt.FormatVersion,
		Generation:                  strings.TrimSpace(receipt.Generation),
		SlotName:                    strings.TrimSpace(receipt.SlotName),
		ClusterID:                   receipt.ClusterID,
		ShardID:                     receipt.ShardID,
		TableID:                     receipt.TableID,
		TimelineID:                  receipt.TimelineID,
		Epoch:                       receipt.Epoch,
		ManifestID:                  strings.TrimSpace(receipt.ManifestID),
		BackupLSN:                   receipt.BackupLSN,
		CheckpointLSN:               receipt.CheckpointLSN,
		ManifestSHA256:              strings.TrimSpace(receipt.ManifestSHA256),
		AggregateSHA256:             strings.TrimSpace(receipt.AggregateSHA256),
		SeedReceiptSHA256:           strings.TrimSpace(receipt.SeedReceiptSHA256),
		CaptureReceiptSHA256:        strings.TrimSpace(receipt.CaptureReceiptSHA256),
		GenerationPath:              strings.TrimSpace(receipt.GenerationPath),
		RawGenerationPath:           strings.TrimSpace(receipt.RawGenerationPath),
		MaterializedReceiptSHA256:   strings.TrimSpace(receipt.MaterializedReceiptSHA256),
		MaterializedAggregateSHA256: strings.TrimSpace(receipt.MaterializedAggregateSHA256),
		TargetLocalNodeID:           receipt.TargetLocalNodeID,
		TargetReplicaID:             receipt.TargetReplicaID,
		TopologyID:                  strings.TrimSpace(receipt.TopologyID),
		TopologyGeneration:          receipt.TopologyGeneration,
		NodeID:                      strings.TrimSpace(receipt.NodeID),
		TargetPVCName:               strings.TrimSpace(receipt.TargetPVCName),
		TargetPVCUID:                strings.TrimSpace(receipt.TargetPVCUID),
		TotalBytes:                  receipt.TotalBytes,
		FileCount:                   int32(len(receipt.Files)), // #nosec G115 -- bounded by math.MaxInt32 above
	}
	if !haSeedArtifactReceiptMatchesStatus(action, result) {
		return nil
	}
	return result
}

func haSeedActivationReceiptForAction(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) *antflyv1.HASeedArtifactReceiptStatus {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return nil
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil {
		return nil
	}
	for i := range cluster.Status.HAStatus.PlannedActions {
		dependency := cluster.Status.HAStatus.PlannedActions[i]
		if haActionKind(dependency.Kind) != haActionActivateSeedArtifact ||
			dependency.SlotName != action.SlotName ||
			dependency.SeedArtifactGeneration != action.SeedArtifactGeneration ||
			!haAdminActionSucceededWithEvidence(dependency) {
			continue
		}
		receipt := dependency.SeedArtifactReceipt
		if receipt == nil || receipt.ClusterID != identity.ClusterID || receipt.ShardID != identity.ShardID ||
			receipt.TableID != identity.TableID || receipt.TimelineID != identity.TimelineID || receipt.Epoch != identity.Epoch {
			return nil
		}
		return receipt.DeepCopy()
	}
	return nil
}

func haCompletedSlotAdminJobResult(action antflyv1.HAPlannedActionStatus) *antflyv1.HAAdminActionResultStatus {
	slotAction := ""
	switch haActionKind(action.Kind) {
	case haActionCreateSlot:
		slotAction = "create"
	case haActionResumeSlot:
		slotAction = "resume"
	case haActionPauseSlot:
		slotAction = "pause"
	case haActionDropSlot:
		slotAction = "drop"
	default:
		return nil
	}
	expectedKind, expectedTarget, expectedState := haDirectAdminActionReceiptExpectation(action)
	if expectedKind == "" || expectedTarget == "" || expectedState == "" {
		return nil
	}
	return &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1,
		ActionID:      expectedKind + ":" + expectedTarget,
		ActionKind:    expectedKind,
		ActionTarget:  expectedTarget,
		ActionState:   expectedState,
		ActionNodeID:  strings.TrimSpace(action.AdminNodeID),
		SlotAction:    slotAction,
		SlotName:      expectedTarget,
	}
}

func parseHAAdminActionResultTable(body string) (*antflyv1.HAAdminActionResultStatus, bool) {
	if result, ok := parseHADirectAdminActionResult([]byte(body)); ok {
		return result, true
	}
	lines := parseHATableLines(body)
	resultName := strings.TrimSpace(lines["result"])
	if resultName == "" {
		return nil, false
	}
	result := &antflyv1.HAAdminActionResultStatus{
		ActionID:     strings.TrimSpace(lines["action.action_id"]),
		ActionKind:   strings.TrimSpace(lines["action.action_kind"]),
		ActionTarget: strings.TrimSpace(lines["action.target"]),
		ActionState:  strings.TrimSpace(lines["action.state"]),
		ActionNodeID: strings.TrimSpace(lines["action.node_id"]),
		SlotAction:   strings.TrimSpace(lines["slot_action"]),
		SlotName:     strings.TrimSpace(lines["slot_name"]),
		ManifestID:   strings.TrimSpace(lines["manifest_id"]),
		FenceToken:   strings.TrimSpace(lines["token"]),
	}
	if result.FenceToken == "" {
		result.FenceToken = strings.TrimSpace(lines["fence_token"])
	}
	result.BackupLSN, _ = parseHAResultUint(lines, "backup_lsn")
	result.StartRecordLSN, _ = parseHAResultUint(lines, "start_record_lsn")
	result.EndRecordLSN, _ = parseHAResultUint(lines, "end_record_lsn")
	result.CheckpointLSN, _ = parseHAResultUint(lines, "checkpoint_lsn")
	result.FenceClusterID, _ = parseHAResultUint(lines, "identity.cluster_id")
	result.FenceShardID, _ = parseHAResultUint(lines, "identity.shard_id")
	result.FenceTableID, _ = parseHAResultUint(lines, "identity.table_id")
	result.FenceOldPrimaryID = strings.TrimSpace(lines["old_primary_id"])
	result.FencePromotedNodeID = strings.TrimSpace(lines["promoted_node_id"])
	result.FenceParentTimelineID, _ = parseHAResultUint(lines, "parent_timeline_id")
	result.FenceParentEpoch, _ = parseHAResultUint(lines, "parent_epoch")
	result.FenceNewTimelineID, _ = parseHAResultUint(lines, "new_timeline_id")
	result.FenceNewEpoch, _ = parseHAResultUint(lines, "new_epoch")
	result.FenceRequiredLSN, _ = parseHAResultUint(lines, "required_lsn")
	result.FenceObservedLSN, _ = parseHAResultUint(lines, "observed_lsn")
	result.FenceForced, _ = parseHAResultBool(lines, "forced")
	result.FenceReason = strings.TrimSpace(lines["reason"])
	result.FenceGeneration, _ = parseHAResultUint(lines, "generation")
	if result.FenceGeneration == 0 {
		result.FenceGeneration, _ = parseHAResultUint(lines, "fence_generation")
	}
	if resultName == "promote_current_fence" || resultName == "promote" {
		promotion, ok := parseHAPromotionJobResult(body)
		if !ok {
			return nil, false
		}
		return haPromotionAdminActionResult(promotion), true
	}
	if strings.HasPrefix(resultName, "rejoin_") {
		rejoin, ok := parseHARejoinJobResult(body)
		if !ok {
			return nil, false
		}
		applyHARejoinAdminActionResult(result, rejoin)
	}
	if result.SlotName == "" {
		result.SlotName = strings.TrimSpace(lines["name"])
	}
	if result.SlotAction == "" && strings.HasPrefix(resultName, "slot") {
		result.SlotAction = resultName
	}
	if result.SlotAction == "" &&
		result.ActionID == "" &&
		result.ActionKind == "" &&
		result.ActionTarget == "" &&
		result.ActionState == "" &&
		result.SlotName == "" &&
		result.ManifestID == "" &&
		result.BackupLSN == 0 &&
		result.StartRecordLSN == 0 &&
		result.EndRecordLSN == 0 &&
		result.CheckpointLSN == 0 &&
		result.FenceGeneration == 0 &&
		result.FenceToken == "" &&
		result.FencePromotedNodeID == "" &&
		result.RejoinAction == "" {
		return nil, false
	}
	return result, true
}

func (r *AntflyClusterReconciler) applyHADirectPromotionResultFromSDK(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, response adminsdk.HAPromotionResponse) bool {
	if cluster == nil || cluster.Status.HAStatus == nil || action == nil {
		return false
	}
	return r.applyHADirectPromotionJobResult(cluster, action, haPromotionJobResultFromSDK(response))
}

func (r *AntflyClusterReconciler) applyHADirectPromotionJobResult(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, result haPromotionJobResult) bool {
	if cluster == nil || cluster.Status.HAStatus == nil || action == nil {
		return false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || action.StandbyName == "" {
		return false
	}
	if !haPromotionResultMatchesAction(result, identity, action) {
		return false
	}
	if !haJobResultActionReceiptMatches(
		result.ActionID,
		result.ActionKind,
		result.ActionTarget,
		result.ActionState,
		"promotion",
		strings.TrimSpace(action.StandbyName),
		"applied",
	) {
		return false
	}
	if !adminsdk.HAReceiptNodeMatches(adminsdk.HAActionReceipt{NodeId: result.ActionNodeID}, action.AdminNodeID, true) {
		return false
	}
	action.AdminResult = haPromotionAdminActionResult(result)
	enrichHAPromotionAdminActionResult(action.AdminResult, identity, *action)
	if cluster.Status.HAStatus.LastPromotion == nil ||
		!haPromotionStatusMatches(cluster.Status.HAStatus.LastPromotion, identity, *action) {
		now := metav1.Now()
		cluster.Status.HAStatus.LastPromotion = &antflyv1.HAPromotionStatus{
			ClusterID:         identity.ClusterID,
			ShardID:           identity.ShardID,
			TableID:           identity.TableID,
			OldPrimaryID:      identity.CurrentPrimaryID,
			PromotedStandbyID: action.StandbyName,
			FenceAuthority:    action.FenceAuthority,
			FenceReason:       haPromotionFenceReason(*action),
			CompletionTime:    &now,
		}
	}
	applyHAPromotionJobResult(cluster.Status.HAStatus.LastPromotion, result)
	if cluster.Status.HAStatus.LastPromotion.FenceAuthority == "" {
		cluster.Status.HAStatus.LastPromotion.FenceAuthority = action.FenceAuthority
	}
	if cluster.Status.HAStatus.LastPromotion.FenceReason == "" {
		cluster.Status.HAStatus.LastPromotion.FenceReason = haPromotionFenceReason(*action)
	}
	return true
}

func haPromotionJobResultFromSDK(response adminsdk.HAPromotionResponse) haPromotionJobResult {
	return haPromotionJobResult{
		SchemaVersion:    response.SchemaVersion,
		ActionID:         strings.TrimSpace(response.Action.ActionId),
		ActionKind:       strings.TrimSpace(string(response.Action.ActionKind)),
		ActionTarget:     strings.TrimSpace(response.Action.Target),
		ActionState:      strings.TrimSpace(string(response.Action.State)),
		ActionNodeID:     strings.TrimSpace(response.Action.NodeId),
		PromotedNodeID:   strings.TrimSpace(response.Promotion.NodeId),
		SwitchLSN:        response.Promotion.SwitchLsn,
		ParentClusterID:  response.Promotion.OldIdentity.ClusterId,
		ParentShardID:    response.Promotion.OldIdentity.ShardId,
		ParentTableID:    response.Promotion.OldIdentity.TableId,
		ParentTimelineID: response.Promotion.OldIdentity.TimelineId,
		ParentEpoch:      response.Promotion.OldIdentity.Epoch,
		NewClusterID:     response.Promotion.NewIdentity.ClusterId,
		NewShardID:       response.Promotion.NewIdentity.ShardId,
		NewTableID:       response.Promotion.NewIdentity.TableId,
		NewTimelineID:    response.Promotion.NewIdentity.TimelineId,
		NewEpoch:         response.Promotion.NewIdentity.Epoch,
		RequiredLSN:      response.Assessment.RequiredLsn,
		ObservedLSN:      response.Assessment.ReceivedLsn,
		FenceGeneration:  response.FenceGeneration,
		FenceToken:       strings.TrimSpace(response.FenceToken),
		Forced:           response.Forced || response.Promotion.Forced,
		PromotionMode:    strings.TrimSpace(string(response.Assessment.Mode)),
		DataLossPossible: response.Promotion.DataLossPossible,
	}
}

func haPromotionAdminActionResult(result haPromotionJobResult) *antflyv1.HAAdminActionResultStatus {
	return &antflyv1.HAAdminActionResultStatus{
		SchemaVersion:             result.SchemaVersion,
		ActionID:                  strings.TrimSpace(result.ActionID),
		ActionKind:                strings.TrimSpace(result.ActionKind),
		ActionTarget:              strings.TrimSpace(result.ActionTarget),
		ActionState:               strings.TrimSpace(result.ActionState),
		ActionNodeID:              strings.TrimSpace(result.ActionNodeID),
		FenceGeneration:           result.FenceGeneration,
		FenceToken:                result.FenceToken,
		FenceClusterID:            result.NewClusterID,
		FenceShardID:              result.NewShardID,
		FenceTableID:              result.NewTableID,
		FencePromotedNodeID:       strings.TrimSpace(result.PromotedNodeID),
		FenceParentTimelineID:     result.ParentTimelineID,
		FenceParentEpoch:          result.ParentEpoch,
		FenceNewTimelineID:        result.NewTimelineID,
		FenceNewEpoch:             result.NewEpoch,
		FenceRequiredLSN:          result.RequiredLSN,
		FenceObservedLSN:          result.ObservedLSN,
		FenceForced:               result.Forced,
		PromotionForce:            result.Forced,
		PromotionMode:             strings.TrimSpace(result.PromotionMode),
		PromotionDataLossPossible: result.DataLossPossible,
	}
}

func enrichHAPromotionAdminActionResult(result *antflyv1.HAAdminActionResultStatus, identity *antflyv1.HAReplicationIdentitySpec, action antflyv1.HAPlannedActionStatus) {
	if result == nil {
		return
	}
	if identity != nil {
		if result.FenceClusterID == 0 {
			result.FenceClusterID = identity.ClusterID
		}
		if result.FenceShardID == 0 {
			result.FenceShardID = identity.ShardID
		}
		if result.FenceTableID == 0 {
			result.FenceTableID = identity.TableID
		}
		if result.FenceOldPrimaryID == "" {
			result.FenceOldPrimaryID = identity.CurrentPrimaryID
		}
	}
	if result.FencePromotedNodeID == "" {
		result.FencePromotedNodeID = strings.TrimSpace(action.StandbyName)
	}
	if result.FenceReason == "" {
		result.FenceReason = haPromotionFenceReason(action)
	}
}

func haPromotionResultMatchesAction(result haPromotionJobResult, identity *antflyv1.HAReplicationIdentitySpec, action *antflyv1.HAPlannedActionStatus) bool {
	if identity == nil || action == nil {
		return false
	}
	if result.ParentClusterID != 0 &&
		(result.ParentClusterID != identity.ClusterID ||
			result.ParentShardID != identity.ShardID ||
			result.ParentTableID != identity.TableID) {
		return false
	}
	if result.NewClusterID != 0 &&
		(result.NewClusterID != identity.ClusterID ||
			result.NewShardID != identity.ShardID ||
			result.NewTableID != identity.TableID) {
		return false
	}
	if result.ParentTimelineID != identity.TimelineID || result.ParentEpoch != identity.Epoch {
		return false
	}
	if result.NewTimelineID != identity.TimelineID+1 || result.NewEpoch != identity.Epoch+1 {
		return false
	}
	if result.SwitchLSN == 0 || result.SwitchLSN != result.ObservedLSN+1 {
		return false
	}
	if action.TargetLSN > 0 && result.RequiredLSN != action.TargetLSN {
		return false
	}
	if strings.TrimSpace(action.StandbyName) != "" && result.PromotedNodeID != strings.TrimSpace(action.StandbyName) {
		return false
	}
	if !result.Forced && result.ObservedLSN < result.RequiredLSN {
		return false
	}
	if action.FenceGeneration > 0 && result.FenceGeneration != action.FenceGeneration {
		return false
	}
	return true
}

func (r *AntflyClusterReconciler) updateHALastPromotionFromAdminJobs(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	if cluster.Status.HAStatus == nil {
		return
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil {
		return
	}
	for i, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind != string(haActionPromoteStandby) ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			action.StandbyName == "" {
			continue
		}
		if !haPlannedActionDependenciesSucceededForStatus(cluster.Status.HAStatus, cluster.Status.HAStatus.PlannedActions, i, cluster) {
			continue
		}
		if action.AdminResult == nil {
			r.updateHAAdminActionResultFromJobLogs(ctx, cluster, &action)
			cluster.Status.HAStatus.PlannedActions[i] = action
		}
		if !haAdminActionSucceededWithEvidence(action) {
			return
		}
		if haPromotionStatusMatches(cluster.Status.HAStatus.LastPromotion, identity, action) {
			if cluster.Status.HAStatus.LastPromotion.FenceAuthority == "" {
				cluster.Status.HAStatus.LastPromotion.FenceAuthority = action.FenceAuthority
			}
			r.updateHAPromotionStatusFromAdminJobLogs(ctx, cluster, action, cluster.Status.HAStatus.LastPromotion)
			return
		}
		now := metav1.Now()
		switchLSN := action.TargetLSN
		observedLSN := action.TargetLSN
		if action.AdminResult != nil && action.AdminResult.FenceObservedLSN > 0 {
			observedLSN = action.AdminResult.FenceObservedLSN
			switchLSN = action.AdminResult.FenceObservedLSN + 1
		} else if action.TargetLSN > 0 {
			switchLSN = action.TargetLSN + 1
		}
		cluster.Status.HAStatus.LastPromotion = &antflyv1.HAPromotionStatus{
			ClusterID:         identity.ClusterID,
			ShardID:           identity.ShardID,
			TableID:           identity.TableID,
			OldPrimaryID:      identity.CurrentPrimaryID,
			PromotedStandbyID: action.StandbyName,
			ParentTimelineID:  identity.TimelineID,
			ParentEpoch:       identity.Epoch,
			NewTimelineID:     identity.TimelineID + 1,
			NewEpoch:          identity.Epoch + 1,
			SwitchLSN:         switchLSN,
			RequiredLSN:       action.TargetLSN,
			ObservedLSN:       observedLSN,
			FenceGeneration:   action.FenceGeneration,
			FenceAuthority:    action.FenceAuthority,
			FenceReason:       haPromotionFenceReason(action),
			CompletionTime:    &now,
		}
		r.updateHAPromotionStatusFromAdminJobLogs(ctx, cluster, action, cluster.Status.HAStatus.LastPromotion)
		return
	}
}

func (r *AntflyClusterReconciler) applyHADirectRejoinAssessResultFromSDK(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, response adminsdk.HARejoinAssessResponse) bool {
	if cluster == nil || cluster.Status.HAStatus == nil || action == nil {
		return false
	}
	return r.applyHADirectRejoinJobResult(cluster, action, haRejoinJobResultFromSDK(response))
}

func (r *AntflyClusterReconciler) applyHADirectRejoinJobResult(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, result haRejoinJobResult) bool {
	if cluster == nil || cluster.Status.HAStatus == nil || action == nil {
		return false
	}
	matchAction := *action
	if haActionKind(matchAction.Kind) != haActionDemoteFormerPrimary && result.FormerLastLSN > 0 {
		matchAction.ObservedLSN = result.FormerLastLSN
	}
	if !haDirectRejoinResultMatchesAction(result, cluster.Status.HAStatus, matchAction) {
		return false
	}
	action.AdminResult = haRejoinAdminActionResult(result)
	if !haActionHasRequiredAdminResult(*action) {
		if haActionKind(action.Kind) == haActionDemoteFormerPrimary &&
			strings.TrimSpace(action.AdminResult.RejoinAction) != "" &&
			strings.TrimSpace(action.AdminResult.FormerNodeID) != "" {
			// DemoteFormerPrimary records the assessment step. A valid assessment
			// may reject rejoin before any rewind/reseed execution result exists.
		} else {
			action.AdminResult = nil
			return false
		}
	}
	if cluster.Status.HAStatus.FormerPrimary == nil {
		cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{NodeID: action.StandbyName}
	}
	applyHAFormerPrimaryActionStatus(cluster.Status.HAStatus.FormerPrimary, *action, cluster.Status.HAStatus.LastPromotion)
	applyHARejoinJobResult(cluster.Status.HAStatus.FormerPrimary, result)
	return true
}

func haRejoinJobResultFromSDK(response adminsdk.HARejoinAssessResponse) haRejoinJobResult {
	result := haRejoinJobResult{
		SchemaVersion:     response.SchemaVersion,
		ActionID:          strings.TrimSpace(response.Action.ActionId),
		ActionKind:        strings.TrimSpace(string(response.Action.ActionKind)),
		ActionTarget:      strings.TrimSpace(response.Action.Target),
		ActionState:       strings.TrimSpace(string(response.Action.State)),
		ActionNodeID:      strings.TrimSpace(response.Action.NodeId),
		Action:            strings.TrimSpace(string(response.Assessment.Action)),
		Reason:            strings.TrimSpace(string(response.Assessment.Reason)),
		FormerNodeID:      strings.TrimSpace(response.Assessment.FormerNodeId),
		TargetTimelineID:  response.Assessment.TargetTimelineId,
		TargetEpoch:       response.Assessment.TargetEpoch,
		ParentClusterID:   response.Assessment.ParentClusterId,
		ParentShardID:     response.Assessment.ParentShardId,
		ParentTableID:     response.Assessment.ParentTableId,
		ParentTimelineID:  response.Assessment.ParentTimelineId,
		ParentEpoch:       response.Assessment.ParentEpoch,
		ForkLSN:           response.Assessment.ForkLsn,
		FormerLastLSN:     response.Assessment.FormerLastLsn,
		RetainedFromLSN:   response.Assessment.RetainedFromLsn,
		DataLossDiscarded: response.Assessment.DataLossDiscarded,
	}
	if strings.TrimSpace(response.Rewind.NodeId) != "" {
		result.RewindExecuted = true
		result.RewindPreviousLastLSN = response.Rewind.PreviousLastLsn
		result.RewindCurrentLastLSN = response.Rewind.CurrentLastLsn
		result.RewindNextLSN = response.Rewind.NextLsn
		result.RewindDiscardedLSNCount = response.Rewind.DiscardedLsnCount
		result.DataLossDiscarded = result.DataLossDiscarded || response.Rewind.DataLossDiscarded
	}
	if strings.TrimSpace(response.Reseed.NodeId) != "" {
		result.ReseedExecuted = true
		result.ReseedSlotName = strings.TrimSpace(response.Reseed.SlotName)
		result.ReseedRequired = response.Reseed.ReseedRequired
		result.ReseedBaseBackupRequired = response.Reseed.BaseBackupRequired
	}
	return result
}

func haDirectRejoinResultMatchesAction(result haRejoinJobResult, status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	var expectedActionKind string
	expectedActionState := "applied"
	switch haActionKind(action.Kind) {
	case haActionDemoteFormerPrimary:
		expectedActionKind = "rejoin_assess"
		expectedActionState = "assessed"
	case haActionRewindFormerPrimary:
		expectedActionKind = "rejoin_rewind"
	case haActionReseedFormerPrimary:
		expectedActionKind = "rejoin_reseed"
	default:
		return false
	}
	if !haJobResultActionReceiptMatches(
		result.ActionID,
		result.ActionKind,
		result.ActionTarget,
		result.ActionState,
		expectedActionKind,
		strings.TrimSpace(action.StandbyName),
		expectedActionState,
	) {
		return false
	}
	if !adminsdk.HAReceiptNodeMatches(adminsdk.HAActionReceipt{NodeId: result.ActionNodeID}, action.AdminNodeID, true) {
		return false
	}
	if haActionKind(action.Kind) == haActionRewindFormerPrimary && !result.RewindExecuted {
		return false
	}
	if haActionKind(action.Kind) == haActionReseedFormerPrimary &&
		(!result.ReseedExecuted ||
			!result.ReseedRequired ||
			!result.ReseedBaseBackupRequired ||
			strings.TrimSpace(result.ReseedSlotName) != strings.TrimSpace(result.FormerNodeID)) {
		return false
	}
	if action.StandbyName != "" && result.FormerNodeID != action.StandbyName {
		return false
	}
	if result.Action == "rewind" &&
		(result.FormerLastLSN != result.ForkLSN || result.DataLossDiscarded) {
		return false
	}
	if haActionKind(action.Kind) != haActionDemoteFormerPrimary &&
		action.TargetLSN > 0 && result.ForkLSN != action.TargetLSN {
		return false
	}
	if action.RetainedFromLSN > 0 && result.RetainedFromLSN != action.RetainedFromLSN {
		return false
	}
	if haActionKind(action.Kind) == haActionDemoteFormerPrimary {
		return true
	}
	if status != nil && status.LastPromotion != nil {
		promotion := status.LastPromotion
		if promotion.NewTimelineID > 0 && result.TargetTimelineID != promotion.NewTimelineID {
			return false
		}
		if promotion.NewEpoch > 0 && result.TargetEpoch != promotion.NewEpoch {
			return false
		}
		if promotion.ClusterID > 0 {
			if result.ParentClusterID != promotion.ClusterID ||
				result.ParentShardID != promotion.ShardID ||
				result.ParentTableID != promotion.TableID {
				return false
			}
		}
		if promotion.ParentTimelineID > 0 && result.ParentTimelineID != promotion.ParentTimelineID {
			return false
		}
		if promotion.ParentEpoch > 0 && result.ParentEpoch != promotion.ParentEpoch {
			return false
		}
	}
	return haRejoinResultMatchesPromotion(status, action, haRejoinAdminActionResult(result))
}

func haJobResultActionReceiptMatches(actionID, actionKind, actionTarget, actionState, expectedKind, expectedTarget, expectedState string) bool {
	return adminsdk.HAReceiptMatches(adminsdk.HAActionReceipt{
		ActionId:   actionID,
		ActionKind: adminsdk.HAActionReceiptActionKind(strings.TrimSpace(actionKind)),
		Target:     actionTarget,
		State:      adminsdk.HAActionReceiptState(strings.TrimSpace(actionState)),
	}, adminsdk.HAReceiptExpectation{
		ActionKind: adminsdk.HAActionReceiptActionKind(strings.TrimSpace(expectedKind)),
		State:      adminsdk.HAActionReceiptState(strings.TrimSpace(expectedState)),
	}, expectedTarget)
}

func haPromotionFenceReason(action antflyv1.HAPlannedActionStatus) string {
	if action.FenceReason != "" {
		return action.FenceReason
	}
	return action.Reason
}

func (r *AntflyClusterReconciler) updateHAPromotionStatusFromAdminJobLogs(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus, promotion *antflyv1.HAPromotionStatus) {
	if r.KubeClient == nil || action.AdminJobName == "" || promotion == nil {
		return
	}
	body, ok := r.haAdminJobLogBody(ctx, cluster, action.AdminJobName)
	if !ok {
		return
	}
	result, ok := parseHAPromotionJobResult(body)
	if !ok {
		return
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	_ = applyHAPromotionJobResultIfMatches(promotion, result, identity, action)
}

func (r *AntflyClusterReconciler) updateHAFormerPrimaryFromAdminJobs(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	if cluster.Status.HAStatus == nil {
		return
	}
	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if !haFormerPrimaryActionKind(action.Kind) ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			action.AdminJobName == "" {
			continue
		}
		if !haPlannedActionDependenciesSucceededForStatus(cluster.Status.HAStatus, cluster.Status.HAStatus.PlannedActions, i, cluster) {
			continue
		}
		if action.AdminResult == nil {
			r.updateHAAdminActionResultFromJobLogs(ctx, cluster, action)
		}
		if !haFormerPrimaryActionSucceededWithPromotionEvidence(cluster.Status.HAStatus, *action) {
			return
		}
		result, ok := haRejoinJobResultFromAdminResult(action.AdminResult)
		if !ok {
			return
		}
		if cluster.Status.HAStatus.FormerPrimary == nil {
			cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{NodeID: action.StandbyName}
		}
		applyHAFormerPrimaryActionStatus(cluster.Status.HAStatus.FormerPrimary, *action, cluster.Status.HAStatus.LastPromotion)
		applyHARejoinJobResult(cluster.Status.HAStatus.FormerPrimary, result)
		return
	}
}

func haFormerPrimaryActionKind(kind string) bool {
	return kind == string(haActionDemoteFormerPrimary) ||
		kind == string(haActionRewindFormerPrimary) ||
		kind == string(haActionReseedFormerPrimary)
}

func applyHAFormerPrimaryActionStatus(former *antflyv1.HAFormerPrimaryStatus, action antflyv1.HAPlannedActionStatus, promotion *antflyv1.HAPromotionStatus) {
	if former == nil {
		return
	}
	if action.StandbyName != "" {
		former.NodeID = action.StandbyName
	}
	if action.TargetLSN != 0 {
		former.SwitchLSN = action.TargetLSN
	}
	if action.ObservedLSN != 0 {
		former.ObservedLSN = action.ObservedLSN
	}
	if action.RetainedFromLSN != 0 {
		former.RetainedFromLSN = action.RetainedFromLSN
	}
	former.FenceAuthority = action.FenceAuthority
	former.FenceHolder = action.FenceHolder
	former.FenceGeneration = action.FenceGeneration
	if promotion != nil {
		if former.NodeID == "" {
			former.NodeID = promotion.OldPrimaryID
		}
		if former.ParentTimelineID == 0 {
			former.ParentTimelineID = promotion.ParentTimelineID
		}
		if former.NewTimelineID == 0 {
			former.NewTimelineID = promotion.NewTimelineID
		}
		if former.SwitchLSN == 0 {
			former.SwitchLSN = promotion.SwitchLSN
		}
		if former.FenceAuthority == "" {
			former.FenceAuthority = promotion.FenceAuthority
		}
		if former.FenceHolder == "" {
			former.FenceHolder = promotion.PromotedStandbyID
		}
		if former.FenceGeneration == 0 {
			former.FenceGeneration = promotion.FenceGeneration
		}
	}
	if former.Action == "" {
		former.Action = action.Kind
	}
	if former.Reason == "" {
		former.Reason = action.Reason
	}
}

func (r *AntflyClusterReconciler) haAdminJobLogBody(ctx context.Context, cluster *antflyv1.AntflyCluster, jobName string) (string, bool) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{"job-name": jobName}); err != nil {
		log.FromContext(ctx).V(1).Info("Unable to list HA admin job pods", "job", jobName, "error", err)
		return "", false
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}
		raw, err := r.KubeClient.CoreV1().Pods(cluster.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
			Container: "ha-admin",
		}).DoRaw(ctx)
		if err != nil {
			log.FromContext(ctx).V(1).Info("Unable to read HA admin job logs", "job", jobName, "pod", pod.Name, "error", err)
			continue
		}
		return string(raw), true
	}
	return "", false
}

func haPromotionStatusMatches(status *antflyv1.HAPromotionStatus, identity *antflyv1.HAReplicationIdentitySpec, action antflyv1.HAPlannedActionStatus) bool {
	return status != nil &&
		(status.ClusterID == 0 || status.ClusterID == identity.ClusterID) &&
		(status.ClusterID == 0 || status.ShardID == identity.ShardID) &&
		(status.ClusterID == 0 || status.TableID == identity.TableID) &&
		status.OldPrimaryID == identity.CurrentPrimaryID &&
		status.PromotedStandbyID == action.StandbyName &&
		status.ParentTimelineID == identity.TimelineID &&
		status.ParentEpoch == identity.Epoch &&
		status.NewTimelineID == identity.TimelineID+1 &&
		status.NewEpoch == identity.Epoch+1 &&
		(action.TargetLSN == 0 || haPromotionRequiredLSN(status) == action.TargetLSN) &&
		(action.TargetLSN == 0 || haPromotionObservedLSN(status) >= action.TargetLSN) &&
		(status.FenceAuthority == "" || status.FenceAuthority == action.FenceAuthority) &&
		status.FenceGeneration == action.FenceGeneration
}

type haPromotionJobResult struct {
	SchemaVersion    uint32
	ActionID         string
	ActionKind       string
	ActionTarget     string
	ActionState      string
	ActionNodeID     string
	PromotedNodeID   string
	SwitchLSN        uint64
	ParentClusterID  uint64
	ParentShardID    uint64
	ParentTableID    uint64
	ParentTimelineID uint64
	ParentEpoch      uint64
	NewClusterID     uint64
	NewShardID       uint64
	NewTableID       uint64
	NewTimelineID    uint64
	NewEpoch         uint64
	RequiredLSN      uint64
	ObservedLSN      uint64
	FenceGeneration  uint64
	FenceToken       string
	Forced           bool
	PromotionMode    string
	DataLossPossible bool
}

func parseHAPromotionJobResult(body string) (haPromotionJobResult, bool) {
	lines := parseHATableLines(body)
	resultName := lines["result"]
	if resultName != "promote_current_fence" && resultName != "promote" {
		return haPromotionJobResult{}, false
	}

	var result haPromotionJobResult
	var ok bool
	result.ActionID = strings.TrimSpace(lines["action.action_id"])
	result.ActionKind = strings.TrimSpace(lines["action.action_kind"])
	result.ActionTarget = strings.TrimSpace(lines["action.target"])
	result.ActionState = strings.TrimSpace(lines["action.state"])
	result.ActionNodeID = strings.TrimSpace(lines["action.node_id"])
	result.PromotedNodeID = strings.TrimSpace(lines["promotion.node_id"])
	if result.PromotedNodeID == "" {
		return haPromotionJobResult{}, false
	}
	if result.SwitchLSN, ok = parseHAResultUint(lines, "promotion.switch_lsn"); !ok {
		return haPromotionJobResult{}, false
	}
	result.ParentClusterID, _ = parseHAResultUint(lines, "promotion.old_identity.cluster_id")
	result.ParentShardID, _ = parseHAResultUint(lines, "promotion.old_identity.shard_id")
	result.ParentTableID, _ = parseHAResultUint(lines, "promotion.old_identity.table_id")
	if result.ParentTimelineID, ok = parseHAResultUint(lines, "promotion.old_identity.timeline_id"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.ParentEpoch, ok = parseHAResultUint(lines, "promotion.old_identity.epoch"); !ok {
		return haPromotionJobResult{}, false
	}
	result.NewClusterID, _ = parseHAResultUint(lines, "promotion.new_identity.cluster_id")
	result.NewShardID, _ = parseHAResultUint(lines, "promotion.new_identity.shard_id")
	result.NewTableID, _ = parseHAResultUint(lines, "promotion.new_identity.table_id")
	if result.NewTimelineID, ok = parseHAResultUint(lines, "promotion.new_identity.timeline_id"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.NewEpoch, ok = parseHAResultUint(lines, "promotion.new_identity.epoch"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.FenceGeneration, ok = parseHAResultUint(lines, "fence_generation"); !ok {
		return haPromotionJobResult{}, false
	}
	result.FenceToken = strings.TrimSpace(lines["fence_token"])
	if result.FenceToken == "" {
		return haPromotionJobResult{}, false
	}
	result.RequiredLSN, _ = parseHAResultUint(lines, "assessment.required_lsn")
	if result.RequiredLSN == 0 {
		return haPromotionJobResult{}, false
	}
	receivedLSN, ok := parseHAResultUint(lines, "assessment.received_lsn")
	if !ok {
		return haPromotionJobResult{}, false
	}
	if _, ok := parseHAResultUint(lines, "assessment.applied_lsn"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.SwitchLSN != receivedLSN+1 {
		return haPromotionJobResult{}, false
	}
	result.ObservedLSN = receivedLSN
	result.PromotionMode = strings.TrimSpace(lines["assessment.mode"])
	if result.PromotionMode == "" {
		return haPromotionJobResult{}, false
	}
	result.Forced, _ = parseHAResultBool(lines, "promotion.forced")
	result.DataLossPossible, _ = parseHAResultBool(lines, "promotion.data_loss_possible")
	if result.PromotionMode != haExpectedPromotionMode(result.Forced, result.DataLossPossible, true) {
		return haPromotionJobResult{}, false
	}
	if !result.Forced && result.ObservedLSN < result.RequiredLSN {
		return haPromotionJobResult{}, false
	}
	return result, true
}

type haPromotionAPIResultEnvelope struct {
	haPromotionAPIResult
	Result struct {
		PromoteCurrentFence *haPromotionAPIResult `json:"promote_current_fence,omitempty"`
		Promote             *haPromotionAPIResult `json:"promote,omitempty"`
	} `json:"result"`
}

type haPromotionAPIResult struct {
	SchemaVersion uint32                    `json:"schema_version"`
	Action        haAdminActionReceiptJSON  `json:"action"`
	Assessment    haPromotionAssessmentJSON `json:"assessment"`
	Promotion     struct {
		NodeID           string              `json:"node_id"`
		SwitchLSN        *uint64             `json:"switch_lsn"`
		OldIdentity      haAdminIdentityJSON `json:"old_identity"`
		NewIdentity      haAdminIdentityJSON `json:"new_identity"`
		Forced           *bool               `json:"forced"`
		DataLossPossible *bool               `json:"data_loss_possible"`
	} `json:"promotion"`
	FenceGeneration uint64 `json:"fence_generation"`
	FenceToken      string `json:"fence_token"`
	Forced          *bool  `json:"forced"`
}

func parseHAPromotionAPIResult(raw []byte) (haPromotionJobResult, bool) {
	var envelope haPromotionAPIResultEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return haPromotionJobResult{}, false
	}
	topLevel := envelope.Promotion.SwitchLSN != nil
	result := &envelope.haPromotionAPIResult
	if result.Promotion.SwitchLSN == nil {
		result = envelope.Result.PromoteCurrentFence
	}
	if result == nil {
		result = envelope.Result.Promote
	}
	if result != nil && result.SchemaVersion == 0 {
		result.SchemaVersion = envelope.SchemaVersion
	}
	if result == nil ||
		result.SchemaVersion == 0 ||
		!haPromotionAssessmentJSONComplete(result.Assessment) ||
		!haPromotionAssessmentJSONConsistent(result.Assessment) ||
		!haPromotionResultJSONComplete(result) ||
		result.FenceGeneration == 0 ||
		strings.TrimSpace(result.FenceToken) == "" {
		return haPromotionJobResult{}, false
	}
	if topLevel && !haAdminActionReceiptPresent(result.Action) {
		return haPromotionJobResult{}, false
	}
	observedLSN := haUint64JSONValue(result.Assessment.ReceivedLSN)
	requiredLSN := haUint64JSONValue(result.Assessment.RequiredLSN)
	if requiredLSN == 0 ||
		haUint64JSONValue(result.Promotion.SwitchLSN) != observedLSN+1 ||
		!haBoolJSONValue(result.Assessment.FencingConfirmed) ||
		!haBoolJSONValue(result.Assessment.CanPromote) ||
		haBoolJSONValue(result.Forced) != haBoolJSONValue(result.Promotion.Forced) ||
		haBoolJSONValue(result.Assessment.Force) != haBoolJSONValue(result.Forced) ||
		haBoolJSONValue(result.Promotion.DataLossPossible) != haBoolJSONValue(result.Assessment.DataLossPossible) ||
		haUint64JSONValue(result.Promotion.OldIdentity.ClusterID) != haUint64JSONValue(result.Promotion.NewIdentity.ClusterID) ||
		haUint64JSONValue(result.Promotion.OldIdentity.ShardID) != haUint64JSONValue(result.Promotion.NewIdentity.ShardID) ||
		haUint64JSONValue(result.Promotion.OldIdentity.TableID) != haUint64JSONValue(result.Promotion.NewIdentity.TableID) ||
		haUint64JSONValue(result.Promotion.NewIdentity.TimelineID) <= haUint64JSONValue(result.Promotion.OldIdentity.TimelineID) ||
		haUint64JSONValue(result.Promotion.NewIdentity.Epoch) <= haUint64JSONValue(result.Promotion.OldIdentity.Epoch) {
		return haPromotionJobResult{}, false
	}
	return haPromotionJobResult{
		SchemaVersion:    result.SchemaVersion,
		ActionID:         strings.TrimSpace(result.Action.ActionID),
		ActionKind:       strings.TrimSpace(result.Action.ActionKind),
		ActionTarget:     strings.TrimSpace(result.Action.Target),
		ActionState:      strings.TrimSpace(result.Action.State),
		ActionNodeID:     strings.TrimSpace(result.Action.NodeID),
		PromotedNodeID:   strings.TrimSpace(result.Promotion.NodeID),
		SwitchLSN:        haUint64JSONValue(result.Promotion.SwitchLSN),
		ParentClusterID:  haUint64JSONValue(result.Promotion.OldIdentity.ClusterID),
		ParentShardID:    haUint64JSONValue(result.Promotion.OldIdentity.ShardID),
		ParentTableID:    haUint64JSONValue(result.Promotion.OldIdentity.TableID),
		ParentTimelineID: haUint64JSONValue(result.Promotion.OldIdentity.TimelineID),
		ParentEpoch:      haUint64JSONValue(result.Promotion.OldIdentity.Epoch),
		NewClusterID:     haUint64JSONValue(result.Promotion.NewIdentity.ClusterID),
		NewShardID:       haUint64JSONValue(result.Promotion.NewIdentity.ShardID),
		NewTableID:       haUint64JSONValue(result.Promotion.NewIdentity.TableID),
		NewTimelineID:    haUint64JSONValue(result.Promotion.NewIdentity.TimelineID),
		NewEpoch:         haUint64JSONValue(result.Promotion.NewIdentity.Epoch),
		RequiredLSN:      requiredLSN,
		ObservedLSN:      observedLSN,
		FenceGeneration:  result.FenceGeneration,
		FenceToken:       strings.TrimSpace(result.FenceToken),
		Forced:           haBoolJSONValue(result.Forced) || haBoolJSONValue(result.Promotion.Forced),
		PromotionMode:    strings.TrimSpace(result.Assessment.Mode),
		DataLossPossible: haBoolJSONValue(result.Promotion.DataLossPossible),
	}, true
}

func haPromotionResultJSONComplete(result *haPromotionAPIResult) bool {
	return result != nil &&
		strings.TrimSpace(result.Promotion.NodeID) != "" &&
		result.Promotion.SwitchLSN != nil &&
		haUint64JSONValue(result.Promotion.SwitchLSN) > 0 &&
		haAdminIdentityJSONComplete(result.Promotion.OldIdentity) &&
		haAdminIdentityJSONComplete(result.Promotion.NewIdentity) &&
		result.Promotion.Forced != nil &&
		result.Promotion.DataLossPossible != nil &&
		result.Forced != nil
}

func haPromotionAssessmentJSONConsistent(assessment haPromotionAssessmentJSON) bool {
	if !haPromotionAssessmentJSONComplete(assessment) {
		return false
	}
	requiredLSN := haUint64JSONValue(assessment.RequiredLSN)
	receivedLSN := haUint64JSONValue(assessment.ReceivedLSN)
	appliedLSN := haUint64JSONValue(assessment.AppliedLSN)
	hasRequiredLSN := receivedLSN >= requiredLSN
	caughtUpToReceived := appliedLSN >= receivedLSN
	dataLossPossible := !hasRequiredLSN || !caughtUpToReceived || appliedLSN < requiredLSN
	fencingConfirmed := haBoolJSONValue(assessment.FencingConfirmed)
	forced := haBoolJSONValue(assessment.Force)
	requiresFencing := !fencingConfirmed && !forced
	requiresForce := dataLossPossible && !forced
	canPromote := !requiresFencing && (!requiresForce || forced)
	mode := haExpectedPromotionMode(forced, dataLossPossible, canPromote)
	return haBoolJSONValue(assessment.HasRequiredLSN) == hasRequiredLSN &&
		haBoolJSONValue(assessment.CaughtUpToReceived) == caughtUpToReceived &&
		strings.TrimSpace(assessment.Mode) == mode &&
		haBoolJSONValue(assessment.DataLossPossible) == dataLossPossible &&
		haBoolJSONValue(assessment.Safe) == (fencingConfirmed && !dataLossPossible) &&
		haBoolJSONValue(assessment.RequiresFencing) == requiresFencing &&
		haBoolJSONValue(assessment.RequiresForce) == requiresForce &&
		haBoolJSONValue(assessment.CanPromote) == canPromote
}

func haExpectedPromotionMode(force bool, dataLossPossible bool, canPromote bool) string {
	if !canPromote {
		return "blocked"
	}
	if dataLossPossible {
		return "lossy"
	}
	if force {
		return "forced"
	}
	return "safe"
}

func haAdminActionReceiptPresent(action haAdminActionReceiptJSON) bool {
	return strings.TrimSpace(action.ActionID) != "" &&
		strings.TrimSpace(action.ActionKind) != "" &&
		strings.TrimSpace(action.Target) != "" &&
		strings.TrimSpace(action.State) != "" &&
		strings.TrimSpace(action.NodeID) != ""
}

type haRejoinJobResult struct {
	SchemaVersion            uint32
	ActionID                 string
	ActionKind               string
	ActionTarget             string
	ActionState              string
	ActionNodeID             string
	Action                   string
	Reason                   string
	FormerNodeID             string
	TargetTimelineID         uint64
	TargetEpoch              uint64
	ParentClusterID          uint64
	ParentShardID            uint64
	ParentTableID            uint64
	ParentTimelineID         uint64
	ParentEpoch              uint64
	ForkLSN                  uint64
	FormerLastLSN            uint64
	RetainedFromLSN          uint64
	DataLossDiscarded        bool
	RewindExecuted           bool
	RewindPreviousLastLSN    uint64
	RewindCurrentLastLSN     uint64
	RewindNextLSN            uint64
	RewindDiscardedLSNCount  uint64
	ReseedExecuted           bool
	ReseedSlotName           string
	ReseedRequired           bool
	ReseedBaseBackupRequired bool
}

func parseHARejoinJobResult(body string) (haRejoinJobResult, bool) {
	lines := parseHATableLines(body)
	resultName := strings.TrimSpace(lines["result"])
	var assessmentPrefix string
	switch resultName {
	case "rejoin_assess":
		assessmentPrefix = ""
	case "rejoin_rewind", "rejoin_reseed":
		assessmentPrefix = "assessment."
	default:
		return haRejoinJobResult{}, false
	}
	result := haRejoinJobResult{
		ActionID:     strings.TrimSpace(lines["action.action_id"]),
		ActionKind:   strings.TrimSpace(lines["action.action_kind"]),
		ActionTarget: strings.TrimSpace(lines["action.target"]),
		ActionState:  strings.TrimSpace(lines["action.state"]),
		ActionNodeID: strings.TrimSpace(lines["action.node_id"]),
		Action:       strings.TrimSpace(lines[assessmentPrefix+"action"]),
		Reason:       strings.TrimSpace(lines[assessmentPrefix+"reason"]),
		FormerNodeID: strings.TrimSpace(lines[assessmentPrefix+"former_node_id"]),
	}
	if result.Action == "" || result.FormerNodeID == "" {
		return haRejoinJobResult{}, false
	}
	var ok bool
	if result.TargetTimelineID, ok = parseHAResultUint(lines, assessmentPrefix+"target_timeline_id"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.TargetEpoch, ok = parseHAResultUint(lines, assessmentPrefix+"target_epoch"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.ParentClusterID, ok = parseHAResultUint(lines, assessmentPrefix+"parent_cluster_id"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.ParentShardID, ok = parseHAResultUint(lines, assessmentPrefix+"parent_shard_id"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.ParentTableID, ok = parseHAResultUint(lines, assessmentPrefix+"parent_table_id"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.ParentTimelineID, ok = parseHAResultUint(lines, assessmentPrefix+"parent_timeline_id"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.ParentEpoch, ok = parseHAResultUint(lines, assessmentPrefix+"parent_epoch"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.ParentClusterID == 0 || result.ParentTimelineID == 0 || result.ParentEpoch == 0 {
		return haRejoinJobResult{}, false
	}
	if result.ForkLSN, ok = parseHAResultUint(lines, assessmentPrefix+"fork_lsn"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.FormerLastLSN, ok = parseHAResultUint(lines, assessmentPrefix+"former_last_lsn"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.RetainedFromLSN, ok = parseHAResultUint(lines, assessmentPrefix+"retained_from_lsn"); !ok {
		return haRejoinJobResult{}, false
	}
	result.DataLossDiscarded, _ = parseHAResultBool(lines, assessmentPrefix+"data_loss_discarded")
	if result.Action == "rewind" &&
		(result.FormerLastLSN != result.ForkLSN || result.DataLossDiscarded) {
		return haRejoinJobResult{}, false
	}
	switch resultName {
	case "rejoin_rewind":
		if strings.TrimSpace(result.Action) != "rewind" {
			return haRejoinJobResult{}, false
		}
		rewindNodeID := strings.TrimSpace(lines["rewind.node_id"])
		if rewindNodeID == "" || rewindNodeID != strings.TrimSpace(result.FormerNodeID) {
			return haRejoinJobResult{}, false
		}
		rewindForkLSN, ok := parseHAResultUint(lines, "rewind.fork_lsn")
		if !ok || rewindForkLSN != result.ForkLSN {
			return haRejoinJobResult{}, false
		}
		if result.RewindPreviousLastLSN, ok = parseHAResultUint(lines, "rewind.previous_last_lsn"); !ok {
			return haRejoinJobResult{}, false
		}
		if result.RewindCurrentLastLSN, ok = parseHAResultUint(lines, "rewind.current_last_lsn"); !ok {
			return haRejoinJobResult{}, false
		}
		if result.RewindNextLSN, ok = parseHAResultUint(lines, "rewind.next_lsn"); !ok {
			return haRejoinJobResult{}, false
		}
		if result.RewindDiscardedLSNCount, ok = parseHAResultUint(lines, "rewind.discarded_lsn_count"); !ok {
			return haRejoinJobResult{}, false
		}
		rewindTargetTimelineID, ok := parseHAResultUint(lines, "rewind.target_timeline_id")
		if !ok || rewindTargetTimelineID != result.TargetTimelineID {
			return haRejoinJobResult{}, false
		}
		rewindTargetEpoch, ok := parseHAResultUint(lines, "rewind.target_epoch")
		if !ok || rewindTargetEpoch != result.TargetEpoch {
			return haRejoinJobResult{}, false
		}
		rewindDataLossDiscarded, _ := parseHAResultBool(lines, "rewind.data_loss_discarded")
		if !haSafeRejoinRewindBounds(
			result.ForkLSN,
			result.FormerLastLSN,
			result.RewindPreviousLastLSN,
			result.RewindCurrentLastLSN,
			result.RewindNextLSN,
			result.RewindDiscardedLSNCount,
		) || result.DataLossDiscarded || rewindDataLossDiscarded {
			return haRejoinJobResult{}, false
		}
		result.RewindExecuted = true
		result.DataLossDiscarded = result.DataLossDiscarded || rewindDataLossDiscarded
	case "rejoin_reseed":
		if strings.TrimSpace(result.Action) != "reseed" {
			return haRejoinJobResult{}, false
		}
		reseedNodeID := strings.TrimSpace(lines["reseed.node_id"])
		if reseedNodeID == "" || reseedNodeID != strings.TrimSpace(result.FormerNodeID) {
			return haRejoinJobResult{}, false
		}
		result.ReseedSlotName = strings.TrimSpace(lines["reseed.slot_name"])
		reseedTargetTimelineID, ok := parseHAResultUint(lines, "reseed.target_timeline_id")
		if !ok || reseedTargetTimelineID != result.TargetTimelineID {
			return haRejoinJobResult{}, false
		}
		reseedTargetEpoch, ok := parseHAResultUint(lines, "reseed.target_epoch")
		if !ok || reseedTargetEpoch != result.TargetEpoch {
			return haRejoinJobResult{}, false
		}
		reseedForkLSN, ok := parseHAResultUint(lines, "reseed.fork_lsn")
		if !ok || reseedForkLSN != result.ForkLSN {
			return haRejoinJobResult{}, false
		}
		reseedFormerLastLSN, ok := parseHAResultUint(lines, "reseed.former_last_lsn")
		if !ok || reseedFormerLastLSN != result.FormerLastLSN {
			return haRejoinJobResult{}, false
		}
		if result.ReseedRequired, ok = parseHAResultBool(lines, "reseed.reseed_required"); !ok {
			return haRejoinJobResult{}, false
		}
		if result.ReseedBaseBackupRequired, ok = parseHAResultBool(lines, "reseed.base_backup_required"); !ok {
			return haRejoinJobResult{}, false
		}
		if result.ReseedSlotName == "" ||
			result.ReseedSlotName != strings.TrimSpace(result.FormerNodeID) ||
			!result.ReseedRequired ||
			!result.ReseedBaseBackupRequired {
			return haRejoinJobResult{}, false
		}
		result.ReseedExecuted = true
	}
	return result, true
}

type haRejoinAPIResultEnvelope struct {
	SchemaVersion uint32                   `json:"schema_version"`
	Action        haAdminActionReceiptJSON `json:"action"`
	Assessment    haRejoinAPIResult        `json:"assessment"`
	Rewind        *haRejoinAPIRewind       `json:"rewind,omitempty"`
	Reseed        *haRejoinAPIReseed       `json:"reseed,omitempty"`
}

type haRejoinAPIResult struct {
	Action            string  `json:"action"`
	Reason            string  `json:"reason"`
	FormerNodeID      string  `json:"former_node_id"`
	TargetTimelineID  *uint64 `json:"target_timeline_id"`
	TargetEpoch       *uint64 `json:"target_epoch"`
	ParentClusterID   *uint64 `json:"parent_cluster_id"`
	ParentShardID     *uint64 `json:"parent_shard_id"`
	ParentTableID     *uint64 `json:"parent_table_id"`
	ParentTimelineID  *uint64 `json:"parent_timeline_id"`
	ParentEpoch       *uint64 `json:"parent_epoch"`
	ForkLSN           *uint64 `json:"fork_lsn"`
	FormerLastLSN     *uint64 `json:"former_last_lsn"`
	RetainedFromLSN   *uint64 `json:"retained_from_lsn"`
	DataLossDiscarded *bool   `json:"data_loss_discarded"`
}

type haRejoinAPIRewind struct {
	NodeID            string  `json:"node_id"`
	ForkLSN           *uint64 `json:"fork_lsn"`
	PreviousLastLSN   *uint64 `json:"previous_last_lsn"`
	CurrentLastLSN    *uint64 `json:"current_last_lsn"`
	NextLSN           *uint64 `json:"next_lsn"`
	DiscardedLSNCount *uint64 `json:"discarded_lsn_count"`
	TargetTimelineID  *uint64 `json:"target_timeline_id"`
	TargetEpoch       *uint64 `json:"target_epoch"`
	DataLossDiscarded *bool   `json:"data_loss_discarded"`
}

type haRejoinAPIReseed struct {
	NodeID             string  `json:"node_id"`
	SlotName           string  `json:"slot_name"`
	TargetTimelineID   *uint64 `json:"target_timeline_id"`
	TargetEpoch        *uint64 `json:"target_epoch"`
	ForkLSN            *uint64 `json:"fork_lsn"`
	FormerLastLSN      *uint64 `json:"former_last_lsn"`
	ReseedRequired     *bool   `json:"reseed_required"`
	BaseBackupRequired *bool   `json:"base_backup_required"`
}

func parseHARejoinAPIResult(raw []byte) (haRejoinJobResult, bool) {
	var envelope haRejoinAPIResultEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return haRejoinJobResult{}, false
	}
	assessment := envelope.Assessment
	if !haRejoinAssessmentJSONComplete(assessment) ||
		envelope.SchemaVersion == 0 ||
		!haAdminActionReceiptPresent(envelope.Action) ||
		haUint64JSONValue(assessment.TargetTimelineID) == 0 ||
		haUint64JSONValue(assessment.TargetEpoch) == 0 {
		return haRejoinJobResult{}, false
	}
	targetTimelineID := haUint64JSONValue(assessment.TargetTimelineID)
	targetEpoch := haUint64JSONValue(assessment.TargetEpoch)
	parentClusterID := haUint64JSONValue(assessment.ParentClusterID)
	parentShardID := haUint64JSONValue(assessment.ParentShardID)
	parentTableID := haUint64JSONValue(assessment.ParentTableID)
	parentTimelineID := haUint64JSONValue(assessment.ParentTimelineID)
	parentEpoch := haUint64JSONValue(assessment.ParentEpoch)
	forkLSN := haUint64JSONValue(assessment.ForkLSN)
	formerLastLSN := haUint64JSONValue(assessment.FormerLastLSN)
	retainedFromLSN := haUint64JSONValue(assessment.RetainedFromLSN)
	result := haRejoinJobResult{
		SchemaVersion:     envelope.SchemaVersion,
		ActionID:          strings.TrimSpace(envelope.Action.ActionID),
		ActionKind:        strings.TrimSpace(envelope.Action.ActionKind),
		ActionTarget:      strings.TrimSpace(envelope.Action.Target),
		ActionState:       strings.TrimSpace(envelope.Action.State),
		ActionNodeID:      strings.TrimSpace(envelope.Action.NodeID),
		Action:            strings.TrimSpace(assessment.Action),
		Reason:            strings.TrimSpace(assessment.Reason),
		FormerNodeID:      strings.TrimSpace(assessment.FormerNodeID),
		TargetTimelineID:  targetTimelineID,
		TargetEpoch:       targetEpoch,
		ParentClusterID:   parentClusterID,
		ParentShardID:     parentShardID,
		ParentTableID:     parentTableID,
		ParentTimelineID:  parentTimelineID,
		ParentEpoch:       parentEpoch,
		ForkLSN:           forkLSN,
		FormerLastLSN:     formerLastLSN,
		RetainedFromLSN:   retainedFromLSN,
		DataLossDiscarded: haBoolJSONValue(assessment.DataLossDiscarded),
	}
	if result.Action == "rewind" &&
		(result.FormerLastLSN != result.ForkLSN || result.DataLossDiscarded) {
		return haRejoinJobResult{}, false
	}
	if rewind := envelope.Rewind; rewind != nil {
		rewindForkLSN := haUint64JSONValue(rewind.ForkLSN)
		rewindPreviousLastLSN := haUint64JSONValue(rewind.PreviousLastLSN)
		rewindCurrentLastLSN := haUint64JSONValue(rewind.CurrentLastLSN)
		rewindNextLSN := haUint64JSONValue(rewind.NextLSN)
		rewindDiscardedLSNCount := haUint64JSONValue(rewind.DiscardedLSNCount)
		rewindTargetTimelineID := haUint64JSONValue(rewind.TargetTimelineID)
		rewindTargetEpoch := haUint64JSONValue(rewind.TargetEpoch)
		if strings.TrimSpace(assessment.Action) != "rewind" ||
			!haRejoinRewindJSONComplete(*rewind) ||
			strings.TrimSpace(rewind.NodeID) != strings.TrimSpace(assessment.FormerNodeID) ||
			rewindForkLSN != forkLSN ||
			rewindPreviousLastLSN != formerLastLSN ||
			rewindTargetTimelineID != targetTimelineID ||
			rewindTargetEpoch != targetEpoch ||
			!haSafeRejoinRewindBounds(
				forkLSN,
				formerLastLSN,
				rewindPreviousLastLSN,
				rewindCurrentLastLSN,
				rewindNextLSN,
				rewindDiscardedLSNCount,
			) || haBoolJSONValue(assessment.DataLossDiscarded) || haBoolJSONValue(rewind.DataLossDiscarded) {
			return haRejoinJobResult{}, false
		}
		result.RewindExecuted = true
		result.RewindPreviousLastLSN = rewindPreviousLastLSN
		result.RewindCurrentLastLSN = rewindCurrentLastLSN
		result.RewindNextLSN = rewindNextLSN
		result.RewindDiscardedLSNCount = rewindDiscardedLSNCount
		result.DataLossDiscarded = result.DataLossDiscarded || haBoolJSONValue(rewind.DataLossDiscarded)
	}
	if reseed := envelope.Reseed; reseed != nil {
		reseedTargetTimelineID := haUint64JSONValue(reseed.TargetTimelineID)
		reseedTargetEpoch := haUint64JSONValue(reseed.TargetEpoch)
		reseedForkLSN := haUint64JSONValue(reseed.ForkLSN)
		reseedFormerLastLSN := haUint64JSONValue(reseed.FormerLastLSN)
		if strings.TrimSpace(assessment.Action) != "reseed" ||
			!haRejoinReseedJSONComplete(*reseed) ||
			strings.TrimSpace(reseed.NodeID) != strings.TrimSpace(assessment.FormerNodeID) ||
			strings.TrimSpace(reseed.SlotName) != strings.TrimSpace(assessment.FormerNodeID) ||
			reseedTargetTimelineID != targetTimelineID ||
			reseedTargetEpoch != targetEpoch ||
			reseedForkLSN != forkLSN ||
			reseedFormerLastLSN != formerLastLSN ||
			!haBoolJSONValue(reseed.ReseedRequired) ||
			!haBoolJSONValue(reseed.BaseBackupRequired) {
			return haRejoinJobResult{}, false
		}
		result.ReseedExecuted = true
		result.ReseedSlotName = strings.TrimSpace(reseed.SlotName)
		result.ReseedRequired = haBoolJSONValue(reseed.ReseedRequired)
		result.ReseedBaseBackupRequired = haBoolJSONValue(reseed.BaseBackupRequired)
	}
	return result, true
}

func haRejoinAssessmentJSONComplete(assessment haRejoinAPIResult) bool {
	return strings.TrimSpace(assessment.Action) != "" &&
		strings.TrimSpace(assessment.Reason) != "" &&
		strings.TrimSpace(assessment.FormerNodeID) != "" &&
		haRejoinAssessmentActionJSONValid(assessment.Action) &&
		haRejoinAssessmentReasonJSONValid(assessment.Reason) &&
		assessment.TargetTimelineID != nil &&
		assessment.TargetEpoch != nil &&
		assessment.ParentClusterID != nil &&
		assessment.ParentShardID != nil &&
		assessment.ParentTableID != nil &&
		assessment.ParentTimelineID != nil &&
		assessment.ParentEpoch != nil &&
		haUint64JSONValue(assessment.ParentClusterID) > 0 &&
		haUint64JSONValue(assessment.ParentTimelineID) > 0 &&
		haUint64JSONValue(assessment.ParentEpoch) > 0 &&
		assessment.ForkLSN != nil &&
		assessment.FormerLastLSN != nil &&
		assessment.RetainedFromLSN != nil &&
		assessment.DataLossDiscarded != nil
}

func haRejoinAssessmentActionJSONValid(action string) bool {
	switch strings.TrimSpace(action) {
	case "reject_unfenced", "already_current", "rewind", "reseed":
		return true
	default:
		return false
	}
}

func haRejoinAssessmentReasonJSONValid(reason string) bool {
	switch strings.TrimSpace(reason) {
	case "no_fence",
		"current_timeline",
		"parent_timeline_retained",
		"parent_timeline_wal_expired",
		"incompatible_timeline",
		"wrong_old_primary",
		"wrong_cluster",
		"wrong_shard",
		"wrong_table",
		"local_lsn_before_fork":
		return true
	default:
		return false
	}
}

func haRejoinRewindJSONComplete(rewind haRejoinAPIRewind) bool {
	return strings.TrimSpace(rewind.NodeID) != "" &&
		rewind.ForkLSN != nil &&
		rewind.PreviousLastLSN != nil &&
		rewind.CurrentLastLSN != nil &&
		rewind.NextLSN != nil &&
		rewind.DiscardedLSNCount != nil &&
		rewind.TargetTimelineID != nil &&
		rewind.TargetEpoch != nil &&
		rewind.DataLossDiscarded != nil
}

func haRejoinReseedJSONComplete(reseed haRejoinAPIReseed) bool {
	return strings.TrimSpace(reseed.NodeID) != "" &&
		strings.TrimSpace(reseed.SlotName) != "" &&
		reseed.TargetTimelineID != nil &&
		reseed.TargetEpoch != nil &&
		reseed.ForkLSN != nil &&
		reseed.FormerLastLSN != nil &&
		reseed.ReseedRequired != nil &&
		reseed.BaseBackupRequired != nil
}

func haRejoinAdminActionResult(result haRejoinJobResult) *antflyv1.HAAdminActionResultStatus {
	status := &antflyv1.HAAdminActionResultStatus{}
	applyHARejoinAdminActionResult(status, result)
	return status
}

func applyHARejoinAdminActionResult(status *antflyv1.HAAdminActionResultStatus, result haRejoinJobResult) {
	if status == nil {
		return
	}
	status.SchemaVersion = result.SchemaVersion
	status.ActionID = strings.TrimSpace(result.ActionID)
	status.ActionKind = strings.TrimSpace(result.ActionKind)
	status.ActionTarget = strings.TrimSpace(result.ActionTarget)
	status.ActionState = strings.TrimSpace(result.ActionState)
	status.ActionNodeID = strings.TrimSpace(result.ActionNodeID)
	status.RejoinAction = result.Action
	status.RejoinReason = result.Reason
	status.FormerNodeID = result.FormerNodeID
	status.TargetTimelineID = result.TargetTimelineID
	status.TargetEpoch = result.TargetEpoch
	status.ForkLSN = result.ForkLSN
	status.FormerLastLSN = result.FormerLastLSN
	status.RetainedFromLSN = result.RetainedFromLSN
	status.DataLossDiscarded = result.DataLossDiscarded
	status.RewindExecuted = result.RewindExecuted
	status.RewindPreviousLastLSN = result.RewindPreviousLastLSN
	status.RewindCurrentLastLSN = result.RewindCurrentLastLSN
	status.RewindNextLSN = result.RewindNextLSN
	status.RewindDiscardedLSNCount = result.RewindDiscardedLSNCount
	status.ReseedExecuted = result.ReseedExecuted
	status.ReseedSlotName = result.ReseedSlotName
	status.ReseedRequired = result.ReseedRequired
	status.ReseedBaseBackupRequired = result.ReseedBaseBackupRequired
}

func haRejoinJobResultFromAdminResult(result *antflyv1.HAAdminActionResultStatus) (haRejoinJobResult, bool) {
	if result == nil ||
		strings.TrimSpace(result.RejoinAction) == "" ||
		strings.TrimSpace(result.FormerNodeID) == "" ||
		result.TargetTimelineID == 0 ||
		result.TargetEpoch == 0 {
		return haRejoinJobResult{}, false
	}
	return haRejoinJobResult{
		SchemaVersion:            result.SchemaVersion,
		ActionID:                 strings.TrimSpace(result.ActionID),
		ActionKind:               strings.TrimSpace(result.ActionKind),
		ActionTarget:             strings.TrimSpace(result.ActionTarget),
		ActionState:              strings.TrimSpace(result.ActionState),
		ActionNodeID:             strings.TrimSpace(result.ActionNodeID),
		Action:                   strings.TrimSpace(result.RejoinAction),
		Reason:                   strings.TrimSpace(result.RejoinReason),
		FormerNodeID:             strings.TrimSpace(result.FormerNodeID),
		TargetTimelineID:         result.TargetTimelineID,
		TargetEpoch:              result.TargetEpoch,
		ForkLSN:                  result.ForkLSN,
		FormerLastLSN:            result.FormerLastLSN,
		RetainedFromLSN:          result.RetainedFromLSN,
		DataLossDiscarded:        result.DataLossDiscarded,
		RewindExecuted:           result.RewindExecuted,
		RewindPreviousLastLSN:    result.RewindPreviousLastLSN,
		RewindCurrentLastLSN:     result.RewindCurrentLastLSN,
		RewindNextLSN:            result.RewindNextLSN,
		RewindDiscardedLSNCount:  result.RewindDiscardedLSNCount,
		ReseedExecuted:           result.ReseedExecuted,
		ReseedSlotName:           strings.TrimSpace(result.ReseedSlotName),
		ReseedRequired:           result.ReseedRequired,
		ReseedBaseBackupRequired: result.ReseedBaseBackupRequired,
	}, true
}

func applyHARejoinJobResult(former *antflyv1.HAFormerPrimaryStatus, result haRejoinJobResult) {
	if former == nil {
		return
	}
	former.NodeID = result.FormerNodeID
	former.TargetTimelineID = result.TargetTimelineID
	former.TargetEpoch = result.TargetEpoch
	former.ForkLSN = result.ForkLSN
	former.FormerLastLSN = result.FormerLastLSN
	former.RetainedFromLSN = result.RetainedFromLSN
	former.DataLossDiscarded = result.DataLossDiscarded
	former.AssessedAction = result.Action
	former.AssessedReason = result.Reason
	former.ObservedLSN = result.FormerLastLSN
	former.RejoinRequired = true
	former.RewindPossible = false
	former.ReseedRequired = false
	former.Diverged = false
	switch result.Action {
	case "reject_unfenced":
		former.Fenced = false
		former.Action = string(haActionDemoteFormerPrimary)
	case "already_current":
		former.Fenced = true
		former.RejoinRequired = false
		former.Action = "None"
	case "rewind":
		former.Fenced = true
		former.RewindPossible = true
		former.Action = string(haActionRewindFormerPrimary)
	case "reseed":
		former.Fenced = true
		former.ReseedRequired = true
		former.Diverged = true
		former.Action = string(haActionReseedFormerPrimary)
	default:
		former.Action = result.Action
	}
	if result.Reason != "" {
		former.Reason = result.Reason
	}
}

func parseHAResultUint(lines map[string]string, key string) (uint64, bool) {
	raw, ok := lines[key]
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	return value, err == nil
}

func haSafeRejoinRewindBounds(
	forkLSN uint64,
	formerLastLSN uint64,
	previousLastLSN uint64,
	currentLastLSN uint64,
	nextLSN uint64,
	discardedLSNCount uint64,
) bool {
	// A logical HA WAL rewind cannot undo writes already applied to the LSM.
	// Safe in-place reuse is therefore limited to an exact-fork former primary.
	// The operation appends the promoted timeline switch at fork+1; it never
	// reports discarded data.
	if forkLSN == ^uint64(0) || currentLastLSN == ^uint64(0) {
		return false
	}
	return formerLastLSN == forkLSN &&
		previousLastLSN == forkLSN &&
		currentLastLSN == forkLSN+1 &&
		nextLSN == currentLastLSN+1 &&
		discardedLSNCount == 0
}

func parseHAResultBool(lines map[string]string, key string) (bool, bool) {
	raw, ok := lines[key]
	if !ok {
		return false, false
	}
	switch strings.TrimSpace(raw) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func applyHAPromotionJobResult(promotion *antflyv1.HAPromotionStatus, result haPromotionJobResult) {
	if result.NewClusterID != 0 {
		promotion.ClusterID = result.NewClusterID
		promotion.ShardID = result.NewShardID
		promotion.TableID = result.NewTableID
	} else if result.ParentClusterID != 0 {
		promotion.ClusterID = result.ParentClusterID
		promotion.ShardID = result.ParentShardID
		promotion.TableID = result.ParentTableID
	}
	promotion.ParentTimelineID = result.ParentTimelineID
	promotion.ParentEpoch = result.ParentEpoch
	promotion.NewTimelineID = result.NewTimelineID
	promotion.NewEpoch = result.NewEpoch
	promotion.SwitchLSN = result.SwitchLSN
	promotion.RequiredLSN = result.RequiredLSN
	promotion.ObservedLSN = result.ObservedLSN
	promotion.FenceGeneration = result.FenceGeneration
	promotion.FenceToken = result.FenceToken
	promotion.Forced = result.Forced
	promotion.DataLossPossible = result.DataLossPossible
}

func applyHAPromotionJobResultIfMatches(promotion *antflyv1.HAPromotionStatus, result haPromotionJobResult, identity *antflyv1.HAReplicationIdentitySpec, action antflyv1.HAPlannedActionStatus) bool {
	if promotion == nil || !haPromotionResultMatchesAction(result, identity, &action) {
		return false
	}
	applyHAPromotionJobResult(promotion, result)
	return true
}

func (r *AntflyClusterReconciler) observeHAPrimaryRouteStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Mode == "" ||
		cluster.Spec.HighAvailability.Mode == antflyv1.HAModeDisabled {
		return nil
	}

	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-public-api", Namespace: cluster.Namespace}, service)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	target := strings.TrimSpace(service.Annotations[haPrimaryRouteTargetAnnotation])
	if target == "" {
		target = "primary"
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: cluster.Spec.HighAvailability.Mode}
	}
	cluster.Status.HAStatus.PrimaryRoute.CurrentTarget = target
	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = antflyv1.HAFencingAuthority(strings.TrimSpace(service.Annotations[haPrimaryRouteFenceAuthorityAnnotation]))
	cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = 0
	if raw := strings.TrimSpace(service.Annotations[haPrimaryRouteFenceGenerationAnnotation]); raw != "" {
		if fenceGeneration, err := strconv.ParseUint(raw, 10, 64); err == nil {
			cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = fenceGeneration
		}
	}
	return nil
}

func (r *AntflyClusterReconciler) observeHAPrimaryAdminStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled ||
		ha.Admin == nil {
		return nil
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: ha.Mode}
	}
	adminURL := haCurrentPrimaryAdminURL(ha, cluster.Status.HAStatus)
	if strings.TrimSpace(adminURL) == "" {
		if promoted := haPromotedPrimaryNodeID(cluster.Status.HAStatus); promoted != "" {
			cluster.Status.HAStatus.PrimaryAdminReachable = false
			cluster.Status.HAStatus.PrimaryAdminLastError = fmt.Sprintf("promoted primary %s admin URL is not configured", promoted)
			cluster.Status.HAStatus.PrimaryAdminStatusCode = 0
		}
		return nil
	}
	status, err := r.observeHAPrimaryStatusTyped(ctx, cluster, adminURL, ha)
	if err != nil {
		haStatus := cluster.Status.HAStatus
		// A pending proof is authenticated process capability, not primary
		// health. Retain only that proof so the exact current owner can advance
		// the just-created Lease once; do not merge LSN, slots, retention, sync,
		// or mark the admin endpoint reachable until authority is granted.
		if status.WatchdogProof != nil && status.WatchdogProof.Active && !status.WatchdogProof.AuthorityGranted {
			haStatus.PrimaryWatchdogProof = status.WatchdogProof.DeepCopy()
		}
		now := r.haNow()
		if haStatus.PrimaryAdminReachable || haStatus.PrimaryAdminUnreachableSince == nil {
			since := metav1.NewTime(now)
			haStatus.PrimaryAdminUnreachableSince = &since
			haStatus.PrimaryAdminConsecutiveFailures = 1
		} else if haStatus.PrimaryAdminConsecutiveFailures < math.MaxInt32 {
			haStatus.PrimaryAdminConsecutiveFailures++
		}
		haStatus.PrimaryAdminReachable = false
		haStatus.PrimaryAdminLastError = err.Error()
		if statusCode, ok := adminsdk.HAStatusCode(err); ok {
			haStatus.PrimaryAdminStatusCode = statusCode
		} else {
			haStatus.PrimaryAdminStatusCode = 0
		}
		minimumFailures, minimumDuration := haAutomaticFailoverFailureThresholds(ha)
		haStatus.PrimaryAdminFailureThresholdMet =
			haStatus.PrimaryAdminStatusCode != http.StatusUnauthorized &&
				haStatus.PrimaryAdminConsecutiveFailures >= minimumFailures &&
				haStatus.PrimaryAdminUnreachableSince != nil &&
				now.Sub(haStatus.PrimaryAdminUnreachableSince.Time) >= minimumDuration
		return err
	}
	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.PrimaryAdminLastError = ""
	cluster.Status.HAStatus.PrimaryAdminStatusCode = 0
	cluster.Status.HAStatus.PrimaryAdminConsecutiveFailures = 0
	cluster.Status.HAStatus.PrimaryAdminUnreachableSince = nil
	cluster.Status.HAStatus.PrimaryAdminFailureThresholdMet = false
	cluster.Status.HAStatus.PrimaryLSN = status.PrimaryLSN
	cluster.Status.HAStatus.Retention = status.Retention
	if status.WatchdogProof != nil {
		cluster.Status.HAStatus.PrimaryWatchdogProof = status.WatchdogProof.DeepCopy()
	}
	cluster.Status.HAStatus.Standbys = status.Standbys
	if status.Sync != nil {
		cluster.Status.HAStatus.Sync = *status.Sync
	} else {
		cluster.Status.HAStatus.Sync = antflyv1.HASyncStatus{}
	}
	return nil
}

func (r *AntflyClusterReconciler) observeHAStandbyAdminStatuses(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled {
		return nil
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: ha.Mode}
	}

	currentPrimaryID := haCurrentPrimaryNodeID(ha, cluster.Status.HAStatus)
	var observedErr error
	for _, standby := range ha.Standbys {
		if strings.TrimSpace(standby.Name) == currentPrimaryID {
			continue
		}
		if !standbyDesired(standby) || strings.TrimSpace(standby.AdminURL) == "" {
			continue
		}
		status, err := r.observeHAStandbyStatusTyped(ctx, cluster, standby.AdminURL, standby.Name, standbySlotName(standby), cluster.Status.HAStatus.PrimaryLSN, ha)
		if err != nil {
			markHAStandbyAdminError(cluster.Status.HAStatus, standby.Name, standbySlotName(standby), err)
			if observedErr == nil {
				observedErr = fmt.Errorf("standby %s: %w", standby.Name, err)
			}
			continue
		}
		mergeHAStandbyStatus(cluster.Status.HAStatus, status)
	}
	return observedErr
}

func (r *AntflyClusterReconciler) observeHAPrimaryStatusTyped(ctx context.Context, cluster *antflyv1.AntflyCluster, baseURL string, ha *antflyv1.HighAvailabilitySpec) (haObservedPrimaryStatus, error) {
	adminClient, err := r.haAdminSDKClient(cluster, baseURL)
	if err != nil {
		return haObservedPrimaryStatus{}, err
	}
	requestStartedAt := r.haNow()
	response, err := adminClient.PrimaryStatusParsedResponse(ctx, haPrimaryStatusParams(ha))
	if err != nil {
		return haObservedPrimaryStatus{}, err
	}
	observedAt := r.haNow()
	status := haObservedPrimaryStatusFromAdminSDK(*response.Value)
	if err := haValidateObservedStatusIdentity(status.Identity, ha, cluster.Status.HAStatus); err != nil {
		return haObservedPrimaryStatus{}, err
	}
	if err := haValidateObservedPrimaryNodeID(status.NodeID, ha, cluster.Status.HAStatus); err != nil {
		return haObservedPrimaryStatus{}, err
	}
	if haRuntimeLeaseWatchdogEnabled(cluster) {
		proof, err := haWatchdogProofFromAdmin(status.RawWatchdogProof, cluster, status.NodeID, false, requestStartedAt, observedAt)
		if err != nil {
			return haObservedPrimaryStatus{}, err
		}
		status.WatchdogProof = proof
		if !proof.AuthorityGranted {
			return status, fmt.Errorf("HA Lease watchdog authority is pending for node %s", status.NodeID)
		}
	}
	return status, nil
}

func (r *AntflyClusterReconciler) observeHAStandbyStatusTyped(ctx context.Context, cluster *antflyv1.AntflyCluster, baseURL string, standbyName string, slotName string, upstreamLSN uint64, ha *antflyv1.HighAvailabilitySpec) (antflyv1.HAStandbyStatus, error) {
	adminClient, err := r.haAdminSDKClient(cluster, baseURL)
	if err != nil {
		return antflyv1.HAStandbyStatus{}, err
	}
	requestStartedAt := r.haNow()
	response, err := adminClient.StandbyStatusParsedResponse(ctx, haStandbyStatusParams(upstreamLSN))
	if err != nil {
		return antflyv1.HAStandbyStatus{}, err
	}
	observedAt := r.haNow()
	observed := haObservedStandbyStatusFromAdminSDK(*response.Value, standbyName, slotName)
	if err := haValidateObservedStatusIdentity(observed.Identity, ha, cluster.Status.HAStatus); err != nil {
		return antflyv1.HAStandbyStatus{}, err
	}
	if err := haValidateObservedNodeID(observed.NodeID, standbyName, "standby"); err != nil {
		return antflyv1.HAStandbyStatus{}, err
	}
	if haRuntimeLeaseWatchdogEnabled(cluster) {
		proof, err := haWatchdogProofFromAdmin(observed.RawWatchdogProof, cluster, observed.NodeID, false, requestStartedAt, observedAt)
		if err != nil {
			return antflyv1.HAStandbyStatus{}, err
		}
		observed.Status.WatchdogProof = proof
	}
	return observed.Status, nil
}

func haPrimaryStatusParams(ha *antflyv1.HighAvailabilitySpec) *adminsdk.HAPrimaryStatusParams {
	params := &adminsdk.HAPrimaryStatusParams{}
	if ha == nil {
		return params
	}
	if ha.Retention != nil && ha.Retention.MaxLagLSN > 0 {
		params.MaxLagLsn = ha.Retention.MaxLagLSN
	}
	if ha.Retention != nil && ha.Retention.MaxRetainedBytes > 0 {
		params.MaxRetainedBytes = ha.Retention.MaxRetainedBytes
	}
	if ha.Retention != nil && ha.Retention.MaxRetainedAgeNS > 0 {
		params.MaxRetainedAgeNs = ha.Retention.MaxRetainedAgeNS
	}
	if ha.SyncPolicy != nil && ha.SyncPolicy.Mode != "" && ha.SyncPolicy.Mode != antflyv1.HADurabilityModeAsync {
		params.SyncMode = haAdminSyncModeParam(ha.SyncPolicy.Mode)
		if ha.SyncPolicy.Selection != "" {
			params.SyncSelection = haAdminSyncSelectionParam(ha.SyncPolicy.Selection)
		}
		if ha.SyncPolicy.Required > 0 && ha.SyncPolicy.Selection != antflyv1.HAStandbySelectionAll {
			params.SyncRequired = uint64(ha.SyncPolicy.Required)
		}
		for _, name := range ha.SyncPolicy.StandbyNames {
			if strings.TrimSpace(name) != "" {
				params.SyncStandby = append(params.SyncStandby, strings.TrimSpace(name))
			}
		}
		if ha.SyncPolicy.FailurePolicy != "" {
			params.SyncFailure = haAdminSyncFailureParam(ha.SyncPolicy.FailurePolicy)
		}
	}
	return params
}

func haStandbyStatusParams(upstreamLSN uint64) *adminsdk.HAStandbyStatusParams {
	params := &adminsdk.HAStandbyStatusParams{}
	if upstreamLSN > 0 {
		params.UpstreamLsn = upstreamLSN
	}
	return params
}

func haAdminSyncModeParam(mode antflyv1.HADurabilityMode) adminsdk.HAPrimaryStatusParamsSyncMode {
	switch mode {
	case antflyv1.HADurabilityModeRemoteWrite:
		return adminsdk.HAPrimaryStatusSyncModeRemoteWrite
	case antflyv1.HADurabilityModeRemoteApply:
		return adminsdk.HAPrimaryStatusSyncModeRemoteApply
	default:
		return adminsdk.HAPrimaryStatusSyncModeAsync
	}
}

func haAdminSyncSelectionParam(selection antflyv1.HAStandbySelection) adminsdk.HAPrimaryStatusParamsSyncSelection {
	switch selection {
	case antflyv1.HAStandbySelectionFirst:
		return adminsdk.HAPrimaryStatusSyncSelectionFirst
	case antflyv1.HAStandbySelectionAll:
		return adminsdk.HAPrimaryStatusSyncSelectionAll
	default:
		return adminsdk.HAPrimaryStatusSyncSelectionAny
	}
}

func haAdminSyncFailureParam(policy antflyv1.HAFailurePolicy) adminsdk.HAPrimaryStatusParamsSyncFail {
	switch policy {
	case antflyv1.HAFailurePolicyFailClosed:
		return adminsdk.HAPrimaryStatusSyncFailureFailClosed
	case antflyv1.HAFailurePolicyDegradeToAsync:
		return adminsdk.HAPrimaryStatusSyncFailureDegradeToAsync
	default:
		return adminsdk.HAPrimaryStatusSyncFailureBlock
	}
}

type haObservedPrimaryStatus struct {
	NodeID           string
	PrimaryLSN       uint64
	Retention        antflyv1.HARetentionStatus
	Standbys         []antflyv1.HAStandbyStatus
	Sync             *antflyv1.HASyncStatus
	Identity         haObservedIdentity
	RawWatchdogProof *adminsdk.HALeaseWatchdogProof
	WatchdogProof    *antflyv1.HAWatchdogProofStatus
}

type haObservedStandbyStatus struct {
	NodeID           string
	Status           antflyv1.HAStandbyStatus
	Identity         haObservedIdentity
	RawWatchdogProof *adminsdk.HALeaseWatchdogProof
}

type haObservedIdentity struct {
	ClusterID  uint64
	ShardID    uint64
	TableID    uint64
	TimelineID uint64
	Epoch      uint64
}

type haAdminIdentityJSON struct {
	ClusterID  *uint64 `json:"cluster_id"`
	ShardID    *uint64 `json:"shard_id"`
	TableID    *uint64 `json:"table_id"`
	TimelineID *uint64 `json:"timeline_id"`
	Epoch      *uint64 `json:"epoch"`
}

func parseHAPrimaryStatusJSON(raw []byte) (haObservedPrimaryStatus, error) {
	parsed, err := adminsdk.ParseHAPrimaryStatus(raw)
	if err != nil {
		return haObservedPrimaryStatus{}, err
	}
	return haObservedPrimaryStatusFromAdminSDK(*parsed), nil
}

func haObservedPrimaryStatusFromAdminSDK(parsed adminsdk.ParsedHAPrimaryStatus) haObservedPrimaryStatus {
	snapshot := parsed.Response.Snapshot
	status := haObservedPrimaryStatus{
		NodeID:     strings.TrimSpace(snapshot.NodeId),
		Identity:   haObservedIdentityFromAdminSDK(snapshot.Identity),
		PrimaryLSN: snapshot.CurrentLsn,
		Retention: antflyv1.HARetentionStatus{
			OldestRestartLSN:  snapshot.Retention.OldestRestartLsn,
			RetainedLSNCount:  snapshot.Retention.RetainedLsnCount,
			RetainedByteCount: snapshot.Retention.RetainedByteCount,
			RetainedAgeNS:     snapshot.Retention.RetainedAgeNs,
			ActiveSlots:       haUint64ToInt32(snapshot.Retention.ActiveSlots),
			ReseedRecommended: haUint64ToInt32(snapshot.Retention.ReseedRecommended),
		},
	}
	if parsed.Response.Snapshot.LeaseWatchdog.CapabilityVersion != 0 {
		proof := parsed.Response.Snapshot.LeaseWatchdog
		status.RawWatchdogProof = &proof
	}
	for _, slot := range snapshot.Slots {
		status.Standbys = append(status.Standbys, antflyv1.HAStandbyStatus{
			Name:           strings.TrimSpace(slot.Name),
			SlotName:       strings.TrimSpace(slot.Name),
			TimelineID:     slot.TimelineId,
			Active:         slot.Active,
			ReseedRequired: slot.ReseedRequired,
			RestartLSN:     slot.RestartLsn,
			ReceivedLSN:    slot.ReceivedLsn,
			AppliedLSN:     slot.AppliedLsn,
			SafeReadLSN:    slot.SafeReadLsn,
			WriteLagLSN:    slot.WriteLagLsn,
			ApplyLagLSN:    slot.ApplyLagLsn,
			SafeReadLagLSN: slot.SafeReadLagLsn,
			Status:         strings.TrimSpace(string(slot.Status)),
			LastError:      strings.TrimSpace(slot.LastError),
		})
	}
	if parsed.HasDurability {
		status.Sync = haSyncStatusFromAdminDurability(snapshot.Durability)
	}
	return status
}

func haSyncStatusFromAdminDurability(durability adminsdk.HADurabilityDecision) *antflyv1.HASyncStatus {
	mode := haDurabilityModeFromAdmin(strings.TrimSpace(string(durability.Mode)))
	if mode == "" {
		mode = antflyv1.HADurabilityModeAsync
	}
	selection := haStandbySelectionFromAdmin(strings.TrimSpace(string(durability.Selection)))
	if selection == "" {
		selection = antflyv1.HAStandbySelectionAny
	}
	degraded, action := haSyncActionFromAdminDurabilityStatus(strings.TrimSpace(string(durability.Status)))
	return &antflyv1.HASyncStatus{
		Mode:       mode,
		Selection:  selection,
		Required:   haUint64ToInt32(durability.RequiredCount),
		Satisfied:  haUint64ToInt32(durability.SatisfiedCount),
		Candidates: haUint64ToInt32(durability.CandidateCount),
		Degraded:   degraded,
		Action:     action,
	}
}

func haDurabilityModeFromAdmin(raw string) antflyv1.HADurabilityMode {
	switch raw {
	case "remote_write", "remote-write":
		return antflyv1.HADurabilityModeRemoteWrite
	case "remote_apply", "remote-apply":
		return antflyv1.HADurabilityModeRemoteApply
	case "async":
		return antflyv1.HADurabilityModeAsync
	default:
		return ""
	}
}

func haStandbySelectionFromAdmin(raw string) antflyv1.HAStandbySelection {
	switch raw {
	case "first":
		return antflyv1.HAStandbySelectionFirst
	case "all":
		return antflyv1.HAStandbySelectionAll
	case "any":
		return antflyv1.HAStandbySelectionAny
	default:
		return ""
	}
}

func haSyncActionFromAdminDurabilityStatus(raw string) (bool, string) {
	switch raw {
	case "satisfied":
		return false, "Satisfied"
	case "degraded_to_async":
		return true, "DegradeToAsync"
	case "fail_closed":
		return true, "RejectWrites"
	case "would_block":
		return true, "BlockWrites"
	default:
		return true, "Unknown"
	}
}

func parseHAStandbyStatusJSON(raw []byte, standbyName string, slotName string) (antflyv1.HAStandbyStatus, error) {
	observed, err := parseHAStandbyStatusJSONWithIdentity(raw, standbyName, slotName)
	if err != nil {
		return antflyv1.HAStandbyStatus{}, err
	}
	return observed.Status, nil
}

func parseHAStandbyStatusJSONWithIdentity(raw []byte, standbyName string, slotName string) (haObservedStandbyStatus, error) {
	response, err := adminsdk.ParseHAStandbyStatus(raw)
	if err != nil {
		return haObservedStandbyStatus{}, err
	}
	return haObservedStandbyStatusFromAdminSDK(*response, standbyName, slotName), nil
}

func haObservedStandbyStatusFromAdminSDK(response adminsdk.HAStandbyStatusResponse, standbyName string, slotName string) haObservedStandbyStatus {
	snapshot := response.Snapshot
	status := antflyv1.HAStandbyStatus{
		Name:               standbyName,
		SlotName:           slotName,
		Active:             true,
		TimelineID:         snapshot.Identity.TimelineId,
		SafeReadLSN:        snapshot.SafeReadLsn,
		UnappliedLSNCount:  snapshot.UnappliedLsnCount,
		CaughtUpToReceived: snapshot.CaughtUpToReceived,
		CanServeSafeReads:  snapshot.CanServeSafeReads,
	}
	status.ReceivedLSN = snapshot.ReceivedLsn
	status.AppliedLSN = snapshot.AppliedLsn
	status.UpstreamLSN = snapshot.UpstreamLsn
	status.WriteLagLSN = snapshot.WriteLagLsn
	status.ReceiveLagLSN = snapshot.ReceiveLagLsn
	status.ApplyLagLSN = snapshot.ApplyLagLsn
	status.LastError = strings.TrimSpace(snapshot.LastError)
	status.LastAttemptNs = snapshot.LastAttemptNs
	status.LastSuccessNs = snapshot.LastSuccessNs
	status.ReplicationFailuresTotal = snapshot.ReplicationFailuresTotal
	if status.LastError != "" {
		status.Status = "unhealthy"
	} else if status.ReceiveLagLSN > 0 || status.ApplyLagLSN > 0 {
		status.Status = "lagging"
	} else if status.CanServeSafeReads && status.Status == "" {
		status.Status = "healthy"
	}
	var rawWatchdogProof *adminsdk.HALeaseWatchdogProof
	if snapshot.LeaseWatchdog.CapabilityVersion != 0 {
		proof := snapshot.LeaseWatchdog
		rawWatchdogProof = &proof
	}
	return haObservedStandbyStatus{
		NodeID:           strings.TrimSpace(snapshot.NodeId),
		Status:           status,
		Identity:         haObservedIdentityFromAdminSDK(snapshot.Identity),
		RawWatchdogProof: rawWatchdogProof,
	}
}

func haObservedIdentityFromAdminSDK(identity adminsdk.HAIdentity) haObservedIdentity {
	return haObservedIdentity{
		ClusterID:  identity.ClusterId,
		ShardID:    identity.ShardId,
		TableID:    identity.TableId,
		TimelineID: identity.TimelineId,
		Epoch:      identity.Epoch,
	}
}

func haWatchdogProofFromAdmin(raw *adminsdk.HALeaseWatchdogProof, cluster *antflyv1.AntflyCluster, nodeID string, requireAuthority bool, requestStartedAt, observedAt time.Time) (*antflyv1.HAWatchdogProofStatus, error) {
	if raw == nil {
		return nil, fmt.Errorf("HA Lease watchdog capability proof is missing")
	}
	lease := cluster.Spec.HighAvailability.Runtime.FencingLease
	if raw.CapabilityVersion != 1 || !raw.Active || (requireAuthority && !raw.AuthorityGranted) {
		return nil, fmt.Errorf("HA Lease watchdog is not active for node %s", nodeID)
	}
	expectedMaxFenceLatencyMS := int32(10_000)
	if lease.WatchdogGraceSeconds > 0 {
		expectedMaxFenceLatencyMS = lease.WatchdogGraceSeconds * 1000
	}
	rtt := observedAt.Sub(requestStartedAt)
	if rtt < 0 {
		return nil, fmt.Errorf("HA Lease watchdog proof request clock moved backwards")
	}
	authorityRemainingMS := int32(0)
	if raw.AuthorityGranted {
		const responseSafetyMarginMS = int64(250)
		rttMS := int64((rtt + time.Millisecond - 1) / time.Millisecond)
		if raw.AuthorityRemainingMs == 0 || raw.AuthorityRemainingMs > math.MaxInt32 {
			return nil, fmt.Errorf("HA Lease watchdog authority expired or has insufficient margin after admin response RTT")
		}
		remainingMS := int64(raw.AuthorityRemainingMs) // #nosec G115 -- rejected above unless the unsigned value is at most MaxInt32.
		remainingMS -= rttMS + responseSafetyMarginMS
		if remainingMS <= 0 {
			return nil, fmt.Errorf("HA Lease watchdog authority expired or has insufficient margin after admin response RTT")
		}
		authorityRemainingMS = int32(remainingMS) // #nosec G115 -- raw and adjusted values are bounded above and below before conversion.
	} else if raw.AuthorityRemainingMs != 0 {
		return nil, fmt.Errorf("HA Lease watchdog denied authority but reported a nonzero authority remainder")
	}
	if strings.TrimSpace(raw.LeaseName) != strings.TrimSpace(lease.Name) ||
		strings.TrimSpace(raw.LeaseNamespace) != cluster.Namespace ||
		strings.TrimSpace(raw.StableTopologyId) != strings.TrimSpace(lease.TopologyID) ||
		strings.TrimSpace(raw.LocalNodeId) != strings.TrimSpace(nodeID) ||
		strings.TrimSpace(raw.ObservedHolderNodeId) == "" ||
		(raw.AuthorityGranted && strings.TrimSpace(raw.ObservedHolderNodeId) != strings.TrimSpace(raw.LocalNodeId)) ||
		strings.TrimSpace(raw.PodUid) == "" ||
		raw.ObservedLeaseTransitions == 0 || raw.ObservedLeaseTransitions > math.MaxInt32 ||
		raw.MaxFenceLatencyMs != uint64(expectedMaxFenceLatencyMS) ||
		!haWatchdogProcessBootIDValid(raw.ProcessBootId) {
		return nil, fmt.Errorf("HA Lease watchdog proof does not match the configured topology and exact runtime process")
	}
	return &antflyv1.HAWatchdogProofStatus{
		CapabilityVersion:        1,
		Active:                   true,
		AuthorityGranted:         raw.AuthorityGranted,
		AuthorityRemainingMS:     authorityRemainingMS,
		LeaseName:                strings.TrimSpace(raw.LeaseName),
		LeaseNamespace:           strings.TrimSpace(raw.LeaseNamespace),
		TopologyID:               strings.TrimSpace(raw.StableTopologyId),
		LocalNodeID:              strings.TrimSpace(raw.LocalNodeId),
		ObservedHolderNodeID:     strings.TrimSpace(raw.ObservedHolderNodeId),
		PodUID:                   strings.TrimSpace(raw.PodUid),
		ProcessBootID:            strings.TrimSpace(raw.ProcessBootId),
		ObservedLeaseTransitions: int32(raw.ObservedLeaseTransitions), // #nosec G115 -- the proof is rejected above unless it is in (0, MaxInt32].
		MaxFenceLatencyMS:        expectedMaxFenceLatencyMS,
		// Anchor process identity at request start. A response delayed across a
		// same-Pod container restart must not be combined with the replacement
		// container's Kubernetes identity by the later uncached Pod check.
		ObservedAt: metav1.NewTime(requestStartedAt),
	}, nil
}

func haWatchdogProcessBootIDValid(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func haValidateObservedStatusIdentity(observed haObservedIdentity, ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) error {
	identity := haReplicationIdentity(ha)
	if identity == nil {
		return nil
	}
	if observed.ClusterID != identity.ClusterID ||
		observed.ShardID != identity.ShardID ||
		observed.TableID != identity.TableID {
		return fmt.Errorf(
			"observed HA admin identity scope mismatch: got cluster_id=%d shard_id=%d table_id=%d, expected cluster_id=%d shard_id=%d table_id=%d",
			observed.ClusterID,
			observed.ShardID,
			observed.TableID,
			identity.ClusterID,
			identity.ShardID,
			identity.TableID,
		)
	}
	expectedTimelineID := identity.TimelineID
	expectedEpoch := identity.Epoch
	if haPromotionAdvancesIdentity(status, identity) {
		expectedTimelineID = status.LastPromotion.NewTimelineID
		expectedEpoch = status.LastPromotion.NewEpoch
	}
	if observed.TimelineID != expectedTimelineID || observed.Epoch != expectedEpoch {
		return fmt.Errorf(
			"observed HA admin identity timeline mismatch: got timeline_id=%d epoch=%d, expected timeline_id=%d epoch=%d",
			observed.TimelineID,
			observed.Epoch,
			expectedTimelineID,
			expectedEpoch,
		)
	}
	return nil
}

func haValidateObservedPrimaryNodeID(observed string, ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) error {
	expected := haCurrentPrimaryNodeID(ha, status)
	if strings.TrimSpace(expected) == "" {
		if strings.TrimSpace(observed) == "" {
			return fmt.Errorf("observed HA admin primary node_id is missing")
		}
		return nil
	}
	return haValidateObservedNodeID(observed, expected, "primary")
}

func haValidateObservedNodeID(observed string, expected string, label string) error {
	observed = strings.TrimSpace(observed)
	expected = strings.TrimSpace(expected)
	if observed == "" {
		return fmt.Errorf("observed HA admin %s node_id is missing", label)
	}
	if expected == "" {
		return fmt.Errorf("expected HA admin %s node_id is missing", label)
	}
	if observed != expected {
		return fmt.Errorf("observed HA admin %s node_id mismatch: got %q, expected %q", label, observed, expected)
	}
	return nil
}

func haPromotionAdvancesIdentity(status *antflyv1.HAStatus, identity *antflyv1.HAReplicationIdentitySpec) bool {
	promotion := haPromotionReceipt(status)
	if promotion == nil || identity == nil {
		return false
	}
	if promotion.ParentTimelineID != identity.TimelineID || promotion.ParentEpoch != identity.Epoch {
		return false
	}
	if promotion.ClusterID != 0 &&
		(promotion.ClusterID != identity.ClusterID ||
			promotion.ShardID != identity.ShardID ||
			promotion.TableID != identity.TableID) {
		return false
	}
	return true
}

func haAdminIdentityJSONComplete(identity haAdminIdentityJSON) bool {
	return identity.ClusterID != nil &&
		haUint64JSONValue(identity.ClusterID) > 0 &&
		identity.ShardID != nil &&
		identity.TableID != nil &&
		identity.TimelineID != nil &&
		haUint64JSONValue(identity.TimelineID) > 0 &&
		identity.Epoch != nil &&
		haUint64JSONValue(identity.Epoch) > 0
}

func haUint64ToInt32(raw uint64) int32 {
	if raw > uint64(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}
	return int32(raw)
}

func mergeHAStandbyStatus(status *antflyv1.HAStatus, observed antflyv1.HAStandbyStatus) {
	if status == nil || observed.Name == "" {
		return
	}
	for i := range status.Standbys {
		existing := &status.Standbys[i]
		if !haStandbyStatusMatches(*existing, observed.Name, observed.SlotName) {
			continue
		}
		if observed.TimelineID != 0 {
			existing.TimelineID = observed.TimelineID
		}
		existing.Active = existing.Active || observed.Active
		if observed.ReceivedLSN != 0 {
			existing.ReceivedLSN = observed.ReceivedLSN
		}
		if observed.AppliedLSN != 0 {
			existing.AppliedLSN = observed.AppliedLSN
		}
		if observed.SafeReadLSN != 0 {
			existing.SafeReadLSN = observed.SafeReadLSN
		}
		if observed.UpstreamLSN != 0 {
			existing.UpstreamLSN = observed.UpstreamLSN
		}
		existing.WriteLagLSN = observed.WriteLagLSN
		existing.ReceiveLagLSN = observed.ReceiveLagLSN
		existing.ApplyLagLSN = observed.ApplyLagLSN
		existing.UnappliedLSNCount = observed.UnappliedLSNCount
		existing.CaughtUpToReceived = observed.CaughtUpToReceived
		existing.CanServeSafeReads = observed.CanServeSafeReads
		existing.LastAttemptNs = observed.LastAttemptNs
		existing.LastSuccessNs = observed.LastSuccessNs
		existing.ReplicationFailuresTotal = observed.ReplicationFailuresTotal
		if observed.WatchdogProof != nil {
			existing.WatchdogProof = observed.WatchdogProof.DeepCopy()
		}
		if observed.Status != "" {
			existing.Status = observed.Status
		}
		existing.LastError = observed.LastError
		existing.AdminStatusCode = 0
		return
	}
	status.Standbys = append(status.Standbys, observed)
}

func markHAStandbyAdminError(status *antflyv1.HAStatus, standbyName string, slotName string, err error) {
	if status == nil || err == nil {
		return
	}
	standbyName = strings.TrimSpace(standbyName)
	slotName = strings.TrimSpace(slotName)
	if standbyName == "" && slotName == "" {
		return
	}
	lastError := strings.TrimSpace(err.Error())
	if lastError == "" {
		lastError = "standby admin status unavailable"
	}
	adminStatusCode := 0
	if statusCode, ok := adminsdk.HAStatusCode(err); ok {
		adminStatusCode = statusCode
	}
	for i := range status.Standbys {
		existing := &status.Standbys[i]
		if !haStandbyStatusMatches(*existing, standbyName, slotName) {
			continue
		}
		if existing.Name == "" {
			existing.Name = standbyName
		}
		if existing.SlotName == "" {
			existing.SlotName = slotName
		}
		existing.Status = "unreachable"
		existing.LastError = lastError
		existing.AdminStatusCode = adminStatusCode
		existing.CaughtUpToReceived = false
		existing.CanServeSafeReads = false
		return
	}
	status.Standbys = append(status.Standbys, antflyv1.HAStandbyStatus{
		Name:            standbyName,
		SlotName:        slotName,
		Status:          "unreachable",
		LastError:       lastError,
		AdminStatusCode: adminStatusCode,
	})
}

func haStandbyStatusMatches(existing antflyv1.HAStandbyStatus, standbyName string, slotName string) bool {
	return (standbyName != "" && existing.Name == standbyName) ||
		(slotName != "" && existing.SlotName == slotName)
}

func parseHATableLines(body string) map[string]string {
	lines := map[string]string{}
	for _, line := range strings.Split(body, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok || key == "" {
			continue
		}
		lines[key] = value
	}
	return lines
}

func (r *AntflyClusterReconciler) reconcileHAPrimaryRoute(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Mode == "" ||
		cluster.Spec.HighAvailability.Mode == antflyv1.HAModeDisabled ||
		cluster.Status.HAStatus == nil {
		return nil
	}
	for i, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind != string(haActionUpdatePrimaryRoute) {
			continue
		}
		if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, i, cluster) {
			return nil
		}
		if !haPrimaryRouteActionHasPromotionEvidence(cluster.Status.HAStatus, cluster.Status.HAStatus.PlannedActions, i) {
			return nil
		}
		if haRuntimeLeaseWatchdogEnabled(cluster) {
			ready, err := r.haPromotedRuntimeWatchdogReady(ctx, cluster, action.RouteTo, action.FenceGeneration)
			if err != nil || !ready {
				return err
			}
			ready, err = r.haCurrentLeaseAuthorizesRoute(ctx, cluster, action.RouteTo, action.FenceGeneration)
			if err != nil || !ready {
				return err
			}
		}
		if action.RouteTo == "" {
			return nil
		}
		return r.updateHAPrimaryRouteService(ctx, cluster, action)
	}
	return nil
}

func (r *AntflyClusterReconciler) haCurrentLeaseAuthorizesRoute(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeID string, generation uint64) (bool, error) {
	lease := &coordinationv1.Lease{}
	if err := r.haBoundaryReader().Get(ctx, types.NamespacedName{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != strings.TrimSpace(nodeID) ||
		lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions <= 0 ||
		lease.Annotations[haFencingLeaseAnnotationTopologyID] != haFencingLeaseTopologyID(cluster) {
		return false, nil
	}
	observedGeneration := uint64(*lease.Spec.LeaseTransitions) // #nosec G115 -- non-positive Kubernetes Lease transitions are rejected above.
	if observedGeneration != generation {
		return false, nil
	}
	ready, _ := haLeaseFenceReady(lease, generation, r.haNow())
	return ready, nil
}

func (r *AntflyClusterReconciler) haCurrentLeaseAuthorizesPromotionBoundary(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	action antflyv1.HAPlannedActionStatus,
) (bool, error) {
	if cluster == nil || cluster.Spec.HighAvailability == nil || action.TargetLSN == 0 ||
		strings.TrimSpace(action.FenceHolder) == "" || action.FenceGeneration == 0 {
		return false, nil
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) == "" {
		return false, nil
	}
	lease := &coordinationv1.Lease{}
	if err := r.haBoundaryReader().Get(ctx, types.NamespacedName{
		Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace,
	}, lease); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) != strings.TrimSpace(action.FenceHolder) ||
		lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions <= 0 ||
		uint64(*lease.Spec.LeaseTransitions) != action.FenceGeneration ||
		lease.Annotations[haFencingLeaseAnnotationTopologyID] != haFencingLeaseTopologyID(cluster) ||
		lease.Annotations[haFencingLeaseAnnotationTransferCommitted] != "true" ||
		lease.Annotations[haFencingLeaseAnnotationFormerHolder] != strings.TrimSpace(identity.CurrentPrimaryID) ||
		lease.Annotations[haFencingLeaseAnnotationTransferOriginUID] != string(cluster.UID) ||
		lease.Annotations[haFencingLeaseAnnotationCommittedTransition] != strconv.FormatUint(action.FenceGeneration, 10) {
		return false, nil
	}
	ready, _ := haLeaseFenceReady(lease, action.FenceGeneration, r.haNow())
	if !ready {
		return false, nil
	}
	// Exact equality is intentional. A higher boundary is not automatically
	// safe: the promotion receipt proves the candidate applied TargetLSN, not an
	// unrelated later value. Identity and the frozen tail advance together.
	scope := haFencingLeaseScopeForIdentity(identity, action.TargetLSN)
	return haLeaseFenceScopeMatches(lease, scope), nil
}

func (r *AntflyClusterReconciler) haPromotedRuntimeWatchdogReady(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeID string, fenceGeneration uint64) (bool, error) {
	if cluster == nil || cluster.Status.HAStatus == nil || cluster.Status.HAStatus.PrimaryWatchdogProof == nil ||
		cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil ||
		cluster.Spec.HighAvailability.Runtime.FencingLease == nil {
		return false, nil
	}
	proof := cluster.Status.HAStatus.PrimaryWatchdogProof
	lease := cluster.Spec.HighAvailability.Runtime.FencingLease
	if proof.ObservedLeaseTransitions <= 0 {
		return false, nil
	}
	observedLeaseTransitions := uint64(proof.ObservedLeaseTransitions) // #nosec G115 -- non-positive persisted proof transitions are rejected above.
	expectedMaxFenceLatencyMS := int32(10_000)
	if lease.WatchdogGraceSeconds > 0 {
		expectedMaxFenceLatencyMS = lease.WatchdogGraceSeconds * 1000
	}
	now := r.haNow()
	if proof.ObservedAt.IsZero() || now.Before(proof.ObservedAt.Time) ||
		now.Sub(proof.ObservedAt.Time) > time.Duration(proof.MaxFenceLatencyMS)*time.Millisecond ||
		proof.AuthorityRemainingMS <= 0 ||
		now.Sub(proof.ObservedAt.Time) >= time.Duration(proof.AuthorityRemainingMS)*time.Millisecond ||
		proof.MaxFenceLatencyMS != expectedMaxFenceLatencyMS ||
		(!proof.Active || !proof.AuthorityGranted || proof.CapabilityVersion != 1 ||
			proof.LocalNodeID != strings.TrimSpace(nodeID) || proof.ObservedHolderNodeID != strings.TrimSpace(nodeID) ||
			proof.PodUID == "" || proof.ProcessBootID == "" ||
			proof.LeaseName != strings.TrimSpace(lease.Name) || proof.LeaseNamespace != cluster.Namespace ||
			proof.TopologyID != strings.TrimSpace(lease.TopologyID) ||
			observedLeaseTransitions != fenceGeneration) {
		return false, nil
	}

	pods := &corev1.PodList{}
	if err := r.haBoundaryReader().List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return false, err
	}
	matches := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) != proof.PodUID || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for j := range pod.Status.ContainerStatuses {
			container := &pod.Status.ContainerStatuses[j]
			if container.Name == "antfly" && container.State.Running != nil &&
				!proof.ObservedAt.Before(&container.State.Running.StartedAt) {
				matches++
				break
			}
		}
	}
	return matches == 1, nil
}

func (r *AntflyClusterReconciler) updateHAPrimaryRouteService(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) error {
	service := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-public-api", Namespace: cluster.Namespace}, service); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	selector, selectorOK := haPublicAPISelector(cluster, effectiveTopologyMode(cluster) == topologyModeStandalone, action.RouteTo)
	if !selectorOK {
		cluster.Status.HAStatus.PrimaryRoute.DesiredTarget = action.RouteTo
		cluster.Status.HAStatus.PrimaryRoute.Stale = true
		cluster.Status.HAStatus.PrimaryRoute.Action = string(haActionUpdatePrimaryRoute)
		cluster.Status.HAStatus.PrimaryRoute.Reason = antflyv1.ReasonHAPrimaryRouteSelectorMissing
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			antflyv1.ReasonHAPrimaryRouteSelectorMissing,
			fmt.Sprintf("HA primary route target %s has no public-api Service selector", action.RouteTo),
		)
		return nil
	}
	patch := client.MergeFrom(service.DeepCopy())
	if service.Annotations == nil {
		service.Annotations = map[string]string{}
	}
	service.Annotations[haPrimaryRouteTargetAnnotation] = action.RouteTo
	if action.FenceAuthority != "" {
		service.Annotations[haPrimaryRouteFenceAuthorityAnnotation] = string(action.FenceAuthority)
	} else {
		delete(service.Annotations, haPrimaryRouteFenceAuthorityAnnotation)
	}
	if action.FenceGeneration > 0 {
		service.Annotations[haPrimaryRouteFenceGenerationAnnotation] = strconv.FormatUint(action.FenceGeneration, 10)
	} else {
		delete(service.Annotations, haPrimaryRouteFenceGenerationAnnotation)
	}
	service.Spec.Selector = selector
	service.Annotations[haPrimaryRouteSelectorAnnotation] = "true"
	if err := r.Patch(ctx, service, patch); err != nil {
		return err
	}
	cluster.Status.HAStatus.PrimaryRoute.CurrentTarget = action.RouteTo
	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = action.FenceAuthority
	cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = action.FenceGeneration
	cluster.Status.HAStatus.PrimaryRoute.Stale = false
	cluster.Status.HAStatus.PrimaryRoute.Action = "None"
	cluster.Status.HAStatus.PrimaryRoute.Reason = "PrimaryRouteCurrent"
	return nil
}

func haPriorAdminActionsSucceeded(actions []antflyv1.HAPlannedActionStatus) bool {
	for _, action := range actions {
		if !haPlannedActionRequiresAdminTarget(action) {
			continue
		}
		if !haAdminActionSucceededWithEvidence(action) {
			return false
		}
	}
	return true
}

func haPlannedActionDependenciesSucceeded(actions []antflyv1.HAPlannedActionStatus, index int, clusters ...*antflyv1.AntflyCluster) bool {
	if index < 0 || index >= len(actions) {
		return false
	}
	action := actions[index]
	if action.DependsOn == "" {
		return haPriorAdminActionsSucceeded(actions[:index])
	}
	for i := index - 1; i >= 0; i-- {
		dependency := actions[i]
		if dependency.Kind != action.DependsOn || !haPlannedActionDependencyScopeMatches(action, dependency) {
			continue
		}
		if dependency.Executor == string(haActionExecutorControllerAction) {
			if haActionKind(dependency.Kind) == haActionIsolateFormerPrimary {
				if len(clusters) > 0 && clusters[0] != nil {
					return haPhysicalIsolationSucceededWithEvidence(clusters[0], dependency)
				}
				return haPhysicalIsolationSucceededStructurallyWithEvidence(dependency)
			}
			return dependency.AdminJobPhase == haAdminJobPhaseSucceeded
		}
		return haAdminActionSucceededWithEvidence(dependency)
	}
	return false
}

func haPlannedActionDependencyScopeMatches(action, dependency antflyv1.HAPlannedActionStatus) bool {
	// Former-primary repair deliberately crosses node identities (for example,
	// demote primary-a depends on promotion of standby-a).
	if haFormerPrimaryActionKind(action.Kind) || haFormerPrimaryActionKind(dependency.Kind) ||
		haActionKind(dependency.Kind) == haActionFenceFormerPrimary || haActionKind(dependency.Kind) == haActionIsolateFormerPrimary ||
		haActionKind(action.Kind) == haActionDemoteFormerPrimary {
		return true
	}
	actionTarget := firstNonEmptyString(action.SlotName, action.StandbyName, action.RouteTo)
	dependencyTarget := firstNonEmptyString(dependency.SlotName, dependency.StandbyName, dependency.RouteTo)
	if actionTarget != "" && dependencyTarget != "" && actionTarget != dependencyTarget {
		return false
	}
	if action.SeedArtifactGeneration != "" && dependency.SeedArtifactGeneration != "" &&
		dependency.SeedArtifactGeneration != action.SeedArtifactGeneration {
		return false
	}
	return true
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func haPlannedActionDependenciesSucceededForStatus(status *antflyv1.HAStatus, actions []antflyv1.HAPlannedActionStatus, index int, clusters ...*antflyv1.AntflyCluster) bool {
	historySatisfied := haPlannedActionDependenciesSucceeded(actions, index, clusters...)
	durableSatisfied := false
	if !historySatisfied {
		if index < 0 || index >= len(actions) {
			return false
		}
		durableSatisfied = haPlannedActionDependencySatisfiedByDurableStatus(status, actions[index])
		if !durableSatisfied {
			return false
		}
	}
	action := actions[index]
	if action.DependsOn == "" {
		for i := 0; i < index; i++ {
			dependency := actions[i]
			if !haFormerPrimaryActionKind(dependency.Kind) {
				continue
			}
			if !haFormerPrimaryActionSucceededWithPromotionEvidence(status, dependency) {
				return false
			}
		}
		return true
	}
	for i := 0; i < index; i++ {
		dependency := actions[i]
		if dependency.Kind != action.DependsOn {
			continue
		}
		if !haFormerPrimaryActionKind(dependency.Kind) {
			return true
		}
		return haFormerPrimaryActionSucceededWithPromotionEvidence(status, dependency)
	}
	return durableSatisfied
}

// haPlannedActionDependencySatisfiedByDurableStatus prevents a completed
// promotion from becoming an unsatisfiable dependency after the transient
// PromoteStandby action is compacted from status. Only DemoteFormerPrimary may
// cross that compaction boundary, and it must match the exact durable promotion
// and fencing receipt. Other missing dependencies continue to fail closed.
func haPlannedActionDependencySatisfiedByDurableStatus(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	if haActionKind(action.Kind) != haActionDemoteFormerPrimary ||
		haActionKind(action.DependsOn) != haActionPromoteStandby {
		return false
	}
	promotion := haPromotionReceipt(status)
	if promotion == nil ||
		strings.TrimSpace(action.StandbyName) != strings.TrimSpace(promotion.OldPrimaryID) ||
		strings.TrimSpace(action.AdminNodeID) != strings.TrimSpace(promotion.OldPrimaryID) ||
		action.FenceAuthority != promotion.FenceAuthority ||
		action.FenceGeneration != promotion.FenceGeneration ||
		strings.TrimSpace(action.FenceHolder) != strings.TrimSpace(promotion.PromotedStandbyID) ||
		action.TargetLSN != haPromotionRequiredLSN(promotion) ||
		haPromotionObservedLSN(promotion) < haPromotionRequiredLSN(promotion) {
		return false
	}
	if routeFrom := strings.TrimSpace(action.RouteFrom); routeFrom != "" && routeFrom != strings.TrimSpace(promotion.OldPrimaryID) {
		return false
	}
	if routeTo := strings.TrimSpace(action.RouteTo); routeTo != "" && routeTo != strings.TrimSpace(promotion.PromotedStandbyID) {
		return false
	}
	return true
}

func haPrimaryRouteActionHasPromotionEvidence(status *antflyv1.HAStatus, actions []antflyv1.HAPlannedActionStatus, index int) bool {
	if index < 0 || index >= len(actions) {
		return false
	}
	action := actions[index]
	if action.Kind != string(haActionUpdatePrimaryRoute) || action.RouteTo == "" || action.RouteTo == "primary" {
		return true
	}
	recordedMatches := haPrimaryRouteActionMatchesRecordedPromotion(status, action)
	for i := index - 1; i >= 0; i-- {
		dependency := actions[i]
		if dependency.Kind != string(haActionPromoteStandby) {
			continue
		}
		return haAdminActionSucceededWithEvidence(dependency) &&
			haPrimaryRouteActionMatchesPromotionResult(status, action, dependency.AdminResult)
	}
	return recordedMatches
}

func haPrimaryRouteActionMatchesRecordedPromotion(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	promotion := haPromotionReceipt(status)
	if promotion == nil ||
		promotion.Forced ||
		promotion.DataLossPossible ||
		strings.TrimSpace(promotion.PromotedStandbyID) != strings.TrimSpace(action.RouteTo) {
		return false
	}
	if action.FenceAuthority != "" && promotion.FenceAuthority != action.FenceAuthority {
		return false
	}
	if action.FenceGeneration != 0 && promotion.FenceGeneration != action.FenceGeneration {
		return false
	}
	if action.TargetLSN != 0 {
		return haPromotionRequiredLSN(promotion) == action.TargetLSN &&
			haPromotionObservedLSN(promotion) >= action.TargetLSN
	}
	return true
}

func haPrimaryRouteActionMatchesPromotionResult(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if result == nil ||
		result.FenceForced ||
		result.PromotionForce ||
		result.PromotionDataLossPossible ||
		strings.TrimSpace(result.FencePromotedNodeID) != strings.TrimSpace(action.RouteTo) ||
		strings.TrimSpace(result.FenceToken) == "" {
		return false
	}
	if action.FenceGeneration != 0 && result.FenceGeneration != action.FenceGeneration {
		return false
	}
	if action.FenceAuthority != "" && result.FenceGeneration == 0 {
		return false
	}
	if action.TargetLSN != 0 {
		if result.FenceRequiredLSN != action.TargetLSN ||
			result.FenceObservedLSN < action.TargetLSN {
			return false
		}
	} else if result.FenceRequiredLSN == 0 || result.FenceObservedLSN < result.FenceRequiredLSN {
		return false
	}
	if promotion := haPromotionReceipt(status); promotion != nil {
		return !promotion.Forced &&
			!promotion.DataLossPossible &&
			strings.TrimSpace(promotion.PromotedStandbyID) == strings.TrimSpace(action.RouteTo) &&
			(action.FenceAuthority == "" || promotion.FenceAuthority == action.FenceAuthority) &&
			(action.FenceGeneration == 0 || promotion.FenceGeneration == action.FenceGeneration) &&
			(promotion.FenceToken == "" || promotion.FenceToken == result.FenceToken) &&
			result.FenceClusterID != 0 &&
			(promotion.ClusterID == 0 || promotion.ClusterID == result.FenceClusterID) &&
			(promotion.ClusterID == 0 || promotion.ShardID == result.FenceShardID) &&
			(promotion.ClusterID == 0 || promotion.TableID == result.FenceTableID) &&
			(promotion.ParentTimelineID == 0 || promotion.ParentTimelineID == result.FenceParentTimelineID) &&
			(promotion.ParentEpoch == 0 || promotion.ParentEpoch == result.FenceParentEpoch) &&
			(promotion.NewTimelineID == 0 || promotion.NewTimelineID == result.FenceNewTimelineID) &&
			(promotion.NewEpoch == 0 || promotion.NewEpoch == result.FenceNewEpoch) &&
			(haPromotionRequiredLSN(promotion) == 0 || haPromotionRequiredLSN(promotion) == result.FenceRequiredLSN) &&
			haPromotionObservedLSN(promotion) >= result.FenceRequiredLSN
	}
	return true
}

func haAdminActionSucceededWithEvidence(action antflyv1.HAPlannedActionStatus) bool {
	if action.AdminJobPhase != haAdminJobPhaseSucceeded {
		return false
	}
	if haActionRequiresSeedArtifactReceipt(haActionKind(action.Kind)) {
		return haSeedArtifactReceiptMatches(action)
	}
	if !haActionRequiresAdminResult(haActionKind(action.Kind)) {
		return true
	}
	if action.AdminJobName == haAdminDirectAPIName {
		if !haDirectAdminActionReceiptMatches(action) {
			return false
		}
	} else if !haAdminActionReceiptMatches(action) {
		return false
	}
	return haActionHasRequiredAdminResult(action)
}

func haActionRequiresSeedArtifactReceipt(kind haActionKind) bool {
	return haPlannedActionKindIsPortableArtifact(kind)
}

func haSeedArtifactReceiptMatches(action antflyv1.HAPlannedActionStatus) bool {
	return haSeedArtifactReceiptMatchesStatus(action, action.SeedArtifactReceipt)
}

func haSeedArtifactReceiptMatchesStatus(action antflyv1.HAPlannedActionStatus, receipt *antflyv1.HASeedArtifactReceiptStatus) bool {
	if receipt == nil ||
		strings.TrimSpace(receipt.Generation) != strings.TrimSpace(action.SeedArtifactGeneration) ||
		strings.TrimSpace(receipt.SlotName) != strings.TrimSpace(action.SlotName) {
		return false
	}
	kind := haActionKind(action.Kind)
	if kind == haActionGCSourceSeedGenerations || kind == haActionGCTargetSeedGenerations {
		expectedScope := "source_capture"
		if kind == haActionGCTargetSeedGenerations {
			expectedScope = "target_activation"
		}
		return receipt.FormatVersion == 1 && receipt.ActionKind == "gc_local_seed_generations" &&
			receipt.Scope == expectedScope && isLowerHexDigest(receipt.CheckpointSHA256) &&
			receipt.RetainedCount > 0 && receipt.ProtectedCount >= 0 && receipt.DeletedCount >= 0 &&
			receipt.ResumedTombstoneCount >= 0 && receipt.SkippedIneligibleCount >= 0
	}
	switch kind {
	case haActionPublishSeedArtifact, haActionRestoreSeedArtifact:
		if receipt.FormatVersion != 1 && receipt.FormatVersion != 2 && receipt.FormatVersion != 3 && receipt.FormatVersion != 4 {
			return false
		}
	case haActionActivateSeedArtifact:
		if receipt.FormatVersion != 2 {
			return false
		}
	default:
		// Prune receipts have a distinct v1 schema. Transport and activation
		// versions must never silently widen that evidence contract.
		if receipt.FormatVersion != 1 {
			return false
		}
	}
	if (kind == haActionPublishSeedArtifact || kind == haActionRestoreSeedArtifact || kind == haActionActivateSeedArtifact) &&
		!haSeedArtifactTopologyReceiptMatchesAction(action, receipt) {
		return false
	}
	if kind == haActionPruneSeedArtifacts {
		return receipt.RetainedCount > 0 &&
			receipt.RetainedCount <= action.SeedArtifactRetainGenerations &&
			receipt.DeletedCount >= 0
	}
	if kind == haActionActivateSeedArtifact {
		return receipt.FileCount == 0 &&
			strings.TrimSpace(receipt.ManifestID) != "" &&
			receipt.BackupLSN > 0 &&
			receipt.CheckpointLSN >= receipt.BackupLSN &&
			receipt.CheckpointLSN >= action.TargetLSN &&
			isLowerHexDigest(receipt.SeedReceiptSHA256) &&
			isLowerHexDigest(receipt.CaptureReceiptSHA256) &&
			receipt.CaptureReceiptSHA256 == strings.TrimSpace(action.SeedCaptureReceiptSHA256) &&
			isLowerHexDigest(receipt.ManifestSHA256) &&
			isLowerHexDigest(receipt.AggregateSHA256) &&
			isLowerHexDigest(receipt.MaterializedReceiptSHA256) &&
			isLowerHexDigest(receipt.MaterializedAggregateSHA256) &&
			receipt.TargetLocalNodeID == action.TargetLocalNodeID &&
			receipt.TargetReplicaID == action.TargetReplicaID &&
			strings.TrimSpace(receipt.GenerationPath) == "live-generations/"+strings.TrimSpace(action.SeedArtifactGeneration) &&
			strings.TrimSpace(receipt.RawGenerationPath) == "generations/"+strings.TrimSpace(action.SeedArtifactGeneration)
	}
	return receipt.FileCount > 0 &&
		strings.TrimSpace(receipt.ManifestID) != "" &&
		receipt.BackupLSN > 0 &&
		receipt.CheckpointLSN >= receipt.BackupLSN &&
		receipt.CheckpointLSN >= action.TargetLSN &&
		isLowerHexDigest(receipt.ManifestSHA256) &&
		isLowerHexDigest(receipt.AggregateSHA256) &&
		(strings.TrimSpace(action.SeedCaptureReceiptSHA256) == "" ||
			(receipt.CaptureReceiptSHA256 == strings.TrimSpace(action.SeedCaptureReceiptSHA256) && isLowerHexDigest(receipt.CaptureReceiptSHA256)))
}

func haSeedArtifactTopologyReceiptMatchesAction(action antflyv1.HAPlannedActionStatus, receipt *antflyv1.HASeedArtifactReceiptStatus) bool {
	bound := strings.TrimSpace(action.TopologyID) != "" || action.TopologyGeneration != 0 ||
		strings.TrimSpace(action.TopologyNodeID) != "" || strings.TrimSpace(action.TargetPVCName) != "" ||
		strings.TrimSpace(action.TargetPVCUID) != ""
	if !bound {
		return true
	}
	if receipt == nil || receipt.TopologyID != strings.TrimSpace(action.TopologyID) ||
		receipt.TopologyGeneration != action.TopologyGeneration || receipt.NodeID != strings.TrimSpace(action.TopologyNodeID) ||
		receipt.TargetPVCName != strings.TrimSpace(action.TargetPVCName) || receipt.TargetPVCUID != strings.TrimSpace(action.TargetPVCUID) {
		return false
	}
	if (haActionKind(action.Kind) == haActionPublishSeedArtifact || haActionKind(action.Kind) == haActionRestoreSeedArtifact) && receipt.FormatVersion != 4 {
		return false
	}
	return true
}

func isLowerHexDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func haAdminActionSucceededWithStatusEvidence(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	if haFormerPrimaryActionKind(action.Kind) {
		return haFormerPrimaryActionSucceededWithPromotionEvidence(status, action)
	}
	if action.Kind == string(haActionPromoteStandby) {
		return haPromotionActionSucceededWithStatusEvidence(status, action)
	}
	return haAdminActionSucceededWithEvidence(action)
}

func haPromotionActionSucceededWithStatusEvidence(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	if !haAdminActionSucceededWithEvidence(action) {
		return false
	}
	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return true
	}
	result := action.AdminResult
	return result != nil &&
		!promotion.Forced &&
		!promotion.DataLossPossible &&
		strings.TrimSpace(promotion.OldPrimaryID) == strings.TrimSpace(result.FenceOldPrimaryID) &&
		strings.TrimSpace(promotion.PromotedStandbyID) == strings.TrimSpace(action.StandbyName) &&
		strings.TrimSpace(promotion.PromotedStandbyID) == strings.TrimSpace(result.FencePromotedNodeID) &&
		result.FenceClusterID != 0 &&
		(promotion.ClusterID == 0 || promotion.ClusterID == result.FenceClusterID) &&
		(promotion.ClusterID == 0 || promotion.ShardID == result.FenceShardID) &&
		(promotion.ClusterID == 0 || promotion.TableID == result.FenceTableID) &&
		(action.FenceAuthority == "" || promotion.FenceAuthority == action.FenceAuthority) &&
		(action.FenceGeneration == 0 || promotion.FenceGeneration == action.FenceGeneration) &&
		promotion.FenceGeneration == result.FenceGeneration &&
		promotion.FenceToken == result.FenceToken &&
		promotion.ParentTimelineID == result.FenceParentTimelineID &&
		promotion.ParentEpoch == result.FenceParentEpoch &&
		promotion.NewTimelineID == result.FenceNewTimelineID &&
		promotion.NewEpoch == result.FenceNewEpoch &&
		haPromotionRequiredLSN(promotion) == result.FenceRequiredLSN &&
		haPromotionObservedLSN(promotion) >= result.FenceRequiredLSN &&
		result.FenceObservedLSN >= result.FenceRequiredLSN
}

func haActionRequiresAdminResult(kind haActionKind) bool {
	_, _, ok := haDirectAdminActionReceiptSpec(kind)
	return ok
}

func haActionHasRequiredAdminResult(action antflyv1.HAPlannedActionStatus) bool {
	result := action.AdminResult
	if result == nil {
		return false
	}
	expectedSlotName := haActionSlotName(action)
	expectedManifestID := haExpectedSeedBeginManifestID(action, expectedSlotName)
	switch haActionKind(action.Kind) {
	case haActionCreateSlot:
		return result.SlotAction == "create" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionResumeSlot:
		return result.SlotAction == "resume" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionPauseSlot:
		return result.SlotAction == "pause" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionDropSlot:
		return result.SlotAction == "drop" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionSeedStandby, haActionMarkReseed:
		return haResultSlotNameMatches(result.SlotName, expectedSlotName) &&
			haResultManifestMatches(result.ManifestID, expectedManifestID) &&
			haResultBackupLSNMatches(result.BackupLSN, action.TargetLSN) &&
			result.StartRecordLSN > 0
	case haActionFinishStandbySeed:
		return haResultManifestMatches(result.ManifestID, expectedManifestID) &&
			haResultBackupLSNMatches(result.BackupLSN, action.TargetLSN) &&
			result.EndRecordLSN > 0
	case haActionCaptureSeedArtifact:
		return haResultSlotNameMatches(result.SlotName, expectedSlotName) &&
			strings.TrimSpace(result.ManifestID) == strings.TrimSpace(action.SeedArtifactGeneration) &&
			strings.TrimSpace(result.SeedArtifactGeneration) == strings.TrimSpace(action.SeedArtifactGeneration) &&
			result.BackupLSN >= action.TargetLSN && result.CheckpointLSN >= result.BackupLSN &&
			result.EndRecordLSN >= result.CheckpointLSN && result.SeedClusterID > 0 &&
			result.SeedTimelineID > 0 && result.SeedEpoch > 0 && result.SeedFileCount > 0 &&
			isLowerHexDigest(result.SeedSourcePlanSHA256) && isLowerHexDigest(result.ManifestSHA256) &&
			isLowerHexDigest(result.CaptureReceiptSHA256) &&
			path.IsAbs(result.SeedGenerationRoot) && path.IsAbs(result.SeedContentRoot) && path.IsAbs(result.SeedManifestPath)
	case haActionActivateSeededSlot:
		return haResultSlotNameMatches(result.SlotName, expectedSlotName) &&
			strings.TrimSpace(result.ManifestID) != "" &&
			result.CheckpointLSN >= action.TargetLSN &&
			strings.TrimSpace(result.SeedArtifactGeneration) == strings.TrimSpace(action.SeedArtifactGeneration) &&
			isLowerHexDigest(result.SeedReceiptSHA256) &&
			isLowerHexDigest(result.CaptureReceiptSHA256) &&
			isLowerHexDigest(result.ManifestSHA256) &&
			isLowerHexDigest(result.AggregateSHA256)
	case haActionBootstrapStandbySeed:
		return haResultManifestMatches(result.ManifestID, expectedManifestID) &&
			haResultBackupLSNMatches(result.BackupLSN, action.TargetLSN) &&
			result.CheckpointLSN > 0
	case haActionAcquireFence:
		return result.FenceGeneration > 0 &&
			(action.FenceGeneration == 0 || result.FenceGeneration == action.FenceGeneration) &&
			result.FenceToken != "" &&
			(action.StandbyName == "" || result.FencePromotedNodeID == action.StandbyName)
	case haActionFenceFormerPrimary:
		return result.FenceGeneration > 0 &&
			(action.FenceGeneration == 0 || result.FenceGeneration == action.FenceGeneration) &&
			result.FenceToken != "" &&
			(action.StandbyName == "" || result.FenceOldPrimaryID == action.StandbyName) &&
			(action.RouteTo == "" || result.FencePromotedNodeID == action.RouteTo)
	case haActionAssessPromotion:
		return haPromotionAssessmentResultMatchesAction(action, result)
	case haActionPromoteStandby:
		return haPromotionAdminResultHasEvidence(action, result)
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		return haRejoinResultMatchesRequiredAdminResult(action, result)
	default:
		return true
	}
}

func haPromotionAssessmentResultMatchesAction(action antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if result == nil ||
		!result.PromotionCanPromote ||
		!result.PromotionFenced ||
		!result.PromotionSafe ||
		result.PromotionForce ||
		strings.TrimSpace(result.PromotionMode) != "safe" ||
		result.PromotionDataLossPossible ||
		result.PromotionRequiresFencing ||
		result.PromotionRequiresForce {
		return false
	}
	if action.TargetLSN > 0 {
		return result.PromotionRequiredLSN == action.TargetLSN &&
			result.PromotionReceivedLSN >= action.TargetLSN &&
			result.PromotionAppliedLSN >= action.TargetLSN
	}
	return result.PromotionReceivedLSN >= result.PromotionRequiredLSN &&
		result.PromotionAppliedLSN >= result.PromotionRequiredLSN
}

func haDirectAdminActionReceiptMatches(action antflyv1.HAPlannedActionStatus) bool {
	result := action.AdminResult
	if result == nil || result.SchemaVersion == 0 {
		return false
	}
	return haAdminActionReceiptMatches(action)
}

func haAdminActionReceiptMatches(action antflyv1.HAPlannedActionStatus) bool {
	result := action.AdminResult
	if result == nil {
		return false
	}
	expectedKind, expectedTarget, expectedState := haDirectAdminActionReceiptExpectation(action)
	requireExpectedNode := haAdminActionReceiptRequiresPlannedNode(action)
	expectedNodeID := action.AdminNodeID
	return adminsdk.HAReceiptMatchesNode(adminsdk.HAActionReceipt{
		ActionId:   result.ActionID,
		ActionKind: adminsdk.HAActionReceiptActionKind(strings.TrimSpace(result.ActionKind)),
		Target:     result.ActionTarget,
		State:      adminsdk.HAActionReceiptState(strings.TrimSpace(result.ActionState)),
		NodeId:     result.ActionNodeID,
	}, adminsdk.HAReceiptExpectation{
		ActionKind: adminsdk.HAActionReceiptActionKind(strings.TrimSpace(expectedKind)),
		State:      adminsdk.HAActionReceiptState(strings.TrimSpace(expectedState)),
	}, expectedTarget, expectedNodeID, requireExpectedNode)
}

func haAdminActionReceiptRequiresPlannedNode(action antflyv1.HAPlannedActionStatus) bool {
	return action.AdminJobName == haAdminDirectAPIName ||
		strings.TrimSpace(action.AdminURL) != "" ||
		strings.TrimSpace(action.AdminMethod) != "" ||
		strings.TrimSpace(action.AdminPath) != ""
}

func haDirectAdminActionReceiptExpectation(action antflyv1.HAPlannedActionStatus) (string, string, string) {
	expectedKind, expectedState, ok := haDirectAdminActionReceiptSpec(haActionKind(action.Kind))
	if !ok {
		return "", "", ""
	}
	return expectedKind, haDirectAdminActionReceiptTarget(action), expectedState
}

func haDirectAdminActionReceiptTarget(action antflyv1.HAPlannedActionStatus) string {
	expectedSlotName := haActionSlotName(action)
	expectedManifestID := haExpectedSeedBeginManifestID(action, expectedSlotName)
	switch haActionKind(action.Kind) {
	case haActionCreateSlot, haActionResumeSlot, haActionPauseSlot, haActionDropSlot:
		return expectedSlotName
	case haActionSeedStandby, haActionMarkReseed:
		return expectedManifestID
	case haActionFinishStandbySeed, haActionBootstrapStandbySeed:
		return expectedManifestID
	case haActionCaptureSeedArtifact:
		return strings.TrimSpace(action.SeedArtifactGeneration)
	case haActionActivateSeededSlot:
		return strings.TrimSpace(action.SeedArtifactGeneration)
	case haActionAcquireFence, haActionAssessPromotion, haActionPromoteStandby, haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		return strings.TrimSpace(action.StandbyName)
	case haActionFenceFormerPrimary:
		return strings.TrimSpace(action.RouteTo)
	default:
		return ""
	}
}

func haPromotionAdminResultHasEvidence(action antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if result == nil ||
		result.FenceGeneration == 0 ||
		(action.FenceGeneration != 0 && result.FenceGeneration != action.FenceGeneration) ||
		strings.TrimSpace(result.FenceToken) == "" ||
		result.FenceClusterID == 0 ||
		strings.TrimSpace(result.FenceOldPrimaryID) == "" ||
		strings.TrimSpace(result.FencePromotedNodeID) == "" ||
		(action.StandbyName != "" && result.FencePromotedNodeID != action.StandbyName) ||
		result.FenceParentTimelineID == 0 ||
		result.FenceParentEpoch == 0 ||
		result.FenceNewTimelineID == 0 ||
		result.FenceNewEpoch == 0 ||
		result.FenceRequiredLSN == 0 ||
		result.FenceObservedLSN < result.FenceRequiredLSN ||
		result.FenceForced ||
		result.PromotionForce ||
		strings.TrimSpace(result.PromotionMode) != "safe" ||
		result.PromotionDataLossPossible {
		return false
	}
	if action.TargetLSN != 0 && result.FenceRequiredLSN != action.TargetLSN {
		return false
	}
	return true
}

func haRejoinResultMatchesRequiredAdminResult(action antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if result == nil ||
		strings.TrimSpace(result.RejoinAction) == "" ||
		strings.TrimSpace(result.FormerNodeID) == "" ||
		result.TargetTimelineID == 0 ||
		result.TargetEpoch == 0 {
		return false
	}
	switch haActionKind(action.Kind) {
	case haActionDemoteFormerPrimary:
		switch result.RejoinAction {
		case "reject_unfenced", "already_current", "rewind", "reseed":
		default:
			return false
		}
	case haActionRewindFormerPrimary:
		if result.RejoinAction != "rewind" {
			return false
		}
		if !result.RewindExecuted || result.DataLossDiscarded ||
			!haSafeRejoinRewindBounds(
				result.ForkLSN,
				result.FormerLastLSN,
				result.RewindPreviousLastLSN,
				result.RewindCurrentLastLSN,
				result.RewindNextLSN,
				result.RewindDiscardedLSNCount,
			) {
			return false
		}
	case haActionReseedFormerPrimary:
		if result.RejoinAction != "reseed" {
			return false
		}
		if !result.ReseedExecuted ||
			!result.ReseedRequired ||
			!result.ReseedBaseBackupRequired ||
			strings.TrimSpace(result.ReseedSlotName) != strings.TrimSpace(result.FormerNodeID) {
			return false
		}
	default:
		return false
	}
	if action.StandbyName != "" && result.FormerNodeID != action.StandbyName {
		return false
	}
	if action.RetainedFromLSN > 0 && result.RetainedFromLSN != action.RetainedFromLSN {
		return false
	}
	return true
}

func haFormerPrimaryActionSucceededWithPromotionEvidence(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	if haActionKind(action.Kind) == haActionDemoteFormerPrimary {
		return haAdminActionSucceededWithEvidence(action)
	}
	return haAdminActionSucceededWithEvidence(action) &&
		haRejoinResultMatchesPromotion(status, action, action.AdminResult)
}

func haRejoinResultMatchesPromotion(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if !haRejoinResultMatchesRequiredAdminResult(action, result) {
		return false
	}
	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return false
	}
	if strings.TrimSpace(result.FormerNodeID) != strings.TrimSpace(promotion.OldPrimaryID) {
		return false
	}
	if result.TargetTimelineID != promotion.NewTimelineID || result.TargetEpoch != promotion.NewEpoch {
		return false
	}
	if forkLSN := haPromotionObservedLSN(promotion); forkLSN != 0 && result.ForkLSN != forkLSN {
		return false
	}
	if haActionKind(action.Kind) == haActionRewindFormerPrimary &&
		(promotion.Forced || promotion.DataLossPossible) {
		return false
	}
	if action.FenceAuthority != "" && promotion.FenceAuthority != action.FenceAuthority {
		return false
	}
	if action.FenceGeneration != 0 && promotion.FenceGeneration != action.FenceGeneration {
		return false
	}
	return true
}

func haResultSlotNameMatches(resultSlotName string, expectedSlotName string) bool {
	resultSlotName = strings.TrimSpace(resultSlotName)
	if resultSlotName == "" {
		return false
	}
	expectedSlotName = strings.TrimSpace(expectedSlotName)
	return expectedSlotName == "" || resultSlotName == expectedSlotName
}

func haResultManifestMatches(resultManifestID string, expectedManifestID string) bool {
	resultManifestID = strings.TrimSpace(resultManifestID)
	if resultManifestID == "" {
		return false
	}
	expectedManifestID = strings.TrimSpace(expectedManifestID)
	return expectedManifestID == "" || resultManifestID == expectedManifestID
}

func haResultBackupLSNMatches(resultBackupLSN uint64, expectedBackupLSN uint64) bool {
	if resultBackupLSN == 0 {
		return false
	}
	return expectedBackupLSN == 0 || resultBackupLSN == expectedBackupLSN
}

func haExpectedSeedBeginManifestID(action antflyv1.HAPlannedActionStatus, slotName string) string {
	for i := 0; i+1 < len(action.AdminCommand); i++ {
		if action.AdminCommand[i] == "--manifest-id" {
			return strings.TrimSpace(action.AdminCommand[i+1])
		}
	}
	if manifestID := haManifestIDFromPath(action.SeedManifestPath); manifestID != "" {
		return manifestID
	}
	if strings.TrimSpace(slotName) == "" || action.TargetLSN == 0 {
		return ""
	}
	return fmt.Sprintf("base-%s-%d", strings.TrimSpace(slotName), action.TargetLSN)
}

func haManifestIDFromPath(manifestPath string) string {
	manifestPath = strings.TrimSpace(manifestPath)
	if manifestPath == "" {
		return ""
	}
	name := path.Base(manifestPath)
	if name == "." || name == "/" {
		return ""
	}
	return strings.TrimSuffix(name, ".afha")
}

func (r *AntflyClusterReconciler) updateHAAdminJobExecutionCondition(cluster *antflyv1.AntflyCluster) {
	if cluster.Status.HAStatus == nil {
		return
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.AdminJobPhase != haAdminJobPhaseMissingAdminURL {
			continue
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			antflyv1.ReasonHAAdminURLMissing,
			fmt.Sprintf("HA admin action %s cannot execute because no target admin URL is configured", action.Kind),
		)
		return
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.AdminJobPhase != haAdminJobPhaseFailed {
			continue
		}
		jobName := action.AdminJobName
		if jobName == "" {
			jobName = "unknown"
		}
		message := fmt.Sprintf("HA admin action %s failed in executor %s after %d attempt(s)", action.Kind, jobName, action.AttemptCount)
		if strings.TrimSpace(action.ErrorClass) != "" {
			message = fmt.Sprintf("%s (class %s)", message, strings.TrimSpace(action.ErrorClass))
		}
		if strings.TrimSpace(action.AdminError) != "" {
			message = fmt.Sprintf("%s: %s", message, strings.TrimSpace(action.AdminError))
		}
		reason := antflyv1.ReasonHAAdminJobFailed
		if action.ErrorClass == "RetryBudgetExhausted" {
			reason = antflyv1.ReasonHAAdminRetryBudgetExhausted
		} else if action.AdminStatusCode == http.StatusUnauthorized {
			reason = antflyv1.ReasonHAAdminUnauthorized
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			reason,
			message,
		)
		return
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.AdminJobPhase != haAdminJobPhasePending || strings.TrimSpace(action.AdminError) == "" {
			continue
		}
		executor := action.AdminJobName
		if executor == "" {
			executor = "unknown"
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			antflyv1.ReasonHAAdminActionRetrying,
			fmt.Sprintf("HA admin action %s is retrying in %s after a transient error: %s", action.Kind, executor, strings.TrimSpace(action.AdminError)),
		)
		return
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			(!haActionRequiresAdminResult(haActionKind(action.Kind)) &&
				!haActionRequiresSeedArtifactReceipt(haActionKind(action.Kind))) ||
			haAdminActionSucceededWithStatusEvidence(cluster.Status.HAStatus, action) {
			continue
		}
		jobName := action.AdminJobName
		if jobName == "" {
			jobName = "unknown"
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			antflyv1.ReasonHAAdminResultMissing,
			fmt.Sprintf("HA admin action %s succeeded in Job %s, but the operator has not observed typed result evidence and a matching action receipt; dependent HA actions remain blocked", action.Kind, jobName),
		)
		return
	}

	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil {
		return
	}
	for i, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind != string(haActionPromoteStandby) ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			action.StandbyName == "" {
			continue
		}
		if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, i, cluster) {
			continue
		}
		if !haPromotionStatusMatches(cluster.Status.HAStatus.LastPromotion, identity, action) {
			continue
		}
		if haPromotionReceipt(cluster.Status.HAStatus) != nil {
			continue
		}
		jobName := action.AdminJobName
		if jobName == "" {
			jobName = "unknown"
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			antflyv1.ReasonHAPromotionReceiptMissing,
			fmt.Sprintf("HA promotion action %s succeeded in Job %s, but the operator has not observed a complete promotion receipt with a fence token; former-primary rejoin remains blocked", action.Kind, jobName),
		)
		return
	}
}

var haSeedIdentityAnnotationKeys = [...]string{
	haSeedRoleAnnotation,
	haTopologyIDAnnotation,
	haTopologyGenerationAnnotation,
	haNodeIDAnnotation,
	haSlotNameAnnotation,
	haSeedGenerationAnnotation,
	haSeedManifestIDAnnotation,
	haSeedManifestSHA256Annotation,
	haSeedSourcePVCNameAnnotation,
	haSeedSourcePVCUIDAnnotation,
	haSeedTargetPVCNameAnnotation,
	haSeedTargetPVCUIDAnnotation,
	haSeedCheckpointLSNAnnotation,
}

type haSeedIdentityEvidence struct {
	manifestID     string
	manifestSHA256 string
	checkpointLSN  uint64
}

// haPortableSeedIdentityAnnotations freezes only evidence that is authoritative
// before this Job is created. In particular, the Job's own eventual receipt is
// never folded back into its identity after the immutable Pod template exists.
func haPortableSeedIdentityAnnotations(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) map[string]string {
	kind := haActionKind(action.Kind)
	if !haPlannedActionKindIsPortableArtifact(kind) {
		return nil
	}
	annotations := map[string]string{}
	setHASeedIdentityAnnotation(annotations, haTopologyIDAnnotation, action.TopologyID)
	if action.TopologyGeneration > 0 {
		annotations[haTopologyGenerationAnnotation] = strconv.FormatInt(action.TopologyGeneration, 10)
	}
	setHASeedIdentityAnnotation(annotations, haNodeIDAnnotation, action.TopologyNodeID)
	setHASeedIdentityAnnotation(annotations, haSlotNameAnnotation, action.SlotName)
	setHASeedIdentityAnnotation(annotations, haSeedGenerationAnnotation, action.SeedArtifactGeneration)

	evidence := haPortableSeedPrerequisiteEvidence(cluster, action)
	setHASeedIdentityAnnotation(annotations, haSeedManifestIDAnnotation, evidence.manifestID)
	setHASeedIdentityAnnotation(annotations, haSeedManifestSHA256Annotation, evidence.manifestSHA256)
	if evidence.checkpointLSN > 0 {
		annotations[haSeedCheckpointLSNAnnotation] = strconv.FormatUint(evidence.checkpointLSN, 10)
	}

	switch kind {
	case haActionPublishSeedArtifact, haActionGCSourceSeedGenerations:
		setHASeedIdentityAnnotation(annotations, haSeedSourcePVCNameAnnotation, action.SourcePVCName)
		setHASeedIdentityAnnotation(annotations, haSeedSourcePVCUIDAnnotation, action.SourcePVCUID)
	default:
		setHASeedIdentityAnnotation(annotations, haSeedTargetPVCNameAnnotation, action.TargetPVCName)
		setHASeedIdentityAnnotation(annotations, haSeedTargetPVCUIDAnnotation, action.TargetPVCUID)
	}
	if kind == haActionRestoreSeedArtifact {
		annotations[haSeedRoleAnnotation] = "restore"
	}
	return annotations
}

func setHASeedIdentityAnnotation(annotations map[string]string, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		annotations[key] = value
	}
}

func haPortableSeedPrerequisiteEvidence(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) haSeedIdentityEvidence {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return haSeedIdentityEvidence{}
	}
	for _, prerequisiteKind := range haPortableSeedEvidencePrerequisiteKinds(haActionKind(action.Kind)) {
		for i := len(cluster.Status.HAStatus.PlannedActions) - 1; i >= 0; i-- {
			candidate := cluster.Status.HAStatus.PlannedActions[i]
			if haActionKind(candidate.Kind) != prerequisiteKind ||
				!haSeedChainBindingMatchesAction(candidate, action) ||
				!haAdminActionSucceededWithEvidence(candidate) {
				continue
			}
			if receipt := candidate.SeedArtifactReceipt; receipt != nil {
				return haSeedIdentityEvidence{
					manifestID: strings.TrimSpace(receipt.ManifestID), manifestSHA256: strings.TrimSpace(receipt.ManifestSHA256),
					checkpointLSN: receipt.CheckpointLSN,
				}
			}
			if result := candidate.AdminResult; result != nil {
				return haSeedIdentityEvidence{
					manifestID: strings.TrimSpace(result.ManifestID), manifestSHA256: strings.TrimSpace(result.ManifestSHA256),
					checkpointLSN: result.CheckpointLSN,
				}
			}
		}
	}
	return haSeedIdentityEvidence{}
}

func haPortableSeedEvidencePrerequisiteKinds(kind haActionKind) []haActionKind {
	switch kind {
	case haActionPublishSeedArtifact:
		return []haActionKind{haActionCaptureSeedArtifact, haActionFinishStandbySeed}
	case haActionGCSourceSeedGenerations, haActionRestoreSeedArtifact:
		return []haActionKind{haActionPublishSeedArtifact, haActionCaptureSeedArtifact, haActionFinishStandbySeed}
	case haActionActivateSeedArtifact:
		return []haActionKind{haActionRestoreSeedArtifact, haActionPublishSeedArtifact, haActionCaptureSeedArtifact, haActionFinishStandbySeed}
	case haActionGCTargetSeedGenerations, haActionPruneSeedArtifacts:
		return []haActionKind{
			haActionActivateSeededSlot, haActionActivateSeedArtifact, haActionRestoreSeedArtifact,
			haActionPublishSeedArtifact, haActionCaptureSeedArtifact, haActionFinishStandbySeed,
		}
	default:
		return nil
	}
}

func haSeedChainBindingMatchesAction(candidate, action antflyv1.HAPlannedActionStatus) bool {
	if strings.TrimSpace(candidate.StandbyName) != strings.TrimSpace(action.StandbyName) ||
		strings.TrimSpace(candidate.SlotName) != strings.TrimSpace(action.SlotName) ||
		strings.TrimSpace(candidate.TopologyID) != strings.TrimSpace(action.TopologyID) ||
		candidate.TopologyGeneration != action.TopologyGeneration ||
		strings.TrimSpace(candidate.TopologyNodeID) != strings.TrimSpace(action.TopologyNodeID) ||
		strings.TrimSpace(candidate.TargetPVCName) != strings.TrimSpace(action.TargetPVCName) ||
		strings.TrimSpace(candidate.TargetPVCUID) != strings.TrimSpace(action.TargetPVCUID) {
		return false
	}
	candidateGeneration := strings.TrimSpace(candidate.SeedArtifactGeneration)
	if candidateGeneration != "" {
		return candidateGeneration == strings.TrimSpace(action.SeedArtifactGeneration)
	}
	// Legacy caller-owned captures do not carry the portable generation on the
	// FinishStandbySeed action. In that case the frozen manifest path is the only
	// exact bridge into the portable publish chain; a slot name alone is not.
	candidateManifestPath := strings.TrimSpace(candidate.SeedManifestPath)
	return candidateManifestPath != "" && candidateManifestPath == strings.TrimSpace(action.SeedManifestPath)
}

func haSeedIdentityAnnotationsEqual(actual, desired map[string]string) bool {
	for _, key := range haSeedIdentityAnnotationKeys {
		actualValue, actualExists := actual[key]
		desiredValue, desiredExists := desired[key]
		if actualExists != desiredExists || actualValue != desiredValue {
			return false
		}
	}
	return true
}

func buildHAAdminJob(cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action antflyv1.HAPlannedActionStatus) *batchv1.Job {
	portableArtifactAction := haPlannedActionKindIsPortableArtifact(haActionKind(action.Kind))
	args := []string{"ha"}
	if !portableArtifactAction {
		args = append(args, "--ha-url", action.AdminURL)
		if tokenEnvVar := haAdminConfiguredTokenEnvVar(admin); tokenEnvVar != "" {
			args = append(args, "--ha-token-env", tokenEnvVar)
		}
		args = append(args, "--")
	}
	args = append(args, action.AdminCommand...)
	envFrom := append([]corev1.EnvFromSource{}, admin.EnvFrom...)
	if artifact := haSeedArtifactForAction(cluster, action); artifact != nil && artifact.CredentialsSecretRef != nil {
		envFrom = append(envFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: *artifact.CredentialsSecretRef.DeepCopy(),
		}})
	}
	volumeMounts, volumes := haAdminJobStorage(cluster, admin, action)
	labels := haAdminJobLabels(cluster, action)
	podTemplateLabels := maps.Clone(labels)
	if kind := haActionKind(action.Kind); kind == haActionPublishSeedArtifact || kind == haActionGCSourceSeedGenerations {
		// Source-PVC jobs must co-locate with the exact live RWO consumer.
		// Runtime identity labels make them match the AntflyCluster pods'
		// required self anti-affinity, producing an impossible placement:
		// podAffinity requires the source node while podAntiAffinity rejects it.
		// Keep those labels on the Job object for ownership/discovery, but do not
		// project runtime topology identity onto the Job's executable Pod.
		delete(podTemplateLabels, "cloud.antfly.io/instance-id")
		delete(podTemplateLabels, "app.kubernetes.io/instance")
	}
	annotations := map[string]string{
		"antfly.io/ha-action-kind":  action.Kind,
		"antfly.io/ha-admin-url":    action.AdminURL,
		"antfly.io/ha-command-hash": haAdminActionHash(action),
	}
	seedIdentityAnnotations := haPortableSeedIdentityAnnotations(cluster, action)
	maps.Copy(annotations, seedIdentityAnnotations)
	if action.DependsOn != "" {
		annotations["antfly.io/ha-action-depends-on"] = action.DependsOn
	}
	if action.AdminMethod != "" {
		annotations["antfly.io/ha-admin-method"] = action.AdminMethod
	}
	if action.AdminPath != "" {
		annotations["antfly.io/ha-admin-path"] = action.AdminPath
	}
	deadlineSeconds := haAdminJobTimeoutSeconds(admin)
	backoffLimit := haAdminJobBackoffLimit(admin)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        haAdminJobName(cluster, action),
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &deadlineSeconds,
			BackoffLimit:          &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podTemplateLabels, Annotations: maps.Clone(seedIdentityAnnotations)},
				Spec: corev1.PodSpec{
					ServiceAccountName: cluster.Spec.ServiceAccountName,
					RestartPolicy:      corev1.RestartPolicyNever,
					SecurityContext:    antflyPodSecurityContext(),
					Containers: []corev1.Container{{
						Name:            "ha-admin",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						Command:         []string{"/antfly"},
						Args:            args,
						Env:             append(haAdminJobTokenEnv(cluster, admin), haPodUIDEnv()...),
						EnvFrom:         envFrom,
						VolumeMounts:    volumeMounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

// ensureHAAdminJobTTLAfterCheckpoint arms TTL cleanup only after a subsequent
// reconcile loaded the terminal action state from the CR status. A fast TTL (or
// TTL=0) therefore cannot delete the only terminal Job evidence before it has
// been durably checkpointed in AntflyCluster status.
func (r *AntflyClusterReconciler) ensureHAAdminJobTTLAfterCheckpoint(ctx context.Context, cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action *antflyv1.HAPlannedActionStatus) error {
	if r == nil || cluster == nil || action == nil || action.AdminJobName == "" || action.AdminJobName == haAdminDirectAPIName {
		return nil
	}
	job := &batchv1.Job{}
	key := types.NamespacedName{Name: action.AdminJobName, Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, job); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get terminal HA admin Job %s before enabling TTL: %w", action.AdminJobName, err)
	}
	desired := haAdminJobTTLSecondsAfterFinished(admin)
	if job.Spec.TTLSecondsAfterFinished != nil && *job.Spec.TTLSecondsAfterFinished == desired {
		return nil
	}
	patch := client.MergeFrom(job.DeepCopy())
	job.Spec.TTLSecondsAfterFinished = &desired
	if err := r.Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("enable TTL cleanup for checkpointed HA admin Job %s: %w", action.AdminJobName, err)
	}
	return nil
}

func haAdminJobStorage(cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action antflyv1.HAPlannedActionStatus) ([]corev1.VolumeMount, []corev1.Volume) {
	artifact := haSeedArtifactForAction(cluster, action)
	if artifact != nil {
		switch haActionKind(action.Kind) {
		case haActionPublishSeedArtifact:
			if artifact.SourcePVC != nil {
				return haSeedArtifactPVCStorage("ha-seed-source", artifact.SourcePVC, true)
			}
		case haActionGCSourceSeedGenerations:
			if artifact.SourcePVC != nil {
				return haSeedArtifactPVCStorage("ha-seed-source", artifact.SourcePVC, false)
			}
		case haActionRestoreSeedArtifact, haActionActivateSeedArtifact:
			if artifact.TargetPVC != nil {
				return haSeedArtifactPVCStorage("ha-seed-target", artifact.TargetPVC, false)
			}
		case haActionGCTargetSeedGenerations:
			if artifact.TargetPVC != nil {
				mounts, volumes := haSeedArtifactPVCStorage("ha-seed-target", artifact.TargetPVC, false)
				mounts = append(mounts, corev1.VolumeMount{
					Name: "ha-seeded-slot-activation", MountPath: path.Dir(haSeededSlotActivationReceiptPath), ReadOnly: true,
				})
				volumes = append(volumes, corev1.Volume{
					Name: "ha-seeded-slot-activation",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: haSeededSlotActivationConfigMapName(cluster, action)},
					}},
				})
				return mounts, volumes
			}
		case haActionPruneSeedArtifacts:
			return nil, nil
		}
	}
	return append([]corev1.VolumeMount{}, admin.VolumeMounts...), append([]corev1.Volume{}, admin.Volumes...)
}

func haPodUIDEnv() []corev1.EnvVar {
	return []corev1.EnvVar{{
		Name: "ANTFLY_POD_UID",
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
			APIVersion: "v1", FieldPath: "metadata.uid",
		}},
	}}
}

// bindHAAdminJobToPVCConsumer prevents a portable artifact Job from being
// scheduled on a different node while its ReadWriteOnce source claim is mounted
// by a runtime pod. Target restores are allowed only with no live consumer, as
// enforced by the startup gate. RWX/ROX claims require no co-location, while
// ReadWriteOncePod can never be shared with a live runtime pod.
func (r *AntflyClusterReconciler) bindHAAdminJobToPVCConsumer(ctx context.Context, job *batchv1.Job) error {
	if r == nil || job == nil {
		return nil
	}
	claimName := ""
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil || strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName) == "" {
			continue
		}
		if claimName != "" && claimName != volume.PersistentVolumeClaim.ClaimName {
			return fmt.Errorf("HA admin Job %s mounts multiple PVCs; portable artifact actions must remain action-scoped", job.Name)
		}
		claimName = volume.PersistentVolumeClaim.ClaimName
	}
	if claimName == "" {
		return nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Name: claimName, Namespace: job.Namespace}, pvc); err != nil {
		return fmt.Errorf("get PVC %s for HA admin Job %s: %w", claimName, job.Name, err)
	}
	if r.Scheme != nil {
		if err := controllerutil.SetOwnerReference(pvc, job, r.Scheme); err != nil {
			return fmt.Errorf("bind HA admin Job %s to PVC incarnation %s/%s: %w", job.Name, claimName, pvc.UID, err)
		}
	}
	multiNode := slices.Contains(pvc.Spec.AccessModes, corev1.ReadWriteMany) ||
		slices.Contains(pvc.Spec.AccessModes, corev1.ReadOnlyMany)
	readWriteOncePod := slices.Contains(pvc.Spec.AccessModes, corev1.ReadWriteOncePod)
	readWriteOnce := slices.Contains(pvc.Spec.AccessModes, corev1.ReadWriteOnce)
	actionKind := haActionKind(strings.TrimSpace(job.Annotations["antfly.io/ha-action-kind"]))
	isPublishSource := actionKind == haActionPublishSeedArtifact || actionKind == haActionGCSourceSeedGenerations
	isGatedTarget := actionKind == haActionRestoreSeedArtifact || actionKind == haActionActivateSeedArtifact || actionKind == haActionGCTargetSeedGenerations
	if multiNode && !isGatedTarget {
		return nil
	}
	if !multiNode && !readWriteOnce && !readWriteOncePod {
		return fmt.Errorf("PVC %s for HA admin Job %s has no supported access mode", claimName, job.Name)
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace)); err != nil {
		return fmt.Errorf("list PVC consumers for HA admin Job %s: %w", job.Name, err)
	}
	consumerPodName := ""
	for i := range pods.Items {
		pod := &pods.Items[i]
		if haPodControlledByJob(pod, job) {
			continue
		}
		if pod.DeletionTimestamp == nil && (pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed) {
			continue
		}
		mountsClaim := false
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claimName {
				mountsClaim = true
				break
			}
		}
		if !mountsClaim {
			continue
		}
		if isGatedTarget {
			return fmt.Errorf("target PVC %s still has consumer pod %s; refusing HA restore/activation until the startup gate has stopped all consumers", claimName, pod.Name)
		}
		if readWriteOncePod {
			return fmt.Errorf("ReadWriteOncePod PVC %s is still mounted by pod %s; HA admin Job %s cannot share it", claimName, pod.Name, job.Name)
		}
		stableName := strings.TrimSpace(pod.Labels[appsv1.StatefulSetPodNameLabel])
		if stableName == "" || stableName != pod.Name {
			return fmt.Errorf("PVC %s consumer pod %s lacks its stable StatefulSet pod-name label", claimName, pod.Name)
		}
		if consumerPodName != "" && consumerPodName != stableName {
			return fmt.Errorf("PVC %s has multiple live consumer pods (%s, %s); refusing ambiguous HA Job placement", claimName, consumerPodName, stableName)
		}
		consumerPodName = stableName
	}
	if consumerPodName == "" {
		if isPublishSource && (pvc.Status.Phase != corev1.ClaimBound || strings.TrimSpace(pvc.Spec.VolumeName) == "") {
			return fmt.Errorf("publish-source PVC %s has no live stable runtime consumer and is not bound to a stable PV; refusing unpinned HA seed publication", claimName)
		}
		return nil
	}
	if job.Spec.Template.Spec.Affinity == nil {
		job.Spec.Template.Spec.Affinity = &corev1.Affinity{}
	}
	if job.Spec.Template.Spec.Affinity.PodAffinity == nil {
		job.Spec.Template.Spec.Affinity.PodAffinity = &corev1.PodAffinity{}
	}
	job.Spec.Template.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		job.Spec.Template.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
		corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				appsv1.StatefulSetPodNameLabel: consumerPodName,
			}},
			TopologyKey: corev1.LabelHostname,
		},
	)
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations["antfly.io/ha-pvc-consumer"] = consumerPodName
	job.Annotations["antfly.io/ha-pvc-claim"] = claimName
	return nil
}

func haPodControlledByJob(pod *corev1.Pod, job *batchv1.Job) bool {
	if pod == nil || job == nil || job.UID == "" {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller || owner.Kind != "Job" || owner.Name != job.Name {
			continue
		}
		if owner.UID != job.UID {
			continue
		}
		return true
	}
	return false
}

func haSeedArtifactPVCStorage(name string, pvc *antflyv1.HASeedArtifactPVCSpec, readOnly bool) ([]corev1.VolumeMount, []corev1.Volume) {
	if pvc == nil {
		return nil, nil
	}
	return []corev1.VolumeMount{{
			Name:      name,
			MountPath: pvc.MountPath,
			ReadOnly:  readOnly,
		}}, []corev1.Volume{{
			Name: name,
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvc.ClaimName,
				ReadOnly:  readOnly,
			}},
		}}
}

func haSeedArtifactForAction(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) *antflyv1.HASeedArtifactSpec {
	if cluster == nil || cluster.Spec.HighAvailability == nil {
		return nil
	}
	for i := range cluster.Spec.HighAvailability.Standbys {
		standby := &cluster.Spec.HighAvailability.Standbys[i]
		if strings.TrimSpace(standby.Name) == strings.TrimSpace(action.StandbyName) ||
			strings.TrimSpace(standbySlotName(*standby)) == strings.TrimSpace(action.SlotName) {
			return standby.SeedArtifact
		}
	}
	return nil
}

func haAdminJobTokenEnv(cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec) []corev1.EnvVar {
	envVar := haAdminConfiguredTokenEnvVar(admin)
	if envVar == "" || cluster == nil || cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil {
		return nil
	}
	secretRef := cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef
	if secretRef == nil {
		return nil
	}
	ref := secretRef.DeepCopy()
	optional := false
	ref.Optional = &optional
	return []corev1.EnvVar{{
		Name: envVar,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: ref,
		},
	}}
}

func haAdminTokenEnvVar(admin *antflyv1.HAAdminSpec) string {
	if configured := haAdminConfiguredTokenEnvVar(admin); configured != "" {
		return configured
	}
	return haAdminTokenDefaultEnvVar
}

func haAdminConfiguredTokenEnvVar(admin *antflyv1.HAAdminSpec) string {
	if admin == nil {
		return ""
	}
	return strings.TrimSpace(admin.TokenEnvVar)
}

func haAdminJobBackoffLimit(admin *antflyv1.HAAdminSpec) int32 {
	if admin != nil && admin.JobBackoffLimit != nil {
		return *admin.JobBackoffLimit
	}
	return 3
}

func haAdminJobTimeoutSeconds(admin *antflyv1.HAAdminSpec) int64 {
	if admin != nil && admin.JobTimeoutSeconds != nil {
		return *admin.JobTimeoutSeconds
	}
	return 600
}

func haAdminJobTTLSecondsAfterFinished(admin *antflyv1.HAAdminSpec) int32 {
	if admin != nil && admin.JobTTLSecondsAfterFinished != nil {
		return *admin.JobTTLSecondsAfterFinished
	}
	return 86400
}

func haAdminJobLabels(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) map[string]string {
	labels := podLabels(cluster, "ha-admin")
	labels["antfly.io/ha-action-kind"] = strings.ToLower(action.Kind)
	labels["antfly.io/ha-command-hash"] = haAdminActionHash(action)[:10]
	if action.StandbyName != "" {
		labels["antfly.io/ha-standby"] = action.StandbyName
	}
	return labels
}

func haAdminJobPhase(job *batchv1.Job) string {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return haAdminJobPhaseFailed
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return haAdminJobPhaseSucceeded
		}
	}
	if job.Status.Active > 0 {
		return haAdminJobPhaseRunning
	}
	return haAdminJobPhasePending
}

func haAdminJobFailureClass(job *batchv1.Job) string {
	if job == nil {
		return "JobFailed"
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type != batchv1.JobFailed || condition.Status != corev1.ConditionTrue {
			continue
		}
		if reason := strings.TrimSpace(condition.Reason); reason != "" {
			return reason
		}
	}
	return "JobFailed"
}

func haAdminJobName(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) string {
	hash := haAdminActionHash(action)[:10]
	kind := strings.ToLower(action.Kind)
	base := fmt.Sprintf("%s-ha-%s", cluster.Name, kind)
	maxBaseLen := 63 - len(hash) - 1
	if len(base) > maxBaseLen {
		base = strings.TrimRight(base[:maxBaseLen], "-")
	}
	if base == "" {
		base = "ha"
	}
	return fmt.Sprintf("%s-%s", base, hash)
}

func haAdminActionHash(action antflyv1.HAPlannedActionStatus) string {
	rendered, err := json.Marshal(struct {
		Kind               string   `json:"kind"`
		Phase              string   `json:"phase,omitempty"`
		Executor           string   `json:"executor,omitempty"`
		DependsOn          string   `json:"dependsOn,omitempty"`
		StandbyName        string   `json:"standbyName,omitempty"`
		SlotName           string   `json:"slotName,omitempty"`
		TargetLSN          uint64   `json:"targetLSN,omitempty"`
		ObservedLSN        uint64   `json:"observedLSN,omitempty"`
		RetainedLSN        uint64   `json:"retainedFromLSN,omitempty"`
		RouteFrom          string   `json:"routeFrom,omitempty"`
		RouteTo            string   `json:"routeTo,omitempty"`
		FenceAuth          string   `json:"fenceAuthority,omitempty"`
		FenceHolder        string   `json:"fenceHolder,omitempty"`
		FenceGen           uint64   `json:"fenceGeneration,omitempty"`
		FenceReason        string   `json:"fenceReason,omitempty"`
		SeedManifest       string   `json:"seedManifestPath,omitempty"`
		SeedRoot           string   `json:"seedContentRoot,omitempty"`
		SeedTargetRoot     string   `json:"seedArtifactTargetRoot,omitempty"`
		SeedLocation       string   `json:"seedArtifactLocation,omitempty"`
		SeedGeneration     string   `json:"seedArtifactGeneration,omitempty"`
		SeedRetention      int32    `json:"seedArtifactRetainGenerations,omitempty"`
		SeedCaptureRoot    string   `json:"seedArtifactCaptureRoot,omitempty"`
		SeedProtected      []string `json:"seedArtifactProtectedGenerations,omitempty"`
		TopologyID         string   `json:"topologyID,omitempty"`
		TopologyGeneration int64    `json:"topologyGeneration,omitempty"`
		TopologyNodeID     string   `json:"topologyNodeID,omitempty"`
		TargetPVCName      string   `json:"targetPVCName,omitempty"`
		TargetPVCUID       string   `json:"targetPVCUID,omitempty"`
		SourcePVCName      string   `json:"sourcePVCName,omitempty"`
		SourcePVCUID       string   `json:"sourcePVCUID,omitempty"`
		AdminURL           string   `json:"adminURL,omitempty"`
		AdminNodeID        string   `json:"adminNodeID,omitempty"`
		AdminMethod        string   `json:"adminMethod,omitempty"`
		AdminPath          string   `json:"adminPath,omitempty"`
		AdminCommand       []string `json:"adminCommand,omitempty"`
		OperationID        string   `json:"operationID,omitempty"`
		Reason             string   `json:"reason,omitempty"`
	}{
		Kind:               action.Kind,
		Phase:              action.Phase,
		Executor:           action.Executor,
		DependsOn:          action.DependsOn,
		StandbyName:        action.StandbyName,
		SlotName:           action.SlotName,
		TargetLSN:          action.TargetLSN,
		ObservedLSN:        action.ObservedLSN,
		RetainedLSN:        action.RetainedFromLSN,
		RouteFrom:          action.RouteFrom,
		RouteTo:            action.RouteTo,
		FenceAuth:          string(action.FenceAuthority),
		FenceHolder:        action.FenceHolder,
		FenceGen:           action.FenceGeneration,
		FenceReason:        action.FenceReason,
		SeedManifest:       action.SeedManifestPath,
		SeedRoot:           action.SeedContentRoot,
		SeedTargetRoot:     action.SeedArtifactTargetRoot,
		SeedLocation:       action.SeedArtifactLocation,
		SeedGeneration:     action.SeedArtifactGeneration,
		SeedRetention:      action.SeedArtifactRetainGenerations,
		SeedCaptureRoot:    action.SeedArtifactCaptureRoot,
		SeedProtected:      action.SeedArtifactProtectedGenerations,
		TopologyID:         action.TopologyID,
		TopologyGeneration: action.TopologyGeneration,
		TopologyNodeID:     action.TopologyNodeID,
		TargetPVCName:      action.TargetPVCName,
		TargetPVCUID:       action.TargetPVCUID,
		SourcePVCName:      action.SourcePVCName,
		SourcePVCUID:       action.SourcePVCUID,
		AdminURL:           action.AdminURL,
		AdminNodeID:        action.AdminNodeID,
		AdminMethod:        action.AdminMethod,
		AdminPath:          action.AdminPath,
		AdminCommand:       action.AdminCommand,
		OperationID:        action.OperationID,
		Reason:             action.Reason,
	})
	if err != nil {
		rendered = []byte(fmt.Sprintf("%s/%s/%s/%d/%s/%d/%s/%s/%v/%s", action.Kind, action.StandbyName, action.SlotName, action.TargetLSN, action.FenceAuthority, action.FenceGeneration, action.FenceHolder, action.AdminURL, action.AdminCommand, action.Reason))
	}
	sum := sha256.Sum256(rendered)
	return fmt.Sprintf("%x", sum)
}

func (r *AntflyClusterReconciler) updateRolloutCondition(cluster *antflyv1.AntflyCluster, statefulSets ...*appsv1.StatefulSet) {
	var waiting []string
	var blocked []string
	for _, sts := range statefulSets {
		if sts == nil || sts.Name == "" {
			waiting = append(waiting, "StatefulSet is not observed yet")
			continue
		}
		replicas := int32(1)
		if sts.Spec.Replicas != nil {
			replicas = *sts.Spec.Replicas
		}
		if sts.Generation > sts.Status.ObservedGeneration {
			waiting = append(waiting, fmt.Sprintf("%s observedGeneration %d is behind generation %d", sts.Name, sts.Status.ObservedGeneration, sts.Generation))
			continue
		}
		if sts.Status.UpdatedReplicas < replicas {
			message := fmt.Sprintf("%s has %d/%d updated replicas", sts.Name, sts.Status.UpdatedReplicas, replicas)
			if statefulSetRolloutAppearsBlocked(sts, replicas) {
				blocked = append(blocked, message)
			} else {
				waiting = append(waiting, message)
			}
			continue
		}
		if sts.Status.ReadyReplicas < replicas {
			waiting = append(waiting, fmt.Sprintf("%s has %d/%d ready replicas", sts.Name, sts.Status.ReadyReplicas, replicas))
		}
	}

	condition := metav1.Condition{
		Type:               antflyv1.TypeRollout,
		ObservedGeneration: cluster.Generation,
	}
	if len(blocked) > 0 {
		condition.Status = metav1.ConditionFalse
		condition.Reason = antflyv1.ReasonRolloutBlocked
		condition.Message = strings.Join(blocked, "; ")
	} else if len(waiting) > 0 {
		condition.Status = metav1.ConditionUnknown
		condition.Reason = antflyv1.ReasonRolloutInProgress
		condition.Message = strings.Join(waiting, "; ")
	} else {
		condition.Status = metav1.ConditionTrue
		condition.Reason = antflyv1.ReasonRolloutComplete
		condition.Message = "All StatefulSet replicas are updated and ready"
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, condition)
}

func statefulSetRolloutAppearsBlocked(sts *appsv1.StatefulSet, replicas int32) bool {
	if sts == nil || replicas <= 0 {
		return false
	}
	if sts.Status.UpdateRevision == "" || sts.Status.CurrentRevision == "" || sts.Status.UpdateRevision == sts.Status.CurrentRevision {
		return false
	}
	return sts.Status.UpdatedReplicas == 0 && sts.Status.ReadyReplicas == 0
}

func (r *AntflyClusterReconciler) repairBlockedStatefulSetRollouts(ctx context.Context, cluster *antflyv1.AntflyCluster) (bool, error) {
	if effectiveTopologyMode(cluster) == topologyModeStandalone {
		standaloneSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace}, standaloneSts); err != nil {
			if !errors.IsNotFound(err) {
				return false, err
			}
			return false, nil
		}
		return r.repairBlockedStatefulSetRollout(ctx, cluster, standaloneSts, standaloneComponent(cluster))
	}

	metadataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}, metadataSts); err != nil {
		if !errors.IsNotFound(err) {
			return false, err
		}
	} else if repaired, err := r.repairBlockedStatefulSetRollout(ctx, cluster, metadataSts, "metadata"); err != nil || repaired {
		return repaired, err
	}

	dataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-data", Namespace: cluster.Namespace}, dataSts); err != nil {
		if !errors.IsNotFound(err) {
			return false, err
		}
	} else if repaired, err := r.repairBlockedStatefulSetRollout(ctx, cluster, dataSts, "data"); err != nil || repaired {
		return repaired, err
	}

	return false, nil
}

func (r *AntflyClusterReconciler) repairBlockedStatefulSetRollout(ctx context.Context, cluster *antflyv1.AntflyCluster, sts *appsv1.StatefulSet, component string) (bool, error) {
	if sts == nil || sts.Name == "" || sts.Status.UpdateRevision == "" {
		return false, nil
	}
	if sts.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
		// OnDelete is an explicit instruction to preserve the running stateful
		// process while publishing a restart-safe template. It is never a blocked
		// rollout for this automatic repair path.
		return false, nil
	}

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas <= 0 || sts.Status.UpdatedReplicas >= replicas {
		return false, nil
	}

	pods, err := r.listComponentPods(ctx, cluster, component)
	if err != nil {
		return false, err
	}
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Name < pods[j].Name
	})

	desiredImage := ""
	if len(sts.Spec.Template.Spec.Containers) > 0 {
		desiredImage = sts.Spec.Template.Spec.Containers[0].Image
	}
	for i := range pods {
		pod := &pods[i]
		if !isPodControlledByStatefulSet(pod, sts.Name) ||
			!isStaleStatefulSetPod(pod, sts.Status.UpdateRevision, desiredImage) ||
			!isUnhealthyPod(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		if effectiveTopologyMode(cluster) == topologyModeStandalone &&
			promotedStandaloneProcessRolloutProtected(cluster, sts, pod) {
			log.FromContext(ctx).Info(
				"Preserving exact promoted process during deferred StatefulSet rollout",
				"statefulset", sts.Name,
				"pod", pod.Name,
				"podUID", pod.UID,
				"component", component,
			)
			continue
		}
		log.FromContext(ctx).Info(
			"Deleting stale unhealthy pod to unblock StatefulSet rollout",
			"statefulset", sts.Name,
			"pod", pod.Name,
			"component", component,
			"currentRevision", pod.Labels["controller-revision-hash"],
			"updateRevision", sts.Status.UpdateRevision,
			"desiredImage", desiredImage,
		)
		if r.Recorder != nil {
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "RepairingBlockedRollout", "DeleteStalePod", "Deleting stale unhealthy %s pod %s to unblock rollout to revision %s", component, pod.Name, sts.Status.UpdateRevision)
		}
		return true, r.Delete(ctx, pod)
	}

	return false, nil
}

func promotedStandaloneProcessRolloutProtected(cluster *antflyv1.AntflyCluster, sts *appsv1.StatefulSet, pod *corev1.Pod) bool {
	if hasExactPromotedProcessBinding(cluster, sts, pod) {
		return true
	}
	if cluster == nil || sts == nil || pod == nil {
		return false
	}
	ha := cluster.Spec.HighAvailability
	return ha != nil && ha.Runtime != nil &&
		ha.Runtime.Role == antflyv1.HARuntimeRolePrimary &&
		isPodControlledByExactStatefulSet(pod, sts) &&
		strings.TrimSpace(pod.Annotations[haNodeIDAnnotation]) == strings.TrimSpace(ha.Runtime.NodeID) &&
		podRunsHAStandbyCommand(pod)
}

func isPodControlledByStatefulSet(pod *corev1.Pod, statefulSetName string) bool {
	if pod == nil {
		return false
	}
	controller := metav1.GetControllerOf(pod)
	return controller != nil && controller.Kind == "StatefulSet" && controller.Name == statefulSetName
}

func isPodControlledByExactStatefulSet(pod *corev1.Pod, statefulSet *appsv1.StatefulSet) bool {
	if pod == nil || statefulSet == nil || statefulSet.UID == "" || pod.Namespace != statefulSet.Namespace {
		return false
	}
	controller := metav1.GetControllerOf(pod)
	return controller != nil && controller.Kind == "StatefulSet" && controller.Name == statefulSet.Name && controller.UID == statefulSet.UID
}

func isStaleStatefulSetPod(pod *corev1.Pod, updateRevision, desiredImage string) bool {
	if pod == nil {
		return false
	}
	if pod.Labels["controller-revision-hash"] != "" && pod.Labels["controller-revision-hash"] != updateRevision {
		return true
	}
	if desiredImage == "" || len(pod.Spec.Containers) == 0 {
		return false
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "antfly" {
			return container.Image != desiredImage
		}
	}
	return pod.Spec.Containers[0].Image != desiredImage
}

func isUnhealthyPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodUnknown {
		return true
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return false
		}
	}
	return true
}

func (r *AntflyClusterReconciler) listComponentPods(ctx context.Context, cluster *antflyv1.AntflyCluster, component string) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, component))); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (r *AntflyClusterReconciler) setComponentCondition(cluster *antflyv1.AntflyCluster, conditionType string, ready, desired int32, findings []poddiagnostics.Finding, component string) {
	status := metav1.ConditionFalse
	reason := antflyv1.ReasonWaitingForPods
	message := fmt.Sprintf("%s has %d/%d ready pods", component, ready, desired)
	if finding, ok := poddiagnostics.First(findings,
		poddiagnostics.FindingUnschedulable,
		poddiagnostics.FindingImagePullFailed,
		poddiagnostics.FindingCrashLooping,
		poddiagnostics.FindingProbeFailed,
		poddiagnostics.FindingInitFailed,
	); ok {
		reason = antflyReasonForFinding(finding)
		message = poddiagnostics.Message(finding)
	} else if ready >= desired && desired > 0 {
		status = metav1.ConditionTrue
		reason = antflyv1.ReasonComponentReady
		message = fmt.Sprintf("%s is ready", component)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func (r *AntflyClusterReconciler) setAvailableCondition(cluster *antflyv1.AntflyCluster, findings []poddiagnostics.Finding, ready bool) {
	status := metav1.ConditionTrue
	reason := antflyv1.ReasonAvailable
	message := "Cluster is available"
	if len(findings) > 0 {
		status = metav1.ConditionFalse
		reason = antflyv1.ReasonRuntimeDegraded
		message = poddiagnostics.Summary(findings)
	} else if !ready {
		status = metav1.ConditionFalse
		reason = antflyv1.ReasonWaitingForPods
		message = "Cluster is waiting for ready pods"
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeAvailable,
		Status:             status,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func antflyReasonForFinding(finding poddiagnostics.Finding) string {
	switch finding.Type {
	case poddiagnostics.FindingUnschedulable:
		return antflyv1.ReasonUnschedulable
	case poddiagnostics.FindingImagePullFailed:
		return antflyv1.ReasonImagePullFailed
	case poddiagnostics.FindingCrashLooping:
		return antflyv1.ReasonCrashLooping
	case poddiagnostics.FindingProbeFailed:
		return antflyv1.ReasonProbeFailed
	default:
		return antflyv1.ReasonRuntimeDegraded
	}
}

func (r *AntflyClusterReconciler) recordClusterRuntimeFailureEvents(cluster *antflyv1.AntflyCluster, originalConditions []metav1.Condition) {
	if r.Recorder == nil {
		return
	}
	for _, condition := range cluster.Status.Conditions {
		if condition.Status != metav1.ConditionFalse {
			continue
		}
		if condition.Type != antflyv1.TypeMetadataReady &&
			condition.Type != antflyv1.TypeDataReady &&
			condition.Type != antflyv1.TypeStandaloneReady &&
			condition.Type != antflyv1.TypeInferenceReady &&
			condition.Type != antflyv1.TypeAvailable {
			continue
		}
		previous := meta.FindStatusCondition(originalConditions, condition.Type)
		if previous != nil && previous.Status == condition.Status && previous.Reason == condition.Reason && previous.Message == condition.Message {
			continue
		}
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, condition.Reason, condition.Reason, "%s", condition.Message)
	}
}

func (r *AntflyClusterReconciler) updateProductTierStatus(cluster *antflyv1.AntflyCluster) {
	if cluster.Spec.ProductTier == nil {
		cluster.Status.ProductTierStatus = nil
		return
	}

	tier := cluster.Spec.ProductTier
	status := &antflyv1.ProductTierStatus{
		Name:               tier.Name,
		Revision:           tier.Revision,
		ManagedBy:          tier.ManagedBy,
		Mode:               effectiveTopologyModeForAPI(cluster),
		StandaloneTier:     tier.StandaloneTier,
		MetadataTier:       tier.MetadataTier,
		DataTier:           tier.DataTier,
		InferenceTier:      tier.InferenceTier,
		InferenceEnabled:   cluster.Spec.Inference != nil && antflyInferenceMode(cluster.Spec.Inference) != antflyv1.AntflyInferenceModeDisabled,
		ObservedGeneration: cluster.Generation,
	}

	if isStandaloneMode(cluster) {
		if cluster.Spec.Standalone != nil {
			status.StandaloneResources = resourceSpecSummary(cluster.Spec.Standalone.Resources)
		}
		status.StandaloneStorage = chooseStandaloneStorageSize(cluster)
	} else {
		status.MetadataReplicas = cluster.Spec.MetadataNodes.Replicas
		if status.MetadataReplicas == 0 {
			status.MetadataReplicas = 3
		}
		status.MetadataResources = resourceSpecSummary(cluster.Spec.MetadataNodes.Resources)
		status.MetadataStorage = cluster.Spec.Storage.MetadataStorage

		status.DataReplicas = effectiveDataNodeReplicas(cluster)
		status.DataResources = resourceSpecSummary(cluster.Spec.DataNodes.Resources)
		status.DataStorage = effectiveDataStorageSize(cluster)
		if cluster.Spec.DataNodes.AutoScaling != nil && cluster.Spec.DataNodes.AutoScaling.Enabled {
			status.DataAutoscaling = fmt.Sprintf("enabled min=%d max=%d", cluster.Spec.DataNodes.AutoScaling.MinReplicas, cluster.Spec.DataNodes.AutoScaling.MaxReplicas)
		} else {
			status.DataAutoscaling = "disabled"
		}
	}

	if cluster.Spec.Inference != nil {
		switch antflyInferenceMode(cluster.Spec.Inference) {
		case antflyv1.AntflyInferenceModeManaged:
			status.InferenceReplicas = fmt.Sprintf("managed=%d", len(cluster.Spec.Inference.ManagedPools))
		case antflyv1.AntflyInferenceModeSharedRef:
			status.InferenceReplicas = fmt.Sprintf("shared=%d", len(cluster.Spec.Inference.SharedPools))
		case antflyv1.AntflyInferenceModePlatformShared:
			status.InferenceReplicas = fmt.Sprintf("platform=%d", len(cluster.Spec.Inference.PlatformPools))
		default:
			status.InferenceReplicas = "disabled"
		}
	}

	cluster.Status.ProductTierStatus = status
}

func effectiveTopologyModeForAPI(cluster *antflyv1.AntflyCluster) antflyv1.ClusterMode {
	if isStandaloneMode(cluster) {
		return antflyv1.ClusterModeStandalone
	}
	return antflyv1.ClusterModeDistributed
}

func resourceSpecSummary(resources antflyv1.ResourceSpec) string {
	parts := []string{}
	if resources.CPU != "" {
		parts = append(parts, "cpu="+resources.CPU)
	}
	if resources.Memory != "" {
		parts = append(parts, "memory="+resources.Memory)
	}
	if resources.Limits.CPU != "" {
		parts = append(parts, "limitCPU="+resources.Limits.CPU)
	}
	if resources.Limits.Memory != "" {
		parts = append(parts, "limitMemory="+resources.Limits.Memory)
	}
	if resources.Limits.GPU != "" {
		parts = append(parts, "limitGPU="+resources.Limits.GPU)
	}
	return strings.Join(parts, " ")
}

// updateServiceMeshReadyCondition updates the ServiceMeshReady condition based on current status
func (r *AntflyClusterReconciler) updateServiceMeshReadyCondition(cluster *antflyv1.AntflyCluster) {
	if cluster.Status.ServiceMeshStatus == nil {
		return
	}

	var condition metav1.Condition
	condition.Type = "ServiceMeshReady"
	condition.ObservedGeneration = cluster.Generation
	condition.LastTransitionTime = metav1.Now()

	if !cluster.Status.ServiceMeshStatus.Enabled {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "Disabled"
		condition.Message = "Service mesh disabled"
	} else {
		switch cluster.Status.ServiceMeshStatus.SidecarInjectionStatus {
		case "Complete":
			condition.Status = metav1.ConditionTrue
			condition.Reason = "SidecarInjectionComplete"
			condition.Message = fmt.Sprintf("All %d pods have sidecars injected", cluster.Status.ServiceMeshStatus.TotalPods)
		case "Partial":
			condition.Status = metav1.ConditionFalse
			condition.Reason = "PartialInjection"
			condition.Message = fmt.Sprintf("%d/%d pods have sidecars injected", cluster.Status.ServiceMeshStatus.PodsWithSidecars, cluster.Status.ServiceMeshStatus.TotalPods)
		case "None":
			condition.Status = metav1.ConditionFalse
			condition.Reason = "NoSidecarInjection"
			condition.Message = "No sidecars injected"
		default:
			condition.Status = metav1.ConditionUnknown
			condition.Reason = "Unknown"
			condition.Message = "Sidecar injection status unknown"
		}
	}

	// Update or append condition
	found := false
	for i, existingCondition := range cluster.Status.Conditions {
		if existingCondition.Type == "ServiceMeshReady" {
			// Only update if status or reason changed
			if existingCondition.Status != condition.Status || existingCondition.Reason != condition.Reason {
				cluster.Status.Conditions[i] = condition
			}
			found = true
			break
		}
	}
	if !found {
		cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
	}
}

// applySchedulingConstraints applies user-specified scheduling constraints to the pod template.
// This is called before cloud-provider-specific functions so that their entries merge on top.
func applySchedulingConstraints(podTemplate *corev1.PodTemplateSpec, tolerations []corev1.Toleration, nodeSelector map[string]string, affinity *corev1.Affinity, topologySpreadConstraints []corev1.TopologySpreadConstraint) {
	// Apply tolerations
	podTemplate.Spec.Tolerations = append(podTemplate.Spec.Tolerations, tolerations...)

	// Apply node selector (merge into existing map)
	if len(nodeSelector) > 0 {
		if podTemplate.Spec.NodeSelector == nil {
			podTemplate.Spec.NodeSelector = make(map[string]string)
		}
		maps.Copy(podTemplate.Spec.NodeSelector, nodeSelector)
	}

	// Apply affinity (deep merge to coexist with cloud-provider entries)
	if affinity != nil {
		if podTemplate.Spec.Affinity == nil {
			podTemplate.Spec.Affinity = affinity.DeepCopy()
		} else {
			if affinity.NodeAffinity != nil {
				if podTemplate.Spec.Affinity.NodeAffinity == nil {
					podTemplate.Spec.Affinity.NodeAffinity = affinity.NodeAffinity.DeepCopy()
				} else {
					podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
						podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
						affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
					)
					if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
						podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.DeepCopy()
					}
				}
			}
			if affinity.PodAffinity != nil {
				podTemplate.Spec.Affinity.PodAffinity = affinity.PodAffinity.DeepCopy()
			}
			if affinity.PodAntiAffinity != nil {
				podTemplate.Spec.Affinity.PodAntiAffinity = affinity.PodAntiAffinity.DeepCopy()
			}
		}
	}

	// Apply topology spread constraints
	if len(topologySpreadConstraints) > 0 {
		podTemplate.Spec.TopologySpreadConstraints = append(podTemplate.Spec.TopologySpreadConstraints, topologySpreadConstraints...)
	}
}

// applyGKEPodSpec applies GKE-specific configuration to pod template spec
func (r *AntflyClusterReconciler) applyGKEPodSpec(podTemplate *corev1.PodTemplateSpec, cluster *antflyv1.AntflyCluster, useSpotPods bool) {
	// GKE Autopilot mode: use compute class annotations
	if cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot {
		// Initialize annotations if nil
		if podTemplate.Annotations == nil {
			podTemplate.Annotations = make(map[string]string)
		}

		// Apply compute class annotation (required for GKE Autopilot)
		// This controls pod scheduling on GKE Autopilot
		if cluster.Spec.GKE.AutopilotComputeClass != "" {
			podTemplate.Annotations["cloud.google.com/compute-class"] = cluster.Spec.GKE.AutopilotComputeClass
		}

		// Set termination grace period for graceful shutdown
		gracePeriod := int64(15)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod

		// Ensure NO node selectors for GKE Autopilot (conflicts with compute class)
		podTemplate.Spec.NodeSelector = nil

		return
	}

	// Standard GKE mode (non-Autopilot): use node selectors
	if useSpotPods {
		// Initialize nodeSelector if nil
		if podTemplate.Spec.NodeSelector == nil {
			podTemplate.Spec.NodeSelector = make(map[string]string)
		}

		// Apply Spot Nodes configuration using node selector
		podTemplate.Spec.NodeSelector["cloud.google.com/gke-spot"] = "true"

		// Set termination grace period for graceful shutdown on eviction
		gracePeriod := int64(15)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod
	}
}

// applyEKSPodSpec applies AWS EKS-specific configuration to pod template spec
func (r *AntflyClusterReconciler) applyEKSPodSpec(podTemplate *corev1.PodTemplateSpec, cluster *antflyv1.AntflyCluster, useSpotInstances bool) {
	// Only apply EKS configuration if EKS is enabled
	if cluster.Spec.EKS == nil || !cluster.Spec.EKS.Enabled {
		return
	}

	eks := cluster.Spec.EKS

	// Initialize annotations if nil
	if podTemplate.Annotations == nil {
		podTemplate.Annotations = make(map[string]string)
	}

	// Initialize nodeSelector if nil
	if podTemplate.Spec.NodeSelector == nil {
		podTemplate.Spec.NodeSelector = make(map[string]string)
	}

	// Apply Spot Instance configuration
	if useSpotInstances || eks.UseSpotInstances {
		// EKS Spot Instances use the capacity type label
		// This works with both managed node groups and Karpenter
		podTemplate.Spec.NodeSelector["eks.amazonaws.com/capacityType"] = "SPOT"

		// Alternative label for self-managed node groups or older EKS versions
		// podTemplate.Spec.NodeSelector["node.kubernetes.io/lifecycle"] = "spot"

		// Set termination grace period for graceful shutdown on Spot interruption
		// AWS gives 2-minute warning before Spot termination
		gracePeriod := int64(25)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod

		// Add toleration for Spot Instance taint (common pattern)
		spotToleration := corev1.Toleration{
			Key:      "eks.amazonaws.com/capacityType",
			Operator: corev1.TolerationOpEqual,
			Value:    "SPOT",
			Effect:   corev1.TaintEffectNoSchedule,
		}
		podTemplate.Spec.Tolerations = appendTolerationIfNotExists(podTemplate.Spec.Tolerations, spotToleration)
	}

	// Apply instance type node affinity if specified
	if len(eks.InstanceTypes) > 0 {
		r.applyEKSInstanceTypeAffinity(podTemplate, eks.InstanceTypes)
	}
}

// applyEKSInstanceTypeAffinity adds node affinity to prefer specific EC2 instance types
func (r *AntflyClusterReconciler) applyEKSInstanceTypeAffinity(podTemplate *corev1.PodTemplateSpec, instanceTypes []string) {
	if len(instanceTypes) == 0 {
		return
	}

	// Create node affinity for instance types
	instanceTypeRequirement := corev1.NodeSelectorRequirement{
		Key:      "node.kubernetes.io/instance-type",
		Operator: corev1.NodeSelectorOpIn,
		Values:   instanceTypes,
	}

	// Initialize affinity if nil
	if podTemplate.Spec.Affinity == nil {
		podTemplate.Spec.Affinity = &corev1.Affinity{}
	}
	if podTemplate.Spec.Affinity.NodeAffinity == nil {
		podTemplate.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	// Use preferred scheduling (soft affinity) to allow fallback to other instance types
	// This prevents pods from being unschedulable if preferred types aren't available
	weight := int32(100)
	preferredTerm := corev1.PreferredSchedulingTerm{
		Weight: weight,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{instanceTypeRequirement},
		},
	}

	podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		preferredTerm,
	)
}

// appendTolerationIfNotExists adds a toleration if it doesn't already exist
func appendTolerationIfNotExists(tolerations []corev1.Toleration, newToleration corev1.Toleration) []corev1.Toleration {
	for _, t := range tolerations {
		if t.Key == newToleration.Key && t.Operator == newToleration.Operator && t.Value == newToleration.Value {
			return tolerations
		}
	}
	return append(tolerations, newToleration)
}

// reconcilePodDisruptionBudget creates or updates PodDisruptionBudgets for StatefulSets
func (r *AntflyClusterReconciler) reconcilePodDisruptionBudget(ctx context.Context, cluster *antflyv1.AntflyCluster, name string, role string) error {
	// Check if PDB is enabled via GKE or EKS configuration
	var pdbSpec *antflyv1.PodDisruptionBudgetSpec

	// Check GKE PDB configuration
	if cluster.Spec.GKE != nil && cluster.Spec.GKE.PodDisruptionBudget != nil && cluster.Spec.GKE.PodDisruptionBudget.Enabled {
		pdbSpec = cluster.Spec.GKE.PodDisruptionBudget
	}

	// Check EKS PDB configuration (EKS takes precedence if both are somehow set)
	if cluster.Spec.EKS != nil && cluster.Spec.EKS.Enabled && cluster.Spec.EKS.PodDisruptionBudget != nil && cluster.Spec.EKS.PodDisruptionBudget.Enabled {
		pdbSpec = cluster.Spec.EKS.PodDisruptionBudget
	}

	// Return if no PDB configuration is enabled
	if pdbSpec == nil {
		return nil
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}

	// Use CreateOrUpdate to ensure PDB is updated with latest configuration
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, pdb, r.Scheme); err != nil {
			return err
		}

		// Update PDB spec — include instance label to scope to this cluster
		pdb.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: serviceSelectorLabels(cluster.Name, role),
		}

		// Set MaxUnavailable or MinAvailable (prefer MaxUnavailable as recommended)
		if pdbSpec.MaxUnavailable != nil {
			maxUnavailable := intstr.FromInt(int(*pdbSpec.MaxUnavailable))
			pdb.Spec.MaxUnavailable = &maxUnavailable
			pdb.Spec.MinAvailable = nil // Clear MinAvailable when MaxUnavailable is set
		} else if pdbSpec.MinAvailable != nil {
			minAvailable := intstr.FromInt(int(*pdbSpec.MinAvailable))
			pdb.Spec.MinAvailable = &minAvailable
			pdb.Spec.MaxUnavailable = nil // Clear MaxUnavailable when MinAvailable is set
		} else {
			// Default to MaxUnavailable=1
			maxUnavailable := intstr.FromInt(1)
			pdb.Spec.MaxUnavailable = &maxUnavailable
			pdb.Spec.MinAvailable = nil
		}

		return nil
	})

	return err
}

type pvcUsageObservation struct {
	matched       int
	usedBytes     int64
	capacityBytes int64
	maxRequest    resource.Quantity
}

type kubeletStatsSummary struct {
	Pods []kubeletPodStats `json:"pods"`
}

type kubeletPodStats struct {
	PodRef kubeletPodReference  `json:"podRef"`
	Volume []kubeletVolumeStats `json:"volume"`
}

type kubeletPodReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type kubeletVolumeStats struct {
	PVCRef        *kubeletPVCReference `json:"pvcRef,omitempty"`
	UsedBytes     *uint64              `json:"usedBytes,omitempty"`
	CapacityBytes *uint64              `json:"capacityBytes,omitempty"`
}

type kubeletPVCReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func (r *AntflyClusterReconciler) reconcileStorageAutoGrow(ctx context.Context, cluster *antflyv1.AntflyCluster, component, vctName, stsName, currentSizeStr, maxSizeStr string) string {
	policy := cluster.Spec.Storage.StorageAutoGrow
	if policy == nil || !policy.Enabled {
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonStorageAutoGrowDisabled, "Storage auto-grow is disabled")
		return currentSizeStr
	}

	if maxSizeStr == "" {
		message := fmt.Sprintf("%s storage auto-grow is enabled but no max size is configured", component)
		r.setStorageAutoGrowStatus(cluster, component, currentSizeStr, "", "", 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}

	currentSize, err := resource.ParseQuantity(currentSizeStr)
	if err != nil {
		message := fmt.Sprintf("%s current storage size %q is invalid: %v", component, currentSizeStr, err)
		r.setStorageAutoGrowStatus(cluster, component, currentSizeStr, "", maxSizeStr, 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}
	maxSize, err := resource.ParseQuantity(maxSizeStr)
	if err != nil {
		message := fmt.Sprintf("%s max storage size %q is invalid: %v", component, maxSizeStr, err)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSizeStr, 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}
	growIncrement, err := resource.ParseQuantity(policy.GrowIncrement)
	if err != nil || growIncrement.Sign() <= 0 {
		message := fmt.Sprintf("%s grow increment %q is invalid", component, policy.GrowIncrement)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}

	observation, err := r.observePVCUsage(ctx, cluster, component, vctName, stsName)
	if observation.maxRequest.Cmp(currentSize) > 0 {
		currentSize = observation.maxRequest
		currentSizeStr = currentSize.String()
	}
	if err != nil {
		message := fmt.Sprintf("%s storage usage is unavailable: %v", component, err)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), 0, 0, 0, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		return currentSizeStr
	}
	if observation.matched == 0 || observation.capacityBytes <= 0 {
		message := fmt.Sprintf("Waiting for %s PVC usage metrics before evaluating auto-grow", component)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), observation.usedBytes, observation.capacityBytes, 0, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		return currentSizeStr
	}

	usagePercentValue := (observation.usedBytes/observation.capacityBytes)*100 + ((observation.usedBytes%observation.capacityBytes)*100)/observation.capacityBytes
	const maxInt32 = int64(1<<31 - 1)
	if usagePercentValue > maxInt32 {
		usagePercentValue = maxInt32
	}
	usagePercent := int32(usagePercentValue)
	threshold := policy.GrowThresholdPercent
	if threshold == 0 {
		threshold = 85
	}
	if usagePercent < threshold {
		message := fmt.Sprintf("%s storage usage is %d%%, below auto-grow threshold %d%%", component, usagePercent, threshold)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), observation.usedBytes, observation.capacityBytes, usagePercent, antflyv1.ReasonStorageAutoGrowReady, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonStorageAutoGrowReady, message)
		return currentSizeStr
	}

	if currentSize.Cmp(maxSize) >= 0 {
		message := fmt.Sprintf("%s storage usage is %d%% but current size %s has reached max %s", component, usagePercent, currentSize.String(), maxSize.String())
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), currentSize.String(), maxSize.String(), observation.usedBytes, observation.capacityBytes, usagePercent, antflyv1.ReasonStorageAutoGrowMaxReached, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowMaxReached, message)
		return currentSizeStr
	}

	recommended := resource.NewQuantity(currentSize.Value()+growIncrement.Value(), resource.BinarySI)
	if recommended.Cmp(maxSize) > 0 {
		maxCopy := maxSize.DeepCopy()
		recommended = &maxCopy
	}
	message := fmt.Sprintf("%s storage usage is %d%%, growing from %s to %s", component, usagePercent, currentSize.String(), recommended.String())
	r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), recommended.String(), maxSize.String(), observation.usedBytes, observation.capacityBytes, usagePercent, antflyv1.ReasonStorageAutoGrowInProgress, message)
	r.setStorageAutoGrowCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonStorageAutoGrowInProgress, message)
	return recommended.String()
}

func (r *AntflyClusterReconciler) observePVCUsage(ctx context.Context, cluster *antflyv1.AntflyCluster, component, vctName, stsName string) (pvcUsageObservation, error) {
	prefix := vctName + "-" + stsName + "-"
	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": cluster.Name}); err != nil {
		return pvcUsageObservation{}, fmt.Errorf("list PVCs: %w", err)
	}

	observation := pvcUsageObservation{}
	pvcNames := map[string]struct{}{}
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !strings.HasPrefix(pvc.Name, prefix) {
			continue
		}
		observation.matched++
		pvcNames[pvc.Name] = struct{}{}
		if request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; request.Cmp(observation.maxRequest) > 0 {
			observation.maxRequest = request
		}
	}
	if len(pvcNames) == 0 {
		return observation, nil
	}
	if r.KubeClient == nil && r.NodeStatsFetcher == nil {
		return observation, fmt.Errorf("kubernetes client is not configured")
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, component))); err != nil {
		return observation, fmt.Errorf("list %s pods: %w", component, err)
	}

	summaries := map[string]*kubeletStatsSummary{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		summary, ok := summaries[pod.Spec.NodeName]
		if !ok {
			fetched, err := r.fetchNodeStatsSummary(ctx, pod.Spec.NodeName)
			if err != nil {
				return observation, err
			}
			summary = fetched
			summaries[pod.Spec.NodeName] = summary
		}
		for _, podStats := range summary.Pods {
			if podStats.PodRef.Namespace != pod.Namespace || podStats.PodRef.Name != pod.Name {
				continue
			}
			for _, volume := range podStats.Volume {
				if volume.PVCRef == nil || volume.PVCRef.Namespace != cluster.Namespace {
					continue
				}
				if _, matched := pvcNames[volume.PVCRef.Name]; !matched {
					continue
				}
				if volume.UsedBytes != nil {
					observation.usedBytes += int64(*volume.UsedBytes) //nolint:gosec // kubelet volume stats fit int64 byte counters in practice
				}
				if volume.CapacityBytes != nil {
					observation.capacityBytes += int64(*volume.CapacityBytes) //nolint:gosec // kubelet volume stats fit int64 byte counters in practice
				}
			}
		}
	}

	return observation, nil
}

func (r *AntflyClusterReconciler) fetchNodeStatsSummary(ctx context.Context, nodeName string) (*kubeletStatsSummary, error) {
	if r.NodeStatsFetcher != nil {
		return r.NodeStatsFetcher(ctx, nodeName)
	}
	raw, err := r.KubeClient.CoreV1().RESTClient().Get().
		Resource("nodes").
		Name(nodeName).
		SubResource("proxy").
		Suffix("stats/summary").
		DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch node %s stats summary: %w", nodeName, err)
	}
	var summary kubeletStatsSummary
	if err := json.Unmarshal(raw, &summary); err != nil {
		return nil, fmt.Errorf("decode node %s stats summary: %w", nodeName, err)
	}
	return &summary, nil
}

func (r *AntflyClusterReconciler) setStorageAutoGrowStatus(cluster *antflyv1.AntflyCluster, component, currentSize, recommendedSize, maxSize string, usedBytes, capacityBytes int64, usagePercent int32, reason, message string) {
	now := metav1.Now()
	cluster.Status.StorageAutoGrowStatus = &antflyv1.StorageAutoGrowStatus{
		Component:          component,
		CurrentSize:        currentSize,
		RecommendedSize:    recommendedSize,
		MaxSize:            maxSize,
		UsedBytes:          usedBytes,
		CapacityBytes:      capacityBytes,
		UsagePercent:       usagePercent,
		Reason:             reason,
		Message:            message,
		LastEvaluationTime: &now,
	}
}

func (r *AntflyClusterReconciler) setStorageAutoGrowCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeStorageAutoGrow,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
}

type pvcExpansionState string

const (
	pvcExpansionSkipped    pvcExpansionState = "skipped"
	pvcExpansionPending    pvcExpansionState = "pending"
	pvcExpansionInProgress pvcExpansionState = "inProgress"
	pvcExpansionComplete   pvcExpansionState = "complete"
	pvcExpansionFailed     pvcExpansionState = "failed"
)

type pvcExpansionResult struct {
	component string
	state     pvcExpansionState
	message   string
}

// reconcilePVCExpansion patches existing PVCs when the CRD specifies a larger storage size.
// This is a best-effort operation — failures are reported as status conditions but don't block reconciliation.
func (r *AntflyClusterReconciler) reconcilePVCExpansion(ctx context.Context, cluster *antflyv1.AntflyCluster, component, vctName, stsName, desiredSizeStr string) pvcExpansionResult {
	if desiredSizeStr == "" {
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionSkipped,
			message:   fmt.Sprintf("%s storage uses the operator default size", component),
		}
	}

	log := log.FromContext(ctx)
	desiredSize := resource.MustParse(desiredSizeStr)

	// List PVCs matching this StatefulSet's VolumeClaimTemplate naming: {vctName}-{stsName}-{ordinal}
	prefix := vctName + "-" + stsName + "-"
	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": cluster.Name}); err != nil {
		log.Error(err, "Failed to list PVCs for expansion check")
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionFailed,
			message:   fmt.Sprintf("Failed to list %s PVCs for expansion: %v", component, err),
		}
	}

	matched := 0
	updated := 0
	waiting := 0
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !strings.HasPrefix(pvc.Name, prefix) {
			continue // secondary guard: multiple StatefulSets share the instance label
		}
		matched++

		currentSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if desiredSize.Cmp(currentSize) > 0 {
			log.Info("Expanding PVC", "pvc", pvc.Name, "from", currentSize.String(), "to", desiredSize.String())
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desiredSize
			if err := r.Update(ctx, pvc); err != nil {
				log.Error(err, "Failed to expand PVC", "pvc", pvc.Name)
				return pvcExpansionResult{
					component: component,
					state:     pvcExpansionFailed,
					message:   fmt.Sprintf("Failed to expand PVC %s: %v", pvc.Name, err),
				}
			}
			updated++
			waiting++
			continue
		}

		capacity := pvc.Status.Capacity[corev1.ResourceStorage]
		if capacity.IsZero() || desiredSize.Cmp(capacity) > 0 || pvcHasFileSystemResizePending(pvc) {
			waiting++
		}
	}

	switch {
	case matched == 0:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionPending,
			message:   fmt.Sprintf("Waiting for %s PVCs with prefix %q to be created", component, prefix),
		}
	case updated > 0:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionInProgress,
			message:   fmt.Sprintf("Expansion requested for %d %s PVC(s) to %s", updated, component, desiredSize.String()),
		}
	case waiting > 0:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionInProgress,
			message:   fmt.Sprintf("Waiting for %d %s PVC(s) to report capacity %s", waiting, component, desiredSize.String()),
		}
	default:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionComplete,
			message:   fmt.Sprintf("All %d %s PVC(s) report requested capacity %s", matched, component, desiredSize.String()),
		}
	}
}

func pvcHasFileSystemResizePending(pvc *corev1.PersistentVolumeClaim) bool {
	for _, condition := range pvc.Status.Conditions {
		if condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *AntflyClusterReconciler) setPVCExpansionCondition(cluster *antflyv1.AntflyCluster, results []pvcExpansionResult) {
	var relevant []pvcExpansionResult
	for _, result := range results {
		if result.state != pvcExpansionSkipped {
			relevant = append(relevant, result)
		}
	}
	if len(relevant) == 0 {
		return
	}

	status := metav1.ConditionTrue
	reason := antflyv1.ReasonPVCExpansionComplete
	for _, result := range relevant {
		switch result.state {
		case pvcExpansionFailed:
			status = metav1.ConditionFalse
			reason = antflyv1.ReasonPVCExpansionFailed
		case pvcExpansionPending:
			if reason != antflyv1.ReasonPVCExpansionFailed {
				status = metav1.ConditionUnknown
				reason = antflyv1.ReasonPVCExpansionPending
			}
		case pvcExpansionInProgress:
			if reason != antflyv1.ReasonPVCExpansionFailed && reason != antflyv1.ReasonPVCExpansionPending {
				status = metav1.ConditionUnknown
				reason = antflyv1.ReasonPVCExpansionInProgress
			}
		}
	}

	messages := make([]string, 0, len(relevant))
	for _, result := range relevant {
		messages = append(messages, result.message)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypePVCExpansion,
		Status:             status,
		Reason:             reason,
		Message:            strings.Join(messages, "; "),
		ObservedGeneration: cluster.Generation,
	})
}

// checkPVCTopologyHealth detects PVC/AZ topology issues by checking for Pending pods
// with "volume node affinity conflict" messages. Sets the StorageHealthy condition.
func (r *AntflyClusterReconciler) checkPVCTopologyHealth(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	log := log.FromContext(ctx)

	mode := effectiveTopologyMode(cluster)
	components := []string{"metadata", "data"}
	if mode == topologyModeStandalone {
		components = []string{standaloneComponent(cluster)}
	}

	// Check pods for topology issues
	for _, component := range components {
		var podList corev1.PodList
		if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace),
			client.MatchingLabels(serviceSelectorLabels(cluster.Name, component))); err != nil {
			log.Error(err, "Failed to list pods for topology health check", "component", component)
			continue
		}

		for _, pod := range podList.Items {
			if pod.Status.Phase != corev1.PodPending {
				continue
			}

			// Check PodScheduled condition for volume affinity issues
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
					if strings.HasPrefix(cond.Message, "0/") && containsVolumeAffinityMessage(cond.Message) {
						msg := fmt.Sprintf("Pod %s is Pending due to PVC/AZ topology mismatch: %s. "+
							"Verify your StorageClass uses volumeBindingMode: WaitForFirstConsumer and "+
							"nodes are available in the AZ where PVCs are bound.", pod.Name, cond.Message)
						r.setStorageCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonPVCAZMismatch, msg)
						r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, antflyv1.ReasonPVCAZMismatch,
							"StorageTopologyMismatch", msg)
						return
					}
				}
			}
		}
	}

	// All good — set healthy
	r.setStorageCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonStorageHealthy, "Storage topology is healthy")
}

// setStorageCondition updates the StorageHealthy condition on the cluster status.
func (r *AntflyClusterReconciler) setStorageCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               antflyv1.TypeStorageHealthy,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	for i, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeStorageHealthy {
			if existing.Status != status || existing.Reason != reason {
				cluster.Status.Conditions[i] = condition
			}
			return
		}
	}
	cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
}

// containsVolumeAffinityMessage checks if a scheduler message indicates a volume node affinity conflict.
func containsVolumeAffinityMessage(msg string) bool {
	return strings.Contains(msg, "volume node affinity")
}

func hasAnyPrefix(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *AntflyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := normalizeClusterDomainField(&r.ClusterDomain); err != nil {
		return fmt.Errorf("configure AntflyCluster controller: %w", err)
	}
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("antflycluster-ha-lease-renewal").
		WithOptions(controller.Options{MaxConcurrentReconciles: 16}).
		For(&antflyv1.AntflyCluster{}, ctrlbuilder.WithPredicates(haLeaseRenewalEventPredicate())).
		Complete(&haLeaseRenewalReconciler{parent: r}); err != nil {
		return err
	}
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&antflyv1.AntflyCluster{}, ctrlbuilder.WithPredicates(antflyClusterDesiredStateEventPredicate())).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&coordinationv1.Lease{}).
		Owns(&batchv1.Job{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPod))
	if r.ManageInferencePools {
		builder = builder.Owns(&inferencev1alpha1.InferencePool{})
	}
	return builder.Complete(r)
}

func (r *AntflyClusterReconciler) requestsForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	clusterName := obj.GetLabels()["app.kubernetes.io/instance"]
	component := obj.GetLabels()["app.kubernetes.io/component"]
	if clusterName == "" || (component != "metadata" && component != "data" && component != "standalone" && component != "swarm") {
		return nil
	}
	cluster := &antflyv1.AntflyCluster{}
	key := types.NamespacedName{Name: clusterName, Namespace: obj.GetNamespace()}
	if err := r.Get(ctx, key, cluster); err != nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: key}}
}
