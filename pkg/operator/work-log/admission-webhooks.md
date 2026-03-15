# 001: Admission Webhooks for Antfly Operators

**Status**: Planned
**Created**: 2025-12-10
**Updated**: 2026-03-01
**Applies to**: antfly-operator, termite-operator

## Summary

Add Kubernetes admission webhook infrastructure to both operators so that validation errors are caught at `kubectl apply` time rather than during reconciliation.

## Current State

Both operators implement validation methods directly on their CRD types using the deprecated `webhook.Validator` interface. These methods are only called from controller reconcilers — they are not registered as admission webhooks with the API server.

| CRD | Webhook File | Controller Fallback |
|-----|-------------|-------------------|
| AntflyCluster | `api/v1/antflycluster_webhook.go` | `validateClusterConfiguration()` calls `cluster.ValidateCreate()` |
| AntflyBackup | `api/v1/antflybackup_webhook.go` | None |
| AntflyRestore | `api/v1/antflyrestore_webhook.go` | None |
| TermitePool | `api/v1alpha1/termitepool_webhook.go` | `validatePool()` reimplements validation inline |
| TermiteRoute | `api/v1alpha1/termiteroute_webhook.go` | None |

**Current behavior:**
1. User runs `kubectl apply -f cluster.yaml`
2. Resource is created in Kubernetes (no admission check)
3. Controller reconciles, calls validation
4. If invalid: status set to "Degraded" with error message
5. User must check `kubectl describe` to see the error

**Desired behavior with admission webhooks:**
1. User runs `kubectl apply -f cluster.yaml`
2. Admission webhook intercepts the request
3. If invalid: `kubectl apply` fails immediately with clear error
4. If valid: resource is created, controller reconciles normally

## Benefits

- **Better UX**: Errors shown immediately in terminal
- **Fail-fast**: Invalid resources never created
- **GitOps friendly**: CI/CD pipelines fail on invalid manifests
- **Standard Kubernetes pattern**: Matches how built-in resources validate

## Trade-offs

- **Complexity**: Requires TLS certificate management (cert-manager or kube-webhook-certgen)
- **Dependency**: Webhook must be available for any create/update (`failurePolicy: Fail`)
- **Failure mode**: If operator pod is down, resources can't be created until it recovers

## Implementation Plan

### Phase 0: Prerequisite Cleanup

Before wiring webhooks, consolidate validation so all CRDs follow the same pattern.

#### 0.1 Export Core Validation Methods

The current validation methods on CRD types are unexported (e.g., `validateAntflyCluster`). Export them so the new validator structs in `internal/webhook/` can call them.

**Files to modify:**
- `api/v1/antflycluster_webhook.go` — export `validateAntflyCluster()` → `ValidateAntflyCluster()`, `validateImmutability()` → `ValidateImmutability()`
- `api/v1/antflybackup_webhook.go` — same pattern
- `api/v1/antflyrestore_webhook.go` — same pattern
- `api/v1alpha1/termitepool_webhook.go` — same pattern
- `api/v1alpha1/termiteroute_webhook.go` — same pattern

The existing `ValidateCreate()` / `ValidateUpdate()` / `ValidateDelete()` methods on the CRD types remain for backward compatibility with controller fallback. They delegate to the exported methods.

#### 0.2 Fix TermitePool Controller Validation

The TermitePool controller's `validatePool()` reimplements validation logic inline rather than delegating to `pool.ValidateCreate()`. This causes validation drift.

**File**: `termite/pkg/operator/controllers/termitepool_controller.go`

Replace the inline `validatePool()` method with:
```go
func (r *TermitePoolReconciler) validatePool(pool *antflyaiv1alpha1.TermitePool) error {
    return pool.ValidateCreate()
}
```

#### 0.3 Add Missing Controller Fallback Validation

Add controller-level fallback validation for CRDs that lack it:
- AntflyBackup controller
- AntflyRestore controller
- TermiteRoute controller

Follow the AntflyCluster pattern: call `obj.ValidateCreate()` during reconciliation with exponential backoff on error.

### Phase 1: antfly-operator Webhooks

#### 1.1 Create Validator Structs

Both operators are on controller-runtime v0.23.1. The old `webhook.Validator` interface (methods on the CRD type returning `error`) is deprecated. Use the typed `admission.Validator[T]` interface on separate structs.

**File**: `internal/webhook/v1/antflycluster_validator.go`

```go
package v1

import (
    "context"

    antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
    "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type AntflyClusterValidator struct{}

var _ admission.Validator[*antflyv1.AntflyCluster] = &AntflyClusterValidator{}

func (v *AntflyClusterValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyCluster) (admission.Warnings, error) {
    return nil, obj.ValidateAntflyCluster()
}

func (v *AntflyClusterValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyCluster) (admission.Warnings, error) {
    if err := newObj.ValidateImmutability(oldObj); err != nil {
        return nil, err
    }
    return nil, newObj.ValidateAntflyCluster()
}

func (v *AntflyClusterValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyCluster) (admission.Warnings, error) {
    return nil, nil
}
```

Create matching validator structs for AntflyBackup and AntflyRestore following the same pattern.

**File**: `internal/webhook/v1/antflybackup_validator.go`
**File**: `internal/webhook/v1/antflyrestore_validator.go`

#### 1.2 Create Webhook Setup

**File**: `internal/webhook/v1/setup.go`

```go
package v1

import (
    antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
    "sigs.k8s.io/controller-runtime/pkg/builder"
    ctrl "sigs.k8s.io/controller-runtime"
)

func SetupWithManager(mgr ctrl.Manager) error {
    if err := builder.WebhookManagedBy(mgr, &antflyv1.AntflyCluster{}).
        WithValidator(&AntflyClusterValidator{}).
        Complete(); err != nil {
        return err
    }

    if err := builder.WebhookManagedBy(mgr, &antflyv1.AntflyBackup{}).
        WithValidator(&AntflyBackupValidator{}).
        Complete(); err != nil {
        return err
    }

    if err := builder.WebhookManagedBy(mgr, &antflyv1.AntflyRestore{}).
        WithValidator(&AntflyRestoreValidator{}).
        Complete(); err != nil {
        return err
    }

    return nil
}
```

#### 1.3 Update Main.go

**File**: `cmd/antfly-operator/main.go`

```go
import (
    // ... existing imports
    "sigs.k8s.io/controller-runtime/pkg/webhook"
    webhookv1 "github.com/antflydb/antfly/pkg/operator/internal/webhook/v1"
)

func main() {
    // ... existing setup

    mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
        // ... existing options
        WebhookServer: webhook.NewServer(webhook.Options{
            Port:    9443,
            CertDir: "/tmp/k8s-webhook-server/serving-certs",
        }),
    })

    // ... existing controller setup

    // Setup webhooks
    if os.Getenv("ENABLE_WEBHOOKS") != "false" {
        if err := webhookv1.SetupWithManager(mgr); err != nil {
            setupLog.Error(err, "unable to create webhooks")
            os.Exit(1)
        }
    }

    // ... rest of main
}
```

#### 1.4 Create Webhook Configuration

**File**: `config/webhook/kustomization.yaml`

```yaml
resources:
  - manifests.yaml
  - service.yaml

configurations:
  - kustomizeconfig.yaml
```

**File**: `config/webhook/service.yaml`

```yaml
apiVersion: v1
kind: Service
metadata:
  name: antfly-operator-webhook-service
  namespace: system
spec:
  ports:
    - port: 443
      protocol: TCP
      targetPort: 9443
  selector:
    control-plane: antfly-operator
```

**File**: `config/webhook/kustomizeconfig.yaml`

```yaml
nameReference:
  - kind: Service
    version: v1
    fieldSpecs:
      - kind: ValidatingWebhookConfiguration
        group: admissionregistration.k8s.io
        path: webhooks/clientConfig/service/name

namespace:
  - kind: ValidatingWebhookConfiguration
    group: admissionregistration.k8s.io
    path: webhooks/clientConfig/service/namespace
    create: true
```

#### 1.5 Certificate Management

**Primary option: cert-manager** (recommended if already installed in cluster)

**File**: `config/certmanager/certificate.yaml`

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: selfsigned-issuer
  namespace: system
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: antfly-operator-serving-cert
  namespace: system
spec:
  dnsNames:
    - antfly-operator-webhook-service.$(NAMESPACE).svc
    - antfly-operator-webhook-service.$(NAMESPACE).svc.cluster.local
  issuerRef:
    kind: Issuer
    name: selfsigned-issuer
  secretName: webhook-server-cert
```

**File**: `config/certmanager/kustomization.yaml`

```yaml
resources:
  - certificate.yaml

replacements:
  - source:
      kind: Certificate
      group: cert-manager.io
      version: v1
      name: antfly-operator-serving-cert
      fieldPath: metadata.namespace
    targets:
      - select:
          kind: ValidatingWebhookConfiguration
        fieldPaths:
          - metadata.annotations.[cert-manager.io/inject-ca-from]
        options:
          delimiter: '/'
          index: 0
          create: true
```

**Lightweight alternative: kube-webhook-certgen** (no cert-manager dependency)

Uses a Kubernetes Job to generate a self-signed certificate and patch the webhook configuration's `caBundle` field. Same approach used by ingress-nginx. See Appendix B for details.

#### 1.6 Update Deployment

**File**: `config/default/manager_webhook_patch.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: controller-manager
  namespace: system
spec:
  template:
    spec:
      containers:
        - name: manager
          ports:
            - containerPort: 9443
              name: webhook-server
              protocol: TCP
          volumeMounts:
            - mountPath: /tmp/k8s-webhook-server/serving-certs
              name: cert
              readOnly: true
      volumes:
        - name: cert
          secret:
            defaultMode: 420
            secretName: webhook-server-cert
```

**File**: `config/default/kustomization.yaml`

```yaml
resources:
  - ../crd
  - ../rbac
  - ../manager
  - ../webhook
  - ../certmanager

patches:
  - path: manager_webhook_patch.yaml
```

#### 1.7 Generate Manifests

```bash
cd pkg/operator
make manifests generate
```

This generates `config/webhook/manifests.yaml` containing the `ValidatingWebhookConfiguration` for all three CRDs.

### Phase 2: termite-operator Webhooks

Apply the same pattern to termite-operator for TermitePool and TermiteRoute.

#### 2.1 Create Validator Structs

**File**: `internal/webhook/v1alpha1/termitepool_validator.go`

```go
package v1alpha1

import (
    "context"

    antflyaiv1alpha1 "github.com/antflydb/antfly/termite/pkg/operator/api/v1alpha1"
    "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type TermitePoolValidator struct{}

var _ admission.Validator[*antflyaiv1alpha1.TermitePool] = &TermitePoolValidator{}

func (v *TermitePoolValidator) ValidateCreate(ctx context.Context, obj *antflyaiv1alpha1.TermitePool) (admission.Warnings, error) {
    return nil, obj.ValidateTermitePool()
}

func (v *TermitePoolValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyaiv1alpha1.TermitePool) (admission.Warnings, error) {
    if err := newObj.ValidateImmutability(oldObj); err != nil {
        return nil, err
    }
    return nil, newObj.ValidateTermitePool()
}

func (v *TermitePoolValidator) ValidateDelete(ctx context.Context, obj *antflyaiv1alpha1.TermitePool) (admission.Warnings, error) {
    return nil, nil
}
```

**File**: `internal/webhook/v1alpha1/termiteroute_validator.go` — same pattern for TermiteRoute.

**File**: `internal/webhook/v1alpha1/setup.go` — registers both validators.

#### 2.2 Update main.go, deployment, and kustomize

Same infrastructure pattern as Phase 1, adapted for the termite-operator namespace and service names.

**Files:**
- `cmd/termite-operator/main.go` — add webhook server and `ENABLE_WEBHOOKS` toggle
- `config/webhook/*` — webhook service and kustomization
- `config/certmanager/*` — TLS certificate
- `config/default/kustomization.yaml` — include webhook/certmanager resources
- `config/default/manager_webhook_patch.yaml` — deployment patch

### Phase 3: Testing and Rollout

#### Unit Tests

Test the new validator structs:

```go
func TestAntflyClusterValidator_ValidateCreate(t *testing.T) {
    v := &AntflyClusterValidator{}
    cluster := &antflyv1.AntflyCluster{
        Spec: antflyv1.AntflyClusterSpec{
            GKE: &antflyv1.GKESpec{
                Autopilot:             false,
                AutopilotComputeClass: "Balanced",
            },
        },
    }

    warnings, err := v.ValidateCreate(context.Background(), cluster)
    if err == nil {
        t.Error("expected validation error for compute class without autopilot")
    }
    _ = warnings
}
```

#### Integration Tests (envtest)

```go
func TestWebhookIntegration(t *testing.T) {
    testEnv := &envtest.Environment{
        CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases")},
        WebhookInstallOptions: envtest.WebhookInstallOptions{
            Paths: []string{filepath.Join("..", "config", "webhook")},
        },
    }

    cfg, err := testEnv.Start()
    // ... setup manager with webhooks

    // Invalid resource should be rejected at admission
    invalidCluster := &antflyv1.AntflyCluster{...}
    err = k8sClient.Create(ctx, invalidCluster)
    if err == nil {
        t.Error("expected webhook to reject invalid cluster")
    }
}
```

#### Manual Testing

```bash
# Deploy with webhooks enabled
ENABLE_WEBHOOKS=true make deploy

# Try to create invalid resource
cat <<EOF | kubectl apply -f -
apiVersion: antfly.antflydb.com/v1
kind: AntflyCluster
metadata:
  name: invalid-cluster
spec:
  gke:
    autopilot: false
    autopilotComputeClass: "Balanced"
EOF

# Expected:
# Error from server: admission webhook denied the request:
# spec.gke.autopilotComputeClass is set but spec.gke.autopilot=false
```

#### Rollout Strategy

1. Ship with `ENABLE_WEBHOOKS=false` (default) — existing deployments unaffected
2. Test in staging with `ENABLE_WEBHOOKS=true`
3. Enable by default, document `ENABLE_WEBHOOKS=false` as escape hatch

## Backward Compatibility

- Controller-level validation remains as fallback for all CRDs
- `ENABLE_WEBHOOKS=false` environment variable disables webhook registration
- Existing deployments continue to work (webhook is additive)
- The old `ValidateCreate()` / `ValidateUpdate()` methods on CRD types remain for controller fallback

## Files to Create/Modify

### antfly-operator

| File | Action | Description |
|------|--------|-------------|
| `api/v1/antflycluster_webhook.go` | Modify | Export core validation methods |
| `api/v1/antflybackup_webhook.go` | Modify | Export core validation methods |
| `api/v1/antflyrestore_webhook.go` | Modify | Export core validation methods |
| `internal/webhook/v1/antflycluster_validator.go` | Create | Typed validator struct |
| `internal/webhook/v1/antflybackup_validator.go` | Create | Typed validator struct |
| `internal/webhook/v1/antflyrestore_validator.go` | Create | Typed validator struct |
| `internal/webhook/v1/setup.go` | Create | Webhook registration |
| `config/webhook/kustomization.yaml` | Create | Webhook kustomization |
| `config/webhook/service.yaml` | Create | Webhook service |
| `config/webhook/kustomizeconfig.yaml` | Create | Kustomize config |
| `config/certmanager/certificate.yaml` | Create | TLS certificate |
| `config/certmanager/kustomization.yaml` | Create | Certmanager kustomization |
| `config/default/kustomization.yaml` | Modify | Include webhook/certmanager |
| `config/default/manager_webhook_patch.yaml` | Create | Deployment patch |
| `cmd/antfly-operator/main.go` | Modify | Add webhook server + ENABLE_WEBHOOKS |

### termite-operator

| File | Action | Description |
|------|--------|-------------|
| `api/v1alpha1/termitepool_webhook.go` | Modify | Export core validation methods |
| `api/v1alpha1/termiteroute_webhook.go` | Modify | Export core validation methods |
| `controllers/termitepool_controller.go` | Modify | Replace inline `validatePool()` with delegation |
| `internal/webhook/v1alpha1/termitepool_validator.go` | Create | Typed validator struct |
| `internal/webhook/v1alpha1/termiteroute_validator.go` | Create | Typed validator struct |
| `internal/webhook/v1alpha1/setup.go` | Create | Webhook registration |
| `config/webhook/*` | Create | Webhook configuration |
| `config/certmanager/*` | Create | Certificate configuration |
| `config/default/kustomization.yaml` | Modify | Include webhook resources |
| `config/default/manager_webhook_patch.yaml` | Create | Deployment patch |
| `cmd/termite-operator/main.go` | Modify | Add webhook server + ENABLE_WEBHOOKS |

## Appendix A: ValidatingAdmissionPolicy

As of Kubernetes 1.30, `ValidatingAdmissionPolicy` is GA. It provides in-process validation using CEL expressions — no webhook server, no TLS certificates, no external dependencies.

For simple validations (enum checks, range validation, replica count >= 0), CEL expressions could replace webhook logic. However, the operators' complex validations — cross-field constraints, immutability checks comparing old vs new objects, multi-part error messages with solutions — are difficult or impossible to express in CEL.

**Recommendation**: Keep webhooks for all current validation. Consider migrating simple enum/range checks to ValidatingAdmissionPolicy as a future optimization once Kubernetes 1.30+ is the minimum supported version.

References:
- [Kubernetes 1.30: ValidatingAdmissionPolicy GA](https://kubernetes.io/blog/2024/04/24/validating-admission-policy-ga/)
- [Validating Admission Policy docs](https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/)

## Appendix B: Certificate Management Alternatives

### cert-manager (recommended)

Full-featured certificate lifecycle management. Handles issuance, renewal, and injection into webhook configurations automatically.

**Prerequisite:**
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```

Best when: cert-manager is already installed in the cluster (common for ingress TLS).

### kube-webhook-certgen (lightweight)

Runs as a Kubernetes Job that generates a self-signed CA + server certificate, creates a TLS secret, and patches the `ValidatingWebhookConfiguration` with the CA bundle. Used by ingress-nginx.

No ongoing dependency — the Job runs once at deploy time. Certificates must be manually rotated or the Job re-run.

Best when: cert-manager is not installed and adding it is undesirable.

## References

- [Kubebuilder Webhook Tutorial](https://book.kubebuilder.io/cronjob-tutorial/webhook-implementation.html)
- [controller-runtime Validator[T] interface](https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/webhook/admission#Validator)
- [Kubebuilder deprecated webhook.Validator migration](https://github.com/kubernetes-sigs/kubebuilder/issues/3721)
- [Kubernetes Admission Webhook Good Practices](https://kubernetes.io/docs/concepts/cluster-administration/admission-webhooks-good-practices/)
- [Kubernetes Dynamic Admission Control](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
- [cert-manager Documentation](https://cert-manager.io/docs/)
