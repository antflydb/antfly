# Omni Operator Plan

## Decision

Antfly should ship one Kubernetes operator distribution that manages both Antfly
database resources and Termite inference resources.

The Termite controllers are not currently consumed as an external product, so the
deployment surface should be simplified now while compatibility requirements are
still small. Termite controller logic should stay separate internally, but the
runtime, image, RBAC, CRDs, webhooks, health checks, metrics, and release path
should be unified under `antfly-operator`.

## Target Shape

- One image: `antfly-operator`.
- One binary: `/manager` built from `pkg/operator/cmd/antfly-operator`.
- Long-term package layout:
  - `pkg/operator/antfly`
  - `pkg/operator/termite`
  - `pkg/operator/cmd/antfly-operator`
- One controller manager process registering:
  - `AntflyCluster`
  - `AntflyBackup`
  - `AntflyRestore`
  - `TermitePool`
  - `TermiteRoute`
- One ServiceAccount and ClusterRole.
- One webhook server and webhook Service.
- One CRD/RBAC manifest package under `pkg/operator/manifests`.
- One CRD bootstrap path that installs Antfly and Termite CRDs.
- One install bundle / kustomize tree.

## Boundaries

- Do not merge Antfly and Termite reconcilers into one large reconciler.
- Do not put Antfly and Termite controllers in the exact same Go package.
- Keep the existing `TermitePool` and `TermiteRoute` CRDs valid.
- Treat `AntflyCluster.spec.termite` as the integrated user-facing path for
  operator-managed Termite pools.

## Execution Plan

### Phase 1: Runtime Unification

- Add Termite API types to the Antfly operator manager scheme.
- Register `TermitePoolReconciler` and `TermiteRouteReconciler` from the Antfly
  operator binary.
- Register Termite admission webhooks on the Antfly operator webhook server.
- Add a runtime switch for Termite controllers, defaulting to enabled.
- Keep Antfly and Termite event recorder names distinct.

### Phase 2: CRD Bootstrap Unification

- Embed Termite CRDs in the Antfly operator manifest package.
- Ensure startup CRD bootstrap applies and waits for both Antfly and Termite
  CRDs.
- Keep the bootstrap field owner as `antfly-operator`.

### Phase 3: Install Bundle Unification

- Merge Termite RBAC permissions into the Antfly operator ClusterRole.
- Point Termite validating webhooks at the Antfly webhook Service.
- Include Termite validating webhook rules in the Antfly webhook manifest.
- Delete the separate Termite operator deployment/config tree.

### Phase 4: Build and Release Cleanup

- Build the Antfly operator image with the Termite controllers source available.
- Remove the standalone Termite operator Dockerfile and image publishing job.
- Publish only the integrated `antfly-operator` image.

### Phase 5: Package Layout Cleanup

- Move the operator code into a neutral package tree:
  - `pkg/operator/antfly`
  - `pkg/operator/termite`
- Keep the controller packages separate.
- Keep `pkg/operator/cmd/antfly-operator` as the only primary operator entrypoint.
- Remove temporary compatibility wrappers once imports no longer cross old module
  `internal` boundaries.

### Phase 6: Product API Follow-Up

- Keep improving `AntflyCluster.spec.termite` as the user-facing integrated
  deployment path.
- Decide later whether direct `TermitePool` and `TermiteRoute` usage remains
  documented as public API or becomes implementation-level API.

## Compatibility

Existing `TermitePool` and `TermiteRoute` resources remain valid. They are
reconciled by the integrated Antfly operator binary. There is no standalone
Termite operator binary or image after this migration.

## Risks

- A single manager increases blast radius. Mitigation: keep controller setup
  independent and allow Termite controllers to be disabled.
- RBAC grows broader. Mitigation: keep rules explicit and generated manifests
  reviewable.
- Webhook routing can drift. Mitigation: one webhook Service and one
  `ValidatingWebhookConfiguration` should own all Antfly and Termite validation
  routes.

## Current Execution Checklist

- [x] Add this plan.
- [x] Register Termite API/controller/webhook code in the Antfly operator
      binary.
- [x] Embed Termite CRDs in the Antfly operator bootstrap path.
- [x] Merge Termite RBAC and webhook manifests into Antfly operator manifests.
- [x] Update Docker build context for the unified binary.
- [x] Run focused operator build/tests.

## Next Execution Checklist

- [x] Create a neutral `pkg/operator` module.
- [x] Copy Antfly operator API/controllers/webhooks/bootstrap/manifests into
      `pkg/operator/antfly`.
- [x] Copy Termite controllers API/controllers/webhooks/manifests into
      `pkg/operator/termite`.
- [x] Update copied imports to use `github.com/antflydb/antfly/pkg/operator/...`
      paths.
- [x] Point the unified Antfly operator binary at the neutral operator packages.
- [x] Delete old `pkg/antfly-operator` and `pkg/termite-operator` packages.
- [x] Verify the neutral operator module and the unified binary.
- [x] Move the primary entrypoint to `pkg/operator/cmd/antfly-operator`.

## Final Cleanup Checklist

- [x] Remove standalone Termite operator Dockerfile.
- [x] Remove standalone Termite operator CI/release workflows.
- [x] Update root build targets to use `pkg/operator`.
- [x] Add `AntflyCluster.spec.termite`.
- [x] Reconcile managed `TermitePool` resources from `AntflyCluster`.
