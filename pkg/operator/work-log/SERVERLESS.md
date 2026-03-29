# Serverless Operator And Proxy Plan

## Goal

Add a serverless deployment/control path to the existing Go operator and add an
Antfly-aware Go proxy without polluting the existing stateful `AntflyCluster`
reconciler.

The shape is:

- `pkg/operator` manages deployment and lifecycle
- `pkg/proxy` becomes the public Antfly-aware gateway
- `antfly-zig/src/serverless/*` remains the query and maintenance data plane

This work should not try to force full parity with the stateful product before
retrieval, graph reads, and operator packaging are solid.

## Why Not Reuse `AntflyCluster`

The existing operator is clearly stateful-cluster oriented:

- metadata and data `StatefulSet` management
- PVC retention and storage reconciliation
- Raft-aware autoscaling and deregistration
- backup/restore CRDs and service mesh integration

Serverless needs different primitives:

- query `Deployment`
- maintenance `Deployment`
- object storage configuration
- query cache configuration
- proxy/gateway integration
- table routing and policy

So the right split is:

- keep `AntflyCluster` for the stateful product
- add a new serverless CRD and reconciler family
- share controller-runtime, RBAC, docs, release flow, and install surfaces

## Target Architecture

### Operator

The operator should own:

- serverless CRD validation and defaulting
- query and maintenance deployment reconciliation
- config and secret wiring
- Service, HPA, and PDB reconciliation
- proxy deployment wiring if enabled
- status reporting from the serverless runtime surfaces

The operator should not own:

- manifest or artifact state
- query execution
- publish/build logic
- cross-project routing policy decisions at request time

### Proxy

The proxy should own:

- one public API over stateful and serverless backends
- tenant-aware table routing
- authn/authz on Antfly resources
- request freshness/consistency policy
- backend capability checks
- future backend-selection heuristics

The proxy should not own:

- query/index execution
- build or maintenance coordination
- storage metadata truth
- serverless publication logic

## Proposed CRD

Start with a new CRD:

- `AntflyServerlessProject`

This should be a project-level deployment surface, not a per-table object.
Tables and their policies can be layered in later if needed.

### Initial Spec Shape

- object storage URIs
  - artifacts
  - manifests
  - wal
  - catalog
  - progress
- runtime images
  - query image
  - maintenance image
  - proxy image
- query runtime config
  - replicas
  - cache limits
- maintenance runtime config
  - replicas
  - tick interval
- proxy config
  - enabled
  - replicas
  - service type
- model/embedder secret refs

### Initial Status Shape

- observed generation
- phase
- validated
- query ready replicas
- maintenance ready replicas
- proxy ready replicas
- conditions

## Proposed Operator Implementation Order

1. Add the new CRD types and reconcile scaffold.
2. Add status-only reconciliation and validation.
3. Reconcile query and maintenance `Deployment` objects.
4. Reconcile Services, HPA, PDB, and config secrets/maps.
5. Reconcile optional proxy deployment/service.
6. Add status probing against serverless `/health`, `/status`, and `/metrics`.
7. Add table/policy integration once the project-level deployment shape is stable.

## Proposed Proxy Package Layout

Create:

- `pkg/proxy/http`
- `pkg/proxy/router`
- `pkg/proxy/authn`
- `pkg/proxy/authz`
- `pkg/proxy/catalog`
- `pkg/proxy/policy`
- `pkg/proxy/backends/stateful`
- `pkg/proxy/backends/serverless`
- `pkg/proxy/metrics`

## Proxy Responsibilities

### First Pass

- authenticate caller
- resolve tenant and table
- authorize read/write/admin operations
- route to stateful or serverless backend
- normalize errors and responses

### Second Pass

- freshness and consistency knobs
- capability-aware routing
- backend fallback rules
- per-tenant quotas and rate limiting
- backend selection heuristics

## Routing Policy

Initial default rules:

- writes route to stateful
- retrieval tables can route to serverless
- graph reads can route to serverless when graph artifacts exist
- unsupported requests fail cleanly at the proxy instead of leaking backend-specific errors

## What We Intentionally Avoid

- bolting serverless fields onto `AntflyCluster`
- making the proxy a second control plane
- putting query logic in Go
- chasing full transaction parity in serverless
- porting the stateful graph engine directly into the serverless control plane

## First Concrete Slice

The first implementation slice in this repo should be:

1. Add `AntflyServerlessProject` API types.
2. Add a dedicated reconciler with status-only reconciliation.
3. Register it in the existing manager.
4. Keep it separate from the existing stateful `AntflyCluster` path.

That gives a clean seam for later deployment reconciliation and proxy packaging.

## Current Implementation Notes

The current Go control-plane slice now includes:

- `AntflyServerlessProject` CRD and reconciler
- query, maintenance, and optional proxy `Deployment` reconciliation
- query and optional proxy `Service` reconciliation
- query/proxy `HPA` reconciliation
- query/maintenance/proxy `PDB` reconciliation
- generated serverless runtime `ConfigMap`
- generated proxy `ConfigMap`
- admission validation/defaulting
- proxy route aggregation across serverless projects in the same Kubernetes namespace

## Proxy Config Conventions

The proxy supports both static env bootstrap and file-backed dynamic reload.

Operator-managed conventions:

- routes JSON is written into the proxy `ConfigMap`
- the same JSON is mounted at `/etc/antfly-proxy/routes.json`
- proxy bearer tokens come from a referenced `Secret`
- bearer tokens are mounted at `/etc/antfly-proxy-secret/bearer_tokens.json`
- optional external route sources can come from `spec.proxy.routeConfigMapRef`
- those external routes are aggregated with inline `spec.proxy.routes`
- Kubernetes-namespace-scoped `ConfigMap`s labeled `antfly.io/serverless-proxy-route-source=true` are also aggregated as shared route sources

Expected proxy env:

- `ANTFLY_PROXY_ROUTES_FILE=/etc/antfly-proxy/routes.json`
- `ANTFLY_PROXY_BEARER_TOKENS_FILE=/etc/antfly-proxy-secret/bearer_tokens.json`
- `ANTFLY_PROXY_REQUIRE_AUTH=true|false`
- `ANTFLY_PROXY_PUBLIC_ADDR=:8080`

This lets the proxy reload route and token changes without requiring a custom
in-memory control channel.

## Public Proxy Path Shape

The proxy's public API is table-oriented:

- `/v1/tenants/<tenant>/tables/<table>/query/search`

The proxy is responsible for:

- parsing tenant/table from headers, query params, or the public path
- parsing request freshness controls like `view`, `required_version`, and `max_lag_records`
- enforcing tenant + table + operation authz
- authorizing tenant/table access
- choosing stateful vs serverless backends
- rewriting public table paths into backend-native paths
- normalizing response headers with `X-Antfly-*` metadata
- normalizing backend error responses into a stable JSON envelope

Internally, proxy routes also carry a separate serving namespace. Public callers
should stay table-oriented; namespace is a serverless serving/debug concept and
maps to internal serverless routes under `/_internal/namespaces/...`.

Supported public path aliases currently include:

- `/v1/tenants/<tenant>/tables/<table>/search`
- `/v1/tenants/<tenant>/tables/<table>/query/...`
- `/v1/tenants/<tenant>/tables/<table>/graph/...`

Internal/debug version-pinned serverless reads should use:

- `/_internal/namespaces/<serving-namespace>/query/versions/<version>/...`

## Next Operator/Proxy Steps

The next implementation priorities are:

1. richer request/response normalization between public proxy API and backend-specific APIs
2. broader table-policy sources beyond static per-project route lists
3. a full proxy deployment story in docs/examples, including multi-project route aggregation
