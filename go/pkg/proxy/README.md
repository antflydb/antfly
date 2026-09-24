# go/pkg/proxy

Module `github.com/antflydb/antfly/go/pkg/proxy`. Two independent proxy
libraries live here as separate Go packages (both named `proxy`, so import
them under distinct aliases):

## `antfly` — Antfly-aware public gateway

`go/pkg/proxy/antfly` ([doc.go](antfly/doc.go)) is the Antfly-aware public
gateway seam: backend routing between stateful and serverless products,
tenant/namespace-aware request policy, request-level freshness/consistency
controls, and authn/authz integration. It stays out of query execution and
storage coordination. Key types: `Gateway` (wires a `Router`, `Authenticator`,
`Authorizer`, and `BackendForwarder`), `Catalog`/`StaticCatalog`/
`ChainedCatalog` (resolve a tenant+resource to a `NamespaceRoute`), and
`ParseRoutesJSON` for loading static route configuration. Licensed under the
Elastic License 2.0 (ELv2) — see the file headers in `antfly/*.go`.

## `inference` — model-aware inference routing proxy

`go/pkg/proxy/inference` implements a model-aware routing proxy for Antfly
inference instances: `Proxy` matches incoming requests against compiled
`Route`s (operation type, model name pattern, headers, source table/org/
project/API key, time window) to a set of weighted `Destination`s, with
fallback, retry, and rate limiting. `RouteWatcher`/`route_watcher.go` and
`K8sWatcher`/`k8s_watcher.go` watch Kubernetes custom resources
(`InferenceProxyGVR`, `ExternalInferencePoolGVR`, `InferencePoolGVR`) and
endpoints to keep `Route`s and the `ModelRegistry` in sync with cluster
state. `latency.go`, `body_admission.go`, and `attachment_envelope.go` handle
latency-aware pool selection, request/response size admission, and
multimodal attachment framing. Licensed Apache-2.0.

The `InferenceProxy` custom resource these packages watch is defined and
reconciled by [`go/pkg/operator`](../operator) (`api/inference/v1alpha1`,
`controllers/inference`); the operator does not import this module directly,
and as of this writing no other in-repo module imports `go/pkg/proxy` either
— it is built and tested standalone.

## Build

```bash
cd go/pkg/proxy && GOWORK=off go build ./...
cd go/pkg/proxy && GOWORK=off go test ./...
```

This matches CI (`.github/workflows/antfly-proxy-go.yml`), which runs on
changes under `go/pkg/proxy/**`.
