# Serverless Project Stack

This kustomization bundles the smallest table-first serverless proxy stack:

- `serverless-proxy-bearer-tokens-secret.yaml`
- `serverless-proxy-route-configmap.yaml`
- `serverless-project-with-proxy.yaml`

## Apply

```bash
cd pkg/operator
go run ./cmd/antfly-operator --print-install-manifests | kubectl apply -f -

kubectl apply -k examples/serverless-project-stack
```

## Verify Resources

```bash
kubectl get antflyserverlessprojects
kubectl get deploy
kubectl get svc
```

The example project creates:

- api deployment/service
- query deployment/service
- maintenance deployment
- optional proxy deployment/service

## Port Forward The Proxy

```bash
kubectl port-forward deployment/docs-serverless-proxy 8080:8080
```

## Verify Public Table API

```bash
# Search
curl -H 'Authorization: Bearer token-1' \
  'http://127.0.0.1:8080/v1/tenants/tenant-a/tables/docs/query/search?q=antfly'

# Graph neighbors
curl -X POST \
  -H 'Authorization: Bearer token-1' \
  -H 'Content-Type: application/json' \
  'http://127.0.0.1:8080/v1/tenants/tenant-a/tables/docs/query/graph/neighbors' \
  -d '{"doc_id":"doc-1","direction":"out"}'
```

## Notes

- Public callers use `tenant/table` paths only.
- Internal serverless routing still maps each table to a `servingNamespace`.
- Version-pinned serverless debug reads stay internal under `/_internal/namespaces/...`.
