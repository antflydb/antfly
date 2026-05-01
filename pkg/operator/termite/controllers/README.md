# Termite Controllers

Kubernetes controllers for managing Termite ML inference pools.

## Controllers

- **TermitePoolReconciler**: Manages TermitePool resources, creating StatefulSets, Services, and ConfigMaps for ML model serving pools
- **TermiteRouteReconciler**: Manages TermiteRoute resources for routing inference requests to pools

## Running Tests

The controller tests use [envtest](https://book.kubebuilder.io/reference/envtest.html) which requires Kubernetes API server and etcd binaries.

```bash
# Install setup-envtest
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

# Set up envtest binaries
eval $(setup-envtest use -p env)

# Run tests
go test -v ./...
```

Tests will skip automatically if `KUBEBUILDER_ASSETS` is not set.

## Code Generation

CRDs and DeepCopy methods are generated from the API types:

```bash
cd pkg/operator/termite && go generate ./api/v1alpha1/...
```

This generates:
- `zz_generated.deepcopy.go` - DeepCopy methods for runtime.Object
- `config/crd/bases/*.yaml` - CustomResourceDefinition manifests
