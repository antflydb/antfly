IMG ?= antfly-operator:latest
VERSION ?= latest
GO := GOWORK=off GOEXPERIMENT=simd go

ifeq (,$(shell $(GO) env GOBIN))
GOBIN=$(shell $(GO) env GOPATH)/bin
else
GOBIN=$(shell $(GO) env GOBIN)
endif

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ Development

.PHONY: manifests
manifests: generate

.PHONY: generate
generate: ## Generate CRDs, RBAC, and DeepCopy.
	$(GO) generate ./...

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: test
test: manifests fmt vet ## Run tests.
	$(GO) test ./... -coverprofile cover.out

.PHONY: lint
lint: ## Run golangci-lint.
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --timeout=5m

##@ Build

.PHONY: build
build: generate fmt vet ## Build operator binary.
	$(GO) build -o bin/manager ./cmd/antfly-operator

.PHONY: run
run: manifests fmt vet ## Run operator locally.
	$(GO) run ./cmd/antfly-operator

.PHONY: docker-build
docker-build: ## Build docker image from the repository root Dockerfile.
	docker build -t ${IMG} -f ../../Dockerfile.antfly-operator ../..

.PHONY: docker-push
docker-push: ## Push docker image.
	docker push ${IMG}

##@ Deployment

.PHONY: deploy-samples
deploy-samples: ## Deploy sample clusters.
	kubectl apply -f examples/

.PHONY: undeploy-samples
undeploy-samples: ## Remove sample clusters.
	kubectl delete -f examples/ --ignore-not-found=true

##@ Release

.PHONY: release
release: test docker-build ## Prepare release.
	@echo "Release ready. Next steps:"
	@echo "  git tag pkg/operator/v${VERSION}"
	@echo "  git push origin pkg/operator/v${VERSION}"
	@echo "  make docker-push"

.PHONY: release-all
release-all: release docker-push

##@ Utilities

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf bin/ cover.out

.PHONY: dev-setup
dev-setup: ## Install dev tools.
	go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

##@ Local Clusters (kind)

.PHONY: kind-create
kind-create:
	kind create cluster --name antfly-operator-test

.PHONY: kind-delete
kind-delete:
	kind delete cluster --name antfly-operator-test

.PHONY: kind-load
kind-load:
	kind load docker-image ${IMG} --name antfly-operator-test

.PHONY: kind-deploy
kind-deploy: docker-build kind-load ## Build and load the operator image into kind.

##@ Local Clusters (minikube)

.PHONY: dev-cluster-deploy
dev-cluster-deploy: ## Deploy dev cluster to minikube.
	kubectl --context=minikube create namespace antfly-dev-ns --dry-run=client -o yaml | kubectl --context=minikube apply -f -
	kubectl --context=minikube apply -f examples/development-cluster.yaml
	kubectl --context=minikube wait --for=condition=Ready pod -l app=antfly,app.kubernetes.io/instance=dev-antfly-cluster -n antfly-dev-ns --timeout=180s

.PHONY: dev-cleanup-clusters
dev-cleanup-clusters: ## Remove dev clusters from minikube.
	kubectl --context=minikube delete antflyclusters --all -n antfly-dev-ns --ignore-not-found=true

.PHONY: small-cluster-deploy
small-cluster-deploy: ## Deploy small cluster to minikube.
	kubectl --context=minikube apply -f examples/small-dev-cluster.yaml
	kubectl --context=minikube wait --for=condition=Ready pod -l app=antfly,app.kubernetes.io/instance=small-antfly-cluster --timeout=180s

.PHONY: production-cluster-deploy
production-cluster-deploy: ## Deploy production cluster to minikube.
	kubectl --context=minikube apply -f examples/production-cluster.yaml
	kubectl --context=minikube wait --for=condition=Ready pod -l app=antfly,app.kubernetes.io/instance=production-antfly-cluster --timeout=300s

.PHONY: cleanup-clusters
cleanup-clusters: ## Remove all clusters from minikube.
	kubectl --context=minikube delete antflyclusters --all --all-namespaces --ignore-not-found=true
