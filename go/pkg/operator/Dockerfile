# Build the manager binary
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

WORKDIR /workspace/go/pkg/operator
# Copy the Go Modules manifests
COPY go/pkg/operator/go.mod go.mod
COPY go/pkg/operator/go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy only source needed for `go build` so docs/examples/work-log changes do
# not invalidate the build layer.
COPY go/pkg/operator/api ./api
COPY go/pkg/operator/bootstrap ./bootstrap
COPY go/pkg/operator/cmd ./cmd
COPY go/pkg/operator/controllers ./controllers
COPY go/pkg/operator/internal ./internal
COPY go/pkg/operator/manifests ./manifests
COPY go/pkg/operator/hack ./hack

# Declare build arguments for multi-arch support (automatically provided by Docker Buildx)
ARG TARGETOS
ARG TARGETARCH

# Build for the target platform
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -o /workspace/manager ./cmd/antfly-operator

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM --platform=$TARGETPLATFORM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
