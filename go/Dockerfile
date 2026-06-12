# Stage 1: Build
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

WORKDIR /app

COPY . .
WORKDIR /app/go/pkg/antfly
RUN go mod download

# Declare build arguments for multi-arch support (automatically provided by Docker Buildx)
ARG TARGETOS
ARG TARGETARCH

# Build the applications as static binaries for the target platform.
# This makes them portable and suitable for minimal container images.
RUN GOEXPERIMENT=simd CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -installsuffix cgo -o antfly ./cmd

# Stage 2: Create the final, minimal image
FROM --platform=$TARGETPLATFORM alpine:3.21

LABEL org.opencontainers.image.source=https://github.com/antflydb/antfly
LABEL org.opencontainers.image.description="AntflyDB - Distributed document database with vector search for AI applications"
LABEL org.opencontainers.image.licenses=Elastic-2.0

# Create non-root user with a stable UID/GID so Kubernetes fsGroup and init
# container ownership fixes match the runtime user across images.
ARG ANTFLY_UID=10001
ARG ANTFLY_GID=10001
RUN addgroup -S -g ${ANTFLY_GID} antfly && \
    adduser -S -u ${ANTFLY_UID} -G antfly -h /home/antfly antfly

# Copy the built binary from the builder stage
COPY --from=builder /app/go/pkg/antfly/antfly /antfly

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:4200/readyz || exit 1

# Run as non-root
USER antfly

EXPOSE 8080 11433 4200

ENTRYPOINT ["/antfly"]

# Default to single-node swarm mode for easy local usage.
# Override with a different subcommand (e.g., "metadata", "store", "inference")
# via Kubernetes Pod args or docker run arguments.
CMD ["swarm"]
