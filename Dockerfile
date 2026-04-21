# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
COPY cmd/antfly /app/cmd/antfly
COPY pkg /app/pkg

RUN go mod download

COPY . .

# Declare build arguments for multi-arch support (automatically provided by Docker Buildx)
ARG TARGETOS
ARG TARGETARCH

# Build the applications as static binaries for the target platform.
# This makes them portable and suitable for minimal container images.
RUN GOEXPERIMENT=simd CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -installsuffix cgo -o antfly ./cmd/antfly

# Stage 2: Create the final, minimal image
FROM alpine:3.21

LABEL org.opencontainers.image.source=https://github.com/antflydb/antfly
LABEL org.opencontainers.image.description="AntflyDB - Distributed document database with vector search for AI applications"
LABEL org.opencontainers.image.licenses=Elastic-2.0

# Create non-root user with home directory (needed for default ~/.antfly storage)
RUN addgroup -S antfly && adduser -S -G antfly -h /home/antfly antfly

# Copy the built binary from the builder stage
COPY --from=builder /app/antfly /antfly

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:4200/readyz || exit 1

# Run as non-root
USER antfly

EXPOSE 8080 11433 4200

ENTRYPOINT ["/antfly"]

# Default to single-node swarm mode for easy local usage.
# Override with a different subcommand (e.g., "metadata", "store", "termite")
# via Kubernetes Pod args or docker run arguments.
CMD ["swarm"]
