# Stage 1: Build
FROM golang:1.26-alpine AS builder

LABEL org.opencontainers.image.source=https://github.com/antflydb/antfly
LABEL org.opencontainers.image.description="AntflyDB - Distributed document database with vector search for AI applications"
LABEL org.opencontainers.image.licenses=Elastic-2.0

WORKDIR /app

COPY go.mod go.sum ./
COPY termite /app/termite
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
FROM alpine:latest

# Set the working directory
WORKDIR /

# Copy the built binary from the builder stage
COPY --from=builder /app/antfly /antfly

# The application requires TLS certificates as defined in the config.
# These are sensitive and should be mounted as Kubernetes Secrets,
# not included in the image.
#
# Example:
# kubectl create secret tls antfly-tls --cert=path/to/certificate.crt --key=path/to/private.key
#
# FIXME (ajr): The following lines are placeholders. Replace them with actual TLS certificate and key.
# openssl req -x509 -nodes -newkey rsa:2048 -keyout private.key -out certificate.crt
# COPY certificate.crt /certificate.crt
# COPY private.key /private.key

# Set the entrypoint for the container.
ENTRYPOINT ["/antfly"]

# Default to single-node swarm mode for easy local usage.
# Override with a different subcommand (e.g., "metadata", "store", "termite")
# via Kubernetes Pod args or docker run arguments.
CMD ["swarm"]
