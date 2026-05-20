# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM alpine:3.21 AS zig

ARG ZIG_VERSION=0.16.0
ARG BUILDARCH

RUN apk add --no-cache curl xz

RUN case "${BUILDARCH}" in \
        amd64) zig_arch="x86_64" ;; \
        arm64) zig_arch="aarch64" ;; \
        *) echo "unsupported BUILDARCH=${BUILDARCH}" >&2; exit 1 ;; \
    esac && \
    curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-${zig_arch}-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz && \
    mkdir -p /opt && \
    tar -xJf /tmp/zig.tar.xz -C /opt && \
    mv /opt/zig-${zig_arch}-linux-${ZIG_VERSION} /opt/zig

FROM --platform=$BUILDPLATFORM alpine:3.21 AS build-base

RUN apk add --no-cache build-base clang lld curl xz git perl python3 linux-headers

COPY --from=zig /opt/zig /opt/zig
ENV PATH="/opt/zig:${PATH}"

WORKDIR /workspace
COPY . .

FROM build-base AS builder

ARG TARGETARCH
ARG ANTFLY_OPTIMIZE=ReleaseFast
ARG ZIG_BUILD_JOBS=1

RUN case "${TARGETARCH}" in \
        amd64) zig_target="x86_64-linux-musl" ;; \
        arm64) zig_target="aarch64-linux-musl" ;; \
        *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    zig build -j"${ZIG_BUILD_JOBS}" -Dtarget="${zig_target}" -Doptimize="${ANTFLY_OPTIMIZE}" install-antfly --prefix /out

FROM --platform=$TARGETPLATFORM alpine:3.21 AS model-puller

ARG INCLUDE_CLIPCLAP=true
ARG CLIPCLAP_REF=hf:antflydb/clipclap:gguf:Q4_K_M
ARG CLIPCLAP_TASKS=embed
ARG CLIPCLAP_CAPABILITIES=text,image,audio

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/bin/antfly /antfly

RUN --mount=type=secret,id=hf_token,required=false \
    mkdir -p /models && \
    if [ "${INCLUDE_CLIPCLAP}" = "true" ]; then \
      if [ -f /run/secrets/hf_token ]; then export HF_TOKEN="$(cat /run/secrets/hf_token)"; fi; \
      /antfly termite pull "${CLIPCLAP_REF}" \
        --models-dir /models \
        --tasks "${CLIPCLAP_TASKS}" \
        --capabilities "${CLIPCLAP_CAPABILITIES}"; \
    fi

FROM --platform=$TARGETPLATFORM alpine:3.21

LABEL org.opencontainers.image.source=https://github.com/antflydb/antfly
LABEL org.opencontainers.image.description="AntflyDB Zig ML runtime image"
LABEL org.opencontainers.image.licenses=Elastic-2.0

ENV ANTFLY_TERMITE_MODELS_DIR="/models"

RUN addgroup -S antfly && \
    adduser -S -G antfly -h /home/antfly antfly && \
    mkdir -p /models

WORKDIR /
COPY --from=model-puller /antfly /antfly
COPY --from=model-puller /models /models

RUN chown -R antfly:antfly /models

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:4200/readyz || exit 1

USER antfly

EXPOSE 8080 11433 4200

ENTRYPOINT ["/antfly"]
CMD ["swarm"]
