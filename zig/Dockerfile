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

FROM --platform=$BUILDPLATFORM alpine:3.21 AS builder

ARG TARGETARCH

RUN apk add --no-cache build-base clang lld curl xz git perl python3 linux-headers

COPY --from=zig /opt/zig /opt/zig
ENV PATH="/opt/zig:${PATH}"

WORKDIR /workspace
COPY . .

RUN case "${TARGETARCH}" in \
        amd64) zig_target="x86_64-linux-musl" ;; \
        arm64) zig_target="aarch64-linux-musl" ;; \
        *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    zig build -Dtarget="${zig_target}" -Doptimize=ReleaseFast install-antfly --prefix /out

FROM --platform=$TARGETPLATFORM alpine:3.21

LABEL org.opencontainers.image.source=https://github.com/antflydb/antfly
LABEL org.opencontainers.image.description="AntflyDB Zig runtime image"
LABEL org.opencontainers.image.licenses=Elastic-2.0

RUN addgroup -S antfly && adduser -S -G antfly -h /home/antfly antfly

WORKDIR /
COPY --from=builder /out/bin/antfly /antfly

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:4200/readyz || exit 1

USER antfly

EXPOSE 8080 11433 4200

ENTRYPOINT ["/antfly"]
CMD ["swarm"]
