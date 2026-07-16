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
ARG ZIG_OPTIMIZE=ReleaseFast
ARG ZIG_BUILD_JOBS=1

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
    cd zig && \
    zig build -j"${ZIG_BUILD_JOBS}" -Dtarget="${zig_target}" -Doptimize="${ZIG_OPTIMIZE}" -Dedition=full install --prefix /out

FROM --platform=$BUILDPLATFORM alpine:3.21 AS wasmtime

ARG WASMTIME_VERSION=v45.0.2
ARG TARGETARCH

RUN apk add --no-cache ca-certificates curl xz

RUN case "${TARGETARCH}" in \
        amd64) wasmtime_arch="x86_64" ;; \
        arm64) wasmtime_arch="aarch64" ;; \
        *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    archive="wasmtime-${WASMTIME_VERSION}-${wasmtime_arch}-musl-c-api.tar.xz" && \
    curl -fsSL "https://github.com/bytecodealliance/wasmtime/releases/download/${WASMTIME_VERSION}/${archive}" -o "/tmp/${archive}" && \
    mkdir -p /opt/wasmtime && \
    tar -xJf "/tmp/${archive}" -C /opt/wasmtime --strip-components=1 && \
    test -f /opt/wasmtime/lib/libwasmtime.so

FROM --platform=$TARGETPLATFORM alpine:3.21

LABEL org.opencontainers.image.source=https://github.com/antflydb/antfly
LABEL org.opencontainers.image.description="AntflyDB Zig runtime image"
LABEL org.opencontainers.image.licenses=Elastic-2.0

RUN addgroup -S -g 10001 antfly && adduser -S -u 10001 -G antfly -h /home/antfly antfly

WORKDIR /
COPY --from=builder /out/bin/antfly /antfly
COPY --from=builder /out/share/antfly /usr/share/antfly
COPY --from=wasmtime /opt/wasmtime/lib/libwasmtime.so /usr/local/lib/libwasmtime.so

ENV ANTFLY_WASMTIME_LIB=/usr/local/lib/libwasmtime.so
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV ANTFLY_EXTENSION_PACKAGE_STORE=/tmp/antflydb/extensions

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:4200/readyz || exit 1

USER antfly

EXPOSE 8080 11433 4200

ENTRYPOINT ["/antfly"]
CMD ["standalone"]
