# Build from the Antfly repository root:
# docker build -f examples/grepstein/Dockerfile -t grepstein .
FROM golang:1.26 AS build
WORKDIR /src
COPY go/pkg/sdk/ go/pkg/sdk/
COPY go/pkg/sources/ go/pkg/sources/
COPY examples/grepstein/ examples/grepstein/
WORKDIR /src/examples/grepstein
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build GOWORK=off GOEXPERIMENT=simd CGO_ENABLED=0 go build -trimpath -o /grepstein .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /grepstein /grepstein
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/grepstein"]
CMD ["explore"]
