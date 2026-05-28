# Public Swarm Search Compat

This compat target compares the Go and Zig servers through the same public HTTP
contract instead of comparing isolated HBC internals.

The shared harness is `bench/storage/public_query_guardrail.zig`. It generates one
deterministic packed-vector dataset, writes it through:

- `POST /db/v1/tables/<table>/batch`
- `sync_level=write` by default
- packed `_embeddings.<index>` payloads

Then it waits for the dense index status endpoint to report query-visible
documents and runs:

- `POST /db/v1/tables/<table>/query`
- `embeddings.<index>` packed query vectors
- the same `k`, query count, repeat count, and concurrency

## Run Zig

```sh
zig build install -Dedition=full
zig build public-query-guardrail -- \
  --mode swarm \
  --server-kind zig \
  --swarm-binary ./zig-out/bin/antfly \
  --docs 50000 \
  --dims 384 \
  --queries 8 \
  --repeats 2 \
  --k 100 \
  --batch-size 250 \
  --search-threads 4 \
  --sync-level write \
  --index-ready-timeout-ms 900000
```

## Run Go

Build the sibling Go binary first:

```sh
(cd ~/go/pkg/antfly/src/github.com/antflydb/antfly && \
  GOEXPERIMENT=simd GOCACHE=/tmp/go-build-cache \
  go build -o /tmp/antfly-go ./go/pkg/antfly/cmd)
```

Then run the same harness against Go swarm:

```sh
zig build public-query-guardrail -- \
  --mode swarm \
  --server-kind go \
  --swarm-binary /tmp/antfly-go \
  --docs 50000 \
  --dims 384 \
  --queries 8 \
  --repeats 2 \
  --k 100 \
  --batch-size 250 \
  --search-threads 4 \
  --sync-level write \
  --index-ready-timeout-ms 900000
```

## Compare

Compare these fields first:

- load completion time and RSS peak
- health/metrics/status max latency during load
- `concurrent_qps`
- `profile_total` when present
- rerank/artifact-read profile fields when present

Go does not publish the same Zig-specific internal profile and resource-manager
metrics. Treat missing profile fields as "not reported", not as zero work.
