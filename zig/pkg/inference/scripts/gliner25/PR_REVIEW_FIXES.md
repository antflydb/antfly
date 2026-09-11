# GLiNER2.5 review fixes

The pre-PR findings are addressed at the compatibility, schema, device ownership,
shader readiness, and API error boundaries.

| Finding | Change |
| --- | --- |
| Legacy Unigram models rejected by boundary-only normalization | GLiNER2.5 production, training, and benchmark loaders explicitly require supported normalization. Generic unsupported legacy profiles retain their complete prior normalization and encoding behavior, including byte fallback and offsets. Parsing does not retain a partially accepted normalization sequence. |
| Structured classification silently ignores `top_k` | Schema compilation retains explicit presence until it can determine the route for the whole classification collection. Structured selection rejects every explicit `top_k`, including `1`; ordinary schemas retain their behavior and canonical fingerprints. |
| Metal output metadata can panic outside the request allocator | Strict uploads, kernels, allocated GEMM, and reduced outputs allocate ownership metadata through the caller before entering Metal. Retained views release through that allocator. Existing buffer reuse remains enabled. |
| Failed precise compilation can silently run fast math | The shared library records actual compilation provenance. Strict preparation and execution require safe compilation and the required pipelines; legacy consumers retain their existing fallback. |
| Legacy preflight loses configured-memory-limit attribution | Serialization installs the terminal allocation observer before its first allocation. Declared caps and backing-allocator failure remain distinct, including after a failed request and retry. |
| Enum work exhaustion becomes HTTP 500 | The actual request-wide enum work error maps to HTTP 413 and resource-limit metrics. A failed later item releases earlier results and publishes no partial batch. |

Python constructors also preserve omission of `top_k`, `hypothesis_template`, and
`multi_label`, preventing implicit legacy defaults from invalidating V2 schemas.
V1 defaults still apply on the server. All clients were regenerated from OpenAPI;
the Zig join step now tracks the extraction schema as a cache input.

## Regression validation

Validation uses serial builds on an Apple M4 with 16 GiB memory. Focused suites
cover compiler/presentation allocation failures, actual enum decoding and atomic
response handling, terminal memory attribution, strict Metal allocation failures,
safe-compilation rejection, scope lifetime, cancellation, and retries.

- Host schema/server/wire suite: 17 passed, no skips.
- Metal ownership and failure-path suite: 13 passed, no skips.
- Tokenizer suite: 80 passed, one optional artifact check skipped.
- Actual GLiNER2 and mxbai tokenizer files: all eight encode/encodeInto cases
  match HEAD, including newline/tab, mixed case, accents, CJK, and emoji.
- Model-free GLiNER contract suite: 301 passed, two external `promtool` checks skipped.
- Python extraction SDK: 45 passed; generated-client comparison passed.
- TypeScript extraction SDK: 45 passed; type checking passed.
- Go extraction SDK: passed.

The rebuilt optimized Metal worker also passed all 30 original cases against
pinned Fastino/PyTorch MPS: ten each for small, base, and multilingual. Actual
token IDs, output decisions/order/offsets, and the original absolute confidence
tolerance of `5e-4` passed. All three native owners reported complete device
cleanup, and all six native/Python workers exited cleanly. The unchanged
supervisor enforced an 8 GiB owned-process-tree ceiling, 120-second startup
timeout, and 30-second request timeout. No timing samples were collected for
latency statistics.

This check used source snapshot
`d2855269e5ca8ac080386ecea66ec4dfd9acacd9a65e9c117b7a1b9226da9c86`
and Metal executable SHA256
`256eb6bde8564b60152b634fa8eb5ffd1d7bb9b6a4d32af7f486fd3facceffe9`.
The 3,701 source/build/script/input files were unchanged through the build and
the model check. These are correctness results for the original corpus, not an
expanded scaling or serving qualification.

Reproduce the focused Zig suites from `zig/`:

```sh
zig build lib-tokenizer-test -Doptimize=ReleaseSafe -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -j1
zig build inference-test -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -j1 -- 'classification top_k' 'legacy direct preflight' 'v2 enum work' 'classification presentation shares' 'extraction v2 wire'
zig build inference-test -Dmetal=true -Dcuda=false -Donnx=false -Dpjrt=false -j1 -- 'strict GLiNER boundary' 'deberta training Metal checked metadata'
```

## Evidence scope

The latency results in [the FP32 results report](METAL_BENCHMARK_RESULTS_V2.md)
belong to its frozen source and binaries before these review fixes. These
regressions and the subsequent model preflight are separate correctness evidence;
they do not establish new latency distributions. Full Linux CI, integration with
newer main, expanded MPS scaling completion, and public serving qualification
remain separate gates.
