# CI dependency sources and inference qualification

Zig dependencies stay pinned by content hash in `build.zig.zon`. CI prepares
remote source packages from the verified GCS mirror before building, with
bounded upstream fallback while the mirror is being bootstrapped or unavailable.
The bucket stays private. CI uses Workload Identity Federation and the dedicated
`zig-dependency-reader` account, which can only read this bucket. Its access token
is scoped to the preparation step, never sent to upstream or compiler subprocesses,
and never persisted in a credential file. The compiler installation and runner
images are unchanged. Developer runs without a token retain upstream fallback.

To use the same preparation locally, select the policy-pinned Zig compiler and
run from the repository root:

```sh
export ZIG_GLOBAL_CACHE_DIR="$PWD/zig/.zig-cache/global"
python3 scripts/ci/prepare_zig_dependencies.py
cd zig
zig build
```

`scripts/ci/zig-dependencies.json` inventories the complete remote dependency
closure, including `translate-c` and its `arocc` dependency. It records immutable
upstream archive URLs and the original declared URLs when a nested package uses
a Git URL. New remote dependencies must be added with their exact Zig hash.
Preparation checks fetched manifests against the inventory and rejects missing
transitive entries. Verified warm package archives are reused without downloads
or another compiler invocation.

The preparer asks Zig to verify package content using an isolated scratch
project. This also materializes the correct package root before Zig 0.17
recompresses the global cache archive. A regression reads the admitted archive
through a subsequent real `zig build --fetch=all`; checking only the initial
fetch's reported hash misses a malformed cache root.

The source mirror publishes only public upstream archives, under hash-derived
paths, using generation-zero writes. It never shares private compiled artifacts.
The manual **Verify private Zig dependency mirror** workflow tests the dedicated
reader from a cold cache with upstream fallback disabled, then checks Zig's
complete dependency graph and repeats with remote fetching disabled (`--system`).
A local mirror-only availability check is available with `--mirror-only` and an
`ANTFLY_ZIG_MIRROR_TOKEN` obtained through your approved cloud identity. A cold source
cache with `--offline --seed PATH` verifies a set of local hash-named archives
without contacting either mirror or upstream.

Full inference E2E runs use three serial model-family lanes: generation, speech,
and embedding/other. Generation endpoints stay together to reuse their decoder;
new test files enter the remaining lane automatically. At most two full-suite
lanes run concurrently, using existing runner sizes, process memory limits, and
the 150-minute job limit. Standalone resource composition runs once in the
generation lane. Per-phase JSONL timings are flushed as tests complete, and
JUnit results and server logs have distinct artifact names for each lane.
These measurements expose cold setup, model loading, and execution costs;
changing the partition does not establish a model throughput improvement.
