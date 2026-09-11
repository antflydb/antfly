# GLiNER2.5 bounded parser property tests

`zig/pkg/inference/src/gliner25_fuzz.zig` exercises the native version-2 request
parser, schema compiler, bounded regex executor, classification constraint
solver and source-word document planner. It loads no model, tokenizer artifact,
network endpoint or GPU. These local checks do not qualify model quality,
serving concurrency or hardware behavior.

## Ordinary test integration

From `zig/`, run the focused unit target:

```sh
zig build inference-test-gliner25-fuzz -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -Dsystem-blas=false -j1
```

The unfiltered `inference-test` aggregate also includes this target. A focused
filter for another inference module does not run it. Its standard Zig test
runner selects five `GLiNER25 fuzz` tests and executes the checked-in corpus
without enabling continuous fuzzing. The first compiled checkpoint passed
four tests and correctly rejected a regex seed whose Unicode case closure
exceeded the tiny compilation budget. That request is now an explicit limit
regression; the positive regex seed is case-sensitive. The complete corrected
five-test target now exits successfully in
`/private/tmp/gliner25-parser-properties-v2.log` (the standard runner is quiet
on success).

The corpus contains seven valid requests covering mixed tasks, complete
per-input schema/options replacement, explicit null/false/empty-array presence,
ordinal constraints, JointIE, regex validators and Unicode. Eleven invalid
seeds cover malformed JSON, invalid UTF-8, duplicate keys, invalid types and
empty replacement schemas. Deterministic tests also truncate a valid request
at every byte and replace each JSON delimiter with an invalid control byte.
An additional valid regex request deliberately exceeds the compilation-step
ceiling and must return the exact typed limit error.

Successful parses are checked after the caller's mutable input bytes have
been overwritten and freed. The effective schema is independently compiled
and its fingerprint compared; an empty option replacement must reset shared
settings. Document plans are likewise checked after their source buffer is
freed. Their windows must preserve original source text, partition source-word
ownership, retain immutable plan identity and round-trip UTF-8 byte, Unicode
codepoint and UTF-16 offsets. Empty text, combining characters, emoji, scripts
without spaces, whitespace and URLs remain part of that contract.

Exact and beam solver outputs must distinguish exhaustion from infeasibility
and validate every returned witness. A deterministic ordinal case first
rejects local-candidate/beam-width limits, exposes zero-node exhaustion, then
retries with sufficient capacity and returns the constrained optimum. Search
rejection does not remove a constraint or become a successful empty result.

## Per-input bounds and failure policy

| Resource | Harness ceiling |
| --- | --- |
| Request bytes / JSON nesting | 8 KiB / 16 levels |
| Live owned heap | 2 MiB, including parser/compiler/regex/planner scratch |
| Inputs / text per input / total text | 4 / 2 KiB / 4 KiB |
| Schema bytes / tasks / labels per task | 4 KiB / 8 / 8 |
| Constraint nodes / local choices / subset visits | 64 / 64 / 256 |
| Exact or beam node visits / beam width | 512 / 8 |
| Regex pattern / NFA states / compile or match steps | 128 bytes / 256 / 4,096 per operation |
| Regex cache / aggregate compile or match steps | 16 patterns, 512 states / 16,384 |
| Document words / windows / scanned window bytes | 256 / 16 / 16 KiB |
| Window body words including synthetic terminal / overlap | 8 / 2 |
| Planner-owned memory | 256 KiB, also charged to the aggregate heap |
| Cooperative control checks | 8,192 before cancellation |

The fuzz input generator admits one extra byte so the overlength rejection
path is exercised. Oversized bytes/depth are rejected before allocation.
Both reject and windowed planner policies use this fixed tiny profile;
request-provided limits cannot increase the harness's work. Plans explicitly
retain the requirement for later encoded-token admission: word planning alone
does not prove a request fits a model.

Publicly mapped input/limit errors are expected. Declared heap exhaustion and
injected cancellation are accepted only when the corresponding local owner
records that condition. Unmapped errors, panics, invariant failures and leaks
fail the test. Every success/error path must return the capped allocator's
live byte count to zero. A separate allocation-failure sweep injects backing
allocator failures through a valid parse/compile/plan, then checks recovery.

## Optional bounded coverage mutation

After the ordinary corpus passes, Zig 0.16 can mutate the same harness locally.
Invoke the package build directly so the native build-runner fuzz option
reaches its standard test runner:

```sh
cd zig/pkg/inference
zig build test-gliner25-fuzz -Doptimize=Debug -Dgliner25-fuzz-no-error-tracing=true -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -Dsystem-blas=false -j1 --fuzz=1000
```

The finite `--fuzz=1000` form does not enable the build runner's web interface.
Bare `--fuzz` enables an unbounded run and a web interface; it is outside this
bounded local workflow.

The completed finite campaign reports 1,133 executions, 89 unique executions
and 3,469 of 18,231 instrumented program counters covered (19.03%), starting
from zero. Its five ordinary tests also pass. These are the native runner's
observed counts, which exceed the requested 1,000-run stopping threshold;
they are not counts of model features or production requests. The receipt is
`/private/tmp/gliner25-parser-fuzz1000-v3.log`, SHA-256
`08da2dfadd9aa640dcd611a6af6ff3d9aef24378eeb90fa3c343edf6ef5d382c`.
This short local campaign found no failing input; it does not establish
complete parser coverage or replace sustained service and hardware tests.

The exact fuzz binary, native corpus/coverage cache, logs and selected
source/build/toolchain files are preserved in
`/private/tmp/antfly-gliner25-fuzz1000-v3-evidence.tar.gz` (2,912,737 bytes),
SHA-256 `836ee95f48162214328fec88a74f597b41912288e40824d5d360ebab128dd9ac`.
The extracted directory's `manifest.json` has SHA-256
`df2d82dafe5dc63c23deef69a6cdffb0843c07443ab8878b75b5cd0019154551`
and records each retained file's original path, size and digest. An independent
read of the coverage header and bitset matches the reported counts. The
cache retains 34 corpus inputs; unique executions and retained inputs are
different quantities. Its current input buffer is not a crash reproducer.
The snapshot was made without rerunning the campaign or changing the original
cache. It is specific to the retained native binary/toolchain and includes a
selected source snapshot, not a complete portable rebuild checkout.

The explicit `gliner25-fuzz-no-error-tracing` option works around a compiler
test-runner bug in the installed Zig 0.16.0. Its fuzz-only `test_one` error
handler passes a `builtin.StackTrace` to `std.debug.writeStackTrace`, which
expects a different `debug.StackTrace` type. The first finite campaign failed
at compile time before mutation; preserve
`/private/tmp/gliner25-parser-fuzz1000-v1.log`, SHA-256
`bf096213ebcd5832f27aeaabda1c1beefa0f8033cc749c07933142ef3a227c08`.
The inspected installed `compiler/test_runner.zig` has SHA-256
`63adeb754894468560fee4cb0ac48af96f9fb71a00ccb5910e2ed8c153b56199`.

The option defaults to false and affects only this harness module. Ordinary
Debug tests retain error-return tracing. The fuzz invocation remains Debug,
with runtime safety, panic/error detection and per-input allocator leak checks
enabled; it loses the error-return stack trace on a caught failure. Error names
and the native crash reproducer remain available. No installed Zig source is
patched. Remove this opt-in workaround when the pinned toolchain's fuzz runner
is fixed and the unchanged harness passes with tracing enabled.

The next attempt passed that compile point but failed to link Clang coverage
symbols from the unrelated platform filesystem-capacity C helper. Preserve
`/private/tmp/gliner25-parser-fuzz1000-v2.log`, SHA-256
`a09d591c3e57ebf7c3ad9fb1b9e68dc85dd8426fa4a1942326f8d0897c1995c5`.
This in-memory harness now selects the platform dependency's existing
`link_libc=false` build profile, which omits that C helper, and gives its ML
dependency the same platform instance. The test executable still links libc.
It does not call filesystem-capacity probes; production platform dependencies
are unchanged. Every Zig module involved in parsing, compilation, validation,
search and document planning retains coverage instrumentation.

Add a minimized failing raw request to `src/fuzz/gliner25_corpus.zig` and add a
deterministic assertion for the repaired contract. The harness frames seed
bytes with the four-byte little-endian length expected by Zig 0.16's `Smith`
slice API. It always uses that API during mutation; relying only on `Smith.in`
would skip real generated inputs. Preserve the native crash reproducer and
exact Zig/build identity with any reported failure.
