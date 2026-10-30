# BL: Latest-main integration, explicit driver admission and model qualification

The combined Tile/native-kernel branch merges `origin/main` at `c8d46b3d47`
without conflicts in `78923e5e6f`. Main changes inference host error identities
and fine-tune build wiring, but adds no native numerical program changes in this
merge. CUDA artifacts remain the qualified BK images.

## Deployment contract

Canonical CUDA artifacts are pinned to NVCC 13.2.86. Native code admits a driver
exposing CUDA Driver API 13.0 or newer, normally R580+. Bundled PTX 9.2 requires
Driver API 13.2 or newer, normally R595+; installing a toolkit does not upgrade
the driver. This distinction preserves native-code minor compatibility without
claiming that an older driver can JIT newer PTX.

The policy resolves the actual load path before checking the driver. Fatbin
SM75/80/89/90/100/110/120 images use native admission, including compatible minor
architectures such as Glowworm SM121. Future major architectures take the stricter
PTX fallback. Portable builds always take PTX admission. Standalone SM89 builds
reject incompatible devices. All profiles require compute capability 7.5 or
newer. L4/SM89 and A100/SM80 remain native targets; physical testing of those cards
is deferred at the user's request.

Admission runs after device discovery and before retaining a primary context,
creating a stream or allocating device storage. Failures identify the selected
profile, device, required/found driver capability and upgrade guidance. Typed
errors cross the engine boundary through regenerated error-name tables. The
policy, artifact selection and context sources enter CUDA qualification identity.

Unit coverage checks both sides of native and PTX driver thresholds, the L4 and
A100 paths, supported native architectures, future-major fallback and invalid
standalone/device combinations. Engine error-name round trips and independent
enabled-backend cache identity tests pass.

## Model protocol

Latest main is built in a separate worktree with the identical benchmark driver
and build scaffold. Production baseline sources are unchanged. Both macOS
builds use ReleaseFast and Accelerate; Linux/CUDA builds are cross-compiled for
AArch64 Linux with the same system-BLAS-disabled settings. Exact binary hashes,
baseline scaffold diff and candidate source hashes are retained.

The fixed matrix has seven CPU, eight Metal and four CUDA cases. It includes
complete MiniLM encoders in safetensors/GGUF forms, rerankers in F16/Q4_K forms,
an imported ONNX encoder on Metal and a tiny autoregressive decoder on CPU and
Metal. Inputs are pretokenized and include padding. This measures whole model
execution, excluding tokenizer and server overhead; it does not establish
large-model generation throughput.

Every case has 16 balanced process arms (`ABBABAABABBAABBA`), with 25 warmups and
101 measurements for encoders/rerankers, or 100/201 for the decoder. AA controls
run the baseline in both arms; AB compares main to the candidate. Acceptance
requires finite, byte-identical model outputs, median ratio <=1.05 and p95 ratio
<=1.10. Protocols are fixed before timing, and failed runs are retained without
automatic retries. The decoder also has an independent numerical reference.
Glowworm GPU process state is recorded before and after the run.

## Results and retained failures

| Comparison | Numerical result | Maximum accepted median ratio | Maximum accepted p95 ratio |
|---|---|---:|---:|
| CPU main/candidate, seven cases | All byte-identical | 1.02524 | 1.02169 |
| Metal main/candidate, seven encoder/reranker cases | All byte-identical | 1.00720 | 1.01504 |
| CUDA main/candidate, three of four cases | All byte-identical | 1.00095 | 1.01905 |

All original CPU and encoder/reranker Metal AA controls pass. All four CUDA
cases produce byte-identical outputs. The unpinned CUDA batch-two MiniLM case
fails the p95 gate: median ratio 1.02544, p95 ratio **1.22516**. Its AA p95 ratio
is 1.09217. This failure remains in the raw receipts and is not an accepted
unpinned tail-parity result.

Follow-up diagnostics keep the exact binaries and original failure. Pinning to
the ten 3.9 GHz CPU cores gives AA pass, AB median failure (1.08990), and reversed
BA pass (0.99053 median). A final fixed-single-core protocol uses 100 warmups and
401 measurements with all AA/AB/BA runs retained:

| Fixed core 5 | Median ratio | p95 ratio | Gate |
|---|---:|---:|---|
| AA | 0.98901 | 0.99853 | pass |
| AB | 1.04730 | 0.99613 | pass |
| BA | 1.03248 | 1.00271 | pass |

This establishes parity within the declared limits in that controlled setup;
it does not erase the original unpinned tail failure or change production CPU
affinity/global GPU clocks. No competing GPU compute application was observed
at the original run's before/after inspections. CPU background work and driver
waiting remain part of the system behavior.

Nsight diagnostics show the same five kernel names and exactly the same launch
counts in both batch-two runs (2,418 launches total). Four kernel median GPU
durations are identical; the dominant linear kernel median changes from 17,424
to 17,456 ns (+0.18%). Much of the end-to-end variation is in host readback/waiting.
These instrumented runs are diagnostic, not replacement performance gates.

## Decoder baseline defect and stronger references

The independent, already-pinned F64 oracle detects a real Metal prefill defect
on main: maximum logit error **0.0187094**, exceeding the unchanged 0.002 budget.
Main also fails the existing full/chunked-prefill 0.0001 parity check. The
candidate already passes that check and every F64 step, with maximum error
**0.00075894**. CPU outputs from both revisions pass the oracle. Candidate
CPU/Metal outputs also meet the separately declared 0.002 absolute + 0.002
relative cross-backend budget on all seven matching model cases. Main's Metal
decoder fails that cross-backend comparison; encoder/reranker cases pass.

The initial suspicion that the chunk threshold was simply too tight was rejected
by the oracle. **Neither numerical budget is relaxed.** No new native numerical
program fix is applied in this batch: the candidate's earlier implementation
already corrects this case. The exact underlying main-side defect is not isolated
by this experiment.

The harness now exports chunked-prefill logits and prints collected timings
before returning its failing chunk-parity status. This permits diagnostic
performance comparisons while preserving the failure. The follow-up main versus
candidate decoder has median ratio 1.01129 and p95 ratio 0.99983, but its overall
gate remains failed because main is numerically incorrect. These are diagnostic
timings, not an accepted comparison against a correct baseline.

A candidate-versus-candidate run passes all reference and timing gates (ratios
0.99487 median / 0.98448 p95). The follow-up uses 16 balanced arms, 10 warmups and
51 measurements, declared before it runs. Eighteen independent oracle checks
cover full CPU outputs from both revisions, candidate Metal outputs and each
corresponding chunked-prefill output. Wrong-position and nonfinite chunk probes
are rejected.

Model qualification retains exact output identity by default. A case can now
explicitly supply an independent F32 reference, its SHA-256 and a finite,
nonnegative absolute tolerance. Both arms must satisfy it; a bad baseline cannot
make a bad candidate pass. Tests reject stale references, nonfinite values,
invalid budgets, and incorrect outputs from either arm. The decoder reference
is generated from the pinned F64 algorithm/weights, rather than from candidate
outputs. Original failed receipts remain separate from this follow-up.

## Final verification

- Main and candidate build successfully for macOS and AArch64 Linux/CUDA.
- Driver admission boundary tests, two complete engine error-name round-trip
  tests and generated error-table freshness pass.
- Ten artifact regeneration tests (including compiler/driver-floor consistency),
  fifteen qualification failure-gate tests and six native-comparator tests pass.
- Both generator freshness checks and all fourteen Tile lowering tests pass.
- Independent Metal/CUDA enabled-backend cache identity test passes (230.8 s).
- Candidate runtime/build source hashes match their frozen input snapshot;
  the updated decoder harness is separately hashed and identical on both sides.
- Numerical native program bodies and all CUDA images remain unchanged from
  `7bbe576fb2`. The CI follow-up below changes legal notices and generated source
  encoding; no kernel-throughput improvement or binary-size reduction is claimed.

Raw qualification receipts, failed experiments, source/binary hashes and Nsight
profiles are retained under `.benchmark-assets/1166/bl-campaign/`. Physical
L4/A100 testing remains deferred, and the unpinned CUDA tail result remains a
qualification limitation. The PR stays draft for review of those limits.


## CI follow-up: canonical headers and one source boundary

The first pushed revision failed the automatic license-header check. Canonical
Apache notices are normalized in 180 inference files with the repository tool;
non-header bodies were verified unchanged before the targeted generator fixes.
Two computed CUDA template embed paths are replaced with equivalent literal
paths so the static dependency audit can resolve them. Their unused filename
constants are removed; embedded template contents are unchanged.

The generated Tile input manifest and Metal scalar ABI header now use one shared
canonical notice. The manifest includes the new `tile/source_boundary.zig` owner,
so both backend qualification identities watch it. That owner also unifies the
Tile comptime and quant runtime Objective-C string encoders. The old quant
encoder did not escape quotes/backslashes; full license notices exposed invalid
Objective-C string literals. Both consumers now share exact-size counting and
encoding while preserving their existing empty-line and unterminated-line
policies. No native arithmetic, device call or runtime kernel policy is added.

After regeneration, Objective-C compilation passes. The complete emitted shader
is identical after comment/whitespace normalization, preserving quoted literals;
26 changed C/CUDA/MSL files have identical non-comment source and no line-sensitive
compilation macros. All shipped CUDA binary/PTX images remain byte-identical.
The source snapshots and model binaries above predate this notice/encoder
follow-up; identity receipts connect their numerical programs to the final tree.
No model timing measurement is silently replaced by a later run.

Final follow-up checks pass: both generators, 15 Tile tests, 142 selected
renderer/compiler/native-source tests, the source-boundary regression in Debug
and ReleaseFast, and the complete local license workflow (35 dependency-boundary,
11 header, three provenance and seven asset tests). The regenerated shader also
passes 52 independent-reference Metal correctness cases and 34 native resource
comparisons against the frozen shader. Its old byte-identity-only harness guard
is replaced only in a retained temporary qualification copy, after separately
proving full non-comment shader identity. No performance timing is taken from
that modified harness.

The enabled-backend cache identity test passes again (223.3 s), with the new
shared owner included. Model-explorer's 11 tests pass and all 12 generated files
remain fresh. Hardware performance claims remain limited to the model and
fixed-core measurements above; legal/source-encoding changes do not earn kernel
source-reduction credit.
