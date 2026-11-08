# Memorilla supplied-feature serving qualification

Evidence collected: 2026-11-07 through 2026-11-08 UTC. Host: Linux aarch64, NVIDIA GB10, compute capability
12.1. The scoped implementation checks pass: current managed GPU composition,
the final source-bound CLI/server builds, and live supervised HTTP lifecycle
and artifact-drift recovery. Representative answer quality, capacity planning,
and deployment load testing remain rollout requirements.

The entry point is opt-in, buffered `POST /ai/v1/memorilla/generate` over
caller-supplied document embeddings. See the [serving guide](../../memorilla-serving.md)
for deployment and request contracts and the [earlier component record](2026-11-07-native-qualification.md)
for isolated encoder, reader, decoder, chat framing, and numerical evidence.

## Scope and fixed execution policy

Serving profiles select managed Qwen3-Embedding-4B and Qwen3-8B artifacts plus a
released reader bundle. Clients select a configured profile, a question, feature
rows, and a bounded output count. They cannot select model paths, memory weights,
sampling, streaming, alternate precision, or precomputed question features.
The feature API checks a supplied execution identity; this is compatibility
validation, not attestation of a caller's feature origin.

This profile uses BF16 source weights widened to resident F32, F32 activations,
and F32 decoder KV on SM121. It rejects incompatible recipes, low-precision
staging, and forbidden host/synchronization fallback policies. Ordinary inference
precision defaults remain separate. The qualified encoder recipe is full-width
2560-dimensional last-token pooling, L2 normalization, empty instruction prefixes,
and trailing token 151643. Stored features pass through FP16 storage and BF16
readout before the F32 reader; the service does not renormalize them.

| Identity | SHA256 |
| --- | --- |
| Encoder execution | `e3d6de85a3204759c8d575bfe109aba3ec1a49318ae01a88c71515415e0ce91a` |
| Decoder execution | `ee58f87b6717501ca7ce0f788945a38c339560d1294cdeb20fc9df14bd77fd71` |
| Stage 3 converted reader weights | `3a7ec610dc29818c2aa29c3867d3be3e69908a191a94d0d6f0f416d564d3470e` |

The public bound is 64 documents and 128 generated tokens, further narrowed by
the named profile and operator request ceilings. The query has an 8192-byte
limit and a 256-token encoder limit checked before GPU model acquisition.
Decoder limits include the full system/memory/question frame. Width, finite
values, compatible identity, and unit norm are checked before model work.
Unknown and duplicate wire fields, null numeric options, and quoted numeric
values are rejected. The default configuration disables the feature.

Unified gateway execution uses existing inference-write authentication and the
supervised inference worker. Its dedicated buffered operation preserves the
profile and request body. Model/request admission and physical allocation caps
remain charged through cleanup; the response lease survives through JSON
serialization. Reader and decoder scratch limits are independent subsets of the
globally held request admission, rather than two uses of an operator-wide cap.

There is no database collection scan, row authorization, freshness, retention,
TTL, source coverage, or citation claim. Database integration remains deferred:
ordinary native producer provisioning does not yet supply the accepted producer
witness required by the proposed collector. No such check was weakened to make
this API work. Gemma variants and workload answer quality remain separate work.

## Completed build and contract checks

These results are recorded in the worktree's ignored
`.benchmark-results/memorilla/` evidence directory. Tests and source snapshots
must be interpreted at their recorded build revision; a successful earlier
binary does not qualify later runtime changes.

| Gate | Recorded result | Receipt |
| --- | --- | --- |
| Python tools, fixtures, and compiled verifier integrations | 202 passed, no skips; all three verifier environment variables set | `production-python-tests.log` |
| Platform settings and ownership | 4 passed, including allocation-failure unwind | `platform-config-tests.log` |
| Final generated API/SDK artifacts | `make generate` exited zero | `production-generate-final.log` |
| Final generated Go SDK package | `go test ./oapi` passed | `generated-go-sdk-test-final.log` |
| Generated Python SDK response codec | Success round trip and 7 mocked error cases passed; no native execution | `generated-python-sdk-tests-final.log` |
| Current focused inference server suite | 73 selected, 73 passed, no skips; post-capture guards and final generation | `production-server-tests-release.log` |
| Focused final gateway suite | 4 passed, no skips, failures, or leaks | `production-gateway-tests-final.log` |
| Actual conditional reader fixture on native CPU | 22 boundaries passed, no mismatches | `composed-conditional-memory-cpu.jsonl` |
| Actual managed GPU composition and lifecycle | Passed cold A, cancellation/recovery, 64-document/128-token B, and repeated A | `composed-verify-eager.jsonl` |
| Final CLI | 28/28 build steps passed; 60 source hashes unchanged through build | `production-cli-ready.json` |
| Live supervised HTTP | 27 main smoke events passed, plus artifact-drift rejection and recovery | `http-live/attempt-01-receipt.json` |

The gateway cases cover missing/read-only/table-only authorization versus
inference-write permission, normalized paths, disabled public execution, exact
profile/body forwarding, buffered delivery, and one runtime-owned admission.
An earlier gateway aggregate recorded 86/87 main cases passing: the sole failure
was a route-inventory expectation of 23 after adding the 24th route. The
inventory was corrected; its final focused test passes. That original failed log
is retained, rather than described as an entirely passing aggregate.
The final server test executable built all 13 steps, including a six-minute
compile with 9 GiB maximum RSS. Its earlier 72-case passing run is retained
separately. The generated Python SDK checks cover 408, 500, 504, 507, and
three 503 classifications: permanent, transient, and model publication.
These mocked response-codec checks do not replace a live SDK/server request.

The final 60-file serving source snapshot `production-source-snapshot-release.json`
has SHA256 `54bb792de1ef66972c74daabeb1a811e1a4d46b20a9099ee0406f9c9a9558984`.
It follows the actual composed GPU pass, capture-policy fix, and final SDK
error-contract generation; those source hashes remained unchanged through the
server and CLI builds. The final CLI SHA256 is
`a70e25d84d1e3b66a4fdcd44424bbc7fc83ecd4868b151f1715822290375a10a`;
all 28 build steps passed. `production-cli-ready.json` binds the executable,
60-file source snapshot, build log, and current server test result; its SHA256
is `327ad652e73bdb2fe19888ade4f29cc79977e954fcd41f95365ebda5c28ed766`.
The server test log SHA256 is
`c6a87545881ce45e3809260745de4a92d421dc376e7758b68f7916c057370f54`.

## Managed encoder preparation

`composed-prepare-v3-receipt.json` records a successful actual managed encoder
load and exact query capture. The process exited zero and unloaded its manager
before the CPU oracle began. It intentionally stopped after question encoding;
the full reader/decoder service did not execute in this preparation run. This
run also predates the final fallback-policy/counter guards.

The operator requested 96 GiB; the manager resolved a 72 GiB process ceiling.
The 10 ms sampled admission peaks were:

| Admission domain | Peak bytes |
| --- | ---: |
| Host | 11,846,032,867 |
| Backend | 18,390,322,688 |
| Combined | 29,162,613,731 |

These are separately sampled high-water marks, so the host and backend maxima
need not sum to the combined maximum. Peak process RSS was 8,153,935,872 bytes;
RSS is distinct from device allocation and admission accounting.

At `encoder_loaded`, backend weight admission was 16,863,592,448 bytes: the
16,087,097,344-byte F32 encoder plus the already-held 776,495,104-byte reader
weight lease. After preparation unwound the request and reader leases, retained
encoder residency was 16,087,097,344 backend weight bytes, 8,192,606,967 host
weight bytes, and 526,336 backend scratch bytes for fixed kernel workspace.
No request KV or host scratch remained. The process then exited and released
the retained model.

The preparation executable SHA256 was
`fce79b6f00833131cd64d275f68349f8cf7c555450deb8c007fc75e652b8cdfb`.
Earlier preparations failed the harness's supervision setup and missing model
admission before producing the required artifact; their logs remain preserved.
The 1 GiB and 4 GiB budget probes returned `ResourceTemporarilyUnavailable`
at initial response admission before GPU loading. They are temporary admission
denials, not evidence of a permanent model-size classification.

## Exact-query conditional CPU oracle

The captured query uses the same canonical question as row 3 of the independent
CPU encoder fixture: “What does Antfly preserve when a source changes?” Its
receipt binds the managed publication, execution identity, model lock, and
recipe. The helper checked that every stored value exactly represents FP16 and
that FP16-to-BF16 readout reproduces the supplied native F32 bit patterns.

Quantization can expose small CPU/CUDA encoder differences. Against the
independent CPU question row, 52 of 2560 stored FP16 elements differed, with
maximum absolute difference `1.52587890625e-5`. Nine BF16 readout elements
differed, with maximum absolute difference `3.0517578125e-5`. The native stored
FP16 vector versus the raw CPU normalized vector had maximum absolute difference
`2.991408109664917e-5` and cosine `0.9999999788925947`. These measurements are
diagnostic; no encoder acceptance limit was changed. The separate four-case and
four-holdout strict-F32 encoder comparisons remain the encoder arithmetic proof.

`conditional_reference.py` then ran the released Stage 3 reader and pinned F32
decoder on CPU using independent CPU document rows 1 and 2 plus that exact native
query. Its memory output matched the hash-pinned unchanged upstream forward
bit for bit. The reference contains 25 reader tensors, including 22 execution
boundaries, and six decoder tensors: 53 prompt IDs, the corresponding injected
embeddings, sixteen 4096-wide memory tokens, 151,936 prefill logits, three forced
decode IDs, and three complete cached-decode logit rows.

Native CPU execution consumed the new reader fixture and passed the unchanged
`trained-f32-v1` contract at all 22 boundaries. Maximum boundary relative L2
error was `1.91850363e-6`. Final memory-token maximum absolute difference was
`3.43322754e-5`, with relative L2 error `9.82560938e-7`. This extends the CPU
fixture gate; it does not constitute a composed GPU pass.

The reader limits remain intermediate `atol=2e-4`, final memory `atol=5e-5`,
`rtol=1e-5`, and per-boundary relative L2 at most `5e-6`. The decoder logit
limits remain `atol=rtol=1e-3`. Conditional fixtures isolate reader/decoder error
from encoder cast differences; they neither independently qualify the encoder
nor authenticate caller features.

| Artifact | SHA256 |
| --- | --- |
| `composed-native-question.json` | `cf58a00c12a117dc79325b427f57d1054e68019b52498843762ffba9d5ea3b7b` |
| `composed-native-query-cast-diagnostic.json` | `d893b154e879d17d6abd641cea342d4b9cea115f12e7c56928b42543640b9e43` |
| `composed-conditional-reference/conditional.json` | `596ea7a4c2f9126a6d19d0388cdb971a08b0b99cb6ae284695765a41cea24989` |
| `composed-conditional-reference/memory/reference.safetensors` | `dfa711a97465e48b123f27343ee16240a10bd895a34ee6b3a1d9ed824c941c49` |
| `composed-conditional-reference/decoder/reference.safetensors` | `849b7a9dd9f500736da88e102a3153aa1190712eab3ffda0b26775dffbac249d` |
| `composed-conditional-memory-cpu.jsonl` | `e9cd8c1eed450c3bc4707600637c1a58a0ddbc083c175b6fe3f81117cb60b90a` |

## Managed Stage 3 GPU composition

The current eager-profile managed run completed successfully and exited zero.
It loaded the exact encoder, Stage 3 reader, and decoder through the real model
manager. Native stored-FP16 and BF16 query bits matched the captured query at all
2560 positions on all five requests. A used two supplied CPU-reference document
features, 53 prompt tokens, and three teacher-forced cached steps. Prompt IDs
matched the independent reference exactly.

The first guarded full attempt had loaded both models and the reader, then
rejected the decoder before its forward because SM121's ordinary default
enabled persistent graph replay. The dedicated qualified constructors now
disable all three capture gates before uploading weights; immutable profile
checks and runtime checks reject capture/lazy state, and explicit capture
environment overrides fail before GPU loading. The failed attempt is retained
in `composed-verify-guarded-rejected.json`. No numerical limit was changed.

The passing executable SHA256 was
`1c92ad6cd8ac35d372f78cf717207efede94aedccbb433e1eb52aa3f2cca7cf9`.
Its execution-source inventory is `composed-eager-build-inputs.json`, SHA256
`39285b1134ec8c352a0b20327b6759a799d1b5ea5b3444cef163755bc2a1a1c8`;
the summary verifies those frozen execution sources remained unchanged. This
inventory includes the capture-policy fix after the earlier qualified snapshot.
The full summary is `composed-verify-eager-summary.json`, SHA256
`f066e05e5e6e99492479ad1f427d477404ad359b71112def90df40c6d0fa4873`.
This was an offline in-process harness using the production service; it does
not qualify the supervised HTTP/watchdog boundary.

The operator requested 112 GiB; the observed manager ceiling was 88 GiB
(94,489,280,512 bytes). This is separate from the earlier encoder-only
preparation's 96-to-72 GiB policy resolution.

| A comparison | Maximum absolute error | Relative L2 error |
| --- | ---: | ---: |
| Injected prompt embeddings | `3.26633453e-5` | `9.54508910e-7` |
| Sixteen memory tokens | `3.26633453e-5` | `9.54541946e-7` |
| All prefill logits | `4.67300415e-5` | `3.06073842e-6` |
| Cached decode row 1 | `4.48226929e-5` | `2.54471609e-6` |
| Cached decode row 2 | `4.76837158e-5` | `2.96868856e-6` |
| Cached decode row 3 | `4.67300415e-5` | `2.29527344e-6` |

All four logit rows contain 151,936 values and have zero component mismatches
under the unchanged `atol=rtol=1e-3` gate. The memory-token relative L2 gate also
passes. The cold A, A after cancellation, and A after B have identical logit
digests. The same exact-query conditional oracle supplies all three comparisons.

Cancellation after prefill returned the expected cancellation and refunded its
request admission. The following A passed unchanged. B exercised 64 documents
and generated all 128 requested tokens with EOS stopping disabled; it is a
resource/isolation stress, not an independent 128-step numerical comparison.
Its request admission was refunded. The following A again matched the original
A bit for bit and refunded its admission.
B repeats the first document feature 64 times; it exercises the maximum input
shape, rather than the information content of a diverse 64-document corpus.

Cold sampled admission peaks were 29,450,332,956 host bytes,
50,080,048,640 backend bytes, and 79,530,381,596 combined bytes. B's warm combined
peak was 77,544,316,756 bytes. Peak process RSS was 16,853,237,760 bytes; it is
not a substitute for GPU physical allocation or shared admission accounting.
Measured cold loader times were 26.572 seconds for the encoder and 52.640
seconds for the decoder, including respective weight-upload phases of 6.130
and 11.315 seconds. These are loader measurements, not request-latency or
serving-throughput benchmarks.

Physical `DeviceBuffer` measurements remained within independently admitted
reader and decoder ceilings:

| Request and owner | Resident baseline bytes | Held transient bytes | Enforced limit bytes | Recorded lifetime peak bytes |
| --- | ---: | ---: | ---: | ---: |
| Cold A decoder | 32,763,467,776 | 183,698,944 | 32,947,166,720 | 32,828,496,296 |
| Cold A reader | 777,021,440 | 134,545,408 | 911,566,848 | 779,571,200 |
| B decoder | 32,763,467,776 | 334,693,888 | 33,098,161,664 | 32,891,017,640 |
| B reader | 777,021,440 | 144,703,488 | 921,724,928 | 780,656,640 |

After B, A's smaller decoder ceiling was enforced again. The lifetime peak
still includes B and is not presented as A's local peak. These counters cover
owned `DeviceBuffer` allocations; vendor-private allocations require deployment
headroom. Runtime pre/post guards enforce zero forbidden host/synchronization
fallback and low-precision counters under execution ownership; this log does
not expose a separate total kernel-dispatch counter.

After all requests, only the managed pair remained: 48,850,038,784 backend
weight bytes, 24,723,139,568 host weight bytes, and 1,052,672 backend scratch
bytes for the two fixed kernel workspaces. Request KV and host scratch were
zero. The per-request reader had been destroyed. The process then unloaded the
manager and exited.

## Inferred greedy-row stability

The offline proof `composed-argmax-stability.json` binds the CPU tensor file and
passing GPU log. For each row, let `E` be the measured maximum absolute error
over all 151,936 logits. If the unique CPU top-one/top-two margin exceeds `2E`,
the native top-one must be the same: its lower bound exceeds every competitor's
upper bound. This is an inference from measured error, not a direct native
argmax measurement or a relaxed acceptance gate.

| Row | CPU top-one ID | CPU top-one/top-two margin | Observed `E` |
| --- | ---: | ---: | ---: |
| Prefill | 785 | `1.21980858` | `4.67300415e-5` |
| Cached row 1 | 2530 | `0.00794982910` | `4.48226929e-5` |
| Cached row 2 | 594 | `1.34119415` | `4.76837158e-5` |
| Cached row 3 | 1482 | `0.144519806` | `4.67300415e-5` |

Every inequality passes in all three A cases; the smallest proof slack is
`0.00786018372`. Fixture input IDs `[785, 2530, 594]` are exactly the CPU greedy
prefix. The proof therefore establishes equivalence for the first three greedy
rows under the fixture's ignore-EOS policy. The fourth row is an additional
cached-decode diagnostic beyond those three generated tokens. It does not
establish longer unforced generation or representative answer quality.

The GPU JSONL SHA256 is
`ec782d8b604c72941f4e5e3b38fc90764fc42b7e0a30af95bc41694876fbe349`.
The argmax-inference receipt SHA256 is
`3d0a90e4278adc9e21d354547e6a9c5700350d1228d29039b2a778554764bd2e`.

## Live supervised HTTP and artifact-drift recovery

The first live attempt passed all 27 smoke events against the final CLI at
`127.0.0.1:18090`, with actual supervised inference workers. The controller ran
from 00:18:48 to 00:22:09 UTC on 2026-11-08. It exercised the ordinary public
embedding API to produce compatible stored document features, then submitted
those features through the public Memorilla endpoint. Native question/document
raw-recipe embeddings were exactly equal, and response identities and the Stage 3
reader digest matched their pinned execution contract.

These native-produced HTTP features are distinct from the independent CPU
document features used in the conditional numerical oracle. HTTP A/B/A equality
is an isolation and API-lifecycle check; it is not an additional independent
CPU-logit or answer-quality comparison.

The live cases covered:

- Strict unknown/unsupported fields and numeric wire types; unknown profiles,
  incompatible identities, invalid feature geometry, and request bounds.
- Rejection of incompatible encoder instructions, truncated dimensions, and
  incompatible task selection; ordinary generation cannot bypass composition
  with the qualified decoder.
- Operator input/output limits, including complete decoder framing, with the
  test deployment configured for 64 input tokens and 8 output tokens.
- A with 53 prompt tokens and 3 completions, B with 64 supplied document rows,
  and repeated A, followed by concurrent A/B requests. Repeated A responses
  were identical, and each concurrent result matched its respective serial
  baseline.
- A 1 ms request deadline returning 504, followed by an identical successful A.
  That deadline response took approximately 0.224 seconds; this is cancellation
  and recovery evidence, not a 1 ms response-time guarantee for native work.

The first successful A took 99.426 seconds with cold service loading. Subsequent
serial A and B took 3.605 and 3.572 seconds. Concurrent A/B completed in 3.661
and 7.132 seconds. These are individual observations from one qualification
attempt, not latency percentiles, a throughput benchmark, or a deployment SLA.

The separate drift probe created only its own temporary forbidden
`added_tokens.json` sidecar in the encoder directory. The service rejected the
incompatible artifact with 409. The probe removed its sidecar in `finally`;
the first restored request returned retryable 503 `MODEL_ARTIFACTS_CHANGING`
with `Retry-After: 1`, and the second returned 200 with exactly the original A
response. Existing weight/config/tokenizer file device, inode, size, mtime,
and ctime observations were unchanged. Publication-receipt and manifest hashes
were also unchanged. The probe did not modify valid weights or receipts.

The controller verified the CLI binary remained unchanged. It shut down the
supervisor using SIGTERM (recorded supervisor exit `-15`) and confirmed its
owned process group, including all supervised workers, was retired. No model
server or worker from the qualification remains running.

| Final serving evidence | SHA256 |
| --- | --- |
| `http-live/attempt-01-receipt.json` | `72cfc3c23885980c7c33dcf8d41de1dc6eb9b506fce1e6e2f7da7c6ae46eb2df` |
| `http-live/attempt-01-smoke.json` | `34533c860563a1f4d2029ede52948865ebeebd22e82bea5c840d429e878e0df2` |
| `http-live/attempt-01-asset-drift.json` | `389ccb4eb35c5cab4e531dd31dff281e2fc0c91148178b5b2e15804be1a763de` |

## Qualification boundary and rollout work

No required serving implementation validation remains pending in this record.
The completed scope is the opt-in supplied-feature API, pinned F32 execution
and F32 KV on SM121, Stage 3 managed composition, and the tested request/lifecycle
bounds. It does not establish support on other GPU architectures or precision
profiles.

All five released readers passed isolated CPU/CUDA numerical qualification in
the earlier record. This serving work targets Stage 3 composition; it does not
extend the Stage 3 composed result to all five checkpoints. The original
mixed encoder and F16 KV failures, including the unresolved paired-F16
discrepancy, remain documented separately and have not been reclassified.
Representative answer quality and deployment capacity/load testing remain
rollout gates outside the completed implementation checks.
