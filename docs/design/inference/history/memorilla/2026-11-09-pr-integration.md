# Native Memorilla PR integration

This record checks the supplied-feature Qwen3 implementation after integrating
`origin/main` at `c776a772e4` with branch checkpoint `ca52a6565c`. It supplements
the earlier numerical, answer-quality and performance records; their binary
hashes and source snapshots remain historical receipts.

## Scope and review map

The opt-in `/ai/v1/memorilla/generate` route runs the pinned F32
Qwen3-Embedding-4B encoder, released memory reader and Qwen3-8B decoder on GB10
CUDA. Document embeddings arrive in the request. Question encoding, readout,
prefill and greedy decode run natively. Python and vLLM are reference tools,
not runtime dependencies. The optional source-verified path checks candidates
against caller-supplied source text and uses a verified fallback or abstention.

Review the implementation in these groups:

- `models/memorilla*.zig`, `pipelines/memorilla*.zig` and the soft-input decoder
  boundary implement the pinned conversion and execution contract.
- `server/memorilla*.zig`, the model manager and session factory enforce model
  identities, finite admission, cancellation, drift checks and reader lifetime.
  Reader residency belongs to the loaded decoder; request tensors and KV do not.
- The database gateway preserves existing inference authorization and forwards
  named profiles with buffered responses. It does not implement collection
  selection, database retrieval or source authorization for supplied vectors.
- OpenAPI sources generate the Zig, Go, Python, TypeScript and Rust contracts.
  The Python tools retain conversion, oracle, QA and official-engine diagnostics.

Integration preserves main's borrowed `ModelSource` manifests and includes a
regression that cancels a Memorilla load before device probing. Two supporting
changes are needed for repository checks: an optional Zod peer binding fixes
the resolver's incompatible hoisted type version during canonical frontend
generation, and the existing frozen-helper registry preserves the original v1
QA scorer bytes under the inference package's Apache license. The scorer hash
remains `c3499c3645d5a42dacdf43c4131e6680a65fa22287016de18a5e5bc6b91cb80b`.

## Refreshed validation

Local receipts live under
`.benchmark-results/memorilla/pr-preparation-20261109/` and are intentionally
excluded from Git along with model weights, environments and build output.
The local `validation-receipts.json` manifest binds the receipts by SHA256; its
hash is `27d1309c739a9b110810526ed7392c824df2812d22491243bf1b10e8837d7e83`.

| Check | Result | Receipt |
| --- | --- | --- |
| Canonical API/client/frontend generation | Passed | `generate-final.log` |
| Generated sources, including error identities and OpenAPI | Passed | `generated-check-final.log` |
| Apache boundary and license headers | Passed | `license-check-final.log` |
| Zig, Go, Python and TypeScript formatting | Passed | `format-check-final.log` |
| Memorilla Python tooling with rebuilt native verifiers | 420 passed, zero skipped | `python-tests-final.log` |
| Focused CUDA test artifact | 114 passed, zero skipped | `focused-cuda-tests.log` |
| Broader inference build/test graph | 802 passed, 41 skipped | `integration-tests-build.log` |
| Integrated inference gateway | 127 passed, one skipped; zero failures/leaks | `gateway-tests.log` |
| Embedded memory admission policy | 7 passed | `policy-tests.log` |
| Frozen-helper provenance tests | 3 passed | `provenance-tests-final.log` |
| Python SDK | 819 passed | `python-sdk-tests.log` |
| TypeScript SDK | 651 passed, one skipped | `ts-sdk-tests.log` |
| Go SDK | Passed | `go-sdk-tests.log` |
| Managed encoder/reader/decoder CUDA oracle | Passed | `integration-composed/result.json` |
| Live native latency and lifecycle checks | 48 matching answer sequences; all checks passed | `integration-http/result.json` |
| Live source-verified HTTP contract | 30 requests passed | `integration-http/verified-http-smoke.json` |

The oracle uses the unchanged full vocabulary of 151,936 logits at prefill and
three cached steps. Logit tolerances remain `atol=rtol=1e-3`; the memory/input
embedding comparison remains `atol=5e-5`, `rtol=1e-5`. Native query bits match
the stored conditional query. Cancellation/recovery, A/B/A isolation, the
64-document/128-token case, physical accounting, active-handle pinning, decoder
eviction and reader admission refund pass.

Live HTTP checks cover cold limit rejection before GPU loading, Stage3/Stage2/
Stage3 profile switching, concurrent profiles, static and in-flight artifact
drift rejection, recovery, and deadline recovery. The source-verified smoke also
checks known answers, A/B/A isolation, concurrent isolation and the unchanged
legacy schema. The harness records no owned server processes left alive.

The gateway build completes all 59 steps. Its main artifact passes 87 tests;
the local artifact passes 40 and skips one model-dependent ASR case. This covers
Memorilla authorization, normalized execution paths, exact profile forwarding,
buffered route inventory and the existing gateway contracts. The watch process
is stopped after the successful run.

The composed verifier SHA256 is
`d58933c535fe3d4bf2432854f2875659c8316346b4f476218170012df829e559`.
The serving CLI SHA256 is
`9ba8cccc0ac78e4a677934a0dfa64d9774716402aceaa6957fe9a9c141b3effd`.
After these builds, canonical license normalization changed 27 Zig source
headers without changing their bodies. Python header/format cleanup preserves
its AST; the frozen scorer was restored byte for byte. `source-body-audit.json`
records the before/after hashes and checks. Supporting provenance metadata and
documentation do not change native execution. Rust source was not changed;
the root Rust formatting check was unavailable because Cargo is not installed.

## Performance and quality interpretation

The refreshed native HTTP check records a warm median of **0.826 seconds** over
48 requests, with all output-token sequences matching the previous native
baseline. This is an integration sanity measurement, not a fresh paired Python
comparison; other build checks were running on the host.

The earlier paired controls report native F32 **0.819 seconds**, Transformers
F32 **0.826 seconds** and official pinned vLLM 0.9.1 BF16 **0.823 seconds** on the
fixed warm short-answer workload. Native includes question encoding, memory
readout, decoder prefill and answer generation using supplied document vectors.
Cold model load and document embedding production are excluded. The small
observed median advantage and intervals crossing zero support **near parity**,
not a general speedup claim. The vLLM comparison also differs in precision:
40/48 native/vLLM output sequences match, so it does not establish bitwise
generation parity between those engines.
See the [paired latency report](2026-11-09-native-python-latency.md) and
[official vLLM qualification](2026-11-09-vllm-engine-qualification.md).

Released-reader limitations remain unchanged: native and Python fail all 22
exact novel operational-fact cases, while correctly handling two missing-fact
cases. Official vLLM and its matched BF16 Transformers control both score
20/32 on the bounded TriviaQA sample; 52/56 output sequences and all 56 prompt
sequences agree. Native numerical compatibility does not establish exact
latent-only factual QA quality. No adapted reader, native BF16 profile,
database collection integration or Gemma training milestone is promoted by
this PR. Serving remains disabled by default.
