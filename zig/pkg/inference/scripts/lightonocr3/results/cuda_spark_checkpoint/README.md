# CUDA DGX Spark progress checkpoint

This is an intermediate checkpoint, not a claim of all-model vLLM parity.
Runs use Glowworm's NVIDIA GB10 (SM 12.1), CUDA 13.0, original checkpoints,
and the installed vLLM 0.32 stack. Reports retain exact commands, environments,
hardware snapshots, model identities, fixture outputs and runtime fingerprints.
Private profiling shims, build caches, checkpoints and binaries are excluded.

## Throughput screens

Each native row lists two measured 256-request rounds after 256 saturation
warmup requests with one resident model. Native LightOn uses client concurrency
256 and physical cohorts up to 128. The separate vLLM capacity screens use
concurrency 128 and 256 requests after 256 warmups. Florence uses concurrency 64.
These are separate screens, not fresh adjacent paired runs.

| Model / profile | Native pages/s | Installed vLLM pages/s |
| --- | ---: | ---: |
| LightOn 0.8B, F32 / compact KV | 9.5734 / 9.7980 | 14.56866 |
| LightOn 0.8B, BF16 prefill FFN only | 9.6128 / 9.9749 | same reference |
| LightOn 4B, F32 / compact KV | 3.3554 / 3.3484 | 4.62958 |
| LightOn 4B, BF16 prefill FFN only | 3.4206 / 3.4271 | same reference |
| LightOn 4B, F32 / sixteen prefix items | 3.3902 / 3.3849 | same reference |
| Florence FP16, canonical decode GEMM, repeat 0 | 25.3468 / 25.7575 | 25.33988 / 25.34760 |
| Florence FP16, canonical decode GEMM, repeat 1 | 25.4923 / 25.5506 | same reference |

Native full outputs match native serial outputs and supplied vLLM fixture
references (cross-runtime whitespace normalization). Tests use three synthetic
invoice/receipt/shipping pages; these do not establish broad OCR accuracy.
Native accepts encoded PNGs; vLLM receives already decoded PIL images. Both
perform model preprocessing. The frontend costs differ.

Default activations, outputs and recurrent state remain F32. Native CUDA KV is
F16, whereas vLLM KV is BF16. The BF16 prefill FFN option changes only large-row
FFN boundaries; full BF16 activation parity remains unimplemented. vLLM's actual
LightOn recurrent state is F32. The 128-row native profiles keep packet kernels
at most 64 rows. Memory caps remain process 80, shared 90, backend 60, scratch 40,
and KV 8 GiB. vLLM uses memory utilization 0.4, max length 1024 and 4096 batched
tokens. Media identifiers are unique and reference cache reuse is disabled.

Florence canonical GEMM passed four complete output runs (two 128-request runs
and the two 256-request runs retained here). An older rare receipt mismatch is
not definitively root-caused; the canonical mode remains opt-in. The sixteen
prefix option also remains experimental: it passed actual >8-prefix execution,
4096-row ceiling and output checks, but delivered no sizable throughput gain.

## Qualification and provenance

Source123 is the qualified runtime at this checkpoint. Its binary SHA-256 is
`24e3fbd35adfcc7cd93894132a05cc8fa2700d9e5ba70a088c0cd672cd65cdf1`;
its original runtime patch SHA-256 is
`3d12b075cf989edf7b3467e6a1134f3aab163fa8be54e795224610da8c221d82`
against `8b3ae50e5c208debbe7ffcd90507385c136054a3`.
`canonical_source123.json` hashes each of the 39 runtime files, independently of
the later commit identity. `canonical_scripts123.json` records the 27 public
scripts at qualification time. Docs and this evidence were added afterwards. Repository-required license headers
were then normalized on new source files; stripping the old/new license headers
confirmed identical source bodies. `published_source_sha256.json` and
`published_scripts_sha256.json` identify the published files after normalization.
The qualified binary predates those comment-only edits.
Reports from earlier candidates retain their own identities; they are not
claimed to have been rerun on source123.

- Broad focused CUDA/CPU selection: 240 selected, 227 passed, 13 skipped.
- Wide selection: 77 selected, 77 passed, no skips.
- Extended 128-row recurrent partition test: bitwise output/state/history,
  including sixteen continued prefixes and 112 decode rows across two packets.
- Positive sixteen-prefix admission and one-byte-short denial checks passed.
- BF16 prefill policy checks retain original 2e-4 numerical tolerance; small-row
  decode remains bitwise unchanged. Original F32 oracles are not relaxed.
- Public Python tooling passes Ruff; changed Zig files pass `zig fmt --check`.

See `qualify123.txt`, `core123-tests.txt`, `core123-wide-tests.txt`,
`bf16-prefill120-tests.txt`, and `candidate123-manifest.json`. Report directories
encode the candidate and exact screen. CUDA-only and geometry-instrumented
0.8B diagnostic profiles completed after this checkpoint; their completion logs
are retained, but instrumented timing is not used as throughput evidence.

## Remaining work

LightOn still needs substantial improvement. Larger vision batches, a larger
GEMM plan cache, sixteen prefix items and several recurrent kernel rewrites did
not close the gap. Partial BF16 prefill FFN improves throughput only modestly.
Whole-model graph replay, a faster prefill recurrent path, attention efficiency,
and a fully matching BF16 activation profile remain under investigation. Fresh
paired comparisons and broader document evaluation remain outstanding.
