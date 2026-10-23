# Moondream2 Native Runtime

Native Zig implementation of Moondream2 for `/read` and single-image
`/generate`. It needs no ONNX Runtime: weights are read from the upstream
safetensors checkpoint and run on the shared native compute backend.

## Model and artifacts

| | |
|---|---|
| Checkpoint | `vikhyatk/moondream2`, tag `2024-03-04` (commit `b3d72d14957e9965facc14f53dc01c7e3bbe0eec`) |
| Format | single `model.safetensors`, f16, 578 tensors, 3.5 GiB |
| Config | `config.json` with `model_type: "moondream1"` |
| Tokenizer | `tokenizer.json` (CodeGen/GPT-2 byte-level BPE, 51200 vocab) |

This is the same set of weights the former ONNX path ran through
`Xenova/moondream2` (a transformers.js export of this revision), so `/read`
behavior is continuous with it. Later moondream2 revisions use multi-crop
vision and a new tokenizer; they are not supported.

Pull it with the registry tool:

```bash
antfly inference pull "vikhyatk/moondream2:safetensors@2024-03-04" --tasks read,generate
```

`--tasks read,generate` lists the model under both readers and generators.
Without it the manifest classifies `moondream1` configs as readers. Either
way the synthesized manifest declares `image` input (pulls made before that
registry change declared only `text`, which makes `/read` and `/generate`
answer `UNSUPPORTED_MODALITY`; pulling again refreshes the manifest from the
cached files).

## Architecture

All code is in three new files; the shared runtime is reused unchanged except
for the two small native-kernel changes listed under Performance.

- `src/architectures/moondream.zig`: config parsing (the original
  `phi-msft` stub config and the expanded transformers.js `PhiConfig`),
  checkpoint inventory and shape validation, and the model over
  `ComputeBackend`:
  - SigLIP so400m/14 at 378x378 in timm layout: linear patch embedding over
    `(c p1 p2)` patches, learned position embeddings, 27 pre-norm blocks with
    fused qkv and exact (erf) GELU, final LayerNorm, no pooling (729 tokens).
  - Projector: `1152 -> 8192 -> 2048`, exact GELU.
  - Phi-1.5 decoder: 24 parallel blocks (one LayerNorm feeds attention and
    MLP; `x + attn + mlp`), fused Wqkv, partial NeoX rotary on the first 32 of
    64 head dims, `gelu_new` MLP, LayerNorm + biased LM head. KV state lives
    in the paged `KvManager`.
- `src/pipelines/moondream.zig`: Pillow-bicubic preprocessing, tokenizer,
  prompt assembly, greedy or sampled decoding, and the `/generate` contract
  (`generate` / `generateStreaming`, single chunk).
- `src/readers/moondream_reader.zig`: the `/read` adapter, plugged into the
  `native_moondream` seam in `src/readers/reader.zig`. A model directory
  routes there when its path names Moondream and it holds a `moondream1`
  `config.json`, `tokenizer.json`, and `model.safetensors`; other Moondream
  directories (such as the removed ONNX exports) fail with
  `UnsupportedMoondreamArtifacts`, reported over HTTP as 400 `INVALID_MODEL`.
  Answers go
  through the existing Moondream parser (text, plus `fields` when the answer
  is JSON).

### Prompt and stopping

The pipeline reproduces upstream `Moondream.answer_question`:

```
[BOS] tok("<image>") [729 image embeddings] tok("</image>\n\nQuestion: {q}\n\nAnswer: ")
```

The two text halves are tokenized separately without special tokens, as
upstream does. Decoding stops on any token of `tok("<END>")` (upstream passes
all three as EOS ids) or `<|endoftext|>`, then strips a trailing `<` or
`<END>`. `/read` sends the request prompt as the question; an empty prompt
becomes the caption question `Describe this image.`. Default `max_tokens` is
256, as upstream.

`/generate` takes exactly one image and uses the last user turn's text as the
question. Chat templates, tools, grammar, and `response_format` are rejected.

### Rotary layout

The shared rope op pairs dimension `j` with `j + head_dim / 2` when
`rope_dim < head_dim`; Phi pairs `j` with `j + rotary_dim / 2`. The decoder
therefore applies rotary to q and k on the host after the fused qkv linear.

## Backends

CPU (`native`) only. The architecture is written against `ComputeBackend`,
but prefill and vision attention use a head-parallel host flash kernel
(`attentionHostParallel`) and the decoder's rotary and KV writes are host
operations, so a Metal or CUDA path needs device rotary, device KV seeding,
and a device-resident weight store for the f16 checkpoint. Requests with
`--backend metal|cuda` still run on CPU.

## Performance (Apple M4 Max, 14 cores, ReleaseFast)

Measured with `antfly inference read <dir> testdata/moondream/hello.png`
(750-row prompt):

| Phase | Time |
|---|---|
| Vision tower + projector (729 tokens) | 0.90 s |
| Prefill (750 rows) | 1.93 s |
| Decode (37 tokens) | 2.8 s (about 75 ms/token) |
| End to end, CLI process including load | 5.7 s |
| `/read` over HTTP, warm page cache | 6.1 s |
| `/read` over HTTP, first request after boot | 14.8 s |

Memory: the checkpoint is memory-mapped, so RSS reaches 4.1 GB, almost all
of it clean file-backed pages; the peak private footprint is 0.59 GB.
Timings were taken on an otherwise idle machine; concurrent builds slowed
prefill several-fold.

Two shared native changes made this usable:

- `NativeCompute.borrow_typed_matrix_weights` keeps the f16 matrices borrowed
  from the mapping instead of widening them to f32 on every use.
- `linalg.sgemmTransBF16Weights` splits few-row GEMMs (decode) across output
  columns; row dispatch could not parallelize `m = 1`. Prefill-sized f16
  linears (32+ rows) convert 8 MiB weight blocks to f32 and use system BLAS.

Set `ANTFLY_MOONDREAM_TIMING=1` to print per-phase timings.

## Qualification

`src/pipelines/moondream_parity_test.zig` compares the native pipeline with
`scripts/moondream/reference.py`, which runs the pinned upstream
`vision_encoder.py` (timm) and transformers' `PhiForCausalLM` loaded with the
renamed phi-msft weights, in float32 on CPU. (Upstream's own
`modeling_phi.py` at this tag does not run under transformers 4.38: its
cache-less rotary path fails on tensor rank and its cached path fails on the
attention-mask length, so the decoder reference is transformers' Phi, the same
mapping Xenova's export uses.)

On the HELLO fixture (`testdata/moondream/hello.png`, byte-identical to the
`/read` e2e image):

| Check | Result | Tolerance |
|---|---|---|
| Prompt token ids, stop ids | exact | exact |
| Preprocessed pixels | max abs 0 | 2/255 |
| Image embeddings (729 x 2048) | min row cosine 1.000000, mean abs 9.0e-5, max abs 7.9e-3 | cos >= 0.9995, mean abs <= 5e-3 |
| First-step logits | max abs 3.7e-4, same argmax | max abs <= 0.05 |
| Greedy answer | token-for-token identical (37 tokens) | exact |

Reproduce:

```bash
python zig/pkg/inference/scripts/moondream/reference.py \
  --model-dir ~/.antfly/inference/models/vikhyatk/<moondream2 dir> \
  --image zig/pkg/inference/testdata/moondream/hello.png --output <dir>
ANTFLY_MOONDREAM_MODEL=<model dir> ANTFLY_MOONDREAM_REFERENCE=<dir> \
  zig/pkg/inference/zig-out/bin/antfly-inference-tests --test-filter moondream
```

The script needs torch, torchvision, `transformers==4.38.2`, timm, einops,
safetensors, and pillow.

The `/read` e2e runs against a server started with
`antfly inference run` (or the standalone `antfly inference run`):

```bash
ANTFLY_INFERENCE_URL=http://127.0.0.1:8090 ANTFLY_INFERENCE_MOONDREAM_EXPECT_TEXT_ANY=hello \
  uv run --project zig/e2e/inference pytest -q zig/e2e/inference/test_read.py -k moondream
```

## Limits and open work

- One image per request; no multi-crop, so text in large documents is read at
  378x378 (the same limit as upstream at this revision).
- The checkpoint is loaded per request, like the former ONNX reader
  (memory-mapped, so warm loads are cheap; the first request after boot pages
  in 3.5 GiB).
- Single-chunk streaming for `/generate`.
- Metal/CUDA execution, and newer moondream2 revisions, are not implemented.
