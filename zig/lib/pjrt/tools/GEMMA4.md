# Gemma 4 native CUDA / native PJRT CUDA qualification

These are offline artifact runners, not a PJRT HTTP serving backend. All model
operations in the PJRT arm run through `gemma4_runner.zig` and the plugin C API.
Python exports HLO on CPU, streams weights, and runs the official CPU media
processor. The native arm uses the production generation pipeline and request
boundary, with fresh paged F16 KV state per call.

The two arms use identical QAT GGUF source files. Native retains Q4 weights and
F16 KV; PJRT expands mapped weights and KV to BF16. This is a runtime/representation
comparison, not an equal-precision kernel comparison. Both ignore EOS and return
the complete fixed output-token budget, synchronizing generated token readback.

## Build and prepare

From the worktree root, using the repository's pinned Zig:

```sh
zig build --build-file zig/pkg/inference/build.zig bench-gemma4-core-build \
  -Doptimize=ReleaseFast -Dcuda=true -Dcuda-artifacts=sm89 \
  --cache-dir .zig-cache/native --prefix zig-out -j1
zig build --build-file zig/lib/pjrt/build.zig gemma4-runner \
  -Doptimize=ReleaseFast --cache-dir .zig-cache/pjrt --prefix zig-out -j1
```

Keep the global Zig cache inside the worktree as well. Preparation used
Torch 2.10.0+cpu, Transformers 5.23.0, torchax 0.0.13, JAX/JAXlib 0.10.2,
NumPy 2.4.6, PyAV 17.1.0, and torchvision 0.25.0+cpu. The CUDA PJRT plugin
comes from the JAX CUDA 12 plugin distribution; the measured plugin and workers
are bound by SHA256 in campaign provenance and executable-cache identity.

Supply the Gemma 4 E4B QAT text GGUF, its matching mmproj, the upstream E4B
processor/config/chat template/tokenizer, and a suite JSON. Each case contains
`id`, `kind` (`text`, `image`, `audio`, `video`), `text`, `max_tokens`, and
`expected_prompt_tokens`; media cases add a path relative to the suite. The suite
also declares fixture hashes/sizes, `fixture_scope`, and `context_tokens: 8192`.
`gemma4_inputs.make_fixtures` creates deterministic synthetic media from a list
of text cases carrying `text`, `id`, `max_tokens`, and prepared `prompt_ids`.
Synthetic media qualify geometry and implementation, not task quality.

```sh
JAX_PLATFORMS=cpu python zig/lib/pjrt/tools/prepare_gemma4.py \
  --config "$CONFIG" --processor "$PROCESSOR" --suite "$SUITE" \
  --gguf "$GGUF" --mmproj "$MMPROJ" --out evidence/gemma4-artifacts
```

The retained three text prompts and expected prepared token IDs are in
`gemma4_text_cases.json`. To generate and prepare the eight-case fixture suite
before export (set `PROCESSOR` to the sidecar directory):

```sh
PYTHONPATH=zig/lib/pjrt/tools python - <<'PYCODE'
import json, os
from pathlib import Path
from gemma4_inputs import make_fixtures, prepare, processor
out = Path("evidence/gemma4-fixtures")
texts = json.loads(Path("zig/lib/pjrt/tools/gemma4_text_cases.json").read_text())
suite = make_fixtures(out, texts)
proc = processor(Path(os.environ["PROCESSOR"]))
expected = {case["id"]: case["prompt_ids"] for case in texts}
for case in suite["cases"]:
    inputs, metadata = prepare(proc, case, out)
    ids = inputs["input_ids"][0].tolist()
    if case["id"] in expected:
        assert ids == expected[case["id"]]
    case["expected_prompt_tokens"] = len(ids)
    case["processor_metadata"] = metadata
    case["input_shapes"] = {key: list(value.shape) for key, value in inputs.items()}
(out / "suite.json").write_text(json.dumps(suite, indent=2) + "\n")
PYCODE
```

Video renders one space after its marker to match the native standalone
formatter; frame sampling and soft-token positions are unchanged. Encoded media
must be retained and compared by hash: codec/build differences can change bytes
when fixtures are regenerated elsewhere.

The exporter rejects unknown/missing weights and unqualified physical layouts.
It retains vision/audio QAT clipping, reorders the vision patch convolution,
accounts for the reversed GGUF audio norm names, and handles the already-softplus
GGUF audio query scales exactly once. The functional sliding KV ring is checked
against the independent official CPU cache, including wraparound and shared KV.

## Measure and validate

Pass the same arguments to `benchmark_gemma4.py` for preparation and warm runs:

```sh
python zig/lib/pjrt/tools/benchmark_gemma4.py \
  --native zig-out/bin/antfly-gemma4-core \
  --runner zig-out/bin/antfly-pjrt-gemma4 --plugin "$PJRT_PLUGIN" \
  --gguf "$GGUF" --mmproj "$MMPROJ" --tokenizer "$TOKENIZER" \
  --template "$TEMPLATE" --processor "$PROCESSOR" --suite "$SUITE" \
  --manifest evidence/gemma4-artifacts/model.json \
  --cache evidence/gemma4-pjrt-cache --out evidence/prepare --prepare-only
```

Use a fresh output directory without `--prepare-only` for three alternating
fresh-process rounds, one warmup and three measured calls per case per arm.
Native additionally primes every requested modality before timing any case.
PJRT executable caches bind the manifest, worker, plugin, GPU/driver, model
sources, and each HLO. Warm runs refuse missing executables. No other GPU process
may be active. Finish builds and CPU oracles before timing. Memory is sampled
process-tree PSS plus NVML every 0.2 s, including startup and inference.

Time covers encoded file read, decode/preprocessing, towers, text decoder,
fixed-budget autoregressive generation, and synchronized token readback; excludes
HTTP, model loading, and cold compilation. PJRT includes Python processing and
worker IPC. Do not present a `--validate` run as performance data.

Run `gemma4_cpu.py` with the exporter's model/processor/suite arguments and a fresh
`--out` to generate independent official CPU FP32 first-step logits. It streams
layers and gathers token/PLE rows, so the complete dense model need not fit in
host RAM. `--only CASE` can partition the checks. This CPU reference uses the
official full forward, not the functional exported KV/media forward.

Then run the benchmark with `--validate --rounds 1 --iterations 1` into a fresh
campaign directory and compare it using:

```sh
python zig/lib/pjrt/tools/validate_gemma4.py --campaign "$VALIDATION" \
  --cpu "$CPU_REFERENCE" --out evidence/validation.json
python -m unittest discover -s zig/lib/pjrt/tools -p 'test_gemma4*.py'
```

The comparator reports cosine, relative L2, maximum absolute error, exact argmax,
and its FP32-reference gap. Its declared logit gate is cosine >= 0.9999 and
relative L2 <= 0.02. Argmax agreement is separate; first-step checks do not prove
whole-generation parity. Preserve failures rather than widening the gate.
Large artifacts, media, vectors, HLO, executable caches and full logs belong under
ignored `evidence/`; keep only compact results/provenance in the branch.

## Native PLE normalization correction

The comparison exposed a native PLE context-projection discrepancy. The official
model applies `1/sqrt(hidden_size)` before RMSNorm. Folding away that multiply
requires multiplying RMSNorm epsilon by `hidden_size`; epsilon prevents exact
scale invariance otherwise. The correction updates direct, token-tensor/fused,
and prepared decoder paths without adding a tensor, dispatch, or scratch buffer.
The regression checks actual PLE outputs against the literal scaled-projection
formula at zero and nonzero epsilon, including low-variance projection rows.
CUDA qualification is measured here; live Metal validation is not available on
this Linux host.

The manifest records export-time source hashes; campaign provenance separately
records the timed preparer and workers. Token IDs are variable program inputs.
The video spacing alignment preserves input shapes, frame metadata and static
soft-token positions, so it does not require recompiling the prepared HLO.
