# Gemma4 Single-Device Pilot Plan

This plan gates Gemma4 LoRA fine-tuning before distributed MLX work. The target is repeatable evidence from one-device text and multimodal pilots, not maximum throughput.

## Text Pilot

Default command:

```sh
zig build run-gemma4-lora-pilot-workflow -- text /Users/tim/models/gemma-4-e2b-it /tmp/gemma4-text-pilot --count 1000 --backend mlx --epochs 1
```

Recommended larger run:

```sh
zig build run-gemma4-lora-pilot-workflow -- text /Users/tim/models/gemma-4-e2b-it /tmp/gemma4-text-pilot-10k --count 10000 --max-examples 10000 --eval-max-examples 128 --backend mlx --epochs 2
```

Acceptance gates:
- `training_config.json` records `trainer=autodiff` and `selected_backend=mlx`.
- `train_eval_report.json` has before/after eval with `optimizer_steps=0`.
- Epoch history sees the requested examples and nonzero supervised tokens.
- Adapter artifacts exist: `adapter_model.safetensors`, `adapter_config.json`, and run contract outputs.
- No unbounded memory growth across at least two epochs.

## Multimodal Pilot

Default generated dataset command:

```sh
zig build run-gemma4-lora-pilot-workflow -- multimodal /Users/tim/models/gemma-4-e2b-it /tmp/gemma4-mm-pilot --projector /Users/tim/models/gemma-4-e2b-it/mmproj-gemma-4-E2B-it-bf16.gguf --image-path /tmp/gemma4-mm-smoke/images/red.png --count 100 --backend mlx --epochs 1
```

Use `--dataset <jsonl>` for a real image dataset. The JSONL should use `gemma_chat/v1` rows with image content parts; the workflow still verifies projector fingerprinting and cache reporting.

Acceptance gates:
- Prepared artifact contains `gguf_projector_sha256` and `gguf_projector_size_bytes`.
- Training fails early with `ProjectorFingerprintMismatch` if a different projector is supplied.
- Report includes `projected_media_cache_entries`, `projected_media_cache_hits`, and `projected_media_cache_misses`.
- `examples_with_media`, image counts, and image soft-token counts match the prepared data.
- MLX train/eval completes with before/after eval `optimizer_steps=0`.

## Task List Before Distributed MLX

1. Run the 1k text pilot and archive `prepared.json`, `training_config.json`, and `train_eval_report.json`.
2. Run the 100-image multimodal pilot with repeated media to verify cache behavior.
3. Run a real multimodal pilot with unique images to measure projection cost and memory pressure.
4. Run a 10k text pilot for at least two epochs to validate longer single-device stability.
5. Add persistent projected-media cache artifacts if multimodal projection dominates runtime.
6. Add checkpoint/resume cadence for long-running jobs.
7. Only then start distributed MLX, using the single-device reports as baseline loss/throughput references.
