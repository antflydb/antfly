# EmbeddingGemma 2 operations

Antfly supports the managed `google/embeddinggemma-2:bf16-safetensors-bundle-v1`
bundle for text, image, audio, and ordered mixed-content embeddings. Pull the
bundle and run the inference service with CUDA:

```sh
antfly-inference pull \
  google/embeddinggemma-2:bf16-safetensors-bundle-v1 --models-dir .models

ANTFLY_INFERENCE_PREFERRED_BACKEND=cuda \
ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda \
antfly-inference run \
  --models-dir .models \
  --preload-model embedder:cuda:google/embeddinggemma-2:bf16-safetensors-bundle-v1
```

Choose `-Dcuda-artifacts=sm89` for an SM89-specific build or
`-Dcuda-artifacts=fatbin` for the distributable CUDA bundle. The BF16 CUDA
path requires an SM80-or-newer GPU and a driver compatible with the CUDA
toolchain used to produce the checked-in artifact. CPU inference remains
available, but native EmbeddingGemma 2 LoRA training requires CUDA.

## Video embeddings on CUDA

On Linux CUDA, one progressive 8-bit SDR H.264 MP4 clip produces one normalized
embedding. Use `antfly-inference embed <model-dir> --backend cuda --video clip.mp4`
or a `video/mp4` media part in an embedding request. Each input must contain
only that clip, with no title. The processor samples at 1 FPS, uniformly caps
at 32 frames, and encodes their visual tokens together in temporal order.

NVDEC decoding, color conversion, bicubic resize, projection, and encoder
inputs remain on the GPU; only the final vector is downloaded. Clips are
limited to 60 seconds, 256 MiB, and 4096 pixels per side. HTTP request limits
may be smaller. Other codecs, HDR, rotation, interlacing, and non-square pixels
are rejected. `libnvcuvid.so.1` is required.

See [CUDA video behavior and limits](../../CUDA_TUNING.md#embeddinggemma2-video-embeddings)
for the supported envelope, admission requirements, and regression fixtures.
Video is not supported by the CPU or Metal embedding route in this implementation.

## Native LoRA workflow

Prepare an authenticated retrieval dataset, train or resume, export the
adapter, and evaluate it through the unified finetune command:

```sh
antfly-inference finetune dataset prepare embeddinggemma2-lora \
  /models/embeddinggemma-2 /data/retrieval.jsonl /data/prepared

antfly-inference finetune train run embeddinggemma2 job.json

antfly-inference finetune train run embeddinggemma2 resume-job.json \
  --resume-from /runs/embeddinggemma2/checkpoint-00000050

antfly-inference finetune adapter export embeddinggemma2-lora \
  job.json /runs/embeddinggemma2/checkpoint-00000100 \
  /models/tuned-embeddinggemma2

antfly-inference finetune eval run embeddinggemma2-retrieval \
  job.json /models/tuned-embeddinggemma2 --split validation --backend cuda
```

The retrieval source is JSONL with one object per query. For example:

```json
{"query_id":"password-1","query":"How do I reset my password?","positive":{"id":"password","text":"Use account settings to reset your password."},"negatives":[{"id":"shipping","text":"Orders ship within two business days."}],"task":"retrieval","split":"train"}
```

`positive` and every `negatives` entry require unique `id` and `text`
fields; `title` is optional. The optional `known_positive_ids` array prevents
known positives from being admitted as negatives. Use
`task: "code_retrieval"` for code records and assign each record to
`train`, `validation`, or `test`. `query_id` and `group_id` are optional.

A minimal training job is:

```json
{
  "version": 1,
  "backend": "cuda",
  "model_dir": "/models/embeddinggemma-2",
  "prepared_dir": "/data/prepared",
  "output_dir": "/runs/embeddinggemma2",
  "device_id": 0,
  "lora": {
    "rank": 16,
    "alpha": 32.0,
    "dropout": 0.0,
    "targets": "encoder_and_embedding_projection"
  },
  "logical_batch_size": 64,
  "microbatch_size": 2,
  "query_max_tokens": 128,
  "document_max_tokens": 512,
  "max_negatives": 4,
  "temperature": 0.05,
  "epochs": 1,
  "seed": 42,
  "checkpoint_every_steps": 50,
  "max_device_bytes": 21474836480,
  "max_host_bytes": 8589934592
}
```

Resume into a fresh `output_dir` with otherwise matching model, dataset,
tokenizer, and training settings. Exported adapters are intended for the same
base-model identity recorded by the checkpoint. Their manifest declares
`inputs=["text"]` and `media_enabled=false`: exported tuned bundles serve
text and code only and cannot be enabled for image, audio, or mixed-content
requests by additional evaluation.

The native evaluator measures retrieval against the bounded candidate corpus
in the prepared validation or test split. Its report is useful for comparing a
checkpoint with its base model, but it is not a full official quality
qualification or evidence for unrelated corpora.

Use `--help` on each subcommand for the complete argument and input schemas.

## Maintainer generators

`generate_casefold_table.py` verifies or updates the pinned Python 3.11 /
Unicode 14 casefold metadata used by dataset preparation. Run it with Python
3.11:

```sh
python3.11 scripts/embedding_gemma2/generate_casefold_table.py
python3.11 scripts/embedding_gemma2/generate_casefold_table.py --write
```

`generate_training_rope_frequencies.py` verifies or updates the CUDA training
RoPE constants from the official model initialization. It requires the pinned
model locally, PyTorch `2.14.0`, and Transformers commit
`a96730c8c97b8efbf35bbaf7f5da33ec99231a49`:

```sh
python scripts/embedding_gemma2/generate_training_rope_frequencies.py \
  --model /models/embeddinggemma-2

python scripts/embedding_gemma2/generate_training_rope_frequencies.py \
  --model /models/embeddinggemma-2 --write
```

After changing either generated CUDA source or kernel code, regenerate the
checked-in CUDA artifacts with `scripts/regen-cuda-artifacts.sh`.
