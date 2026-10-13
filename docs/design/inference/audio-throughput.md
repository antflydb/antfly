# Gemma4 and CLAP audio throughput

Dated decisions and rejected trials: [media throughput history](history/gemma4/media-throughput-2026-10.md#audio-preparation-and-embedding-batches).

Audio follow-up to [Gemma4 vision throughput](gemma4-vision-throughput.md), in the
same change. Audio measurements compare against `6017034d7a`, which already
includes the earlier GPU media residency work. [Machine-readable results](audio-throughput.json)
retain samples, parity and transfer observations.

## Execution

Gemma4 audio-only accelerated calls decode each clip once, downmix/resample on
CPU, and prepare the existing Hann-window frames, DFT basis, filterbank and
validity mask. Spectral features, subsampling, conformer intermediates, compact
projected rows, embedding lookup, prompt packing, encoder and pooling stay on
the accelerator where the backend has the required operations. Only final
normalized vectors are exported in the qualified Metal path.

The conformer executes independently for each clip. Its relative-position local
attention and masks require their existing specialized kernels; the unmasked
vision Flash kernel does not apply. This change physically batches the
**downstream embedding encoder**, rather than claiming batched conformer work.

Each preparation wave holds at most four clips. A small length sort chooses
encoder cohorts with at most 25% padding, restores original indexes and applies
an audio prompt limit of 1,024 tokens, including four framing tokens. The
existing 30-second input limit remains strict. This admits long audio exceeding
the image path's 512-token limit without cropping it to image capacity.

CLAP keeps its existing native encoder batches and fixed feature geometry. Waves
hold at most eight clips, decreasing when working-set policy requires it.
Feature preparation preserves singleton fusion channels, `is_longer`, valid-row
indexes and the fallback for graphs that collapse a batch. This avoids replacing
an existing eight-row native forward with smaller forwards. The prepared feature
buffer feeds the existing resident CLAP encoder and projection path.

Both pipelines prepare at most one future CPU wave while the current GPU wave
runs. Gemma4 CPU preparation uses a 64 MiB bounded allocator; its admitted current
GPU phase can use the existing 512 MiB host workspace. CLAP uses the configured
audio decode working budget. Speculation holds a host-only admission lease and
never reserves device scratch. Current admission may drain and discard
speculation, then retry; a future pressure failure retries once after the
current wave is released. Allocation, codec and control failures are distinct.
All exits join producers before releasing caller-owned inputs, allocators or
controls. CPU codec and feature operations are checked between bounded phases;
this does not make a codec call instantly interruptible.

The active call holds the model execution gate before preparation. Waiting calls
do not accumulate CPU preprocessing leases. Encoded audio and mono/interleaved
PCM entry points share the accelerated path; CPU/ONNX retain compatibility.

Audio-only Gemma4 groups can coalesce across requests through the existing
broker. Generation, task prefix, output dimension, source modality and options
remain exact. Unknown encoded lengths conservatively reserve 1,024 tokens per
item, at most four items and 4,096 tokens; length qualification happens after
bounded decode. Titled and mixed prompts keep their compatibility executor.
The broker retains caller indexes, per-item codec failures and live-peer
cancellation under the existing hard interruption boundary.

## Qualification

Apple M4 Max, 36 GiB, Metal, real HF `google/embeddinggemma-2` weights and the
native `antflydb/clipclap` bundle. Alternate distinct 0.4-second and 0.5-second
mono 16 kHz WAV clips, two warmups, five synchronized measured iterations.
Timing includes encoded decode through final vectors, excluding file reads and
model loading. Baseline and candidate run consecutively for each case.

| Model / clips | Baseline p50 | Candidate p50 | Speedup |
| --- | ---: | ---: | ---: |
| Gemma4 / 1 | 0.1523 s | 0.1527 s | 1.00× |
| Gemma4 / 4 | 0.6276 s | 0.5831 s | 1.08× |
| Gemma4 / 8 | 1.2679 s | 1.1677 s | 1.09× |
| Gemma4 / 16 | 2.5327 s | 2.3404 s | 1.08× |
| CLAP / 1 | 0.2034 s | 0.2080 s | 0.98× |
| CLAP / 4 | 0.3964 s | 0.3953 s | 1.00× |
| CLAP / 8 | 0.6293 s | 0.6194 s | 1.02× |
| CLAP / 16 | 1.0184 s | 0.8998 s | 1.13× |

Gemma4 minimum final-vector cosine is 0.999999999999 across these cases; CLAP
outputs match exactly. Eight Gemma4 rows download 24,576 bytes in eight final
vector readbacks, with no intermediate activation download. Transfer counters
exclude model loading and do not measure total device memory.

CPU compilation ran during these local measurements. An earlier unpaired run
showed much larger apparent gains caused by host-load drift and is excluded.
These results do not establish a universal gain, saturated serving throughput,
CUDA performance, or a speedup for GPU audio decoding. CUDA still uses existing
host compatibility for conformer operations lacking CUDA kernels.

Additional qualification covers mixed 0.4/0.5/2/25-second clips, 48 kHz stereo,
PCM versus encoded input, and lookahead on/off. Mixed Gemma4 vectors retain
cosine above 0.999999999999; CLAP matches exactly. PCM and lookahead switches
match exactly. Malformed codecs and the 31-second Gemma4 duration failure
preserve valid neighboring rows and original error indexes. MP3, AAC and Ogg
clips also match baseline final vectors (Gemma4 cosine above 0.999999999999;
CLAP exact). Focused tests passed 166 cases with 14 optional fixture skips;
42 producer/broker checks and two long-packing/collapsed-output checks passed.

Production HTTP validation passed 39 requests: arrays of 1/4/8/16, concurrent
callers 2/4/8, per-item codec errors and grouped Gemma4 128-dimensional output.
Scratch policy remained at its default. Weighted request admission was set to
512 units for this concurrency exercise; lower/default capacity produced the
expected retryable 503 responses before executor work. This is correctness and
broker qualification, not a saturated serving throughput measurement.

## Reproduction and controls

Build `zig build build-audio-embedding-bench -Doptimize=ReleaseFast`. Run
`antfly-audio-embedding-bench --model MODEL --audio CLIP --audio OTHER --batch 16
--warmups 2 --iterations 5 --output result.json`. Final vectors remain in JSON
for row-by-row parity. `--prepare-ahead off` isolates lookahead; `--batching off`
selects the legacy Gemma4 path or legacy CLAP preparation, retaining CLAP's native
batching. `--scope pcm` excludes codec decode from warm timing and exercises raw
PCM. `--allow-item-errors on` records stable indexed failures alongside vectors.

Pipeline options are `gemma4_audio_batching`, `audio_prepared_batches` and
`audio_prepare_ahead`. `ANTFLY_GEMMA4_DISABLE_AUDIO_BROKER=1` disables server
coalescing; `ANTFLY_GEMMA4_AUDIO_BROKER_TRACE=1` records actual cohort execution.
