# Projected DeBERTa training attention oracle

The additive source capture for the dedicated replay-tiled training attention
path passed all nine cases. Each case checks the unmodified source forward
against an exact projection-leaf replay and records all five autograd VJPs.
Source inspection, seven pure counter/contract tests and seven supervisor
ownership tests also pass. The native CPU and dedicated Metal consumers now
pass all nine cases at the original tolerance. The [execution ledger](../../testdata/gliner25/training_attention_execution_v1/README.md)
separately records tiny full/head replay training and exact partial resume.
Published-model/full-context training and activation recomputation remain
open; existing defaults remain unchanged.

The [generator](capture_training_attention.py) and
[versioned contract](training_attention_contract_v1.json) use Transformers
4.55.4, Torch 2.9.1 and the complete existing oracle dependency lock. The actual
`modeling_deberta_v2.py` is 56,783 bytes, SHA-256
`98fa398f62e446e1f6303ff67fa7aceddac4f746a1a6013226896c3fa4e6cdd6`.
That identity also matches the accepted full-encoder training fixture. The
contract pins its source methods separately and binds Fastino commit
`3c913c7369301133d3b7699252074c4303ada50e`, the boundary model and encoder-loader
source files, and the frozen oracle helper. Preflight verifies the clean source
checkout, exact installed file and dependency metadata without importing Torch.
The numerical entry also checks the source path of the imported encoder class.

## Source behavior that the tiled path must preserve

`DebertaV2Encoder.get_attention_mask` (lines 609–616) expands a `[B,S]` mask
into the product of query and key validity. Attention lines 263–269 replace
invalid scores with finite `torch.finfo(float32).min`, apply softmax, then
probability dropout. A fully masked query therefore has uniform probabilities
over **all S keys** before dropout. Mask replacement blocks its score gradient,
but its output cotangent can produce nonzero value gradients, including at
masked key positions. A key-only mask, negative infinity, or zeroed empty
softmax row would change training semantics.

Both relative attention modes are enabled in all three published configurations.
The scale is `sqrt(3*head_dim)`. Source evaluates content attention as
`Q @ (K.transpose / scale)`; each relative term is divided after its dot product
and gather. The terms are then added in source order. This arithmetic order is
recorded independently of the mathematically equivalent combined score formula.

For equal-length self-attention, let `r(q,k)=bucket(q-k)`. The content-to-position
term reads `Kr[r(q,k)+256]`. The position-to-content implementation gathers
with a negative relative position and transposes the result; the effective
pair `(q,k)` also reads `Qr[r(q,k)+256]`. It does not read `bucket(k-q)`.
The oracle calls the actual source bucket builder and records the complete
one-dimensional lookup for `-(S-1)..S-1`; it does not compute logarithmic buckets
using a copied formula. Unequal query/key lengths are outside this profile.

`get_rel_embedding` (lines 603–607) normalizes the complete shared table first.
Each layer then applies relative-position dropout to that full table, projects
it with the shared query/key Linear modules, and gathers score columns. The
published table has 512 rows from 256 buckets and a maximum relative position
of 512. Gathering rows before dropout or projection would change both the
dropout counter and parameter-gradient contributions.

Fastino's boundary constructor calls the shared encoder loader. Its standard
path uses Transformers and falls back from an unsupported `sdpa` request to
`eager`; FlashDeBERTa is optional and is disabled by the frozen oracle runtime.
This capture directly invokes the verified eager source methods and loads no
checkpoint, tokenizer, training corpus or adapter.

## Capture boundary

A tiny source encoder uses H8, two heads, D4 and one layer, with the full 512-row
relative table. It provides actual initialized Linear projections and the
source relative LayerNorm/dropout ordering. Each case has two passes:

1. Execute the original attention methods and original learned Linear forwards
   with explicit counter masks at the two dropout sites. Capture Q, K, V and
   the projected full Qr/Kr tables.
2. Execute the same attention methods and masks, substituting only those exact
   projection returns as five independent autograd leaves. Context and
   post-dropout probabilities must equal the first pass byte for byte. Obtain
   all five VJPs with actual Torch autograd.

Source table repetition across batch remains inside the attention method, so
the Qr/Kr leaf gradients sum contributions from both samples. This projected
primitive fixture does not independently qualify gradients through the raw
relative embedding, LayerNorm or shared projection weights.

| Geometry | B | S | Valid lengths | Output cotangent | Probabilities |
| --- | --- | --- | --- | --- | --- |
| Ragged | 2 | 7 | 7, 4 | All rows, including padded queries | 0, f32(0.1), 0.125 |
| Wholly masked sample | 2 | 7 | 7, 0 | Only the fully masked sample | 0, f32(0.1), 0.125 |
| Published bucket boundaries | 2 | 512 | 512, 259 | All rows | 0, f32(0.1), 0.125 |

The nine cases include relative distances ±127, ±128, ±129 and ±511. The wholly
masked cases require exact zero Q/K/Qr/Kr VJPs and a nonzero V VJP. This avoids
the earlier full-encoder fixture's zero padded-query cotangents hiding the
empty-row behavior. All captured floating tensors must be finite.

Native inputs are QKV `[3*B*S,H]`, relative `[2*R,H]` ordered Qr then Kr, and
physical-i32 control `[6+B*S+(2*S-1)]`. Control starts with low/high bit limbs
for seed, microbatch and replica, then the `[B,S]` mask, then source bucket IDs.
The extra backward input is dOut `[B*S,H]`; expected gradients are concatenated
as `[dQ;dK;dV;dQr;dKr]`, with `3*B*S+2*R` rows. Individual VJPs, before/after
dropout probabilities and masks are also retained for diagnosis.

## Dropout replay and limits

The source initialization uses `torch.manual_seed(251019)`. Dropout uses a
separate, explicit native counter override and makes no claim about reproducing
Torch RNG. The contract records every stream ID and the exact formula:

- Wrapping SplitMix64 mixes seed, microbatch, replica plus
  `0x7265706c696361`, and the stream ID.
- Logical-layer streams are `(layer<<32)|3` for probabilities and
  `(layer<<32)|2` for relative positions. The three probability variants use
  logical layers 0, 2 and 7 to exercise both stream-ID limbs.
- The probability index is `((b*heads+h)*S+q)*S+k`, independent of tile shape.
- Drop when the high 32 counter bits are below
  `floor(f64(f32(p))*2^32)`. Keep scaling is `f32(1/f32(1-f32(p)))`.

Seed, microbatch and replica include high nonzero bits; no counter limb passes
through floating-point storage. The contract tests match all 3,672 saved
probability-mask values from the prior actual Transformers encoder fixture,
including traversal in uneven tiles. They also check f32 threshold rounding,
counter/stream identity, physical-i32 packing and malformed admission.

The fixed ceilings are 2 GiB sampled child-tree RSS, 120 seconds cooperative
capture time, 180 seconds in an outer process supervisor, 4 MiB per output
stream, 56 MiB tensor payload, 2 MiB metadata and 64 MiB total artifact files.
Both Torch and inter-op threads are one. The outer deadline can interrupt a
blocked Torch call. Its bounded TERM/KILL/reap phase follows that running
deadline. RSS and artifact sizes are sampled every 50 ms; these are supervised
limits, rather than operating-system reservations. Output requires fresh
capture and evidence directories. Partial artifacts and process logs remain
evidence if any assertion or guard fails.

Model-free preflight:

```sh
/private/tmp/antfly-gliner25-oracle-venv/bin/python -B zig/pkg/inference/scripts/gliner25/capture_training_attention.py --preflight-only
python3 -B -m unittest discover -s zig/pkg/inference/scripts/gliner25 -p test_training_attention.py -v
/private/tmp/antfly-gliner25-oracle-venv/bin/python -B zig/pkg/inference/scripts/gliner25/supervise_training_attention.py --preflight-only
python3 -B -m unittest discover -s zig/pkg/inference/scripts/gliner25 -p test_training_attention_supervisor.py -v
```

The [supervised entry point](supervise_training_attention.py) reuses the
unchanged published process supervisor, SHA-256
`a937237975be2ed879f62afd285494ccb51a7c1d7cc1dff7160621fb26b87332`.
Before launch it verifies the generator, contract, pure-test source, oracle
manifest, prior preflight/test receipts, selected Python executable and virtual
environment. It stages the exact helper bytes in a private evidence owner and
preserves the virtualenv invocation path. The wrapper adds a descriptor-based
artifact-size guard; cleanup disables that guard so an artifact violation
cannot prevent reaping. SIGINT and SIGTERM initiate the same cleanup. After
completion, both source and private helper bytes are checked again.

Run only after the root grants the serial numerical lane. The controlled-child
RSS inspector needs the same sandbox escalation used by the published
supervisor; no network or model download occurs:

```sh
/private/tmp/antfly-gliner25-oracle-venv/bin/python -B zig/pkg/inference/scripts/gliner25/supervise_training_attention.py --output-dir /private/tmp/gliner25-training-attention-v1 --evidence-dir /private/tmp/gliner25-training-attention-probe-v1
```

Expected artifact names are `weights.safetensors`, `tensors.safetensors` and
`capture.json`. The separate evidence owner retains `start.json`,
`process.json`, both output streams and the copied helper closure. There is
one launch and no automatic retry. No fixture is enrolled in the numerical reference manifest
until actual capture, reproducibility and native consumption are separately
verified.

## First supervised capture

The exact frozen wrapper completed one child in 5.953 seconds, with sampled
peak child RSS of 685,047,808 bytes and 40,291,527 artifact bytes. Exit status
was zero; the direct child was reaped and no tracked process remained.
The receipt is `/private/tmp/gliner25-training-attention-probe-v1/process.json`,
5,481 bytes, SHA-256
`7f07386811d18a15b8ffb7d33d369d5366c2a593efccd97f951c5ab2581d7991`.
The capture remains at `/private/tmp/gliner25-training-attention-v1`:

| File | Bytes | SHA-256 |
|---|---:|---|
| `capture.json` | 29,667 | `6a536daec41bfd515d7e3fe5ae2522a4fdd2e67c15880071b732783f8cb59dc1` |
| `tensors.safetensors` | 40,241,300 | `45c7b042146132e9103807f61e49b7a44a7ef59e70c338e542e9de709d38b9e3` |
| `weights.safetensors` | 20,560 | `715aa526fb9cc711352b2b57531d8648e76cba29972c1c04ecad720044d4fa54` |

The [Zig source consumer](../../src/ops/deberta_training_attention_source_test.zig)
reads this external fixture only when
`ANTFLY_GLINER25_TRAINING_ATTENTION_FIXTURE_DIR` is set. It validates the exact
file/source/runtime pins, all 153 reference and 19 weight tensor headers,
every counter-mask bit, and physical high-bit control limbs. The CPU and
dedicated Metal paths compare context and the five Q/K/V/Qr/Kr gradient
slices at the predeclared `2e-5 + 3e-5*abs(expected)` bound. The Metal variant
requires strict typed uploads and checks no activation readbacks before the
final diagnostic comparisons. A missing device operation fails explicitly;
it cannot invoke a generic host fallback. CPU v8 passed 18 of 22 selected tests
with four expected GPU skips. Metal v1 stopped at an alignment type error;
no attention test ran. After the aligned control-slice correction, Metal v2
passed all 23 selected tests without skips or reported leaks, including both
nine-case consumers, the device ownership/rejection tests and tiny full/head
CPU/Metal replay jobs with exact partial resume. The ledger retains both
successful logs, the failed compiler log, immutable capture pins and the
actual observed Metal v2 executable identity. S4096/S16384 admission tests are
structural only; no long-context numerical or performance claim follows from
them. Source-capture reproducibility and reference-manifest enrollment remain
separate from this execution record.

A subsequent seven-test Metal checkpoint also passes 18 CPU–Metal cases at
head widths 64, 128 and 256, sequence lengths one, 17 and 65, R512 and dropout
zero/0.1, including ragged and fully masked inputs. Context and all five VJPs
retain the same tolerance while CPU tiles differ from the Metal key tile.
These extend device geometry coverage against the independently tested CPU
primitive, without adding an upstream source fixture or a published-model
gradient claim. The ledger retains the exact log and scoped source snapshots;
the attempted live observer missed that short execution, so its executable
identity is explicitly unrecorded.
