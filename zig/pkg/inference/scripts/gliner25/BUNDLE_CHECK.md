# Strict CPU/Metal converted-bundle evidence

The completed version-2 diagnostics passed **100/100 same-bundle CPU/Metal
comparisons**: ten curated requests for each of nine reduced storage profiles
and the small FP32 bundle. All comparisons used exact ordered decisions and
the unchanged `5e-4` absolute confidence tolerance. The largest aligned
confidence difference was `0.000006079674`.

This establishes the tested bundles' implementation parity on those requests.
It does not establish held-out task quality, performance, operational readiness,
or general availability. These are the same development requests already used
for CPU/Python qualification; they are not a new held-out corpus.

| Model / storage | Native vs published FP32: exact ordered decisions / 10 | Native vs published FP32: also within confidence tolerance / 10 | Metal vs same-bundle native parity / 10 | Maximum aligned CPU/Metal confidence difference |
| --- | ---: | ---: | ---: | ---: |
| Small FP32 | 10 | 10 | 10 | 0.000001728535 |
| Small FP16 encoder | 10 | 9 | 10 | 0.000003397465 |
| Small Q8_0 | 10 | 6 | 10 | 0.000000536442 |
| Small Q4_0 | 7 | 2 | 10 | 0.000001847744 |
| Base FP16 encoder | 10 | 9 | 10 | 0.000002384186 |
| Base Q8_0 | 10 | 4 | 10 | 0.000001370907 |
| Base Q4_K | 7 | 3 | 10 | 0.000002324581 |
| Multilingual FP16 encoder | 9 | 9 | 10 | 0.000005960464 |
| Multilingual Q8_0 | 10 | 3 | 10 | 0.000006079674 |
| Multilingual Q4_K | 7 | 1 | 10 | 0.000005304813 |

The source-FP32 counts are quality diagnostics for changed storage. CPU/Metal
parity compares the exact same converted bytes, so it remains successful when
both backends make the same different decision relative to FP32. Reduced
precision still changes some relation/attribute/record selections or ordering.
Do not use CPU/Metal agreement as evidence that those changes are acceptable.

The runner declares `strict_f32_activations_v1`: only permitted encoder weights
use reduced storage, while activations, accumulation and protected heads stay
FP32. Earlier version-1 reports used a native reduced-linear path that could
also quantize activations. They are rejected by the current driver as strict
native references. In particular, the new strict base and multilingual Q8_0
runs each match all ten source-FP32 ordered decisions; the older arithmetic
matched nine. This is a measured result on this corpus, not a general quality
improvement claim.

Both new reports used the same ReleaseFast executable SHA-256:

```text
6abf00a5a2baf74038d3803df75d2a0f9999cf117af4013fe761dfcf24438166
```

The comparison driver SHA-256 was:

```text
4c58f89ea38065223696752743aa5db89d4fa6f430fe21765e0d729c21dc8b74
```

The native report is `/private/tmp/gliner25-bundle-native-strict-v3/report.json`:

```text
7ce959984c0bc87534736068bca7adeaa6162a4c6ecf5df932da3f48b4d0ceac
```

The Metal report is `/private/tmp/gliner25-bundle-metal-strict-v3/report.json`:

```text
a2e87cce22bc0b051a45a28f2ff54d007c160058657ab9409fde2f28a4c9d198
```

The reports are local run artifacts; this document preserves their identities
and measured summary. Each report binds the complete conversion receipt,
source/output file hashes, model and precision, fixture hash, exact token IDs,
math policy and every result. Metal additionally binds the native report and
compares record confidence/anchors, scalar/list presentation, attribute modes,
typed/derived edges and solver status/exhaustion. Solver utilities and visited
node counts are retained separately. All cases completed; no failures or skips
were removed from the denominator.

Workers executed serially with the diagnostic driver's one-thread environment
and sampled process-RSS guard. These runs contain no fair timing measurement,
and process RSS is not a complete GPU-memory accounting. They do not cover
long documents, concurrent service requests, sustained load, all language or
domain slices, or fine-tuned checkpoints. Dataset-specific held-out gates are
described in [held-out evaluation](../../../../../work-log/completed/gliner2.5.md#held-out-evaluation).

The [work log](../../../../../work-log/completed/gliner2.5.md#artifacts-and-precision)
summarizes bundle integrity and precision policy. A fresh run is
required after a model, conversion policy, runtime math, tokenizer, schema,
decoder, fixture or checker contract change.
