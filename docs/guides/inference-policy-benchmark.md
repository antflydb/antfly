# Compare OCR budgets and CPU architectures

Grepstein's revision-pinned LightOnOCR profile already requests 8192 output
tokens on the first call. A larger deployment ceiling does not raise a smaller
producer request. Short completions still stop early; larger ceilings increase
KV/work admission and can affect concurrency. Do not change the runtime default
for every `/read` consumer based on one corpus.

`zig/tools/benchmark_inference_policies.py` compares identical fixtures at 4096
and 8192 tokens, including the structured truncation contract and the direct
`OUTPUT_TRUNCATED` error. It also compares native CPU embeddings and audio reads
across independently selected endpoints. It never changes source tables,
producer definitions, repair jobs, or extraction checkpoints.

## Prepare a bounded campaign

Use a private directory containing a manifest and local fixtures:

```json
[
  {
    "id": "dense-table-page",
    "task": "read",
    "kind": "image",
    "fixture": "dense-page.png",
    "model": "lightonai/LightOnOCR-3-0.8B:safetensors@36d8636925327c671bd8e479c517a004f7566bd9",
    "expected": ["a verified table value", "the verified last visible line"]
  },
  {
    "id": "speech-sample",
    "task": "read",
    "kind": "audio",
    "fixture": "speech.wav",
    "model": "openai/whisper-small:safetensors@973afd24965f72e36ca33b3055d56a652f456b4d",
    "expected": ["verified speech near the beginning", "verified speech near the end"]
  },
  {
    "id": "embedding-text",
    "task": "embed",
    "fixture": "embedding.txt",
    "model": "google/embeddinggemma-2:safetensors@914f7f89142e33e77833254d9c9b90c3cef7303b",
    "dimensions": 768
  }
]
```

Replace the example fragments with checked ground truth; do not use the
placeholders to qualify a model. Include short scans, dense tables, and difficult
pages, not just one convenient sample. Render each PDF page once using the
production render/sampling settings; send those same PNG bytes to both policies
and architectures. Audio fixtures should be bounded PCM WAV samples. The tool
caps each fixture at 8 MiB. Audio decoding options can be supplied in `options`;
the automatic 4096/8192 comparison applies only to image OCR.

Verify the actual running image ID, architecture, cgroup CPU/memory requests,
thread limits, BLAS selection, and immutable model publication before running.
Pass the verified multi-architecture OCI index as `--runtime-image`; this is
operator-supplied provenance, not remote attestation by the benchmark.
Use isolated trial pools or an idle GPU; record competing work and throttling.
GKE Arm hardware is different from the DGX Spark, so do not substitute its results.

Port-forward the isolated pool services. Each endpoint must include `/ai/v1`.
For an authenticated endpoint set `ANTFLY_INFERENCE_TOKEN` securely; HTTPS is
required except for local port-forwards. Redirects are rejected.

```sh
python3 zig/tools/benchmark_inference_policies.py \
  --cases /private/path/cases.json --report /private/path/comparison.json \
  --endpoint x86=http://127.0.0.1:18091/ai/v1 \
  --endpoint arm=http://127.0.0.1:18092/ai/v1 \
  --backend x86=native --backend arm=native \
  --reference-endpoint x86 \
  --hourly-usd x86=0.3438641 --hourly-usd arm=0.218246 \
  --runtime-image ghcr.io/antflydb/antfly@sha256:d0f703b4f4b0333115d5f489ab1b548559f6e3ee2b2dc5757c50ec48b11e1407 \
  --trials 3 --timeout 600
```

The illustrative hourly prices assume 4.25 vCPU/17 GiB after sidecar/Autopilot
adjustments, us-central1 Scale-Out on-demand CPU/RAM list prices. Read actual pod
requests after scheduling and adjust the prices. Storage, network, discounts,
and idle time are additional; do not call this total billing cost.
Compare GPU budget policies in a separate report with the same 4B model used by
Grepstein, a CUDA backend expectation, and the complete GPU node hourly price.
Do not compare different model sizes as an architecture speedup.

## Interpret and resume

Each execution epoch warms the pending endpoint/policies again, including after
resume. Warmup includes any lazy load and is excluded from warm throughput. The
runner checks the loaded pinned model's instance ID and backend before and after
every timed call. An idle eviction triggers another untimed warmup; an instance
change during execution invalidates that sample. The isolated CPU trials disable
idle expiry while retaining the one-model bound. OS/GCS caches are uncontrolled;
these are warm-model measurements, not clean cold-cache measurements. Endpoint
and policy order alternate between trials. Metadata probes and journal commits
are outside the timed inference call; the request duration includes transport.
`--response-grace` allows bounded timeout cleanup without extending the inference
execution deadline.

Embedding cases require an explicit trusted `--reference-endpoint`. Its first
valid warmup establishes the reference for that fixture/model. Every embedding,
including later baseline calls, must be finite, have the expected dimensions and
unit norm, and match that reference. Defaults require cosine >= 0.9999, maximum
absolute error <= 0.001, and RMSE <= 0.0001. The explicit
`--embedding-min-cosine`, `--embedding-max-abs-error`, and `--embedding-max-rmse`
options are recorded in campaign identity. Tolerances and baseline cannot change
on resume. A unit vector with unrelated contents fails. These checks establish
reference equivalence, not the semantic quality of an untrusted baseline.

Reports retain durations, fixture hashes, normalized output hashes, numeric usage,
completion/truncation, failures, and cost per completed result. They omit response
text and expected fragments. Failed attempts are charged to measured work; groups
with no completed results have no cost-per-completion estimate. A 4096-token
truncation is an expected comparison result; an 8192-token truncation or any
other failure makes the command fail. Inspect truncated full-budget pages instead
of counting them as successes. Expected fragments and EOS do not prove full
transcription accuracy; review the output separately against the source page.

`policy_comparison` pairs only the same endpoint, fixture, trial, execution epoch
and resident model instance. It estimates
the old policy's elapsed time as the 4096-token call plus the 8192-token call
when retry is necessary. This reuses the measured high-budget call; it is not
a separately executed retry campaign or proof of an exact production speedup.
Report per-page distributions and completeness, not just a corpus average.
An interrupted or evicted pair is excluded rather than silently pairing different
worker incarnations. Coverage includes planned/matched policy pairs, and
`policy_comparison_complete` and warnings expose missing pairs even when all
individual timed measurements pass (`measurement_success: true`). The command
succeeds only when both the measurements and requested paired comparisons pass;
it does not return success for incomplete policy coverage. Start a new campaign when a complete paired
policy estimate is needed; recorded timed calls are never silently repeated.

Use the identical command with `--resume` after interruption. Recorded timed
attempts are not executed again; only pending work receives fresh warmup. Changed
fixtures, cases, endpoints, prices, code, runtime identity, tolerances or trial
counts reject resume. The complete campaign is hashed without retaining fixture
payloads; only one fixture is encoded at a time. Failures remain in the record;
start a new campaign after fixing them. This benchmark does not rerun corpus OCR.

The private `<report>.state/` directory is the authoritative checkpoint. An
exclusive OS lock rejects simultaneous writers before report replay or inference.
Each measured request and warmup is appended to a checksummed, fsynced journal;
resume discards only an unterminated tail and rejects committed corruption or
duplicate measurements. A failed journal commit stops further checkpoint writes
until the campaign is reopened for recovery; storage errors are never classified
as model-residency failures, including during rewarming. The JSON report is an atomic projection at normal exit
or handled interruption, not a per-request rewrite. During execution use stdout
for progress. Keep the state directory with the report for resume; copying only
the report is insufficient. Reports from the earlier journal-less format are
rejected without overwriting them; use a new campaign path.

Reference vectors are sensitive derived data. They are stored in separate private,
checksummed files and loaded one case at a time, never embedded in the report or
journal. State directories use mode 0700; state files, references and report use
0600. Public report rows contain only hashes, numeric comparisons, usage and fixed
error codes. This tool uses POSIX advisory locks and supports macOS/Linux. A
process crash between inference completion and journal commit can repeat that
unrecorded request; the journal provides durable recorded-attempt semantics, not
exactly-once remote execution.

For corpus recovery, inspect native artifact configuration and repair issues,
then submit a bounded native repair/reprocess job for the affected documents.
Retain completed page checkpoints. Do not delete tables, recreate producers, or
blanket-reprocess successful documents to measure a token-policy change. Changing
a producer definition can invalidate its extraction identity; review that impact
before changing any live configuration. Grepstein's `qualify` command verifies
corpus coverage separately from this inference benchmark.
