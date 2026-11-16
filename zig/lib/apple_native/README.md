# Apple bridge generation prewarming

The ABI remains version 2. Recorded audio, cancellation, and complete-output checks
retain their existing contracts.

Set `ANTFLY_APPLE_PREFIX_PREWARM=1` on the runtime process to opt in. It is off
by default because short dictations did not show a reliable latency win locally.

For stateless dictation prompts beginning with `Transcript:\n`, the bridge keeps
one unused `LanguageModelSession` containing only the exact system-instruction
prefix. After a successful response, it constructs a fresh session and calls
`prewarm(promptPrefix:)` with the shared `Transcript:\n` prefix. The completed
session is discarded, so user prompts and generated transcripts never become the
next dictation's history. Requests containing conversation history bypass this cache.

The cache accepts at most 16 KiB of instructions, expires after 60 seconds, and is
owned by a Swift actor. Prefix mismatches use a fresh session. Availability adds
`generation_prewarm` counters (`enabled`, `ready`, `hits`, and `preparations`); it exposes no
prompt contents and does not itself load or download models.

Build and verify on an Apple Silicon Mac:

```sh
make apple-bridge-generate
make apple-bridge-check
```

A synthetic benchmark exercises complete output, cross-request isolation, and
repeated latency without audio, network requests, or downloads:

```sh
python3 scripts/benchmark_apple_cleanup.py --library /path/to/libantfly-apple.dylib --prewarm
```

Run each bridge in a separate process. The first call is not a controlled
hardware-cold model load. Compare repeated timings and actual output preservation;
small synthetic differences are not evidence of an end-to-end speed guarantee.
An isolated local run with eight requests observed repeat medians of 0.322 seconds
for the published v0.3.0 bridge and 0.344 seconds with prefix prewarming, with seven
cache hits and the same preserved wording. Earlier runs varied; these short samples
show no reliable gain, which is why the feature is opt-in. Longer prompts and real
recordings need separate evaluation before enabling it by default.

The shorter common cleanup prompt preserves the existing output, correction,
formatting, and non-translation requirements. Public request/output limits and
natural-completion validation are unchanged.

App releases continue to bundle immutable released runtimes. These changes require
an Antfly release before updating the dictation app's runtime pin; do not replace a
pinned release's library with an untracked development binary.
