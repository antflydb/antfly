# Stored installed baseline preflight and failure evidence (draft v1)

Status: unapplied design patch. It does not enable a serving path or grant qualification authority.

## Observed defect

`CommittedContextStorageDiagnostic.owner_count` currently reports `globals.len + sliding.len` (36 for the Main3 packed session), while the external manifest sets `expected_owner_count` to the compacted global/profile count (3). The installed run therefore reserves the single-use held tuple and then deterministically rejects `36 != 3`. The static 58,228,736-byte prediction is derived from a formula and is not an authoritative tracker delta.

## Required contract

1. Rename the measured fields to `global_owner_count` and `total_owner_count`; never compare one to the other. The Main3 baseline is expected to expose global 3 and total 36.
2. Add a preflight-only branch to the existing installed test after the exact installed-model prefill and source fingerprint checks. It writes a fresh atomic `stored-installed-baseline-preflight.v1` receipt and exits before loading the stored manifest driver or calling `reserve_fn`.
3. The receipt binds build/model/runtime identity, capture input manifest, prompt digest and length, source descriptor/payload fingerprints, session identity, tracker owner identity/live/peak, tracker-derived physical bytes, replay-control bytes, global owners, and total owners.
4. A successor manifest must bind that receipt byte-for-byte. Its reservation carries exact tracker-derived baseline bytes, expected global owners, and expected total owners. Static prediction remains diagnostic evidence only.
5. `Request.run` remeasures and validates those three expected values before constructing the provider binding or calling `reserve_fn`. The coordinator rechecks the same measurement immediately before reserve. No mismatch may create a tuple claim or ledger file.
6. Add a versioned failure envelope. The installed caller catches every diagnostic error, writes the error name and current terminal receipt atomically, then returns the original error. A failure after reserve must include the durable abort or committed state. A pre-reserve failure must show reservation identity zero and no ledger evidence.
7. The public path remains disabled (`builtin.is_test` and the existing diagnostic environment gate). Numerical, memory, 25-percent, zero-swap, and no-serving gates are unchanged.

## Targeted tests

- A session with 3 global and 33 sliding owners reports global 3 and total 36.
- Exact measured baseline passes without reserving; byte, global-count, total-count, and nonzero replay-control mutations fail while the mock provider reserve count stays zero.
- A post-reserve injected failure writes `failed_before_commit` with durable nonreuse evidence.
- A postcommit injected failure writes `committed_but_receipt_failed`.
- Failure-envelope persistence failure returns a distinct receipt-persistence error while the durable ledger remains authoritative.
- Unknown future manifest or receipt schemas reject.

The patch is intentionally uncompiled pending the active timing diagnostic. A new strict build and a real preflight receipt are required before any new held tuple is generated or reserved.
