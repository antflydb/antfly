# Regional training checkpoint ledger

`manifest.json` records local regional-training checkpoints. Raw logs are copied
verbatim and pinned by byte count and SHA-256. Failed compile and unsupported
fixture checkpoints are retained beside their successful follow-ups.

Historical core v3 and integration v2 do not have a preserved complete source
snapshot or an exact test executable receipt. Their source state must not be
inferred from the current dirty workspace or a later build-cache entry. The
integration v3 snapshot now preserves 30 named source files captured under the
root agent's freeze. It is a partial relevant-source archive, not the complete
build dependency closure. Integration v3 passed 61 of 63 selected tests with
two expected Metal skips. It includes eight tiny regional source profiles and
four retained controlled-dropout regressions. No executable identity was
recorded. The later 6 GiB aggregate/512 MiB caller defaults and resource-wire
mapping are outside that frozen source receipt.

Integration Metal v1 passed 43 selected tests with no skips or leaks. It covers
eight regional Step source profiles on each backend, full/head-only managed
resume on CPU and Metal, and all eight inactive-adapter NativeTrainer source
profiles per backend with native-owned updates and durable resume. GPU Step
comparisons alone do not prove optimizer updates. Numeric resource-wire and
job mapping checks also passed. The `/75` progress denominator is not the
selected test count.

The later receipt preserves the exact live-observed test executable identity,
raw process and stdout/stderr receipts, and an unchanged 2,544-file source
inventory. The repository includes 38 matching relevant source copies. The
complete recorded selection and executable are separately archived under
`/private/tmp/antfly-gliner25-recomputed-integration-metal-v1-archive` with a
pinned receipt. Neither source selection is a complete dependency closure.
The failed pre-test CPU wrapper attempt is preserved separately; its original
wrapper was reconstructed to its recorded SHA-256, not replaced by the later
successful wrapper.

This ledger stays outside the upstream numerical `reference_manifest.json`.
It does not qualify published regional jobs, long-context performance or
production rollout. Public runtime availability remains false.
See [the implementation contract](../../../../../../docs/GLINER25_RECOMPUTED_TRAINING.md).
