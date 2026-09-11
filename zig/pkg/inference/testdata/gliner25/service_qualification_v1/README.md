# Local service qualification receipts

`manifest.json` binds the original-small FP32 source fixture, all five model
pins, five test sources, declared resource profiles and five local checkpoint
logs. `socket_v2` contains the passing actual managed-Metal HTTP case within a
failed aggregate; `concurrency_v3` separately passes the corrected competing
request admission case. Preserve the earlier ABRT and executor-capacity failure
as historical receipts. `queued_cancellation_v2` separately passes actual
managed-Metal request cancellation while the request waits for the existing
execution mutex, including resource release before unlock and same-session
retry. Its preceding unrelated interpreter compile failure is retained; no
interruption inside a learned forward or kernel is claimed.

This is service evidence, not an upstream numerical oracle or a release receipt.
It is intentionally outside `reference_manifest.json`. Public runtime
availability remains false. Only named test sources are hashed; the ledger is
not a complete dirty-workspace snapshot. The quiet concurrency v3 and queued
cancellation v2 logs do not record executable paths, so no cache identity is
inferred. Commands and their provenance
are recorded explicitly in the manifest.

Raw logs are copied verbatim and their digests/lengths verified. The model was
not loaded or executed while producing this ledger. Further changed-source
qualification should create another versioned receipt instead of rewriting
these historical logs. See the scoped results and remaining gaps in
[`GLINER25_OPERATIONS.md`](../../../../../../docs/GLINER25_OPERATIONS.md).
