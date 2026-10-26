# Proposed transaction-context correction to the qualification control

The currently approved control is `21205a7021`. A separate proposed control,
`8a61f42df84ffec2214b1d292e3cdac996cd36e8`, changes only
`examples/cloudflare-serverless/r2-coordinator-store.mjs` on top of that control.
It is prepared in `.worktrees/cloudflare-gate-control-transactions`; it has not
been installed on any live baseline.

The [one-file patch](../../examples/cloudflare-serverless/qualification/transaction-control-correctness.patch)
fixes the metadata transaction drain. Previously, every subsequent native group
ran under the first RPC's continuation, including after its result was delivered.
A real workerd reproduction against the exact approved source completes all
three operations but records native group contexts `a, a` instead of `a, b`.
Both the prepared control and the candidate record `a, b` in the same fixture.
The reproduction uses a deterministic native-admission stub inside real Durable
Objects, isolating scheduling from storage performance; it is not a live latency
comparison or proof that every observed lease error has this cause.

Each pending submitter now registers its own batch continuation. Admission
selects a waiting owner before recovery and keeps that owner first until its
callback is consumed. The next group resumes its selected caller instead of
continuing a long drain loop under the previous RPC. A failed owner callback
waits for the surviving group commit/rollback before receiving its rejection.
Its overlay is rolled back independently. The first owner therefore remains
pending through the group's native work. Unused admissions are closed when a
callback is coalesced or rejected, preserving the existing bounded pending set.

The proposed control retains the approved full and compact WASM files byte for
byte, all memory pools, transaction/group limits, checkpoint policy, offered
load and gate thresholds. It introduces none of the candidate's background
checkpoint, app-local observation, cache or data-plane changes. Its original
90 Node tests pass. The candidate's related implementation passes 217 Node and
13 persisted Worker checks, including the actual workerd context regression and
an interrupted commit with a failed owner callback. Hashes and results are in
[the reproduction evidence](../../examples/cloudflare-serverless/qualification/transaction-control-correctness-evidence.json).

This additional proposal needs a new control choice because the earlier approval
named `21205a7021`. Keep that control's sealed bundles and failed reports intact.
If selected, freeze `8a61f42df8` separately and use fresh paired corpora after any
measured workload; do not relabel failed reports or reset consumed cold epochs.
The gate has not passed, and all 300 reserved cold epochs remain unused.
