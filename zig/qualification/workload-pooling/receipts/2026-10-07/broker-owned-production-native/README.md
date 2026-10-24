# Production ownership conversions: native archive and worker probes

Immutable source base `697ca0826c` passes all 26 steps of combined `linked-inference-abi-integration-test` and `linked-inference-worker-integration-test`. The production inference archive compiled in six minutes with a measured 9 GiB peak. Exact arguments and six source hashes are retained in `native.json`; full output is compressed alongside it.

This qualifies compilation of reachable production owning-output conversions and the existing exported C/private worker handoff probes. Their requests test contract plumbing and failure ownership, not numerical model outputs. The model-free embedding fixture conversion is not exercised by these probes.

The later owning broker checkpoint and caller parking changes (`109accbb73` and its successor) were in another worktree and are not qualified by this build. Production owner installation, GNU production compilation, numerical/multi-caller tests, complete memory/worker wait contracts and default overload qualification remain required.
