# Concurrent broker cohort through real API owner capsules

Base `923b738552`. Native macOS and pinned Nix/glibc GNU AArch64 pass all 12 tests in `antfly-inference-owner-handoff-test`, including a new two-caller concurrent fixture. Each caller starts its own real query capsule and owns its own thread-local checkpoint scope. Both submissions park before enrollment. Their two tickets share one physical broker callback and execution identity, with one inference CPU grant, two resource-blocked parents and three total operations observed during execution.

The callback successfully starts protected control work while the shared inference grant is active. Both callers receive their original scalar outputs. Each reacquires and retires its original query capsule; final operation, runnable CPU and retained byte counters are zero.

JSON files retain exact arguments and five source hashes. Full logs are compressed alongside them. GNU uses two CPUs and 3 GiB without extra swap. The initial OCI failure occurred before compilation because the read-only source did not have an empty cache mountpoint; creating that mountpoint resolved it. Failed and successful receipts are preserved.

This is real multi-caller owner/broker behavior, not a numerical model or open-loop fairness benchmark. Production Node/host installation is developed separately and is not compiled by this gate. Collective cancellation, original deadlines, CPU worker scopes and owned result disposal have separate receipts; broad scheduler/memory/backend coverage and default qualification remain required.
