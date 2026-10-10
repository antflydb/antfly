# Cohort isolation with the production provider factory

Native macOS and pinned Nix/glibc GNU AArch64 pass all 12 owner test functions. The concurrent two-parent fixture now runs success, cancellation, and deadline-expiry cases. Both original query parents park before joining one physical batch. The callback observes exactly one inference CPU, two blocked parents and three operations, and starts protected control work. It then cancels the first parent or advances that parent's original native deadline clock.

The first caller receives Canceled or DeadlineExceeded at original-parent reacquisition. The second caller retains its correct output and completes normally. Their execution identities still match, proving they shared a physical invocation. Every operation, runnable slot and retained byte counter drains in all three cases. The results are scalars; allocated output disposal is qualified separately by broker publication and caller-parking receipts.

The provider source is the exact generic factory used by the pending production Node installation: ProviderFor accepts the existing checkpoint module and runtime ABI so no duplicate TLS module is introduced. This gate instantiates that factory with the real API owner's shared control module. JSON records pin five source hashes and exact arguments; compressed full logs are retained. GNU uses the pinned Nix/glibc image, two CPUs and 3 GiB, without extra swap.

This gate does not compile or qualify production host installation, numerical inference, open-loop fairness, physical worker waits, full retained-memory coverage or enabled defaults. The separate production installation integration remains required.
