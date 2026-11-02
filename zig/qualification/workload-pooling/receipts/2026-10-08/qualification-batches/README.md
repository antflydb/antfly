# Serial pooling compiler batches

The old owned-cleanup GNU build terminated its monolithic pooling test compiler
with signal KILL in a 22 GiB container. The full graph was not green: 89/95 steps
succeeded, one failed. Its terminal manifest/log are preserved. This is compiler
failure evidence; the signal alone does not establish OOM or a runtime failure.
Dependent qualification dispatchers then exited without launching compilers.

The pooling gate keeps all 31 existing filters and divides compiler inputs into
four serial batches of 8/8/8/7. Each compile depends on the preceding compile.
Combined inventories precede the existing owner-selection audit; test runs stay
serial. Overlapping filters give each named test to its first matching batch,
using prior filters as skips. This prevents duplicate owners while preserving
the original union of selected tests. Compiler allowances and Antfly runtime
limits are unchanged. A dedicated participant-team target requires the actual
coordinator CPU/child-allocation regression with no skips. The broader existing
transaction gate already selects it; a preceding 17-test dependency log was not
the main gate's result.

Build graph configuration/typechecking passes on native macOS and pinned
Nix/glibc GNU AArch64. The unchanged audit helper's ten existing tests pass.
The source selection check preserves every original filter and assigns 121 static
named source tests exactly once (88/12/14/7). Static source names are not compiled
inventory or execution counts. Real compiled inventory, test execution, peak
compiler memory and full API/CLI/default performance qualification remain pending.
The first batch includes the broad runnable contracts; its actual compiler peak
still needs measurement. No lower memory claim is made by these graph checks.

The supplemental production proposal is relative to 56983d8023 plus this commit's
build batching/participant registration. HTTP/pgwire completion wiring and other
production proposals remain unqualified. Storage completion deadlines, SQL
sealing/acceptance task handoff, recovery transfer and whole-node/default proof
remain incomplete. Pooling is disabled by default.
