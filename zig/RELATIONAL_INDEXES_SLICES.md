# Relational Indexes Remaining Slices

This is a live backlog, not a progress log. Keep durable design rules in
`RELATIONAL.md` and `SQL.md`; keep completed proof in code, tests, benchmark
output, and git history. Delete completed checkboxes instead of checking them
off or appending progress notes.

Each checkbox must be closable by one focused patch. The patch should include
implementation, fail-closed correctness coverage, public/API or fixture evidence
where relevant, and benchmark or profile evidence when the change affects
performance. If a checkbox needs unrelated patches, split it before starting.
Delete completed work from this file in the same patch that closes it.

Use data-driven tests when the behavior is a matrix: lifecycle state,
generation-record shape, access method, operation kind, fallback reason,
malformed metadata, selectivity band, total mode, row shape, or old/new write
delta. Prefer one table of cases over repeated one-off tests when the setup and
assertions are the same. If a slice has more than two meaningful cases, include
a named table-driven test or benchmark matrix in the patch. One-off tests are
still fine for single invariants with no meaningful case matrix.

Keep this ordered by largest runtime/product gaps first.
