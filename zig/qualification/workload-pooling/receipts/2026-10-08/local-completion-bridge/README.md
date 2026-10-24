# Stable local SQL completion bridge

HTTP and pgwire retain local scheduler Executions. The existing SQL write team
could resize these roots, but had no completion callback. Completion now bridges
that same local write/control root into a checked runtime Borrowed capability.
It stores completion I/O and clock receivers at stable ingress-owned addresses
which must remain alive through the original execution's owning finish.

Suspension/checkpoint hooks remain unavailable until completion is activated.
Original cancellation/deadline/close fences govern reservation and activation.
Duplicate reservation cannot replace a retained receiver. A failed reservation
clears staged receivers. Native completion deadlines require an existing native
clock; otherwise they fail explicitly. Protected COMMIT remains one control
slot, while ordinary writes retain their bounded team grant. Whole-team changes
require all existing children to be quiescent; this is not a per-child token.

Six SQL-team contracts, three actual task contracts, 24 owner/inference contracts,
two worker/output contracts and two public HTTP cleanup contracts execute and
pass on native macOS and pinned Nix/glibc GNU AArch64: 37 per platform, no skips.
The new real task test exercises the local bridge on both write and protected
control roots using actual bounded concurrency. A canceled activation starts no
callback; successful activation preserves one root and its byte charge through
foreground cancellation/expiry, health progress, admission close and joined
cleanup. The other new tests check stable receivers, completion-deadline stopped
retention, native-clock denial, failed reservation cleanup and close fences.
These tests perform no physical storage/WAL/Raft write.

The supplemental production proposal constructs stable bridges at HTTP/pgwire
ingress and passes them to the SQL adapter. These call sites have not been
qualified by the focused roots. The native manifest excludes the three unused
production files edited after native testing; the core tested bytes match both
platform receipts. Full API/archive qualification remains pending. No production
caller activates completion or reserves its completion task yet. SQL sealing,
participant acceptance, finite physical storage/quorum waits and authoritative
recovery transfer still require implementation and qualification. Pooling remains
disabled by default. The supplemental patch is relative to c2817e3536 plus this
commit's core bridge/tests and includes both process fixture files.
