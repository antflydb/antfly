# Checked retained CPU team capability

Runnable ABI 4 appends optional resize_team callbacks to Owner and Borrowed.
Consumers requesting unsupported team resizing receive UnsupportedOperation
without fresh admission. Legacy ABI 3 and malformed headers/demand are rejected.
Borrowed.resizeTeam updates its cpu_slots snapshot only after successful parent
admission; failed growth leaves the snapshot unchanged and the owning capsule
retains its reservation until finish. Previous borrowers must be quiesced before
resizing; borrowed snapshots do not follow later changes automatically.

The API-kernel owner implements resizing on its existing stable Execution. It
adds no operation, byte reservation or owner lease. A protected transport cleanup
capsule remains single-slot even under policies with multiple control slots.
Real-owner tests cover four-slot growth, shrink, invalid demand, unchanged memory/
borrower counts, health progress and rejection after close retaining the running
grant. Existing callback and actual listener/worker/subprocess contracts remain
passing.

Header-first Owner.read and Borrowed.read check foreign version/size before
copying appended callback fields. Lease.initFrom and Transfer.initFrom use those
readers. Header-only fixtures reject both old and truncated values. The two
production inference host boundaries now call the safe readers; those call-site
changes remain in the supplemental production candidate until full archive
qualification. This focused graph does not compile the actual provider host.

Native macOS and pinned Nix/glibc GNU AArch64 pass all 37 ABI contracts. Each
platform executes 31 owner/listener/worker/descriptor contracts; unchanged 26 RPC
contracts are cached from the earlier successful gate. Exact native sources
match the GNU launch hashes. Full terminal commands, logs and hashes are included.
The supplemental patch is relative to 30541d9bb3, includes inherited pending
production changes and the two newly proposed safe host call sites, and is not
a claim of full production qualification.

Actual transaction participant/numerical team integration, joined child heap
accounting, and accepted-write completion transfer are still required. The SQL
read adapter does not advertise resize_team: its resumption fences must be
preserved when that separate capability is added. Pooling remains disabled.
The older production pair is queued on a frozen ABI 3 snapshot, not this ABI 4
candidate; its eventual result cannot qualify these new changes.
