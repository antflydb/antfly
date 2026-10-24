# Checked completion capability

Runnable ABI version 5 adds optional reserve/activate completion callbacks to
Owner and Borrowed. Old version 4 prefixes are rejected before reading appended
fields. The completion descriptor requires an explicit deadline and provides
no foreground cancellation hook. Unsupported callbacks fail explicitly. Header,
reserved-byte and clock validation precede callback invocation; callback errors
retain their exact stable statuses. Borrowed.fromOwner forwards both callbacks.
The capability grants no new admission and does not prove a durable decision.

The API owner stores completion I/O and optional clock receivers at stable capsule
addresses, through the owning finish. Duplicate reservation cannot replace these
receivers. Failed reservation clears staged receivers. Transport cleanup cannot
claim completion authority. Original foreground fences still govern reservation
and activation; active completion uses the independent finite completion deadline.
The original operation, CPU partition and byte reservation remain authoritative.

All 38 directly discovered runnable ABI/runtime contracts execute and pass on
native macOS and pinned Nix/glibc GNU AArch64. The focused owner builds pass on both
platforms: 24 owner/inference contracts, two worker/output contracts and two actual
HTTP cleanup contracts execute. Four SQL team contracts also execute on Linux;
the native SQL team target is cached.
The two new real-owner tests cover foreground cancellation/expiry, health progress,
admission close, retained stopped ownership at completion deadline, failed
reservation cleanup, activation fences and denial of cleanup authority. These
in-process tests do not exercise an independently compiled foreign I/O dispatch.
The runtime's stable receiver storage supports that path, whose full archive
qualification remains pending. Source manifests match the tested source bytes.

No production consumer activates completion yet. Required remaining work includes
completion tasks and bounded resources reserved before SQL seal/participant
acceptance, physical storage/WAL/Raft/quorum waits, joined ambiguous-outcome cleanup
and authoritative durable recovery transfer. Write-team suspension remains
unsupported and pooling stays disabled by default. This is capability coverage,
not production durability, whole-node resource accounting or default qualification.
The supplemental production patch is relative to 33ad1e57ea and excludes the
committed ABI/owner changes; it includes both process fixture files.
