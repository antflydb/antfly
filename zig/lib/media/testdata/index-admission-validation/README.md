# MP4 index admission qualification

MP4 packet/list construction is charged as allocations occur. Moving growth
reserves old and new buffers together; denied growth preserves the prior owner.
The reader retains exactly its final packet-array reservation. Source leases
continue to charge metadata separately. The configured packet-index limit still
bounds the returned array; construction has a separate conservative bound for
list capacities and growth overlap.

ReleaseSafe checks pass 142 media and nine production HTTP tests on macOS and
actual Linux ARM64. Tests cover default limits under a 16 KiB shared budget,
progressive and fragmented MP4, exhaustive allocation failures, shared denial,
forced moving growth and final release. Video/live and model checks are recorded
in the accompanying logs and manifest.

The completed serial coordinator benchmark compares the previously qualified
278c5a1667 executable (with its documented counter instrumentation) against the
current production module. The coordinator algorithm and emitted windows are
unchanged. The prior executable hash is checked against its historical receipt.
Each case runs three samples, including cold setup; timings are diagnostic.
Sintel's six-second, six-fragment clip drops peak shared admission from
101,990,501 to 1,328,885 bytes. All three window digests and validation counts
match. The normalized-clock interleaved-field control retains the same peak:
its fixture helper already shrinks reservations to one selected packet.
These figures describe admitted host bytes, not RSS or physical GPU memory.

This change adds no runtime dependency. VP9/AV1 remain maintained checked-in Zig
ports; upstream snapshots are reviewed source provenance, not downloaded build
or runtime dependencies. Codec support is qualified by the existing receipts.
No new pretrained inference or codec-conformance claim is made here.
