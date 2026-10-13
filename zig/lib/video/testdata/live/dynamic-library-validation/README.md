# Persistent dynamic-AVC library qualification

The maintained Zig logical stream now supports incremental in-band parameter
planning and reconstruction across compatible configuration changes and storage
geometry epochs. Registry history owns parameter bytes; rebasing retains the
latest declaration for each pre-boundary ID and future events. Owned bootstraps
establish parameter availability for independent suffix replay. Full eviction
retains an empty active epoch for later appends.

For the dynamic geometry and PPS fixtures, every prefix produces the same
completed picture metadata, packet configuration selectors and epoch boundaries
as fresh registry/reconstruction. Successful native reconstruction and parameter
scanning visit each packet once. Allocation failure coverage includes registry
history, decoder replacement, bootstrap copies, snapshots and rebase tables.
The PAFF reset suffix matches the full source's rebased pictures; an ordinary
I-picture suffix still returns MissingVideoReference.

ReleaseSafe macOS passes 237 video tests (two skips), 20 live tests and 49
CPU/Metal model tests. Actual Linux ARM64 passes 212 video tests (27 platform
skips) and 20 live tests. No new runtime dependency is introduced. Shared static
prefix validation uses bulk byte hashing, preserving mutation/retry checks.

This is the library phase. The default live coordinator still uses replay for
in-band configurations. Source-owned parameter checkpoints must be integrated
with prepared windows, eviction and failed-decoder retry before switching that
adapter. Bounded unused configuration-entry compaction, dynamic pretrained
qualification, persistent-owner fuzzing and original broader non-CUDA work remain
active. No fresh pretrained or performance claim is made here.

Source, compiler, fixture, executed Linux binary and log hashes are recorded in
qualification.json. The suite uses the same pinned offline/read-only Linux image
as preceding receipts. Build/test logs retain the exact gate counts and commands.
