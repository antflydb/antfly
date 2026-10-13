# Stateful native AVC fuzz qualification

The new coverage-guided target preserves known MP4 indexes while mutating coded
packet payloads. Unlike whole-container fuzzing, box corruption cannot prevent
it from reaching the persistent decoder. Six seeds cover static prediction,
dynamic geometry/PPS, non-consecutive split PAFF, data partitions and independent
high-depth colour planes. Every attempt uses bounded decode, packet, slice,
picture and shared-admission budgets plus a control callback deadline.

The persistent owner receives up to eight chunked appends. Malformed appends run
before the replay oracle, exercising partial-state failure and destruction.
Successful appends compare all logical picture fields and pending assembly
metadata with complete-prefix replay, rebased to the same retained selectors.
The newest completed picture and all pending assemblies bound attempted eviction;
subsequent appends use retained native references and dynamic configurations.
Published snapshot contents are compared with an independent saved copy across
later mutation. Final destruction checks zero shared host admission and packet
lease retention; the testing allocator checks leaks.

Replay-success/persistent-failure is a differential failure except for resource,
deadline and allocation limits. Rejection ordering between two failing decodes
is not required to match. A successful persistent decode requires successful
replay unless the oracle exhausts a resource/deadline/allocation bound. Eviction
may be refused for a dependent or unfinished assembly; no artificial restart is
manufactured. This target compares metadata, not pixels, and does not claim full
H.264 conformance or new pretrained/model performance qualification.

Run the checked-in seeds with the ordinary video tests and the campaign with:

```sh
zig build fuzz-video \
  -Dvideo-fuzz-filter='video fuzz H264 persistent payloads append eviction snapshots and replay' \
  -Doptimize=ReleaseSafe --summary all --fuzz=100000
```

The first campaign exposed a replay error-classification bug: a preceding
independent colour plane reported pending metadata, which caused replay to swallow
a later corrupt incomplete frame as a valid pending tail. The full recovered
930-byte Smith input is checked in as `h264-persistent-tail.smith`. Before the fix,
the deterministic replay/incremental differential fails. After the fix, the
regression also explicitly requires replay to reject the corrupt four-packet
prefix with `IncompleteVideoPicture`. Only an unfinished selected field/partition
at a packet boundary reports `PendingVideoPicture` to partial indexing.

Qualification logs retain the failing campaign and before-fix reproduction, as
well as the new campaign and source-bound builds. Initial sandboxed macOS runs
failed because Metal/VideoToolbox devices were unavailable; the final runs use
device access. Compiler, executed Linux binaries, source and artifact hashes bind the
receipt. The full original non-CUDA objective remains active, including wider
changing-tool pretrained inputs, sustained full-pipeline measurements and broader
live protocol qualification. No runtime dependency is introduced.
