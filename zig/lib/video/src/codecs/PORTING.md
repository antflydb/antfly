# Maintained Zig VP9 and AV1 source

Antfly owns and maintains these checked-in source ports. Building and running the
decoders requires no libvpx, dav1d, FFmpeg, C codec compilation, source download,
or dynamically loaded codec library. FFmpeg is used only to regenerate offline
qualification fixtures. The platform SDK requirements of the existing Apple
backends are unchanged.

| Core | Pinned algorithm source | Source archive SHA-256 |
|---|---|---|
| AV1 | VideoLAN dav1d 1.5.1 | `fa635e2bdb25147b1384007c83e15de44c589582bb3b9a53fc1579cb9d74b695` |
| VP9 | WebM libvpx v1.15.2 | `26fcd3db88045dee380e581862a6ef106f49b74b6396ee95c2993a260b4636aa` |

Sources: <https://github.com/videolan/dav1d/tree/1.5.1/src> and
<https://github.com/webmproject/libvpx/tree/v1.15.2/vp9>.
Each port retains its upstream LICENSE and source authorship notices;
VP9 also retains AUTHORS and PATENTS. Antfly's wrappers and services are
Apache-2.0. The repository license checker preserves the core notices.

The initial port preprocessed the scalar reference implementations, lowered
`goto` control flow into explicit local state machines and translated the
result to Zig 0.17. It retained both 8-bit and high-depth reconstruction paths,
shared repeated declarations and pruned unreachable API/header declarations.
This is an initial source snapshot, not an automatic upstream dependency.
Future changes must be reviewed and maintained as Zig in this tree.

Translation corrections include C integer promotion of boolean expressions,
const pointees, packed field alignment, addressing the end of coefficient/tree
arrays, replacing local prediction bitfields with byte flags, and initializing
motion-vector scan loops at zero. In particular, `@intFromBool` has a one-bit
result: arithmetic must explicitly widen to the C algorithm's integer type.
The interframe fixtures exercise the scan-loop initialization correction.

`runtime.zig` provides allocation, byte operations, atomics, once initialization
and the bounded tile-array sort in Zig. All linker symbols are private Antfly
names. TLS selects a stable request owner; every allocation has an intrusive
ownership record and uses the existing decode Budget. Realloc preserves its
old allocation on failure. Output frames have separate ownership/admission.
Session destruction sweeps the allocation list, including partially constructed
state, without traversing a failed decoder's reference structures.

VP9's reference `setjmp`/`longjmp` error exits were replaced by a first-error
flag and explicit propagation guards. The core never performs a non-local jump.
Single-statement branches preserve their control flow when checking the flag.
AV1 uses its explicit error returns. Cancellation polls at AV1 tile rows,
allocations, and amortized VP9 propagation checkpoints. Both configurations
force one decoder worker; thread/condition hooks cannot be selected by callers.
Independent sessions can run concurrently, and initialization uses native atomic
once guards. Do not enable internal parallelism without replacing and qualifying
those single-worker hooks.

The public API bounds packets, pixels, live core allocations, emitted frames and
output bytes. VP9 preflights reference-derived dimensions and superframe sizes
before reconstruction. AV1 also applies its native frame-size limit. A failure
before core entry leaves the session retryable. Any failure after entry destroys
core state and invalidates the session; recovery creates a new session and
replays from a validated synchronization point. There is no promise of an atomic
retry of a failed coded packet.

`test.zig` checks independently decoded per-picture pixel hashes, every
first-picture allocation failure, cancellation, bounded header mutations,
truncation, superframe output, concurrent admission and output lifetime.
Before changing an algorithm or expanding an advertised profile, run those tests
in Debug and ReleaseSafe, compare additional streams with a pinned reference,
and compile/link both Linux architectures without libc or codec libraries.
Broader corpus/fuzz qualification and throughput tuning remain work; generated
fixtures do not establish complete codec conformance.

Expanded AV1 qualification exercises profiles 0–2, additional native bit depths,
monochrome and chroma layouts, Annex B framing and operating points. The runtime
copy shim treats identical source/destination ranges as a no-op: the scalar
restoration path can copy a retained parameter record to itself. Partial overlap
still fails Zig's checked `@memcpy` contract. See the independent AOM fixtures and
execution receipts in `../../testdata/av1-extended`.


Frame-buffer view getters remain pure pointer accessors. A cancellation poll
inside a getter can return null after the caller's failure guard but before its
dereference; outer decode checkpoints handle cancellation instead. The two
owned VP9 frame-view helpers now follow this rule. A deterministic boundary
regression is explicitly discovered from `mod.zig`, fails with the prior getter
and passes with the pure view. Producer-abandonment qualification also covers
the runtime path that exposed this defect.

Table rows in `av1/qm.zig`, `av1/cdf.zig` and `vp9/vp9_scan.zig` use bounded
`zig fmt: off/on` regions to keep literal data readable without one scalar per
line. The layout cleanup preserves every executable token and all copyright
notices; it does not fold expressions, regenerate constants or change algorithms.
Treat table contents as maintained source, and retain differential decoder checks
when editing them.
