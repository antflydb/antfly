# Allocation-based VP9/AV1 admission

Decoder sessions reserve their own object footprint, then charge core allocations
as they occur through `media.admission.TrackedAllocator`. Allocation headers,
Annex B normalization and potential old/new moving-growth overlap are included.
The configured decode limit remains an upper bound, not an eagerly retained
reservation. Batch metadata has its own tracked construction/retained token;
frames and batch metadata can outlive the decoder independently.

Shared denial preserves another owner's reservation. Native realloc failure
preserves the old allocation and bytes. After core entry, failure sweeps every
core allocation and invalidates the decoder. The failed session object's charge
remains until its actual destruction. Annex B preflight denial is retryable;
stale denial flags cannot mask a later corrupt-packet error. The existing MP4
tracked allocator is reused rather than duplicating these growth/free rules.

ReleaseSafe and Debug pass 225 video tests on macOS (two platform skips).
Actual Linux ARM64 passes 200 (27 platform skips). Forty-five CPU/Metal model
checks and 142 media plus nine production HTTP checks pass. Four concurrent
sessions use default 256 MiB ceilings under an 80 MiB shared pool. Tests cover
exact live/retained bytes, peer denial, failed-object lifetime, allocation
failures, moving/realloc overlap, independent output lifetime and retry.

The benchmark uses the current driver for both variants. Its historical root
contains the exact 7080e3f9da decoder sources; only `webm_benchmark.zig` is replaced
by the identical current telemetry driver. Both use the same current media/image
modules and platform shims. This comparison isolates core/batch admission from
prior selected-output work. Three completed samples per real clip compare exact
pixel/metadata selection digests, packet counts and copied output bytes.

| Selected replay | Previous admitted peak | Current admitted peak |
|---|---:|---:|
| VP9 480p | 302,648,111 | 38,352,472 |
| AV1 480p | 302,652,606 | 41,583,993 |
| VP9 1080p | 305,144,863 | 55,042,128 |
| AV1 1080p | 305,175,005 | 69,854,713 |

These are shared admission charges, including retained container/index owners,
not RSS or physical GPU memory. Timing is shared-host diagnostic. Actual Linux
replay uses the same final sources and independently checks full/selected parity.
The source/binary/fixture manifest and fuzz logs retain their exact scope.
Explicit reconstruction campaigns pass 86,635 VP9 and 102,026 AV1 runs.
No fresh pretrained claim is made here: this phase changes memory admission,
not decoded pixels or model numerics. Previous pretrained receipts remain bound
to their historical source snapshots. No runtime dependency is added.

The full non-CUDA goal remains active, including incremental live AVC validation,
cross-window model projection reuse, broader field/dynamic/HDR pretrained
qualification and further scheduling/streaming coverage.
