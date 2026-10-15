# Reader-backed projection reuse

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Validated AVC logical pictures record the packet that completes their fields,
partitions or independent planes. Independently cloned indexes rebase both first
and completion selectors. ReaderSelection owns admitted per-selection visual
cache keys in addition to its policy/source binding. A key hashes track/display
values and every available packet descriptor/payload through completion. Later
appends preserve earlier keys; changed coded dependencies or display metadata
change them. Evicting preroll conservatively changes keys rather than attempting
an unproved shorter dependency identity. Caller-built metadata without completion
selectors falls back to the whole reader prefix.

Reader-backed synchronous and asynchronous encoding now use the existing bounded
CPU/Metal FrameCache. Cached jobs skip reconstruction/preparation/projection;
missing jobs retain their existing overlap and batching behavior. Tickets and
retained device tensors keep their existing admission, eviction and abandonment
contracts. `embedNextLiveCached` exposes this for an explicitly owned live cache.
The cache, immutable model session and control/watchdog must outlive every call
and joined producer. Destroy the cache before closing its session/control. No
URL-keyed persistent or cross-model cache is introduced.

MacOS ReleaseSafe passes 47 CPU/Metal model checks, 225 video and 14 live checks.
Actual Linux ARM64 passes 42 model checks (five Metal skips). Tests prove key
stability after append, completion-packet/display invalidation, exact key/selector
admission and allocation-failure cleanup. A fully cached reader producer completes
under 256 KiB headroom and restores borrowed tickets after joining.

The opt-in pretrained executable compares overlapping one-second-offset windows
from the real six-second Sintel clip. CPU, Metal and actual Linux each record one
hit and three stored projections; cached and uncached 246-token embeddings are
exactly equal. Three ordinary live-window vectors remain exactly equal to
complete-clip source-time inference and pass the pinned PyTorch CPU score/ranking
and top-k checks. Timing is shared-host diagnostic, not a sustained throughput
claim. These results preserve reference behavior, not application relevance.

`qualification.json` binds source, fixture, checkpoint, compiler, binary and
artifact hashes. The full non-CUDA goal remains active: incremental append-time
AVC validation, broader real-stream field/dynamic/HDR pretrained qualification,
preroll-independent reuse and further scheduling/streaming coverage remain work.
