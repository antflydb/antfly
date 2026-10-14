# Partial DASH qualification

Explicit partial-availability selection inherits BaseURL policy and lets effective
SegmentTemplate/SegmentList declarations override it. The Reader keeps the selected
resource pending across a short clean HTTP EOF and an unchanged body version.
Acknowledged prefixes are verified before new fragments are published, and native
continuous decode duration determines cursor completion. A monotone MPD completion
announcement cannot bypass missing media; a new decode gap rejects before callback.

The portable suite checks inheritance, exact request timing, buffered-path rejection,
short/unchanged/full versions, cursor/ordinal/completion counts, attempt budgets,
gap rejection and healthy retry, and all allocation failures. Production authenticated
HTTP tests serve actual growing versions and retain an independent packet lease.
Both suites execute on macOS ARM64 and the pinned native Linux ARM64 VM, with no
external network and a writable temporary test directory inside the read-only VM.

The pretrained test adds a model-output failure, retains its exact vector for retry,
then polls a short clean EOF before fetching the full selected DASH resource. The
server holds the final response tail until a model output executes. Its six copied
window vectors, token counts and source clocks are compared with buffered complete
DASH inference using the same pinned checkpoint and backend. CPU, Metal and actual Linux ARM64 checkpoint runs each completed with six
windows, seven callbacks, five requests and zero maximum coordinate difference.
The receipt binds this Linux test to implementation commit `8abc3421f3`, before
the subsequent encrypted-streaming work.

This is a generated six-second AVC fixture and bounded qualification. It does not
establish broad live-origin interoperability, new retrieval quality, CUDA support,
or streaming AES/TS/LL-HLS part reconciliation. See receipt.json for source, binary,
checkpoint and log hashes and exact result scope.
