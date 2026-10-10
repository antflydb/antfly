# Worker output failures retain their original cause

The caller-owned output helper remembers the first local execution failure while
layout-only callbacks report a coarse status. Later callback results preserve
that original cancellation, deadline, queue timeout or admission failure. Header
start attempts are recorded before invoking the sink: a sink can publish headers
and then fail weighted reentry, so the worker must propagate that error rather
than synthesize a replacement response. HTTPx uses actual published-header state
to choose an initial error response or a late stream reset.

HTTP scheduler overload classes map to 503 with Retry-After: 1 and a complete
SERVICE_UNAVAILABLE JSON envelope. Original timeouts, deadlines and cancellation
retain their existing distinct handling. Tests decode HTTP/2 HPACK and DATA and
observe an actual HTTP/1 TCP response through EOF.

Native ReleaseSafe passes all three helper tests and 43 selected HTTP tests with
11 Linux-only skips. Pinned Nix/glibc GNU AArch64 passes all three helper tests and
all 54 selected HTTP tests. Native hashes were captured after completion and
match GNU launch hashes. The initial compile failure used an unavailable Zig
ASCII API; it is preserved. The restricted native run failed 19 network tests
with Unexpected, including basic listener/connect tests; the unchanged source
passed with localhost networking permitted. That failure is preserved too.

The helper and HTTP implementation are qualified components. The actual worker
call sites and inherited ABI 33/runtime/provider/standalone changes remain in the
supplemental production candidate patch, replayable on this component commit.
Worker source hashes in the helper result are provenance only: this focused gate
does not compile production Worker.Client or start numerical models. Full
production and API qualification remains required. Initial error output still
uses normal transport waits; protected independently admitted control CPU and
bounded initial error cleanup remain unfinished. Default pooling remains disabled.
Compiler/container reservations are not runtime resource bounds.
