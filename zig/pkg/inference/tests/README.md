# Inference cancellation E2E

Run from `zig/` with `zig build inference-test-cancellation-e2e`, or from
`zig/pkg/inference/` with `zig build test-cancellation-e2e`. The unfiltered
`inference-test` / package `test` aggregate also runs this suite on native Linux
and macOS. It needs Python 3, but no downloaded model, GPU, or Python packages.

The fixture runs production HTTP routes, tokenization, request/weighted/run
admission, execution control, the hard-cancellation watchdog, and the process
supervisor. Only the model session is synthetic. Its control routes exist only
in this test executable, which is not installed or linked into the server.

The embedding cases first verify a successful embedding, arm a blocking model call,
waits until the real HTTP request owns admission and scratch memory, and then
aborts its TCP client with a reset (RST):

- A cooperative session must observe cancellation, unwind its permits, and
  continue serving in the same worker.
- An uninterruptible session never checks cancellation or requests a restart;
  the production watchdog must terminate it and the supervisor must replace it.

Both paths must return to baseline resource accounting and serve the same
embedding successfully afterward. All network waits are bounded, and cleanup
terminates the fixture process group even on failure.

By default, an orderly TCP half-close (FIN) is not cancellation: an HTTP/1
client may still be waiting to read the response. The embedding tests use RST
to establish response abandonment, matching the default transport contract.

The HTTP reranker and linked reranker cases abort their requests with a TCP
reset. Their synthetic GPU batch returns only
after the test releases it. The worker must remain alive while the batch is
in progress, then observe cancellation and release admission before the next
rerank request.

The embedding case exercises the same safe native-call grace through the
shared inference control. A cancelled request releases at the end of its
current call without replacing the worker; a genuinely stuck call still
restarts the worker after the bounded grace.

This covers transport-abort propagation through model execution. It does not
exercise real GPU drivers, cold model loading, or application-deadline wiring;
those need separate coverage.
