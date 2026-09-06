# Inference cancellation E2E

Run from `zig/` with `zig build inference-test-cancellation-e2e`, or from
`zig/pkg/inference/` with `zig build test-cancellation-e2e`. The unfiltered
`inference-test` / package `test` aggregate also runs this suite on native Linux
and macOS. It needs Python 3, but no downloaded model, GPU, or Python packages.

The fixture runs production HTTP routes, tokenization, request/weighted/run
admission, execution control, the hard-cancellation watchdog, and the process
supervisor. Only the model session is synthetic. Its control routes exist only
in this test executable, which is not installed or linked into the server.

Each test first verifies a successful embedding, arms a blocking model call,
waits until the real HTTP request owns admission and scratch memory, and then
aborts its TCP client with a reset (RST):

- A cooperative session must observe cancellation, unwind its permits, and
  continue serving in the same worker.
- An uninterruptible session never checks cancellation or requests a restart;
  the production watchdog must terminate it and the supervisor must replace it.

Both paths must return to baseline resource accounting and serve the same
embedding successfully afterward. All network waits are bounded, and cleanup
terminates the fixture process group even on failure.

An orderly TCP half-close (FIN) is not cancellation: an HTTP/1 client may still
be waiting to read the response. The tests deliberately use RST to establish
response abandonment, matching the production transport contract.

This covers transport-abort propagation through model execution. It does not
exercise real GPU drivers, cold model loading, or application-deadline wiring;
those need separate coverage.
