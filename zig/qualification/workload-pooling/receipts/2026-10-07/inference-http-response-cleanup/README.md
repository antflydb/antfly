# Post-dispatch inference HTTP response cleanup

The real in-memory host HTTP handler creates an owned response with the test allocator, then cancels the original invocation before returning. The production consumer installs the host destructor before rechecking the original cancellation/deadline fence, so rejected publication releases that response and the provider lifetime guard. Missing-response errors retain original-control precedence.

The native ReleaseSafe antfly-inference-response-cleanup-test gate passes with exit 0. The selected integration test uses the actual route manifest, host HTTP dispatch, owned response allocation, production consumer and response destructor; the allocator reports no leaked allocations. Exact output, command and five checked source hashes are retained. Linux and consolidated qualification with later model-lock/nested-scope code remain pending. This is a cleanup boundary proof, not numerical model or automatic-default qualification.
