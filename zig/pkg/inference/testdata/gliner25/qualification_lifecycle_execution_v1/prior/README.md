# Additive focused-build and teardown evidence

This private checkpoint copies the prior evidence directory unchanged under `historical/` and adds two focused Metal build failures plus nine independent model-free child probes. The prior ledger remains SHA-256 `9acc6ffcca8724acdd4247dd7a8e4c2b0295478f8cf9bbd2bb5c3f13b0aaafc3`. All five failed build commands remain failed; no release, quality, performance, remote CI, or runtime qualification is granted.

| New checkpoint | Result | Scope |
| --- | --- | --- |
| Final-focused Metal v1 | Main test compilation failed | Learned-window test inferred a required watchdog pointer but used optional capture. Exact pre-existing draft source is preserved. |
| Final-focused Metal v2 | 67 selected: 65 passed, 1 skipped, 1 failed, 0 leaked | Final TTL eviction after reload and metrics retained one model owner. |
| Teardown child probes v2 | Nine expected exit-86 results; all reaped | Fake cache/session destructors in fresh model-free child processes, including blocked stderr polling and final-release checks. |

The final-focused v2 executable is 85,775,944 bytes, SHA-256 `7e9519f774211ef0065137eafaa9ef9c7995f19d2f75817ed0fbe36ad01aee74`. Its archived bytes and full source tar were streamed and rehashed against the process/source archive receipt; neither binary nor tar is copied here. The child report binds this same executable and its exact 7,487-byte driver, SHA-256 `bff654f292e18d5402b234b27b1cdb3a049268fe251f3b34969703841a2d9423`.

The raw final-focused archive initially called the remaining failure a possible stale observation timestamp. Subsequent parent review identified a production bug: metrics snapshot release refreshes the model's idle-use time. That diagnosis is recorded separately from the unchanged raw receipt. No stale-test-timestamp adaptation is recorded as applied or correct. The production fix is pending at this checkpoint.

Individual passing scopes are preserved without promoting the aggregate. The four learned-window tests include fixed geometry and comparison units plus actual pinned-small native and Metal execution, with more than one overlapping window and task/coordinate/ownership/retry assertions. Their predeclared diagnostic relation profiles remain exercise coverage, not an upstream whole-document quality oracle. The allocation regressions verify terminal backing OOM after a speculative denied resize. Raw test names and exact source snapshots provide the bounded scope; the complete aggregate failed on actual-model cache lifecycle behavior.

The nine model-free probes cover cache cleanup before physical session close, TTL, admission pressure, retired release, shutdown, failed-load rollback, an escaped raw session, and two blocked-stderr cases. Their raw streams show case-specific destructor entry before expected fatal exit. The cache probe confirms both primary and optional tickets are active before cached object destruction. The escaped session intentionally has no admission lease, so its proof is ticket/owner lifetime only. The other probes retain the 64-byte synthetic admission lease; no lease-release or cleanup-return marker appears. Every process returned 86 before the five-second outer limit and was reaped. Blocked-stderr polling and final-release cases took approximately 0.117 and 0.113 seconds; these are timeout diagnostics, not performance benchmarks. No live model or Metal driver is needed by these child fixtures.

Raw receipts, process observations, stdout/stderr, and all 2,588-entry inventories are preserved. Seventeen selected source files per new checkpoint are hash-checked; shared historical content is reused. Inventories remain source selections rather than complete dependency closures. The complete archive is capped at 8 MiB. No binaries, full tars, model weights, or altered historical logs are included.

Run the local, model-free integrity check without original temporary artifacts:

```sh
python3 verify.py /absolute/path/to/this-directory --self-test
```

This verifies the original ledger transitively, both new failed-run bindings, every saved file, case-specific child markers, expected watchdog exits, binary identity, and probe cleanup facts. Adversarial checks reject failure promotion, a foreign child binary, and an outer kill presented as a watchdog exit. No build/model/process campaign occurs during verification. Further corrected execution must be added as fresh evidence.
