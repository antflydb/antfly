# SQL preparation runnable ownership

HTTP SQL preparation and pgwire plan preparation now acquire query runnable
capacity before parsing, cache access and Describe. Each releases its preparation
grant before coarse execution admission. Protected SQL controls use the reserved
control class. Pgwire read setup acquires its execution grant using a stable,
heap-owned request before identity and request cloning; ordinary Describe also
uses runnable execution capacity.

The existing mixed HTTP/pgwire socket fixture holds the ordinary runnable slot
and requires both requests to queue before coarse execution admission, with no
new catalog calls. Authenticated ROLLBACK still completes on protected capacity.
After release, both ordinary requests complete and ownership is reconciled.

Native qualification used Zig 0.17.0 on macOS with:

```sh
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-default-pooling-global \
/Users/ajroetker/bin/zig-0.17.0/zig build antfly-workload-pooling-test \
  -Doptimize=ReleaseSafe -j1 \
  --cache-dir /private/tmp/antfly-default-pooling-cache --summary all
```

The command exited zero: 50/50 build steps, 36 embedded and 39 coordinator
tests, with no skips or leaks. `native-api.log` preserves the complete output;
`source-hashes.json` pins the changed implementation files. The build log's
“failed command” labels accompany successful test stderr routed through the
repository test runner; the terminal build result is successful.

This establishes bounded preparation concurrency, not parser preemption or
exact plan-cache wait suspension. Preparation checkpoints, serialization
ownership, ordinary writes, inference, background work, fan-out, complete
production qualification and automatic defaults remain pending. Linux API
qualification remains pending: the current GNU ARM64 storage compiler declares
26 GiB, exceeding the available 23.43 GiB Docker VM budget. Runtime resource
limits are unchanged.
