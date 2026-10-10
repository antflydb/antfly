# Physical broker admission boundary

Source base: merge `6c0884748b`. Source hash is pinned beside this receipt.

Both singleton compatibility calls and native groups now pass through an
optional immutable execution admission provider, outside all shard locks.
The provider borrows collective member control until finish. Members canceled
or expired during admission are removed before backend invocation; a live
peer keeps its own lifetime. Rejection and staging allocation failure dispose
of charges without running the backend. No owner or CPU scope is installed
by production code yet; parent parking, actual owner/CPU provider wiring and
whole-production qualification remain required. Shape bytes supplied to the
provider are payload estimates, not a complete memory accounting claim.

Both final commands exited 0 and passed all 21 broker tests (19 existing and
two added). Native macOS uses `/Users/ajroetker/bin/zig-0.17.0/zig`:

```sh
zig test -Osafe zig/pkg/inference/src/server/executor_microbatch.zig \
  --cache-dir /private/tmp/antfly-broker-admission-native-cache
```

Linux uses the pinned image
`sha256:fb3236b6cc7411028d62f76eb418110a6c34e246b9437a9d093d58a53a3e2954`,
entrypoint `/opt/zig-nix/bin/zig`, 2 CPUs, 1 GiB memory, no extra swap,
read-only source at `/work`, writable cache cwd `/cache` and global `/global`:

```sh
zig test -Osafe -target aarch64-linux-gnu -lc \
  /work/zig/pkg/inference/src/server/executor_microbatch.zig \
  --cache-dir /cache --global-cache-dir /global
```

Full final logs are compressed alongside this file. The preserved initial
native failure was an invalid serial test fixture (max_items=2); correcting
it to singleton capacity changed no production logic. The first Linux launch
was denied Docker socket access by the sandbox; the authorized retry passed.
Defaults remain disabled. These are broker unit gates, not full inference,
API, production startup or overload qualification.
