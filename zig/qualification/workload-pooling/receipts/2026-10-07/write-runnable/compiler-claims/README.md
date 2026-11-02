# Routed implementation compiler admission

The native GNU ARM64 ReleaseSafe routed-read implementation compiler measured
17,479,122,944 bytes during the earlier retained-owner integration run, exceeding
its old 8 GiB allowance. The checked profile now supplies 21 GiB, over 25% above
the measured peak. macOS retains 12 GiB; other hosts, targets, modes and sanitized
builds retain 8 GiB. Runtime limits are unchanged. `source-hashes.json` pins the
selector and build wiring.

All five compiler-profile tests pass on native macOS and the pinned Nix/glibc
GNU ARM64 compiler. `zig build --help` also exits zero on both platforms,
typechecking the build integration. Complete logs are adjacent. Commands:

```sh
zig test pkg/antfly/build/runtime_memory.zig -Osafe --cache-dir <cache>
zig build --help --cache-dir <cache>
# GNU ARM64 graph check also supplies -Dtarget=aarch64-linux-gnu.
```

Linux checks use the verified image/compiler recorded in ../linux-compiler.json
under two CPUs and 1 GiB without extra swap. These checks prove selection and
build parsing; they do not establish a complete routed-owner or production build
under the corrected reservations. The production storage archive still declares
26 GiB, exceeding the Docker VM's default 23.43 GiB scheduling budget. The whole
pooling API test remains separately qualified under its unchanged 23 GiB claim.
