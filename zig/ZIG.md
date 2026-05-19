# Zig Notes

## 2026-04-20: freestanding wasm stdlib breakage in Zig 0.16

This repo's embedded Termite wasm build currently targets `wasm32-freestanding`.
While working on the `pkg/termite` wasm32/wasm64 split, the build hit a set of
upstream Zig stdlib issues before repo codegen/typechecking could complete.

### Repro

From [pkg/termite](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/pkg/termite):

```sh
zig build -Dwasm=true wasm
```

Also reproduced with:

```sh
/Users/ajroetker/bin/zig-0.16.0-dev/zig build -Dwasm=true wasm
```

### Toolchains

- `/Users/ajroetker/bin/zig-0.16.0`
- `/Users/ajroetker/bin/zig-0.16.0-dev`

Both showed the same freestanding stdlib failures.

### Initial failures

The first blockers were unconditional top-level references in stdlib:

- `std/Io/Threaded.zig` referenced `posix.system.getrandom`
- `std/posix.zig` referenced `system.IOV_MAX`

Those were patched locally in both toolchains so the build could proceed far
enough to reveal the broader problem:

- guard `posix.system.getrandom` with `@hasDecl(posix.system, "getrandom")`
- guard `system.IOV_MAX` with `@hasDecl(system, "IOV_MAX")`

### Additional issues found after the first guards

Once the `getrandom` / `IOV_MAX` guards were in place, more freestanding-only
stdlib issues showed up immediately:

- `std/process/Environ.zig`: `GlobalBlock` lacked the `view()` surface that
  other code paths assume exists
- `std/process/Environ.zig`: `createPosixBlock()` mixed `existing.block.view()`
  with direct `existing.block.slice` access, which is invalid for `GlobalBlock`
- `std/Io/Threaded.zig`: environment scanning used
  `environ.process_environ.block.slice` directly instead of the block view
- `std/Io/Threaded.zig`: the later secure-random branch still referenced
  `posix.system.getrandom` without the same `@hasDecl(...)` guard
- `std/Thread.zig`: `UnsupportedImpl.getCpuCount()` and the shared
  `unsupported()` helper were still causing compile-time failure on
  freestanding instead of surfacing runtime or typed `error.Unsupported`

Those also appear to be upstream stdlib issues rather than repo-specific ones.

### Broader remaining problem

After those patches, the same `zig build -Dwasm=true wasm` command still fails
in `std.Io.Threaded`, `std.posix`, and related code paths because freestanding
targets are still typechecking hosted/POSIX assumptions.

Representative failures:

- `std/Io/Dir.zig`: `PATH_MAX not implemented for freestanding`
- `std/Io/Threaded.zig`: references to missing freestanding declarations such as
  `preadv`, `pread`, `pwrite`, `readv`, `read`, `clock_nanosleep`, `socketpair`,
  `getrandom`, `ioctl`
- `std/Thread.zig`: `Unsupported operating system freestanding`
- `std/process/Environ.zig` and `std/Io/Threaded.zig`: mismatched assumptions
  around `process.Environ.GlobalBlock` vs `PosixBlock`
- `std/posix.zig`: many unconditional `system.*` aliases are still invalid on
  the freestanding fallback struct

### Deeper `std.posix.system` fallback issues

After patching the first round of `GlobalBlock` / `Threaded` mismatches, the
next blocker became the freestanding fallback `std.posix.system` surface.

The fallback struct for `.freestanding` / `.other` is far too small for the
current `std.Io.Threaded` implementation to typecheck. Missing declarations
observed during the build included:

- file and directory ops:
  `fstat`, `fstatat`, `lseek`, `ftruncate`, `pwritev`, `mkdirat`, `linkat`,
  `unlinkat`, `renameat`, `symlinkat`, `readlinkat`, `fchmodat`, `fchown`,
  `fsync`, `fchmod`, `utimensat`, `futimens`, `flock`, `writev`
- process / cwd ops:
  `getcwd`, `waitpid`, `kill`, `execve`, `fchdir`, `chdir`, `close`
- time / memory / socket ops:
  `clock_gettime`, `clock_getres`, `clock_nanosleep`, `munmap`, `sendmsg`,
  `shutdown`, `socketpair`
- POSIX constants and types:
  `AF`, `MAP`, `POLL`, `SOCK`, `SOL`, `R_OK`, `msghdr`, `socklen_t`

Even where some placeholders were added locally for exploration, more shape
mismatches appeared:

- vector-I/O signatures expected `posix.iovec`, while an attempted fallback
  borrowed `linux.msghdr_const`, which is not type-compatible
- Linux constant reuse is itself not safe on `wasm32-freestanding`; for example
  `std.os.linux.O` hit its own `"missing ... constants for this architecture"`
  compile error

This strongly suggests the root issue is architectural:

- `std.Io.Threaded` currently assumes a broad hosted POSIX surface
- `std.posix`'s freestanding fallback is not designed to satisfy that surface
- simply adding a few guards is not enough to make `Threaded` freestanding-safe

### Repo-side mitigations discovered

One useful repo-local mitigation did help, but it is only partial:

- `std/Io/Dir.zig` already consults `root.os.PATH_MAX` / `root.os.NAME_MAX` for
  unsupported targets, so the Termite wasm root can define those values locally
  without patching Zig for that specific part

That addresses the `PATH_MAX`/`NAME_MAX` error path, but it does not solve the
broader `Threaded` / `posix` freestanding incompatibility.

### Local toolchain workaround currently in use

To keep `pkg/termite` moving locally, the toolchains now carry a temporary
freestanding workaround instead of trying to fully emulate hosted POSIX:

- `std/Io.zig` conditionally imports a local freestanding `Threaded` shim for
  `.freestanding` / `.other`
- the same file also avoids importing hosted `Dispatch`, `Kqueue`, and `Uring`
  backends for those targets
- the shim only provides the minimal `Threaded` surface needed for freestanding
  wasm builds to typecheck
- the Termite wasm root sets:
  - `std_options_debug_threaded_io = null`
  - `std_options_debug_io = undefined`
  so the build does not try to materialize hosted debug IO at comptime

With that local workaround in place, both of these now build successfully:

```sh
zig build -Dwasm=true wasm
zig build -Dwasm=true -Dwasm-memory-model=wasm64 wasm
```

Important:

- this is a local workaround, not an upstream fix
- it is intended to unblock repo work and narrow the upstream bug report
- the upstream issue remains that freestanding builds currently route through
  `std.Io` / `std.Io.Threaded` assumptions that are not coherent without a
  hosted POSIX surface

### Working hypothesis

Zig 0.16's `std.Io.Threaded` path is intended to be the default general-purpose
`std.Io` implementation, but it is not currently freestanding-safe. The current
stdlib still imports and typechecks POSIX/hosted declarations too eagerly for
`wasm32-freestanding`.

This does not look specific to `pkg/termite`; it reproduces across both local
0.16 toolchains and fails in stdlib before the Termite wasm build can complete.

### If we raise an upstream Zig bug

Include:

- the exact repro command above
- target: `wasm32-freestanding`
- Zig versions from both toolchains
- the first two minimal guards (`getrandom`, `IOV_MAX`) that move the build
  forward but do not solve the broader issue
- the follow-on failures in `std.Io.Threaded`, `std.posix`, `std.Thread`, and
  `std.process.Environ`
- the `GlobalBlock` / `view()` mismatch in `std.process.Environ`
- the `std.posix.system` fallback surface missing dozens of declarations needed
  by `std.Io.Threaded`
- the fact that trying to stub that surface quickly exposes deeper ABI/type
  mismatches rather than one or two missing decls

The likely ask is not "make Threaded work fully on freestanding immediately",
but at minimum:

- avoid unconditional references to missing hosted/POSIX decls on freestanding
- avoid mismatched `GlobalBlock` / `PosixBlock` assumptions in `std.Io.Threaded`
- ensure the default debug/stdio path for freestanding wasm does not require
  hosted `std.Io.Threaded` machinery to compile
- either give `std.posix` a coherent freestanding surface that `Threaded` can
  actually compile against, or stop routing freestanding builds through
  `Threaded`-dependent debug/stdio code paths by default
- ideally make a local shim like the one above unnecessary
