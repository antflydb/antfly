# Zig Notes

## 2026-05-30: Zig 0.17.0 nightly bring-up (build-system split + `**` removal)

Tracking the 0.17.0 pre-release announced in the
[2026-05-26 devlog](https://ziglang.org/devlog/2026/#2026-05-26). 0.17.0 is not
released yet; this is against the official nightly `0.17.0-dev.607+456b2ec07`.

### Installing the nightly via pip

The official `ziglang` PyPI package only publishes stable releases (max `0.16.0`),
so `pip install ziglang` cannot get a 0.17 dev build. The canonical "pip install
nightly" path is the [zig-pypi](https://github.com/ziglang/zig-pypi)
`make_wheels.py`, which repackages any `ziglang.org/builds` tarball as a wheel:

```sh
git clone --depth 1 https://github.com/ziglang/zig-pypi.git
cd zig-pypi
# One-time patch: the nightly archive ships lib/libtsan/LICENSE.TXT, which the
# builder's strict license allowlist rejects. Add it to required_license_paths.
uv run --with wheel python make_wheels.py --version master --platform x86_64-linux --outdir dist
pip install --force-reinstall dist/ziglang-0.17.0.dev*.whl
python3 -m ziglang version   # -> 0.17.0-dev.607+456b2ec07
```

The wheel exposes the compiler as `python3 -m ziglang`; wrap it in a `zig` shim on
PATH for the Makefile (`ZIG ?= zig`).

### What the devlog change requires (build-system split — DONE)

The devlog's headline change is the configurer/maker split. The user-visible API
break is that `b.args` is gone (the build graph can no longer observe the post-`--`
passthru args at configure time). Migrations applied across all `build.zig` /
build-helper files:

- `if (b.args) |a| run.addArgs(a);`  → `run.addPassthruArgs();`
- `if (b.args) |a| run.addArgs(a) else run.addArgs(&defaults);`
  → `run.addArgs(&defaults); run.addPassthruArgs();`
  (defaults are now *always* added and user args are appended after; for the
  last-wins flag parsers in our benches this preserves override intent, but it is a
  behavioral change — defaults are no longer suppressed when args are passed.)
- `forwardBuildArgs` lost its now-unused `*std.Build` parameter.
- `b.getInstallPath(.prefix, "…")` was removed →
  `run.addFileArg(b.graph.path(.install_prefix, "…"))` (a cache-correct LazyPath).
- `selectTestFilters` read `b.args` at configure time to set compile-time test
  `.filters`. That capability is gone; it now reads a repeatable `-Dtest-filter=…`
  option. The option must be declared exactly once (`b.option` panics on a second
  declaration), so the top-level `build.zig` declares it up front and threads the
  resolved value into the (now pure) helper.

After these edits both `zig build --help` (top-level) and the delegated
`pkg/inference` build configure cleanly under the nightly. **These build files now
require Zig 0.17+ and will not compile under 0.16** (no `addPassthruArgs`,
`b.graph.path`, etc.), so CI cannot be flipped to nightly until the items below are
resolved.

### What blocks actually compiling the codebase (NOT in the devlog)

This nightly **also removed the `**` array/string repeat operator** (undocumented
in the devlog as of 2026-05-30). The tokenizer no longer has an `asterisk_asterisk`
token, so:

- `"ab" ** 3` → error: *binary operator '*' has whitespace on one side, but not the
  other* (the new symmetric-whitespace lint, since `**` is now two `*` tokens).
- `"ab"**3` → parses as `"ab" * *3` (multiply by a pointer type) → type error.

`std` itself contains zero real `**` uses (already migrated), confirming this is
deliberate. Replacements:

- `++` (concat) is unchanged.
- Scalar fill `[_]u8{0} ** 16` → `@splat(0)` **with an explicit result type**
  (`const a: [16]u8 = @splat(0);`) — the bare `[_]…` length-inferred form has no
  result type for `@splat` to use, so each site needs a type annotation.
- Sequence repeat like `"0123456789" ** 32` has no one-liner replacement (needs a
  comptime loop / generated constant).

Scope in this repo: ~1,716 real `**` repeat sites across 119 files.

#### `**` migration — DONE

Migrated mechanically with two deterministic rules applied innermost-first to a
fixpoint (so nested repeats resolve automatically):

- `.{X} ** N` → `@splat(X)` (relies on the result-type context that `.{…}` already
  required).
- `[_]T{X} ** N` → `@as([N]T, @splat(X))` (self-typed; works in any context, and a
  nested inner result becomes the scalar `X` for the outer rule).

1,716 sites were rewritten by script; 16 were handled by hand:

- string repeats `"x" ** N` → `&@as([N]u8, @splat('x'))` (single char) or a
  `comptime` `++` concat constant (multi-char, e.g. the json `digits_repeated_32`
  fixture);
- one multi-element array repeat `[_]f32{…} ** 5` →
  `@as([15]f32, @bitCast(@as([5][3]f32, @splat(…))))`;
- a few element types my scanner skips (parens/spaces, e.g.
  `std.ArrayListUnmanaged(Route)`, `?[]const u8`) done directly as `@as(…, @splat(…))`.

Validated: `lib-json-test`, `lib-httpx-test`, `lib-toon-test` compile and pass under
the nightly.

### Remaining: general std/builtin churn (separate, open-ended)

Compiling the whole tree on this nightly surfaces *unrelated* 0.17 std/builtin
changes, independent of `**`. Found so far:

- `std.meta.Int(.unsigned, n)` removed → `@Int(.unsigned, n)` (2 sites, FIXED). The
  `@Type` builtin has been split into targeted builtins (`@Int`, `@Pointer`,
  `@FieldType`, …); the repo doesn't call `@Type` directly so only the `meta.Int`
  shim was affected.
- `std.bit_set` static bitsets lost `.initEmpty()` (e.g. `lib/regex`); not yet
  migrated.
- ~3k `std.Io` references not yet probed for behavioral/API drift.

This general port is large and open-ended; it is tracked separately from the `**`
work above.

## 2026-04-20: freestanding wasm stdlib breakage in Zig 0.16

This repo's embedded Antfly inference wasm build currently targets `wasm32-freestanding`.
While working on the `go/pkg/termite` wasm32/wasm64 split, the build hit a set of
upstream Zig stdlib issues before repo codegen/typechecking could complete.

### Repro

From [go/pkg/termite](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/go/pkg/termite):

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
  unsupported targets, so the Antfly inference wasm root can define those values locally
  without patching Zig for that specific part

That addresses the `PATH_MAX`/`NAME_MAX` error path, but it does not solve the
broader `Threaded` / `posix` freestanding incompatibility.

### Local toolchain workaround currently in use

To keep `go/pkg/termite` moving locally, the toolchains now carry a temporary
freestanding workaround instead of trying to fully emulate hosted POSIX:

- `std/Io.zig` conditionally imports a local freestanding `Threaded` shim for
  `.freestanding` / `.other`
- the same file also avoids importing hosted `Dispatch`, `Kqueue`, and `Uring`
  backends for those targets
- the shim only provides the minimal `Threaded` surface needed for freestanding
  wasm builds to typecheck
- the Antfly inference wasm root sets:
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

This does not look specific to `go/pkg/termite`; it reproduces across both local
0.16 toolchains and fails in stdlib before the Antfly inference wasm build can complete.

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
