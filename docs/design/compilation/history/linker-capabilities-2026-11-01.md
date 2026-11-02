# Pinned linker capability screening

These tiny probes ran while the media-session production regression build was
in progress. They are capability checks, not production timing/size comparisons.
Zig version: 0.17.0; native host: ARM64 macOS 27.0.1; SDK: 27.0.

## ThinLTO

The installed standard library exposes
`std.zig.LtoMode = enum { none, full, thin }` and
`std.Build.Step.Compile.lto: ?std.zig.LtoMode`. Its build runner emits
`-flto=thin`. The short `zig build-exe --help` text does not enumerate that value,
so the earlier plan's help-only observation understated ELF support.

Observed on tiny fixtures:

- `zig cc -target x86_64-linux-musl -O2 -flto=thin` compiled and linked.
  `llvm-bcanalyzer` found `GLOBALVAL_SUMMARY_BLOCK` in a separately emitted object.
- A real Zig build with a static owner and a separate executable caller linked
  with `owner.lto = .thin` and `exe.lto = .thin`, both using LLVM. x86-64 and ARM64
  musl builds succeeded.
- Matched ARM64 ELF Zig and C probes ran in the existing local ARM64 Linux
  development image, with networking disabled. Arithmetic results matched.
  The C probes preserved distinct address-observable functions in both modes.
- Identical Zig functions shared one address both with and without LTO. The
  initial probe incorrectly assumed the no-LTO baseline would give them distinct
  addresses. The corrected paired probe reports that observation separately
  from arithmetic correctness; this did not establish a ThinLTO regression.

The same pinned compiler rejects the default product's Mach-O path:

```text
zig build-lib owner.zig -target aarch64-macos -Ofast -fPIC -flto=thin ...
error: LTO requires using LLD

# Adding -flld:
error: using LLD to link macho files is unsupported
```

The non-LTO archive linked through `/usr/bin/ld` into a dylib, and native `ctypes`
calls returned the expected values. The ThinLTO archive could not be emitted for
that target, so compatibility with the production external CAPI link was not
established. Do not enable this on production owners shared by the Mac server
and C library. It remains a Linux-only experiment until the toolchain supports
the required target/link topology. Full-product compiler memory, backend
concurrency, final size and incremental cache behavior are still unmeasured.

## Safe code folding

The pinned Zig C driver rejected `-Wl,--icf=safe`:

```text
error: unsupported linker arg: --icf
```

No external `ld.lld` was installed in the native LLVM tools or the existing local
Linux development image. An external ELF relink therefore remains untested; no
production linker was replaced and no folding flag was enabled. A future probe
must preserve address-significant functions and the complete public export set.

## Receipts and cleanup

Raw commands, source fixtures, exit statuses, hashes and logs are ignored under
`.benchmark-results/compile-costs/`:

- `linker-capabilities.json`, `darwin-lto-capabilities.json`.
- `lto-matched-arm64-runtime.json` (Zig exit 2 means correct arithmetic with merged
  function addresses; C exit 0 means correct arithmetic and distinct addresses).
- `lto-none-zig-arm64-build.log`, `lto-thin-zig-arm64-build.log`.
- Earlier exploratory `thin-*`/`no-lto-*` logs are retained for audit.

The temporary source/build cache lived under `zig/.zig-cache/optimization-probes`
and was deleted after recording these results. Temporary installed probe
executables were removed from the single existing `zig/zig-out` prefix. No
production source or backend flags changed for these probes.
