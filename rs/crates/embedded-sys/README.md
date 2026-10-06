# antfly-embedded-sys

Raw, hand-written `extern "C"` declarations for `libantfly`, the stable
embedded Antfly C ABI (`zig/pkg/antfly-embedded/include/antfly.h`). There is no
`bindgen`/`libclang` dependency: the header is small and stable enough that a
hand-written mirror is easier to audit, and this crate does not require a C
toolchain to build.

Most applications should depend on `antfly-embedded` instead, which wraps this
crate in a safe API. Use `antfly-embedded-sys` directly only if you need C ABI
access this crate's safe layer does not yet expose.

## Linking

Building this crate as a library never links against `libantfly` -- only a
test binary, example, or downstream binary that actually calls one of its
`extern "C"` functions does, and only when the `libantfly` Cargo feature is
enabled (`--features libantfly`, or `antfly-embedded/libantfly` from a consumer).
Without the feature, `build.rs` does nothing beyond registering the crate's
`links = "antfly"` key.

With the feature enabled, `build.rs` resolves the library in this order:

1. `ANTFLY_LIB_DIR`, an explicit directory override; a missing library is an error.
2. `pkg-config libantfly`, including `PKG_CONFIG_PATH` and the pkg-config crate's
   target-specific environment variables for cross-compilation.
3. `zig/zig-out/lib` in an Antfly source checkout.

Install `pkg-config` and extract the Apache **`antfly-embedded`** archive,
then build with:

```sh
export PKG_CONFIG_PATH="/path/to/antfly-embedded/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
cargo build --features libantfly
```

The archive's relocatable `lib/pkgconfig/libantfly.pc` also works with Go
and C build tools. Keep `lib/` and `include/` together. No ELv2 server is
required. For cross-compilation, point pkg-config at a target installation
using its target-specific configuration; do not allow it to select host
libraries. Alternatively set `ANTFLY_LIB_DIR` to a target library directory.

The sys crate emits library search paths and passes the resolved directories
to the safe crate, so both crates' test/example binaries get runtime search
paths on Linux/macOS. Downstream build tools can use the `.pc` file's `Libs`
flags or configure their own loader paths. The library must remain available
at runtime.


Pkg-config resolves the archive's current installation directory when you
build. These crates' test/example binaries receive an absolute runtime search
path; downstream applications follow their own loader configuration. If a
binary uses the archive directory as its rpath, moving the archive afterward
requires rebuilding that binary or configuring the system loader. Distribution
packages should install the library into the final system prefix, remove the
`-Wl,-rpath,${libdir}` flag from `libantfly.pc` for C/Go consumers, and configure
application binaries according to the distribution's loader policy.

## Testing

- `cargo test -p antfly-embedded-sys` (no feature) runs only pure, non-linking
  tests: `tests/header_drift.rs` parses `antfly.h` and this crate's own
  source text (via `include_str!`, at compile time) to check that every
  declared function exists in the header with a matching parameter count --
  it never references an `extern "C"` item, so it needs no dylib.
- `cargo test -p antfly-embedded-sys --features libantfly` additionally runs
  `tests/abi_sizes.rs`, which links against the real library and checks that
  this crate's `#[repr(C)]` `antfly_open_options`/`antfly_inference_options`/
  `antfly_inference_pull_progress` structs (sizes and field offsets) agree
  with `antfly_open_options_size()`/`antfly_inference_options_size()` and
  the header's documented field order.

## ABI coverage

This crate declares every function in `antfly.h`, plus every
`ANTFLY_*`/`antfly_*` constant and `#[repr(C)]` type the header defines --
not just the subset `antfly-embedded`'s safe API currently wraps. This is ABI
version 2: `antfly_db` is a typed opaque handle (`*mut antfly_db`, not
`*mut c_void`); `antfly_*` functions are library-level and take no handle,
`antfly_db_*` functions take an `antfly_db` handle of any storage kind
(`.aflite` file or a normal Antfly directory, selected by
`antfly_open_options.storage_kind`), `antfly_lite_*` functions are
`.aflite` file-format operations plus shortcuts for opening one, and
`antfly_inference_*` functions take an `antfly_inference` handle -- a
separate, database-less embedded inference runtime opened with
`antfly_inference_open` (see "Embedded inference without a database" in
`antfly.h` and `zig/CAPI.md`'s "Inference" section) and closed with
`antfly_inference_close`. There is a single `antfly_open_options` struct
(no more separate `antfly_lite_open_options`).

Two callback-taking calls can be cancelled by their callback returning
`false`, reported as the `ANTFLY_CANCELLED` (10) error code:
`antfly_inference_pull_json`'s `antfly_inference_pull_progress_fn` (`true`
continues, `false` cancels the download) and
`antfly_inference_generate_stream_json`'s `antfly_inference_stream_fn`
(`true` continues, `false` stops generation). Both callbacks are called
synchronously on the calling thread and are a rendezvous -- the download/
generation waits for the callback to return before continuing -- so a
`false` return always takes effect at that call, even at the very last
report or chunk.
