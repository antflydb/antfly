# antfly-lite-sys

Raw, hand-written `extern "C"` declarations for `libantfly`, the stable
embedded Antfly C ABI (`zig/pkg/antfly/include/antfly.h`). There is no
`bindgen`/`libclang` dependency: the header is small and stable enough that a
hand-written mirror is easier to audit, and this crate does not require a C
toolchain to build.

Most applications should depend on `antfly-lite` instead, which wraps this
crate in a safe API. Use `antfly-lite-sys` directly only if you need C ABI
access this crate's safe layer does not yet expose.

## Linking

Building this crate as a library never links against `libantfly` -- only a
test binary, example, or downstream binary that actually calls one of its
`extern "C"` functions does, and only when the `libantfly` Cargo feature is
enabled (`--features libantfly`, or `antfly-lite/libantfly` from a consumer).
Without the feature, `build.rs` does nothing beyond registering the crate's
`links = "antfly"` key.

With the feature enabled, `build.rs` looks for the `libantfly` dylib/so in,
in order:

1. The `ANTFLY_LIB_DIR` environment variable.
2. `<this crate>/../../../zig/zig-out/lib`, i.e. `zig/zig-out/lib` from the
   root of an `antfly` source checkout, if that directory exists.

and emits `-L`/`-l` link directives plus (on macOS and Linux) an `-rpath`
link argument for that directory, so a linked test binary finds the dylib at
runtime without `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`. If neither location
resolves, the build script panics with a message telling you to set
`ANTFLY_LIB_DIR`.

Outside an `antfly` source checkout, install an Antfly release package or
archive containing `lib/libantfly.*`, then set `ANTFLY_LIB_DIR` to that
`lib` directory when building.

## Testing

- `cargo test -p antfly-lite-sys` (no feature) runs only pure, non-linking
  tests: `tests/header_drift.rs` parses `antfly.h` and this crate's own
  source text (via `include_str!`, at compile time) to check that every
  declared function exists in the header with a matching parameter count --
  it never references an `extern "C"` item, so it needs no dylib.
- `cargo test -p antfly-lite-sys --features libantfly` additionally runs
  `tests/abi_sizes.rs`, which links against the real library and checks that
  this crate's `#[repr(C)]` `antfly_open_options`/`antfly_inference_options`/
  `antfly_inference_pull_progress` structs (sizes and field offsets) agree
  with `antfly_open_options_size()`/`antfly_inference_options_size()` and
  the header's documented field order.

## ABI coverage

This crate declares every function in `antfly.h`, plus every
`ANTFLY_*`/`antfly_*` constant and `#[repr(C)]` type the header defines --
not just the subset `antfly-lite`'s safe API currently wraps. This is ABI
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
