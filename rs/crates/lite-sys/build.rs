//! Link-directive plumbing for `antfly-lite-sys`.
//!
//! This crate declares `links = "antfly"` so Cargo enforces at most one copy
//! of `libantfly` gets linked into a given build graph, but the actual
//! `-lantfly`/`-L`/rpath directives are only emitted when the `libantfly`
//! feature is enabled. Building the crate (or its dependents) as a plain
//! rlib, and running tests that don't opt into `libantfly`, must never
//! require the dylib to be present.
use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=ANTFLY_LIB_DIR");
    println!("cargo:rerun-if-changed=build.rs");

    // Only wire up linking when a consumer actually asked for it. Without
    // this, `cargo test --workspace` (no features) would fail to link any
    // test binary in this build graph if the dylib search directory does not
    // exist, even though no pure test calls into libantfly.
    if env::var_os("CARGO_FEATURE_LIBANTFLY").is_none() {
        return;
    }

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo"));
    let default_lib_dir = manifest_dir.join("../../../zig/zig-out/lib");

    let lib_dir = match env::var_os("ANTFLY_LIB_DIR") {
        Some(dir) => PathBuf::from(dir),
        None if default_lib_dir.is_dir() => default_lib_dir,
        None => {
            panic!(
                "antfly-lite-sys: the `libantfly` feature is enabled but no libantfly \
                 library directory was found. Set ANTFLY_LIB_DIR to the directory \
                 containing libantfly's dylib/so, or build it at {} (`zig build` in \
                 zig/).",
                default_lib_dir.display()
            );
        }
    };

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=antfly");

    // Bake an rpath into test/example binaries so they find the dylib at
    // runtime without the caller having to set DYLD_LIBRARY_PATH/
    // LD_LIBRARY_PATH.
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" || target_os == "linux" {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }
}
