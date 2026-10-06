// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Emits an rpath link-arg for `libantfly`'s directory so that this
//! package's own test binaries (the only targets in this workspace that
//! actually link the dylib) find it at runtime without DYLD_LIBRARY_PATH/
//! LD_LIBRARY_PATH.
//!
//! `antfly-lite-sys`'s build script already emits the `-L`/`-l` directives
//! (those propagate to any dependent binary that links it), but Cargo's
//! `cargo:rustc-link-arg` only applies to targets (bins/tests/examples) in
//! the *emitting* package -- it does not propagate to dependents. Since the
//! `-sys` crate has no such targets, the rpath has to be re-emitted here,
//! in the package that actually produces test binaries.
use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=ANTFLY_LIB_DIR");
    println!("cargo:rerun-if-changed=build.rs");

    if env::var_os("CARGO_FEATURE_LIBANTFLY").is_none() {
        return;
    }

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo"));
    let default_lib_dir = manifest_dir.join("../../../zig/zig-out/lib");

    let lib_dir = match env::var_os("ANTFLY_LIB_DIR") {
        Some(dir) => PathBuf::from(dir),
        None if default_lib_dir.is_dir() => default_lib_dir,
        // antfly-lite-sys's build script already panics with a clear
        // message in this case; nothing more to do here.
        None => return,
    };

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" || target_os == "linux" {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }
}
