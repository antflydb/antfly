// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Link-directive plumbing for `antfly-embedded-sys`.
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
    // Track discovery settings even when ANTFLY_LIB_DIR bypasses pkg-config.
    for name in [
        "PKG_CONFIG_PATH",
        "PKG_CONFIG_LIBDIR",
        "PKG_CONFIG_SYSROOT_DIR",
        "PKG_CONFIG",
        "PKG_CONFIG_ALLOW_CROSS",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
        for prefix in ["HOST", "TARGET"] {
            println!("cargo:rerun-if-env-changed={prefix}_{name}");
        }
        if let Ok(target) = env::var("TARGET") {
            println!("cargo:rerun-if-env-changed={name}_{target}");
            println!(
                "cargo:rerun-if-env-changed={name}_{}",
                target.replace('-', "_")
            );
        }
    }
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

    let lib_dirs = if let Some(dir) = env::var_os("ANTFLY_LIB_DIR") {
        vec![PathBuf::from(dir)]
    } else if let Ok(library) = pkg_config::Config::new()
        .cargo_metadata(false)
        .probe("libantfly")
    {
        library.link_paths
    } else if env::var_os("HOST") == env::var_os("TARGET") && default_lib_dir.is_dir() {
        vec![default_lib_dir.clone()]
    } else {
        panic!(
            "antfly-embedded-sys: install the Apache antfly-embedded archive and add its lib/pkgconfig directory to PKG_CONFIG_PATH, or set ANTFLY_LIB_DIR to its lib directory. Source builds use `zig build capi` in zig/."
        );
    };
    for lib_dir in &lib_dirs {
        let target = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        let filename = match target.as_str() {
            "macos" => "libantfly.dylib",
            "windows" => "antfly.dll",
            _ => "libantfly.so",
        };
        assert!(
            lib_dir.join(filename).is_file(),
            "libantfly not found in {}",
            lib_dir.display()
        );
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        if target == "macos" || target == "linux" {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
    }
    println!(
        "cargo:lib_dirs={}",
        env::join_paths(&lib_dirs)
            .expect("valid library paths")
            .to_string_lossy()
    );
    println!("cargo:rustc-link-lib=dylib=antfly");
}
