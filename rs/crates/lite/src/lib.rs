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

//! Safe Rust binding for Antfly Lite, the embedded `libantfly` C ABI.
//!
//! This crate mirrors the reference Go binding (`go/pkg/lite`) idiomatically:
//! [`Database`] is a `Send + Sync` handle safe for concurrent use from any
//! thread (libantfly's only threading mode is "serialized" -- see
//! [`threading_mode`] and `zig/CAPI.md`'s "Thread Safety" section). Every
//! fallible call returns a stable [`Error`] carrying the C ABI error code.
//!
//! Most operations are exposed as `*_json` methods that take `impl
//! AsRef<[u8]>` request bytes and return raw `Vec<u8>` response bytes,
//! matching the wire-level JSON contract shared with the Antfly server API
//! and the other language bindings. With the default-on `serde` feature,
//! typed convenience wrappers ([`Status`], [`Capabilities`], [`CheckReport`],
//! ...) are layered on top and re-exported here (see [`Status`] and
//! [`CheckReport`], for example).
//!
//! Calling into the real `libantfly` library requires the `libantfly`
//! Cargo feature (see `antfly-lite-sys`'s docs); without it this crate still
//! builds (as an rlib) and its pure types (`Error`, `OpenOptions`, ...) are
//! fully usable.

mod db;
mod error;
mod ffi;
mod files;
mod options;
mod transactions;

#[cfg(feature = "serde")]
mod maintenance;
#[cfg(feature = "serde")]
mod status;

pub use db::{
    Database, MIN_THREAD_STACK_SIZE, SUPPORTED_ABI_VERSION, THREADING_SERIALIZED, abi_version,
    decode_artifact_id_json, open_options_size, threading_mode, validate_abi,
};
pub use error::{Error, Result};
pub use files::{check_file_json, copy_stable_snapshot_file_json, restore, restore_file};
pub use options::{
    GraphDirection, OpenMode, OpenOptions, Profile, RestoreOptions, Storage, TtlCleanupOptions,
    WriteIntent, inference_mode,
};
pub use transactions::{TxnId, TxnStatus};

#[cfg(feature = "serde")]
pub use maintenance::{
    CheckReport, CompactReport, StableSnapshotReport, VacuumReport, check_file,
    copy_stable_snapshot_file,
};
#[cfg(feature = "serde")]
pub use status::{
    Capabilities, InferenceStatus, PendingWorkStatus, ReplayGeneratedEnrichmentsResult, Status,
    StorageStatus, TypedError, TypedResult,
};
