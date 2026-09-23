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

//! Typed convenience wrappers around integrity check, vacuum, compact, and
//! stable-snapshot JSON results. Only compiled with the (default-on)
//! `serde` feature. Mirrors `go/pkg/lite/maintenance.go`.

use std::path::Path;

use serde::Deserialize;

use crate::db::Database;
use crate::error::Error;
use crate::ffi::path_has_suffix;
use crate::status::TypedResult;

/// The typed form of [`Database::check_json`] / [`crate::files::check_file_json`].
#[derive(Debug, Clone, Deserialize)]
pub struct CheckReport {
    pub valid: bool,
    pub file_size: u64,
    pub valid_prefix_size: u64,
    pub tail_bytes: u64,
    pub record_count: u64,
    pub live_file_count: u64,
    pub live_bytes: u64,
    pub compact_size: u64,
    pub reclaimable_bytes: u64,
    #[serde(default)]
    pub issue: Option<String>,
}

/// The typed form of [`Database::vacuum_json`].
#[derive(Debug, Clone, Deserialize)]
pub struct VacuumReport {
    pub before_size: u64,
    pub after_size: u64,
    pub reclaimed_bytes: u64,
    pub live_file_count: u64,
    pub live_bytes: u64,
}

/// The typed form of [`Database::compact_json`].
#[derive(Debug, Clone, Deserialize)]
pub struct CompactReport {
    pub compacted: bool,
    pub vacuum: VacuumReport,
}

/// The typed form of [`Database::copy_stable_snapshot_json`].
#[derive(Debug, Clone, Deserialize)]
pub struct StableSnapshotReport {
    pub source_size: u64,
    pub snapshot_size: u64,
    pub checkpoint_sequence: u64,
    pub page_count: u64,
    pub tail_bytes: u64,
}

impl Database {
    /// Runs Lite integrity checks and returns the typed result.
    pub fn check(&self) -> TypedResult<CheckReport> {
        let body = self.check_json()?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Compacts free space and returns the typed result.
    pub fn vacuum(&self) -> TypedResult<VacuumReport> {
        let body = self.vacuum_json()?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Drains maintenance, compacts indexes, vacuums free space, and
    /// returns the typed result.
    pub fn compact(&self) -> TypedResult<CompactReport> {
        let body = self.compact_json()?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Copies a stable Lite snapshot to `dest_path` and returns the typed
    /// result.
    pub fn copy_stable_snapshot(
        &self,
        dest_path: impl AsRef<Path>,
        replace: bool,
    ) -> TypedResult<StableSnapshotReport> {
        let body = self.copy_stable_snapshot_json(dest_path, replace)?;
        Ok(serde_json::from_slice(&body)?)
    }
}

/// Runs Lite integrity checks for `path` without opening a database handle
/// and returns the typed result.
pub fn check_file(path: impl AsRef<Path>) -> TypedResult<CheckReport> {
    let body = crate::files::check_file_json(path)?;
    Ok(serde_json::from_slice(&body)?)
}

/// Opens `src_path` read-only, copies a stable physical `.aflite` snapshot
/// to `dest_path`, and returns the typed snapshot report. Both paths must
/// end in `.aflite`.
pub fn copy_stable_snapshot_file(
    src_path: impl AsRef<Path>,
    dest_path: impl AsRef<Path>,
    replace: bool,
) -> TypedResult<StableSnapshotReport> {
    let src_path = src_path.as_ref();
    let dest_path = dest_path.as_ref();
    if !path_has_suffix(src_path, ".aflite") || !path_has_suffix(dest_path, ".aflite") {
        return Err(Error::InvalidArgument.into());
    }
    let body = crate::files::copy_stable_snapshot_file_json(src_path, dest_path, replace)?;
    Ok(serde_json::from_slice(&body)?)
}
