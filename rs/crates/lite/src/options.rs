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

//! Options for opening/creating an Antfly Lite database. Mirrors
//! `go/pkg/lite`'s `OpenOptions`/`TTLCleanupOptions`.

use std::time::Duration;

use antfly_lite_sys as sys;

/// Selects how a database is stored. The default is a single-file `.aflite`
/// database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Storage {
    /// A single-file `.aflite` database.
    #[default]
    Lite,
    /// A normal single-node Antfly directory.
    Directory,
}

impl Storage {
    pub(crate) fn as_u32(self) -> u32 {
        match self {
            Storage::Lite => sys::ANTFLY_STORAGE_KIND_LITE,
            Storage::Directory => sys::ANTFLY_STORAGE_KIND_DIRECTORY,
        }
    }
}

/// Controls how an Antfly Lite file is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenMode {
    #[default]
    Writer,
    Readonly,
    StatusOnly,
}

impl OpenMode {
    pub(crate) fn as_u32(self) -> u32 {
        match self {
            OpenMode::Writer => sys::ANTFLY_OPEN_MODE_WRITER,
            OpenMode::Readonly => sys::ANTFLY_OPEN_MODE_READONLY,
            OpenMode::StatusOnly => sys::ANTFLY_OPEN_MODE_STATUS_ONLY,
        }
    }
}

/// Selects the Lite runtime profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Profile {
    #[default]
    Native,
    Hosted,
}

impl Profile {
    pub(crate) fn as_u32(self) -> u32 {
        match self {
            Profile::Native => sys::ANTFLY_PROFILE_NATIVE,
            Profile::Hosted => sys::ANTFLY_PROFILE_HOSTED,
        }
    }
}

/// Inference execution mode strings returned by Lite status and
/// capabilities documents. Kept as `&'static str` constants (not an enum)
/// because the C ABI defines them as stable strings, not a closed set of
/// integers.
pub mod inference_mode {
    use antfly_lite_sys as sys;

    pub const CALLER_SUPPLIED_OR_DISABLED: &str =
        sys::ANTFLY_INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED;
    pub const CALLER_SUPPLIED_ARTIFACTS: &str =
        sys::ANTFLY_INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS;
    pub const REMOTE_PROVIDER: &str = sys::ANTFLY_INFERENCE_MODE_REMOTE_PROVIDER;
    pub const LOCAL_EMBEDDED: &str = sys::ANTFLY_INFERENCE_MODE_LOCAL_EMBEDDED;
    pub const MANUAL_MAINTENANCE: &str = sys::ANTFLY_INFERENCE_MODE_MANUAL_MAINTENANCE;
    pub const DISABLED_DEFERRED: &str = sys::ANTFLY_INFERENCE_MODE_DISABLED_DEFERRED;
}

/// Configures the optional Lite TTL cleanup runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TtlCleanupOptions {
    pub enabled: bool,
    pub lease_owned: bool,
    pub owner_id: String,
    pub lease_ttl_ms: u64,
    pub interval_ms: u64,
    pub batch_size: u32,
    pub grace_period_ns: u64,
}

/// Configures [`crate::Database::open`] and [`crate::Database::create`].
///
/// `busy_timeout`, like `sqlite3_busy_timeout`, keeps retrying a writer open
/// while another process or handle holds the database's writer lock.
/// `None` (or `Some(Duration::ZERO)`) fails immediately with
/// [`crate::Error::Busy`]. The C ABI takes whole milliseconds, so a
/// sub-millisecond duration is rounded up.
///
/// `host_budget_mb`, `backend_budget_mb`, `combined_budget_mb`,
/// `kv_budget_mb`, `scratch_budget_mb`, and `process_memory_budget_mb` are
/// explicit resource-budget overrides for the local embedded inference
/// runtime (0 means the embedded node's own host-clamped default). They are
/// only consulted when `local_runtime_configured` is set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenOptions {
    /// Selects a `.aflite` file (the default) or a directory. Directory
    /// storage is created by opening a missing path; [`crate::Database::create`]
    /// only creates `.aflite` files.
    pub storage: Storage,
    pub mode: OpenMode,
    pub profile: Profile,
    pub no_sync: bool,
    pub remote_provider_configured: bool,
    pub local_runtime_configured: bool,
    pub generated_enrichment_replay: bool,
    pub map_size: u64,
    pub ttl_cleanup: Option<TtlCleanupOptions>,
    pub host_budget_mb: u32,
    pub backend_budget_mb: u32,
    pub combined_budget_mb: u32,
    pub kv_budget_mb: u32,
    pub scratch_budget_mb: u32,
    pub process_memory_budget_mb: u32,
    pub busy_timeout: Option<Duration>,
}

impl OpenOptions {
    /// Starts a builder with all defaults (writer/native, no timeout, no TTL
    /// cleanup, automatic inference budgets).
    pub fn new() -> Self {
        Self::default()
    }

    pub fn storage(mut self, storage: Storage) -> Self {
        self.storage = storage;
        self
    }

    pub fn mode(mut self, mode: OpenMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn profile(mut self, profile: Profile) -> Self {
        self.profile = profile;
        self
    }

    pub fn no_sync(mut self, no_sync: bool) -> Self {
        self.no_sync = no_sync;
        self
    }

    pub fn remote_provider_configured(mut self, configured: bool) -> Self {
        self.remote_provider_configured = configured;
        self
    }

    pub fn local_runtime_configured(mut self, configured: bool) -> Self {
        self.local_runtime_configured = configured;
        self
    }

    pub fn generated_enrichment_replay(mut self, replay: bool) -> Self {
        self.generated_enrichment_replay = replay;
        self
    }

    pub fn map_size(mut self, map_size: u64) -> Self {
        self.map_size = map_size;
        self
    }

    pub fn ttl_cleanup(mut self, ttl_cleanup: TtlCleanupOptions) -> Self {
        self.ttl_cleanup = Some(ttl_cleanup);
        self
    }

    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = Some(timeout);
        self
    }

    pub fn host_budget_mb(mut self, mb: u32) -> Self {
        self.host_budget_mb = mb;
        self
    }

    pub fn backend_budget_mb(mut self, mb: u32) -> Self {
        self.backend_budget_mb = mb;
        self
    }

    pub fn combined_budget_mb(mut self, mb: u32) -> Self {
        self.combined_budget_mb = mb;
        self
    }

    pub fn kv_budget_mb(mut self, mb: u32) -> Self {
        self.kv_budget_mb = mb;
        self
    }

    pub fn scratch_budget_mb(mut self, mb: u32) -> Self {
        self.scratch_budget_mb = mb;
        self
    }

    pub fn process_memory_budget_mb(mut self, mb: u32) -> Self {
        self.process_memory_budget_mb = mb;
        self
    }

    /// Rounds `busy_timeout` up to whole milliseconds, like the Go binding.
    /// Returns 0 (fail immediately) when unset or zero. Exposed so callers
    /// can see exactly what will be sent to the C ABI.
    pub fn busy_timeout_ms(&self) -> u64 {
        match self.busy_timeout {
            Some(d) if !d.is_zero() => {
                let nanos = d.as_nanos();
                let ms = nanos.div_ceil(1_000_000);
                u64::try_from(ms).unwrap_or(u64::MAX)
            }
            _ => 0,
        }
    }
}

/// Configures [`crate::restore`] and [`crate::restore_file`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreOptions {
    /// Selects the kind of database created at the destination: a `.aflite`
    /// file (the default) or a directory.
    pub storage: Storage,
    /// Atomically replaces an existing destination.
    pub replace: bool,
}

/// Direction for [`crate::Database::edges_json`] and
/// [`crate::Database::neighbors_json`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GraphDirection {
    #[default]
    Out,
    In,
    Both,
}

impl GraphDirection {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            GraphDirection::Out => sys::ANTFLY_GRAPH_DIRECTION_OUT,
            GraphDirection::In => sys::ANTFLY_GRAPH_DIRECTION_IN,
            GraphDirection::Both => sys::ANTFLY_GRAPH_DIRECTION_BOTH,
        }
    }
}

/// A single key/value write or delete in a Lite batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteIntent {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub delete: bool,
}

impl WriteIntent {
    /// A write that stores `value` at `key`.
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        WriteIntent {
            key: key.into(),
            value: value.into(),
            delete: false,
        }
    }

    /// A write that deletes `key`.
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        WriteIntent {
            key: key.into(),
            value: Vec::new(),
            delete: true,
        }
    }
}
