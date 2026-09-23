//! Typed convenience wrappers around the raw `*_json` status/capabilities
//! calls. Only compiled with the (default-on) `serde` feature. Mirrors
//! `go/pkg/lite/status.go`.

use std::fmt;

use serde::Deserialize;

use crate::db::Database;
use crate::error::Error;

/// Either an FFI error or a JSON decode failure from a typed convenience
/// method. The raw `*_json` methods on [`Database`] never produce this;
/// they return [`crate::Result`].
#[derive(Debug)]
pub enum TypedError {
    Ffi(Error),
    Json(serde_json::Error),
}

impl From<Error> for TypedError {
    fn from(err: Error) -> Self {
        TypedError::Ffi(err)
    }
}

impl From<serde_json::Error> for TypedError {
    fn from(err: serde_json::Error) -> Self {
        TypedError::Json(err)
    }
}

impl fmt::Display for TypedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypedError::Ffi(err) => fmt::Display::fmt(err, f),
            TypedError::Json(err) => write!(f, "invalid Lite JSON response: {err}"),
        }
    }
}

impl std::error::Error for TypedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TypedError::Ffi(err) => Some(err),
            TypedError::Json(err) => Some(err),
        }
    }
}

/// A `Result` whose error is [`TypedError`], used by the typed convenience
/// methods (as opposed to [`crate::Result`], used by the raw `*_json`
/// methods).
pub type TypedResult<T> = std::result::Result<T, TypedError>;

/// Describes the physical Lite database file backing a handle.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageStatus {
    pub format: String,
    pub engine: String,
    pub primary_layout: String,
    pub replay_layout: String,
    pub index_layout: String,
    #[serde(default)]
    pub index_namespace: Option<String>,
    #[serde(default)]
    pub format_version: Option<u32>,
    #[serde(default)]
    pub page_size: Option<u32>,
    #[serde(default)]
    pub active_checkpoint: Option<u8>,
    #[serde(default)]
    pub checkpoint_sequence: Option<u64>,
    #[serde(default)]
    pub page_count: Option<u64>,
}

/// Reports the configured inference execution mode for a Lite handle. See
/// `go/pkg/lite/status.go`'s `InferenceStatus` doc comment for the budget
/// field semantics (0 vs. "resolved" values).
#[derive(Debug, Clone, Deserialize)]
pub struct InferenceStatus {
    pub mode: String,
    pub available_modes: Vec<String>,
    pub configured: bool,
    pub remote_provider_configured: bool,
    pub local_runtime_configured: bool,
    pub local_runtime_available: bool,
    pub caller_supplied_artifacts: bool,
    pub no_inference_configured_ok: bool,
    pub host_budget_mb: u32,
    pub backend_budget_mb: u32,
    pub combined_budget_mb: u32,
    pub kv_budget_mb: u32,
    pub scratch_budget_mb: u32,
    pub process_memory_budget_mb: u32,
    pub process_memory_limit_bytes: u64,
    pub process_memory_limit_source: String,
}

/// Describes the Lite feature contract advertised by a handle.
#[derive(Debug, Clone, Deserialize)]
pub struct Capabilities {
    pub freestanding_build: bool,
    pub threading: String,
    pub hosted_profile: bool,
    pub manual_maintenance: bool,
    pub background_enrichment_runtime: bool,
    pub ttl_cleanup_runtime: bool,
    pub transaction_recovery_runtime: bool,
    pub local_template_rendering: bool,
    pub remote_template_rendering: bool,
    pub remote_template_host_callbacks: bool,
    pub inference_mode: String,
    pub supported_inference_modes: Vec<String>,
    pub available_inference_modes: Vec<String>,
    pub inference_required: bool,
    pub no_inference_configured_ok: bool,
    pub caller_supplied_artifacts: bool,
    pub caller_supplied_embeddings: bool,
    pub remote_inference_providers: bool,
    pub local_inference_runtime: bool,
    pub generated_enrichment_planning: bool,
    pub text_search: bool,
    pub dense_vector_search: bool,
    pub sparse_vector_search: bool,
    pub hybrid_search: bool,
    pub graph_search: bool,
    pub distributed_shard_ownership: bool,
    pub raft_replication: bool,
    pub cluster_placement: bool,
    pub cross_node_joins: bool,
    pub remote_shard_fanout: bool,
    pub distributed_transaction_coordination: bool,
    pub cluster_heartbeat_status_aggregation: bool,
    pub server_side_autoscaling: bool,
    pub kubernetes_operator: bool,
    pub object_storage_primary: bool,
}

/// Describes the stable Lite readiness fields for derived work. Nested
/// maintenance telemetry is kept as raw JSON (`serde_json::Value`) because
/// its shape is operational detail rather than a stable control-plane
/// contract.
#[derive(Debug, Clone, Deserialize)]
pub struct PendingWorkStatus {
    pub derived_target_sequence: u64,
    pub has_async_indexes: bool,
    pub portable_import_publication_in_progress: bool,
    pub portable_import_recovery_required: bool,
    pub portable_runtime_activation_pending: bool,
    #[serde(default)]
    pub enrichment: serde_json::Value,
    #[serde(default)]
    pub resolution: serde_json::Value,
    #[serde(default)]
    pub promotion: serde_json::Value,
    #[serde(default)]
    pub text_merge: serde_json::Value,
}

/// The typed form of [`Database::status_json`]. `stats` is kept as raw JSON
/// because its shape is broader than the stable Lite control fields
/// bindings need for feature branching.
#[derive(Debug, Clone, Deserialize)]
pub struct Status {
    pub storage: StorageStatus,
    #[serde(default)]
    pub stats: serde_json::Value,
    pub pending_work: PendingWorkStatus,
    pub inference: InferenceStatus,
    pub capabilities: Capabilities,
}

/// Reports how many generated enrichment references were recreated from
/// stored documents.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplayGeneratedEnrichmentsResult {
    pub replayed: u64,
}

impl Database {
    /// Returns the typed Lite status document for the database.
    pub fn status(&self) -> TypedResult<Status> {
        let body = self.status_json()?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Returns the typed Lite pending-work readiness document.
    pub fn pending_work_stats(&self) -> TypedResult<PendingWorkStatus> {
        let body = self.pending_work_stats_json()?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Drains pending enrichment and index work and returns the typed
    /// post-drain readiness document.
    pub fn run_until_idle_status(&self) -> TypedResult<PendingWorkStatus> {
        let body = self.run_until_idle_json()?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Recreates generated enrichment work from stored documents and
    /// returns the replay count.
    pub fn replay_generated_enrichments(&self) -> TypedResult<ReplayGeneratedEnrichmentsResult> {
        let body = self.replay_generated_enrichments_json()?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Returns the typed Lite capability document for the database.
    pub fn capabilities(&self) -> TypedResult<Capabilities> {
        let body = self.capabilities_json()?;
        Ok(serde_json::from_slice(&body)?)
    }
}
