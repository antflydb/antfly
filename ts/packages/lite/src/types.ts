// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import type { Uint64Like } from "./marshal.js";

/** How an Antfly Lite file is opened (antfly_lite_open_mode_* in antfly.h). */
export enum OpenMode {
  Writer = 0,
  Readonly = 1,
  StatusOnly = 2,
}

/** The Lite runtime profile (antfly_lite_profile_* in antfly.h). */
export enum Profile {
  Native = 0,
  Hosted = 1,
}

/** Graph edge traversal direction (antfly_graph_direction_* in antfly.h). */
export enum GraphDirection {
  Out = 0,
  In = 1,
  Both = 2,
}

/** Transaction intent lifecycle state (antfly_txn_status in antfly.h). */
export enum TxnStatus {
  Pending = 0,
  Committed = 1,
  Aborted = 2,
}

/** Inference mode strings returned by Lite status and capabilities JSON. */
export const InferenceMode = {
  CallerSuppliedOrDisabled: "caller_supplied_or_disabled",
  CallerSuppliedArtifacts: "caller_supplied_artifacts",
  RemoteProvider: "remote_provider",
  LocalEmbedded: "local_embedded",
  ManualMaintenance: "manual_maintenance",
  DisabledDeferred: "disabled_deferred",
} as const;

/** The only libantfly threading contract (antfly_threading_mode() return value), like sqlite3_threadsafe(). */
export const THREADING_SERIALIZED = 1;

/** The Antfly C ABI version this binding was written against (antfly_abi_version()). */
export const SUPPORTED_ABI_VERSION = 1;

/** Configures TTL cleanup for a native-profile Lite handle. */
export interface TTLCleanupOptions {
  enabled: boolean;
  leaseOwned: boolean;
  ownerId?: string;
  leaseTtlMs?: Uint64Like;
  intervalMs?: Uint64Like;
  batchSize?: number;
  gracePeriodNs?: Uint64Like;
}

/**
 * Configures open/create. See antfly_lite_open_options in antfly.h and
 * zig/LITE.md's "Local Embedded Inference" section for the inference budget
 * fields. All fields are optional; unset numeric budgets mean "automatic".
 */
export interface OpenOptions {
  mode?: OpenMode;
  profile?: Profile;
  noSync?: boolean;
  remoteProviderConfigured?: boolean;
  localRuntimeConfigured?: boolean;
  generatedEnrichmentReplay?: boolean;
  mapSize?: Uint64Like;
  ttlCleanup?: TTLCleanupOptions;
  /** Milliseconds to retry while another writer holds the writer lock (ANTFLY_BUSY), like sqlite3_busy_timeout. 0 (default) fails immediately. */
  busyTimeoutMs?: Uint64Like;
  inferenceHostBudgetMb?: number;
  inferenceBackendBudgetMb?: number;
  inferenceCombinedBudgetMb?: number;
  inferenceKvBudgetMb?: number;
  inferenceScratchBudgetMb?: number;
  inferenceProcessMemoryBudgetMb?: number;
}

/** A single key/value write or delete in a batch. */
export interface WriteIntent {
  key: string;
  value?: Uint8Array | Record<string, unknown>;
  delete?: boolean;
}

// --- Typed status/capabilities/report shapes (mirrors go/pkg/lite/status.go and maintenance.go) ---

export interface StorageStatus {
  format: string;
  engine: string;
  primary_layout: string;
  replay_layout: string;
  index_layout: string;
  index_namespace?: string;
  format_version?: number;
  page_size?: number;
  active_checkpoint?: number;
  checkpoint_sequence?: number;
  page_count?: number;
}

export interface InferenceStatus {
  mode: string;
  available_modes: string[];
  configured: boolean;
  remote_provider_configured: boolean;
  local_runtime_configured: boolean;
  local_runtime_available: boolean;
  caller_supplied_artifacts: boolean;
  no_inference_configured_ok: boolean;
  host_budget_mb: number;
  backend_budget_mb: number;
  combined_budget_mb: number;
  kv_budget_mb: number;
  scratch_budget_mb: number;
  process_memory_budget_mb: number;
  process_memory_limit_bytes: number;
  process_memory_limit_source: string;
}

export interface Capabilities {
  freestanding_build: boolean;
  /** Always "serialized" for the current libantfly (see THREADING_SERIALIZED). */
  threading: string;
  hosted_profile: boolean;
  manual_maintenance: boolean;
  background_enrichment_runtime: boolean;
  ttl_cleanup_runtime: boolean;
  transaction_recovery_runtime: boolean;
  local_template_rendering: boolean;
  remote_template_rendering: boolean;
  remote_template_host_callbacks: boolean;
  inference_mode: string;
  supported_inference_modes: string[];
  available_inference_modes: string[];
  inference_required: boolean;
  no_inference_configured_ok: boolean;
  caller_supplied_artifacts: boolean;
  caller_supplied_embeddings: boolean;
  remote_inference_providers: boolean;
  local_inference_runtime: boolean;
  generated_enrichment_planning: boolean;
  text_search: boolean;
  dense_vector_search: boolean;
  sparse_vector_search: boolean;
  hybrid_search: boolean;
  graph_search: boolean;
  distributed_shard_ownership: boolean;
  raft_replication: boolean;
  cluster_placement: boolean;
  cross_node_joins: boolean;
  remote_shard_fanout: boolean;
  distributed_transaction_coordination: boolean;
  cluster_heartbeat_status_aggregation: boolean;
  server_side_autoscaling: boolean;
  kubernetes_operator: boolean;
  object_storage_primary: boolean;
}

export interface PendingWorkStatus {
  derived_target_sequence: number;
  has_async_indexes: boolean;
  portable_import_publication_in_progress: boolean;
  portable_import_recovery_required: boolean;
  portable_runtime_activation_pending: boolean;
  enrichment: unknown;
  resolution: unknown;
  promotion: unknown;
  text_merge: unknown;
}

export interface Status {
  storage: StorageStatus;
  stats: unknown;
  pending_work: PendingWorkStatus;
  inference: InferenceStatus;
  capabilities: Capabilities;
}

export interface ReplayGeneratedEnrichmentsResult {
  replayed: number;
}

export interface CheckReport {
  valid: boolean;
  file_size: number;
  valid_prefix_size: number;
  tail_bytes: number;
  record_count: number;
  live_file_count: number;
  live_bytes: number;
  compact_size: number;
  reclaimable_bytes: number;
  issue?: string;
}

export interface VacuumReport {
  before_size: number;
  after_size: number;
  reclaimed_bytes: number;
  live_file_count: number;
  live_bytes: number;
}

export interface CompactReport {
  compacted: boolean;
  vacuum: VacuumReport;
}

export interface StableSnapshotReport {
  source_size: number;
  snapshot_size: number;
  checkpoint_sequence: number;
  page_count: number;
  tail_bytes: number;
}
