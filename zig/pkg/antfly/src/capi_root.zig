// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Focused dependency surface for the compiled storage kernel C ABI.
//! Keep this list aligned with production references in `db.zig`; importing the
//! broad public root would reintroduce unrelated command and test surfaces.

pub const aggregation = @import("antfly_local_sources").search_aggregation;
pub const backup_codec = @import("antfly_local_sources").storage_backup_codec;
pub const vector_migration = @import("antfly_local_sources").common_vector_migration;
pub const common_config = @import("antfly_local_sources").common_config;
pub const common_secrets = @import("antfly_local_sources").common_secrets;
pub const data_snapshot = @import("data/storage/shard_state_store.zig");
pub const data_raft_apply = @import("data/storage/raft_apply_store.zig");
pub const data_raft_projection_wire = @import("storage/data_raft_projection_wire.zig");
pub const common = @import("common/mod.zig");
pub const db = @import("antfly_source_root").antfly_sources.selected_db;
pub const geo = @import("antfly_local_sources").search_geo;
pub const graph = @import("antfly_local_sources").graph_graph;
pub const graph_pattern = @import("antfly_local_sources").graph_pattern;
pub const graph_query = @import("antfly_local_sources").graph_query;
pub const hot_standby_seed_activation = @import("storage/hot_standby/seed_activation.zig");
pub const hot_standby_seed_snapshot = @import("storage/hot_standby/seed_snapshot.zig");
pub const hot_standby_validation = @import("storage/hot_standby/validation.zig");
pub const hbc = @import("antfly_local_sources").storage_hbc_adapter;
pub const managed_embedder = @import("antfly_local_sources").inference_managed_embedder;
pub const lite = @import("antfly_local_sources").storage_lite_mod;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
pub const kernel_wal_owner = @import("storage/kernel_wal_owner.zig");
pub const metadata_raft_apply = @import("metadata/storage/raft_apply_store.zig");
pub const metadata_table_manager = @import("metadata/table_manager.zig");
pub const metadata_table_provisioner = @import("metadata/table_provisioner.zig");
pub const paths = @import("antfly_local_sources").graph_paths;
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_sync = @import("antfly_platform").sync;
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("antfly_local_sources").storage_portable_backup;
pub const restore_state_contract = @import("storage/restore_state_contract.zig");
pub const restore_admission = @import("storage/restore_admission.zig");
pub const scraping = @import("antfly_scraping");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const raft_catalog = @import("raft/storage/catalog.zig");
pub const shard = @import("antfly_local_sources").storage_shard;
pub const storage_backend = @import("antfly_local_sources").storage_backend_types;
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const storage_maintenance = @import("antfly_local_sources").storage_maintenance;
pub const transactions = @import("antfly_local_sources").storage_transactions;
pub const traversal = @import("antfly_local_sources").graph_traversal;
pub const testing = @import("antfly_local_sources").common_test_directory;

pub const local_write = @import("antfly_source_root").antfly_sources.local_write;

pub const local_query_contract = @import("antfly_local_sources").api_local_query_contract;
pub const local_query_controls = @import("antfly_local_sources").storage_local_query_controls;

pub const inference_provider = @import("standalone/inference_provider.zig");

pub const physical_resources = @import("storage/physical_resources.zig");

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_storage.zig");
pub const test_error_logs = @import("antfly_test_error_logs");

pub const kernel_runtime_services = @import("storage/kernel_runtime_services.zig");
pub const memory_budget = @import("storage/memory_budget.zig");

pub const capi_dependencies = @import("capi_dependencies.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
