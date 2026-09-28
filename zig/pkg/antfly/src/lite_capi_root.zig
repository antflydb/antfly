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

//! Focused dependency surface for the compiled storage kernel C ABI.
//! Keep this list aligned with production references in `db.zig`; importing the
//! broad public root would reintroduce unrelated command and test surfaces.

pub const aggregation = @import("search/aggregation.zig");
pub const backup_codec = @import("storage/backup_codec.zig");
pub const vector_migration = @import("common/vector_migration.zig");
pub const common_config = @import("common/config.zig");
pub const common_secrets = @import("common/secrets.zig");
pub const data_snapshot = struct {};
pub const data_raft_apply = struct {};
pub const data_raft_projection_wire = @import("storage/data_raft_projection_wire.zig");
pub const db = @import("antfly_source_root").antfly_sources.selected_db;
pub const geo = @import("search/geo.zig");
pub const graph = @import("graph/graph.zig");
pub const graph_pattern = @import("graph/pattern.zig");
pub const graph_query = @import("graph/query.zig");
pub const ha_seed_activation = struct {};
pub const ha_seed_snapshot = struct {};
pub const ha_validation = struct {};
pub const hbc = @import("storage/hbc_adapter.zig");
pub const managed_embedder = @import("inference/managed_embedder.zig");
pub const lite = @import("storage/lite/mod.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const kernel_wal_owner = @import("storage/kernel_wal_owner.zig");
pub const metadata_raft_apply = struct {};
pub const metadata_table_manager = struct {};
pub const metadata_table_provisioner = struct {};
pub const paths = @import("graph/paths.zig");
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_sync = @import("antfly_platform").sync;
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("storage/portable_backup.zig");
pub const restore_state_contract = @import("storage/restore_state_contract.zig");
pub const restore_admission = @import("storage/restore_admission.zig");
pub const scraping = @import("antfly_scraping");
pub const public_api = struct {
    pub const batch = @import("api/batch.zig");
    pub const query = @import("api/query.zig");
    pub const tables = @import("api/local_tables.zig");
    pub const indexes = @import("api/local_indexes.zig");
    pub const distributed_graph = @import("api/local_graph.zig");
    pub const runtime_status = @import("api/runtime_status.zig");
    pub const backups = @import("api/local_backups.zig");
};
pub const raft = struct {
    pub const ReadSafetyBarrier = @import("raft/read_gate.zig").ReadSafetyBarrier;
    pub const FeatureReads = @import("raft/feature_reads.zig").FeatureReads;
};
pub const raft_catalog = struct {};
pub const shard = @import("storage/shard.zig");
pub const storage_backend = @import("storage/backend_types.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const storage_maintenance = @import("storage/maintenance.zig");
pub const transactions = @import("storage/transactions.zig");
pub const traversal = @import("graph/traversal.zig");
pub const testing = @import("common/test_directory.zig");

pub const local_write = @import("antfly_source_root").antfly_sources.local_write;

pub const local_query_contract = @import("api/local_query_contract.zig");
pub const local_query_controls = @import("storage/local_query_controls.zig");

pub const inference_provider = @import("standalone/inference_provider.zig");

pub const physical_resources = @import("storage/physical_resources.zig");

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_storage.zig");
pub const test_error_logs = @import("test_error_logs.zig");

pub const kernel_runtime_services = @import("storage/kernel_runtime_services.zig");
pub const memory_budget = @import("storage/memory_budget.zig");

pub const capi_dependencies = @import("lite_capi_dependencies.zig");
