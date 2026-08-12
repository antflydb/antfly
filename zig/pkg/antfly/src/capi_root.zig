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

pub const aggregation = @import("search/aggregation.zig");
pub const backup_codec = @import("storage/backup_codec.zig");
pub const common_config = @import("common/config.zig");
pub const common_secrets = @import("common/secrets.zig");
pub const data_snapshot = @import("data/storage/shard_state_store.zig");
pub const data_raft_apply = @import("data/storage/raft_apply_store.zig");
pub const data_raft_projection_wire = @import("storage/data_raft_projection_wire.zig");
pub const db = @import("storage/db/mod.zig");
pub const geo = @import("search/geo.zig");
pub const graph = @import("graph/graph.zig");
pub const graph_pattern = @import("graph/pattern.zig");
pub const graph_query = @import("graph/query.zig");
pub const ha_seed_activation = @import("storage/ha/seed_activation.zig");
pub const hbc = @import("storage/hbc_adapter.zig");
pub const lite = @import("storage/lite/mod.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const kernel_wal_owner = @import("storage/kernel_wal_owner.zig");
pub const metadata_raft_apply = @import("metadata/storage/raft_apply_store.zig");
pub const metadata_table_manager = @import("metadata/table_manager.zig");
pub const metadata_table_provisioner = @import("metadata/table_provisioner.zig");
pub const paths = @import("graph/paths.zig");
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_sync = @import("antfly_platform").sync;
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("storage/portable_backup.zig");
pub const restore_state_contract = @import("storage/restore_state_contract.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const raft_catalog = @import("raft/catalog.zig");
pub const storage_backend = @import("storage/backend_types.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const storage_maintenance = @import("storage/maintenance.zig");
pub const transactions = @import("storage/transactions.zig");
pub const traversal = @import("graph/traversal.zig");
