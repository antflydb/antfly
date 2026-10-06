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

test {
    _ = @import("storage/hot_standby/restore_staging_integration_test.zig");
    _ = @import("storage/hot_standby/native_topology_receipt_integration_test.zig");
    _ = @import("storage/hot_standby/online_source_integration_test.zig");
    _ = @import("storage/hot_standby/db_integration_test.zig");
    _ = @import("vopr/index_maintenance.zig");
    _ = @import("antfly_source_root").antfly_sources.physical_db;
    _ = @import("antfly_local_sources").graph_query;
    _ = @import("antfly_local_sources").storage_db_graph_runtime;
    _ = @import("antfly_local_sources").storage_db_primary_effect;
    _ = @import("storage/db_split_vopr.zig");
    _ = @import("antfly_local_sources").storage_db_promotion_runtime;
    _ = @import("antfly_local_sources").storage_db_resolution_runtime;
    _ = @import("antfly_local_sources").storage_db_relational_index_catalog;
    _ = @import("antfly_local_sources").storage_db_relational_index_gc;
}

pub const antfly_sources = struct {
    pub const physical_db = @import("antfly_local_sources").storage_db_db;
    pub const selected_db = @import("antfly_local_sources").storage_db_mod;
};

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
