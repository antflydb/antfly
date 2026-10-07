// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

const public_table_http = @import("api/public_table_http.zig");

test "public table HTTP module compiles" {
    _ = public_table_http;
    _ = @import("api/relational_rows.zig");
    _ = @import("api/tables.zig");
    _ = @import("api/fk_generation_publication_coordinator.zig");
    _ = @import("api/fk_initial_create_coordinator.zig");
    _ = @import("api/sql_truncate.zig");
    _ = @import("api/relational_row_merge.zig");
    _ = @import("api/relational_index_mutation.zig");
    _ = @import("api/relational_index_maintenance.zig");
    _ = @import("antfly_local_sources").api_batch;
    _ = @import("api/distributed_txn.zig");
    _ = @import("api/http_route_helpers.zig");
    _ = @import("antfly_local_sources").api_local_query_contract;
    _ = @import("antfly_local_sources").schema_relational_declarations;
    _ = @import("antfly_local_sources").schema_relational_expression;
    _ = @import("api/table_reads.zig");
    _ = @import("api/relational_constraint_status.zig");
    _ = @import("api/relational_index_status.zig");
    _ = @import("storage/hot_standby/mutation_inventory.zig");
    _ = @import("serverless/catalog/storage_capabilities.zig");
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
