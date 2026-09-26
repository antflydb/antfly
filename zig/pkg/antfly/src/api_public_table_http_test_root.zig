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

const public_table_http = @import("api/public_table_http.zig");

test {
    _ = public_table_http;
    _ = @import("api/relational_rows.zig");
    _ = @import("api/tables.zig");
    _ = @import("api/fk_generation_publication_coordinator.zig");
    _ = @import("api/fk_initial_create_coordinator.zig");
    _ = @import("api/sql_truncate.zig");
    _ = @import("api/relational_row_merge.zig");
    _ = @import("api/relational_index_mutation.zig");
    _ = @import("api/relational_index_maintenance.zig");
    _ = @import("api/batch.zig");
    _ = @import("api/distributed_txn.zig");
    _ = @import("api/http_route_helpers.zig");
    _ = @import("api/local_query_contract.zig");
    _ = @import("schema/relational_declarations.zig");
    _ = @import("schema/relational_expression.zig");
    _ = @import("api/table_reads.zig");
    _ = @import("api/relational_constraint_status.zig");
    _ = @import("api/relational_index_status.zig");
    _ = @import("storage/hot_standby/mutation_inventory.zig");
    _ = @import("serverless/catalog/storage_capabilities.zig");
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");
