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
    _ = @import("api/tables.zig");
    _ = @import("api/table_contract.zig");
    _ = @import("serverless/build/lake_sidecar_text.zig");
    _ = @import("api/lake_sql_cursor.zig");
    _ = @import("api/lake_index_row_source.zig");
    _ = @import("api/lake_index_publication.zig");
    _ = @import("api/lake_index_store.zig");
    _ = @import("api/lake_index_coordinator.zig");
    _ = @import("api/lake_index_selection.zig");
    _ = @import("api/lake_schema_detection.zig");
    _ = @import("api/lake_table_reads.zig");
    _ = @import("antfly_local_sources").serverless_query_lake_schema;
    _ = @import("antfly_local_sources").schema_mod;
    _ = @import("antfly_local_sources").serverless_query_lake_read_context;
    _ = @import("antfly_local_sources").serverless_query_lake_serving_cache;
}
