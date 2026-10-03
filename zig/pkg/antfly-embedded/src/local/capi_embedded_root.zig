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

//! Dependencies of the public embedded C API and shared local handles.
pub const aggregation = @import("search/aggregation.zig");
pub const db = @import("antfly_source_root").antfly_sources.selected_db;
pub const geo = @import("search/geo.zig");
pub const graph = @import("graph/graph.zig");
pub const graph_pattern = @import("graph/pattern.zig");
pub const graph_query = @import("graph/query.zig");
pub const hbc = @import("storage/hbc_adapter.zig");
pub const managed_embedder = @import("inference/managed_embedder.zig");
pub const lite = @import("storage/lite/mod.zig");
pub const paths = @import("graph/paths.zig");
pub const platform_sync = @import("antfly_platform").sync;
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("storage/portable_backup.zig");
pub const storage_maintenance = @import("storage/maintenance.zig");
pub const transactions = @import("storage/transactions.zig");
pub const traversal = @import("graph/traversal.zig");
pub const testing = @import("common/test_directory.zig");
pub const local_write = @import("antfly_source_root").antfly_sources.local_write;
pub const local_query_contract = @import("api/local_query_contract.zig");
pub const local_query_controls = @import("storage/local_query_controls.zig");
pub const inference_provider = @import("storage/inference_provider.zig");
pub const public_api = struct {
    pub const batch = @import("api/batch.zig");
    pub const query = @import("api/query.zig");
    pub const tables = @import("api/local_tables.zig");
    pub const indexes = @import("api/local_indexes.zig");
};
pub const capi_dependencies = struct {
    pub const sql_catalog = @import("sql/catalog.zig");
    pub const sql_ast = @import("sql/ast.zig");
    pub const sql_compiler = @import("sql/compiler.zig");
    pub const sql_mutation_images = @import("sql/mutation_images.zig");
    pub const sql_document_row = @import("sql/document_row.zig");
    pub const storage_row_identity = @import("storage/row_identity.zig");
    pub const api_relational_integrity_commit = @import("api/relational_integrity_commit.zig");
    pub const sql_conflict_predicate = @import("sql/conflict_predicate.zig");
    pub const api_table_read_source = @import("api/table_read_source.zig");
    pub const api_query_response = @import("api/query_response.zig");
    pub const api_local_query_contract = @import("api/local_query_contract.zig");
    pub const api_distributed_txn_contract = @import("api/distributed_txn_contract.zig");
    pub const common_topology_records = @import("common/topology_records.zig");
    pub const raft_read_gate = @import("storage/read_consistency.zig");
    pub const sql_errors = @import("sql/errors.zig");
    pub const sql_memory_budget = @import("sql/memory_budget.zig");
    pub const sql_runtime = @import("sql/runtime.zig");
};
