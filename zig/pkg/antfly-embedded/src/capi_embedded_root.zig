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
pub const database_backup = @import("capi/database_backup.zig");
pub const portable_backup = @import("storage/portable_backup.zig");
pub const storage_maintenance = @import("storage/maintenance.zig");
pub const transactions = @import("storage/transactions.zig");
pub const traversal = @import("graph/traversal.zig");
pub const testing = @import("common/test_directory.zig");
pub const local_write = @import("antfly_source_root").antfly_sources.local_write;
pub const local_query_contract = @import("api/query_execution_contract.zig");
pub const local_query_controls = @import("storage/query_controls.zig");
pub const inference_provider = @import("storage/inference_provider.zig");
pub const public_api = struct {
    pub const batch = @import("api/batch.zig");
    pub const query = @import("api/query.zig");
    pub const tables = @import("api/tables.zig");
    pub const indexes = @import("api/indexes.zig");
};
pub const capi_dependencies = struct {
    pub const storage_db_relational_integrity_topology_contract = @import("storage/db/relational_integrity_topology_contract.zig");
    pub const storage_db_relational_integrity_catalog = @import("storage/db/relational_integrity_catalog.zig");
    pub const storage_db_relational_integrity_activation_contract = @import("storage/db/relational_integrity_activation_contract.zig");
    pub const storage_db_relational_integrity_retirement_contract = @import("storage/db/relational_integrity_retirement_contract.zig");
    pub const schema_relational_declarations = @import("schema/relational_declarations.zig");
    pub const storage_schema = @import("storage/schema.zig");
    pub const sql_catalog = @import("sql/catalog.zig");
    pub const schema_relational_foreign_key_target = @import("schema/relational_foreign_key_target.zig");
    pub const schema_relational_witness_indexes = @import("schema/relational_witness_indexes.zig");
    pub const sql_schema_ddl = @import("sql/schema_ddl.zig");
    pub const sql_ast = @import("sql/ast.zig");
    pub const sql_compiler = @import("sql/compiler.zig");
    pub const sql_mutation_images = @import("sql/mutation_images.zig");
    pub const sql_document_row = @import("sql/document_row.zig");
    pub const storage_row_identity = @import("storage/row_identity.zig");
    pub const api_relational_integrity_commit = @import("api/relational_integrity_commit.zig");
    pub const api_relational_activation_worker = @import("api/relational_activation_worker.zig");
    pub const api_relational_constraint_recovery = @import("api/relational_constraint_recovery.zig");
    pub const api_table_write_source = @import("api/table_write_source.zig");
    pub const sql_conflict_predicate = @import("sql/conflict_predicate.zig");
    pub const api_table_read_source = @import("api/table_read_source.zig");
    pub const api_query_response = @import("api/query_response.zig");
    pub const api_local_query_contract = @import("api/query_execution_contract.zig");
    pub const api_distributed_txn_contract = @import("api/distributed_txn_contract.zig");
    pub const common_topology_records = @import("common/topology_records.zig");
    pub const raft_read_gate = @import("storage/read_consistency.zig");
    pub const sql_errors = @import("sql/errors.zig");
    pub const sql_memory_budget = @import("sql/memory_budget.zig");
    pub const sql_describe = @import("sql/describe.zig");
    pub const sql_scalar = @import("sql/scalar.zig");
    pub const sql_read_stream = @import("sql/read_stream.zig");
    pub const sql_runtime = @import("sql/runtime.zig");
};
