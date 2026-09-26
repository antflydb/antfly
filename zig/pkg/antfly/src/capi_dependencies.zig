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

//! C ABI dependencies resolved in the storage owner's module directory.
//! Both the focused kernel root and broad benchmark root share this surface.

pub const relational_expression_errors = @import("schema/relational_expression_errors.zig");
pub const runtime_native_abi = @import("runtime_native_abi.zig");
pub const runtime_error_abi = @import("runtime_error_abi.zig");
pub const relational_read_provider = @import("storage/relational_read_provider.zig");
pub const statement_read_fence = @import("storage/statement_read_fence.zig");
pub const sql_catalog = @import("sql/catalog.zig");
pub const sql_ast = @import("sql/ast.zig");
pub const sql_compiler = @import("sql/compiler.zig");
pub const sql_runtime = @import("sql/runtime.zig");
pub const sql_errors = @import("sql/errors.zig");
pub const sql_memory_budget = @import("sql/memory_budget.zig");
pub const sql_mutation_images = @import("sql/mutation_images.zig");
pub const sql_document_row = @import("sql/document_row.zig");
pub const storage_row_identity = @import("storage/row_identity.zig");
pub const storage_range_protection = @import("storage/range_protection.zig");
pub const storage_coordinated_ttl = @import("storage/coordinated_ttl.zig");
pub const storage_metadata_ha_port = @import("storage/metadata_ha_port.zig");
pub const storage_hot_standby_replication_record = @import("storage/hot_standby/replication_record.zig");
pub const storage_docstore = @import("storage/docstore.zig");
pub const metadata_restore_staging = @import("metadata/restore_staging.zig");
pub const metadata_storage_raft_apply_store = @import("metadata/storage/raft_apply_store.zig");
pub const metadata_storage_raft_apply_contract = @import("metadata/storage/raft_apply_contract.zig");
pub const storage_db_restore_staging_contract = @import("storage/db/restore_staging_contract.zig");
pub const storage_db_relational_initial_child_publication = @import("storage/db/relational_initial_child_publication.zig");
pub const storage_source_authority = @import("storage/source_authority.zig");
pub const api_bounded_diagnostic_gate = @import("api/bounded_diagnostic_gate.zig");
pub const storage_db_online_merge_io_contract = @import("storage/db/online_merge_io_contract.zig");
pub const storage_db_online_merge_io = @import("storage/db/online_merge_io.zig");
pub const storage_db_source_artifact_transfer = @import("storage/db/source_artifact_transfer.zig");
pub const storage_db_online_source_contract = @import("storage/db/online_source_contract.zig");
pub const storage_db_native_backup_seal_contract = @import("storage/db/native_backup_seal_contract.zig");
pub const api_table_writes = @import("api/table_writes.zig");
pub const storage_db_native_backup_seal = @import("storage/db/native_backup_seal.zig");
pub const storage_db_relational_integrity_topology_contract = @import("storage/db/relational_integrity_topology_contract.zig");
pub const storage_db_native_raft_snapshot = @import("storage/db/native_raft_snapshot.zig");
pub const storage_db_doc_identity = @import("storage/db/doc_identity.zig");
pub const api_restore_owner_contract = @import("api/restore_owner_contract.zig");
pub const storage_restore_owner = @import("storage/restore_owner.zig");
pub const api_operation = @import("api/operation.zig");
pub const api_batch = @import("api/batch.zig");
pub const api_relational_integrity_commit = @import("api/relational_integrity_commit.zig");
pub const sql_conflict_predicate = @import("sql/conflict_predicate.zig");
pub const api_table_read_source = @import("api/table_read_source.zig");
pub const api_query_response = @import("api/query_response.zig");
pub const api_local_query_contract = @import("api/local_query_contract.zig");
pub const api_distributed_txn_contract = @import("api/distributed_txn_contract.zig");
pub const common_topology_records = @import("common/topology_records.zig");
pub const raft_read_gate = @import("raft/read_gate.zig");
pub const storage_db_relational_transition_contract = @import("storage/db/relational_transition_contract.zig");
pub const storage_db_relational_integrity_json = @import("storage/db/relational_integrity_json.zig");
