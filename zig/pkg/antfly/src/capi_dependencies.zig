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

//! C ABI dependencies resolved in the storage owner's module directory.
//! Both the focused kernel root and broad benchmark root share this surface.

pub const relational_expression_errors = @import("antfly_local_sources").schema_relational_expression_errors;
pub const runtime_native_abi = @import("antfly_runtime_abi").native_abi;
pub const runtime_error_abi = @import("antfly_runtime_abi").error_abi;
pub const relational_read_provider = @import("storage/relational_read_provider.zig");
pub const statement_read_fence = @import("antfly_local_sources").storage_statement_read_fence;
pub const storage_db_relational_integrity_catalog = @import("antfly_local_sources").storage_db_relational_integrity_catalog;
pub const storage_db_relational_integrity_activation_contract = @import("antfly_local_sources").storage_db_relational_integrity_activation_contract;
pub const storage_db_relational_integrity_retirement_contract = @import("antfly_local_sources").storage_db_relational_integrity_retirement_contract;
pub const schema_relational_declarations = @import("antfly_local_sources").schema_relational_declarations;
pub const storage_schema = @import("antfly_local_sources").storage_schema;
pub const sql_catalog = @import("antfly_local_sources").sql_catalog;
pub const schema_relational_foreign_key_target = @import("antfly_local_sources").schema_relational_foreign_key_target;
pub const schema_relational_witness_indexes = @import("antfly_local_sources").schema_relational_witness_indexes;
pub const sql_schema_ddl = @import("antfly_local_sources").sql_schema_ddl;
pub const sql_ast = @import("antfly_local_sources").sql_ast;
pub const sql_compiler = @import("antfly_local_sources").sql_compiler;
pub const sql_runtime = @import("antfly_local_sources").sql_runtime;
pub const sql_errors = @import("antfly_local_sources").sql_errors;
pub const sql_memory_budget = @import("antfly_local_sources").sql_memory_budget;
pub const sql_mutation_images = @import("antfly_local_sources").sql_mutation_images;
pub const sql_document_row = @import("antfly_local_sources").sql_document_row;
pub const storage_row_identity = @import("antfly_local_sources").storage_row_identity;
pub const storage_range_protection = @import("antfly_local_sources").storage_range_protection;
pub const storage_coordinated_ttl = @import("antfly_local_sources").storage_coordinated_ttl;
pub const storage_metadata_hot_standby_port = @import("storage/metadata_hot_standby_port.zig");
pub const storage_hot_standby_replication_record = @import("antfly_local_sources").storage_db_replication_record;
pub const storage_docstore = @import("antfly_local_sources").storage_docstore;
pub const metadata_restore_staging = @import("metadata/restore_staging.zig");
pub const metadata_storage_raft_apply_store = @import("metadata/storage/raft_apply_store.zig");
pub const metadata_storage_raft_apply_contract = @import("metadata/storage/raft_apply_contract.zig");
pub const storage_db_restore_staging_contract = @import("antfly_local_sources").storage_db_restore_staging_contract;
pub const storage_db_relational_initial_child_publication = @import("antfly_local_sources").storage_db_relational_initial_child_publication;
pub const storage_source_authority = @import("antfly_local_sources").storage_source_authority;
pub const api_bounded_diagnostic_gate = @import("antfly_local_sources").api_bounded_diagnostic_gate;
pub const storage_db_online_merge_io_contract = @import("antfly_local_sources").storage_db_online_merge_io_contract;
pub const storage_db_online_merge_io = @import("antfly_local_sources").storage_db_online_merge_io;
pub const storage_db_source_artifact_transfer = @import("antfly_local_sources").storage_db_source_artifact_transfer;
pub const storage_db_online_source_contract = @import("antfly_local_sources").storage_db_online_source_contract;
pub const storage_db_native_backup_seal_contract = @import("antfly_local_sources").storage_db_native_backup_seal_contract;
pub const storage_db_backup_pin_control = @import("antfly_local_sources").storage_db_backup_pin_control;
pub const storage_db_native_backup_seal = @import("antfly_local_sources").storage_db_native_backup_seal;
pub const storage_db_relational_integrity_topology_contract = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract;
pub const storage_db_native_raft_snapshot = @import("raft/storage/native_snapshot.zig");
pub const storage_db_doc_identity = @import("antfly_local_sources").storage_db_doc_identity;
pub const api_restore_owner_contract = @import("api/restore_owner_contract.zig");
pub const storage_restore_owner = @import("storage/restore_owner.zig");
pub const api_operation = @import("antfly_local_sources").api_operation;
pub const api_batch = @import("antfly_local_sources").api_batch;
pub const api_relational_integrity_commit = @import("antfly_local_sources").api_relational_integrity_commit;
pub const api_relational_activation_worker = @import("antfly_local_sources").api_relational_activation_worker;
pub const api_relational_constraint_recovery = @import("antfly_local_sources").api_relational_constraint_recovery;
pub const api_table_write_source = @import("antfly_local_sources").api_table_write_source;
pub const sql_conflict_predicate = @import("antfly_local_sources").sql_conflict_predicate;
pub const api_table_read_source = @import("antfly_local_sources").api_table_read_source;
pub const api_query_response = @import("antfly_local_sources").api_query_response;
pub const api_local_query_contract = @import("antfly_local_sources").api_local_query_contract;
pub const api_distributed_txn_contract = @import("antfly_local_sources").api_distributed_txn_contract;
pub const common_topology_records = @import("antfly_local_sources").common_topology_records;
pub const raft_read_gate = @import("raft/read_gate.zig");
pub const storage_db_relational_transition_contract = @import("antfly_local_sources").storage_db_relational_transition_contract;
pub const storage_db_relational_integrity_json = @import("antfly_local_sources").storage_db_relational_integrity_json;

pub const storage_db_replication_ingress = @import("antfly_local_sources").storage_db_replication_ingress;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
