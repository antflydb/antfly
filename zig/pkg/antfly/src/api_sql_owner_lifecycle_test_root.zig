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

//! Linked-control owner composition for mounted SQL catalog lifecycle.

const sql_primary_key_rewrite_integration_test = @import("api/sql_primary_key_rewrite_integration_test.zig");

// The private online-merge donor port is only installed by the linked-control
// runtime. A physical/monolithic API root cannot exercise this protocol.
pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = @import("metadata/column_sequence_handoff.zig");
    _ = @import("api/transactions.zig");
    _ = @import("api/sql_connections.zig");
    _ = @import("api/sql_sequence_execution.zig");
    _ = @import("api/sql_sequence_cache.zig");
    _ = sql_primary_key_rewrite_integration_test;
    _ = @import("api/sql_catalog.zig");
    _ = @import("api/sql_catalog_campaign_test.zig");
    _ = @import("api/sql_role_defaults_lifecycle_integration_test.zig");
    _ = @import("api/sql_nullability_lifecycle_integration_test.zig");
    _ = @import("api/sql_schema_validation_lifecycle_integration_test.zig");
    _ = @import("api/sql_schema_lifecycle_integration_test.zig");
    _ = @import("api/sql_policy_lifecycle_integration_test.zig");
    _ = @import("api/sql_index_lifecycle_integration_test.zig");
    _ = @import("api/sql_view_lifecycle_integration_test.zig");
    _ = @import("api/sql_view_document_integration_test.zig");
    _ = @import("api/sql_document_alias_integration_test.zig");
    _ = @import("api/sql_document_generated_integration_test.zig");
    _ = @import("api/sql_document_mutation_lifecycle_integration_test.zig");
    _ = @import("api/sql_sequence_lifecycle_integration_test.zig");
    _ = @import("api/sql_document_truncate_lifecycle_integration_test.zig");
    _ = @import("api/sql_expression_index_lifecycle_integration_test.zig");
    _ = @import("api/sql_default_lifecycle_integration_test.zig");
    _ = @import("api/sql_table_creation_integration_test.zig");
    _ = @import("metadata/schema_migration_finalization.zig");
    _ = @import("metadata/relation_publication_fence.zig");
    _ = @import("metadata/relation_reconciliation_worker.zig");
    _ = @import("metadata/http_client.zig");
    _ = @import("metadata/storage/raft_apply_store.zig");
    _ = @import("data/raft_batch.zig").consumer_tests;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
