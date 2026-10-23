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

const storage = @import("metadata/storage/mod.zig");

test {
    _ = storage.raft_apply_store;
    _ = @import("antfly_local_sources").metadata_lake_index_catalog;
}

test {
    _ = @import("metadata/report_collection.zig");
    _ = @import("metadata/fk_generation_publication.zig");
    _ = @import("metadata/relational_index_candidate.zig");
    _ = @import("metadata/relational_index_publication.zig");
    _ = @import("metadata/schema_validation_attestation.zig");
    _ = @import("metadata/schema_validation_publication.zig");
    _ = @import("metadata/schema_validation_plan.zig");
    _ = @import("metadata/schema_validation_ledger.zig");
    _ = @import("metadata/schema_migration_finalization.zig");
    _ = @import("api/tables.zig");
    _ = @import("api/native_retained_cut.zig");
    _ = @import("metadata/relational_index_worker.zig");
    _ = @import("metadata/relational_index_delivery_journal.zig");
    _ = @import("api/relational_index_owner_execution.zig");
    _ = @import("api/relational_index_plan_builder.zig");
    _ = @import("api/index_publication_recovery.zig");
    _ = @import("api/relational_index_admission.zig");
    _ = @import("api/relational_index_claim_delivery_test.zig");
    _ = @import("metadata/fk_initial_retirement_ack.zig");
    _ = @import("system_catalog/operations.zig");
    _ = @import("metadata/schema_publication_shape.zig");
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
