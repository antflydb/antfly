// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual physical FK control receipts and schema commit/restart qualification.
pub const antfly_sources = @import("source_owner_embedded_db.zig");

test {
    _ = @import("storage/db/db.zig");
    _ = @import("storage/transactions.zig");
    _ = @import("storage/db/row_policy_schema_pair_test.zig");
    _ = @import("storage/db/relational_integrity_generation_admission.zig");
    _ = @import("storage/db/native_empty_generation.zig");
    _ = @import("api/backups.zig");
    _ = @import("metadata/backup_cohort.zig").consumer_tests;
}
