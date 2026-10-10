// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Actual recurring maintenance callback and physical join ownership.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("storage/db/independent_maintenance.zig");
    _ = @import("common/maintenance_scheduler.zig");
    _ = @import("storage/db/graph_cleanup_owner.zig");
    _ = @import("storage/db/quarantine_recovery.zig");
}
