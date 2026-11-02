// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
test {
    _ = @import("api/relational_activation_worker.zig");
    @import("std").testing.refAllDecls(@import("api/relational_constraint_recovery.zig"));
}
