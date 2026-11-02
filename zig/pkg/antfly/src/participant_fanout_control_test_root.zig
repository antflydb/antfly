// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Independent participant ownership without physical storage fixtures.
pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
test {
    _ = @import("api/distributed_txn.zig");
    _ = @import("api/group_admission_wait.zig");
}
