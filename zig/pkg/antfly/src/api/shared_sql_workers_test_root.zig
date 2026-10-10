// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Actual node SQL worker pools and imported execution capabilities.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("sql_worker_lease.zig");
}
