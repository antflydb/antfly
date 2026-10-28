// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("storage/server_transaction_recovery.zig");
}
