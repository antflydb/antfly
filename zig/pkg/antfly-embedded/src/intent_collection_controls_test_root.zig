// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Physical transaction snapshots without the complete DB/API implementation.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("storage/transactions.zig");
}
