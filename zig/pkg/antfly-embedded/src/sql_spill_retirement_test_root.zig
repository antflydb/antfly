// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Physical SQL spill admission, file ownership, quotas and cleanup.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("sql/spill.zig");
}
