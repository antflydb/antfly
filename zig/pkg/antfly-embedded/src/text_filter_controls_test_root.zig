// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("search/query.zig");
}
