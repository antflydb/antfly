// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Actual packed text provider IO, speculative joins and protected control.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("api/lake_index_seekable_text.zig");
}
