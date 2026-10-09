// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Own the physical cache module without resident storage/inference archives.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
comptime {
    _ = @import("storage/lsm_backend/cache.zig");
}
