// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

test {
    _ = @import("metadata/restore_staging.zig");
}

pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
