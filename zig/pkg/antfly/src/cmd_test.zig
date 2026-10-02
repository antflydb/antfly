// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Remote CLI contracts share the product CLI dependency boundary.
pub const antfly_sources = @import("source_owner_common.zig");
test {
    _ = @import("cmd/cli/mod.zig");
    _ = @import("cmd/cli/maintenance.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
