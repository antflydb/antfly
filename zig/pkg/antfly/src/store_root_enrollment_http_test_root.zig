// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
test {
    _ = @import("api/store_root_enrollment_http.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
