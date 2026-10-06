// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

test {
    _ = @import("sql/test_root.zig");
    _ = @import("sql/subquery_shape_test.zig");
    _ = @import("sql/joined_mutation_test.zig");
    _ = @import("antfly_local_sources").system_catalog_policies;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
