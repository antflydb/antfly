// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

test {
    _ = @import("sql/test_root.zig");
    _ = @import("sql/subquery_shape_test.zig");
    _ = @import("sql/joined_mutation_test.zig");
    _ = @import("system_catalog/policies.zig");
}
