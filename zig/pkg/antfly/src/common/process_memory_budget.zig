// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");

pub const canonical_env = "ANTFLY_PROCESS_MEMORY_BUDGET_MB";
pub const inference_compat_env = "ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB";

pub fn mibToBytes(value: usize) !usize {
    return std.math.mul(usize, value, 1024 * 1024) catch error.InvalidArguments;
}

/// Resolve operator input in strict precedence order. Optional CLI state is
/// intentionally preserved so an explicit zero can disable an inherited
/// environment value and request automatic cgroup/host detection.
pub fn resolve(
    cli_mib: ?usize,
    canonical_env_value: ?[]const u8,
    compatibility_env_value: ?[]const u8,
) !usize {
    const value_mib = cli_mib orelse blk: {
        const raw = canonical_env_value orelse compatibility_env_value orelse return 0;
        break :blk std.fmt.parseUnsigned(usize, raw, 10) catch return error.InvalidArguments;
    };
    return mibToBytes(value_mib);
}

test "process memory budget resolution preserves precedence and explicit zero" {
    try std.testing.expectEqual(@as(usize, 0), try resolve(0, "invalid", "14000"));
    try std.testing.expectEqual(@as(usize, 12 * 1024 * 1024), try resolve(null, "12", "14"));
    try std.testing.expectEqual(@as(usize, 14 * 1024 * 1024), try resolve(null, null, "14"));
    try std.testing.expectError(error.InvalidArguments, resolve(null, "invalid", null));
    try std.testing.expectError(error.InvalidArguments, resolve(std.math.maxInt(usize), null, null));
}
