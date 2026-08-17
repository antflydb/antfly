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

pub const Source = enum {
    cli,
    canonical_environment,
    compatibility_environment,
    automatic,
};

pub const Resolution = struct {
    limit_bytes: usize,
    source: Source,
};

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
    return (try resolveDetailed(
        cli_mib,
        canonical_env_value,
        compatibility_env_value,
    )).limit_bytes;
}

pub fn resolveDetailed(
    cli_mib: ?usize,
    canonical_env_value: ?[]const u8,
    compatibility_env_value: ?[]const u8,
) !Resolution {
    if (cli_mib) |value| return .{
        .limit_bytes = try mibToBytes(value),
        .source = .cli,
    };
    if (canonical_env_value) |raw| return .{
        .limit_bytes = try mibToBytes(
            std.fmt.parseUnsigned(usize, raw, 10) catch return error.InvalidArguments,
        ),
        .source = .canonical_environment,
    };
    if (compatibility_env_value) |raw| return .{
        .limit_bytes = try mibToBytes(
            std.fmt.parseUnsigned(usize, raw, 10) catch return error.InvalidArguments,
        ),
        .source = .compatibility_environment,
    };
    return .{ .limit_bytes = 0, .source = .automatic };
}

test "process memory budget resolution preserves precedence and explicit zero" {
    try std.testing.expectEqual(@as(usize, 0), try resolve(0, "invalid", "14000"));
    try std.testing.expectEqual(@as(usize, 12 * 1024 * 1024), try resolve(null, "12", "14"));
    try std.testing.expectEqual(@as(usize, 14 * 1024 * 1024), try resolve(null, null, "14"));
    try std.testing.expectError(error.InvalidArguments, resolve(null, "invalid", null));
    try std.testing.expectError(error.InvalidArguments, resolve(std.math.maxInt(usize), null, null));

    try std.testing.expectEqual(
        Source.cli,
        (try resolveDetailed(0, "12", "14")).source,
    );
    try std.testing.expectEqual(
        Source.canonical_environment,
        (try resolveDetailed(null, "12", "14")).source,
    );
    try std.testing.expectEqual(
        Source.compatibility_environment,
        (try resolveDetailed(null, null, "14")).source,
    );
    try std.testing.expectEqual(
        Source.automatic,
        (try resolveDetailed(null, null, null)).source,
    );
}
