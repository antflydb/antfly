// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");
const process_memory = @import("antfly_platform").process_memory;

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

pub const EffectiveSource = enum {
    explicit,
    cgroup_v2,
    cgroup_v1,
    host,
    unavailable,
};

pub const EffectiveResolution = struct {
    configured_limit_bytes: usize,
    limit_bytes: usize,
    source: Source,
    effective_source: EffectiveSource,
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

fn detectedSource(source: process_memory.EnvelopeSource) EffectiveSource {
    return switch (source) {
        .cgroup_v2 => .cgroup_v2,
        .cgroup_v1 => .cgroup_v1,
        .host => .host,
        .unavailable => .unavailable,
    };
}

/// Resolve one concrete process envelope before storage and inference managers
/// are constructed. A finite kernel envelope always clamps an operator value;
/// zero retains its documented meaning of automatic detection.
pub fn resolveEffectiveDetailed(
    configured: Resolution,
    detected: process_memory.Envelope,
) EffectiveResolution {
    const configured_u64: u64 = @intCast(configured.limit_bytes);
    if (configured_u64 != 0 and
        (detected.limit_bytes == 0 or configured_u64 <= detected.limit_bytes))
    {
        return .{
            .configured_limit_bytes = configured.limit_bytes,
            .limit_bytes = configured.limit_bytes,
            .source = configured.source,
            .effective_source = .explicit,
        };
    }
    return .{
        .configured_limit_bytes = configured.limit_bytes,
        .limit_bytes = std.math.cast(usize, detected.limit_bytes) orelse std.math.maxInt(usize),
        .source = configured.source,
        .effective_source = if (detected.limit_bytes == 0) .unavailable else detectedSource(detected.source),
    };
}

pub fn resolveSystemDetailed(
    cli_mib: ?usize,
    canonical_env_value: ?[]const u8,
    compatibility_env_value: ?[]const u8,
) !EffectiveResolution {
    return resolveEffectiveDetailed(
        try resolveDetailed(cli_mib, canonical_env_value, compatibility_env_value),
        process_memory.systemEnvelope(),
    );
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

test "effective process memory envelope clamps explicit input and resolves automatic" {
    const detected = process_memory.Envelope{
        .limit_bytes = 8 * 1024 * 1024,
        .source = .cgroup_v2,
    };
    const explicit_smaller = resolveEffectiveDetailed(
        .{ .limit_bytes = 4 * 1024 * 1024, .source = .cli },
        detected,
    );
    try std.testing.expectEqual(@as(usize, 4 * 1024 * 1024), explicit_smaller.limit_bytes);
    try std.testing.expectEqual(EffectiveSource.explicit, explicit_smaller.effective_source);

    const explicit_larger = resolveEffectiveDetailed(
        .{ .limit_bytes = 16 * 1024 * 1024, .source = .canonical_environment },
        detected,
    );
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), explicit_larger.limit_bytes);
    try std.testing.expectEqual(EffectiveSource.cgroup_v2, explicit_larger.effective_source);
    try std.testing.expectEqual(@as(usize, 16 * 1024 * 1024), explicit_larger.configured_limit_bytes);

    const automatic = resolveEffectiveDetailed(
        .{ .limit_bytes = 0, .source = .automatic },
        .{ .limit_bytes = 32 * 1024 * 1024, .source = .host },
    );
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024), automatic.limit_bytes);
    try std.testing.expectEqual(EffectiveSource.host, automatic.effective_source);
}
