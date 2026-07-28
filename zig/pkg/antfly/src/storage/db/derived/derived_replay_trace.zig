// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const build_options = @import("build_options");
const tracing = @import("../../../tracing/mod.zig");

pub fn traceId(ptr: *anyopaque, hint: anytype) u128 {
    return (@as(u128, @intFromPtr(ptr)) << 8) | @intFromEnum(hint);
}

pub fn target(ptr: *anyopaque, hint: anytype, from_sequence: u64, target_sequence: u64) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "derived-replay",
        .trace_id = traceId(ptr, hint),
        .name = "ObserveTarget",
        .facts = .{
            .index_name = @tagName(hint),
            .applied_sequence = from_sequence,
            .target_sequence = target_sequence,
        },
    });
}

pub fn begin(ptr: *anyopaque, hint: anytype, from_sequence: u64, target_sequence: u64) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "derived-replay",
        .trace_id = traceId(ptr, hint),
        .name = "BeginCatchUp",
        .facts = .{
            .index_name = @tagName(hint),
            .applied_sequence = from_sequence,
            .target_sequence = target_sequence,
        },
    });
}

pub fn scan(
    ptr: *anyopaque,
    hint: anytype,
    name: []const u8,
    from_sequence: u64,
    stats: anytype,
    fallback_used: bool,
) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "derived-replay",
        .trace_id = traceId(ptr, hint),
        .name = name,
        .facts = .{
            .index_name = @tagName(hint),
            .applied_sequence = from_sequence,
            .last_sequence = stats.last_sequence,
            .scanned_entries = @intCast(stats.scanned_entries),
            .matched_entries = @intCast(stats.matched_entries),
            .filtered_entries = @intCast(stats.hint_filter_skips),
            .fallback_used = fallback_used,
        },
    });
}

pub fn finish(
    ptr: *anyopaque,
    hint: anytype,
    from_sequence: u64,
    target_sequence: u64,
    stats: anytype,
) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "derived-replay",
        .trace_id = traceId(ptr, hint),
        .name = "FinishCatchUp",
        .facts = .{
            .index_name = @tagName(hint),
            .applied_sequence = @max(from_sequence, stats.last_applied_sequence),
            .target_sequence = target_sequence,
            .last_sequence = stats.last_sequence,
            .scanned_entries = @intCast(stats.scanned_entries),
            .matched_entries = @intCast(stats.applied_entries),
            .applied = stats.last_applied_sequence > from_sequence,
        },
    });
}
