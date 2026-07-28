// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const build_options = @import("build_options");
const tracing = @import("../../tracing/mod.zig");

pub fn admission(
    trace_id: u128,
    index_name: []const u8,
    config_hash: u64,
    target_sequence: u64,
    durable_work: bool,
) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "index-lifecycle",
        .trace_id = trace_id,
        .name = "RequestGeneration",
        .facts = .{
            .index_name = index_name,
            .config_hash = config_hash,
            .target_sequence = target_sequence,
            .durable_work = durable_work,
            .phase = "admitted",
        },
    });
}

pub fn queued(
    trace_id: u128,
    index_name: []const u8,
    config_hash: u64,
    target_sequence: u64,
) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "index-lifecycle",
        .trace_id = trace_id,
        .name = "QueueDurableWork",
        .facts = .{
            .index_name = index_name,
            .config_hash = config_hash,
            .target_sequence = target_sequence,
            .durable_work = true,
            .phase = "outbox",
        },
    });
}

pub fn activation(
    trace_id: u128,
    index_name: []const u8,
    repair_id: u128,
    config_hash: u64,
    applied_sequence: u64,
    target_sequence: u64,
) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "index-lifecycle",
        .trace_id = trace_id,
        .name = "SwapGeneration",
        .facts = .{
            .index_name = index_name,
            .phase = "activating",
            .repair_id = repair_id,
            .config_hash = config_hash,
            .applied_sequence = applied_sequence,
            .target_sequence = target_sequence,
            .durable_work = true,
            .worker_admitted = true,
        },
    });
}

pub fn intent(
    event_name: []const u8,
    value: anytype,
    worker_admitted: ?bool,
    reason: ?[]const u8,
) void {
    if (comptime !build_options.with_tla) return;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "index-lifecycle",
        .trace_id = value.group_id,
        .name = event_name,
        .facts = .{
            .group_id = value.group_id,
            .index_name = value.index_name,
            .phase = @tagName(value.phase),
            .reason = reason,
            .repair_id = value.repair_id,
            .config_hash = value.config_hash,
            .applied_sequence = value.candidate_applied_sequence,
            .target_sequence = value.target_sequence,
            .next_retry_at_ms = value.next_retry_at_ms,
            .durable_work = true,
            .worker_admitted = worker_admitted,
        },
    });
}
