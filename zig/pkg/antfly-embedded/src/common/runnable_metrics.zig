// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const scheduling = @import("runnable_scheduler.zig");
const prometheus = @import("prometheus.zig");
pub const Snapshot = @import("antfly_runtime_abi").RunnableStats;

/// Capacity is immutable; live ownership and counters are captured under the
/// scheduler's lock. Disabled scheduling reports no fabricated capacity.
pub fn snapshot(scheduler: ?*scheduling.Scheduler) Snapshot {
    const owner = scheduler orelse return .{};
    const stats = owner.stats();
    return .{
        .enabled = true,
        .capacity_slots = owner.config.slots,
        .control_capacity_slots = owner.config.control_slots + owner.config.cleanup_slots,
        .operation_capacity = owner.config.max_operations,
        .metadata_capacity_bytes = owner.config.max_bytes,
        .control_metadata_capacity_bytes = owner.config.control_bytes,
        .max_wait_ms = owner.config.max_wait_ms,
        .quantum_ms = owner.config.quantum_ms,
        .checkpoint_budget = owner.config.checkpoint_budget,
        .operations = stats.operations,
        .retained_bytes = stats.retained_bytes,
        .control_operations = stats.control_operations,
        .control_retained_bytes = stats.control_retained_bytes,
        .queued = stats.queued,
        .runnable_slots = stats.runnable_slots,
        .control_slots = stats.control_slots,
        .resource_blocked = stats.resource_blocked,
        .output_blocked = stats.output_blocked,
        .grants = stats.grants,
        .yields = stats.yields,
        .backfills = stats.backfills,
        .rejected = stats.rejected,
        .wait_ns = stats.wait_ns,
    };
}

pub fn appendPrometheusMetrics(writer: *std.Io.Writer, stats: Snapshot) !void {
    try prometheus.appendPromMetric(writer, "antfly_runnable_enabled", "gauge", "Whether runnable scheduling is configured on this execution owner", @intFromBool(stats.enabled));
    inline for (.{
        .{ "capacity_slots", "capacity_slots", "Configured total runnable slot capacity" },
        .{ "control_capacity_slots", "control_capacity_slots", "Runnable slots reserved inside total capacity for verified controls" },
        .{ "operation_capacity", "operation_capacity", "Maximum registered runnable operations including parked owners" },
        .{ "metadata_capacity_bytes", "metadata_capacity_bytes", "Maximum caller-declared scheduling metadata bytes; not a process heap limit" },
        .{ "control_metadata_capacity_bytes", "control_metadata_capacity_bytes", "Scheduling metadata bytes reserved inside the total limit for verified controls" },
        .{ "max_wait_ms", "max_wait_milliseconds", "Configured maximum time per runnable queue wait" },
        .{ "quantum_ms", "quantum_milliseconds", "Configured cooperative time quantum checked at execution checkpoints" },
        .{ "checkpoint_budget", "checkpoint_budget", "Maximum execution checkpoints per cooperative quantum" },
        .{ "operations", "outstanding_operations", "Registered queued running and parked runnable owners" },
        .{ "retained_bytes", "retained_metadata_bytes", "Caller-declared scheduling metadata retained by all registered owners" },
        .{ "control_operations", "control_outstanding_operations", "Registered owners with protected control metadata reservations" },
        .{ "control_retained_bytes", "control_retained_metadata_bytes", "Scheduling metadata retained from the protected control reservation" },
        .{ "queued", "queued_operations", "Operations waiting for runnable capacity" },
        .{ "runnable_slots", "active_slots", "Runnable slots currently held by executing operations" },
        .{ "control_slots", "control_active_slots", "Reserved runnable slots currently held by executing controls" },
        .{ "resource_blocked", "resource_blocked_operations", "Registered owners parked at explicit resource waits" },
        .{ "output_blocked", "output_blocked_operations", "Registered owners parked while awaiting output consumption" },
    }) |metric| try prometheus.appendPromMetric(writer, "antfly_runnable_" ++ metric[1], "gauge", metric[2], @field(stats, metric[0]));
    inline for (.{
        .{ "yields", "yields_total", "Cooperative runnable yields" },
        .{ "backfills", "backfills_total", "Small operations granted behind a blocked queue head" },
        .{ "rejected", "rejected_operations_total", "Runnable submissions rejected by finite outstanding capacity" },
        .{ "wait_ns", "queue_wait_nanoseconds_total", "Accumulated time in runnable queues including resumption waits" },
    }) |metric| try prometheus.appendPromMetric(writer, "antfly_runnable_" ++ metric[1], "counter", metric[2], @field(stats, metric[0]));
    try prometheus.appendPromMetricHeader(writer, "antfly_runnable_grants_total", "counter", "Runnable grants by bounded server execution class");
    inline for (.{ "query", "write", "inference", "background", "control" }, 0..) |class, index| {
        try prometheus.appendPromSampleLabeled(writer, "antfly_runnable_grants_total", &.{.{ .name = "class", .value = class }}, stats.grants[index]);
    }
}

test "runnable metrics distinguish disabled scheduling from configured capacity" {
    const stats = snapshot(null);
    try std.testing.expectEqualDeep(Snapshot{}, stats);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, stats);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_enabled 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_capacity_slots 0\n") != null);
}

test "runnable metrics preserve parked memory and separate protected execution" {
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 3, .max_operations = 8, .max_bytes = 1024, .control_bytes = 256 });
    defer scheduler.close();
    var query: scheduling.Entry = .{ .options = .{ .io = std.testing.io }, .class = .query, .bytes = 80 };
    var control: scheduling.Entry = .{ .options = .{ .io = std.testing.io }, .class = .control, .bytes = 40 };
    try scheduler.submit(&query);
    defer scheduler.finish(&query);
    try scheduler.submit(&control);
    defer scheduler.finish(&control);
    try scheduler.block(&query, .output_blocked);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, snapshot(&scheduler));
    const rendered = output.writer.buffered();
    for ([_][]const u8{
        "antfly_runnable_enabled 1\n",
        "antfly_runnable_capacity_slots 3\n",
        "antfly_runnable_control_capacity_slots 1\n",
        "antfly_runnable_outstanding_operations 2\n",
        "antfly_runnable_retained_metadata_bytes 120\n",
        "antfly_runnable_control_outstanding_operations 1\n",
        "antfly_runnable_control_retained_metadata_bytes 40\n",
        "antfly_runnable_active_slots 1\n",
        "antfly_runnable_control_active_slots 1\n",
        "antfly_runnable_output_blocked_operations 1\n",
        "antfly_runnable_grants_total{class=\"query\"} 1\n",
        "antfly_runnable_grants_total{class=\"control\"} 1\n",
    }) |expected| try std.testing.expect(std.mem.indexOf(u8, rendered, expected) != null);
    try scheduler.block(&control, .resource_blocked);
    try scheduler.reclassifyBlocked(&query, .control);
    try scheduler.unblock(&query);
    const promoted = snapshot(&scheduler);
    // Completion promotion borrows protected execution, not a new protected
    // metadata reservation. Telemetry must retain the original classification.
    try std.testing.expectEqual(@as(u64, 1), promoted.control_slots);
    try std.testing.expectEqual(@as(u64, 1), promoted.control_operations);
    try std.testing.expectEqual(@as(u64, 40), promoted.control_retained_bytes);
    try std.testing.expectEqual(@as(u64, 120), promoted.retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), promoted.resource_blocked);
    scheduler.finish(&query);
    scheduler.finish(&control);
    const retired = snapshot(&scheduler);
    try std.testing.expectEqual(@as(u64, 0), retired.operations);
    try std.testing.expectEqual(@as(u64, 0), retired.retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), retired.runnable_slots);
    try std.testing.expectEqual(@as(u64, 1), retired.grants[0]);
    try std.testing.expectEqual(@as(u64, 2), retired.grants[4]);
}
