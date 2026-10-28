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

/// Effective startup policy, live ownership and counters are captured under the
/// scheduler's lock. Disabled scheduling reports no fabricated capacity.
pub fn snapshot(scheduler: ?*scheduling.Scheduler) Snapshot {
    const owner = scheduler orelse return .{};
    const observed = owner.policyAndStats();
    const stats = observed.stats;
    const policy = observed.config;
    var service: @import("antfly_runtime_abi").RunnableServiceStats = .{
        .available = true,
        .quantum_ns = @as(u64, policy.quantum_ms) * std.time.ns_per_ms,
        .estimated_slot_ns = observed.service.estimated_slot_ns,
        .observed_slot_ns = observed.service.observed_slot_ns,
        .unobserved_estimate_ns = observed.service.unobserved_estimate_ns,
        .refunded_slot_ns = observed.service.refunded_slot_ns,
        .debt_paid_slot_ns = observed.service.debt_paid_slot_ns,
        .clipped_adjustments = observed.service.clipped_adjustments,
    };
    for (observed.service.adjustment_slot_ns, 0..) |correction, c| {
        if (correction < 0) service.debt_slot_ns[c] = @intCast(-correction) else service.rebate_slot_ns[c] = @intCast(correction);
    }
    return .{
        .enabled = true,
        .service = service,
        .capacity_slots = policy.slots,
        .control_capacity_slots = policy.control_slots + policy.cleanup_slots,
        .operation_capacity = policy.max_operations,
        .metadata_capacity_bytes = policy.max_bytes,
        .control_metadata_capacity_bytes = policy.control_bytes,
        .max_wait_ms = policy.max_wait_ms,
        .quantum_ms = policy.quantum_ms,
        .checkpoint_budget = policy.checkpoint_budget,
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
        .read_isolation_enabled = @intFromBool(policy.readIsolation()),
        .general_capacity_slots = policy.generalSlots(),
        .bounded_read_capacity_slots = policy.boundedReadSlots(),
        .bounded_read_operation_capacity = if (policy.readIsolation()) (policy.max_operations - @min(@as(usize, 4), policy.max_operations / 2)) / 4 else 0,
        .bounded_read_metadata_capacity_bytes = policy.boundedReadMetadataBytes(),
        .bounded_read_operations = stats.bounded_read_operations,
        .bounded_read_bytes = stats.bounded_read_bytes,
        .bounded_read_slots = stats.bounded_read_slots,
        .read_lane_grants = stats.read_lane_grants,
        .read_transition_reserved_operations = stats.read_transition_reserved_operations,
        .read_transition_reserved_bytes = stats.read_transition_reserved_bytes,
        .read_transition_operations = stats.read_transition_operations,
        .read_transition_bytes = stats.read_transition_bytes,
        .read_demotions = stats.read_demotions,
        .read_transition_expired = stats.read_transition_expired,
        .read_service_debt_paid = stats.read_service_debt_paid,
        .cpu_borrowed_slots = stats.cpu_borrowed_slots,
        .cpu_borrow_grants = stats.cpu_borrow_grants,
        .cpu_borrow_returns = stats.cpu_borrow_returns,
    };
}

pub fn appendPrometheusMetrics(writer: *std.Io.Writer, stats: Snapshot) !void {
    try stats.service.validate();
    try prometheus.appendPromMetric(writer, "antfly_runnable_enabled", "gauge", "Whether runnable scheduling is configured on this execution owner", @intFromBool(stats.enabled));
    // Derive both from the same captured capacity; no second owner observation
    // or extern telemetry layout change is needed.
    const publication_capacity = stats.capacity_slots -| (stats.control_capacity_slots +| stats.general_capacity_slots +| stats.bounded_read_capacity_slots);
    try prometheus.appendPromMetric(writer, "antfly_runnable_publication_capacity_slots", "gauge", "CPU slots reserved inside total capacity for guarded publication return", publication_capacity);
    const floor: u64 = @intFromBool(stats.bounded_read_capacity_slots != 0);
    try prometheus.appendPromMetric(writer, "antfly_runnable_bounded_read_floor_slots", "gauge", "Nonborrowable bounded read CPU floor inside the reserve", floor);
    try prometheus.appendPromMetric(writer, "antfly_runnable_cpu_borrow_capacity_slots", "gauge", "Maximum CPU loans above the bounded read floor", @min(@as(u64, 1), stats.bounded_read_capacity_slots - floor));
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
        .{ "read_isolation_enabled", "read_isolation_enabled", "Whether bounded read CPU and scheduling metadata are reserved" },
        .{ "general_capacity_slots", "general_capacity_slots", "Maximum simultaneous ordinary CPU slots outside the bounded read reserve" },
        .{ "bounded_read_capacity_slots", "bounded_read_capacity_slots", "CPU slots reserved for server-verified bounded reads" },
        .{ "bounded_read_operation_capacity", "bounded_read_operation_capacity", "Scheduling entries reserved for verified bounded reads" },
        .{ "bounded_read_metadata_capacity_bytes", "bounded_read_metadata_capacity_bytes", "Caller-declared scheduling metadata reserved for verified bounded reads" },
        .{ "bounded_read_operations", "bounded_read_outstanding_operations", "Bounded read owners including parked and queued operations" },
        .{ "bounded_read_bytes", "bounded_read_retained_metadata_bytes", "Retained scheduling metadata owned by bounded reads" },
        .{ "bounded_read_slots", "bounded_read_active_slots", "CPU slots currently executing bounded reads" },
        .{ "cpu_borrowed_slots", "cpu_borrowed_slots", "Reserved read CPU slots loaned to verified general stages; already included in active slots" },
        .{ "read_transition_reserved_operations", "read_transition_reserved_operations", "Capacity credits reserved before bounded read admission; not additional live operations" },
        .{ "read_transition_reserved_bytes", "read_transition_reserved_bytes", "Scheduling metadata capacity reserved for demotion; not duplicate live allocation" },
        .{ "read_transition_operations", "read_transition_operations", "Retained reads suspended in the bounded demotion allowance" },
        .{ "read_transition_bytes", "read_transition_retained_metadata_bytes", "Live scheduling metadata owned by demoting reads, already included in total retained bytes" },
    }) |metric| try prometheus.appendPromMetric(writer, "antfly_runnable_" ++ metric[1], "gauge", metric[2], @field(stats, metric[0]));
    inline for (.{
        .{ "yields", "yields_total", "Cooperative runnable yields" },
        .{ "backfills", "backfills_total", "Small operations granted behind a blocked queue head" },
        .{ "rejected", "rejected_operations_total", "Runnable submissions rejected by finite outstanding capacity" },
        .{ "wait_ns", "queue_wait_nanoseconds_total", "Accumulated time in runnable queues including resumption waits" },
        .{ "read_demotions", "read_demotions_total", "Bounded reads moved into their reserved transition owner" },
        .{ "read_transition_expired", "read_transition_expired_total", "Transition waits stopped at their finite allowance deadline" },
        .{ "read_service_debt_paid", "read_service_debt_paid_total", "Deficit slot credits repaid for measured excess bounded execution" },
        .{ "cpu_borrow_grants", "cpu_borrow_grants_total", "Reserved read CPU loans to verified cooperative general stages" },
        .{ "cpu_borrow_returns", "cpu_borrow_returns_total", "CPU loans returned or moved into free general capacity" },
    }) |metric| try prometheus.appendPromMetric(writer, "antfly_runnable_" ++ metric[1], "counter", metric[2], @field(stats, metric[0]));
    try prometheus.appendPromMetricHeader(writer, "antfly_runnable_grants_total", "counter", "Runnable grants by bounded server execution class");
    inline for (.{ "query", "write", "inference", "background", "control" }, 0..) |class, index| {
        try prometheus.appendPromSampleLabeled(writer, "antfly_runnable_grants_total", &.{.{ .name = "class", .value = class }}, stats.grants[index]);
    }
    try prometheus.appendPromMetricHeader(writer, "antfly_runnable_read_lane_grants_total", "counter", "Read CPU grants by trusted execution lane");
    inline for (.{ "general", "bounded", "analytical" }, 0..) |lane, index| {
        try prometheus.appendPromSampleLabeled(writer, "antfly_runnable_read_lane_grants_total", &.{.{ .name = "lane", .value = lane }}, stats.read_lane_grants[index]);
    }
    try prometheus.appendPromMetric(writer, "antfly_runnable_service_available", "gauge", "Whether this owner supplies acknowledged CPU lease slot-time observations", @intFromBool(stats.service.available));
    if (stats.service.available) {
        try prometheus.appendPromMetric(writer, "antfly_runnable_service_quantum_ns", "gauge", "Configured per-slot service estimate in nanoseconds", stats.service.quantum_ns);
        inline for (.{
            .{ "estimated_slot_ns", "estimated_slot_ns_total", "counter", "Estimated active execution lease slot nanoseconds" },
            .{ "observed_slot_ns", "observed_slot_ns_total", "counter", "Observed active execution lease slot nanoseconds" },
            .{ "unobserved_estimate_ns", "unobserved_estimate_ns_total", "counter", "Conservative estimated slot nanoseconds retained without a usable clock sample" },
            .{ "refunded_slot_ns", "refunded_slot_ns_total", "counter", "Unused estimated slot nanoseconds returned to class credit" },
            .{ "debt_paid_slot_ns", "debt_paid_slot_ns_total", "counter", "Measured excess slot nanoseconds repaid by the class" },
            .{ "clipped_adjustments", "clipped_adjustments_total", "counter", "Service corrections clipped by the bounded accounting policy" },
            .{ "debt_slot_ns", "debt_slot_ns", "gauge", "Pending measured class debt in slot nanoseconds" },
            .{ "rebate_slot_ns", "rebate_slot_ns", "gauge", "Pending fractional class rebate in slot nanoseconds" },
        }) |metric| {
            const name = "antfly_runnable_service_" ++ metric[1];
            try prometheus.appendPromMetricHeader(writer, name, metric[2], metric[3]);
            inline for (.{ "query_general", "write", "inference", "background", "query_bounded", "query_analytical" }, 0..) |class, c| {
                try prometheus.appendPromSampleLabeled(writer, name, &.{.{ .name = "service_class", .value = class }}, @field(stats.service, metric[0])[c]);
            }
        }
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

test "runnable metrics expose effective read isolation and parked bounded ownership" {
    var owner = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var read: scheduling.Execution = .{};
    defer read.finish();
    try read.startRead(&owner, .{ .io = std.testing.io, .retained_bytes = 100 }, .bounded);
    try read.park(.output_blocked);
    const observed = snapshot(&owner);
    try std.testing.expectEqual(@as(u64, 1), observed.read_isolation_enabled);
    try std.testing.expectEqual(@as(u64, 1), observed.general_capacity_slots);
    try std.testing.expectEqual(@as(u64, 1), observed.bounded_read_capacity_slots);
    try std.testing.expectEqual(@as(u64, 1), observed.bounded_read_operations);
    try std.testing.expectEqual(@as(u64, 100), observed.bounded_read_bytes);
    try std.testing.expectEqual(@as(u64, 0), observed.bounded_read_slots);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, observed);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_bounded_read_outstanding_operations 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_read_lane_grants_total{lane=\"bounded\"} 1\n") != null);
    read.finish();
    const retired = snapshot(&owner);
    try std.testing.expectEqual(@as(u64, 0), retired.bounded_read_operations);
    try std.testing.expectEqual(@as(u64, 0), retired.bounded_read_bytes);
}

test "runnable metrics distinguish transition credits from live retained ownership" {
    var owner = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var read: scheduling.Execution = .{};
    defer read.finish();
    try read.startDemotableRead(&owner, .{ .io = std.testing.io, .retained_bytes = 100 }, .{ .max_retained_bytes = 200, .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io }, 250 * std.time.ns_per_ms);
    const reserved = snapshot(&owner);
    try std.testing.expectEqual(@as(u64, 1), reserved.operations);
    try std.testing.expectEqual(@as(u64, 100), reserved.retained_bytes);
    try std.testing.expectEqual(@as(u64, 200), reserved.read_transition_reserved_bytes);
    try std.testing.expectEqual(@as(u64, 0), reserved.read_transition_bytes);
    try owner.beginReadDemotion(&read.entry.?, .general);
    const live = snapshot(&owner);
    try std.testing.expectEqual(@as(u64, 1), live.operations);
    try std.testing.expectEqual(@as(u64, 100), live.retained_bytes);
    try std.testing.expectEqual(@as(u64, 100), live.read_transition_bytes);
    try std.testing.expectEqual(@as(u64, 0), live.bounded_read_operations);
    try std.testing.expectEqual(@as(u64, 0), live.bounded_read_slots);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, live);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_read_transition_reserved_bytes 200\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_read_transition_retained_metadata_bytes 100\n") != null);
    read.finish();
    const finished = snapshot(&owner);
    try std.testing.expectEqual(@as(u64, 0), finished.operations);
    try std.testing.expectEqual(@as(u64, 0), finished.read_transition_reserved_bytes);
    try std.testing.expectEqual(@as(u64, 0), finished.read_transition_bytes);
}

/// Reserved bytes are part of the request envelope, not an additional heap.
pub fn appendReadMemoryMetrics(writer: *std.Io.Writer, stats: @import("antfly_runtime_abi").ReadMemoryStats) !void {
    try prometheus.appendPromMetric(writer, "antfly_read_memory_enabled", "gauge", "Whether physical read pools are configured on this execution owner", @intFromBool(stats.enabled));
    try prometheus.appendPromMetric(writer, "antfly_read_memory_reserved_bytes", "gauge", "Read pool reservation inside the request memory envelope; not live allocation", stats.reserved_bytes);
    inline for (.{
        .{ "capacity_bytes", "capacity_bytes", "Physical read allocation capacity" },
        .{ "group_capacity", "group_capacity", "Maximum physical read allocation owners" },
        .{ "used_bytes", "used_bytes", "Live physical read allocations including allocator headers" },
        .{ "groups", "groups", "Registered physical read allocation owners" },
    }) |metric| {
        try prometheus.appendPromMetricHeader(writer, "antfly_read_memory_" ++ metric[1], "gauge", metric[2]);
        inline for (.{ "general", "bounded", "transition" }, 0..) |lane, index| {
            try prometheus.appendPromSampleLabeled(writer, "antfly_read_memory_" ++ metric[1], &.{.{ .name = "lane", .value = lane }}, @field(stats, metric[0])[index]);
        }
    }
    try prometheus.appendPromMetric(writer, "antfly_read_memory_transition_reserved_bytes", "gauge", "Demotion capacity credits; not additional live allocation", stats.transition_reserved_bytes);
    try prometheus.appendPromMetric(writer, "antfly_read_memory_transition_reserved_groups", "gauge", "Demotion owner credits; not additional live owners", stats.transition_reserved_groups);
    try prometheus.appendPromMetric(writer, "antfly_read_memory_rejected_total", "counter", "Physical read allocator operations refused by ownership or byte limits", stats.rejected);
}

test "runnable metrics physical read telemetry distinguishes capacity credits and live allocation" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    try appendReadMemoryMetrics(&writer.writer, .{
        .enabled = true,
        .reserved_bytes = 400,
        .capacity_bytes = .{ 200, 100, 100 },
        .group_capacity = .{ 8, 8, 8 },
        .used_bytes = .{ 20, 0, 40 },
        .groups = .{ 1, 0, 1 },
        .transition_reserved_bytes = 80,
        .transition_reserved_groups = 1,
        .rejected = 3,
    });
    for ([_][]const u8{
        "antfly_read_memory_enabled 1\n",
        "# TYPE antfly_read_memory_capacity_bytes gauge\n",
        "antfly_read_memory_reserved_bytes 400\n",
        "antfly_read_memory_capacity_bytes{lane=\"general\"} 200\n",
        "antfly_read_memory_used_bytes{lane=\"transition\"} 40\n",
        "antfly_read_memory_transition_reserved_bytes 80\n",
        "antfly_read_memory_rejected_total 3\n",
    }) |expected| try std.testing.expect(std.mem.indexOf(u8, writer.writer.buffered(), expected) != null);
    var disabled: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer disabled.deinit();
    try appendReadMemoryMetrics(&disabled.writer, .{});
    try std.testing.expect(std.mem.indexOf(u8, disabled.writer.buffered(), "antfly_read_memory_enabled 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, disabled.writer.buffered(), "antfly_read_memory_capacity_bytes{lane=\"bounded\"} 0\n") != null);
}

test "runnable metrics expose CPU loans without duplicating active slots or memory" {
    var owner = try scheduling.Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 });
    defer owner.close();
    var base: scheduling.Entry = .{ .options = .{ .io = std.testing.io }, .class = .background, .bytes = 11 };
    try owner.submit(&base);
    defer owner.finish(&base);
    var loan: scheduling.Execution = .{};
    defer loan.finish();
    try loan.startBorrowable(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query, .{ .max_non_yield_ns = 1000 });
    const stats = snapshot(&owner);
    try std.testing.expectEqual(@as(u64, 1), stats.cpu_borrowed_slots);
    try std.testing.expectEqual(@as(u64, 1), stats.cpu_borrow_grants);
    try std.testing.expectEqual(@as(u64, 0), stats.cpu_borrow_returns);
    try std.testing.expectEqual(@as(u64, 2), stats.runnable_slots);
    try std.testing.expectEqual(@as(u64, 28), stats.retained_bytes);
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, stats);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_runnable_cpu_borrowed_slots 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_runnable_cpu_borrow_grants_total 1") != null);
    loan.finish();
    const retired = snapshot(&owner);
    try std.testing.expectEqual(@as(u64, 0), retired.cpu_borrowed_slots);
    try std.testing.expectEqual(retired.cpu_borrow_grants, retired.cpu_borrow_returns);
    try std.testing.expectEqual(@as(u64, 11), retired.retained_bytes);
}

test "runnable metrics expose nonborrowable floor and elastic loan capacity coherently" {
    for ([_]usize{ 1, 2, 4 }) |reserve| {
        var owner = try scheduling.Scheduler.init(.{ .slots = reserve + 3, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = reserve });
        defer owner.close();
        const stats = snapshot(&owner);
        try std.testing.expectEqual(@as(u64, reserve), stats.bounded_read_capacity_slots);
        try std.testing.expectEqual(@as(u64, 1), stats.general_capacity_slots);
        var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer output.deinit();
        try appendPrometheusMetrics(&output.writer, stats);
        try std.testing.expect(std.mem.indexOf(u8, output.written(), "antfly_runnable_bounded_read_floor_slots 1\n") != null);
        const expected = if (reserve == 1) "antfly_runnable_cpu_borrow_capacity_slots 0\n" else "antfly_runnable_cpu_borrow_capacity_slots 1\n";
        try std.testing.expect(std.mem.indexOf(u8, output.written(), expected) != null);
    }
}

test "runnable metrics service captures native debt rebates and fixed class labels coherently" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 3, .weights = .{ 1, 1, 1, 1 }, .quantum_ms = 1 });
    var execution: scheduling.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick += 5 * std.time.ns_per_ms;
    execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .write);
    Clock.tick += std.time.ns_per_ms / 4;
    execution.finish();
    const observed = snapshot(&scheduler);
    try observed.service.validate();
    try std.testing.expect(observed.service.available);
    try std.testing.expectEqual(@as(u64, 1_000_000), observed.service.quantum_ns);
    try std.testing.expectEqual(@as(u64, 5_000_000), observed.service.observed_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 4_000_000), observed.service.debt_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 750_000), observed.service.rebate_slot_ns[1]);
    try std.testing.expectEqual(@as(u64, 0), observed.operations);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, observed);
    const data = output.writer.buffered();
    for ([_][]const u8{
        "antfly_runnable_service_available 1\n",
        "antfly_runnable_service_quantum_ns 1000000\n",
        "antfly_runnable_service_observed_slot_ns_total{service_class=\"query_general\"} 5000000\n",
        "antfly_runnable_service_debt_slot_ns{service_class=\"query_general\"} 4000000\n",
        "antfly_runnable_service_rebate_slot_ns{service_class=\"write\"} 750000\n",
        "antfly_runnable_service_observed_slot_ns_total{service_class=\"query_bounded\"} 0\n",
        "antfly_runnable_service_observed_slot_ns_total{service_class=\"query_analytical\"} 0\n",
    }) |expected| try std.testing.expect(std.mem.indexOf(u8, data, expected) != null);
    var lines = std.mem.splitScalar(u8, data, '\n');
    var samples: usize = 0;
    while (lines.next()) |line| if (std.mem.startsWith(u8, line, "antfly_runnable_service_observed_slot_ns_total{")) {
        samples += 1;
    };
    try std.testing.expectEqual(@as(usize, 6), samples);
}

test "runnable metrics service absence and incompatible snapshots never fabricate zero usage" {
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, .{ .enabled = true });
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_service_available 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_service_observed_slot_ns_total") == null);
    const before = output.writer.buffered().len;
    var malformed: Snapshot = .{};
    malformed.service.version = 0;
    try std.testing.expectError(error.UnsupportedVersion, appendPrometheusMetrics(&output.writer, malformed));
    try std.testing.expectEqual(before, output.writer.buffered().len);
    malformed.service.version = 1;
    malformed.service.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, appendPrometheusMetrics(&output.writer, malformed));
    malformed.service.struct_size += 1;
    malformed.service.measurement_model = 99;
    try std.testing.expectError(error.UnsupportedVersion, appendPrometheusMetrics(&output.writer, malformed));
    malformed.service.measurement_model = 1;
    malformed.service.available = true;
    try std.testing.expectError(error.InvalidArgument, appendPrometheusMetrics(&output.writer, malformed));
    malformed.service.quantum_ns = 1_000_000;
    malformed.service.debt_slot_ns[0] = 64_000_001;
    try std.testing.expectError(error.InvalidArgument, appendPrometheusMetrics(&output.writer, malformed));
    malformed.service.debt_slot_ns[0] = 1;
    malformed.service.rebate_slot_ns[0] = 1;
    try std.testing.expectError(error.InvalidArgument, appendPrometheusMetrics(&output.writer, malformed));
    try std.testing.expectEqual(before, output.writer.buffered().len);
}

test "runnable metrics service emits every accounting signal from actual execution leases" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 3, .weights = .{ 1, 1, 1, 1 }, .quantum_ms = 1 });
    var execution: scheduling.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick += 5 * std.time.ns_per_ms;
    execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick += std.time.ns_per_ms;
    execution.finish();
    for (0..2) |_| {
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .write);
        Clock.tick += std.time.ns_per_ms / 4;
        execution.finish();
    }
    var unobserved: scheduling.Entry = .{ .options = .{ .io = std.testing.io, .native_now_ns = Clock.now }, .class = .inference };
    try scheduler.submit(&unobserved);
    scheduler.finish(&unobserved);
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .background);
    Clock.tick += 66 * std.time.ns_per_ms;
    execution.finish();
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, snapshot(&scheduler));
    for ([_][]const u8{
        "antfly_runnable_service_estimated_slot_ns_total{service_class=\"query_general\"} 2000000\n",
        "antfly_runnable_service_observed_slot_ns_total{service_class=\"query_general\"} 6000000\n",
        "antfly_runnable_service_unobserved_estimate_ns_total{service_class=\"inference\"} 1000000\n",
        "antfly_runnable_service_refunded_slot_ns_total{service_class=\"write\"} 1000000\n",
        "antfly_runnable_service_debt_paid_slot_ns_total{service_class=\"query_general\"} 4000000\n",
        "antfly_runnable_service_clipped_adjustments_total{service_class=\"background\"} 1\n",
        "antfly_runnable_service_debt_slot_ns{service_class=\"background\"} 64000000\n",
        "antfly_runnable_service_rebate_slot_ns{service_class=\"write\"} 500000\n",
    }) |expected| try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), expected) != null);
}

test "runnable metrics expose guarded publication reserve within the same effective node ceiling" {
    const policy = try (scheduling.Config{ .capacity_mode = .automatic }).resolveCapacity(.{ .millicpus = 1000 }, .{});
    var scheduler = try scheduling.Scheduler.init(policy);
    defer scheduler.close();
    const stats = snapshot(&scheduler);
    try std.testing.expectEqual(@as(u64, 4), stats.capacity_slots);
    try std.testing.expectEqual(@as(u64, 1), stats.general_capacity_slots);
    try std.testing.expectEqual(@as(u64, 2), stats.control_capacity_slots);
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, stats);
    try std.testing.expect(std.mem.indexOf(u8, output.writer.buffered(), "antfly_runnable_publication_capacity_slots 1\n") != null);
}
