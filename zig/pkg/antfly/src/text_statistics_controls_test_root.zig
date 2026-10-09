// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
pub const antfly_sources = @import("source_owner_embedded_db.zig");
const sources = @import("antfly_local_sources");
const controls = sources.storage_local_query_controls;
const types = sources.storage_db_types;
const runtime = @import("antfly_runtime_abi");
const native = sources.common_request_admission.runnable_scheduler;
const clock = @import("antfly_platform").time;
const Read = @import("api/runnable_local_read.zig").Read;

fn requested(raw: *const anyopaque) bool {
    return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
}
fn checked(borrow: runtime.runnable_abi.Borrowed) !void {
    try borrow.validate();
    const status = borrow.checkpoint(borrow.context);
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}

test "text statistics trusted controls retain original phase capacity and tighter deadline" {
    for ([_]native.Class{ .query, .inference }) |phase| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        try parent.start(&scheduler, .{
            .io = std.testing.io,
            .native_now_ns = clock.monotonicNs,
            .deadline_ns = deadline,
            .retained_bytes = 23,
            .cancellation = .{ .ptr = &canceled, .requested = requested },
        }, phase);
        var read: Read = .{ .execution = &parent, .allocator = std.testing.allocator };
        const team = try read.team();
        const trusted: types.SearchRequest = .{
            .execution_runnable = team.borrow,
            .execution_deadline_ns = deadline,
            .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested },
        };
        for ([_]?u64{ null, deadline - std.time.ns_per_s, deadline + std.time.ns_per_s }) |parsed_deadline| {
            var parsed: types.SearchRequest = .{ .execution_deadline_ns = parsed_deadline, .index_name = "ft", .include_stored = false };
            controls.applyTrustedControls(&parsed, trusted);
            try std.testing.expectEqual(@as(?u64, if (parsed_deadline) |value| @min(value, deadline) else deadline), parsed.execution_deadline_ns);
            try std.testing.expect(parsed.execution_runnable.?.context == team.borrow.context);
            try std.testing.expectEqual(@as(runtime.runnable_abi.Class, @fromBackingInt(@intCast(@backingInt(phase)))), try parsed.execution_runnable.?.currentClass());
            try checked(parsed.execution_runnable.?);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
            try std.testing.expectEqualStrings("ft", parsed.index_name.?);
            try std.testing.expect(!parsed.include_stored);
            canceled.store(true, .release);
            try std.testing.expect(parsed.cancellation.?.isCancelled());
            try std.testing.expectError(error.Canceled, checked(parsed.execution_runnable.?));
            canceled.store(false, .release);
        }
    }
}

test "text statistics trusted controls cross checked callbacks and retire rejected responses" {
    const source_mod = sources.api_table_read_source;
    const route_mod = sources.metadata_catalog_route_contract;
    const Case = enum { success, cancel, expire, shutdown };
    const Fake = struct {
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        expected: types.SearchRequest,
        case: Case,
        calls: usize = 0,
        legacy_calls: usize = 0,
        fn legacy(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?sources.api_query_response.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.legacy_calls += 1;
            return error.TestUnexpectedLegacyDispatch;
        }
        fn collect(raw: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table: []const u8, body: []const u8, req: types.SearchRequest) !?sources.api_query_response.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table);
            try std.testing.expectEqualStrings("{\"fields\":[]}", body);
            try std.testing.expect(req.execution_runnable.?.context == self.expected.execution_runnable.?.context);
            try std.testing.expectEqual(self.expected.execution_deadline_ns, req.execution_deadline_ns);
            try std.testing.expectEqual(try self.expected.execution_runnable.?.currentClass(), try req.execution_runnable.?.currentClass());
            const bytes = try alloc.dupe(u8, "statistics result");
            errdefer alloc.free(bytes);
            switch (self.case) {
                .success => {},
                .cancel => self.canceled.store(true, .release),
                .expire => while (clock.monotonicNs() < req.execution_deadline_ns.?) {
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                },
                .shutdown => {
                    const status = req.execution_runnable.?.park(req.execution_runnable.?.context, @backingInt(runtime.runnable_abi.Wait.resource));
                    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
                    self.scheduler.close();
                },
            }
            return .{ .json = bytes };
        }
        fn collectRouted(raw: *anyopaque, alloc: std.mem.Allocator, fence: route_mod.CatalogRouteFence, group_id: u64, table: []const u8, body: []const u8, req: types.SearchRequest) !?sources.api_query_response.QueryResponse {
            try std.testing.expectEqual(@as(u64, 11), fence.table_id);
            try std.testing.expectEqual(@as(u64, 13), fence.route.range_id);
            return collect(raw, alloc, group_id, table, body, req);
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| for ([_]bool{ false, true }) |routed| for (std.enums.values(Case)) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        const deadline = clock.monotonicNs() + (if (case == .expire) @as(u64, 50 * std.time.ns_per_ms) else 60 * std.time.ns_per_s);
        var parent: native.Execution = .{};
        defer parent.finish();
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = std.testing.allocator };
        const team = try read.team();
        const trusted: types.SearchRequest = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = types.CancellationToken.fromAtomic(&canceled) };
        var fake: Fake = .{ .scheduler = &scheduler, .canceled = &canceled, .expected = trusted, .case = case };
        const missing: source_mod.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .text_stats_group_local = Fake.legacy } };
        try std.testing.expectError(error.UnsupportedQueryRequest, missing.textStatsGroupLocalWithControls(std.testing.allocator, 7, "docs", "{\"fields\":[]}", trusted));
        try std.testing.expectEqual(@as(usize, 0), fake.legacy_calls);
        const source: source_mod.TableReadSource = .{
            .ptr = &fake,
            .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .text_stats_group_local = Fake.legacy, .text_stats_group_local_controlled = Fake.collect, .text_stats_group_local_controlled_routed = Fake.collectRouted },
            .route_fence = if (routed) .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 11, .topology_epoch = 1, .route = .{ .group_id = 7, .range_id = 13, .identity_namespace = .{ .table_id = 11, .shard_id = 7, .range_id = 13 } } } else null,
        };
        const result = source.textStatsGroupLocalWithControls(std.testing.allocator, 7, "docs", "{\"fields\":[]}", trusted);
        switch (case) {
            .success => {
                var response = (try result).?;
                defer response.deinit(std.testing.allocator);
                try std.testing.expectEqualStrings("statistics result", response.json);
            },
            .cancel => try std.testing.expectError(error.Canceled, result),
            .expire => try std.testing.expectError(error.DeadlineExceeded, result),
            .shutdown => try std.testing.expectError(error.SchedulerClosed, result),
        }
        try std.testing.expectEqual(@as(usize, 1), fake.calls);
        try std.testing.expectEqual(@as(usize, 0), fake.legacy_calls);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    };
}

fn checkReadBudget(req: types.SearchRequest) !void {
    if (req.execution_runnable) |borrow| try checked(borrow);
    if (req.cancellation) |token| try token.check();
    if (req.execution_deadline_ns) |deadline| if (clock.monotonicNs() >= deadline) return error.DeadlineExceeded;
}

test "text statistics trusted controls retain original authority during primary read admission waits" {
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    const Case = enum { success, cancel, expire, shutdown, sleep_failure, shutdown_and_sleep_failure };
    const Fixture = struct {
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        case: Case,
        calls: usize = 0,
        fn sleep(raw: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            const stats = self.scheduler.stats();
            std.debug.assert(stats.runnable_slots == 0 and stats.resource_blocked == 1 and stats.operations == 1 and stats.retained_bytes == 23);
            var control: native.Execution = .{};
            defer control.finish();
            control.start(self.scheduler, .{ .io = std.testing.io }, .control) catch unreachable;
            control.checkpoint() catch unreachable;
            control.finish();
            switch (self.case) {
                .success => try std.testing.io.sleep(.fromMilliseconds(1), .awake),
                .cancel => self.canceled.store(true, .release),
                .expire => try std.testing.io.sleep(.fromMilliseconds(100), .awake),
                .shutdown => self.scheduler.close(),
                .sleep_failure => return error.Canceled,
                .shutdown_and_sleep_failure => {
                    self.scheduler.close();
                    return error.Canceled;
                },
            }
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + 50 * std.time.ns_per_ms;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = std.testing.allocator };
        const team = try read.team();
        var fixture: Fixture = .{ .scheduler = &scheduler, .canceled = &canceled, .case = case };
        var vtable = std.testing.io.vtable.*;
        vtable.sleep = Fixture.sleep;
        const io: std.Io = .{ .userdata = &fixture, .vtable = &vtable };
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested } }, .io = io };
        const result = budget.waitForProgress();
        const failure: ?anyerror = switch (case) {
            .success => null,
            .cancel, .sleep_failure => error.Canceled,
            .expire => error.DeadlineExceeded,
            .shutdown, .shutdown_and_sleep_failure => error.SchedulerClosed,
        };
        if (failure) |cause| {
            try std.testing.expectError(cause, result);
            const grants = scheduler.stats().grants;
            try std.testing.expectError(cause, budget.checkpoint());
            try std.testing.expectError(cause, budget.waitForProgress());
            try std.testing.expectEqual(grants, scheduler.stats().grants);
        } else {
            try result;
            try budget.checkpoint();
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}
