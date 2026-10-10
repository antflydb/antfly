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

test "text statistics trusted controls loan CPU only during native physical file reads" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const admission = sources.common_read_admission;
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    const Case = enum { success, cancel, expire, shutdown, io_failure, shutdown_and_io_failure };
    const Probe = struct {
        budget: *Budget,
        storage: *storage_io.NativeStorage,
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        case: Case,
        begins: usize = 0,
        ends: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.budget.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.budget.waitForProgress();
        }
        pub fn executionBoundary(self: *@This()) ?sources.common_execution_wait.Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.budget.executionBoundary().?.beginWait();
            self.begins += 1;
            try std.testing.expect(self.storage.snapshotStats().fd_admitted_descriptors > 0);
            const stats = self.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), stats.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), stats.resource_blocked);
            try std.testing.expectEqual(@as(usize, 1), stats.operations);
            try std.testing.expectEqual(@as(u64, 23), stats.retained_bytes);
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io }, .control);
            try control.checkpoint();
            control.finish();
            switch (self.case) {
                .success, .io_failure => {},
                .cancel => self.canceled.store(true, .release),
                .expire => try std.testing.io.sleep(.fromMilliseconds(100), .awake),
                .shutdown, .shutdown_and_io_failure => self.scheduler.close(),
            }
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            // The file use survives the syscall AND rejected resumption.
            try std.testing.expect(self.storage.snapshotStats().fd_admitted_descriptors > 0);
            try self.budget.executionBoundary().?.endWait();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/physical-statistics-read", .{tmp.sub_path});
    defer a.free(path);
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| {
        var storage = try storage_io.NativeStorage.init(a, .threaded);
        defer storage.deinit();
        const view = storage.storage();
        try view.writeFileAbsolute(path, "alpha beta");
        // Isolate the positional syscall from cold descriptor acquisition.
        _ = try view.fileSize(path);
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + (if (case == .expire) @as(u64, 50) else 60000) * std.time.ns_per_ms;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = a };
        const team = try read.team();
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested } }, .io = std.testing.io };
        var probe: Probe = .{ .budget = &budget, .storage = &storage, .scheduler = &scheduler, .canceled = &canceled, .case = case };
        var adapter = admission.Adapter(*Probe){ .budget = &probe };
        const payload = adapter.payload();
        const port = admission.Port.init(&payload);
        const physical_failure = case == .io_failure or case == .shutdown_and_io_failure;
        const result = view.readFileRangeAllocWithAdmission(a, path, 0, if (physical_failure) 11 else 10, port);
        switch (case) {
            .success => {
                const bytes = try result;
                defer a.free(bytes);
                try std.testing.expectEqualStrings("alpha beta", bytes);
                try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
            },
            .cancel => try std.testing.expectError(error.Canceled, result),
            .expire => try std.testing.expectError(error.DeadlineExceeded, result),
            .shutdown, .shutdown_and_io_failure => try std.testing.expectError(error.SchedulerClosed, result),
            .io_failure => try std.testing.expectError(error.EndOfStream, result),
        }
        try std.testing.expectEqual(@as(usize, 1), probe.begins);
        try std.testing.expectEqual(@as(usize, 1), probe.ends);
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "text statistics trusted controls keep memory copies charged and reject older admission payloads" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const admission = sources.common_read_admission;
    const Fixture = struct {
        execution: *native.Execution,
        checks: usize = 0,
        begins: usize = 0,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            try self.execution.checkpoint();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.begins += 1;
            return error.TestUnexpectedResult;
        }
        fn end(_: *anyopaque) !void {
            return error.TestUnexpectedResult;
        }
    };
    const a = std.testing.allocator;
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var parent: native.Execution = .{};
    defer parent.finish();
    try parent.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
    var fixture: Fixture = .{ .execution = &parent };
    const vtable: admission.VTable = .{ .checkpoint = Fixture.checkpoint };
    const payload: admission.Payload = .{ .ptr = &fixture, .vtable = &vtable, .execution_wait = .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } };
    const port = admission.Port.init(&payload);
    var memory = storage_io.MemoryStorage.init(a);
    defer memory.deinit();
    const view = memory.storage();
    try view.writeFileAbsolute("/virtual", "alpha beta");
    const bytes = try view.readFileRangeAllocWithAdmission(a, "/virtual", 0, 10, port);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("alpha beta", bytes);
    try std.testing.expectEqual(@as(usize, 0), fixture.begins);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    const LegacyPayload = struct {
        vtable_contract: runtime.native_abi.TypeContract = .of(admission.VTable),
        ptr: *anyopaque,
        vtable: *const admission.VTable,
        dispatch: admission.Boundary.Dispatch = admission.Boundary.local_dispatch,
    };
    const legacy: LegacyPayload = .{ .ptr = &fixture, .vtable = &vtable };
    const rejected: admission.Port = .{ .contract = .of(LegacyPayload), .payload = &legacy };
    const checks = fixture.checks;
    try std.testing.expectError(error.UnsupportedVersion, rejected.checkpoint());
    try std.testing.expectError(error.UnsupportedVersion, rejected.executionWait());
    try std.testing.expectEqual(checks, fixture.checks);
    try std.testing.expectEqual(@as(usize, 0), fixture.begins);
}

test "text statistics trusted controls pin metadata and whole file reads across pathname replacement" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const admission = sources.common_read_admission;
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    const Shape = enum { size, trailer, whole };
    const Case = enum { success, replace, cancel_metadata, cancel_data, expire_metadata, shutdown_metadata, shutdown_data, grow_whole, shrink_whole };
    const Probe = struct {
        budget: *Budget,
        storage: *storage_io.NativeStorage,
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        case: Case,
        path: []const u8,
        replacement: []const u8,
        begins: usize = 0,
        ends: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.budget.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.budget.waitForProgress();
        }
        pub fn executionBoundary(self: *@This()) ?sources.common_execution_wait.Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.budget.executionBoundary().?.beginWait();
            self.begins += 1;
            try std.testing.expect(self.storage.snapshotStats().fd_admitted_descriptors > 0);
            const stats = self.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), stats.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), stats.resource_blocked);
            try std.testing.expectEqual(@as(usize, 1), stats.operations);
            try std.testing.expectEqual(@as(u64, 23), stats.retained_bytes);
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io }, .control);
            try control.checkpoint();
            control.finish();
            if (self.begins == 1) switch (self.case) {
                .cancel_metadata => self.canceled.store(true, .release),
                .expire_metadata => try std.testing.io.sleep(.fromMilliseconds(100), .awake),
                .shutdown_metadata => self.scheduler.close(),
                else => {},
            };
            if (self.begins == 2) switch (self.case) {
                .cancel_data => self.canceled.store(true, .release),
                .shutdown_data => self.scheduler.close(),
                else => {},
            };
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            try std.testing.expect(self.storage.snapshotStats().fd_admitted_descriptors > 0);
            try self.budget.executionBoundary().?.endWait();
            if (self.case == .replace and self.ends == 1) {
                // Replace while the same admitted operation owns CPU again.
                // Its active FD use must outlive cache invalidation/vacuum.
                try self.storage.storage().renameAbsolute(self.replacement, self.path);
            }
            if (self.ends == 2) switch (self.case) {
                .grow_whole => try self.storage.storage().appendFileAbsolute(std.testing.allocator, self.path, "extra", false),
                .shrink_whole => try self.storage.storage().writeFileAbsolute(self.path, "short"),
                else => {},
            };
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/metadata-statistics-read", .{tmp.sub_path});
    defer a.free(path);
    const replacement = try std.fmt.allocPrint(a, "{s}.replacement", .{path});
    defer a.free(replacement);
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Shape)) |shape| for (std.enums.values(Case)) |case| {
        if (shape == .size and (case == .cancel_data or case == .shutdown_data)) continue;
        if (shape != .whole and (case == .grow_whole or case == .shrink_whole)) continue;
        var storage = try storage_io.NativeStorage.init(a, .threaded);
        defer storage.deinit();
        const view = storage.storage();
        try view.writeFileAbsolute(path, "old body end");
        try view.writeFileAbsolute(replacement, "replacement new");
        // This fixture targets metadata/data retention on an already cached FD.
        _ = try view.fileSize(path);
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + (if (case == .expire_metadata) @as(u64, 50) else 60000) * std.time.ns_per_ms;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = a };
        const team = try read.team();
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested } }, .io = std.testing.io };
        var probe: Probe = .{ .budget = &budget, .storage = &storage, .scheduler = &scheduler, .canceled = &canceled, .case = case, .path = path, .replacement = replacement };
        var adapter = admission.Adapter(*Probe){ .budget = &probe };
        const payload = adapter.payload();
        const port = admission.Port.init(&payload);
        const expected_error: ?anyerror = switch (case) {
            .success, .replace => null,
            .grow_whole => error.StreamTooLong,
            .shrink_whole => error.EndOfStream,
            .cancel_metadata, .cancel_data => error.Canceled,
            .expire_metadata => error.DeadlineExceeded,
            .shutdown_metadata, .shutdown_data => error.SchedulerClosed,
        };
        switch (shape) {
            .size => {
                const result = view.fileSizeWithAdmission(path, port);
                if (expected_error) |cause| try std.testing.expectError(cause, result) else try std.testing.expectEqual(@as(u64, 12), try result);
            },
            .trailer => {
                const result = view.readFileTrailerAllocWithAdmission(a, path, 3, port);
                if (expected_error) |cause| try std.testing.expectError(cause, result) else {
                    var trailer = try result;
                    defer trailer.deinit(a);
                    try std.testing.expectEqual(@as(u64, 12), trailer.file_size);
                    try std.testing.expectEqualStrings("end", trailer.bytes);
                }
            },
            .whole => {
                const result = view.readFileAllocWithAdmission(a, path, 12, port);
                if (expected_error) |cause| try std.testing.expectError(cause, result) else {
                    const bytes = try result;
                    defer a.free(bytes);
                    try std.testing.expectEqualStrings("old body end", bytes);
                }
            },
        }
        const expected_waits: usize = if (case == .grow_whole or case == .shrink_whole) 3 else if (expected_error != null) (if (case == .cancel_data or case == .shutdown_data) 2 else 1) else switch (shape) {
            .size => 1,
            .trailer => 2,
            .whole => 3,
        };
        try std.testing.expectEqual(expected_waits, probe.begins);
        try std.testing.expectEqual(expected_waits, probe.ends);
        if (case == .replace) try std.testing.expectEqual(@as(u64, 15), try view.fileSize(path));
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "text statistics trusted controls descriptor FIFO preserves original native wait authority and unlinks rejected waiters" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const admission = sources.common_read_admission;
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    const Case = enum { fifo, cancel, expire, shutdown, wait_failure };
    const Shape = enum { reservation, file_read };
    const Probe = struct {
        budget: *Budget,
        pool: *storage_io.NativeStoragePool,
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        case: Case,
        waits: usize = 0,
        remaining: usize = 2,
        pub fn checkpoint(self: *@This()) !void {
            try self.budget.checkpoint();
        }
        pub fn executionBoundary(self: *@This()) ?sources.common_execution_wait.Boundary {
            return self.budget.executionBoundary();
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            try self.budget.executionBoundary().?.run(progress, .{self});
        }
        fn progress(self: *@This()) !void {
            const stats = self.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), stats.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), stats.resource_blocked);
            try std.testing.expectEqual(@as(usize, 1), stats.operations);
            try std.testing.expectEqual(@as(u64, 23), stats.retained_bytes);
            try std.testing.expectEqual(@as(usize, 1), self.pool.snapshotStats().fd_admission_waiters);
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io }, .control);
            control.finish();
            switch (self.case) {
                .fifo => {
                    // A newcomer cannot pass the original FIFO head even
                    // when sufficient capacity for the newcomer is available.
                    self.pool.releaseDescriptorsForTest(std.testing.io, 1);
                    self.remaining -= 1;
                    const Parent = @TypeOf(self);
                    const Tail = struct {
                        parent: Parent,
                        rejected: bool = false,
                        waits: usize = 0,
                        pub fn checkpoint(tail: *@This()) !void {
                            if (tail.rejected) return error.Canceled;
                        }
                        pub fn waitForProgress(tail: *@This()) !void {
                            tail.waits += 1;
                            try std.testing.expectEqual(@as(usize, 2), tail.parent.pool.snapshotStats().fd_admission_waiters);
                            try std.testing.expectEqual(@as(usize, 1), tail.parent.pool.snapshotStats().fd_admitted_descriptors);
                            tail.parent.pool.releaseDescriptorsForTest(std.testing.io, 1);
                            tail.parent.remaining -= 1;
                            tail.rejected = true;
                        }
                    };
                    var tail: Tail = .{ .parent = self };
                    var adapter = admission.Adapter(*Tail){ .budget = &tail };
                    const payload = adapter.payload();
                    try std.testing.expectError(error.Canceled, self.pool.reserveDescriptorsWithAdmissionForTest(std.testing.io, 1, admission.Port.init(&payload)));
                    try std.testing.expectEqual(@as(usize, 1), tail.waits);
                    try std.testing.expectEqual(@as(usize, 1), self.pool.snapshotStats().fd_admission_waiters);
                },
                .cancel => self.canceled.store(true, .release),
                .expire => try std.testing.io.sleep(.fromMilliseconds(100), .awake),
                .shutdown => self.scheduler.close(),
                .wait_failure => return error.TestWaitFailed,
            }
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/saturated-statistics-read", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| for (std.enums.values(Shape)) |shape| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
        defer pool.deinit();
        var storage = try storage_io.NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
        defer storage.deinit();
        try storage.storage().writeFileAbsolute(path, "alpha beta");
        try pool.reserveDescriptorsForTest(std.testing.io, 2);
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + (if (case == .expire) @as(u64, 50) else 60000) * std.time.ns_per_ms;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = std.testing.allocator };
        const team = try read.team();
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested } }, .io = std.testing.io };
        var probe: Probe = .{ .budget = &budget, .pool = &pool, .scheduler = &scheduler, .canceled = &canceled, .case = case };
        defer if (probe.remaining > 0) pool.releaseDescriptorsForTest(std.testing.io, probe.remaining);
        var adapter = admission.Adapter(*Probe){ .budget = &probe };
        const payload = adapter.payload();
        const port = admission.Port.init(&payload);
        const result: anyerror!void = read_result: {
            if (shape == .reservation) break :read_result pool.reserveDescriptorsWithAdmissionForTest(std.testing.io, 2, port);
            const bytes = storage.storage().readFileRangeAllocWithAdmission(std.testing.allocator, path, 0, 10, port) catch |err| break :read_result err;
            defer std.testing.allocator.free(bytes);
            try std.testing.expectEqualStrings("alpha beta", bytes);
            break :read_result {};
        };
        switch (case) {
            .fifo => {
                try result;
                if (shape == .reservation) pool.releaseDescriptorsForTest(std.testing.io, 2);
            },
            .cancel => try std.testing.expectError(error.Canceled, result),
            .expire => try std.testing.expectError(error.DeadlineExceeded, result),
            .shutdown => try std.testing.expectError(error.SchedulerClosed, result),
            .wait_failure => try std.testing.expectError(error.TestWaitFailed, result),
        }
        try std.testing.expectEqual(@as(usize, 1), probe.waits);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admission_waiters);
        const cached: usize = if (shape == .file_read and case == .fifo) 1 else 0;
        try std.testing.expectEqual(cached, pool.snapshotStats().fd_cache_entries);
        try std.testing.expectEqual(probe.remaining + cached, pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "text statistics trusted controls cold native descriptor open retires successful handles under rejected resumption" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const admission = sources.common_read_admission;
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    const Case = enum { success, cancel, expire, shutdown, missing, shutdown_and_missing };
    const Probe = struct {
        budget: *Budget,
        pool: *storage_io.NativeStoragePool,
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        case: Case,
        begins: usize = 0,
        ends: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.budget.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.budget.waitForProgress();
        }
        pub fn executionBoundary(self: *@This()) ?sources.common_execution_wait.Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.budget.executionBoundary().?.beginWait();
            self.begins += 1;
            try std.testing.expectEqual(@as(usize, 1), self.pool.snapshotStats().fd_admitted_descriptors);
            try std.testing.expectEqual(@as(usize, if (self.begins == 1) 0 else 1), self.pool.snapshotStats().fd_cache_entries);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().resource_blocked);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io }, .control);
            control.finish();
            if (self.begins == 1) switch (self.case) {
                .cancel => self.canceled.store(true, .release),
                .expire => try std.testing.io.sleep(.fromMilliseconds(100), .awake),
                .shutdown, .shutdown_and_missing => self.scheduler.close(),
                else => {},
            };
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            try std.testing.expectEqual(@as(usize, 1), self.pool.snapshotStats().fd_admitted_descriptors);
            try self.budget.executionBoundary().?.endWait();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/cold-statistics-read", .{tmp.sub_path});
    defer a.free(path);
    const missing_path = try std.fmt.allocPrint(a, "{s}.missing", .{path});
    defer a.free(missing_path);
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 4);
        defer pool.deinit();
        var storage = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer storage.deinit();
        const view = storage.storage();
        try view.writeFileAbsolute(path, "alpha beta");
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + (if (case == .expire) @as(u64, 50) else 60000) * std.time.ns_per_ms;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = a };
        const team = try read.team();
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested } }, .io = std.testing.io };
        var probe: Probe = .{ .budget = &budget, .pool = &pool, .scheduler = &scheduler, .canceled = &canceled, .case = case };
        var adapter = admission.Adapter(*Probe){ .budget = &probe };
        const payload = adapter.payload();
        const missing = case == .missing or case == .shutdown_and_missing;
        const result = view.readFileRangeAllocWithAdmission(a, if (missing) missing_path else path, 0, 10, admission.Port.init(&payload));
        switch (case) {
            .success => {
                const bytes = try result;
                defer a.free(bytes);
                try std.testing.expectEqualStrings("alpha beta", bytes);
            },
            .cancel => try std.testing.expectError(error.Canceled, result),
            .expire => try std.testing.expectError(error.DeadlineExceeded, result),
            .shutdown, .shutdown_and_missing => try std.testing.expectError(error.SchedulerClosed, result),
            .missing => try std.testing.expectError(error.FileNotFound, result),
        }
        try std.testing.expectEqual(@as(usize, if (case == .success) 2 else 1), probe.begins);
        try std.testing.expectEqual(probe.begins, probe.ends);
        try std.testing.expectEqual(@as(usize, if (case == .success) 1 else 0), pool.snapshotStats().fd_cache_entries);
        try std.testing.expectEqual(@as(usize, if (case == .success) 1 else 0), pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admission_waiters);
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "text statistics trusted controls asynchronous native read joins physical work before stopped ownership retires" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const hook = storage_io.NativeRangeReadTestControl;
    const admission = sources.common_read_admission;
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    const Case = enum { success, cancel_join, expire_join, shutdown_join, cancel_before_join, explicit_cancel, io_failure, shutdown_and_io_failure };
    const Probe = struct {
        budget: *Budget,
        pool: *storage_io.NativeStoragePool,
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        case: Case,
        begins: usize = 0,
        ends: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.budget.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.budget.waitForProgress();
        }
        pub fn executionBoundary(self: *@This()) ?sources.common_execution_wait.Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.budget.executionBoundary().?.beginWait();
            self.begins += 1;
            try std.testing.expect(hook.started.load(.acquire));
            try std.testing.expect(!hook.completed.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), self.pool.snapshotStats().fd_admitted_descriptors);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().resource_blocked);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), self.scheduler.stats().retained_bytes);
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io }, .control);
            control.finish();
            switch (self.case) {
                .cancel_join => self.canceled.store(true, .release),
                .expire_join => try std.testing.io.sleep(.fromMilliseconds(100), .awake),
                .shutdown_join, .shutdown_and_io_failure => self.scheduler.close(),
                else => {},
            }
            hook.release.store(true, .release);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            try std.testing.expect(hook.completed.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), self.pool.snapshotStats().fd_admitted_descriptors);
            try self.budget.executionBoundary().?.endWait();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/asynchronous-statistics-read", .{tmp.sub_path});
    defer a.free(path);
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 4);
        defer pool.deinit();
        var storage = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        var storage_alive = true;
        defer if (storage_alive) storage.deinit();
        const view = storage.storage();
        try view.writeFileAbsolute(path, "alpha beta");
        _ = try view.fileSize(path);
        var worker_io: std.Io.Threaded = .init(a, .{ .async_limit = .limited(1), .concurrent_limit = .limited(1) });
        defer worker_io.deinit();
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + (if (case == .expire_join) @as(u64, 50) else 60000) * std.time.ns_per_ms;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = a };
        const team = try read.team();
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested } }, .io = std.testing.io };
        var probe: Probe = .{ .budget = &budget, .pool = &pool, .scheduler = &scheduler, .canceled = &canceled, .case = case };
        var adapter = admission.Adapter(*Probe){ .budget = &probe };
        const payload = adapter.payload();
        const port = admission.Port.init(&payload);
        hook.started.store(false, .release);
        hook.release.store(false, .release);
        hook.completed.store(false, .release);
        hook.pause.store(true, .release);
        defer {
            hook.release.store(true, .release);
            hook.pause.store(false, .release);
        }
        const physical_failure = case == .io_failure or case == .shutdown_and_io_failure;
        var future = try view.beginReadFileRangeAllocWithAdmission(storage_io.ReadRuntime.init(worker_io.io()), a, path, 0, if (physical_failure) 11 else 10, port);
        var future_alive = true;
        defer if (future_alive) future.cancel();
        const start = clock.monotonicNs();
        while (!hook.started.load(.acquire)) {
            if (clock.monotonicNs() - start > std.time.ns_per_s) return error.WorkerDidNotStart;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        // Retain the same inode and runtime even if the cache owner closes
        // before the worker's physical read and the caller's completion join.
        storage.deinit();
        storage_alive = false;
        try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_admitted_descriptors);
        if (case == .cancel_before_join) canceled.store(true, .release);
        future_alive = false;
        if (case == .explicit_cancel) {
            future.cancel();
        } else {
            const result = future.waitWithAdmission(a, port);
            switch (case) {
                .success => {
                    const bytes = try result;
                    defer a.free(bytes);
                    try std.testing.expectEqualStrings("alpha beta", bytes);
                },
                .cancel_join, .cancel_before_join => try std.testing.expectError(error.Canceled, result),
                .expire_join => try std.testing.expectError(error.DeadlineExceeded, result),
                .shutdown_join, .shutdown_and_io_failure => try std.testing.expectError(error.SchedulerClosed, result),
                .io_failure => try std.testing.expectError(error.EndOfStream, result),
                .explicit_cancel => unreachable,
            }
        }
        try std.testing.expect(hook.completed.load(.acquire));
        const joins: usize = if (case == .explicit_cancel or case == .cancel_before_join) 0 else 1;
        try std.testing.expectEqual(joins, probe.begins);
        try std.testing.expectEqual(joins, probe.ends);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_cache_entries);
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "text statistics trusted controls unconsumed async results release descriptor progress and inline fallback keeps exact bytes" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const admission = sources.common_read_admission;
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = std.testing.allocator;
    const first_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/first-async-read", .{tmp.sub_path});
    defer a.free(first_path);
    const second_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/second-async-read", .{tmp.sub_path});
    defer a.free(second_path);
    for ([_]native.Class{ .query, .inference }) |phase| for ([_]usize{ 0, 1 }) |workers| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 1);
        defer pool.deinit();
        var storage = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer storage.deinit();
        const view = storage.storage();
        // Setup does not use the deliberately one-descriptor read pool:
        // native creation itself needs two transient descriptor permits.
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = first_path, .data = "first exact bytes" });
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = second_path, .data = "second exact bytes" });
        var worker_io: std.Io.Threaded = .init(a, .{ .async_limit = .limited(workers), .concurrent_limit = .limited(workers) });
        defer worker_io.deinit();
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + 2 * std.time.ns_per_s;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23 }, phase);
        var read: Read = .{ .execution = &parent, .allocator = a };
        const team = try read.team();
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline }, .io = std.testing.io };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const port = admission.Port.init(&payload);
        const io = storage_io.ReadRuntime.init(worker_io.io());
        var first = try view.beginReadFileRangeAllocWithAdmission(io, a, first_path, 0, 17, port);
        var first_alive = true;
        defer if (first_alive) first.cancel();
        // A completed first read still owns its output, but must not fence the
        // only descriptor needed to prepare a second independent file read.
        var second = try view.beginReadFileRangeAllocWithAdmission(io, a, second_path, 0, 18, port);
        var second_alive = true;
        defer if (second_alive) second.cancel();
        first_alive = false;
        const first_bytes = try first.waitWithAdmission(a, port);
        defer a.free(first_bytes);
        second_alive = false;
        const second_bytes = try second.waitWithAdmission(a, port);
        defer a.free(second_bytes);
        try std.testing.expectEqualStrings("first exact bytes", first_bytes);
        try std.testing.expectEqualStrings("second exact bytes", second_bytes);
        const AllocationFixture = struct {
            fn run(allocator: std.mem.Allocator, storage_view: storage_io.Storage, runtime_io: storage_io.ReadRuntime, read_path: []const u8, authority: admission.Port) !void {
                var future = try storage_view.beginReadFileRangeAllocWithAdmission(runtime_io, allocator, read_path, 0, 17, authority);
                const bytes = try future.waitWithAdmission(allocator, authority);
                defer allocator.free(bytes);
                try std.testing.expectEqualStrings("first exact bytes", bytes);
            }
        };
        try std.testing.checkAllAllocationFailures(a, AllocationFixture.run, .{ view, io, first_path, port });
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admission_waiters);
        try std.testing.expect(pool.snapshotStats().fd_admitted_descriptors <= 1);
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "text statistics trusted controls legacy async descriptor wait remains cancellable before controlled consumption" {
    const storage_io = sources.storage_lsm_backend_storage_io;
    const admission = sources.common_read_admission;
    const Budget = sources.common_read_execution_budget.Budget(types.SearchRequest, checkReadBudget);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = std.testing.allocator;
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/legacy-async-cancellation", .{tmp.sub_path});
    defer a.free(path);
    for ([_]native.Class{ .query, .inference }) |phase| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 2);
        defer pool.deinit();
        var storage = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer storage.deinit();
        const view = storage.storage();
        try view.writeFileAbsolute(path, "legacy bytes");
        var worker_io: std.Io.Threaded = .init(a, .{ .async_limit = .limited(1), .concurrent_limit = .limited(1) });
        defer worker_io.deinit();
        try pool.reserveDescriptorsForTest(std.testing.io, 2);
        defer pool.releaseDescriptorsForTest(std.testing.io, 2);
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
        try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
        var read: Read = .{ .execution = &parent, .allocator = a };
        const team = try read.team();
        var budget: Budget = .{ .controls = .{ .execution_runnable = team.borrow, .execution_deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested } }, .io = std.testing.io };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        var future = try view.beginReadFileRangeAllocWithRuntime(storage_io.ReadRuntime.init(worker_io.io()), a, path, 0, 12);
        var alive = true;
        defer if (alive) future.cancel();
        const start = clock.monotonicNs();
        while (pool.snapshotStats().fd_admission_waiters == 0) {
            if (clock.monotonicNs() - start > std.time.ns_per_s) return error.WorkerDidNotStart;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        canceled.store(true, .release);
        alive = false;
        try std.testing.expectError(error.Canceled, future.waitWithAdmission(a, admission.Port.init(&payload)));
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admission_waiters);
        try std.testing.expectEqual(@as(usize, 2), pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_cache_entries);
        try std.testing.expectEqual(phase, try parent.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "text statistics trusted controls shared async point pipeline retires stopped batch ownership" {
    const Budget = struct {
        execution: *native.Execution,
        canceled: *std.atomic.Value(bool),
        stop_at: usize,
        calls: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.stop_at) self.canceled.store(true, .release);
            try self.execution.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    const admission = sources.common_read_admission;
    for ([_]native.Class{ .query, .inference }) |phase| {
        // Stop during preparation, shared followers, and consumption; later
        // ordinals also exercise buffers already owned by completed futures.
        for ([_]usize{ 0, 1, 8, 24, 48, 96 }) |stop_at| {
            var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
            defer scheduler.close();
            var canceled = std.atomic.Value(bool).init(false);
            var parent: native.Execution = .{};
            defer parent.finish();
            const deadline = clock.monotonicNs() + 30 * std.time.ns_per_s;
            try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
            var budget = Budget{ .execution = &parent, .canceled = &canceled, .stop_at = stop_at };
            var adapter = admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            const result = sources.storage_lsm_backend_runtime.qualifyControlledAsyncBatch(admission.Port.init(&payload));
            if (stop_at != 0 and budget.calls >= stop_at) {
                try std.testing.expectError(error.Canceled, result);
            } else try result;
            try std.testing.expectEqual(phase, parent.entry.?.class);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
            parent.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        }
    }
}

test "text statistics trusted controls native snapshot batch planning retains original authority" {
    const Budget = struct {
        execution: *native.Execution,
        canceled: *std.atomic.Value(bool),
        stop_at: usize,
        calls: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.stop_at) self.canceled.store(true, .release);
            try self.execution.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    const admission = sources.common_read_admission;
    for ([_]native.Class{ .query, .inference }) |phase| {
        // Stop during preparation, shared followers, and consumption; later
        // ordinals also exercise buffers already owned by completed futures.
        for ([_]usize{ 0, 1, 8, 24, 48, 96 }) |stop_at| {
            var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
            defer scheduler.close();
            var canceled = std.atomic.Value(bool).init(false);
            var parent: native.Execution = .{};
            defer parent.finish();
            const deadline = clock.monotonicNs() + 30 * std.time.ns_per_s;
            try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
            var budget = Budget{ .execution = &parent, .canceled = &canceled, .stop_at = stop_at };
            var adapter = admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            const result = sources.storage_lsm_backend_runtime.qualifyControlledNativeReadBatch(admission.Port.init(&payload), phase == .inference);
            if (stop_at != 0 and budget.calls >= stop_at) {
                try std.testing.expectError(error.Canceled, result);
            } else try result;
            try std.testing.expectEqual(phase, parent.entry.?.class);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
            parent.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        }
    }
}

test "text statistics trusted controls generic batch adapters reject missing authority capability" {
    const erased = sources.storage_backend_erased;
    const admission = sources.common_read_admission;
    const Provider = struct {
        const Owner = @This();
        legacy_calls: usize = 0,
        retired: usize = 0,
        const Handle = struct {
            owner: *Owner,
            pub fn get(self: *@This(), _: []const u8) ![]const u8 {
                self.owner.legacy_calls += 1;
                return error.LegacyReadExecuted;
            }
            pub fn openCursor(self: *@This()) !erased.Cursor {
                self.owner.legacy_calls += 1;
                return error.LegacyReadExecuted;
            }
            pub fn abort(self: *@This()) void {
                self.owner.retired += 1;
            }
            pub fn close(self: *@This()) void {
                self.owner.retired += 1;
            }
        };
    };
    const Budget = struct {
        stopped: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.stopped) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var provider = Provider{};
    var budget = Budget{};
    var adapter = admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = admission.Port.init(&payload);
    {
        var read = try erased.readTxnFrom(std.testing.allocator, Provider.Handle{ .owner = &provider });
        defer read.abort();
        var scope = try erased.readScopeFrom(std.testing.allocator, Provider.Handle{ .owner = &provider });
        defer scope.close();
        var values = [_]?[]const u8{"stale"};
        try std.testing.expectError(error.ExecutionWaitUnsupported, read.getManySortedWithAdmission(&.{"key"}, &values, port));
        try std.testing.expect(values[0] == null);
        values[0] = "stale";
        try std.testing.expectError(error.ExecutionWaitUnsupported, scope.getManySortedWithAdmission(&.{"key"}, &values, port));
        try std.testing.expect(values[0] == null);
        budget.stopped = true;
        try std.testing.expectError(error.Canceled, read.getManySortedWithAdmission(&.{"key"}, &values, port));
        try std.testing.expectError(error.Canceled, scope.getManySortedWithAdmission(&.{"key"}, &values, port));
        try std.testing.expectEqual(@as(usize, 0), provider.legacy_calls);
    }
    try std.testing.expectEqual(@as(usize, 2), provider.retired);
}

test "text statistics trusted controls Lite native and transaction batches loan only physical reads and retire stopped output" {
    const admission = sources.common_read_admission;
    const Case = enum { success, cancel, shutdown };
    const Budget = struct {
        parent: *native.Execution,
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        phase: native.Class,
        mode: Case,
        loans: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.parent.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        pub fn executionBoundary(self: *@This()) sources.common_execution_wait.Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.parent.checkpoint();
            try self.parent.park(.resource_blocked);
            self.loans += 1;
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), self.scheduler.stats().retained_bytes);
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs }, .control);
            try control.checkpoint();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.mode == .cancel) self.canceled.store(true, .release);
            if (self.mode == .shutdown) self.scheduler.close();
            try self.parent.enter(self.phase);
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| {
        for (0..4) |fixture| {
            for ([_]Case{ .success, .cancel, .shutdown }) |mode| {
                var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
                defer scheduler.close();
                var canceled = std.atomic.Value(bool).init(false);
                var parent: native.Execution = .{};
                defer parent.finish();
                const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
                try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
                var budget = Budget{ .parent = &parent, .scheduler = &scheduler, .canceled = &canceled, .phase = phase, .mode = mode };
                var adapter = admission.Adapter(*Budget){ .budget = &budget };
                const payload = adapter.payload();
                const port = admission.Port.init(&payload);
                const result = switch (fixture) {
                    0 => sources.storage_lite_native.qualifyControlledBatch(port),
                    1 => sources.storage_lite_docstore.qualifyControlledReadBatch(port, .transaction),
                    2 => sources.storage_lite_docstore.qualifyControlledReadBatch(port, .scope),
                    else => sources.storage_lite_docstore.qualifyControlledReadBatch(port, .overlay),
                };
                switch (mode) {
                    .success => try result,
                    .cancel => try std.testing.expectError(error.Canceled, result),
                    .shutdown => try std.testing.expectError(error.SchedulerClosed, result),
                }
                try std.testing.expect(budget.loans > 0);
                try std.testing.expectEqual(phase, parent.entry.?.class);
                try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
                try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
                parent.finish();
                try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
            }
        }
    }
}

test "text statistics trusted controls Lite batch checkpoints retire partial cache and scope results" {
    const admission = sources.common_read_admission;
    const Budget = struct {
        parent: *native.Execution,
        canceled: *std.atomic.Value(bool),
        stop: usize,
        calls: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.stop) self.canceled.store(true, .release);
            try self.parent.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| {
        for (0..3) |fixture| {
            for ([_]usize{ 1, 8, 16, 24, 32, 48, 64, 96, std.math.maxInt(usize) }) |stop| {
                var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
                defer scheduler.close();
                var canceled = std.atomic.Value(bool).init(false);
                var parent: native.Execution = .{};
                defer parent.finish();
                try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
                var budget = Budget{ .parent = &parent, .canceled = &canceled, .stop = stop };
                var adapter = admission.Adapter(*Budget){ .budget = &budget };
                const payload = adapter.payload();
                const port = admission.Port.init(&payload);
                const result = switch (fixture) {
                    0 => sources.storage_lite_docstore.qualifyControlledReadBatch(port, .transaction),
                    1 => sources.storage_lite_docstore.qualifyControlledReadBatch(port, .scope),
                    else => sources.storage_lite_docstore.qualifyControlledReadBatch(port, .overlay),
                };
                if (budget.calls >= stop) try std.testing.expectError(error.Canceled, result) else try result;
                try std.testing.expect(budget.calls > 0);
                try std.testing.expectEqual(phase, parent.entry.?.class);
                try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
                parent.finish();
                try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            }
        }
    }
}

test "text statistics trusted controls bounded sort preserves duplicate ordering and stops" {
    const admission = sources.common_read_admission;
    const Budget = struct {
        stop: usize = std.math.maxInt(usize),
        calls: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls >= self.stop) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = admission.Port.init(&payload);
    var items: [300]u64 = undefined;
    for (&items, 0..) |*item, i| item.* = @intCast((299 - i) % 19);
    var expected = items;
    std.mem.sort(u64, &expected, {}, std.sort.asc(u64));
    try admission.sort(u64, &items, {}, std.sort.asc(u64), port);
    try std.testing.expectEqualSlices(u64, &expected, &items);
    try admission.sort(u64, items[0..0], {}, std.sort.asc(u64), port);
    try admission.sort(u64, items[0..1], {}, std.sort.asc(u64), port);
    try admission.sort(u64, &items, {}, std.sort.asc(u64), port);
    try std.testing.expectEqualSlices(u64, &expected, &items);
    budget.calls = 0;
    budget.stop = 4;
    try std.testing.expectError(error.Canceled, admission.sort(u64, &items, {}, std.sort.asc(u64), port));
}

test "text statistics trusted controls public physical batch retains original snapshot and retires rejected results" {
    const Budget = struct {
        execution: *native.Execution,
        canceled: *std.atomic.Value(bool),
        stop_at: usize,
        calls: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.stop_at) self.canceled.store(true, .release);
            try self.execution.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| {
        for ([_]usize{ 0, 1, 4, 8, 16, 24, 32, 48, 64 }) |stop_at| {
            var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
            defer scheduler.close();
            var canceled = std.atomic.Value(bool).init(false);
            var parent: native.Execution = .{};
            defer parent.finish();
            const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
            try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
            var budget = Budget{ .execution = &parent, .canceled = &canceled, .stop_at = stop_at };
            const result = sources.storage_docstore.qualifyBudgetedPhysicalBatch(&budget);
            if (stop_at != 0 and budget.calls >= stop_at) try std.testing.expectError(error.Canceled, result) else try result;
            try std.testing.expectEqual(phase, parent.entry.?.class);
            try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            parent.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}

test "text statistics trusted controls uncached LSM batches loan only physical reads and retire stopped output" {
    const admission = sources.common_read_admission;
    const Case = enum { success, cancel, shutdown };
    const Budget = struct {
        parent: *native.Execution,
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        phase: native.Class,
        mode: Case,
        loans: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.parent.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        pub fn executionBoundary(self: *@This()) sources.common_execution_wait.Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.parent.checkpoint();
            try self.parent.park(.resource_blocked);
            self.loans += 1;
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), self.scheduler.stats().retained_bytes);
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs }, .control);
            try control.checkpoint();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.mode == .cancel) self.canceled.store(true, .release);
            if (self.mode == .shutdown) self.scheduler.close();
            try self.parent.enter(self.phase);
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| {
        for (0..4) |fixture| {
            for ([_]Case{ .success, .cancel, .shutdown }) |mode| {
                var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
                defer scheduler.close();
                var canceled = std.atomic.Value(bool).init(false);
                var parent: native.Execution = .{};
                defer parent.finish();
                const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
                try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
                var budget = Budget{ .parent = &parent, .scheduler = &scheduler, .canceled = &canceled, .phase = phase, .mode = mode };
                var adapter = admission.Adapter(*Budget){ .budget = &budget };
                const payload = adapter.payload();
                const port = admission.Port.init(&payload);
                const result = sources.storage_lsm_backend_runtime.qualifyControlledUncachedReadBatch(port, fixture >= 2, fixture % 2 == 1);
                switch (mode) {
                    .success => try result,
                    .cancel => try std.testing.expectError(error.Canceled, result),
                    .shutdown => try std.testing.expectError(error.SchedulerClosed, result),
                }
                try std.testing.expect(budget.loans > 0);
                try std.testing.expectEqual(phase, parent.entry.?.class);
                try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
                try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
                parent.finish();
                try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
            }
        }
    }
}

test "text statistics trusted controls uncached batch stopped checkpoints retire compact and promoted block ownership" {
    const Budget = struct {
        execution: *native.Execution,
        canceled: *std.atomic.Value(bool),
        stop_at: usize,
        calls: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.stop_at) self.canceled.store(true, .release);
            try self.execution.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| {
        for (0..4) |fixture| {
            for ([_]usize{ 0, 1, 4, 8, 16, 24, 32, 48, 64 }) |stop_at| {
                var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
                defer scheduler.close();
                var canceled = std.atomic.Value(bool).init(false);
                var parent: native.Execution = .{};
                defer parent.finish();
                const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
                try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
                var budget = Budget{ .execution = &parent, .canceled = &canceled, .stop_at = stop_at };
                const admission = sources.common_read_admission;
                var adapter = admission.Adapter(*Budget){ .budget = &budget };
                const payload = adapter.payload();
                const result = sources.storage_lsm_backend_runtime.qualifyControlledUncachedReadBatch(admission.Port.init(&payload), fixture >= 2, fixture % 2 == 1);
                if (stop_at != 0 and budget.calls >= stop_at) try std.testing.expectError(error.Canceled, result) else try result;
                try std.testing.expectEqual(phase, parent.entry.?.class);
                try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
                try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
                parent.finish();
                try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
            }
        }
    }
}

test "text statistics trusted controls prefix and large Snappy decode checkpoints preserve original phase and retire stopped scratch" {
    const Budget = struct {
        execution: *native.Execution,
        canceled: *std.atomic.Value(bool),
        stop_at: usize,
        calls: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.stop_at) self.canceled.store(true, .release);
            try self.execution.checkpoint();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| {
        for (0..2) |fixture| {
            for ([_]usize{ 0, 1, 4, 8, 16, 24, 32, 48, 64, 96, 128, 256, 512 }) |stop_at| {
                var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
                defer scheduler.close();
                var canceled = std.atomic.Value(bool).init(false);
                var parent: native.Execution = .{};
                defer parent.finish();
                const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
                try parent.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = requested } }, phase);
                var budget = Budget{ .execution = &parent, .canceled = &canceled, .stop_at = stop_at };
                const admission = sources.common_read_admission;
                var adapter = admission.Adapter(*Budget){ .budget = &budget };
                const payload = adapter.payload();
                const port = admission.Port.init(&payload);
                const result = if (fixture == 0) sources.storage_lsm_table_file.qualifyControlledPrefixPointReader(port) else sources.storage_lsm_backend_runtime.qualifyControlledLargeSnappyBatch(port);
                if (stop_at != 0 and budget.calls >= stop_at) try std.testing.expectError(error.Canceled, result) else try result;
                try std.testing.expectEqual(phase, parent.entry.?.class);
                try std.testing.expectEqual(@as(?u64, deadline), parent.entry.?.options.deadline_ns);
                try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
                parent.finish();
                try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
            }
        }
    }
}
