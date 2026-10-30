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
const sources = @import("antfly_local_sources");
const compiler = sources.sql_compiler;
const scheduling = sources.common_request_admission.runnable_scheduler;

/// Classification uses compiler-owned structure, never client cost hints.
/// Preparation remains general/analytical. Only the separately verified pure
/// stage can transfer to bounded service after setup. LIMIT is not a work bound.
pub fn classify(compiled: *const compiler.Compiled) scheduling.ReadLane {
    const select = switch (compiled.statement) {
        .select => |value| value,
        else => return .general,
    };
    if (select.source != null or select.ctes.len != 0 or select.set_operation != null or
        select.values_arms.len != 0 or select.group_by.len != 0 or select.count_all or
        select.having != null or select.order_by.len != 0 or select.windows.len != 0 or
        (select.table != null and select.limit == null)) return .analytical;
    for (select.columns) |column| {
        const expression = column.expression orelse continue;
        // Function calls can include aggregate/window, provider or unknown work.
        if (expression.* != .literal and expression.* != .column) return .analytical;
    }
    return .general;
}

pub fn apply(execution: *scheduling.Execution, compiled: *const compiler.Compiled) !void {
    try execution.classifyRead(classify(compiled));
}

test "SQL read lane classification uses verified structure rather than limits or hints" {
    const cases = .{
        .{ "SELECT 1", scheduling.ReadLane.general },
        .{ "SELECT n FROM docs LIMIT 8", scheduling.ReadLane.general },
        .{ "SELECT n FROM docs", scheduling.ReadLane.analytical },
        .{ "SELECT n FROM docs ORDER BY n LIMIT 1", scheduling.ReadLane.analytical },
        .{ "SELECT count(*) FROM docs LIMIT 1", scheduling.ReadLane.analytical },
        .{ "SELECT n, count(*) FROM docs GROUP BY n", scheduling.ReadLane.analytical },
        .{ "SELECT a.n FROM docs a JOIN peers b ON a.n = b.n LIMIT 1", scheduling.ReadLane.analytical },
    };
    inline for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case[0], .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case[1], classify(&compiled));
    }
}

test "SQL read lane classification preserves the original root and admitted footprint" {
    var owner = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var execution: scheduling.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 512 }, .query);
    const entry = &execution.entry.?;
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM docs", .{});
    defer compiled.deinit();
    try apply(&execution, &compiled);
    try std.testing.expect(entry == &execution.entry.?);
    try std.testing.expectEqual(scheduling.ReadLane.analytical, entry.read_lane);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 512), owner.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, execution.classifyRead(.bounded));
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "SQL read lane transition cannot refresh expired or canceled authority" {
    const Clock = struct {
        var tick: u64 = 100;
        fn now() u64 {
            return tick;
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM docs", .{});
    defer compiled.deinit();
    for ([_]bool{ false, true }) |expire| {
        Clock.tick = 100;
        var canceled = false;
        var owner = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
        defer owner.close();
        var execution: scheduling.Execution = .{};
        defer execution.finish();
        try execution.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 200, .cancellation = .{ .ptr = &canceled, .requested = Clock.requested }, .retained_bytes = 512 }, .query);
        try apply(&execution, &compiled);
        try std.testing.expectEqual(@as(?u64, 200), execution.entry.?.options.deadline_ns);
        const grants = owner.stats().grants;
        if (expire) Clock.tick = 201 else canceled = true;
        try std.testing.expectError(if (expire) error.DeadlineExceeded else error.Canceled, execution.classifyRead(.general));
        try std.testing.expectEqualDeep(grants, owner.stats().grants);
        try std.testing.expectEqual(scheduling.ReadLane.analytical, execution.entry.?.read_lane);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 512), owner.stats().retained_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

const runtime = sources.sql_runtime;
const catalog = sources.sql_catalog;
const ast = sources.sql_ast;
const read_memory = @import("../common/read_memory.zig");

/// A finite, source-free scalar program is the first qualified operator family.
/// Providers, sessions, functions, sets and scans retain their existing path.
/// This is an IR proof, never a LIMIT, SQL text match or client cost hint.
pub fn preparedScalar(compiled: *const compiler.Compiled, parameters: []const std.json.Value, limits: runtime.Limits) bool {
    if (compiled.uses_current_setting or parameters.len != compiled.parameter_count or parameters.len > 16) return false;
    if (limits.parameter_owner) |owner| if (owner.bytes > 8192) return false;
    const select = switch (compiled.statement) {
        .select => |s| s,
        else => return false,
    };
    if (select.table != null or select.source != null or select.ctes.len != 0 or select.set_operation != null or
        select.values_arms.len != 0 or select.windows.len != 0 or select.group_by.len != 0 or select.order_by.len != 0 or
        select.having != null or select.predicate != null or select.count_all or select.columns.len == 0 or select.columns.len > 16) return false;
    if (limits.parameter_cells) |cells| {
        if (cells.len != parameters.len) return false;
        for (cells) |cell| if (cell.array != null or cell.numeric != null or cell.patterns != null) return false;
    }
    var proof: ScalarProof = .{ .parameters = parameters };
    // The parameter frame can prepare unused positions too. Bound the whole
    // immutable input, not just leaves referenced by this expression tree.
    for (parameters) |parameter| switch (parameter) {
        .null, .bool, .integer, .float => {},
        .string => |text| if (!proof.bytes(text)) return false,
        .number_string => |text| if (!proof.number(text)) return false,
        else => return false,
    };
    if (limits.parameter_cells) |cells| for (cells) |cell| switch (cell.value) {
        .null, .bool, .integer, .float => {},
        .string => |text| if (!proof.bytes(text)) return false,
        .number_string => |text| if (!proof.number(text)) return false,
        else => return false,
    };
    for (select.columns) |column| {
        if (column.wildcard or column.expression == null) return false;
        if (!proof.bytes(column.field) or !proof.bytes(column.alias orelse "") or !proof.scalar(column.expression.?, 0)) return false;
    }
    if (select.limit) |value| if (!proof.value(value)) return false;
    if (select.offset) |value| if (!proof.value(value)) return false;
    return true;
}

const ScalarProof = struct {
    parameters: []const std.json.Value,
    nodes: usize = 0,
    retained: usize = 0,
    fn bytes(self: *@This(), text: []const u8) bool {
        if (text.len > 2048 or text.len > 8192 -| self.retained) return false;
        self.retained += text.len;
        return true;
    }
    fn number(self: *@This(), text: []const u8) bool {
        return text.len <= 64 and std.mem.indexOfAny(u8, text, "eE") == null and self.bytes(text);
    }
    fn value(self: *@This(), v: ast.Value) bool {
        return switch (v) {
            .null, .boolean, .integer, .number => true,
            .numeric => |text| self.number(text),
            .string => |text| self.bytes(text),
            .parameter => |index| if (index == 0 or index > self.parameters.len) false else switch (self.parameters[index - 1]) {
                .null, .bool, .integer, .float => true,
                .string => |text| self.bytes(text),
                .number_string => |text| self.number(text),
                else => false,
            },
        };
    }
    fn scalar(self: *@This(), v: *const ast.Scalar, depth: usize) bool {
        return self.expression(v, depth, true);
    }
    fn expression(self: *@This(), v: *const ast.Scalar, depth: usize, strings: bool) bool {
        self.nodes += 1;
        if (self.nodes > 128 or depth > 16) return false;
        return switch (v.*) {
            .literal => |literal| blk: {
                if (!strings) switch (literal) {
                    .string => break :blk false,
                    .numeric => |text| if (std.mem.indexOfAny(u8, text, "eE") != null) break :blk false,
                    .parameter => |index| if (index > 0 and index <= self.parameters.len and self.parameters[index - 1] == .string) break :blk false,
                    else => {},
                };
                break :blk self.value(literal);
            },
            .unary => |u| self.expression(u.operand, depth + 1, false),
            .binary => |b| switch (b.op) {
                .concat, .like, .ilike, .json_get, .json_text => false,
                else => self.expression(b.left, depth + 1, false) and self.expression(b.right, depth + 1, false),
            },
            else => false,
        };
    }
};

/// The heap owner survives the request stack when its materialized result does.
/// The caller retains a reference through worker join and callback detachment;
/// runtime error cleanup therefore cannot invalidate scheduler callbacks.
const ScalarMemory = struct {
    parent: std.mem.Allocator,
    memory: read_memory.Group,
    references: std.atomic.Value(usize) = .init(1),

    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const parent = self.parent;
        self.memory.deinit();
        parent.destroy(self);
    }
    fn begin(raw: *anyopaque) callconv(.c) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.memory.beginTransition() catch return false;
        return true;
    }
    fn general(raw: *anyopaque) callconv(.c) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.memory.tryEnterGeneral() catch false;
    }
};

// Borrowed query/authority pointers live only on the joined worker job. The
// result's independent ScalarMemory owner retains no request-stack pointers.
const ScalarStage = struct {
    heap: *ScalarMemory,
    execution: *scheduling.Execution,
    compiled: *const compiler.Compiled,
    parameters: []const std.json.Value,
    limits: runtime.Limits,
    parameter_descriptors: []const sources.sql_scalar.Type,
    parameter_fallback_types: []const ?ast.ColumnType,
    original_context: sources.api_operation.RequestContext,
    backend_error_context: ?*sources.sql_errors.Context,
    result: ?runtime.Result = null,
    failure: ?anyerror = null,

    fn checkpoint(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // This fixed pure stage cannot suspend its private worker into general
        // service. Measured demotion occurs on the coordinator after its join.
        try self.original_context.ensureActive();
        try self.execution.checkLifetime();
    }
    fn scalarCheckpoint(raw: ?*anyopaque) !void {
        try checkpoint(raw.?);
    }
    fn checkOriginal(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.original_context.ensureActive();
    }
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return error.InvalidBoundedReadProof;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.InvalidBoundedReadProof;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.InvalidBoundedReadProof;
    }
    fn run(self: *@This()) void {
        _ = self.heap.references.fetchAdd(1, .monotonic);
        self.result = runtime.executeOwned(.{ .allocator = self.heap.memory.allocator(), .ptr = self.heap, .release = ScalarMemory.release }, .{
            .error_context = self.backend_error_context,
            .ptr = self,
            .parameter_descriptor_hints = self.parameter_descriptors,
            .parameter_fallback_types = self.parameter_fallback_types,
            .scalar_control = .{ .ptr = self, .checkpoint = scalarCheckpoint },
            .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint },
        }, self.compiled, self.parameters, self.limits) catch |err| {
            self.failure = err;
            return;
        };
    }
};

/// Called only after ordinary authority/session/provider preparation is done.
/// The same root crosses the private worker and heap, then returns to general
/// service before delivery. No backend pointer is published to that worker.
pub fn executePreparedScalar(server: *@import("http_server.zig").ApiHttpServer, execution: *scheduling.Execution, compiled: *const compiler.Compiled, parameters: []const std.json.Value, limits: runtime.Limits, backend: catalog.Backend, original_context: sources.api_operation.RequestContext) !?runtime.Result {
    if (!preparedScalar(compiled, parameters, limits) or backend.execution_binding_validator != null or backend.setting_capture != null or backend.settings_view != null or backend.parameter_invocation != null) return null;
    if (backend.parameter_descriptor_hints.len > 16 or backend.parameter_fallback_types.len > 16) return null;
    for (backend.parameter_descriptor_hints) |descriptor| if (descriptor.numeric_modifier != null) return null;
    const entry = if (execution.entry) |*e| e else return null;
    const scheduler = entry.scheduler orelse return null;
    if (!scheduler.config.readIsolation() or entry.class != .query or entry.read_resource_transfer != null or entry.children != 0) return null;
    if (entry.parent != null or entry.cpu_borrow_budget != null or entry.completion_options != null or entry.pending_resources != null or entry.publication_active) return null;
    // The original input and its transition credit must both fit. Oversized
    // immutable inputs remain ordinary reads, rather than new capacity errors.
    if (entry.bytes > scheduler.config.boundedReadMetadataBytes() / 2) return null;
    // Preserve explicit legacy admission and older imported IO capabilities.
    // Only hosts that publish the private shared role can qualify this stage.
    if (server.cfg.query_max_concurrent_requests == 0 or server.cfg.write_max_concurrent_requests == 0) return null;
    if (server.cfg.imported_runtime_io) |views| {
        if (views.sql_workers == null) return null;
    } else {
        const host = server.cfg.backend_runtime orelse return null;
        if (host.borrowed_io != null) return null;
    }
    const pool = server.readMemoryPool() orelse return null;
    const heap_bound = 512 << 10;
    if (pool.limits.bytes[1] < heap_bound or pool.limits.bytes[2] < heap_bound) return null;
    const io = try server.sqlBoundedReadIo();
    const memory = try server.alloc.create(ScalarMemory);
    memory.* = .{ .parent = server.alloc, .memory = read_memory.Group.init(pool, .bounded, heap_bound) catch |err| {
        server.alloc.destroy(memory);
        return err;
    } };
    defer ScalarMemory.release(memory);
    try memory.memory.reserveTransition();
    var stage: ScalarStage = .{ .heap = memory, .execution = execution, .compiled = compiled, .parameters = parameters, .limits = limits, .parameter_descriptors = backend.parameter_descriptor_hints, .parameter_fallback_types = backend.parameter_fallback_types, .original_context = original_context, .backend_error_context = backend.error_context };
    const stage_cleanup = entry.cleanup_options == null;
    if (stage_cleanup) try execution.reserveCleanupFor(.{ .io = try server.sqlCleanupIo() }, 250 * std.time.ns_per_ms);
    try execution.park(.resource_blocked);
    try scheduler.prepareBoundedRead(entry, .{ .max_retained_bytes = entry.bytes, .max_wait_ms = scheduler.config.max_wait_ms, .estimated_work_ns = @as(u64, scheduler.config.quantum_ms) * std.time.ns_per_ms }, .{ .context = memory, .begin = ScalarMemory.begin, .try_general = ScalarMemory.general });
    defer scheduler.detachReadResources(entry, stage_cleanup) catch unreachable;
    return runPreparedScalar(&stage, io) catch |err| {
        scheduler.stopPreparedRead(entry, err);
        return err;
    };
}

fn runPreparedScalar(self: *ScalarStage, io: std.Io) !runtime.Result {
    const execution = self.execution;
    try execution.enter(.query);
    var future = io.concurrent(ScalarStage.run, .{self}) catch |err| {
        try execution.checkLifetime();
        return if (err == error.ConcurrencyUnavailable) error.SqlWriteCapacityUnavailable else err;
    };
    @import("protected_future.zig").wait(io, &future);
    errdefer if (self.result) |*result| result.deinit();
    // All physical use of the private worker has ended before demotion can
    // wait on general capacity. It remains available to another bounded read.
    if (self.failure) |err| return err;
    try execution.demoteReadWithCheck(.general, .{ .context = self, .check = ScalarStage.checkOriginal });
    const result = self.result.?;
    self.result = null;
    return result;
}

test "SQL prepared scalar execution uses the private worker and returns the original root to general service" {
    const a = std.testing.allocator;
    const http = @import("http_server.zig");
    var backend = try sources.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer backend.deinit();
    var server = try http.ApiHttpServer.initWithConfig(a, .{
        .backend_runtime = backend.ptr(),
        .request_memory_bytes = 64 << 20,
        .runnable_policy = .{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 64, .checkpoint_budget = 1 },
    }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    _ = try server.sqlExecutionIo();
    var root: scheduling.Execution = .{};
    defer root.finish();
    try server.startSqlRunnable(&root, .{ .io = std.testing.io, .retained_bytes = 1024 }, .query);
    const entry = &root.entry.?;
    var identity: ?http.AuthenticatedIdentity = null;
    var adapter: @import("sql_execution.zig").Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &root, .serial_execution = true };
    var compiled = try compiler.compile(a, "SELECT $1 + 1 AS n, 'private scalar' AS label", .{});
    defer compiled.deinit();
    var typed_backend = adapter.backend();
    typed_backend.parameter_descriptor_hints = &.{.{ .kind = .integer, .element_type = .int64 }};
    var result = try adapter.execute(a, &compiled, &.{.{ .integer = 9007199254740992 }}, .{}, typed_backend);
    var result_live = true;
    defer if (result_live) result.deinit();
    var baseline = try runtime.execute(a, typed_backend, &compiled, &.{.{ .integer = 9007199254740992 }}, .{});
    defer baseline.deinit();
    try std.testing.expectEqualDeep(baseline.output.rows, result.output.rows);
    try std.testing.expectEqualStrings("9007199254740993", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("private scalar", result.output.rows[0][1].string);
    try std.testing.expect(entry == &root.entry.?);
    try std.testing.expectEqual(scheduling.ReadLane.general, entry.read_lane);
    const stats = entry.scheduler.?.stats();
    try std.testing.expect(stats.read_lane_grants[@backingInt(scheduling.ReadLane.bounded)] > 0);
    try std.testing.expectEqual(@as(usize, 1), stats.operations);
    try std.testing.expectEqual(@as(u64, 1024), stats.retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.read_transition_reserved_operations);
    try std.testing.expectEqual(@as(usize, 0), stats.bounded_read_operations);
    try std.testing.expect(entry.read_resource_transfer == null);
    try std.testing.expect(entry.cleanup_options == null);
    try std.testing.expect(server.readMemoryStats().used_bytes[0] > 0);
    result.deinit();
    result_live = false;
    try std.testing.expectEqual(@as(u64, 0), server.readMemoryStats().used_bytes[0]);
}

test "SQL prepared scalar proof rejects provider work and unbounded value shapes" {
    const cases = .{
        .{ "SELECT 1", true },
        .{ "SELECT $1 + 1", true },
        .{ "SELECT id FROM docs LIMIT 1", false },
        .{ "SELECT count(*) FROM docs", false },
        .{ "SELECT current_setting('timezone')", false },
        .{ "SELECT 1 UNION ALL SELECT 2", false },
        .{ "SELECT 'a' || 'b'", false },
        .{ "SELECT 1 WHERE true", false },
    };
    inline for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case[0], .{});
        defer compiled.deinit();
        const parameters: []const std.json.Value = if (compiled.parameter_count == 1) &.{.{ .integer = 1 }} else &.{};
        try std.testing.expectEqual(case[1], preparedScalar(&compiled, parameters, .{}));
    }
    var compiled = try compiler.compile(std.testing.allocator, "SELECT $1", .{});
    defer compiled.deinit();
    try std.testing.expect(!preparedScalar(&compiled, &.{.{ .array = std.array_list.Managed(std.json.Value).init(std.testing.allocator) }}, .{}));
}

test "SQL prepared scalar cancellation and deadline preserve the retained root through private worker join" {
    const a = std.testing.allocator;
    const http = @import("http_server.zig");
    var backend = try sources.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer backend.deinit();
    var server = try http.ApiHttpServer.initWithConfig(a, .{
        .backend_runtime = backend.ptr(),
        .request_memory_bytes = 64 << 20,
        .runnable_policy = .{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 64 },
    }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    _ = try server.sqlExecutionIo();
    const Refusal = struct {
        var tick: std.atomic.Value(u64) = .init(100);
        coordinator: std.Thread.Id,
        expire: bool,
        observed: std.atomic.Value(bool) = .init(false),
        calls: std.atomic.Value(usize) = .init(0),
        reject_at: usize = 1,
        canceled: std.atomic.Value(bool) = .init(false),
        fn now() u64 {
            return tick.load(.acquire);
        }
        fn requested(raw: *const anyopaque) bool {
            const self: *@This() = @constCast(@as(*const @This(), @ptrCast(@alignCast(raw))));
            if (std.Thread.getCurrentId() != self.coordinator) {
                self.observed.store(true, .release);
                const call = self.calls.fetchAdd(1, .monotonic) + 1;
                if (self.reject_at != 0 and call >= self.reject_at) {
                    if (self.expire) tick.store(201, .release) else self.canceled.store(true, .release);
                }
            }
            return self.canceled.load(.acquire);
        }
    };
    var compiled = try compiler.compile(a, "SELECT 1", .{});
    defer compiled.deinit();
    var baseline_calls: usize = 0;
    {
        Refusal.tick.store(100, .release);
        var refusal: Refusal = .{ .coordinator = std.Thread.getCurrentId(), .expire = false, .reject_at = 0 };
        var root: scheduling.Execution = .{};
        defer root.finish();
        try server.startSqlRunnable(&root, .{ .io = std.testing.io, .retained_bytes = 1024, .native_now_ns = Refusal.now, .deadline_ns = 200, .cancellation = .{ .ptr = &refusal, .requested = Refusal.requested } }, .query);
        var identity: ?http.AuthenticatedIdentity = null;
        var adapter: @import("sql_execution.zig").Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &root, .serial_execution = true };
        var result = try adapter.execute(a, &compiled, &.{}, .{}, null);
        defer result.deinit();
        baseline_calls = refusal.calls.load(.acquire);
        try std.testing.expect(baseline_calls > 2);
        std.debug.print("prepared scalar private checkpoints: {d}; cancellation and deadline rejected at each\n", .{baseline_calls});
    }
    for ([_]bool{ false, true }) |expire| for (1..baseline_calls + 1) |reject_at| {
        Refusal.tick.store(100, .release);
        var refusal: Refusal = .{ .coordinator = std.Thread.getCurrentId(), .expire = expire, .reject_at = reject_at };
        var root: scheduling.Execution = .{};
        defer root.finish();
        try server.startSqlRunnable(&root, .{ .io = std.testing.io, .retained_bytes = 1024, .native_now_ns = Refusal.now, .deadline_ns = 200, .cancellation = .{ .ptr = &refusal, .requested = Refusal.requested } }, .query);
        var identity: ?http.AuthenticatedIdentity = null;
        var adapter: @import("sql_execution.zig").Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &root, .serial_execution = true };
        try std.testing.expectError(if (expire) error.DeadlineExceeded else error.Canceled, adapter.execute(a, &compiled, &.{}, .{}, null));
        try std.testing.expect(refusal.observed.load(.acquire));
        const entry = &root.entry.?;
        try std.testing.expectEqual(scheduling.State.stopped, entry.state);
        const stats = entry.scheduler.?.stats();
        try std.testing.expectEqual(@as(usize, 1), stats.operations);
        try std.testing.expectEqual(@as(u64, 1024), stats.retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), stats.runnable_slots);
        try std.testing.expect(entry.read_resource_transfer == null);
        try std.testing.expectEqual(@as(u64, 0), server.readMemoryStats().used_bytes[1]);
        root.finish();
        try std.testing.expectEqual(@as(u64, 0), server.runnableSchedulingStats().operations);
        try std.testing.expectEqual(@as(u64, 0), server.readMemoryStats().transition_reserved_bytes);
    };
}

test "SQL prepared scalar general heap pressure lends private worker and protects control progress" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const http = @import("http_server.zig");
    var backend = try sources.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer backend.deinit();
    var server = try http.ApiHttpServer.initWithConfig(a, .{
        .backend_runtime = backend.ptr(),
        .request_memory_bytes = 64 << 20,
        .runnable_policy = .{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 64, .max_wait_ms = 5000 },
    }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    _ = try server.sqlExecutionIo();
    const pool = server.readMemoryPool().?;
    var filler = try read_memory.Group.init(pool, .general, pool.limits.bytes[0]);
    defer filler.deinit();
    const occupied = try filler.allocator().alloc(u8, pool.limits.bytes[0] - read_memory.Group.overhead(.of(u8)));
    var occupied_live = true;
    defer if (occupied_live) filler.allocator().free(occupied);
    var root: scheduling.Execution = .{};
    defer root.finish();
    try server.startSqlRunnable(&root, .{ .io = io, .retained_bytes = 1024 }, .query);
    var identity: ?http.AuthenticatedIdentity = null;
    var adapter: @import("sql_execution.zig").Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &root, .serial_execution = true };
    var compiled = try compiler.compile(a, "SELECT 42 AS n", .{});
    defer compiled.deinit();
    const Job = struct {
        adapter: *@import("sql_execution.zig").Adapter,
        compiled: *const compiler.Compiled,
        result: ?runtime.Result = null,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.result = self.adapter.execute(std.testing.allocator, self.compiled, &.{}, .{}, null) catch |err| {
                self.failure = err;
                return;
            };
        }
        fn probe(done: *bool) void {
            done.* = true;
        }
    };
    var job: Job = .{ .adapter = &adapter, .compiled = &compiled };
    var future = try io.concurrent(Job.run, .{&job});
    defer {
        if (occupied_live) {
            filler.allocator().free(occupied);
            occupied_live = false;
        }
        @import("protected_future.zig").wait(io, &future);
        if (job.result) |*result| result.deinit();
    }
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (server.runnableSchedulingStats().read_transition_operations == 0) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.PreparedReadDidNotReachGeneralTransfer;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const parked = server.runnableSchedulingStats();
    try std.testing.expectEqual(@as(u64, 1), parked.operations);
    try std.testing.expectEqual(@as(u64, 0), parked.runnable_slots);
    try std.testing.expectEqual(@as(u64, 1024), parked.retained_bytes);
    // The stage is now waiting for its physical general heap. Its private
    // worker must already be joined and available, independent of that wait.
    var probed = false;
    const private_io = try server.sqlBoundedReadIo();
    var probe = try private_io.concurrent(Job.probe, .{&probed});
    @import("protected_future.zig").wait(private_io, &probe);
    try std.testing.expect(probed);
    var control: scheduling.Execution = .{};
    defer control.finish();
    try server.startSqlRunnable(&control, .{ .io = io, .allow_wait = false, .retained_bytes = 7 }, .control);
    try std.testing.expectEqual(scheduling.State.running, control.entry.?.state);
    control.finish();
    filler.allocator().free(occupied);
    occupied_live = false;
    @import("protected_future.zig").wait(io, &future);
    if (job.failure) |err| return err;
    try std.testing.expectEqualStrings("42", job.result.?.output.rows[0][0].string);
    try std.testing.expectEqual(scheduling.ReadLane.general, root.entry.?.read_lane);
    try std.testing.expect(root.entry.?.read_resource_transfer == null);
    job.result.?.deinit();
    job.result = null;
    root.finish();
    try std.testing.expectEqual(@as(u64, 0), server.runnableSchedulingStats().operations);
    for (server.readMemoryStats().used_bytes) |used| try std.testing.expectEqual(@as(u64, 0), used);
    try std.testing.expectEqual(@as(u64, 0), server.readMemoryStats().transition_reserved_bytes);
    for ([_]bool{ false, true }) |context_only| {
        // Cancellation after a successful private result must retire that
        // result while the physical general destination remains completely full.
        const full = try filler.allocator().alloc(u8, pool.limits.bytes[0] - read_memory.Group.overhead(.of(u8)));
        defer filler.allocator().free(full);
        var signal: std.atomic.Value(bool) = .init(false);
        var canceled_root: scheduling.Execution = .{};
        defer canceled_root.finish();
        try server.startSqlRunnable(&canceled_root, .{ .io = io, .retained_bytes = 17, .cancellation = if (context_only) null else .{ .ptr = &signal, .requested = struct {
            fn check(raw: *const anyopaque) bool {
                return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
            }
        }.check } }, .query);
        const previous_context = adapter.context;
        defer adapter.context = previous_context;
        adapter.context.cancellation = if (context_only) .fromAtomic(&signal) else .none;
        adapter.runnable = &canceled_root;
        var canceled_job: Job = .{ .adapter = &adapter, .compiled = &compiled };
        var canceled_future = try io.concurrent(Job.run, .{&canceled_job});
        defer {
            signal.store(true, .release);
            @import("protected_future.zig").wait(io, &canceled_future);
            if (canceled_job.result) |*result| result.deinit();
        }
        const cancellation_until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
        while (server.runnableSchedulingStats().read_transition_operations == 0) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= cancellation_until.nanoseconds) return error.PreparedReadDidNotReachCanceledTransfer;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        signal.store(true, .release);
        @import("protected_future.zig").wait(io, &canceled_future);
        try std.testing.expectEqual(@as(anyerror, error.Canceled), canceled_job.failure.?);
        try std.testing.expect(canceled_job.result == null);
        try std.testing.expectEqual(scheduling.State.stopped, canceled_root.entry.?.state);
        try std.testing.expectEqual(@as(u64, 17), server.runnableSchedulingStats().retained_bytes);
        try std.testing.expectEqual(@as(u64, 0), server.readMemoryStats().used_bytes[2]);
        try std.testing.expectEqual(@as(u64, 0), server.readMemoryStats().transition_reserved_bytes);
        canceled_root.finish();
        try std.testing.expectEqual(@as(u64, 0), server.runnableSchedulingStats().operations);
    }
    const Block = struct {
        fn run(wait_io: std.Io, gate: *std.Io.Event) void {
            gate.waitUncancelable(wait_io);
        }
    };
    var gate: std.Io.Event = .unset;
    var held = try private_io.concurrent(Block.run, .{ private_io, &gate });
    defer {
        gate.set(io);
        @import("protected_future.zig").wait(private_io, &held);
    }
    var refused: scheduling.Execution = .{};
    defer refused.finish();
    try server.startSqlRunnable(&refused, .{ .io = io, .retained_bytes = 17 }, .query);
    adapter.runnable = &refused;
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, adapter.execute(a, &compiled, &.{}, .{}, null));
    try std.testing.expectEqual(scheduling.State.stopped, refused.entry.?.state);
    try std.testing.expectEqual(@as(u64, 17), server.runnableSchedulingStats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), server.runnableSchedulingStats().runnable_slots);
    try std.testing.expect(refused.entry.?.read_resource_transfer == null);
    refused.finish();
    gate.set(io);
    @import("protected_future.zig").wait(private_io, &held);
    var retry: scheduling.Execution = .{};
    defer retry.finish();
    try server.startSqlRunnable(&retry, .{ .io = io, .retained_bytes = 17 }, .query);
    adapter.runnable = &retry;
    var retried = try adapter.execute(a, &compiled, &.{}, .{}, null);
    retried.deinit();
    retry.finish();
    try std.testing.expectEqual(@as(u64, 0), server.runnableSchedulingStats().operations);
    try std.testing.expectEqual(@as(u64, 0), server.readMemoryStats().transition_reserved_bytes);
}

test "SQL prepared scalar compatibility declines unsupported hosts and oversized retained input before publication" {
    const a = std.testing.allocator;
    const http = @import("http_server.zig");
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_bytes = 4096, .control_bytes = 1024 });
    defer scheduler.close();
    var compiled = try compiler.compile(a, "SELECT 1", .{});
    defer compiled.deinit();
    var root: scheduling.Execution = .{};
    defer root.finish();
    try root.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = scheduler.config.boundedReadMetadataBytes() / 2 + 1 }, .query);
    // Capability refusal must precede private-pool access, as at the imported
    // dispatch boundary. Only the fields supplied by that host are available.
    var server: http.ApiHttpServer = undefined;
    const backend: catalog.Backend = .{ .ptr = undefined, .vtable = undefined };
    try std.testing.expect((try executePreparedScalar(&server, &root, &compiled, &.{}, .{}, backend, .{})) == null);
    root.finish();
    try root.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    server.cfg = .{ .imported_runtime_io = .{ .sql_execution = std.testing.io } };
    try std.testing.expect((try executePreparedScalar(&server, &root, &compiled, &.{}, .{}, backend, .{})) == null);
    var host = try sources.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .manual, .borrowed_io = .{ .general = std.testing.io } });
    defer host.deinit();
    server.cfg = .{ .backend_runtime = host.ptr() };
    try std.testing.expect((try executePreparedScalar(&server, &root, &compiled, &.{}, .{}, backend, .{})) == null);
    server.cfg = .{ .query_max_concurrent_requests = 0 };
    try std.testing.expect((try executePreparedScalar(&server, &root, &compiled, &.{}, .{}, backend, .{})) == null);
    try std.testing.expectEqual(scheduling.ReadLane.general, root.entry.?.read_lane);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().read_lane_grants[@backingInt(scheduling.ReadLane.bounded)]);
    try std.testing.expect(root.entry.?.cleanup_options == null);
}

test "SQL prepared scalar maximum instruction column and input envelopes match ordinary execution" {
    const a = std.testing.allocator;
    const http = @import("http_server.zig");
    var host = try sources.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer host.deinit();
    var server = try http.ApiHttpServer.initWithConfig(a, .{
        .backend_runtime = host.ptr(),
        .request_memory_bytes = 64 << 20,
        .runnable_policy = .{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 64, .max_wait_ms = 5000 },
    }, .{ .ptr = undefined, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    const Program = struct {
        fn tree(w: *std.Io.Writer, depth: usize, literal: []const u8) anyerror!void {
            if (depth == 0) return w.writeAll(literal);
            try w.writeAll("(");
            try tree(w, depth - 1, literal);
            try w.writeAll(" * ");
            try tree(w, depth - 1, literal);
            try w.writeAll(")");
        }
    };
    for (0..4) |mode| {
        var text: std.Io.Writer.Allocating = .init(a);
        defer text.deinit();
        const w = &text.writer;
        try w.writeAll("SELECT ");
        switch (mode) {
            0, 1 => {
                try Program.tree(w, 6, if (mode == 0) "99999999" else "1");
                try w.writeAll(" AS n");
            },
            2 => for (0..16) |column| {
                if (column != 0) try w.writeAll(", ");
                try w.print("(1 + 2) * (3 + 4) AS n{d}", .{column});
            },
            else => for (0..4) |column| {
                if (column != 0) try w.writeAll(", ");
                try w.writeAll("'");
                try w.splatByteAll('x', 2030);
                try w.print("' AS s{d}", .{column});
            },
        }
        var compiled = try compiler.compile(a, text.written(), .{});
        defer compiled.deinit();
        try std.testing.expect(preparedScalar(&compiled, &.{}, .{}));
        var root: scheduling.Execution = .{};
        defer root.finish();
        try server.startSqlRunnable(&root, .{ .io = std.testing.io, .retained_bytes = text.written().len + 1024 }, .query);
        var identity: ?http.AuthenticatedIdentity = null;
        var adapter: @import("sql_execution.zig").Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &root, .serial_execution = true };
        const before = server.runnableSchedulingStats().read_lane_grants[@backingInt(scheduling.ReadLane.bounded)];
        var baseline = runtime.execute(a, adapter.backend(), &compiled, &.{}, .{}) catch |expected| {
            // The maximum tree can overflow ordinary SQL integer arithmetic.
            // Eligibility preserves that semantic error, not a capacity error.
            std.debug.print("prepared scalar envelope {d}: ordinary/qualified error {s}\n", .{ mode, @errorName(expected) });
            try std.testing.expectError(expected, adapter.execute(a, &compiled, &.{}, .{}, null));
            try std.testing.expect(server.runnableSchedulingStats().read_lane_grants[@backingInt(scheduling.ReadLane.bounded)] > before);
            try std.testing.expectEqual(scheduling.State.stopped, root.entry.?.state);
            continue;
        };
        defer baseline.deinit();
        var result = try adapter.execute(a, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqualDeep(baseline.output.columns, result.output.columns);
        try std.testing.expectEqualDeep(baseline.output.rows, result.output.rows);
        try std.testing.expect(server.runnableSchedulingStats().read_lane_grants[@backingInt(scheduling.ReadLane.bounded)] > before);
        try std.testing.expect(root.entry.?.cleanup_options == null);
    }
    for (server.readMemoryStats().used_bytes) |used| try std.testing.expectEqual(@as(u64, 0), used);
    for (server.readMemoryStats().groups) |groups| try std.testing.expectEqual(@as(u64, 0), groups);
    try std.testing.expectEqual(@as(u64, 0), server.runnableSchedulingStats().operations);
}
