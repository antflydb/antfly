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

//! SQL planner boundary. Lower only proven direct lake equijoins; decline small
//! or unsupported plans before dispatch. All other SQL operators stay in the
//! existing engine and consume typed result batches without a JSON round-trip.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const scalar = local.sql_scalar;
const plan = @import("plan.zig");
const state = @import("state.zig");
const runtime = @import("runtime.zig");
const expression = @import("expression.zig");
const A = std.mem.Allocator;
pub const Bridge = struct {
    service: runtime.Service,
    dispatcher: runtime.Dispatcher,
    /// API-role hosts forward orchestration to the query service.
    gateway: ?*@import("transport.zig").Client = null,
    limits: plan.Limits = .{},
    principal_scopes: bool = false,
    local_bytes: u64 = 8 * 1024 * 1024,
    pub fn port(self: *Bridge) catalog.DistributedJoin {
        return .{ .ptr = self, .open = open };
    }
    fn open(raw: *anyopaque, a: A, node: *const local.sql_relation_binding.Node, scans: []const catalog.StatementScan, cursors: []const catalog.Cursor, parameters: []const std.json.Value, request: local.api_operation.RequestContext) !?catalog.RelationCursor {
        const self: *Bridge = @ptrCast(@alignCast(raw));
        try request.ensureActive();
        if (node.operation != .join) return null;
        const join = node.operation.join;
        if (join.left.operation != .scan or join.right.operation != .scan or join.membership != null or join.correlation or join.left_keys.len == 0 or join.left_keys.len > 32 or join.left_keys.len != join.right_keys.len) return null;
        const left = join.left.operation.scan;
        const right = join.right.operation.scan;
        if (left.index >= scans.len or right.index >= scans.len or scans.len != cursors.len) return error.InvalidSqlBackendResponse;
        const left_export = cursors[left.index].export_snapshot orelse return null;
        const right_export = cursors[right.index].export_snapshot orelse return null;
        if ((cursors[left.index].estimated_bytes orelse std.math.maxInt(u64)) +| (cursors[right.index].estimated_bytes orelse std.math.maxInt(u64)) <= self.local_bytes) return null;
        const owner = try a.create(Owner);
        owner.* = .{ .a = a, .arena = .init(a), .decode = .init(a), .block_arena = .init(a), .service = self.service, .request = request, .parent_context = self.service.context };
        var transferred = false;
        defer if (!transferred) {
            owner.decode.deinit();
            owner.arena.deinit();
            a.destroy(owner);
        };
        var service = self.service;
        if (self.principal_scopes) service.tenant = principalScope(service.tenant, request);
        const inherited = try request.platformDeadline();
        if (inherited.deadline_ns) |deadline| service.context.deadline_ns = if (service.context.deadline_ns) |parent| @min(parent, deadline) else deadline;
        service.context.cancellation = .{ .ptr = owner, .is_cancelled_fn = Owner.canceled };
        service.exchange_store.context = service.context;
        owner.service = service;
        const scratch = owner.arena.allocator();
        var left_keys: [32]u16 = undefined;
        var right_keys: [32]u16 = undefined;
        for (join.left_keys, join.right_keys, 0..) |left_key, right_key, index| {
            left_keys[index] = std.math.cast(u16, left_key.directOutputColumn() orelse return null) orelse return null;
            right_keys[index] = std.math.cast(u16, right_key.directOutputColumn() orelse return null) orelse return null;
        }
        const condition: ?expression.Program = if (join.condition) |program| .{ .instructions = program.instructions, .root = program.root, .output = program.output_type, .parameters = parameters } else null;
        if (condition) |value| value.validate(join.left.columns.len + join.right.columns.len) catch return null;
        const left_filter = conditions(scratch, scans[left.index], left.source_columns) catch |err| switch (err) {
            error.SqlDistributedJoinUnsupported => return null,
            else => return err,
        };
        const right_filter = conditions(scratch, scans[right.index], right.source_columns) catch |err| switch (err) {
            error.SqlDistributedJoinUnsupported => return null,
            else => return err,
        };
        const l = (try left_export.export_cut(left_export.ptr, scratch)) orelse return null;
        const r = (try right_export.export_cut(right_export.ptr, scratch)) orelse return null;
        const projection = try scratch.alloc(u16, if (join.output_order) |order| order.len else 0);
        if (join.output_order) |order| for (order, projection) |ordinal, *out| {
            out.* = std.math.cast(u16, ordinal) orelse return error.InvalidSqlBackendResponse;
        };
        const specification = plan.choose(try self.input(scratch, scans[left.index].table, left.source_columns, l, left_filter), try self.input(scratch, scans[right.index].table, right.source_columns, r, right_filter), service.tenant, @min(l.expires_ms, r.expires_ms), .{ .kind = std.meta.stringToEnum(@FieldType(plan.Join, "kind"), @tagName(join.kind)) orelse return null, .left_keys = left_keys[0..join.left_keys.len], .right_keys = right_keys[0..join.right_keys.len], .projection = projection, .condition = condition }, self.limits, service.clock.now()) catch |err| switch (err) {
            error.SqlDistributedJoinUnsupported => return null,
            else => return err,
        };
        const query = if (self.gateway) |gateway| try gateway.submit(specification, service.context) else try service.submit(specification);
        errdefer {
            // Cleanup has its own short I/O deadline: the statement's canceled
            // token must not suppress the durable cancellation request itself.
            const cleanup: local.serverless_query_lake_read_context.Context = .{ .io = service.context.io, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s };
            if (self.gateway) |gateway| gateway.cancel(.{ .tenant = service.tenant, .query = query }, cleanup) catch {} else {
                var abandoned = service;
                abandoned.context = cleanup;
                abandoned.requestCancel(query) catch {};
            }
        }
        var poll_delay_ms: u64 = 1;
        while (true) {
            try service.context.ensureActive();
            var poll_arena = std.heap.ArenaAllocator.init(a);
            defer poll_arena.deinit();
            const result_ = if (self.gateway) |gateway| try gateway.result(poll_arena.allocator(), .{ .tenant = service.tenant, .query = query }, service.context) else try service.pollResult(poll_arena.allocator(), query);
            if (result_) |result| {
                const pages = try scratch.dupe(state.Output, result.pages);
                for (pages) |*page| page.key = try scratch.dupe(u8, page.key);
                owner.pages = pages;
                const manifests = try scratch.dupe(state.Output, result.manifests);
                for (manifests) |*manifest| manifest.key = try scratch.dupe(u8, manifest.key);
                owner.manifests = manifests;
                transferred = true;
                return .{ .ptr = owner, .next = Owner.next, .close = Owner.close };
            }
            // Remote submission durably schedules execution. Native recovery
            // drivers and hosted alarms own progress; status polls must not
            // reset their heartbeat or failure backoff. Inline embedders still
            // drive their coordinator when no remote gateway is installed.
            if (self.gateway == null) _ = try service.turn(query, self.dispatcher);
            if (service.context.io) |io| try std.Io.sleep(io, .fromMilliseconds(@intCast(poll_delay_ms)), .awake);
            poll_delay_ms = @min(poll_delay_ms * 2, 100);
        }
    }
    fn input(self: Bridge, a: A, table: catalog.Table, names: []const []const u8, cut: catalog.ReadCapability, filter: ?expression.Program) !plan.Input {
        const columns = try a.alloc(plan.Column, names.len);
        for (names, columns) |name, *column| {
            const original = try table.column(name);
            column.* = .{ .name = name, .type = original.type, .element_type = original.element_type };
        }
        const rows = @max(@as(u64, 1), self.limits.task_scan_rows / 2);
        const bytes = @max(@as(u64, 1), self.limits.task_output_bytes / 8);
        const splits = @max(@as(u64, cut.min_splits), @max((cut.estimated_rows +| rows - 1) / rows, (@max(cut.estimated_bytes, plan.estimatedRetained(.{ .table_id = table.id, .schema = cut.schema, .snapshot = cut.snapshot, .wal_cut = cut.wal_cut, .cut = cut.token, .columns = columns, .estimated_bytes = cut.estimated_bytes, .estimated_rows = cut.estimated_rows, .splits = 1 })) +| bytes - 1) / bytes));
        if (splits > 65536) return error.SqlJoinBudgetExceeded;
        return .{ .table_id = table.id, .schema = cut.schema, .snapshot = cut.snapshot, .wal_cut = cut.wal_cut, .cut = cut.token, .columns = columns, .estimated_bytes = cut.estimated_bytes, .estimated_rows = cut.estimated_rows, .splits = @intCast(splits), .filter = filter, .bucket = if (cut.bucket) |layout| blk: {
            for (names, 0..) |name, index| if (std.mem.eql(u8, name, layout.column)) break :blk plan.Bucket{ .column = @intCast(index), .count = layout.count };
            break :blk null;
        } else null };
    }
    fn conditions(a: A, scan: catalog.StatementScan, names: []const []const u8) !?expression.Program {
        if (scan.request.conditions.len == 0) return null;
        var instructions: std.ArrayList(scalar.Instruction) = .empty;
        var root: ?u32 = null;
        for (scan.request.conditions) |condition| {
            const ordinal = for (names, 0..) |name, index| {
                if (std.mem.eql(u8, name, condition.column)) break index;
            } else return error.SqlDistributedJoinUnsupported;
            const column = try scan.table.column(condition.column);
            const type_: scalar.Type = .{ .kind = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier };
            const before: u32 = @intCast(instructions.items.len);
            try instructions.append(a, .{ .type = type_, .operation = .{ .column = @intCast(ordinal) } });
            const boolean: scalar.Type = .{ .kind = .boolean };
            if (condition.op == .is_null or condition.op == .is_not_null) {
                try instructions.append(a, .{ .type = boolean, .operation = .{ .unary = .{ .op = if (condition.op == .is_null) .is_null else .is_not_null, .operand = before } } });
            } else {
                const value = condition.datum();
                if (value.numeric != null or value.array != null or value.patterns != null) return error.SqlDistributedJoinUnsupported;
                try instructions.append(a, .{ .type = type_, .operation = .{ .literal = value.value } });
                try instructions.append(a, .{ .type = boolean, .operation = .{ .binary = .{ .op = std.meta.stringToEnum(local.sql_ast.Scalar.Binary, @tagName(condition.op)) orelse return error.SqlDistributedJoinUnsupported, .left = before, .right = before + 1 } } });
            }
            const next: u32 = @intCast(instructions.items.len - 1);
            if (root) |prior| try instructions.append(a, .{ .type = boolean, .operation = .{ .binary = .{ .op = .@"and", .left = prior, .right = next } } });
            root = @intCast(instructions.items.len - 1);
        }
        return .{ .instructions = instructions.items, .root = root.?, .output = .{ .kind = .boolean } };
    }
};
/// Scope identity is local authority, never a value accepted from a SQL body.
pub fn principalScope(seed: plan.Digest, context: local.api_operation.RequestContext) plan.Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-distributed-sql-principal-v1");
    hash.update(&seed);
    hash.update(context.setting_read_principal orelse "anonymous");
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    decode: std.heap.ArenaAllocator,
    block_arena: std.heap.ArenaAllocator,
    service: runtime.Service,
    request: local.api_operation.RequestContext,
    parent_context: local.serverless_query_lake_read_context.Context,
    pages: []const state.Output = &.{},
    page: usize = 0,
    manifests: []const state.Output = &.{},
    manifest: usize = 0,
    previous_manifest: ?state.Output = null,
    manifest_arena: ?std.heap.ArenaAllocator = null,
    data: ?@import("exchange.zig").Data = null,
    block: ?local.sql_spill.ColumnarBlock = null,
    row: usize = 0,
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Owner = @ptrCast(@alignCast(raw));
        return self.request.cancellation.isCancelled() or (if (self.parent_context.cancellation) |token| token.isCancelled() else false);
    }
    fn next(raw: *anyopaque, a: A, maximum: usize) !local.sql_execution_batch.Batch {
        const self: *Owner = @ptrCast(@alignCast(raw));
        var rows: std.ArrayList([]const scalar.Datum) = .empty;
        while (rows.items.len < maximum) {
            try self.service.context.ensureActive();
            if (self.block) |block| {
                if (self.row < block.count()) {
                    const cells = try a.alloc(scalar.Datum, block.values.len);
                    for (cells, 0..) |*cell, column| cell.* = try local.sql_operators.cloneDatum(a, try block.cell(self.row, column));
                    try rows.append(a, cells);
                    self.row += 1;
                    continue;
                }
                block.deinit();
                self.block = null;
            }
            if (self.data) |*data| {
                self.block = try data.nextBlock(&self.block_arena, @import("exchange.zig").max_object_bytes);
                if (self.block != null) {
                    self.row = 0;
                    continue;
                }
                self.data = null;
            }
            if (self.page == self.pages.len) {
                if (self.previous_manifest == null and self.manifest == self.manifests.len) break;
                // Copy the borrowed link before resetting the previous page arena.
                const link = self.previous_manifest orelse self.manifests[self.manifest];
                const next_key = try self.a.dupe(u8, link.key);
                defer self.a.free(next_key);
                var owned = link;
                owned.key = next_key;
                if (self.previous_manifest == null) self.manifest += 1;
                if (self.manifest_arena == null) self.manifest_arena = .init(self.a);
                _ = self.manifest_arena.?.reset(.free_all);
                const page = try self.service.readResultPage(self.manifest_arena.?.allocator(), owned);
                self.pages = page.pages;
                self.previous_manifest = page.previous;
                self.page = 0;
                continue;
            }
            _ = self.decode.reset(.free_all);
            self.data = try self.service.exchange_store.read(self.decode.allocator(), self.pages[self.page], 0, @import("exchange.zig").max_object_bytes);
            self.page += 1;
        }
        return .{ .rows = rows.items };
    }
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.block) |block| block.deinit();
        if (self.manifest_arena) |*arena| arena.deinit();
        self.block_arena.deinit();
        self.decode.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
};

test "distributed SQL paged WAL parallelism is independent of base scan rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bridge: Bridge = .{ .service = undefined, .dispatcher = undefined };
    const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "id", .path = "id", .type = .integer }} };
    var cap: catalog.ReadCapability = .{ .token = "retained", .schema = @splat('a'), .snapshot = "1", .wal_cut = 1, .expires_ms = 10000, .estimated_bytes = 10 * 1024 * 1024, .estimated_rows = 100000 };
    const ordinary = try bridge.input(a, table, &.{"id"}, cap, null);
    var small = ordinary;
    small.estimated_rows = 1;
    small.estimated_bytes = 64;
    small.splits = 1;
    const join: plan.Join = .{ .left_keys = &.{0}, .right_keys = &.{0} };
    const normal = try plan.choose(ordinary, small, @splat('a'), 10000, join, .{}, 1);
    // Paging 385 WAL images requires four image intervals, not 782 base scans.
    cap.min_splits = 4;
    const paged = try bridge.input(a, table, &.{"id"}, cap, null);
    try std.testing.expectEqual(ordinary.splits, paged.splits);
    const selected = try plan.choose(paged, small, @splat('a'), 10000, join, .{}, 1);
    try std.testing.expectEqual(try normal.taskCount(), try selected.taskCount());
    cap.min_splits = 1000;
    try std.testing.expectEqual(@as(u32, 1000), (try bridge.input(a, table, &.{"id"}, cap, null)).splits);
}
