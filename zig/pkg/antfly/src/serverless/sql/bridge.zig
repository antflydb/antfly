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
        const condition: ?expression.Program = if (join.condition) |program| try expression.Program.fromBound(scratch, &program, parameters) else null;
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
        const specification = plan.choosePhysical(scratch, try self.input(scratch, scans[left.index].table, left.source_columns, l, left_filter), try self.input(scratch, scans[right.index].table, right.source_columns, r, right_filter), service.tenant, @min(l.expires_ms, r.expires_ms), .{ .kind = std.meta.stringToEnum(@FieldType(plan.Join, "kind"), @tagName(join.kind)) orelse return null, .left_keys = left_keys[0..join.left_keys.len], .right_keys = right_keys[0..join.right_keys.len], .projection = projection, .condition = condition }, self.limits, service.clock.now()) catch |err| switch (err) {
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
    // A borrowed codec lease keeps lazy JSON caches and native vector traits
    // alive until the next pull advances beyond the current exchange block.
    block: ?local.sql_spill.Sequential.InputBlock = null,
    batch: ?local.sql_execution_batch.Batch = null,
    positions: [@import("exchange.zig").max_block_rows]usize = undefined,
    row: usize = 0,
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Owner = @ptrCast(@alignCast(raw));
        return self.request.cancellation.isCancelled() or (if (self.parent_context.cancellation) |token| token.isCancelled() else false);
    }
    fn next(raw: *anyopaque, a: A, maximum: usize) !local.sql_execution_batch.Batch {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (maximum == 0) return .{ .rows = &.{} };
        while (true) {
            try self.service.context.ensureActive();
            if (self.block) |*block| {
                const count = block.view().count();
                if (self.row < count) {
                    if (self.batch == null) self.batch = try block.view().batch(self.block_arena.allocator(), false);
                    const length = @min(maximum, count - self.row);
                    const result = if (self.row == 0 and length == count) self.batch.? else selected: {
                        for (self.positions[0..length], self.row..) |*position, row| position.* = row;
                        break :selected try self.batch.?.select(a, self.positions[0..length]);
                    };
                    // Advance only after descriptors are admitted. Consumers
                    // finish borrowing this slice before asking for another.
                    self.row += length;
                    return result;
                }
                block.deinit();
                self.block = null;
                self.batch = null;
            }
            if (self.data) |*data| {
                if (try data.nextBlockForForwarding(&self.block_arena, self.a, @import("exchange.zig").max_object_bytes)) |encoded| {
                    self.block = .{ .borrowed = .{ .a = self.a, .arena = .init(self.a), .encoded = encoded } };
                    self.row = 0;
                    continue;
                }
                self.data = null;
            }
            if (self.page == self.pages.len) {
                if (self.previous_manifest == null and self.manifest == self.manifests.len) return .{ .rows = &.{} };
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
    }
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.block) |*block| block.deinit();
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

const ResultFixture = struct {
    const exchange = @import("exchange.zig");
    const Batch = local.sql_execution_batch.Batch;
    const Datum = scalar.Datum;
    fn open(a: A, objects: exchange.Store, pages: []const state.Output, manifests: []const state.Output) !*Owner {
        const owner = try a.create(Owner);
        owner.* = .{ .a = a, .arena = .init(a), .decode = .init(a), .block_arena = .init(a), .request = .{}, .parent_context = .{}, .service = .{ .a = a, .state_store = .{ .client = objects.client, .bucket = objects.bucket, .prefix = objects.prefix, .now_ms = 0 }, .exchange_store = objects, .tenant = @splat('a'), .owner = "result-test", .role = .query_only, .source = undefined, .retention = undefined } };
        errdefer Owner.close(owner);
        const work = owner.arena.allocator();
        const copied_pages = try work.dupe(state.Output, pages);
        for (copied_pages) |*page| page.key = try work.dupe(u8, page.key);
        owner.pages = copied_pages;
        const copied_manifests = try work.dupe(state.Output, manifests);
        for (copied_manifests) |*page| page.key = try work.dupe(u8, page.key);
        owner.manifests = copied_manifests;
        return owner;
    }
    fn manifest(a: A, objects: exchange.Store, page: runtime.Service.ResultPage) !state.Output {
        const bytes = try std.json.Stringify.valueAlloc(a, page, .{});
        const hash = local.serverless_external_source_mod.lake_catalog.types.digestHex(bytes);
        const key = try std.fmt.allocPrint(a, "{s}/manifest/{s}", .{ objects.prefix, hash });
        var writer = objects.client;
        writer.allocator = a;
        var saved = try writer.putObject(objects.bucket, key, bytes, .{});
        saved.deinit(a);
        return .{ .key = key, .hash = hash, .bytes = bytes.len, .rows = page.rows };
    }
    /// Preserve the pre-typed bridge's scalar decoder and eager clone loop as
    /// an executable baseline. This fixture only consumes direct result pages.
    fn legacyNext(owner: *Owner, a: A, maximum: usize) !Batch {
        var rows: std.ArrayList([]const Datum) = .empty;
        while (rows.items.len < maximum) {
            try owner.service.context.ensureActive();
            if (owner.block) |*block| {
                if (owner.row < block.view().count()) {
                    const cells = try a.alloc(Datum, block.view().width());
                    for (cells, 0..) |*cell, column| cell.* = try local.sql_operators.cloneDatum(a, try block.view().cell(owner.row, column));
                    try rows.append(a, cells);
                    owner.row += 1;
                    continue;
                }
                block.deinit();
                owner.block = null;
            }
            if (owner.data) |*data| {
                if (try data.nextBlock(&owner.block_arena, exchange.max_object_bytes)) |encoded| {
                    owner.block = .{ .borrowed = .{ .a = owner.a, .arena = .init(owner.a), .encoded = encoded } };
                    owner.row = 0;
                    continue;
                }
                owner.data = null;
            }
            if (owner.page == owner.pages.len) break;
            _ = owner.decode.reset(.free_all);
            owner.data = try owner.service.exchange_store.read(owner.decode.allocator(), owner.pages[owner.page], 0, exchange.max_object_bytes);
            owner.page += 1;
        }
        return .{ .rows = rows.items };
    }
};

test "distributed SQL result bridge borrows typed slices and preserves array decimal dictionary and JSON identity" {
    const a = std.testing.allocator;
    var seed = std.heap.ArenaAllocator.init(a);
    defer seed.deinit();
    const sa = seed.allocator();
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    const objects: ResultFixture.exchange.Store = .{ .client = memory.client(), .bucket = "result", .prefix = "test" };
    var numeric: local.sql_numeric_value.Context = .{ .alloc = sa };
    const number = try local.sql_numeric_value.parse(&numeric, "9007199254740993.1200");
    const array = try local.sql_array_value.Value.init(.int64, &.{.{ .length = 2, .lower = -3 }}, &.{ scalar.Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
    const payload = try sa.alloc(u8, 8192);
    @memset(payload, 'x');
    var json: std.json.Value = .{ .object = .empty };
    try json.object.put(sa, "payload", .{ .string = payload });
    const rows = try sa.alloc(local.sql_operators.Row, 520);
    for (rows, 0..) |*row, index| {
        const cells = try sa.dupe(scalar.Datum, &.{ scalar.Datum.json(.{ .integer = @intCast(index) }), scalar.Datum.typedNumeric(&number.value), scalar.Datum.json(json), scalar.Datum.typedArray(&array), scalar.Datum.json(.{ .string = "dictionary" }), .{}, scalar.Datum.json(.null) });
        row.* = .{ .values = cells, .keys = &.{}, .ordinal = index };
    }
    const output = try objects.put(sa, @splat('a'), 1000000, &.{rows}, 16 << 20);
    const owner = try ResultFixture.open(a, objects, &.{output}, &.{});
    defer Owner.close(owner);
    try std.testing.expectEqual(@as(usize, 0), (try Owner.next(owner, std.testing.failing_allocator, 0)).len());
    try std.testing.expect(owner.block == null);
    const first = try Owner.next(owner, std.testing.failing_allocator, 256);
    try std.testing.expectEqual(@as(usize, 256), first.len());
    try std.testing.expect(first != .rows);
    try std.testing.expect(try first.frozenJson(0, 2) != null);
    try std.testing.expectEqual(@as(usize, 0), owner.block.?.view().encoded.?.json_owner.?.retained_capacity.load(.acquire));
    const primitive = (try first.physicalColumn(sa, 0)).?;
    try std.testing.expectEqual(@as(i64, 255), try primitive.vector.integerAt(primitive.rowAt(255)));
    const decimal = (try first.decimalColumnSelected(sa, 1, null)).?;
    try std.testing.expectEqual(@as(i128, 90071992547409931200), decimal.physical.vector.values.decimal128.values[0]);
    try std.testing.expectEqual(@as(u16, 4), decimal.scales[0]);
    const dictionary = (try first.dictionaryColumn(sa, 4)).?;
    try std.testing.expectEqualStrings("dictionary", (try dictionary.cell(sa, 0, 0)).value.string);
    try std.testing.expect((try first.cell(sa, 0, 5)).sql_null);
    try std.testing.expect(!(try first.cell(sa, 0, 6)).sql_null);
    try std.testing.expectEqual(@as(i32, -3), (try first.cell(sa, 0, 3)).array.?.dimensions[0].lower);
    const second = try Owner.next(owner, sa, 7);
    try std.testing.expectEqual(@as(usize, 7), second.len());
    try std.testing.expectEqual(@as(i64, 256), (try second.cell(sa, 0, 0)).value.integer);
    const selected = (try second.physicalColumnSelected(sa, 0, &.{ 6, 0, 3 })).?;
    try std.testing.expectEqual(@as(i64, 262), try selected.vector.integerAt(selected.rowAt(0)));
    try std.testing.expectEqual(@as(i64, 256), try selected.vector.integerAt(selected.rowAt(1)));
    try std.testing.expect(try second.frozenJson(6, 2) != null);
    // A failed partial descriptor allocation must not consume a row.
    try std.testing.expectError(error.OutOfMemory, Owner.next(owner, std.testing.failing_allocator, 1));
    try std.testing.expectEqual(@as(usize, 7), owner.row);
    const resumed = try Owner.next(owner, sa, 1);
    try std.testing.expectEqual(@as(i64, 263), (try resumed.cell(sa, 0, 0)).value.integer);
    var count: usize = 264;
    while (true) {
        const batch = try Owner.next(owner, sa, 4096);
        if (batch.len() == 0) break;
        for (0..batch.len()) |row| try std.testing.expectEqual(@as(i64, @intCast(count + row)), (try batch.cell(sa, row, 0)).value.integer);
        count += batch.len();
    }
    try std.testing.expectEqual(@as(usize, 520), count);
    try std.testing.expectEqual(@as(usize, 0), (try Owner.next(owner, sa, 1)).len());
}

test "distributed SQL result bridge manifests cancellation and allocation failures retain bounded ownership" {
    const a = std.testing.allocator;
    var seed = std.heap.ArenaAllocator.init(a);
    defer seed.deinit();
    const sa = seed.allocator();
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    const objects: ResultFixture.exchange.Store = .{ .client = memory.client(), .bucket = "result", .prefix = "test" };
    const first = try objects.put(sa, @splat('a'), 1000000, &.{&.{.{ .values = &.{scalar.Datum.json(.{ .string = "first" })}, .keys = &.{}, .ordinal = 0 }}}, 1 << 20);
    const second = try objects.put(sa, @splat('b'), 1000000, &.{&.{.{ .values = &.{scalar.Datum.json(.{ .string = "second" })}, .keys = &.{}, .ordinal = 0 }}}, 1 << 20);
    const empty = try objects.put(sa, @splat('c'), 1000000, &.{&.{}}, 1 << 20);
    const tail = try ResultFixture.manifest(sa, objects, .{ .pages = &.{second}, .rows = 1 });
    const head = try ResultFixture.manifest(sa, objects, .{ .pages = &.{ empty, first }, .rows = 1, .previous = tail });
    const Walk = struct {
        fn run(allocator: A, store: ResultFixture.exchange.Store, manifest: state.Output) !void {
            const owner = try ResultFixture.open(allocator, store, &.{}, &.{manifest});
            defer Owner.close(owner);
            var work = std.heap.ArenaAllocator.init(allocator);
            defer work.deinit();
            for ([_][]const u8{ "first", "second" }) |expected| {
                _ = work.reset(.free_all);
                const batch = try Owner.next(owner, work.allocator(), 17);
                try std.testing.expectEqual(@as(usize, 1), batch.len());
                try std.testing.expectEqualStrings(expected, (try batch.cell(work.allocator(), 0, 0)).value.string);
            }
            try std.testing.expectEqual(@as(usize, 0), (try Owner.next(owner, work.allocator(), 17)).len());
        }
    };
    try Walk.run(a, objects, head);
    try std.testing.checkAllAllocationFailures(a, Walk.run, .{ objects, head });
    const owner = try ResultFixture.open(a, objects, &.{first}, &.{});
    defer Owner.close(owner);
    var cancelled = true;
    owner.service.context.cancellation = .{ .ptr = &cancelled, .is_cancelled_fn = struct {
        fn check(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    }.check };
    const before = memory.operationCount();
    try std.testing.expectError(error.Canceled, Owner.next(owner, sa, 256));
    try std.testing.expectEqual(before, memory.operationCount());
    cancelled = false;
    try std.testing.expectEqual(@as(usize, 1), (try Owner.next(owner, sa, 1)).len());
    // Retire a partly consumed block during cursor close, including lazy caches.
    _ = try owner.batch.?.cell(sa, 0, 0);
}

test "distributed SQL result bridge forwards wide JSON with bounded pull allocations" {
    const a = std.testing.allocator;
    var seed = std.heap.ArenaAllocator.init(a);
    defer seed.deinit();
    const sa = seed.allocator();
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    const objects: ResultFixture.exchange.Store = .{ .client = memory.client(), .bucket = "result", .prefix = "test" };
    const text = try sa.alloc(u8, 16 * 1024);
    @memset(text, 'x');
    var json: std.json.Value = .{ .object = .empty };
    try json.object.put(sa, "payload", .{ .string = text });
    const rows = try sa.alloc(local.sql_operators.Row, 512);
    for (rows, 0..) |*row, index| row.* = .{ .values = &.{scalar.Datum.json(json)}, .keys = &.{}, .ordinal = index };
    const output = try objects.put(sa, @splat('a'), 1000000, &.{rows}, 16 << 20);
    var pull_bytes: [2]usize = undefined;
    var peak_bytes: [2][3]usize = undefined;
    var elapsed_ns: [2][3]u64 = undefined;
    // Alternate ordering to avoid always warming the object-store/cache path
    // for the typed run. Timing is diagnostic; correctness/bytes are asserted.
    for (0..3) |trial| for (0..2) |turn| {
        const mode = if (trial % 2 == 0) turn else 1 - turn;
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = std.math.maxInt(usize) };
        const measured = budget.allocator();
        const owner = try ResultFixture.open(measured, objects, &.{output}, &.{});
        var closed = false;
        defer if (!closed) Owner.close(owner);
        var counted = std.testing.FailingAllocator.init(measured, .{});
        var pull = std.heap.ArenaAllocator.init(counted.allocator());
        defer pull.deinit();
        var destination = local.sql_typed_store.Store.init(measured);
        defer destination.deinit();
        const started = @import("antfly_platform").time.monotonicNs();
        while (true) {
            _ = pull.reset(.free_all);
            const batch = if (mode == 0) try ResultFixture.legacyNext(owner, pull.allocator(), 256) else try Owner.next(owner, pull.allocator(), 256);
            if (batch.len() == 0) break;
            try destination.appendBatch(batch);
        }
        elapsed_ns[mode][trial] = @import("antfly_platform").time.monotonicNs() - started;
        Owner.close(owner);
        closed = true;
        try std.testing.expectEqual(@as(usize, 512), destination.len);
        const retained = try destination.cell(sa, 511, 0);
        try std.testing.expectEqualStrings(text, retained.value.object.get("payload").?.string);
        if (trial == 0) {
            pull_bytes[mode] = counted.allocated_bytes;
        } else {
            try std.testing.expectEqual(pull_bytes[mode], counted.allocated_bytes);
        }
        peak_bytes[mode][trial] = budget.peak;
    };
    std.debug.print("result bridge rows=512 payload=16384 legacy_pull_bytes={d} typed_pull_bytes={d} legacy_peak_bytes={any} typed_peak_bytes={any} legacy_ns={any} typed_ns={any}\n", .{ pull_bytes[0], pull_bytes[1], peak_bytes[0], peak_bytes[1], elapsed_ns[0], elapsed_ns[1] });
    for (peak_bytes[0], peak_bytes[1]) |legacy, typed| try std.testing.expect(typed < legacy);
    try std.testing.expect(pull_bytes[0] > 8 * 1024 * 1024);
    try std.testing.expect(pull_bytes[1] < 16 * 1024);
}
