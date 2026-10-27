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

//! Synchronous query output delivery into the shared unpublished owner lane.
//! The coordinator holds the unique request-owned capture until this returns.
//! Recovery may inspect/seal recorded output, but must never reopen its query.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_population_contract;
const staging = local.storage_db_restore_staging_contract;
const catalog = local.sql_catalog;
const owners = @import("restore_owner.zig");
const operation = local.api_operation;

pub const Destination = struct {
    port: owners.Port,
    table: []const u8,
    group: u64,
    scope: staging.Scope,
    range: local.storage_byte_range.ByteRange = .{ .start = "", .end = "" },
};
pub const Stats = struct { source_rows: u64 = 0, page_requests: u64 = 0, buffer_peak_bytes: usize = 0 };
pub const Options = struct { buffer_bytes: usize = 8 * 1024 * 1024, stats: ?*Stats = null };

pub fn deliver(a: std.mem.Allocator, destination: Destination, source: catalog.PopulationSource, context: operation.RequestContext) !contract.Cursor {
    const receipts = try deliverCohort(a, &.{destination}, source, context, .{});
    defer a.free(receipts);
    return receipts[0];
}

const BufferedRow = struct {
    storage: []align(@alignOf([]const u8)) u8,
    row: contract.Row,
    fn size(row: contract.Row) !usize {
        var count = std.math.add(usize, row.document.len, std.math.mul(usize, row.json_null_fields.len, @sizeOf([]const u8)) catch return error.RelationPopulationPageTooLarge) catch return error.RelationPopulationPageTooLarge;
        for (row.json_null_fields) |field| count = std.math.add(usize, count, field.len) catch return error.RelationPopulationPageTooLarge;
        return count;
    }
    fn clone(a: std.mem.Allocator, row: contract.Row) !BufferedRow {
        const storage = try a.alignedAlloc(u8, .of([]const u8), try size(row));
        const names_bytes = row.json_null_fields.len * @sizeOf([]const u8);
        const names = std.mem.bytesAsSlice([]const u8, storage[0..names_bytes]);
        var offset = names_bytes;
        const document = storage[offset..][0..row.document.len];
        @memcpy(document, row.document);
        offset += document.len;
        for (row.json_null_fields, names) |field, *name| {
            name.* = storage[offset..][0..field.len];
            @memcpy(storage[offset..][0..field.len], field);
            offset += field.len;
        }
        return .{ .storage = storage, .row = .{ .ordinal = row.ordinal, .document = document, .json_null_fields = names } };
    }
};
const Pending = struct {
    rows: std.ArrayList(BufferedRow) = .empty,
    bytes: usize = 0,
    fn deinit(self: *Pending, a: std.mem.Allocator) void {
        for (self.rows.items) |row| a.free(row.storage);
        self.rows.deinit(a);
        self.* = .{};
    }
    fn append(self: *Pending, a: std.mem.Allocator, row: contract.Row) !void {
        try self.rows.ensureUnusedCapacity(a, 1);
        const copy = try BufferedRow.clone(a, row);
        self.rows.appendAssumeCapacity(copy);
        self.bytes += copy.storage.len;
    }
};

/// One captured query, complete sorted range coverage, bounded owned buffering.
/// a must reclaim individual allocations (the DDL scratch allocator). The
/// returned cursors belong to a. No request-owned callbacks escape this call.
pub fn deliverCohort(a: std.mem.Allocator, destinations: []const Destination, source: catalog.PopulationSource, context: operation.RequestContext, options: Options) ![]contract.Cursor {
    if (destinations.len == 0 or destinations.len > 4096 or options.buffer_bytes < 16 * 1024 or options.buffer_bytes > 64 * 1024 * 1024) return error.InvalidRelationPopulation;
    if (options.stats) |stats| stats.* = .{};
    const first = destinations[0];
    const binding = first.scope.population orelse return error.InvalidRelationPopulation;
    var groups: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer groups.deinit(a);
    for (destinations, 0..) |destination, index| {
        try destination.scope.validate();
        var common = destination.scope.population orelse return error.InvalidRelationPopulation;
        common.partition = binding.partition;
        if (!std.meta.eql(common, binding) or destination.group != destination.scope.target_namespace.shard_id or
            destination.scope.target_namespace.table_id != first.scope.target_namespace.table_id or
            !std.mem.eql(u8, destination.table, first.table) or !std.mem.eql(u8, &destination.scope.plan_id, &first.scope.plan_id) or
            !std.mem.eql(u8, &destination.scope.plan_digest, &first.scope.plan_digest) or
            !std.mem.eql(u8, &binding.definition, &source.definition) or !std.mem.eql(u8, &binding.source_definition, &source.source_definition)) return error.RelationPopulationChanged;
        if ((try groups.getOrPut(a, destination.group)).found_existing) return error.InvalidRelationPopulation;
        if (index == 0) {
            if (destination.range.start.len != 0) return error.InvalidRelationPopulation;
        } else if (destination.range.start.len == 0 or !std.mem.eql(u8, destinations[index - 1].range.end, destination.range.start)) return error.InvalidRelationPopulation;
        if ((index + 1 != destinations.len and destination.range.end.len == 0) or
            (index + 1 == destinations.len and destination.range.end.len != 0) or
            (destination.range.end.len != 0 and std.mem.order(u8, destination.range.start, destination.range.end) != .lt)) return error.InvalidRelationPopulation;
    }
    const cursors = try a.alloc(contract.Cursor, destinations.len);
    errdefer a.free(cursors);
    var sealed: usize = 0;
    var fresh: usize = 0;
    // Inspect the whole cohort before consuming any fresh producer lane.
    for (destinations, cursors) |destination, *cursor| {
        const status = try destination.port.execute(a, destination.table, destination.group, .{ .scope = destination.scope, .action = .status }, context);
        cursor.* = try contract.Cursor.init(destination.scope.population.?);
        if (status.population) |observed| {
            try observed.validate();
            if (!std.mem.eql(u8, &observed.binding, &cursor.binding) or status.rows != observed.rows) return error.RelationPopulationChanged;
            if (observed.sealed and (status.phase == .imported or status.phase == .validated or status.phase == .published)) {
                sealed += 1;
                cursor.* = observed;
                continue;
            }
        }
        if (status.phase != .reserved) return error.SourceSnapshotIncomplete;
        fresh += 1;
    }
    if (sealed == destinations.len) {
        try verifyCompletion(cursors);
        return cursors;
    }
    if (fresh != destinations.len) return error.SourceSnapshotIncomplete;
    for (destinations, cursors) |destination, cursor| {
        const begun = try acknowledge(a, destination, .{ .scope = destination.scope, .action = .begin }, cursor, context);
        if (begun.phase != .importing) return error.RelationPopulationChanged;
    }
    var memory: local.sql_memory_budget = .{ .backing = a, .limit = options.buffer_bytes };
    const buffered_alloc = memory.allocator();
    const pending = try a.alloc(Pending, destinations.len);
    @memset(pending, .{});
    defer {
        for (pending) |*buffer| buffer.deinit(buffered_alloc);
        a.free(pending);
        std.debug.assert(memory.live == 0);
        if (options.stats) |stats| {
            stats.buffer_peak_bytes = memory.peak;
            for (cursors) |cursor| stats.page_requests +|= cursor.pages;
        }
    }
    var source_rows: u64 = 0;
    while (try source.next(source.ptr, contract.max_page_rows)) |rows| {
        if (rows.len == 0 or rows.len > contract.max_page_rows) return error.InvalidRelationPopulation;
        for (rows) |row| {
            if (row.ordinal != source_rows or source_rows == std.math.maxInt(u64)) return error.InvalidRelationPopulation;
            source_rows += 1;
            if (options.stats) |stats| stats.source_rows = source_rows;
            const key = contract.rowKey(binding.capture_id, row.ordinal);
            const owner = route(destinations, &key);
            const destination = destinations[owner];
            if (row.document.len == 0 or row.json_null_fields.len > contract.max_null_fields) return error.InvalidRelationPopulation;
            const bytes = try BufferedRow.size(row);
            if (bytes >= options.buffer_bytes / 2) {
                try flush(a, buffered_alloc, destination, &cursors[owner], &pending[owner], context);
                try send(a, destination, &cursors[owner], &.{row}, context);
                continue;
            }
            if (pending[owner].bytes + bytes > 256 * 1024) try flush(a, buffered_alloc, destination, &cursors[owner], &pending[owner], context);
            pending[owner].append(buffered_alloc, row) catch |err| {
                if (err != error.OutOfMemory) return err;
                // Allocation failure leaves previous rows intact. Drain their
                // acknowledged prefix, then copy the SAME still-borrowed row.
                for (destinations, cursors, pending) |target, *cursor, *buffer| try flush(a, buffered_alloc, target, cursor, buffer, context);
                try pending[owner].append(buffered_alloc, row);
            };
            if (pending[owner].rows.items.len == contract.max_page_rows) try flush(a, buffered_alloc, destination, &cursors[owner], &pending[owner], context);
        }
    }
    try source.finish(source.ptr);
    for (destinations, cursors, pending) |destination, *cursor, *buffer| try flush(a, buffered_alloc, destination, cursor, buffer, context);
    for (destinations, cursors) |destination, *cursor| {
        const completion = cursor.completionForSource(source_rows);
        const expected = try cursor.seal(completion);
        const receipt = try acknowledge(a, destination, .{ .scope = destination.scope, .action = .import_page, .population_complete = completion }, expected, context);
        if (receipt.phase != .imported) return error.RelationPopulationChanged;
        cursor.* = expected;
    }
    try verifyCompletion(cursors);
    return cursors;
}

fn route(destinations: []const Destination, key: []const u8) usize {
    var low: usize = 0;
    var high = destinations.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        const upper = destinations[mid].range.end;
        if (upper.len != 0 and std.mem.order(u8, key, upper) != .lt) low = mid + 1 else high = mid;
    }
    return low;
}
fn verifyCompletion(cursors: []const contract.Cursor) !void {
    var rows: u64 = 0;
    const total = cursors[0].source_rows orelse return error.SourceSnapshotIncomplete;
    for (cursors) |cursor| {
        try cursor.validate();
        if (!cursor.sealed or cursor.source_rows.? != total) return error.SourceSnapshotIncomplete;
        rows = std.math.add(u64, rows, cursor.rows) catch return error.InvalidRelationPopulation;
    }
    if (rows != total) return error.SourceSnapshotIncomplete;
}
fn flush(a: std.mem.Allocator, buffered_alloc: std.mem.Allocator, destination: Destination, cursor: *contract.Cursor, buffer: *Pending, context: operation.RequestContext) !void {
    if (buffer.rows.items.len != 0) {
        var rows: [contract.max_page_rows]contract.Row = undefined;
        for (buffer.rows.items, rows[0..buffer.rows.items.len]) |owned, *row| row.* = owned.row;
        try send(a, destination, cursor, rows[0..buffer.rows.items.len], context);
    }
    buffer.deinit(buffered_alloc);
}
fn send(a: std.mem.Allocator, destination: Destination, cursor: *contract.Cursor, rows: []const contract.Row, context: operation.RequestContext) !void {
    const page: contract.Page = .{ .binding = cursor.binding, .sequence = cursor.pages, .first_row = cursor.rows, .previous = cursor.chain, .rows = rows };
    const next = switch (try cursor.admit(page)) {
        .apply => |value| value,
        .replay => return error.RelationPopulationChanged,
    };
    const receipt = try acknowledge(a, destination, .{ .scope = destination.scope, .action = .import_page, .population_page = page }, next, context);
    if (receipt.phase != .importing) return error.RelationPopulationChanged;
    cursor.* = next;
}

fn acknowledge(a: std.mem.Allocator, destination: Destination, request: owners.Request, expected: contract.Cursor, context: operation.RequestContext) !owners.Response {
    const response = destination.port.execute(a, destination.table, destination.group, request, context) catch |send_error| blk: {
        // Never ask the source for another page after an uncertain response.
        // A read-indexed exact receipt can resolve a lost ACK without redoing
        // expression evaluation or mutating the receiver again.
        const observed = destination.port.execute(a, destination.table, destination.group, request.statusRead(), context) catch return send_error;
        if (!std.meta.eql(observed.population orelse return send_error, expected)) return send_error;
        break :blk observed;
    };
    if (!std.meta.eql(response.population orelse return error.InvalidSqlBackendResponse, expected) or response.rows != expected.rows) return error.RelationPopulationChanged;
    return response;
}

const CohortFixture = struct {
    const count = 16;
    states: [count]struct { cursor: ?contract.Cursor = null, phase: local.storage_db_restore_staging.Phase = .reserved, lose_reply: bool = true } = @splat(.{}),
    destinations: [count]Destination = undefined,
    calls: usize = 0,
    producer_calls: usize = 0,
    source_limit: u64 = 4097,
    ordinal: u64 = 0,
    fail_after: ?u64 = null,
    scratch: [256]u8 = undefined,
    nulls: [1][]const u8 = undefined,
    null_name: [1]u8 = .{'j'},
    row: [1]contract.Row = undefined,
    fn init(self: *@This()) void {
        const alphabet = "0123456789abcdef";
        const capture: contract.Capture = .{ .expires_at_ms = 9000000000000, .capture_id = @splat(1), .definition = @splat(2), .source_definition = @splat(3), .target_schema = @splat(4), .principal_instance = @splat(5), .catalog_revision = 6, .write_version = 7 };
        for (&self.destinations, 0..) |*destination, i| {
            const binding = capture.forPartition(i + 1);
            destination.* = .{ .port = .{ .ptr = self, .execute_fn = execute }, .table = "dst", .group = i + 1, .range = .{ .start = if (i == 0) "" else alphabet[i..][0..1], .end = if (i + 1 == count) "" else alphabet[i + 1 ..][0..1] }, .scope = .{ .plan_id = @splat(8), .plan_digest = @splat(9), .source_artifact_digest = binding.fingerprint(), .source_namespace = .{ .table_id = 0, .shard_id = 0, .range_id = 0 }, .target_namespace = .{ .table_id = 31, .shard_id = i + 1, .range_id = i + 1 }, .target_schema_digest = capture.target_schema, .population = binding } };
        }
    }
    fn source(self: *@This()) catalog.PopulationSource {
        return .{ .ptr = self, .schema_json = "{}", .definition = @splat(2), .source_definition = @splat(3), .next = next, .finish = finish };
    }
    fn next(raw: *anyopaque, _: u32) !?[]const contract.Row {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.producer_calls += 1;
        if (self.fail_after) |cut| if (self.ordinal >= cut) return error.QueryCanceled;
        if (self.ordinal == self.source_limit) return null;
        self.null_name[0] = 'j';
        self.nulls[0] = &self.null_name;
        const document = try std.fmt.bufPrint(&self.scratch, "{{\"n\":{d},\"j\":null}}", .{self.ordinal});
        self.row[0] = .{ .ordinal = self.ordinal, .document = document, .json_null_fields = &self.nulls };
        self.ordinal += 1;
        return &self.row;
    }
    fn finish(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.ordinal != self.source_limit) return error.SourceSnapshotIncomplete;
        @memset(&self.scratch, 'X');
        self.null_name[0] = 'Q';
    }
    fn execute(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, group: u64, request: owners.Request, _: operation.RequestContext) !owners.Response {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.calls += 1;
        try request.validate(group);
        const index: usize = @intCast(group - 1);
        const state = &self.states[index];
        switch (request.action) {
            .status => {},
            .begin => {
                if (state.phase != .reserved) return error.SourceSnapshotIncomplete;
                state.cursor = try contract.Cursor.init(request.scope.population.?);
                state.phase = .importing;
            },
            .import_page => {
                if (request.population_page) |page| {
                    for (page.rows) |row| {
                        const key = contract.rowKey(request.scope.population.?.capture_id, row.ordinal);
                        try std.testing.expect(self.destinations[index].range.contains(&key));
                        var document = try std.json.parseFromSlice(std.json.Value, a, row.document, .{});
                        defer document.deinit();
                        try std.testing.expectEqual(@as(i64, @intCast(row.ordinal)), document.value.object.get("n").?.integer);
                        try std.testing.expectEqual(@as(usize, 1), row.json_null_fields.len);
                        try std.testing.expectEqualStrings("j", row.json_null_fields[0]);
                    }
                    state.cursor = (try state.cursor.?.admit(page)).apply;
                    if (state.lose_reply) {
                        state.lose_reply = false;
                        return error.InjectedPopulationReplyLoss;
                    }
                } else {
                    state.cursor = try state.cursor.?.seal(request.population_complete.?);
                    state.phase = .imported;
                }
            },
            else => return error.UnexpectedMutation,
        }
        return .{ .phase = state.phase, .rows = if (state.cursor) |cursor| cursor.rows else 0, .receipt = @splat(1), .population = state.cursor };
    }
};

test "restore owner SQL population cohort coalesces tiny source pages across sixteen ranges with bounded owned buffers" {
    const a = std.testing.allocator;
    for ([_]usize{ 16 * 1024, 8 * 1024 * 1024 }) |buffer_bytes| {
        var fixture: CohortFixture = .{};
        fixture.init();
        var stats: Stats = .{};
        const receipts = try deliverCohort(a, &fixture.destinations, fixture.source(), .{}, .{ .buffer_bytes = buffer_bytes, .stats = &stats });
        defer a.free(receipts);
        var total: u64 = 0;
        for (receipts) |receipt| {
            try std.testing.expect(receipt.sealed and receipt.rows > 100);
            try std.testing.expectEqual(@as(?u64, fixture.source_limit), receipt.source_rows);
            total += receipt.rows;
        }
        try std.testing.expectEqual(fixture.source_limit, total);
        try std.testing.expectEqual(total, stats.source_rows);
        try std.testing.expect(stats.buffer_peak_bytes <= buffer_bytes);
        if (buffer_bytes > 16 * 1024) try std.testing.expect(stats.page_requests <= fixture.source_limit / contract.max_page_rows + CohortFixture.count);
        try std.testing.expect(stats.page_requests < fixture.source_limit / 2);
        const calls = fixture.producer_calls;
        const replay = try deliverCohort(a, &fixture.destinations, fixture.source(), .{}, .{});
        defer a.free(replay);
        try std.testing.expectEqualDeep(receipts, replay);
        try std.testing.expectEqual(calls, fixture.producer_calls);
        fixture.states[0].cursor.?.source_rows.? += 1;
        try std.testing.expectError(error.SourceSnapshotIncomplete, deliverCohort(a, &fixture.destinations, fixture.source(), .{}, .{}));
        try std.testing.expectEqual(calls, fixture.producer_calls);
    }
}

test "restore owner SQL population cohort rejects partial recovery and incomplete range coverage before producer evaluation" {
    const a = std.testing.allocator;
    var fixture: CohortFixture = .{};
    fixture.init();
    fixture.destinations[1].range.start = "!";
    try std.testing.expectError(error.InvalidRelationPopulation, deliverCohort(a, &fixture.destinations, fixture.source(), .{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    try std.testing.expectEqual(@as(usize, 0), fixture.producer_calls);
    fixture.init();
    fixture.fail_after = 3000;
    try std.testing.expectError(error.QueryCanceled, deliverCohort(a, &fixture.destinations, fixture.source(), .{}, .{}));
    const calls = fixture.producer_calls;
    fixture.fail_after = null;
    fixture.ordinal = 0;
    try std.testing.expectError(error.SourceSnapshotIncomplete, deliverCohort(a, &fixture.destinations, fixture.source(), .{}, .{}));
    try std.testing.expectEqual(calls, fixture.producer_calls);
}
