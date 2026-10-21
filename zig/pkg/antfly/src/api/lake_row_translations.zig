// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Compact original-coordinate -> current-coordinate/rank maps. Sorted joins
//! use private bounded scratch; readers need one short-key seek per candidate.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const Pages = @import("../serverless/graph_segment/page_store.zig").PageStore;
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
pub fn build(a: A, pages: *Pages, original: identity.Space, current: identity.Space, context: Context, spill_bytes: u64) !?identity.Translation {
    if (spill_bytes == 0 or original.rows == 0 or original.file_slots == null or current.file_slots == null or current.by_order == null) return null;
    const io = context.io orelse return error.InvalidLogicalRowIdentity;
    const scheduler = local.sql_parallel_scheduler.global();
    const retained = scheduler.reservePullUpTo(1, 1024 * 1024, 64 * 1024 * 1024) orelse return null;
    defer scheduler.releasePull(1, retained);
    const allowance = scheduler.cpuWorkspaceBudget(@min(retained, 8 * 1024 * 1024));
    if (allowance < 1024 * 1024) return null;
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            try @as(*const Context, @ptrCast(@alignCast(raw))).ensureActive();
        }
    };
    var check_context = context;
    var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = scheduler, .bytes = allowance, .checkpoint = .{ .ptr = &check_context, .check = Check.call } };
    defer execution.pause();
    try execution.enter(io);
    var phase: @import("../serverless/artifacts/store.zig").ExecutionIo = .{ .execution = &execution, .io = io };
    var store = pages.artifacts.*;
    store.io_phase = phase.capability();
    var admitted = pages.*;
    admitted.artifacts = &store;
    admitted.read_phase = store.io_phase;
    var workspace: local.sql_memory_budget = .{ .backing = a, .limit = retained };
    return buildInner(workspace.allocator(), &admitted, original, current, context, spill_bytes) catch |err| {
        if (err == error.SqlProgramLimitExceeded or (err == error.OutOfMemory and workspace.isLocallyExhausted())) return null;
        return err;
    };
}
fn buildInner(a: A, pages: *Pages, original: identity.Space, current: identity.Space, context: Context, spill_bytes: u64) !?identity.Translation {
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            try @as(*const Context, @ptrCast(@alignCast(raw))).ensureActive();
        }
    };
    var check_context = context;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = context.io orelse return error.InvalidLogicalRowIdentity, .context = &check_context, .checkpoint = Check.call, .max_bytes = spill_bytes };
    defer manager.deinit();
    var ranks = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 1024 * 1024);
    defer ranks.deinit();
    var forward = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 1024 * 1024);
    defer forward.deinit();
    var reverse = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 1024 * 1024);
    defer reverse.deinit();
    inline for (.{ &ranks, &forward, &reverse }) |sort| {
        sort.shared_runs = true;
        sort.encoded_bytes = true;
    }
    var order = tree.Cursor.init(a, pages.store(), current.by_order, "", null) catch |err| return identity.derivedReadFailure(err);
    defer order.deinit();
    var rank: u64 = 0;
    while (order.next() catch |err| return identity.derivedReadFailure(err)) |record| {
        try context.ensureActive();
        if (record.key.len != 44 or record.value.len != 32 or rank > std.math.maxInt(i64)) return error.InvalidLogicalRowIdentity;
        try ranks.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = record.value })}, .values = &.{local.sql_scalar.Datum.json(.{ .integer = @intCast(rank) })}, .ordinal = rank });
        rank += 1;
    }
    if (rank != current.rows or rank != original.rows) return error.InvalidLogicalRowIdentity;
    var old = tree.Cursor.init(a, pages.store(), original.by_id, "", null) catch |err| return identity.derivedReadFailure(err);
    defer old.deinit();
    var next = tree.Cursor.init(a, pages.store(), current.by_id, "", null) catch |err| return identity.derivedReadFailure(err);
    defer next.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var count: u64 = 0;
    while (old.next() catch |err| return identity.derivedReadFailure(err)) |previous| {
        try context.ensureActive();
        _ = scratch.reset(.retain_capacity);
        const current_record = (next.next() catch |err| return identity.derivedReadFailure(err)) orelse return error.InvalidLogicalRowIdentity;
        const row = (try ranks.next(scratch.allocator())) orelse return error.InvalidLogicalRowIdentity;
        if (previous.key.len != 32 or previous.value.len != 56 or current_record.value.len != 56 or row.keys.len != 1 or row.keys[0].value != .string or row.values.len != 1 or row.values[0].value != .integer or row.values[0].value.integer < 0 or !std.mem.eql(u8, previous.key, current_record.key) or !std.mem.eql(u8, previous.key, row.keys[0].value.string)) return error.InvalidLogicalRowIdentity;
        var value: [24]u8 = undefined;
        value[0..16].* = current_record.value[0..16].*;
        std.mem.writeInt(u64, value[16..24], @intCast(row.values[0].value.integer), .big);
        try forward.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = previous.value[0..16] })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = &value })}, .ordinal = count });
        try reverse.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = current_record.value[0..16] })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = previous.value[0..16] })}, .ordinal = count });
        count += 1;
    }
    if (count != current.rows or (next.next() catch |err| return identity.derivedReadFailure(err)) != null or (try ranks.next(scratch.allocator())) != null) return error.InvalidLogicalRowIdentity;
    var forward_rows: Sorted = .{ .sort = &forward, .scratch = .init(a), .width = 24 };
    defer forward_rows.scratch.deinit();
    const forward_ref = (try tree.buildSortedWithOptions(a, pages.store(), &forward_rows, .{ .target_bytes = tree.bulk_target_page_bytes })) orelse return error.InvalidLogicalRowIdentity;
    var reverse_rows: Sorted = .{ .sort = &reverse, .scratch = .init(a), .width = 16 };
    defer reverse_rows.scratch.deinit();
    const reverse_ref = (try tree.buildSortedWithOptions(a, pages.store(), &reverse_rows, .{ .target_bytes = tree.bulk_target_page_bytes })) orelse return error.InvalidLogicalRowIdentity;
    if (forward_ref.records != current.rows or reverse_ref.records != current.rows) return error.InvalidLogicalRowIdentity;
    return .{ .forward = forward_ref, .reverse = reverse_ref };
}
const Sorted = struct {
    sort: *local.sql_spill.Sort,
    scratch: std.heap.ArenaAllocator,
    last: ?[16]u8 = null,
    width: usize,
    pub fn next(self: *Sorted) !?tree.Cursor.Record {
        _ = self.scratch.reset(.retain_capacity);
        const row = (try self.sort.next(self.scratch.allocator())) orelse return null;
        if (row.keys.len != 1 or row.values.len != 1 or row.keys[0].value != .string or row.values[0].value != .string or row.keys[0].value.string.len != 16 or row.values[0].value.string.len != self.width) return error.InvalidLogicalRowIdentity;
        const key = row.keys[0].value.string;
        if (self.last) |last| if (std.mem.order(u8, &last, key) != .lt) return error.InvalidLogicalRowIdentity;
        self.last = key[0..16].*;
        return .{ .key = key, .value = row.values[0].value.string };
    }
};

test "external lake compact translations preserve shuffled coordinates ranks and bounded fallback" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-compact-translations");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call };
    defer manager.deinit();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: Pages = .{ .domain = @splat(7), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    const Builder = @import("lake_row_identity_delta.zig").Builder;
    const first = try Builder.create(a, &manager, &pages, &.{"old"}, null, null, &.{}, "source", "one");
    defer first.deinit();
    const next = try Builder.create(a, &manager, &pages, &.{"new"}, null, null, &.{}, "source", "two");
    defer next.deinit();
    for (0..20) |row| {
        var id: [32]u8 = @splat(3);
        id[31] = @intCast(row);
        const state: identity.logical.State = .{ .id = id, .values = @splat(4) };
        try first.add(state, .{ .file = 0, .group = 0, .row = row });
        try next.add(state, .{ .file = 0, .group = 2, .row = 19 - row });
    }
    const old = try first.finish(@splat(1), @splat(2), @splat(3));
    const current = try next.finish(@splat(1), @splat(2), @splat(4));
    const context: Context = .{ .io = std.testing.io };
    try std.testing.expect((try build(a, &pages, old.current, current.current, context, 0)) == null);
    const translation = (try build(a, &pages, old.current, current.current, context, 1024 * 1024)).?;
    var reader: identity.Reader = .{ .root = current, .pages = &pages };
    for (0..20) |row| {
        const coordinate: identity.logical.Coordinate = .{ .file = 0, .group = 0, .row = row };
        const mapped = try reader.translateAt(a, translation, coordinate, true);
        try std.testing.expectEqual(@as(u64, 19 - row), mapped.coordinate.row);
        try std.testing.expectEqual(mapped.coordinate.row, mapped.rank);
        const reverse = try reader.translateAt(a, translation, mapped.coordinate, false);
        try std.testing.expectEqual(coordinate, reverse.coordinate);
    }
    const batch = try reader.translateDenseMany(a, translation, &.{ .{ .file = 0, .group = 0, .row = 19 }, .{ .file = 0, .group = 0, .row = 1 }, .{ .file = 0, .group = 0, .row = 19 } }, true);
    defer a.free(batch);
    try std.testing.expectEqual(@as(u64, 0), batch[0].coordinate.row);
    try std.testing.expectEqual(@as(u64, 18), batch[1].coordinate.row);
    try std.testing.expectEqual(@as(u64, 0), batch[2].coordinate.row);
}
