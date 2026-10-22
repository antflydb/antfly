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

//! Persistent COW coordinate maps. Canonical ranks stay in current.by_order;
//! small rewrites mutate only changed addresses and never sort the archive.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const Pages = @import("../serverless/graph_segment/page_store.zig").PageStore;
const Mutations = @import("lake_index_page_mutations.zig").Mutations;
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;

pub fn build(a: A, pages: *Pages, original: identity.Space, current: identity.Space, previous: ?identity.Space, prior: ?identity.Translation, context: Context, spill_bytes: u64) !?identity.Translation {
    return buildWithOriginal(a, pages, original, current, previous, null, prior, context, spill_bytes);
}
/// Logical fragment replacement can change both sides of the coordinate map.
/// Visit the union of changed logical IDs and update only their live addresses.
pub fn buildWithOriginal(a: A, pages: *Pages, original: identity.Space, current: identity.Space, previous: ?identity.Space, previous_original: ?identity.Space, prior: ?identity.Translation, context: Context, spill_bytes: u64) !?identity.Translation {
    if (spill_bytes == 0 or original.rows == 0 or original.rows != current.rows or original.file_slots == null or current.file_slots == null or current.by_order == null) return null;
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
    admitted.admitPublication();
    var workspace: local.sql_memory_budget = .{ .backing = a, .limit = retained };
    var manager: local.sql_spill.Manager = .{ .alloc = workspace.allocator(), .io = io, .context = &check_context, .checkpoint = Check.call, .max_bytes = spill_bytes };
    defer manager.deinit();
    return buildInnerExtended(workspace.allocator(), &admitted, &manager, original, current, previous, previous_original, prior, context) catch |err| {
        if (err == error.SqlProgramLimitExceeded or (err == error.OutOfMemory and workspace.isLocallyExhausted())) return null;
        return err;
    };
}
fn buildInner(a: A, pages: *Pages, manager: *local.sql_spill.Manager, original: identity.Space, current: identity.Space, previous: ?identity.Space, prior: ?identity.Translation, context: Context) !identity.Translation {
    return buildInnerExtended(a, pages, manager, original, current, previous, null, prior, context);
}
fn buildInnerExtended(a: A, pages: *Pages, manager: *local.sql_spill.Manager, original: identity.Space, current: identity.Space, previous: ?identity.Space, previous_original: ?identity.Space, prior: ?identity.Translation, context: Context) !identity.Translation {
    if (prior != null and !prior.?.ranked and previous != null and previous_original != null) return changedOriginal(a, pages, manager, original, current, previous.?, previous_original.?, prior.?, context);
    if (prior != null and !prior.?.ranked and previous != null and previous.?.rows == current.rows) {
        var forward = Mutations.init(a, manager, pages.store());
        forward.private_pages.phase = pages.artifacts.io_phase;
        defer forward.deinit();
        var reverse = Mutations.init(a, manager, pages.store());
        reverse.private_pages.phase = pages.artifacts.io_phase;
        // Swaps delete an address and insert another ID at that address.
        // Additions must win across both sort batches and forest levels.
        reverse.addition_first = true;
        reverse.forest.policy = .addition_first;
        defer reverse.deinit();
        const Visitor = struct {
            a: A,
            pages: *Pages,
            original: identity.Space,
            cursor: ?tree.Cursor = null,
            forward: *Mutations,
            reverse: *Mutations,
            context: Context,
            pub fn change(self: *@This(), left: ?tree.Cursor.Record, right: ?tree.Cursor.Record) !void {
                try self.context.ensureActive();
                const old = left orelse return error.InvalidLogicalRowIdentity;
                const next = right orelse return error.InvalidLogicalRowIdentity;
                if (old.key.len != 32 or old.value.len != 56 or next.value.len != 56 or !std.mem.eql(u8, old.key, next.key)) return error.InvalidLogicalRowIdentity;
                if (std.mem.eql(u8, old.value[0..16], next.value[0..16])) return;
                if (self.cursor == null) self.cursor = try tree.Cursor.init(self.a, self.pages.store(), self.original.by_id, old.key, null);
                try self.cursor.?.seekForward(old.key);
                const address = (try self.cursor.?.next()) orelse return error.InvalidLogicalRowIdentity;
                if (!std.mem.eql(u8, address.key, old.key) or address.value.len != 56) return error.InvalidLogicalRowIdentity;
                try self.forward.addRecord(address.value[0..16], next.value[0..16]);
                try self.reverse.addRecord(old.value[0..16], null);
                try self.reverse.addRecord(next.value[0..16], address.value[0..16]);
            }
        };
        var visitor: Visitor = .{ .a = a, .pages = pages, .original = original, .forward = &forward, .reverse = &reverse, .context = context };
        defer if (visitor.cursor) |*cursor| cursor.deinit();
        tree.visitDifferences(a, pages.store(), previous.?.by_id, current.by_id, &visitor) catch |err| return identity.derivedReadFailure(err);
        const result: identity.Translation = .{ .forward = (try forward.finish(prior.?.forward)) orelse return error.InvalidLogicalRowIdentity, .reverse = (try reverse.finish(prior.?.reverse)) orelse return error.InvalidLogicalRowIdentity, .ranked = false };
        if (result.forward.records != current.rows or result.reverse.records != current.rows) return error.InvalidLogicalRowIdentity;
        return result;
    }
    var forward = local.sql_spill.Sort.init(a, manager, &.{.{}}, 1024 * 1024);
    defer forward.deinit();
    var reverse = local.sql_spill.Sort.init(a, manager, &.{.{}}, 1024 * 1024);
    defer reverse.deinit();
    inline for (.{ &forward, &reverse }) |sort| {
        sort.shared_runs = true;
        sort.encoded_bytes = true;
    }
    var old = try tree.Cursor.init(a, pages.store(), original.by_id, "", null);
    defer old.deinit();
    var next = try tree.Cursor.init(a, pages.store(), current.by_id, "", null);
    defer next.deinit();
    var count: u64 = 0;
    while (try old.next()) |record| {
        try context.ensureActive();
        const mapped = (try next.next()) orelse return error.InvalidLogicalRowIdentity;
        if (record.key.len != 32 or record.value.len != 56 or mapped.value.len != 56 or !std.mem.eql(u8, record.key, mapped.key)) return error.InvalidLogicalRowIdentity;
        try forward.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = record.value[0..16] })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = mapped.value[0..16] })}, .ordinal = count });
        try reverse.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = mapped.value[0..16] })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = record.value[0..16] })}, .ordinal = count });
        count += 1;
    }
    if (count != current.rows or (try next.next()) != null) return error.InvalidLogicalRowIdentity;
    var rows: Sorted = .{ .sort = &forward, .scratch = .init(a) };
    defer rows.scratch.deinit();
    const f = (try tree.buildSortedWithOptions(a, pages.store(), &rows, .{ .target_bytes = tree.bulk_target_page_bytes })) orelse return error.InvalidLogicalRowIdentity;
    var inverse: Sorted = .{ .sort = &reverse, .scratch = .init(a) };
    defer inverse.scratch.deinit();
    const r = (try tree.buildSortedWithOptions(a, pages.store(), &inverse, .{ .target_bytes = tree.bulk_target_page_bytes })) orelse return error.InvalidLogicalRowIdentity;
    return .{ .forward = f, .reverse = r, .ranked = false };
}
fn changedOriginal(a: A, pages: *Pages, manager: *local.sql_spill.Manager, original: identity.Space, current: identity.Space, previous: identity.Space, old_original: identity.Space, prior: identity.Translation, context: Context) !identity.Translation {
    if (previous.rows != old_original.rows or prior.forward.records != previous.rows or prior.reverse.records != previous.rows) return error.InvalidLogicalRowIdentity;
    var changes = local.sql_spill.Sort.init(a, manager, &.{.{}}, 512 * 1024);
    defer changes.deinit();
    changes.shared_runs = true;
    changes.encoded_bytes = true;
    const Visitor = struct {
        sort: *local.sql_spill.Sort,
        context: Context,
        pub fn change(self: *@This(), left: ?tree.Cursor.Record, right: ?tree.Cursor.Record) !void {
            try self.context.ensureActive();
            if (left) |row| if (row.key.len != 32 or row.value.len != 56) return error.InvalidLogicalRowIdentity;
            if (right) |row| if (row.key.len != 32 or row.value.len != 56) return error.InvalidLogicalRowIdentity;
            if (left != null and right != null and std.mem.eql(u8, left.?.value[0..16], right.?.value[0..16])) return;
            const row = right orelse left.?;
            try self.sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = row.key })}, .values = &.{}, .ordinal = self.sort.total });
        }
    };
    var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store(), .max_bytes = 2 * 1024 * 1024 };
    defer cache.deinit();
    var visitor: Visitor = .{ .sort = &changes, .context = context };
    tree.visitDifferences(a, cache.store(), previous.by_id, current.by_id, &visitor) catch |err| return identity.derivedReadFailure(err);
    tree.visitDifferences(a, cache.store(), old_original.by_id, original.by_id, &visitor) catch |err| return identity.derivedReadFailure(err);
    var forward = Mutations.init(a, manager, pages.store());
    forward.private_pages.phase = pages.artifacts.io_phase;
    forward.addition_first = true;
    forward.forest.policy = .addition_first;
    defer forward.deinit();
    var reverse = Mutations.init(a, manager, pages.store());
    reverse.private_pages.phase = pages.artifacts.io_phase;
    reverse.addition_first = true;
    reverse.forest.policy = .addition_first;
    defer reverse.deinit();
    var reader: identity.Reader = .{ .root = undefined, .pages = pages, .cache = &cache };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var last: ?[32]u8 = null;
    while (true) {
        try context.ensureActive();
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const row = (try changes.next(sa)) orelse break;
        if (row.keys.len != 1 or row.keys[0].value != .string or row.keys[0].value.string.len != 32) return error.InvalidLogicalRowIdentity;
        const id = row.keys[0].value.string[0..32].*;
        if (last) |old| if (std.mem.eql(u8, &old, &id)) continue;
        last = id;
        const old_index = try reader.byId(sa, old_original, id);
        const old_source = try reader.byId(sa, previous, id);
        const new_index = try reader.byId(sa, original, id);
        const new_source = try reader.byId(sa, current, id);
        if ((old_index == null) != (old_source == null) or (new_index == null) != (new_source == null)) return error.InvalidLogicalRowIdentity;
        if (old_index) |old| {
            try forward.addRecord(&old.coordinate.encode(), null);
            try reverse.addRecord(&old_source.?.coordinate.encode(), null);
        }
        if (new_index) |next| {
            try forward.addRecord(&next.coordinate.encode(), &new_source.?.coordinate.encode());
            try reverse.addRecord(&new_source.?.coordinate.encode(), &next.coordinate.encode());
        }
    }
    const result: identity.Translation = .{ .forward = (try forward.finish(prior.forward)) orelse return error.InvalidLogicalRowIdentity, .reverse = (try reverse.finish(prior.reverse)) orelse return error.InvalidLogicalRowIdentity, .ranked = false };
    if (result.forward.records != current.rows or result.reverse.records != current.rows) return error.InvalidLogicalRowIdentity;
    return result;
}
const Sorted = struct {
    sort: *local.sql_spill.Sort,
    scratch: std.heap.ArenaAllocator,
    last: ?[16]u8 = null,
    pub fn next(self: *Sorted) !?tree.Cursor.Record {
        _ = self.scratch.reset(.retain_capacity);
        const row = (try self.sort.next(self.scratch.allocator())) orelse return null;
        if (row.keys.len != 1 or row.values.len != 1 or row.keys[0].value != .string or row.values[0].value != .string or row.keys[0].value.string.len != 16 or row.values[0].value.string.len != 16) return error.InvalidLogicalRowIdentity;
        const key = row.keys[0].value.string;
        if (self.last) |last| if (std.mem.order(u8, &last, key) != .lt) return error.InvalidLogicalRowIdentity;
        self.last = key[0..16].*;
        return .{ .key = key, .value = row.values[0].value.string };
    }
};

test "external lake persistent translations retain unchanged maps and update swapped addresses" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-cow-translations");
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
    const second = try Builder.create(a, &manager, &pages, &.{"new"}, null, null, &.{}, "source", "two");
    defer second.deinit();
    for (0..300) |row| {
        var id: [32]u8 = @splat(3);
        std.mem.writeInt(u32, id[28..32], @intCast(row), .big);
        const state: identity.logical.State = .{ .id = id, .values = @splat(4) };
        try first.add(state, .{ .file = 0, .group = 0, .row = row });
        try second.add(state, .{ .file = 0, .group = 2, .row = 299 - row });
    }
    const original = try first.finish(@splat(1), @splat(2), @splat(3));
    const current = try second.finish(@splat(1), @splat(2), @splat(4));
    const context: Context = .{ .io = std.testing.io };
    const initial = (try build(a, &pages, original.current, current.current, null, null, context, 1024 * 1024)).?;
    try std.testing.expect(!initial.ranked);
    const same = (try build(a, &pages, original.current, current.current, current.current, initial, context, 1024 * 1024)).?;
    try std.testing.expect(same.forward.eql(initial.forward) and same.reverse.eql(initial.reverse));
    try std.testing.expect(try build(a, &pages, original.current, current.current, null, null, context, 0) == null);
    try std.testing.expectError(error.DeadlineExceeded, build(a, &pages, original.current, current.current, current.current, initial, .{ .io = std.testing.io, .deadline_ns = 0 }, 1024 * 1024));
    var third = current.current;
    var ids: [2][32]u8 = @splat(@splat(3));
    var values: [2][56]u8 = undefined;
    var edits: [2]tree.Mutation = undefined;
    var reader: identity.Reader = .{ .root = current, .pages = &pages };
    for (&ids, &values, &edits, 0..) |*id, *value, *edit, row| {
        std.mem.writeInt(u32, id[28..32], @intCast(row), .big);
        value[0..16].* = (identity.logical.Coordinate{ .file = 0, .group = 2, .row = 298 + row }).encode();
        value[16..48].* = @splat(4);
        value[48..56].* = @splat(0);
        edit.* = .{ .key = id, .value = value };
    }
    third.by_id = try tree.apply(a, pages.store(), third.by_id, &edits);
    const swapped = (try build(a, &pages, original.current, third, current.current, initial, context, 1024 * 1024)).?;
    for (0..2) |row| {
        const from: identity.logical.Coordinate = .{ .file = 0, .group = 0, .row = row };
        const mapped = try reader.translateAt(a, swapped, from, true);
        try std.testing.expectEqual(@as(u64, 298 + row), mapped.coordinate.row);
        const reversed = try reader.translateAt(a, swapped, mapped.coordinate, false);
        try std.testing.expectEqual(from, reversed.coordinate);
    }
    const Failure = struct {
        fn run(alloc: A, map_pages: *Pages, old: identity.Space, next: identity.Space, previous: identity.Space, prior: identity.Translation) !void {
            var marker_: u8 = 0;
            var spill: local.sql_spill.Manager = .{ .alloc = alloc, .io = std.testing.io, .context = &marker_, .checkpoint = Check.call, .max_bytes = 1024 * 1024 };
            defer spill.deinit();
            const result = try buildInner(alloc, map_pages, &spill, old, next, previous, prior, .{ .io = std.testing.io });
            try std.testing.expectEqual(next.rows, result.forward.records);
            try std.testing.expectEqual(next.rows, result.reverse.records);
            const extended = try buildInnerExtended(alloc, map_pages, &spill, old, next, previous, old, prior, .{ .io = std.testing.io });
            try std.testing.expectEqual(next.rows, extended.forward.records);
            try std.testing.expectEqual(next.rows, extended.reverse.records);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ &pages, original.current, third, current.current, initial });
    // No original ID tree is needed when changed values leave addresses intact.
    // Keep the original coordinate map and only compare current COW state.
    for (&values, &edits) |*value, *edit| {
        value[16..48].* = @splat(8);
        edit.value = value;
    }
    var changed_values = third;
    changed_values.by_id = try tree.apply(a, pages.store(), third.by_id, &edits);
    try store.delete(&try Pages.identity(pages.domain, original.current.by_id.?));
    const unchanged_maps = (try build(a, &pages, original.current, changed_values, third, swapped, context, 1024 * 1024)).?;
    try std.testing.expect(unchanged_maps.forward.eql(swapped.forward) and unchanged_maps.reverse.eql(swapped.reverse));
}
