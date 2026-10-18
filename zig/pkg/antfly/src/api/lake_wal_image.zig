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

//! Independently owned normalized row partitions. A cut retains only its live
//! partitions; changed prefixes clone their rows, never an ancestor cut.
const std = @import("std");
const local = @import("antfly_local_sources");
const ingestion = @import("../serverless/lake_ingestion.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
const Change = ingestion.Batch.Change;
const Set = @import("lake_index_physical_set.zig").Set;
pub fn digest(value: anytype) [32]u8 {
    var buffer: [4096]u8 = undefined;
    var writer: std.Io.Writer.Hashing(std.crypto.hash.Blake3) = .init(&buffer);
    std.json.Stringify.value(value, .{}, &writer.writer) catch unreachable;
    writer.writer.flush() catch unreachable;
    var result: [32]u8 = undefined;
    writer.hasher.final(&result);
    return result;
}
pub fn partition(id: []const u8) usize {
    const tail = if (std.mem.startsWith(u8, id, "wal1:")) id[5..] else id;
    if (tail.len < 2) return 0;
    const hi = std.fmt.charToDigit(tail[0], 16) catch return 0;
    const lo = std.fmt.charToDigit(tail[1], 16) catch return 0;
    return @as(usize, hi) * 16 + lo;
}
pub const Entry = struct { id: []const u8, change: Change };
pub const Mask = struct {
    refs: std.atomic.Value(usize) = .init(1),
    budget: local.sql_memory_budget,
    arena: std.heap.ArenaAllocator = undefined,
    mutex: std.atomic.Mutex = .unlocked,
    set: ?Set = null,
    fn create(a: A) !*Mask {
        const self = try a.create(Mask);
        self.* = .{ .budget = .{ .backing = a, .limit = 512 * 1024 } };
        self.arena = .init(self.budget.allocator());
        return self;
    }
    pub fn release(self: *Mask) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        self.budget.backing.destroy(self);
    }
};
pub const Part = struct {
    refs: std.atomic.Value(usize) = .init(1),
    budget: local.sql_memory_budget,
    arena: std.heap.ArenaAllocator = undefined,
    mask: ?*Mask = null,
    entries: []const Entry = &.{},
    changes: []const Change = &.{},
    ids: []const []const u8 = &.{},
    fingerprint: [32]u8 = @splat(0),
    wire_digest: [32]u8 = @splat(0),
    key_filter: []const u8 = "",
    pub fn release(self: *Part) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        if (self.mask) |mask| mask.release();
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        self.budget.backing.destroy(self);
    }
    fn build(backing: A, keys: []const []const u8, previous: ?*Part, delta: []const Entry, borrow_rows: bool) !*Part {
        const self = try backing.create(Part);
        self.* = .{ .budget = .{ .backing = backing, .limit = 128 * 1024 * 1024 } };
        self.arena = .init(self.budget.allocator());
        errdefer self.release();
        return self.fill(keys, previous, delta, borrow_rows) catch |err| {
            if (err == error.OutOfMemory and self.budget.isLocallyExhausted()) return error.LakeOverlayTooLarge;
            return err;
        };
    }
    fn fill(self: *Part, keys: []const []const u8, previous: ?*Part, delta: []const Entry, borrow_rows: bool) !*Part {
        const backing = self.budget.backing;
        var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
        defer scratch.deinit();
        const sa = scratch.allocator();
        var latest: std.StringHashMapUnmanaged(Change) = .empty;
        if (previous) |old| for (old.entries) |entry| try latest.put(sa, entry.id, entry.change);
        for (delta) |entry| try latest.put(sa, entry.id, entry.change);
        const ids = try sa.alloc([]const u8, latest.count());
        var iterator = latest.keyIterator();
        for (ids) |*id| id.* = iterator.next().?.*;
        std.mem.sort([]const u8, ids, {}, struct {
            fn less(_: void, l: []const u8, r: []const u8) bool {
                return std.mem.lessThan(u8, l, r);
            }
        }.less);
        const a = self.arena.allocator();
        const entries = try a.alloc(Entry, ids.len);
        const changes = try a.alloc(Change, ids.len);
        var live: std.ArrayList([]const u8) = .empty;
        var clauses: std.ArrayList(V) = .empty;
        var hash = std.crypto.hash.Blake3.init(.{});
        for (ids, entries, changes) |id, *entry, *change| {
            const source = latest.get(id).?;
            change.* = .{ .op = source.op, .row = if (borrow_rows) source.row else try local.storage_typed_json.clone(a, source.row) };
            entry.* = .{ .id = try a.dupe(u8, id), .change = change.* };
            if (change.op == .upsert) {
                try live.append(a, entry.id);
                hash.update(&digest(.{ entry.id, change.row }));
            }
            var conjunction: std.ArrayList(V) = .empty;
            for (keys) |key| {
                const value = change.row.object.get(key) orelse return error.InvalidWal;
                const clause = try std.json.Stringify.valueAlloc(sa, .{ .term = .{ .path = try std.fmt.allocPrint(sa, "/{s}", .{key}), .value = value } }, .{});
                try conjunction.append(sa, try std.json.parseFromSliceLeaky(V, sa, clause, .{}));
            }
            const clause = try std.json.Stringify.valueAlloc(sa, .{ .conjuncts = conjunction.items }, .{});
            try clauses.append(sa, try std.json.parseFromSliceLeaky(V, sa, clause, .{}));
        }
        self.entries = entries;
        self.changes = changes;
        self.wire_digest = digest(changes);
        self.ids = live.items;
        self.key_filter = try std.json.Stringify.valueAlloc(a, .{ .disjuncts = clauses.items }, .{});
        hash.final(&self.fingerprint);
        if (previous) |old| if (std.mem.eql(u8, old.key_filter, self.key_filter)) {
            self.mask = old.mask.?;
            _ = self.mask.?.refs.fetchAdd(1, .monotonic);
        };
        if (self.mask == null) self.mask = try Mask.create(backing);
        return self;
    }
};
pub const Image = struct {
    a: A,
    borrowed: bool = false,
    parts: [256]?*Part = @splat(null),
    blocks: [256][]const Change = @splat(&.{}),
    block_digests: [256][32]u8 = @splat(@splat(0)),
    key_fields: []const []const u8 = &.{},
    row_count: usize = 0,
    change_count: usize = 0,
    rebuilt_rows: usize = 0,
    pub fn release(self: *Image) void {
        std.debug.assert(!self.borrowed);
        for (self.parts) |part| if (part) |owned| owned.release();
        for (self.key_fields) |key| self.a.free(key);
        self.a.free(self.key_fields);
        self.a.destroy(self);
    }
    /// Row bytes belong to the enclosing decoded group. Its cleanup releases
    /// this image before destroying the group's row arena.
    pub fn fromBorrowedChanges(a: A, delta: ingestion.Pending) !*Image {
        return build(a, null, delta, true);
    }
    pub fn update(a: A, previous: ?*const Image, delta: ingestion.Pending) !*Image {
        return build(a, previous, delta, false);
    }
    fn build(a: A, previous: ?*const Image, delta: ingestion.Pending, borrow_rows: bool) !*Image {
        const self = try a.create(Image);
        self.* = .{ .a = a };
        errdefer self.release();
        const keys = if (delta.key_fields.len != 0) delta.key_fields else if (previous) |old| old.key_fields else &.{};
        if (previous) |old| if (delta.key_fields.len != 0) {
            if (old.key_fields.len != keys.len) return error.LakeSourceConflict;
            for (keys, old.key_fields) |l, r| if (!std.mem.eql(u8, l, r)) return error.LakeSourceConflict;
        };
        const owned_keys = try a.alloc([]const u8, keys.len);
        @memset(owned_keys, "");
        self.key_fields = owned_keys;
        for (keys, owned_keys) |key, *out| out.* = try a.dupe(u8, key);
        if (previous) |old| for (old.parts, &self.parts) |part, *out| if (part) |held| {
            _ = held.refs.fetchAdd(1, .monotonic);
            out.* = held;
        };
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        var changed: [256]std.ArrayList(Entry) = @splat(.empty);
        for (delta.changes) |change| {
            const id = try ingestion.rowId(sa, keys, change.row, .canonical);
            try changed[partition(id)].append(sa, .{ .id = id, .change = change });
        }
        for (&self.parts, changed) |*part, changes| if (changes.items.len != 0) {
            const next = try Part.build(a, keys, part.*, changes.items, borrow_rows);
            if (part.*) |old| old.release();
            part.* = next;
            self.rebuilt_rows += next.entries.len;
        };
        for (self.parts, &self.blocks, &self.block_digests) |part, *block, *fingerprint| if (part) |held| {
            self.row_count += held.ids.len;
            self.change_count += held.changes.len;
            block.* = held.changes;
            fingerprint.* = held.wire_digest;
        };
        var retained: usize = 0;
        for (self.parts) |part| if (part) |held| {
            @import("antfly_platform").sync.lockYielding(&held.budget.mutex);
            const bytes = held.budget.live;
            held.budget.mutex.unlock();
            retained = std.math.add(usize, retained, bytes) catch return error.LakeOverlayTooLarge;
        };
        if (self.change_count > 65536 or retained > 128 * 1024 * 1024) return error.LakeOverlayTooLarge;
        return self;
    }
};
pub const Rows = struct {
    pub const empty: @This() = .{};
    local_rows: std.StringHashMapUnmanaged(V) = .empty,
    image: ?*const Image = null,
    selected_prefix: ?u8 = null,
    fn selected(self: Rows, index: usize) bool {
        return if (self.selected_prefix) |prefix| index >> 4 == (std.fmt.charToDigit(prefix, 16) catch return false) else true;
    }
    pub fn subset(self: Rows, prefix: u8) Rows {
        var view = self;
        view.selected_prefix = prefix;
        return view;
    }
    pub fn count(self: Rows) usize {
        if (self.selected_prefix == null) return if (self.image) |image| image.row_count else self.local_rows.count();
        var n: usize = 0;
        if (self.image) |image| {
            for (image.parts, 0..) |part, i| if (self.selected(i)) if (part) |held| {
                n += held.ids.len;
            };
        } else {
            var it = self.iterator();
            while (it.next() != null) n += 1;
        }
        return n;
    }
    pub fn get(self: Rows, id: []const u8) ?V {
        if (!self.selected(partition(id))) return null;
        const image = self.image orelse return self.local_rows.get(id);
        const part = image.parts[partition(id)] orelse return null;
        var lo: usize = 0;
        var hi = part.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, part.entries[mid].id, id) == .lt) lo = mid + 1 else hi = mid;
        }
        if (lo == part.entries.len or !std.mem.eql(u8, part.entries[lo].id, id) or part.entries[lo].change.op != .upsert) return null;
        return part.entries[lo].change.row;
    }
    pub fn put(self: *Rows, a: A, id: []const u8, value: V) !void {
        std.debug.assert(self.image == null and self.selected_prefix == null);
        try self.local_rows.put(a, id, value);
    }
    pub fn clearRetainingCapacity(self: *Rows) void {
        std.debug.assert(self.image == null and self.selected_prefix == null);
        self.local_rows.clearRetainingCapacity();
    }
    pub fn remove(self: *Rows, id: []const u8) bool {
        std.debug.assert(self.image == null and self.selected_prefix == null);
        return self.local_rows.remove(id);
    }
    pub const Iterator = struct {
        local_iterator: ?std.StringHashMapUnmanaged(V).Iterator = null,
        selected_prefix: ?u8 = null,
        image: ?*const Image = null,
        part: usize = 0,
        row: usize = 0,
        pub fn next(self: *@This()) ?struct { key_ptr: *const []const u8, value_ptr: *const V } {
            const view: Rows = .{ .selected_prefix = self.selected_prefix };
            if (self.local_iterator) |*it| {
                while (it.next()) |entry| if (view.selected(partition(entry.key_ptr.*))) return .{ .key_ptr = entry.key_ptr, .value_ptr = entry.value_ptr };
                return null;
            }
            const image = self.image orelse return null;
            while (self.part < image.parts.len) {
                if (view.selected(self.part)) if (image.parts[self.part]) |part| while (self.row < part.entries.len) {
                    const entry = &part.entries[self.row];
                    self.row += 1;
                    if (entry.change.op == .upsert) return .{ .key_ptr = &entry.id, .value_ptr = &entry.change.row };
                };
                self.part += 1;
                self.row = 0;
            }
            return null;
        }
    };
    pub const KeyIterator = struct {
        iterator: Iterator,
        pub fn next(self: *@This()) ?*const []const u8 {
            const entry = self.iterator.next() orelse return null;
            return entry.key_ptr;
        }
    };
    pub fn iterator(self: *const Rows) Iterator {
        return if (self.image) |image| .{ .image = image, .selected_prefix = self.selected_prefix } else .{ .local_iterator = self.local_rows.iterator(), .selected_prefix = self.selected_prefix };
    }
    pub fn keyIterator(self: *const Rows) KeyIterator {
        return .{ .iterator = self.iterator() };
    }
};

test "external lake bounded WAL images share unchanged rows without ancestor ownership" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    const keys = &.{@as([]const u8, "id")};
    const first = try std.json.parseFromSliceLeaky(V, sa, "{\"id\":0,\"body\":\"first\"}", .{});
    const last = try std.json.parseFromSliceLeaky(V, sa, "{\"id\":1000,\"body\":\"last\"}", .{});
    const first_id = try std.fmt.allocPrint(sa, "wal1:{s}", .{local.serverless_external_source_mod.lake_catalog.types.digestHex("[0]")});
    const last_id = try std.fmt.allocPrint(sa, "wal1:{s}", .{local.serverless_external_source_mod.lake_catalog.types.digestHex("[1000]")});
    // Parent cache pressure stays retryable instead of claiming a 128 MiB
    // partition-size violation. Part ownership is released on failed admission.
    var parent: local.sql_memory_budget = .{ .backing = a, .limit = @sizeOf(Part) + 64 };
    try std.testing.expectError(error.OutOfMemory, Part.build(parent.allocator(), keys, null, &.{.{ .id = first_id, .change = .{ .op = .upsert, .row = first } }}, false));
    try std.testing.expect(parent.isExhausted());
    try std.testing.expectEqual(@as(usize, 0), parent.live);
    var image = try Image.update(a, null, .{ .lsn = 1, .key_fields = keys, .changes = &.{ .{ .op = .upsert, .row = first }, .{ .op = .upsert, .row = last } } });
    defer image.release();
    const untouched = image.parts[partition(last_id)].?;
    const prefix_rows = (Rows{ .image = image }).subset(last_id[5]);
    try std.testing.expectEqual(image, prefix_rows.image.?);
    try std.testing.expectEqual(@as(usize, 0), prefix_rows.local_rows.count());
    try std.testing.expectEqual(@as(usize, 1), prefix_rows.count());
    try std.testing.expect(prefix_rows.get(first_id) == null);
    var selected = prefix_rows.iterator();
    try std.testing.expectEqualStrings(last_id, selected.next().?.key_ptr.*);
    try std.testing.expect(selected.next() == null);
    for (0..24) |i| {
        const json = try std.fmt.allocPrint(sa, "{{\"id\":0,\"body\":\"updated-{d}\"}}", .{i});
        const row = try std.json.parseFromSliceLeaky(V, sa, json, .{});
        const next = try Image.update(a, image, .{ .lsn = i + 2, .key_fields = keys, .changes = &.{.{ .op = .upsert, .row = row }} });
        try std.testing.expectEqual(untouched, next.parts[partition(last_id)].?);
        try std.testing.expectEqual(@as(usize, 1), next.rebuilt_rows);
        try std.testing.expectEqual(image.parts[partition(first_id)].?.mask.?, next.parts[partition(first_id)].?.mask.?);
        image.release();
        image = next;
        const rows: Rows = .{ .image = image };
        try std.testing.expectEqualStrings("last", rows.get(last_id).?.object.get("body").?.string);
    }
    const deleted = try Image.update(a, image, .{ .lsn = 99, .key_fields = keys, .changes = &.{.{ .op = .delete, .row = first }} });
    defer deleted.release();
    const rows: Rows = .{ .image = deleted };
    try std.testing.expect(rows.get(first_id) == null);
    try std.testing.expectEqual(@as(usize, 1), rows.count());
    const pending: ingestion.Pending = .{ .lsn = 99, .key_fields = deleted.key_fields, .change_blocks = &deleted.blocks };
    const bytes = try std.json.Stringify.valueAlloc(a, pending, .{});
    defer a.free(bytes);
    const recovered = try std.json.parseFromSliceLeaky(ingestion.Pending, sa, bytes, .{});
    try std.testing.expectEqual(@as(usize, 2), recovered.changes.len);
    const Failure = struct {
        fn run(alloc: A, old: *Image, delta: ingestion.Pending) !void {
            const next = try Image.update(alloc, old, delta);
            next.release();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ image, ingestion.Pending{ .lsn = 100, .key_fields = keys, .changes = &.{.{ .op = .upsert, .row = first }} } });
}

test "external lake bounded WAL equivalent numeric keys replace" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(V, sa, "{\"id\":-0.0,\"body\":\"old\"}", .{});
    const new = try std.json.parseFromSliceLeaky(V, sa, "{\"id\":0.0,\"body\":\"new\"}", .{});
    const stable = local.serverless_external_source_mod.lake_catalog.row_commit.stable_key;
    try std.testing.expectEqualStrings(try stable.identity(sa, &.{"id"}, old, &.{}), try stable.identity(sa, &.{"id"}, new, &.{}));
    const first = try Image.update(a, null, .{ .lsn = 1, .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = old }} });
    defer first.release();
    const second = try Image.update(a, first, .{ .lsn = 2, .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = new }} });
    defer second.release();
    try std.testing.expectEqual(@as(usize, 1), second.row_count);
    const encoded = try std.json.Stringify.valueAlloc(sa, ingestion.Pending{ .lsn = 2, .key_fields = second.key_fields, .key_identity = .canonical, .change_blocks = &second.blocks }, .{});
    const recovered = try std.json.parseFromSliceLeaky(ingestion.Pending, sa, encoded, .{});
    const overlay = try @import("lake_search_overlay.zig").Overlay.init(sa, recovered);
    const key = try ingestion.rowId(sa, &.{"id"}, new, .canonical);
    try std.testing.expectEqualStrings("new", overlay.row(key).?.object.get("body").?.string);
    const deleted = try Image.update(a, second, .{ .lsn = 3, .key_fields = &.{"id"}, .changes = &.{.{ .op = .delete, .row = new }} });
    defer deleted.release();
    try std.testing.expectEqual(@as(usize, 0), deleted.row_count);
}
