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

//! File-local copy-on-write row identity. Unchanged rows never enter a sort.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const Pages = @import("../serverless/graph_segment/page_store.zig").PageStore;
const Mutations = @import("lake_index_page_mutations.zig").Mutations;
const A = std.mem.Allocator;
const Root = identity.Root;
const Coordinate = identity.logical.Coordinate;
const State = identity.logical.State;
const absent = std.math.maxInt(u32);
pub const Builder = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    pages: *Pages,
    prior: ?Root,
    space: identity.Space,
    files: []const []const u8,
    slots: []u32,
    scan: []bool,
    cohorts: []const @import("lake_row_input_recipes.zig").Recipe = &.{},
    prior_cohorts: []const @import("lake_row_input_recipes.zig").Recipe = &.{},
    added_cohorts: []const @import("lake_row_input_recipes.zig").Recipe = &.{},
    extension_cursor: ?tree.Cursor = null,
    extension_file: ?u32 = null,
    extension_seen: std.AutoHashMapUnmanaged(u32, void) = .empty,
    unchanged: std.AutoHashMapUnmanaged(u32, void) = .empty,
    pending: local.sql_spill.Sort,
    edits: [7]Mutations,
    pub fn create(a: A, manager: *local.sql_spill.Manager, pages: *Pages, files: []const []const u8, prior: ?Root, delta: ?@import("lake_source_navigation.zig").Delta, recipes: []const @import("lake_row_input_recipes.zig").Recipe, source: []const u8, snapshot: []const u8) !*Builder {
        return createWithOptions(a, manager, pages, files, prior, delta, recipes, source, snapshot, false);
    }
    pub fn createWithOptions(a: A, manager: *local.sql_spill.Manager, pages: *Pages, files: []const []const u8, prior: ?Root, delta: ?@import("lake_source_navigation.zig").Delta, recipes: []const @import("lake_row_input_recipes.zig").Recipe, source: []const u8, snapshot: []const u8, compact: bool) !*Builder {
        return createWithScan(a, manager, pages, files, prior, delta, recipes, source, snapshot, compact, null);
    }
    /// Rebuild an explicitly selected file layout under the same durable maps.
    pub fn createWithScan(a: A, manager: *local.sql_spill.Manager, pages: *Pages, files: []const []const u8, prior: ?Root, delta: ?@import("lake_source_navigation.zig").Delta, recipes: []const @import("lake_row_input_recipes.zig").Recipe, source: []const u8, snapshot: []const u8, compact: bool, selected: ?[]const bool) !*Builder {
        if (selected) |mask| if (mask.len != files.len) return error.InvalidLogicalRowIdentity;
        if (prior) |old| if (old.current.compact != compact) return error.InvalidLogicalRowIdentity;
        if (compact) pages.prefix_keys = true;
        const self = try a.create(Builder);
        self.* = .{ .a = a, .arena = .init(a), .pages = pages, .prior = prior, .space = .{ .compact = compact, .source = source, .snapshot = snapshot, .rows = 0, .file_count = 0, .files = null, .by_id = null, .by_location = null, .input_recipes = recipes }, .files = files, .slots = &.{}, .scan = &.{}, .pending = .init(a, manager, &.{.{}}, 1024 * 1024), .edits = undefined };
        self.pending.shared_runs = true;
        self.pending.encoded_bytes = true;
        for (&self.edits) |*edit| {
            edit.* = Mutations.init(a, manager, pages.store());
        }
        errdefer self.deinit();
        const ma = self.arena.allocator();
        self.cohorts = try @import("lake_row_input_recipes.zig").unique(ma, recipes);
        self.slots = try ma.alloc(u32, files.len);
        @memset(self.slots, absent);
        self.scan = try ma.alloc(bool, files.len);
        @memset(self.scan, true);
        var current: std.StringHashMapUnmanaged(u32) = .empty;
        defer current.deinit(a);
        for (files, 0..) |file, position| {
            const entry = try current.getOrPut(a, file);
            if (entry.found_existing) return error.InvalidLogicalRowIdentity;
            entry.value_ptr.* = @intCast(position);
        }
        var used: std.AutoHashMapUnmanaged(u32, void) = .empty;
        defer used.deinit(a);
        const Removed = struct { slot: u32, name: []const u8 };
        var removed: std.ArrayList(Removed) = .empty;
        defer removed.deinit(a);
        if (prior) |old| {
            self.prior_cohorts = try @import("lake_row_input_recipes.zig").unique(ma, old.current.input_recipes);
            var cursor = tree.Cursor.init(a, pages.store(), old.current.files, "", null) catch |err| return identity.derivedReadFailure(err);
            defer cursor.deinit();
            while (cursor.next() catch |err| return identity.derivedReadFailure(err)) |record| {
                if (record.key.len != 4) return error.InvalidLogicalRowIdentity;
                const slot = std.mem.readInt(u32, record.key[0..4], .big);
                if (slot >= old.current.file_count) return error.InvalidLogicalRowIdentity;
                const position = current.get(record.value);
                if (position) |index| {
                    self.slots[index] = slot;
                    try used.put(a, slot, {});
                    if (if (selected) |mask| !mask[index] else delta != null and !delta.?.isChanged(index)) {
                        self.scan[index] = false;
                        try self.unchanged.put(a, slot, {});
                        continue;
                    }
                }
                try removed.append(a, .{ .slot = slot, .name = try ma.dupe(u8, record.value) });
            }
            // No retained file means there is no old row state to merge or
            // erase. Rebuild the seven maps from the fresh sorted stream.
            if (self.unchanged.count() != 0) for (removed.items) |file| try self.eraseFile(file.slot, file.name);
        }
        var available: u32 = 0;
        for (files, self.slots, self.scan) |file, *slot, changed| {
            if (slot.* == absent) {
                while (used.contains(available)) available = std.math.add(u32, available, 1) catch return error.InvalidLogicalRowIdentity;
                slot.* = available;
                try used.put(a, available, {});
            }
            self.space.file_count = @max(self.space.file_count, try std.math.add(u32, slot.*, 1));
            if (changed) {
                var key: [4]u8 = undefined;
                std.mem.writeInt(u32, &key, slot.*, .big);
                try self.edits[0].addRecord(&key, file);
                const digest = fileKey(file);
                try self.edits[1].addRecord(&digest, &key);
            }
        }
        if (self.unchanged.count() != 0) {
            var added: std.ArrayList(@import("lake_row_input_recipes.zig").Recipe) = .empty;
            for (self.cohorts) |recipe| {
                const exists = for (self.prior_cohorts) |old| {
                    if (std.mem.eql(u8, &recipe.digest, &old.digest)) break true;
                } else false;
                if (!exists) try added.append(ma, recipe);
            }
            self.added_cohorts = added.items;
            for (self.prior_cohorts) |old| {
                const retained = for (self.cohorts) |recipe| {
                    if (std.mem.eql(u8, &recipe.digest, &old.digest)) break true;
                } else false;
                if (!retained) try self.eraseCohort(old.digest);
            }
        }
        return self;
    }
    pub fn deinit(self: *Builder) void {
        if (self.extension_cursor) |*cursor| cursor.deinit();
        self.extension_seen.deinit(self.a);
        for (&self.edits) |*edit| edit.deinit();
        self.pending.deinit();
        self.unchanged.deinit(self.a);
        self.arena.deinit();
        self.a.destroy(self);
    }
    /// A bounded layout planner can retain a file provisionally, compare its
    /// ordering commitment, then rebuild only a genuinely changed coordinate
    /// layout. Unchanged layouts keep their original COW pages and ownership.
    pub fn rescanFile(self: *Builder, index: usize) !void {
        if (index >= self.files.len or self.prior == null or self.extension_cursor != null) return error.InvalidLogicalRowIdentity;
        if (self.scan[index]) return;
        const slot = self.slots[index];
        if (!self.unchanged.contains(slot)) return error.InvalidLogicalRowIdentity;
        // With no retained membership left, finish starts from empty maps.
        if (self.unchanged.count() > 1) try self.eraseFile(slot, self.files[index]);
        _ = self.unchanged.remove(slot);
        self.scan[index] = true;
        var key: [4]u8 = undefined;
        std.mem.writeInt(u32, &key, slot, .big);
        try self.edits[0].addRecord(&key, self.files[index]);
        const digest = fileKey(self.files[index]);
        try self.edits[1].addRecord(&digest, &key);
    }
    fn eraseFile(self: *Builder, slot: u32, name: []const u8) !void {
        const old = self.prior.?;
        var prefix: [4]u8 = undefined;
        std.mem.writeInt(u32, &prefix, slot, .big);
        const digest = fileKey(name);
        try self.edits[0].addRecord(&prefix, null);
        try self.edits[1].addRecord(&digest, null);
        var upper: [5]u8 = @splat(0);
        @memcpy(upper[0..4], &prefix);
        std.mem.writeInt(u32, upper[0..4], slot +| 1, .big);
        var cursor = tree.Cursor.init(self.a, self.pages.store(), old.current.by_file, &prefix, if (slot == std.math.maxInt(u32)) null else upper[0..4]) catch |err| return identity.derivedReadFailure(err);
        defer cursor.deinit();
        while (cursor.next() catch |err| return identity.derivedReadFailure(err)) |record| {
            if (record.key.len != 16 or !std.mem.eql(u8, record.key[0..4], &prefix) or record.value.len != old.current.memberWidth()) return error.InvalidLogicalRowIdentity;
            const coordinate = try Coordinate.decode(record.key);
            try self.edits[2].addRecord(record.value[0..32], null);
            const location = identity.locationKey(name, coordinate.group, coordinate.row);
            if (!self.space.compact) try self.edits[3].addRecord(&location, null);
            try self.edits[4].addRecord(record.key, null);
            const order = identity.orderKey(name, coordinate.group, coordinate.row);
            try self.edits[5].addRecord(&order, null);
            for (self.prior_cohorts) |recipe| {
                var input_key: [64]u8 = undefined;
                input_key[0..32].* = recipe.digest;
                input_key[32..64].* = record.value[0..32].*;
                try self.edits[6].addRecord(&input_key, null);
            }
        }
    }
    pub fn add(self: *Builder, state: State, coordinate: Coordinate) !void {
        return self.addWithInputs(state, coordinate, &.{});
    }
    pub fn addWithInputs(self: *Builder, state: State, coordinate: Coordinate, inputs: []const identity.logical.Digest) !void {
        try state.validate();
        if (inputs.len != self.cohorts.len or (self.space.compact and !std.mem.eql(u8, &state.id, &state.values))) return error.InvalidLogicalRowIdentity;
        const offset: usize = if (self.space.compact) 16 else 48;
        const value = try self.a.alloc(u8, offset + inputs.len * 32);
        defer self.a.free(value);
        value[0..16].* = coordinate.encode();
        if (!self.space.compact) value[16..48].* = state.values;
        for (inputs, 0..) |input, i| @memcpy(value[offset + i * 32 ..][0..32], &input);
        try self.pending.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = &state.id })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = value })}, .ordinal = self.pending.total });
    }
    fn eraseCohort(self: *Builder, digest: [32]u8) !void {
        var cursor = try tree.Cursor.init(self.a, self.pages.store(), self.prior.?.current.input_versions, &digest, null);
        defer cursor.deinit();
        while (try cursor.next()) |row| {
            if (row.key.len != 64 or row.value.len != 32) return error.InvalidLogicalRowIdentity;
            if (!std.mem.eql(u8, row.key[0..32], &digest)) break;
            try self.edits[6].addRecord(row.key, null);
        }
    }
    /// Supplemental scans bind only new cohorts to unchanged, authenticated
    /// physical membership. They never rewrite identity or old value evidence.
    pub fn addExtension(self: *Builder, id: [32]u8, coordinate: Coordinate, inputs: []const [32]u8) !void {
        if (!self.space.compact or inputs.len != self.added_cohorts.len or !self.unchanged.contains(coordinate.file)) return error.InvalidLogicalRowIdentity;
        if (self.extension_file == null or self.extension_file.? != coordinate.file) {
            try self.finishExtensionFile();
            const seen = try self.extension_seen.getOrPut(self.a, coordinate.file);
            if (seen.found_existing) return error.InvalidLogicalRowIdentity;
            const key = coordinate.encode();
            self.extension_cursor = try tree.Cursor.init(self.a, self.pages.store(), self.prior.?.current.by_file, key[0..4], null);
            self.extension_file = coordinate.file;
        }
        const member = (try self.extension_cursor.?.next()) orelse return error.InvalidLogicalRowIdentity;
        const key = coordinate.encode();
        if (!std.mem.eql(u8, member.key, &key) or !std.mem.eql(u8, member.value, &id)) return error.InvalidLogicalRowIdentity;
        for (self.added_cohorts, inputs) |recipe, input| {
            var input_key: [64]u8 = undefined;
            input_key[0..32].* = recipe.digest;
            input_key[32..64].* = id;
            try self.edits[6].addRecord(&input_key, &input);
        }
    }
    fn finishExtensionFile(self: *Builder) !void {
        if (self.extension_cursor) |*cursor| {
            if (try cursor.next()) |member| {
                if (member.key.len != 16 or std.mem.readInt(u32, member.key[0..4], .big) == self.extension_file.?) return error.InvalidLogicalRowIdentity;
            }
            cursor.deinit();
            self.extension_cursor = null;
        }
    }
    pub fn finish(self: *Builder, namespace: identity.logical.Digest, recipe: identity.logical.Digest, cut: identity.logical.Digest) !Root {
        try self.finishExtensionFile();
        var rows: @import("lake_index_sort_stream.zig").Records = .{ .sort = &self.pending };
        defer rows.deinit();
        // A fresh complete map has no queued removals or retained membership.
        // The pending stream already proves strict ID order. A single cohort
        // adds a constant prefix, preserving that order for input evidence.
        const fresh = self.unchanged.count() == 0;
        const ordered_ids = fresh and self.space.by_id == null and self.edits[2].ordinal == 0;
        const ordered_inputs = fresh and self.space.input_versions == null and self.cohorts.len == 1 and self.edits[6].ordinal == 0;
        var id_tree: tree.SortedBuilder = .{ .alloc = self.a, .store = self.pages.store() };
        defer id_tree.deinit();
        var input_tree: tree.SortedBuilder = .{ .alloc = self.a, .store = self.pages.store() };
        defer input_tree.deinit();
        const input_offset: usize = if (self.space.compact) 16 else 48;
        var previous_id: ?[32]u8 = null;
        var inverse: std.AutoHashMapUnmanaged(u32, []const u8) = .empty;
        defer inverse.deinit(self.a);
        for (self.files, self.slots) |name, slot| try inverse.put(self.a, slot, name);
        // Only unchanged files can conflict with the incoming ID stream. Keep
        // one ordered cursor: restarting a point lookup per row rereads every
        // ancestor, even for a complete rewrite with no retained membership.
        var retained = if (self.prior != null and self.unchanged.count() != 0)
            (tree.Cursor.init(self.a, self.pages.store(), self.prior.?.current.by_id, "", null) catch |err| return identity.derivedReadFailure(err))
        else
            null;
        defer if (retained) |*cursor| cursor.deinit();
        var candidate: ?tree.Cursor.Record = null;
        while (true) {
            const row = (try rows.next()) orelse break;
            const id = row.key;
            const value = row.value;
            if (id.len != 32 or value.len != input_offset + self.cohorts.len * 32) return error.InvalidLogicalRowIdentity;
            if (previous_id) |previous| if (std.mem.eql(u8, &previous, id)) return error.DuplicateLogicalRowKey;
            previous_id = id[0..32].*;
            if (retained) |*cursor| {
                if (candidate == null or std.mem.order(u8, candidate.?.key, id) == .lt) {
                    cursor.seekForward(id) catch |err| return identity.derivedReadFailure(err);
                    candidate = cursor.next() catch |err| return identity.derivedReadFailure(err);
                }
                if (candidate) |existing| if (std.mem.eql(u8, existing.key, id)) {
                    const existing_row = try identity.decodeState(self.prior.?.current, existing.key, existing.value);
                    if (self.unchanged.contains(existing_row.coordinate.file)) return error.DuplicateLogicalRowKey;
                };
            }
            const coordinate = try Coordinate.decode(value[0..16]);
            const name = inverse.get(coordinate.file) orelse return error.InvalidLogicalRowIdentity;
            var forward: [56]u8 = @splat(0);
            if (!self.space.compact) @memcpy(forward[0..48], value[0..48]);
            var state: [64]u8 = undefined;
            state[0..32].* = id[0..32].*;
            if (!self.space.compact) state[32..64].* = value[16..48].*;
            if (ordered_ids) try id_tree.add(id, if (self.space.compact) value[0..16] else &forward) else try self.edits[2].addRecord(id, if (self.space.compact) value[0..16] else &forward);
            const location = identity.locationKey(name, coordinate.group, coordinate.row);
            if (!self.space.compact) try self.edits[3].addRecord(&location, &state);
            try self.edits[4].addRecord(value[0..16], if (self.space.compact) id else &state);
            const order = identity.orderKey(name, coordinate.group, coordinate.row);
            try self.edits[5].addRecord(&order, id);
            for (self.cohorts, 0..) |input_recipe, i| {
                var input_key: [64]u8 = undefined;
                input_key[0..32].* = input_recipe.digest;
                input_key[32..64].* = id[0..32].*;
                if (ordered_inputs) try input_tree.add(&input_key, value[input_offset + i * 32 ..][0..32]) else try self.edits[6].addRecord(&input_key, value[input_offset + i * 32 ..][0..32]);
            }
        }
        const old = if (self.prior != null and self.unchanged.count() != 0) self.prior.?.current else self.space;
        self.space.files = try self.edits[0].finish(old.files);
        self.space.file_slots = try self.edits[1].finish(old.file_slots);
        self.space.by_id = if (ordered_ids) try id_tree.finish() else try self.edits[2].finish(old.by_id);
        self.space.by_file = try self.edits[4].finish(old.by_file);
        self.space.by_location = if (self.space.compact) self.space.by_file else try self.edits[3].finish(old.by_location);
        self.space.by_order = try self.edits[5].finish(old.by_order);
        self.space.input_versions = if (ordered_inputs) try input_tree.finish() else try self.edits[6].finish(old.input_versions);
        self.space.rows = if (self.space.by_id) |ref| ref.records else 0;
        const root: Root = .{ .domain = self.pages.domain, .namespace = namespace, .values_recipe = recipe, .values_format = if (self.space.compact) identity.cohort_values_format else @import("lake_row_fingerprints.zig").format, .cut = cut, .current = self.space };
        try root.validate(self.a);
        return root;
    }
};
pub fn fileKey(name: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(name, &digest, .{});
    return digest;
}

test "external lake identity deltas reuse untouched rows and slots across append replacement and deletion" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-identity-delta");
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
    const first = try Builder.create(a, &manager, &pages, &.{"a"}, null, null, &.{}, "source", "one");
    defer first.deinit();
    const id: [32]u8 = @splat(3);
    const state: State = .{ .id = id, .values = @splat(4) };
    try first.add(state, .{ .file = first.slots[0], .group = 0, .row = 17 });
    const old = try first.finish(@splat(1), @splat(2), @splat(3));
    var changed = [_]bool{ false, true };
    var delta: @import("lake_source_navigation.zig").Delta = undefined;
    delta.changed = &changed;
    delta.selection = null;
    const second = try Builder.create(a, &manager, &pages, &.{ "a", "b" }, old, delta, &.{}, "source", "two");
    defer second.deinit();
    try std.testing.expect(!second.scan[0] and second.scan[1]);
    try std.testing.expectEqual(first.slots[0], second.slots[0]);
    try std.testing.expectEqual(@as(u64, 0), second.pending.total);
    try second.add(.{ .id = @splat(5), .values = @splat(6) }, .{ .file = second.slots[1], .group = 1, .row = 2 });
    const next = try second.finish(@splat(1), @splat(2), @splat(4));
    try std.testing.expectEqual(@as(u64, 2), next.current.rows);
    var reader: identity.Reader = .{ .root = next, .pages = &pages };
    const original = (try reader.byId(a, next.current, id)).?;
    try std.testing.expectEqual(@as(u64, 17), original.coordinate.row);
    // Removing a file streams only its by-file membership. A reused slot may
    // represent a different file, so old memberships must disappear first.
    changed = .{ false, true };
    const third = try Builder.create(a, &manager, &pages, &.{ "b", "c" }, next, delta, &.{}, "source", "three");
    defer third.deinit();
    try third.add(state, .{ .file = third.slots[1], .group = 2, .row = 99 });
    const final = try third.finish(@splat(1), @splat(2), @splat(5));
    reader.root = final;
    const moved = (try reader.byId(a, final.current, id)).?;
    try std.testing.expectEqual(@as(u64, 99), moved.coordinate.row);
    try std.testing.expectEqual(@as(u64, 2), final.current.rows);
    try std.testing.expect((try reader.byLocation(a, final.current, "a", 0, 17)) == null);
    const rank = try reader.canonicalRank(a, final.current, moved);
    try std.testing.expect(rank < final.current.rows);
    // Complete rewrites must not open old row maps: there is no retained
    // membership to collide with or merge. Keep only the small file census.
    for ([_]?tree.Ref{ final.current.by_id, final.current.by_location, final.current.by_file, final.current.by_order }) |ref| if (ref) |root| try store.delete(&try Pages.identity(pages.domain, root));
    const fourth = try Builder.create(a, &manager, &pages, &.{"z"}, final, null, &.{}, "source", "four");
    defer fourth.deinit();
    try std.testing.expectEqual(@as(usize, 0), fourth.unchanged.count());
    try fourth.add(.{ .id = @splat(9), .values = @splat(10) }, .{ .file = fourth.slots[0], .group = 0, .row = 2 });
    const rebuilt = try fourth.finish(@splat(1), @splat(2), @splat(6));
    try std.testing.expectEqual(@as(u64, 1), rebuilt.current.rows);
    reader.root = rebuilt;
    try std.testing.expect((try reader.byId(a, rebuilt.current, @splat(9))) != null);
}

test "external lake index input proofs preserve only unchanged index dependencies" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-input-proof");
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
    const recipes = [_]@import("lake_row_input_recipes.zig").Recipe{
        .{ .name = "body", .digest = @splat(10), .columns = &.{"body"} },
        .{ .name = "cover", .digest = @splat(11), .columns = &.{ "body", "label" } },
        .{ .name = "another_body", .digest = @splat(10), .columns = &.{"body"} },
    };
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const oa = output.allocator();
    const first = try Builder.create(a, &manager, &pages, &.{"a"}, null, null, &recipes, "source", "one");
    defer first.deinit();
    try first.addWithInputs(.{ .id = @splat(3), .values = @splat(4) }, .{ .file = 0, .group = 0, .row = 0 }, &.{ @splat(12), @splat(13) });
    var old = try first.finish(@splat(1), @splat(2), @splat(3));
    old.artifacts = @splat(9);
    try std.testing.expectEqual(@as(u64, 2), old.current.input_versions.?.records);
    const next = try Builder.create(a, &manager, &pages, &.{"b"}, null, null, &recipes, "source", "two");
    defer next.deinit();
    try next.addWithInputs(.{ .id = @splat(3), .values = @splat(5) }, .{ .file = 0, .group = 2, .row = 17 }, &.{ @splat(12), @splat(14) });
    var current = try next.finish(@splat(1), @splat(6), @splat(4));
    const binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "one", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} };
    const declarations = [_]local.serverless_segment_sidecar_manifest.DeclaredArtifact{
        .{ .name = "body", .binding = binding, .artifact = .{ .name = "body", .kind = .text_segment, .artifact_id = "unused", .checksum = "unused", .byte_len = 1 } },
        .{ .name = "cover", .binding = binding, .artifact = .{ .name = "cover", .kind = .text_segment, .artifact_id = "unused", .checksum = "unused", .byte_len = 1 } },
    };
    const changed_bindings = try oa.dupe(@import("lake_row_input_recipes.zig").Recipe, current.current.input_recipes);
    changed_bindings[0].binding = @splat(99);
    var reconfigured = current;
    reconfigured.current.input_recipes = changed_bindings;
    try std.testing.expectEqual(@as(usize, 0), (try identity.proveIndexReuse(a, oa, &pages, old, reconfigured, &declarations)).len);
    const reused = try identity.proveIndexReuse(a, oa, &pages, old, current, &declarations);
    try std.testing.expectEqual(@as(usize, 1), reused.len);
    try std.testing.expectEqualStrings("body", reused[0].name);
    const translation = (try @import("lake_row_translations.zig").build(a, &pages, reused[0].space, current.current, .{ .io = std.testing.io }, 1024 * 1024)).?;
    reused[0].translation = translation;
    current.reused = reused;
    current.artifacts = @splat(9);
    try current.reseal(a);
    try current.validate(a);
    var dependencies = identity.dependencies(current);
    var forward_retained = false;
    var reverse_retained = false;
    while (dependencies.next()) |ref| {
        forward_retained = forward_retained or ref.eql(translation.forward);
        reverse_retained = reverse_retained or ref.eql(translation.reverse);
    }
    try std.testing.expect(forward_retained and reverse_retained);
    var reader: identity.Reader = .{ .root = current, .pages = &pages, .verify_values = false };
    reader.root.indexes = current.spaceForIndex("body");
    const moved = (try reader.fromIndex(a, "a", 0, 0)).?;
    try std.testing.expectEqual(@as(u64, 17), moved.coordinate.row);
    reader.verify_values = true;
    try std.testing.expect((try reader.fromIndex(a, "a", 0, 0)) == null);
    // A certificate covers the exact name-to-space evidence, not only roots.
    const tampered = try oa.dupe(identity.ReusedIndex, reused);
    tampered[0].name = "cover";
    var invalid = current;
    invalid.reused = tampered;
    try std.testing.expectError(error.InvalidLogicalRowIdentity, invalid.validate(a));
}

test "external lake compact coordinate maps reduce durable bytes and preserve exact lookups" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("compact-row-map-size");
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
    const Census = struct {
        a: A,
        seen: std.AutoHashMapUnmanaged(tree.Ref, void) = .empty,
        bytes: usize = 0,
        pub fn skip(self: *@This(), ref: tree.Ref) !bool {
            return self.seen.contains(ref);
        }
        pub fn visit(self: *@This(), ref: tree.Ref) !void {
            try self.seen.put(self.a, ref, {});
            self.bytes += ref.bytes;
        }
    };
    var sizes: [2]usize = undefined;
    var roots: [2]Root = undefined;
    for (0..2) |mode| {
        pages.prefix_keys = false;
        const builder = try Builder.createWithOptions(a, &manager, &pages, &.{"file"}, null, null, &.{.{ .name = "body", .digest = @splat(9), .columns = &.{"body"} }}, "source", "snapshot", mode == 1);
        defer builder.deinit();
        for (0..256) |row| {
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, row, .big);
            var id: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(&key, &id, .{});
            try builder.addWithInputs(.{ .id = id, .values = id }, .{ .file = 0, .group = 0, .row = row }, &.{id});
        }
        roots[mode] = try builder.finish(@splat(1), @splat(2), @splat(3));
        var census: Census = .{ .a = a };
        defer census.seen.deinit(a);
        var dependencies = identity.dependencies(roots[mode]);
        while (dependencies.next()) |ref| try tree.walkPostOrder(a, pages.store(), ref, &census, false);
        sizes[mode] = census.bytes;
        var reader: identity.Reader = .{ .root = roots[mode], .pages = &pages };
        const refs = &.{ local.storage_rowsource_types.RowRef{ .external = .{ .source_id = "source", .snapshot_id = "snapshot", .file_id = "file", .row_group_ordinal = 0, .row_ordinal = 255 } }, local.storage_rowsource_types.RowRef{ .external = .{ .source_id = "source", .snapshot_id = "snapshot", .file_id = "file", .row_group_ordinal = 0, .row_ordinal = 0 } } };
        const resolved = try reader.translateMany(a, refs, true);
        defer a.free(resolved);
        try std.testing.expectEqual(@as(u64, 255), resolved[0].coordinate.row);
        try std.testing.expectEqual(@as(u64, 0), resolved[1].coordinate.row);
    }
    std.debug.print("compact row navigation: rows=256 legacy_bytes={d} compact_bytes={d}\n", .{ sizes[0], sizes[1] });
    try std.testing.expect(sizes[1] * 10 < sizes[0] * 7);
}

test "external lake supplemental cohorts reject duplicate omitted and mismatched membership" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("cohort-membership-errors");
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
    const recipes = [_]@import("lake_row_input_recipes.zig").Recipe{ .{ .name = "a", .digest = @splat(8), .columns = &.{"body"} }, .{ .name = "b", .digest = @splat(9), .columns = &.{"label"} } };
    const first = try Builder.createWithOptions(a, &manager, &pages, &.{"file"}, null, null, recipes[0..1], "source", "cut", true);
    defer first.deinit();
    try first.addWithInputs(.{ .id = @splat(1), .values = @splat(1) }, .{ .file = 0, .group = 0, .row = 0 }, &.{@splat(4)});
    try first.addWithInputs(.{ .id = @splat(2), .values = @splat(2) }, .{ .file = 0, .group = 0, .row = 1 }, &.{@splat(5)});
    var root = try first.finish(@splat(1), @splat(2), @splat(3));
    root.artifacts = @splat(7);
    var changed = [_]bool{false};
    var delta: @import("lake_source_navigation.zig").Delta = undefined;
    delta.changed = &changed;
    delta.selection = null;
    for (0..4) |mode| {
        const next = try Builder.createWithOptions(a, &manager, &pages, &.{"file"}, root, delta, &recipes, "source", "cut", true);
        defer next.deinit();
        if (mode == 0) {
            try std.testing.expectError(error.InvalidLogicalRowIdentity, next.addExtension(@splat(3), .{ .file = 0, .group = 0, .row = 0 }, &.{@splat(6)}));
            continue;
        }
        try next.addExtension(@splat(1), .{ .file = 0, .group = 0, .row = 0 }, &.{@splat(6)});
        if (mode == 1) {
            try std.testing.expectError(error.InvalidLogicalRowIdentity, next.addExtension(@splat(1), .{ .file = 0, .group = 0, .row = 0 }, &.{@splat(6)}));
        } else if (mode == 2) {
            try std.testing.expectError(error.InvalidLogicalRowIdentity, next.finish(@splat(1), @splat(2), @splat(4)));
        } else {
            try next.addExtension(@splat(2), .{ .file = 0, .group = 0, .row = 1 }, &.{@splat(7)});
            var complete = try next.finish(@splat(1), @splat(2), @splat(4));
            try std.testing.expectEqual(root.current.by_id, complete.current.by_id);
            try std.testing.expectEqual(@as(u64, 4), complete.current.input_versions.?.records);
            // Membership equality alone must never certify value/index reuse.
            try std.testing.expect(!try identity.proveReuse(a, &pages, root, &complete));
        }
    }
}

test "external lake ordered identity publication matches mutation fallback for fresh single and multiple cohorts" {
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("ordered-identity-maps");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call, .async_writes = false };
    defer manager.deinit();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: Pages = .{ .domain = @splat(7), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    const recipes = [_]@import("lake_row_input_recipes.zig").Recipe{
        .{ .name = "one", .digest = @splat(11), .columns = &.{"one"} },
        .{ .name = "two", .digest = @splat(22), .columns = &.{"two"} },
    };
    for (1..3) |cohort_count| {
        const direct = try Builder.createWithOptions(a, &manager, &pages, &.{"file"}, null, null, recipes[0..cohort_count], "source", "snapshot", true);
        defer direct.deinit();
        const fallback = try Builder.createWithOptions(a, &manager, &pages, &.{"file"}, null, null, recipes[0..cohort_count], "source", "snapshot", true);
        defer fallback.deinit();
        // An absent deletion changes no rows but makes the mutation stream
        // unordered, requiring the general reducer and testing the proof gate.
        const absent_id: [32]u8 = @splat(255);
        const absent_input: [64]u8 = @splat(255);
        try fallback.edits[2].addRecord(&absent_id, null);
        try fallback.edits[6].addRecord(&absent_input, null);
        for (0..48) |i| {
            var id: [32]u8 = @splat(0);
            std.mem.writeInt(u64, id[24..32], 48 - i, .big);
            const state: State = .{ .id = id, .values = id };
            const coordinate: Coordinate = .{ .file = 0, .group = 0, .row = i };
            const inputs = [_][32]u8{ @splat(@intCast(i + 1)), @splat(@intCast(i + 2)) };
            try direct.addWithInputs(state, coordinate, inputs[0..cohort_count]);
            try fallback.addWithInputs(state, coordinate, inputs[0..cohort_count]);
        }
        const actual = try direct.finish(@splat(1), @splat(2), @splat(3));
        const expected = try fallback.finish(@splat(1), @splat(2), @splat(3));
        try std.testing.expectEqual(@as(u64, 48), actual.current.rows);
        try std.testing.expectEqual(@as(u64, 48 * cohort_count), actual.current.input_versions.?.records);
        inline for (.{ "by_id", "by_file", "by_order", "input_versions" }) |field| try std.testing.expect(try tree.equivalent(a, pages.store(), @field(expected.current, field), @field(actual.current, field), .{}));
    }
}
