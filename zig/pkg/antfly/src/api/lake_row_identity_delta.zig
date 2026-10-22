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
    unchanged: std.AutoHashMapUnmanaged(u32, void) = .empty,
    pending: local.sql_spill.Sort,
    edits: [7]Mutations,
    pub fn create(a: A, manager: *local.sql_spill.Manager, pages: *Pages, files: []const []const u8, prior: ?Root, delta: ?@import("lake_source_navigation.zig").Delta, recipes: []const @import("lake_row_input_recipes.zig").Recipe, source: []const u8, snapshot: []const u8) !*Builder {
        const self = try a.create(Builder);
        self.* = .{ .a = a, .arena = .init(a), .pages = pages, .prior = prior, .space = .{ .source = source, .snapshot = snapshot, .rows = 0, .file_count = 0, .files = null, .by_id = null, .by_location = null, .input_recipes = recipes }, .files = files, .slots = &.{}, .scan = &.{}, .pending = .init(a, manager, &.{.{}}, 1024 * 1024), .edits = undefined };
        self.pending.shared_runs = true;
        self.pending.encoded_bytes = true;
        for (&self.edits) |*edit| {
            edit.* = Mutations.init(a, manager, pages.store());
            edit.private_pages.phase = pages.artifacts.io_phase;
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
                    if (delta != null and !delta.?.isChanged(index)) {
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
        return self;
    }
    pub fn deinit(self: *Builder) void {
        for (&self.edits) |*edit| edit.deinit();
        self.pending.deinit();
        self.unchanged.deinit(self.a);
        self.arena.deinit();
        self.a.destroy(self);
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
            if (record.key.len != 16 or !std.mem.eql(u8, record.key[0..4], &prefix) or record.value.len != 64) return error.InvalidLogicalRowIdentity;
            const coordinate = try Coordinate.decode(record.key);
            try self.edits[2].addRecord(record.value[0..32], null);
            const location = identity.locationKey(name, coordinate.group, coordinate.row);
            try self.edits[3].addRecord(&location, null);
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
        if (inputs.len != self.cohorts.len) return error.InvalidLogicalRowIdentity;
        const value = try self.a.alloc(u8, 48 + inputs.len * 32);
        defer self.a.free(value);
        value[0..16].* = coordinate.encode();
        value[16..48].* = state.values;
        for (inputs, 0..) |input, i| @memcpy(value[48 + i * 32 ..][0..32], &input);
        try self.pending.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = &state.id })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = value })}, .ordinal = self.pending.total });
    }
    pub fn finish(self: *Builder, namespace: identity.logical.Digest, recipe: identity.logical.Digest, cut: identity.logical.Digest) !Root {
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
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
            _ = scratch.reset(.retain_capacity);
            const a = scratch.allocator();
            const row = (try self.pending.next(a)) orelse break;
            if (row.keys.len != 1 or row.values.len != 1 or row.keys[0].value != .string or row.values[0].value != .string or row.keys[0].value.string.len != 32 or row.values[0].value.string.len != 48 + self.cohorts.len * 32) return error.InvalidLogicalRowIdentity;
            const id = row.keys[0].value.string;
            const value = row.values[0].value.string;
            if (previous_id) |previous| if (std.mem.eql(u8, &previous, id)) return error.DuplicateLogicalRowKey;
            previous_id = id[0..32].*;
            if (retained) |*cursor| {
                if (candidate == null or std.mem.order(u8, candidate.?.key, id) == .lt) {
                    cursor.seekForward(id) catch |err| return identity.derivedReadFailure(err);
                    candidate = cursor.next() catch |err| return identity.derivedReadFailure(err);
                }
                if (candidate) |existing| if (std.mem.eql(u8, existing.key, id)) {
                    if (existing.value.len != 56) return error.InvalidLogicalRowIdentity;
                    const coordinate = try Coordinate.decode(existing.value[0..16]);
                    if (coordinate.file >= self.prior.?.current.file_count or std.mem.readInt(u64, existing.value[48..56], .big) >= self.prior.?.current.rows or std.mem.allEqual(u8, existing.value[16..48], 0)) return error.InvalidLogicalRowIdentity;
                    if (self.unchanged.contains(coordinate.file)) return error.DuplicateLogicalRowKey;
                };
            }
            const coordinate = try Coordinate.decode(value[0..16]);
            const name = inverse.get(coordinate.file) orelse return error.InvalidLogicalRowIdentity;
            var forward: [56]u8 = @splat(0);
            @memcpy(forward[0..48], value[0..48]);
            var state: [64]u8 = undefined;
            state[0..32].* = id[0..32].*;
            state[32..64].* = value[16..48].*;
            try self.edits[2].addRecord(id, &forward);
            const location = identity.locationKey(name, coordinate.group, coordinate.row);
            try self.edits[3].addRecord(&location, &state);
            try self.edits[4].addRecord(value[0..16], &state);
            const order = identity.orderKey(name, coordinate.group, coordinate.row);
            try self.edits[5].addRecord(&order, id);
            for (self.cohorts, 0..) |input_recipe, i| {
                var input_key: [64]u8 = undefined;
                input_key[0..32].* = input_recipe.digest;
                input_key[32..64].* = id[0..32].*;
                try self.edits[6].addRecord(&input_key, value[48 + i * 32 ..][0..32]);
            }
        }
        const old = if (self.prior != null and self.unchanged.count() != 0) self.prior.?.current else self.space;
        self.space.files = try self.edits[0].finish(old.files);
        self.space.file_slots = try self.edits[1].finish(old.file_slots);
        self.space.by_id = try self.edits[2].finish(old.by_id);
        self.space.by_location = try self.edits[3].finish(old.by_location);
        self.space.by_file = try self.edits[4].finish(old.by_file);
        self.space.by_order = try self.edits[5].finish(old.by_order);
        self.space.input_versions = try self.edits[6].finish(old.input_versions);
        self.space.rows = if (self.space.by_id) |ref| ref.records else 0;
        const root: Root = .{ .domain = self.pages.domain, .namespace = namespace, .values_recipe = recipe, .values_format = @import("lake_row_fingerprints.zig").format, .cut = cut, .current = self.space };
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
