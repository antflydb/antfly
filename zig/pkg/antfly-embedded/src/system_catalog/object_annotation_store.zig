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

//! Transaction-local catalog annotation effects. The caller verifies guards,
//! capability activation and the complete table preimage before applying them.
const std = @import("std");
const objects = @import("object_annotations.zig");
const definitions = @import("object_definitions.zig");
const A = std.mem.Allocator;
pub const Definition = struct { schema_json: []const u8, indexes_json: []const u8 };
pub const CopySource = struct { table: u64, definition: Definition };
fn objectKey(a: A, key: objects.Key) ![]const u8 {
    return std.fmt.allocPrint(a, "{s}:{s}", .{ @tagName(key.kind), key.name });
}

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

/// No persistent initialization is permitted here. Every returned allocation
/// belongs to the caller's read/preparation region, including a provisional
/// directory for a database created before this capability was activated.
const Directory = struct { state: objects.State, persistent: bool };
pub fn directoryLeaky(a: A, txn: anytype, group: u64, table: u64, definition: Definition) !Directory {
    var declared = try definitions.init(a, definition.schema_json, definition.indexes_json);
    defer declared.deinit();
    return directoryForDeclarationsLeaky(a, txn, group, table, declared);
}

fn directoryForDeclarationsLeaky(a: A, txn: anytype, group: u64, table: u64, declared: definitions.Owned) !Directory {
    const key = try objects.directoryKey(group, table);
    if (try optional(txn, &key)) |bytes| {
        if (bytes.len == 0 or bytes.len > objects.max_encoded_bytes) return error.InvalidCatalogRecord;
        const state = try std.json.parseFromSliceLeaky(objects.State, a, bytes, .{ .allocate = .alloc_always });
        try state.validate(a);
        if (state.table_id != table or !std.mem.eql(u8, &state.epoch.definition_digest, &declared.digest) or state.objects.len != declared.declarations.len) return error.InvalidCatalogRecord;
        for (state.objects) |object| {
            const actual = declared.byKey(object.key) orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &object.signature, &actual.signature) or !std.mem.eql(u8, &object.declared_description_digest, &objects.descriptionDigest(actual.declared_description))) return error.InvalidCatalogRecord;
        }
        return .{ .state = state, .persistent = true };
    }
    var prepared = try objects.prepare(a, table, null, declared.digest, declared.declarations);
    defer prepared.deinit();
    return .{ .state = try prepared.state.cloneLeaky(a), .persistent = false };
}

pub const PreparedChange = struct {
    alloc: A,
    table: u64,
    directory: []u8,
    retire_annotations: []u64,
    inherited: []Inherited = &.{},
    const Inherited = struct { object: u64, value: []u8 };

    pub fn deinit(self: *PreparedChange) void {
        self.alloc.free(self.directory);
        self.alloc.free(self.retire_annotations);
        for (self.inherited) |note| self.alloc.free(note.value);
        self.alloc.free(self.inherited);
        self.* = undefined;
    }
    /// Retained logical objects cross a physical generation in the accepting
    /// transaction. Read the latest point notes, never caller-carried text or
    /// a preflight snapshot. An explicit inline-description edit supersedes a
    /// prior SQL override exactly as it does for in-place schema evolution.
    pub fn inherit(self: *PreparedChange, txn: anytype, group: u64, source_table: u64, source_definition: Definition) !void {
        if (source_table == self.table or self.inherited.len != 0) return error.InvalidCatalogMutation;
        if (try optional(txn, &try objects.directoryKey(group, source_table)) == null) return;
        var region = std.heap.ArenaAllocator.init(self.alloc);
        defer region.deinit();
        const a = region.allocator();
        const source = try directoryLeaky(a, txn, group, source_table, source_definition);
        var destination = try objects.State.decode(a, self.directory);
        defer destination.deinit();
        if (destination.value.table_id != self.table) return error.InvalidCatalogRecord;
        var names: std.StringHashMapUnmanaged(objects.Object) = .empty;
        for (source.state.objects) |object| try names.put(a, try std.fmt.allocPrint(a, "{s}:{s}", .{ @tagName(object.key.kind), object.key.name }), object);
        var notes: std.ArrayList(Inherited) = .empty;
        defer notes.deinit(self.alloc);
        errdefer for (notes.items) |note| self.alloc.free(note.value);
        for (destination.value.objects) |object| {
            const key = try std.fmt.allocPrint(a, "{s}:{s}", .{ @tagName(object.key.kind), object.key.name });
            const prior = names.get(key) orelse continue;
            if (!std.mem.eql(u8, &prior.declared_description_digest, &object.declared_description_digest)) continue;
            const raw = (try optional(txn, &try objects.annotationKey(group, source_table, prior.id))) orelse continue;
            var parsed = try objects.Annotation.decode(self.alloc, raw);
            defer parsed.deinit();
            if (parsed.value.table_id != source_table or parsed.value.object_id != prior.id) return error.InvalidCatalogRecord;
            var note = parsed.value;
            note.table_id = self.table;
            note.object_id = object.id;
            const value = try note.encode(self.alloc);
            errdefer self.alloc.free(value);
            try notes.append(self.alloc, .{ .object = object.id, .value = value });
        }
        self.inherited = try notes.toOwnedSlice(self.alloc);
    }

    /// LIKE carries only guarded source keys and final declaration slots. Read
    /// current notes once per source at the accepting cut and allocate fresh
    /// destination identities. Preparation never mutates the transaction.
    pub fn copyAnnotations(self: *PreparedChange, txn: anytype, group: u64, destination_definition: Definition, sources: []const CopySource, copies: []const objects.Copy) !void {
        if (sources.len > 16) return error.InvalidCatalogMutation;
        for (sources, 0..) |source, index| {
            if (source.table == self.table) return error.InvalidCatalogMutation;
            for (sources[0..index]) |prior| if (source.table == prior.table) return error.InvalidCatalogMutation;
        }
        const Provider = struct {
            definitions: []const CopySource,
            pub fn load(provider: @This(), _: A, table: u64) !Definition {
                for (provider.definitions) |source| if (source.table == table) return source.definition;
                return error.InvalidCatalogMutation;
            }
        };
        return self.copyAnnotationsFrom(txn, group, destination_definition, Provider{ .definitions = sources }, copies);
    }

    /// Group intents once, then release each source's parse/directory region
    /// before loading another. Peak working memory is one source plus the
    /// destination and bounded prepared notes, independent of source count.
    /// The provider reads only the already authenticated source-ID read set.
    pub fn copyAnnotationsFrom(self: *PreparedChange, txn: anytype, group: u64, destination_definition: Definition, provider: anytype, copies: []const objects.Copy) !void {
        if (self.inherited.len != 0 or copies.len > objects.max_objects) return error.InvalidCatalogMutation;
        var region = std.heap.ArenaAllocator.init(self.alloc);
        defer region.deinit();
        const a = region.allocator();
        var target = try definitions.init(a, destination_definition.schema_json, destination_definition.indexes_json);
        defer target.deinit();
        var destination = try objects.State.decode(a, self.directory);
        defer destination.deinit();
        if (destination.value.table_id != self.table or !std.mem.eql(u8, &destination.value.epoch.definition_digest, &target.digest)) return error.InvalidCatalogRecord;
        var targets: std.StringHashMapUnmanaged(objects.Object) = .empty;
        for (destination.value.objects) |object| try targets.put(a, try objectKey(a, object.key), object);
        var groups: std.AutoHashMapUnmanaged(u64, std.ArrayList(objects.Copy)) = .empty;
        for (copies) |intent| {
            try intent.validate();
            if (intent.source_table_id == self.table) return error.InvalidCatalogMutation;
            const entry = try groups.getOrPut(a, intent.source_table_id);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            if (groups.count() > 16) return error.InvalidCatalogMutation;
            try entry.value_ptr.append(a, intent);
        }
        var assigned: std.AutoHashMapUnmanaged(u64, void) = .empty;
        var notes: std.ArrayList(Inherited) = .empty;
        defer notes.deinit(self.alloc);
        errdefer for (notes.items) |note| self.alloc.free(note.value);
        var total: usize = 0;
        var sources = groups.iterator();
        while (sources.next()) |entry| {
            var source_region = std.heap.ArenaAllocator.init(self.alloc);
            defer source_region.deinit();
            const sa = source_region.allocator();
            const table = entry.key_ptr.*;
            const definition = try provider.load(sa, table);
            var declared = try definitions.init(self.alloc, definition.schema_json, definition.indexes_json);
            defer declared.deinit();
            const current = try directoryForDeclarationsLeaky(sa, txn, group, table, declared);
            var names: std.StringHashMapUnmanaged(objects.Object) = .empty;
            for (current.state.objects) |object| try names.put(sa, try objectKey(sa, object.key), object);
            for (entry.value_ptr.items) |intent| {
                const prior = names.get(try objectKey(sa, intent.source)) orelse return error.InvalidCatalogMutation;
                const object = targets.get(try objectKey(a, try target.copyDestination(intent))) orelse return error.InvalidCatalogMutation;
                const declaration = target.byKey(object.key) orelse return error.InvalidCatalogMutation;
                if (!std.mem.eql(u8, &declaration.signature, &object.signature)) return error.InvalidCatalogRecord;
                if (!std.mem.eql(u8, &prior.signature, &object.signature) or (try assigned.getOrPut(a, object.id)).found_existing) return error.InvalidCatalogMutation;
                var note: ?std.json.Parsed(objects.Annotation) = null;
                defer if (note) |*value| value.deinit();
                const description = if (try optional(txn, &try objects.annotationKey(group, table, prior.id))) |raw| blk: {
                    if (!current.persistent) return error.InvalidCatalogRecord;
                    note = try objects.Annotation.decode(sa, raw);
                    if (note.?.value.table_id != table or note.?.value.object_id != prior.id) return error.InvalidCatalogRecord;
                    break :blk note.?.value.description;
                } else (declared.byKey(intent.source) orelse return error.InvalidCatalogRecord).declared_description;
                if (note == null and description == null) continue;
                const encoded = try (objects.Annotation{ .format = 1, .table_id = self.table, .object_id = object.id, .description = description }).encode(self.alloc);
                errdefer self.alloc.free(encoded);
                total = std.math.add(usize, total, encoded.len) catch return error.CatalogCommandTooLarge;
                if (total > objects.max_index_description_bytes) return error.CatalogCommandTooLarge;
                try notes.append(self.alloc, .{ .object = object.id, .value = encoded });
            }
        }
        self.inherited = try notes.toOwnedSlice(self.alloc);
    }

    pub fn write(self: PreparedChange, txn: anytype, group: u64) !void {
        try txn.put(&try objects.directoryKey(group, self.table), self.directory);
        for (self.inherited) |note| try txn.put(&try objects.annotationKey(group, self.table, note.object), note.value);
        for (self.retire_annotations) |id| try txn.delete(&try objects.annotationKey(group, self.table, id));
    }
};

/// Existing directories must track every definition mutation. First tracking
/// materializes the BEFORE identities too, so a concurrent first COMMENT cannot
/// confuse DROP/CREATE with the same name or a byte-identical postimage.
pub fn prepareTableChange(backing: A, txn: anytype, group: u64, table: u64, before: ?Definition, after: Definition, renames: []const objects.Rename) !?PreparedChange {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const directory_key = try objects.directoryKey(group, table);
    const current = try optional(txn, &directory_key);
    if (current != null and before == null) return error.InvalidCatalogRecord;
    const predecessor: Definition = before orelse .{ .schema_json = "", .indexes_json = "" };
    const old = try directoryLeaky(a, txn, group, table, predecessor);
    var next = definitions.init(a, after.schema_json, after.indexes_json) catch |err| switch (err) {
        error.OutOfMemory, error.CatalogCommandTooLarge => return err,
        // Only the predecessor is stored authority. A malformed proposed
        // postimage is ordinary nonadmission, not stored catalog corruption.
        else => return error.InvalidCatalogMutation,
    };
    defer next.deinit();
    if (!old.persistent and old.state.objects.len == 0 and next.declarations.len == 0 and renames.len == 0) return null;
    if (old.persistent and renames.len == 0 and std.mem.eql(u8, &old.state.epoch.definition_digest, &next.digest)) return null;
    var prepared = try objects.prepareWithRenames(a, table, old.state, next.digest, next.declarations, renames);
    defer prepared.deinit();
    const encoded = try prepared.state.encode(backing);
    errdefer backing.free(encoded);
    if (current) |bytes| if (std.mem.eql(u8, bytes, encoded)) {
        backing.free(encoded);
        return null;
    };
    const retired = try backing.dupe(u64, prepared.retire_annotations);
    return .{ .alloc = backing, .table = table, .directory = encoded, .retire_annotations = retired };
}

pub const Snapshot = struct {
    table_id: u64,
    epoch: objects.Epoch,
    object: objects.Object,
    annotation: ?objects.Annotation,
};
pub fn snapshotLeaky(a: A, txn: anytype, group: u64, table: u64, definition: Definition, key: objects.Key) !Snapshot {
    try key.validate();
    const directory = try directoryLeaky(a, txn, group, table, definition);
    const object = directory.state.byKey(key) orelse return if (key.kind == .index) error.SqlIndexNotFound else error.SqlConstraintNotFound;
    const note_key = try objects.annotationKey(group, table, object.id);
    const annotation: ?objects.Annotation = if (try optional(txn, &note_key)) |bytes| note: {
        if (bytes.len == 0 or bytes.len > objects.max_encoded_bytes) return error.InvalidCatalogRecord;
        const value = try std.json.parseFromSliceLeaky(objects.Annotation, a, bytes, .{ .allocate = .alloc_always });
        try value.validate();
        if (value.table_id != table or value.object_id != object.id) return error.InvalidCatalogRecord;
        break :note value;
    } else null;
    return .{ .table_id = table, .epoch = directory.state.epoch, .object = object, .annotation = annotation };
}

pub const Expected = struct { epoch: objects.Epoch, object_id: u64 };
pub const PreparedComment = struct {
    alloc: A,
    table: u64,
    object: u64,
    directory: ?[]u8,
    annotation: []u8,

    pub fn deinit(self: *PreparedComment) void {
        if (self.directory) |bytes| self.alloc.free(bytes);
        self.alloc.free(self.annotation);
        self.* = undefined;
    }
    pub fn write(self: PreparedComment, txn: anytype, group: u64) !void {
        if (self.directory) |bytes| try txn.put(&try objects.directoryKey(group, self.table), bytes);
        try txn.put(&try objects.annotationKey(group, self.table, self.object), self.annotation);
    }
};
pub fn prepareComment(backing: A, txn: anytype, group: u64, table: u64, definition: Definition, key: objects.Key, expected: Expected, description: ?[]const u8) !PreparedComment {
    try objects.validateDescription(description);
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const directory = try directoryLeaky(a, txn, group, table, definition);
    if (!std.meta.eql(expected.epoch, directory.state.epoch)) return error.CatalogGenerationChanged;
    const object = directory.state.byKey(key) orelse return error.CatalogGenerationChanged;
    if (object.id != expected.object_id) return error.CatalogGenerationChanged;
    const bytes = if (!directory.persistent) try directory.state.encode(backing) else null;
    errdefer if (bytes) |owned| backing.free(owned);
    const value: objects.Annotation = .{ .format = 1, .table_id = table, .object_id = object.id, .description = objects.normalized(description) };
    return .{ .alloc = backing, .table = table, .object = object.id, .directory = bytes, .annotation = try value.encode(backing) };
}

pub fn retireTable(a: A, txn: anytype, group: u64, table: u64) !void {
    const key = try objects.directoryKey(group, table);
    const bytes = (try optional(txn, &key)) orelse return;
    var state = try objects.State.decode(a, bytes);
    defer state.deinit();
    if (state.value.table_id != table) return error.InvalidCatalogRecord;
    for (state.value.objects) |object| try txn.delete(&try objects.annotationKey(group, table, object.id));
    try txn.delete(&key);
}

const TestTxn = struct {
    alloc: A,
    rows: std.StringHashMapUnmanaged([]u8) = .empty,
    writes: usize = 0,

    fn deinit(self: *@This()) void {
        var iterator = self.rows.iterator();
        while (iterator.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.rows.deinit(self.alloc);
    }
    fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
        return self.rows.get(key) orelse error.NotFound;
    }
    fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        const owned_value = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned_value);
        const entry = try self.rows.getOrPut(self.alloc, owned_key);
        if (entry.found_existing) {
            self.alloc.free(owned_key);
            self.alloc.free(entry.value_ptr.*);
        }
        entry.value_ptr.* = owned_value;
        self.writes += 1;
    }
    fn delete(self: *@This(), key: []const u8) !void {
        if (self.rows.fetchRemove(key)) |removed| {
            self.alloc.free(removed.key);
            self.alloc.free(removed.value);
        }
        self.writes += 1;
    }
};

test "catalog object annotation effects keep snapshots and preparations read only and update comments with one point write" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const ra = region.allocator();
    var txn: TestTxn = .{ .alloc = a };
    defer txn.deinit();
    const definition: Definition = .{ .schema_json = "{\"checks\":[{\"name\":\"status_check\"}],\"relational_indexes\":[{\"name\":\"lookup\",\"description\":\"inline\",\"keys\":[{\"column\":\"status\"}]}]}", .indexes_json = "{}" };
    const check: objects.Key = .{ .kind = .constraint, .name = "status_check" };
    const index: objects.Key = .{ .kind = .index, .name = "lookup" };
    const before = try snapshotLeaky(ra, &txn, 41, 7, definition, check);
    var first = try prepareComment(a, &txn, 41, 7, definition, check, .{ .epoch = before.epoch, .object_id = before.object.id }, "first check");
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 0), txn.writes);
    try first.write(&txn, 41);
    try std.testing.expectEqual(@as(usize, 2), txn.writes);
    const indexed = try snapshotLeaky(ra, &txn, 41, 7, definition, index);
    var second = try prepareComment(a, &txn, 41, 7, definition, index, .{ .epoch = indexed.epoch, .object_id = indexed.object.id }, null);
    defer second.deinit();
    try std.testing.expect(second.directory == null);
    try second.write(&txn, 41);
    try std.testing.expectEqual(@as(usize, 3), txn.writes);
    const unchanged = try snapshotLeaky(ra, &txn, 41, 7, definition, check);
    try std.testing.expectEqualStrings("first check", unchanged.annotation.?.description.?);
    const cleared = try snapshotLeaky(ra, &txn, 41, 7, definition, index);
    try std.testing.expect(cleared.annotation != null and cleared.annotation.?.description == null);
    try std.testing.expectEqualDeep(indexed.epoch, cleared.epoch);
    try retireTable(a, &txn, 41, 7);
    try std.testing.expectEqual(@as(usize, 0), txn.rows.count());
}

test "catalog object annotation effects reject stale early-bound identities after identical-name recreation and detect forged directory declarations" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const ra = region.allocator();
    var txn: TestTxn = .{ .alloc = a };
    defer txn.deinit();
    const original: Definition = .{ .schema_json = "{\"relational_indexes\":[{\"name\":\"lookup\",\"keys\":[{\"column\":\"status\"}]}]}", .indexes_json = "{}" };
    const removed: Definition = .{ .schema_json = "{}", .indexes_json = "{}" };
    const key: objects.Key = .{ .kind = .index, .name = "lookup" };
    const captured = try snapshotLeaky(ra, &txn, 41, 7, original, key);
    // First tracking must retain the predecessor's provisional high-water;
    // a DROP/CREATE race cannot make its ID usable by a stale first COMMENT.
    var drop = (try prepareTableChange(a, &txn, 41, 7, original, removed, &.{})).?;
    defer drop.deinit();
    try drop.write(&txn, 41);
    var recreate = (try prepareTableChange(a, &txn, 41, 7, removed, original, &.{})).?;
    defer recreate.deinit();
    try recreate.write(&txn, 41);
    const live = try snapshotLeaky(ra, &txn, 41, 7, original, key);
    try std.testing.expect(live.object.id != captured.object.id);
    const writes = txn.writes;
    try std.testing.expectError(error.CatalogGenerationChanged, prepareComment(a, &txn, 41, 7, original, key, .{ .epoch = captured.epoch, .object_id = captured.object.id }, "stale"));
    try std.testing.expectEqual(writes, txn.writes);
    var directory = try objects.State.decode(a, recreate.directory);
    defer directory.deinit();
    const entries = try a.dupe(objects.Object, directory.value.objects);
    defer a.free(entries);
    entries[0].key.name = "forged";
    var invalid = directory.value;
    invalid.objects = entries;
    const bytes = try invalid.encode(a);
    defer a.free(bytes);
    try txn.put(&try objects.directoryKey(41, 7), bytes);
    try std.testing.expectError(error.InvalidCatalogRecord, snapshotLeaky(ra, &txn, 41, 7, original, key));
}

// Match the platform fault harness policy without importing runtime modules
// into this standard-library-only model suite. Refuse resize/remap so backing
// address reuse cannot change allocation counts between failure iterations.
fn checkAllocationFailures(comptime function: anytype) !void {
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), function, .{});
    try std.testing.expectEqual(stable.allocated_bytes, stable.freed_bytes);
}

fn inheritanceAllocationCase(a: A) !void {
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const ra = region.allocator();
    var txn: TestTxn = .{ .alloc = a };
    defer txn.deinit();
    const definition: Definition = .{ .schema_json = "{\"checks\":[{\"name\":\"check\"}],\"relational_indexes\":[{\"name\":\"index\",\"description\":\"inline\",\"keys\":[{\"column\":\"value\"}]}]}", .indexes_json = "{}" };
    const check: objects.Key = .{ .kind = .constraint, .name = "check" };
    const index: objects.Key = .{ .kind = .index, .name = "index" };
    const before = try snapshotLeaky(ra, &txn, 41, 7, definition, check);
    var first = try prepareComment(a, &txn, 41, 7, definition, check, .{ .epoch = before.epoch, .object_id = before.object.id }, "old");
    defer first.deinit();
    try first.write(&txn, 41);
    const indexed = try snapshotLeaky(ra, &txn, 41, 7, definition, index);
    var clear = try prepareComment(a, &txn, 41, 7, definition, index, .{ .epoch = indexed.epoch, .object_id = indexed.object.id }, null);
    defer clear.deinit();
    try clear.write(&txn, 41);
    var next = (try prepareTableChange(a, &txn, 41, 8, null, definition, &.{})).?;
    defer next.deinit();
    var changed = try prepareComment(a, &txn, 41, 7, definition, check, .{ .epoch = before.epoch, .object_id = before.object.id }, "latest Ω");
    defer changed.deinit();
    try changed.write(&txn, 41);
    const writes = txn.writes;
    try next.inherit(&txn, 41, 7, definition);
    try std.testing.expectEqual(writes, txn.writes);
    try next.write(&txn, 41);
    try retireTable(a, &txn, 41, 7);
    const kept = try snapshotLeaky(ra, &txn, 41, 8, definition, check);
    try std.testing.expectEqualStrings("latest Ω", kept.annotation.?.description.?);
    const null_note = try snapshotLeaky(ra, &txn, 41, 8, definition, index);
    try std.testing.expect(null_note.annotation != null and null_note.annotation.?.description == null);
    const edited: Definition = .{ .schema_json = "{\"checks\":[{\"name\":\"check\"}],\"relational_indexes\":[{\"name\":\"index\",\"description\":\"changed inline\",\"keys\":[{\"column\":\"value\"}]}]}", .indexes_json = "{}" };
    var replacement = (try prepareTableChange(a, &txn, 41, 9, null, edited, &.{})).?;
    defer replacement.deinit();
    try replacement.inherit(&txn, 41, 8, definition);
    try replacement.write(&txn, 41);
    const reset = try snapshotLeaky(ra, &txn, 41, 9, edited, index);
    try std.testing.expect(reset.annotation == null);
    const preserved = try snapshotLeaky(ra, &txn, 41, 9, edited, check);
    try std.testing.expectEqualStrings("latest Ω", preserved.annotation.?.description.?);
}

test "catalog object annotation generation inheritance captures latest notes preserves explicit null resets inline edits and releases every failed allocation" {
    try inheritanceAllocationCase(std.testing.allocator);
    try checkAllocationFailures(inheritanceAllocationCase);
}

test "catalog object annotation maintenance churn causes no directory or note writes and preserves epoch identity" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var txn: TestTxn = .{ .alloc = a };
    defer txn.deinit();
    const before: Definition = .{ .schema_json = "{\"version\":1}", .indexes_json = "{\"search\":{\"type\":\"full_text\"},\"_resolver_curation\":{\"revision\":1}}" };
    const after: Definition = .{ .schema_json = "{\"version\":2}", .indexes_json = "{\"_resolver_curation\":{\"revision\":2},\"enrichments\":[],\"search\":{\"type\":\"full_text\"}}" };
    const key: objects.Key = .{ .kind = .index, .name = "search" };
    const initial = try snapshotLeaky(region.allocator(), &txn, 41, 7, before, key);
    var comment = try prepareComment(a, &txn, 41, 7, before, key, .{ .epoch = initial.epoch, .object_id = initial.object.id }, "retained");
    defer comment.deinit();
    try comment.write(&txn, 41);
    const writes = txn.writes;
    var change = try prepareTableChange(a, &txn, 41, 7, before, after, &.{});
    defer if (change) |*value| value.deinit();
    try std.testing.expect(change == null);
    try std.testing.expectEqual(writes, txn.writes);
    const result = try snapshotLeaky(region.allocator(), &txn, 41, 7, after, key);
    try std.testing.expectEqualDeep(initial.epoch, result.epoch);
    try std.testing.expectEqual(initial.object.id, result.object.id);
    try std.testing.expectEqualStrings("retained", result.annotation.?.description.?);
}

fn copyAllocationCase(a: A) !void {
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const ra = region.allocator();
    var txn: TestTxn = .{ .alloc = a };
    defer txn.deinit();
    const source: Definition = .{ .schema_json = "{\"checks\":[{\"name\":\"source_check\"}],\"unique_constraints\":[{\"name\":\"source_key\",\"columns\":[\"id\"]}]}", .indexes_json = "{}" };
    const destination: Definition = .{ .schema_json = "{\"checks\":[{\"name\":\"copy_check\"}],\"unique_constraints\":[{\"name\":\"copy_key_collision_1\",\"columns\":[\"id\"]}]}", .indexes_json = "{}" };
    const keys = [_]objects.Key{ .{ .kind = .constraint, .name = "source_check" }, .{ .kind = .constraint, .name = "source_key" }, .{ .kind = .index, .name = "source_key" } };
    for (keys, 0..) |key, index| {
        const captured = try snapshotLeaky(ra, &txn, 41, 7, source, key);
        var note = try prepareComment(a, &txn, 41, 7, source, key, .{ .epoch = captured.epoch, .object_id = captured.object.id }, if (index == 2) null else "source note");
        defer note.deinit();
        try note.write(&txn, 41);
    }
    var prepared = (try prepareTableChange(a, &txn, 41, 8, null, destination, &.{})).?;
    defer prepared.deinit();
    const before = try snapshotLeaky(ra, &txn, 41, 7, source, keys[0]);
    var latest = try prepareComment(a, &txn, 41, 7, source, keys[0], .{ .epoch = before.epoch, .object_id = before.object.id }, "latest Ω");
    defer latest.deinit();
    try latest.write(&txn, 41);
    const intents = [_]objects.Copy{
        .{ .source_table_id = 7, .source = keys[0], .destination_family = .checks, .destination_ordinal = 0 },
        .{ .source_table_id = 7, .source = keys[1], .destination_family = .unique_constraints, .destination_ordinal = 0 },
        .{ .source_table_id = 7, .source = keys[2], .destination_family = .unique_constraints, .destination_ordinal = 0 },
    };
    const sources = [_]CopySource{.{ .table = 7, .definition = source }};
    const writes = txn.writes;
    try prepared.copyAnnotations(&txn, 41, destination, &sources, &intents);
    try std.testing.expectEqual(writes, txn.writes);
    try prepared.write(&txn, 41);
    const check = try snapshotLeaky(ra, &txn, 41, 8, destination, .{ .kind = .constraint, .name = "copy_check" });
    try std.testing.expectEqualStrings("latest Ω", check.annotation.?.description.?);
    const constraint = try snapshotLeaky(ra, &txn, 41, 8, destination, .{ .kind = .constraint, .name = "copy_key_collision_1" });
    try std.testing.expectEqualStrings("source note", constraint.annotation.?.description.?);
    const index = try snapshotLeaky(ra, &txn, 41, 8, destination, .{ .kind = .index, .name = "copy_key_collision_1" });
    try std.testing.expect(index.annotation != null and index.annotation.?.description == null);
    var invalid = (try prepareTableChange(a, &txn, 41, 9, null, destination, &.{})).?;
    defer invalid.deinit();
    invalid.copyAnnotations(&txn, 41, destination, &sources, &.{ intents[0], intents[0] }) catch |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.InvalidCatalogMutation, err);
    };
    invalid.copyAnnotations(&txn, 41, destination, &.{}, &intents) catch |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.InvalidCatalogMutation, err);
    };
    var wrong = intents[0];
    wrong.destination_family = .unique_constraints;
    invalid.copyAnnotations(&txn, 41, destination, &sources, &.{wrong}) catch |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.InvalidCatalogMutation, err);
    };
    try std.testing.expectEqual(@as(usize, 0), invalid.inherited.len);
}

test "catalog object annotation LIKE copies use final slots latest notes separate index constraints explicit null and bounded fault safe preparation" {
    try copyAllocationCase(std.testing.allocator);
    try checkAllocationFailures(copyAllocationCase);
}

fn groupedCopyAllocationCase(a: A) !void {
    const input = "{\"checks\":[{\"name\":\"c1\",\"description\":\"first effective note\"},{\"name\":\"c2\",\"description\":\"second effective note\"}]}";
    const output: Definition = .{ .schema_json = "{\"checks\":[{\"name\":\"d1\"},{\"name\":\"d2\"},{\"name\":\"d3\"},{\"name\":\"d4\"}]}", .indexes_json = "{}" };
    var txn: TestTxn = .{ .alloc = a };
    defer txn.deinit();
    var prepared = (try prepareTableChange(a, &txn, 41, 20, null, output, &.{})).?;
    defer prepared.deinit();
    const Provider = struct {
        counts: *[2]u32,
        pub fn load(self: @This(), region: A, id: u64) !Definition {
            const position: usize = switch (id) {
                7 => 0,
                9 => 1,
                else => return error.InvalidCatalogMutation,
            };
            self.counts[position] += 1;
            // This owned source buffer must leave with the per-source region.
            return .{ .schema_json = try region.dupe(u8, input), .indexes_json = "{}" };
        }
    };
    var counts: [2]u32 = @splat(0);
    const copies = [_]objects.Copy{
        .{ .source_table_id = 7, .source = .{ .kind = .constraint, .name = "c1" }, .destination_family = .checks, .destination_ordinal = 0 },
        .{ .source_table_id = 9, .source = .{ .kind = .constraint, .name = "c2" }, .destination_family = .checks, .destination_ordinal = 3 },
        .{ .source_table_id = 7, .source = .{ .kind = .constraint, .name = "c2" }, .destination_family = .checks, .destination_ordinal = 1 },
        .{ .source_table_id = 9, .source = .{ .kind = .constraint, .name = "c1" }, .destination_family = .checks, .destination_ordinal = 2 },
    };
    try prepared.copyAnnotationsFrom(&txn, 41, output, Provider{ .counts = &counts }, &copies);
    try std.testing.expectEqualDeep([_]u32{ 1, 1 }, counts);
    try std.testing.expectEqual(@as(usize, 0), txn.writes);
    try prepared.write(&txn, 41);
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    for ([_][]const u8{ "d1", "d2", "d3", "d4" }, 0..) |name, i| {
        const value = try snapshotLeaky(region.allocator(), &txn, 41, 20, output, .{ .kind = .constraint, .name = name });
        try std.testing.expectEqualStrings(if (i % 2 == 0) "first effective note" else "second effective note", value.annotation.?.description.?);
    }
}

test "catalog object annotation LIKE groups authenticated sources once and copies effective descriptions with bounded fault safe regions" {
    try groupedCopyAllocationCase(std.testing.allocator);
    try checkAllocationFailures(groupedCopyAllocationCase);
}

test "catalog object annotation LIKE releases each wide source before loading the next" {
    const a = std.testing.allocator;
    // Wide source metadata is intentionally irrelevant to copied declarations.
    // Meter only preparation, so the caller's immutable input is not charged.
    const padding = try a.alloc(u8, 512 * 1024);
    defer a.free(padding);
    @memset(padding, 'x');
    const source = try std.fmt.allocPrint(a, "{{\"checks\":[{{\"name\":\"c\"}}],\"opaque\":\"{s}\"}}", .{padding});
    defer a.free(source);
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(a);
    try output.appendSlice(a, "{\"checks\":[");
    var copies: [16]objects.Copy = undefined;
    for (&copies, 0..) |*copy, index| {
        var name: [64]u8 = undefined;
        try output.appendSlice(a, try std.fmt.bufPrint(&name, "{s}{{\"name\":\"d{d}\"}}", .{ if (index == 0) "" else ",", index }));
        copy.* = .{ .source_table_id = index + 1, .source = .{ .kind = .constraint, .name = "c" }, .destination_family = .checks, .destination_ordinal = @intCast(index) };
    }
    try output.appendSlice(a, "]}");
    const destination: Definition = .{ .schema_json = output.items, .indexes_json = "{}" };
    var meter = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    const ma = meter.allocator();
    {
        var txn: TestTxn = .{ .alloc = ma };
        defer txn.deinit();
        var prepared = (try prepareTableChange(ma, &txn, 41, 20, null, destination, &.{})).?;
        defer prepared.deinit();
        const Provider = struct {
            meter: *std.testing.FailingAllocator,
            source: []const u8,
            loads: *[16]u8,
            max_retained_before_load: *usize,
            pub fn load(self: @This(), region: A, id: u64) !Definition {
                if (id == 0 or id > self.loads.len) return error.InvalidCatalogMutation;
                self.loads[id - 1] += 1;
                self.max_retained_before_load.* = @max(self.max_retained_before_load.*, self.meter.allocated_bytes - self.meter.freed_bytes);
                return .{ .schema_json = try region.dupe(u8, self.source), .indexes_json = "{}" };
            }
        };
        var loads: [16]u8 = @splat(0);
        var retained: usize = 0;
        try prepared.copyAnnotationsFrom(&txn, 41, destination, Provider{ .meter = &meter, .source = source, .loads = &loads, .max_retained_before_load = &retained }, &copies);
        try std.testing.expectEqualDeep(@as([16]u8, @splat(1)), loads);
        // Retaining even one 512 KiB source would violate this bound. Notes
        // are absent here, isolating the source-working-set contract.
        try std.testing.expect(retained < 128 * 1024);
        try std.testing.expectEqual(@as(usize, 0), prepared.inherited.len);
        try std.testing.expectEqual(@as(usize, 0), txn.writes);
    }
    try std.testing.expectEqual(meter.allocated_bytes, meter.freed_bytes);
}
