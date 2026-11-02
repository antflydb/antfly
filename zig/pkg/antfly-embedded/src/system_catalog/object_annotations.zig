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

//! Table-scoped catalog object identities and point-addressed annotations.
//! Directory evolution and annotation retirement belong to the containing
//! native table transaction. Reads never initialize either persistent family.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_objects = 8192;
pub const max_description_bytes = 1024 * 1024;
pub const max_encoded_bytes = 4 * 1024 * 1024;
pub const directory_prefix = "\x00\x00__metadata__:sql_object_identities:v1:";
pub const annotation_prefix = "\x00\x00__metadata__:sql_object_annotations:v1:";
pub const Kind = enum { index, constraint };
pub const Key = struct {
    kind: Kind,
    name: []const u8,

    pub fn validate(self: Key) !void {
        if (self.name.len == 0 or self.name.len > 256 or !std.unicode.utf8ValidateSlice(self.name) or std.mem.indexOfScalar(u8, self.name, 0) != null) return error.InvalidCatalogMutation;
    }
    pub fn eql(self: Key, other: Key) bool {
        return self.kind == other.kind and std.mem.eql(u8, self.name, other.name);
    }
    fn mapKey(self: Key, a: A) ![]const u8 {
        return std.fmt.allocPrint(a, "{s}:{s}", .{ @tagName(self.kind), self.name });
    }
};
pub const Family = enum { checks, unique_constraints, relational_indexes };
/// An immutable source-to-destination declaration slot. Ordinals select an
/// already validated containing schema; they cannot allocate object identities.
pub const Copy = struct {
    source_table_id: u64,
    source: Key,
    destination_family: Family,
    destination_ordinal: u32,

    pub fn validate(self: Copy) !void {
        try self.source.validate();
        if (self.source_table_id == 0 or self.destination_ordinal >= max_objects or
            (self.destination_family == .checks and self.source.kind != .constraint) or
            (self.destination_family == .relational_indexes and self.source.kind != .index)) return error.InvalidCatalogMutation;
    }
};

pub const Rename = struct {
    kind: Kind,
    from: []const u8,
    to: []const u8,

    pub fn validate(self: Rename) !void {
        try (Key{ .kind = self.kind, .name = self.from }).validate();
        try (Key{ .kind = self.kind, .name = self.to }).validate();
        if (std.mem.eql(u8, self.from, self.to)) return error.InvalidCatalogMutation;
    }
};

/// One SQL rename can retain a constraint and its backing index together.
pub fn validateSingleRename(renames: []const Rename) !void {
    if (renames.len > 2) return error.InvalidCatalogMutation;
    for (renames) |rename| try rename.validate();
    if (renames.len == 2 and (renames[0].kind == renames[1].kind or
        !std.mem.eql(u8, renames[0].from, renames[1].from) or !std.mem.eql(u8, renames[0].to, renames[1].to))) return error.InvalidCatalogMutation;
}

pub fn cloneRenames(a: A, renames: []const Rename) ![]Rename {
    const out = try a.alloc(Rename, renames.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |copy| {
            a.free(copy.from);
            a.free(copy.to);
        }
        a.free(out);
    }
    for (renames, out) |rename, *copy| {
        const from = try a.dupe(u8, rename.from);
        errdefer a.free(from);
        copy.* = .{ .kind = rename.kind, .from = from, .to = try a.dupe(u8, rename.to) };
        initialized += 1;
    }
    return out;
}

pub fn freeRenames(a: A, renames: []const Rename) void {
    for (renames) |rename| {
        a.free(rename.from);
        a.free(rename.to);
    }
    a.free(renames);
}

pub const Declaration = struct {
    key: Key,
    signature: [32]u8,
    declared_description: ?[]const u8 = null,
};
pub const Object = struct {
    id: u64,
    key: Key,
    signature: [32]u8,
    declared_description_digest: [32]u8,
};
pub const Epoch = struct { revision: u64, definition_digest: [32]u8 };
pub const State = struct {
    format: u8,
    table_id: u64,
    epoch: Epoch,
    next_id: u64,
    objects: []const Object,

    pub fn validate(self: State, backing: A) !void {
        if (self.format != 1 or self.table_id == 0 or self.epoch.revision == 0 or self.next_id == 0 or std.mem.allEqual(u8, &self.epoch.definition_digest, 0) or self.objects.len > max_objects) return error.InvalidCatalogRecord;
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        var names: std.StringHashMapUnmanaged(void) = .empty;
        var previous: u64 = 0;
        for (self.objects) |object| {
            object.key.validate() catch return error.InvalidCatalogRecord;
            if (object.id <= previous or object.id >= self.next_id or std.mem.allEqual(u8, &object.signature, 0) or std.mem.allEqual(u8, &object.declared_description_digest, 0)) return error.InvalidCatalogRecord;
            if ((try names.getOrPut(a, try object.key.mapKey(a))).found_existing) return error.InvalidCatalogRecord;
            previous = object.id;
        }
    }
    pub fn byId(self: State, id: u64) ?Object {
        var low: usize = 0;
        var high = self.objects.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const object = self.objects[middle];
            if (object.id == id) return object;
            if (object.id < id) low = middle + 1 else high = middle;
        }
        return null;
    }
    pub fn byKey(self: State, key: Key) ?Object {
        for (self.objects) |object| if (object.key.eql(key)) return object;
        return null;
    }
    pub fn cloneLeaky(self: State, a: A) !State {
        const entries = try a.dupe(Object, self.objects);
        for (entries) |*object| object.key.name = try a.dupe(u8, object.key.name);
        var result = self;
        result.objects = entries;
        return result;
    }
    pub fn encode(self: State, a: A) ![]u8 {
        try self.validate(a);
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_encoded_bytes) return error.CatalogCommandTooLarge;
        return bytes;
    }
    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(State) {
        if (bytes.len == 0 or bytes.len > max_encoded_bytes) return error.InvalidCatalogRecord;
        var parsed = try std.json.parseFromSlice(State, a, bytes, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try parsed.value.validate(a);
        return parsed;
    }
};

pub fn validateDescription(value: ?[]const u8) !void {
    if (value) |text| {
        if (text.len > max_description_bytes) return error.CatalogCommandTooLarge;
        if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidCatalogMutation;
    }
}
pub fn normalized(value: ?[]const u8) ?[]const u8 {
    return if (value) |text| if (text.len != 0) text else null else null;
}
pub fn descriptionDigest(value: ?[]const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/catalog-declared-description/v1\x00");
    const text = normalized(value);
    hash.update(&.{@intFromBool(text != null)});
    if (text) |bytes| hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    state: State,
    /// Removed objects and explicitly replaced inline descriptions retire only
    /// their own override rows. The durable ID high-water survives empty sets.
    retire_annotations: []const u64,

    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Schema names are identity within one table epoch. DROP followed by CREATE
/// receives a new ID even for byte-identical definitions. Counter allocation
/// and the directory are never committed separately from their table mutation.
pub fn prepare(backing: A, table: u64, previous: ?State, definition_digest: [32]u8, declarations: []const Declaration) !Prepared {
    return prepareWithRenames(backing, table, previous, definition_digest, declarations, &.{});
}

pub fn prepareWithRenames(backing: A, table: u64, previous: ?State, definition_digest: [32]u8, declarations: []const Declaration, renames: []const Rename) !Prepared {
    if (table == 0 or declarations.len > max_objects or renames.len > max_objects or std.mem.allEqual(u8, &definition_digest, 0)) return error.InvalidCatalogMutation;
    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const a = arena.allocator();
    const old = previous orelse State{ .format = 1, .table_id = table, .epoch = .{ .revision = 1, .definition_digest = definition_digest }, .next_id = 1, .objects = &.{} };
    try old.validate(a);
    if (old.table_id != table) return error.InvalidCatalogRecord;
    var old_names: std.StringHashMapUnmanaged(Object) = .empty;
    var names: std.StringHashMapUnmanaged(void) = .empty;
    for (old.objects) |object| try old_names.put(a, try object.key.mapKey(a), object);
    var proposed: std.StringHashMapUnmanaged(Declaration) = .empty;
    for (declarations) |declaration| {
        try declaration.key.validate();
        const slot = try proposed.getOrPut(a, try declaration.key.mapKey(a));
        if (slot.found_existing) return error.InvalidCatalogMutation;
        slot.value_ptr.* = declaration;
    }
    var renamed_to: std.StringHashMapUnmanaged(Object) = .empty;
    var renamed_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (renames) |rename| {
        try rename.validate();
        const from = try (Key{ .kind = rename.kind, .name = rename.from }).mapKey(a);
        const to = try (Key{ .kind = rename.kind, .name = rename.to }).mapKey(a);
        const source = old_names.get(from) orelse return error.InvalidCatalogMutation;
        const target = proposed.get(to) orelse return error.InvalidCatalogMutation;
        if (old_names.contains(to) or proposed.contains(from) or !std.mem.eql(u8, &source.signature, &target.signature)) return error.InvalidCatalogMutation;
        if ((try renamed_ids.getOrPut(a, source.id)).found_existing) return error.InvalidCatalogMutation;
        const slot = try renamed_to.getOrPut(a, to);
        if (slot.found_existing) return error.InvalidCatalogMutation;
        slot.value_ptr.* = source;
    }
    const objects = try a.alloc(Object, declarations.len);
    var retired: std.ArrayList(u64) = .empty;
    var next_id = old.next_id;
    for (declarations, objects) |declaration, *object| {
        try declaration.key.validate();
        try validateDescription(declaration.declared_description);
        if (std.mem.allEqual(u8, &declaration.signature, 0)) return error.InvalidCatalogMutation;
        const key = try declaration.key.mapKey(a);
        if ((try names.getOrPut(a, key)).found_existing) return error.InvalidCatalogMutation;
        const prior = renamed_to.get(key) orelse old_names.get(key);
        const digest = descriptionDigest(declaration.declared_description);
        const id = if (prior) |existing| existing.id else fresh: {
            const id = next_id;
            next_id = std.math.add(u64, next_id, 1) catch return error.CatalogGenerationExhausted;
            break :fresh id;
        };
        if (prior) |existing| if (!std.mem.eql(u8, &existing.declared_description_digest, &digest)) try retired.append(a, id);
        object.* = .{ .id = id, .key = .{ .kind = declaration.key.kind, .name = try a.dupe(u8, declaration.key.name) }, .signature = declaration.signature, .declared_description_digest = digest };
    }
    for (old.objects) |object| if (!renamed_ids.contains(object.id) and !names.contains(try object.key.mapKey(a))) try retired.append(a, object.id);
    std.mem.sort(Object, objects, {}, struct {
        fn less(_: void, left: Object, right: Object) bool {
            return left.id < right.id;
        }
    }.less);
    var epoch = old.epoch;
    if (!std.mem.eql(u8, &old.epoch.definition_digest, &definition_digest)) {
        epoch.revision = std.math.add(u64, epoch.revision, 1) catch return error.CatalogGenerationExhausted;
        epoch.definition_digest = definition_digest;
    } else if (previous != null) {
        if (next_id != old.next_id or objects.len != old.objects.len or retired.items.len != 0) return error.InvalidCatalogRecord;
        for (objects, old.objects) |next, prior| if (next.id != prior.id or !next.key.eql(prior.key) or !std.mem.eql(u8, &next.signature, &prior.signature)) return error.InvalidCatalogRecord;
    }
    const state: State = .{ .format = 1, .table_id = table, .epoch = epoch, .next_id = next_id, .objects = objects };
    try state.validate(a);
    return .{ .arena = arena, .state = state, .retire_annotations = try retired.toOwnedSlice(a) };
}

/// A present null override suppresses an inline schema description. Absence
/// instead inherits that declaration. Annotation writes are point updates;
/// they never rewrite the identity directory or advance its structural epoch.
pub const Annotation = struct {
    format: u8,
    table_id: u64,
    object_id: u64,
    description: ?[]const u8,

    pub fn validate(self: Annotation) !void {
        if (self.format != 1 or self.table_id == 0 or self.object_id == 0) return error.InvalidCatalogRecord;
        validateDescription(self.description) catch return error.InvalidCatalogRecord;
        if (self.description) |text| if (text.len == 0) return error.InvalidCatalogRecord;
    }
    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(Annotation) {
        if (bytes.len == 0 or bytes.len > max_encoded_bytes) return error.InvalidCatalogRecord;
        var parsed = try std.json.parseFromSlice(Annotation, a, bytes, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
    pub fn encode(self: Annotation, a: A) ![]u8 {
        try self.validate();
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_encoded_bytes) return error.CatalogCommandTooLarge;
        return bytes;
    }
};

pub fn directoryKey(group: u64, table: u64) ![directory_prefix.len + 16]u8 {
    if (group == 0 or table == 0) return error.InvalidCatalogRecord;
    var key: [directory_prefix.len + 16]u8 = undefined;
    @memcpy(key[0..directory_prefix.len], directory_prefix);
    std.mem.writeInt(u64, key[directory_prefix.len..][0..8], group, .big);
    std.mem.writeInt(u64, key[directory_prefix.len + 8 ..][0..8], table, .big);
    return key;
}
pub fn annotationKey(group: u64, table: u64, object: u64) ![annotation_prefix.len + 24]u8 {
    if (group == 0 or table == 0 or object == 0) return error.InvalidCatalogRecord;
    var key: [annotation_prefix.len + 24]u8 = undefined;
    @memcpy(key[0..annotation_prefix.len], annotation_prefix);
    std.mem.writeInt(u64, key[annotation_prefix.len..][0..8], group, .big);
    std.mem.writeInt(u64, key[annotation_prefix.len + 8 ..][0..8], table, .big);
    std.mem.writeInt(u64, key[annotation_prefix.len + 16 ..][0..8], object, .big);
    return key;
}
pub const Scope = struct { group: u64, table: u64, object: ?u64 = null };
pub fn scopeFromKey(key: []const u8, comptime annotation: bool) !Scope {
    const prefix = if (annotation) annotation_prefix else directory_prefix;
    const suffix: usize = if (annotation) 24 else 16;
    if (key.len != prefix.len + suffix or !std.mem.startsWith(u8, key, prefix)) return error.InvalidCatalogRecord;
    const result: Scope = .{
        .group = std.mem.readInt(u64, key[prefix.len..][0..8], .big),
        .table = std.mem.readInt(u64, key[prefix.len + 8 ..][0..8], .big),
        .object = if (annotation) std.mem.readInt(u64, key[prefix.len + 16 ..][0..8], .big) else null,
    };
    if (result.group == 0 or result.table == 0 or (result.object != null and result.object.? == 0)) return error.InvalidCatalogRecord;
    return result;
}
pub fn directoryPrefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return groupPrefix(buffer, directory_prefix, group);
}
pub fn annotationPrefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return groupPrefix(buffer, annotation_prefix, group);
}
fn groupPrefix(buffer: []u8, prefix: []const u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidCatalogRecord;
    if (buffer.len < prefix.len + 8) return error.NoSpaceLeft;
    const bytes = buffer[0 .. prefix.len + 8];
    @memcpy(bytes[0..prefix.len], prefix);
    std.mem.writeInt(u64, bytes[prefix.len..][0..8], group, .big);
    return bytes;
}

test "catalog object annotations retain separate index constraint identities and reject name reuse ABA after cold empty-directory evolution" {
    const a = std.testing.allocator;
    const index: Declaration = .{ .key = .{ .kind = .index, .name = "same_name" }, .signature = @splat(1) };
    const constraint: Declaration = .{ .key = .{ .kind = .constraint, .name = "same_name" }, .signature = @splat(2) };
    var created = try prepare(a, 7, null, @splat(1), &.{ index, constraint });
    defer created.deinit();
    const index_id = created.state.byKey(index.key).?.id;
    const constraint_id = created.state.byKey(constraint.key).?.id;
    try std.testing.expect(index_id != constraint_id);
    var reordered = try prepare(a, 7, created.state, @splat(2), &.{ constraint, index });
    defer reordered.deinit();
    try std.testing.expectEqual(index_id, reordered.state.byKey(index.key).?.id);
    try std.testing.expectEqual(constraint_id, reordered.state.byKey(constraint.key).?.id);
    try std.testing.expectEqual(@as(usize, 0), reordered.retire_annotations.len);
    var dropped = try prepare(a, 7, reordered.state, @splat(3), &.{});
    defer dropped.deinit();
    try std.testing.expectEqual(@as(usize, 2), dropped.retire_annotations.len);
    const bytes = try dropped.state.encode(a);
    defer a.free(bytes);
    var cold = try State.decode(a, bytes);
    defer cold.deinit();
    var recreated = try prepare(a, 7, cold.value, @splat(1), &.{index});
    defer recreated.deinit();
    try std.testing.expect(recreated.state.byKey(index.key).?.id > constraint_id);
    try std.testing.expect(recreated.state.byId(index_id) == null);
    try std.testing.expect(recreated.state.epoch.revision > created.state.epoch.revision);
    try std.testing.expectEqualSlices(u8, &created.state.epoch.definition_digest, &recreated.state.epoch.definition_digest);
}

test "catalog object annotations retire overrides only on object removal or explicit inline-description replacement" {
    const a = std.testing.allocator;
    const original: Declaration = .{ .key = .{ .kind = .index, .name = "lookup" }, .signature = @splat(1), .declared_description = "inline" };
    var first = try prepare(a, 7, null, @splat(1), &.{original});
    defer first.deinit();
    var changed = original;
    changed.signature = @splat(2);
    var altered = try prepare(a, 7, first.state, @splat(2), &.{changed});
    defer altered.deinit();
    try std.testing.expectEqual(@as(usize, 0), altered.retire_annotations.len);
    try std.testing.expectEqual(first.state.objects[0].id, altered.state.objects[0].id);
    changed.declared_description = "replacement";
    var replacement = try prepare(a, 7, altered.state, @splat(3), &.{changed});
    defer replacement.deinit();
    try std.testing.expectEqualSlices(u64, &.{first.state.objects[0].id}, replacement.retire_annotations);
    try std.testing.expectError(error.InvalidCatalogRecord, prepare(a, 7, altered.state, @splat(2), &.{changed}));
    try std.testing.expectError(error.InvalidCatalogMutation, prepare(a, 7, null, @splat(1), &.{ original, original }));
}

test "catalog object annotations point keys and explicit-null records cannot cross table object or group scope" {
    const a = std.testing.allocator;
    const directory = try directoryKey(41, 7);
    const note = try annotationKey(41, 7, 2);
    try std.testing.expectEqualDeep(Scope{ .group = 41, .table = 7 }, try scopeFromKey(&directory, false));
    try std.testing.expectEqualDeep(Scope{ .group = 41, .table = 7, .object = 2 }, try scopeFromKey(&note, true));
    try std.testing.expect(!std.mem.eql(u8, &note, &try annotationKey(41, 8, 2)));
    try std.testing.expect(!std.mem.eql(u8, &note, &try annotationKey(42, 7, 2)));
    try std.testing.expectError(error.InvalidCatalogRecord, scopeFromKey(note[0 .. note.len - 1], true));
    const cleared: Annotation = .{ .format = 1, .table_id = 7, .object_id = 2, .description = null };
    const encoded = try cleared.encode(a);
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(Annotation, a, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(cleared, parsed.value);
    try std.testing.expectError(error.InvalidCatalogRecord, (Annotation{ .format = 1, .table_id = 7, .object_id = 2, .description = "" }).validate());
    try std.testing.expectEqualDeep(descriptionDigest(null), descriptionDigest(""));
    const empty: State = .{ .format = 1, .table_id = 7, .epoch = .{ .revision = 1, .definition_digest = @splat(1) }, .next_id = 1, .objects = &.{} };
    const canonical = try empty.encode(a);
    defer a.free(canonical);
    const unversioned = try std.mem.replaceOwned(u8, a, canonical, "\"format\":1,", "");
    defer a.free(unversioned);
    try std.testing.expectError(error.MissingField, State.decode(a, unversioned));
}

fn checkPreparationFailures(a: A) !void {
    var created = try prepare(a, 7, null, @splat(1), &.{.{ .key = .{ .kind = .constraint, .name = "status_check" }, .signature = @splat(2) }});
    defer created.deinit();
    const bytes = try created.state.encode(a);
    defer a.free(bytes);
    var decoded = try State.decode(a, bytes);
    defer decoded.deinit();
    var removed = try prepare(a, 7, decoded.value, @splat(3), &.{});
    defer removed.deinit();
}
test "catalog object annotations directory preparation and cold decoding release every allocation on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkPreparationFailures, .{});
}

test "catalog object annotations verified renames preserve point-annotation identity without accepting replacement or duplicated ownership" {
    const a = std.testing.allocator;
    const old: Declaration = .{ .key = .{ .kind = .constraint, .name = "old_check" }, .signature = @splat(1) };
    var created = try prepare(a, 7, null, @splat(1), &.{old});
    defer created.deinit();
    var next = old;
    next.key.name = "new_check";
    const rename: Rename = .{ .kind = .constraint, .from = "old_check", .to = "new_check" };
    var renamed = try prepareWithRenames(a, 7, created.state, @splat(2), &.{next}, &.{rename});
    defer renamed.deinit();
    try std.testing.expectEqual(created.state.objects[0].id, renamed.state.objects[0].id);
    try std.testing.expectEqual(@as(usize, 0), renamed.retire_annotations.len);
    try std.testing.expectEqual(created.state.next_id, renamed.state.next_id);
    try std.testing.expectError(error.InvalidCatalogMutation, prepareWithRenames(a, 7, created.state, @splat(2), &.{ old, next }, &.{rename}));
    try std.testing.expectError(error.InvalidCatalogMutation, prepareWithRenames(a, 7, created.state, @splat(2), &.{next}, &.{ rename, rename }));
    next.signature = @splat(2);
    try std.testing.expectError(error.InvalidCatalogMutation, prepareWithRenames(a, 7, created.state, @splat(2), &.{next}, &.{rename}));
}

pub const max_index_description_bytes = 16 * 1024 * 1024;
pub const IndexDescriptionsRead = struct {
    table_id: u64,
    name: ?[]const u8 = null,

    pub fn validate(self: @This()) !void {
        if (self.table_id == 0) return error.InvalidCatalogMutation;
        if (self.name) |name| try (Key{ .kind = .index, .name = name }).validate();
    }
};
pub const IndexDescription = struct { name: []const u8, description: ?[]const u8 };
pub const IndexDescriptions = struct {
    table_id: u64,
    definition_digest: [32]u8,
    entries: []const IndexDescription,
};
