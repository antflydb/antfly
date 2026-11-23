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

//! Catalog-owned attribute identities. Names and type signatures may change;
//! IDs never do. Dropped entries need no tombstones: the durable high-water
//! counter prevents reuse while keeping storage bounded by the live schema.
//! Callers publish this state in the same transaction as the source schema.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_columns = 1024;
pub const max_name_bytes = 256;
pub const max_encoded_bytes = 1024 * 1024;
const prefix = "\x00\x00__metadata__:sql_column_identities:v1:";
pub const key_bytes = prefix.len + 16;
pub const Key = struct { group: u64, table: u64 };
pub const Epoch = struct { version: u32, digest: [32]u8 };
pub const Declaration = struct { name: []const u8, signature: [32]u8 };
pub const Rename = struct { from: []const u8, to: []const u8 };
pub const Column = struct {
    id: u64,
    name: []const u8,
    signature: [32]u8,
    description: ?[]const u8 = null,

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        // Unannotated columns keep their exact canonical representation.
        if (self.description) |description|
            try writer.write(.{ .id = self.id, .name = self.name, .signature = self.signature, .description = description })
        else
            try writer.write(.{ .id = self.id, .name = self.name, .signature = self.signature });
    }
};

pub const Description = struct {
    name: []const u8,
    value: ?[]const u8,

    pub fn validate(self: @This()) !void {
        if (!validName(self.name)) return error.InvalidCatalogMutation;
        if (self.value) |value| {
            if (value.len > max_encoded_bytes) return error.CatalogCommandTooLarge;
            if (!std.unicode.utf8ValidateSlice(value) or std.mem.indexOfScalar(u8, value, 0) != null) return error.InvalidCatalogMutation;
        }
    }
};

fn validName(name: []const u8) bool {
    return name.len != 0 and name.len <= max_name_bytes and std.mem.indexOfScalar(u8, name, 0) == null and std.unicode.utf8ValidateSlice(name);
}
fn validDigest(digest: [32]u8) bool {
    return !std.mem.allEqual(u8, &digest, 0);
}

pub const State = struct {
    format: u8,
    table_id: u64,
    epoch: Epoch,
    next_id: u64,
    columns: []const Column,

    pub fn validate(self: State, a: A) !void {
        if (self.format != 1 or self.table_id == 0 or !validDigest(self.epoch.digest) or self.next_id == 0 or self.columns.len > max_columns) return error.InvalidColumnIdentityState;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        var prior: u64 = 0;
        for (self.columns) |column| {
            if (column.id <= prior or column.id >= self.next_id or !validName(column.name) or !validDigest(column.signature)) return error.InvalidColumnIdentityState;
            if (column.description) |description| {
                if (description.len == 0 or description.len > max_encoded_bytes or !std.unicode.utf8ValidateSlice(description) or std.mem.indexOfScalar(u8, description, 0) != null) return error.InvalidColumnIdentityState;
            }
            if ((try names.getOrPut(a, column.name)).found_existing) return error.InvalidColumnIdentityState;
            prior = column.id;
        }
    }

    pub fn columnById(self: State, id: u64) ?Column {
        var low: usize = 0;
        var high = self.columns.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const value = self.columns[middle];
            if (value.id == id) return value;
            if (value.id < id) low = middle + 1 else high = middle;
        }
        return null;
    }

    pub fn requireDependency(self: State, id: u64, signature: [32]u8) !Column {
        const found = self.columnById(id) orelse return error.SqlViewColumnDropped;
        if (!std.mem.eql(u8, &found.signature, &signature)) return error.SqlViewColumnTypeChanged;
        return found;
    }

    /// Prepare the entire bounded successor before journal admission. The
    /// same path runs again at apply against the latest committed state.
    pub fn encodeDescription(self: State, a: A, id: u64, value: ?[]const u8) ![]u8 {
        const columns = try a.dupe(Column, self.columns);
        defer a.free(columns);
        var found = false;
        for (columns) |*column| if (column.id == id) {
            column.description = if (value) |text| if (text.len != 0) text else null else null;
            found = true;
            break;
        };
        if (!found) return error.UndefinedColumn;
        var updated = self;
        updated.columns = columns;
        return updated.encode(a);
    }

    pub fn encode(self: State, a: A) ![]u8 {
        try self.validate(a);
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_encoded_bytes) return error.ColumnIdentityLimitExceeded;
        return bytes;
    }

    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(State) {
        if (bytes.len > max_encoded_bytes) return error.ColumnIdentityLimitExceeded;
        var result = try std.json.parseFromSlice(State, a, bytes, .{ .allocate = .alloc_always });
        errdefer result.deinit();
        try result.value.validate(a);
        return result;
    }
};

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    value: State,
    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Publication/dependency guards belong to the caller's catalog transaction.
/// An absent predecessor is bootstrap, not a read-time allocation capability.
pub fn prepare(backing: A, table_id: u64, previous: ?State, epoch: Epoch, declarations: []const Declaration, renames: []const Rename) !Owned {
    // Native CREATE TABLE starts at schema version zero. Presence and the
    // nonzero schema digest distinguish that valid epoch from absent state.
    if (table_id == 0 or !validDigest(epoch.digest)) return error.InvalidColumnIdentityState;
    if (declarations.len > max_columns or renames.len > max_columns) return error.ColumnIdentityLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const a = arena.allocator();
    const old = previous orelse State{ .format = 1, .table_id = table_id, .epoch = epoch, .next_id = 1, .columns = &.{} };
    try old.validate(a);
    if (old.table_id != table_id or epoch.version < old.epoch.version or
        (epoch.version == old.epoch.version and !std.mem.eql(u8, &epoch.digest, &old.epoch.digest))) return error.ColumnIdentityEpochChanged;
    var old_names: std.StringHashMapUnmanaged(Column) = .empty;
    var declared: std.StringHashMapUnmanaged(void) = .empty;
    var renamed_to: std.StringHashMapUnmanaged(Column) = .empty;
    var renamed_from: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (old.columns) |column| try old_names.put(a, column.name, column);
    for (declarations) |declaration| {
        if (!validName(declaration.name) or !validDigest(declaration.signature)) return error.InvalidColumnIdentityState;
        if ((try declared.getOrPut(a, declaration.name)).found_existing) return error.InvalidColumnIdentityState;
    }
    for (renames) |rename| {
        if (!validName(rename.from) or !validName(rename.to) or std.mem.eql(u8, rename.from, rename.to) or !declared.contains(rename.to)) return error.InvalidColumnIdentityRename;
        const source = old_names.get(rename.from) orelse return error.InvalidColumnIdentityRename;
        if ((try renamed_from.getOrPut(a, source.id)).found_existing or (try renamed_to.getOrPut(a, rename.to)).found_existing) return error.InvalidColumnIdentityRename;
        try renamed_to.put(a, rename.to, source);
    }
    for (renames) |rename| if (old_names.get(rename.to)) |target| {
        if (!renamed_from.contains(target.id)) return error.InvalidColumnIdentityRename;
    };
    var next_id = old.next_id;
    const columns = try a.alloc(Column, declarations.len);
    for (declarations, columns) |declaration, *out| {
        const retained = renamed_to.get(declaration.name) orelse blk: {
            const found = old_names.get(declaration.name) orelse break :blk null;
            break :blk if (renamed_from.contains(found.id)) null else found;
        };
        const id = if (retained) |found| found.id else fresh: {
            const id = next_id;
            next_id = std.math.add(u64, next_id, 1) catch return error.ColumnIdentityExhausted;
            break :fresh id;
        };
        out.* = .{ .id = id, .name = try a.dupe(u8, declaration.name), .signature = declaration.signature, .description = if (retained) |found| if (found.description) |description| try a.dupe(u8, description) else null else null };
    }
    std.mem.sort(Column, columns, {}, struct {
        fn less(_: void, left: Column, right: Column) bool {
            return left.id < right.id;
        }
    }.less);
    const result: State = .{ .format = 1, .table_id = table_id, .epoch = epoch, .next_id = next_id, .columns = columns };
    try result.validate(a);
    if (previous != null and epoch.version == old.epoch.version) {
        if (old.next_id != result.next_id or old.columns.len != result.columns.len) return error.ColumnIdentityEpochChanged;
        for (old.columns, result.columns) |before, after| {
            if (before.id != after.id or !std.mem.eql(u8, before.name, after.name) or !std.mem.eql(u8, &before.signature, &after.signature)) return error.ColumnIdentityEpochChanged;
        }
    }
    return .{ .arena = arena, .value = result };
}

pub fn key(value: Key) ![key_bytes]u8 {
    if (value.group == 0 or value.table == 0) return error.InvalidColumnIdentityState;
    var result: [key_bytes]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    std.mem.writeInt(u64, result[prefix.len..][0..8], value.group, .big);
    std.mem.writeInt(u64, result[prefix.len + 8 ..][0..8], value.table, .big);
    return result;
}

pub fn decodeKey(bytes: []const u8) !?Key {
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    if (bytes.len != key_bytes) return error.InvalidColumnIdentityState;
    const value: Key = .{ .group = std.mem.readInt(u64, bytes[prefix.len..][0..8], .big), .table = std.mem.readInt(u64, bytes[prefix.len + 8 ..][0..8], .big) };
    if (value.group == 0 or value.table == 0) return error.InvalidColumnIdentityState;
    return value;
}

pub fn allGroupsPrefix() []const u8 {
    return prefix;
}

pub fn prefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0 or buffer.len < prefix.len + 8) return error.InvalidColumnIdentityState;
    @memcpy(buffer[0..prefix.len], prefix);
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], group, .big);
    return buffer[0 .. prefix.len + 8];
}

pub fn Store(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        group_id: u64,
        pub fn load(self: @This(), a: A, table_id: u64) !?std.json.Parsed(State) {
            const encoded = self.txn.get(&try key(.{ .group = self.group_id, .table = table_id })) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            var result = try State.decode(a, encoded);
            errdefer result.deinit();
            if (result.value.table_id != table_id) return error.InvalidColumnIdentityState;
            return result;
        }
        pub fn save(self: @This(), a: A, value: State) !void {
            const encoded = try value.encode(a);
            defer a.free(encoded);
            try self.txn.put(&try key(.{ .group = self.group_id, .table = value.table_id }), encoded);
        }
    };
}

test "SQL column identities admit native zero schema epochs without accepting absent or substituted state" {
    const a = std.testing.allocator;
    var initial = try prepare(a, 7, null, .{ .version = 0, .digest = @splat(1) }, &.{.{ .name = "id", .signature = @splat(2) }}, &.{});
    defer initial.deinit();
    const bytes = try initial.value.encode(a);
    defer a.free(bytes);
    var restored = try State.decode(a, bytes);
    defer restored.deinit();
    try std.testing.expectEqual(@as(u32, 0), restored.value.epoch.version);
    try std.testing.expectEqual(@as(u64, 1), (try restored.value.requireDependency(1, @splat(2))).id);
    try std.testing.expectError(error.InvalidColumnIdentityState, prepare(a, 7, null, .{ .version = 0, .digest = @splat(0) }, &.{}, &.{}));
    try std.testing.expectError(error.ColumnIdentityEpochChanged, prepare(a, 7, restored.value, .{ .version = 0, .digest = @splat(3) }, &.{}, &.{}));
    var next = try prepare(a, 7, restored.value, .{ .version = 1, .digest = @splat(3) }, &.{.{ .name = "id", .signature = @splat(2) }}, &.{});
    defer next.deinit();
    try std.testing.expectEqual(@as(u64, 1), next.value.columns[0].id);
}

test "SQL column identities retain rename and reorder while dropped names never recycle IDs" {
    const a = std.testing.allocator;
    var first = try prepare(a, 7, null, .{ .version = 1, .digest = @splat(1) }, &.{ .{ .name = "id", .signature = @splat(1) }, .{ .name = "status", .signature = @splat(2) } }, &.{});
    defer first.deinit();
    var renamed = try prepare(a, 7, first.value, .{ .version = 2, .digest = @splat(2) }, &.{ .{ .name = "state", .signature = @splat(2) }, .{ .name = "id", .signature = @splat(1) } }, &.{.{ .from = "status", .to = "state" }});
    defer renamed.deinit();
    try std.testing.expectEqualStrings("state", (try renamed.value.requireDependency(2, @splat(2))).name);
    try std.testing.expectEqual(@as(u64, 3), renamed.value.next_id);
    var dropped = try prepare(a, 7, renamed.value, .{ .version = 3, .digest = @splat(3) }, &.{.{ .name = "id", .signature = @splat(1) }}, &.{});
    defer dropped.deinit();
    try std.testing.expectError(error.SqlViewColumnDropped, dropped.value.requireDependency(2, @splat(2)));
    var recreated = try prepare(a, 7, dropped.value, .{ .version = 4, .digest = @splat(4) }, &.{ .{ .name = "status", .signature = @splat(2) }, .{ .name = "id", .signature = @splat(1) } }, &.{});
    defer recreated.deinit();
    try std.testing.expectEqual(@as(u64, 3), recreated.value.columns[1].id);
    try std.testing.expectError(error.SqlViewColumnDropped, recreated.value.requireDependency(2, @splat(2)));
    try std.testing.expectEqual(@as(usize, 2), recreated.value.columns.len);
    const encoded = try recreated.value.encode(a);
    defer a.free(encoded);
    var recovered = try State.decode(a, encoded);
    defer recovered.deinit();
    try std.testing.expectEqualStrings("status", (try recovered.value.requireDependency(3, @splat(2))).name);
}

test "SQL column identities reject epoch substitution rename overwrite type drift and exhausted IDs" {
    const a = std.testing.allocator;
    var initial = try prepare(a, 7, null, .{ .version = 1, .digest = @splat(1) }, &.{ .{ .name = "a", .signature = @splat(1) }, .{ .name = "b", .signature = @splat(2) } }, &.{});
    defer initial.deinit();
    try std.testing.expectError(error.ColumnIdentityEpochChanged, prepare(a, 8, initial.value, .{ .version = 2, .digest = @splat(2) }, &.{}, &.{}));
    try std.testing.expectError(error.ColumnIdentityEpochChanged, prepare(a, 7, initial.value, .{ .version = 1, .digest = @splat(2) }, &.{}, &.{}));
    try std.testing.expectError(error.InvalidColumnIdentityRename, prepare(a, 7, initial.value, .{ .version = 2, .digest = @splat(2) }, &.{.{ .name = "b", .signature = @splat(1) }}, &.{.{ .from = "a", .to = "b" }}));
    var changed = try prepare(a, 7, initial.value, .{ .version = 2, .digest = @splat(2) }, &.{ .{ .name = "a", .signature = @splat(3) }, .{ .name = "b", .signature = @splat(2) } }, &.{});
    defer changed.deinit();
    try std.testing.expectError(error.SqlViewColumnTypeChanged, changed.value.requireDependency(1, @splat(1)));
    var exhausted = initial.value;
    exhausted.next_id = std.math.maxInt(u64);
    try std.testing.expectError(error.ColumnIdentityExhausted, prepare(a, 7, exhausted, .{ .version = 2, .digest = @splat(2) }, &.{.{ .name = "fresh", .signature = @splat(1) }}, &.{}));
    const bytes = try key(.{ .group = 3, .table = 7 });
    try std.testing.expectEqualDeep(Key{ .group = 3, .table = 7 }, (try decodeKey(&bytes)).?);
    try std.testing.expectError(error.InvalidColumnIdentityState, decodeKey(bytes[0 .. bytes.len - 1]));
}

test "SQL column identities bound live storage after repeated drop and recreate churn" {
    const T = struct {
        fn epoch(version: u32) Epoch {
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, version, .big);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(&bytes, &digest, .{});
            return .{ .version = version, .digest = digest };
        }
    };
    const a = std.testing.allocator;
    const columns = [_]Declaration{.{ .name = "reused", .signature = @splat(1) }};
    var current = try prepare(a, 7, null, T.epoch(1), &columns, &.{});
    defer current.deinit();
    for (0..128) |iteration| {
        const prior_id = current.value.columns[0].id;
        const version: u32 = @intCast(iteration * 2 + 2);
        var dropped = try prepare(a, 7, current.value, T.epoch(version), &.{}, &.{});
        defer dropped.deinit();
        const next = try prepare(a, 7, dropped.value, T.epoch(version + 1), &columns, &.{});
        current.deinit();
        current = next;
        try std.testing.expectEqual(@as(usize, 1), current.value.columns.len);
        try std.testing.expect(current.value.columns[0].id > prior_id);
        try std.testing.expectError(error.SqlViewColumnDropped, current.value.requireDependency(prior_id, @splat(1)));
        const bytes = try current.value.encode(a);
        defer a.free(bytes);
        try std.testing.expect(bytes.len < 1024);
    }
}

test "SQL column identities require explicit format and high water and reject same epoch semantic changes" {
    const a = std.testing.allocator;
    const epoch: Epoch = .{ .version = 1, .digest = @splat(1) };
    const no_format = try std.json.Stringify.valueAlloc(a, .{ .table_id = @as(u64, 7), .epoch = epoch, .next_id = @as(u64, 1), .columns = [_]Column{} }, .{});
    defer a.free(no_format);
    try std.testing.expectError(error.MissingField, State.decode(a, no_format));
    const no_counter = try std.json.Stringify.valueAlloc(a, .{ .format = @as(u8, 1), .table_id = @as(u64, 7), .epoch = epoch, .columns = [_]Column{} }, .{});
    defer a.free(no_counter);
    try std.testing.expectError(error.MissingField, State.decode(a, no_counter));
    var state = try prepare(a, 7, null, epoch, &.{.{ .name = "a", .signature = @splat(1) }}, &.{});
    defer state.deinit();
    try std.testing.expectError(error.ColumnIdentityEpochChanged, prepare(a, 7, state.value, epoch, &.{.{ .name = "a", .signature = @splat(2) }}, &.{}));
    try std.testing.expectError(error.ColumnIdentityEpochChanged, prepare(a, 7, state.value, epoch, &.{}, &.{}));
}

fn allocationCase(a: A) !void {
    var initial = try prepare(a, 7, null, .{ .version = 1, .digest = @splat(1) }, &.{ .{ .name = "a", .signature = @splat(1) }, .{ .name = "b", .signature = @splat(2) } }, &.{});
    defer initial.deinit();
    var changed = try prepare(a, 7, initial.value, .{ .version = 2, .digest = @splat(2) }, &.{ .{ .name = "renamed", .signature = @splat(2) }, .{ .name = "c", .signature = @splat(3) } }, &.{.{ .from = "b", .to = "renamed" }});
    defer changed.deinit();
    const bytes = try changed.value.encode(a);
    defer a.free(bytes);
    var decoded = try State.decode(a, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualStrings("renamed", (try decoded.value.requireDependency(2, @splat(2))).name);
}

test "SQL column identities release preparation and decode allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}

test "SQL column comments follow stable identities through rename and never survive drop reuse" {
    const a = std.testing.allocator;
    const original: State = .{ .format = 1, .table_id = 7, .epoch = .{ .version = 1, .digest = @splat(1) }, .next_id = 3, .columns = &.{ .{ .id = 1, .name = "id", .signature = @splat(2) }, .{ .id = 2, .name = "status", .signature = @splat(3), .description = "application state" } } };
    const encoded = try original.encode(a);
    defer a.free(encoded);
    var restored = try State.decode(a, encoded);
    defer restored.deinit();
    try std.testing.expectEqualStrings("application state", restored.value.columnById(2).?.description.?);
    var renamed = try prepare(a, 7, restored.value, .{ .version = 2, .digest = @splat(4) }, &.{ .{ .name = "id", .signature = @splat(2) }, .{ .name = "renamed", .signature = @splat(3) } }, &.{.{ .from = "status", .to = "renamed" }});
    defer renamed.deinit();
    try std.testing.expectEqualStrings("application state", renamed.value.columnById(2).?.description.?);
    var dropped = try prepare(a, 7, renamed.value, .{ .version = 3, .digest = @splat(5) }, &.{.{ .name = "id", .signature = @splat(2) }}, &.{});
    defer dropped.deinit();
    var recreated = try prepare(a, 7, dropped.value, .{ .version = 4, .digest = @splat(6) }, &.{ .{ .name = "id", .signature = @splat(2) }, .{ .name = "status", .signature = @splat(3) } }, &.{});
    defer recreated.deinit();
    try std.testing.expect(recreated.value.columnById(2) == null);
    try std.testing.expect(recreated.value.columnById(3).?.description == null);
    try std.testing.expectEqual(@as(usize, 2), recreated.value.columns.len);
}

test "SQL column comments bound aggregate encoded size without mutating the predecessor" {
    const a = std.testing.allocator;
    const original: State = .{ .format = 1, .table_id = 7, .epoch = .{ .version = 1, .digest = @splat(1) }, .next_id = 3, .columns = &.{ .{ .id = 1, .name = "a", .signature = @splat(2) }, .{ .id = 2, .name = "b", .signature = @splat(3) } } };
    const text = try a.alloc(u8, max_encoded_bytes / 2);
    defer a.free(text);
    @memset(text, 'x');
    const first = try original.encodeDescription(a, 1, text);
    defer a.free(first);
    var current = try State.decode(a, first);
    defer current.deinit();
    try std.testing.expectError(error.ColumnIdentityLimitExceeded, current.value.encodeDescription(a, 2, text));
    try std.testing.expect(current.value.columns[1].description == null);
    try std.testing.expect(original.columns[0].description == null);
    const removed = try current.value.encodeDescription(a, 1, "");
    defer a.free(removed);
    const initial = try original.encode(a);
    defer a.free(initial);
    try std.testing.expectEqualStrings(initial, removed);
}
