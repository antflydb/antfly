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

//! Attribute-to-sequence references, distinct from sequence OWNED BY edges.
//! Prepared changes share the table transaction; unchanged bindings write nothing.
const std = @import("std");
const local = @import("antfly_local_sources");
const bindings = local.system_catalog_column_sequences;
const A = std.mem.Allocator;
pub const max_references = local.system_catalog_column_identities.max_columns;
const width = 41;
const magic = "ASDR01";

pub const Reference = struct {
    column_id: u64,
    sequence_id: u64,
    namespace_id: u64,
    incarnation: [16]u8,
    generation: bindings.Generation,

    pub fn fromBinding(value: bindings.Binding) Reference {
        return .{ .column_id = value.owner.column_id, .sequence_id = value.sequence_id, .namespace_id = value.namespace_id, .incarnation = value.incarnation, .generation = value.generation };
    }
    pub fn fingerprint(self: Reference, table: u64) [32]u8 {
        return (bindings.Binding{ .owner = .{ .table_id = table, .column_id = self.column_id }, .sequence_id = self.sequence_id, .namespace_id = self.namespace_id, .incarnation = self.incarnation, .generation = self.generation, .target = .{ .table = "_reference" } }).fingerprint();
    }
    pub fn validate(self: Reference) !void {
        if (self.column_id == 0 or self.sequence_id == 0 or self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSequenceDefaultReferences;
    }
};

pub fn tableKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    if (table == 0) return error.InvalidSequenceDefaultReferences;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sequence-default-table:{d}", .{ group, table });
}
pub fn dependentPrefix(buffer: []u8, group: u64, sequence: u64) ![]const u8 {
    if (sequence == 0) return error.InvalidSequenceDefaultReferences;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sequence-default-dependent:{d}:", .{ group, sequence });
}
pub fn edgeKey(buffer: []u8, group: u64, sequence: u64, table: u64, column: u64) ![]const u8 {
    if (table == 0 or column == 0) return error.InvalidSequenceDefaultReferences;
    const prefix = try dependentPrefix(buffer, group, sequence);
    if (buffer.len - prefix.len < 16) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], table, .big);
    std.mem.writeInt(u64, buffer[prefix.len + 8 ..][0..8], column, .big);
    return buffer[0 .. prefix.len + 16];
}

fn rootPrefix(buffer: []u8, group: u64, lane: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sequence-default-{s}:", .{ group, lane });
}
fn number(bytes: []const u8) !u64 {
    if (bytes.len == 0 or bytes[0] == '0') return error.InvalidSequenceDefaultReferences;
    for (bytes) |byte| if (byte < '0' or byte > '9') return error.InvalidSequenceDefaultReferences;
    return std.fmt.parseInt(u64, bytes, 10) catch error.InvalidSequenceDefaultReferences;
}
pub fn tableFromKey(bytes: []const u8, group: u64) !?u64 {
    var buffer: [192]u8 = undefined;
    const prefix = try rootPrefix(&buffer, group, "table");
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    return try number(bytes[prefix.len..]);
}
pub const Edge = struct { sequence: u64, table: u64, column: u64 };
pub fn edgeFromKey(bytes: []const u8, group: u64) !?Edge {
    var buffer: [192]u8 = undefined;
    const prefix = try rootPrefix(&buffer, group, "dependent");
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    const tail = bytes[prefix.len..];
    const end = std.mem.indexOfScalar(u8, tail, ':') orelse return error.InvalidSequenceDefaultReferences;
    if (tail.len != end + 17) return error.InvalidSequenceDefaultReferences;
    const sequence = try number(tail[0..end]);
    const table = std.mem.readInt(u64, tail[end + 1 ..][0..8], .big);
    const column = std.mem.readInt(u64, tail[end + 9 ..][0..8], .big);
    if (table == 0 or column == 0) return error.InvalidSequenceDefaultReferences;
    return .{ .sequence = sequence, .table = table, .column = column };
}
/// A reverse-edge check needs one indexed forward entry, not a decoded copy of
/// every default in that table. Complete frame validation runs on forward rows.
pub fn findEncoded(bytes: []const u8, column: u64) !?Reference {
    if (bytes.len < magic.len + 4 or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidSequenceDefaultReferences;
    const count = std.mem.readInt(u32, bytes[magic.len..][0..4], .little);
    if (count == 0 or count > max_references or bytes.len != magic.len + 4 + @as(usize, count) * width) return error.InvalidSequenceDefaultReferences;
    var low: usize = 0;
    var high: usize = count;
    while (low < high) {
        const middle = low + (high - low) / 2;
        const row = bytes[magic.len + 4 + middle * width ..][0..width];
        const id = std.mem.readInt(u64, row[0..8], .little);
        if (id < column) {
            low = middle + 1;
            continue;
        }
        if (id > column) {
            high = middle;
            continue;
        }
        const result: Reference = .{ .column_id = id, .sequence_id = std.mem.readInt(u64, row[8..16], .little), .namespace_id = std.mem.readInt(u64, row[16..24], .little), .incarnation = row[24..40].*, .generation = switch (row[40]) {
            0 => .serial,
            1 => .always,
            2 => .by_default,
            else => return error.InvalidSequenceDefaultReferences,
        } };
        try result.validate();
        return result;
    }
    return null;
}

pub fn summaryKey(buffer: []u8, group: u64, sequence: u64) ![]const u8 {
    if (sequence == 0) return error.InvalidSequenceDefaultReferences;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sequence-default-summary:{d}", .{ group, sequence });
}
pub fn summaryFromKey(bytes: []const u8, group: u64) !?u64 {
    var buffer: [192]u8 = undefined;
    const prefix = try rootPrefix(&buffer, group, "summary");
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    return try number(bytes[prefix.len..]);
}
pub const Stamp = struct {
    revision: u64 = 0,
    count: u64 = 0,
    pub const encoded_len = 22;
    pub fn encode(self: Stamp) ![encoded_len]u8 {
        if (self.revision == 0) return error.InvalidSequenceDefaultReferences;
        var bytes: [encoded_len]u8 = undefined;
        bytes[0..6].* = "ASDS01".*;
        std.mem.writeInt(u64, bytes[6..14], self.revision, .little);
        std.mem.writeInt(u64, bytes[14..22], self.count, .little);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Stamp {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..6], "ASDS01")) return error.InvalidSequenceDefaultReferences;
        const value: Stamp = .{ .revision = std.mem.readInt(u64, bytes[6..14], .little), .count = std.mem.readInt(u64, bytes[14..22], .little) };
        if (value.revision == 0) return error.InvalidSequenceDefaultReferences;
        return value;
    }
    fn changed(self: Stamp, removed: u32, added: u32) !Stamp {
        if (self.count < removed) return error.InvalidSequenceDefaultReferences;
        return .{
            .revision = std.math.add(u64, self.revision, 1) catch return error.CatalogRevisionExhausted,
            .count = std.math.add(u64, self.count - removed, added) catch return error.InvalidSequenceDefaultReferences,
        };
    }
};
pub fn stamp(txn: anytype, group: u64, sequence: u64) !Stamp {
    var buffer: [192]u8 = undefined;
    const bytes = txn.get(try summaryKey(&buffer, group, sequence)) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    return Stamp.decode(bytes);
}
/// A zero-count tombstone prevents ABA while the sequence exists. Resource
/// deletion may reclaim it because catalog IDs are never reused.
pub fn removeUnusedSummary(txn: anytype, group: u64, sequence: u64) !void {
    const current = try stamp(txn, group, sequence);
    // Resource deletion must never strand a live reference. Admission reports
    // SQL dependency errors before writes; reaching this boundary is corruption
    // or a missing caller guard and must abort the entire transaction.
    if (current.count != 0 or try firstDependent(txn, group, sequence, null) != null) return error.InvalidSequenceDefaultReferences;
    if (current.revision == 0) return;
    var buffer: [192]u8 = undefined;
    try txn.delete(try summaryKey(&buffer, group, sequence));
}
const SummaryChange = struct { sequence: u64, removed: u32 = 0, added: u32 = 0, bytes: [Stamp.encoded_len]u8 = undefined };
fn referenceById(references: []const Reference, id: u64) ?Reference {
    var low: usize = 0;
    var high = references.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        if (references[middle].column_id == id) return references[middle];
        if (references[middle].column_id < id) low = middle + 1 else high = middle;
    }
    return null;
}
fn summaryChanges(a: A, before: []const Reference, after: []const Reference) ![]SummaryChange {
    var updates: std.AutoArrayHashMapUnmanaged(u64, SummaryChange) = .empty;
    defer updates.deinit(a);
    for (before) |old| {
        if (referenceById(after, old.column_id)) |next| if (std.meta.eql(old, next)) continue;
        const entry = try updates.getOrPut(a, old.sequence_id);
        if (!entry.found_existing) entry.value_ptr.* = .{ .sequence = old.sequence_id };
        entry.value_ptr.removed += 1;
    }
    for (after) |next| {
        if (referenceById(before, next.column_id)) |old| if (std.meta.eql(old, next)) continue;
        const entry = try updates.getOrPut(a, next.sequence_id);
        if (!entry.found_existing) entry.value_ptr.* = .{ .sequence = next.sequence_id };
        entry.value_ptr.added += 1;
    }
    return a.dupe(SummaryChange, updates.values());
}

fn validate(references: []const Reference) !void {
    if (references.len > max_references) return error.InvalidSequenceDefaultReferences;
    var previous: u64 = 0;
    for (references) |reference| {
        try reference.validate();
        if (reference.column_id <= previous) return error.InvalidSequenceDefaultReferences;
        previous = reference.column_id;
    }
}
fn mode(value: bindings.Generation) u8 {
    return switch (value) {
        .serial => 0,
        .always => 1,
        .by_default => 2,
    };
}
pub fn encode(a: A, references: []const Reference) ![]u8 {
    try validate(references);
    if (references.len == 0) return error.InvalidSequenceDefaultReferences;
    const bytes = try a.alloc(u8, magic.len + 4 + references.len * width);
    @memcpy(bytes[0..magic.len], magic);
    std.mem.writeInt(u32, bytes[magic.len..][0..4], @intCast(references.len), .little);
    for (references, 0..) |reference, i| {
        const row = bytes[magic.len + 4 + i * width ..][0..width];
        std.mem.writeInt(u64, row[0..8], reference.column_id, .little);
        std.mem.writeInt(u64, row[8..16], reference.sequence_id, .little);
        std.mem.writeInt(u64, row[16..24], reference.namespace_id, .little);
        row[24..40].* = reference.incarnation;
        row[40] = mode(reference.generation);
    }
    return bytes;
}
pub fn decode(a: A, bytes: []const u8) ![]Reference {
    if (bytes.len < magic.len + 4 or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidSequenceDefaultReferences;
    const count = std.mem.readInt(u32, bytes[magic.len..][0..4], .little);
    if (count == 0 or count > max_references or bytes.len != magic.len + 4 + @as(usize, count) * width) return error.InvalidSequenceDefaultReferences;
    const output = try a.alloc(Reference, count);
    errdefer a.free(output);
    for (output, 0..) |*reference, i| {
        const row = bytes[magic.len + 4 + i * width ..][0..width];
        reference.* = .{ .column_id = std.mem.readInt(u64, row[0..8], .little), .sequence_id = std.mem.readInt(u64, row[8..16], .little), .namespace_id = std.mem.readInt(u64, row[16..24], .little), .incarnation = row[24..40].*, .generation = switch (row[40]) {
            0 => .serial,
            1 => .always,
            2 => .by_default,
            else => return error.InvalidSequenceDefaultReferences,
        } };
    }
    try validate(output);
    return output;
}
pub fn load(a: A, txn: anytype, group: u64, table: u64) ![]Reference {
    var buffer: [192]u8 = undefined;
    const bytes = txn.get(try tableKey(&buffer, group, table)) catch |err| switch (err) {
        error.NotFound => return a.alloc(Reference, 0),
        else => return err,
    };
    return decode(a, bytes);
}
fn equal(left: []const Reference, right: []const Reference) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| if (!std.meta.eql(l, r)) return false;
    return true;
}

/// All allocation and semantic validation finish before the first journal write.
pub const Prepared = struct {
    alloc: A,
    table: u64,
    before: []Reference,
    after: []Reference,
    bytes: ?[]u8,
    summaries: []SummaryChange,
    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.before);
        self.alloc.free(self.after);
        self.alloc.free(self.summaries);
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn write(self: Prepared, txn: anytype, group: u64) !void {
        // Rebase on this transaction, including prior table participants in a
        // staged replacement. All point reads and checked arithmetic precede
        // this participant's first write; preparation owns every output byte.
        for (self.summaries) |*change| change.bytes = try (try (try stamp(txn, group, change.sequence)).changed(change.removed, change.added)).encode();
        var old: usize = 0;
        var next: usize = 0;
        while (old < self.before.len or next < self.after.len) {
            const remove = old < self.before.len and (next == self.after.len or self.before[old].column_id <= self.after[next].column_id);
            const add = next < self.after.len and (old == self.before.len or self.after[next].column_id <= self.before[old].column_id);
            if (remove and add and std.meta.eql(self.before[old], self.after[next])) {
                old += 1;
                next += 1;
                continue;
            }
            var buffer: [192]u8 = undefined;
            if (remove) {
                const item = self.before[old];
                try txn.delete(try edgeKey(&buffer, group, item.sequence_id, self.table, item.column_id));
            }
            if (add) {
                const item = self.after[next];
                try txn.put(try edgeKey(&buffer, group, item.sequence_id, self.table, item.column_id), &item.fingerprint(self.table));
            }
            if (remove) old += 1;
            if (add) next += 1;
        }
        var buffer: [192]u8 = undefined;
        const key = try tableKey(&buffer, group, self.table);
        if (self.bytes) |bytes| try txn.put(key, bytes) else try txn.delete(key);
        for (self.summaries) |change| try txn.put(try summaryKey(&buffer, group, change.sequence), &change.bytes);
    }
};
pub fn prepare(a: A, txn: anytype, group: u64, table: u64, references: []const Reference) !?Prepared {
    if (table == 0) return error.InvalidSequenceDefaultReferences;
    try validate(references);
    const before = try load(a, txn, group, table);
    errdefer a.free(before);
    if (equal(before, references)) {
        a.free(before);
        return null;
    }
    const after = try a.dupe(Reference, references);
    errdefer a.free(after);
    const bytes = if (after.len != 0) try encode(a, after) else null;
    errdefer if (bytes) |value| a.free(value);
    const summaries = try summaryChanges(a, before, after);
    errdefer a.free(summaries);
    for (summaries) |change| _ = try (try stamp(txn, group, change.sequence)).changed(change.removed, change.added);
    return .{ .alloc = a, .table = table, .before = before, .after = after, .bytes = bytes, .summaries = summaries };
}
pub fn synchronize(a: A, txn: anytype, group: u64, table: u64, references: []const Reference) !void {
    var prepared = (try prepare(a, txn, group, table, references)) orelse return;
    defer prepared.deinit();
    try prepared.write(txn, group);
}

/// Point lookup of a default's actual immutable destination attribute.
pub fn forColumn(txn: anytype, group: u64, table: u64, column: u64) !?Reference {
    var buffer: [192]u8 = undefined;
    const bytes = txn.get(try tableKey(&buffer, group, table)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return findEncoded(bytes, column);
}

pub const Dependent = struct { table: u64, column: u64, fingerprint: [32]u8 };
pub fn firstDependent(txn: anytype, group: u64, sequence: u64, excluding_table: ?u64) !?Dependent {
    var buffer: [192]u8 = undefined;
    const prefix = try dependentPrefix(&buffer, group, sequence);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) return null;
        if (item.key.len != prefix.len + 16 or item.value.len != 32) return error.InvalidSequenceDefaultReferences;
        const table = std.mem.readInt(u64, item.key[prefix.len..][0..8], .big);
        const column = std.mem.readInt(u64, item.key[prefix.len + 8 ..][0..8], .big);
        if (table == 0 or column == 0 or std.mem.allEqual(u8, item.value, 0)) return error.InvalidSequenceDefaultReferences;
        if (excluding_table) |excluded| if (excluded == table) continue;
        var forward_buffer: [192]u8 = undefined;
        const forward = txn.get(try tableKey(&forward_buffer, group, table)) catch |err| switch (err) {
            error.NotFound => return error.InvalidSequenceDefaultReferences,
            else => return err,
        };
        const reference = (try findEncoded(forward, column)) orelse return error.InvalidSequenceDefaultReferences;
        if (reference.sequence_id != sequence or !std.mem.eql(u8, item.value, &reference.fingerprint(table))) return error.InvalidSequenceDefaultReferences;
        return .{ .table = table, .column = column, .fingerprint = item.value[0..32].* };
    }
    return null;
}

/// Direct sequence DROP cannot remove a counter still used by a default.
/// Keep SQL nonadmission separate from malformed directory state so replicated
/// apply may persist a rejection without swallowing storage corruption.
pub fn requireUnused(txn: anytype, group: u64, sequence: u64) !void {
    const current = try stamp(txn, group, sequence);
    const dependent = try firstDependent(txn, group, sequence, null);
    if ((current.count == 0) != (dependent == null)) return error.InvalidSequenceDefaultReferences;
    if (dependent != null) return error.SqlDependentObjectsExist;
}

pub const Captured = struct {
    tables: []u64,
    stamp: Stamp,
    pub fn deinit(self: Captured, a: A) void {
        a.free(self.tables);
    }
};
/// Complete discovery outside the serialized apply section. Admission can
/// compare the transactional stamp with one point read, then rejoin the bounded
/// owner directory. This result grants no mutation or sequence privileges.
pub fn capture(a: A, txn: anytype, group: u64, sequence: u64, limit: usize, context: local.api_operation.RequestContext) !Captured {
    return captureExcluding(a, txn, group, sequence, limit, null, context);
}
/// Owning-table retirement excludes its own defaults from survivor capacity;
/// the complete counter stamp and validation still include those references.
pub fn captureExcluding(a: A, txn: anytype, group: u64, sequence: u64, limit: usize, excluded: ?u64, context: local.api_operation.RequestContext) !Captured {
    try context.ensureActive();
    if (limit == 0 or limit > 128 or (excluded != null and excluded.? == 0)) return error.SqlProgramLimitExceeded;
    const current = try stamp(txn, group, sequence);
    var buffer: [192]u8 = undefined;
    const prefix = try dependentPrefix(&buffer, group, sequence);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var tables: std.ArrayList(u64) = .empty;
    errdefer tables.deinit(a);
    var previous: u64 = 0;
    var count: u64 = 0;
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |entry| : (row = try cursor.next()) {
        try context.ensureActive();
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        const edge = (try edgeFromKey(entry.key, group)) orelse return error.InvalidSequenceDefaultReferences;
        if (entry.value.len != 32 or edge.table < previous) return error.InvalidSequenceDefaultReferences;
        if (edge.table != previous) {
            if (excluded == null or edge.table != excluded.?) {
                if (tables.items.len == limit) return error.SqlProgramLimitExceeded;
                try tables.append(a, edge.table);
            }
            previous = edge.table;
        }
        var forward_buffer: [192]u8 = undefined;
        const bytes = txn.get(try tableKey(&forward_buffer, group, edge.table)) catch |err| switch (err) {
            error.NotFound => return error.InvalidSequenceDefaultReferences,
            else => return err,
        };
        const reference = (try findEncoded(bytes, edge.column)) orelse return error.InvalidSequenceDefaultReferences;
        if (reference.sequence_id != sequence or !std.mem.eql(u8, entry.value, &reference.fingerprint(edge.table))) return error.InvalidSequenceDefaultReferences;
        count += 1;
        if (count > (128 + @as(usize, @intFromBool(excluded != null))) * max_references) return error.SqlProgramLimitExceeded;
    }
    if (count != current.count) return error.InvalidSequenceDefaultReferences;
    return .{ .tables = try tables.toOwnedSlice(a), .stamp = current };
}

/// Schema mutations use actual stable attributes, never a supplied ordinal.
pub fn fromSchema(a: A, table: u64, schema: std.json.Value, attributes: local.system_catalog_column_identities.State) ![]Reference {
    if (attributes.table_id != table or schema != .object) return error.InvalidSequenceDefaultReferences;
    try attributes.validate(a);
    const defaults = schema.object.get("column_defaults") orelse return a.alloc(Reference, 0);
    if (defaults != .array or defaults.array.items.len > max_references) return error.InvalidSequenceDefaultReferences;
    var output: std.ArrayList(Reference) = .empty;
    errdefer output.deinit(a);
    for (defaults.array.items) |entry| {
        if (entry != .object) return error.InvalidSequenceDefaultReferences;
        const value = entry.object.get("sequence") orelse continue;
        const name = entry.object.get("column") orelse return error.InvalidSequenceDefaultReferences;
        if (name != .string) return error.InvalidSequenceDefaultReferences;
        var binding = try bindings.Binding.parse(a, value);
        defer binding.deinit();
        const attribute = attributes.columnById(binding.value.owner.column_id) orelse return error.InvalidSequenceDefaultReferences;
        if (binding.value.owner.table_id != table or !std.mem.eql(u8, name.string, attribute.name)) return error.InvalidSequenceDefaultReferences;
        try output.append(a, Reference.fromBinding(binding.value));
    }
    std.mem.sort(Reference, output.items, {}, struct {
        fn less(_: void, left: Reference, right: Reference) bool {
            return left.column_id < right.column_id;
        }
    }.less);
    try validate(output.items);
    return output.toOwnedSlice(a);
}

test "system catalog creation sources default reference frames retain exact modes and reject corrupt identities" {
    const a = std.testing.allocator;
    const references = [_]Reference{
        .{ .column_id = 1, .sequence_id = 11, .namespace_id = 2, .incarnation = @splat(1), .generation = .serial },
        .{ .column_id = 3, .sequence_id = 11, .namespace_id = 2, .incarnation = @splat(1), .generation = .always },
    };
    const bytes = try encode(a, &references);
    defer a.free(bytes);
    const decoded = try decode(a, bytes);
    defer a.free(decoded);
    try std.testing.expect(std.meta.eql(references[0], decoded[0]) and std.meta.eql(references[1], decoded[1]));
    try std.testing.expect(!std.mem.eql(u8, &references[0].fingerprint(7), &references[1].fingerprint(7)));
    for (0..bytes.len) |end| try std.testing.expectError(error.InvalidSequenceDefaultReferences, decode(a, bytes[0..end]));
    const invalid = try a.dupe(u8, bytes);
    defer a.free(invalid);
    invalid[magic.len + 4 + width - 1] = 255;
    try std.testing.expectError(error.InvalidSequenceDefaultReferences, decode(a, invalid));
    try std.testing.expectError(error.InvalidSequenceDefaultReferences, encode(a, &.{ references[1], references[0] }));
}
