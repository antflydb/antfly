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

//! Typed column kernels for relocation evidence. Dictionary IDs and physical
//! encodings are not identities. Only JSON/encoded values require interpretation.
const std = @import("std");
const rows = @import("antfly_local_sources").storage_rowsource_types;
const A = std.mem.Allocator;
const Hash = std.crypto.hash.Blake3;
const Digest = [32]u8;
pub const format: u16 = 2;
fn bytes(hash: *Hash, value: []const u8) void {
    word(hash, value.len);
    hash.update(value);
}
fn word(hash: *Hash, value: u64) void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .big);
    hash.update(&encoded);
}
fn json(hash: *Hash, value: std.json.Value) void {
    bytes(hash, @tagName(value));
    switch (value) {
        .null => {},
        .bool => |v| hash.update(&.{@intFromBool(v)}),
        .integer => |v| word(hash, @bitCast(v)),
        .float => |v| word(hash, @bitCast(v)),
        .string, .number_string => |v| bytes(hash, v),
        .array => |v| {
            word(hash, v.items.len);
            for (v.items) |item| json(hash, item);
        },
        .object => |v| {
            word(hash, v.count());
            var iterator = v.iterator();
            while (iterator.next()) |entry| {
                bytes(hash, entry.key_ptr.*);
                json(hash, entry.value_ptr.*);
            }
        },
    }
}
const Column = struct {
    vector: rows.ColumnVector,
    domain: Digest,
    // Fixed-size semantic memo avoids an unbounded array for a borrowed large
    // dictionary. Collisions merely recompute; cached values never cross columns.
    dictionary: [16]?struct { id: u32, value: Digest } = @splat(null),
    fn value(self: *Column, a: A, row: usize) !Digest {
        const id: ?u32 = switch (self.vector.values) {
            .dictionary_bytes => |v| v.indices[row],
            .dictionary_i64 => |v| v.indices[row],
            .dictionary_f64 => |v| v.indices[row],
            else => null,
        };
        if (id) |index| if (self.dictionary[index % self.dictionary.len]) |cached| {
            if (cached.id == index) return cached.value;
        };
        var hash = Hash.init(.{});
        switch (self.vector.values) {
            .bytes => |v| json(&hash, .{ .string = v[row] }),
            .dictionary_bytes => |v| json(&hash, .{ .string = v.at(row) }),
            .i64 => |v| json(&hash, .{ .integer = v[row] }),
            .dictionary_i64 => |v| json(&hash, .{ .integer = v.at(row) }),
            .f64 => |v| json(&hash, .{ .float = v[row] }),
            .dictionary_f64 => |v| json(&hash, .{ .float = v.at(row) }),
            .bool => |v| json(&hash, .{ .bool = v[row] }),
            .decimal128 => |v| {
                bytes(&hash, "decimal128");
                word(&hash, @bitCast(@as(i64, v.precision)));
                word(&hash, @bitCast(@as(i64, v.scale)));
                var coefficient: [16]u8 = undefined;
                std.mem.writeInt(u128, &coefficient, @bitCast(v.values[row]), .big);
                hash.update(&coefficient);
            },
            .vector_f32 => |v| {
                bytes(&hash, "vector_f32");
                word(&hash, v[row].len);
                for (v[row]) |item| {
                    var bits: [4]u8 = undefined;
                    std.mem.writeInt(u32, &bits, @bitCast(item), .big);
                    hash.update(&bits);
                }
            },
            .json => |v| json(&hash, try std.json.parseFromSliceLeaky(std.json.Value, a, v[row], .{ .parse_numbers = false })),
            .encoded => |v| json(&hash, try v.at(row)),
        }
        var result: Digest = undefined;
        hash.final(&result);
        if (id) |index| self.dictionary[index % self.dictionary.len] = .{ .id = index, .value = result };
        return result;
    }
};
pub const Plan = struct {
    a: A,
    base: Hash,
    columns: []Column,
    count: usize,
    pub fn bind(a: A, recipe: Digest, batch: rows.ColumnBatch, names: []const []const u8) !Plan {
        const columns = try a.alloc(Column, names.len);
        errdefer a.free(columns);
        for (columns, names) |*column, name| {
            var hash = Hash.init(.{});
            bytes(&hash, name);
            var domain: Digest = undefined;
            hash.final(&domain);
            column.* = .{ .vector = batch.findColumn(name) orelse return error.InvalidLogicalRowIdentity, .domain = domain };
        }
        var base = Hash.init(.{});
        base.update("antfly.logical-row.typed-values.v2");
        base.update(&recipe);
        word(&base, names.len);
        return .{ .a = a, .base = base, .columns = columns, .count = batch.rowCount() };
    }
    pub fn deinit(self: *Plan) void {
        self.a.free(self.columns);
    }
    /// A bounded tile of independent row hashes; columns are bound once and
    /// repeated native/dictionary values never allocate or stringify JSON.
    pub fn fill(self: *Plan, scratch: A, begin: usize, result: []Digest) !void {
        if (result.len > 64 or begin > self.count or result.len > self.count - begin) return error.InvalidLogicalRowIdentity;
        var hashes: [64]Hash = undefined;
        for (hashes[0..result.len]) |*hash| hash.* = self.base;
        var arena = std.heap.ArenaAllocator.init(scratch);
        defer arena.deinit();
        for (self.columns) |*column| {
            for (hashes[0..result.len], begin..) |*hash, row| {
                hash.update(&column.domain);
                const absent = column.vector.nulls.isNull(row);
                hash.update(&.{@intFromBool(absent)});
                if (!absent) {
                    _ = arena.reset(.retain_capacity);
                    hash.update(&try column.value(arena.allocator(), row));
                }
            }
        }
        for (hashes[0..result.len], result) |*hash, *digest| hash.final(digest);
    }
};

test "external lake typed fingerprints preserve dictionary semantics exact integers and null domains" {
    const a = std.testing.allocator;
    const refs = [_]rows.RowRef{ .{ .relational_key = "a" }, .{ .relational_key = "b" } };
    var batch: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &.{
        .{ .name = "id", .values = .{ .i64 = &.{ 9007199254740993, 9007199254740992 } } },
        .{ .name = "body", .values = .{ .bytes = &.{ "same", "same" } } },
    } };
    var plain = try Plan.bind(a, @splat(1), batch, &.{ "id", "body" });
    defer plain.deinit();
    var expected: [2]Digest = undefined;
    try plain.fill(a, 0, &expected);
    try std.testing.expect(!std.mem.eql(u8, &expected[0], &expected[1]));
    batch.columns = &.{
        .{ .name = "id", .values = .{ .dictionary_i64 = .{ .values = &.{ 9007199254740992, 9007199254740993 }, .indices = &.{ 1, 0 } } } },
        .{ .name = "body", .values = .{ .dictionary_bytes = .{ .values = &.{ "unused", "same" }, .indices = &.{ 1, 1 } } } },
    };
    var dictionary = try Plan.bind(a, @splat(1), batch, &.{ "id", "body" });
    defer dictionary.deinit();
    var actual: [2]Digest = undefined;
    try dictionary.fill(a, 0, &actual);
    try std.testing.expectEqualDeep(expected, actual);
    batch.columns = &.{.{ .name = "body", .nulls = .{ .bytes = &.{ 1, 0 } }, .values = .{ .json = &.{ "null", "null" } } }};
    var nulls = try Plan.bind(a, @splat(1), batch, &.{"body"});
    defer nulls.deinit();
    try nulls.fill(a, 0, &actual);
    try std.testing.expect(!std.mem.eql(u8, &actual[0], &actual[1]));
}
