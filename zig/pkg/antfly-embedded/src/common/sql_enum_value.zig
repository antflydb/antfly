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

//! Immutable enum atoms shared by typed execution, physical row encoding and
//! ordered index preparation. Wire labels never substitute for type identity.
const std = @import("std");
const types = @import("../system_catalog/named_types.zig");
const A = std.mem.Allocator;
pub const encoded_size = 4;
pub const Value = struct {
    type_id: u64,
    origin_id: [16]u8,
    symbol: u32,
};

/// Borrows the immutable record. Both the record and this owner outlive every
/// returned atom. All atom/directory allocation occurs at view preparation.
pub const View = struct {
    record: types.Record,
    labels: types.View,
    symbols: []Value,

    pub fn init(a: A, record: types.Record) !View {
        try record.validateHeader();
        var labels = try types.View.init(a, record.definition);
        errdefer labels.deinit(a);
        const symbols = try a.alloc(Value, record.definition.labels.len);
        for (symbols, record.definition.labels) |*value, label| value.* = .{ .type_id = record.identity.id, .origin_id = record.identity.origin_id, .symbol = label.id };
        return .{ .record = record, .labels = labels, .symbols = symbols };
    }
    pub fn deinit(self: *View, a: A) void {
        a.free(self.symbols);
        self.labels.deinit(a);
        self.* = undefined;
    }
    pub fn input(self: *const View, text: []const u8) !*const Value {
        const label = self.labels.find(text) orelse return error.SqlInvalidTextRepresentation;
        return self.bySymbol(label.id);
    }
    pub fn bySymbol(self: *const View, id: u32) !*const Value {
        _ = try self.record.definition.byId(id);
        return &self.symbols[id - 1];
    }
    fn check(self: *const View, value: *const Value) !types.Label {
        if (value.type_id != self.record.identity.id or !std.mem.eql(u8, &value.origin_id, &self.record.identity.origin_id)) return error.SqlTypeMismatch;
        return self.record.definition.byId(value.symbol);
    }
    pub fn output(self: *const View, value: *const Value) ![]const u8 {
        return (try self.check(value)).text;
    }
    /// The pinned schema supplies the type; each row needs only its symbol ID.
    /// SQL NULL is handled by the row presence/null bitmap, never symbol zero.
    pub fn encode(self: *const View, value: *const Value) ![encoded_size]u8 {
        _ = try self.check(value);
        var bytes: [encoded_size]u8 = undefined;
        std.mem.writeInt(u32, &bytes, value.symbol, .little);
        return bytes;
    }
    pub fn decode(self: *const View, bytes: []const u8) !*const Value {
        if (bytes.len != encoded_size) return error.InvalidSqlEnumSymbol;
        return self.bySymbol(std.mem.readInt(u32, bytes[0..encoded_size], .little));
    }
    pub fn compare(self: *const View, left: *const Value, right: *const Value) !std.math.Order {
        const a = self.check(left) catch |err| switch (err) {
            error.SqlTypeMismatch => return error.SqlUndefinedOperator,
            else => return err,
        };
        const b = self.check(right) catch |err| switch (err) {
            error.SqlTypeMismatch => return error.SqlUndefinedOperator,
            else => return err,
        };
        return std.mem.order(u8, a.order_key, b.order_key);
    }
    /// Borrowed stable bytes. The ordinary tuple encoder adds its existing
    /// NULL, direction and component-escaping envelope around these ranks.
    pub fn orderedKey(self: *const View, value: *const Value) ![]const u8 {
        return (try self.check(value)).order_key;
    }
    /// Logical content excludes remappable local IDs, wire OIDs and physical
    /// ordering keys. SQL hash-join/group identity includes the local type too.
    pub fn hashLogical(self: *const View, value: *const Value, hasher: anytype) !void {
        _ = try self.check(value);
        hasher.update("antfly:sql-enum-symbol:v1\x00");
        hasher.update(&value.origin_id);
        const bytes = try self.encode(value);
        hasher.update(&bytes);
    }
    pub fn hashSql(self: *const View, value: *const Value, hasher: anytype) !void {
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, value.type_id, .little);
        hasher.update(&id);
        try self.hashLogical(value, hasher);
    }
    pub fn semanticHash(self: *const View, value: *const Value) ![32]u8 {
        var hasher = std.crypto.hash.Blake3.init(.{});
        try self.hashLogical(value, &hasher);
        var result: [32]u8 = undefined;
        hasher.final(&result);
        return result;
    }
};

test "SQL named enum atoms encode projected symbols preserve order and allocate only at preparation" {
    const a = std.testing.allocator;
    var definition = try types.Owned.create(a, &.{ "z", "a", "m", "" });
    defer definition.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(5, 2, 1, @splat(1)), .definition = definition.definition, .owner = .auth_disabled };
    var view = try View.init(a, record);
    defer view.deinit(a);
    const z = try view.input("z");
    const a_value = try view.input("a");
    try std.testing.expect(try view.compare(z, a_value) == .lt);
    try std.testing.expectEqualStrings("", try view.output(try view.input("")));
    try std.testing.expectError(error.SqlInvalidTextRepresentation, view.input("Z"));
    for (0..100000) |i| {
        const value = try view.bySymbol(@intCast(i % 4 + 1));
        const encoded = try view.encode(value);
        const decoded = try view.decode(&encoded);
        try std.testing.expectEqual(value, decoded);
        _ = try view.orderedKey(decoded);
    }
    try std.testing.expectError(error.InvalidSqlEnumSymbol, view.decode(&.{ 0, 0, 0, 0 }));
    try std.testing.expectError(error.InvalidSqlEnumSymbol, view.decode(&.{ 5, 0, 0, 0 }));
    try std.testing.expectError(error.InvalidSqlEnumSymbol, view.decode(&.{ 1, 0, 0 }));
}

test "SQL named enum restore remaps local IDs without changing logical hashes or existing ordered keys" {
    const a = std.testing.allocator;
    var definition = try types.Owned.create(a, &.{ "queued", "done" });
    defer definition.deinit();
    const record: types.Record = .{ .identity = try types.Identity.init(5, 2, 1, @splat(1)), .definition = definition.definition, .owner = .auth_disabled };
    var before = try View.init(a, record);
    defer before.deinit(a);
    const original = try before.input("done");
    const physical = try before.encode(original);
    const hash = try before.semanticHash(original);
    var successor = try types.Owned.add(a, record.definition, "started", .{ .before = "done" });
    defer successor.deinit();
    var restored_record = record;
    restored_record.identity = try record.identity.remap(17, 9);
    restored_record.identity.generation += 1;
    restored_record.definition = successor.definition;
    var restored = try View.init(a, restored_record);
    defer restored.deinit(a);
    const decoded = try restored.decode(&physical);
    try std.testing.expectEqualDeep(hash, try restored.semanticHash(decoded));
    try std.testing.expectEqualSlices(u8, try before.orderedKey(original), try restored.orderedKey(decoded));
    try std.testing.expect(restored_record.identity.oid != record.identity.oid);
    try std.testing.expectError(error.SqlUndefinedOperator, before.compare(original, decoded));
    var impostor = original.*;
    impostor.origin_id = @splat(2);
    try std.testing.expectError(error.SqlUndefinedOperator, before.compare(original, &impostor));
}

test "SQL named enum atom views unwind every preparation allocation" {
    const Harness = struct {
        fn run(a: A) !void {
            var definition = try types.Owned.create(a, &.{ "queued", "processing", "done" });
            defer definition.deinit();
            const record: types.Record = .{ .identity = try types.Identity.init(5, 2, 1, @splat(1)), .definition = definition.definition, .owner = .auth_disabled };
            var view = try View.init(a, record);
            defer view.deinit(a);
            _ = try view.encode(try view.input("queued"));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}
