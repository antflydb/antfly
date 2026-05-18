// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;
const token = @import("token.zig");
const value = @import("value.zig");

pub const Role = enum {
    group,
    measure,
    time,
    vector_constraint,
};

pub const FieldSpec = struct {
    name: []const u8,
    path: []const u8,
    type: []const u8,
    role: Role,
};

pub const Fact = struct {
    role: Role,
    field: []u8,
    scalar: []u8,

    pub fn deinit(self: *Fact, alloc: Allocator) void {
        alloc.free(self.field);
        alloc.free(self.scalar);
        self.* = undefined;
    }
};

pub const FactList = struct {
    facts: []Fact = &.{},

    pub fn deinit(self: *FactList, alloc: Allocator) void {
        for (self.facts) |*item| item.deinit(alloc);
        if (self.facts.len > 0) alloc.free(self.facts);
        self.* = .{};
    }
};

pub fn projectDocumentAlloc(alloc: Allocator, specs: []const FieldSpec, root: std.json.Value) !FactList {
    var out = std.ArrayListUnmanaged(Fact).empty;
    errdefer {
        for (out.items) |*item| item.deinit(alloc);
        out.deinit(alloc);
    }
    for (specs) |spec| {
        const found = valueAtPath(root, spec.path) orelse continue;
        const scalar = (try value.scalarFromJsonAlloc(alloc, spec.type, found)) orelse continue;
        errdefer alloc.free(scalar);
        try out.append(alloc, .{
            .role = spec.role,
            .field = try alloc.dupe(u8, spec.name),
            .scalar = scalar,
        });
    }
    return .{ .facts = try out.toOwnedSlice(alloc) };
}

pub fn encodeListAlloc(alloc: Allocator, facts: []const Fact) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    try parts.append(alloc, "facts:v1");
    for (facts) |item| {
        try parts.append(alloc, @tagName(item.role));
        try parts.append(alloc, item.field);
        try parts.append(alloc, item.scalar);
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn decodeListAlloc(alloc: Allocator, encoded: []const u8) !FactList {
    const parts = try token.decodeTupleAlloc(alloc, encoded);
    defer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    if (parts.len == 0 or !std.mem.eql(u8, parts[0], "facts:v1")) return error.InvalidAlgebraicFact;
    if ((parts.len - 1) % 3 != 0) return error.InvalidAlgebraicFact;
    var out = std.ArrayListUnmanaged(Fact).empty;
    errdefer {
        for (out.items) |*item| item.deinit(alloc);
        out.deinit(alloc);
    }
    var pos: usize = 1;
    while (pos < parts.len) : (pos += 3) {
        const role = std.meta.stringToEnum(Role, parts[pos]) orelse return error.InvalidAlgebraicFact;
        try out.append(alloc, .{
            .role = role,
            .field = try alloc.dupe(u8, parts[pos + 1]),
            .scalar = try alloc.dupe(u8, parts[pos + 2]),
        });
    }
    return .{ .facts = try out.toOwnedSlice(alloc) };
}

pub fn axisTupleAlloc(alloc: Allocator, facts: []const Fact, fields: []const []const u8) ![]u8 {
    var axes = try alloc.alloc([]const u8, fields.len);
    defer alloc.free(axes);
    for (fields, 0..) |field, i| {
        axes[i] = findScalar(facts, .group, field) orelse return error.MissingField;
    }
    return try token.canonicalTupleAlloc(alloc, axes);
}

pub fn findScalar(facts: []const Fact, role: Role, field: []const u8) ?[]const u8 {
    for (facts) |item| {
        if (item.role == role and std.mem.eql(u8, item.field, field)) return item.scalar;
    }
    return null;
}

fn valueAtPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var it = std.mem.splitScalar(u8, path, '.');
    while (it.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

test "fact projection stores explicit typed document facts" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tenant\":\"t1\",\"amount\":12.5,\"created\":\"2026-05-13T00:00:00Z\"}", .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const specs = [_]FieldSpec{
        .{ .name = "tenant", .path = "tenant", .type = "keyword", .role = .group },
        .{ .name = "amount", .path = "amount", .type = "number", .role = .measure },
        .{ .name = "created", .path = "created", .type = "datetime", .role = .time },
    };
    var facts = try projectDocumentAlloc(alloc, specs[0..], parsed.value);
    defer facts.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), facts.facts.len);

    const encoded = try encodeListAlloc(alloc, facts.facts);
    defer alloc.free(encoded);
    var decoded = try decodeListAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), decoded.facts.len);
    try std.testing.expect(findScalar(decoded.facts, .group, "tenant") != null);
}

test "fact axis tuple is independent of document shape" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tenant\":\"t1\",\"product\":\"p1\"}", .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const specs = [_]FieldSpec{
        .{ .name = "tenant", .path = "tenant", .type = "keyword", .role = .group },
        .{ .name = "product", .path = "product", .type = "keyword", .role = .group },
    };
    var facts = try projectDocumentAlloc(alloc, specs[0..], parsed.value);
    defer facts.deinit(alloc);
    const axis = try axisTupleAlloc(alloc, facts.facts, &.{ "tenant", "product" });
    defer alloc.free(axis);
    try std.testing.expect(axis.len > 0);
}
