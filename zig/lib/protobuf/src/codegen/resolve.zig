// Copyright 2026 Antfly, Inc.
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

//! Symbol table for protoc-zig. Walks a `FileDescriptorSet` once and records
//! every message / enum declaration together with its fully-qualified proto
//! name, the package it lives in, the source file, and its nested "path"
//! (e.g. `["FieldDescriptorProto", "Type"]` for a nested enum).
//!
//! The emitter uses this table to resolve `FieldDescriptorProto.type_name`
//! strings into Zig references — including across files (via package-based
//! import aliases) and through nested scopes.

const std = @import("std");
const Allocator = std.mem.Allocator;
const descriptor = @import("../descriptor.zig");

pub const SymbolKind = enum { message, @"enum" };

pub const Symbol = struct {
    /// Fully-qualified proto name, e.g. "antfly.lib.vector.quantize.RaBitQCodeSet".
    /// Stored WITHOUT a leading dot.
    fqn: []const u8,
    /// Proto package this symbol lives in, e.g. "antfly.lib.vector.quantize".
    /// May be empty for files declaring no package.
    package: []const u8,
    /// Proto file path the declaration came from, e.g. "lib/vector/vector.proto".
    file: []const u8,
    /// Nested type path within the file — does NOT include the package.
    /// For `FieldDescriptorProto.Type` this is `["FieldDescriptorProto","Type"]`.
    path: []const []const u8,
    kind: SymbolKind,
};

pub const SymbolTable = struct {
    allocator: Allocator,
    symbols: std.ArrayListUnmanaged(Symbol) = .empty,
    /// FQN (no leading dot) → index into `symbols`.
    by_fqn: std.StringHashMapUnmanaged(usize) = .empty,

    pub fn deinit(self: *SymbolTable) void {
        for (self.symbols.items) |sym| {
            self.allocator.free(sym.fqn);
            self.allocator.free(sym.path);
        }
        self.symbols.deinit(self.allocator);
        self.by_fqn.deinit(self.allocator);
    }

    pub fn build(allocator: Allocator, set: *const descriptor.FileDescriptorSet) !SymbolTable {
        var table: SymbolTable = .{ .allocator = allocator };
        errdefer table.deinit();

        for (set.file) |*file| {
            try collectFromFile(&table, file);
        }
        return table;
    }

    /// Look up a symbol by its fully-qualified name. Accepts either `.a.b.C`
    /// (proto wire convention with leading dot) or `a.b.C`.
    pub fn lookup(self: *const SymbolTable, fqn: []const u8) ?*const Symbol {
        const name = if (fqn.len > 0 and fqn[0] == '.') fqn[1..] else fqn;
        if (self.by_fqn.get(name)) |idx| return &self.symbols.items[idx];
        return null;
    }

    pub fn packageOfFile(self: *const SymbolTable, file_path: []const u8) []const u8 {
        for (self.symbols.items) |sym| {
            if (std.mem.eql(u8, sym.file, file_path)) return sym.package;
        }
        return "";
    }
};

fn collectFromFile(table: *SymbolTable, file: *const descriptor.FileDescriptorProto) !void {
    for (file.message_type) |*msg| {
        try collectMessage(table, file, &.{}, msg);
    }
    for (file.enum_type) |*e| {
        try collectEnum(table, file, &.{}, e);
    }
}

fn collectMessage(
    table: *SymbolTable,
    file: *const descriptor.FileDescriptorProto,
    prefix: []const []const u8,
    msg: *const descriptor.DescriptorProto,
) !void {
    const alloc = table.allocator;

    var path_buf: std.ArrayListUnmanaged([]const u8) = .empty;
    defer path_buf.deinit(alloc);
    try path_buf.appendSlice(alloc, prefix);
    try path_buf.append(alloc, msg.name);

    const fqn = try buildFqn(alloc, file.package, path_buf.items);
    const owned_path = try alloc.dupe([]const u8, path_buf.items);

    const idx = table.symbols.items.len;
    try table.symbols.append(alloc, .{
        .fqn = fqn,
        .package = file.package,
        .file = file.name,
        .path = owned_path,
        .kind = .message,
    });
    try table.by_fqn.put(alloc, fqn, idx);

    for (msg.nested_type) |*nested| {
        try collectMessage(table, file, path_buf.items, nested);
    }
    for (msg.enum_type) |*nested_enum| {
        try collectEnum(table, file, path_buf.items, nested_enum);
    }
}

fn collectEnum(
    table: *SymbolTable,
    file: *const descriptor.FileDescriptorProto,
    prefix: []const []const u8,
    e: *const descriptor.EnumDescriptorProto,
) !void {
    const alloc = table.allocator;

    var path_buf: std.ArrayListUnmanaged([]const u8) = .empty;
    defer path_buf.deinit(alloc);
    try path_buf.appendSlice(alloc, prefix);
    try path_buf.append(alloc, e.name);

    const fqn = try buildFqn(alloc, file.package, path_buf.items);
    const owned_path = try alloc.dupe([]const u8, path_buf.items);

    const idx = table.symbols.items.len;
    try table.symbols.append(alloc, .{
        .fqn = fqn,
        .package = file.package,
        .file = file.name,
        .path = owned_path,
        .kind = .@"enum",
    });
    try table.by_fqn.put(alloc, fqn, idx);
}

fn buildFqn(alloc: Allocator, package: []const u8, path: []const []const u8) ![]u8 {
    var list: std.ArrayListUnmanaged(u8) = .empty;
    errdefer list.deinit(alloc);
    try list.appendSlice(alloc, package);
    for (path) |p| {
        if (list.items.len > 0) try list.append(alloc, '.');
        try list.appendSlice(alloc, p);
    }
    return list.toOwnedSlice(alloc);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const message = @import("../message.zig");

test "SymbolTable indexes top-level and nested types" {
    const alloc = testing.allocator;

    // Build a minimal FileDescriptorSet in memory:
    //   file "x.proto" package "a.b"
    //     message Outer {
    //       enum E { X = 0; }
    //       message Inner {}
    //     }
    //     enum TopEnum { A = 0; }

    var inner_path_mem = [_][]const u8{};
    _ = &inner_path_mem;

    var e_values = [_]descriptor.EnumValueDescriptorProto{
        .{ .name = "X", .number = 0 },
    };
    var inner_msgs = [_]descriptor.DescriptorProto{
        .{ .name = "Inner" },
    };
    var inner_enums = [_]descriptor.EnumDescriptorProto{
        .{ .name = "E", .value = e_values[0..] },
    };
    var outer_msg = [_]descriptor.DescriptorProto{
        .{
            .name = "Outer",
            .nested_type = inner_msgs[0..],
            .enum_type = inner_enums[0..],
        },
    };
    var top_enum_values = [_]descriptor.EnumValueDescriptorProto{
        .{ .name = "A", .number = 0 },
    };
    var top_enums = [_]descriptor.EnumDescriptorProto{
        .{ .name = "TopEnum", .value = top_enum_values[0..] },
    };
    var files = [_]descriptor.FileDescriptorProto{
        .{
            .name = "x.proto",
            .package = "a.b",
            .message_type = outer_msg[0..],
            .enum_type = top_enums[0..],
        },
    };
    const set = descriptor.FileDescriptorSet{ .file = files[0..] };

    var table = try SymbolTable.build(alloc, &set);
    defer table.deinit();

    const outer = table.lookup("a.b.Outer") orelse return error.TestMissingOuter;
    try testing.expectEqual(SymbolKind.message, outer.kind);
    try testing.expectEqualStrings("a.b", outer.package);
    try testing.expectEqualStrings("x.proto", outer.file);
    try testing.expectEqual(@as(usize, 1), outer.path.len);
    try testing.expectEqualStrings("Outer", outer.path[0]);

    const inner = table.lookup("a.b.Outer.Inner") orelse return error.TestMissingInner;
    try testing.expectEqual(SymbolKind.message, inner.kind);
    try testing.expectEqual(@as(usize, 2), inner.path.len);
    try testing.expectEqualStrings("Outer", inner.path[0]);
    try testing.expectEqualStrings("Inner", inner.path[1]);

    const enum_e = table.lookup(".a.b.Outer.E") orelse return error.TestMissingE;
    try testing.expectEqual(SymbolKind.@"enum", enum_e.kind);

    const top = table.lookup("a.b.TopEnum") orelse return error.TestMissingTopEnum;
    try testing.expectEqual(SymbolKind.@"enum", top.kind);
    try testing.expectEqual(@as(usize, 1), top.path.len);
}

test "SymbolTable builds from real descriptor.desc" {
    const alloc = testing.allocator;
    const desc_bytes = @embedFile("../testdata/descriptor.desc");

    var set = try descriptor.FileDescriptorSet.decode(alloc, desc_bytes);
    defer set.deinit(alloc);

    var table = try SymbolTable.build(alloc, &set);
    defer table.deinit();

    // Top-level messages.
    try testing.expect(table.lookup("google.protobuf.FileDescriptorSet") != null);
    try testing.expect(table.lookup("google.protobuf.FileDescriptorProto") != null);
    // Nested enum under FieldDescriptorProto.
    try testing.expect(table.lookup("google.protobuf.FieldDescriptorProto.Type") != null);
    try testing.expect(table.lookup("google.protobuf.FieldDescriptorProto.Label") != null);
    // Leading-dot lookup should also work (matches FieldDescriptorProto.type_name format).
    try testing.expect(table.lookup(".google.protobuf.DescriptorProto") != null);
}

test "SymbolTable builds from quantize.desc" {
    const alloc = testing.allocator;
    const desc_bytes = @embedFile("../testdata/quantize.desc");

    var set = try descriptor.FileDescriptorSet.decode(alloc, desc_bytes);
    defer set.deinit(alloc);

    var table = try SymbolTable.build(alloc, &set);
    defer table.deinit();

    const code_set = table.lookup("antfly.lib.vector.quantize.RaBitQCodeSet") orelse
        return error.TestMissingRaBitQCodeSet;
    try testing.expectEqualStrings("antfly.lib.vector.quantize", code_set.package);

    const quantized = table.lookup("antfly.lib.vector.quantize.RaBitQuantizedVectorSet") orelse
        return error.TestMissingQuantized;
    try testing.expectEqualStrings("antfly.lib.vector.quantize", quantized.package);

    const vset = table.lookup("antfly.lib.vector.Set") orelse return error.TestMissingVectorSet;
    try testing.expectEqualStrings("antfly.lib.vector", vset.package);

    const dmetric = table.lookup("antfly.lib.vector.DistanceMetric") orelse return error.TestMissingDMetric;
    try testing.expectEqual(SymbolKind.@"enum", dmetric.kind);

    _ = message;
}

test "SymbolTable includes synthetic map-entry messages" {
    const alloc = testing.allocator;

    var fields = [_]descriptor.FieldDescriptorProto{
        .{
            .name = "entries",
            .number = 1,
            .label = .repeated,
            .type = .message,
            .type_name = ".pkg.Container.EntriesEntry",
        },
    };
    var entry_fields = [_]descriptor.FieldDescriptorProto{
        .{
            .name = "key",
            .number = 1,
            .label = .optional,
            .type = .string,
        },
        .{
            .name = "value",
            .number = 2,
            .label = .optional,
            .type = .int32,
        },
    };
    var nested = [_]descriptor.DescriptorProto{
        .{
            .name = "EntriesEntry",
            .field = entry_fields[0..],
            .options = .{ .map_entry = true },
        },
    };
    var msgs = [_]descriptor.DescriptorProto{
        .{
            .name = "Container",
            .field = fields[0..],
            .nested_type = nested[0..],
        },
    };
    var files = [_]descriptor.FileDescriptorProto{
        .{
            .name = "map.proto",
            .package = "pkg",
            .message_type = msgs[0..],
        },
    };
    const set = descriptor.FileDescriptorSet{ .file = files[0..] };

    var table = try SymbolTable.build(alloc, &set);
    defer table.deinit();

    const container = table.lookup("pkg.Container") orelse return error.TestMissingContainer;
    try testing.expectEqual(SymbolKind.message, container.kind);

    const entry = table.lookup("pkg.Container.EntriesEntry") orelse return error.TestMissingMapEntry;
    try testing.expectEqual(SymbolKind.message, entry.kind);
    try testing.expectEqual(@as(usize, 2), entry.path.len);
    try testing.expectEqualStrings("Container", entry.path[0]);
    try testing.expectEqualStrings("EntriesEntry", entry.path[1]);
}
