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

//! Canonical, dependency-scoped schema contracts shared by producer recipes
//! and row-input proofs. Unknown schema dialects retain the complete contract.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;

/// `null` means the complete row (templates, source retention or uncaptured
/// dependencies). Source/version authority is independently bound by publication.
pub fn project(a: A, bytes: []const u8, columns: ?[]const []const u8) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var value = try std.json.parseFromSliceLeaky(V, ca, bytes, .{});
    if (value != .object) return error.InvalidTableSchema;
    _ = value.object.swapRemove("version");
    _ = value.object.swapRemove("base_source");
    _ = value.object.swapRemove("relational_indexes");
    // Keep the untouched contract for conservative fallback after any dialect
    // or path we cannot prove. Projection mutates a separate parsed value.
    const full = try std.json.Stringify.valueAlloc(ca, value, .{});
    if (columns) |paths| {
        if (value.object.getPtr("document_schemas")) |docs| {
            var known = docs.* == .object;
            if (known) for (docs.object.values()) |*doc| {
                if (doc.* != .object) {
                    known = false;
                    break;
                }
                const schema = doc.object.getPtr("schema") orelse {
                    known = false;
                    break;
                };
                if (!try select(ca, schema, paths)) {
                    known = false;
                    break;
                }
            };
            if (!known) value = try std.json.parseFromSliceLeaky(V, ca, full, .{});
        }
    }
    try canonical(ca, &value);
    return std.json.Stringify.valueAlloc(a, value, .{});
}
fn hasOpaqueDialect(v: V) bool {
    if (v == .object) {
        for ([_][]const u8{ "$ref", "allOf", "anyOf", "oneOf", "if", "then", "else", "dependentSchemas", "dependencies", "patternProperties" }) |key| if (v.object.contains(key)) return true;
        for (v.object.values()) |child| if (hasOpaqueDialect(child)) return true;
    } else if (v == .array) for (v.array.items) |child| {
        if (hasOpaqueDialect(child)) return true;
    };
    return false;
}
fn select(a: A, schema: *V, paths: []const []const u8) anyerror!bool {
    if (schema.* != .object or hasOpaqueDialect(schema.*)) return false;
    const props = schema.object.getPtr("properties") orelse return paths.len == 0;
    if (props.* != .object) return false;
    for (paths) |path| {
        const end = std.mem.indexOfScalar(u8, path, '.') orelse path.len;
        if (!props.object.contains(path[0..end])) return false;
    }
    var selected: std.json.ObjectMap = .empty;
    for (props.object.keys(), props.object.values()) |name, child| {
        var whole = false;
        var nested: std.ArrayList([]const u8) = .empty;
        for (paths) |path| {
            if (std.mem.eql(u8, name, path)) whole = true;
            if (path.len > name.len and std.mem.startsWith(u8, path, name) and path[name.len] == '.') try nested.append(a, path[name.len + 1 ..]);
        }
        if (!whole and nested.items.len == 0) continue;
        var projected = child;
        if (!whole and !try select(a, &projected, nested.items)) return false;
        try selected.put(a, name, projected);
    }
    props.* = .{ .object = selected };
    if (schema.object.getPtr("required")) |required| {
        if (required.* != .array) return false;
        var retained: std.array_list.Managed(V) = .init(a);
        for (required.array.items) |name| {
            if (name != .string) return false;
            if (selected.contains(name.string)) try retained.append(name);
        }
        std.mem.sort(V, retained.items, {}, struct {
            fn less(_: void, l: V, r: V) bool {
                return std.mem.lessThan(u8, l.string, r.string);
            }
        }.less);
        if (retained.items.len == 0) {
            _ = schema.object.swapRemove("required");
        } else required.* = .{ .array = retained };
    }
    return true;
}
fn canonical(a: A, value: *V) anyerror!void {
    switch (value.*) {
        .object => |*obj| {
            const names = try a.dupe([]const u8, obj.keys());
            std.mem.sort([]const u8, names, {}, struct {
                fn less(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.less);
            var sorted: std.json.ObjectMap = .empty;
            for (names) |name| {
                var child = obj.get(name).?;
                try canonical(a, &child);
                try sorted.put(a, name, child);
            }
            value.* = .{ .object = sorted };
        },
        .array => |*arr| for (arr.items) |*child| try canonical(a, child),
        else => {},
    }
}

test "external lake dependency contracts isolate unrelated columns and retain interpretation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const before =
        \\{"version":1,"document_schemas":{"row":{"schema":{"type":"object","required":["body"],"properties":{"body":{"type":"string","x-antfly-field":{"type":"text"}}}}}}}
    ;
    const after =
        \\{"document_schemas":{"row":{"schema":{"properties":{"extra":{"type":"integer"},"body":{"x-antfly-field":{"type":"text"},"type":"string"}},"required":["extra","body"],"type":"object"}}},"version":2}
    ;
    const first = try project(a, before, &.{"body"});
    try std.testing.expectEqualStrings(first, try project(a, after, &.{"body"}));
    try std.testing.expect(!std.mem.eql(u8, try project(a, before, null), try project(a, after, null)));
    const changed = try std.mem.replaceOwned(u8, a, after, "text", "keyword");
    try std.testing.expect(!std.mem.eql(u8, first, try project(a, changed, &.{"body"})));
    // Unknown paths cannot authorize reuse of a partial schema.
    try std.testing.expectEqualStrings(try project(a, after, null), try project(a, after, &.{"missing"}));
}

test "external lake dependency contracts preserve nested nullability and unknown dialect boundaries" {
    const Fault = struct {
        fn run(a: A) !void {
            const schema =
                \\{"document_schemas":{"row":{"schema":{"properties":{"payload":{"type":"object","properties":{"body":{"type":"string"},"flag":{"type":"boolean"}}},"other":{"type":"string"}}}}}}
            ;
            const changed = try std.mem.replaceOwned(u8, a, schema, "boolean", "number");
            defer a.free(changed);
            const selected = try project(a, schema, &.{"payload.body"});
            defer a.free(selected);
            const same = try project(a, changed, &.{"payload.body"});
            defer a.free(same);
            try std.testing.expectEqualStrings(selected, same);
            const whole = try project(a, schema, &.{"payload"});
            defer a.free(whole);
            const different = try project(a, changed, &.{"payload"});
            defer a.free(different);
            try std.testing.expect(!std.mem.eql(u8, whole, different));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fault.run, .{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const optional =
        \\{"document_schemas":{"row":{"schema":{"properties":{"body":{"type":"string"},"extra":{"type":"string"}}}}}}
    ;
    const unrelated_required = try std.mem.replaceOwned(u8, a, optional, "\"properties\":", "\"required\":[\"extra\"],\"properties\":");
    const selected_required = try std.mem.replaceOwned(u8, a, unrelated_required, "[\"extra\"]", "[\"body\"]");
    try std.testing.expectEqualStrings(try project(a, optional, &.{"body"}), try project(a, unrelated_required, &.{"body"}));
    try std.testing.expect(!std.mem.eql(u8, try project(a, optional, &.{"body"}), try project(a, selected_required, &.{"body"})));
    const composed = try std.mem.replaceOwned(u8, a, optional, "\"properties\":", "\"allOf\":[{}],\"properties\":");
    try std.testing.expectEqualStrings(try project(a, composed, null), try project(a, composed, &.{"body"}));
}
