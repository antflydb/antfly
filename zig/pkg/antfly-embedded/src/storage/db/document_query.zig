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

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

pub fn lookupJson(
    alloc: Allocator,
    raw: []const u8,
    opts: types.LookupOptions,
) !types.LookupResult {
    if (opts.fields.len == 0 and opts.include_all_fields) {
        return .{ .json = try alloc.dupe(u8, raw) };
    }

    var projected = try projectLookupJsonValue(alloc, raw, opts);
    defer freeJsonValue(alloc, &projected);

    return .{
        .json = try std.json.Stringify.valueAlloc(alloc, projected, .{}),
    };
}

/// Projection containers use scratch; only serialized bytes use output.
/// Strings and number lexemes borrow the parsed source until encoding ends.
pub fn lookupJsonWithOutput(alloc: Allocator, output: Allocator, raw: []const u8, opts: types.LookupOptions) !types.LookupResult {
    if (opts.fields.len == 0 and opts.include_all_fields) return .{ .json = try output.dupe(u8, raw) };
    if (spanProjectionSupported(opts.fields)) {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var view = @import("query/json_view.zig").View.init(a, raw);
        // Plain JSON lookup historically parses numbers before serialization.
        view.parse_numbers = true;
        try validateUniqueJson(a, raw, &view, try view.projectionPaths(opts.fields));
        var projected: std.json.ObjectMap = .empty;
        var path: std.ArrayListUnmanaged([]const u8) = .empty;
        var excluded: std.ArrayListUnmanaged([]const u8) = .empty;
        var has_positive = false;
        var has_negative = false;
        var include_all = false;
        for (opts.fields) |field| {
            if (field.len > 0 and field[0] == '-') {
                has_negative = true;
                if (field.len > 1) try excluded.append(a, field[1..]);
            } else {
                has_positive = true;
                include_all = include_all or std.mem.eql(u8, field, "*");
            }
        }
        if (include_all or (!has_positive and has_negative)) {
            try view.includeRootExcept(a, &projected, excluded.items);
        } else for (opts.fields) |field| {
            if (field.len > 0 and field[0] == '-') continue;
            path.clearRetainingCapacity();
            var tokens = std.mem.tokenizeScalar(u8, field, '.');
            while (tokens.next()) |part| try path.append(a, part);
            if (path.items.len == 0) continue;
            const omit = for (excluded.items) |name| {
                if (std.mem.eql(u8, name, "*") or std.mem.eql(u8, name, path.items[0])) break true;
            } else false;
            if (!omit) try view.includeRoot(a, &projected, path.items);
        }
        return .{ .json = try serializeWithOutput(a, output, .{ .object = projected }) };
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    var projected = try projectLookupView(alloc, parsed.value, opts);
    defer deinitProjectionView(alloc, &projected);
    return .{ .json = try serializeWithOutput(alloc, output, projected) };
}

fn spanProjectionSupported(fields: []const []const u8) bool {
    var positives: usize = 0;
    var all = false;
    for (fields) |field| {
        const negative = field.len > 0 and field[0] == '-';
        const name = if (negative) field[1..] else field;
        if (negative and std.mem.indexOfScalar(u8, name, '.') != null) return false;
        if (std.mem.indexOfScalar(u8, name, '*') != null and !std.mem.eql(u8, name, "*")) return false;
        if (!negative) {
            positives += 1;
            all = all or std.mem.eql(u8, name, "*");
        }
    }
    // Mixing wildcard inclusion with later array selections is order-sensitive.
    return !all or positives == 1;
}

/// Validate skipped subtrees too, including duplicate object keys. Reuse one
/// key arena per nesting level rather than allocating a DOM for every value.
/// Scalar values are scanned without decoding or retaining their strings.
fn validateUniqueJson(alloc: Allocator, raw: []const u8, view: *@import("query/json_view.zig").View, paths: []const @import("query/json_view.zig").View.ProjectionPath) !void {
    const View = @import("query/json_view.zig").View;
    const Path = View.ProjectionPath;
    const Paths = std.ArrayListUnmanaged(Path);
    const scratch = @import("antfly_platform").allocator.reclaimingScratch(alloc);
    const Frame = struct {
        object: bool = false,
        key: bool = true,
        arena: std.heap.ArenaAllocator,
        names: std.StringHashMapUnmanaged(void) = .empty,
        node: ?*View.Node = null,
        children: std.ArrayListUnmanaged(View.Child) = .empty,
        object_paths: std.StringHashMapUnmanaged(Paths) = .empty,
        index_paths: std.AutoHashMapUnmanaged(usize, Paths) = .empty,
        fanout: Paths = .empty,
        name: []const u8 = "",
        ordinal: usize = 0,
        start: usize = 0,
        pending_node: ?*View.Node = null,
        pending_paths: []const Path = &.{},

        fn plan(self: *@This(), a: Allocator, selected: []const Path) !void {
            if (self.node == null) return;
            for (selected) |path| {
                if (self.object) {
                    if (path.parts.len <= 1) continue;
                    const entry = try self.object_paths.getOrPut(a, path.parts[0]);
                    if (!entry.found_existing) entry.value_ptr.* = .empty;
                    try entry.value_ptr.append(a, .{ .parts = path.parts[1..] });
                } else if (View.projectionIndex(path.parts[0])) |i| {
                    const entry = try self.index_paths.getOrPut(a, i);
                    if (!entry.found_existing) entry.value_ptr.* = .empty;
                    if (path.parts.len > 1) try entry.value_ptr.append(a, .{ .parts = path.parts[1..] });
                } else try self.fanout.append(a, path);
            }
        }
        fn beginValue(self: *@This(), a: Allocator, start: usize, object_value: bool) !void {
            self.start = start;
            self.pending_node = null;
            self.pending_paths = &.{};
            if (self.node == null) return;
            if (self.object) {
                if (self.object_paths.get(self.name)) |selected| self.pending_paths = selected.items;
                // Scalar/whole-subtree fields need only one raw span, no node allocation.
                if (self.pending_paths.len != 0) self.pending_node = try a.create(View.Node);
            } else {
                const numeric = self.index_paths.get(self.ordinal);
                if (numeric == null and self.fanout.items.len == 0) return;
                self.pending_node = try a.create(View.Node);
                if (object_value) {
                    if (numeric) |selected| self.pending_paths = selected.items;
                    if (self.fanout.items.len != 0) {
                        var combined: Paths = .empty;
                        try combined.appendSlice(a, self.pending_paths);
                        try combined.appendSlice(a, self.fanout.items);
                        self.pending_paths = combined.items;
                    }
                }
            }
            if (self.pending_node) |node| node.* = .{ .raw = &.{} };
        }
        fn finishValue(self: *@This(), a: Allocator, source: []const u8, end: usize) !void {
            if (self.node) |parent| {
                const span = source[self.start..end];
                if (self.pending_node) |node| node.raw = span;
                if (self.object) {
                    try parent.object_index.put(a, self.name, self.children.items.len);
                    try self.children.append(a, .{ .node = if (self.pending_node) |node| node.* else .{ .raw = span } });
                } else if (self.pending_node) |node| {
                    if (self.fanout.items.len != 0) try self.children.append(a, .{ .node = node.* }) else try parent.array_index.put(a, self.ordinal, node);
                }
            }
            self.ordinal += 1;
        }
        fn finishContainer(self: *@This(), a: Allocator) !void {
            if (self.node) |node| {
                if (self.object or self.fanout.items.len != 0) node.children = try self.children.toOwnedSlice(a) else try node.array_index.put(a, std.math.maxInt(usize), node);
            }
        }
    };
    var frames: std.ArrayListUnmanaged(Frame) = .empty;
    defer {
        for (frames.items) |*frame| frame.arena.deinit();
        frames.deinit(scratch);
    }
    var scanner = std.json.Scanner.initCompleteInput(scratch, raw);
    defer scanner.deinit();
    var depth: usize = 0;
    var root_seen = false;
    while (!root_seen or depth != 0) {
        const token = try scanner.peekNextTokenType();
        if (depth != 0) {
            const frame = &frames.items[depth - 1];
            if ((frame.object and frame.key and token == .object_end) or (!frame.object and token == .array_end)) {
                _ = try scanner.next();
                try frame.finishContainer(alloc);
                depth -= 1;
                if (depth != 0) try frames.items[depth - 1].finishValue(alloc, raw, scanner.cursor);
                continue;
            }
            if (frame.object and frame.key) {
                const name = switch (try scanner.nextAllocMax(frame.arena.allocator(), .alloc_if_needed, raw.len)) {
                    .string, .allocated_string => |key_name| key_name,
                    else => return error.SyntaxError,
                };
                if ((try frame.names.getOrPut(frame.arena.allocator(), name)).found_existing) return error.DuplicateField;
                frame.name = if (frame.node != null) try alloc.dupe(u8, name) else name;
                frame.key = false;
                continue;
            }
            frame.key = true;
            try frame.beginValue(alloc, scanner.cursor, token == .object_begin);
        }
        root_seen = true;
        if (token == .object_begin or token == .array_begin) {
            const selected_node: ?*View.Node = if (depth == 0) (if (token == .object_begin) &view.root else null) else if (frames.items[depth - 1].pending_paths.len != 0) frames.items[depth - 1].pending_node else null;
            const selected_paths = if (depth == 0) paths else frames.items[depth - 1].pending_paths;
            _ = try scanner.next();
            if (depth == frames.items.len) try frames.append(scratch, .{ .arena = .init(scratch) });
            const frame = &frames.items[depth];
            var key_arena = frame.arena;
            _ = key_arena.reset(.{ .retain_with_limit = 16 * 1024 });
            frame.* = .{ .arena = key_arena, .object = token == .object_begin, .node = selected_node };
            try frame.plan(alloc, selected_paths);
            depth += 1;
        } else {
            if (token == .object_end or token == .array_end or token == .end_of_document) return error.SyntaxError;
            try scanner.skipValue();
            if (depth != 0) try frames.items[depth - 1].finishValue(alloc, raw, scanner.cursor);
        }
    }
    if (try scanner.next() != .end_of_document) return error.SyntaxError;
}

/// Encode once in reclaimable scratch, then publish one exact-sized body.
/// Growing writers must never leave superseded buffers in a response arena.
pub fn serializeWithOutput(scratch: Allocator, output: Allocator, value: std.json.Value) ![]u8 {
    var writer = std.Io.Writer.Allocating.init(scratch);
    defer writer.deinit();
    try std.json.Stringify.value(value, .{}, &writer.writer);
    return output.dupe(u8, writer.written());
}

pub fn projectLookupJsonValue(
    alloc: Allocator,
    raw: []const u8,
    opts: types.LookupOptions,
) !std.json.Value {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    return try projectLookupValue(alloc, parsed.value, opts);
}

/// Project an already decoded source without a JSON encode/parse round trip.
/// The returned tree owns its keys and values; root remains borrowed.
pub fn projectLookupValue(alloc: Allocator, root: std.json.Value, opts: types.LookupOptions) !std.json.Value {
    if (root != .object) {
        if (opts.fields.len == 0 and opts.include_all_fields) {
            return try cloneJsonValue(alloc, root);
        }
        return std.json.Value{ .object = std.json.ObjectMap.empty };
    }

    return try projectValue(false, alloc, root, opts);
}

/// Container-owned projection borrowing immutable string/number lexemes.
/// Keep root alive until serialization completes; release with deinitProjectionView.
pub fn projectLookupView(alloc: Allocator, root: std.json.Value, opts: types.LookupOptions) !std.json.Value {
    if (root != .object) return if (opts.fields.len == 0 and opts.include_all_fields) try cloneProjectionValue(true, alloc, root) else .{ .object = .empty };
    return projectValue(true, alloc, root, opts);
}
pub fn deinitProjectionView(alloc: Allocator, value: *std.json.Value) void {
    freeProjectionValue(true, alloc, value);
}

fn projectValue(comptime borrowed: bool, alloc: Allocator, root: std.json.Value, opts: types.LookupOptions) !std.json.Value {
    if (opts.fields.len == 0) {
        return if (opts.include_all_fields)
            try cloneProjectionValue(borrowed, alloc, root)
        else
            std.json.Value{ .object = std.json.ObjectMap.empty };
    }

    var includes = std.ArrayListUnmanaged([]const u8).empty;
    defer includes.deinit(alloc);
    var excludes = std.ArrayListUnmanaged([]const u8).empty;
    defer excludes.deinit(alloc);

    for (opts.fields) |field| {
        if (field.len > 0 and field[0] == '-') {
            try excludes.append(alloc, field[1..]);
        } else {
            try includes.append(alloc, field);
        }
    }

    var result = if (includes.items.len > 0)
        std.json.Value{ .object = std.json.ObjectMap.empty }
    else
        try cloneProjectionValue(borrowed, alloc, root);
    errdefer freeProjectionValue(borrowed, alloc, &result);

    if (includes.items.len > 0) {
        for (includes.items) |pattern| {
            try applyIncludePattern(borrowed, alloc, root.object, &result.object, pattern);
        }
    }

    for (excludes.items) |pattern| {
        try applyExcludePattern(borrowed, alloc, &result.object, pattern);
    }

    return result;
}

fn applyIncludePattern(
    comptime borrowed: bool,
    alloc: Allocator,
    src: std.json.ObjectMap,
    dst: *std.json.ObjectMap,
    pattern: []const u8,
) Allocator.Error!void {
    var parts = std.mem.tokenizeScalar(u8, pattern, '.');
    var path = std.ArrayListUnmanaged([]const u8).empty;
    defer path.deinit(alloc);
    while (parts.next()) |part| try path.append(alloc, part);
    if (path.items.len == 0) return;
    try applyIncludeRecursive(borrowed, alloc, src, dst, path.items, 0);
}

pub fn applyIncludePath(alloc: Allocator, src: std.json.ObjectMap, dst: *std.json.ObjectMap, parts: []const []const u8) Allocator.Error!void {
    return applyIncludeRecursive(false, alloc, src, dst, parts, 0);
}

fn applyIncludeRecursive(
    comptime borrowed: bool,
    alloc: Allocator,
    src: std.json.ObjectMap,
    dst: *std.json.ObjectMap,
    parts: []const []const u8,
    depth: usize,
) Allocator.Error!void {
    if (depth >= parts.len) return;

    const part = parts[depth];
    if (std.mem.eql(u8, part, "*")) {
        var it = src.iterator();
        while (it.next()) |entry| {
            try includeField(borrowed, alloc, entry.key_ptr.*, entry.value_ptr.*, dst, parts, depth);
        }
        return;
    }

    if (src.get(part)) |value| {
        try includeField(borrowed, alloc, part, value, dst, parts, depth);
    }
}

fn includeField(
    comptime borrowed: bool,
    alloc: Allocator,
    key: []const u8,
    value: std.json.Value,
    dst: *std.json.ObjectMap,
    parts: []const []const u8,
    depth: usize,
) Allocator.Error!void {
    if (depth == parts.len - 1) {
        try putCloneProjectionValue(borrowed, alloc, dst, key, value);
        return;
    }

    switch (value) {
        .object => |obj| {
            const nested = try ensureObjectValue(borrowed, alloc, dst, key);
            try applyIncludeRecursive(borrowed, alloc, obj, nested, parts, depth + 1);
        },
        .array => |arr| {
            if (depth + 1 < parts.len) {
                if (parseArrayIndex(parts[depth + 1])) |idx| {
                    if (idx >= arr.items.len) return;

                    var projected_items = std.json.Array.init(alloc);
                    errdefer {
                        for (projected_items.items) |*item| freeProjectionValue(borrowed, alloc, item);
                        projected_items.deinit();
                    }

                    const item = arr.items[idx];
                    if (depth + 1 == parts.len - 1) {
                        try projected_items.ensureTotalCapacity(1);
                        projected_items.appendAssumeCapacity(try cloneProjectionValue(borrowed, alloc, item));
                    } else switch (item) {
                        .object => |item_obj| {
                            var projected_item = std.json.Value{ .object = std.json.ObjectMap.empty };
                            errdefer freeProjectionValue(borrowed, alloc, &projected_item);
                            try applyIncludeRecursive(borrowed, alloc, item_obj, &projected_item.object, parts, depth + 2);
                            if (projected_item.object.count() > 0) {
                                try projected_items.append(projected_item);
                            } else {
                                freeProjectionValue(borrowed, alloc, &projected_item);
                            }
                        },
                        else => {},
                    }

                    if (projected_items.items.len == 0) return;
                    try putProjectionValue(borrowed, alloc, dst, key, .{ .array = projected_items });
                    return;
                }
            }

            var projected_items = std.json.Array.init(alloc);
            errdefer {
                for (projected_items.items) |*item| freeProjectionValue(borrowed, alloc, item);
                projected_items.deinit();
            }

            for (arr.items) |item| {
                switch (item) {
                    .object => |item_obj| {
                        var projected_item = std.json.Value{ .object = std.json.ObjectMap.empty };
                        errdefer freeProjectionValue(borrowed, alloc, &projected_item);
                        try applyIncludeRecursive(borrowed, alloc, item_obj, &projected_item.object, parts, depth + 1);
                        if (projected_item.object.count() > 0) {
                            try projected_items.append(projected_item);
                        } else {
                            freeProjectionValue(borrowed, alloc, &projected_item);
                        }
                    },
                    else => {},
                }
            }

            if (projected_items.items.len == 0) return;
            try putProjectionValue(borrowed, alloc, dst, key, .{ .array = projected_items });
        },
        else => {},
    }
}

fn applyExcludePattern(
    comptime borrowed: bool,
    alloc: Allocator,
    doc: *std.json.ObjectMap,
    pattern: []const u8,
) Allocator.Error!void {
    var parts_iter = std.mem.tokenizeScalar(u8, pattern, '.');
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    while (parts_iter.next()) |part| try parts.append(alloc, part);
    if (parts.items.len == 0) return;

    try applyExcludeRecursive(borrowed, alloc, doc, parts.items, 0);
}

fn applyExcludeRecursive(
    comptime borrowed: bool,
    alloc: Allocator,
    doc: *std.json.ObjectMap,
    parts: []const []const u8,
    depth: usize,
) Allocator.Error!void {
    if (depth >= parts.len) return;

    const part = parts[depth];
    const is_last = depth == parts.len - 1;

    if (std.mem.eql(u8, part, "*")) {
        var keys = std.ArrayListUnmanaged([]const u8).empty;
        defer keys.deinit(alloc);
        var it = doc.iterator();
        while (it.next()) |entry| try keys.append(alloc, entry.key_ptr.*);

        for (keys.items) |key| {
            if (is_last) {
                if (doc.fetchSwapRemove(key)) |entry| {
                    alloc.free(entry.key);
                    var removed = entry.value;
                    freeProjectionValue(borrowed, alloc, &removed);
                }
            } else if (doc.getPtr(key)) |value| {
                try descendExclude(borrowed, alloc, value, parts, depth + 1);
            }
        }
        return;
    }

    if (is_last) {
        if (doc.fetchSwapRemove(part)) |entry| {
            alloc.free(entry.key);
            var removed = entry.value;
            freeProjectionValue(borrowed, alloc, &removed);
        }
        return;
    }

    if (doc.getPtr(part)) |value| {
        try descendExclude(borrowed, alloc, value, parts, depth + 1);
    }
}

fn descendExclude(comptime borrowed: bool, alloc: Allocator, value: *std.json.Value, parts: []const []const u8, depth: usize) Allocator.Error!void {
    switch (value.*) {
        .object => |*nested| try applyExcludeRecursive(borrowed, alloc, nested, parts, depth),
        .array => |*arr| {
            const part = parts[depth];
            if (parseArrayIndex(part)) |idx| {
                if (idx >= arr.items.len) return;
                const is_last = depth == parts.len - 1;
                if (is_last) {
                    var removed = arr.orderedRemove(idx);
                    freeProjectionValue(borrowed, alloc, &removed);
                    return;
                }
                if (arr.items[idx] == .object) try applyExcludeRecursive(borrowed, alloc, &arr.items[idx].object, parts, depth + 1);
                return;
            }
            for (arr.items) |*item| {
                if (item.* == .object) try applyExcludeRecursive(borrowed, alloc, &item.object, parts, depth);
            }
        },
        else => {},
    }
}

fn parseArrayIndex(part: []const u8) ?usize {
    if (part.len == 0) return null;
    for (part) |ch| {
        if (ch < '0' or ch > '9') return null;
    }
    return std.fmt.parseInt(usize, part, 10) catch null;
}

fn ensureObjectValue(
    comptime borrowed: bool,
    alloc: Allocator,
    dst: *std.json.ObjectMap,
    key: []const u8,
) !*std.json.ObjectMap {
    if (dst.getPtr(key)) |existing| {
        if (existing.* == .object) return &existing.object;
        freeProjectionValue(borrowed, alloc, existing);
        existing.* = .{ .object = std.json.ObjectMap.empty };
        return &existing.object;
    }

    try putProjectionValue(borrowed, alloc, dst, key, .{ .object = std.json.ObjectMap.empty });
    return &dst.getPtr(key).?.object;
}

const owned_json = @import("../../common/owned_json.zig");
const putOwnedValue = owned_json.put;
const cloneJsonValue = owned_json.clone;
const freeJsonValue = owned_json.deinit;

fn cloneProjectionValue(comptime borrowed: bool, a: Allocator, value: std.json.Value) Allocator.Error!std.json.Value {
    if (!borrowed) return cloneJsonValue(a, value);
    switch (value) {
        .object => |object| {
            var result: std.json.Value = .{ .object = .empty };
            errdefer freeProjectionValue(true, a, &result);
            var iterator = object.iterator();
            while (iterator.next()) |entry| try putCloneProjectionValue(true, a, &result.object, entry.key_ptr.*, entry.value_ptr.*);
            return result;
        },
        .array => |array| {
            var result: std.json.Value = .{ .array = std.json.Array.init(a) };
            errdefer freeProjectionValue(true, a, &result);
            try result.array.ensureTotalCapacity(array.items.len);
            for (array.items) |item| result.array.appendAssumeCapacity(try cloneProjectionValue(true, a, item));
            return result;
        },
        else => return value,
    }
}
fn freeProjectionValue(comptime borrowed: bool, a: Allocator, value: *std.json.Value) void {
    if (!borrowed) return freeJsonValue(a, value);
    switch (value.*) {
        .object => |*object| {
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                a.free(entry.key_ptr.*);
                freeProjectionValue(true, a, entry.value_ptr);
            }
            object.deinit(a);
        },
        .array => |*array| {
            for (array.items) |*item| freeProjectionValue(true, a, item);
            array.deinit();
        },
        else => {},
    }
    value.* = .null;
}
fn putCloneProjectionValue(comptime borrowed: bool, a: Allocator, object: *std.json.ObjectMap, key: []const u8, value: std.json.Value) Allocator.Error!void {
    var owned = try cloneProjectionValue(borrowed, a, value);
    errdefer freeProjectionValue(borrowed, a, &owned);
    try putProjectionValue(borrowed, a, object, key, owned);
}
fn putProjectionValue(comptime borrowed: bool, a: Allocator, object: *std.json.ObjectMap, key: []const u8, value: std.json.Value) Allocator.Error!void {
    if (!borrowed) return putOwnedValue(a, object, key, value);
    if (object.getPtr(key)) |existing| {
        freeProjectionValue(true, a, existing);
        existing.* = value;
        return;
    }
    const name = try a.dupe(u8, key);
    errdefer a.free(name);
    try object.put(a, name, value);
}

test "document query lookupJson projects nested fields and exclusions" {
    const alloc = std.testing.allocator;

    const raw =
        \\{"title":"alpha","author":{"name":"ann","age":42},"tags":[{"name":"db","score":1},{"name":"zig","score":2}],"body":"hello"}
    ;

    var result = try lookupJson(alloc, raw, .{
        .fields = &.{ "title", "author.name", "tags.name", "-body" },
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    try std.testing.expect(parsed.value.object.get("title") != null);
    try std.testing.expect(parsed.value.object.get("body") == null);
    try std.testing.expectEqualStrings("ann", parsed.value.object.get("author").?.object.get("name").?.string);
    const tags = parsed.value.object.get("tags").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), tags.len);
    try std.testing.expectEqualStrings("db", tags[0].object.get("name").?.string);
}

test "document query lookupJson returns full document when include_all_fields" {
    const alloc = std.testing.allocator;
    const raw = "{\"title\":\"alpha\",\"body\":\"hello\"}";

    var result = try lookupJson(alloc, raw, .{});
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings(raw, result.json);
}

test "document query lookupJson returns no stored fields for an explicit empty projection" {
    const alloc = std.testing.allocator;
    const raw = "{\"title\":\"alpha\",\"body\":\"hello\"}";

    var result = try lookupJson(alloc, raw, .{
        .fields = &.{},
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings("{}", result.json);
}

test "document query lookupJson supports indexed array paths" {
    const alloc = std.testing.allocator;

    const raw =
        \\{"title":"alpha","tags":[{"name":"db","score":1},{"name":"zig","score":2}],"body":"hello"}
    ;

    var result = try lookupJson(alloc, raw, .{
        .fields = &.{"tags.1.name"},
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    const tags = parsed.value.object.get("tags").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), tags.len);
    try std.testing.expectEqualStrings("zig", tags[0].object.get("name").?.string);
}

test "document query lookupJson supports indexed array exclusions" {
    const alloc = std.testing.allocator;

    const raw =
        \\{"title":"alpha","tags":[{"name":"db","score":1},{"name":"zig","score":2}],"body":"hello"}
    ;

    var result = try lookupJson(alloc, raw, .{
        .fields = &.{ "tags", "-tags.1.score" },
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    const tags = parsed.value.object.get("tags").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), tags.len);
    try std.testing.expectEqual(@as(i64, 1), tags[0].object.get("score").?.integer);
    try std.testing.expect(tags[1].object.get("score") == null);
}

fn projectionViewScenario(a: Allocator) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"body":"large immutable text","nested":{"visible":"keep","private":"omit"},"tags":[{"name":"db","score":1},{"name":"zig","score":2}],"amount":9007199254740993}
    , .{});
    defer parsed.deinit();
    const cases = [_]types.LookupOptions{
        .{},
        .{ .fields = &.{ "body", "nested.*", "tags.name", "amount", "-nested.private" }, .include_all_fields = false },
        .{ .fields = &.{ "tags.1.name", "-body" }, .include_all_fields = false },
        .{ .fields = &.{ "-nested.private", "-tags.*.score" } },
        .{ .fields = &.{ "body", "body", "nested", "nested.visible" }, .include_all_fields = false },
    };
    for (cases) |options| {
        var view = try projectLookupView(a, parsed.value, options);
        defer deinitProjectionView(a, &view);
        var owned = try projectLookupValue(a, parsed.value, options);
        defer freeJsonValue(a, &owned);
        const encoded = try std.json.Stringify.valueAlloc(a, view, .{});
        defer a.free(encoded);
        const expected = try std.json.Stringify.valueAlloc(a, owned, .{});
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, encoded);
        if (view.object.get("body")) |body| try std.testing.expect(body.string.ptr == parsed.value.object.get("body").?.string.ptr);
        try std.testing.expect(parsed.value.object.get("nested").?.object.get("private") != null);
    }
}
test "external lake projection views borrow lexemes and own containers across allocation failures" {
    try projectionViewScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, projectionViewScenario, .{});
}

test "public graph metric borrowed wire bounded projection writer response memory" {
    const a = std.testing.allocator;
    const Counter = @import("../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"values\":[");
    for (0..8192) |i| {
        if (i > 0) try input.writer.writeAll(",");
        try input.writer.writeAll("123456789");
    }
    try input.writer.writeAll("],\"omit\":true}");
    const opts: types.LookupOptions = .{ .fields = &.{"values"}, .include_all_fields = false };
    var previous_retained: usize = 0;
    for (0..2) |mode| {
        var counted: Counter = .{ .backing = a, .limit = 8 * 1024 * 1024 };
        var output = std.heap.ArenaAllocator.init(counted.allocator());
        var scratch = std.heap.ArenaAllocator.init(a);
        var body_bytes: usize = 0;
        for (0..64) |_| {
            const document = if (mode == 0) old: {
                const projected = try lookupJson(scratch.allocator(), input.written(), opts);
                break :old try output.allocator().dupe(u8, projected.json);
            } else (try lookupJsonWithOutput(scratch.allocator(), output.allocator(), input.written(), opts)).json;
            body_bytes += document.len;
            try std.testing.expect(std.mem.startsWith(u8, document, "{\"values\":[123456789"));
            _ = scratch.reset(.{ .retain_with_limit = 64 * 1024 });
        }
        std.debug.print("PROJECTION_RESPONSE mode={d} documents=64 body_bytes={d} retained_output_bytes={d} peak_output_bytes={d} allocations={d}\n", .{ mode, body_bytes, counted.live, counted.peak, counted.alloc_calls });
        if (mode == 0) previous_retained = counted.live else try std.testing.expect(counted.live <= previous_retained + 4096);
        scratch.deinit();
        output.deinit();
        try std.testing.expectEqual(@as(usize, 0), counted.live);
    }
}

test "public graph metric borrowed wire bounded native projection scratch response memory" {
    const a = std.testing.allocator;
    const ProbeSchema = @import("../schema.zig");
    const ProbeCodec = @import("algebraic/relational_row_codec.zig");
    const ProbePlan = @import("query/relational_projection.zig").Plan;
    const Counter = @import("../lite/test_allocator.zig").BudgetAllocator;
    const columns = [_]ProbeSchema.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any }};
    const table = ProbeSchema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try ProbeCodec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..1024) |i| {
        if (i > 0) try input.writer.writeAll(",");
        try input.writer.writeAll("{\"id\":123,\"junk\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}");
    }
    try input.writer.writeAll("]}");
    const cells = [_]ProbeCodec.Cell{.{ .ordinal = 0, .path = "payload", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = input.written() } }};
    const encoded = try ProbeCodec.serializeOrdinal(a, 1, &columns, &cells, @splat(0));
    defer a.free(encoded);
    const row = try ProbeCodec.ordinalRowViewTrusted(encoded, table, &layout);
    var previous_retained: usize = 0;
    for (0..2) |mode| {
        var counted: Counter = .{ .backing = a, .limit = 8 * 1024 * 1024 };
        var output = std.heap.ArenaAllocator.init(counted.allocator());
        var scratch = std.heap.ArenaAllocator.init(a);
        var body_bytes: usize = 0;
        for (0..16) |_| {
            var plan = try ProbePlan.init(scratch.allocator(), table, &layout, &.{"payload.items.id"});
            const document = if (mode == 0) old: {
                const projected = try plan.project(scratch.allocator(), row);
                break :old try output.allocator().dupe(u8, projected);
            } else try plan.projectWithOutput(scratch.allocator(), output.allocator(), row);
            body_bytes += document.len;
            try std.testing.expect(std.mem.startsWith(u8, document, "{\"payload\":{\"items\":[{\"id\":123}"));
            plan.deinit();
            _ = scratch.reset(.{ .retain_with_limit = 64 * 1024 });
        }
        std.debug.print("NATIVE_PROJECTION_RESPONSE mode={d} documents=16 body_bytes={d} retained_output_bytes={d} peak_output_bytes={d} allocations={d}\n", .{ mode, body_bytes, counted.live, counted.peak, counted.alloc_calls });
        if (mode == 0) previous_retained = counted.live else try std.testing.expect(counted.live <= previous_retained + 4096);
        scratch.deinit();
        output.deinit();
        try std.testing.expectEqual(@as(usize, 0), counted.live);
    }
}

test "public graph metric borrowed wire span projection preserves document selectors and errors" {
    const Case = struct {
        fn run(a: Allocator) !void {
            const inputs = [_][]const u8{
                "{\"a\":{\"x\":1e2,\"y\":true},\"b\":[{\"x\":1,\"y\":2},{\"x\":3},null,[1]],\"c\":null}",
                "{\"a\":{\"x\":\"hello\\nworld\",\"y\":9007199254740993},\"b\":[1,2]}",
                "{\"a\":{\"x\":null},\"b\":[]}",
                "42",
                "{\"\\u0061\":{\"\\u0078\":\"escaped key\"}}",
                "{\"a\":{\"x\":[{\"y\":1},{\"y\":2}]},\"b\":[{\"x\":1},{\"x\":2},3,{\"x\":4}]}",
            };
            const selections = [_][]const []const u8{
                &.{"a.x"},                       &.{ "b.x", "b.y" },                              &.{"b.01.x"},                 &.{"b.0"},
                &.{ "a", "a.x" },                &.{ "a.x", "a" },                                &.{ "c", "a.y" },             &.{ "missing.x", "a.missing" },
                &.{ "", ".a..x." },              &.{"*"},                                         &.{ "a", "-a.y" },            &.{"-b"},
                &.{ "b.x", "b.3.x", "b.0.x" },   &.{ "b.0.x", "b.x" },                            &.{ "b.3", "b.0", "b.01" },   &.{ "a.x.0.y", "a.x.1.y" },
                &.{ "a", "a.x.1.y", "a.x.0.y" }, &.{"b.999999999999999999999999999999999999999"}, &.{"b.18446744073709551615"}, &.{},
            };
            for (inputs) |raw| for (selections) |fields| {
                const opts: types.LookupOptions = .{ .fields = fields, .include_all_fields = false };
                var old = try lookupJson(a, raw, opts);
                defer old.deinit(a);
                var fresh = try lookupJsonWithOutput(a, a, raw, opts);
                defer fresh.deinit(a);
                try std.testing.expectEqualStrings(old.json, fresh.json);
            };
        }
    };
    try Case.run(std.testing.allocator);
    const opts: types.LookupOptions = .{ .fields = &.{"keep"}, .include_all_fields = false };
    for ([_][]const u8{ "{\"keep\":1,\"omit\":{\"x\":1,\"x\":2}}", "{\"keep\":1,\"omit\":[{\"x\":1,\"x\":2}]}" }) |raw|
        try std.testing.expectError(error.DuplicateField, lookupJsonWithOutput(std.testing.allocator, std.testing.allocator, raw, opts));
    for ([_][]const u8{ "{\"keep\":1,\"omit\":[}", "{\"keep\":1}{}", "", "{\"keep\":}", "{\"keep\":1,\"omit\":\"\\uZZZZ\"}" }) |raw| {
        if (lookupJsonWithOutput(std.testing.allocator, std.testing.allocator, raw, opts)) |result| {
            std.testing.allocator.free(result.json);
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expect(err != error.OutOfMemory);
    }
    const Fail = struct {
        fn run(a: Allocator) !void {
            var result = try lookupJsonWithOutput(a, a, "{\"keep\":[{\"n\":1}],\"omit\":[{\"n\":2},{\"n\":3}]}", .{ .fields = &.{ "keep.n", "keep.0.n" }, .include_all_fields = false });
            defer result.deinit(a);
            try std.testing.expectEqualStrings("{\"keep\":[{\"n\":1}]}", result.json);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fail.run, .{});
}

test "public graph metric borrowed wire span projection bounds omitted document scratch" {
    const a = std.testing.allocator;
    const Counter = @import("../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"keep\":7,\"omit\":[");
    for (0..16384) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"n\":123,\"s\":\"ignored\"}");
    }
    try input.writer.writeAll("]}");
    var peaks: [3]usize = undefined;
    for (0..3) |mode| {
        var counted: Counter = .{ .backing = a, .limit = if (mode == 0) 16 * 1024 * 1024 else 64 * 1024 };
        const opts: types.LookupOptions = .{ .fields = if (mode == 2) &.{"-omit"} else &.{"keep"}, .include_all_fields = false };
        var result = if (mode == 0) try lookupJson(counted.allocator(), input.written(), opts) else try lookupJsonWithOutput(counted.allocator(), a, input.written(), opts);
        try std.testing.expectEqualStrings("{\"keep\":7}", result.json);
        peaks[mode] = counted.peak;
        result.deinit(if (mode == 0) counted.allocator() else a);
        try std.testing.expectEqual(@as(usize, 0), counted.live);
    }
    try std.testing.expect(peaks[1] < peaks[0] / 10 and peaks[2] < peaks[0] / 10);
    std.debug.print("SPAN_PROJECTION omitted_objects=16384 dom_peak={d} span_peak={d} exclusion_peak={d}\n", .{ peaks[0], peaks[1], peaks[2] });
}

test "public graph metric borrowed wire span projection fuses validation and sparse array indices" {
    const a = std.testing.allocator;
    const Counter = @import("../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"arr\":[");
    for (0..20000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.print("{d}", .{i});
    }
    try input.writer.writeAll("]}");
    const fields = [_][]const u8{ "arr.19999", "arr.19990", "arr.19998", "arr.19997", "arr.19996", "arr.19995", "arr.19994", "arr.19993", "arr.19992", "arr.19991", "arr.19989", "arr.19988", "arr.19987", "arr.19986", "arr.19985", "arr.19984" };
    var budget: Counter = .{ .backing = a, .limit = 64 * 1024 };
    var result = try lookupJsonWithOutput(budget.allocator(), a, input.written(), .{ .fields = &fields, .include_all_fields = false });
    defer result.deinit(a);
    try std.testing.expectEqualStrings("{\"arr\":[19984]}", result.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("FUSED_ARRAY_PROJECTION rows=20000 selectors=16 scratch_peak={d}\n", .{budget.peak});
    // Inspect the navigation directly: unselected elements never become nodes.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const View = @import("query/json_view.zig").View;
    var view = View.init(arena.allocator(), input.written());
    try validateUniqueJson(arena.allocator(), input.written(), &view, try view.projectionPaths(&fields));
    const arr = &view.root.children.?[0].node;
    try std.testing.expect(arr.children == null);
    try std.testing.expect(arr.array_index.contains(std.math.maxInt(usize)));
    try std.testing.expectEqual(@as(u32, 17), arr.array_index.count());
}
