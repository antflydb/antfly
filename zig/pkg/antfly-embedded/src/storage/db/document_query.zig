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

/// Borrowed only during a synchronous search. Copies that change fields or
/// inclusion mode cannot accidentally reuse a plan for another projection.
pub const BorrowedSpanProjection = struct {
    plan: *const SpanProjectionPlan,
    fields: []const []const u8,
    include_all_fields: bool,
    pub fn matches(self: @This(), opts: types.LookupOptions) bool {
        return self.fields.ptr == opts.fields.ptr and self.fields.len == opts.fields.len and self.include_all_fields == opts.include_all_fields;
    }
};

pub const SpanProjectionPlan = struct {
    arena: std.heap.ArenaAllocator,
    selector: *const @import("query/json_projection_plan.zig").Selector,
    paths: []const @import("query/json_projection_plan.zig").Path,
    excluded: []const []const u8,
    include_all: bool,
    passthrough: bool,

    pub fn init(alloc: Allocator, opts: types.LookupOptions) !?@This() {
        if (!spanProjectionSupported(opts.fields)) return null;
        var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
        errdefer arena.deinit();
        const a = arena.allocator();
        var excluded: std.ArrayListUnmanaged([]const u8) = .empty;
        var has_positive = false;
        var has_negative = false;
        var include_all = false;
        for (opts.fields) |field| {
            if (field.len > 0 and field[0] == '-') {
                has_negative = true;
                if (field.len > 1) try excluded.append(a, try a.dupe(u8, field[1..]));
            } else {
                has_positive = true;
                include_all = include_all or std.mem.eql(u8, field, "*");
            }
        }
        var paths: std.ArrayListUnmanaged(@import("query/json_projection_plan.zig").Path) = .empty;
        for (opts.fields) |field| {
            if (field.len == 0 or field[0] == '-' or std.mem.eql(u8, field, "*")) continue;
            var parts: std.ArrayListUnmanaged([]const u8) = .empty;
            const owned = try a.dupe(u8, field);
            var tokens = std.mem.tokenizeScalar(u8, owned, '.');
            while (tokens.next()) |part| try parts.append(a, part);
            if (parts.items.len == 0) continue;
            const omit = for (excluded.items) |name| {
                if (std.mem.eql(u8, name, "*") or std.mem.eql(u8, name, parts.items[0])) break true;
            } else false;
            if (!omit) try paths.append(a, .{ .parts = parts.items });
        }
        const selector = try @import("query/json_projection_plan.zig").compile(a, paths.items);
        for (paths.items) |*path| {
            if (path.parts.len > 2) path.selector = if (paths.items.len == 1) selector else try @import("query/json_projection_plan.zig").compile(a, &.{path.*});
        }
        return .{
            .arena = arena,
            .selector = selector,
            .paths = paths.items,
            .excluded = excluded.items,
            .include_all = include_all or (!has_positive and has_negative),
            .passthrough = opts.fields.len == 0 and opts.include_all_fields,
        };
    }
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
    }
};

/// Per-row spans and projection containers use scratch; selector plans can be
/// shared across the request and remain immutable during row preparation.
pub const ProjectionSource = @import("query/json_view.zig").View.RootSource;
pub fn lookupJsonWithPlan(alloc: Allocator, output: Allocator, raw: []const u8, plan: *const SpanProjectionPlan) !types.LookupResult {
    return lookupJsonWithSources(alloc, output, raw, plan, &.{});
}
/// Synthetic fields replace their primary counterparts without constructing a
/// DOM of the primary document. Sources remain borrowed until serialization
/// completes; projected containers are independent so later paths can mutate them.
pub fn lookupJsonWithSources(alloc: Allocator, output: Allocator, raw: []const u8, plan: *const SpanProjectionPlan, sources: []const ProjectionSource) !types.LookupResult {
    return lookupJsonWithSourcesPolicy(alloc, output, raw, plan, sources, false);
}
pub fn lookupJsonWithSourcesPolicy(alloc: Allocator, output: Allocator, raw: []const u8, plan: *const SpanProjectionPlan, sources: []const ProjectionSource, exact: bool) !types.LookupResult {
    return lookupJsonDestination(alloc, output, raw, plan, sources, exact, null);
}
pub fn writeLookupFields(alloc: Allocator, writer: *std.Io.Writer, raw: []const u8, plan: *const SpanProjectionPlan) !void {
    _ = try lookupJsonDestination(alloc, alloc, raw, plan, &.{}, false, writer);
}
/// Stream an admitted stored/projected object without retaining a second body.
/// Keep the legacy number-normalization boundary when a literal _id forced a
/// logical parse; otherwise preserve original value lexemes and escape spelling.
pub fn writeRawScanFields(alloc: Allocator, writer: *std.Io.Writer, raw: []const u8) !void {
    if (try writePassthroughFields(alloc, writer, raw)) return;
    var plan = (try SpanProjectionPlan.init(alloc, .{})).?;
    defer plan.deinit();
    try writeLookupFields(alloc, writer, raw, &plan);
}
fn writePassthroughFields(alloc: Allocator, writer: *std.Io.Writer, raw: []const u8) !bool {
    if (std.mem.indexOf(u8, raw, "\"_id\"") != null) return false;
    const object = std.mem.trim(u8, raw, " \t\r\n");
    if (object.len < 2 or object[0] != '{' or object[object.len - 1] != '}') return error.InvalidProjectedDocumentJson;
    const fields = object[1 .. object.len - 1];
    if (std.mem.trim(u8, fields, " \t\r\n").len == 0) return true;
    if (std.mem.indexOfScalar(u8, object, '\\') == null) {
        try writer.writeByte(',');
        try writer.writeAll(fields);
        return true;
    }
    // Escaped root names can spell _id without the literal token. Decode only
    // keys and stream original member bytes, retaining no value-sized scratch.
    const work = @import("antfly_platform").allocator.reclaimingScratch(alloc);
    var scanner = std.json.Scanner.initCompleteInput(work, object);
    defer scanner.deinit();
    var keys = std.heap.ArenaAllocator.init(work);
    defer keys.deinit();
    _ = try scanner.next();
    while (try scanner.peekNextTokenType() != .object_end) {
        if (!keys.reset(.{ .retain_with_limit = 4096 })) return error.OutOfMemory;
        const start = scanner.cursor;
        const token = try scanner.nextAllocMax(keys.allocator(), .alloc_if_needed, object.len);
        const name = switch (token) {
            .string, .allocated_string => |key| key,
            else => return error.SyntaxError,
        };
        try scanner.skipValue();
        if (!std.mem.eql(u8, name, "_id")) {
            try writer.writeByte(',');
            try writer.writeAll(object[start..scanner.cursor]);
        }
    }
    _ = try scanner.next();
    if (try scanner.next() != .end_of_document) return error.SyntaxError;
    return true;
}
fn lookupJsonDestination(alloc: Allocator, output: Allocator, raw: []const u8, plan: *const SpanProjectionPlan, sources: []const ProjectionSource, exact: bool, writer: ?*std.Io.Writer) !types.LookupResult {
    if (writer == null and plan.passthrough and sources.len == 0) return .{ .json = try output.dupe(u8, raw) };
    if (writer != null and plan.passthrough and sources.len == 0 and try writePassthroughFields(alloc, writer.?, raw)) return .{ .json = @constCast("") };
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
    defer arena.deinit();
    const a = arena.allocator();
    const JsonView = @import("query/json_view.zig").View;
    var view = JsonView.init(a, raw);
    view.parse_numbers = true;
    var ranges = JsonView.ValidatedRanges.init(a);
    defer ranges.deinit();
    try view.prepareProjectionWithOptions(plan.selector, .all, .object_only, .{ .defer_fanout = true, .retain_root_fields = plan.include_all or plan.passthrough, .root_excluded = plan.excluded, .ranges = &ranges });
    const overlays = try a.alloc(JsonView.RootSource, sources.len);
    for (sources, overlays) |source, *overlay| overlay.* = source;
    view.root_sources = overlays;
    const Projection = @import("query/projection_output.zig").Output;
    var projection_owner = @import("query/projection_output.zig").Owner.init(a);
    defer projection_owner.deinit();
    var projected: Projection.Object = .empty;
    if (plan.include_all or plan.passthrough) {
        try Projection.includePreparedRootExcept(&projection_owner, &projected, &view, plan.excluded);
    } else for (plan.paths) |path| try Projection.includeRoot(&projection_owner, &projected, &view, path);
    if (writer) |sink| {
        try (Projection{ .object = projected }).writeFieldsTo(a, sink);
        return .{ .json = @constCast("") };
    }
    return .{ .json = try (Projection{ .object = projected }).serializeWithPolicy(a, output, exact) };
}

pub fn lookupJsonWithOutput(alloc: Allocator, output: Allocator, raw: []const u8, opts: types.LookupOptions) !types.LookupResult {
    if (opts.fields.len == 0 and opts.include_all_fields) return .{ .json = try output.dupe(u8, raw) };
    if (try SpanProjectionPlan.init(alloc, opts)) |value| {
        var plan = value;
        defer plan.deinit();
        return lookupJsonWithSourcesPolicy(alloc, output, raw, &plan, &.{}, opts.output_exact);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    var projected = try projectLookupView(alloc, parsed.value, opts);
    defer deinitProjectionView(alloc, &projected);
    return .{ .json = try serializeWithOutput(alloc, output, projected) };
}

pub fn spanProjectionSupported(fields: []const []const u8) bool {
    var positives: usize = 0;
    var all = false;
    for (fields) |field| {
        const negative = field.len > 0 and field[0] == '-';
        const name = if (negative) field[1..] else field;
        if (negative and std.mem.indexOfScalar(u8, name, '.') != null) return false;
        const special_wildcard = std.mem.eql(u8, name, "_embeddings.*") or std.mem.eql(u8, name, "_chunks.*") or std.mem.eql(u8, name, "_artifacts.*");
        if (std.mem.indexOfScalar(u8, name, '*') != null and !std.mem.eql(u8, name, "*") and !special_wildcard) return false;
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
/// Encode once in reclaimable scratch, then publish one exact-sized body.
/// Growing writers must never leave superseded buffers in a response arena.
pub fn serializeWithOutput(scratch: Allocator, output: Allocator, value: std.json.Value) ![]u8 {
    var writer = std.Io.Writer.Allocating.init(scratch);
    defer writer.deinit();
    std.json.Stringify.value(value, .{}, &writer.writer) catch return error.OutOfMemory;
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

/// Clone projection containers while borrowing immutable scalar storage.
/// Source owners must remain alive until the projected result is serialized.
pub fn applyIncludeBorrowedPath(alloc: Allocator, src: std.json.ObjectMap, dst: *std.json.ObjectMap, parts: []const []const u8) Allocator.Error!void {
    return applyIncludeRecursive(true, alloc, src, dst, parts, 0);
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
                &.{"a.x"},                     &.{ "b.x", "b.y" },                   &.{"b.01.x"},                    &.{"b.0"},
                &.{ "a", "a.x" },              &.{ "a.x", "a" },                     &.{ "c", "a.y" },                &.{ "missing.x", "a.missing" },
                &.{ "", ".a..x." },            &.{"*"},                              &.{ "a", "-a.y" },               &.{"-b"},
                &.{ "b.x", "b.3.x", "b.0.x" }, &.{ "b.00.x", "b.0.y", "b.x", "-a" }, &.{ "a.x.0.y", "-a", "b.0" },    &.{ "b.0.x", "b.x" },
                &.{ "b.3", "b.0", "b.01" },    &.{ "a.x.0.y", "a.x.1.y" },           &.{ "a", "a.x.1.y", "a.x.0.y" }, &.{"b.999999999999999999999999999999999999999"},
                &.{"b.18446744073709551615"},  &.{},
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
    var peaks: [4]usize = undefined;
    for (0..4) |mode| {
        var counted: Counter = .{ .backing = a, .limit = if (mode == 0) 16 * 1024 * 1024 else 64 * 1024 };
        const opts: types.LookupOptions = .{ .fields = if (mode == 3) &.{ "keep", "omit.n", "-omit" } else if (mode == 2) &.{"-omit"} else &.{"keep"}, .include_all_fields = false };
        var result = if (mode == 0) try lookupJson(counted.allocator(), input.written(), opts) else try lookupJsonWithOutput(counted.allocator(), a, input.written(), opts);
        try std.testing.expectEqualStrings("{\"keep\":7}", result.json);
        peaks[mode] = counted.peak;
        result.deinit(if (mode == 0) counted.allocator() else a);
        try std.testing.expectEqual(@as(usize, 0), counted.live);
    }
    try std.testing.expect(peaks[1] < peaks[0] / 10 and peaks[2] < peaks[0] / 10 and peaks[3] < 64 * 1024);
    std.debug.print("SPAN_PROJECTION omitted_objects=16384 dom_peak={d} span_peak={d} exclusion_peak={d} contradictory_peak={d}\n", .{ peaks[0], peaks[1], peaks[2], peaks[3] });
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
    var plan = (try SpanProjectionPlan.init(a, .{ .fields = &fields, .include_all_fields = false })).?;
    defer plan.deinit();
    try view.prepareProjection(plan.selector, .all, .object_only);
    const arr = &view.root.children.?[0].node;
    try std.testing.expect(arr.children == null);
    try std.testing.expectEqual(@as(u32, 16), arr.array_index.count());
}

test "public graph metric borrowed wire immutable selector plans share nested fanout state" {
    const a = std.testing.allocator;
    const opts: types.LookupOptions = .{ .fields = &.{ "items.a.b", "items.0.a.c", "items.00.a.d" }, .include_all_fields = false };
    var plan = (try SpanProjectionPlan.init(a, opts)).?;
    defer plan.deinit();
    const compiled_bytes = plan.arena.queryCapacity();
    for (0..16) |_| {
        var result = try lookupJsonWithPlan(a, a, "{\"items\":[{\"a\":{\"b\":1,\"c\":2,\"d\":3}},{\"a\":{\"b\":4}}]}", &plan);
        defer result.deinit(a);
        var old = try lookupJson(a, "{\"items\":[{\"a\":{\"b\":1,\"c\":2,\"d\":3}},{\"a\":{\"b\":4}}]}", opts);
        defer old.deinit(a);
        try std.testing.expectEqualStrings(old.json, result.json);
        try std.testing.expectEqual(compiled_bytes, plan.arena.queryCapacity());
    }
    const Fail = struct {
        fn run(failing: Allocator) !void {
            var owned = (try SpanProjectionPlan.init(failing, opts)).?;
            defer owned.deinit();
            var result = try lookupJsonWithPlan(failing, failing, "{\"items\":[{\"a\":{\"b\":1,\"c\":2,\"d\":3}}]}", &owned);
            defer result.deinit(failing);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fail.run, .{});
}

test "public graph metric borrowed wire selector compiler bounds depth and iterative union work" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const compiler = @import("query/json_projection_plan.zig");
    const parts = try a.alloc([]const u8, compiler.max_path_depth);
    defer a.free(parts);
    @memset(parts, "x");
    _ = try compiler.compile(arena.allocator(), &.{.{ .parts = parts }});
    const too_deep = try a.alloc([]const u8, 100000);
    defer a.free(too_deep);
    @memset(too_deep, "x");
    try std.testing.expectError(error.InvalidQueryRequest, compiler.compile(arena.allocator(), &.{.{ .parts = too_deep }}));
    // The previously crashing public constructor fails cleanly on shallow data.
    var path = std.Io.Writer.Allocating.init(a);
    defer path.deinit();
    try path.writer.writeAll("missing");
    for (0..100000) |_| try path.writer.writeAll(".x");
    try std.testing.expectError(error.InvalidQueryRequest, lookupJsonWithOutput(a, a, "{}", .{ .fields = &.{path.written()}, .include_all_fields = false }));
    const aliases = try a.alloc(compiler.Path, compiler.max_components + 1);
    defer a.free(aliases);
    @memset(aliases, .{ .parts = &.{"x"} });
    try std.testing.expectError(error.InvalidQueryRequest, compiler.compile(arena.allocator(), aliases));
}

test "public graph metric borrowed wire wide projection retains only selected object spans" {
    const a = std.testing.allocator;
    const Counter = @import("../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"keep\":1");
    for (0..20000) |i| try input.writer.print(",\"field{d}\":0", .{i});
    try input.writer.writeByte('}');
    var plan = (try SpanProjectionPlan.init(a, .{ .fields = &.{"keep"}, .include_all_fields = false })).?;
    defer plan.deinit();
    var counter: Counter = .{ .backing = a, .limit = 4 * 1024 * 1024 };
    var result = try lookupJsonWithPlan(counter.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqualStrings("{\"keep\":1}", result.json);
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    std.debug.print("WIDE_OBJECT_FIXED fields=20000 input_bytes={d} scratch_peak={d}\n", .{ input.written().len, counter.peak });
    var arena2 = std.heap.ArenaAllocator.init(a);
    defer arena2.deinit();
    const View = @import("query/json_view.zig").View;
    var view = View.init(arena2.allocator(), input.written());
    try view.prepareProjection(plan.selector, .all, .object_only);
    try std.testing.expectEqual(@as(usize, 1), view.root.children.?.len);
    try std.testing.expectEqual(@as(u32, 1), view.root.object_index.count());
    try std.testing.expectError(error.DuplicateField, lookupJsonWithPlan(a, a, "{\"keep\":1,\"skip\":2,\"skip\":3}", &plan));
    var all = try lookupJsonWithOutput(a, a, "{\"keep\":1,\"skip\":2}", .{ .fields = &.{"-skip"}, .include_all_fields = false });
    defer all.deinit(a);
    try std.testing.expectEqualStrings("{\"keep\":1}", all.json);
}

test "public graph metric borrowed wire compact fanout object spans bound total projection scratch" {
    const a = std.testing.allocator;
    const Counter = @import("../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"a\":{\"b\":1,\"c\":2},\"skip\":3}");
    }
    try input.writer.writeAll("]}");
    var counter: Counter = .{ .backing = a, .limit = (switch (@import("builtin").mode) {
        .debug, .safe => @as(usize, 20),
        .fast, .small => 12,
    }) * 1024 * 1024 };
    var plan = (try SpanProjectionPlan.init(a, .{ .fields = &.{"items.a.b"}, .include_all_fields = false })).?;
    defer plan.deinit();
    var result = try lookupJsonWithPlan(counter.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 112011), result.json.len);
    try std.testing.expect(std.mem.startsWith(u8, result.json, "{\"items\":[{\"a\":{\"b\":1}}"));
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    std.debug.print("COMPACT_FANOUT rows=8000 scratch_peak={d}\n", .{counter.peak});
}

test "public graph metric borrowed wire exact policy keeps raw passthrough independent of scratch" {
    const a = std.testing.allocator;
    var plan = (try SpanProjectionPlan.init(a, .{})).?;
    defer plan.deinit();
    var refused = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    const raw = "{\"items\":[1,2,3]}";
    var result = try lookupJsonWithSourcesPolicy(refused.allocator(), a, raw, &plan, &.{}, true);
    defer result.deinit(a);
    try std.testing.expectEqualStrings(raw, result.json);
    try std.testing.expectEqual(@as(usize, 0), refused.alloc_index);
}

test "public graph metric borrowed wire raw scan fields preserve lexemes and reserve decoded identity" {
    const a = std.testing.allocator;
    const inputs = [_][]const u8{
        "{}",                               "{ }",                                  "  {\n}\t",                       "{\"n\":1e2}",
        "{\"n\":1e2,\"text\":\"\\u0061\"}", "{\"\\u005fid\":\"stored\",\"n\":1e2}", "{\"_id\":\"stored\",\"n\":1e2}",
    };
    const expected = [_][]const u8{ "", "", "", ",\"n\":1e2", ",\"n\":1e2,\"text\":\"\\u0061\"", ",\"n\":1e2", ",\"n\":100" };
    for (inputs, expected) |raw, want| {
        var writer = std.Io.Writer.Allocating.init(a);
        defer writer.deinit();
        try writeRawScanFields(a, &writer.writer, raw);
        try std.testing.expectEqualStrings(want, writer.written());
    }
}
