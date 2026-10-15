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
const Value = std.json.Value;
const EmbeddingFields = @import("embedding_fields.zig").EmbeddingFields;
const View = @import("json_view.zig").View;

/// Row-owned projection structure. Terminal values borrow immutable sources;
/// containers become mutable only when an ordered selector needs to change one.
/// Neither the view nor a loaded source is ever mutated by projection.
pub const Source = union(enum) {
    span: struct { view: *View, node: *View.Node },
    logical: *const Value,
    embeddings: *const EmbeddingFields,
    embedding_stream: *@import("embedding_fields.zig").EmbeddingStream,
    vector: []const f32,
    component: f32,

    fn kind(self: Source) u8 {
        return switch (self) {
            .embeddings, .embedding_stream => '{',
            .vector => '[',
            .component => 0,
            .span => |s| s.node.kind(),
            .logical => |v| switch (v.*) {
                .object => '{',
                .array => '[',
                else => 0,
            },
        };
    }
    fn field(self: Source, alloc: Allocator, name: []const u8) !?Source {
        return switch (self) {
            .embedding_stream => |stream| if (try stream.getField(alloc, name)) |v| .{ .vector = v } else null,
            .embeddings => |fields| if (fields.values.get(name)) |v| .{ .vector = v } else null,
            .vector, .component => null,
            .span => |s| if (try s.view.fieldSparse(s.node, name)) |node| .{ .span = .{ .view = s.view, .node = node } } else null,
            .logical => |v| if (v.* == .object) (if (v.object.getPtr(name)) |value| .{ .logical = value } else null) else null,
        };
    }
    fn element(self: Source, index: usize) !?Source {
        return switch (self) {
            .vector => |v| if (index < v.len) .{ .component = v[index] } else null,
            .embeddings, .embedding_stream, .component => null,
            .span => |s| if (try s.view.element(s.node, index)) |node| .{ .span = .{ .view = s.view, .node = node } } else null,
            .logical => |v| if (v.* == .array and index < v.array.items.len) .{ .logical = &v.array.items[index] } else null,
        };
    }
};

pub const Output = union(enum) {
    borrowed: Source,
    object: Object,
    overlay: *Overlay,
    array: std.ArrayListUnmanaged(Output),
    fanout: *Fanout,
    const Fanout = struct {
        source: Source,
        path: []const []const u8,
        selector: ?*const @import("json_projection_plan.zig").Selector = null,
        prepared: ?*FanoutRows = null,
        first: ?Object = null,
    };
    pub const Object = std.StringArrayHashMapUnmanaged(Output);
    // Keep this behind a pointer: ordinary fanout entries must not pay for a
    // base source plus an override map when no copy-on-write is needed.
    const Overlay = struct { base: Source, changed: Object = .empty };
    const Target = struct { map: *Object, base: ?Source = null };

    fn objectTarget(self: *Output, a: Allocator) !Target {
        switch (self.*) {
            .object => |*object| return .{ .map = object },
            .overlay => |overlay| return .{ .map = &overlay.changed, .base = overlay.base },
            .borrowed => |source| if (source.kind() == '{') {
                const overlay = try a.create(Overlay);
                overlay.* = .{ .base = source };
                self.* = .{ .overlay = overlay };
                return .{ .map = &overlay.changed, .base = source };
            },
            else => {},
        }
        self.* = .{ .object = .empty };
        return .{ .map = &self.object };
    }

    fn put(a: Allocator, object: *Object, name: []const u8, value: Output) !void {
        if (object.entries.capacity == 0) try object.entries.setCapacity(a, 1);
        try object.put(a, name, value);
    }

    fn copyObject(a: Allocator, dst: *Object, source: Source) !void {
        switch (source) {
            .embedding_stream => |stream| try copyObject(a, dst, .{ .embeddings = try stream.fields(a) }),
            .embeddings => |fields| for (fields.values.keys(), fields.values.values()) |name, vector| try put(a, dst, name, .{ .borrowed = .{ .vector = vector } }),
            .vector, .component => unreachable,
            .logical => |v| for (v.object.keys(), v.object.values()) |name, *value| {
                try put(a, dst, name, .{ .borrowed = .{ .logical = value } });
            },
            .span => |s| {
                // A native view may belong to a retained column page. Copy-on-
                // write navigation belongs to this output row, not that page.
                const shallow = try a.create(View);
                shallow.* = .init(a, s.node.raw);
                shallow.parse_numbers = s.view.parse_numbers;
                const children = try shallow.children(&shallow.root);
                const names = try a.alloc([]const u8, children.len);
                var it = shallow.root.object_index.iterator();
                while (it.next()) |entry| if (entry.value_ptr.* < names.len) {
                    names[entry.value_ptr.*] = entry.key_ptr.*;
                };
                for (children, names) |*child, name| try put(a, dst, name, .{ .borrowed = .{ .span = .{ .view = shallow, .node = &child.node } } });
            },
        }
    }

    /// Apply includes in caller order. Object selections merge; every array
    /// selection produces a fresh compact array, preserving legacy replacement.
    pub fn include(a: Allocator, dst: *Object, name: []const u8, source: Source, path: []const []const u8) anyerror!void {
        return includeWithBase(a, .{ .map = dst }, name, source, path);
    }
    fn includeWithBase(a: Allocator, target: Target, name: []const u8, source: Source, path: []const []const u8) anyerror!void {
        const dst = target.map;
        if (path.len == 0 or (source == .embedding_stream and path.len == 1 and std.mem.eql(u8, path[0], "*"))) return put(a, dst, name, .{ .borrowed = source });
        switch (source.kind()) {
            '{' => {
                if (!dst.contains(name)) {
                    const previous = if (target.base) |base| try base.field(a, name) else null;
                    try put(a, dst, name, if (previous) |child| .{ .borrowed = child } else .{ .object = .empty });
                }
                const nested = try dst.getPtr(name).?.objectTarget(a);
                if (std.mem.eql(u8, path[0], "*")) {
                    var all: Object = .empty;
                    try copyObject(a, &all, source);
                    for (all.keys(), all.values()) |key, child| try includeWithBase(a, nested, key, child.borrowed, path[1..]);
                } else if (try source.field(a, path[0])) |child| try includeWithBase(a, nested, path[0], child, path[1..]);
            },
            '[' => {
                var result: std.ArrayListUnmanaged(Output) = .empty;
                if (View.projectionIndex(path[0])) |index| {
                    const item = (try source.element(index)) orelse return;
                    if (path.len == 1) {
                        try result.append(a, .{ .borrowed = item });
                    } else if (item.kind() == '{') {
                        var object: Object = .empty;
                        try includeObjectPath(a, &object, item, path[1..]);
                        if (object.count() != 0) try result.append(a, .{ .object = object });
                    }
                } else {
                    // Keep the primed cursor at a stable address. All of its
                    // allocations belong to the enclosing row arena, including
                    // abandoned/replaced selectors and failed preparations.
                    // Serialization resumes it instead of replaying nested
                    // survival checks. No-survivor selectors leave dst alone.
                    const fanout = try a.create(Fanout);
                    fanout.* = .{ .source = source, .path = path, .selector = if (source == .span and path.len > 1) try @import("json_projection_plan.zig").compile(a, &.{.{ .parts = path }}) else null };
                    const rows = try a.create(FanoutRows);
                    rows.* = FanoutRows.initOwned(a, fanout.*);
                    // Primed rows may be replaced or abandoned. Their retained
                    // navigation belongs to a; no independent scratch escapes.
                    defer rows.preparation.deinit();
                    if (try rows.next()) |first| {
                        fanout.prepared = rows;
                        fanout.first = first;
                        try put(a, dst, name, .{ .fanout = fanout });
                    }
                    return;
                }
                if (result.items.len != 0) try put(a, dst, name, .{ .array = result });
            },
            else => {},
        }
    }
    fn includeObjectPath(a: Allocator, dst: *Object, source: Source, path: []const []const u8) !void {
        if (std.mem.eql(u8, path[0], "*")) {
            var all: Object = .empty;
            try copyObject(a, &all, source);
            for (all.keys(), all.values()) |name, value| try include(a, dst, name, value.borrowed, path[1..]);
        } else if (try source.field(a, path[0])) |child| try include(a, dst, path[0], child, path[1..]);
    }

    pub fn includeRoot(a: Allocator, dst: *Object, view: *View, path: []const []const u8) !void {
        if (path.len == 0) return;
        for (view.root_sources) |*source| if (std.mem.eql(u8, source.name, path[0])) {
            return include(a, dst, source.name, source.outputSource(), path[1..]);
        };
        if (try view.field(&view.root, path[0])) |node| try include(a, dst, path[0], .{ .span = .{ .view = view, .node = node } }, path[1..]);
    }

    pub fn includePreparedRootExcept(a: Allocator, dst: *Object, view: *View, excluded: []const []const u8) !void {
        if (omitted(excluded, "*")) return;
        const children = view.root.children orelse &.{};
        const names = try a.alloc([]const u8, children.len);
        var it = view.root.object_index.iterator();
        while (it.next()) |entry| if (entry.value_ptr.* < names.len) {
            names[entry.value_ptr.*] = entry.key_ptr.*;
        };
        for (names) |name| if (!omitted(excluded, name)) try includeRoot(a, dst, view, &.{name});
        for (view.root_sources) |*source| {
            if (omitted(excluded, source.name) or dst.contains(source.name)) continue;
            try put(a, dst, source.name, .{ .borrowed = source.outputSource() });
        }
    }
    fn omitted(excluded: []const []const u8, name: []const u8) bool {
        for (excluded) |entry| if (std.mem.eql(u8, name, entry)) return true;
        return false;
    }

    pub fn serialize(self: Output, scratch: Allocator, output: Allocator) ![]u8 {
        return self.serializeWithPolicy(scratch, output, false);
    }
    /// Carry allocation policy through wrappers without unwrapping their
    /// synchronization or quota accounting. Only growing writers need scratch.
    pub fn serializeWithPolicy(self: Output, scratch: Allocator, output: Allocator, exact: bool) ![]u8 {
        const reclaimable = @import("antfly_platform").allocator.reclaimingScratch(output);
        // An arena cannot reclaim superseded growing buffers. Preserve one
        // exact allocation there; ordinary owners can take the writer buffer.
        const arena_output = exact or reclaimable.ptr != output.ptr or reclaimable.vtable != output.vtable;
        var writer = std.Io.Writer.Allocating.init(if (arena_output) @import("antfly_platform").allocator.reclaimingScratch(scratch) else output);
        defer writer.deinit();
        self.writeTo(scratch, &writer.writer) catch |err| switch (err) {
            error.WriteFailed => return error.OutOfMemory,
            else => return err,
        };
        return if (arena_output) output.dupe(u8, writer.written()) else writer.toOwnedSlice();
    }

    /// A transport with its own writer can stream the body without allocating
    /// an intermediate response. The source remains borrowed until completion.
    pub fn writeTo(self: Output, scratch: Allocator, writer: *std.Io.Writer) !void {
        var context = StreamContext.init(scratch);
        defer context.deinit();
        try self.write(&context, writer);
    }
    fn write(self: Output, context: *StreamContext, writer: *std.Io.Writer) anyerror!void {
        switch (self) {
            .borrowed => |source| switch (source) {
                .embedding_stream => |stream| try stream.write(writer),
                .span => |s| try context.writeSpan(writer, s.node.raw, s.view.parse_numbers),
                .logical => |v| try std.json.Stringify.value(v.*, .{}, writer),
                .component => |v| try std.json.Stringify.value(@as(f64, v), .{}, writer),
                .vector => |v| try writeVector(writer, v),
                .embeddings => |fields| {
                    try writer.writeByte('{');
                    for (fields.values.keys(), fields.values.values(), 0..) |name, vector, i| {
                        if (i != 0) try writer.writeByte(',');
                        try std.json.Stringify.value(name, .{}, writer);
                        try writer.writeByte(':');
                        try writeVector(writer, vector);
                    }
                    try writer.writeByte('}');
                },
            },
            .overlay => |overlay| try writeOverlay(context, writer, overlay),
            .object => |object| {
                try writer.writeByte('{');
                for (object.keys(), object.values(), 0..) |name, child, i| {
                    if (i != 0) try writer.writeByte(',');
                    try std.json.Stringify.value(name, .{}, writer);
                    try writer.writeByte(':');
                    try child.write(context, writer);
                }
                try writer.writeByte('}');
            },
            .fanout => |fanout| {
                var fallback = FanoutRows.init(context.work, fanout.*);
                const rows = fanout.prepared orelse &fallback;
                // Detach before writing: errors and repeated writes must never
                // reuse a partially consumed cursor or its retired first row.
                const first = fanout.first;
                fanout.prepared = null;
                fanout.first = null;
                defer rows.deinit();
                try writer.writeByte('[');
                var count: usize = 0;
                if (first) |object| {
                    try (Output{ .object = object }).write(context, writer);
                    count = 1;
                    // Only the primed row uses its enclosing arena. Subsequent
                    // siblings share independently reclaimable bounded scratch.
                    rows.arena.deinit();
                    rows.arena = .init(context.work);
                    rows.preparation = .init(context.work);
                }
                while (try rows.next()) |object| {
                    if (count != 0) try writer.writeByte(',');
                    try (Output{ .object = object }).write(context, writer);
                    count += 1;
                }
                try writer.writeByte(']');
            },
            .array => |array| {
                try writer.writeByte('[');
                for (array.items, 0..) |child, i| {
                    if (i != 0) try writer.writeByte(',');
                    try child.write(context, writer);
                }
                try writer.writeByte(']');
            },
        }
    }

    fn writeVector(writer: *std.Io.Writer, vector: []const f32) !void {
        try writer.writeByte('[');
        for (vector, 0..) |component, i| {
            if (i != 0) try writer.writeByte(',');
            try std.json.Stringify.value(@as(f64, component), .{}, writer);
        }
        try writer.writeByte(']');
    }
    fn writeMember(context: *StreamContext, writer: *std.Io.Writer, count: *usize, name: []const u8, value: Output) !void {
        if (count.* != 0) try writer.writeByte(',');
        try std.json.Stringify.value(name, .{}, writer);
        try writer.writeByte(':');
        try value.write(context, writer);
        count.* += 1;
    }
    fn writeBaseMember(context: *StreamContext, writer: *std.Io.Writer, overlay: *const Overlay, emitted: []bool, count: *usize, name: []const u8, source: Source) !void {
        if (overlay.changed.getIndex(name)) |index| {
            try writeMember(context, writer, count, name, overlay.changed.values()[index]);
            emitted[index] = true;
        } else try writeMember(context, writer, count, name, .{ .borrowed = source });
    }
    fn writeOverlay(context: *StreamContext, writer: *std.Io.Writer, overlay: *const Overlay) anyerror!void {
        const work = context.work;
        const emitted = try work.alloc(bool, overlay.changed.count());
        defer work.free(emitted);
        @memset(emitted, false);
        var count: usize = 0;
        try writer.writeByte('{');
        switch (overlay.base) {
            .vector, .component => unreachable,
            .embedding_stream => |stream| {
                if (stream.cached_complete) {
                    const fields = &stream.cached.?;
                    for (fields.values.keys(), fields.values.values()) |name, vector| try writeBaseMember(context, writer, overlay, emitted, &count, name, .{ .vector = vector });
                } else {
                    try stream.reset_fn(stream.ctx);
                    while (try stream.next_fn(stream.ctx)) |field| try writeBaseMember(context, writer, overlay, emitted, &count, field.name, .{ .vector = field.vector });
                }
            },
            .embeddings => |fields| for (fields.values.keys(), fields.values.values()) |name, vector| try writeBaseMember(context, writer, overlay, emitted, &count, name, .{ .vector = vector }),
            .logical => |value| for (value.object.keys(), value.object.values()) |name, *child| {
                try writeBaseMember(context, writer, overlay, emitted, &count, name, .{ .logical = child });
            },
            .span => |span| {
                var scanner = std.json.Scanner.initCompleteInput(work, span.node.raw);
                defer scanner.deinit();
                var keys = std.heap.ArenaAllocator.init(work);
                defer keys.deinit();
                var names: std.StringHashMapUnmanaged(void) = .empty;
                var token_arena = std.heap.ArenaAllocator.init(work);
                defer token_arena.deinit();
                if (try scanner.next() != .object_begin) return error.SyntaxError;
                while (try scanner.peekNextTokenType() != .object_end) {
                    try resetScratch(&token_arena);
                    const token = try scanner.nextAllocMax(token_arena.allocator(), .alloc_if_needed, span.node.raw.len);
                    const name = switch (token) {
                        .string, .allocated_string => |name| name,
                        else => return error.SyntaxError,
                    };
                    const owned_name = if (token == .allocated_string) try keys.allocator().dupe(u8, name) else name;
                    if ((try names.getOrPut(keys.allocator(), owned_name)).found_existing) return error.DuplicateField;
                    switch (try scanner.peekNextTokenType()) {
                        .object_end, .array_end, .end_of_document => return error.SyntaxError,
                        else => {},
                    }
                    if (overlay.changed.getIndex(name)) |index| {
                        // Discarded bytes still need syntax validation. An
                        // unchanged value is consumed and emitted in one pass.
                        try scanner.skipValue();
                        try writeMember(context, writer, &count, name, overlay.changed.values()[index]);
                        emitted[index] = true;
                    } else {
                        if (count != 0) try writer.writeByte(',');
                        try std.json.Stringify.value(name, .{}, writer);
                        try writer.writeByte(':');
                        try context.writeValue(writer, &scanner, span.node.raw.len, span.view.parse_numbers);
                        count += 1;
                    }
                }
                _ = try scanner.next();
                if (try scanner.next() != .end_of_document) return error.SyntaxError;
            },
        }
        for (overlay.changed.keys(), overlay.changed.values(), emitted) |name, child, seen| {
            if (!seen) try writeMember(context, writer, &count, name, child);
        }
        try writer.writeByte('}');
    }
};

/// Retire large buffers without allocating a replacement just to shrink one.
/// Small arenas consolidate once and then reset in place across siblings.
fn resetScratch(arena: *std.heap.ArenaAllocator) Allocator.Error!void {
    if (arena.queryCapacity() > 16 * 1024) {
        _ = arena.reset(.free_all);
    } else if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
}

/// One bounded pool for all terminals in a response. No borrowed token or
/// duplicate-key entry survives its container; completed depths are reusable.
const StreamContext = struct {
    work: Allocator,
    tokens: std.heap.ArenaAllocator,
    scanners: std.heap.ArenaAllocator,
    keys: @import("antfly-json").ObjectKeyWorkspace(FrameState),
    const FrameState = struct { object: bool, key: bool = true, count: usize = 0 };
    fn init(scratch: Allocator) StreamContext {
        const work = @import("antfly_platform").allocator.reclaimingScratch(scratch);
        return .{ .work = work, .tokens = .init(work), .scanners = .init(work), .keys = .init(work) };
    }
    fn deinit(self: *StreamContext) void {
        self.keys.deinit();
        self.tokens.deinit();
        self.scanners.deinit();
    }
    fn writeSpan(self: *StreamContext, writer: *std.Io.Writer, raw: []const u8, parse_numbers: bool) !void {
        try resetScratch(&self.scanners);
        var scanner = std.json.Scanner.initCompleteInput(self.scanners.allocator(), raw);
        defer scanner.deinit();
        errdefer self.keys.deinit();
        try self.writeValue(writer, &scanner, raw.len, parse_numbers);
        if (try scanner.next() != .end_of_document) return error.SyntaxError;
    }
    fn writeValue(self: *StreamContext, writer: *std.Io.Writer, scanner: *std.json.Scanner, max_len: usize, parse_numbers: bool) !void {
        errdefer self.keys.deinit();
        var root_seen = false;
        // Reuse arenas across siblings, but keep each live ancestor's duplicate
        // keys until its object ends. Retention is capped at every reuse.
        var depth: usize = 0;
        while (!root_seen or depth != 0) {
            try resetScratch(&self.tokens);
            const expecting_key = depth != 0 and self.keys.stack.items[depth - 1].state.object and self.keys.stack.items[depth - 1].state.key;
            const token = if (expecting_key)
                try self.keys.nextKey(depth - 1, scanner, max_len)
            else
                try scanner.nextAllocMax(self.tokens.allocator(), .alloc_if_needed, max_len);
            if (token == .end_of_document) {
                if (!root_seen or depth != 0) return error.SyntaxError;
                break;
            }
            if (token == .object_end or token == .array_end) {
                try writer.writeByte(if (token == .object_end) '}' else ']');
                if (depth == 0) return error.SyntaxError;
                depth -= 1;
                try self.keys.finishFrame(depth);
                continue;
            }
            if (depth == 0) {
                if (root_seen) return error.SyntaxError;
                root_seen = true;
            }
            if (depth != 0) {
                const frame = &self.keys.stack.items[depth - 1].state;
                if (frame.object and frame.key) {
                    const name = switch (token) {
                        .string, .allocated_string => |s| s,
                        else => return error.SyntaxError,
                    };
                    try self.keys.remember(depth - 1, name, false);
                    if (frame.count != 0) try writer.writeByte(',');
                    try std.json.Stringify.value(name, .{}, writer);
                    try writer.writeByte(':');
                    frame.count += 1;
                    frame.key = false;
                    continue;
                }
                if (frame.object) {
                    frame.key = true;
                } else {
                    if (frame.count != 0) try writer.writeByte(',');
                    frame.count += 1;
                }
            }
            switch (token) {
                .object_begin, .array_begin => {
                    try writer.writeByte(if (token == .object_begin) '{' else '[');
                    try self.keys.enter(depth, .{ .object = token == .object_begin });
                    depth += 1;
                },
                .string, .allocated_string => |s| try std.json.Stringify.value(s, .{}, writer),
                .number, .allocated_number => |s| try std.json.Stringify.value(if (parse_numbers) Value.parseFromNumberSlice(s) else Value{ .number_string = s }, .{}, writer),
                .true => try writer.writeAll("true"),
                .false => try writer.writeAll("false"),
                .null => try writer.writeAll("null"),
                else => return error.SyntaxError,
            }
        }
        self.keys.finishValue();
    }
};

/// Each returned object borrows this iterator's row arena and view until next.
/// In particular, navigation never enters a retained native column-page view.
const FanoutRows = struct {
    source: Source,
    path: []const []const u8,
    selector: ?*const @import("json_projection_plan.zig").Selector,
    arena: std.heap.ArenaAllocator,
    preparation: View.PreparationWorkspace,
    scanner: ?std.json.Scanner = null,
    started: bool = false,
    index: usize = 0,
    view: View = undefined,
    fn init(a: Allocator, fanout: Output.Fanout) FanoutRows {
        return initOwned(@import("antfly_platform").allocator.reclaimingScratch(a), fanout);
    }
    fn initOwned(work: Allocator, fanout: Output.Fanout) FanoutRows {
        return .{ .source = fanout.source, .path = fanout.path, .selector = fanout.selector, .arena = .init(work), .preparation = .init(work), .scanner = switch (fanout.source) {
            .span => |span| std.json.Scanner.initCompleteInput(work, span.node.raw),
            .logical, .embeddings, .embedding_stream, .vector, .component => null,
        } };
    }
    fn deinit(self: *FanoutRows) void {
        if (self.scanner) |*scanner| scanner.deinit();
        self.preparation.deinit();
        self.arena.deinit();
    }
    fn next(self: *FanoutRows) anyerror!?Output.Object {
        while (true) {
            try resetScratch(&self.arena);
            const a = self.arena.allocator();
            const item: Source = switch (self.source) {
                .vector => return null,
                .embeddings, .embedding_stream, .component => unreachable,
                .logical => |value| blk: {
                    if (self.index == value.array.items.len) return null;
                    const item = &value.array.items[self.index];
                    self.index += 1;
                    break :blk .{ .logical = item };
                },
                .span => |span| blk: {
                    const scanner = &self.scanner.?;
                    if (!self.started) {
                        if (try scanner.next() != .array_begin) return error.SyntaxError;
                        self.started = true;
                    }
                    const token = try scanner.peekNextTokenType();
                    if (token == .array_end) {
                        _ = try scanner.next();
                        if (try scanner.next() != .end_of_document) return error.SyntaxError;
                        return null;
                    }
                    if (token == .object_end or token == .end_of_document) return error.SyntaxError;
                    const start = scanner.cursor;
                    if (token == .object_begin and self.selector != null) {
                        // Bound navigation to this item. Preparing the complete
                        // selected path once avoids rescanning every ancestor's
                        // remaining subtree for a deep field selection.
                        self.view = .init(a, span.node.raw[start..]);
                        try self.view.prepareProjectionValueWithWorkspace(self.selector.?, .visited, .object_only, .{ .defer_fanout = true }, scanner, span.node.raw, &self.preparation);
                    } else if (token == .object_begin and !std.mem.eql(u8, self.path[0], "*")) {
                        // Discover the selected root member while advancing
                        // the array scanner, avoiding a second scan per item.
                        _ = try scanner.next();
                        var names: std.StringHashMapUnmanaged(void) = .empty;
                        var selected: ?[]const u8 = null;
                        while (try scanner.peekNextTokenType() != .object_end) {
                            const key = switch (try scanner.nextAllocMax(a, .alloc_if_needed, span.node.raw.len)) {
                                .string, .allocated_string => |key| key,
                                else => return error.SyntaxError,
                            };
                            if (!span.node.state.fanout_keys_validated) if ((try names.getOrPut(a, key)).found_existing) return error.DuplicateField;
                            switch (try scanner.peekNextTokenType()) {
                                .object_end, .array_end, .end_of_document => return error.SyntaxError,
                                else => {},
                            }
                            const value_start = scanner.cursor;
                            try scanner.skipValue();
                            if (std.mem.eql(u8, key, self.path[0])) selected = span.node.raw[value_start..scanner.cursor];
                        }
                        _ = try scanner.next();
                        self.view = try View.initProjectedObject(a, span.node.raw[start..scanner.cursor], self.path[0], selected);
                    } else {
                        try scanner.skipValue();
                        self.view = .init(a, span.node.raw[start..scanner.cursor]);
                    }
                    self.view.parse_numbers = span.view.parse_numbers;
                    break :blk .{ .span = .{ .view = &self.view, .node = &self.view.root } };
                },
            };
            if (item.kind() != '{') continue;
            var object: Output.Object = .empty;
            try Output.includeObjectPath(a, &object, item, self.path);
            if (object.count() != 0) return object;
        }
    }
};

fn differentialScenario(a: Allocator) !void {
    const query = @import("../document_query.zig");
    const raw = "{\"a\":{\"numbers\":[1.00,1e2,-0,9223372036854775808,1e999],\"text\":\"escape\\n\\u0061\",\"other\":true},\"items\":[{\"x\":{\"m\":1,\"n\":2},\"z\":4},{\"x\":{\"m\":3},\"z\":5},null],\"omit\":null}";
    const selections = [_][]const []const u8{
        &.{"a"},                    &.{"a.numbers"},                                 &.{"a.text"},
        &.{ "a", "a.numbers.0" },   &.{ "a.numbers.0", "a" },                        &.{ "items", "items.x.m" },
        &.{ "items.x.m", "items" }, &.{ "items.0.x", "items.1.x.n", "items.0.x.m" }, &.{ "a", "a", "-omit" },
        &.{"-omit"},                &.{"*"},
    };
    const overlay_selections = [_][]const []const u8{
        &.{ "a", "a.numbers.0", "a.numbers.1" },
        &.{ "a.numbers.0", "a", "a.numbers.1" },
        &.{ "a", "a.missing.value" },
        &.{ "items.0.x", "items", "items.1.x.m" },
    };
    for (overlay_selections) |fields| {
        const opts: @import("../types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
        var expected = try query.lookupJson(a, raw, opts);
        defer expected.deinit(a);
        var actual = try query.lookupJsonWithOutput(a, a, raw, opts);
        defer actual.deinit(a);
        try std.testing.expectEqualStrings(expected.json, actual.json);
    }
    for (selections) |fields| {
        const opts: @import("../types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
        var expected = try query.lookupJson(a, raw, opts);
        defer expected.deinit(a);
        var actual = try query.lookupJsonWithOutput(a, a, raw, opts);
        defer actual.deinit(a);
        try std.testing.expectEqualStrings(expected.json, actual.json);
    }
    // Native JSON spans keep original numeric lexemes, but still reject
    // duplicate members inside a whole selected terminal as the DOM did.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var view = View.init(scratch, "{\"n\":[1.00,1e2],\"s\":\"a\\u0062\"}");
    const output = Output{ .borrowed = .{ .span = .{ .view = &view, .node = &view.root } } };
    const native = try output.serialize(scratch, a);
    defer a.free(native);
    try std.testing.expectEqualStrings("{\"n\":[1.00,1e2],\"s\":\"ab\"}", native);
}
test "public graph metric borrowed wire streaming projection preserves ordered copy on write and numeric contracts" {
    try differentialScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, differentialScenario, .{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var view = View.init(arena.allocator(), "{\"dup\":1,\"d\\u0075p\":2}");
    const value = Output{ .borrowed = .{ .span = .{ .view = &view, .node = &view.root } } };
    try std.testing.expectError(error.DuplicateField, value.serialize(arena.allocator(), std.testing.allocator));
    for ([_][]const u8{ "", "[1,]", "{\"x\":}", "1 2" }) |invalid| {
        var invalid_view = View.init(arena.allocator(), invalid);
        const invalid_value = Output{ .borrowed = .{ .span = .{ .view = &invalid_view, .node = &invalid_view.root } } };
        if (invalid_value.serialize(arena.allocator(), std.testing.allocator)) |bytes| {
            std.testing.allocator.free(bytes);
            return error.TestExpectedError;
        } else |_| {}
    }
}

test "public graph metric borrowed wire streaming whole terminal has bounded scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"values\":[");
    for (0..100000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("123456789");
    }
    try input.writer.writeAll("],\"omit\":true}");
    const opts: @import("../types.zig").LookupOptions = .{ .fields = &.{"values"}, .include_all_fields = false };
    var plan = (try query.SpanProjectionPlan.init(a, opts)).?;
    defer plan.deinit();
    var budget: Counter = .{ .backing = a, .limit = 6 * 1024 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 1000012), result.json.len);
    try std.testing.expectEqualSlices(u8, input.written()[0 .. input.written().len - 13], result.json[0 .. result.json.len - 1]);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("STREAMING_TERMINAL output_bytes={d} scratch_peak={d}\n", .{ result.json.len, budget.peak });
}

fn overlayAppendScenario(a: Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var base = View.init(scratch, "{\"a\":[1,2],\"b\":{\"x\":1,\"y\":2}}");
    base.parse_numbers = true;
    const selected = try @import("json_projection_plan.zig").compile(scratch, &.{.{ .parts = &.{ "a", "1" } }});
    try base.prepareProjection(selected, .visited, .any_container);
    var replacement = View.init(scratch, "{\"x\":9,\"z\":3}");
    replacement.parse_numbers = true;
    const extra: Value = .{ .integer = 7 };
    var root: Output.Object = .empty;
    try Output.include(scratch, &root, "v", .{ .span = .{ .view = &base, .node = &base.root } }, &.{});
    // Select from a different source into a borrowed base. Existing members
    // retain base order; new members append in first insertion order.
    try Output.include(scratch, &root, "v", .{ .span = .{ .view = &replacement, .node = &replacement.root } }, &.{"z"});
    try Output.include(scratch, &root, "v", .{ .span = .{ .view = &replacement, .node = &replacement.root } }, &.{"x"});
    try Output.include(scratch, &root, "v", .{ .span = .{ .view = &base, .node = &base.root } }, &.{ "a", "1" });
    var extra_source: Value = .{ .object = .empty };
    try extra_source.object.put(scratch, "last", extra);
    try Output.include(scratch, &root, "v", .{ .logical = &extra_source }, &.{"last"});
    const bytes = try (Output{ .object = root }).serialize(scratch, a);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"v\":{\"a\":[2],\"b\":{\"x\":1,\"y\":2},\"z\":3,\"x\":9,\"last\":7}}", bytes);
    try std.testing.expectEqual(@as(usize, 1), base.root.children.?.len);
}

test "public graph metric borrowed wire streaming sparse overlays preserve base order and appended members" {
    try overlayAppendScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, overlayAppendScenario, .{});
}

test "public graph metric borrowed wire streaming sparse wide object stays within bounded scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"wide\":{");
    for (0..20000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.print("\"k{d}\":{d}", .{ i, i });
    }
    try input.writer.writeAll("}}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{ "wide", "wide.k0" }, .include_all_fields = false })).?;
    defer plan.deinit();
    var budget: Counter = .{ .backing = a, .limit = 6 * 1024 * 1024 };
    var body = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer body.deinit(a);
    try std.testing.expectEqualStrings(input.written(), body.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("SPARSE_WIDE fields=20000 output_bytes={d} peak={d}\n", .{ body.json.len, budget.peak });
}

test "public graph metric borrowed wire streaming siblings reuse bounded key arenas" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll(if (i % 2 == 0) "{\"x\":1,\"y\":2}" else "{\"x\\u0061\":1,\"y\":2}");
    }
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{"items"}, .include_all_fields = false })).?;
    defer plan.deinit();
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 2 * 1024 * 1024 };
    var body = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer body.deinit(a);
    const expected = try query.lookupJson(a, input.written(), .{ .fields = &.{"items"}, .include_all_fields = false });
    defer a.free(expected.json);
    try std.testing.expectEqualStrings(expected.json, body.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.alloc_calls < 128);
    std.debug.print("STREAMING_SIBLINGS count=8000 backing_allocations={d} peak={d}\n", .{ budget.alloc_calls, budget.peak });
}

test "public graph metric borrowed wire streaming sparse overlays validate discarded base members" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var replacement: Value = .{ .object = .empty };
    try replacement.object.put(arena.allocator(), "dup", .{ .integer = 7 });
    for ([_][]const u8{ "{\"dup\":1,\"d\\u0075p\":2}", "{\"dup\":[1,]}", "{\"dup\":}" }) |raw| {
        var base = View.init(arena.allocator(), raw);
        var root: Output.Object = .empty;
        try Output.include(arena.allocator(), &root, "v", .{ .span = .{ .view = &base, .node = &base.root } }, &.{});
        try Output.include(arena.allocator(), &root, "v", .{ .logical = &replacement }, &.{"dup"});
        if ((Output{ .object = root }).serialize(arena.allocator(), a)) |bytes| {
            a.free(bytes);
            return error.TestExpectedError;
        } else |_| {}
    }
}

test "public graph metric borrowed wire streaming scratch retires oversized buffers without replacement allocation" {
    const counters = @import("../../lite/test_allocator.zig");
    var no_resize: counters.NoResizeAllocator = .{ .backing = std.testing.allocator };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator() };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    _ = try arena.allocator().alloc(u8, 64 * 1024);
    const allocated = budget.alloc_calls;
    try resetScratch(&arena);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(allocated, budget.alloc_calls);
    _ = try arena.allocator().alloc(u8, 128);
    const small_allocated = budget.alloc_calls;
    try resetScratch(&arena);
    _ = try arena.allocator().alloc(u8, 128);
    try std.testing.expectEqual(small_allocated, budget.alloc_calls);
}

fn fanoutOrderingScenario(a: Allocator) !void {
    const query = @import("../document_query.zig");
    const raw = "{\"items\":[{\"x\":{\"m\":1.00,\"n\":2},\"nested\":[{\"z\":3},{\"q\":4}]},null,{\"x\":{},\"nested\":[]},{\"other\":5},{\"x\":[{\"m\":6}]}],\"empty\":[]}";
    const selections = [_][]const []const u8{
        &.{"items.x"},
        &.{ "items", "items.missing" },
        &.{ "items.x.m", "items.missing" },
        &.{ "items.x", "items.0.x.n" },
        &.{ "items.0.x.n", "items.x" },
        &.{ "items.nested.z", "items.x" },
        &.{ "items.x", "items.nested.z" },
        &.{ "items.x.m", "items", "items.nested.missing" },
        &.{ "items.nested.z", "empty.x" },
    };
    for (selections) |fields| {
        const opts: @import("../types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
        var expected = try query.lookupJson(a, raw, opts);
        defer expected.deinit(a);
        var actual = try query.lookupJsonWithOutput(a, a, raw, opts);
        defer actual.deinit(a);
        try std.testing.expectEqualStrings(expected.json, actual.json);
    }
    // Loaded logical fields use the same lazy fanout and replacement contract.
    var parsed = try std.json.parseFromSlice(Value, a, raw, .{});
    defer parsed.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var object: Output.Object = .empty;
    try Output.include(arena.allocator(), &object, "items", .{ .logical = parsed.value.object.getPtr("items").? }, &.{ "nested", "z" });
    const result = try (Output{ .object = object }).serialize(arena.allocator(), a);
    defer a.free(result);
    var expected = try query.lookupJson(a, raw, .{ .fields = &.{"items.nested.z"}, .include_all_fields = false });
    defer expected.deinit(a);
    try std.testing.expectEqualStrings(expected.json, result);
}

test "public graph metric borrowed wire streaming fanout preserves ordered arrays and allocation failure ownership" {
    try fanoutOrderingScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, fanoutOrderingScenario, .{});
}

test "public graph metric borrowed wire streaming fanout bounds navigation maps and terminal scanner scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":{\"m\":1,\"n\":2}}");
    }
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{"items.x"}, .include_all_fields = false })).?;
    defer plan.deinit();
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 1024 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqualStrings(input.written(), result.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.alloc_calls < 128);
    std.debug.print("STREAMING_FANOUT count=8000 allocations={d} peak={d}\n", .{ budget.alloc_calls, budget.peak });
}

test "public graph metric borrowed wire streaming fused overlay consumes exactly one value including escaped keys" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var view = View.init(arena.allocator(), "{\"a\\u0062\":{\"s\":\"\\n\\u0061\",\"n\":1e2},\"arr\":[1,true,null,{\"z\":2}],\"last\":false}");
    var replacement: Value = .{ .object = .empty };
    try replacement.object.put(arena.allocator(), "last", .{ .bool = true });
    var object: Output.Object = .empty;
    try Output.include(arena.allocator(), &object, "v", .{ .span = .{ .view = &view, .node = &view.root } }, &.{});
    try Output.include(arena.allocator(), &object, "v", .{ .logical = &replacement }, &.{"last"});
    const bytes = try (Output{ .object = object }).serialize(arena.allocator(), a);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"v\":{\"ab\":{\"s\":\"\\na\",\"n\":1e2},\"arr\":[1,true,null,{\"z\":2}],\"last\":true}}", bytes);
    // The transcode context is reused across object members, but never shares
    // duplicate-name sets between distinct objects or overlooks trailing data.
    for ([_][]const u8{ "{\"x\":1,\"x\":2}", "{\"x\":1} true", "[1,]" }) |raw| {
        view = View.init(arena.allocator(), raw);
        const invalid: Output = .{ .borrowed = .{ .span = .{ .view = &view, .node = &view.root } } };
        if (invalid.serialize(arena.allocator(), a)) |result| {
            a.free(result);
            return error.TestExpectedError;
        } else |_| {}
    }
}

test "public graph metric borrowed wire streaming fanout validates unprepared sibling keys before reusing proof" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var view = View.init(arena.allocator(), "[{\"x\":1},{\"x\":2,\"\\u0078\":3}]");
    var object: Output.Object = .empty;
    try Output.include(arena.allocator(), &object, "items", .{ .span = .{ .view = &view, .node = &view.root } }, &.{"x"});
    try std.testing.expectError(error.DuplicateField, (Output{ .object = object }).serialize(arena.allocator(), a));
}

test "public graph metric borrowed wire deep streaming fanout retains only one prepared item" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    var field = std.Io.Writer.Allocating.init(a);
    defer field.deinit();
    try input.writer.writeAll("{\"items\":[");
    try field.writer.writeAll("items");
    for (0..32) |_| {
        try input.writer.writeAll("{\"x\":");
        try field.writer.writeAll(".x");
    }
    try input.writer.writeByte('[');
    for (0..100000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("123456789");
    }
    try input.writer.writeByte(']');
    for (0..32) |_| try input.writer.writeByte('}');
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{field.written()}, .include_all_fields = false })).?;
    defer plan.deinit();
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var budget: Counter = .{ .backing = a, .limit = 512 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqualStrings(input.written(), result.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("DEEP_FANOUT depth=32 output_bytes={d} scratch_peak={d}\n", .{ result.json.len, budget.peak });
}

test "public graph metric borrowed wire direct writer streams compact vectors with no scratch allocation" {
    const a = std.testing.allocator;
    const vector = [_]f32{ 0.1, -0.0, 1.25 };
    const value: Output = .{ .borrowed = .{ .vector = &vector } };
    var expected = std.Io.Writer.Allocating.init(a);
    defer expected.deinit();
    var logical = [_]Value{ .{ .float = @as(f64, vector[0]) }, .{ .float = @as(f64, vector[1]) }, .{ .float = @as(f64, vector[2]) } };
    const array: Value = .{ .array = .fromOwnedSlice(a, &logical) };
    try std.json.Stringify.value(array, .{}, &expected.writer);
    var buffer: [256]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var budget: Counter = .{ .backing = a, .limit = 0 };
    try value.writeTo(budget.allocator(), &writer);
    try std.testing.expectEqualStrings(expected.written(), writer.buffered());
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_attempts);
}

test "public graph metric borrowed wire multi component fanout reuses item preparation scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":{\"m\":1,\"n\":2}}");
    }
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{"items.x.m"}, .include_all_fields = false })).?;
    defer plan.deinit();
    const counters = @import("../../lite/test_allocator.zig");
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 1024 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    var expected = try query.lookupJson(a, input.written(), .{ .fields = &.{"items.x.m"}, .include_all_fields = false });
    defer expected.deinit(a);
    try std.testing.expectEqualStrings(expected.json, result.json);
    try std.testing.expect(budget.alloc_calls < 128);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("PREPARED_FANOUT count=8000 allocations={d} peak={d}\n", .{ budget.alloc_calls, budget.peak });
}

test "public graph metric borrowed wire primed nested fanout survives replacement failures and repeated output" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var arena: std.heap.ArenaAllocator = .init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const raw = "[{\"x\":[{\"x\":[{\"leaf\":[1,2,3]},{\"other\":1}]}]},{\"x\":[{\"x\":[{\"leaf\":[4]}]}]}]";
            var view: View = .init(scratch, raw);
            var root: Output.Object = .empty;
            const source: Source = .{ .span = .{ .view = &view, .node = &view.root } };
            try Output.include(scratch, &root, "items", source, &.{ "x", "x", "leaf" });
            // Abandon a live prepared chain, then preserve its replacement when
            // a later selector produces no surviving objects.
            try Output.include(scratch, &root, "items", source, &.{ "x", "x", "leaf" });
            try Output.include(scratch, &root, "items", source, &.{"absent"});
            const output: Output = .{ .object = root };
            var storage: [32]u8 = undefined;
            var failed: std.Io.Writer = .fixed(&storage);
            if (output.writeTo(scratch, &failed)) |_| return error.ExpectedWriteFailure else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expect(err == error.WriteFailed);
            }
            for (0..2) |_| {
                const bytes = try output.serialize(scratch, a);
                defer a.free(bytes);
                try std.testing.expectEqualStrings("{\"items\":[{\"x\":[{\"x\":[{\"leaf\":[1,2,3]}]}]},{\"x\":[{\"x\":[{\"leaf\":[4]}]}]}]}", bytes);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "public graph metric borrowed wire streaming writer reuses wide escaped key tables" {
    const a = std.testing.allocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    var expected = std.Io.Writer.Allocating.init(a);
    defer expected.deinit();
    try input.writer.writeByte('{');
    try expected.writer.writeByte('{');
    for (0..1000) |i| {
        if (i != 0) {
            try input.writer.writeByte(',');
            try expected.writer.writeByte(',');
        }
        try input.writer.print("\"\\u006b{d}\":0", .{i});
        try expected.writer.print("\"k{d}\":0", .{i});
    }
    try input.writer.writeByte('}');
    try expected.writer.writeByte('}');
    const Fixture = struct {
        fn run(alloc: Allocator, raw: []const u8, canonical: []const u8) !void {
            var counted: std.testing.FailingAllocator = .init(alloc, .{});
            var context = StreamContext.init(counted.allocator());
            defer context.deinit();
            for (0..4) |iteration| {
                var out = std.Io.Writer.Allocating.init(alloc);
                defer out.deinit();
                const calls = counted.allocations;
                context.writeSpan(&out.writer, raw, true) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
                try std.testing.expectEqualStrings(canonical, out.written());
                if (iteration != 0) try std.testing.expectEqual(calls, counted.allocations);
                try std.testing.expect(context.keys.retainedBytes() <= @TypeOf(context.keys).reuse_limit);
            }
        }
    };
    try Fixture.run(a, input.written(), expected.written());
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{ input.written(), expected.written() });
}

test "public graph metric borrowed wire streaming writer retires sparse tables and recovers from write failure" {
    const a = std.testing.allocator;
    var context = StreamContext.init(a);
    defer context.deinit();
    var wide = std.Io.Writer.Allocating.init(a);
    defer wide.deinit();
    try wide.writer.writeByte('{');
    for (0..5000) |i| {
        if (i != 0) try wide.writer.writeByte(',');
        try wide.writer.print("\"k{d}\":0", .{i});
    }
    try wide.writer.writeByte('}');
    var output = std.Io.Writer.Allocating.init(a);
    defer output.deinit();
    try context.writeSpan(&output.writer, wide.written(), true);
    try std.testing.expect(context.keys.table_bytes > 64 * 1024);
    try context.writeSpan(&output.writer, "{\"a\":0}", true);
    try context.writeSpan(&output.writer, "{\"a\":0}", true);
    try std.testing.expect(context.keys.table_bytes < 1024);
    var memory: [2]u8 = undefined;
    var fixed = std.Io.Writer.fixed(&memory);
    try std.testing.expectError(error.WriteFailed, context.writeSpan(&fixed, "{\"a\":{\"b\":0}}", true));
    try std.testing.expectEqual(@as(usize, 0), context.keys.retainedBytes());
    try std.testing.expectError(error.DuplicateField, context.writeSpan(&output.writer, "{\"x\":0,\"\\u0078\":1}", true));
    try std.testing.expectEqual(@as(usize, 0), context.keys.retainedBytes());
    var recovered = std.Io.Writer.Allocating.init(a);
    defer recovered.deinit();
    try context.writeSpan(&recovered.writer, "{\"\\u0061\":{\"a\":1}}", true);
    try std.testing.expectEqualStrings("{\"a\":{\"a\":1}}", recovered.written());
    const depth = 10000;
    const deep = try a.alloc(u8, depth * 2 + 1);
    defer a.free(deep);
    @memset(deep[0..depth], '[');
    deep[depth] = '0';
    @memset(deep[depth + 1 ..], ']');
    var deep_output = std.Io.Writer.Allocating.init(a);
    defer deep_output.deinit();
    try context.writeSpan(&deep_output.writer, deep, true);
    try std.testing.expectEqualStrings(deep, deep_output.written());
    try std.testing.expectEqual(@as(usize, 0), context.keys.retainedBytes());
}
