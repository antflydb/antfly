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
const View = @import("json_view.zig").View;

/// Row-owned projection structure. Terminal values borrow immutable sources;
/// containers become mutable only when an ordered selector needs to change one.
/// Neither the view nor a loaded source is ever mutated by projection.
pub const Source = union(enum) {
    span: struct { view: *View, node: *View.Node },
    logical: *const Value,

    fn kind(self: Source) u8 {
        return switch (self) {
            .span => |s| s.node.kind(),
            .logical => |v| switch (v.*) {
                .object => '{',
                .array => '[',
                else => 0,
            },
        };
    }
    fn field(self: Source, name: []const u8) !?Source {
        return switch (self) {
            .span => |s| if (try s.view.field(s.node, name)) |node| .{ .span = .{ .view = s.view, .node = node } } else null,
            .logical => |v| if (v.* == .object) (if (v.object.getPtr(name)) |value| .{ .logical = value } else null) else null,
        };
    }
    fn element(self: Source, index: usize) !?Source {
        return switch (self) {
            .span => |s| if (try s.view.element(s.node, index)) |node| .{ .span = .{ .view = s.view, .node = node } } else null,
            .logical => |v| if (v.* == .array and index < v.array.items.len) .{ .logical = &v.array.items[index] } else null,
        };
    }
    fn arrayLen(self: Source) !usize {
        return switch (self) {
            .span => |s| (try s.view.children(s.node)).len,
            .logical => |v| v.array.items.len,
        };
    }
};

pub const Output = union(enum) {
    borrowed: Source,
    object: Object,
    array: std.ArrayListUnmanaged(Output),
    pub const Object = std.StringArrayHashMapUnmanaged(Output);

    fn makeObject(self: *Output, a: Allocator) !void {
        if (self.* == .object) return;
        var object: Object = .empty;
        if (self.* == .borrowed and self.borrowed.kind() == '{') try copyObject(a, &object, self.borrowed);
        self.* = .{ .object = object };
    }

    fn put(a: Allocator, object: *Object, name: []const u8, value: Output) !void {
        if (object.entries.capacity == 0) try object.entries.setCapacity(a, 1);
        try object.put(a, name, value);
    }

    fn copyObject(a: Allocator, dst: *Object, source: Source) !void {
        switch (source) {
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
    /// selection builds a fresh compact array, preserving legacy replacement.
    pub fn include(a: Allocator, dst: *Object, name: []const u8, source: Source, path: []const []const u8) anyerror!void {
        if (path.len == 0) return put(a, dst, name, .{ .borrowed = source });
        switch (source.kind()) {
            '{' => {
                if (!dst.contains(name)) try put(a, dst, name, .{ .object = .empty });
                const value = dst.getPtr(name).?;
                try value.makeObject(a);
                if (std.mem.eql(u8, path[0], "*")) {
                    var all: Object = .empty;
                    try copyObject(a, &all, source);
                    for (all.keys(), all.values()) |key, child| try include(a, &value.object, key, child.borrowed, path[1..]);
                } else if (try source.field(path[0])) |child| try include(a, &value.object, path[0], child, path[1..]);
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
                    const len = try source.arrayLen();
                    for (0..len) |i| {
                        const item = (try source.element(i)).?;
                        if (item.kind() != '{') continue;
                        var object: Object = .empty;
                        try includeObjectPath(a, &object, item, path);
                        if (object.count() != 0) try result.append(a, .{ .object = object });
                    }
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
        } else if (try source.field(path[0])) |child| try include(a, dst, path[0], child, path[1..]);
    }

    pub fn includeRoot(a: Allocator, dst: *Object, view: *View, path: []const []const u8) !void {
        if (path.len == 0) return;
        for (view.root_sources) |*source| if (std.mem.eql(u8, source.name, path[0])) {
            return include(a, dst, source.name, .{ .logical = &source.value }, path[1..]);
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
            try put(a, dst, source.name, .{ .borrowed = .{ .logical = &source.value } });
        }
    }
    fn omitted(excluded: []const []const u8, name: []const u8) bool {
        for (excluded) |entry| if (std.mem.eql(u8, name, entry)) return true;
        return false;
    }

    pub fn serialize(self: Output, scratch: Allocator, output: Allocator) ![]u8 {
        var writer = std.Io.Writer.Allocating.init(@import("antfly_platform").allocator.reclaimingScratch(scratch));
        defer writer.deinit();
        self.write(scratch, &writer.writer) catch |err| switch (err) {
            error.WriteFailed => return error.OutOfMemory,
            else => return err,
        };
        return output.dupe(u8, writer.written());
    }
    fn write(self: Output, scratch: Allocator, writer: *std.Io.Writer) anyerror!void {
        switch (self) {
            .borrowed => |source| switch (source) {
                .span => |s| try writeSpan(scratch, writer, s.node.raw, s.view.parse_numbers),
                .logical => |v| try std.json.Stringify.value(v.*, .{}, writer),
            },
            .object => |object| {
                try writer.writeByte('{');
                for (object.keys(), object.values(), 0..) |name, child, i| {
                    if (i != 0) try writer.writeByte(',');
                    try std.json.Stringify.value(name, .{}, writer);
                    try writer.writeByte(':');
                    try child.write(scratch, writer);
                }
                try writer.writeByte('}');
            },
            .array => |array| {
                try writer.writeByte('[');
                for (array.items, 0..) |child, i| {
                    if (i != 0) try writer.writeByte(',');
                    try child.write(scratch, writer);
                }
                try writer.writeByte(']');
            },
        }
    }
};

/// Transcode a validated terminal without constructing its DOM. Decode one
/// string/number at a time; container state scales only with nesting depth.
fn writeSpan(scratch: Allocator, writer: *std.Io.Writer, raw: []const u8, parse_numbers: bool) !void {
    const work = @import("antfly_platform").allocator.reclaimingScratch(scratch);
    var scanner = std.json.Scanner.initCompleteInput(work, raw);
    defer scanner.deinit();
    var token_arena = std.heap.ArenaAllocator.init(work);
    defer token_arena.deinit();
    const Frame = struct { object: bool, key: bool = true, count: usize = 0, arena: std.heap.ArenaAllocator, names: std.StringHashMapUnmanaged(void) = .empty };
    var stack: std.ArrayListUnmanaged(Frame) = .empty;
    defer {
        for (stack.items) |*frame| frame.arena.deinit();
        stack.deinit(work);
    }
    var root_seen = false;
    while (true) {
        _ = token_arena.reset(.{ .retain_with_limit = 16 * 1024 });
        const token = try scanner.nextAllocMax(token_arena.allocator(), .alloc_if_needed, raw.len);
        if (token == .end_of_document) {
            if (!root_seen or stack.items.len != 0) return error.SyntaxError;
            break;
        }
        if (token == .object_end or token == .array_end) {
            try writer.writeByte(if (token == .object_end) '}' else ']');
            var frame = stack.pop().?;
            frame.arena.deinit();
            continue;
        }
        if (stack.items.len == 0) {
            if (root_seen) return error.SyntaxError;
            root_seen = true;
        }
        if (stack.items.len != 0) {
            const frame = &stack.items[stack.items.len - 1];
            if (frame.object and frame.key) {
                const name = switch (token) {
                    .string, .allocated_string => |s| s,
                    else => return error.SyntaxError,
                };
                const key = if (token == .allocated_string) try frame.arena.allocator().dupe(u8, name) else name;
                if ((try frame.names.getOrPut(frame.arena.allocator(), key)).found_existing) return error.DuplicateField;
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
                try stack.append(work, .{ .object = token == .object_begin, .arena = .init(work) });
            },
            .string, .allocated_string => |s| try std.json.Stringify.value(s, .{}, writer),
            .number, .allocated_number => |s| try std.json.Stringify.value(if (parse_numbers) Value.parseFromNumberSlice(s) else Value{ .number_string = s }, .{}, writer),
            .true => try writer.writeAll("true"),
            .false => try writer.writeAll("false"),
            .null => try writer.writeAll("null"),
            else => return error.SyntaxError,
        }
    }
}

fn differentialScenario(a: Allocator) !void {
    const query = @import("../document_query.zig");
    const raw = "{\"a\":{\"numbers\":[1.00,1e2,-0,9223372036854775808,1e999],\"text\":\"escape\\n\\u0061\",\"other\":true},\"items\":[{\"x\":{\"m\":1,\"n\":2},\"z\":4},{\"x\":{\"m\":3},\"z\":5},null],\"omit\":null}";
    const selections = [_][]const []const u8{
        &.{"a"},                    &.{"a.numbers"},                                 &.{"a.text"},
        &.{ "a", "a.numbers.0" },   &.{ "a.numbers.0", "a" },                        &.{ "items", "items.x.m" },
        &.{ "items.x.m", "items" }, &.{ "items.0.x", "items.1.x.n", "items.0.x.m" }, &.{ "a", "a", "-omit" },
        &.{"-omit"},                &.{"*"},
    };
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
