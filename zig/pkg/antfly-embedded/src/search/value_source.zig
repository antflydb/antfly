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

//! Single-pass projection for logical string predicates. Every token and every
//! duplicate key is validated; only selected path structure/string values get
//! a DOM. Arrays retain element boundaries and traversal counts. Scalars that
//! cannot match strings become null, preserving matcher behavior.
const std = @import("std");
const A = std.mem.Allocator;

/// Query-local admission for optional stored-field reads. Required full-source
/// parsing supplies a lower bound on the selected bytes without another scan.
/// A dominant value cannot benefit from the directory's half-source threshold.
/// Keep observing full reads so heterogeneous inputs can become eligible again.
pub const StoredReadPolicy = struct {
    selected_fraction: ?u16 = null,
    skipped_rows: u16 = 0,
    backoff_rows: u16 = 0,

    pub fn mayBenefit(self: *const @This()) bool {
        return if (self.selected_fraction) |fraction| fraction < 512 else true;
    }

    pub fn forgetCoverage(self: *@This()) void {
        self.selected_fraction = null;
    }

    pub fn observeSource(self: *@This(), original: u64, selected: u64) void {
        if (original == 0) return;
        const was_dominant = !self.mayBenefit();
        const fraction: u16 = @intCast(@min(@as(u128, selected) * 1024 / original, 1024));
        self.selected_fraction = if (self.selected_fraction) |prior| @intCast((@as(u32, prior) * 3 + fraction) / 4) else fraction;
        if (was_dominant and self.mayBenefit()) {
            self.skipped_rows = 0;
            self.backoff_rows = 0;
        }
    }

    pub fn admit(self: *@This(), rows: usize) bool {
        if (rows == 0 or !self.mayBenefit()) return false;
        if (self.skipped_rows != 0) {
            self.skipped_rows -|= @intCast(@min(rows, std.math.maxInt(u16)));
            return false;
        }
        return true;
    }

    pub fn observeProbe(self: *@This(), rows: usize, projected: usize) void {
        if (rows == 0) return;
        if (projected != 0) {
            self.backoff_rows = 0;
            self.skipped_rows = 0;
        } else {
            self.backoff_rows = if (self.backoff_rows == 0) 32 else @min(self.backoff_rows * 2, 1024);
            self.skipped_rows = self.backoff_rows;
        }
    }
};
pub fn parse(a: A, bytes: []const u8, plan: ?*Plan) !struct { value: std.json.Value, projected: bool } {
    if (bytes.len >= 1024) if (plan) |compiled| {
        if (compiled.parse(a, bytes)) |value| return .{ .value = value, .projected = true } else |err| switch (err) {
            error.ProjectionTooDeep => {},
            else => return err,
        }
    };
    return .{ .value = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .allocate = .alloc_if_needed }), .projected = false };
}
pub const Plan = struct {
    const Node = struct {
        children: std.StringHashMapUnmanaged(usize) = .empty,
        terminal: bool = false,
    };
    a: A,
    nodes: std.ArrayListUnmanaged(Node) = .empty,
    keys: @import("antfly-json").ObjectKeyWorkspace(void),
    unused: @import("antfly-json").ValueScanner,
    selected_string_bytes: u64 = 0,
    pub fn init(a: A, fields: []const []const u8) !Plan {
        if (@import("antfly_platform").env.getenv("ANTFLY_BENCH_VALUE_SOURCE_FULL_DOM")) |knob| {
            if (std.mem.eql(u8, knob, "1")) return error.ProjectionDisabled;
        }
        var self: Plan = .{ .a = a, .keys = .init(@import("../sql/memory_budget.zig").reclaimingAllocator(a)), .unused = .init(@import("../sql/memory_budget.zig").reclaimingAllocator(a)) };
        errdefer self.deinit();
        try self.nodes.append(a, .{});
        for (fields) |field| {
            var index: usize = 0;
            if (field.len != 0) {
                var path = field;
                while (path.len != 0) {
                    const dot = std.mem.indexOfScalar(u8, path, '.');
                    const part = if (dot) |i| path[0..i] else path;
                    path = if (dot) |i| path[i + 1 ..] else "";
                    if (self.nodes.items[index].children.get(part)) |found| {
                        index = found;
                    } else {
                        if (self.nodes.items.len == 4096) return error.ProjectionTooLarge;
                        const next = self.nodes.items.len;
                        try self.nodes.append(a, .{});
                        try self.nodes.items[index].children.put(a, part, next);
                        index = next;
                    }
                }
            }
            self.nodes.items[index].terminal = true;
        }
        return self;
    }
    pub fn deinit(self: *Plan) void {
        self.keys.deinit();
        self.unused.deinit();
        for (self.nodes.items) |*node| node.children.deinit(self.a);
        self.nodes.deinit(self.a);
    }
    /// `a` is a document scratch arena. Strings borrow input when unescaped;
    /// selected escaped strings and path structure live in that same arena.
    pub fn parse(self: *Plan, a: A, bytes: []const u8) !std.json.Value {
        self.selected_string_bytes = 0;
        var scanner = std.json.Scanner.initCompleteInput(@import("../sql/memory_budget.zig").reclaimingAllocator(a), bytes);
        defer scanner.deinit();
        // Failed traversals can leave active duplicate-key frames. Reset them
        // before reuse, including optional fallback on unusually deep sources.
        errdefer self.keys.deinit();
        errdefer self.unused.deinit();
        const result = try self.value(&scanner, a, 0, 0, bytes.len);
        if (try scanner.next() != .end_of_document) return error.UnexpectedToken;
        self.keys.finishValue();
        return result;
    }
    fn value(self: *Plan, scanner: *std.json.Scanner, a: A, selected: ?usize, depth: usize, max_len: usize) anyerror!std.json.Value {
        if (selected == null) {
            // Reuse the iterative validator for arbitrary-depth unused values.
            // It checks duplicate keys and escapes without materializing arrays
            // or strings, and bounds retained scratch between source documents.
            try self.unused.scan(scanner, max_len, null);
            return .null;
        }
        if (depth == 128) return error.ProjectionTooDeep;
        const kind = try scanner.peekNextTokenType();
        switch (kind) {
            .object_begin => {
                _ = try scanner.next();
                try self.keys.enter(depth, {});
                var object: std.json.ObjectMap = .empty;
                while (true) {
                    const token = try self.keys.nextKey(depth, scanner, max_len);
                    if (token == .object_end) break;
                    const name = switch (token) {
                        .string, .allocated_string => |s| s,
                        else => return error.UnexpectedToken,
                    };
                    try self.keys.remember(depth, name, false);
                    const child = if (selected) |index| self.nodes.items[index].children.getEntry(name) else null;
                    const output = try self.value(scanner, a, if (child) |c| c.value_ptr.* else null, depth + 1, max_len);
                    // Use the stable compiled path key; decoded input keys may
                    // belong to a duplicate-key frame recycled below.
                    if (child) |c| try object.put(a, c.key_ptr.*, output);
                }
                try self.keys.finishFrame(depth);
                return if (selected != null) .{ .object = object } else .null;
            },
            .array_begin => {
                _ = try scanner.next();
                try self.keys.enter(depth, {});
                var array = std.json.Array.init(a);
                while (try scanner.peekNextTokenType() != .array_end) {
                    const item = try self.value(scanner, a, selected, depth + 1, max_len);
                    if (selected != null) try array.append(item);
                }
                _ = try scanner.next();
                try self.keys.finishFrame(depth);
                return if (selected != null) .{ .array = array } else .null;
            },
            .string => {
                if (selected) |index| if (self.nodes.items[index].terminal) {
                    if (@import("antfly-json").value_strings.borrow(scanner)) |bytes| return self.selectedString(bytes);
                    return switch (try scanner.nextAllocMax(a, .alloc_if_needed, max_len)) {
                        .string, .allocated_string => |s| self.selectedString(s),
                        else => error.UnexpectedToken,
                    };
                };
                if (!try @import("antfly-json").value_strings.skip(scanner)) try scanner.skipValue();
                return .null;
            },
            .number, .true, .false, .null => {
                try scanner.skipValue();
                return .null;
            },
            else => return error.UnexpectedToken,
        }
    }
    fn selectedString(self: *Plan, bytes: []const u8) std.json.Value {
        self.selected_string_bytes +|= bytes.len;
        return .{ .string = bytes };
    }
};

test "logical value stored read admission learns dominance and bounds unsuccessful probes" {
    var policy: StoredReadPolicy = .{};
    try std.testing.expect(policy.admit(1));
    policy.observeSource(16384, 16300);
    for (0..4096) |_| {
        try std.testing.expect(!policy.admit(64));
        policy.observeSource(16384, 16300);
    }
    // Continuous required reads discover a changed shape without metadata I/O.
    for (0..4) |_| policy.observeSource(65536, 6);
    try std.testing.expect(policy.admit(64));
    for (0..20) |_| {
        policy.observeProbe(64, 0);
        try std.testing.expect(policy.backoff_rows <= 1024);
        var skipped: usize = 0;
        while (!policy.admit(64)) skipped += 64;
        try std.testing.expect(skipped >= policy.backoff_rows);
        try std.testing.expect(skipped < @as(usize, policy.backoff_rows) + 64);
    }
    policy.observeProbe(64, 1);
    try std.testing.expectEqual(@as(u16, 0), policy.backoff_rows);
    try std.testing.expect(policy.admit(1));
    try std.testing.expect(!policy.admit(0));
    policy.observeSource(std.math.maxInt(u64), std.math.maxInt(u64));
    try std.testing.expect(policy.selected_fraction.? <= 1024);
    policy.forgetCoverage();
    try std.testing.expect(policy.mayBenefit());
}

test "logical value source selected byte feedback is decoded deduplicated and reset" {
    const a = std.testing.allocator;
    var plan = try Plan.init(a, &.{ "wanted", "wanted", "nested.value" });
    defer plan.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    _ = try plan.parse(scratch.allocator(), "{\"wanted\":[\"a\\u0062\",\"cd\"],\"nested\":[{\"value\":\"ef\"}],\"unused\":\"not counted\"}");
    try std.testing.expectEqual(@as(u64, 6), plan.selected_string_bytes);
    _ = try plan.parse(scratch.allocator(), "{\"wanted\":\"x\"}");
    try std.testing.expectEqual(@as(u64, 1), plan.selected_string_bytes);
}

test "logical value source projection preserves nested arrays escapes and boundaries" {
    const a = std.testing.allocator;
    const bytes = "{\"unused\":{\"payload\":[1,true,null]},\"nested\":[{\"name\":\"h\\u0065llo\"},{\"name\":[null,\"world\"]}],\"a.b\":\"unused\"}";
    var full = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer full.deinit();
    var plan = try Plan.init(a, &.{"nested.name"});
    defer plan.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const projected = try plan.parse(scratch.allocator(), bytes);
    for ([_][]const u8{ "hello", "world", "helloworld", "missing" }) |text| {
        var matcher: @import("value_query.zig").Matcher = undefined;
        try matcher.init(a, .{ .kind = .contains, .field = "nested.name", .text = text });
        defer matcher.deinit();
        try std.testing.expectEqual(try matcher.document(full.value), try matcher.document(projected));
    }
    try std.testing.expect(projected.object.get("unused") == null);
    // Invalid unused content must still fail before any predicate can succeed.
    try std.testing.expectError(error.DuplicateField, plan.parse(scratch.allocator(), "{\"nested\":{},\"unused\":{\"x\":1,\"x\":2}}"));
    try std.testing.expectError(error.SyntaxError, plan.parse(scratch.allocator(), "{\"nested\":{},\"unused\":[1,]}"));
    _ = try plan.parse(scratch.allocator(), bytes);
}

test "logical value source projection skips escaped payload allocation" {
    const a = std.testing.allocator;
    var input: std.Io.Writer.Allocating = .init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"wanted\":\"needle\",\"unused\":\"");
    for (0..65536) |_| try input.writer.writeAll("\\n");
    try input.writer.writeAll("\"}");
    var plan = try Plan.init(a, &.{"wanted"});
    defer plan.deinit();
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    const projected = try plan.parse(fixed.allocator(), input.written());
    try std.testing.expectEqualStrings("needle", projected.object.get("wanted").?.string);
    try std.testing.expect(plan.keys.retainedBytes() <= 256 * 1024);
    // Unused deeply nested data is validated iteratively, without forcing a
    // full-DOM fallback or consuming the selected-value traversal depth.
    var deep: std.Io.Writer.Allocating = .init(a);
    defer deep.deinit();
    try deep.writer.writeAll("{\"wanted\":\"needle\",\"unused\":");
    for (0..1000) |_| try deep.writer.writeByte('[');
    try deep.writer.writeByte('0');
    for (0..1000) |_| try deep.writer.writeByte(']');
    try deep.writer.writeByte('}');
    fixed.reset();
    const nested = try plan.parse(fixed.allocator(), deep.written());
    try std.testing.expectEqualStrings("needle", nested.object.get("wanted").?.string);
}

test "logical value source projection differential shapes and allocation failures" {
    const a = std.testing.allocator;
    const fields = [_][]const u8{ "content", "nested.name", "", "items.content", "nested..name", "nested.", "." };
    var plan = try Plan.init(a, &fields);
    defer plan.deinit();
    for ([_][]const u8{
        "null",                                                       "123",                                                              "\"needle\"",                                                       "[\"needle\",null,{},[\"other\"]]",
        "{\"content\":[\"nee\",\"dle\",null,17,{\"x\":\"needle\"}]}", "{\"nested\":[{\"name\":\"needle\"},{\"\":{\"name\":\"other\"}}]}", "{\"items\":[{\"content\":[[\"needle\"]]},17,{\"content\":true}]}", "{\"cont\\u0065nt\":\"\\u212Aelvin\",\"unused\":{\"key\\u0061\":\"\\n\"}}",
    }) |bytes| {
        var full = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer full.deinit();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const projected = try plan.parse(scratch.allocator(), bytes);
        for (fields) |field| for ([_][]const u8{ "needle", "", "kelvin" }) |text| {
            var matcher: @import("value_query.zig").Matcher = undefined;
            try matcher.init(a, .{ .kind = .contains, .field = field, .text = text, .case_sensitive = false });
            defer matcher.deinit();
            try std.testing.expectEqual(try matcher.document(full.value), try matcher.document(projected));
        };
    }
    const Failure = struct {
        fn run(alloc: A) !void {
            var owned = try Plan.init(alloc, &.{"nested.name"});
            defer owned.deinit();
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            _ = try owned.parse(scratch.allocator(), "{\"unused\":[{\"k\\u0065y\":0}],\"nested\":{\"name\":\"nee\\u0064le\"}}");
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Failure.run, .{});
}
