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

/// Validate generated OpenAPI objects without retaining their values. Arrays
/// retire each element immediately; object key storage lives only for that
/// object. Custom leaf/union parsers remain authoritative for their contract.
/// The default strict parse semantics (including duplicate and unknown fields)
/// are preserved. Dynamic JSON uses an explicit stack rather than one native
/// call frame per nested value.
/// Supply reclaimable scratch: each completed value retires its temporary
/// owners. This is schema validation, not just a syntax check.
pub fn validateTyped(comptime T: type, alloc: std.mem.Allocator, input: []const u8) !void {
    var scanner = std.json.Scanner.initCompleteInput(alloc, input);
    defer scanner.deinit();
    var validator: Validator = .{ .alloc = alloc, .scanner = &scanner, .options = .{ .allocate = .alloc_if_needed, .max_value_len = input.len }, .dynamic_scan = .init(alloc), .typed_keys = .init(alloc), .schema_keys = .init(alloc) };
    defer validator.dynamic_scan.deinit();
    defer validator.typed_keys.deinit();
    defer validator.schema_keys.deinit();
    try validator.value(T);
    if (try scanner.next() != .end_of_document) return error.UnexpectedToken;
}

/// Observe validated members of the root object. Names borrow object scratch
/// until this callback returns; ranges borrow the scanner's complete input.
pub const MemberObserver = struct {
    ctx: *anyopaque,
    member_fn: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
};

/// Validate one value at the current position, leaving the scanner immediately
/// after it. Each active object owns independently reclaimable duplicate-key
/// storage. Completed siblings reuse bounded key and table storage. This requires a
/// complete-input scanner; optional member observation adds no second scan.
pub fn scanValue(alloc: std.mem.Allocator, scanner: *std.json.Scanner, max_len: usize, observer: ?MemberObserver) !void {
    var workspace: ValueScanner = .init(alloc);
    defer workspace.deinit();
    try workspace.scan(scanner, max_len, observer);
}

/// Parser-owned workspace. Small key arenas survive sibling values and rows;
/// retained frames, tables and arenas share a 256 KiB ceiling between calls. Active
/// ancestor key sets remain live for duplicate detection, regardless of depth.
pub const ValueScanner = struct {
    const Keys = @import("object_key_workspace.zig").ObjectKeyWorkspace(bool);
    keys: Keys,

    pub fn init(allocator: std.mem.Allocator) @This() {
        return .{ .keys = .init(allocator) };
    }
    pub fn deinit(self: *@This()) void {
        self.keys.deinit();
    }
    pub fn scan(self: *@This(), scanner: *std.json.Scanner, max_len: usize, observer: ?MemberObserver) !void {
        errdefer self.deinit();
        const stack = &self.keys.stack;
        var root_seen = false;
        var depth: usize = 0;
        var member_name: ?[]const u8 = null;
        var member_start: usize = 0;
        while (!root_seen or depth != 0) {
            if (depth != 0) {
                const top = &stack.items[depth - 1];
                if (top.state) {
                    const token = try self.keys.nextKey(depth - 1, scanner, max_len);
                    const name = switch (token) {
                        .string, .allocated_string => |name| name,
                        .object_end => {
                            try self.keys.finishFrame(depth - 1);
                            depth -= 1;
                            if (depth == 1 and member_name != null) {
                                try observer.?.member_fn(observer.?.ctx, member_name.?, scanner.input[member_start..scanner.cursor]);
                                member_name = null;
                            }
                            continue;
                        },
                        else => return error.UnexpectedToken,
                    };
                    try self.keys.remember(depth - 1, name, false);
                    if (observer != null and depth == 1) {
                        member_name = name;
                        member_start = scanner.cursor;
                        while (member_start < scanner.input.len and (std.ascii.isWhitespace(scanner.input[member_start]) or scanner.input[member_start] == ':')) : (member_start += 1) {}
                    }
                } else if (try scanner.peekNextTokenType() == .array_end) {
                    _ = try scanner.next();
                    try self.keys.finishFrame(depth - 1);
                    depth -= 1;
                    if (depth == 1 and member_name != null) {
                        try observer.?.member_fn(observer.?.ctx, member_name.?, scanner.input[member_start..scanner.cursor]);
                        member_name = null;
                    }
                    continue;
                }
            }
            root_seen = true;
            switch (try scanner.peekNextTokenType()) {
                .object_begin, .array_begin => |kind| {
                    _ = try scanner.next();
                    try self.keys.enter(depth, kind == .object_begin);
                    depth += 1;
                },
                .string, .number, .true, .false, .null => {
                    if (try scanner.peekNextTokenType() != .string or !try @import("string_scan.zig").skip(scanner)) try scanner.skipValue();
                    if (depth == 1 and member_name != null) {
                        try observer.?.member_fn(observer.?.ctx, member_name.?, scanner.input[member_start..scanner.cursor]);
                        member_name = null;
                    }
                },
                else => return error.UnexpectedToken,
            }
        }
        self.keys.finishValue();
    }
};

const Validator = struct {
    alloc: std.mem.Allocator,
    scanner: *std.json.Scanner,
    options: std.json.ParseOptions,
    dynamic_scan: ValueScanner,
    typed_keys: @import("object_key_workspace.zig").ObjectKeyWorkspace(void),
    typed_depth: usize = 0,
    schema_keys: std.heap.ArenaAllocator,

    fn expect(self: *@This(), comptime token: std.meta.Tag(std.json.Token)) !void {
        if (try self.scanner.next() != token) return error.UnexpectedToken;
    }
    fn dynamic(self: *@This()) anyerror!void {
        try self.dynamic_scan.scan(self.scanner, self.options.max_value_len.?, null);
    }
    fn array(self: *@This(), comptime Child: type) anyerror!void {
        try self.expect(.array_begin);
        while (try self.scanner.peekNextTokenType() != .array_end) try self.value(Child);
        try self.expect(.array_end);
    }
    fn map(self: *@This(), comptime Child: type) anyerror!void {
        if (Child == std.json.Value) {
            // A JSON map must still be an object. Its arbitrary children and
            // duplicate keys then share the iterative, bounded workspace.
            if (try self.scanner.peekNextTokenType() != .object_begin) return error.UnexpectedToken;
            return self.dynamic();
        }
        try self.expect(.object_begin);
        const depth = self.typed_depth;
        try self.typed_keys.enter(depth, {});
        self.typed_depth += 1;
        defer self.typed_depth -= 1;
        while (true) {
            const token = try self.typed_keys.nextKey(depth, self.scanner, self.options.max_value_len.?);
            const name = switch (token) {
                .string, .allocated_string => |name| name,
                .object_end => break,
                else => return error.UnexpectedToken,
            };
            try self.typed_keys.remember(depth, name, false);
            try self.value(Child);
        }
        try self.typed_keys.finishFrame(depth);
        if (depth == 0) self.typed_keys.finishValue();
    }
    fn object(self: *@This(), comptime T: type) anyerror!void {
        @setEvalBranchQuota(100000);
        const info = @typeInfo(T).@"struct";
        var seen: [info.field_names.len]bool = @splat(false);
        try self.expect(.object_begin);
        // A decoded schema name is used only to dispatch the next child. No
        // borrow survives that dispatch, so nested objects share this scratch.
        const key_scratch = &self.schema_keys;
        while (true) {
            if (!key_scratch.reset(.{ .retain_with_limit = 4096 })) _ = key_scratch.reset(.free_all);
            const token = try self.scanner.nextAllocMax(key_scratch.allocator(), .alloc_if_needed, self.options.max_value_len.?);
            const name = switch (token) {
                .string, .allocated_string => |name| name,
                .object_end => break,
                else => return error.UnexpectedToken,
            };
            inline for (info.field_types, T.openApiFieldMetadata, 0..) |Field, descriptor, i| {
                if (std.mem.eql(u8, name, descriptor[0])) {
                    if (seen[i]) return error.DuplicateField;
                    if (descriptor[2] and try self.scanner.peekNextTokenType() == .null) return error.UnexpectedToken;
                    try self.value(Field);
                    seen[i] = true;
                    break;
                }
            } else return error.UnknownField;
        }
        inline for (info.field_types, info.field_attrs, 0..) |Field, attrs, i| {
            if (!seen[i] and attrs.defaultValue(Field) == null) return error.MissingField;
        }
    }
    fn value(self: *@This(), comptime T: type) anyerror!void {
        if (T == std.json.Value) return self.dynamic();
        switch (@typeInfo(T)) {
            .optional => |info| {
                if (try self.scanner.peekNextTokenType() == .null) return self.expect(.null);
                return self.value(info.child);
            },
            .pointer => |info| if (info.size == .slice) {
                if (info.child != u8 or try self.scanner.peekNextTokenType() == .array_begin) return self.array(info.child);
                if (try self.scanner.peekNextTokenType() != .string) return error.UnexpectedToken;
                return self.scanner.skipValue();
            },
            .@"struct" => {
                if (@hasDecl(T, "openApiFieldMetadata")) return self.object(T);
                if (@hasField(T, "map")) {
                    const Map = @FieldType(T, "map");
                    if (@typeInfo(Map) == .@"struct" and @hasDecl(Map, "values")) {
                        const Values = @typeInfo(@TypeOf(@as(Map, undefined).values())).pointer.child;
                        if (T == std.json.ArrayHashMap(Values)) return self.map(Values);
                    }
                }
            },
            else => {},
        }
        // Enums, generated oneOf unions, and other custom leaves keep their
        // original parser. Their temporary representation retires immediately.
        var scratch = std.heap.ArenaAllocator.init(self.alloc);
        defer scratch.deinit();
        _ = try std.json.innerParse(T, scratch.allocator(), self.scanner, self.options);
    }
};

test "typed validation scans deep dynamic values without native recursion" {
    const a = std.testing.allocator;
    const depth = 10000;
    const input = try a.alloc(u8, 2 * depth + 1);
    defer a.free(input);
    @memset(input[0..depth], '[');
    input[depth] = '0';
    @memset(input[depth + 1 ..], ']');
    try validateTyped(std.json.Value, a, input);
    try std.testing.expectError(error.UnexpectedEndOfInput, validateTyped(std.json.Value, a, input[0 .. input.len - 1]));
    try std.testing.expectError(error.DuplicateField, validateTyped(std.json.Value, a, "[{\"x\":0,\"\\u0078\":1}]"));
    const Fail = struct {
        fn run(alloc: std.mem.Allocator) !void {
            try validateTyped(std.json.Value, alloc, "{\"a\":[{\"b\":1},{\"c\":[2,3]}]}");
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fail.run, .{});
}

test "value scan observes only root members and preserves lexical ranges" {
    const Capture = struct {
        count: usize = 0,
        fn member(ctx: *anyopaque, name: []const u8, raw: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const names = [_][]const u8{ "a", "b", "c", "d", "e" };
            const ranges = [_][]const u8{ "{\"nested\":[1,2]}", "[true,{\"a\":2}]", "1e2", "\"escaped \\u263a\"", "null" };
            try std.testing.expect(self.count < names.len);
            try std.testing.expectEqualStrings(names[self.count], name);
            try std.testing.expectEqualStrings(ranges[self.count], raw);
            self.count += 1;
        }
        fn run(alloc: std.mem.Allocator) !void {
            const input = "[{\"\\u0061\" : {\"nested\":[1,2]},\"b\":[true,{\"a\":2}],\"c\":1e2,\"d\":\"escaped \\u263a\",\"e\":null},0]";
            var scanner = std.json.Scanner.initCompleteInput(alloc, input);
            defer scanner.deinit();
            try std.testing.expectEqual(std.json.TokenType.array_begin, try scanner.peekNextTokenType());
            _ = try scanner.next();
            var capture: @This() = .{};
            try scanValue(alloc, &scanner, input.len, .{ .ctx = &capture, .member_fn = member });
            try std.testing.expectEqual(@as(usize, 5), capture.count);
            try std.testing.expectEqual(std.json.TokenType.number, try scanner.peekNextTokenType());
        }
    };
    try Capture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Capture.run, .{});
}

test "value scan observer survives deep stack growth and callback errors" {
    const a = std.testing.allocator;
    const depth = 10000;
    const prefix = "{\"\\u0061\":";
    const input = try a.alloc(u8, prefix.len + 2 * depth + 2);
    defer a.free(input);
    @memcpy(input[0..prefix.len], prefix);
    @memset(input[prefix.len..][0..depth], '[');
    input[prefix.len + depth] = '0';
    @memset(input[prefix.len + depth + 1 ..][0..depth], ']');
    input[input.len - 1] = '}';
    const Capture = struct {
        reject: bool,
        calls: usize = 0,
        fn member(ctx: *anyopaque, name: []const u8, raw: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqualStrings("a", name);
            try std.testing.expectEqual(@as(usize, 2 * depth + 1), raw.len);
            try std.testing.expectEqual(@as(u8, '0'), raw[depth]);
            self.calls += 1;
            if (self.reject) return error.ObserverRejected;
        }
    };
    var workspace: ValueScanner = .init(a);
    defer workspace.deinit();
    for ([_]bool{ false, true }) |reject| {
        var scanner = std.json.Scanner.initCompleteInput(a, input);
        defer scanner.deinit();
        var capture: Capture = .{ .reject = reject };
        const result = workspace.scan(&scanner, input.len, .{ .ctx = &capture, .member_fn = Capture.member });
        if (reject) try std.testing.expectError(error.ObserverRejected, result) else try result;
        try std.testing.expectEqual(@as(usize, 1), capture.calls);
        try std.testing.expectEqual(@as(usize, 0), workspace.keys.stack.capacity);
    }
}

test "value scanner reuses scratch across rows and resets after errors" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var counted: std.testing.FailingAllocator = .init(a, .{});
            var workspace: ValueScanner = .init(counted.allocator());
            defer workspace.deinit();
            const good = "{\"a\":[{\"\\u0061\":0},{\"b\":1}]}";
            var warm = std.json.Scanner.initCompleteInput(a, good);
            defer warm.deinit();
            try workspace.scan(&warm, good.len, null);
            const calls = counted.allocations;
            for (0..16) |_| {
                var scanner = std.json.Scanner.initCompleteInput(a, good);
                defer scanner.deinit();
                try workspace.scan(&scanner, good.len, null);
            }
            try std.testing.expectEqual(calls, counted.allocations);
            const bad = "{\"x\":0,\"\\u0078\":1}";
            var invalid = std.json.Scanner.initCompleteInput(a, bad);
            defer invalid.deinit();
            if (workspace.scan(&invalid, bad.len, null)) |_| return error.TestUnexpectedResult else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.DuplicateField, err);
            }
            try std.testing.expectEqual(@as(usize, 0), workspace.keys.stack.capacity);
            var recovered = std.json.Scanner.initCompleteInput(a, good);
            defer recovered.deinit();
            try workspace.scan(&recovered, good.len, null);
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "value scanner retains wide sibling tables within the shared budget" {
    const a = std.testing.allocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeByte('{');
    for (0..1000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.print("\"k{d}\":0", .{i});
    }
    try input.writer.writeByte('}');
    const Fixture = struct {
        fn run(backing: std.mem.Allocator, json: []const u8) !void {
            // Failure enumeration must not depend on whether a particular
            // Linux allocator layout can resize/remap a decoded-key arena.
            // Refuse resizes so every growth uses the counted alloc/copy path.
            var counted: std.testing.FailingAllocator = .init(backing, .{ .resize_fail_index = 0 });
            var workspace: ValueScanner = .init(counted.allocator());
            defer workspace.deinit();
            for (0..4) |iteration| {
                var scanner = std.json.Scanner.initCompleteInput(backing, json);
                defer scanner.deinit();
                const calls = counted.allocations;
                try workspace.scan(&scanner, json.len, null);
                if (iteration != 0) try std.testing.expectEqual(calls, counted.allocations);
                // Table and decoded-name storage both contribute to the
                // budget; names without escapes borrow input instead.
                try std.testing.expect(workspace.keys.table_bytes > 16 * 1024);
                if (std.mem.indexOfScalar(u8, json, '\\') == null) try std.testing.expectEqual(@as(usize, 0), workspace.keys.key_bytes);
                try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, workspace.keys.retainedBytes());
                try std.testing.expect(workspace.keys.retainedBytes() <= ValueScanner.Keys.reuse_limit);
            }
        }
    };
    try Fixture.run(a, input.written());
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{input.written()});
    var escaped = std.Io.Writer.Allocating.init(a);
    defer escaped.deinit();
    try escaped.writer.writeByte('{');
    for (0..1000) |i| {
        if (i != 0) try escaped.writer.writeByte(',');
        try escaped.writer.print("\"\\u006b{d}\":0", .{i});
    }
    try escaped.writer.writeByte('}');
    try Fixture.run(a, escaped.written());
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{escaped.written()});
}

test "value scanner retires oversized tables and recovers across depths" {
    const a = std.testing.allocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("[{\"\\u0061\":{");
    for (0..12000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.print("\"k{d}\":0", .{i});
    }
    // Retiring a child table must preserve its active ancestor set.
    try input.writer.writeAll("},\"child\":0}]");
    var counted: std.testing.FailingAllocator = .init(a, .{});
    var workspace: ValueScanner = .init(counted.allocator());
    defer workspace.deinit();
    var scanner = std.json.Scanner.initCompleteInput(a, input.written());
    defer scanner.deinit();
    try workspace.scan(&scanner, input.written().len, null);
    try std.testing.expect(workspace.keys.retainedBytes() <= ValueScanner.Keys.reuse_limit);
    try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, workspace.keys.retainedBytes());
    // Replace the final member name with the ancestor's decoded name.
    const valid_len = input.written().len;
    var duplicate = std.Io.Writer.Allocating.init(a);
    defer duplicate.deinit();
    try duplicate.writer.writeAll(input.written()[0 .. valid_len - "\"child\":0}]".len]);
    try duplicate.writer.writeAll("\"a\":0}]");
    const small = duplicate.written();
    var invalid = std.json.Scanner.initCompleteInput(a, small);
    defer invalid.deinit();
    try std.testing.expectError(error.DuplicateField, workspace.scan(&invalid, small.len, null));
    try std.testing.expectEqual(@as(usize, 0), workspace.keys.retainedBytes());
    var recovered = std.json.Scanner.initCompleteInput(a, "0");
    defer recovered.deinit();
    try workspace.scan(&recovered, 1, null);
}

test "value scanner sheds historical width before reusing tiny objects" {
    const a = std.testing.allocator;
    var wide = std.Io.Writer.Allocating.init(a);
    defer wide.deinit();
    try wide.writer.writeByte('{');
    for (0..5000) |i| {
        if (i != 0) try wide.writer.writeByte(',');
        try wide.writer.print("\"k{d}\":0", .{i});
    }
    try wide.writer.writeByte('}');
    var counted: std.testing.FailingAllocator = .init(a, .{});
    var workspace: ValueScanner = .init(counted.allocator());
    defer workspace.deinit();
    var prime = std.json.Scanner.initCompleteInput(a, wide.written());
    defer prime.deinit();
    try workspace.scan(&prime, wide.written().len, null);
    try std.testing.expect(workspace.keys.table_bytes > 64 * 1024);
    // One tiny sibling between wide objects must not force each wide object
    // to rebuild its table. Sustained small objects should retire it instead.
    for (0..8) |_| {
        const calls = counted.allocations;
        var tiny = std.json.Scanner.initCompleteInput(a, "{\"a\":0}");
        defer tiny.deinit();
        try workspace.scan(&tiny, 7, null);
        var large = std.json.Scanner.initCompleteInput(a, wide.written());
        defer large.deinit();
        try workspace.scan(&large, wide.written().len, null);
        try std.testing.expectEqual(calls, counted.allocations);
    }
    for (0..32) |i| {
        var tiny = std.json.Scanner.initCompleteInput(a, "{\"a\":0}");
        defer tiny.deinit();
        const calls = counted.allocations;
        try workspace.scan(&tiny, 7, null);
        if (i > 2) try std.testing.expectEqual(calls, counted.allocations);
        if (i != 0) try std.testing.expect(workspace.keys.table_bytes < 1024);
        try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, workspace.keys.retainedBytes());
    }
}

test "typed dynamic maps reuse sibling scratch and preserve strict object semantics" {
    const a = std.testing.allocator;
    const Map = std.json.ArrayHashMap(std.json.Value);
    for ([_][]const u8{ "null", "[]", "0" }) |invalid| try std.testing.expectError(error.UnexpectedToken, validateTyped(Map, a, invalid));
    try std.testing.expectError(error.DuplicateField, validateTyped(Map, a, "{\"x\":0,\"\\u0078\":1}"));
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            try validateTyped([]Map, alloc, "[{\"\\u0061\":[{\"a\":0}]},{\"a\":{\"b\":null}}]");
        }
    };
    try Fixture.run(a);
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{});
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeByte('[');
    for (0..100) |row| {
        if (row != 0) try input.writer.writeByte(',');
        try input.writer.writeByte('{');
        for (0..1000) |i| {
            if (i != 0) try input.writer.writeByte(',');
            try input.writer.print("\"k{d}\":0", .{i});
        }
        try input.writer.writeByte('}');
    }
    try input.writer.writeByte(']');
    var counted: std.testing.FailingAllocator = .init(a, .{});
    try validateTyped([]Map, counted.allocator(), input.written());
    try std.testing.expect(counted.allocations <= 32);
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
}

test "value scanner remains movable after decoded key allocation" {
    const a = std.testing.allocator;
    var counted: std.testing.FailingAllocator = .init(a, .{});
    var original: ValueScanner = .init(counted.allocator());
    defer original.deinit();
    const raw = "{\"\\u0061\":{\"\\u0062\":0}}";
    var prime = std.json.Scanner.initCompleteInput(a, raw);
    defer prime.deinit();
    try original.scan(&prime, raw.len, null);
    var moved = original;
    original = .init(counted.allocator());
    defer moved.deinit();
    var scanner = std.json.Scanner.initCompleteInput(a, raw);
    defer scanner.deinit();
    try moved.scan(&scanner, raw.len, null);
    try std.testing.expectEqual(@as(usize, 0), original.keys.retainedBytes());
    try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, moved.keys.retainedBytes());
}

test "wide active ancestors preserve bounded child reuse regardless of member order" {
    const a = std.testing.allocator;
    var calls: [2]usize = undefined;
    for (0..2) |mode| {
        var input: std.Io.Writer.Allocating = .init(a);
        defer input.deinit();
        try input.writer.writeByte('{');
        for (0..2) |part| {
            if (part != 0) try input.writer.writeByte(',');
            if (part == mode) {
                for (0..12000) |i| {
                    if (i != 0) try input.writer.writeByte(',');
                    try input.writer.print("\"k{d}\":0", .{i});
                }
            } else {
                try input.writer.writeAll("\"items\":[");
                for (0..1000) |i| {
                    if (i != 0) try input.writer.writeByte(',');
                    try input.writer.writeAll("{\"\\u0061\":0}");
                }
                try input.writer.writeByte(']');
            }
        }
        try input.writer.writeByte('}');
        var counted: std.testing.FailingAllocator = .init(a, .{});
        var workspace: ValueScanner = .init(counted.allocator());
        defer workspace.deinit();
        var scanner = std.json.Scanner.initCompleteInput(a, input.written());
        defer scanner.deinit();
        try workspace.scan(&scanner, input.written().len, null);
        calls[mode] = counted.allocations;
        try std.testing.expect(calls[mode] < 32);
        try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, workspace.keys.retainedBytes());
        try std.testing.expect(workspace.keys.retainedBytes() <= ValueScanner.Keys.reuse_limit);
    }
    try std.testing.expectEqual(calls[0], calls[1]);
}

test "idle key storage remains bounded while ancestors are active" {
    const Keys = @import("object_key_workspace.zig").ObjectKeyWorkspace(void);
    var counted: std.testing.FailingAllocator = .init(std.testing.allocator, .{});
    var keys: Keys = .init(counted.allocator());
    defer keys.deinit();
    for (0..6) |depth| {
        try keys.enter(depth, {});
        for (0..2000) |i| {
            var buf: [32]u8 = undefined;
            const name = try std.fmt.bufPrint(&buf, "k{d}", .{i});
            try keys.remember(depth, name, true);
        }
    }
    for (0..6) |i| {
        try keys.finishFrame(5 - i);
        try std.testing.expect(keys.idle_bytes <= Keys.reuse_limit);
        try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, keys.retainedBytes());
        var total: usize = 0;
        var idle: usize = 0;
        for (keys.stack.items) |frame| {
            total += frame.bytes;
            if (!frame.active) idle += frame.bytes;
        }
        try std.testing.expectEqual(total, keys.table_bytes + keys.key_bytes);
        try std.testing.expectEqual(idle, keys.idle_bytes);
    }
    keys.finishValue();
    try std.testing.expect(keys.retainedBytes() <= Keys.reuse_limit);
}

test "typed scalar maps reuse keys while preserving child contracts and ownership" {
    const a = std.testing.allocator;
    const Map = std.json.ArrayHashMap(f64);
    for ([_][]const u8{ "[]", "null", "{\"a\":[]}", "{\"a\":\"bad\"}", "{\"a\":0,\"\\u0061\":1}" }) |invalid| {
        if (validateTyped(Map, a, invalid)) |_| return error.ExpectedInvalidMap else |_| {}
    }
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            try validateTyped([]std.json.ArrayHashMap(Map), alloc, "[{\"\\u0061\":{\"b\":0}},{\"x\":{\"a\":1.2}}]");
        }
    };
    try Fixture.run(a);
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{});
    var input: std.Io.Writer.Allocating = .init(a);
    defer input.deinit();
    try input.writer.writeByte('[');
    for (0..1000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"\\u0061\":0.4,\"dense\":0.7}");
    }
    try input.writer.writeByte(']');
    var counted: std.testing.FailingAllocator = .init(a, .{});
    try validateTyped([]Map, counted.allocator(), input.written());
    try std.testing.expect(counted.allocations <= 4);
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
}

test "deep active traversal preserves sibling reuse and retires metadata between values" {
    const a = std.testing.allocator;
    const depth = 5000;
    var input: std.Io.Writer.Allocating = .init(a);
    defer input.deinit();
    try input.writer.splatByteAll('[', depth);
    for (0..1000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"\\u0061\":0}");
    }
    try input.writer.splatByteAll(']', depth);
    var counted: std.testing.FailingAllocator = .init(a, .{});
    var workspace: ValueScanner = .init(counted.allocator());
    defer workspace.deinit();
    var scanner = std.json.Scanner.initCompleteInput(a, input.written());
    defer scanner.deinit();
    try workspace.scan(&scanner, input.written().len, null);
    // Only stack growth and the first sibling need backing allocations.
    try std.testing.expect(counted.allocations < 32);
    try std.testing.expectEqual(@as(usize, 0), workspace.keys.retainedBytes());
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
    var next = std.json.Scanner.initCompleteInput(a, "{\"a\":0}");
    defer next.deinit();
    try workspace.scan(&next, 7, null);
    try std.testing.expect(workspace.keys.retainedBytes() <= ValueScanner.Keys.reuse_limit);
}

test "schema name scratch reuses across siblings and recursive dispatch" {
    const Leaf = struct {
        value: u32,
        pub const openApiFieldMetadata = .{.{ "value", "value", false }};
    };
    const Row = struct {
        a: u32 = 0,
        child: ?Leaf = null,
        pub const openApiFieldMetadata = .{ .{ "a", "a", false }, .{ "child", "child", false } };
    };
    const a = std.testing.allocator;
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            try validateTyped([]Row, alloc, "[{\"\\u0063hild\":{\"\\u0076alue\":1},\"\\u0061\":2},{\"\\u0061\":3}]");
        }
    };
    try Fixture.run(a);
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{});
    for ([_][]const u8{
        "{\"\\u0063hild\":{\"value\":1},\"child\":null}",
        "{\"child\":{\"value\":1,\"\\u0076alue\":2}}",
        "{\"child\":{}}",
        "{\"child\":{\"unknown\":1}}",
        "{\"a\":false}",
    }) |input| {
        var parsed = std.json.parseFromSlice(Row, a, input, .{});
        const expected: ?anyerror = if (parsed) |*result| done: {
            result.deinit();
            break :done null;
        } else |err| err;
        const actual: ?anyerror = if (validateTyped(Row, a, input)) |_| null else |err| err;
        try std.testing.expectEqual(expected, actual);
    }
    var input: std.Io.Writer.Allocating = .init(a);
    defer input.deinit();
    try input.writer.writeByte('[');
    for (0..1000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"\\u0063hild\":{\"\\u0076alue\":1},\"\\u0061\":2}");
    }
    try input.writer.writeByte(']');
    var counted: std.testing.FailingAllocator = .init(a, .{});
    try validateTyped([]Row, counted.allocator(), input.written());
    try std.testing.expect(counted.allocations <= 4);
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
}
