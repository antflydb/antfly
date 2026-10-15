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
    var validator: Validator = .{ .alloc = alloc, .scanner = &scanner, .options = .{ .allocate = .alloc_if_needed, .max_value_len = input.len }, .dynamic_scan = .init(alloc) };
    defer validator.dynamic_scan.deinit();
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
    const Frame = struct {
        object: bool,
        keys: std.heap.ArenaAllocator,
        seen: std.StringHashMapUnmanaged(void) = .empty,
    };
    allocator: std.mem.Allocator,
    stack: std.ArrayListUnmanaged(Frame) = .empty,
    table_bytes: usize = 0,
    key_bytes: usize = 0,
    const reuse_limit = 256 * 1024;

    // Count actual backing bytes, including arena headers, without depending
    // on private container layouts. Maps do not retain their allocator; arenas
    // use this adapter only during synchronous operations and restore their
    // backing allocator before returning. The workspace remains movable.
    const ScratchAllocator = struct {
        backing: std.mem.Allocator,
        bytes: *usize,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
            self.bytes.* += len;
            return result;
        }
        fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!self.backing.rawResize(buf, alignment, len, ra)) return false;
            self.bytes.* = self.bytes.* - buf.len + len;
            return true;
        }
        fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const result = self.backing.rawRemap(buf, alignment, len, ra) orelse return null;
            self.bytes.* = self.bytes.* - buf.len + len;
            return result;
        }
        fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.backing.rawFree(buf, alignment, ra);
            self.bytes.* -= buf.len;
        }
    };

    fn retainedBytes(self: *const @This()) usize {
        return self.stack.capacity * @sizeOf(Frame) + self.table_bytes + self.key_bytes;
    }

    fn finishFrame(self: *@This(), frame: *Frame, table_allocator: std.mem.Allocator) !void {
        // Clear borrowed names before retiring their decoded storage. Active
        // ancestors keep their sets; only completed frames may shed capacity.
        if (frame.seen.count() != 0) frame.seen.clearRetainingCapacity();
        var key_allocator: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.key_bytes };
        frame.keys.child_allocator = key_allocator.allocator();
        defer frame.keys.child_allocator = self.allocator;
        if (!frame.keys.reset(if (frame.keys.queryCapacity() > 16 * 1024) .free_all else .retain_capacity)) return error.OutOfMemory;
        if (self.retainedBytes() > reuse_limit) {
            frame.seen.deinit(table_allocator);
            frame.seen = .empty;
            _ = frame.keys.reset(.free_all);
        }
    }

    pub fn init(allocator: std.mem.Allocator) @This() {
        return .{ .allocator = allocator };
    }
    pub fn deinit(self: *@This()) void {
        for (self.stack.items) |*frame| {
            frame.seen.deinit(self.allocator);
            frame.keys.deinit();
        }
        self.stack.deinit(self.allocator);
        self.stack = .empty;
        self.table_bytes = 0;
        self.key_bytes = 0;
    }
    pub fn scan(self: *@This(), scanner: *std.json.Scanner, max_len: usize, observer: ?MemberObserver) !void {
        errdefer self.deinit();
        const stack = &self.stack;
        var table_allocator: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.table_bytes };
        var key_allocator: ScratchAllocator = .{ .backing = self.allocator, .bytes = &self.key_bytes };
        var root_seen = false;
        var depth: usize = 0;
        var member_name: ?[]const u8 = null;
        var member_start: usize = 0;
        while (!root_seen or depth != 0) {
            if (depth != 0) {
                const top = &stack.items[depth - 1];
                if (top.object) {
                    const token = token: {
                        top.keys.child_allocator = key_allocator.allocator();
                        defer top.keys.child_allocator = self.allocator;
                        break :token try scanner.nextAllocMax(top.keys.allocator(), .alloc_if_needed, max_len);
                    };
                    const name = switch (token) {
                        .string, .allocated_string => |name| name,
                        .object_end => {
                            try self.finishFrame(top, table_allocator.allocator());
                            depth -= 1;
                            if (depth == 1 and member_name != null) {
                                try observer.?.member_fn(observer.?.ctx, member_name.?, scanner.input[member_start..scanner.cursor]);
                                member_name = null;
                            }
                            continue;
                        },
                        else => return error.UnexpectedToken,
                    };
                    if ((try top.seen.getOrPut(table_allocator.allocator(), name)).found_existing) return error.DuplicateField;
                    if (observer != null and depth == 1) {
                        member_name = name;
                        member_start = scanner.cursor;
                        while (member_start < scanner.input.len and (std.ascii.isWhitespace(scanner.input[member_start]) or scanner.input[member_start] == ':')) : (member_start += 1) {}
                    }
                } else if (try scanner.peekNextTokenType() == .array_end) {
                    _ = try scanner.next();
                    try self.finishFrame(top, table_allocator.allocator());
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
                    if (depth == stack.items.len) try stack.append(self.allocator, .{ .object = kind == .object_begin, .keys = .init(self.allocator) });
                    stack.items[depth].object = kind == .object_begin;
                    depth += 1;
                },
                .string, .number, .true, .false, .null => {
                    try scanner.skipValue();
                    if (depth == 1 and member_name != null) {
                        try observer.?.member_fn(observer.?.ctx, member_name.?, scanner.input[member_start..scanner.cursor]);
                        member_name = null;
                    }
                },
                else => return error.UnexpectedToken,
            }
        }
        if (self.retainedBytes() > reuse_limit) self.deinit();
    }
};

const Validator = struct {
    alloc: std.mem.Allocator,
    scanner: *std.json.Scanner,
    options: std.json.ParseOptions,
    dynamic_scan: ValueScanner,

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
        try self.expect(.object_begin);
        var keys = std.heap.ArenaAllocator.init(self.alloc);
        defer keys.deinit();
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            const token = try self.scanner.nextAllocMax(keys.allocator(), .alloc_if_needed, self.options.max_value_len.?);
            const name = switch (token) {
                .string, .allocated_string => |name| name,
                .object_end => break,
                else => return error.UnexpectedToken,
            };
            if ((try seen.getOrPut(keys.allocator(), name)).found_existing) return error.DuplicateField;
            try self.value(Child);
        }
    }
    fn object(self: *@This(), comptime T: type) anyerror!void {
        @setEvalBranchQuota(100000);
        const info = @typeInfo(T).@"struct";
        var seen: [info.field_names.len]bool = @splat(false);
        try self.expect(.object_begin);
        var key_scratch = std.heap.ArenaAllocator.init(self.alloc);
        defer key_scratch.deinit();
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
        try std.testing.expectEqual(@as(usize, 0), workspace.stack.capacity);
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
            try std.testing.expectEqual(@as(usize, 0), workspace.stack.capacity);
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
            var counted: std.testing.FailingAllocator = .init(backing, .{});
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
                try std.testing.expect(workspace.table_bytes > 16 * 1024);
                if (std.mem.indexOfScalar(u8, json, '\\') == null) try std.testing.expectEqual(@as(usize, 0), workspace.key_bytes);
                try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, workspace.retainedBytes());
                try std.testing.expect(workspace.retainedBytes() <= ValueScanner.reuse_limit);
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
    try std.testing.expect(workspace.retainedBytes() <= ValueScanner.reuse_limit);
    try std.testing.expectEqual(counted.allocated_bytes - counted.freed_bytes, workspace.retainedBytes());
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
    try std.testing.expectEqual(@as(usize, 0), workspace.retainedBytes());
    var recovered = std.json.Scanner.initCompleteInput(a, "0");
    defer recovered.deinit();
    try workspace.scan(&recovered, 1, null);
}
