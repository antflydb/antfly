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

//! Allocation-free bounded structural JSON ordering and semantic hashing.
//! Object order is independent of insertion order; decimal number tokens are
//! compared exactly, without collapsing integers through floating point.
const std = @import("std");
const Json = std.json.Value;
const Order = std.math.Order;
/// Validate nesting before a dynamic JSON tree is constructed. The scanner
/// owns only a bounded nesting stack; strings and numeric tokens are borrowed.
pub fn admitText(text: []const u8, budget: *Budget) !void {
    try budget.consume(text.len);
    if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.SqlInvalidTextEncoding;
    // Sixty-four admitted levels plus one overflow bit. Preflight must not
    // allocate a dynamic stack (or any DOM) for an invalid input.
    var stack: [9]u8 = undefined;
    var fixed: std.heap.FixedBufferAllocator = .init(&stack);
    const owned = fixed.allocator().alloc(u8, stack.len) catch unreachable;
    var scanner = std.json.Scanner.initCompleteInput(fixed.allocator(), text);
    scanner.stack.bytes = std.array_list.Managed(u8).fromOwnedSlice(fixed.allocator(), owned);
    scanner.stack.bytes.items.len = 0;
    defer scanner.deinit();
    var depth: usize = 0;
    while (true) {
        const token = scanner.next() catch |err| return switch (err) {
            error.OutOfMemory => err,
            else => error.SqlInvalidTextRepresentation,
        };
        try budget.consume(1);
        switch (token) {
            .number => |raw| _ = try @import("jsonb_number.zig").Layout.init(raw, budget),
            .string, .partial_string => |raw| try validateJsonbString(raw, budget),
            .partial_string_escaped_1 => |raw| try validateJsonbString(&raw, budget),
            .partial_string_escaped_2 => |raw| try validateJsonbString(&raw, budget),
            .partial_string_escaped_3 => |raw| try validateJsonbString(&raw, budget),
            .partial_string_escaped_4 => |raw| try validateJsonbString(&raw, budget),
            .object_begin, .array_begin => {
                depth += 1;
                if (depth > 64) return error.SqlProgramLimitExceeded;
            },
            .object_end, .array_end => {
                if (depth == 0) return error.SqlInvalidTextRepresentation;
                depth -= 1;
            },
            .end_of_document => return,
            else => {},
        }
    }
}

pub const Budget = struct {
    remaining: usize = @import("resource_limits.zig").default_memory_bytes,
    /// Optional shared row/program work and cancellation identity. Local JSON
    /// admission remains independently bounded, without restarting its owner.
    shared: ?*@import("numeric_value.zig").Context = null,
    /// A numeric subkernel must not restart cancellation or spend work outside
    /// its enclosing row. The caller reconciles remaining on all exits.
    pub fn numericContext(self: *Budget, alloc: std.mem.Allocator) @import("numeric_value.zig").Context {
        var context: @import("numeric_value.zig").Context = .{
            .alloc = alloc,
            .remaining = self.remaining,
            .parent = self.shared,
        };
        if (self.shared) |shared| {
            context.max_input_bytes = shared.max_input_bytes;
            context.max_output_bytes = shared.max_output_bytes;
            context.max_groups = shared.max_groups;
        }
        return context;
    }
    pub fn consume(self: *Budget, amount: usize) !void {
        if (amount > self.remaining) return if (self.shared) |context| context.limit() else error.SqlProgramLimitExceeded;
        if (self.shared) |context| try context.charge(amount);
        self.remaining -= amount;
    }

    pub fn orderBytes(self: *Budget, left: []const u8, right: []const u8) !Order {
        const count = @min(left.len, right.len);
        var offset: usize = 0;
        while (offset < count) {
            const end = offset + @min(count - offset, 256);
            try self.consume(end - offset);
            const order = std.mem.order(u8, left[offset..end], right[offset..end]);
            if (order != .eq) return order;
            offset = end;
        }
        try self.consume(0);
        return std.math.order(left.len, right.len);
    }
};

/// Admit already materialized JSONB once at its typed ownership boundary.
/// Generic document JSON and semantic JSON-number hashing remain unrestricted
/// by PostgreSQL's JSONB numeric limits.
pub fn validateJsonbDomain(value: Json, budget: *Budget, depth: usize) !void {
    try budget.consume(1);
    if (depth > 64) return if (budget.shared) |context| context.limit() else error.SqlProgramLimitExceeded;
    switch (value) {
        .string => |raw| try validateJsonbString(raw, budget),
        .number_string => |raw| _ = try @import("jsonb_number.zig").Layout.init(raw, budget),
        .float => |number| if (!std.math.isFinite(number)) return error.SqlNumericOutOfRange,
        .array => |items| for (items.items) |item| try validateJsonbDomain(item, budget, depth + 1),
        .object => |items| for (items.keys(), items.values()) |key, item| {
            try validateJsonbString(key, budget);
            try validateJsonbDomain(item, budget, depth + 1);
        },
        else => {},
    }
}

fn validateJsonbString(text: []const u8, budget: *Budget) !void {
    var offset: usize = 0;
    while (offset < text.len) {
        const end = offset + @min(text.len - offset, 256);
        try budget.consume(end - offset);
        if (std.mem.indexOfScalar(u8, text[offset..end], 0) != null) return error.SqlUntranslatableCharacter;
        offset = end;
    }
    if (!std.unicode.utf8ValidateSlice(text)) return error.SqlInvalidTextEncoding;
}

/// PostgreSQL JSONB strings and object keys obey the text domain recursively.
/// Share this allocation-free walk between arrays, native ingress and restore.
pub fn validateTextDomain(value: Json, budget: *Budget, depth: usize) !void {
    try budget.consume(1);
    if (depth > 64) return error.SqlProgramLimitExceeded;
    switch (value) {
        .string => |text| try validateText(text, budget),
        .array => |items| for (items.items) |item| try validateTextDomain(item, budget, depth + 1),
        .object => |items| {
            for (items.keys(), items.values()) |key, item| {
                try validateText(key, budget);
                try validateTextDomain(item, budget, depth + 1);
            }
        },
        else => {},
    }
}

fn validateText(text: []const u8, budget: *Budget) !void {
    try budget.consume(text.len);
    if (!std.unicode.utf8ValidateSlice(text)) return error.SqlInvalidTextEncoding;
    if (std.mem.indexOfScalar(u8, text, 0) != null) return error.SqlUntranslatableCharacter;
}

/// The caller owns the allocation region and its byte admission. All retained
/// tokens own their bytes, including exact decimal tokens and escaped strings.
pub fn parseTextLeaky(a: std.mem.Allocator, text: []const u8, budget: *Budget) !Json {
    try admitText(text, budget);
    return parseTreeLeaky(a, text, budget, true);
}

/// Temporary structural comparison may borrow unescaped tokens from a pinned
/// immutable input. The input and allocator must both outlive the returned
/// tree; unlike parseTextLeaky this is not independently owned decoded data.
pub fn parsePinnedTextLeaky(a: std.mem.Allocator, text: []const u8, budget: *Budget) !Json {
    try admitText(text, budget);
    return parseTreeLeaky(a, text, budget, false);
}

fn parseTreeLeaky(a: std.mem.Allocator, text: []const u8, budget: *Budget, owned: bool) !Json {
    // One cooperative assembler supplies both lifetime policies. Owned mode
    // copies tokens; pinned mode borrows unescaped immutable source slices.
    var scanner = std.json.Scanner.initCompleteInput(a, text);
    defer scanner.deinit();
    const value = try parseNode(a, &scanner, budget, text.len, try nextToken(a, &scanner, text.len, owned), 0, owned);
    if (try nextToken(a, &scanner, text.len, owned) != .end_of_document) return error.SqlInvalidTextRepresentation;
    return value;
}

fn nextToken(a: std.mem.Allocator, scanner: *std.json.Scanner, max_len: usize, owned: bool) !std.json.Token {
    return scanner.nextAllocMax(a, if (owned) .alloc_always else .alloc_if_needed, max_len) catch |err| return switch (err) {
        error.OutOfMemory => err,
        else => error.SqlInvalidTextRepresentation,
    };
}

fn parseNode(a: std.mem.Allocator, scanner: *std.json.Scanner, budget: *Budget, max_len: usize, token: std.json.Token, depth: usize, owned: bool) anyerror!Json {
    try budget.consume(1);
    if (depth > 64) return if (budget.shared) |context| context.limit() else error.SqlProgramLimitExceeded;
    return switch (token) {
        .string, .allocated_string => |text| .{ .string = text },
        .number, .allocated_number => |text| .{ .number_string = text },
        .null => .null,
        .true => .{ .bool = true },
        .false => .{ .bool = false },
        .array_begin => value: {
            var items = std.json.Array.init(a);
            while (true) {
                const next = try nextToken(a, scanner, max_len, owned);
                if (next == .array_end) break;
                try items.append(try parseNode(a, scanner, budget, max_len, next, depth + 1, owned));
            }
            break :value .{ .array = items };
        },
        .object_begin => value: {
            var items: std.json.ObjectMap = .empty;
            while (true) {
                try budget.consume(1);
                const next = try nextToken(a, scanner, max_len, owned);
                if (next == .object_end) break;
                const key = switch (next) {
                    .string, .allocated_string => |text| text,
                    else => return error.SqlInvalidTextRepresentation,
                };
                const child = try parseNode(a, scanner, budget, max_len, try nextToken(a, scanner, max_len, owned), depth + 1, owned);
                try items.put(a, key, child);
            }
            break :value .{ .object = items };
        },
        else => error.SqlInvalidTextRepresentation,
    };
}

const DomainReference = struct {
    entries: []const struct {
        input: []const u8,
        @"error": ?[]const u8 = null,
        kind: ?[]const u8 = null,
        length: ?usize = null,
        md5: ?[]const u8 = null,
        text: ?[]const u8 = null,
    },
    binary_errors: []const struct { binary: []const u8, @"error": []const u8 },
};

fn domainArrayBinary(a: std.mem.Allocator, input: []const u8) ![]u8 {
    const bytes = try a.alloc(u8, 25 + input.len);
    std.mem.writeInt(i32, bytes[0..4], 1, .big);
    std.mem.writeInt(i32, bytes[4..8], 0, .big);
    std.mem.writeInt(u32, bytes[8..12], 3802, .big);
    std.mem.writeInt(i32, bytes[12..16], 1, .big);
    std.mem.writeInt(i32, bytes[16..20], 1, .big);
    std.mem.writeInt(i32, bytes[20..24], @intCast(input.len + 1), .big);
    bytes[24] = 1;
    @memcpy(bytes[25..], input);
    return bytes;
}

fn domainArrayText(a: std.mem.Allocator, input: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(a);
    errdefer out.deinit();
    try out.writer.writeAll("{\"");
    for (input) |byte| {
        if (byte == '"' or byte == '\\') try out.writer.writeByte('\\');
        try out.writer.writeByte(byte);
    }
    try out.writer.writeAll("\"}");
    return out.toOwnedSlice();
}

test "SQL JSONB domain matches PostgreSQL owned pinned scalar and array admission" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(DomainReference, a, @embedFile("fixtures/sql_jsonb_domain_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 22), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        const binary = try domainArrayBinary(a, entry.input);
        defer a.free(binary);
        const text = try domainArrayText(a, entry.input);
        defer a.free(text);
        for (0..5) |mode| {
            var memory: @import("memory_budget.zig") = .{ .backing = a, .limit = 4096 };
            var region: std.heap.ArenaAllocator = .init(memory.allocator());
            defer region.deinit();
            const r = region.allocator();
            var work: Budget = .{ .remaining = 4096 };
            var frame: ?@import("parameter_frame.zig").Frame = null;
            defer if (frame) |*owned| owned.deinit();
            const result: anyerror!Json = switch (mode) {
                0 => parseTextLeaky(r, entry.input, &work),
                1 => parsePinnedTextLeaky(r, entry.input, &work),
                2 => if (@import("array_text.zig").decodeLeaky(r, .jsonb, text, .{ .values = .{ .bytes = 4096, .work = 4096 } })) |decoded| decoded.value.elements[0].value else |err| err,
                3 => if (@import("array_binary.zig").decodeLeaky(r, .jsonb, binary, .{ .values = .{ .bytes = 4096, .work = 4096 } })) |decoded| decoded.value.elements[0].value else |err| err,
                4 => value: {
                    frame = @import("parameter_frame.zig").Frame.prepare(a, &.{.{ .kind = .json, .element_type = .jsonb }}, &.{.{ .text = entry.input }}, .{ .bytes = 4096, .work = 4096 }) catch |err| break :value err;
                    try std.testing.expect(!frame.?.values[0].sql_null);
                    break :value frame.?.values[0].value;
                },
                else => unreachable,
            };
            if (entry.@"error") |code| {
                if (result) |_| return error.ExpectedPostgresRejection else |err| try std.testing.expectEqualStrings(code, @import("errors.zig").describe(err).code);
                continue;
            }
            const value = try result;
            try std.testing.expect(memory.peak <= 4096);
            work = .{};
            const display = try @import("jsonb_text.zig").format(a, value, 256 * 1024, &work);
            defer a.free(display);
            try std.testing.expectEqual(entry.length.?, try std.unicode.utf8CountCodepoints(display));
            var digest: [16]u8 = undefined;
            std.crypto.hash.Md5.hash(display, &digest, .{});
            try std.testing.expectEqualStrings(entry.md5.?, &std.fmt.bytesToHex(digest, .lower));
            if (entry.text) |expected| try std.testing.expectEqualStrings(expected, display);
        }
    }
    for (fixture.value.binary_errors) |entry| {
        const bytes = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(bytes);
        _ = try std.fmt.hexToBytes(bytes, entry.binary);
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        var work: Budget = .{};
        if (@import("array_binary.zig").decodeElementLeaky(region.allocator(), .jsonb, bytes, &work)) |_| return error.ExpectedPostgresRejection else |err| try std.testing.expectEqualStrings(entry.@"error", @import("errors.zig").describe(err).code);
    }
}

test "SQL JSONB domain admits non-rendered large parameters without exponent expansion" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |array| {
        const statement = if (array) "cardinality($1)" else "jsonb_typeof($1)";
        const descriptor: @import("scalar.zig").Type = .{ .kind = if (array) .array else .json, .element_type = .jsonb };
        var compiled = try @import("compiler.zig").compileScalar(a, statement, .{});
        defer compiled.deinit();
        var program = try @import("scalar.zig").bindTyped(a, compiled.expression, &.{}, &.{descriptor}, .{});
        defer program.deinit();
        var frame = try @import("parameter_frame.zig").Frame.prepare(a, program.parameter_descriptors, &.{.{ .text = if (array) "{1e131071}" else "1e131071" }}, .{ .bytes = 4096, .work = 512 });
        defer frame.deinit();
        try std.testing.expect(frame.budget.peak <= 4096);
        const bound = try program.bindParameters(&frame);
        const result = try bound.evaluate(std.mem.Allocator.failing, &.{}, .{ .steps = 512 });
        if (array) try std.testing.expectEqual(@as(i64, 1), result.value.integer) else try std.testing.expectEqualStrings("number", result.value.string);
        try std.testing.expectError(error.SqlNumericOutOfRange, @import("parameter_frame.zig").Frame.prepare(a, program.parameter_descriptors, &.{.{ .text = if (array) "{1e131072}" else "1e131072" }}, .{ .bytes = 4096, .work = 512 }));
    }
}

test "SQL JSONB domain validates discarded duplicate values before allocating a DOM" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "{\"k\":1e131072,\"k\":2}", "{\"k\":\"\\u0000\",\"k\":2}" }) |input| {
        var memory: @import("memory_budget.zig") = .{ .backing = a, .limit = 128 };
        var work: Budget = .{};
        const expected = if (std.mem.indexOf(u8, input, "131072") != null) error.SqlNumericOutOfRange else error.SqlUntranslatableCharacter;
        try std.testing.expectError(expected, parseTextLeaky(memory.allocator(), input, &work));
        try std.testing.expect(memory.peak <= 128);
        try std.testing.expectEqual(@as(usize, 0), memory.live);
    }
    var work: Budget = .{ .remaining = 64 };
    try std.testing.expectError(error.SqlNumericOutOfRange, parseTextLeaky(std.mem.Allocator.failing, "1e131072", &work));
    work = .{ .remaining = 64 };
    try validateJsonbDomain(.{ .number_string = "1e131071" }, &work, 0);
    try std.testing.expect(work.remaining > 32);
    const document = try @import("../storage/db/document_content_hash.zig").canonicalJsonValueAlloc(a, .{ .number_string = "1e999999999999999999999" });
    defer a.free(document);
    try std.testing.expectEqualStrings("1e999999999999999999999", document);
}

test "SQL JSONB domain parser preserves lifetimes and every allocation failure" {
    const a = std.testing.allocator;
    const input = "{\"k\":[\"old\",1],\"\\u006b\":{\"text\":\"line\\ntext\",\"n\":1.20}}";
    const Faults = struct {
        fn run(alloc: std.mem.Allocator, pinned: bool) !void {
            var region = std.heap.ArenaAllocator.init(alloc);
            defer region.deinit();
            var work: Budget = .{};
            const value = if (pinned) try parsePinnedTextLeaky(region.allocator(), input, &work) else try parseTextLeaky(region.allocator(), input, &work);
            try std.testing.expectEqual(@as(usize, 1), value.object.count());
            try std.testing.expectEqualStrings("1.20", value.object.get("k").?.object.get("n").?.number_string);
        }
    };
    var deterministic = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    for ([_]bool{ false, true }) |pinned| try std.testing.checkAllAllocationFailures(deterministic.allocator(), Faults.run, .{pinned});
    const copied = try a.dupe(u8, input);
    defer a.free(copied);
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var work: Budget = .{};
    var value = try parseTextLeaky(region.allocator(), copied, &work);
    @memset(copied, 0);
    try std.testing.expectEqualStrings("line\ntext", value.object.get("k").?.object.get("text").?.string);
    try std.testing.expectEqualStrings("1.20", value.object.get("k").?.object.get("n").?.number_string);
    try value.object.getPtr("k").?.object.put(region.allocator(), "after", .{ .bool = true });
}

test "SQL JSONB domain cancellation is sticky through lexical and DOM phases" {
    const a = std.testing.allocator;
    const source = "[{\"n\":1.20,\"v\":[\"a\",\"b\"]},{\"n\":2.00,\"v\":[\"c\",\"d\"]}]";
    const Counter = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        fn poll(ptr: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.calls == self.stop) return error.QueryCanceled;
        }
    };
    // Repeat enough nodes to checkpoint during DOM assembly, not only the
    // lexical pass. Source construction is outside the measured region.
    var encoded: std.Io.Writer.Allocating = .init(a);
    defer encoded.deinit();
    try encoded.writer.writeByte('[');
    for (0..32) |i| {
        if (i != 0) try encoded.writer.writeByte(',');
        try encoded.writer.writeAll(source);
    }
    try encoded.writer.writeByte(']');
    for ([_]bool{ false, true }) |pinned| {
        var counter: Counter = .{};
        var owner: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Counter.poll, .ptr = &counter };
        var work: Budget = .{ .shared = &owner };
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        if (pinned) _ = try parsePinnedTextLeaky(region.allocator(), encoded.written(), &work) else _ = try parseTextLeaky(region.allocator(), encoded.written(), &work);
        try std.testing.expect(counter.calls > 4);
        for (1..counter.calls + 1) |stop| {
            var failed: Counter = .{ .stop = stop };
            owner = .{ .alloc = a, .checkpoint = Counter.poll, .ptr = &failed };
            work = .{ .shared = &owner };
            var temporary = std.heap.ArenaAllocator.init(a);
            defer temporary.deinit();
            if (pinned) try std.testing.expectError(error.QueryCanceled, parsePinnedTextLeaky(temporary.allocator(), encoded.written(), &work)) else try std.testing.expectError(error.QueryCanceled, parseTextLeaky(temporary.allocator(), encoded.written(), &work));
            try std.testing.expect(owner.failure.? == error.QueryCanceled);
            const remaining = owner.remaining;
            try std.testing.expectError(error.QueryCanceled, parseTextLeaky(std.mem.Allocator.failing, "1", &work));
            try std.testing.expectEqual(remaining, owner.remaining);
        }
    }
    var limited: @import("numeric_value.zig").Context = .{ .alloc = std.mem.Allocator.failing, .remaining = 1 };
    var work: Budget = .{ .shared = &limited };
    try std.testing.expectError(error.SqlProgramLimitExceeded, parseTextLeaky(std.mem.Allocator.failing, "1.20", &work));
    try std.testing.expect(limited.failure.? == error.SqlProgramLimitExceeded);
    var genuine: @import("numeric_value.zig").Context = .{ .alloc = std.mem.Allocator.failing };
    work = .{ .shared = &genuine };
    try std.testing.expectError(error.OutOfMemory, parseTextLeaky(std.mem.Allocator.failing, "[1]", &work));
    try std.testing.expect(genuine.failure == null);
}

test "SQL pinned JSON structural parsing preserves borrowed tokens escapes and strict syntax" {
    const a = std.testing.allocator;
    const inputs = [_][]const u8{
        "{\"x\":1,\"x\":2}", "{\"k\":1,\"\\u006b\":1.20}",
        "null",              "true",
        "false",             "[]",
        "{}",                "9007199254740993.000",
        "-1e1000",           "\"plain\"",
        "\"line\\ntext\"",   "{\"\\u0078\":[1,null,true,{},[\"a\\tb\"]]}",
    };
    for (inputs) |input| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var work: Budget = .{};
        const pinned = try parsePinnedTextLeaky(arena.allocator(), input, &work);
        var owned = try std.json.parseFromSlice(Json, a, input, .{ .parse_numbers = false, .duplicate_field_behavior = .use_last });
        defer owned.deinit();
        try std.testing.expectEqual(Order.eq, try compare(pinned, owned.value, &work, 0));
        if (std.mem.eql(u8, input, "\"plain\"")) try std.testing.expectEqual(@intFromPtr(input.ptr + 1), @intFromPtr(pinned.string.ptr));
    }
    for ([_][]const u8{ "{", "[] true", "[1,]", "1e", "\"bad\\xescape\"" }) |input| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var work: Budget = .{};
        try std.testing.expectError(error.SqlInvalidTextRepresentation, parsePinnedTextLeaky(arena.allocator(), input, &work));
    }
    const Run = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var work: Budget = .{};
            const value = try parsePinnedTextLeaky(arena.allocator(), "{\"\\u0078\":[1,null,true,{},[\"a\\tb\"]]}", &work);
            try std.testing.expect(value.object.contains("x"));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{});
}

/// A temporary quota allocator may own admission, but managed JSON arrays
/// must retain the enclosing region's stable allocator, not its stack wrapper.
pub fn rehomeArrayAllocators(value: *Json, owner: std.mem.Allocator, budget: *Budget, depth: usize) !void {
    try budget.consume(1);
    if (depth > 64) return error.SqlProgramLimitExceeded;
    switch (value.*) {
        .array => |*items| {
            items.allocator = owner;
            for (items.items) |*item| try rehomeArrayAllocators(item, owner, budget, depth + 1);
        },
        .object => |*items| for (items.values()) |*item| try rehomeArrayAllocators(item, owner, budget, depth + 1),
        else => {},
    }
}

test "JSON text admission bounds nesting before DOM allocation and unwinds faults" {
    const a = std.testing.allocator;
    var nested: [131]u8 = undefined;
    @memset(nested[0..65], '[');
    nested[65] = '0';
    @memset(nested[66..], ']');
    var budget: Budget = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, parseTextLeaky(a, &nested, &budget));
    budget = .{};
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    _ = try parseTextLeaky(arena.allocator(), nested[1..130], &budget);
    budget = .{ .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, admitText("[0]", &budget));
    budget = .{};
    try std.testing.expectError(error.SqlInvalidTextRepresentation, admitText("[0,]", &budget));
    const Faults = struct {
        fn run(backing: std.mem.Allocator) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            var work: Budget = .{};
            _ = try parseTextLeaky(region.allocator(), "{\"n\":9007199254740993,\"s\":\"escaped\\ntext\",\"a\":[null,true]}", &work);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{});
}

const Decimal = struct {
    negative: bool,
    magnitude: i64,
    digits: []const u8,

    fn parse(text: []const u8, budget: *Budget) !Decimal {
        try budget.consume(text.len);
        if (text.len == 0) return error.SqlTypeMismatch;
        var at: usize = @intFromBool(text[0] == '-');
        const start = at;
        var before: i64 = 0;
        var leading: i64 = 0;
        var seen_dot = false;
        var first: ?usize = null;
        var last: usize = 0;
        while (at < text.len and text[at] != 'e' and text[at] != 'E') : (at += 1) {
            if (text[at] == '.' and !seen_dot) {
                seen_dot = true;
                continue;
            }
            if (!std.ascii.isDigit(text[at])) return error.SqlTypeMismatch;
            if (!seen_dot) before += 1;
            if (text[at] != '0') {
                if (first == null) first = at;
                last = at;
            } else if (first == null) leading += 1;
        }
        if (at == start) return error.SqlTypeMismatch;
        const exponent = if (at < text.len) std.fmt.parseInt(i64, text[at + 1 ..], 10) catch return error.SqlNumericOutOfRange else 0;
        if (first == null) return .{ .negative = false, .magnitude = 0, .digits = "" };
        const magnitude = std.math.add(i64, exponent, before - leading - 1) catch return error.SqlNumericOutOfRange;
        return .{ .negative = text[0] == '-', .magnitude = magnitude, .digits = text[first.? .. last + 1] };
    }

    fn compare(a: Decimal, b: Decimal) Order {
        if (a.negative != b.negative) return if (a.negative) .lt else .gt;
        if (a.digits.len == 0 or b.digits.len == 0) return if (a.digits.len == b.digits.len) .eq else if (a.digits.len == 0) (if (b.negative) .gt else .lt) else if (a.negative) .lt else .gt;
        var order = std.math.order(a.magnitude, b.magnitude);
        if (order == .eq) {
            var i: usize = 0;
            var j: usize = 0;
            while (i < a.digits.len or j < b.digits.len) {
                if (i < a.digits.len and a.digits[i] == '.') i += 1;
                if (j < b.digits.len and b.digits[j] == '.') j += 1;
                const ac: u8 = if (i < a.digits.len) a.digits[i] else '0';
                const bc: u8 = if (j < b.digits.len) b.digits[j] else '0';
                order = std.math.order(ac, bc);
                if (order != .eq) break;
                i += @intFromBool(i < a.digits.len);
                j += @intFromBool(j < b.digits.len);
            }
        }
        return if (a.negative) order.invert() else order;
    }

    fn hash(self: Decimal) u64 {
        var state = std.hash.Wyhash.init(2);
        state.update(&.{@intFromBool(self.negative)});
        var magnitude: [8]u8 = undefined;
        std.mem.writeInt(i64, &magnitude, self.magnitude, .little);
        state.update(&magnitude);
        for (self.digits) |digit| if (digit != '.') state.update(&.{digit});
        return state.final();
    }
};

/// PostgreSQL NUMERIC-to-integer rounding (ties away from zero), without a
/// float intermediate. Work is bounded by input bytes plus at most 20 digits.
pub fn roundedInteger(text: []const u8, budget: *Budget) !i64 {
    const value = try Decimal.parse(text, budget);
    if (value.digits.len == 0 or value.magnitude < -1) return 0;
    if (value.magnitude > 18) return error.SqlNumericOutOfRange;
    var at: usize = 0;
    var magnitude: u64 = 0;
    const count: usize = @intCast(@max(0, value.magnitude + 1));
    for (0..count) |_| {
        if (at < value.digits.len and value.digits[at] == '.') at += 1;
        const digit = if (at < value.digits.len) value.digits[at] - '0' else 0;
        magnitude = magnitude * 10 + digit;
        at += @intFromBool(at < value.digits.len);
    }
    if (at < value.digits.len and value.digits[at] == '.') at += 1;
    if (at < value.digits.len and value.digits[at] >= '5') magnitude += 1;
    if (magnitude > @as(u64, std.math.maxInt(i64)) + @intFromBool(value.negative)) return error.SqlNumericOutOfRange;
    if (value.negative and magnitude == @as(u64, 1) << 63) return std.math.minInt(i64);
    return if (value.negative) -@as(i64, @intCast(magnitude)) else @intCast(magnitude);
}

fn rank(value: Json) u8 {
    return switch (value) {
        .null => 0,
        .string => 1,
        .integer, .float, .number_string => 2,
        .bool => 3,
        .array => 4,
        .object => 5,
    };
}
fn decimal(value: Json, buffer: []u8, budget: *Budget) !Decimal {
    const text = switch (value) {
        .integer => |v| try std.fmt.bufPrint(buffer, "{d}", .{v}),
        .float => |v| {
            const parts = @import("../common/json_float_decimal.zig").Parts.init(v) catch return error.SqlNumericOutOfRange;
            try budget.consume(parts.work());
            var result = try Decimal.parse(try parts.coefficientText(buffer), budget);
            result.negative = parts.negative;
            result.magnitude += parts.decimalExponent();
            return result;
        },
        .number_string => |v| v,
        else => return error.SqlTypeMismatch,
    };
    return Decimal.parse(text, budget);
}
fn keyOrder(a: []const u8, b: []const u8) Order {
    const lengths = std.math.order(a.len, b.len);
    return if (lengths == .eq) std.mem.order(u8, a, b) else lengths;
}

/// Every finite primitive number has one exact dyadic representation. Hash
/// that representation without formatting decimal text or using big integers.
/// Decimal JSON tokens enter this domain only after an exact equality check.
fn primitiveHash(value: Json) !u64 {
    var negative: bool = false;
    var mantissa: u64 = 0;
    var exponent: i32 = 0;
    switch (value) {
        .integer => |n| {
            negative = n < 0;
            mantissa = @intCast(@abs(n));
        },
        .float => |n| {
            if (!std.math.isFinite(n)) return error.SqlNumericOutOfRange;
            const bits: u64 = @bitCast(n);
            negative = bits >> 63 != 0;
            const raw = (bits >> 52) & 0x7ff;
            mantissa = bits & 0xfffffffffffff;
            if (raw != 0) mantissa |= 1 << 52;
            exponent = if (raw == 0) -1074 else @as(i32, @intCast(raw)) - 1023 - 52;
        },
        else => unreachable,
    }
    if (mantissa == 0) {
        negative = false;
        exponent = 0;
    } else {
        const zeros = @ctz(mantissa);
        mantissa >>= @intCast(zeros);
        exponent += zeros;
    }
    var bytes: [13]u8 = undefined;
    bytes[0] = @intFromBool(negative);
    std.mem.writeInt(u64, bytes[1..9], mantissa, .little);
    std.mem.writeInt(i32, bytes[9..13], exponent, .little);
    return std.hash.Wyhash.hash(2, &bytes);
}
fn tokenHash(text: []const u8, budget: *Budget) !u64 {
    const normalized = try Decimal.parse(text, budget);
    if (normalized.digits.len == 0) return primitiveHash(.{ .integer = 0 });
    // Reconstruct only the bounded integer domain, including spellings such
    // as 9007199254740993.0 that must not round through an f64.
    if (normalized.magnitude >= 0 and normalized.magnitude <= 18) integer: {
        var magnitude: u64 = 0;
        var digits: usize = 0;
        for (normalized.digits) |digit| {
            if (digit == '.') continue;
            magnitude = std.math.mul(u64, magnitude, 10) catch break :integer;
            magnitude = std.math.add(u64, magnitude, digit - '0') catch break :integer;
            digits += 1;
        }
        const width: usize = @intCast(normalized.magnitude + 1);
        if (digits > width) break :integer;
        for (digits..width) |_| magnitude = std.math.mul(u64, magnitude, 10) catch break :integer;
        const signed: i128 = if (normalized.negative) -@as(i128, magnitude) else magnitude;
        if (std.math.cast(i64, signed)) |n| return primitiveHash(.{ .integer = n });
    }
    const number = std.fmt.parseFloat(f64, text) catch return normalized.hash();
    if (std.math.isFinite(number)) {
        var buffer: [768]u8 = undefined;
        if (Decimal.compare(normalized, try decimal(.{ .float = number }, &buffer, budget)) == .eq) return primitiveHash(.{ .float = number });
    }
    return normalized.hash();
}

/// Observable JSONB display equality for durable generated-result verification.
/// SQL comparison/hash intentionally ignore numeric display scale; this does
/// not. Inputs are admitted typed JSONB. No output rendering, exponent
/// expansion, sort vectors or heap allocation are needed.
pub fn sameDisplay(a: Json, b: Json, budget: *Budget, depth: usize) anyerror!bool {
    try budget.consume(1);
    if (depth > 64) return if (budget.shared) |owner| owner.limit() else error.SqlProgramLimitExceeded;
    if (rank(a) != rank(b)) return false;
    if (rank(a) == 2) {
        var left: [768]u8 = undefined;
        var right: [768]u8 = undefined;
        if (Decimal.compare(try decimal(a, &left, budget), try decimal(b, &right, budget)) != .eq) return false;
        return try displayScale(a, budget) == try displayScale(b, budget);
    }
    return switch (a) {
        .null => true,
        .bool => a.bool == b.bool,
        .string => try budget.orderBytes(a.string, b.string) == .eq,
        .array => equal: {
            if (a.array.items.len != b.array.items.len) break :equal false;
            for (a.array.items, b.array.items) |x, y| if (!try sameDisplay(x, y, budget, depth + 1)) break :equal false;
            break :equal true;
        },
        .object => equal: {
            if (a.object.count() != b.object.count()) break :equal false;
            for (a.object.keys(), a.object.values()) |key, value| {
                try budget.consume(key.len + 1);
                const other = b.object.get(key) orelse break :equal false;
                if (!try sameDisplay(value, other, budget, depth + 1)) break :equal false;
            }
            break :equal true;
        },
        else => unreachable,
    };
}

fn displayScale(value: Json, work: *Budget) !u16 {
    return switch (value) {
        .integer => 0,
        .number_string => |raw| (try @import("jsonb_number.zig").Layout.init(raw, work)).scale,
        .float => |number| scale: {
            const parts = @import("../common/json_float_decimal.zig").Parts.init(number) catch return error.SqlNumericOutOfRange;
            break :scale @intCast(-parts.decimalExponent());
        },
        else => error.SqlTypeMismatch,
    };
}

test "SQL JSONB display equality matches PostgreSQL without changing logical hashes" {
    const a = std.testing.allocator;
    const Fixture = struct { entries: []const struct { left: []const u8, right: []const u8, logical_equal: bool, display_equal: bool } };
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_jsonb_display_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 19), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        var parse: Budget = .{};
        const left = try parseTextLeaky(region.allocator(), entry.left, &parse);
        const right = try parseTextLeaky(region.allocator(), entry.right, &parse);
        var work: Budget = .{};
        try std.testing.expectEqual(entry.logical_equal, try compare(left, right, &work, 0) == .eq);
        try std.testing.expectEqual(entry.display_equal, try sameDisplay(left, right, &work, 0));
        if (entry.logical_equal) try std.testing.expectEqual(try hash(left, &work, 0), try hash(right, &work, 0));
    }
    var work: Budget = .{ .remaining = 512 };
    try std.testing.expect(try sameDisplay(.{ .number_string = "1e131071" }, .{ .number_string = "10e131070" }, &work, 0));
    try std.testing.expect(512 - work.remaining < 128);
    for ([_]struct { left: Json, right: Json, expected: bool }{
        .{ .left = .{ .float = 0.125 }, .right = .{ .number_string = "0.1250" }, .expected = false },
        .{ .left = .{ .float = 2.5 }, .right = .{ .number_string = "25e-1" }, .expected = true },
        .{ .left = .{ .float = -0.0 }, .right = .{ .integer = 0 }, .expected = true },
        .{ .left = .{ .float = 0.1 }, .right = .{ .number_string = "0.1" }, .expected = false },
    }) |entry| {
        work = .{};
        try std.testing.expectEqual(entry.expected, try sameDisplay(entry.left, entry.right, &work, 0));
    }
}

test "SQL JSONB display equality retains exact shared quota and every cancellation cut" {
    const Context = @import("numeric_value.zig").Context;
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = 0,
        fn check(ptr: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    const key: [512]u8 = @splat('k');
    var object: std.json.ObjectMap = .empty;
    defer object.deinit(std.testing.allocator);
    var cells: [64]Json = @splat(.{ .number_string = "1.20" });
    const array = std.json.Array.fromOwnedSlice(std.mem.Allocator.failing, &cells);
    // The stack vector is borrowed for this immutable kernel fixture.
    try object.put(std.testing.allocator, &key, .{ .array = array });
    const value: Json = .{ .object = object };
    var observed: Control = .{};
    var parent: Context = .{ .alloc = .failing, .checkpoint = Control.check, .ptr = &observed };
    var child: Context = .{ .alloc = .failing, .parent = &parent };
    var work: Budget = .{ .shared = &child };
    try std.testing.expect(try sameDisplay(value, value, &work, 0));
    const cost = 8 * 1024 * 1024 - parent.remaining;
    try std.testing.expectEqual(parent.remaining, child.remaining);
    var exact: Context = .{ .alloc = .failing, .remaining = cost };
    work = .{ .remaining = @intCast(cost), .shared = &exact };
    try std.testing.expect(try sameDisplay(value, value, &work, 0));
    try std.testing.expectEqual(@as(u64, 0), exact.remaining);
    var short: Context = .{ .alloc = .failing, .remaining = cost - 1 };
    work = .{ .shared = &short };
    try std.testing.expectError(error.SqlProgramLimitExceeded, sameDisplay(value, value, &work, 0));
    try std.testing.expectError(error.SqlProgramLimitExceeded, sameDisplay(.null, .{ .bool = true }, &work, 0));
    try std.testing.expect(observed.polls > 1);
    for (1..observed.polls + 1) |cut| {
        var control: Control = .{ .fail_at = cut };
        var canceled: Context = .{ .alloc = .failing, .checkpoint = Control.check, .ptr = &control };
        work = .{ .shared = &canceled };
        try std.testing.expectError(error.QueryCanceled, sameDisplay(value, value, &work, 0));
        const remaining = canceled.remaining;
        try std.testing.expectError(error.QueryCanceled, sameDisplay(.null, .{ .bool = true }, &work, 0));
        try std.testing.expectEqual(remaining, canceled.remaining);
    }
}

pub fn compare(a: Json, b: Json, budget: *Budget, depth: usize) anyerror!Order {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    try budget.consume(1);
    const ranks = std.math.order(rank(a), rank(b));
    if (ranks != .eq) return ranks;
    if (rank(a) == 2) {
        var left: [768]u8 = undefined;
        var right: [768]u8 = undefined;
        return Decimal.compare(try decimal(a, &left, budget), try decimal(b, &right, budget));
    }
    return switch (a) {
        .null => .eq,
        .bool => std.math.order(@intFromBool(a.bool), @intFromBool(b.bool)),
        .string => budget.orderBytes(a.string, b.string),
        .array => blk: {
            const sizes = std.math.order(a.array.items.len, b.array.items.len);
            if (sizes != .eq) break :blk sizes;
            for (a.array.items, b.array.items) |left, right| {
                const result = try compare(left, right, budget, depth + 1);
                if (result != .eq) break :blk result;
            }
            break :blk .eq;
        },
        .object => blk: {
            const sizes = std.math.order(a.object.count(), b.object.count());
            if (sizes != .eq) break :blk sizes;
            var smallest: ?[]const u8 = null;
            var result: Order = .eq;
            for (a.object.keys(), a.object.values()) |key, value| {
                try budget.consume(key.len + 1);
                const other = b.object.get(key);
                const difference: Order = if (other) |item| try compare(value, item, budget, depth + 1) else .lt;
                if (difference != .eq and (smallest == null or keyOrder(key, smallest.?) == .lt)) {
                    smallest = key;
                    result = difference;
                }
            }
            for (b.object.keys()) |key| {
                try budget.consume(key.len + 1);
                if (!a.object.contains(key) and (smallest == null or keyOrder(key, smallest.?) == .lt)) {
                    smallest = key;
                    result = .gt;
                }
            }
            break :blk result;
        },
        else => unreachable,
    };
}

pub fn hash(value: Json, budget: *Budget, depth: usize) anyerror!u64 {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    try budget.consume(1);
    if (rank(value) == 2) {
        return switch (value) {
            .integer, .float => primitiveHash(value),
            .number_string => |text| tokenHash(text, budget),
            else => unreachable,
        };
    }
    var state = std.hash.Wyhash.init(rank(value));
    switch (value) {
        .null => {},
        .bool => state.update(&.{@intFromBool(value.bool)}),
        .string => {
            try budget.consume(value.string.len);
            state.update(value.string);
        },
        .array => for (value.array.items) |item| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, try hash(item, budget, depth + 1), .little);
            state.update(&bytes);
        },
        .object => {
            var sum: u64 = 0;
            var mixed: u64 = 0;
            for (value.object.keys(), value.object.values()) |key, item| {
                try budget.consume(key.len + 1);
                const pair = std.hash.Wyhash.hash(try hash(item, budget, depth + 1), key);
                sum +%= pair;
                mixed ^= std.math.rotl(u64, pair, 23);
            }
            var bytes: [16]u8 = undefined;
            std.mem.writeInt(u64, bytes[0..8], sum, .little);
            std.mem.writeInt(u64, bytes[8..16], mixed, .little);
            state.update(&bytes);
        },
        else => unreachable,
    }
    return state.final();
}

test "structural JSON ordering and hashes ignore object order and exact numeric spelling" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const pairs = [_][2][]const u8{
        .{ "{\"b\":[1,2],\"a\":null}", "{\"a\":null,\"b\":[1.0,2e0]}" },
        .{ "-0.0", "0" },
        .{ "10000e-4", "1.0000" },
        .{ "9007199254740993", "9007199254740993.0" },
    };
    for (pairs) |pair| {
        const a = try std.json.parseFromSliceLeaky(Json, arena.allocator(), pair[0], .{ .parse_numbers = false });
        const b = try std.json.parseFromSliceLeaky(Json, arena.allocator(), pair[1], .{ .parse_numbers = false });
        var budget: Budget = .{};
        try std.testing.expectEqual(Order.eq, try compare(a, b, &budget, 0));
        try std.testing.expectEqual(try hash(a, &budget, 0), try hash(b, &budget, 0));
    }
    var budget: Budget = .{};
    try std.testing.expectEqual(Order.gt, try compare(.{ .number_string = "9007199254740993" }, .{ .float = 9007199254740992 }, &budget, 0));
    const rounded: Json = .{ .float = 1000000000000000128.0 };
    const exact: Json = .{ .integer = 1000000000000000128 };
    try std.testing.expectEqual(Order.eq, try compare(rounded, exact, &budget, 0));
    try std.testing.expectEqual(try hash(rounded, &budget, 0), try hash(exact, &budget, 0));
    var tiny: Budget = .{ .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, compare(.{ .string = "abc" }, .{ .string = "abc" }, &tiny, 0));
}

test "primitive numeric hashes preserve exact mixed integer float and decimal equivalence" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    for ([_]i64{ 0, 1, -1, 10000, 9007199254740993, std.math.minInt(i64), std.math.maxInt(i64) }) |integer| {
        const token = try std.fmt.allocPrint(a, "{d}.000", .{integer});
        defer a.free(token);
        try std.testing.expectEqual(try hash(.{ .integer = integer }, &budget, 0), try hash(.{ .number_string = token }, &budget, 0));
    }
    for ([_]f64{ 0, -0.0, 0.5, -1.25, 0.1, std.math.floatMin(f64), std.math.floatMax(f64), 1000000000000000128.0 }) |number| {
        var bytes: [768]u8 = undefined;
        const exact = try decimal(.{ .float = number }, &bytes, &budget);
        const digits = if (exact.digits.len == 0) "0" else exact.digits;
        const token = try std.fmt.allocPrint(a, "{s}{s}e{d}", .{ if (exact.negative) "-" else "", digits, exact.magnitude - @as(i64, @intCast(digits.len)) + 1 });
        defer a.free(token);
        try std.testing.expectEqual(Order.eq, try compare(.{ .float = number }, .{ .number_string = token }, &budget, 0));
        try std.testing.expectEqual(try hash(.{ .float = number }, &budget, 0), try hash(.{ .number_string = token }, &budget, 0));
    }
    try std.testing.expect((try hash(.{ .float = 0.1 }, &budget, 0)) != (try hash(.{ .number_string = "0.1" }, &budget, 0)));
    try std.testing.expectEqual(try hash(.{ .integer = 1 }, &budget, 0), try hash(.{ .float = 1 }, &budget, 0));
}

test "native pipeline refinements benchmark primitive numeric hashing" {
    if (@import("builtin").mode == .debug) return error.SkipZigTest;
    const count = 100_000;
    var values: [1024]Json = undefined;
    for (&values, 0..) |*value, index| value.* = if (index % 2 == 0) .{ .integer = @intCast(index * 1009) } else .{ .float = @as(f64, @floatFromInt(index)) + 0.1 };
    var prior_checksums: [2]?u64 = .{ null, null };
    for (0..3) |sample| {
        var elapsed: [2]i96 = undefined;
        for (0..2) |pass| {
            const fast = (sample + pass) % 2 != 0;
            const slot = @intFromBool(fast);
            const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            var checksum: u64 = 0;
            for (0..count) |index| {
                var budget: Budget = .{};
                const value = values[index % values.len];
                if (fast) {
                    checksum ^= try hash(value, &budget, 0);
                } else {
                    // Frozen prior primitive path, with the same exact decimal
                    // normalization and semantic hash used before this PR.
                    var buffer: [768]u8 = undefined;
                    checksum ^= (try decimal(value, &buffer, &budget)).hash();
                }
            }
            elapsed[slot] = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
            if (prior_checksums[slot]) |prior| try std.testing.expectEqual(prior, checksum) else prior_checksums[slot] = checksum;
        }
        std.debug.print("native_refinement {{\"case\":\"primitive_numeric_hash\",\"rows\":{d},\"sample\":{d},\"decimal_ns\":{d},\"primitive_ns\":{d}}}\n", .{ count, sample, elapsed[0], elapsed[1] });
    }
}
