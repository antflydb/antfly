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

//! Decode borrowed request bytes under an existing execution owner. The caller
//! keeps input and allocator storage alive through all decoded-value borrowers.
const std = @import("std");

pub const Control = @import("cooperative_json_output.zig").Control;

/// Preserve complete-input parse defaults while bounding scanner work by bytes
/// and tokens. A giant string needs refills even if it is the only JSON value.
/// This does not create execution authority or reserve decoded memory; callers
/// must supply an already admitted owner and a charged allocator.
pub fn parse(comptime T: type, allocator: std.mem.Allocator, input: []const u8, options: std.json.ParseOptions, control: Control) !std.json.Parsed(T) {
    var source = Source.init(allocator, input, control);
    defer source.deinit();
    var resolved = options;
    if (resolved.max_value_len == null) resolved.max_value_len = input.len;
    if (resolved.allocate == null) resolved.allocate = .alloc_if_needed;
    source.check() catch |err| return source.original(err);
    var parsed = std.json.parseFromTokenSource(T, allocator, &source, resolved) catch |err| return source.original(err);
    errdefer parsed.deinit();
    source.check() catch |err| return source.original(err);
    return parsed;
}

pub const Source = struct {
    scanner: std.json.Scanner,
    input: []const u8,
    offset: usize = 0,
    tokens: usize = 0,
    control: Control,
    failure: ?anyerror = null,

    pub const NextError = std.json.Scanner.Error || std.mem.Allocator.Error || error{SqlDecodeInterrupted};
    pub const PeekError = std.json.Scanner.Error || error{SqlDecodeInterrupted};
    pub const AllocError = NextError || error{ValueTooLong};
    pub const SkipError = NextError;

    pub fn init(allocator: std.mem.Allocator, input: []const u8, control: Control) Source {
        return .{ .scanner = std.json.Scanner.initStreaming(allocator), .input = input, .control = control };
    }

    pub fn deinit(self: *Source) void {
        self.scanner.deinit();
        self.* = undefined;
    }

    pub fn original(self: *const Source, err: anyerror) anyerror {
        return if (err == error.SqlDecodeInterrupted) self.failure orelse error.InvalidRunnableTransition else err;
    }

    fn check(self: *Source) error{SqlDecodeInterrupted}!void {
        self.control.check(self.control.ptr) catch |err| {
            self.failure = err;
            return error.SqlDecodeInterrupted;
        };
    }

    fn refill(self: *Source) error{SqlDecodeInterrupted}!void {
        try self.check();
        const end = self.offset + @min(@as(usize, 4096), self.input.len - self.offset);
        self.scanner.feedInput(self.input[self.offset..end]);
        self.offset = end;
        if (end == self.input.len) self.scanner.endInput();
    }

    pub fn next(self: *Source) NextError!std.json.Token {
        if (self.tokens % 64 == 0) try self.check();
        while (true) {
            const token = self.scanner.next() catch |err| switch (err) {
                error.BufferUnderrun => {
                    try self.refill();
                    continue;
                },
                error.SyntaxError => return error.SyntaxError,
                error.UnexpectedEndOfInput => return error.UnexpectedEndOfInput,
                error.OutOfMemory => return error.OutOfMemory,
            };
            self.tokens +%= 1;
            return token;
        }
    }

    pub fn peekNextTokenType(self: *Source) PeekError!std.json.TokenType {
        while (true) {
            return self.scanner.peekNextTokenType() catch |err| switch (err) {
                error.BufferUnderrun => {
                    try self.refill();
                    continue;
                },
                error.SyntaxError => return error.SyntaxError,
                error.UnexpectedEndOfInput => return error.UnexpectedEndOfInput,
            };
        }
    }

    pub fn nextAllocMax(self: *Source, allocator: std.mem.Allocator, when: std.json.AllocWhen, max_value_len: usize) AllocError!std.json.Token {
        const kind = try self.peekNextTokenType();
        if (kind != .number and kind != .string) return self.next();
        var bytes = std.array_list.Managed(u8).init(allocator);
        errdefer bytes.deinit();
        const borrowed = try self.allocNextIntoArrayListMax(&bytes, when, max_value_len);
        if (borrowed) |text| return if (kind == .number) .{ .number = text } else .{ .string = text };
        const owned = try self.takeOwned(&bytes);
        return if (kind == .number) .{ .allocated_number = owned } else .{ .allocated_string = owned };
    }

    pub fn allocNextIntoArrayListMax(self: *Source, bytes: *std.array_list.Managed(u8), when: std.json.AllocWhen, max_value_len: usize) AllocError!?[]const u8 {
        var prefix: ?[]const u8 = null;
        while (true) {
            const token = try self.next();
            switch (token) {
                .partial_number, .partial_string => |text| {
                    if (when == .alloc_if_needed and bytes.items.len == 0) {
                        if (prefix) |old| {
                            if (try joinBorrowed(old, text)) |joined| {
                                prefix = joined;
                                continue;
                            }
                        } else {
                            prefix = text;
                            continue;
                        }
                    }
                    try self.flushPrefix(bytes, &prefix, max_value_len);
                    try self.append(bytes, text, max_value_len);
                },
                inline .partial_string_escaped_1, .partial_string_escaped_2, .partial_string_escaped_3, .partial_string_escaped_4 => |text| {
                    try self.flushPrefix(bytes, &prefix, max_value_len);
                    try self.append(bytes, &text, max_value_len);
                },
                .number, .string => |text| {
                    if (when == .alloc_if_needed and bytes.items.len == 0) {
                        if (prefix) |old| {
                            if (try joinBorrowed(old, text)) |joined| return joined;
                        } else return text;
                    }
                    try self.flushPrefix(bytes, &prefix, max_value_len);
                    try self.append(bytes, text, max_value_len);
                    return null;
                },
                else => unreachable,
            }
        }
    }

    fn flushPrefix(self: *Source, bytes: *std.array_list.Managed(u8), prefix: *?[]const u8, limit: usize) AllocError!void {
        if (prefix.*) |text| try self.append(bytes, text, limit);
        prefix.* = null;
    }

    pub fn skipValue(self: *Source) SkipError!void {
        const height = self.scanner.stackHeight();
        const kind = try self.peekNextTokenType();
        switch (kind) {
            .array_begin, .object_begin => {
                _ = try self.next();
                while (self.scanner.stackHeight() > height) _ = try self.next();
            },
            .number, .string => while (true) {
                switch (try self.next()) {
                    .number, .string => break,
                    .partial_number, .partial_string, .partial_string_escaped_1, .partial_string_escaped_2, .partial_string_escaped_3, .partial_string_escaped_4 => {},
                    else => unreachable,
                }
            },
            .true, .false, .null => _ = try self.next(),
            else => unreachable,
        }
    }

    fn copy(self: *Source, destination: []u8, input: []const u8) error{SqlDecodeInterrupted}!void {
        var offset: usize = 0;
        while (offset < input.len) {
            try self.check();
            const end = offset + @min(@as(usize, 4096), input.len - offset);
            @memcpy(destination[offset..end], input[offset..end]);
            offset = end;
        }
        try self.check();
    }

    fn append(self: *Source, bytes: *std.array_list.Managed(u8), text: []const u8, limit: usize) AllocError!void {
        const end = std.math.add(usize, bytes.items.len, text.len) catch return error.ValueTooLong;
        if (end > limit) return error.ValueTooLong;
        if (end > bytes.capacity) {
            const capacity = @min(limit, @max(end, bytes.capacity +| bytes.capacity / 2 +| 8));
            const replacement = try bytes.allocator.alloc(u8, capacity);
            errdefer bytes.allocator.free(replacement);
            try self.copy(replacement[0..bytes.items.len], bytes.items);
            bytes.allocator.free(bytes.allocatedSlice());
            bytes.items = replacement[0..bytes.items.len];
            bytes.capacity = capacity;
        }
        try self.copy(bytes.items.ptr[bytes.items.len..end], text);
        bytes.items.len = end;
    }

    fn takeOwned(self: *Source, bytes: *std.array_list.Managed(u8)) AllocError![]u8 {
        try self.check();
        const allocator = bytes.allocator;
        if (bytes.items.len == bytes.capacity) {
            const result = bytes.items;
            bytes.* = .init(allocator);
            return result;
        }
        const result = try allocator.alloc(u8, bytes.items.len);
        errdefer allocator.free(result);
        try self.copy(result, bytes.items);
        bytes.clearAndFree();
        return result;
    }
};

fn joinBorrowed(first: []const u8, next: []const u8) error{ValueTooLong}!?[]const u8 {
    // Scanner chunks borrow the same immutable complete input. Escape tokens
    // are flushed separately; only adjacent raw slices may be rejoined.
    if (@intFromPtr(first.ptr) + first.len != @intFromPtr(next.ptr)) return null;
    const len = std.math.add(usize, first.len, next.len) catch return error.ValueTooLong;
    // Complete-input Scanner ignores max_value_len for borrowed values.
    // Keep that behavior even when cooperative refills split a raw token.
    // If escapes require an owned buffer, append enforces the limit.
    return first.ptr[0..len];
}

const TestControl = struct {
    checks: usize = 0,
    fail_at: usize = 0,
    failure: anyerror = error.Canceled,

    fn check(raw: *anyopaque) !void {
        const self: *TestControl = @ptrCast(@alignCast(raw));
        self.checks += 1;
        if (self.fail_at != 0 and self.checks == self.fail_at) return self.failure;
    }

    fn control(self: *TestControl) Control {
        return .{ .ptr = self, .check = check };
    }
};

test "SQL JSON source preserves split strings exact integers and skipped nested fields" {
    const Input = struct { statement: []const u8, parameters: []const std.json.Value };
    const padding = try std.testing.allocator.alloc(u8, 8192);
    defer std.testing.allocator.free(padding);
    @memset(padding, 'x');
    const body = try std.fmt.allocPrint(std.testing.allocator, "{{\"statement\":\"{s}\\uD83D\\uDE00\",\"parameters\":[9007199254740993,null,\"tail\"],\"ignored\":{{\"nested\":[[\"{s}\"],true]}}}}", .{ padding, padding });
    defer std.testing.allocator.free(body);
    var control: TestControl = .{};
    var expected = try std.json.parseFromSlice(Input, std.testing.allocator, body, .{ .parse_numbers = false, .ignore_unknown_fields = true });
    defer expected.deinit();
    var actual = try parse(Input, std.testing.allocator, body, .{ .parse_numbers = false, .ignore_unknown_fields = true }, control.control());
    defer actual.deinit();
    try std.testing.expectEqualStrings(expected.value.statement, actual.value.statement);
    try std.testing.expectEqual(@as(usize, 8196), actual.value.statement.len);
    try std.testing.expectEqualStrings("9007199254740993", actual.value.parameters[0].number_string);
    try std.testing.expect(actual.value.parameters[1] == .null);
    try std.testing.expectEqualStrings("tail", actual.value.parameters[2].string);
    try std.testing.expect(control.checks > body.len / 4096);
    try std.testing.expectError(error.UnexpectedToken, parse(Input, std.testing.allocator, "{\"statement\":false,\"parameters\":[]}", .{}, control.control()));
}

test "SQL JSON source interrupts within a single large value with original authority" {
    const body = try std.testing.allocator.alloc(u8, 65538);
    defer std.testing.allocator.free(body);
    @memset(body, 'x');
    body[0] = '"';
    body[body.len - 1] = '"';
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RunnableWaitTimeout, error.SchedulerClosed }) |failure| {
        var control: TestControl = .{ .fail_at = 5, .failure = failure };
        try std.testing.expectError(failure, parse([]const u8, std.testing.allocator, body, .{}, control.control()));
        try std.testing.expectEqual(@as(usize, 5), control.checks);
    }
    var control: TestControl = .{};
    var result = try parse([]const u8, std.testing.allocator, body, .{}, control.control());
    defer result.deinit();
    try std.testing.expectEqualStrings(body[1 .. body.len - 1], result.value);
}

test "SQL JSON source checks many small tokens and retires interrupted decode" {
    const count = 8192;
    const body = try std.testing.allocator.alloc(u8, count * 2 + 1);
    defer std.testing.allocator.free(body);
    body[0] = '[';
    for (0..count) |index| {
        body[index * 2 + 1] = '0';
        body[index * 2 + 2] = if (index + 1 == count) ']' else ',';
    }
    var interrupted: TestControl = .{ .fail_at = 32 };
    try std.testing.expectError(error.Canceled, parse([]const u8, std.testing.allocator, body, .{}, interrupted.control()));
    var control: TestControl = .{};
    var result = try parse([]const u8, std.testing.allocator, body, .{}, control.control());
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, count), result.value.len);
    try std.testing.expect(control.checks >= count / 64);
    for (result.value) |value| try std.testing.expectEqual(@as(u8, 0), value);
}

test "SQL JSON source partial decode allocator faults release scanner and parsed arena" {
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator, body: []const u8) !void {
            var control: TestControl = .{};
            var result = try parse(std.json.Value, allocator, body, .{ .parse_numbers = false }, control.control());
            defer result.deinit();
            try std.testing.expectEqualStrings("9007199254740993", result.value.object.get("n").?.number_string);
            try std.testing.expectEqual(@as(usize, 8192), result.value.object.get("text").?.string.len);
        }
    };
    const padding = try std.testing.allocator.alloc(u8, 8192);
    defer std.testing.allocator.free(padding);
    @memset(padding, 'x');
    const body = try std.fmt.allocPrint(std.testing.allocator, "{{\"n\":9007199254740993,\"text\":\"{s}\",\"nested\":[{{\"key\":\"value\"}},null]}}", .{padding});
    defer std.testing.allocator.free(body);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{body});
}

test "SQL JSON source NoResize token growth checks work while both buffers remain charged" {
    const Allocator = struct {
        live: usize = 0,
        live_allocations: usize = 0,
        peak: usize = 0,
        interrupted_with_live_bytes: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
            self.live += len;
            self.live_allocations += 1;
            self.peak = @max(self.peak, self.live);
            return result;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live -= bytes.len;
            self.live_allocations -= 1;
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // The next growth step keeps an old full buffer and a replacement
            // simultaneously live. Reject before that copy completes.
            if (self.live_allocations >= 2 and self.live > 20000) {
                self.interrupted_with_live_bytes = true;
                return error.DeadlineExceeded;
            }
        }
    };
    var owner: Allocator = .{};
    defer std.debug.assert(owner.live == 0);
    const body = try std.testing.allocator.alloc(u8, 65538);
    defer std.testing.allocator.free(body);
    @memset(body, 'x');
    body[0] = '"';
    body[body.len - 1] = '"';
    {
        // Use the token source directly so these are token buffers, rather
        // than underlying chunks of the parsed-value arena.
        var source = Source.init(owner.allocator(), body, .{ .ptr = &owner, .check = Allocator.check });
        defer source.deinit();
        try std.testing.expectError(error.SqlDecodeInterrupted, source.nextAllocMax(owner.allocator(), .alloc_always, body.len));
        try std.testing.expect(source.original(error.SqlDecodeInterrupted) == error.DeadlineExceeded);
    }
    try std.testing.expect(owner.interrupted_with_live_bytes);
    try std.testing.expect(owner.peak > 20000);
    try std.testing.expectEqual(@as(usize, 0), owner.live);
}

test "SQL JSON source borrows complete unescaped values across checked chunks without allocation" {
    const body = try std.testing.allocator.alloc(u8, 65538);
    defer std.testing.allocator.free(body);
    for ([_]bool{ true, false }) |string| {
        @memset(body, if (string) 'x' else '7');
        if (string) {
            body[0] = '"';
            body[body.len - 1] = '"';
        }
        var control: TestControl = .{};
        var source = Source.init(std.testing.failing_allocator, body, control.control());
        defer source.deinit();
        const token = try source.nextAllocMax(std.testing.failing_allocator, .alloc_if_needed, body.len);
        const borrowed = if (string) token.string else token.number;
        const expected = if (string) body[1 .. body.len - 1] else body;
        try std.testing.expectEqualStrings(expected, borrowed);
        try std.testing.expectEqual(@intFromPtr(expected.ptr), @intFromPtr(borrowed.ptr));
        try std.testing.expect(control.checks >= body.len / 4096);
    }
}

test "SQL JSON source preserves complete-input value limits across borrowing and escapes" {
    const padding = try std.testing.allocator.alloc(u8, 8192);
    defer std.testing.allocator.free(padding);
    @memset(padding, '7');
    const plain = try std.fmt.allocPrint(std.testing.allocator, "\"{s}\"", .{padding});
    defer std.testing.allocator.free(plain);
    const escaped = try std.fmt.allocPrint(std.testing.allocator, "\"{s}\\u0041\"", .{padding});
    defer std.testing.allocator.free(escaped);
    for ([_][]const u8{ "\"abcdef\"", "123456", "\"abcd\\u0041\"", plain, padding, escaped }) |body| {
        for ([_]std.json.AllocWhen{ .alloc_if_needed, .alloc_always }) |when| {
            for ([_]usize{ 0, 4, 8192, 16384 }) |limit| {
                var control: TestControl = .{};
                var scanner = std.json.Scanner.initCompleteInput(std.testing.allocator, body);
                defer scanner.deinit();
                var source = Source.init(std.testing.allocator, body, control.control());
                defer source.deinit();
                const expected = scanner.nextAllocMax(std.testing.allocator, when, limit) catch |err| {
                    try std.testing.expectError(err, source.nextAllocMax(std.testing.allocator, when, limit));
                    continue;
                };
                defer switch (expected) {
                    .allocated_string, .allocated_number => |bytes| std.testing.allocator.free(bytes),
                    else => {},
                };
                const actual = try source.nextAllocMax(std.testing.allocator, when, limit);
                defer switch (actual) {
                    .allocated_string, .allocated_number => |bytes| std.testing.allocator.free(bytes),
                    else => {},
                };
                try std.testing.expectEqual(std.meta.activeTag(expected), std.meta.activeTag(actual));
                const expected_bytes = switch (expected) {
                    .string, .number, .allocated_string, .allocated_number => |bytes| bytes,
                    else => unreachable,
                };
                const actual_bytes = switch (actual) {
                    .string, .number, .allocated_string, .allocated_number => |bytes| bytes,
                    else => unreachable,
                };
                try std.testing.expectEqualStrings(expected_bytes, actual_bytes);
                try std.testing.expect((try source.next()) == .end_of_document);
            }
        }
    }
}
