// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
//! PostgreSQL ARE backend. No search-index byte automaton, host locale, or
//! provider allocation is used. Patterns own native blocks; each execution
//! owns independent scratch. This module is not yet activated in public SQL.
const std = @import("std");
const A = std.mem.Allocator;

pub const Limits = struct { heap_bytes: usize = 8 * 1024 * 1024, stack_bytes: usize = 64 * 1024, pattern_bytes: usize = 16 * 1024 };
pub const Budget = struct {
    remaining: usize = 8 * 1024 * 1024,
    /// Runs synchronously inside a native call; must not yield/suspend.
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    ptr: ?*anyopaque = null,
    failure: ?anyerror = null,
    fn consume(self: *Budget) bool {
        if (self.failure != null) return false;
        if (self.remaining == 0) {
            self.failure = error.SqlExpressionTooLarge;
            return false;
        }
        self.remaining -= 1;
        if (self.checkpoint) |check| check(self.ptr) catch |err| {
            self.failure = err;
            return false;
        };
        return true;
    }
};
pub const Span = extern struct { start: c_long = -1, end: c_long = -1 };
const Context = extern struct {
    user: *anyopaque,
    allocate: *const fn (*anyopaque, usize) callconv(.c) ?*anyopaque = Memory.allocate,
    resize: *const fn (*anyopaque, ?*anyopaque, usize) callconv(.c) ?*anyopaque = Memory.resize,
    release: *const fn (*anyopaque, ?*anyopaque) callconv(.c) void = Memory.release,
    poll: *const fn (*anyopaque) callconv(.c) c_int = Memory.poll,
    stack_base: usize = 0,
    stack_limit: usize,
    classes: [14]?*anyopaque = @splat(null),
};
extern fn antfly_regex_compile(*Context, [*]const u32, usize, c_int, *c_int) ?*anyopaque;
extern fn antfly_regex_search(*Context, *anyopaque, [*]const u32, usize, usize, [*]Span, usize) c_int;
extern fn antfly_regex_captures(*anyopaque) usize;
extern fn antfly_regex_destroy(*Context, *anyopaque) void;

const Memory = struct {
    const Header = struct { previous: ?*Header, next: ?*Header, bytes: usize };
    const header_bytes = std.mem.alignForward(usize, @sizeOf(Header), 16);
    alloc: A,
    limit: usize,
    head: ?*Header = null,
    live: usize = 0,
    peak: usize = 0,
    budget: ?*Budget,
    failure: ?anyerror = null,
    fn from(raw: *anyopaque) *Memory {
        return @ptrCast(@alignCast(raw));
    }
    fn header(raw: *anyopaque) *Header {
        return @ptrFromInt(@intFromPtr(raw) - header_bytes);
    }
    fn allocate(raw: *anyopaque, bytes: usize) callconv(.c) ?*anyopaque {
        const self = from(raw);
        const size = std.math.add(usize, header_bytes, @max(1, bytes)) catch {
            self.failure = error.SqlExpressionTooLarge;
            return null;
        };
        if (size > self.limit -| self.live) {
            self.failure = error.SqlExpressionTooLarge;
            return null;
        }
        const buffer = self.alloc.alignedAlloc(u8, .@"16", size) catch |err| {
            self.failure = err;
            return null;
        };
        const node: *Header = @ptrCast(buffer.ptr);
        node.* = .{ .previous = null, .next = self.head, .bytes = size };
        if (self.head) |old| old.previous = node;
        self.head = node;
        self.live += size;
        self.peak = @max(self.peak, self.live);
        return @ptrCast(buffer.ptr + header_bytes);
    }
    fn release(raw: *anyopaque, pointer: ?*anyopaque) callconv(.c) void {
        const node = header(pointer orelse return);
        const self = from(raw);
        if (node.previous) |previous| previous.next = node.next else self.head = node.next;
        if (node.next) |next| next.previous = node.previous;
        self.live -= node.bytes;
        const buffer: [*]align(16) u8 = @ptrCast(@alignCast(node));
        self.alloc.free(buffer[0..node.bytes]);
    }
    fn resize(raw: *anyopaque, pointer: ?*anyopaque, bytes: usize) callconv(.c) ?*anyopaque {
        const old = pointer orelse return allocate(raw, bytes);
        const replacement = allocate(raw, bytes) orelse return null;
        const size = @min(bytes, header(old).bytes - header_bytes);
        @memcpy(@as([*]u8, @ptrCast(replacement))[0..size], @as([*]const u8, @ptrCast(old))[0..size]);
        release(raw, old);
        return replacement;
    }
    fn poll(raw: *anyopaque) callconv(.c) c_int {
        return @intFromBool((from(raw).budget orelse return 0).consume());
    }
    fn context(self: *Memory, stack_bytes: usize) Context {
        return .{ .user = self, .stack_limit = stack_bytes };
    }
    fn check(self: *const Memory) !void {
        if (self.budget) |budget| if (budget.failure) |err| return err;
        if (self.failure) |err| return err;
    }
    fn deinit(self: *Memory) void {
        while (self.head) |node| release(self, @ptrFromInt(@intFromPtr(node) + header_bytes));
        std.debug.assert(self.live == 0);
    }
};

/// Decode once per subject. Sparse byte checkpoints bound returned-span
/// conversion to at most 63 codepoints without a machine-word offset per cell.
/// Matching never slices anchors/lookbehind off the original subject.
pub const Subject = struct {
    const stride = 64;
    alloc: A,
    bytes: []const u8,
    codepoints: []u32,
    offsets: []u32,
    pub fn init(alloc: A, bytes: []const u8) !Subject {
        if (bytes.len > 1024 * 1024) return error.SqlExpressionTooLarge;
        const count = std.unicode.utf8CountCodepoints(bytes) catch return error.SqlInvalidText;
        const points = try alloc.alloc(u32, count + 1);
        errdefer alloc.free(points);
        const offsets = try alloc.alloc(u32, count / stride + 1);
        errdefer alloc.free(offsets);
        var iterator = (try std.unicode.Utf8View.init(bytes)).iterator();
        var index: usize = 0;
        while (iterator.nextCodepoint()) |point| : (index += 1) {
            points[index] = point;
            if (index % stride == 0) offsets[index / stride] = @intCast(iterator.i - (std.unicode.utf8CodepointSequenceLength(point) catch unreachable));
        }
        points[count] = 0;
        if (count % stride == 0) offsets[count / stride] = @intCast(bytes.len);
        return .{ .alloc = alloc, .bytes = bytes, .codepoints = points, .offsets = offsets };
    }
    pub fn len(self: Subject) usize {
        return self.codepoints.len - 1;
    }
    pub fn slice(self: Subject, span: Span) !?[]const u8 {
        if (span.start == -1 and span.end == -1) return null;
        if (span.start < 0 or span.end < span.start or span.end > self.len()) return error.InvalidRegexResponse;
        return self.bytes[self.byteOffset(@intCast(span.start))..self.byteOffset(@intCast(span.end))];
    }
    fn byteOffset(self: Subject, point: usize) usize {
        std.debug.assert(point <= self.len());
        const block = point / stride;
        var offset: usize = self.offsets[block];
        for (self.codepoints[block * stride .. point]) |value| offset += std.unicode.utf8CodepointSequenceLength(@intCast(value)) catch unreachable;
        return offset;
    }
    pub fn deinit(self: *Subject) void {
        self.alloc.free(self.codepoints);
        self.alloc.free(self.offsets);
    }
};

pub const Program = struct {
    memory: Memory,
    native: *anyopaque,
    captures: usize,
    limits: Limits,
    pub fn compile(alloc: A, pattern: []const u8, flags: c_int, limits: Limits, budget: *Budget) !Program {
        if (pattern.len > limits.pattern_bytes) return error.SqlExpressionTooLarge;
        var input = try Subject.init(alloc, pattern);
        defer input.deinit();
        var memory: Memory = .{ .alloc = alloc, .limit = limits.heap_bytes, .budget = budget };
        errdefer memory.deinit();
        var context = memory.context(limits.stack_bytes);
        var status: c_int = 0;
        const native = antfly_regex_compile(&context, input.codepoints.ptr, input.len(), flags, &status);
        try memory.check();
        try checkStatus(status);
        // Prepared patterns never retain the compiling request's budget.
        memory.budget = null;
        return .{ .memory = memory, .native = native orelse return error.InvalidRegexResponse, .captures = antfly_regex_captures(native.?), .limits = limits };
    }
    pub fn find(self: *const Program, subject: Subject, start: usize, matches: []Span, budget: *Budget) !bool {
        if (start > subject.len()) return error.SqlInvalidArgument;
        var memory: Memory = .{ .alloc = self.memory.alloc, .limit = self.limits.heap_bytes, .budget = budget };
        defer memory.deinit();
        var context = memory.context(self.limits.stack_bytes);
        if (!budget.consume()) return budget.failure.?;
        @memset(matches, .{});
        const status = antfly_regex_search(&context, self.native, subject.codepoints.ptr, subject.len(), start, matches.ptr, matches.len);
        try memory.check();
        if (status == 1) return false;
        try checkStatus(status);
        for (matches) |match| _ = try subject.slice(match);
        return true;
    }
    pub fn deinit(self: *Program) void {
        var context = self.memory.context(self.limits.stack_bytes);
        antfly_regex_destroy(&context, self.native);
        self.memory.deinit();
    }
};
fn checkStatus(status: c_int) !void {
    switch (status) {
        0 => {},
        12 => return error.OutOfMemory,
        19, 20 => return error.SqlExpressionTooLarge,
        15, 16, 17 => return error.InvalidRegexResponse,
        else => return error.SqlInvalidRegularExpression,
    }
}

test "PostgreSQL ARE captures Unicode character spans and preserves the original anchor domain" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    var program = try Program.compile(a, "([A-Z])([0-9]+)", 3, .{}, &budget);
    defer program.deinit();
    var subject = try Subject.init(a, "雪😀A1B22");
    defer subject.deinit();
    var matches: [3]Span = undefined;
    try std.testing.expect(try program.find(subject, 0, &matches, &budget));
    try std.testing.expectEqual(@as(c_long, 2), matches[0].start);
    try std.testing.expectEqualStrings("A1", (try subject.slice(matches[0])).?);
    try std.testing.expectEqualStrings("A", (try subject.slice(matches[1])).?);
    try std.testing.expectEqualStrings("1", (try subject.slice(matches[2])).?);
    try std.testing.expect(try program.find(subject, 4, &matches, &budget));
    try std.testing.expectEqualStrings("B22", (try subject.slice(matches[0])).?);
}

test "PostgreSQL ARE keeps longest shortest empty lookaround and backreference semantics" {
    const a = std.testing.allocator;
    for ([_]struct { pattern: []const u8, input: []const u8, expected: []const u8 }{
        .{ .pattern = "a|ab", .input = "abc", .expected = "ab" },
        .{ .pattern = "a+?", .input = "aaa", .expected = "a" },
        .{ .pattern = "", .input = "雪", .expected = "" },
        .{ .pattern = ".", .input = "😀", .expected = "😀" },
        .{ .pattern = "(?<=雪)😀(?=A)", .input = "雪😀A", .expected = "😀" },
        .{ .pattern = "([a-z]+)-\\1", .input = "x cat-cat y", .expected = "cat-cat" },
    }) |case| {
        var budget: Budget = .{};
        var program = try Program.compile(a, case.pattern, 3, .{}, &budget);
        defer program.deinit();
        var subject = try Subject.init(a, case.input);
        defer subject.deinit();
        var matches: [1]Span = undefined;
        try std.testing.expect(try program.find(subject, 0, &matches, &budget));
        try std.testing.expectEqualStrings(case.expected, (try subject.slice(matches[0])).?);
    }
}

test "PostgreSQL ARE releases native allocations on errors admission refusal and cancellation" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    try std.testing.expectError(error.SqlInvalidRegularExpression, Program.compile(a, "[", 3, .{}, &budget));
    try std.testing.expectError(error.SqlExpressionTooLarge, Program.compile(a, "abc", 3, .{ .heap_bytes = 32 }, &budget));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    var canceled: Budget = .{ .checkpoint = Cancel.check };
    try std.testing.expectError(error.Canceled, Program.compile(a, "(ab)+", 3, .{}, &canceled));
}

test "PostgreSQL ARE allocation faults unwind compiled ownership and independent execution scratch" {
    const Faults = struct {
        fn run(a: A) !void {
            var budget: Budget = .{};
            var program = try Program.compile(a, "([[:alpha:]]+)-\\1", 3, .{}, &budget);
            defer program.deinit();
            var subject = try Subject.init(a, "雪 cat-cat dog-dog");
            defer subject.deinit();
            var spans: [2]Span = undefined;
            try std.testing.expect(try program.find(subject, 0, &spans, &budget));
            try std.testing.expectEqualStrings("cat-cat", (try subject.slice(spans[0])).?);
            try std.testing.expect(try program.find(subject, @intCast(spans[0].end), &spans, &budget));
            try std.testing.expectEqualStrings("dog-dog", (try subject.slice(spans[0])).?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "PostgreSQL ARE cached transitions obey work cancellation and linear regular search" {
    const a = std.testing.allocator;
    var compile_budget: Budget = .{};
    var program = try Program.compile(a, "a+b", 3, .{}, &compile_budget);
    defer program.deinit();
    var previous: usize = 0;
    for ([_]usize{ 4096, 16384 }) |size| {
        const bytes = try a.alloc(u8, size + 1);
        defer a.free(bytes);
        @memset(bytes, 'a');
        bytes[size] = 'b';
        var subject = try Subject.init(a, bytes);
        defer subject.deinit();
        var spans: [1]Span = undefined;
        var budget: Budget = .{};
        const initial = budget.remaining;
        try std.testing.expect(try program.find(subject, 0, &spans, &budget));
        const work = initial - budget.remaining;
        try std.testing.expect(work >= size);
        if (previous != 0) try std.testing.expect(work <= previous * 5);
        previous = work;
        var refused: Budget = .{ .remaining = 32 };
        try std.testing.expectError(error.SqlExpressionTooLarge, program.find(subject, 0, &spans, &refused));
        try std.testing.expectEqual(@as(usize, 0), refused.remaining);
    }
}

test "PostgreSQL ARE independently generated spans preserve captures flags and C collation" {
    const Golden = struct {
        format: u32,
        collation: []const u8,
        entries: []const struct {
            id: []const u8,
            pattern: []const u8,
            input: []const u8,
            flags: c_int,
            start: usize,
            captures: usize,
            matched: bool,
            spans: []const struct { start: c_long, end: c_long, text: ?[]const u8 },
        },
    };
    const a = std.testing.allocator;
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("testdata/postgres.json"), .{});
    defer golden.deinit();
    try std.testing.expectEqual(@as(u32, 1), golden.value.format);
    try std.testing.expectEqualStrings("C", golden.value.collation);
    for (golden.value.entries) |case| {
        errdefer std.debug.print("PostgreSQL ARE fixture {s}\n", .{case.id});
        var budget: Budget = .{};
        var program = try Program.compile(a, case.pattern, case.flags, .{}, &budget);
        defer program.deinit();
        try std.testing.expectEqual(case.captures, program.captures);
        var subject = try Subject.init(a, case.input);
        defer subject.deinit();
        const spans = try a.alloc(Span, case.spans.len);
        defer a.free(spans);
        try std.testing.expectEqual(case.matched, try program.find(subject, case.start, spans, &budget));
        for (spans, case.spans) |actual, expected| {
            try std.testing.expectEqual(expected.start, actual.start);
            try std.testing.expectEqual(expected.end, actual.end);
            const text = try subject.slice(actual);
            if (expected.text) |value| try std.testing.expectEqualStrings(value, text orelse return error.ExpectedRegexMatch) else try std.testing.expect(text == null);
        }
    }
}

test "PostgreSQL ARE shares immutable patterns across independent Io workers" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    var program = try Program.compile(a, "([[:alpha:]]+)([0-9]+)", 3, .{}, &budget);
    defer program.deinit();
    var io_impl = std.Io.Threaded.init(a, .{ .concurrent_limit = .limited(2) });
    defer io_impl.deinit();
    const io = io_impl.io();
    const Worker = struct {
        fn run(alloc: A, compiled: *const Program, input: []const u8, expected: []const u8) !void {
            var subject = try Subject.init(alloc, input);
            defer subject.deinit();
            var local_budget: Budget = .{};
            var spans: [3]Span = undefined;
            for (0..100) |_| {
                try std.testing.expect(try compiled.find(subject, 0, &spans, &local_budget));
                try std.testing.expectEqualStrings(expected, (try subject.slice(spans[1])).?);
            }
        }
    };
    var first = try io.concurrent(Worker.run, .{ a, &program, "雪ABC123", "ABC" });
    defer first.cancel(io) catch {};
    var second = try io.concurrent(Worker.run, .{ a, &program, "😀def456", "def" });
    defer second.cancel(io) catch {};
    try first.await(io);
    try second.await(io);
}

test "PostgreSQL ARE nested native calls restore the outer allocation context" {
    const Nested = struct {
        entered: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.entered) return;
            self.entered = true;
            var budget: Budget = .{};
            var nested = try Program.compile(std.testing.allocator, "[[:digit:]]+", 3, .{}, &budget);
            defer nested.deinit();
            var subject = try Subject.init(std.testing.allocator, "雪123");
            defer subject.deinit();
            var spans: [1]Span = undefined;
            try std.testing.expect(try nested.find(subject, 0, &spans, &budget));
            try std.testing.expectEqualStrings("123", (try subject.slice(spans[0])).?);
        }
    };
    var nested: Nested = .{};
    var budget: Budget = .{ .checkpoint = Nested.check, .ptr = &nested };
    var outer = try Program.compile(std.testing.allocator, "([[:alpha:]]+)-\\1", 3, .{}, &budget);
    defer outer.deinit();
    try std.testing.expect(nested.entered);
    var subject = try Subject.init(std.testing.allocator, "cat-cat");
    defer subject.deinit();
    var spans: [2]Span = undefined;
    try std.testing.expect(try outer.find(subject, 0, &spans, &budget));
}

test "PostgreSQL ARE sparse byte checkpoints preserve every mixed Unicode boundary" {
    const a = std.testing.allocator;
    var input: [800]u8 = undefined;
    for (0..100) |i| @memcpy(input[i * 8 ..][0..8], "a雪😀");
    var subject = try Subject.init(a, &input);
    defer subject.deinit();
    try std.testing.expectEqual(@as(usize, 300), subject.len());
    try std.testing.expectEqual(@as(usize, 5), subject.offsets.len);
    for (0..subject.len() + 1) |index| {
        const expected = (index / 3) * 8 + switch (index % 3) {
            0 => @as(usize, 0),
            1 => 1,
            2 => 4,
            else => unreachable,
        };
        try std.testing.expectEqual(expected, subject.byteOffset(index));
        if (index < subject.len()) try std.testing.expectEqualStrings(switch (index % 3) {
            0 => "a",
            1 => "雪",
            2 => "😀",
            else => unreachable,
        }, (try subject.slice(.{ .start = @intCast(index), .end = @intCast(index + 1) })).?);
    }
}
