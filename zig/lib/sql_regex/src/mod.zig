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
        return self.consumeWork(1);
    }
    fn consumeWork(self: *Budget, amount: usize) bool {
        if (self.failure != null) return false;
        if (amount > self.remaining) {
            self.remaining = 0;
            self.failure = error.SqlExpressionTooLarge;
            return false;
        }
        self.remaining -= amount;
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
    work: *const fn (*anyopaque, usize) callconv(.c) c_int = Memory.work,
    stack_base: usize = 0,
    stack_limit: usize,
    classes: [14]?*anyopaque = @splat(null),
};
extern fn antfly_regex_compile(*Context, [*]const u32, usize, c_int, *c_int) ?*anyopaque;
extern fn antfly_regex_search(*Context, *anyopaque, [*]const u32, usize, usize, [*]Span, usize) c_int;
extern fn antfly_regex_captures(*anyopaque) usize;
extern fn antfly_regex_destroy(*Context, *anyopaque) void;

const Memory = struct {
    const Header = struct { previous: ?*Header, next: ?*Header, bytes: usize, requested: usize };
    const header_bytes = std.mem.alignForward(usize, @sizeOf(Header), 16);
    alloc: A,
    limit: usize,
    head: ?*Header = null,
    live: usize = 0,
    peak: usize = 0,
    resident: usize = 0,
    resident_peak: usize = 0,
    allocations: usize = 0,
    reused: usize = 0,
    retain: bool = false,
    cached: [@bitSizeOf(usize)]?*Header = @splat(null),
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
        const required = std.math.add(usize, header_bytes, @max(1, bytes)) catch {
            self.failure = error.SqlExpressionTooLarge;
            return null;
        };
        const size = if (self.retain) std.math.ceilPowerOfTwo(usize, required) catch {
            self.failure = error.SqlExpressionTooLarge;
            return null;
        } else required;
        if (size > self.limit -| self.live) {
            self.failure = error.SqlExpressionTooLarge;
            return null;
        }
        const bin = std.math.log2_int(usize, size);
        const node: *Header = reuse: {
            if (self.retain) if (self.cached[bin]) |node| {
                self.cached[bin] = node.next;
                self.reused += 1;
                break :reuse node;
            };
            // Admission includes cached blocks. Reclaim the largest idle
            // blocks first; varying patterns cannot grow physical residency
            // beyond the execution's heap limit.
            self.trim(self.limit - size);
            const buffer = self.alloc.alignedAlloc(u8, .@"16", size) catch |err| {
                self.failure = err;
                return null;
            };
            self.resident += size;
            self.resident_peak = @max(self.resident_peak, self.resident);
            self.allocations += 1;
            break :reuse @ptrCast(buffer.ptr);
        };
        node.* = .{ .previous = null, .next = self.head, .bytes = size, .requested = bytes };
        if (self.head) |old| old.previous = node;
        self.head = node;
        self.live += size;
        self.peak = @max(self.peak, self.live);
        return @ptrFromInt(@intFromPtr(node) + header_bytes);
    }
    fn release(raw: *anyopaque, pointer: ?*anyopaque) callconv(.c) void {
        const node = header(pointer orelse return);
        const self = from(raw);
        if (node.previous) |previous| previous.next = node.next else self.head = node.next;
        if (node.next) |next| next.previous = node.previous;
        self.live -= node.bytes;
        if (self.retain) {
            const bin = std.math.log2_int(usize, node.bytes);
            node.previous = null;
            node.next = self.cached[bin];
            self.cached[bin] = node;
        } else self.freeBlock(node);
    }
    fn resize(raw: *anyopaque, pointer: ?*anyopaque, bytes: usize) callconv(.c) ?*anyopaque {
        const old = pointer orelse return allocate(raw, bytes);
        const replacement = allocate(raw, bytes) orelse return null;
        const size = @min(bytes, header(old).requested);
        @memcpy(@as([*]u8, @ptrCast(replacement))[0..size], @as([*]const u8, @ptrCast(old))[0..size]);
        release(raw, old);
        return replacement;
    }
    fn poll(raw: *anyopaque) callconv(.c) c_int {
        return @intFromBool((from(raw).budget orelse return 0).consume());
    }
    fn work(raw: *anyopaque, amount: usize) callconv(.c) c_int {
        return @intFromBool((from(raw).budget orelse return 0).consumeWork(amount));
    }
    fn context(self: *Memory, stack_bytes: usize) Context {
        return .{ .user = self, .stack_limit = stack_bytes };
    }
    fn check(self: *const Memory) !void {
        if (self.budget) |budget| if (budget.failure) |err| return err;
        if (self.failure) |err| return err;
    }
    fn freeBlock(self: *Memory, node: *Header) void {
        self.resident -= node.bytes;
        const buffer: [*]align(16) u8 = @ptrCast(@alignCast(node));
        self.alloc.free(buffer[0..node.bytes]);
    }
    fn trim(self: *Memory, maximum: usize) void {
        var bin = self.cached.len;
        while (bin > 0 and self.resident > maximum) {
            bin -= 1;
            while (self.cached[bin]) |node| {
                if (self.resident <= maximum) break;
                self.cached[bin] = node.next;
                self.freeBlock(node);
            }
        }
    }
    fn reset(self: *Memory) void {
        while (self.head) |node| release(self, @ptrFromInt(@intFromPtr(node) + header_bytes));
        self.budget = null;
        self.failure = null;
        std.debug.assert(self.live == 0);
    }
    fn deinit(self: *Memory) void {
        self.retain = false;
        self.reset();
        self.trim(0);
        std.debug.assert(self.resident == 0);
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
        var execution = Executor.init(self.memory.alloc, self.limits);
        defer execution.deinit();
        return execution.find(self, subject, start, matches, budget);
    }
    pub fn deinit(self: *Program) void {
        var context = self.memory.context(self.limits.stack_bytes);
        antfly_regex_destroy(&context, self.native);
        self.memory.deinit();
    }
};

/// One synchronous execution owner, reusable across rows and occurrences.
/// Never share this mutable scratch between concurrent callers. It retains
/// neither subject bytes nor a request budget after a call (including errors).
/// Immutable Programs may be used by independently owned Executors in parallel.
pub const Executor = struct {
    memory: Memory,
    limits: Limits,
    pub const Stats = struct { resident_bytes: usize, peak_bytes: usize, allocations: usize, reuses: usize };
    pub fn init(alloc: A, limits: Limits) Executor {
        return .{ .memory = .{ .alloc = alloc, .limit = limits.heap_bytes, .budget = null, .retain = true }, .limits = limits };
    }
    pub fn snapshot(self: *const Executor) Stats {
        return .{ .resident_bytes = self.memory.resident, .peak_bytes = self.memory.resident_peak, .allocations = self.memory.allocations, .reuses = self.memory.reused };
    }
    pub fn find(self: *Executor, program: *const Program, subject: Subject, start: usize, matches: []Span, budget: *Budget) !bool {
        @memset(matches, .{});
        errdefer @memset(matches, .{});
        if (start > subject.len()) return error.SqlInvalidArgument;
        self.memory.limit = @min(self.limits.heap_bytes, program.limits.heap_bytes);
        self.memory.trim(self.memory.limit);
        self.memory.budget = budget;
        defer self.memory.reset();
        var context = self.memory.context(@min(self.limits.stack_bytes, program.limits.stack_bytes));
        if (!budget.consume()) return budget.failure.?;
        const status = antfly_regex_search(&context, program.native, subject.codepoints.ptr, subject.len(), start, matches.ptr, matches.len);
        try self.memory.check();
        if (status == 1) return false;
        try checkStatus(status);
        for (matches) |match| _ = try subject.slice(match);
        return true;
    }
    pub fn deinit(self: *Executor) void {
        self.memory.deinit();
    }
    /// start is a zero-based character offset; occurrence zero replaces all,
    /// otherwise only that one-based occurrence. SQL arity/NULL/argument
    /// validation belongs to the scalar binder. No result escapes on failure.
    pub fn replaceAlloc(self: *Executor, alloc: A, program: *const Program, subject: Subject, replacement: *const Replacement, start: usize, occurrence: usize, maximum: usize, budget: *Budget) ![]u8 {
        var output: BoundedOutput = .{ .alloc = alloc, .maximum = maximum, .budget = budget };
        defer output.bytes.deinit(alloc);
        if (start > subject.len()) {
            try output.append(subject.bytes);
            return output.bytes.toOwnedSlice(alloc);
        }
        // PostgreSQL replacement syntax refers only to groups 1..9 and the
        // whole match. Internal backreference matching still owns its native
        // capture bookkeeping independently of this bounded result array.
        var captures: [10]Span = undefined;
        const spans = captures[0..@min(captures.len, program.captures + 1)];
        var cursor = try MatchCursor.init(program, self, subject, start, spans);
        var seen: usize = 0;
        var emitted: usize = 0;
        while (try cursor.next(budget)) {
            seen += 1;
            if (occurrence != 0 and seen != occurrence) continue;
            const begin = subject.byteOffset(@intCast(spans[0].start));
            const end = subject.byteOffset(@intCast(spans[0].end));
            try output.append(subject.bytes[emitted..begin]);
            for (replacement.tokens) |token| switch (token) {
                .literal => |text| try output.append(text),
                .group => |group| {
                    if (group < spans.len) if (try subject.slice(spans[group])) |text| try output.append(text);
                },
            };
            emitted = end;
            if (occurrence != 0) break;
        }
        try output.append(subject.bytes[emitted..]);
        return output.bytes.toOwnedSlice(alloc);
    }
};

/// Owned and immutable; prepare once for a constant replacement, reuse across
/// rows. Unknown escapes (including backslash-zero) remain literal as in PG.
pub const Replacement = struct {
    const Token = union(enum) { literal: []const u8, group: u4 };
    alloc: A,
    text: []u8,
    tokens: []Token,
    pub fn init(alloc: A, text: []const u8, budget: *Budget) !Replacement {
        if (text.len > 1024 * 1024) return error.SqlExpressionTooLarge;
        if (!budget.consumeWork(text.len + 1)) return budget.failure.?;
        if (!std.unicode.utf8ValidateSlice(text)) return error.SqlInvalidText;
        const owned = try alloc.dupe(u8, text);
        errdefer alloc.free(owned);
        var tokens: std.ArrayList(Token) = .empty;
        defer tokens.deinit(alloc);
        var literal: usize = 0;
        var index: usize = 0;
        while (index + 1 < owned.len) {
            if (owned[index] != '\\') {
                index += 1;
                continue;
            }
            const escape = owned[index + 1];
            if (escape != '\\' and escape != '&' and (escape < '1' or escape > '9')) {
                index += 2;
                continue;
            }
            if (index > literal) try tokens.append(alloc, .{ .literal = owned[literal..index] });
            if (escape == '\\') try tokens.append(alloc, .{ .literal = owned[index + 1 .. index + 2] }) else try tokens.append(alloc, .{ .group = if (escape == '&') 0 else @intCast(escape - '0') });
            index += 2;
            literal = index;
        }
        if (literal < owned.len) try tokens.append(alloc, .{ .literal = owned[literal..] });
        return .{ .alloc = alloc, .text = owned, .tokens = try tokens.toOwnedSlice(alloc) };
    }
    pub fn deinit(self: *Replacement) void {
        self.alloc.free(self.tokens);
        self.alloc.free(self.text);
    }
};

const BoundedOutput = struct {
    alloc: A,
    maximum: usize,
    budget: *Budget,
    bytes: std.ArrayList(u8) = .empty,
    fn append(self: *BoundedOutput, bytes: []const u8) !void {
        if (bytes.len > self.maximum -| self.bytes.items.len) return error.SqlExpressionTooLarge;
        if (!self.budget.consumeWork(bytes.len + 1)) return self.budget.failure.?;
        const required = self.bytes.items.len + bytes.len;
        if (required > self.bytes.capacity) {
            const growth = self.bytes.capacity +| (self.bytes.capacity / 2 +| 16);
            try self.bytes.ensureTotalCapacityPrecise(self.alloc, @min(self.maximum, @max(required, growth)));
        }
        self.bytes.appendSliceAssumeCapacity(bytes);
    }
};

/// Nonoverlapping PostgreSQL occurrence iteration. All searches retain the
/// whole subject: a continuation is a character offset, never a UTF-8 slice.
/// Empty matches advance one codepoint, including exactly one match at EOF.
/// The caller owns captures and shares one budget across the whole operation.
pub const MatchCursor = struct {
    program: *const Program,
    executor: *Executor,
    subject: Subject,
    spans: []Span,
    position: usize,
    finished: bool = false,
    pub fn init(program: *const Program, executor: *Executor, subject: Subject, start: usize, spans: []Span) !MatchCursor {
        if (start > subject.len() or spans.len == 0) return error.SqlInvalidArgument;
        return .{ .program = program, .executor = executor, .subject = subject, .spans = spans, .position = start };
    }
    pub fn next(self: *MatchCursor, budget: *Budget) !bool {
        errdefer @memset(self.spans, .{});
        if (self.finished) return false;
        if (!try self.executor.find(self.program, self.subject, self.position, self.spans, budget)) {
            self.finished = true;
            return false;
        }
        const match = self.spans[0];
        if (match.start < self.position or match.end < match.start) return error.InvalidRegexResponse;
        const end: usize = @intCast(match.end);
        if (match.start == match.end) {
            if (end == self.subject.len()) self.finished = true else self.position = end + 1;
        } else self.position = end;
        return true;
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
            var execution = Executor.init(a, .{});
            defer execution.deinit();
            var spans: [2]Span = undefined;
            try std.testing.expect(try execution.find(&program, subject, 0, &spans, &budget));
            try std.testing.expectEqualStrings("cat-cat", (try subject.slice(spans[0])).?);
            try std.testing.expect(try execution.find(&program, subject, @intCast(spans[0].end), &spans, &budget));
            try std.testing.expectEqualStrings("dog-dog", (try subject.slice(spans[0])).?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "PostgreSQL ARE execution reuses bounded scratch without retaining failed request state" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    var program = try Program.compile(a, "([[:alpha:]]+)-\\1", 3, .{}, &budget);
    defer program.deinit();
    var subject = try Subject.init(a, "雪 cat-cat dog-dog");
    defer subject.deinit();
    var execution = Executor.init(a, .{});
    defer execution.deinit();
    var spans: [2]Span = undefined;
    try std.testing.expect(try execution.find(&program, subject, 0, &spans, &budget));
    const warmed = execution.snapshot();
    try std.testing.expect(warmed.allocations > 0);
    for (0..1000) |_| {
        var row_budget: Budget = .{};
        try std.testing.expect(try execution.find(&program, subject, 0, &spans, &row_budget));
        try std.testing.expectEqualStrings("cat-cat", (try subject.slice(spans[0])).?);
    }
    try std.testing.expectEqual(warmed.allocations, execution.snapshot().allocations);
    try std.testing.expect(execution.snapshot().reuses > 1000);
    try std.testing.expect(execution.snapshot().peak_bytes <= execution.limits.heap_bytes);
    var refused: Budget = .{ .remaining = 4 };
    try std.testing.expectError(error.SqlExpressionTooLarge, execution.find(&program, subject, 0, &spans, &refused));
    for (spans) |span| try std.testing.expectEqual(Span{}, span);
    try std.testing.expect(execution.memory.budget == null);
    try std.testing.expect(execution.memory.failure == null);
    try std.testing.expectEqual(@as(usize, 0), execution.memory.live);
    var retry: Budget = .{};
    try std.testing.expect(try execution.find(&program, subject, 0, &spans, &retry));
    try std.testing.expectEqualStrings("cat-cat", (try subject.slice(spans[0])).?);
}

test "PostgreSQL ARE scratch admission counts physical cached residency and reclaims idle bins" {
    var memory: Memory = .{ .alloc = std.testing.allocator, .limit = 512, .budget = null, .retain = true };
    defer memory.deinit();
    const small = Memory.allocate(&memory, 17).?;
    const medium = Memory.allocate(&memory, 91).?;
    @memset(@as([*]u8, @ptrCast(small))[0..17], 9);
    const copied = Memory.resize(&memory, small, 34).?;
    try std.testing.expectEqualSlices(u8, &@as([17]u8, @splat(9)), @as([*]const u8, @ptrCast(copied))[0..17]);
    Memory.release(&memory, medium);
    Memory.release(&memory, copied);
    try std.testing.expect(memory.resident > 0);
    // The new size class fits only after reclaiming other cached classes.
    const large = Memory.allocate(&memory, 400).?;
    try std.testing.expectEqual(@as(usize, 512), memory.resident);
    Memory.release(&memory, large);
    try std.testing.expect(Memory.allocate(&memory, 513) == null);
    try std.testing.expectError(error.SqlExpressionTooLarge, memory.check());
    memory.reset();
    memory.limit = 64;
    memory.trim(memory.limit);
    try std.testing.expect(memory.resident <= 64);
    const bounded = Memory.allocate(&memory, 17).?;
    Memory.release(&memory, bounded);
    try std.testing.expect(memory.resident <= 64);
}

test "PostgreSQL ARE global occurrences use independent PostgreSQL oracle spans" {
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
            occurrences: []const []const struct { start: c_long, end: c_long, text: ?[]const u8 },
        },
    };
    const a = std.testing.allocator;
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("testdata/global-postgres.json"), .{});
    defer golden.deinit();
    try std.testing.expectEqual(@as(u32, 1), golden.value.format);
    try std.testing.expectEqualStrings("C", golden.value.collation);
    var execution = Executor.init(a, .{});
    defer execution.deinit();
    for (golden.value.entries) |case| {
        errdefer std.debug.print("PostgreSQL global ARE fixture {s}\n", .{case.id});
        var budget: Budget = .{};
        var program = try Program.compile(a, case.pattern, case.flags, .{}, &budget);
        defer program.deinit();
        try std.testing.expectEqual(case.captures, program.captures);
        var subject = try Subject.init(a, case.input);
        defer subject.deinit();
        const spans = try a.alloc(Span, case.captures + 1);
        defer a.free(spans);
        var cursor = try MatchCursor.init(&program, &execution, subject, case.start, spans);
        for (case.occurrences) |expected| {
            try std.testing.expect(try cursor.next(&budget));
            for (spans, expected) |span, capture| {
                try std.testing.expectEqual(capture.start, span.start);
                try std.testing.expectEqual(capture.end, span.end);
                if (capture.text) |text| try std.testing.expectEqualStrings(text, (try subject.slice(span)).?) else try std.testing.expect((try subject.slice(span)) == null);
            }
        }
        try std.testing.expect(!try cursor.next(&budget));
        try std.testing.expect(!try cursor.next(&budget));
    }
}

test "PostgreSQL ARE streaming replacement agrees with independent PostgreSQL results" {
    const Golden = struct {
        format: u32,
        collation: []const u8,
        entries: []const struct { id: []const u8, pattern: []const u8, input: []const u8, replacement: []const u8, flags: c_int, start: usize, occurrence: usize, expected: []const u8 },
    };
    const a = std.testing.allocator;
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("testdata/replacement-postgres.json"), .{});
    defer golden.deinit();
    try std.testing.expectEqual(@as(u32, 1), golden.value.format);
    try std.testing.expectEqualStrings("C", golden.value.collation);
    var execution = Executor.init(a, .{});
    defer execution.deinit();
    for (golden.value.entries) |case| {
        errdefer std.debug.print("PostgreSQL replacement fixture {s}\n", .{case.id});
        var budget: Budget = .{};
        var program = try Program.compile(a, case.pattern, case.flags, .{}, &budget);
        defer program.deinit();
        var subject = try Subject.init(a, case.input);
        defer subject.deinit();
        var replacement = try Replacement.init(a, case.replacement, &budget);
        defer replacement.deinit();
        const output = try execution.replaceAlloc(a, &program, subject, &replacement, case.start, case.occurrence, 1024, &budget);
        defer a.free(output);
        try std.testing.expectEqualStrings(case.expected, output);
    }
}

test "PostgreSQL ARE streaming replacement bounds output and unwinds every allocation fault" {
    const Faults = struct {
        fn run(a: A) !void {
            var budget: Budget = .{};
            var program = try Program.compile(a, "(a)(b)?", 3, .{}, &budget);
            defer program.deinit();
            var subject = try Subject.init(a, "ab雪a😀ab");
            defer subject.deinit();
            var replacement = try Replacement.init(a, "<\\2>-\\1-\\&", &budget);
            defer replacement.deinit();
            var execution = Executor.init(a, .{});
            defer execution.deinit();
            const refused: ?[]u8 = execution.replaceAlloc(a, &program, subject, &replacement, 0, 0, 1, &budget) catch |err| switch (err) {
                error.SqlExpressionTooLarge => null,
                else => return err,
            };
            if (refused) |unexpected| {
                a.free(unexpected);
                return error.ExpectedOutputLimit;
            }
            const output = try execution.replaceAlloc(a, &program, subject, &replacement, 0, 0, 1024, &budget);
            defer a.free(output);
            try std.testing.expectEqualStrings("<b>-a-ab雪<>-a-a😀<b>-a-ab", output);
        }
    };
    try Faults.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "PostgreSQL ARE every native compile and match checkpoint unwinds cancellation" {
    const Check = struct {
        calls: usize = 0,
        cancel_at: usize = std.math.maxInt(usize),
        fn checkpoint(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const index = self.calls;
            self.calls += 1;
            if (index == self.cancel_at) return error.Canceled;
        }
    };
    const a = std.testing.allocator;
    var observed: Check = .{};
    var compile_budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &observed };
    var program = try Program.compile(a, "(a|ab|abc)+\\1", 3, .{}, &compile_budget);
    defer program.deinit();
    const compile_checks = observed.calls;
    for (0..compile_checks) |index| {
        var check: Check = .{ .cancel_at = index };
        var budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &check };
        try std.testing.expectError(error.Canceled, Program.compile(a, "(a|ab|abc)+\\1", 3, .{}, &budget));
    }
    var subject = try Subject.init(a, "x abcabc y");
    defer subject.deinit();
    var execution = Executor.init(a, .{});
    defer execution.deinit();
    var spans: [2]Span = undefined;
    observed.calls = 0;
    var match_budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &observed };
    try std.testing.expect(try execution.find(&program, subject, 0, &spans, &match_budget));
    const match_checks = observed.calls;
    for (0..match_checks) |index| {
        var check: Check = .{ .cancel_at = index };
        var budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &check };
        try std.testing.expectError(error.Canceled, execution.find(&program, subject, 0, &spans, &budget));
        for (spans) |span| try std.testing.expectEqual(Span{}, span);
    }
    var retry: Budget = .{};
    try std.testing.expect(try execution.find(&program, subject, 0, &spans, &retry));
    try std.testing.expectEqualStrings("abcabc", (try subject.slice(spans[0])).?);
    var replacement = try Replacement.init(a, "<\\&>", &retry);
    defer replacement.deinit();
    observed.calls = 0;
    var replacement_budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &observed };
    const output = try execution.replaceAlloc(a, &program, subject, &replacement, 0, 0, 1024, &replacement_budget);
    defer a.free(output);
    try std.testing.expectEqualStrings("x <abcabc> y", output);
    const replacement_checks = observed.calls;
    for (0..replacement_checks) |index| {
        var check: Check = .{ .cancel_at = index };
        var budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &check };
        try std.testing.expectError(error.Canceled, execution.replaceAlloc(a, &program, subject, &replacement, 0, 0, 1024, &budget));
    }
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
    if (comptime @import("builtin").single_threaded) return error.SkipZigTest;
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
