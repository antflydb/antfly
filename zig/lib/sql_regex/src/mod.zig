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

//! PostgreSQL ARE backend. No search-index byte automaton, host locale, or
//! provider allocation is used. Patterns own native blocks; each execution
//! owns independent scratch and bounded execution-local preparation caches.
const std = @import("std");
const A = std.mem.Allocator;
const engine = @import("antfly_capture_regex");

pub const Limits = struct { heap_bytes: usize = 8 * 1024 * 1024, max_depth: usize = 128, pattern_bytes: usize = 16 * 1024 };
pub const Budget = engine.Budget;
fn regexError(err: anyerror) anyerror {
    return switch (err) {
        error.InvalidPattern => error.SqlInvalidRegularExpression,
        error.RegexLimitExceeded => error.SqlExpressionTooLarge,
        error.InvalidStart => error.SqlInvalidArgument,
        error.InvalidRegexProgram => error.InvalidRegexResponse,
        else => err,
    };
}
test "PostgreSQL ARE checkpoint intervals amortize callbacks without discounting work" {
    const Control = struct {
        calls: usize = 0,
        canceled: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.canceled) return error.QueryCanceled;
        }
    };
    var control: Control = .{};
    var budget: Budget = .{ .remaining = 10000, .checkpoint = Control.check, .ptr = &control, .checkpoint_interval = 256, .limit_error = error.SqlExpressionTooLarge };
    for (0..10000) |_| try budget.charge(1);
    try std.testing.expectEqual(@as(usize, 0), budget.remaining);
    try std.testing.expect(control.calls >= 39 and control.calls <= 41);
    try std.testing.expectError(error.SqlExpressionTooLarge, budget.charge(1));
    control.canceled = true;
    var canceled: Budget = .{ .checkpoint = Control.check, .ptr = &control, .checkpoint_interval = 256 };
    try std.testing.expectError(error.QueryCanceled, canceled.charge(1));
    try std.testing.expectError(error.QueryCanceled, canceled.charge(1));
}

pub const Span = engine.Span;
/// Ordered PostgreSQL flag semantics, including the server's actual basic/
/// extended flavor transitions. Global occurrence selection is a SQL overload
/// concern, separate from native compilation flags.
pub const Flags = struct {
    native: c_int = 3,
    global: bool = false,
    pub fn parse(text: []const u8, allow_global: bool) !Flags {
        if (text.len > 16 * 1024) return error.SqlExpressionTooLarge;
        var result: Flags = .{};
        for (text) |flag| switch (flag) {
            'g' => if (allow_global) {
                result.global = true;
            } else return error.SqlInvalidArgument,
            'b' => result.native &= ~@as(c_int, 7),
            'c' => result.native &= ~@as(c_int, 8),
            'e' => {
                result.native |= 1;
                result.native &= ~@as(c_int, 7);
            },
            'i' => result.native |= 8,
            'm', 'n' => result.native |= 192,
            'p' => {
                result.native |= 64;
                result.native &= ~@as(c_int, 128);
            },
            'q' => {
                result.native |= 4;
                result.native &= ~@as(c_int, 3);
            },
            's' => result.native &= ~@as(c_int, 192),
            't' => result.native &= ~@as(c_int, 32),
            'w' => {
                result.native &= ~@as(c_int, 64);
                result.native |= 128;
            },
            'x' => result.native |= 32,
            else => return error.SqlInvalidArgument,
        };
        return result;
    }
};
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
    fn allocate(raw: *anyopaque, bytes: usize) ?*anyopaque {
        const self = from(raw);
        if (self.budget) |budget| if (budget.failure != null) return null;
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
    fn release(raw: *anyopaque, pointer: ?*anyopaque) void {
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
    fn resize(raw: *anyopaque, pointer: ?*anyopaque, bytes: usize) ?*anyopaque {
        const old = pointer orelse return allocate(raw, bytes);
        const size = @min(bytes, header(old).requested);
        // Failed realloc must retain the old allocation and its contents.
        if (from(raw).budget) |budget| if (!budget.consumeWork(size)) return null;
        const replacement = allocate(raw, bytes) orelse return null;
        @memcpy(@as([*]u8, @ptrCast(replacement))[0..size], @as([*]const u8, @ptrCast(old))[0..size]);
        release(raw, old);
        return replacement;
    }
    fn allocator(self: *Memory) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocBlock, .resize = resizeBlock, .remap = remapBlock, .free = freeAllocation } };
    }
    fn allocBlock(raw: *anyopaque, bytes: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
        if (alignment.toByteUnits() > 16) return null;
        return @ptrCast(allocate(raw, bytes));
    }
    fn resizeBlock(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remapBlock(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, size: usize, _: usize) ?[*]u8 {
        if (alignment.toByteUnits() > 16) return null;
        return @ptrCast(resize(raw, bytes.ptr, size));
    }
    fn freeAllocation(raw: *anyopaque, bytes: []u8, _: std.mem.Alignment, _: usize) void {
        release(raw, bytes.ptr);
    }
    fn check(self: *const Memory) !void {
        if (self.budget) |budget| if (budget.failure) |err| return regexError(err);
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

test "PostgreSQL ARE realloc work refusal preserves old ownership and permits retry" {
    var refused: Budget = .{ .remaining = 8 };
    var memory: Memory = .{ .alloc = std.testing.allocator, .limit = 4096, .budget = &refused };
    defer memory.deinit();
    const initial = Memory.allocate(&memory, 16) orelse return error.OutOfMemory;
    const original: [*]u8 = @ptrCast(initial);
    @memset(original[0..16], 0x5a);
    const live = memory.live;
    try std.testing.expect(Memory.resize(&memory, initial, 32) == null);
    try std.testing.expectError(error.SqlExpressionTooLarge, memory.check());
    try std.testing.expectEqual(live, memory.live);
    try std.testing.expectEqual(@as(usize, 1), memory.allocations);
    try std.testing.expectEqual(@as(usize, 16), Memory.header(initial).requested);
    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat(0x5a))), original[0..16]);
    try std.testing.expect(Memory.allocate(&memory, 16) == null);
    var retry: Budget = .{};
    memory.budget = &retry;
    const resized = Memory.resize(&memory, initial, 32) orelse return error.OutOfMemory;
    const bytes: [*]const u8 = @ptrCast(resized);
    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat(0x5a))), bytes[0..16]);
    try std.testing.expectEqual(@as(usize, 16), (Budget{}).remaining - retry.remaining);
    Memory.release(&memory, resized);
    try std.testing.expectEqual(@as(usize, 0), memory.live);
}

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
    native: engine.Program,
    captures: usize,
    limits: Limits,
    pub fn compile(alloc: A, pattern: []const u8, flags: c_int, limits: Limits, budget: *Budget) !Program {
        budget.limit_error = error.SqlExpressionTooLarge;
        if (pattern.len > limits.pattern_bytes) return error.SqlExpressionTooLarge;
        var input = try Subject.init(alloc, pattern);
        defer input.deinit();
        var memory: Memory = .{ .alloc = alloc, .limit = limits.heap_bytes, .budget = budget };
        errdefer memory.deinit();
        if (flags < 0 or flags & ~@as(c_int, 239) != 0) return error.SqlInvalidRegularExpression;
        const options: engine.Options = .{
            .syntax = switch (flags & 7) {
                0 => .basic,
                1 => .extended,
                3 => .advanced,
                4 => .literal,
                else => return error.SqlInvalidRegularExpression,
            },
            .case_insensitive = flags & 8 != 0,
            .expanded = flags & 32 != 0,
            .newline_sensitive = flags & 64 != 0,
            .line_anchors = flags & 128 != 0,
        };
        const native = engine.Program.compile(memory.allocator(), input.codepoints[0..input.len()], options, .{ .max_depth = limits.max_depth, .pattern_bytes = limits.pattern_bytes }, budget) catch |err| {
            try memory.check();
            return regexError(err);
        };
        try memory.check();
        // Prepared patterns never retain the compiling request's budget.
        memory.budget = null;
        return .{ .memory = memory, .native = native, .captures = native.captures, .limits = limits };
    }
    pub fn find(self: *const Program, subject: Subject, start: usize, matches: []Span, budget: *Budget) !bool {
        var execution = Executor.init(self.memory.alloc, self.limits);
        defer execution.deinit();
        return execution.find(self, subject, start, matches, budget);
    }
    pub fn deinit(self: *Program) void {
        self.native.deinit(self.memory.allocator());
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
        budget.limit_error = error.SqlExpressionTooLarge;
        @memset(matches, .{});
        errdefer @memset(matches, .{});
        if (start > subject.len()) return error.SqlInvalidArgument;
        self.memory.limit = @min(self.limits.heap_bytes, program.limits.heap_bytes);
        self.memory.trim(self.memory.limit);
        self.memory.budget = budget;
        defer self.memory.reset();
        if (!budget.consume()) return regexError(budget.failure.?);
        var executable = program.native;
        executable.depth_limit = @min(executable.depth_limit, self.limits.max_depth);
        const found = executable.find(self.memory.allocator(), subject.codepoints[0..subject.len()], start, matches, budget) catch |err| {
            try self.memory.check();
            return regexError(err);
        };
        try self.memory.check();
        if (!found) return false;
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
        budget.limit_error = error.SqlExpressionTooLarge;
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

/// Execution-owned bounded LRU, never a mutable global/prepared-plan cache.
/// The backing allocator must reclaim frees; do not use a per-row arena.
/// Borrowed programs live until the next pattern admission or session close.
/// A session is synchronous and must not be shared by concurrent callers.
pub const Session = struct {
    const Entry = struct { text: []u8, flags: c_int, hash: u64, touched: u64, program: Program };
    const ReplacementEntry = struct { hash: u64, touched: u64, plan: Replacement };
    alloc: A,
    limits: Limits,
    maximum_bytes: usize,
    executor: Executor,
    entries: [8]?Entry = @splat(null),
    replacements: [8]?ReplacementEntry = @splat(null),
    resident: usize = 0,
    replacement_resident: usize = 0,
    tick: u64 = 0,
    hits: u64 = 0,
    compilations: u64 = 0,
    evictions: u64 = 0,
    replacement_hits: u64 = 0,
    replacement_preparations: u64 = 0,
    pub fn init(alloc: A, limits: Limits, maximum_bytes: usize) Session {
        return .{ .alloc = alloc, .limits = limits, .maximum_bytes = maximum_bytes, .executor = Executor.init(alloc, limits) };
    }
    fn remove(self: *Session, index: usize) void {
        const entry = &self.entries[index].?;
        self.resident -= entry.text.len + entry.program.memory.resident;
        entry.program.deinit();
        self.alloc.free(entry.text);
        self.entries[index] = null;
        self.evictions +|= 1;
    }
    fn oldest(self: *const Session) ?usize {
        var result: ?usize = null;
        for (self.entries, 0..) |entry, i| if (entry) |value| {
            if (result == null or value.touched < self.entries[result.?].?.touched) result = i;
        };
        return result;
    }
    pub fn pattern(self: *Session, text: []const u8, flags: c_int, budget: *Budget) !*const Program {
        budget.limit_error = error.SqlExpressionTooLarge;
        if (text.len > self.limits.pattern_bytes or text.len >= self.maximum_bytes) return error.SqlExpressionTooLarge;
        if (!budget.consumeWork(text.len + self.entries.len)) return regexError(budget.failure.?);
        const hash = std.hash.Wyhash.hash(@as(u32, @bitCast(flags)), text);
        self.tick +|= 1;
        for (&self.entries) |*slot| if (slot.*) |*entry| {
            if (entry.hash != hash or entry.flags != flags or entry.text.len != text.len) continue;
            if (!budget.consumeWork(text.len)) return regexError(budget.failure.?);
            if (!std.mem.eql(u8, text, entry.text)) continue;
            entry.touched = self.tick;
            self.hits +|= 1;
            return &entry.program;
        };
        const heap = @min(self.limits.heap_bytes, self.maximum_bytes - text.len);
        // Reserve the full compile admission, not merely the eventual retained
        // NFA size. A miss cannot exceed the cache bound while compiling.
        while (self.resident > self.maximum_bytes - text.len - heap) {
            if (self.oldestReplacement()) |index| self.removeReplacement(index) else self.remove(self.oldest() orelse return error.InvalidRegexResponse);
        }
        var index: usize = 0;
        while (index < self.entries.len and self.entries[index] != null) : (index += 1) {}
        if (index == self.entries.len) {
            index = self.oldest().?;
            self.remove(index);
        }
        const owned = try self.alloc.dupe(u8, text);
        errdefer self.alloc.free(owned);
        var limits = self.limits;
        limits.heap_bytes = heap;
        self.compilations +|= 1;
        const program = try Program.compile(self.alloc, text, flags, limits, budget);
        self.entries[index] = .{ .text = owned, .flags = flags, .hash = hash, .touched = self.tick, .program = program };
        self.resident += owned.len + program.memory.resident;
        std.debug.assert(self.resident <= self.maximum_bytes);
        return &self.entries[index].?.program;
    }
    fn removeReplacement(self: *Session, index: usize) void {
        const plan = &self.replacements[index].?.plan;
        const bytes = plan.text.len + plan.tokens.len * @sizeOf(Replacement.Token);
        self.resident -= bytes;
        self.replacement_resident -= bytes;
        plan.deinit();
        self.replacements[index] = null;
    }
    fn oldestReplacement(self: *const Session) ?usize {
        var result: ?usize = null;
        for (self.replacements, 0..) |entry, i| if (entry) |value| {
            if (result == null or value.touched < self.replacements[result.?].?.touched) result = i;
        };
        return result;
    }
    /// Valid until the next preparation on this synchronous session. Separate
    /// replacement eviction never invalidates the program currently executing.
    /// Oversized templates use a caller-owned fallback instead of a rejection.
    pub const ReplacementLease = union(enum) {
        cached: *const Replacement,
        owned: Replacement,
        pub fn plan(self: *const ReplacementLease) *const Replacement {
            return switch (self.*) {
                .cached => |value| value,
                .owned => |*value| value,
            };
        }
        pub fn deinit(self: *ReplacementLease) void {
            switch (self.*) {
                .cached => {},
                .owned => |*value| value.deinit(),
            }
        }
    };
    pub fn replacement(self: *Session, fallback: A, text: []const u8, budget: *Budget) !ReplacementLease {
        budget.limit_error = error.SqlExpressionTooLarge;
        if (text.len > 1024 * 1024) return error.SqlExpressionTooLarge;
        try budget.charge(text.len + self.replacements.len);
        const hash = std.hash.Wyhash.hash(0, text);
        self.tick +|= 1;
        for (&self.replacements) |*slot| if (slot.*) |*entry| {
            if (entry.hash != hash or entry.plan.text.len != text.len) continue;
            try budget.charge(text.len);
            if (!std.mem.eql(u8, text, entry.plan.text)) continue;
            entry.touched = self.tick;
            self.replacement_hits +|= 1;
            return .{ .cached = &entry.plan };
        };
        // Conservative peak for ArrayList growth and slice conversion. The
        // subquota prevents dynamic templates monopolizing compilation space;
        // never evict a regex while its program is borrowed for replacement.
        const reservation = text.len + @max(@as(usize, 32), (text.len + 1) * 4) * @sizeOf(Replacement.Token);
        const maximum = @min(@as(usize, 256 * 1024), self.maximum_bytes / 8);
        self.replacement_preparations +|= 1;
        if (reservation > maximum) return .{ .owned = try Replacement.init(fallback, text, budget) };
        while (self.replacement_resident > maximum - reservation or self.resident > self.maximum_bytes - reservation) {
            self.removeReplacement(self.oldestReplacement() orelse return .{ .owned = try Replacement.init(fallback, text, budget) });
        }
        var index: usize = 0;
        while (index < self.replacements.len and self.replacements[index] != null) : (index += 1) {}
        if (index == self.replacements.len) {
            index = self.oldestReplacement().?;
            self.removeReplacement(index);
        }
        const prepared = try Replacement.init(self.alloc, text, budget);
        self.replacements[index] = .{ .hash = hash, .touched = self.tick, .plan = prepared };
        const bytes = prepared.text.len + prepared.tokens.len * @sizeOf(Replacement.Token);
        std.debug.assert(bytes <= reservation);
        self.resident += bytes;
        self.replacement_resident += bytes;
        std.debug.assert(self.resident <= self.maximum_bytes);
        return .{ .cached = &self.replacements[index].?.plan };
    }
    pub fn deinit(self: *Session) void {
        self.executor.deinit();
        for (0..self.entries.len) |index| if (self.entries[index] != null) self.remove(index);
        for (0..self.replacements.len) |index| if (self.replacements[index] != null) self.removeReplacement(index);
        std.debug.assert(self.resident == 0);
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
        budget.limit_error = error.SqlExpressionTooLarge;
        if (text.len > 1024 * 1024) return error.SqlExpressionTooLarge;
        if (!budget.consumeWork(text.len + 1)) return regexError(budget.failure.?);
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

test "PostgreSQL ARE replacement cache reuses templates without evicting a borrowed program" {
    const a = std.testing.allocator;
    var session = Session.init(a, .{}, 16 * 1024 * 1024);
    defer session.deinit();
    var budget: Budget = .{};
    const program = try session.pattern("(a)", 3, &budget);
    var subject = try Subject.init(a, "aba");
    defer subject.deinit();
    for (0..1000) |_| {
        var lease = try session.replacement(a, "<\\1>", &budget);
        defer lease.deinit();
        const output = try session.executor.replaceAlloc(a, program, subject, lease.plan(), 0, 0, 1024, &budget);
        defer a.free(output);
        try std.testing.expectEqualStrings("<a>b<a>", output);
    }
    try std.testing.expectEqual(@as(u64, 1), session.replacement_preparations);
    try std.testing.expectEqual(@as(u64, 999), session.replacement_hits);
    for (0..16) |i| {
        var text: [32]u8 = undefined;
        var lease = try session.replacement(a, try std.fmt.bufPrint(&text, "replacement-{d}", .{i}), &budget);
        defer lease.deinit();
        const output = try session.executor.replaceAlloc(a, program, subject, lease.plan(), 0, 1, 1024, &budget);
        defer a.free(output);
        try std.testing.expect(std.mem.startsWith(u8, output, "replacement-"));
    }
    try std.testing.expectEqual(@as(u64, 1), session.compilations);
    try std.testing.expect(session.resident <= session.maximum_bytes);
    var refused: Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlExpressionTooLarge, session.replacement(a, "replacement-15", &refused));
}

test "PostgreSQL ARE warm replacement preparation performs no allocations" {
    var tracked = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var session = Session.init(tracked.allocator(), .{}, 16 * 1024 * 1024);
    defer session.deinit();
    var budget: Budget = .{};
    {
        var lease = try session.replacement(tracked.allocator(), "<\\1>", &budget);
        defer lease.deinit();
    }
    const allocations = tracked.alloc_index;
    tracked.fail_index = allocations;
    for (0..1000) |_| {
        var lease = try session.replacement(tracked.allocator(), "<\\1>", &budget);
        defer lease.deinit();
        try std.testing.expect(lease == .cached);
    }
    try std.testing.expectEqual(allocations, tracked.alloc_index);
    try std.testing.expectEqual(@as(u64, 1000), session.replacement_hits);
}

test "PostgreSQL ARE replacement admission owns oversized fallbacks and unwinds allocation faults" {
    const Faults = struct {
        fn run(a: A) !void {
            var session = Session.init(a, .{}, 16 * 1024 * 1024);
            defer session.deinit();
            var budget: Budget = .{};
            {
                var first = try session.replacement(a, "<\\1>", &budget);
                defer first.deinit();
                try std.testing.expect(first == .cached);
            }
            {
                var repeat = try session.replacement(a, "<\\1>", &budget);
                defer repeat.deinit();
                try std.testing.expect(repeat == .cached);
            }
            var large: [8192]u8 = @splat('x');
            var fallback = try session.replacement(a, &large, &budget);
            defer fallback.deinit();
            try std.testing.expect(fallback == .owned);
            try std.testing.expectEqualStrings(&large, fallback.plan().text);
            try std.testing.expect(session.replacement_resident <= 256 * 1024);
        }
    };
    try Faults.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

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

test "PostgreSQL ARE execution session owns bounded LRU patterns and reuses warm scratch" {
    const a = std.testing.allocator;
    var session = Session.init(a, .{}, 16 * 1024 * 1024);
    defer session.deinit();
    var budget: Budget = .{};
    var text = [_]u8{ 'a', 'b', 'c' };
    _ = try session.pattern(&text, 3, &budget);
    text[0] = 'x';
    var subject = try Subject.init(a, "雪abc😀");
    defer subject.deinit();
    var spans: [1]Span = undefined;
    const first = try session.pattern("abc", 3, &budget);
    try std.testing.expect(try session.executor.find(first, subject, 0, &spans, &budget));
    const warmed = session.executor.snapshot().allocations;
    for (0..1000) |_| {
        var row_budget: Budget = .{};
        const program = try session.pattern("abc", 3, &row_budget);
        try std.testing.expect(program.memory.budget == null);
        try std.testing.expect(try session.executor.find(program, subject, 0, &spans, &row_budget));
        try std.testing.expectEqualStrings("abc", (try subject.slice(spans[0])).?);
    }
    try std.testing.expectEqual(@as(u64, 1), session.compilations);
    try std.testing.expectEqual(warmed, session.executor.snapshot().allocations);
    _ = try session.pattern("abc", 11, &budget);
    try std.testing.expectEqual(@as(u64, 2), session.compilations);
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i" }) |pattern| {
        _ = try session.pattern(pattern, 3, &budget);
        try std.testing.expect(session.resident <= session.maximum_bytes);
    }
    try std.testing.expect(session.evictions > 0);
    try std.testing.expectError(error.SqlInvalidRegularExpression, session.pattern("[", 3, &budget));
    const program = try session.pattern("abc", 3, &budget);
    try std.testing.expect(try session.executor.find(program, subject, 0, &spans, &budget));
    var refused = Session.init(a, .{}, 32);
    defer refused.deinit();
    try std.testing.expectError(error.SqlExpressionTooLarge, refused.pattern("abc", 3, &budget));
    try std.testing.expectEqual(@as(usize, 0), refused.resident);
}

test "PostgreSQL ARE session admission unwinds every allocation failure" {
    const Faults = struct {
        fn run(a: A) !void {
            var session = Session.init(a, .{}, 16 * 1024 * 1024);
            defer session.deinit();
            var budget: Budget = .{};
            var subject = try Subject.init(a, "abc abc");
            defer subject.deinit();
            var spans: [1]Span = undefined;
            for ([_][]const u8{ "a", "(ab)+", "a|ab" }) |pattern| {
                const program = try session.pattern(pattern, 3, &budget);
                try std.testing.expect(try session.executor.find(program, subject, 0, &spans, &budget));
            }
            const retained = try session.pattern("a", 3, &budget);
            try std.testing.expect(try session.executor.find(retained, subject, 0, &spans, &budget));
            try std.testing.expectEqual(@as(u64, 3), session.compilations);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "PostgreSQL ARE ordered flags reject unknown and disallowed global options" {
    try std.testing.expect((try Flags.parse("gi", true)).global);
    try std.testing.expectEqual(@as(c_int, 11), (try Flags.parse("gi", true)).native);
    try std.testing.expectError(error.SqlInvalidArgument, Flags.parse("g", false));
    try std.testing.expectError(error.SqlInvalidArgument, Flags.parse("z", true));
    try std.testing.expectError(error.SqlInvalidArgument, Flags.parse("é", true));
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

test "PostgreSQL ARE rejects malformed bound syntax without treating valid literal braces as bounds" {
    for ([_][]const u8{ "{1}", "a{1}{2}", "a{1", "a{256}" }) |pattern| {
        var budget: Budget = .{};
        try std.testing.expectError(error.SqlInvalidRegularExpression, Program.compile(std.testing.allocator, pattern, 3, .{}, &budget));
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
    for ([_][]const u8{ "(a|ab|abc)+\\1", "(a|ab|abc)+?\\1" }) |pattern| {
        var observed: Check = .{};
        var compile_budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &observed };
        var program = try Program.compile(a, pattern, 3, .{}, &compile_budget);
        defer program.deinit();
        const compile_checks = observed.calls;
        for (0..compile_checks) |index| {
            var check: Check = .{ .cancel_at = index };
            var budget: Budget = .{ .checkpoint = Check.checkpoint, .ptr = &check };
            try std.testing.expectError(error.Canceled, Program.compile(a, pattern, 3, .{}, &budget));
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
            options: []const u8,
            start: usize,
            captures: usize,
            matched: bool,
            spans: []const struct { start: c_long, end: c_long, text: ?[]const u8 },
        },
    };
    const a = std.testing.allocator;
    for ([_][]const u8{ @embedFile("testdata/postgres.json"), @embedFile("testdata/capture-postgres.json") }) |fixture| {
        const golden = try std.json.parseFromSlice(Golden, a, fixture, .{});
        defer golden.deinit();
        try std.testing.expectEqual(@as(u32, 1), golden.value.format);
        try std.testing.expectEqualStrings("C", golden.value.collation);
        for (golden.value.entries) |case| {
            errdefer std.debug.print("PostgreSQL ARE fixture {s}\n", .{case.id});
            var budget: Budget = .{};
            const flags = try Flags.parse(case.options, false);
            try std.testing.expectEqual(case.flags, flags.native);
            var program = try Program.compile(a, case.pattern, flags.native, .{}, &budget);
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

test "native regex nested calls preserve independent allocation owners" {
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

test "native regex regular captures and late failures scale with subject length" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "(a+)(b+)", "(a|aa)*", "(a*)*", "a+b", "(a|aa)*b", "(?!a*z)a+", "(?<!z.*)a+", "(?=a*z)a+" }, 0..) |pattern, kind| {
        var compile_budget: Budget = .{};
        var program = try Program.compile(a, pattern, 3, .{}, &compile_budget);
        defer program.deinit();
        var previous: usize = 0;
        for ([_]usize{ 4096, 16384 }) |size| {
            const input = try a.alloc(u8, if (kind == 0) size * 2 else size);
            defer a.free(input);
            @memset(input, 'a');
            if (kind == 0) @memset(input[size..], 'b');
            var subject = try Subject.init(a, input);
            defer subject.deinit();
            var execution = Executor.init(a, .{});
            defer execution.deinit();
            var spans: [3]Span = undefined;
            var budget: Budget = .{};
            const initial = budget.remaining;
            const matched = try execution.find(&program, subject, 0, &spans, &budget);
            try std.testing.expectEqual(kind < 3 or kind == 5 or kind == 6, matched);
            if (matched) {
                try std.testing.expectEqual(@as(isize, 0), spans[0].start);
                try std.testing.expectEqual(@as(isize, @intCast(input.len)), spans[0].end);
                if (kind < 3) {
                    try std.testing.expectEqual(@as(isize, @intCast(if (kind == 1) size - 2 else 0)), spans[1].start);
                    try std.testing.expectEqual(@as(isize, @intCast(size)), spans[1].end);
                }
            }
            const work = initial - budget.remaining;
            try std.testing.expect(work >= size);
            if (previous != 0) try std.testing.expect(work <= previous * 5);
            previous = work;
            try std.testing.expect(execution.snapshot().peak_bytes <= execution.limits.heap_bytes);
        }
    }
}

test "native regex repeated backreferences and assertion frontiers unwind allocation faults" {
    const Faults = struct {
        fn run(a: A) !void {
            for ([_][]const u8{ "((a|aa)\\2)*", "(?<!z.*)(a+)" }) |pattern| {
                var budget: Budget = .{};
                var program = try Program.compile(a, pattern, 3, .{}, &budget);
                defer program.deinit();
                var subject = try Subject.init(a, "aaaaaa");
                defer subject.deinit();
                var execution = Executor.init(a, .{});
                defer execution.deinit();
                var spans: [3]Span = undefined;
                try std.testing.expect(try execution.find(&program, subject, 0, &spans, &budget));
                try std.testing.expectEqual(Span{ .start = 0, .end = 6 }, spans[0]);
                var refused: Budget = .{ .remaining = 8 };
                try std.testing.expectError(error.SqlExpressionTooLarge, execution.find(&program, subject, 0, &spans, &refused));
                for (spans) |span| try std.testing.expectEqual(Span{}, span);
                var retry: Budget = .{};
                try std.testing.expect(try execution.find(&program, subject, 0, &spans, &retry));
            }
        }
    };
    try Faults.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "native regex capture benchmark with reusable execution owners" {
    if (@import("builtin").mode == .debug) return error.SkipZigTest;
    const a = std.testing.allocator;
    for ([_][]const u8{ "a+b", "(a+)(b+)", "(a|aa)*", "([a-z]+)-\\1" }, 0..) |pattern, kind| {
        var compile_budget: Budget = .{};
        var program = try Program.compile(a, pattern, 3, .{}, &compile_budget);
        defer program.deinit();
        const input = try a.alloc(u8, if (kind == 3) 65 else 4096);
        defer a.free(input);
        @memset(input, 'a');
        if (kind == 0) input[input.len - 1] = 'b';
        if (kind == 1) @memset(input[2048..], 'b');
        if (kind == 3) input[32] = '-';
        var subject = try Subject.init(a, input);
        defer subject.deinit();
        var execution = Executor.init(a, .{});
        defer execution.deinit();
        var spans: [3]Span = undefined;
        var warm: Budget = .{};
        try std.testing.expect(try execution.find(&program, subject, 0, &spans, &warm));
        const warmed = execution.snapshot().allocations;
        const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        for (0..100) |_| {
            var budget: Budget = .{};
            try std.testing.expect(try execution.find(&program, subject, 0, &spans, &budget));
            try std.testing.expectEqual(@as(isize, @intCast(input.len)), spans[0].end);
        }
        const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
        try std.testing.expectEqual(warmed, execution.snapshot().allocations);
        std.debug.print("native_regex pattern={s} bytes={d} iterations=100 elapsed_ns={d} resident_bytes={d} warm_allocations={d}\n", .{ pattern, input.len, elapsed, execution.snapshot().resident_bytes, warmed });
    }
}
