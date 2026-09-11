// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded Unicode regex validation, pinned to Python 3.12 / Unicode 15.
//! Thompson simulation has no backtracking or lazily growing DFA cache.
//! Captures do not affect this boolean API. Constructs requiring capture state,
//! lookaround, atomicity, locale or inline flag scopes are explicitly rejected.
const std = @import("std");
const schema = @import("extraction_schema.zig");
const unicode = @import("../finetune/gliner2_unicode_tables.zig");
const case_data = @import("gliner_boundary_case_data.zig");
const literals = @import("gliner_boundary_unicode.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Mode = enum { fullmatch, search, match };
pub const Flags = struct {
    ignore_case: bool,
    multiline: bool,
    dot_all: bool,
    verbose: bool,
    ascii: bool,
    pub fn parse(raw: u32) !Flags {
        const supported: u32 = 2 | 8 | 16 | 32 | 64 | 256;
        if (raw & ~supported != 0) return error.UnsupportedExtractionRegexFlags;
        if (raw & 32 != 0 and raw & 256 != 0) return error.InvalidExtractionRegex;
        return .{ .ignore_case = raw & 2 != 0, .multiline = raw & 8 != 0, .dot_all = raw & 16 != 0, .verbose = raw & 64 != 0, .ascii = raw & 256 != 0 };
    }
};
pub const CompileOptions = struct {
    max_pattern_bytes: usize = 4096,
    max_ast_nodes: usize = 4096,
    max_states: usize = 4096,
    max_depth: usize = 64,
    max_repeat: usize = 1024,
    max_class_ranges: usize = 32768,
    max_steps: usize = 2000000,
    control: ?Control = null,
};
pub const MatchOptions = struct {
    max_text_bytes: usize = 1024 * 1024,
    max_steps: usize = 10000000,
    control: ?Control = null,
};
pub const MatchResult = struct { matched: bool, steps: usize };
const Range = struct { first: u21, last: u21 };
const Property = enum(u3) { digit, not_digit, word, not_word, space, not_space };
const Properties = std.StaticBitSet(6);
const Class = struct { ranges: []const Range, properties: Properties, negated: bool };
const Assertion = enum { start, end, absolute_start, absolute_end, word_boundary, not_word_boundary };
const Pair = struct { left: u32, right: u32 };
const Repeat = struct { child: u32, minimum: usize, maximum: ?usize };
const Node = union(enum) {
    empty,
    literal: u21,
    any,
    class: u32,
    assertion: Assertion,
    group: u32,
    sequence: Pair,
    alternate: Pair,
    repeat: Repeat,
};
const State = union(enum) {
    accept,
    split: Pair,
    literal: struct { cp: u21, next: u32 },
    any: u32,
    class: struct { index: u32, next: u32 },
    assertion: struct { kind: Assertion, next: u32 },
};
const Work = struct {
    limit: usize,
    control: ?Control,
    steps: usize = 0,
    fn tick(self: *Work) !void {
        if (self.steps >= self.limit) return error.ExtractionRegexLimitExceeded;
        self.steps += 1;
        if (self.steps % 64 == 1) if (self.control) |control| try control.check();
    }
};

/// All program data is owned. A compiled program is immutable and shareable;
/// simulation scratch and control are supplied independently for each match.
pub const Program = struct {
    arena: std.heap.ArenaAllocator,
    pattern: []const u8,
    raw_flags: u32,
    cache_key: u64,
    flags: Flags,
    states: []const State,
    classes: []const Class,
    start: u32,
    compile_steps: usize,
    pub fn deinit(self: *Program) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn run(self: *const Program, allocator: Allocator, text: []const u8, mode: Mode, options: MatchOptions) !MatchResult {
        if (options.control) |control| try control.check();
        if (text.len > options.max_text_bytes) return error.ExtractionRegexLimitExceeded;
        if (options.max_steps == 0) return error.ExtractionRegexLimitExceeded;
        const view = std.unicode.Utf8View.init(text) catch return error.InvalidUtf8;
        if (self.states.len == 0 or self.start >= self.states.len) return error.InvalidExtractionRegexProgram;
        const visited = try allocator.alloc(u32, self.states.len);
        defer allocator.free(visited);
        const pending = try allocator.alloc(u32, self.states.len);
        defer allocator.free(pending);
        const active = try allocator.alloc(u32, self.states.len);
        defer allocator.free(active);
        var work = Work{ .limit = options.max_steps, .control = options.control };
        var iterator = view.iterator();
        var previous: ?u21 = null;
        var position: usize = 0;
        var current = iterator.nextCodepoint();
        var pending_count: usize = 0;
        // Generation stamps avoid clearing every compiled state for every
        // input codepoint when only a few states are active.
        @memset(visited, 0);
        var generation: u32 = 1;
        try enqueue(self.start, pending, &pending_count, visited, generation);
        while (true) {
            try work.tick();
            if (mode == .search) try enqueue(self.start, pending, &pending_count, visited, generation);
            var active_count: usize = 0;
            while (pending_count > 0) {
                try work.tick();
                pending_count -= 1;
                const index = pending[pending_count];
                switch (self.states[index]) {
                    .accept => if (mode != .fullmatch or current == null) {
                        if (options.control) |control| try control.check();
                        return .{ .matched = true, .steps = work.steps };
                    },
                    .split => |pair| {
                        try enqueue(pair.left, pending, &pending_count, visited, generation);
                        try enqueue(pair.right, pending, &pending_count, visited, generation);
                    },
                    .assertion => |assertion| {
                        if (self.asserts(assertion.kind, previous, current, position, iterator.i, text.len))
                            try enqueue(assertion.next, pending, &pending_count, visited, generation);
                    },
                    else => {
                        if (active_count >= active.len) return error.InvalidExtractionRegexProgram;
                        active[active_count] = index;
                        active_count += 1;
                    },
                }
            }
            const cp = current orelse break;
            generation +%= 1;
            if (generation == 0) {
                @memset(visited, 0);
                generation = 1;
            }
            for (active[0..active_count]) |index| {
                try work.tick();
                const next: ?u32 = switch (self.states[index]) {
                    .literal => |literal| if (self.equal(literal.cp, cp)) literal.next else null,
                    .any => |target| if (self.flags.dot_all or cp != '\n') target else null,
                    .class => |class| if (try self.classMatches(class.index, cp, &work)) class.next else null,
                    else => return error.InvalidExtractionRegexProgram,
                };
                if (next) |target| try enqueue(target, pending, &pending_count, visited, generation);
            }
            if (pending_count == 0 and mode != .search) break;
            previous = cp;
            position = iterator.i;
            current = iterator.nextCodepoint();
        }
        if (options.control) |control| try control.check();
        return .{ .matched = false, .steps = work.steps };
    }
    fn equal(self: *const Program, expected: u21, actual: u21) bool {
        if (!self.flags.ignore_case) return expected == actual;
        if (self.flags.ascii) return asciiLower(expected) == asciiLower(actual);
        return literals.regexCaseEquivalent(expected, actual);
    }
    fn word(self: *const Program, cp: ?u21) bool {
        const value = cp orelse return false;
        return if (self.flags.ascii) asciiWord(value) else unicode.isWord(value);
    }
    fn asserts(self: *const Program, kind: Assertion, previous: ?u21, current: ?u21, position: usize, next_position: usize, length: usize) bool {
        return switch (kind) {
            .start => position == 0 or (self.flags.multiline and previous == '\n'),
            .end => current == null or (current == '\n' and (self.flags.multiline or next_position == length)),
            .absolute_start => position == 0,
            .absolute_end => current == null,
            .word_boundary => self.word(previous) != self.word(current),
            // Python 3.12 does not match \B against the empty string.
            .not_word_boundary => length != 0 and self.word(previous) == self.word(current),
        };
    }
    fn classMatches(self: *const Program, index: u32, cp: u21, work: *Work) !bool {
        if (index >= self.classes.len) return error.InvalidExtractionRegexProgram;
        const class = self.classes[index];
        var matched = try contains(class.ranges, cp, work);
        var properties = class.properties.iterator(.{});
        while (!matched) {
            const raw = properties.next() orelse break;
            try work.tick();
            const property: Property = @enumFromInt(raw);
            const digit = if (self.flags.ascii) cp >= '0' and cp <= '9' else unicodeDigit(cp);
            matched = switch (property) {
                .digit => digit,
                .not_digit => !digit,
                .word => self.word(cp),
                .not_word => !self.word(cp),
                .space => if (self.flags.ascii) asciiSpace(cp) else unicode.isWhitespace(cp),
                .not_space => !(if (self.flags.ascii) asciiSpace(cp) else unicode.isWhitespace(cp)),
            };
        }
        return matched != class.negated;
    }
};
fn enqueue(index: u32, queue: []u32, count: *usize, visited: []u32, generation: u32) !void {
    if (index >= visited.len) return error.InvalidExtractionRegexProgram;
    if (visited[index] == generation) return;
    if (count.* >= queue.len) return error.InvalidExtractionRegexProgram;
    visited[index] = generation;
    queue[count.*] = index;
    count.* += 1;
}
fn asciiLower(cp: u21) u21 {
    return if (cp >= 'A' and cp <= 'Z') cp + ('a' - 'A') else cp;
}
fn asciiWord(cp: u21) bool {
    return (cp >= 'a' and cp <= 'z') or (cp >= 'A' and cp <= 'Z') or (cp >= '0' and cp <= '9') or cp == '_';
}
fn asciiSpace(cp: u21) bool {
    return cp == ' ' or (cp >= '\t' and cp <= '\r');
}
// Unicode 15.0 Nd blocks. Five adjacent mathematical digit alphabets form the
// one fifty-codepoint block; all other blocks contain ten decimal digits.
const decimal_starts = [_]u21{
    0x30,    0x660,   0x6f0,   0x7c0,   0x966,   0x9e6,   0xa66,   0xae6,   0xb66,   0xbe6,
    0xc66,   0xce6,   0xd66,   0xde6,   0xe50,   0xed0,   0xf20,   0x1040,  0x1090,  0x17e0,
    0x1810,  0x1946,  0x19d0,  0x1a80,  0x1a90,  0x1b50,  0x1bb0,  0x1c40,  0x1c50,  0xa620,
    0xa8d0,  0xa900,  0xa9d0,  0xa9f0,  0xaa50,  0xabf0,  0xff10,  0x104a0, 0x10d30, 0x11066,
    0x110f0, 0x11136, 0x111d0, 0x112f0, 0x11450, 0x114d0, 0x11650, 0x116c0, 0x11730, 0x118e0,
    0x11950, 0x11c50, 0x11d50, 0x11da0, 0x11f50, 0x16a60, 0x16ac0, 0x16b50, 0x1d7ce, 0x1e140,
    0x1e2f0, 0x1e4f0, 0x1e950, 0x1fbf0,
};
fn unicodeDigit(cp: u21) bool {
    var lo: usize = 0;
    var hi = decimal_starts.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (decimal_starts[mid] <= cp) lo = mid + 1 else hi = mid;
    }
    if (lo == 0) return false;
    const start = decimal_starts[lo - 1];
    return cp - start < @as(u21, if (start == 0x1d7ce) 50 else 10);
}
fn contains(ranges: []const Range, cp: u21, work: *Work) !bool {
    var lo: usize = 0;
    var hi = ranges.len;
    while (lo < hi) {
        try work.tick();
        const mid = lo + (hi - lo) / 2;
        if (ranges[mid].last < cp) lo = mid + 1 else hi = mid;
    }
    return lo < ranges.len and ranges[lo].first <= cp;
}
fn normalize(ranges: *std.ArrayListUnmanaged(Range)) void {
    const Order = struct {
        fn less(_: void, a: Range, b: Range) bool {
            return if (a.first != b.first) a.first < b.first else a.last < b.last;
        }
    };
    std.mem.sort(Range, ranges.items, {}, Order.less);
    var n: usize = 0;
    for (ranges.items) |range| {
        if (n > 0 and @as(u32, range.first) <= @as(u32, ranges.items[n - 1].last) + 1) {
            ranges.items[n - 1].last = @max(ranges.items[n - 1].last, range.last);
        } else {
            ranges.items[n] = range;
            n += 1;
        }
    }
    ranges.items.len = n;
}

pub fn compile(allocator: Allocator, pattern: []const u8, raw_flags: u32, options: CompileOptions) !Program {
    if (options.control) |control| try control.check();
    if (pattern.len > options.max_pattern_bytes) return error.ExtractionRegexLimitExceeded;
    if (options.max_states == 0 or options.max_states > 65536 or options.max_ast_nodes == 0 or options.max_ast_nodes > 65536 or options.max_depth == 0 or options.max_depth > 128 or options.max_repeat > 65536)
        return error.InvalidExtractionRegexLimits;
    if (!std.unicode.utf8ValidateSlice(pattern)) return error.InvalidExtractionRegex;
    const flags = try Flags.parse(raw_flags);
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const copy = try owned.dupe(u8, pattern);
    var work = Work{ .limit = options.max_steps, .control = options.control };
    var parser = Parser{ .allocator = owned, .pattern = copy, .flags = flags, .options = options, .work = &work };
    const root = try parser.expression(0);
    try parser.skip();
    if (parser.position != pattern.len) return error.InvalidExtractionRegex;
    var builder = Builder{ .allocator = owned, .nodes = parser.nodes.items, .options = options, .work = &work };
    const end = try builder.emit(.accept);
    const start = try builder.build(root, end, 0);
    if (options.control) |control| try control.check();
    return .{ .arena = arena, .pattern = copy, .raw_flags = raw_flags, .cache_key = std.hash.Wyhash.hash(raw_flags, copy), .flags = flags, .states = try builder.states.toOwnedSlice(owned), .classes = try parser.classes.toOwnedSlice(owned), .start = start, .compile_steps = work.steps };
}

const Parser = struct {
    allocator: Allocator,
    pattern: []const u8,
    flags: Flags,
    options: CompileOptions,
    work: *Work,
    position: usize = 0,
    nodes: std.ArrayListUnmanaged(Node) = .empty,
    classes: std.ArrayListUnmanaged(Class) = .empty,
    ranges_charged: usize = 0,
    fn node(self: *Parser, value: Node) !u32 {
        try self.work.tick();
        if (self.nodes.items.len >= self.options.max_ast_nodes) return error.ExtractionRegexLimitExceeded;
        const index: u32 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.allocator, value);
        return index;
    }
    fn peek(self: *const Parser) ?u8 {
        return if (self.position < self.pattern.len) self.pattern[self.position] else null;
    }
    fn rune(self: *Parser) !u21 {
        try self.work.tick();
        if (self.position >= self.pattern.len) return error.InvalidExtractionRegex;
        const length = std.unicode.utf8ByteSequenceLength(self.pattern[self.position]) catch return error.InvalidExtractionRegex;
        if (length > self.pattern.len - self.position) return error.InvalidExtractionRegex;
        const cp = std.unicode.utf8Decode(self.pattern[self.position..][0..length]) catch return error.InvalidExtractionRegex;
        self.position += length;
        return cp;
    }
    fn skip(self: *Parser) !void {
        if (!self.flags.verbose) return;
        while (self.peek()) |byte| {
            if (asciiSpace(byte)) {
                try self.work.tick();
                self.position += 1;
            } else if (byte == '#') {
                while (self.peek()) |comment_byte| {
                    try self.work.tick();
                    self.position += 1;
                    if (comment_byte == '\n') break;
                }
            } else break;
        }
    }
    fn expression(self: *Parser, depth: usize) anyerror!u32 {
        if (depth >= self.options.max_depth) return error.ExtractionRegexLimitExceeded;
        var left = try self.sequence(depth);
        while (true) {
            try self.skip();
            if (self.peek() != '|') break;
            self.position += 1;
            left = try self.node(.{ .alternate = .{ .left = left, .right = try self.sequence(depth) } });
        }
        return left;
    }
    fn sequence(self: *Parser, depth: usize) anyerror!u32 {
        var left: ?u32 = null;
        while (true) {
            try self.skip();
            const byte = self.peek() orelse break;
            if (byte == '|' or byte == ')') break;
            const atom_index = try self.atom(depth);
            const repeated = try self.repetition(atom_index);
            left = if (left) |previous| try self.node(.{ .sequence = .{ .left = previous, .right = repeated } }) else repeated;
        }
        return left orelse try self.node(.empty);
    }
    fn atom(self: *Parser, depth: usize) anyerror!u32 {
        const cp = try self.rune();
        return switch (cp) {
            '.' => self.node(.any),
            '^' => self.node(.{ .assertion = .start }),
            '$' => self.node(.{ .assertion = .end }),
            '[' => self.characterClass(),
            '\\' => switch (try self.escape(false)) {
                .literal => |value| self.node(.{ .literal = value }),
                .property => |value| self.propertyNode(value),
                .assertion => |value| self.node(.{ .assertion = value }),
            },
            '(' => blk: {
                if (self.peek() == '?') {
                    self.position += 1;
                    if (self.peek() != ':') return error.UnsupportedExtractionRegex;
                    self.position += 1;
                }
                const child = try self.expression(depth + 1);
                if (self.peek() != ')') return error.InvalidExtractionRegex;
                self.position += 1;
                // Python permits a grouped zero-width assertion to repeat,
                // while a directly quantified assertion is invalid syntax.
                break :blk try self.node(.{ .group = child });
            },
            '*', '+', '?' => error.InvalidExtractionRegex,
            '{' => blk: {
                self.position -= 1;
                const repeated = try self.braces();
                if (repeated != null) return error.InvalidExtractionRegex;
                self.position += 1;
                break :blk try self.node(.{ .literal = '{' });
            },
            else => self.node(.{ .literal = cp }),
        };
    }
    fn repetition(self: *Parser, child: u32) !u32 {
        try self.skip();
        const byte = self.peek() orelse return child;
        var repeat: ?Repeat = switch (byte) {
            '*' => .{ .child = child, .minimum = 0, .maximum = null },
            '+' => .{ .child = child, .minimum = 1, .maximum = null },
            '?' => .{ .child = child, .minimum = 0, .maximum = 1 },
            '{' => try self.braces(),
            else => null,
        };
        if (repeat == null) return child;
        if (byte != '{') self.position += 1;
        repeat.?.child = child;
        switch (self.nodes.items[child]) {
            .assertion => return error.InvalidExtractionRegex,
            else => {},
        }
        // Greedy/lazy choice changes captures, not boolean language. Possessive
        // quantifiers do change the language and are never approximated.
        if (self.peek() == '?') self.position += 1 else if (self.peek() == '+') return error.UnsupportedExtractionRegex;
        try self.skip();
        if (self.peek()) |next| {
            if (next == '*' or next == '+' or next == '?') return error.InvalidExtractionRegex;
            if (next == '{') {
                const saved = self.position;
                if (try self.braces() != null) return error.InvalidExtractionRegex;
                self.position = saved;
            }
        }
        return self.node(.{ .repeat = repeat.? });
    }
    fn decimal(self: *Parser) !?usize {
        var value: usize = 0;
        var any = false;
        while (self.peek()) |byte| {
            if (byte < '0' or byte > '9') break;
            try self.work.tick();
            any = true;
            value = std.math.mul(usize, value, 10) catch return error.ExtractionRegexLimitExceeded;
            value = std.math.add(usize, value, byte - '0') catch return error.ExtractionRegexLimitExceeded;
            if (value > self.options.max_repeat) return error.ExtractionRegexLimitExceeded;
            self.position += 1;
        }
        return if (any) value else null;
    }
    fn braces(self: *Parser) !?Repeat {
        const saved = self.position;
        // A brace sequence which is not syntactically a repetition is literal,
        // even if its decimal-looking text would exceed a repetition budget.
        var scan = saved + 1;
        while (scan < self.pattern.len and self.pattern[scan] >= '0' and self.pattern[scan] <= '9') : (scan += 1) {}
        if (scan < self.pattern.len and self.pattern[scan] == ',') {
            scan += 1;
            while (scan < self.pattern.len and self.pattern[scan] >= '0' and self.pattern[scan] <= '9') : (scan += 1) {}
        }
        if (scan >= self.pattern.len or self.pattern[scan] != '}') return null;
        self.position += 1;
        const minimum = try self.decimal();
        var maximum = minimum;
        var comma = false;
        if (self.peek() == ',') {
            comma = true;
            self.position += 1;
            maximum = try self.decimal();
        }
        if (self.peek() != '}' or (!comma and minimum == null)) {
            self.position = saved;
            return null;
        }
        self.position += 1;
        if (maximum != null and maximum.? < (minimum orelse 0)) return error.InvalidExtractionRegex;
        return .{ .child = 0, .minimum = minimum orelse 0, .maximum = maximum };
    }
    const Escape = union(enum) { literal: u21, property: Property, assertion: Assertion };
    fn escape(self: *Parser, in_class: bool) !Escape {
        const cp = try self.rune();
        if (cp >= '0' and cp <= '9') {
            var value: u21 = cp - '0';
            var count: usize = 1;
            // Octal needs three digits except the explicit leading-zero form.
            const remaining = self.pattern[self.position..];
            const octal = cp <= '7' and (in_class or cp == '0' or (remaining.len >= 2 and remaining[0] >= '0' and remaining[0] <= '7' and remaining[1] >= '0' and remaining[1] <= '7'));
            if (!octal) return if (in_class) error.InvalidExtractionRegex else error.UnsupportedExtractionRegex;
            while (count < 3) : (count += 1) {
                const byte = self.peek() orelse break;
                if (byte < '0' or byte > '7') break;
                self.position += 1;
                value = value * 8 + byte - '0';
            }
            if (value > 0xff) return error.InvalidExtractionRegex;
            return .{ .literal = value };
        }
        return switch (cp) {
            'a' => .{ .literal = 7 },
            'b' => if (in_class) .{ .literal = 8 } else .{ .assertion = .word_boundary },
            'f' => .{ .literal = '\x0c' },
            'n' => .{ .literal = '\n' },
            'r' => .{ .literal = '\r' },
            't' => .{ .literal = '\t' },
            'v' => .{ .literal = '\x0b' },
            'd' => .{ .property = .digit },
            'D' => .{ .property = .not_digit },
            'w' => .{ .property = .word },
            'W' => .{ .property = .not_word },
            's' => .{ .property = .space },
            'S' => .{ .property = .not_space },
            'A' => if (in_class) error.InvalidExtractionRegex else .{ .assertion = .absolute_start },
            'Z' => if (in_class) error.InvalidExtractionRegex else .{ .assertion = .absolute_end },
            'B' => if (in_class) error.InvalidExtractionRegex else .{ .assertion = .not_word_boundary },
            'x' => .{ .literal = try self.hex(2) },
            'u' => .{ .literal = try self.hex(4) },
            'U' => .{ .literal = try self.hex(8) },
            'N' => error.UnsupportedExtractionRegex,
            else => if ((cp >= 'a' and cp <= 'z') or (cp >= 'A' and cp <= 'Z')) error.InvalidExtractionRegex else .{ .literal = cp },
        };
    }
    fn hex(self: *Parser, count: usize) !u21 {
        var value: u32 = 0;
        for (0..count) |_| {
            const cp = try self.rune();
            const digit: u32 = if (cp >= '0' and cp <= '9') cp - '0' else if (cp >= 'a' and cp <= 'f') cp - 'a' + 10 else if (cp >= 'A' and cp <= 'F') cp - 'A' + 10 else return error.InvalidExtractionRegex;
            value = std.math.mul(u32, value, 16) catch return error.InvalidExtractionRegex;
            value = std.math.add(u32, value, digit) catch return error.InvalidExtractionRegex;
        }
        if (value > 0x10ffff) return error.InvalidExtractionRegex;
        return @intCast(value);
    }
    fn propertyNode(self: *Parser, property: Property) !u32 {
        var properties = Properties.initEmpty();
        properties.set(@intFromEnum(property));
        const index: u32 = @intCast(self.classes.items.len);
        try self.classes.append(self.allocator, .{ .ranges = &.{}, .properties = properties, .negated = false });
        return self.node(.{ .class = index });
    }
    fn classUnit(self: *Parser) !Escape {
        const cp = try self.rune();
        return if (cp == '\\') self.escape(true) else .{ .literal = cp };
    }
    fn addRange(self: *Parser, ranges: *std.ArrayListUnmanaged(Range), first: u21, last: u21) !void {
        try self.work.tick();
        if (self.ranges_charged >= self.options.max_class_ranges) return error.ExtractionRegexLimitExceeded;
        self.ranges_charged += 1;
        try ranges.append(self.allocator, .{ .first = first, .last = last });
    }
    fn characterClass(self: *Parser) !u32 {
        var ranges = std.ArrayListUnmanaged(Range).empty;
        var properties = Properties.initEmpty();
        const negated = self.peek() == '^';
        if (negated) self.position += 1;
        var first = true;
        while (true) {
            const byte = self.peek() orelse return error.InvalidExtractionRegex;
            if (byte == ']' and !first) {
                self.position += 1;
                break;
            }
            const left = try self.classUnit();
            first = false;
            if (self.peek() == '-' and self.position + 1 < self.pattern.len and self.pattern[self.position + 1] != ']') {
                self.position += 1;
                const right = try self.classUnit();
                if (left != .literal or right != .literal or right.literal < left.literal) return error.InvalidExtractionRegex;
                try self.addRange(&ranges, left.literal, right.literal);
            } else switch (left) {
                .literal => |cp| try self.addRange(&ranges, cp, cp),
                .property => |property| properties.set(@intFromEnum(property)),
                .assertion => return error.InvalidExtractionRegex,
            }
        }
        normalize(&ranges);
        if (self.flags.ignore_case and ranges.items.len > 0) {
            // Compute equivalence-class closure once at compilation. Runtime
            // membership remains a bounded binary search over merged ranges.
            const original = try self.allocator.dupe(Range, ranges.items);
            if (self.flags.ascii) {
                for ('A'..'Z' + 1) |letter| {
                    const upper: u21 = @intCast(letter);
                    const lower = asciiLower(upper);
                    if (try contains(original, upper, self.work) or try contains(original, lower, self.work)) {
                        try self.addRange(&ranges, upper, upper);
                        try self.addRange(&ranges, lower, lower);
                    }
                }
            } else {
                var selected = std.AutoHashMapUnmanaged(u21, void).empty;
                for (case_data.pairs) |pair| {
                    try self.work.tick();
                    if (try contains(original, pair.cp, self.work) or try contains(original, pair.representative, self.work))
                        try selected.put(self.allocator, pair.representative, {});
                }
                for (case_data.pairs) |pair| {
                    try self.work.tick();
                    if (selected.contains(pair.representative)) {
                        try self.addRange(&ranges, pair.cp, pair.cp);
                        try self.addRange(&ranges, pair.representative, pair.representative);
                    }
                }
            }
            normalize(&ranges);
        }
        const index: u32 = @intCast(self.classes.items.len);
        try self.classes.append(self.allocator, .{ .ranges = try ranges.toOwnedSlice(self.allocator), .properties = properties, .negated = negated });
        return self.node(.{ .class = index });
    }
};

const Builder = struct {
    allocator: Allocator,
    nodes: []const Node,
    options: CompileOptions,
    work: *Work,
    states: std.ArrayListUnmanaged(State) = .empty,
    fn emit(self: *Builder, state: State) !u32 {
        try self.work.tick();
        if (self.states.items.len >= self.options.max_states) return error.ExtractionRegexLimitExceeded;
        const index: u32 = @intCast(self.states.items.len);
        try self.states.append(self.allocator, state);
        return index;
    }
    fn build(self: *Builder, index: u32, next: u32, depth: usize) anyerror!u32 {
        try self.work.tick();
        // Sequences form a left-linked AST. Iteration keeps long literal
        // patterns off the native call stack; only grouped alternatives and
        // nested repetitions consume the bounded recursive depth.
        if (depth >= self.options.max_depth) return error.ExtractionRegexLimitExceeded;
        var node_index = index;
        var continuation = next;
        while (self.nodes[node_index] == .sequence) {
            const pair = self.nodes[node_index].sequence;
            continuation = try self.build(pair.right, continuation, depth + 1);
            node_index = pair.left;
        }
        return switch (self.nodes[node_index]) {
            .empty => continuation,
            .literal => |cp| self.emit(.{ .literal = .{ .cp = cp, .next = continuation } }),
            .any => self.emit(.{ .any = continuation }),
            .class => |class| self.emit(.{ .class = .{ .index = class, .next = continuation } }),
            .assertion => |kind| self.emit(.{ .assertion = .{ .kind = kind, .next = continuation } }),
            .group => |child| self.build(child, continuation, depth + 1),
            .sequence => unreachable,
            .alternate => |pair| self.emit(.{ .split = .{ .left = try self.build(pair.left, continuation, depth + 1), .right = try self.build(pair.right, continuation, depth + 1) } }),
            .repeat => |repeat| blk: {
                var target = continuation;
                if (repeat.maximum) |maximum| {
                    for (0..maximum - repeat.minimum) |_| {
                        const child = try self.build(repeat.child, target, depth + 1);
                        target = try self.emit(.{ .split = .{ .left = child, .right = target } });
                    }
                } else {
                    const split = try self.emit(.{ .split = .{ .left = 0, .right = target } });
                    const child = try self.build(repeat.child, split, depth + 1);
                    self.states.items[split].split.left = child;
                    target = split;
                }
                for (0..repeat.minimum) |_| target = try self.build(repeat.child, target, depth + 1);
                break :blk target;
            },
        };
    }
};

pub const ContextOptions = struct {
    compile_options: CompileOptions = .{},
    match_options: MatchOptions = .{},
    max_patterns: usize = 128,
    max_total_pattern_bytes: usize = 64 * 1024,
    max_total_states: usize = 32768,
    max_total_compile_steps: usize = 8000000,
    max_total_match_steps: usize = 40000000,
};
/// Request-local cache and work budget. Keep this value at a stable address
/// after assigning its callbacks to schema compiler and pipeline options.
pub const Context = struct {
    allocator: Allocator,
    options: ContextOptions,
    programs: std.ArrayListUnmanaged(Program) = .empty,
    pattern_bytes: usize = 0,
    state_count: usize = 0,
    compile_steps: usize = 0,
    match_steps: usize = 0,
    pub fn init(allocator: Allocator, options: ContextOptions) Context {
        return .{ .allocator = allocator, .options = options };
    }
    pub fn deinit(self: *Context) void {
        for (self.programs.items) |*program| program.deinit();
        self.programs.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn compilerOptions(self: *Context, base: schema.Options) schema.Options {
        var options = base;
        options.regex_context = self;
        options.validate_regex_fn = validateCompile;
        return options;
    }
    fn find(self: *Context, validator: schema.RegexValidator) !?*Program {
        if (validator.pattern.len > self.options.compile_options.max_pattern_bytes) return error.ExtractionRegexLimitExceeded;
        if (self.options.match_options.control orelse self.options.compile_options.control) |control| try control.check();
        const key = std.hash.Wyhash.hash(validator.flags, validator.pattern);
        for (self.programs.items) |*program| {
            if (self.options.match_options.control orelse self.options.compile_options.control) |control| try control.check();
            if (program.cache_key == key and program.raw_flags == validator.flags and std.mem.eql(u8, program.pattern, validator.pattern)) return program;
        }
        return null;
    }
    pub fn validateCompile(raw: ?*anyopaque, validator: schema.RegexValidator) anyerror!void {
        const self: *Context = @ptrCast(@alignCast(raw orelse return error.MissingExtractionValidator));
        if (try self.find(validator) != null) return;
        if (self.programs.items.len >= self.options.max_patterns or validator.pattern.len > self.options.max_total_pattern_bytes -| self.pattern_bytes or self.state_count >= self.options.max_total_states)
            return error.ExtractionRegexLimitExceeded;
        var options = self.options.compile_options;
        options.max_states = @min(options.max_states, self.options.max_total_states - self.state_count);
        options.max_steps = @min(options.max_steps, self.options.max_total_compile_steps -| self.compile_steps);
        var program = compile(self.allocator, validator.pattern, validator.flags, options) catch |err| {
            if (err == error.ExtractionRegexLimitExceeded) self.compile_steps += options.max_steps;
            return err;
        };
        errdefer program.deinit();
        try self.programs.append(self.allocator, program);
        self.pattern_bytes += validator.pattern.len;
        self.state_count += program.states.len;
        self.compile_steps += program.compile_steps;
    }
    pub fn validateValue(raw: ?*anyopaque, validator: schema.RegexValidator, text: []const u8) anyerror!bool {
        const self: *Context = @ptrCast(@alignCast(raw orelse return error.MissingExtractionValidator));
        const program = (try self.find(validator)) orelse return error.MissingExtractionValidator;
        var options = self.options.match_options;
        options.max_steps = @min(options.max_steps, self.options.max_total_match_steps -| self.match_steps);
        const result = program.run(self.allocator, text, if (validator.mode == .full) .fullmatch else .search, options) catch |err| {
            // An exhausted attempt consumes its entire admitted work budget.
            if (err == error.ExtractionRegexLimitExceeded) self.match_steps += options.max_steps;
            return err;
        };
        self.match_steps += result.steps;
        return result.matched != validator.exclude;
    }
};

test "extraction regex matches pinned Python fullmatch search and match oracle" {
    const Fixture = struct {
        format_version: u32,
        provenance: std.json.Value,
        decimal_ranges: []const Range,
        cases: []const struct {
            pattern: []const u8,
            flags: u32,
            texts: []const struct { text: []const u8, fullmatch: bool, search: bool, match: bool },
        },
    };
    const allocator = std.testing.allocator;
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(allocator, "regex.json");
    defer allocator.free(bytes);
    var fixture = try std.json.parseFromSlice(Fixture, allocator, bytes, .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(u32, 1), fixture.value.format_version);
    try std.testing.expectEqualStrings("15.0.0", fixture.value.provenance.object.get("unicode").?.string);
    for (fixture.value.cases) |case| {
        var program = try compile(allocator, case.pattern, case.flags, .{});
        defer program.deinit();
        for (case.texts) |expected| {
            inline for (.{ Mode.fullmatch, Mode.search, Mode.match }) |mode| {
                const got = try program.run(allocator, expected.text, mode, .{});
                if (got.matched != @field(expected, @tagName(mode))) {
                    std.debug.print("regex mismatch pattern={f} flags={d} mode={s} text={f}\n", .{ std.json.fmt(case.pattern, .{}), case.flags, @tagName(mode), std.json.fmt(expected.text, .{}) });
                    return error.TestExpectedEqual;
                }
            }
        }
    }
    var range_index: usize = 0;
    for (0..0x110000) |raw| {
        const cp: u21 = @intCast(raw);
        while (range_index < fixture.value.decimal_ranges.len and fixture.value.decimal_ranges[range_index].last < cp) range_index += 1;
        const expected = range_index < fixture.value.decimal_ranges.len and fixture.value.decimal_ranges[range_index].first <= cp;
        if (unicodeDigit(cp) != expected) {
            std.debug.print("Unicode Nd mismatch U+{x}\n", .{cp});
            return error.TestExpectedEqual;
        }
    }
}

test "extraction regex unsupported syntax is rejected instead of approximated" {
    const allocator = std.testing.allocator;
    for ([_][]const u8{ "(a)\\1", "(?=a)a", "(?!b)a", "(?<=x)a", "(?<!x)a", "(?>a|ab)b", "a*+a", "a++a", "a?+a", "a{1,2}+a", "(?i)a", "(?i:a)", "(?P<name>a)", "(?#comment)a", "\\N{LATIN CAPITAL LETTER A}", "(a)(?(1)b|c)" }) |pattern|
        try std.testing.expectError(error.UnsupportedExtractionRegex, compile(allocator, pattern, 2, .{}));
    for ([_]u32{ 1, 4, 128, 512, std.math.maxInt(u32) }) |flags|
        try std.testing.expectError(error.UnsupportedExtractionRegexFlags, compile(allocator, "a", flags, .{}));
    try std.testing.expectError(error.InvalidExtractionRegex, compile(allocator, "a", 32 | 256, .{}));
    for ([_][]const u8{ "[", "[]", "(", ")", "*a", "a**", "[z-a]", "[\\d-a]", "a{2,1}", "\\", "\\p", "\\x0", "\\U00110000", "\\400", "a$+" }) |pattern|
        try std.testing.expectError(error.InvalidExtractionRegex, compile(allocator, pattern, 2, .{}));
}

test "extraction regex compilation and nonlinear looking patterns have hard bounds" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, compile(allocator, "abc", 0, .{ .max_pattern_bytes = 2 }));
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, compile(allocator, "a{1025}", 0, .{}));
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, compile(allocator, "(a{1024}){1024}", 0, .{ .max_states = 100 }));
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, compile(allocator, "((((a))))", 0, .{ .max_depth = 4 }));
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, compile(allocator, "[A-Z]", 2, .{ .max_class_ranges = 1 }));
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, compile(allocator, "a", 2, .{ .max_steps = 0 }));
    var program = try compile(allocator, "(a+)+b", 0, .{});
    defer program.deinit();
    const text = [_]u8{'a'} ** 4096;
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, program.run(allocator, &text, .search, .{ .max_steps = 32 }));
    try std.testing.expect(!(try program.run(allocator, &text, .search, .{ .max_steps = 100000 })).matched);
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, program.run(allocator, "long", .search, .{ .max_text_bytes = 1 }));
    try std.testing.expectError(error.InvalidUtf8, program.run(allocator, "\xff", .search, .{}));
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, program.run(failing.allocator(), "text", .search, .{ .max_steps = 0 }));
    try std.testing.expect(!failing.has_induced_failure);
}

test "extraction regex cancellation propagates from compile and active simulation" {
    const Cancel = struct {
        fn immediately(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
        fn later(raw: ?*anyopaque) anyerror!void {
            const count: *usize = @ptrCast(@alignCast(raw.?));
            count.* += 1;
            if (count.* >= 3) return error.Cancelled;
        }
    };
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.Cancelled, compile(allocator, "a", 0, .{ .control = .{ .check_fn = Cancel.immediately } }));
    var program = try compile(allocator, "a+b", 0, .{});
    defer program.deinit();
    var checks: usize = 0;
    const text = [_]u8{'a'} ** 1024;
    try std.testing.expectError(error.Cancelled, program.run(allocator, &text, .search, .{ .control = .{ .ptr = &checks, .check_fn = Cancel.later } }));
    try std.testing.expectEqual(@as(usize, 3), checks);
}

test "extraction regex context compiles once and preserves full partial exclude semantics" {
    const allocator = std.testing.allocator;
    var context = Context.init(allocator, .{});
    defer context.deinit();
    var compiled = try schema.compile(allocator,
        \\{"entities":["word"],"entity_definitions":{"word":{"validators":[{"pattern":"[A-Z]+"},{"pattern":"[A-Z]+","mode":"partial","exclude":true}]}}}
    , context.compilerOptions(.{}));
    defer compiled.deinit();
    try std.testing.expectEqual(@as(usize, 1), context.programs.items.len);
    const validators = compiled.schema.entities[0].validators;
    try std.testing.expect(try Context.validateValue(&context, validators[0], "aBc"));
    try std.testing.expect(!try Context.validateValue(&context, validators[0], "-aBc-"));
    try std.testing.expect(!try Context.validateValue(&context, validators[1], "-aBc-"));
    try std.testing.expect(try Context.validateValue(&context, validators[1], "123"));
    context.options.max_total_match_steps = context.match_steps;
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, Context.validateValue(&context, validators[0], "abc"));
    try std.testing.expectError(error.MissingExtractionValidator, Context.validateValue(&context, .{ .pattern = "uncompiled" }, "abc"));
    context.options.max_total_compile_steps = context.compile_steps;
    try std.testing.expectError(error.ExtractionRegexLimitExceeded, Context.validateCompile(&context, .{ .pattern = "new" }));
}

fn allocationLifecycle(allocator: Allocator) !void {
    var context = Context.init(allocator, .{});
    defer context.deinit();
    const validator = schema.RegexValidator{ .pattern = "^(?:[A-Z]+|\\d{2,4})$" };
    try Context.validateCompile(&context, validator);
    try Context.validateCompile(&context, validator);
    try std.testing.expect(try Context.validateValue(&context, validator, "AbCd"));
    try std.testing.expect(!try Context.validateValue(&context, validator, "Ab 12"));
}
test "extraction regex compile and simulation release allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationLifecycle, .{});
}
