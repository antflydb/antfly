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

//! Immutable Unicode capture programs; caller-owned, budgeted execution.
//! Separate from the byte/FST interface. No C, libc, global state or host locale.
const std = @import("std");
const A = std.mem.Allocator;
pub const Span = struct { start: isize = -1, end: isize = -1 };
/// Actual-byte admission is provided by the caller's allocator; these bounds
/// independently cap parser depth and pattern work.
pub const Limits = struct { max_depth: usize = 128, pattern_bytes: usize = 16 * 1024 };
pub const Options = struct {
    syntax: enum { basic, extended, advanced, literal } = .advanced,
    case_insensitive: bool = false,
    expanded: bool = false,
    newline_sensitive: bool = false,
    line_anchors: bool = false,
    fn bits(self: Options) u32 {
        return @as(u32, switch (self.syntax) {
            .basic => 0,
            .extended => 1,
            .advanced => 3,
            .literal => 4,
        }) |
            (if (self.case_insensitive) @as(u32, 8) else 0) | (if (self.expanded) @as(u32, 32) else 0) |
            (if (self.newline_sensitive) @as(u32, 64) else 0) | (if (self.line_anchors) @as(u32, 128) else 0);
    }
};
pub const Budget = struct {
    remaining: usize = 8 * 1024 * 1024,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    checkpoint_interval: usize = 1,
    until_checkpoint: usize = 0,
    ptr: ?*anyopaque = null,
    failure: ?anyerror = null,
    /// Embedders may preserve their quota vocabulary without changing the engine.
    limit_error: anyerror = error.RegexLimitExceeded,
    pub fn charge(self: *Budget, amount: usize) !void {
        if (!self.consumeWork(amount)) return self.failure.?;
    }
    pub fn consume(self: *Budget) bool {
        return self.consumeWork(1);
    }
    pub fn consumeWork(self: *Budget, amount: usize) bool {
        if (self.failure != null) return false;
        if (amount > self.remaining) {
            self.remaining = 0;
            self.failure = self.limit_error;
            return false;
        }
        self.remaining -= amount;
        if (self.checkpoint) |check| {
            if (amount >= self.until_checkpoint) {
                check(self.ptr) catch |err| {
                    self.failure = err;
                    return false;
                };
                self.until_checkpoint = self.checkpoint_interval -| 1;
            } else self.until_checkpoint -= amount;
        }
        return true;
    }
};
const invalid = error.InvalidPattern;
const none = std.math.maxInt(u32);
const Range = struct { first: u32, last: u32 };
const Tag = enum { empty, literal, any, class, concat, alternate, repeat, capture, start, end, absolute_start, absolute_end, word_start, word_end, word_boundary, not_word_boundary, backref, ahead, not_ahead, behind, not_behind };
const Node = struct { tag: Tag, left: u32 = 0, right: u32 = 0, value: u32 = 0, minimum: u32 = 0, maximum: u32 = 0, flags: u32 = 0, short: ?bool = null, negated: bool = false, captures: bool = false, backrefs: bool = false, suffix: usize = 0, suffix_count: usize = 0, priority: u32 = none, min_width: usize = 0, max_width: usize = 0 };
const Op = enum { accept, literal, any, class, split, save_start, save_end, assertion, backref, look, progress, priority_start, priority_end, regular_capture, iteration_start, iteration_end };
const Instruction = struct { op: Op, next: u32 = 0, other: u32 = 0, node: u32 = 0, value: u32 = 0 };

fn setWidth(node: *Node, nodes: []const Node) void {
    switch (node.tag) {
        .literal, .class, .any => {
            node.min_width = 1;
            node.max_width = 1;
        },
        .capture => {
            node.min_width = nodes[node.left].min_width;
            node.max_width = nodes[node.left].max_width;
        },
        .concat => {
            node.min_width = nodes[node.left].min_width +| nodes[node.right].min_width;
            node.max_width = nodes[node.left].max_width +| nodes[node.right].max_width;
        },
        .alternate => {
            node.min_width = @min(nodes[node.left].min_width, nodes[node.right].min_width);
            node.max_width = @max(nodes[node.left].max_width, nodes[node.right].max_width);
        },
        .repeat => {
            node.min_width = nodes[node.left].min_width *| node.minimum;
            node.max_width = if (node.maximum == none and nodes[node.left].max_width != 0) std.math.maxInt(usize) else nodes[node.left].max_width *| node.maximum;
        },
        .backref => node.max_width = std.math.maxInt(usize),
        else => {},
    }
}

/// Reverse adjacency is shared by every execution. Reverse reachability
/// evaluates the original edge predicates at their original subject positions.
const Reverse = struct {
    offsets: []u32,
    sources: []u32,
    fn init(a: A, instructions: []const Instruction, budget: *Budget) !Reverse {
        const offsets = try a.alloc(u32, instructions.len + 1);
        errdefer a.free(offsets);
        @memset(offsets, 0);
        for (instructions) |instruction| {
            try budget.charge(1);
            if (instruction.op == .accept) continue;
            offsets[instruction.next + 1] += 1;
            if (instruction.op == .split) offsets[instruction.other + 1] += 1;
        }
        for (1..offsets.len) |index| offsets[index] += offsets[index - 1];
        const sources = try a.alloc(u32, offsets[offsets.len - 1]);
        errdefer a.free(sources);
        const cursors = try a.dupe(u32, offsets);
        defer a.free(cursors);
        for (instructions, 0..) |instruction, index| {
            try budget.charge(1);
            if (instruction.op == .accept) continue;
            sources[cursors[instruction.next]] = @intCast(index);
            cursors[instruction.next] += 1;
            if (instruction.op == .split) {
                sources[cursors[instruction.other]] = @intCast(index);
                cursors[instruction.other] += 1;
            }
        }
        return .{ .offsets = offsets, .sources = sources };
    }
    fn deinit(self: *Reverse, a: A) void {
        a.free(self.offsets);
        a.free(self.sources);
    }
};
const Reachable = struct {
    base: usize,
    end: usize,
    bits: []u64 = &.{},
    inline_bits: u64 = 0,
    fn record(self: *Reachable, a: A, pos: usize, budget: *Budget) !void {
        const offset = pos - self.base;
        if (self.end - self.base < 64) {
            self.inline_bits |= @as(u64, 1) << @intCast(offset);
            return;
        }
        if (self.bits.len == 0) {
            self.bits = try a.alloc(u64, (self.end - self.base) / 64 + 1);
            @memset(self.bits, 0);
            try budget.charge(self.bits.len);
        }
        self.bits[offset / 64] |= @as(u64, 1) << @intCast(offset % 64);
    }
    fn contains(self: Reachable, pos: usize) bool {
        if (pos < self.base or pos > self.end) return false;
        const offset = pos - self.base;
        if (self.bits.len == 0) return offset < 64 and self.inline_bits & (@as(u64, 1) << @intCast(offset)) != 0;
        return self.bits[offset / 64] & (@as(u64, 1) << @intCast(offset % 64)) != 0;
    }
};

test "capture interface is standalone Unicode with typed options and generic errors" {
    const a = std.testing.allocator;
    var budget: Budget = .{};
    var program = try Program.compile(a, &.{ '(', 'a', '+', ')', '-', '\\', '1' }, .{}, .{}, &budget);
    defer program.deinit(a);
    var spans: [2]Span = undefined;
    try std.testing.expect(try program.find(a, &.{ 0x96ea, 'a', 'a', '-', 'a', 'a' }, 0, &spans, &budget));
    try std.testing.expectEqual(Span{ .start = 1, .end = 6 }, spans[0]);
    try std.testing.expectEqual(Span{ .start = 1, .end = 3 }, spans[1]);
    var literal = try Program.compile(a, &.{ 'a', '+' }, .{ .syntax = .literal, .case_insensitive = true }, .{}, &budget);
    defer literal.deinit(a);
    try std.testing.expect(try literal.find(a, &.{ 'A', '+' }, 0, &spans, &budget));
    try std.testing.expectEqual(Span{ .start = 0, .end = 2 }, spans[0]);
    try std.testing.expectError(error.InvalidPattern, Program.compile(a, &.{'['}, .{}, .{}, &budget));
    var refused: Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.RegexLimitExceeded, Program.compile(a, &.{'a'}, .{}, .{}, &refused));
    var bytes: [32]u8 = undefined;
    var memory = std.heap.FixedBufferAllocator.init(&bytes);
    var available: Budget = .{};
    try std.testing.expectError(error.OutOfMemory, Program.compile(memory.allocator(), &.{'a'}, .{}, .{}, &available));
}

pub const Program = struct {
    nodes: []Node,
    ranges: []Range,
    instructions: []Instruction,
    entry: u32,
    captures: usize,
    short: bool,
    backrefs: bool,
    loops: usize,
    preferences: []bool,
    depth_limit: usize,
    root: u32,
    entries: []u32,
    suffixes: []u32,
    priorities: []bool,
    reverse: Reverse,
    entry_widths: []usize,
    atomics: usize,
    priority_nodes: []u32,
    pub fn compile(a: A, pattern: []const u32, options: Options, limits: Limits, budget: *Budget) !Program {
        if (pattern.len > limits.pattern_bytes) return error.RegexLimitExceeded;
        var parser: Parser = .{ .a = a, .text = pattern, .flags = options.bits(), .budget = budget, .depth_limit = @min(128, limits.max_depth) };
        defer parser.nodes.deinit(a);
        defer parser.ranges.deinit(a);
        defer parser.preferences.deinit(a);
        const root = try parser.parse();
        for (parser.nodes.items) |*node| setWidth(node, parser.nodes.items);
        for (parser.nodes.items) |*node| node.captures = switch (node.tag) {
            .capture => true,
            .concat, .alternate => parser.nodes.items[node.left].captures or parser.nodes.items[node.right].captures,
            .repeat => parser.nodes.items[node.left].captures,
            else => false,
        };
        for (parser.nodes.items) |*node| node.backrefs = switch (node.tag) {
            .backref => true,
            .concat, .alternate => parser.nodes.items[node.left].backrefs or parser.nodes.items[node.right].backrefs,
            .capture, .repeat => parser.nodes.items[node.left].backrefs,
            else => false,
        };
        // Positive-minimum capture repetition binds only the final copy.
        // The capture-free prefix takes the quantifier's length preference.
        // This differs observably from iterating a zero-minimum capture group.
        {
            const count = parser.nodes.items.len;
            for (0..count) |index| {
                const node = parser.nodes.items[index];
                if (node.tag != .repeat or node.minimum == 0 or !node.captures or parser.nodes.items[node.left].backrefs) continue;
                var prefix = node;
                prefix.minimum -= 1;
                if (prefix.maximum != none) prefix.maximum -= 1;
                prefix.captures = false;
                setWidth(&prefix, parser.nodes.items);
                const left = try parser.add(prefix);
                parser.nodes.items[index] = .{ .tag = .concat, .left = left, .right = node.left, .short = node.short, .captures = true, .min_width = node.min_width, .max_width = node.max_width };
            }
        }
        var priorities: std.ArrayList(bool) = .empty;
        defer priorities.deinit(a);
        if (parser.backrefs) try assignPriorities(a, parser.nodes.items, root, &priorities, 0, parser.depth_limit, budget);
        const owned_priorities = try priorities.toOwnedSlice(a);
        errdefer a.free(owned_priorities);
        const priority_nodes = try a.alloc(u32, owned_priorities.len);
        errdefer a.free(priority_nodes);
        for (parser.nodes.items, 0..) |node, index| if (node.priority != none) {
            priority_nodes[node.priority] = @intCast(index);
        };
        var compiler: Compiler = .{ .a = a, .nodes = parser.nodes.items, .budget = budget, .depth_limit = parser.depth_limit, .atomic_mode = parser.backrefs };
        defer compiler.instructions.deinit(a);
        defer compiler.cache.deinit(a);
        defer compiler.suffixes.deinit(a);
        const entry = try compiler.lower(root, try compiler.emit(.{ .op = .accept }));
        compiler.atomic_mode = false;
        const entries = try a.alloc(u32, parser.nodes.items.len);
        errdefer a.free(entries);
        for (entries, 0..) |*target, index| target.* = try compiler.lower(@intCast(index), 0);
        const suffixes = try compiler.suffixes.toOwnedSlice(a);
        errdefer a.free(suffixes);
        const nodes = try parser.nodes.toOwnedSlice(a);
        errdefer a.free(nodes);
        const ranges = try parser.ranges.toOwnedSlice(a);
        errdefer a.free(ranges);
        const preferences = try parser.preferences.toOwnedSlice(a);
        errdefer a.free(preferences);
        const instructions = try compiler.instructions.toOwnedSlice(a);
        errdefer a.free(instructions);
        var reverse = try Reverse.init(a, instructions, budget);
        errdefer reverse.deinit(a);
        const entry_widths = try a.alloc(usize, instructions.len);
        errdefer a.free(entry_widths);
        @memset(entry_widths, std.math.maxInt(usize));
        for (nodes, entries) |node, target| {
            entry_widths[target] = node.max_width;
            if (node.tag == .repeat and node.suffix_count != 0) for (suffixes[node.suffix..][0..node.suffix_count], 0..) |suffix, count| {
                entry_widths[suffix] = if (node.maximum == none and nodes[node.left].max_width != 0) std.math.maxInt(usize) else nodes[node.left].max_width *| (@as(usize, node.maximum) -| count);
            };
        }
        return .{ .nodes = nodes, .ranges = ranges, .instructions = instructions, .entry = entry, .captures = parser.captures, .short = nodes[root].short orelse false, .backrefs = parser.backrefs, .loops = compiler.loops, .preferences = preferences, .depth_limit = parser.depth_limit, .root = root, .entries = entries, .suffixes = suffixes, .priorities = owned_priorities, .reverse = reverse, .entry_widths = entry_widths, .atomics = compiler.atomic_count, .priority_nodes = priority_nodes };
    }
    pub fn deinit(self: *Program, a: A) void {
        a.free(self.nodes);
        a.free(self.ranges);
        a.free(self.instructions);
        a.free(self.preferences);
        a.free(self.entries);
        a.free(self.suffixes);
        a.free(self.priorities);
        self.reverse.deinit(a);
        a.free(self.entry_widths);
        a.free(self.priority_nodes);
        self.* = undefined;
    }
    pub fn find(self: *const Program, a: A, text: []const u32, start: usize, output: []Span, budget: *Budget) !bool {
        @memset(output, .{});
        errdefer @memset(output, .{});
        if (start > text.len) return error.InvalidStart;
        var runner: Runner = .{ .a = a, .program = self, .text = text, .budget = budget };
        defer runner.deinit();
        if (self.backrefs) return runner.backtracking(start, output);
        var span: [1]Span = undefined;
        if (!try runner.regular(self.entry, start, text.len, false, null, &span, 0, null)) return false;
        if (output.len != 0) output[0] = span[0];
        if (output.len > 1 and self.captures != 0) {
            var dissection: Dissection = .{ .runner = &runner, .output = output };
            defer dissection.deinit();
            try dissection.walk(self.root, @intCast(span[0].start), @intCast(span[0].end), 0);
        }
        return true;
    }
};

const Parser = struct {
    a: A,
    text: []const u32,
    flags: u32,
    budget: *Budget,
    nodes: std.ArrayList(Node) = .empty,
    ranges: std.ArrayList(Range) = .empty,
    preferences: std.ArrayList(bool) = .empty,
    position: usize = 0,
    captures: usize = 0,
    depth: usize = 0,
    depth_limit: usize,
    in_look: usize = 0,
    backrefs: bool = false,
    closed: [256]bool = @splat(false),
    fn add(self: *Parser, node: Node) !u32 {
        try self.budget.charge(1);
        if (self.nodes.items.len >= 4096) return error.RegexLimitExceeded;
        try self.nodes.append(self.a, node);
        return @intCast(self.nodes.items.len - 1);
    }
    fn advanced(self: *const Parser) bool {
        return self.flags & 3 == 3;
    }
    fn basic(self: *const Parser) bool {
        return self.flags & 3 == 0;
    }
    fn skip(self: *Parser) void {
        if (self.flags & 32 == 0 or self.flags & 4 != 0) return;
        while (self.position < self.text.len) {
            const ch = self.text[self.position];
            if (ch == '#') {
                while (self.position < self.text.len and self.text[self.position] != '\n') self.position += 1;
            } else if (ch == ' ' or (ch >= '\t' and ch <= '\r')) self.position += 1 else break;
        }
    }
    fn at(self: *Parser, ch: u32) bool {
        self.skip();
        return self.position < self.text.len and self.text[self.position] == ch;
    }
    fn take(self: *Parser, ch: u32) bool {
        if (!self.at(ch)) return false;
        self.position += 1;
        return true;
    }
    fn groupEnd(self: *Parser) bool {
        self.skip();
        // ERE permits an unmatched closing parenthesis as a literal at root.
        if (!self.basic() and !self.advanced() and self.depth == 1) return false;
        return if (self.basic()) self.position + 1 < self.text.len and self.text[self.position] == '\\' and self.text[self.position + 1] == ')' else self.at(')');
    }
    fn bound(self: *Parser) bool {
        if (self.basic()) return self.position + 1 < self.text.len and self.text[self.position] == '\\' and self.text[self.position + 1] == '{';
        return self.at('{') and self.position + 1 < self.text.len and self.text[self.position + 1] >= '0' and self.text[self.position + 1] <= '9';
    }
    fn parse(self: *Parser) !u32 {
        if (self.text.len >= 4 and std.mem.eql(u32, self.text[0..4], &.{ '*', '*', '*', '=' })) {
            self.flags = (self.flags & ~@as(u32, 3)) | 4;
            self.position = 4;
        }
        if (self.text.len >= 4 and std.mem.eql(u32, self.text[0..4], &.{ '*', '*', '*', ':' })) {
            self.flags = (self.flags & ~@as(u32, 4)) | 3;
            self.position = 4;
        }
        if (self.flags & 4 == 0 and self.text.len - self.position >= 4 and self.text[self.position] == '(' and self.text[self.position + 1] == '?') {
            var p = self.position + 2;
            while (p < self.text.len and self.text[p] != ')' and self.text[p] != ':' and self.text[p] != '=' and self.text[p] != '!' and self.text[p] != '<') : (p += 1) {}
            if (p < self.text.len and self.text[p] == ')' and p > self.position + 2) {
                for (self.text[self.position + 2 .. p]) |ch| switch (ch) {
                    'b' => self.flags &= ~@as(u32, 7),
                    'e' => self.flags = (self.flags & ~@as(u32, 7)) | 1,
                    'q' => self.flags = (self.flags & ~@as(u32, 3)) | 4,
                    'i' => self.flags |= 8,
                    'c' => self.flags &= ~@as(u32, 8),
                    'n', 'm' => self.flags |= 192,
                    'p' => self.flags = (self.flags | 64) & ~@as(u32, 128),
                    'w' => self.flags = (self.flags | 128) & ~@as(u32, 64),
                    's' => self.flags &= ~@as(u32, 192),
                    'x' => self.flags |= 32,
                    't' => self.flags &= ~@as(u32, 32),
                    else => return invalid,
                };
                self.position = p + 1;
            }
        }
        if (self.flags & 4 != 0) {
            var root = try self.add(.{ .tag = .empty });
            while (self.position < self.text.len) : (self.position += 1) {
                const literal = try self.add(.{ .tag = .literal, .value = self.text[self.position], .flags = self.flags });
                root = try self.add(.{ .tag = .concat, .left = root, .right = literal });
            }
            return root;
        }
        const root = try self.expression();
        self.skip();
        if (self.position != self.text.len) return invalid;
        return root;
    }
    fn expression(self: *Parser) anyerror!u32 {
        self.depth += 1;
        defer self.depth -= 1;
        if (self.depth > self.depth_limit) return error.RegexLimitExceeded;
        var root = try self.sequence();
        while (!self.basic() and self.take('|')) {
            const right = try self.sequence();
            root = try self.add(.{ .tag = .alternate, .left = root, .right = right, .short = false });
        }
        return root;
    }
    fn sequence(self: *Parser) anyerror!u32 {
        var items: std.ArrayList(u32) = .empty;
        defer items.deinit(self.a);
        while (true) {
            self.skip();
            if (self.position == self.text.len or self.groupEnd() or (!self.basic() and self.at('|'))) break;
            try items.append(self.a, try self.piece());
        }
        if (items.items.len == 0) return self.add(.{ .tag = .empty });
        var index = items.items.len - 1;
        var root = items.items[index];
        while (index > 0) {
            index -= 1;
            const left = items.items[index];
            root = try self.add(.{ .tag = .concat, .left = left, .right = root, .short = self.nodes.items[left].short orelse self.nodes.items[root].short });
        }
        return root;
    }
    fn piece(self: *Parser) anyerror!u32 {
        const item = try self.atom();
        self.skip();
        var minimum: u32 = 0;
        var maximum: u32 = none;
        var quantified = true;
        if (self.take('*')) {} else if (!self.basic() and self.take('+')) {
            minimum = 1;
        } else if (!self.basic() and self.take('?')) {
            maximum = 1;
        } else {
            const brace = self.bound();
            if (!brace) quantified = false else {
                self.position += if (self.basic()) @as(usize, 2) else 1;
                minimum = try self.number();
                maximum = minimum;
                if (self.take(',')) maximum = if (self.at('}') or (self.basic() and self.at('\\'))) none else try self.number();
                if (self.basic() and !self.take('\\')) return invalid;
                if (!self.take('}') or minimum > maximum or minimum > 255 or (maximum != none and maximum > 255)) return invalid;
            }
        }
        if (!quantified) return item;
        switch (self.nodes.items[item].tag) {
            .start, .end, .absolute_start, .absolute_end, .word_start, .word_end, .word_boundary, .not_word_boundary, .ahead, .not_ahead, .behind, .not_behind => return invalid,
            else => {},
        }
        const short = self.advanced() and self.take('?');
        if (self.at('*') or self.bound() or (!self.basic() and (self.at('+') or self.at('?')))) return invalid;
        return self.add(.{ .tag = .repeat, .left = item, .minimum = minimum, .maximum = maximum, .short = short });
    }
    fn number(self: *Parser) !u32 {
        var value: u32 = 0;
        var count: usize = 0;
        while (self.position < self.text.len and self.text[self.position] >= '0' and self.text[self.position] <= '9') : (count += 1) {
            if (value > 255) return invalid;
            value = value * 10 + self.text[self.position] - '0';
            self.position += 1;
        }
        if (count == 0) return invalid;
        return value;
    }
    fn atom(self: *Parser) anyerror!u32 {
        self.skip();
        if (self.position == self.text.len) return invalid;
        const ch = self.text[self.position];
        if (!self.basic() and ch == '{' and self.bound()) return invalid;
        self.position += 1;
        if (ch == '(' and !self.basic()) return self.group();
        if (ch == '\\') {
            if (self.position == self.text.len) return invalid;
            if (self.basic() and self.text[self.position] == '(') {
                self.position += 1;
                return self.group();
            }
            return self.escape(false);
        }
        return switch (ch) {
            '[' => self.characterClass(),
            '.' => self.add(.{ .tag = .any, .flags = self.flags }),
            '^' => self.add(.{ .tag = .start, .flags = self.flags }),
            '$' => self.add(.{ .tag = .end, .flags = self.flags }),
            '*', '+', '?' => if (!self.basic() or ch == '*') invalid else self.add(.{ .tag = .literal, .value = ch, .flags = self.flags }),
            else => self.add(.{ .tag = .literal, .value = ch, .flags = self.flags }),
        };
    }
    fn group(self: *Parser) anyerror!u32 {
        var tag: Tag = .capture;
        if (!self.basic() and self.take('?')) {
            if (!self.advanced()) return invalid;
            if (self.take(':')) tag = .empty else if (self.take('=')) tag = .ahead else if (self.take('!')) tag = .not_ahead else if (self.take('<')) {
                if (self.take('=')) tag = .behind else if (self.take('!')) tag = .not_behind else return invalid;
            } else if (self.take('#')) {
                while (self.position < self.text.len and self.text[self.position] != ')') self.position += 1;
                if (!self.take(')')) return invalid;
                return self.add(.{ .tag = .empty });
            } else return invalid;
        }
        const look = tag == .ahead or tag == .not_ahead or tag == .behind or tag == .not_behind;
        if (look) self.in_look += 1;
        defer if (look) {
            self.in_look -= 1;
        };
        var number_value: usize = 0;
        if (tag == .capture and self.in_look == 0) {
            self.captures += 1;
            if (self.captures >= self.closed.len) return error.RegexLimitExceeded;
            number_value = self.captures;
            try self.preferences.append(self.a, false);
        }
        const child = try self.expression();
        if (self.basic() and !self.take('\\')) return invalid;
        if (!self.take(')')) return invalid;
        if (tag == .empty or (tag == .capture and number_value == 0)) return child;
        if (number_value != 0) {
            self.closed[number_value] = true;
            self.preferences.items[number_value - 1] = self.nodes.items[child].short orelse false;
        }
        return self.add(.{ .tag = tag, .left = child, .value = @intCast(number_value), .maximum = @intCast(self.captures), .short = if (look) null else self.nodes.items[child].short });
    }
    fn escape(self: *Parser, in_class: bool) !u32 {
        const ch = self.text[self.position];
        self.position += 1;
        if (ch >= '1' and ch <= '9' and !in_class) {
            var index = ch - '0';
            var after = self.position;
            if (self.advanced()) while (after < self.text.len and self.text[after] >= '0' and self.text[after] <= '9') : (after += 1) {
                index = @min(256, index * 10 + self.text[after] - '0');
            };
            if (after == self.position or (index < self.closed.len and self.closed[index])) {
                if (self.in_look != 0 or index >= self.closed.len or !self.closed[index]) return invalid;
                self.position = after;
                self.backrefs = true;
                return self.add(.{ .tag = .backref, .value = index, .flags = self.flags });
            }
            if (ch > '7') return invalid;
        }
        if (!self.advanced()) return self.add(.{ .tag = .literal, .value = ch, .flags = self.flags });
        const tag: ?Tag = switch (ch) {
            'A' => .absolute_start,
            'Z' => .absolute_end,
            'm' => .word_start,
            'M' => .word_end,
            'y' => .word_boundary,
            'Y' => .not_word_boundary,
            else => null,
        };
        if (tag) |t| {
            if (in_class) return invalid;
            return self.add(.{ .tag = t });
        }
        if (ch == 'd' or ch == 'D' or ch == 's' or ch == 'S' or ch == 'w' or ch == 'W') {
            const first: u32 = @intCast(self.ranges.items.len);
            for (0..128) |value| if (classMatches(switch (ch) {
                'd', 'D' => "digit",
                's', 'S' => "space",
                else => "word",
            }, @intCast(value))) {
                try self.classPoint(first, @intCast(value));
            };
            return self.add(.{ .tag = .class, .left = first, .right = @intCast(self.ranges.items.len), .negated = ch >= 'A' and ch <= 'Z', .flags = self.flags });
        }
        var value: u32 = switch (ch) {
            'B' => '\\',
            'a' => 7,
            'b' => 8,
            'e' => 27,
            'f' => 12,
            'n' => 10,
            'r' => 13,
            't' => 9,
            'v' => 11,
            else => ch,
        };
        if (ch == 'u' or ch == 'U' or ch == 'x') {
            const required: usize = if (ch == 'u') 4 else if (ch == 'U') 8 else 0;
            const maximum: usize = if (required == 0) 8 else required;
            value = 0;
            var count: usize = 0;
            while (count < maximum and self.position < self.text.len) {
                const digit = std.fmt.charToDigit(@intCast(@min(255, self.text[self.position])), 16) catch break;
                value = std.math.mul(u32, value, 16) catch return invalid;
                value = std.math.add(u32, value, digit) catch return invalid;
                self.position += 1;
                count += 1;
            }
            if (count == 0 or (required != 0 and count != required) or value > 0x10ffff or (value >= 0xd800 and value <= 0xdfff)) return invalid;
        } else if (ch == 'c') {
            if (self.position == self.text.len or self.text[self.position] > 127) return invalid;
            value = self.text[self.position] & 31;
            self.position += 1;
        } else if (ch >= '0' and ch <= '7') {
            value = ch - '0';
            var count: usize = 1;
            while (count < 3 and self.position < self.text.len and self.text[self.position] >= '0' and self.text[self.position] <= '7') : (count += 1) {
                value = value * 8 + self.text[self.position] - '0';
                self.position += 1;
            }
        } else if (std.ascii.isAlphabetic(@intCast(@min(ch, 255))) and ch != 'a' and ch != 'b' and ch != 'B' and ch != 'e' and ch != 'f' and ch != 'n' and ch != 'r' and ch != 't' and ch != 'v') return invalid;
        return self.add(.{ .tag = .literal, .value = value, .flags = self.flags });
    }
    fn characterClass(self: *Parser) anyerror!u32 {
        if (self.text.len - self.position >= 6 and self.text[self.position] == '[' and self.text[self.position + 1] == ':' and
            (self.text[self.position + 2] == '<' or self.text[self.position + 2] == '>') and std.mem.eql(u32, self.text[self.position + 3 ..][0..3], &.{ ':', ']', ']' }))
        {
            const tag: Tag = if (self.text[self.position + 2] == '<') .word_start else .word_end;
            self.position += 6;
            return self.add(.{ .tag = tag });
        }
        const flags = self.flags;
        self.flags &= ~@as(u32, 32);
        defer self.flags = flags;
        const first: u32 = @intCast(self.ranges.items.len);
        const negated = self.take('^');
        var initial = true;
        while (self.position < self.text.len and (initial or !self.at(']'))) {
            initial = false;
            if (self.at('[') and self.position + 1 < self.text.len and (self.text[self.position + 1] == ':' or self.text[self.position + 1] == '.' or self.text[self.position + 1] == '=')) {
                const marker = self.text[self.position + 1];
                self.position += 2;
                const begin = self.position;
                while (self.position + 1 < self.text.len and !(self.text[self.position] == marker and self.text[self.position + 1] == ']')) self.position += 1;
                if (self.position + 1 >= self.text.len) return invalid;
                const name = self.text[begin..self.position];
                self.position += 2;
                if (marker != ':') {
                    const value = try collatingElement(name);
                    try self.ranges.append(self.a, .{ .first = value, .last = value });
                } else {
                    var buffer: [16]u8 = undefined;
                    if (name.len > buffer.len) return invalid;
                    for (name, 0..) |c, i| {
                        if (c > 127) return invalid;
                        buffer[i] = @intCast(c);
                    }
                    const text = buffer[0..name.len];
                    if (!validClass(text)) return invalid;
                    const class_start = self.ranges.items.len;
                    for (0..128) |value| if (classMatches(text, @intCast(value))) {
                        try self.classPoint(class_start, @intCast(value));
                    };
                }
                continue;
            }
            var from: u32 = undefined;
            if (self.advanced() and self.take('\\')) {
                if (self.position == self.text.len) return invalid;
                const escaped = self.nodes.items[try self.escape(true)];
                if (escaped.tag == .class) {
                    if (self.at('-') and self.position + 1 < self.text.len and self.text[self.position + 1] != ']') return invalid;
                    if (escaped.negated) {
                        // Shorthand ranges are sorted ASCII singletons. Replace
                        // them with their Unicode complement inside the union.
                        const cut = escaped.left;
                        var next: u32 = 0;
                        var values: [128]Range = undefined;
                        const count = escaped.right - escaped.left;
                        for (self.ranges.items[escaped.left..escaped.right], 0..) |range, i| values[i] = range;
                        self.ranges.items.len = cut;
                        for (values[0..count]) |value| {
                            if (next < value.first) try self.ranges.append(self.a, .{ .first = next, .last = value.first - 1 });
                            next = value.last + 1;
                        }
                        try self.ranges.append(self.a, .{ .first = next, .last = 0x10ffff });
                    }
                    continue;
                }
                from = escaped.value;
            } else from = try self.classChar();
            if (self.at('-') and self.position + 1 < self.text.len and self.text[self.position + 1] != ']') {
                self.position += 1;
                const to = try self.classChar();
                if (from > to) return invalid;
                try self.ranges.append(self.a, .{ .first = from, .last = to });
            } else try self.ranges.append(self.a, .{ .first = from, .last = from });
        }
        if (!self.take(']')) return invalid;
        return self.add(.{ .tag = .class, .left = first, .right = @intCast(self.ranges.items.len), .negated = negated, .flags = flags });
    }
    fn classChar(self: *Parser) !u32 {
        if (self.position == self.text.len) return invalid;
        const ch = self.text[self.position];
        self.position += 1;
        if (ch != '\\' or !self.advanced()) return ch;
        if (self.position == self.text.len) return invalid;
        const node = try self.escape(true);
        if (self.nodes.items[node].tag != .literal) return invalid;
        return self.nodes.items[node].value;
    }
    fn classPoint(self: *Parser, begin: usize, value: u32) !void {
        if (self.ranges.items.len > begin) {
            const last = &self.ranges.items[self.ranges.items.len - 1];
            if (last.last + 1 == value) {
                last.last = value;
                return;
            }
        }
        try self.ranges.append(self.a, .{ .first = value, .last = value });
    }
};
fn assignPriorities(a: A, nodes: []Node, index: u32, priorities: *std.ArrayList(bool), depth: usize, maximum: usize, budget: *Budget) anyerror!void {
    try budget.charge(1);
    if (depth >= maximum) return error.RegexLimitExceeded;
    if (nodes[index].priority != none) return;
    switch (nodes[index].tag) {
        .concat, .alternate, .capture, .repeat => {
            nodes[index].priority = @intCast(priorities.items.len);
            try priorities.append(a, nodes[index].short orelse false);
            try assignPriorities(a, nodes, nodes[index].left, priorities, depth + 1, maximum, budget);
            if (nodes[index].tag == .concat or nodes[index].tag == .alternate) try assignPriorities(a, nodes, nodes[index].right, priorities, depth + 1, maximum, budget);
        },
        else => {},
    }
}
fn validClass(name: []const u8) bool {
    for ([_][]const u8{ "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct", "space", "upper", "word", "xdigit" }) |candidate| if (std.mem.eql(u8, name, candidate)) return true;
    return false;
}
fn collatingElement(name: []const u32) !u32 {
    if (name.len == 1) return name[0];
    var bytes: [32]u8 = undefined;
    if (name.len > bytes.len) return invalid;
    for (name, 0..) |ch, i| {
        if (ch > 127) return invalid;
        bytes[i] = @intCast(ch);
    }
    const text = bytes[0..name.len];
    const controls = [_][]const u8{ "NUL", "SOH", "STX", "ETX", "EOT", "ENQ", "ACK", "BEL", "BS", "HT", "LF", "VT", "FF", "CR", "SO", "SI", "DLE", "DC1", "DC2", "DC3", "DC4", "NAK", "SYN", "ETB", "CAN", "EM", "SUB", "ESC", "IS4", "IS3", "IS2", "IS1" };
    for (controls, 0..) |control, value| if (std.mem.eql(u8, control, text)) return @intCast(value);
    const names = std.StaticStringMap(u32).initComptime(.{
        .{ "alert", 7 },               .{ "backspace", 8 },          .{ "tab", 9 },                    .{ "newline", 10 },            .{ "vertical-tab", 11 },       .{ "form-feed", 12 },            .{ "carriage-return", 13 },
        .{ "FS", 28 },                 .{ "GS", 29 },                .{ "RS", 30 },                    .{ "US", 31 },                 .{ "DEL", 127 },               .{ "space", ' ' },               .{ "exclamation-mark", '!' },
        .{ "quotation-mark", '"' },    .{ "number-sign", '#' },      .{ "dollar-sign", '$' },          .{ "percent-sign", '%' },      .{ "ampersand", '&' },         .{ "apostrophe", '\'' },         .{ "left-parenthesis", '(' },
        .{ "right-parenthesis", ')' }, .{ "asterisk", '*' },         .{ "plus-sign", '+' },            .{ "comma", ',' },             .{ "hyphen", '-' },            .{ "hyphen-minus", '-' },        .{ "period", '.' },
        .{ "full-stop", '.' },         .{ "slash", '/' },            .{ "solidus", '/' },              .{ "zero", '0' },              .{ "one", '1' },               .{ "two", '2' },                 .{ "three", '3' },
        .{ "four", '4' },              .{ "five", '5' },             .{ "six", '6' },                  .{ "seven", '7' },             .{ "eight", '8' },             .{ "nine", '9' },                .{ "colon", ':' },
        .{ "semicolon", ';' },         .{ "less-than-sign", '<' },   .{ "equals-sign", '=' },          .{ "greater-than-sign", '>' }, .{ "question-mark", '?' },     .{ "commercial-at", '@' },       .{ "left-square-bracket", '[' },
        .{ "backslash", '\\' },        .{ "reverse-solidus", '\\' }, .{ "right-square-bracket", ']' }, .{ "circumflex", '^' },        .{ "circumflex-accent", '^' }, .{ "underscore", '_' },          .{ "low-line", '_' },
        .{ "grave-accent", '`' },      .{ "left-brace", '{' },       .{ "left-curly-bracket", '{' },   .{ "vertical-line", '|' },     .{ "right-brace", '}' },       .{ "right-curly-bracket", '}' }, .{ "tilde", '~' },
    });
    return names.get(text) orelse invalid;
}

fn classMatches(name: []const u8, ch: u32) bool {
    if (ch > 127) return false;
    const c: u8 = @intCast(ch);
    if (std.mem.eql(u8, name, "alnum")) return std.ascii.isAlphanumeric(c);
    if (std.mem.eql(u8, name, "alpha")) return std.ascii.isAlphabetic(c);
    if (std.mem.eql(u8, name, "ascii")) return true;
    if (std.mem.eql(u8, name, "blank")) return c == ' ' or c == '\t';
    if (std.mem.eql(u8, name, "cntrl")) return c < 32 or c == 127;
    if (std.mem.eql(u8, name, "digit")) return std.ascii.isDigit(c);
    if (std.mem.eql(u8, name, "graph")) return c > 32 and c < 127;
    if (std.mem.eql(u8, name, "lower")) return std.ascii.isLower(c);
    if (std.mem.eql(u8, name, "print")) return c >= 32 and c < 127;
    if (std.mem.eql(u8, name, "punct")) return c > 32 and c < 127 and !std.ascii.isAlphanumeric(c);
    if (std.mem.eql(u8, name, "space")) return std.ascii.isWhitespace(c);
    if (std.mem.eql(u8, name, "upper")) return std.ascii.isUpper(c);
    if (std.mem.eql(u8, name, "word")) return std.ascii.isAlphanumeric(c) or c == '_';
    return std.ascii.isHex(c);
}
fn fold(ch: u32) u32 {
    return if (ch >= 'A' and ch <= 'Z') ch + 32 else ch;
}
fn word(ch: u32) bool {
    return classMatches("word", ch);
}

const History = struct { previous: isize, start: isize, end: isize };
const HistoryOrder = struct {
    a: A,
    left: std.ArrayList(usize) = .empty,
    right: std.ArrayList(usize) = .empty,
    fn deinit(self: *HistoryOrder) void {
        self.left.deinit(self.a);
        self.right.deinit(self.a);
    }
    fn compare(self: *HistoryOrder, lhs: isize, rhs: isize, histories: []const History, budget: *Budget) !std.math.Order {
        self.left.clearRetainingCapacity();
        self.right.clearRetainingCapacity();
        var head = lhs;
        while (head >= 0) {
            try budget.charge(1);
            const index: usize = @intCast(head);
            try self.left.append(self.a, index);
            head = histories[index].previous;
        }
        head = rhs;
        while (head >= 0) {
            try budget.charge(1);
            const index: usize = @intCast(head);
            try self.right.append(self.a, index);
            head = histories[index].previous;
        }
        for (0..@min(self.left.items.len, self.right.items.len)) |offset| {
            try budget.charge(1);
            const left = histories[self.left.items[self.left.items.len - 1 - offset]];
            const right = histories[self.right.items[self.right.items.len - 1 - offset]];
            const order = std.math.order(left.end - left.start, right.end - right.start);
            if (order != .eq) return order;
        }
        return std.math.order(self.left.items.len, self.right.items.len);
    }
};

const Runner = struct {
    const LookKey = struct { entry: u32, behind: bool };
    a: A,
    program: *const Program,
    text: []const u32,
    budget: *Budget,
    look_depth: usize = 0,
    look_memo: std.AutoHashMapUnmanaged(LookKey, Reachable) = .empty,
    fn deinit(self: *Runner) void {
        var iterator = self.look_memo.valueIterator();
        while (iterator.next()) |value| self.a.free(value.bits);
        self.look_memo.deinit(self.a);
    }
    fn mark(self: *Runner, states: []bool, pc: u32, queue: *std.ArrayList(u32)) !void {
        try self.budget.charge(1);
        if (states[pc]) return;
        states[pc] = true;
        try queue.append(self.a, pc);
    }
    fn reachable(self: *Runner, entry: u32, end: usize, all_positions: bool) anyerror!Reachable {
        const count = self.program.instructions.len;
        const width = self.program.entry_widths[entry];
        var result: Reachable = .{ .base = if (all_positions) 0 else end -| width, .end = end };
        errdefer self.a.free(result.bits);
        const buffers = try self.a.alloc(bool, count * 3);
        defer self.a.free(buffers);
        @memset(buffers, false);
        try self.budget.charge(buffers.len);
        const allowed = buffers[0..count];
        var current = buffers[count..][0..count];
        var next = buffers[count * 2 ..];
        var pending: std.ArrayList(u32) = .empty;
        defer pending.deinit(self.a);
        var future: std.ArrayList(u32) = .empty;
        defer future.deinit(self.a);
        try self.mark(allowed, entry, &pending);
        var cursor: usize = 0;
        while (cursor < pending.items.len) : (cursor += 1) {
            const instruction = self.program.instructions[pending.items[cursor]];
            if (instruction.op == .accept) continue;
            try self.mark(allowed, instruction.next, &pending);
            if (instruction.op == .split) try self.mark(allowed, instruction.other, &pending);
        }
        pending.clearRetainingCapacity();
        try self.mark(current, 0, &pending);
        var pos = end;
        while (true) {
            if (all_positions) try self.mark(current, 0, &pending);
            cursor = 0;
            while (cursor < pending.items.len) : (cursor += 1) {
                const target = pending.items[cursor];
                try self.budget.charge(1);
                if (target == entry) {
                    if (end - result.base < 64) {
                        result.inline_bits |= @as(u64, 1) << @intCast(pos - result.base);
                    } else {
                        if (result.bits.len == 0) {
                            result.bits = try self.a.alloc(u64, (end - result.base) / 64 + 1);
                            @memset(result.bits, 0);
                            try self.budget.charge(result.bits.len);
                        }
                        const offset = pos - result.base;
                        result.bits[offset / 64] |= @as(u64, 1) << @intCast(offset % 64);
                    }
                }
                for (self.program.reverse.sources[self.program.reverse.offsets[target]..self.program.reverse.offsets[target + 1]]) |source| {
                    try self.budget.charge(1);
                    if (!allowed[source]) continue;
                    const instruction = self.program.instructions[source];
                    switch (instruction.op) {
                        .literal, .class, .any => if (pos > result.base and try self.accepts(self.program.nodes[instruction.node], self.text[pos - 1])) {
                            try self.mark(next, source, &future);
                        },
                        .assertion => if (self.assertion(self.program.nodes[instruction.node], pos)) {
                            try self.mark(current, source, &pending);
                        },
                        .look => if (try self.look(instruction, pos, 0)) {
                            try self.mark(current, source, &pending);
                        },
                        .backref, .regular_capture => return error.InvalidRegexProgram,
                        .accept => unreachable,
                        else => try self.mark(current, source, &pending),
                    }
                }
            }
            if (pos == result.base or (!all_positions and future.items.len == 0)) break;
            for (pending.items) |pc| current[pc] = false;
            std.mem.swap([]bool, &current, &next);
            std.mem.swap(std.ArrayList(u32), &pending, &future);
            future.clearRetainingCapacity();
            pos -= 1;
        }
        return result;
    }
    fn priorityBetter(self: *Runner, candidate: []const isize, prior: []const isize, candidate_heads: []const isize, prior_heads: []const isize, histories: []const History, history_order: *HistoryOrder, captures: []const isize, prior_captures: []const isize, pos: usize) !bool {
        for (self.program.priorities, 0..) |short, index| {
            try self.budget.charge(1);
            const offset = index * 2;
            if (candidate[offset] < 0 or prior[offset] < 0) {
                if (candidate[offset] != prior[offset]) return candidate[offset] >= 0;
                continue;
            }
            const lhs = candidate[offset + 1] - candidate[offset];
            const rhs = prior[offset + 1] - prior[offset];
            if (lhs != rhs) return if (short) lhs < rhs else lhs > rhs;
            const node = self.program.nodes[self.program.priority_nodes[index]];
            if (node.tag == .repeat and candidate_heads[index] != prior_heads[index]) {
                const order = try history_order.compare(candidate_heads[index], prior_heads[index], histories, self.budget);
                if (order != .eq) return if (self.program.nodes[node.left].short orelse false) order == .lt else order == .gt;
            }
        }
        try self.budget.charge(captures.len);
        return self.better(captures, prior_captures, pos);
    }
    fn accepts(self: *Runner, node: Node, ch: u32) !bool {
        try self.budget.charge(1);
        if (node.tag == .any) return node.flags & 64 == 0 or ch != '\n';
        if (node.tag == .literal) return if (node.flags & 8 != 0) fold(ch) == fold(node.value) else ch == node.value;
        var found = false;
        for (self.program.ranges[node.left..node.right]) |range| {
            try self.budget.charge(1);
            if ((ch >= range.first and ch <= range.last) or (node.flags & 8 != 0 and ((fold(ch) >= range.first and fold(ch) <= range.last) or (ch >= 'a' and ch <= 'z' and ch - 32 >= range.first and ch - 32 <= range.last)))) {
                found = true;
                break;
            }
        }
        return (found != node.negated) and !(node.negated and node.flags & 64 != 0 and ch == '\n');
    }
    fn assertion(self: *Runner, node: Node, pos: usize) bool {
        const before = pos > 0 and word(self.text[pos - 1]);
        const after = pos < self.text.len and word(self.text[pos]);
        return switch (node.tag) {
            .start => pos == 0 or (node.flags & 128 != 0 and self.text[pos - 1] == '\n'),
            .end => pos == self.text.len or (node.flags & 128 != 0 and self.text[pos] == '\n'),
            .absolute_start => pos == 0,
            .absolute_end => pos == self.text.len,
            .word_start => !before and after,
            .word_end => before and !after,
            .word_boundary => before != after,
            .not_word_boundary => before == after,
            else => unreachable,
        };
    }
    fn look(self: *Runner, instruction: Instruction, pos: usize, depth: usize) anyerror!bool {
        _ = depth;
        self.look_depth += 1;
        defer self.look_depth -= 1;
        if (self.look_depth >= self.program.depth_limit) return error.RegexLimitExceeded;
        const node = self.program.nodes[instruction.node];
        const behind = node.tag == .behind or node.tag == .not_behind;
        const negative = node.tag == .not_ahead or node.tag == .not_behind;
        const entry = self.program.entries[node.left];
        const key: LookKey = .{ .entry = entry, .behind = behind };
        if (self.look_memo.get(key)) |cached| return cached.contains(pos) != negative;
        const width = @min(@as(usize, 64), self.program.nodes[node.left].max_width);
        const matched = try self.regular(entry, if (behind) pos -| width else pos, if (behind) pos else @min(self.text.len, pos +| width), !behind, if (behind) pos else null, &.{}, 0, null);
        if (matched or self.program.nodes[node.left].max_width <= 64) return matched != negative;
        var cached: Reachable = .{ .base = 0, .end = self.text.len };
        errdefer self.a.free(cached.bits);
        if (behind) {
            _ = try self.regular(entry, 0, self.text.len, false, null, &.{}, 0, &cached);
        } else cached = try self.reachable(entry, self.text.len, true);
        try self.look_memo.put(self.a, key, cached);
        return cached.contains(pos) != negative;
    }
    fn better(self: *Runner, candidate: []const isize, prior: []const isize, position: usize) bool {
        if (candidate[0] != prior[0]) return candidate[0] < prior[0];
        var group: usize = 1;
        while (group <= self.program.captures) : (group += 1) {
            const index = group * 2;
            if (candidate[index] == prior[index] and candidate[index + 1] == prior[index + 1]) continue;
            if (candidate[index] == -1) return false;
            if (prior[index] == -1) return true;
            const lhs = (if (candidate[index + 1] < candidate[index]) @as(isize, @intCast(position)) else candidate[index + 1]) - candidate[index];
            const rhs = (if (prior[index + 1] < prior[index]) @as(isize, @intCast(position)) else prior[index + 1]) - prior[index];
            if (lhs != rhs) return if (self.program.preferences[group - 1]) lhs < rhs else lhs > rhs;
            if (candidate[index] != prior[index]) return candidate[index] < prior[index];
        }
        return false;
    }
    fn publish(self: *Runner, matrix: []isize, pc: u32, state: []const isize, pos: usize, stride: usize, pending: *std.ArrayList(u32)) !void {
        const target = matrix[@as(usize, pc) * stride ..][0..stride];
        try self.budget.charge(stride);
        if (target[0] != -1 and (stride == 2 and state[0] >= target[0] or stride > 2 and !self.better(state, target, pos))) return;
        @memcpy(target, state);
        try pending.append(self.a, pc);
    }
    /// Tagged Thompson execution merges equivalent regular futures. Memory
    /// depends on program/capture count, never on subject length.
    fn regular(self: *Runner, entry: u32, start: usize, end: usize, anchored: bool, required_end: ?usize, output: []Span, depth: usize, all_ends: ?*Reachable) anyerror!bool {
        if (depth >= self.program.depth_limit) return error.RegexLimitExceeded;
        const stride: usize = 2;
        const size = std.math.mul(usize, self.program.instructions.len, stride) catch return error.RegexLimitExceeded;
        const memory = try self.a.alloc(isize, size * 2 + stride * 2);
        defer self.a.free(memory);
        var current = memory[0..size];
        var next = memory[size .. size * 2];
        const state = memory[size * 2 ..][0..stride];
        const best = memory[size * 2 + stride ..];
        @memset(current, -1);
        @memset(next, -1);
        @memset(best, -1);
        try self.budget.charge(memory.len);
        var pending: std.ArrayList(u32) = .empty;
        defer pending.deinit(self.a);
        var next_pending: std.ArrayList(u32) = .empty;
        defer next_pending.deinit(self.a);
        var found = false;
        var pos = start;
        while (pos <= end) : (pos += 1) {
            try self.budget.charge(1);
            if ((!anchored or pos == start) and (!found or all_ends != null)) {
                @memset(state, -1);
                state[0] = @intCast(pos);
                try self.publish(current, entry, state, pos, stride, &pending);
            }
            var cursor: usize = 0;
            while (cursor < pending.items.len) : (cursor += 1) {
                const pc = pending.items[cursor];
                const instruction = self.program.instructions[pc];
                @memcpy(state, current[@as(usize, pc) * stride ..][0..stride]);
                if (found and state[0] > best[0] and all_ends == null) continue;
                try self.budget.charge(1);
                switch (instruction.op) {
                    .accept => if (required_end == null or pos == required_end.?) {
                        if (output.len == 0 and all_ends == null) return true;
                        if (all_ends) |ends| try ends.record(self.a, pos, self.budget);
                        state[1] = @intCast(pos);
                        const better_end = if (self.program.short) state[1] < best[1] else state[1] > best[1];
                        if (!found or state[0] < best[0] or (state[0] == best[0] and (better_end or (state[1] == best[1] and stride > 2 and self.better(state, best, pos))))) {
                            @memcpy(best, state);
                            found = true;
                        }
                    },
                    .split => {
                        try self.publish(current, instruction.next, state, pos, stride, &pending);
                        try self.publish(current, instruction.other, state, pos, stride, &pending);
                    },
                    .save_start, .save_end => {
                        if (stride > 2) {
                            const index = @as(usize, instruction.value) * 2;
                            if (instruction.op == .save_start) {
                                state[index] = @intCast(pos);
                                state[index + 1] = -1;
                            } else state[index + 1] = @intCast(pos);
                        }
                        try self.publish(current, instruction.next, state, pos, stride, &pending);
                    },
                    .progress => try self.publish(current, instruction.next, state, pos, stride, &pending),
                    .priority_start, .priority_end, .iteration_start, .iteration_end => try self.publish(current, instruction.next, state, pos, stride, &pending),
                    .assertion => if (self.assertion(self.program.nodes[instruction.node], pos)) {
                        try self.publish(current, instruction.next, state, pos, stride, &pending);
                    },
                    .look => if (try self.look(instruction, pos, depth)) {
                        try self.publish(current, instruction.next, state, pos, stride, &pending);
                    },
                    .literal, .any, .class => if (pos < end and try self.accepts(self.program.nodes[instruction.node], self.text[pos])) {
                        try self.publish(next, instruction.next, state, pos + 1, stride, &next_pending);
                    },
                    .backref, .regular_capture => return error.InvalidRegexProgram,
                }
            }
            if (found and next_pending.items.len == 0 and (anchored or all_ends == null)) break;
            try self.budget.charge(pending.items.len);
            for (pending.items) |pc| current[@as(usize, pc) * stride] = -1;
            std.mem.swap([]isize, &current, &next);
            std.mem.swap(std.ArrayList(u32), &pending, &next_pending);
            next_pending.clearRetainingCapacity();
        }
        if (found and output.len != 0) output[0] = .{ .start = best[0], .end = best[1] };
        return found;
    }
    /// Nonregular backreferences use an explicit, heap-bounded work stack.
    /// Progress registers prevent nullable repetition from cycling forever.
    fn backtracking(self: *Runner, start: usize, output: []Span) anyerror!bool {
        const capture_size = (self.program.captures + 1) * 2;
        const atomic_base = 2 + capture_size + self.program.loops;
        const priority_base = atomic_base + self.program.atomics;
        const history_base = priority_base + self.program.priorities.len * 2;
        const stride = history_base + self.program.priorities.len;
        const buffers = try self.a.alloc(isize, stride * 2);
        defer self.a.free(buffers);
        const state = buffers[0..stride];
        const best = buffers[stride..];
        var pending: std.ArrayList(isize) = .empty;
        defer pending.deinit(self.a);
        const captures = try self.a.alloc(Span, self.program.captures + 1);
        defer self.a.free(captures);
        var dissection: Dissection = .{ .runner = self, .output = captures };
        defer dissection.deinit();
        var histories: std.ArrayList(History) = .empty;
        defer histories.deinit(self.a);
        var history_order: HistoryOrder = .{ .a = self.a };
        defer history_order.deinit();
        for (start..self.text.len + 1) |begin| {
            histories.clearRetainingCapacity();
            @memset(state, -1);
            state[0] = @intCast(self.program.entry);
            state[1] = @intCast(begin);
            state[2] = @intCast(begin);
            try pending.appendSlice(self.a, state);
            var found = false;
            while (pending.items.len != 0) {
                const tail = pending.items.len - stride;
                @memcpy(state, pending.items[tail..]);
                pending.items.len = tail;
                var pc: u32 = @intCast(state[0]);
                var pos: usize = @intCast(state[1]);
                while (true) {
                    try self.budget.charge(1);
                    const instruction = self.program.instructions[pc];
                    switch (instruction.op) {
                        .accept => {
                            state[3] = @intCast(pos);
                            if (!found or (if (self.program.short) state[3] < best[3] else state[3] > best[3]) or (state[3] == best[3] and try self.priorityBetter(state[priority_base..history_base], best[priority_base..history_base], state[history_base..], best[history_base..], histories.items, &history_order, state[2..][0..capture_size], best[2..][0..capture_size], pos))) {
                                @memcpy(best, state);
                                found = true;
                            }
                            break;
                        },
                        .literal, .any, .class => {
                            if (pos == self.text.len or !try self.accepts(self.program.nodes[instruction.node], self.text[pos])) break;
                            pos += 1;
                            pc = instruction.next;
                        },
                        .split => {
                            state[0] = @intCast(instruction.other);
                            state[1] = @intCast(pos);
                            try self.budget.charge(stride);
                            try pending.appendSlice(self.a, state);
                            pc = instruction.next;
                        },
                        .save_start => {
                            for (instruction.value..instruction.other + 1) |group| {
                                try self.budget.charge(2);
                                state[2 + group * 2] = -1;
                                state[3 + group * 2] = -1;
                            }
                            const index = 2 + @as(usize, instruction.value) * 2;
                            state[index] = @intCast(pos);
                            state[index + 1] = -1;
                            pc = instruction.next;
                        },
                        .save_end => {
                            state[3 + @as(usize, instruction.value) * 2] = @intCast(pos);
                            pc = instruction.next;
                        },
                        .priority_start, .priority_end => {
                            const index = priority_base + @as(usize, instruction.value) * 2;
                            state[index + @intFromBool(instruction.op == .priority_end)] = @intCast(pos);
                            if (instruction.op == .priority_start) state[history_base + instruction.value] = -1;
                            pc = instruction.next;
                        },
                        .iteration_start => {
                            state[2 + capture_size + instruction.value] = @intCast(pos);
                            pc = instruction.next;
                        },
                        .iteration_end => {
                            const priority = self.program.nodes[instruction.node].priority;
                            try histories.append(self.a, .{ .previous = state[history_base + priority], .start = state[2 + capture_size + instruction.value], .end = @intCast(pos) });
                            state[history_base + priority] = @intCast(histories.items.len - 1);
                            pc = instruction.next;
                        },
                        .progress => {
                            const index = 2 + capture_size + instruction.value;
                            if (state[index] == pos) pc = instruction.other else {
                                state[index] = @intCast(pos);
                                pc = instruction.next;
                            }
                        },
                        .assertion => {
                            if (!self.assertion(self.program.nodes[instruction.node], pos)) break;
                            pc = instruction.next;
                        },
                        .look => {
                            if (!try self.look(instruction, pos, 0)) break;
                            pc = instruction.next;
                        },
                        .backref => {
                            const index = 2 + @as(usize, instruction.value) * 2;
                            if (state[index] < 0 or state[index + 1] < state[index]) break;
                            const length: usize = @intCast(state[index + 1] - state[index]);
                            if (length > self.text.len - pos) break;
                            var equal = true;
                            for (self.text[@intCast(state[index])..@intCast(state[index + 1])], self.text[pos..][0..length]) |lhs, rhs| {
                                try self.budget.charge(1);
                                if (if (self.program.nodes[instruction.node].flags & 8 != 0) fold(lhs) != fold(rhs) else lhs != rhs) {
                                    equal = false;
                                    break;
                                }
                            }
                            if (!equal) break;
                            pos += length;
                            pc = instruction.next;
                        },
                        .regular_capture => {
                            const node = self.program.nodes[instruction.node];
                            const entry = self.program.entries[instruction.node];
                            const ends = try dissection.ends(entry, pos);
                            var candidate = if (state[atomic_base + instruction.value] < 0) ends.end else @min(ends.end, @as(usize, @intCast(state[atomic_base + instruction.value])));
                            var selected = false;
                            while (candidate >= pos) {
                                try self.budget.charge(1);
                                if (ends.contains(candidate)) {
                                    selected = true;
                                    break;
                                }
                                if (candidate == pos) break;
                                candidate -= 1;
                            }
                            if (!selected) break;
                            if (candidate > pos) {
                                state[0] = @intCast(pc);
                                state[1] = @intCast(pos);
                                state[atomic_base + instruction.value] = @intCast(candidate - 1);
                                try self.budget.charge(stride);
                                try pending.appendSlice(self.a, state);
                            }
                            state[atomic_base + instruction.value] = -1;
                            for (captures, 0..) |*capture, i| capture.* = .{ .start = state[2 + i * 2], .end = state[3 + i * 2] };
                            dissection.clear(instruction.node);
                            try dissection.walk(instruction.node, pos, candidate, 0);
                            for (captures, 0..) |capture, i| {
                                state[2 + i * 2] = capture.start;
                                state[3 + i * 2] = capture.end;
                            }
                            _ = node;
                            pos = candidate;
                            pc = instruction.next;
                        },
                    }
                }
            }
            if (found) {
                for (output, 0..) |*span, i| if (i <= self.program.captures) {
                    span.* = .{ .start = best[2 + i * 2], .end = best[3 + i * 2] };
                };
                return true;
            }
        }
        return false;
    }
};

/// Match extent and capture dissection are separate. In particular, a repeated
/// group's *last* capture must not outrank an earlier iteration's preference.
/// Reachability queries share memoized immutable-program results.
const Dissection = struct {
    const Key = struct { entry: u32, end: usize };
    const FrontKey = struct { entry: u32, start: usize };
    runner: *Runner,
    output: []Span,
    memo: std.AutoHashMapUnmanaged(Key, Reachable) = .empty,
    fronts: std.AutoHashMapUnmanaged(FrontKey, Reachable) = .empty,
    fn deinit(self: *Dissection) void {
        var iterator = self.memo.valueIterator();
        while (iterator.next()) |value| self.runner.a.free(value.bits);
        self.memo.deinit(self.runner.a);
        var fronts = self.fronts.valueIterator();
        while (fronts.next()) |value| self.runner.a.free(value.bits);
        self.fronts.deinit(self.runner.a);
    }
    fn ends(self: *Dissection, entry: u32, start: usize) !Reachable {
        const key: FrontKey = .{ .entry = entry, .start = start };
        if (self.fronts.get(key)) |answer| return answer;
        var answer: Reachable = .{ .base = start, .end = @min(self.runner.text.len, start +| self.runner.program.entry_widths[entry]) };
        errdefer self.runner.a.free(answer.bits);
        _ = try self.runner.regular(entry, start, answer.end, true, null, &.{}, 0, &answer);
        try self.fronts.put(self.runner.a, key, answer);
        return answer;
    }
    fn can(self: *Dissection, entry: u32, start: usize, end: usize) !bool {
        try self.runner.budget.charge(1);
        if (self.runner.program.entry_widths[entry] < 64) {
            const answer = try self.runner.reachable(entry, end, false);
            defer self.runner.a.free(answer.bits);
            return answer.contains(start);
        }
        const key: Key = .{ .entry = entry, .end = end };
        if (self.memo.get(key)) |answer| return answer.contains(start);
        const answer = try self.runner.reachable(entry, end, false);
        errdefer self.runner.a.free(answer.bits);
        try self.memo.put(self.runner.a, key, answer);
        return answer.contains(start);
    }
    fn clear(self: *Dissection, index: u32) void {
        const node = self.runner.program.nodes[index];
        if (!node.captures) return;
        switch (node.tag) {
            .capture => {
                if (node.value < self.output.len) self.output[node.value] = .{};
                self.clear(node.left);
            },
            .concat, .alternate => {
                self.clear(node.left);
                self.clear(node.right);
            },
            .repeat => self.clear(node.left),
            else => {},
        }
    }
    fn walk(self: *Dissection, index: u32, start: usize, end: usize, depth: usize) anyerror!void {
        try self.runner.budget.charge(1);
        const program = self.runner.program;
        if (depth >= program.depth_limit) return error.RegexLimitExceeded;
        const node = program.nodes[index];
        if (!node.captures) return;
        switch (node.tag) {
            .capture => {
                if (node.value < self.output.len) self.output[node.value] = .{ .start = @intCast(start), .end = @intCast(end) };
                try self.walk(node.left, start, end, depth + 1);
            },
            .alternate => {
                if (try self.can(program.entries[node.left], start, end)) try self.walk(node.left, start, end, depth + 1) else try self.walk(node.right, start, end, depth + 1);
            },
            .concat => {
                const short = program.nodes[node.left].short orelse false;
                const lhs = program.nodes[node.left];
                const rhs = program.nodes[node.right];
                const lower = @max(start +| lhs.min_width, end -| rhs.max_width);
                const upper = @min(start +| lhs.max_width, end -| rhs.min_width);
                if (lower > upper) return error.InvalidRegexProgram;
                for (0..upper - lower + 1) |offset| {
                    const split = if (short) lower + offset else upper - offset;
                    if (try self.can(program.entries[node.right], split, end) and try self.can(program.entries[node.left], start, split)) {
                        try self.walk(node.left, start, split, depth + 1);
                        try self.walk(node.right, split, end, depth + 1);
                        return;
                    }
                }
                return error.InvalidRegexProgram;
            },
            .repeat => {
                var pos = start;
                var count: usize = 0;
                const child_short = program.nodes[node.left].short orelse false;
                const empty_once = !(node.short orelse false) and node.maximum > 0 and try self.can(program.entries[node.left], start, start);
                while (pos < end or count < node.minimum or (count == 0 and empty_once)) {
                    if (count >= node.maximum) return error.InvalidRegexProgram;
                    var selected = false;
                    const child = program.nodes[node.left];
                    const lower = pos +| child.min_width;
                    const upper = @min(end, pos +| child.max_width);
                    if (lower > upper) return error.InvalidRegexProgram;
                    for (0..upper - lower + 1) |offset| {
                        const split = if (child_short) lower + offset else upper - offset;
                        if (split == pos and pos < end and count >= node.minimum) continue;
                        const suffix = program.suffixes[node.suffix + @min(count + 1, node.suffix_count - 1)];
                        if (!try self.can(suffix, split, end) or !try self.can(program.entries[node.left], pos, split)) continue;
                        self.clear(node.left);
                        try self.walk(node.left, pos, split, depth + 1);
                        count += 1;
                        if (split == pos and count >= node.minimum) return;
                        pos = split;
                        selected = true;
                        break;
                    }
                    if (!selected) return error.InvalidRegexProgram;
                }
            },
            else => {},
        }
    }
};

const Compiler = struct {
    const Key = struct { node: u32, next: u32, atomic: bool };
    a: A,
    nodes: []Node,
    budget: *Budget,
    instructions: std.ArrayList(Instruction) = .empty,
    loops: usize = 0,
    cache: std.AutoHashMapUnmanaged(Key, u32) = .empty,
    suffixes: std.ArrayList(u32) = .empty,
    depth: usize = 0,
    depth_limit: usize,
    atomic_mode: bool = false,
    atomic_count: usize = 0,
    fn emit(self: *Compiler, instruction: Instruction) !u32 {
        try self.budget.charge(1);
        if (self.instructions.items.len >= 16384) return error.RegexLimitExceeded;
        try self.instructions.append(self.a, instruction);
        return @intCast(self.instructions.items.len - 1);
    }
    fn lower(self: *Compiler, index: u32, next: u32) anyerror!u32 {
        const key: Key = .{ .node = index, .next = next, .atomic = self.atomic_mode };
        if (self.cache.get(key)) |entry| return entry;
        self.depth += 1;
        defer self.depth -= 1;
        if (self.depth >= self.depth_limit) return error.RegexLimitExceeded;
        const node = self.nodes[index];
        var entry: u32 = undefined;
        if (node.priority != none and self.atomic_mode) {
            const finish = try self.emit(.{ .op = .priority_end, .value = node.priority, .next = next });
            entry = try self.emit(.{ .op = .priority_start, .value = node.priority, .next = try self.lowerNew(index, finish) });
        } else entry = try self.lowerNew(index, next);
        try self.cache.put(self.a, key, entry);
        return entry;
    }
    fn lowerConcat(self: *Compiler, root: u32, next: u32) !u32 {
        var pending: std.ArrayList(u32) = .empty;
        defer pending.deinit(self.a);
        try pending.append(self.a, self.nodes[root].left);
        try pending.append(self.a, self.nodes[root].right);
        var entry = next;
        while (pending.pop()) |index| {
            try self.budget.charge(1);
            const node = self.nodes[index];
            if (node.tag == .concat and node.priority == none) {
                try pending.append(self.a, node.left);
                try pending.append(self.a, node.right);
            } else entry = try self.lower(index, entry);
        }
        return entry;
    }
    fn lowerNew(self: *Compiler, index: u32, next: u32) anyerror!u32 {
        const node = self.nodes[index];
        if (self.atomic_mode and node.captures and !node.backrefs) {
            const slot: u32 = @intCast(self.atomic_count);
            self.atomic_count += 1;
            return self.emit(.{ .op = .regular_capture, .node = index, .next = next, .value = slot });
        }
        return switch (node.tag) {
            .empty => next,
            .concat => self.lowerConcat(index, next),
            .alternate => self.emit(.{ .op = .split, .next = try self.lower(node.left, next), .other = try self.lower(node.right, next) }),
            .capture => self.emit(.{ .op = .save_start, .value = node.value, .other = node.maximum, .next = try self.lower(node.left, try self.emit(.{ .op = .save_end, .value = node.value, .next = next })) }),
            .repeat => repeat: {
                var entry = next;
                const loop: u32 = @intCast(self.loops);
                if (node.maximum == none or (self.atomic_mode and node.priority != none)) self.loops += 1;
                var suffix: usize = 0;
                const count: usize = if (node.maximum == none) node.minimum else node.maximum;
                if (next == 0) {
                    suffix = self.suffixes.items.len;
                    try self.suffixes.appendNTimes(self.a, 0, count + 1);
                    self.nodes[index].suffix = suffix;
                    self.nodes[index].suffix_count = count + 1;
                }
                if (node.maximum == none) {
                    const split = try self.emit(.{ .op = .split, .other = next });
                    const body = try self.lowerIteration(index, split, loop);
                    const progress = try self.emit(.{ .op = .progress, .next = body, .other = next, .value = loop });
                    self.instructions.items[split].next = progress;
                    entry = split;
                    if (next == 0) self.suffixes.items[suffix + count] = entry;
                } else {
                    var remaining = node.maximum;
                    while (remaining > node.minimum) {
                        remaining -= 1;
                        entry = try self.emit(.{ .op = .split, .next = try self.lowerIteration(index, entry, loop), .other = next });
                        if (next == 0) self.suffixes.items[suffix + remaining] = entry;
                    }
                }
                var remaining = node.minimum;
                while (remaining > 0) {
                    remaining -= 1;
                    entry = try self.lowerIteration(index, entry, loop);
                    if (next == 0) self.suffixes.items[suffix + remaining] = entry;
                }
                break :repeat entry;
            },
            .literal => self.emit(.{ .op = .literal, .node = index, .next = next }),
            .any => self.emit(.{ .op = .any, .node = index, .next = next }),
            .class => self.emit(.{ .op = .class, .node = index, .next = next }),
            .backref => self.emit(.{ .op = .backref, .node = index, .next = next, .value = node.value }),
            .ahead, .not_ahead, .behind, .not_behind => self.emit(.{ .op = .look, .node = index, .other = try self.lower(node.left, try self.emit(.{ .op = .accept })), .next = next }),
            else => self.emit(.{ .op = .assertion, .node = index, .next = next }),
        };
    }
    fn lowerIteration(self: *Compiler, index: u32, next: u32, loop: u32) !u32 {
        const node = self.nodes[index];
        if (!self.atomic_mode or node.priority == none) return self.lower(node.left, next);
        const finish = try self.emit(.{ .op = .iteration_end, .node = index, .value = loop, .next = next });
        return self.emit(.{ .op = .iteration_start, .value = loop, .next = try self.lower(node.left, finish) });
    }
};
