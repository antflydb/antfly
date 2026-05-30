// Copyright 2026 Antfly, Inc.
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
const automaton = @import("automaton.zig");
const vellum = @import("antfly_vellum");

const Allocator = std.mem.Allocator;

pub const RegexAutomaton = automaton.RegexAutomaton;
pub const compile = automaton.compile;

const CharClass = struct {
    bytes: [256]bool = @as([256]bool, @splat(false)),
    negated: bool = false,

    fn matches(self: *const CharClass, b: u8) bool {
        return self.bytes[b] != self.negated;
    }
};

const Node = struct {
    tag: Tag,
    literal: u8 = 0,
    char_class: ?*const CharClass = null,
    child: ?*const Node = null,
    children: []const *const Node = &.{},
    min: usize = 0,
    max: ?usize = null,

    const Tag = enum {
        empty,
        literal,
        any,
        char_class,
        seq,
        alt,
        repeat,
        anchor_start,
        anchor_end,
    };
};

pub const Error = error{InvalidRegex} || Allocator.Error;

pub fn matchesCompiled(pattern: []const u8, compiled: *RegexAutomaton, text: []const u8) bool {
    _ = pattern;
    const automaton_view = compiled.automaton();
    if (compiled.prefix_literals.len > 0) {
        if (compiled.anchored_start) {
            for (compiled.prefix_literals) |prefix| {
                if (std.mem.startsWith(u8, text, prefix)) {
                    return verifyCompiledFrom(automaton_view, compiled.anchored_end, text, 0);
                }
            }
            return false;
        }

        var search_start: usize = 0;
        while (findAnyPrefixCandidate(text, compiled.prefix_literals, compiled.prefix_first_bytes, compiled.prefix_check_offsets, search_start)) |start_idx| {
            if (verifyCompiledFrom(automaton_view, compiled.anchored_end, text, start_idx)) return true;
            search_start = start_idx + 1;
        }
        return false;
    }

    const start_limit: usize = if (compiled.anchored_start) 1 else text.len + 1;
    for (0..start_limit) |start_idx| {
        if (verifyCompiledFrom(automaton_view, compiled.anchored_end, text, start_idx)) return true;
    }

    return false;
}

fn findAnyPrefixCandidate(
    text: []const u8,
    prefixes: [][]u8,
    prefix_first_bytes: []const u8,
    prefix_check_offsets: []const u8,
    start_idx: usize,
) ?usize {
    if (prefix_first_bytes.len == 0) return null;

    var candidate = findAnyLiteralByte(text, prefix_first_bytes, start_idx);
    while (candidate) |idx| {
        for (prefixes, prefix_check_offsets) |prefix, check_offset| {
            if (prefix.len == 0 or prefix[0] != text[idx]) continue;
            if (idx + prefix.len > text.len) continue;
            if (text[idx + check_offset] != prefix[check_offset]) continue;

            if (std.mem.eql(u8, text[idx .. idx + prefix.len], prefix)) {
                return idx;
            }
        }
        candidate = findAnyLiteralByte(text, prefix_first_bytes, idx + 1);
    }
    return null;
}

pub fn matches(alloc: Allocator, pattern: []const u8, text: []const u8) Error!bool {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var parser = Parser{
        .alloc = arena.allocator(),
        .pattern = pattern,
    };
    const root = try parser.parse();

    for (0..text.len + 1) |start| {
        var results = std.ArrayListUnmanaged(usize).empty;
        defer results.deinit(alloc);
        try matchNode(alloc, root, text, start, &results);
        if (results.items.len > 0) return true;
    }

    return false;
}

fn verifyCompiledFrom(automaton_view: vellum.Automaton, anchored_end: bool, text: []const u8, start_idx: usize) bool {
    var state = automaton_view.start();
    if (automaton_view.isMatch(state) and (!anchored_end or start_idx == text.len)) return true;

    for (text[start_idx..], 0..) |b, offset| {
        state = automaton_view.accept(state, b);
        if (!automaton_view.canMatch(state)) break;
        if (automaton_view.isMatch(state)) {
            const match_end = start_idx + offset + 1;
            if (!anchored_end or match_end == text.len) return true;
        }
    }
    return false;
}

fn findPrefixCandidate(text: []const u8, prefix: []const u8, start_idx: usize) ?usize {
    if (prefix.len == 0 or start_idx > text.len) return null;
    if (prefix.len == 1) return findLiteralByte(text, prefix[0], start_idx);
    if (text.len < prefix.len) return null;

    const searchable_len = text.len - prefix.len + 1;
    if (start_idx >= searchable_len) return null;

    const first_byte = prefix[0];
    var candidate = findLiteralByte(text[0..searchable_len], first_byte, start_idx);
    while (candidate) |idx| {
        if (std.mem.eql(u8, text[idx .. idx + prefix.len], prefix)) return idx;
        candidate = findLiteralByte(text[0..searchable_len], first_byte, idx + 1);
    }
    return null;
}

fn findLiteralByte(text: []const u8, needle: u8, start_idx: usize) ?usize {
    if (start_idx >= text.len) return null;

    const vector_len = comptime std.simd.suggestVectorLength(u8) orelse 1;
    if (vector_len > 1 and text.len - start_idx >= vector_len) {
        const Vector = @Vector(vector_len, u8);
        const needle_vec: Vector = @splat(needle);

        var idx = start_idx;
        const last_vector_start = text.len - vector_len;
        while (idx <= last_vector_start) : (idx += vector_len) {
            const chunk = text[idx..][0..vector_len];
            const mask = chunk.* == needle_vec;
            if (!@reduce(.Or, mask)) continue;

            inline for (0..vector_len) |lane| {
                if (mask[lane]) return idx + lane;
            }
        }

        for (text[idx..], idx..) |b, pos| {
            if (b == needle) return pos;
        }
        return null;
    }

    return std.mem.indexOfScalarPos(u8, text, start_idx, needle);
}

fn findAnyLiteralByte(text: []const u8, needles: []const u8, start_idx: usize) ?usize {
    if (needles.len == 0 or start_idx >= text.len) return null;
    if (needles.len == 1) return findLiteralByte(text, needles[0], start_idx);

    const vector_len = comptime std.simd.suggestVectorLength(u8) orelse 1;
    if (vector_len > 1 and text.len - start_idx >= vector_len) {
        const Vector = @Vector(vector_len, u8);

        var idx = start_idx;
        const last_vector_start = text.len - vector_len;
        while (idx <= last_vector_start) : (idx += vector_len) {
            const chunk = text[idx..][0..vector_len];
            var mask = chunk.* == @as(Vector, @splat(needles[0]));
            for (needles[1..]) |needle| {
                mask = mask | (chunk.* == @as(Vector, @splat(needle)));
            }
            if (!@reduce(.Or, mask)) continue;

            inline for (0..vector_len) |lane| {
                if (mask[lane]) return idx + lane;
            }
        }

        for (text[idx..], idx..) |b, pos| {
            for (needles) |needle| {
                if (b == needle) return pos;
            }
        }
        return null;
    }

    for (text[start_idx..], start_idx..) |b, pos| {
        for (needles) |needle| {
            if (b == needle) return pos;
        }
    }
    return null;
}

const Parser = struct {
    alloc: Allocator,
    pattern: []const u8,
    pos: usize = 0,

    fn parse(self: *Parser) Error!*const Node {
        const root = try self.parseExpr();
        if (self.pos != self.pattern.len) return error.InvalidRegex;
        return root;
    }

    fn parseExpr(self: *Parser) Error!*const Node {
        var alts = std.ArrayListUnmanaged(*const Node).empty;
        defer alts.deinit(self.alloc);
        try alts.append(self.alloc, try self.parseConcat());

        while (self.pos < self.pattern.len and self.pattern[self.pos] == '|') {
            self.pos += 1;
            try alts.append(self.alloc, try self.parseConcat());
        }

        if (alts.items.len == 1) return alts.items[0];
        return try self.makeNode(.{
            .tag = .alt,
            .children = try alts.toOwnedSlice(self.alloc),
        });
    }

    fn parseConcat(self: *Parser) Error!*const Node {
        var items = std.ArrayListUnmanaged(*const Node).empty;
        defer items.deinit(self.alloc);

        while (self.pos < self.pattern.len) {
            const c = self.pattern[self.pos];
            if (c == '|' or c == ')') break;
            try items.append(self.alloc, try self.parseQuantified());
        }

        if (items.items.len == 0) return try self.makeNode(.{ .tag = .empty });
        if (items.items.len == 1) return items.items[0];
        return try self.makeNode(.{
            .tag = .seq,
            .children = try items.toOwnedSlice(self.alloc),
        });
    }

    fn parseQuantified(self: *Parser) Error!*const Node {
        const atom = try self.parseAtom();
        if (self.pos >= self.pattern.len) return atom;

        return switch (self.pattern[self.pos]) {
            '*' => blk: {
                self.pos += 1;
                break :blk try self.makeNode(.{
                    .tag = .repeat,
                    .child = atom,
                    .min = 0,
                    .max = null,
                });
            },
            '+' => blk: {
                self.pos += 1;
                break :blk try self.makeNode(.{
                    .tag = .repeat,
                    .child = atom,
                    .min = 1,
                    .max = null,
                });
            },
            '?' => blk: {
                self.pos += 1;
                break :blk try self.makeNode(.{
                    .tag = .repeat,
                    .child = atom,
                    .min = 0,
                    .max = 1,
                });
            },
            '{' => try self.parseCountedQuantifier(atom),
            else => atom,
        };
    }

    fn parseCountedQuantifier(self: *Parser, atom: *const Node) Error!*const Node {
        self.pos += 1;
        const min = try self.parseCount();
        var max: ?usize = min;

        if (self.pos >= self.pattern.len) return error.InvalidRegex;
        if (self.pattern[self.pos] == ',') {
            self.pos += 1;
            if (self.pos < self.pattern.len and self.pattern[self.pos] != '}') {
                max = try self.parseCount();
            } else {
                max = null;
            }
        }
        if (self.pos >= self.pattern.len or self.pattern[self.pos] != '}') return error.InvalidRegex;
        self.pos += 1;

        if (max) |max_value| {
            if (max_value < min) return error.InvalidRegex;
        }

        return try self.makeNode(.{
            .tag = .repeat,
            .child = atom,
            .min = min,
            .max = max,
        });
    }

    fn parseCount(self: *Parser) Error!usize {
        const start = self.pos;
        while (self.pos < self.pattern.len and std.ascii.isDigit(self.pattern[self.pos])) : (self.pos += 1) {}
        if (self.pos == start) return error.InvalidRegex;
        return std.fmt.parseInt(usize, self.pattern[start..self.pos], 10) catch return error.InvalidRegex;
    }

    fn parseAtom(self: *Parser) Error!*const Node {
        if (self.pos >= self.pattern.len) return error.InvalidRegex;

        switch (self.pattern[self.pos]) {
            '(' => {
                self.pos += 1;
                const child = try self.parseExpr();
                if (self.pos >= self.pattern.len or self.pattern[self.pos] != ')') return error.InvalidRegex;
                self.pos += 1;
                return child;
            },
            '[' => return try self.parseCharClass(),
            '.' => {
                self.pos += 1;
                return try self.makeNode(.{ .tag = .any });
            },
            '\\' => {
                self.pos += 1;
                if (self.pos >= self.pattern.len) return error.InvalidRegex;
                const literal = self.pattern[self.pos];
                self.pos += 1;
                return try self.makeNode(.{ .tag = .literal, .literal = literal });
            },
            '^' => {
                self.pos += 1;
                return try self.makeNode(.{ .tag = .anchor_start });
            },
            '$' => {
                self.pos += 1;
                return try self.makeNode(.{ .tag = .anchor_end });
            },
            else => {
                const literal = self.pattern[self.pos];
                self.pos += 1;
                return try self.makeNode(.{ .tag = .literal, .literal = literal });
            },
        }
    }

    fn parseCharClass(self: *Parser) Error!*const Node {
        self.pos += 1;

        const class = try self.alloc.create(CharClass);
        class.* = .{};

        if (self.pos < self.pattern.len and self.pattern[self.pos] == '^') {
            class.negated = true;
            self.pos += 1;
        }

        var first = true;
        while (self.pos < self.pattern.len) {
            const c = self.pattern[self.pos];
            if (c == ']' and !first) break;
            first = false;

            const start_char = try self.parseClassChar();
            if (self.pos + 1 < self.pattern.len and self.pattern[self.pos] == '-' and self.pattern[self.pos + 1] != ']') {
                self.pos += 1;
                const end_char = try self.parseClassChar();
                if (start_char > end_char) return error.InvalidRegex;
                for (start_char..end_char + 1) |value| class.bytes[value] = true;
                continue;
            }

            class.bytes[start_char] = true;
        }

        if (self.pos >= self.pattern.len or self.pattern[self.pos] != ']') return error.InvalidRegex;
        self.pos += 1;

        return try self.makeNode(.{
            .tag = .char_class,
            .char_class = class,
        });
    }

    fn parseClassChar(self: *Parser) Error!u8 {
        if (self.pos >= self.pattern.len) return error.InvalidRegex;
        if (self.pattern[self.pos] == '\\') {
            self.pos += 1;
            if (self.pos >= self.pattern.len) return error.InvalidRegex;
        }
        const value = self.pattern[self.pos];
        self.pos += 1;
        return value;
    }

    fn makeNode(self: *Parser, value: Node) Error!*const Node {
        const node = try self.alloc.create(Node);
        node.* = value;
        return node;
    }
};

fn appendUnique(list: *std.ArrayListUnmanaged(usize), alloc: Allocator, value: usize) !void {
    for (list.items) |existing| {
        if (existing == value) return;
    }
    try list.append(alloc, value);
}

fn matchNode(
    alloc: Allocator,
    node: *const Node,
    text: []const u8,
    pos: usize,
    out: *std.ArrayListUnmanaged(usize),
) Error!void {
    switch (node.tag) {
        .empty => try appendUnique(out, alloc, pos),
        .literal => {
            if (pos < text.len and text[pos] == node.literal) {
                try appendUnique(out, alloc, pos + 1);
            }
        },
        .any => {
            if (pos < text.len) try appendUnique(out, alloc, pos + 1);
        },
        .char_class => {
            if (pos < text.len and node.char_class.?.matches(text[pos])) {
                try appendUnique(out, alloc, pos + 1);
            }
        },
        .anchor_start => {
            if (pos == 0) try appendUnique(out, alloc, pos);
        },
        .anchor_end => {
            if (pos == text.len) try appendUnique(out, alloc, pos);
        },
        .alt => {
            for (node.children) |child| try matchNode(alloc, child, text, pos, out);
        },
        .seq => {
            var current = std.ArrayListUnmanaged(usize).empty;
            defer current.deinit(alloc);
            try current.append(alloc, pos);

            for (node.children) |child| {
                var next = std.ArrayListUnmanaged(usize).empty;
                defer next.deinit(alloc);
                for (current.items) |candidate| try matchNode(alloc, child, text, candidate, &next);
                current.clearAndFree(alloc);
                current = next;
                next = .empty;
                if (current.items.len == 0) return;
            }

            for (current.items) |candidate| try appendUnique(out, alloc, candidate);
        },
        .repeat => try matchRepeat(alloc, node, text, pos, out),
    }
}

fn matchRepeat(
    alloc: Allocator,
    node: *const Node,
    text: []const u8,
    pos: usize,
    out: *std.ArrayListUnmanaged(usize),
) Error!void {
    const child = node.child orelse return error.InvalidRegex;

    var frontier = std.ArrayListUnmanaged(usize).empty;
    defer frontier.deinit(alloc);
    try frontier.append(alloc, pos);

    var current_count: usize = 0;
    while (current_count < node.min) : (current_count += 1) {
        var next = std.ArrayListUnmanaged(usize).empty;
        defer next.deinit(alloc);
        for (frontier.items) |candidate| try matchNode(alloc, child, text, candidate, &next);
        if (next.items.len == 0) return;
        frontier.clearAndFree(alloc);
        frontier = next;
        next = .empty;
    }

    var accepted = std.ArrayListUnmanaged(usize).empty;
    defer accepted.deinit(alloc);
    for (frontier.items) |candidate| try appendUnique(&accepted, alloc, candidate);

    var frontier_count = current_count;
    while (node.max == null or frontier_count < node.max.?) : (frontier_count += 1) {
        var next = std.ArrayListUnmanaged(usize).empty;
        defer next.deinit(alloc);

        for (frontier.items) |candidate| {
            var child_matches = std.ArrayListUnmanaged(usize).empty;
            defer child_matches.deinit(alloc);
            try matchNode(alloc, child, text, candidate, &child_matches);
            for (child_matches.items) |matched| {
                if (matched == candidate) continue;
                try appendUnique(&next, alloc, matched);
            }
        }

        if (next.items.len == 0) break;
        for (next.items) |candidate| try appendUnique(&accepted, alloc, candidate);
        frontier.clearAndFree(alloc);
        frontier = next;
        next = .empty;
    }

    for (accepted.items) |candidate| try appendUnique(out, alloc, candidate);
}

test "regex supports counted character classes" {
    try std.testing.expect(try matches(std.testing.allocator, "^[A-Z]{3}-[0-9]{2}$", "ABC-12"));
    try std.testing.expect(!(try matches(std.testing.allocator, "^[A-Z]{3}-[0-9]{2}$", "AB-12")));
}

test "regex supports alternation and grouping" {
    try std.testing.expect(try matches(std.testing.allocator, "^(foo|bar)+$", "foobar"));
    try std.testing.expect(!(try matches(std.testing.allocator, "^(foo|bar)+$", "foobaz")));
}

test "regex supports substring semantics" {
    try std.testing.expect(try matches(std.testing.allocator, "cat", "bobcatdog"));
    try std.testing.expect(!(try matches(std.testing.allocator, "dog$", "bobcat")));
}

test "compiled regex substring helper respects anchors" {
    const alloc = std.testing.allocator;

    var start_regex = try compile(alloc, "^cat");
    defer start_regex.deinit();
    try std.testing.expect(matchesCompiled("^cat", &start_regex, "catnap"));
    try std.testing.expect(!matchesCompiled("^cat", &start_regex, "bobcat"));

    var end_regex = try compile(alloc, "cat$");
    defer end_regex.deinit();
    try std.testing.expect(matchesCompiled("cat$", &end_regex, "bobcat"));
    try std.testing.expect(!matchesCompiled("cat$", &end_regex, "catnap"));

    var exact_regex = try compile(alloc, "^cat$");
    defer exact_regex.deinit();
    try std.testing.expect(matchesCompiled("^cat$", &exact_regex, "cat"));
    try std.testing.expect(!matchesCompiled("^cat$", &exact_regex, "bobcat"));
}

test "compiled regex substring helper uses extracted literal prefix correctly" {
    const alloc = std.testing.allocator;

    var regex = try compile(alloc, "cat.*dog");
    defer regex.deinit();
    try std.testing.expect(matchesCompiled("cat.*dog", &regex, "xxcat123dogyy"));
    try std.testing.expect(!matchesCompiled("cat.*dog", &regex, "xxcab123dogyy"));

    var repeated = try compile(alloc, "a+bc");
    defer repeated.deinit();
    try std.testing.expect(matchesCompiled("a+bc", &repeated, "zzaaabczz"));
    try std.testing.expect(!matchesCompiled("a+bc", &repeated, "zzxbczz"));
}

test "compiled regex substring helper uses extracted literal prefix sets correctly" {
    const alloc = std.testing.allocator;

    var alt = try compile(alloc, "foo|bar");
    defer alt.deinit();
    try std.testing.expect(matchesCompiled("foo|bar", &alt, "xxbarzz"));
    try std.testing.expect(matchesCompiled("foo|bar", &alt, "xxfoozz"));
    try std.testing.expect(!matchesCompiled("foo|bar", &alt, "xxbazzz"));

    var grouped = try compile(alloc, "(foo|bar)baz");
    defer grouped.deinit();
    try std.testing.expect(matchesCompiled("(foo|bar)baz", &grouped, "xxbarbazzz"));
    try std.testing.expect(matchesCompiled("(foo|bar)baz", &grouped, "xxfoobazzz"));
    try std.testing.expect(!matchesCompiled("(foo|bar)baz", &grouped, "xxfoobuzz"));

    var shared_start = try compile(alloc, "cat|car");
    defer shared_start.deinit();
    try std.testing.expect(matchesCompiled("cat|car", &shared_start, "xxcarzz"));
    try std.testing.expect(matchesCompiled("cat|car", &shared_start, "xxcatzz"));
    try std.testing.expect(!matchesCompiled("cat|car", &shared_start, "xxcazzz"));

    var wider = try compile(alloc, "ant|bat|cat|dog|eel");
    defer wider.deinit();
    try std.testing.expect(matchesCompiled("ant|bat|cat|dog|eel", &wider, "xxdogzz"));
    try std.testing.expect(matchesCompiled("ant|bat|cat|dog|eel", &wider, "xxeelzz"));
    try std.testing.expect(!matchesCompiled("ant|bat|cat|dog|eel", &wider, "xxfoxzz"));
}

test "compiled regex substring helper handles dense shared-start prefixes" {
    const alloc = std.testing.allocator;

    var regex = try compile(alloc, "car|cat|cap|can");
    defer regex.deinit();
    try std.testing.expect(matchesCompiled("car|cat|cap|can", &regex, "xxcapzz"));
    try std.testing.expect(matchesCompiled("car|cat|cap|can", &regex, "xxcanzz"));
    try std.testing.expect(!matchesCompiled("car|cat|cap|can", &regex, "xxcazzz"));
}
