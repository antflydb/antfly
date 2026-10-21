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

//! Native table-driven GLR grammar execution. Grammar tables and lexical
//! automata are generated ahead of time; no foreign parser/runtime is linked.
const std = @import("std");
const A = std.mem.Allocator;
const scanners = @import("scanners.zig");
pub const Token = struct { symbol: u16, start: usize, end: usize, scanner: scanners.State = .{} };
pub const Lexer = struct {
    bytes: []const u8,
    at: usize,
    start: usize,
    mark: ?usize = null,
    pub fn init(bytes: []const u8, at: usize) Lexer {
        return .{ .bytes = bytes, .at = at, .start = at };
    }
    pub fn codepoint(self: *const Lexer) u32 {
        if (self.at == self.bytes.len) return 0;
        const n = std.unicode.utf8ByteSequenceLength(self.bytes[self.at]) catch return 0xfffd;
        if (n > self.bytes.len - self.at) return 0xfffd;
        return std.unicode.utf8Decode(self.bytes[self.at..][0..n]) catch 0xfffd;
    }
    pub fn advance(self: *Lexer, skip: bool) void {
        if (self.at < self.bytes.len) {
            const n = std.unicode.utf8ByteSequenceLength(self.bytes[self.at]) catch 1;
            self.at += @min(n, self.bytes.len - self.at);
        }
        if (skip) self.start = self.at;
    }
};
pub fn contains(ranges: []const [2]u32, cp: u32) bool {
    var lo: usize = 0;
    var hi = ranges.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const range = ranges[mid];
        if (cp < range[0]) hi = mid else if (cp > range[1]) lo = mid + 1 else return true;
    }
    return false;
}
pub const Grammar = struct {
    scanner: scanners.Kind = .none,
    data: []const u8,
    lex: *const fn (*Lexer, u16) ?Token,
    keywords: *const fn (*Lexer, u16) ?Token,
    tokens: u16,
    state_count: u16,
    alias_width: usize,
    word: u16,
    builtin_identifiers: []const u16 = &.{},
    rows: usize,
    entries: usize,
    actions: usize,
    action_data: usize,
    modes: usize,
    aliases: usize,
    fields: usize,
    field_entries: usize,
    reserved: usize,
    reserved_data: usize,
    external: usize,
    external_symbols: usize,
    names: []const []const u8,
    flags: []const u8,
    symbol_map: []const u16,
    field_names: []const []const u8,
    fn read16(self: *const Grammar, at: usize) u16 {
        return std.mem.readInt(u16, self.data[at..][0..2], .little);
    }
    fn read32(self: *const Grammar, at: usize) u32 {
        return std.mem.readInt(u32, self.data[at..][0..4], .little);
    }
    fn entry(self: *const Grammar, state: u16, symbol: u16) u16 {
        const row = self.rows + @as(usize, state) * 6;
        const begin = self.entries + @as(usize, self.read32(row)) * 4;
        var lo: usize = 0;
        var hi: usize = self.read16(row + 4);
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const key = self.read16(begin + mid * 4);
            if (key < symbol) lo = mid + 1 else if (key > symbol) hi = mid else return self.read16(begin + mid * 4 + 2);
        }
        return 0;
    }
    fn lexMode(self: *const Grammar, state: u16, part: usize) u16 {
        return self.read16(self.modes + @as(usize, state) * 6 + part * 2);
    }
    fn isReserved(self: *const Grammar, state: u16, symbol: u16) bool {
        const row = self.reserved + @as(usize, self.lexMode(state, 2)) * 6;
        const start = self.reserved_data + @as(usize, self.read32(row)) * 2;
        for (0..self.read16(row + 4)) |i| if (self.read16(start + i * 2) == symbol) return true;
        return false;
    }
    fn tokenize(self: *const Grammar, source: []const u8, at: usize, state: u16, store: *scanners.Store, scanner_state: scanners.State) !?Token {
        const external_mode = self.lexMode(state, 1);
        if (external_mode != 0) {
            var external_input: Lexer = .init(source, at);
            var after = scanner_state;
            const mask = self.read32(self.external + @as(usize, external_mode) * 4);
            if (try scanners.scan(self.scanner, store, &after, &external_input, mask)) |symbol| {
                const end = external_input.mark orelse external_input.at;
                return .{ .symbol = self.read16(self.external_symbols + @as(usize, symbol) * 2), .start = @min(end, external_input.start), .end = end, .scanner = after };
            }
        }
        var input: Lexer = .init(source, at);
        var token = self.lex(&input, self.lexMode(state, 0)) orelse return null;
        if (token.symbol == self.word) {
            var keyword_input: Lexer = .init(source, token.start);
            if (self.keywords(&keyword_input, 0)) |keyword| {
                if (keyword.end == token.end and (self.isReserved(state, keyword.symbol) or self.entry(state, keyword.symbol) != 0)) token.symbol = keyword.symbol;
            }
        }
        token.scanner = scanner_state;
        return token;
    }
};
pub const Node = struct { symbol: u16, production: u16 = 0, start: usize, end: usize, children: []const u32 = &.{} };
pub const Ref = struct { index: u32, alias: u16 = 0 };
pub const Tree = struct {
    arena: std.heap.ArenaAllocator,
    grammar: *const Grammar,
    nodes: []const Node,
    root: ?Ref,
    complete: bool,
    peak_bytes: usize = 0,
    work: usize = 0,
    stack_count: usize = 0,
    error_offset: usize = 0,
    pub fn deinit(self: *Tree) void {
        self.arena.deinit();
    }
    pub fn node(self: *const Tree, ref: Ref) Node {
        var result = self.nodes[ref.index];
        result.symbol = if (ref.alias != 0) ref.alias else self.grammar.symbol_map[result.symbol];
        return result;
    }
    pub fn kind(self: *const Tree, ref: Ref) []const u8 {
        return self.grammar.names[self.node(ref).symbol];
    }
    pub fn child(self: *const Tree, ref: Ref, index: usize) Ref {
        const parent = self.nodes[ref.index];
        const alias = if (index < self.grammar.alias_width) self.grammar.read16(self.grammar.aliases + (@as(usize, parent.production) * self.grammar.alias_width + index) * 2) else 0;
        return .{ .index = parent.children[index], .alias = alias };
    }
    pub fn field(self: *const Tree, ref: Ref, name: []const u8) ?Ref {
        for (self.grammar.field_names, 0..) |candidate, id| if (std.mem.eql(u8, candidate, name)) return self.fieldId(ref, @intCast(id), 0);
        return null;
    }
    fn fieldId(self: *const Tree, ref: Ref, id: u16, depth: usize) ?Ref {
        if (depth > 256) return null;
        const parent = self.nodes[ref.index];
        const row = self.grammar.fields + @as(usize, parent.production) * 6;
        const begin = self.grammar.field_entries + @as(usize, self.grammar.read32(row)) * 4;
        for (0..self.grammar.read16(row + 4)) |i| {
            const at = begin + i * 4;
            if (self.grammar.read16(at) != id) continue;
            const index = self.grammar.data[at + 2];
            if (index >= parent.children.len) continue;
            const value = self.child(ref, index);
            if (self.grammar.data[at + 3] == 0) return value;
            if (self.fieldId(value, id, depth + 1)) |found| return found;
        }
        return null;
    }
};
const Stack = struct { previous: ?u32, state: u16, node: u32, hash: u64 = 0 };
const Branch = struct { head: u32, at: usize = 0, precedence: i64 = 0, token: ?Token = null, scanner: scanners.State = .{} };
fn branchOrder(_: void, left: Branch, right: Branch) std.math.Order {
    const position = std.math.order(left.at, right.at);
    return if (position == .eq) std.math.order(right.precedence, left.precedence) else position;
}
/// Persistent stacks share prefixes across conflicts. Immutable trees and grammar
/// tables can be read across workers. Parser allocations are scoped to this tree.
fn stackHash(previous: u64, state: u16) u64 {
    return (previous ^ @as(u64, state)) *% 0x100000001b3;
}
// Deterministic GLR tie-break: precedence first, then grammar-symbol order,
// child count and the same order recursively. Shared subtrees compare in O(1).
fn compareNodes(a: A, nodes: []const Node, left: u32, right: u32) !i8 {
    var pending: std.ArrayList([2]u32) = .empty;
    defer pending.deinit(a);
    try pending.append(a, .{ left, right });
    while (pending.pop()) |pair| {
        if (pair[0] == pair[1]) continue;
        const l = nodes[pair[0]];
        const r = nodes[pair[1]];
        if (l.symbol != r.symbol) return if (l.symbol < r.symbol) -1 else 1;
        if (l.children.len != r.children.len) return if (l.children.len < r.children.len) -1 else 1;
        var i = l.children.len;
        while (i > 0) {
            i -= 1;
            try pending.append(a, .{ l.children[i], r.children[i] });
        }
    }
    return 0;
}
fn compareStacks(a: A, nodes: []const Node, stacks: []const Stack, left: u32, right: u32) !i8 {
    var pending: std.ArrayList([2]u32) = .empty;
    defer pending.deinit(a);
    var l = left;
    var r = right;
    while (l != r) {
        const ls = stacks[l];
        const rs = stacks[r];
        if (ls.previous == null or rs.previous == null) break;
        try pending.append(a, .{ ls.node, rs.node });
        l = ls.previous.?;
        r = rs.previous.?;
    }
    while (pending.pop()) |pair| {
        const result = try compareNodes(a, nodes, pair[0], pair[1]);
        if (result != 0) return result;
    }
    return 0;
}

pub const Options = struct { memory_limit: usize = 64 * 1024 * 1024 };
pub fn parse(a: A, grammar: *const Grammar, source: []const u8) !Tree {
    return parseOptions(a, grammar, source, .{});
}
pub fn parseOptions(a: A, grammar: *const Grammar, source: []const u8, options: Options) !Tree {
    var budget = @import("bounded.zig").Budget{ .backing = a, .limit = options.memory_limit };
    var tree = parseBounded(budget.allocator(), grammar, source) catch |err| {
        if (err == error.OutOfMemory and budget.denied) return error.ParseBudgetExceeded;
        return err;
    };
    // Quota admission does not change allocation layout. Transfer the arena to
    // the same backing allocator before the stack-local budget leaves scope.
    tree.arena.child_allocator = a;
    tree.peak_bytes = budget.peak;
    return tree;
}
fn parseBounded(a: A, grammar: *const Grammar, source: []const u8) !Tree {
    var arena: std.heap.ArenaAllocator = .init(a);
    errdefer arena.deinit();
    const allocator = arena.allocator();
    var nodes: std.ArrayList(Node) = .empty;
    var stacks: std.ArrayList(Stack) = .empty;
    var pending: std.PriorityQueue(Branch, void, branchOrder) = .initContext({});
    defer pending.deinit(a);
    try stacks.append(allocator, .{ .previous = null, .state = 1, .node = 0 });
    try pending.push(a, .{ .head = 0 });
    var store = try scanners.Store.init(allocator);
    const Key = struct { at: usize, hash: u64, token: u16, cached_token: u16, scanner: scanners.State, collision: u32 = 0 };
    const Visited = struct { head: u32, precedence: i64 };
    var visited: std.AutoHashMap(Key, Visited) = .init(a);
    defer visited.deinit();
    var winner: ?Branch = null;
    var steps: usize = 0;
    var frontier: usize = 0;
    var furthest: usize = 0;
    const budget = std.math.add(usize, std.math.mul(usize, source.len, 2048) catch return error.ParseBudgetExceeded, 65536) catch return error.ParseBudgetExceeded;
    while (pending.pop()) |branch| {
        if (branch.at != frontier) {
            visited.clearRetainingCapacity();
            frontier = branch.at;
        }
        steps += 1;
        if (steps > budget or nodes.items.len > 8 * 1024 * 1024 or pending.items.len > 4096) return error.ParseBudgetExceeded;
        const state = stacks.items[branch.head].state;
        const lexerless = grammar.lexMode(state, 0) == 65535;
        const token = if (lexerless) Token{ .symbol = 0, .start = branch.at, .end = branch.at, .scanner = branch.scanner } else branch.token orelse (try grammar.tokenize(source, branch.at, state, &store, branch.scanner)) orelse continue;
        var key = Key{ .at = branch.at, .hash = stacks.items[branch.head].hash, .token = token.symbol, .cached_token = if (branch.token) |cached| cached.symbol else 65535, .scanner = branch.scanner };
        const prior = lookup: while (true) {
            const candidate = try visited.getOrPut(key);
            if (!candidate.found_existing) break :lookup candidate;
            var left: ?u32 = branch.head;
            var right: ?u32 = candidate.value_ptr.head;
            while (left != null and right != null) {
                if (stacks.items[left.?].state != stacks.items[right.?].state) break;
                left = stacks.items[left.?].previous;
                right = stacks.items[right.?].previous;
            }
            if (left == null and right == null) break :lookup candidate;
            key.collision += 1;
        };
        if (prior.found_existing) {
            if (prior.value_ptr.precedence > branch.precedence) continue;
            if (prior.value_ptr.precedence == branch.precedence and try compareStacks(a, nodes.items, stacks.items, prior.value_ptr.head, branch.head) <= 0) continue;
        }
        prior.value_ptr.* = .{ .head = branch.head, .precedence = branch.precedence };
        // Go builtins can be shadowed; Go 1.26 also permits new(expression).
        // Preserve the type-argument branch while admitting the ordinary call
        // grammar, sharing its immutable tables and persistent stack prefix.
        if (token.symbol != grammar.word and grammar.entry(state, grammar.word) != 0) {
            for (grammar.builtin_identifiers) |symbol| if (token.symbol == symbol) {
                var identifier_branch = branch;
                var identifier = token;
                identifier.symbol = grammar.word;
                identifier_branch.token = identifier;
                try pending.push(a, identifier_branch);
                break;
            };
        }

        const action_index = grammar.entry(state, token.symbol);
        furthest = @max(furthest, token.start);

        if (action_index == 0) continue;
        const action_row = grammar.actions + @as(usize, action_index) * 6;
        const action_start = grammar.read32(action_row);
        for (0..grammar.read16(action_row + 4)) |i| {
            const op = action_start + i * 12;
            var next = branch;
            switch (grammar.data[op]) {
                1 => {
                    // Repetition markers describe reductions; they are not executable shifts.
                    if (grammar.data[op + 1] & 2 != 0) continue;
                    if (token.end == branch.at and grammar.data[op + 1] != 0) continue;
                    if (grammar.data[op + 1] == 0) {
                        const index: u32 = @intCast(nodes.items.len);
                        try nodes.append(allocator, .{ .symbol = token.symbol, .start = token.start, .end = token.end });
                        next.head = @intCast(stacks.items.len);
                        try stacks.append(allocator, .{ .previous = branch.head, .state = grammar.read16(op + 6), .node = index, .hash = stackHash(stacks.items[branch.head].hash, grammar.read16(op + 6)) });
                    }
                    next.at = token.end;
                    next.token = null;
                    next.scanner = token.scanner;
                    try pending.push(a, next);
                },
                2 => {
                    const count = grammar.data[op + 2];
                    const children = try allocator.alloc(u32, count);
                    var head = branch.head;
                    var child_index: usize = count;
                    while (child_index > 0) {
                        child_index -= 1;
                        const item = stacks.items[head];
                        children[child_index] = item.node;
                        head = item.previous orelse return error.InvalidGrammar;
                    }
                    const symbol = grammar.read16(op + 4);
                    const index: u32 = @intCast(nodes.items.len);
                    const begin = if (count == 0) token.start else nodes.items[children[0]].start;
                    const end = if (count == 0) token.start else nodes.items[children[count - 1]].end;
                    try nodes.append(allocator, .{ .symbol = symbol, .production = grammar.read16(op + 8), .start = begin, .end = end, .children = children });
                    next.head = @intCast(stacks.items.len);
                    const target = grammar.entry(stacks.items[head].state, symbol);
                    if (target == 0) return error.InvalidGrammar;
                    if (lexerless and target == stacks.items[head].state) {
                        // A nonterminal extra (e.g. a nested Rust comment) is
                        // parsed with the grammar, then omitted from the stack.
                        next.head = head;
                    } else try stacks.append(allocator, .{ .previous = head, .state = target, .node = index, .hash = stackHash(stacks.items[head].hash, target) });
                    next.token = if (lexerless) branch.token else token;
                    next.precedence += @as(i16, @bitCast(grammar.read16(op + 10)));
                    try pending.push(a, next);
                },
                3 => if (token.end == source.len and (winner == null or (branch.precedence > winner.?.precedence or (branch.precedence == winner.?.precedence and try compareStacks(a, nodes.items, stacks.items, branch.head, winner.?.head) < 0)))) {
                    winner = branch;
                },
                else => {},
            }
        }
    }
    return .{ .arena = arena, .grammar = grammar, .nodes = nodes.items, .root = if (winner) |branch| .{ .index = stacks.items[branch.head].node } else null, .complete = winner != null, .work = steps, .stack_count = stacks.items.len, .error_offset = if (winner == null) furthest else source.len };
}

test "Go grammar parses declarations and generic calls" {
    const grammar = &@import("grammars/go.zig").grammar;
    var tree = try parse(std.testing.allocator, grammar, "package main\nfunc identity[T any](x T) T { return x }\nfunc main() { identity[int](1) }\n");
    defer tree.deinit();
    try std.testing.expect(tree.complete);
    try std.testing.expectEqualStrings("source_file", tree.kind(tree.root.?));
}

test "Go upstream grammar corpus" {
    try corpus(&@import("grammars/go.zig").grammar, @embedFile("grammars/go_corpus.bin"));
}
test "Python upstream grammar corpus" {
    try corpus(&@import("grammars/python.zig").grammar, @embedFile("grammars/python_corpus.bin"));
}
test "Rust upstream grammar corpus" {
    try corpus(&@import("grammars/rust.zig").grammar, @embedFile("grammars/rust_corpus.bin"));
}
fn corpus(grammar: *const Grammar, fixtures: []const u8) !void {
    var at: usize = 0;
    var failures: usize = 0;
    var total: usize = 0;
    while (at < fixtures.len) {
        const title_size = std.mem.readInt(u32, fixtures[at..][0..4], .little);
        const source_size = std.mem.readInt(u32, fixtures[at + 4 ..][0..4], .little);
        const expected_size = std.mem.readInt(u32, fixtures[at + 8 ..][0..4], .little);
        const valid = fixtures[at + 12] != 0;
        at += 13;
        const title = fixtures[at..][0..title_size];
        at += title_size;
        const source = fixtures[at..][0..source_size];
        at += source_size;
        const expected = fixtures[at..][0..expected_size];
        at += expected_size;
        if (!valid) continue;
        total += 1;
        var tree = parse(std.testing.allocator, grammar, source) catch |err| {
            failures += 1;
            std.debug.print("failed: {s}: {s}\n", .{ title, @errorName(err) });
            continue;
        };
        defer tree.deinit();
        if (!tree.complete) {
            failures += 1;
            std.debug.print("failed: {s}\n", .{title});
        } else {
            var rendered: std.ArrayList(u8) = .empty;
            defer rendered.deinit(std.testing.allocator);
            try renderNamed(&tree, tree.root.?, &rendered);
            if (!std.mem.eql(u8, expected, rendered.items)) {
                failures += 1;
                std.debug.print("tree mismatch: {s}\nexpected: {s}\nactual: {s}\n", .{ title, expected, rendered.items });
            }
        }
    }
    std.debug.print("Grammar corpus: {d}/{d} pass\n", .{ total - failures, total });
    try std.testing.expectEqual(@as(usize, 0), failures);
}

test "Go single const" {
    var tree = try parse(std.testing.allocator, &@import("grammars/go.zig").grammar, "package main\nconst zero = 0\n");
    defer tree.deinit();
    try std.testing.expect(tree.complete);
}

test "Go grouped const" {
    var tree = try parse(std.testing.allocator, &@import("grammars/go.zig").grammar, "package main\nconst (\nzero = 0\n)\n");
    defer tree.deinit();
    try std.testing.expect(tree.complete);
}

test "Go map type" {
    var tree = try parse(std.testing.allocator, &@import("grammars/go.zig").grammar, "package main\nvar m map[string]int\n");
    defer tree.deinit();
    try std.testing.expect(tree.complete);
}

test "Rust basic comment" {
    var tree = try parse(std.testing.allocator, &@import("grammars/rust.zig").grammar, "// hi\nfn f() {}\n");
    defer tree.deinit();
    try std.testing.expect(tree.complete);
}

test "grammar admission rejects invalid input and respects the memory budget" {
    const grammar = &@import("grammars/go.zig").grammar;
    try std.testing.expectError(error.ParseBudgetExceeded, parseOptions(std.testing.allocator, grammar, "package main\n", .{ .memory_limit = 1 }));
    var tree = try parse(std.testing.allocator, grammar, "package main\nfunc broken( {\n");
    defer tree.deinit();
    try std.testing.expect(!tree.complete);
}
test "Go automatic semicolon at EOF" {
    var tree = try parse(std.testing.allocator, &@import("grammars/go.zig").grammar, "package main\nvar x = 1");
    defer tree.deinit();
    try std.testing.expect(tree.complete);
}

test "Go field projection diagnostics" {
    var tree = try parse(std.testing.allocator, &@import("grammars/go.zig").grammar, "package p\nfunc f() { STOP() }\n");
    defer tree.deinit();
    try std.testing.expect(tree.complete);
    for (tree.nodes, 0..) |n, i| {
        if (std.mem.eql(u8, tree.grammar.names[n.symbol], "function_declaration")) {
            const ref: Ref = .{ .index = @intCast(i) };

            try std.testing.expect(tree.field(ref, "name") != null);
        }
    }
}

fn renderNamed(tree: *const Tree, ref: Ref, out: *std.ArrayList(u8)) !void {
    const n = tree.node(ref);
    const kind = tree.kind(ref);
    if (std.mem.eql(u8, kind, "comment") or std.mem.eql(u8, kind, "line_comment") or std.mem.eql(u8, kind, "block_comment") or std.mem.eql(u8, kind, "line_continuation")) return;
    const visible = tree.grammar.flags[n.symbol] & 3 == 3;
    if (visible) {
        try out.append(std.testing.allocator, '(');
        try out.appendSlice(std.testing.allocator, kind);
    }
    for (0..n.children.len) |i| try renderNamed(tree, tree.child(ref, i), out);
    if (visible) try out.append(std.testing.allocator, ')');
}

test "frontier parser handles a repeated repository shape within a fixed budget" {
    const a = std.testing.allocator;
    var source: std.ArrayList(u8) = .empty;
    defer source.deinit(a);
    try source.appendSlice(a, "package p\n");
    for (0..500) |_| try source.appendSlice(a, "func f() { x := f(1); _ = x }\n");
    var tree = try parseOptions(a, &@import("grammars/go.zig").grammar, source.items, .{ .memory_limit = 16 * 1024 * 1024 });
    defer tree.deinit();
    try std.testing.expect(tree.complete);
}

test "Go shadowed builtins and new expression arguments" {
    const source = "package p\nfunc f(x int) { a := new(int32(3)); b := new(x + 1); _ = a; _ = b }\n";
    var tree = try parse(std.testing.allocator, &@import("grammars/go.zig").grammar, source);
    defer tree.deinit();
    try std.testing.expect(tree.complete);
}
