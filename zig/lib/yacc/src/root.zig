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

pub const Grammar = struct {
    start_symbol: []const u8 = "",
    expected_conflicts: ?usize = null,
    postgres_major: []const u8 = "",
    postgres_branch: []const u8 = "",
    postgres_commit: []const u8 = "",
    postgres_commit_date: []const u8 = "",
    postgres_gram_y: []const u8 = "",
    postgres_scan_l: []const u8 = "",
    cockroach_sql_y: []const u8 = "",
    tokens: std.ArrayListUnmanaged([]const u8) = .empty,
    rules: std.ArrayListUnmanaged(Rule) = .empty,
};

pub const Rule = struct {
    name: []const u8,
    alternatives: std.ArrayListUnmanaged(Alternative) = .empty,
};

pub const Alternative = struct {
    symbols: []const []const u8,
};

pub const SymbolKind = enum {
    terminal,
    nonterminal,
};

pub const Symbol = struct {
    name: []const u8,
    kind: SymbolKind,
};

pub const Production = struct {
    lhs: u16,
    rhs: []const u16,
};

pub const Item = struct {
    production: u16,
    dot: u16,
};

pub const State = struct {
    items: []const Item,
};

pub const ActionKind = enum {
    shift,
    reduce,
    accept,
};

pub const Action = struct {
    state: u16,
    terminal: u16,
    kind: ActionKind,
    target: u16,
};

pub const Goto = struct {
    state: u16,
    nonterminal: u16,
    target: u16,
};

pub const Conflict = struct {
    state: u16,
    terminal: u16,
    existing: ActionKind,
    candidate: ActionKind,
};

pub const Tables = struct {
    symbols: []const Symbol,
    terminal_count: usize,
    nonterminal_count: usize,
    productions: []const Production,
    states: []const State,
    actions: []const Action,
    gotos: []const Goto,
    conflicts: []const Conflict,
    eof_symbol: u16,
    augmented_start_symbol: u16,
};

pub const ConflictExpectation = struct {
    expected: ?usize,
    actual: usize,

    pub fn matches(self: @This()) bool {
        return self.expected == null or self.expected.? == self.actual;
    }
};

pub fn generateZigMetadata(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    source: []const u8,
) ![]u8 {
    const grammar = try parseGrammar(allocator, source);
    try validateGrammar(grammar);
    const tables = try buildSlrTables(allocator, grammar);
    if (grammar.expected_conflicts) |expected_conflicts| {
        if (tables.conflicts.len != expected_conflicts) return error.ConflictCountMismatch;
    }
    return try emitZigMetadata(allocator, input_path, source, grammar, tables);
}

pub fn conflictExpectation(allocator: std.mem.Allocator, source: []const u8) !ConflictExpectation {
    const grammar = try parseGrammar(allocator, source);
    try validateGrammar(grammar);
    const tables = try buildSlrTables(allocator, grammar);
    return .{
        .expected = grammar.expected_conflicts,
        .actual = tables.conflicts.len,
    };
}

pub fn conflictReportAlloc(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    source: []const u8,
    max_conflicts: usize,
) ![]u8 {
    const grammar = try parseGrammar(allocator, source);
    try validateGrammar(grammar);
    const tables = try buildSlrTables(allocator, grammar);

    var out: std.ArrayListUnmanaged(u8) = .empty;
    try appendFmt(allocator, &out,
        \\grammar conflict report: {s}
        \\expected: {s}
        \\actual: {d}
        \\
    , .{
        input_path,
        if (grammar.expected_conflicts) |expected|
            try std.fmt.allocPrint(allocator, "{d}", .{expected})
        else
            "unset",
        tables.conflicts.len,
    });
    if (tables.conflicts.len == 0) return try out.toOwnedSlice(allocator);

    const limit = @min(max_conflicts, tables.conflicts.len);
    try appendFmt(allocator, &out, "first {d} conflicts:\n", .{limit});
    for (tables.conflicts[0..limit]) |conflict| {
        try appendFmt(
            allocator,
            &out,
            "  state={d} terminal={s} existing={s} candidate={s}\n",
            .{
                conflict.state,
                tables.symbols[conflict.terminal].name,
                @tagName(conflict.existing),
                @tagName(conflict.candidate),
            },
        );
    }
    if (limit < tables.conflicts.len) {
        try appendFmt(allocator, &out, "  ... {d} more conflicts\n", .{tables.conflicts.len - limit});
    }
    return try out.toOwnedSlice(allocator);
}

pub fn parseGrammar(allocator: std.mem.Allocator, source: []const u8) !Grammar {
    var grammar: Grammar = .{};
    var active_rule: ?usize = null;

    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |raw_line| {
        const without_comment = stripLineComment(raw_line);
        var line = std.mem.trim(u8, without_comment, " \t\r");
        if (line.len == 0) continue;

        if (std.mem.startsWith(u8, line, "%reference ")) {
            try parseReference(&grammar, line);
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%expect ")) {
            const value = std.mem.trim(u8, line["%expect ".len..], " \t\r");
            grammar.expected_conflicts = try std.fmt.parseUnsigned(usize, value, 10);
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%start ")) {
            grammar.start_symbol = try dupToken(allocator, line["%start ".len..]);
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%token ")) {
            try parseTokens(allocator, &grammar, line["%token ".len..]);
            active_rule = null;
            continue;
        }

        var ends_rule = false;
        if (std.mem.endsWith(u8, line, ";")) {
            ends_rule = true;
            line = std.mem.trim(u8, line[0 .. line.len - 1], " \t\r");
            if (line.len == 0) {
                active_rule = null;
                continue;
            }
        }

        if (std.mem.indexOfScalar(u8, line, ':')) |colon| {
            const name = std.mem.trim(u8, line[0..colon], " \t\r");
            if (name.len == 0) return error.InvalidRuleName;
            try grammar.rules.append(allocator, .{ .name = try dupToken(allocator, name) });
            active_rule = grammar.rules.items.len - 1;
            const rest = std.mem.trim(u8, line[colon + 1 ..], " \t\r");
            if (rest.len != 0) try appendAlternative(allocator, &grammar.rules.items[active_rule.?], rest);
            if (ends_rule) active_rule = null;
            continue;
        }

        if (active_rule) |rule_idx| {
            const alt = if (std.mem.startsWith(u8, line, "|"))
                std.mem.trim(u8, line[1..], " \t\r")
            else
                line;
            try appendAlternative(allocator, &grammar.rules.items[rule_idx], alt);
            if (ends_rule) active_rule = null;
            continue;
        }

        return error.UnrecognizedGrammarLine;
    }

    return grammar;
}

fn stripLineComment(line: []const u8) []const u8 {
    var index: usize = 0;
    while (std.mem.indexOfPos(u8, line, index, "//")) |idx| {
        if (idx == 0 or std.ascii.isWhitespace(line[idx - 1])) return line[0..idx];
        index = idx + 2;
    }
    return line;
}

fn parseReference(grammar: *Grammar, line: []const u8) !void {
    var parts = std.mem.tokenizeAny(u8, line, " \t\r");
    _ = parts.next();
    const key = parts.next() orelse return error.InvalidReference;
    const value = parts.next() orelse return error.InvalidReference;
    if (std.mem.eql(u8, key, "postgres_major")) {
        grammar.postgres_major = value;
    } else if (std.mem.eql(u8, key, "postgres_branch")) {
        grammar.postgres_branch = value;
    } else if (std.mem.eql(u8, key, "postgres_commit")) {
        grammar.postgres_commit = value;
    } else if (std.mem.eql(u8, key, "postgres_commit_date")) {
        grammar.postgres_commit_date = value;
    } else if (std.mem.eql(u8, key, "postgres_gram_y")) {
        grammar.postgres_gram_y = value;
    } else if (std.mem.eql(u8, key, "postgres_scan_l")) {
        grammar.postgres_scan_l = value;
    } else if (std.mem.eql(u8, key, "cockroach_sql_y")) {
        grammar.cockroach_sql_y = value;
    }
}

fn parseTokens(allocator: std.mem.Allocator, grammar: *Grammar, text: []const u8) !void {
    var parts = std.mem.tokenizeAny(u8, text, " \t\r");
    while (parts.next()) |name| {
        if (!containsName(grammar.tokens.items, name)) {
            try grammar.tokens.append(allocator, try dupToken(allocator, name));
        }
    }
}

fn appendAlternative(allocator: std.mem.Allocator, rule: *Rule, text: []const u8) !void {
    const trimmed = std.mem.trim(u8, text, " \t\r");
    if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "/* empty */")) {
        try rule.alternatives.append(allocator, .{ .symbols = &.{} });
        return;
    }

    var symbols: std.ArrayListUnmanaged([]const u8) = .empty;
    var parts = std.mem.tokenizeAny(u8, trimmed, " \t\r");
    while (parts.next()) |symbol| {
        if (std.mem.startsWith(u8, symbol, "/*")) break;
        try symbols.append(allocator, try dupToken(allocator, symbol));
    }
    try rule.alternatives.append(allocator, .{ .symbols = try symbols.toOwnedSlice(allocator) });
}

pub fn validateGrammar(grammar: Grammar) !void {
    if (grammar.start_symbol.len == 0) return error.MissingStartSymbol;
    if (grammar.tokens.items.len == 0) return error.MissingTokens;
    if (grammar.rules.items.len == 0) return error.MissingRules;
    if (findRule(grammar, grammar.start_symbol) == null) return error.UnknownStartSymbol;
    for (grammar.rules.items) |rule| {
        if (rule.alternatives.items.len == 0) return error.EmptyRule;
        for (rule.alternatives.items) |alt| {
            for (alt.symbols) |symbol| {
                if (!containsName(grammar.tokens.items, symbol) and findRule(grammar, symbol) == null) {
                    return error.UnknownSymbol;
                }
            }
        }
    }
}

pub fn buildSlrTables(allocator: std.mem.Allocator, grammar: Grammar) !Tables {
    try validateGrammar(grammar);

    const terminal_count = grammar.tokens.items.len + 1;
    const eof_symbol: u16 = 0;
    var symbols: std.ArrayListUnmanaged(Symbol) = .empty;
    try symbols.append(allocator, .{ .name = "$end", .kind = .terminal });
    for (grammar.tokens.items) |token| try symbols.append(allocator, .{ .name = token, .kind = .terminal });

    const augmented_start_symbol: u16 = @intCast(symbols.items.len);
    try symbols.append(allocator, .{ .name = "$accept", .kind = .nonterminal });
    for (grammar.rules.items) |rule| try symbols.append(allocator, .{ .name = rule.name, .kind = .nonterminal });

    var productions: std.ArrayListUnmanaged(Production) = .empty;
    const start_rule = findRule(grammar, grammar.start_symbol) orelse return error.UnknownStartSymbol;
    const start_symbol = ruleSymbolId(terminal_count, start_rule);
    try productions.append(allocator, .{ .lhs = augmented_start_symbol, .rhs = try copySymbolIds(allocator, &.{start_symbol}) });

    for (grammar.rules.items, 0..) |rule, rule_idx| {
        const lhs = ruleSymbolId(terminal_count, rule_idx);
        for (rule.alternatives.items) |alt| {
            var rhs: std.ArrayListUnmanaged(u16) = .empty;
            for (alt.symbols) |symbol| try rhs.append(allocator, try symbolId(grammar, terminal_count, symbol));
            try productions.append(allocator, .{ .lhs = lhs, .rhs = try rhs.toOwnedSlice(allocator) });
        }
    }

    const nullable = try computeNullable(allocator, symbols.items.len, productions.items);
    const first = try computeFirst(allocator, symbols.items, productions.items, nullable);
    const follow = try computeFollow(allocator, symbols.items, productions.items, nullable, first, start_symbol, eof_symbol);

    var states: std.ArrayListUnmanaged(State) = .empty;
    var transitions: std.ArrayListUnmanaged(Goto) = .empty;
    const start_items = try closure(allocator, productions.items, symbols.items, &.{.{ .production = 0, .dot = 0 }});
    try states.append(allocator, .{ .items = start_items });

    var state_idx: usize = 0;
    while (state_idx < states.items.len) : (state_idx += 1) {
        const next_symbols = try itemNextSymbols(allocator, productions.items, states.items[state_idx].items);
        for (next_symbols) |next_symbol| {
            const next_items = try gotoItems(allocator, productions.items, symbols.items, states.items[state_idx].items, next_symbol);
            const target = stateIndex(states.items, next_items) orelse target: {
                try states.append(allocator, .{ .items = next_items });
                break :target states.items.len - 1;
            };
            try transitions.append(allocator, .{
                .state = @intCast(state_idx),
                .nonterminal = next_symbol,
                .target = @intCast(target),
            });
        }
    }

    var actions: std.ArrayListUnmanaged(Action) = .empty;
    var gotos: std.ArrayListUnmanaged(Goto) = .empty;
    var conflicts: std.ArrayListUnmanaged(Conflict) = .empty;

    for (states.items, 0..) |state, i| {
        for (transitions.items) |transition| {
            if (transition.state != i) continue;
            if (symbols.items[transition.nonterminal].kind == .terminal) {
                try addAction(allocator, &actions, &conflicts, .{
                    .state = @intCast(i),
                    .terminal = transition.nonterminal,
                    .kind = .shift,
                    .target = transition.target,
                });
            } else {
                try gotos.append(allocator, transition);
            }
        }

        for (state.items) |item| {
            const production = productions.items[item.production];
            if (item.dot != production.rhs.len) continue;
            if (item.production == 0) {
                try addAction(allocator, &actions, &conflicts, .{
                    .state = @intCast(i),
                    .terminal = eof_symbol,
                    .kind = .accept,
                    .target = 0,
                });
                continue;
            }
            for (follow[production.lhs]) |terminal| {
                try addAction(allocator, &actions, &conflicts, .{
                    .state = @intCast(i),
                    .terminal = terminal,
                    .kind = .reduce,
                    .target = item.production,
                });
            }
        }
    }

    sortActions(actions.items);
    sortGotos(gotos.items);
    const symbol_count = symbols.items.len;
    return .{
        .symbols = try symbols.toOwnedSlice(allocator),
        .terminal_count = terminal_count,
        .nonterminal_count = symbol_count - terminal_count,
        .productions = try productions.toOwnedSlice(allocator),
        .states = try states.toOwnedSlice(allocator),
        .actions = try actions.toOwnedSlice(allocator),
        .gotos = try gotos.toOwnedSlice(allocator),
        .conflicts = try conflicts.toOwnedSlice(allocator),
        .eof_symbol = eof_symbol,
        .augmented_start_symbol = augmented_start_symbol,
    };
}

fn computeNullable(allocator: std.mem.Allocator, symbol_count: usize, productions: []const Production) ![]bool {
    const nullable = try allocator.alloc(bool, symbol_count);
    @memset(nullable, false);
    var changed = true;
    while (changed) {
        changed = false;
        for (productions) |production| {
            if (nullable[production.lhs]) continue;
            var rhs_nullable = true;
            for (production.rhs) |symbol| {
                if (!nullable[symbol]) {
                    rhs_nullable = false;
                    break;
                }
            }
            if (rhs_nullable) {
                nullable[production.lhs] = true;
                changed = true;
            }
        }
    }
    return nullable;
}

fn computeFirst(
    allocator: std.mem.Allocator,
    symbols: []const Symbol,
    productions: []const Production,
    nullable: []const bool,
) ![][]const u16 {
    var first = try allocator.alloc(std.ArrayListUnmanaged(u16), symbols.len);
    for (first, 0..) |*set, i| {
        set.* = .empty;
        if (symbols[i].kind == .terminal) try set.append(allocator, @intCast(i));
    }

    var changed = true;
    while (changed) {
        changed = false;
        for (productions) |production| {
            for (production.rhs) |symbol| {
                if (try appendSet(allocator, &first[production.lhs], first[symbol].items)) changed = true;
                if (!nullable[symbol]) break;
            }
        }
    }

    var out = try allocator.alloc([]const u16, symbols.len);
    for (first, 0..) |set, i| out[i] = try sortedCopy(allocator, set.items);
    return out;
}

fn computeFollow(
    allocator: std.mem.Allocator,
    symbols: []const Symbol,
    productions: []const Production,
    nullable: []const bool,
    first: []const []const u16,
    start_symbol: u16,
    eof_symbol: u16,
) ![][]const u16 {
    var follow = try allocator.alloc(std.ArrayListUnmanaged(u16), symbols.len);
    for (follow) |*set| set.* = .empty;
    try follow[start_symbol].append(allocator, eof_symbol);

    var changed = true;
    while (changed) {
        changed = false;
        for (productions) |production| {
            for (production.rhs, 0..) |symbol, idx| {
                if (symbols[symbol].kind != .nonterminal) continue;

                var suffix_nullable = true;
                var j = idx + 1;
                while (j < production.rhs.len) : (j += 1) {
                    const suffix_symbol = production.rhs[j];
                    if (try appendSet(allocator, &follow[symbol], first[suffix_symbol])) changed = true;
                    if (!nullable[suffix_symbol]) {
                        suffix_nullable = false;
                        break;
                    }
                }
                if (suffix_nullable) {
                    if (try appendSet(allocator, &follow[symbol], follow[production.lhs].items)) changed = true;
                }
            }
        }
    }

    var out = try allocator.alloc([]const u16, symbols.len);
    for (follow, 0..) |set, i| out[i] = try sortedCopy(allocator, set.items);
    return out;
}

fn closure(
    allocator: std.mem.Allocator,
    productions: []const Production,
    symbols: []const Symbol,
    seed: []const Item,
) ![]const Item {
    var items: std.ArrayListUnmanaged(Item) = .empty;
    for (seed) |item| _ = try appendItem(allocator, &items, item);

    var changed = true;
    while (changed) {
        changed = false;
        var idx: usize = 0;
        while (idx < items.items.len) : (idx += 1) {
            const item = items.items[idx];
            const production = productions[item.production];
            if (item.dot >= production.rhs.len) continue;
            const next = production.rhs[item.dot];
            if (symbols[next].kind != .nonterminal) continue;
            for (productions, 0..) |candidate, production_idx| {
                if (candidate.lhs != next) continue;
                if (try appendItem(allocator, &items, .{ .production = @intCast(production_idx), .dot = 0 })) {
                    changed = true;
                }
            }
        }
    }
    sortItems(items.items);
    return try items.toOwnedSlice(allocator);
}

fn gotoItems(
    allocator: std.mem.Allocator,
    productions: []const Production,
    symbols: []const Symbol,
    items: []const Item,
    next_symbol: u16,
) ![]const Item {
    var shifted: std.ArrayListUnmanaged(Item) = .empty;
    for (items) |item| {
        const production = productions[item.production];
        if (item.dot < production.rhs.len and production.rhs[item.dot] == next_symbol) {
            try shifted.append(allocator, .{ .production = item.production, .dot = item.dot + 1 });
        }
    }
    return try closure(allocator, productions, symbols, shifted.items);
}

fn itemNextSymbols(allocator: std.mem.Allocator, productions: []const Production, items: []const Item) ![]const u16 {
    var symbols: std.ArrayListUnmanaged(u16) = .empty;
    for (items) |item| {
        const production = productions[item.production];
        if (item.dot >= production.rhs.len) continue;
        _ = try appendUniqueU16(allocator, &symbols, production.rhs[item.dot]);
    }
    std.mem.sort(u16, symbols.items, {}, lessThanU16);
    return try symbols.toOwnedSlice(allocator);
}

fn addAction(
    allocator: std.mem.Allocator,
    actions: *std.ArrayListUnmanaged(Action),
    conflicts: *std.ArrayListUnmanaged(Conflict),
    candidate: Action,
) !void {
    for (actions.items) |*existing| {
        if (existing.state != candidate.state or existing.terminal != candidate.terminal) continue;
        if (std.meta.eql(existing.*, candidate)) return;
        try conflicts.append(allocator, .{
            .state = candidate.state,
            .terminal = candidate.terminal,
            .existing = existing.kind,
            .candidate = candidate.kind,
        });
        if (preferAction(candidate, existing.*)) existing.* = candidate;
        return;
    }
    try actions.append(allocator, candidate);
}

fn preferAction(candidate: Action, existing: Action) bool {
    if (candidate.kind == .accept) return true;
    if (existing.kind == .accept) return false;
    if (candidate.kind == .shift and existing.kind == .reduce) return true;
    if (candidate.kind == .reduce and existing.kind == .reduce) return candidate.target < existing.target;
    return false;
}

fn emitZigMetadata(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    source: []const u8,
    grammar: Grammar,
    tables: Tables,
) ![]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(source, &digest, .{});
    const source_hash = std.fmt.bytesToHex(digest, .lower);

    var production_rhs_count: usize = 0;
    for (tables.productions) |production| production_rhs_count += production.rhs.len;

    var state_item_count: usize = 0;
    for (tables.states) |state| state_item_count += state.items.len;

    var symbol_name_bytes: usize = 0;
    for (tables.symbols) |symbol| symbol_name_bytes += symbol.name.len;
    const action_range_max = maxActionRangeLen(tables.states.len, tables.actions);
    const goto_range_max = maxGotoRangeLen(tables.states.len, tables.gotos);

    var out: std.ArrayListUnmanaged(u8) = .empty;
    try appendFmt(allocator, &out,
        \\// Generated by zig/lib/yacc.
        \\// Source: {s}
        \\// Do not edit by hand.
        \\
        \\const std = @import("std");
        \\
        \\pub const postgres_reference = .{{
        \\    .major = "{s}",
        \\    .branch = "{s}",
        \\    .commit = "{s}",
        \\    .commit_date = "{s}",
        \\    .gram_y = "{s}",
        \\    .scan_l = "{s}",
        \\}};
        \\pub const cockroach_reference = .{{
        \\    .sql_y = "{s}",
        \\}};
        \\
        \\pub const source_sha256_hex = "{s}";
        \\pub const start_symbol = "{s}";
        \\pub const token_count = {d};
        \\pub const rule_count = {d};
        \\pub const production_count = {d};
        \\pub const state_count = {d};
        \\pub const action_count = {d};
        \\pub const goto_count = {d};
        \\pub const conflict_count = {d};
        \\pub const expected_conflict_count: ?usize = {s};
        \\
        \\const SymbolKind = enum {{ terminal, nonterminal }};
        \\const Symbol = struct {{ name: []const u8, kind: SymbolKind }};
        \\const symbols = [_]Symbol{{
        \\
    , .{
        input_path,
        grammar.postgres_major,
        grammar.postgres_branch,
        grammar.postgres_commit,
        grammar.postgres_commit_date,
        grammar.postgres_gram_y,
        grammar.postgres_scan_l,
        grammar.cockroach_sql_y,
        &source_hash,
        grammar.start_symbol,
        grammar.tokens.items.len,
        grammar.rules.items.len,
        tables.productions.len - 1,
        tables.states.len,
        tables.actions.len,
        tables.gotos.len,
        tables.conflicts.len,
        if (grammar.expected_conflicts) |expected_conflicts|
            try std.fmt.allocPrint(allocator, "{d}", .{expected_conflicts})
        else
            "null",
    });
    for (tables.symbols) |symbol| {
        try appendFmt(allocator, &out, "    .{{ .name = \"{s}\", .kind = .{s} }},\n", .{ symbol.name, @tagName(symbol.kind) });
    }
    try out.appendSlice(allocator,
        \\};
        \\
        \\const Production = struct { lhs: u16, rhs_start: u16, rhs_len: u16 };
        \\const production_rhs = [_]u16{
        \\
    );
    for (tables.productions) |production| {
        for (production.rhs) |symbol| try appendFmt(allocator, &out, "    {d},\n", .{symbol});
    }
    try out.appendSlice(allocator,
        \\};
        \\const productions = [_]Production{
        \\
    );
    var rhs_start: usize = 0;
    for (tables.productions) |production| {
        try appendFmt(allocator, &out, "    .{{ .lhs = {d}, .rhs_start = {d}, .rhs_len = {d} }},\n", .{ production.lhs, rhs_start, production.rhs.len });
        rhs_start += production.rhs.len;
    }
    try out.appendSlice(allocator,
        \\};
        \\
        \\const Item = struct { production: u16, dot: u16 };
        \\const state_items = [_]Item{
        \\
    );
    for (tables.states) |state| {
        for (state.items) |item| try appendFmt(allocator, &out, "    .{{ .production = {d}, .dot = {d} }},\n", .{ item.production, item.dot });
    }
    try out.appendSlice(allocator,
        \\};
        \\const State = struct { item_start: u32, item_len: u16 };
        \\const states = [_]State{
        \\
    );
    var item_start: usize = 0;
    for (tables.states) |state| {
        try appendFmt(allocator, &out, "    .{{ .item_start = {d}, .item_len = {d} }},\n", .{ item_start, state.items.len });
        item_start += state.items.len;
    }
    try out.appendSlice(allocator,
        \\};
        \\
        \\const ActionKind = enum { shift, reduce, accept };
        \\const Action = struct { terminal: u16, kind: ActionKind, target: u16 };
        \\const TableRange = struct { start: u32, len: u16 };
        \\const actions = [_]Action{
        \\
    );
    for (tables.actions) |action| {
        try appendFmt(allocator, &out, "    .{{ .terminal = {d}, .kind = .{s}, .target = {d} }},\n", .{ action.terminal, @tagName(action.kind), action.target });
    }
    try out.appendSlice(allocator,
        \\};
        \\const action_ranges = [_]TableRange{
        \\
    );
    try emitTableRanges(allocator, &out, tables.states.len, tables.actions, actionState);
    try out.appendSlice(allocator,
        \\};
        \\
        \\const Goto = struct { nonterminal: u16, target: u16 };
        \\const gotos = [_]Goto{
        \\
    );
    for (tables.gotos) |goto_entry| {
        try appendFmt(allocator, &out, "    .{{ .nonterminal = {d}, .target = {d} }},\n", .{ goto_entry.nonterminal, goto_entry.target });
    }
    try out.appendSlice(allocator,
        \\};
        \\const goto_ranges = [_]TableRange{
        \\
    );
    try emitTableRanges(allocator, &out, tables.states.len, tables.gotos, gotoState);
    try out.appendSlice(allocator,
        \\};
        \\
        \\const Conflict = struct { state: u16, terminal: u16, existing: ActionKind, candidate: ActionKind };
        \\const conflicts = [_]Conflict{
        \\
    );
    for (tables.conflicts) |conflict| {
        try appendFmt(allocator, &out, "    .{{ .state = {d}, .terminal = {d}, .existing = .{s}, .candidate = .{s} }},\n", .{ conflict.state, conflict.terminal, @tagName(conflict.existing), @tagName(conflict.candidate) });
    }
    try out.appendSlice(allocator,
        \\};
        \\
        \\pub const Token = enum {
        \\
    );
    for (grammar.tokens.items) |token| try appendFmt(allocator, &out, "    {s},\n", .{token});
    try out.appendSlice(allocator,
        \\};
        \\
        \\pub fn tokenId(token: Token) u16 {
        \\    return @intFromEnum(token) + 1;
        \\}
        \\
        \\pub fn terminalIdByName(name: []const u8) ?u16 {
        \\    inline for (std.meta.fields(Token)) |field| {
        \\        if (std.mem.eql(u8, field.name, name)) return @intFromEnum(@field(Token, field.name)) + 1;
        \\    }
        \\    return null;
        \\}
        \\
        \\pub const Rule = enum {
        \\
    );
    for (grammar.rules.items) |rule| try appendFmt(allocator, &out, "    {s},\n", .{rule.name});
    try out.appendSlice(allocator,
        \\};
        \\
    );
    try appendFmt(allocator, &out,
        \\pub const production_rhs_count = {d};
        \\pub const state_item_count = {d};
        \\pub const symbol_name_bytes = {d};
        \\pub const action_entry_bytes = @sizeOf(Action);
        \\pub const goto_entry_bytes = @sizeOf(Goto);
        \\pub const table_range_entry_bytes = @sizeOf(TableRange);
        \\pub const action_range_max = {d};
        \\pub const goto_range_max = {d};
        \\pub const parse_table_static_bytes =
        \\    @sizeOf(@TypeOf(symbols)) +
        \\    @sizeOf(@TypeOf(production_rhs)) +
        \\    @sizeOf(@TypeOf(productions)) +
        \\    @sizeOf(@TypeOf(state_items)) +
        \\    @sizeOf(@TypeOf(states)) +
        \\    @sizeOf(@TypeOf(actions)) +
        \\    @sizeOf(@TypeOf(action_ranges)) +
        \\    @sizeOf(@TypeOf(gotos)) +
        \\    @sizeOf(@TypeOf(goto_ranges)) +
        \\    @sizeOf(@TypeOf(conflicts));
        \\pub const parse_table_estimated_bytes = parse_table_static_bytes + symbol_name_bytes;
        \\
    , .{ production_rhs_count, state_item_count, symbol_name_bytes, action_range_max, goto_range_max });
    try out.appendSlice(allocator,
        \\
        \\pub const ParseError = error{
        \\    InvalidGoto,
        \\    StackOverflow,
        \\    StackUnderflow,
        \\    UnexpectedToken,
        \\};
        \\
        \\pub const ParseErrorInfo = struct {
        \\    state: u16,
        \\    lookahead: u16,
        \\    token_index: usize,
        \\};
        \\
        \\pub fn parse(allocator: std.mem.Allocator, token_ids: []const u16) !void {
        \\    var stack: std.ArrayListUnmanaged(u16) = .empty;
        \\    defer stack.deinit(allocator);
        \\    try stack.append(allocator, 0);
        \\
        \\    var index: usize = 0;
        \\    while (true) {
        \\        const state = stack.items[stack.items.len - 1];
        \\        const lookahead: u16 = if (index < token_ids.len) token_ids[index] else 0;
        \\        const action = findAction(state, lookahead) orelse return ParseError.UnexpectedToken;
        \\        switch (action.kind) {
        \\            .shift => {
        \\                try stack.append(allocator, action.target);
        \\                index += 1;
        \\            },
        \\            .reduce => {
        \\                const production = productions[action.target];
        \\                if (production.rhs_len > stack.items.len - 1) return ParseError.StackUnderflow;
        \\                stack.items.len -= production.rhs_len;
        \\                const goto_entry = findGoto(stack.items[stack.items.len - 1], production.lhs) orelse return ParseError.InvalidGoto;
        \\                try stack.append(allocator, goto_entry.target);
        \\            },
        \\            .accept => return,
        \\        }
        \\    }
        \\}
        \\
        \\pub fn parseWithStackBuffer(token_ids: []const u16, stack_buffer: []u16) ParseError!void {
        \\    if (stack_buffer.len == 0) return ParseError.StackOverflow;
        \\    var stack_len: usize = 1;
        \\    stack_buffer[0] = 0;
        \\
        \\    var index: usize = 0;
        \\    while (true) {
        \\        const state = stack_buffer[stack_len - 1];
        \\        const lookahead: u16 = if (index < token_ids.len) token_ids[index] else 0;
        \\        const action = findAction(state, lookahead) orelse return ParseError.UnexpectedToken;
        \\        switch (action.kind) {
        \\            .shift => {
        \\                if (stack_len == stack_buffer.len) return ParseError.StackOverflow;
        \\                stack_buffer[stack_len] = action.target;
        \\                stack_len += 1;
        \\                index += 1;
        \\            },
        \\            .reduce => {
        \\                const production = productions[action.target];
        \\                if (production.rhs_len > stack_len - 1) return ParseError.StackUnderflow;
        \\                stack_len -= production.rhs_len;
        \\                const goto_entry = findGoto(stack_buffer[stack_len - 1], production.lhs) orelse return ParseError.InvalidGoto;
        \\                if (stack_len == stack_buffer.len) return ParseError.StackOverflow;
        \\                stack_buffer[stack_len] = goto_entry.target;
        \\                stack_len += 1;
        \\            },
        \\            .accept => return,
        \\        }
        \\    }
        \\}
        \\
        \\pub fn parseError(allocator: std.mem.Allocator, token_ids: []const u16) !?ParseErrorInfo {
        \\    var stack: std.ArrayListUnmanaged(u16) = .empty;
        \\    defer stack.deinit(allocator);
        \\    try stack.append(allocator, 0);
        \\
        \\    var index: usize = 0;
        \\    while (true) {
        \\        const state = stack.items[stack.items.len - 1];
        \\        const lookahead: u16 = if (index < token_ids.len) token_ids[index] else 0;
        \\        const action = findAction(state, lookahead) orelse return .{ .state = state, .lookahead = lookahead, .token_index = index };
        \\        switch (action.kind) {
        \\            .shift => {
        \\                try stack.append(allocator, action.target);
        \\                index += 1;
        \\            },
        \\            .reduce => {
        \\                const production = productions[action.target];
        \\                if (production.rhs_len > stack.items.len - 1) return .{ .state = state, .lookahead = lookahead, .token_index = index };
        \\                stack.items.len -= production.rhs_len;
        \\                const goto_entry = findGoto(stack.items[stack.items.len - 1], production.lhs) orelse return .{ .state = stack.items[stack.items.len - 1], .lookahead = production.lhs, .token_index = index };
        \\                try stack.append(allocator, goto_entry.target);
        \\            },
        \\            .accept => return null,
        \\        }
        \\    }
        \\}
        \\
        \\pub fn parseErrorWithStackBuffer(token_ids: []const u16, stack_buffer: []u16) ParseError!?ParseErrorInfo {
        \\    if (stack_buffer.len == 0) return ParseError.StackOverflow;
        \\    var stack_len: usize = 1;
        \\    stack_buffer[0] = 0;
        \\
        \\    var index: usize = 0;
        \\    while (true) {
        \\        const state = stack_buffer[stack_len - 1];
        \\        const lookahead: u16 = if (index < token_ids.len) token_ids[index] else 0;
        \\        const action = findAction(state, lookahead) orelse return .{ .state = state, .lookahead = lookahead, .token_index = index };
        \\        switch (action.kind) {
        \\            .shift => {
        \\                if (stack_len == stack_buffer.len) return ParseError.StackOverflow;
        \\                stack_buffer[stack_len] = action.target;
        \\                stack_len += 1;
        \\                index += 1;
        \\            },
        \\            .reduce => {
        \\                const production = productions[action.target];
        \\                if (production.rhs_len > stack_len - 1) return .{ .state = state, .lookahead = lookahead, .token_index = index };
        \\                stack_len -= production.rhs_len;
        \\                const goto_entry = findGoto(stack_buffer[stack_len - 1], production.lhs) orelse return .{ .state = stack_buffer[stack_len - 1], .lookahead = production.lhs, .token_index = index };
        \\                if (stack_len == stack_buffer.len) return ParseError.StackOverflow;
        \\                stack_buffer[stack_len] = goto_entry.target;
        \\                stack_len += 1;
        \\            },
        \\            .accept => return null,
        \\        }
        \\    }
        \\}
        \\
        \\fn actionsForState(state: u16) []const Action {
        \\    if (state >= action_ranges.len) return &.{};
        \\    const range = action_ranges[state];
        \\    const start: usize = @intCast(range.start);
        \\    return actions[start .. start + range.len];
        \\}
        \\
        \\pub fn expectedTerminalCountForState(state: u16) usize {
        \\    return actionsForState(state).len;
        \\}
        \\
        \\pub fn expectedTerminalNameForState(state: u16, index: usize) ?[]const u8 {
        \\    const state_actions = actionsForState(state);
        \\    if (index >= state_actions.len) return null;
        \\    return symbolName(state_actions[index].terminal);
        \\}
        \\
        \\fn symbolName(id: u16) []const u8 {
        \\    if (id >= symbols.len) return "";
        \\    return symbols[id].name;
        \\}
        \\
        \\fn findAction(state: u16, terminal: u16) ?Action {
        \\    if (state >= action_ranges.len) return null;
        \\    const range = action_ranges[state];
        \\    var lo: usize = @intCast(range.start);
        \\    var hi: usize = lo + range.len;
        \\    while (lo < hi) {
        \\        const mid = lo + (hi - lo) / 2;
        \\        const action = actions[mid];
        \\        if (terminal == action.terminal) return action;
        \\        if (terminal < action.terminal) {
        \\            hi = mid;
        \\        } else {
        \\            lo = mid + 1;
        \\        }
        \\    }
        \\    return null;
        \\}
        \\
        \\fn findGoto(state: u16, nonterminal: u16) ?Goto {
        \\    if (state >= goto_ranges.len) return null;
        \\    const range = goto_ranges[state];
        \\    var lo: usize = @intCast(range.start);
        \\    var hi: usize = lo + range.len;
        \\    while (lo < hi) {
        \\        const mid = lo + (hi - lo) / 2;
        \\        const goto_entry = gotos[mid];
        \\        if (nonterminal == goto_entry.nonterminal) return goto_entry;
        \\        if (nonterminal < goto_entry.nonterminal) {
        \\            hi = mid;
        \\        } else {
        \\            lo = mid + 1;
        \\        }
        \\    }
        \\    return null;
        \\}
        \\
    );
    return out.toOwnedSlice(allocator);
}

fn emitTableRanges(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    state_count: usize,
    entries: anytype,
    comptime stateFn: fn (@TypeOf(entries[0])) u16,
) !void {
    var cursor: usize = 0;
    var state: usize = 0;
    while (state < state_count) : (state += 1) {
        const start = cursor;
        while (cursor < entries.len and stateFn(entries[cursor]) == state) cursor += 1;
        try appendFmt(allocator, out, "    .{{ .start = {d}, .len = {d} }},\n", .{ start, cursor - start });
    }
}

fn maxTableRangeLen(state_count: usize, entries: anytype, comptime stateFn: fn (@TypeOf(entries[0])) u16) usize {
    var cursor: usize = 0;
    var max_len: usize = 0;
    var state: usize = 0;
    while (state < state_count) : (state += 1) {
        const start = cursor;
        while (cursor < entries.len and stateFn(entries[cursor]) == state) cursor += 1;
        max_len = @max(max_len, cursor - start);
    }
    return max_len;
}

fn maxActionRangeLen(state_count: usize, actions: []const Action) usize {
    return maxTableRangeLen(state_count, actions, actionState);
}

fn maxGotoRangeLen(state_count: usize, gotos: []const Goto) usize {
    return maxTableRangeLen(state_count, gotos, gotoState);
}

fn actionState(action: Action) u16 {
    return action.state;
}

fn gotoState(goto_entry: Goto) u16 {
    return goto_entry.state;
}

fn appendFmt(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const fragment = try std.fmt.allocPrint(allocator, fmt, args);
    try out.appendSlice(allocator, fragment);
}

fn copySymbolIds(allocator: std.mem.Allocator, symbols: []const u16) ![]const u16 {
    return try allocator.dupe(u16, symbols);
}

fn symbolId(grammar: Grammar, terminal_count: usize, symbol: []const u8) !u16 {
    for (grammar.tokens.items, 0..) |token, idx| {
        if (std.mem.eql(u8, token, symbol)) return @intCast(idx + 1);
    }
    if (findRule(grammar, symbol)) |rule_idx| return ruleSymbolId(terminal_count, rule_idx);
    return error.UnknownSymbol;
}

fn ruleSymbolId(terminal_count: usize, rule_idx: usize) u16 {
    return @intCast(terminal_count + 1 + rule_idx);
}

fn findRule(grammar: Grammar, name: []const u8) ?usize {
    for (grammar.rules.items, 0..) |rule, idx| {
        if (std.mem.eql(u8, rule.name, name)) return idx;
    }
    return null;
}

fn containsName(names: []const []const u8, name: []const u8) bool {
    for (names) |existing| {
        if (std.mem.eql(u8, existing, name)) return true;
    }
    return false;
}

fn appendSet(allocator: std.mem.Allocator, target: *std.ArrayListUnmanaged(u16), source: []const u16) !bool {
    var changed = false;
    for (source) |item| {
        if (try appendUniqueU16(allocator, target, item)) changed = true;
    }
    return changed;
}

fn appendUniqueU16(allocator: std.mem.Allocator, target: *std.ArrayListUnmanaged(u16), value: u16) !bool {
    for (target.items) |existing| {
        if (existing == value) return false;
    }
    try target.append(allocator, value);
    return true;
}

fn appendItem(allocator: std.mem.Allocator, target: *std.ArrayListUnmanaged(Item), value: Item) !bool {
    for (target.items) |existing| {
        if (existing.production == value.production and existing.dot == value.dot) return false;
    }
    try target.append(allocator, value);
    return true;
}

fn sortedCopy(allocator: std.mem.Allocator, items: []const u16) ![]const u16 {
    const out = try allocator.dupe(u16, items);
    std.mem.sort(u16, out, {}, lessThanU16);
    return out;
}

fn stateIndex(states: []const State, items: []const Item) ?usize {
    for (states, 0..) |state, idx| {
        if (itemsEqual(state.items, items)) return idx;
    }
    return null;
}

fn itemsEqual(lhs: []const Item, rhs: []const Item) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |a, b| {
        if (a.production != b.production or a.dot != b.dot) return false;
    }
    return true;
}

fn dupToken(allocator: std.mem.Allocator, token: []const u8) ![]const u8 {
    return try allocator.dupe(u8, std.mem.trim(u8, token, " \t\r"));
}

fn sortItems(items: []Item) void {
    std.mem.sort(Item, items, {}, lessThanItem);
}

fn sortActions(actions: []Action) void {
    std.mem.sort(Action, actions, {}, lessThanAction);
}

fn sortGotos(gotos: []Goto) void {
    std.mem.sort(Goto, gotos, {}, lessThanGoto);
}

fn lessThanU16(_: void, lhs: u16, rhs: u16) bool {
    return lhs < rhs;
}

fn lessThanItem(_: void, lhs: Item, rhs: Item) bool {
    if (lhs.production != rhs.production) return lhs.production < rhs.production;
    return lhs.dot < rhs.dot;
}

fn lessThanAction(_: void, lhs: Action, rhs: Action) bool {
    if (lhs.state != rhs.state) return lhs.state < rhs.state;
    if (lhs.terminal != rhs.terminal) return lhs.terminal < rhs.terminal;
    if (@intFromEnum(lhs.kind) != @intFromEnum(rhs.kind)) return @intFromEnum(lhs.kind) < @intFromEnum(rhs.kind);
    return lhs.target < rhs.target;
}

fn lessThanGoto(_: void, lhs: Goto, rhs: Goto) bool {
    if (lhs.state != rhs.state) return lhs.state < rhs.state;
    if (lhs.nonterminal != rhs.nonterminal) return lhs.nonterminal < rhs.nonterminal;
    return lhs.target < rhs.target;
}

test "parseGrammar handles tokens start productions and empty alternatives" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%start stmt
        \\%token SELECT IDENT
        \\stmt:
        \\    SELECT opt_ident
        \\  ;
        \\opt_ident:
        \\    /* empty */
        \\  | IDENT
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqualStrings("stmt", grammar.start_symbol);
    try std.testing.expectEqual(@as(usize, 2), grammar.tokens.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.rules.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.rules.items[1].alternatives.items.len);
    try std.testing.expectEqual(@as(usize, 0), grammar.rules.items[1].alternatives.items[0].symbols.len);
}

test "parseGrammar preserves reference URLs while stripping real comments" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\// leading comment
        \\%expect 0
        \\%reference postgres_major 19
        \\%reference postgres_gram_y https://github.com/postgres/postgres/blob/hash/src/backend/parser/gram.y
        \\%reference postgres_scan_l https://github.com/postgres/postgres/blob/hash/src/backend/parser/scan.l // trailing comment
        \\%reference cockroach_sql_y https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/parser/sql.y
        \\%start stmt
        \\%token SELECT
        \\stmt:
        \\    SELECT // trailing comment
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(?usize, 0), grammar.expected_conflicts);
    try std.testing.expectEqualStrings("19", grammar.postgres_major);
    try std.testing.expectEqualStrings("https://github.com/postgres/postgres/blob/hash/src/backend/parser/gram.y", grammar.postgres_gram_y);
    try std.testing.expectEqualStrings("https://github.com/postgres/postgres/blob/hash/src/backend/parser/scan.l", grammar.postgres_scan_l);
    try std.testing.expectEqualStrings("https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/parser/sql.y", grammar.cockroach_sql_y);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items[0].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("SELECT", grammar.rules.items[0].alternatives.items[0].symbols[0]);
}

test "buildSlrTables builds conflict-free tables for a small grammar" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%start stmt
        \\%token SELECT IDENT
        \\stmt:
        \\    SELECT opt_ident
        \\  ;
        \\opt_ident:
        \\    /* empty */
        \\  | IDENT
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expect(tables.states.len > 0);
    try std.testing.expect(tables.actions.len > 0);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
}

test "buildSlrTables detects shift reduce conflicts" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%start expr
        \\%token ID PLUS
        \\expr:
        \\    expr PLUS expr
        \\  | ID
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expect(tables.conflicts.len > 0);
}

test "generateZigMetadata emits deterministic parser table metadata" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%reference postgres_major 19
        \\%reference postgres_commit abc
        \\%reference postgres_gram_y https://example.test/postgres/gram.y
        \\%reference postgres_scan_l https://example.test/postgres/scan.l
        \\%reference cockroach_sql_y https://example.test/cockroach/sql.y
        \\%start stmt
        \\%token SELECT IDENT
        \\stmt:
        \\    SELECT IDENT
        \\  ;
    ;
    const first = try generateZigMetadata(arena, "fixture.y", source);
    const second = try generateZigMetadata(arena, "fixture.y", source);
    try std.testing.expectEqualStrings(first, second);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const state_count = ") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "const actions = [_]Action") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "const conflicts = [_]Conflict") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const actions") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const conflicts") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const expected_conflict_count: ?usize = 0;") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const symbol_name_bytes = ") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const action_entry_bytes = @sizeOf(Action);") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const goto_entry_bytes = @sizeOf(Goto);") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const table_range_entry_bytes = @sizeOf(TableRange);") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const action_range_max = ") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const goto_range_max = ") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const parse_table_static_bytes =") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const parse_table_estimated_bytes = parse_table_static_bytes + symbol_name_bytes;") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const symbols") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const Symbol") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, ".gram_y = \"https://example.test/postgres/gram.y\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, ".scan_l = \"https://example.test/postgres/scan.l\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const cockroach_reference") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, ".sql_y = \"https://example.test/cockroach/sql.y\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parse(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseWithStackBuffer(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseErrorWithStackBuffer(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn tokenId(token: Token) u16") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn terminalIdByName(name: []const u8) ?u16") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn tokenIdByName") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "symbols[idx]") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn expectedTerminalCountForState(state: u16) usize") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn expectedTerminalNameForState(state: u16, index: usize) ?[]const u8") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn actionsForState") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn symbolName") == null);
}

test "generateZigMetadata rejects unexpected conflict count drift" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start expr
        \\%token ID PLUS
        \\expr:
        \\    expr PLUS expr
        \\  | ID
        \\  ;
    ;
    try std.testing.expectError(error.ConflictCountMismatch, generateZigMetadata(arena, "ambiguous.y", source));
}

test "conflictExpectation reports expected and actual counts" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start expr
        \\%token ID PLUS
        \\expr:
        \\    expr PLUS expr
        \\  | ID
        \\  ;
    ;
    const expectation = try conflictExpectation(arena, source);
    try std.testing.expectEqual(@as(?usize, 0), expectation.expected);
    try std.testing.expect(expectation.actual > 0);
    try std.testing.expect(!expectation.matches());
}

test "conflictReportAlloc names representative conflicts" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start expr
        \\%token ID PLUS
        \\expr:
        \\    expr PLUS expr
        \\  | ID
        \\  ;
    ;
    const report = try conflictReportAlloc(arena, "ambiguous.y", source, 1);
    try std.testing.expect(std.mem.indexOf(u8, report, "grammar conflict report: ambiguous.y") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "expected: 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "actual: ") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "terminal=PLUS") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "existing=") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "candidate=") != null);
}

test "validateGrammar rejects unknown symbols" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%start stmt
        \\%token SELECT
        \\stmt:
        \\    SELECT missing_rule
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectError(error.UnknownSymbol, validateGrammar(grammar));
}
