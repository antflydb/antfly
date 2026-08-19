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
    token_aliases: std.ArrayListUnmanaged(TokenAlias) = .empty,
    token_precedences: std.ArrayListUnmanaged(TokenPrecedence) = .empty,
    precedence_level_count: u16 = 0,
    rules: std.ArrayListUnmanaged(Rule) = .empty,
};

pub const TokenAlias = struct {
    literal: []const u8,
    token: []const u8,
};

pub const PrecedenceAssociativity = enum {
    left,
    right,
    nonassoc,
};

pub const Precedence = struct {
    level: u16,
    associativity: PrecedenceAssociativity,
};

pub const TokenPrecedence = struct {
    token: []const u8,
    precedence: Precedence,
};

pub const Rule = struct {
    name: []const u8,
    alternatives: std.ArrayListUnmanaged(Alternative) = .empty,
};

pub const Alternative = struct {
    symbols: []const []const u8,
    precedence_symbol: ?[]const u8 = null,
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
    precedence: ?Precedence = null,
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
    nullable_symbols: []const bool,
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

const PendingDeclaration = union(enum) {
    token,
    precedence: PendingPrecedenceDeclaration,
};

const PendingPrecedenceDeclaration = struct {
    associativity: PrecedenceAssociativity,
    level: u16,
};

/// Generates Zig source. The caller owns the returned slice and must free it
/// with `allocator`; all parser-generator working memory is released before
/// this function returns.
pub fn generateZigMetadata(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    source: []const u8,
) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const grammar = try parseGrammar(arena, source);
    try validateGrammar(grammar);
    const tables = try buildSlrTables(arena, grammar);
    if (grammar.expected_conflicts) |expected_conflicts| {
        if (tables.conflicts.len != expected_conflicts) return error.ConflictCountMismatch;
    }
    const generated = try emitZigMetadata(arena, input_path, source, grammar, tables);
    return allocator.dupe(u8, generated);
}

/// Computes the conflict count without transferring any allocated state to the
/// caller.
pub fn conflictExpectation(allocator: std.mem.Allocator, source: []const u8) !ConflictExpectation {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const grammar = try parseGrammar(arena, source);
    try validateGrammar(grammar);
    const tables = try buildSlrTables(arena, grammar);
    return .{
        .expected = grammar.expected_conflicts,
        .actual = tables.conflicts.len,
    };
}

/// Formats a conflict report. The caller owns the returned slice and must free
/// it with `allocator`.
pub fn conflictReportAlloc(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    source: []const u8,
    max_conflicts: usize,
) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const grammar = try parseGrammar(arena, source);
    try validateGrammar(grammar);
    const tables = try buildSlrTables(arena, grammar);

    var out: std.ArrayListUnmanaged(u8) = .empty;
    try appendFmt(arena, &out,
        \\grammar conflict report: {s}
        \\expected: {s}
        \\actual: {d}
        \\
    , .{
        input_path,
        if (grammar.expected_conflicts) |expected|
            try std.fmt.allocPrint(arena, "{d}", .{expected})
        else
            "unset",
        tables.conflicts.len,
    });
    if (tables.conflicts.len == 0) return allocator.dupe(u8, out.items);

    const limit = @min(max_conflicts, tables.conflicts.len);
    try appendFmt(arena, &out, "first {d} conflicts:\n", .{limit});
    for (tables.conflicts[0..limit]) |conflict| {
        try appendFmt(
            arena,
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
        try appendFmt(arena, &out, "  ... {d} more conflicts\n", .{tables.conflicts.len - limit});
    }
    return allocator.dupe(u8, out.items);
}

fn parseGrammar(allocator: std.mem.Allocator, source: []const u8) !Grammar {
    var grammar: Grammar = .{};
    var active_rule: ?usize = null;
    var skip_percent_prologue = false;
    var skip_brace_block_depth: usize = 0;
    var skip_block_comment = false;
    var pending_declaration: ?PendingDeclaration = null;
    var section: enum { declarations, rules, epilogue } = .declarations;

    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |raw_line| {
        // The epilogue is arbitrary host-language code, not yacc syntax.
        if (section == .epilogue) continue;
        if (skip_percent_prologue) {
            if (std.mem.indexOf(u8, raw_line, "%}") != null) skip_percent_prologue = false;
            continue;
        }
        if (skip_brace_block_depth != 0) {
            skip_brace_block_depth = try braceDepthAfterLine(raw_line, skip_brace_block_depth);
            continue;
        }

        const without_comment = try stripCommentsAlloc(allocator, raw_line, &skip_block_comment);
        var line = std.mem.trim(u8, without_comment, " \t\r");
        if (line.len == 0) continue;

        if (std.mem.startsWith(u8, line, "%{")) {
            if (std.mem.indexOf(u8, line, "%}") == null) skip_percent_prologue = true;
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%reference ")) {
            try parseReference(&grammar, line);
            active_rule = null;
            pending_declaration = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%expect ")) {
            const value = std.mem.trim(u8, line["%expect ".len..], " \t\r");
            grammar.expected_conflicts = try std.fmt.parseUnsigned(usize, value, 10);
            active_rule = null;
            pending_declaration = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%start ")) {
            grammar.start_symbol = try dupToken(allocator, line["%start ".len..]);
            active_rule = null;
            pending_declaration = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%left ")) {
            pending_declaration = try parsePrecedenceDeclaration(allocator, &grammar, .left, line["%left ".len..]);
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%right ")) {
            pending_declaration = try parsePrecedenceDeclaration(allocator, &grammar, .right, line["%right ".len..]);
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%nonassoc ")) {
            pending_declaration = try parsePrecedenceDeclaration(allocator, &grammar, .nonassoc, line["%nonassoc ".len..]);
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%precedence ")) {
            pending_declaration = try parsePrecedenceDeclaration(allocator, &grammar, .nonassoc, line["%precedence ".len..]);
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%token ")) {
            try parseTokens(allocator, &grammar, line["%token ".len..]);
            pending_declaration = .token;
            active_rule = null;
            continue;
        }
        if (std.mem.startsWith(u8, line, "%type ") or std.mem.startsWith(u8, line, "%nterm ")) {
            active_rule = null;
            pending_declaration = null;
            continue;
        }
        if (isIgnoredBisonDirective(line)) {
            skip_brace_block_depth = try braceDepthAfterLine(line, 0);
            active_rule = null;
            pending_declaration = null;
            continue;
        }
        if (std.mem.eql(u8, line, "%%")) {
            section = switch (section) {
                .declarations => .rules,
                .rules => .epilogue,
                .epilogue => unreachable,
            };
            active_rule = null;
            pending_declaration = null;
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

        if (active_rule == null) {
            if (indexOfRuleColon(line)) |colon| {
                pending_declaration = null;
                const name = std.mem.trim(u8, line[0..colon], " \t\r");
                if (name.len == 0) return error.InvalidRuleName;
                try grammar.rules.append(allocator, .{ .name = try dupToken(allocator, name) });
                active_rule = grammar.rules.items.len - 1;
                const rest = std.mem.trim(u8, line[colon + 1 ..], " \t\r");
                if (rest.len != 0) {
                    if (try splitUnclosedActionBlockPrefix(rest)) |split| {
                        const prefix = std.mem.trim(u8, split.prefix, " \t\r");
                        if (prefix.len != 0) try appendAlternatives(allocator, &grammar, &grammar.rules.items[active_rule.?], prefix);
                        skip_brace_block_depth = split.depth;
                    } else {
                        try appendAlternatives(allocator, &grammar, &grammar.rules.items[active_rule.?], rest);
                    }
                }
                if (ends_rule) active_rule = null;
                continue;
            }
            if (pending_declaration) |pending| {
                switch (pending) {
                    .token => try parseTokens(allocator, &grammar, line),
                    .precedence => |precedence| try parsePrecedenceItems(allocator, &grammar, precedence.associativity, precedence.level, line),
                }
                continue;
            }
        }

        if (active_rule) |rule_idx| {
            if (line[0] == '{') {
                skip_brace_block_depth = try braceDepthAfterLine(line, 0);
                continue;
            }
            const starts_new_alternative = std.mem.startsWith(u8, line, "|");
            const alt = if (std.mem.startsWith(u8, line, "|"))
                std.mem.trim(u8, line[1..], " \t\r")
            else
                line;
            if (try splitUnclosedActionBlockPrefix(alt)) |split| {
                const prefix = std.mem.trim(u8, split.prefix, " \t\r");
                if (prefix.len != 0) {
                    if (starts_new_alternative or grammar.rules.items[rule_idx].alternatives.items.len == 0) {
                        try appendAlternatives(allocator, &grammar, &grammar.rules.items[rule_idx], prefix);
                    } else {
                        try appendAlternativeContinuation(allocator, &grammar, &grammar.rules.items[rule_idx], prefix);
                    }
                }
                skip_brace_block_depth = split.depth;
            } else {
                if (starts_new_alternative or grammar.rules.items[rule_idx].alternatives.items.len == 0) {
                    try appendAlternatives(allocator, &grammar, &grammar.rules.items[rule_idx], alt);
                } else {
                    try appendAlternativeContinuation(allocator, &grammar, &grammar.rules.items[rule_idx], alt);
                }
            }
            if (ends_rule) active_rule = null;
            continue;
        }

        return error.UnrecognizedGrammarLine;
    }

    return grammar;
}

fn indexOfRuleColon(line: []const u8) ?usize {
    var index: usize = 0;
    var quote: ?u8 = null;
    while (index < line.len) : (index += 1) {
        if ((line[index] == '\'' or line[index] == '"') and (index == 0 or line[index - 1] != '\\')) {
            if (quote == line[index]) {
                quote = null;
            } else if (quote == null) {
                quote = line[index];
            }
            continue;
        }
        if (quote == null and line[index] == ':') return index;
    }
    return null;
}

fn isIgnoredBisonDirective(line: []const u8) bool {
    const prefixes = [_][]const u8{
        "%code",
        "%debug",
        "%define",
        "%destructor",
        "%expect-rr",
        "%glr-parser",
        "%initial-action",
        "%language",
        "%lex-param",
        "%locations",
        "%name-prefix",
        "%no-lines",
        "%parse-param",
        "%printer",
        "%pure-parser",
        "%require",
        "%skeleton",
        "%union",
        "%verbose",
        "%yacc",
    };
    for (prefixes) |prefix| {
        if (std.mem.eql(u8, line, prefix)) return true;
        if (line.len > prefix.len and std.mem.startsWith(u8, line, prefix) and std.ascii.isWhitespace(line[prefix.len])) return true;
    }
    return false;
}

fn braceDepthAfterLine(line: []const u8, initial_depth: usize) !usize {
    var depth = initial_depth;
    var index: usize = 0;
    while (index < line.len) : (index += 1) {
        switch (line[index]) {
            '\'' => try skipQuotedFragment(line, &index, '\''),
            '"' => try skipQuotedFragment(line, &index, '"'),
            '{' => depth += 1,
            '}' => {
                if (depth == 0) return error.InvalidActionBlock;
                depth -= 1;
            },
            else => {},
        }
    }
    return depth;
}

const UnclosedActionSplit = struct {
    prefix: []const u8,
    depth: usize,
};

fn splitUnclosedActionBlockPrefix(line: []const u8) !?UnclosedActionSplit {
    var index: usize = 0;
    var quote: ?u8 = null;
    while (index < line.len) : (index += 1) {
        if ((line[index] == '\'' or line[index] == '"') and (index == 0 or line[index - 1] != '\\')) {
            if (quote == line[index]) {
                quote = null;
            } else if (quote == null) {
                quote = line[index];
            }
            continue;
        }
        if (quote == null and line[index] == '{') {
            const depth = try braceDepthAfterLine(line[index..], 0);
            if (depth != 0) return .{ .prefix = line[0..index], .depth = depth };
        }
    }
    return null;
}

fn stripCommentsAlloc(allocator: std.mem.Allocator, line: []const u8, in_block_comment: *bool) ![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    var index: usize = 0;
    var quote: ?u8 = null;
    while (index < line.len) {
        if (in_block_comment.*) {
            if (index + 1 < line.len and line[index] == '*' and line[index + 1] == '/') {
                in_block_comment.* = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }

        if ((line[index] == '\'' or line[index] == '"') and (index == 0 or line[index - 1] != '\\')) {
            if (quote == line[index]) {
                quote = null;
            } else if (quote == null) {
                quote = line[index];
            }
            try out.append(allocator, line[index]);
            index += 1;
            continue;
        }
        if (quote == null and index + 1 < line.len and line[index] == '/' and line[index + 1] == '*') {
            if (std.mem.indexOfPos(u8, line, index + 2, "*/")) |end_comment| {
                if (isEmptyBlockCommentContent(line[index + 2 .. end_comment])) {
                    try out.appendSlice(allocator, " %empty ");
                }
            }
            in_block_comment.* = true;
            index += 2;
            continue;
        }
        if (quote == null and index + 1 < line.len and line[index] == '/' and line[index + 1] == '/') {
            if (index == 0 or std.ascii.isWhitespace(line[index - 1])) break;
        }
        try out.append(allocator, line[index]);
        index += 1;
    }
    return try out.toOwnedSlice(allocator);
}

fn isEmptyBlockCommentContent(content: []const u8) bool {
    const trimmed = std.mem.trim(u8, content, " \t\r\n");
    return std.ascii.eqlIgnoreCase(trimmed, "empty");
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
    const parts = try declarationPartsAlloc(allocator, text);
    var previous_token: ?[]const u8 = null;
    var previous_can_alias = false;
    for (parts, 0..) |name, index| {
        if (isLiteralAlias(name)) {
            const next_is_literal = index + 1 < parts.len and isLiteralAlias(parts[index + 1]);
            const token_name = if (previous_can_alias and !next_is_literal)
                previous_token orelse return error.InvalidTokenAlias
            else
                try ensureLiteralTerminal(allocator, grammar, name);
            try appendTokenAlias(allocator, grammar, name, token_name);
            previous_token = token_name;
            previous_can_alias = false;
            continue;
        }
        previous_token = try appendTokenName(allocator, grammar, name);
        previous_can_alias = true;
    }
}

fn parsePrecedenceDeclaration(
    allocator: std.mem.Allocator,
    grammar: *Grammar,
    associativity: PrecedenceAssociativity,
    text: []const u8,
) !PendingDeclaration {
    grammar.precedence_level_count += 1;
    const level = grammar.precedence_level_count;
    try parsePrecedenceItems(allocator, grammar, associativity, level, text);
    return .{ .precedence = .{ .associativity = associativity, .level = level } };
}

fn parsePrecedenceItems(
    allocator: std.mem.Allocator,
    grammar: *Grammar,
    associativity: PrecedenceAssociativity,
    level: u16,
    text: []const u8,
) !void {
    const parts = try declarationPartsAlloc(allocator, text);
    var previous_token: ?[]const u8 = null;
    var previous_can_alias = false;
    for (parts, 0..) |name, index| {
        if (isLiteralAlias(name)) {
            const next_is_literal = index + 1 < parts.len and isLiteralAlias(parts[index + 1]);
            const token_name = if (previous_can_alias and !next_is_literal)
                previous_token orelse return error.InvalidTokenAlias
            else
                resolveTerminalName(grammar.*, name) orelse try ensureLiteralTerminal(allocator, grammar, name);
            try appendTokenAlias(allocator, grammar, name, token_name);
            try setTokenPrecedence(allocator, grammar, token_name, .{ .level = level, .associativity = associativity });
            previous_token = token_name;
            previous_can_alias = false;
            continue;
        }
        const token_name = try appendTokenName(allocator, grammar, name);
        try setTokenPrecedence(allocator, grammar, token_name, .{ .level = level, .associativity = associativity });
        previous_token = token_name;
        previous_can_alias = true;
    }
}

fn declarationPartsAlloc(allocator: std.mem.Allocator, text: []const u8) ![]const []const u8 {
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    var parts = std.mem.tokenizeAny(u8, text, " \t\r");
    while (parts.next()) |name| {
        if (isBisonTypeTag(name) or isBisonTokenCode(name)) continue;
        try out.append(allocator, name);
    }
    return try out.toOwnedSlice(allocator);
}

fn appendAlternative(allocator: std.mem.Allocator, grammar: *Grammar, rule: *Rule, text: []const u8) !void {
    const trimmed = std.mem.trim(u8, text, " \t\r");
    if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "%empty") or std.mem.eql(u8, trimmed, "/* empty */")) {
        try rule.alternatives.append(allocator, .{ .symbols = &.{} });
        return;
    }

    var symbols: std.ArrayListUnmanaged([]const u8) = .empty;
    var index: usize = 0;
    var precedence_symbol: ?[]const u8 = null;
    while (index < trimmed.len) {
        const symbol = (try nextGrammarSymbol(trimmed, &index)) orelse break;
        if (std.mem.eql(u8, symbol, "%prec")) {
            const override = (try nextGrammarSymbol(trimmed, &index)) orelse return error.InvalidPrecedenceOverride;
            if (isLiteralAlias(override)) _ = try ensureLiteralTerminal(allocator, grammar, override);
            precedence_symbol = try dupToken(allocator, override);
            continue;
        }
        if (std.mem.eql(u8, symbol, "%dprec") or std.mem.eql(u8, symbol, "%merge")) {
            _ = (try nextGrammarSymbol(trimmed, &index)) orelse return error.InvalidProductionAnnotation;
            continue;
        }
        if (isLiteralAlias(symbol)) _ = try ensureLiteralTerminal(allocator, grammar, symbol);
        try symbols.append(allocator, try dupToken(allocator, symbol));
    }
    try rule.alternatives.append(allocator, .{ .symbols = try symbols.toOwnedSlice(allocator), .precedence_symbol = precedence_symbol });
}

fn appendAlternatives(allocator: std.mem.Allocator, grammar: *Grammar, rule: *Rule, text: []const u8) !void {
    var start: usize = 0;
    var index: usize = 0;
    while (index < text.len) {
        switch (text[index]) {
            '\'' => {
                try skipQuotedFragment(text, &index, '\'');
                index += 1;
            },
            '"' => {
                try skipQuotedFragment(text, &index, '"');
                index += 1;
            },
            '{' => try skipActionBlock(text, &index),
            '|' => {
                try appendAlternative(allocator, grammar, rule, text[start..index]);
                index += 1;
                start = index;
            },
            else => index += 1,
        }
    }
    try appendAlternative(allocator, grammar, rule, text[start..]);
}

fn appendAlternativeContinuation(allocator: std.mem.Allocator, grammar: *Grammar, rule: *Rule, text: []const u8) !void {
    if (rule.alternatives.items.len == 0) {
        try appendAlternatives(allocator, grammar, rule, text);
        return;
    }

    var parsed: Rule = .{ .name = rule.name };
    try appendAlternative(allocator, grammar, &parsed, text);
    if (parsed.alternatives.items.len == 0) return;
    const continuation = parsed.alternatives.items[0];
    if (continuation.symbols.len == 0 and continuation.precedence_symbol == null) return;

    const target = &rule.alternatives.items[rule.alternatives.items.len - 1];
    if (continuation.symbols.len != 0) {
        var symbols = try allocator.alloc([]const u8, target.symbols.len + continuation.symbols.len);
        @memcpy(symbols[0..target.symbols.len], target.symbols);
        @memcpy(symbols[target.symbols.len..], continuation.symbols);
        target.symbols = symbols;
    }
    if (continuation.precedence_symbol) |precedence_symbol| {
        target.precedence_symbol = precedence_symbol;
    }
}

fn nextGrammarSymbol(text: []const u8, index: *usize) !?[]const u8 {
    while (true) {
        while (index.* < text.len and std.ascii.isWhitespace(text[index.*])) index.* += 1;
        if (index.* + 1 < text.len and text[index.*] == '/' and text[index.* + 1] == '*') {
            index.* += 2;
            while (index.* + 1 < text.len and !(text[index.*] == '*' and text[index.* + 1] == '/')) index.* += 1;
            if (index.* + 1 >= text.len) return null;
            index.* += 2;
            continue;
        }
        break;
    }
    if (index.* >= text.len) return null;
    if (text[index.*] == '{') {
        try skipActionBlock(text, index);
        return try nextGrammarSymbol(text, index);
    }

    const start = index.*;
    if (text[index.*] == '\'' or text[index.*] == '"') {
        const quote = text[index.*];
        index.* += 1;
        while (index.* < text.len) : (index.* += 1) {
            if (text[index.*] == quote and text[index.* - 1] != '\\') {
                index.* += 1;
                break;
            }
        }
        if (index.* > text.len or text[index.* - 1] != quote) return error.UnterminatedLiteralTerminal;
    } else {
        while (index.* < text.len and !std.ascii.isWhitespace(text[index.*])) index.* += 1;
    }
    return text[start..index.*];
}

fn skipActionBlock(text: []const u8, index: *usize) !void {
    if (text[index.*] != '{') return error.InvalidActionBlock;
    var depth: usize = 1;
    index.* += 1;
    while (index.* < text.len) : (index.* += 1) {
        switch (text[index.*]) {
            '\'' => try skipQuotedFragment(text, index, '\''),
            '"' => try skipQuotedFragment(text, index, '"'),
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if (depth == 0) {
                    index.* += 1;
                    return;
                }
            },
            else => {},
        }
    }
    return error.UnterminatedActionBlock;
}

fn skipQuotedFragment(text: []const u8, index: *usize, quote: u8) !void {
    index.* += 1;
    while (index.* < text.len) : (index.* += 1) {
        if (text[index.*] == '\\') {
            if (index.* + 1 < text.len) index.* += 1;
            continue;
        }
        if (text[index.*] == quote) return;
    }
    return error.UnterminatedQuotedFragment;
}

pub fn validateGrammar(grammar: Grammar) !void {
    if (grammar.start_symbol.len == 0) return error.MissingStartSymbol;
    if (grammar.tokens.items.len == 0) return error.MissingTokens;
    if (grammar.rules.items.len == 0) return error.MissingRules;
    if (findRule(grammar, grammar.start_symbol) == null) return error.UnknownStartSymbol;
    for (grammar.rules.items) |rule| {
        if (rule.alternatives.items.len == 0) return error.EmptyRule;
        for (rule.alternatives.items) |alt| {
            if (alt.precedence_symbol) |precedence_symbol| {
                if (tokenPrecedenceForSymbol(grammar, precedence_symbol) == null) return error.UnknownPrecedenceSymbol;
            }
            for (alt.symbols) |symbol| {
                if (resolveTerminalName(grammar, symbol) == null and findRule(grammar, symbol) == null) {
                    return error.UnknownSymbol;
                }
            }
        }
    }
}

fn buildSlrTables(allocator: std.mem.Allocator, grammar: Grammar) !Tables {
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
            try productions.append(allocator, .{
                .lhs = lhs,
                .rhs = try rhs.toOwnedSlice(allocator),
                .precedence = productionPrecedence(grammar, alt),
            });
        }
    }

    const terminal_precedences = try terminalPrecedences(allocator, grammar, terminal_count);
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
                .state = try tableIndex(state_idx),
                .nonterminal = next_symbol,
                .target = try tableIndex(target),
            });
        }
    }

    const action_slot_count = try std.math.mul(usize, states.items.len, terminal_count);
    const action_slots = try allocator.alloc(?Action, action_slot_count);
    defer allocator.free(action_slots);
    @memset(action_slots, null);

    var gotos: std.ArrayListUnmanaged(Goto) = .empty;
    var conflicts: std.ArrayListUnmanaged(Conflict) = .empty;

    for (states.items, 0..) |state, i| {
        for (transitions.items) |transition| {
            if (transition.state != i) continue;
            if (symbols.items[transition.nonterminal].kind == .terminal) {
                try addAction(allocator, action_slots, terminal_count, &conflicts, .{
                    .state = try tableIndex(i),
                    .terminal = transition.nonterminal,
                    .kind = .shift,
                    .target = transition.target,
                }, productions.items, terminal_precedences);
            } else {
                try gotos.append(allocator, transition);
            }
        }

        for (state.items) |item| {
            const production = productions.items[item.production];
            if (item.dot != production.rhs.len) continue;
            if (item.production == 0) {
                try addAction(allocator, action_slots, terminal_count, &conflicts, .{
                    .state = try tableIndex(i),
                    .terminal = eof_symbol,
                    .kind = .accept,
                    .target = 0,
                }, productions.items, terminal_precedences);
                continue;
            }
            for (follow[production.lhs]) |terminal| {
                try addAction(allocator, action_slots, terminal_count, &conflicts, .{
                    .state = try tableIndex(i),
                    .terminal = terminal,
                    .kind = .reduce,
                    .target = item.production,
                }, productions.items, terminal_precedences);
            }
        }
    }

    var actions: std.ArrayListUnmanaged(Action) = .empty;
    var action_count: usize = 0;
    for (action_slots) |action| {
        if (action != null) action_count += 1;
    }
    try actions.ensureTotalCapacity(allocator, action_count);
    for (action_slots) |action| {
        if (action) |present| actions.appendAssumeCapacity(present);
    }
    sortGotos(gotos.items);
    const symbol_count = symbols.items.len;
    return .{
        .symbols = try symbols.toOwnedSlice(allocator),
        .terminal_count = terminal_count,
        .nonterminal_count = symbol_count - terminal_count,
        .productions = try productions.toOwnedSlice(allocator),
        .nullable_symbols = nullable,
        .states = try states.toOwnedSlice(allocator),
        .actions = try actions.toOwnedSlice(allocator),
        .gotos = try gotos.toOwnedSlice(allocator),
        .conflicts = try conflicts.toOwnedSlice(allocator),
        .eof_symbol = eof_symbol,
        .augmented_start_symbol = augmented_start_symbol,
    };
}

fn tableIndex(value: usize) !u16 {
    if (value > std.math.maxInt(u16)) return error.GrammarTooLarge;
    return @intCast(value);
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
    action_slots: []?Action,
    terminal_count: usize,
    conflicts: *std.ArrayListUnmanaged(Conflict),
    candidate: Action,
    productions: []const Production,
    terminal_precedences: []const ?Precedence,
) !void {
    const slot_index = @as(usize, candidate.state) * terminal_count + candidate.terminal;
    std.debug.assert(slot_index < action_slots.len);
    const slot = &action_slots[slot_index];
    if (slot.*) |existing| {
        if (std.meta.eql(existing, candidate)) return;
        if (precedenceResolution(candidate, existing, productions, terminal_precedences)) |resolution| {
            slot.* = switch (resolution) {
                .candidate => candidate,
                .existing => existing,
                .none => null,
            };
            return;
        }
        try conflicts.append(allocator, .{
            .state = candidate.state,
            .terminal = candidate.terminal,
            .existing = existing.kind,
            .candidate = candidate.kind,
        });
        if (preferAction(candidate, existing)) slot.* = candidate;
        return;
    }
    slot.* = candidate;
}

const ActionResolution = enum {
    candidate,
    existing,
    none,
};

fn precedenceResolution(
    candidate: Action,
    existing: Action,
    productions: []const Production,
    terminal_precedences: []const ?Precedence,
) ?ActionResolution {
    if (candidate.kind == existing.kind) return null;
    const shift_action = if (candidate.kind == .shift and existing.kind == .reduce)
        candidate
    else if (existing.kind == .shift and candidate.kind == .reduce)
        existing
    else
        return null;
    const reduce_action = if (candidate.kind == .reduce) candidate else existing;
    if (shift_action.terminal >= terminal_precedences.len or reduce_action.target >= productions.len) return null;
    const shift_precedence = terminal_precedences[shift_action.terminal] orelse return null;
    const reduce_precedence = productions[reduce_action.target].precedence orelse return null;

    const chosen = if (shift_precedence.level > reduce_precedence.level)
        shift_action
    else if (shift_precedence.level < reduce_precedence.level)
        reduce_action
    else switch (shift_precedence.associativity) {
        .left => reduce_action,
        .right => shift_action,
        .nonassoc => return .none,
    };
    if (std.meta.eql(chosen, candidate)) return .candidate;
    return .existing;
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
        \\pub const nullable_symbol_count = {d};
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
        nullableSymbolCount(tables.nullable_symbols),
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
        \\const nullable_symbols = [_]bool{
        \\
    );
    for (tables.nullable_symbols) |nullable_symbol| {
        try appendFmt(allocator, &out, "    {s},\n", .{if (nullable_symbol) "true" else "false"});
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
        \\pub fn terminalName(terminal: u16) ?[]const u8 {
        \\    if (terminal == 0) return "$end";
        \\    inline for (std.meta.fields(Token)) |field| {
        \\        if (terminal == @intFromEnum(@field(Token, field.name)) + 1) return field.name;
        \\    }
        \\    return null;
        \\}
        \\
        \\const Rule = enum {
        \\
    );
    for (grammar.rules.items) |rule| try appendFmt(allocator, &out, "    {s},\n", .{rule.name});
    try out.appendSlice(allocator,
        \\};
        \\
        \\pub const RuleId = enum(u16) {
        \\
    );
    for (grammar.rules.items) |rule| try appendFmt(allocator, &out, "    {s},\n", .{rule.name});
    try out.appendSlice(allocator,
        \\};
        \\
        \\pub fn ruleName(rule: RuleId) []const u8 {
        \\    return @tagName(rule);
        \\}
        \\
        \\pub const ProductionInfo = struct {
        \\    rule: ?RuleId,
        \\    lhs: u16,
        \\    rhs_len: u16,
        \\};
        \\
        \\pub fn productionInfo(production: u16) ?ProductionInfo {
        \\    if (production >= productions.len) return null;
        \\    const item = productions[production];
        \\    return .{
        \\        .rule = ruleIdForLhs(item.lhs),
        \\        .lhs = item.lhs,
        \\        .rhs_len = item.rhs_len,
        \\    };
        \\}
        \\
        \\pub fn productionRhs(production: u16) ?[]const u16 {
        \\    if (production >= productions.len) return null;
        \\    const item = productions[production];
        \\    const start: usize = @intCast(item.rhs_start);
        \\    return production_rhs[start .. start + item.rhs_len];
        \\}
        \\
        \\pub fn symbolRule(symbol: u16) ?RuleId {
        \\    return ruleIdForLhs(symbol);
        \\}
        \\
        \\pub fn symbolIsNullable(symbol: u16) ?bool {
        \\    if (symbol >= nullable_symbols.len) return null;
        \\    return nullable_symbols[symbol];
        \\}
        \\
        \\fn ruleIdForLhs(lhs: u16) ?RuleId {
        \\    return switch (lhs) {
        \\
    );
    const terminal_count = grammar.tokens.items.len + 1;
    for (grammar.rules.items, 0..) |rule, rule_idx| {
        try appendFmt(allocator, &out, "        {d} => .{s},\n", .{ ruleSymbolId(terminal_count, rule_idx), rule.name });
    }
    try out.appendSlice(allocator,
        \\        else => null,
        \\    };
        \\}
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
        \\    @sizeOf(@TypeOf(nullable_symbols)) +
        \\    @sizeOf(@TypeOf(production_rhs)) +
        \\    @sizeOf(@TypeOf(productions)) +
        \\    @sizeOf(@TypeOf(actions)) +
        \\    @sizeOf(@TypeOf(action_ranges)) +
        \\    @sizeOf(@TypeOf(gotos)) +
        \\    @sizeOf(@TypeOf(goto_ranges));
        \\pub const parse_table_estimated_bytes = parse_table_static_bytes + symbol_name_bytes;
        \\
    , .{ production_rhs_count, state_item_count, symbol_name_bytes, action_range_max, goto_range_max });
    try out.appendSlice(allocator,
        \\
        \\const ParseError = error{
        \\    InvalidFallbackTokenCount,
        \\    InvalidGoto,
        \\    StackOverflow,
        \\    StackUnderflow,
        \\    UnexpectedToken,
        \\};
        \\
        \\const ParseErrorInfo = struct {
        \\    state: u16,
        \\    lookahead: u16,
        \\    token_index: usize,
        \\};
        \\
        \\pub const ParseDiagnostic = struct {
        \\    token_index: usize,
        \\    expected: []const []const u8,
        \\
        \\    pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        \\        allocator.free(self.expected);
        \\    }
        \\};
        \\
        \\pub fn parse(allocator: std.mem.Allocator, token_ids: []const u16) !void {
        \\    var stack = DynamicStateStack.init(allocator);
        \\    defer stack.deinit();
        \\    try stack.push(0);
        \\    const failure = try parseCore(token_ids, null, &stack, IgnoreEvents{});
        \\    if (failure) |item| return item.parse_error;
        \\}
        \\
        \\pub fn parseWithFallback(allocator: std.mem.Allocator, token_ids: []const u16, fallback_token_ids: []const u16) !void {
        \\    if (fallback_token_ids.len != token_ids.len) return ParseError.InvalidFallbackTokenCount;
        \\    var stack = DynamicStateStack.init(allocator);
        \\    defer stack.deinit();
        \\    try stack.push(0);
        \\    const failure = try parseCore(token_ids, fallback_token_ids, &stack, IgnoreEvents{});
        \\    if (failure) |item| return item.parse_error;
        \\}
        \\
        \\pub fn parseWithStackBuffer(token_ids: []const u16, stack_buffer: []u16) !void {
        \\    var stack = try FixedStateStack.init(stack_buffer);
        \\    const failure = try parseCore(token_ids, null, &stack, IgnoreEvents{});
        \\    if (failure) |item| return item.parse_error;
        \\}
        \\
        \\pub fn parseWithFallbackStackBuffer(token_ids: []const u16, fallback_token_ids: []const u16, stack_buffer: []u16) !void {
        \\    if (fallback_token_ids.len != token_ids.len) return ParseError.InvalidFallbackTokenCount;
        \\    var stack = try FixedStateStack.init(stack_buffer);
        \\    const failure = try parseCore(token_ids, fallback_token_ids, &stack, IgnoreEvents{});
        \\    if (failure) |item| return item.parse_error;
        \\}
        \\
        \\pub const Reduction = struct {
        \\    production: u16,
        \\    lhs: u16,
        \\    rhs_len: u16,
        \\};
        \\
        \\pub const Shift = struct {
        \\    token_index: usize,
        \\    terminal: u16,
        \\};
        \\
        \\pub const Accept = struct {
        \\    token_count: usize,
        \\};
        \\
        \\pub fn parseWithReductions(token_ids: []const u16, stack_buffer: []u16, reducer: anytype) !void {
        \\    const Adapter = struct {
        \\        inner: @TypeOf(reducer),
        \\
        \\        pub fn shift(_: @This(), _: Shift) !void {}
        \\
        \\        pub fn reduce(self: @This(), reduction: Reduction) !void {
        \\            try self.inner.reduce(reduction);
        \\        }
        \\
        \\        pub fn accept(_: @This(), _: Accept) !void {}
        \\    };
        \\    return parseWithEvents(token_ids, stack_buffer, Adapter{ .inner = reducer });
        \\}
        \\
        \\pub fn parseWithEvents(token_ids: []const u16, stack_buffer: []u16, event_handler: anytype) !void {
        \\    var stack = try FixedStateStack.init(stack_buffer);
        \\    const failure = try parseCore(token_ids, null, &stack, event_handler);
        \\    if (failure) |item| return item.parse_error;
        \\}
        \\
        \\fn maybeAccept(event_handler: anytype, accept: Accept) !void {
        \\    const Handler = switch (@typeInfo(@TypeOf(event_handler))) {
        \\        .pointer => |pointer| pointer.child,
        \\        else => @TypeOf(event_handler),
        \\    };
        \\    if (comptime @hasDecl(Handler, "accept")) try event_handler.accept(accept);
        \\}
        \\
        \\const ParseFailure = struct {
        \\    parse_error: ParseError,
        \\    info: ParseErrorInfo,
        \\};
        \\
        \\const DynamicStateStack = struct {
        \\    allocator: std.mem.Allocator,
        \\    states: std.ArrayListUnmanaged(u16) = .empty,
        \\
        \\    fn init(allocator: std.mem.Allocator) @This() {
        \\        return .{ .allocator = allocator };
        \\    }
        \\
        \\    fn deinit(self: *@This()) void {
        \\        self.states.deinit(self.allocator);
        \\    }
        \\
        \\    fn push(self: *@This(), state: u16) !void {
        \\        try self.states.append(self.allocator, state);
        \\    }
        \\
        \\    fn top(self: *const @This()) u16 {
        \\        return self.states.items[self.states.items.len - 1];
        \\    }
        \\
        \\    fn canPop(self: *const @This(), count: u16) bool {
        \\        return count <= self.states.items.len - 1;
        \\    }
        \\
        \\    fn pop(self: *@This(), count: u16) void {
        \\        self.states.items.len -= count;
        \\    }
        \\};
        \\
        \\const FixedStateStack = struct {
        \\    states: []u16,
        \\    len: usize,
        \\
        \\    fn init(buffer: []u16) !@This() {
        \\        if (buffer.len == 0) return ParseError.StackOverflow;
        \\        buffer[0] = 0;
        \\        return .{ .states = buffer, .len = 1 };
        \\    }
        \\
        \\    fn push(self: *@This(), state: u16) !void {
        \\        if (self.len == self.states.len) return ParseError.StackOverflow;
        \\        self.states[self.len] = state;
        \\        self.len += 1;
        \\    }
        \\
        \\    fn top(self: *const @This()) u16 {
        \\        return self.states[self.len - 1];
        \\    }
        \\
        \\    fn canPop(self: *const @This(), count: u16) bool {
        \\        return count <= self.len - 1;
        \\    }
        \\
        \\    fn pop(self: *@This(), count: u16) void {
        \\        self.len -= count;
        \\    }
        \\};
        \\
        \\const IgnoreEvents = struct {
        \\    fn shift(_: @This(), _: Shift) !void {}
        \\    fn reduce(_: @This(), _: Reduction) !void {}
        \\};
        \\
        \\fn parseFailure(parse_error: ParseError, state: u16, lookahead: u16, token_index: usize) ParseFailure {
        \\    return .{
        \\        .parse_error = parse_error,
        \\        .info = .{ .state = state, .lookahead = lookahead, .token_index = token_index },
        \\    };
        \\}
        \\
        \\fn parseCore(token_ids: []const u16, fallback_token_ids: ?[]const u16, stack: anytype, event_handler: anytype) !?ParseFailure {
        \\    var index: usize = 0;
        \\    while (true) {
        \\        const state = stack.top();
        \\        const lookahead: u16 = if (index < token_ids.len) token_ids[index] else 0;
        \\        var effective_lookahead = lookahead;
        \\        const action = findAction(state, lookahead) orelse action: {
        \\            if (fallback_token_ids) |fallbacks| {
        \\                if (index < fallbacks.len and fallbacks[index] != 0) {
        \\                    if (findAction(state, fallbacks[index])) |fallback_action| {
        \\                        effective_lookahead = fallbacks[index];
        \\                        break :action fallback_action;
        \\                    }
        \\                }
        \\            }
        \\            return parseFailure(ParseError.UnexpectedToken, state, lookahead, index);
        \\        };
        \\        switch (action.kind) {
        \\            .shift => {
        \\                try event_handler.shift(.{ .token_index = index, .terminal = effective_lookahead });
        \\                try stack.push(action.target);
        \\                index += 1;
        \\            },
        \\            .reduce => {
        \\                const production = productions[action.target];
        \\                if (!stack.canPop(production.rhs_len))
        \\                    return parseFailure(ParseError.StackUnderflow, state, lookahead, index);
        \\                try event_handler.reduce(.{
        \\                    .production = action.target,
        \\                    .lhs = production.lhs,
        \\                    .rhs_len = production.rhs_len,
        \\                });
        \\                stack.pop(production.rhs_len);
        \\                const goto_state = stack.top();
        \\                const goto_entry = findGoto(goto_state, production.lhs) orelse
        \\                    return parseFailure(ParseError.InvalidGoto, goto_state, production.lhs, index);
        \\                try stack.push(goto_entry.target);
        \\            },
        \\            .accept => {
        \\                try maybeAccept(event_handler, .{ .token_count = index });
        \\                return null;
        \\            },
        \\        }
        \\    }
        \\}
        \\
        \\pub fn parseDiagnostic(allocator: std.mem.Allocator, token_ids: []const u16) !?ParseDiagnostic {
        \\    const info = try parseError(allocator, token_ids) orelse return null;
        \\    return try parseDiagnosticFromInfo(allocator, info);
        \\}
        \\
        \\pub fn parseDiagnosticWithStackBuffer(allocator: std.mem.Allocator, token_ids: []const u16, stack_buffer: []u16) !?ParseDiagnostic {
        \\    const info = try parseErrorWithStackBuffer(token_ids, stack_buffer) orelse return null;
        \\    return try parseDiagnosticFromInfo(allocator, info);
        \\}
        \\
        \\pub fn parseDiagnosticWithFallback(allocator: std.mem.Allocator, token_ids: []const u16, fallback_token_ids: []const u16) !?ParseDiagnostic {
        \\    if (fallback_token_ids.len != token_ids.len) return ParseError.InvalidFallbackTokenCount;
        \\    var stack = DynamicStateStack.init(allocator);
        \\    defer stack.deinit();
        \\    try stack.push(0);
        \\    const failure = try parseCore(token_ids, fallback_token_ids, &stack, IgnoreEvents{});
        \\    return if (failure) |item| try parseDiagnosticFromInfo(allocator, item.info) else null;
        \\}
        \\
        \\pub fn parseDiagnosticWithFallbackStackBuffer(allocator: std.mem.Allocator, token_ids: []const u16, fallback_token_ids: []const u16, stack_buffer: []u16) !?ParseDiagnostic {
        \\    if (fallback_token_ids.len != token_ids.len) return ParseError.InvalidFallbackTokenCount;
        \\    var stack = try FixedStateStack.init(stack_buffer);
        \\    const failure = try parseCore(token_ids, fallback_token_ids, &stack, IgnoreEvents{});
        \\    return if (failure) |item| try parseDiagnosticFromInfo(allocator, item.info) else null;
        \\}
        \\
        \\fn parseError(allocator: std.mem.Allocator, token_ids: []const u16) !?ParseErrorInfo {
        \\    var stack = DynamicStateStack.init(allocator);
        \\    defer stack.deinit();
        \\    try stack.push(0);
        \\    const failure = try parseCore(token_ids, null, &stack, IgnoreEvents{});
        \\    return if (failure) |item| item.info else null;
        \\}
        \\
        \\fn parseErrorWithStackBuffer(token_ids: []const u16, stack_buffer: []u16) ParseError!?ParseErrorInfo {
        \\    var stack = try FixedStateStack.init(stack_buffer);
        \\    const failure = try parseCore(token_ids, null, &stack, IgnoreEvents{});
        \\    return if (failure) |item| item.info else null;
        \\}
        \\
        \\fn parseDiagnosticFromInfo(allocator: std.mem.Allocator, info: ParseErrorInfo) !ParseDiagnostic {
        \\    return .{
        \\        .token_index = info.token_index,
        \\        .expected = try expectedTerminalNamesAlloc(allocator, info),
        \\    };
        \\}
        \\
        \\fn actionsForState(state: u16) []const Action {
        \\    if (state >= action_ranges.len) return &.{};
        \\    const range = action_ranges[state];
        \\    const start: usize = @intCast(range.start);
        \\    return actions[start .. start + range.len];
        \\}
        \\
        \\fn expectedTerminalNamesAlloc(allocator: std.mem.Allocator, info: ParseErrorInfo) ![]const []const u8 {
        \\    const expected_count = expectedTerminalCountForState(info.state);
        \\    const expected = try allocator.alloc([]const u8, expected_count);
        \\    for (expected, 0..) |*name, idx| name.* = expectedTerminalNameForState(info.state, idx);
        \\    return expected;
        \\}
        \\
        \\fn expectedTerminalCountForState(state: u16) usize {
        \\    return actionsForState(state).len;
        \\}
        \\
        \\fn expectedTerminalNameForState(state: u16, index: usize) []const u8 {
        \\    const state_actions = actionsForState(state);
        \\    if (index >= state_actions.len) return "";
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

fn nullableSymbolCount(nullable_symbols: []const bool) usize {
    var count: usize = 0;
    for (nullable_symbols) |nullable_symbol| {
        if (nullable_symbol) count += 1;
    }
    return count;
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
    if (resolveTerminalIndex(grammar, symbol)) |idx| return @intCast(idx + 1);
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

fn appendTokenName(allocator: std.mem.Allocator, grammar: *Grammar, name: []const u8) ![]const u8 {
    for (grammar.tokens.items) |existing| {
        if (std.mem.eql(u8, existing, name)) return existing;
    }
    const owned = try dupToken(allocator, name);
    try grammar.tokens.append(allocator, owned);
    return owned;
}

fn appendTokenAlias(
    allocator: std.mem.Allocator,
    grammar: *Grammar,
    literal: []const u8,
    token_name: []const u8,
) !void {
    for (grammar.token_aliases.items) |alias| {
        if (std.mem.eql(u8, alias.literal, literal)) {
            if (!std.mem.eql(u8, alias.token, token_name)) return error.ConflictingTokenAlias;
            return;
        }
    }
    try grammar.token_aliases.append(allocator, .{
        .literal = try dupToken(allocator, literal),
        .token = token_name,
    });
}

fn ensureLiteralTerminal(
    allocator: std.mem.Allocator,
    grammar: *Grammar,
    literal: []const u8,
) ![]const u8 {
    if (resolveTerminalName(grammar.*, literal)) |token_name| return token_name;
    const token_name = try literalTokenNameAlloc(allocator, literal);
    const owned_token = try appendTokenName(allocator, grammar, token_name);
    try appendTokenAlias(allocator, grammar, literal, owned_token);
    return owned_token;
}

fn literalTokenNameAlloc(allocator: std.mem.Allocator, literal: []const u8) ![]const u8 {
    const content = if (literal.len >= 2) literal[1 .. literal.len - 1] else literal;
    var out: std.ArrayListUnmanaged(u8) = .empty;
    try out.appendSlice(allocator, "LIT");
    if (content.len == 0) {
        try out.appendSlice(allocator, "_EMPTY");
        return try out.toOwnedSlice(allocator);
    }
    for (content) |byte| {
        try appendFmt(allocator, &out, "_{X:0>2}", .{byte});
    }
    return try out.toOwnedSlice(allocator);
}

fn setTokenPrecedence(
    allocator: std.mem.Allocator,
    grammar: *Grammar,
    token_name: []const u8,
    precedence: Precedence,
) !void {
    for (grammar.token_precedences.items) |*entry| {
        if (std.mem.eql(u8, entry.token, token_name)) {
            entry.precedence = precedence;
            return;
        }
    }
    try grammar.token_precedences.append(allocator, .{
        .token = token_name,
        .precedence = precedence,
    });
}

fn isLiteralAlias(symbol: []const u8) bool {
    return isQuotedLiteral(symbol) or isStringLiteral(symbol);
}

fn isQuotedLiteral(symbol: []const u8) bool {
    return symbol.len >= 2 and symbol[0] == '\'' and symbol[symbol.len - 1] == '\'';
}

fn isStringLiteral(symbol: []const u8) bool {
    return symbol.len >= 2 and symbol[0] == '"' and symbol[symbol.len - 1] == '"';
}

fn isBisonTypeTag(symbol: []const u8) bool {
    return symbol.len >= 2 and symbol[0] == '<' and symbol[symbol.len - 1] == '>';
}

fn isBisonTokenCode(symbol: []const u8) bool {
    if (symbol.len == 0) return false;
    for (symbol) |ch| {
        if (!std.ascii.isDigit(ch)) return false;
    }
    return true;
}

fn resolveTerminalName(grammar: Grammar, symbol: []const u8) ?[]const u8 {
    for (grammar.tokens.items) |token| {
        if (std.mem.eql(u8, token, symbol)) return token;
    }
    for (grammar.token_aliases.items) |alias| {
        if (std.mem.eql(u8, alias.literal, symbol)) return alias.token;
    }
    return null;
}

fn resolveTerminalIndex(grammar: Grammar, symbol: []const u8) ?usize {
    const token_name = resolveTerminalName(grammar, symbol) orelse return null;
    for (grammar.tokens.items, 0..) |token, idx| {
        if (std.mem.eql(u8, token, token_name)) return idx;
    }
    return null;
}

fn tokenPrecedenceForSymbol(grammar: Grammar, symbol: []const u8) ?Precedence {
    const token_name = resolveTerminalName(grammar, symbol) orelse return null;
    for (grammar.token_precedences.items) |entry| {
        if (std.mem.eql(u8, entry.token, token_name)) return entry.precedence;
    }
    return null;
}

fn productionPrecedence(grammar: Grammar, alt: Alternative) ?Precedence {
    if (alt.precedence_symbol) |symbol| return tokenPrecedenceForSymbol(grammar, symbol);
    var index = alt.symbols.len;
    while (index > 0) {
        index -= 1;
        if (tokenPrecedenceForSymbol(grammar, alt.symbols[index])) |precedence| return precedence;
    }
    return null;
}

fn terminalPrecedences(allocator: std.mem.Allocator, grammar: Grammar, terminal_count: usize) ![]?Precedence {
    const out = try allocator.alloc(?Precedence, terminal_count);
    @memset(out, null);
    for (grammar.token_precedences.items) |entry| {
        const idx = resolveTerminalIndex(grammar, entry.token) orelse return error.UnknownPrecedenceSymbol;
        out[idx + 1] = entry.precedence;
    }
    return out;
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

test "parseGrammar resolves yacc literal terminals through symbolic token aliases" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start stmt
        \\%token SELECT IDENT COMMA ',' SLASHES '//'
        \\stmt:
        \\    SELECT identifier_list
        \\  | SLASHES IDENT
        \\  ;
        \\identifier_list:
        \\    IDENT
        \\  | identifier_list ',' IDENT
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 4), grammar.tokens.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.token_aliases.items.len);
    try std.testing.expectEqualStrings("','", grammar.token_aliases.items[0].literal);
    try std.testing.expectEqualStrings("COMMA", grammar.token_aliases.items[0].token);
    try std.testing.expectEqualStrings("'//'", grammar.token_aliases.items[1].literal);
    try std.testing.expectEqualStrings("SLASHES", grammar.token_aliases.items[1].token);
    try validateGrammar(grammar);

    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
    try std.testing.expectEqual(@as(u16, 3), try symbolId(grammar, grammar.tokens.items.len + 1, "COMMA"));
    try std.testing.expectEqual(@as(u16, 3), try symbolId(grammar, grammar.tokens.items.len + 1, "','"));

    const generated = try generateZigMetadata(arena, "literal.y", source);
    try std.testing.expect(std.mem.indexOf(u8, generated, "    COMMA,") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "    ',',") == null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "terminalIdByName") != null);
}

test "parseGrammar creates stable terminals for unaliased bison literals" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start expr
        \\%token ID '(' ')'
        \\%left '+'
        \\expr:
        \\    expr '+' expr %prec '+'
        \\  | '(' expr ')'
        \\  | ID
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 4), grammar.tokens.items.len);
    try std.testing.expectEqualStrings("LIT_28", resolveTerminalName(grammar, "'('").?);
    try std.testing.expectEqualStrings("LIT_29", resolveTerminalName(grammar, "')'").?);
    try std.testing.expectEqualStrings("LIT_2B", resolveTerminalName(grammar, "'+'").?);
    try validateGrammar(grammar);

    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
    const generated = try generateZigMetadata(arena, "literal-auto.y", source);
    try std.testing.expect(std.mem.indexOf(u8, generated, "    LIT_2B,") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "    LIT_28,") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "    '+',") == null);
}

test "parseGrammar supports wrapped token and precedence declarations" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start expr
        \\%token ID
        \\       NUMBER "number"
        \\%left '+'
        \\      '-'
        \\%%
        \\expr:
        \\    expr '+' expr
        \\  | expr '-' expr
        \\  | ID
        \\  | "number"
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 4), grammar.tokens.items.len);
    try std.testing.expectEqualStrings("NUMBER", resolveTerminalName(grammar, "\"number\"").?);
    try std.testing.expectEqualStrings("LIT_2B", resolveTerminalName(grammar, "'+'").?);
    try std.testing.expectEqualStrings("LIT_2D", resolveTerminalName(grammar, "'-'").?);
    try std.testing.expectEqual(@as(usize, 2), grammar.token_precedences.items.len);
    try std.testing.expectEqual(grammar.token_precedences.items[0].precedence.level, grammar.token_precedences.items[1].precedence.level);

    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
}

test "parseGrammar treats non-pipe rule lines as alternative continuations" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start expr
        \\%token ID
        \\%left '+'
        \\expr:
        \\    expr '+'
        \\    expr
        \\    %prec '+'
        \\  | ID
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.rules.items[0].alternatives.items.len);
    try std.testing.expectEqual(@as(usize, 3), grammar.rules.items[0].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("expr", grammar.rules.items[0].alternatives.items[0].symbols[0]);
    try std.testing.expectEqualStrings("'+'", grammar.rules.items[0].alternatives.items[0].symbols[1]);
    try std.testing.expectEqualStrings("expr", grammar.rules.items[0].alternatives.items[0].symbols[2]);
    try std.testing.expectEqualStrings("'+'", grammar.rules.items[0].alternatives.items[0].precedence_symbol.?);
    try std.testing.expectEqualStrings("ID", grammar.rules.items[0].alternatives.items[1].symbols[0]);

    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
}

test "parseGrammar ignores bison production conflict annotations" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start stmt
        \\%token ID
        \\stmt:
        \\    ID %dprec 1 %merge <merge_nodes>
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items[0].alternatives.items.len);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items[0].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("ID", grammar.rules.items[0].alternatives.items[0].symbols[0]);
    try validateGrammar(grammar);
}

test "parseGrammar accepts bison typed declarations and strips semantic actions" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start stmt
        \\%token <str> IDENT 258 "identifier"
        \\%token PLUS 259 '+'
        \\%precedence '+'
        \\%type <node> stmt expr
        \\%%
        \\stmt:
        \\    expr { $$ = $1; }
        \\  ;
        \\expr:
        \\    expr '+' expr %prec '+' { $$ = make_binary("+", $1, $3); }
        \\  | "identifier" { $$ = make_ident("{not a grammar symbol}"); }
        \\  ;
        \\%%
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 2), grammar.tokens.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.token_aliases.items.len);
    try std.testing.expectEqual(@as(usize, 1), grammar.token_precedences.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.rules.items.len);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items[0].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("expr", grammar.rules.items[0].alternatives.items[0].symbols[0]);
    try std.testing.expectEqual(@as(usize, 3), grammar.rules.items[1].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("'+'", grammar.rules.items[1].alternatives.items[0].precedence_symbol.?);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items[1].alternatives.items[1].symbols.len);
    try std.testing.expectEqualStrings("\"identifier\"", grammar.rules.items[1].alternatives.items[1].symbols[0]);
    try validateGrammar(grammar);
    try std.testing.expectEqual(try symbolId(grammar, grammar.tokens.items.len + 1, "IDENT"), try symbolId(grammar, grammar.tokens.items.len + 1, "\"identifier\""));

    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
}

test "parseGrammar splits same-line alternatives outside literals and actions" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start stmt
        \\%token IDENT STRING BAR '|'
        \\stmt:
        \\    IDENT { note("|"); } | STRING | BAR '|'
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items.len);
    try std.testing.expectEqual(@as(usize, 3), grammar.rules.items[0].alternatives.items.len);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items[0].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("IDENT", grammar.rules.items[0].alternatives.items[0].symbols[0]);
    try std.testing.expectEqualStrings("STRING", grammar.rules.items[0].alternatives.items[1].symbols[0]);
    try std.testing.expectEqual(@as(usize, 2), grammar.rules.items[0].alternatives.items[2].symbols.len);
    try std.testing.expectEqualStrings("BAR", grammar.rules.items[0].alternatives.items[2].symbols[0]);
    try std.testing.expectEqualStrings("'|'", grammar.rules.items[0].alternatives.items[2].symbols[1]);

    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
}

test "parseGrammar skips multiline bison prologue directive and action blocks" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%{
        \\#include "postgres.h"
        \\%}
        \\%union {
        \\    int ival;
        \\    const char *str;
        \\}
        \\%destructor {
        \\    free($$);
        \\} <str>
        \\%start stmt
        \\%token <str> IDENT
        \\%nterm <node> stmt
        \\%%
        \\stmt:
        \\    IDENT {
        \\        $$ = make_ident($1);
        \\        note("{ not grammar }");
        \\    }
        \\  | /* empty */
        \\  ;
        \\%%
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqualStrings("stmt", grammar.start_symbol);
    try std.testing.expectEqual(@as(usize, 1), grammar.tokens.items.len);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.rules.items[0].alternatives.items.len);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items[0].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("IDENT", grammar.rules.items[0].alternatives.items[0].symbols[0]);
    try std.testing.expectEqual(@as(usize, 0), grammar.rules.items[0].alternatives.items[1].symbols.len);
    try validateGrammar(grammar);
}

test "parseGrammar handles bison empty productions and block comments" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%reference postgres_gram_y https://example.test/postgres/gram.y
        \\%start stmt
        \\%token IDENT URL
        \\/*
        \\ * top-level comment
        \\ */
        \\stmt:
        \\    IDENT /* inline comment */ URL
        \\  | "https://example.test/not//comment" /* trailing comment */
        \\  | %empty
        \\  | /*EMPTY*/
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqualStrings("https://example.test/postgres/gram.y", grammar.postgres_gram_y);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items.len);
    try std.testing.expectEqual(@as(usize, 4), grammar.rules.items[0].alternatives.items.len);
    try std.testing.expectEqual(@as(usize, 2), grammar.rules.items[0].alternatives.items[0].symbols.len);
    try std.testing.expectEqualStrings("IDENT", grammar.rules.items[0].alternatives.items[0].symbols[0]);
    try std.testing.expectEqualStrings("URL", grammar.rules.items[0].alternatives.items[0].symbols[1]);
    try std.testing.expectEqualStrings("\"https://example.test/not//comment\"", grammar.rules.items[0].alternatives.items[1].symbols[0]);
    try std.testing.expectEqual(@as(usize, 0), grammar.rules.items[0].alternatives.items[2].symbols.len);
    try std.testing.expectEqual(@as(usize, 0), grammar.rules.items[0].alternatives.items[3].symbols.len);
}

test "parseGrammar ignores host-language epilogue after second separator" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%{
        \\#include <stddef.h>
        \\%}
        \\%start stmt
        \\%token IDENT
        \\%%
        \\stmt:
        \\    IDENT
        \\  ;
        \\%%
        \\static int helper(void) {
        \\    return 0;
        \\}
        \\/* This does not need to be valid yacc input. */
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 1), grammar.rules.items.len);
    try std.testing.expectEqualStrings("stmt", grammar.rules.items[0].name);
    try validateGrammar(grammar);
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
    try std.testing.expectEqual(@as(usize, 1), nullableSymbolCount(tables.nullable_symbols));
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

test "buildSlrTables resolves shift reduce conflicts with yacc precedence" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const source =
        \\%expect 0
        \\%start expr
        \\%token ID
        \\%left PLUS '+'
        \\expr:
        \\    expr '+' expr
        \\  | ID
        \\  ;
    ;
    const grammar = try parseGrammar(arena, source);
    try std.testing.expectEqual(@as(usize, 2), grammar.tokens.items.len);
    try std.testing.expectEqual(@as(usize, 1), grammar.token_aliases.items.len);
    const tables = try buildSlrTables(arena, grammar);
    try std.testing.expectEqual(@as(usize, 0), tables.conflicts.len);
    _ = try generateZigMetadata(arena, "precedence.y", source);
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
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const nullable_symbol_count = 0;") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "const nullable_symbols = [_]bool") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const state_count = ") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "const actions = [_]Action") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "const conflicts = [_]Conflict") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "const state_items = [_]Item") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "const states = [_]State") == null);
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
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const Rule =") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const RuleId = enum(u16)") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn terminalName(terminal: u16) ?[]const u8") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn ruleName(rule: RuleId) []const u8") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const ProductionInfo = struct") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn productionInfo(production: u16) ?ProductionInfo") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn productionRhs(production: u16) ?[]const u16") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn symbolRule(symbol: u16) ?RuleId") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn symbolIsNullable(symbol: u16) ?bool") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "fn ruleIdForLhs(lhs: u16) ?RuleId") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, ".gram_y = \"https://example.test/postgres/gram.y\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, ".scan_l = \"https://example.test/postgres/scan.l\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const cockroach_reference") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, ".sql_y = \"https://example.test/cockroach/sql.y\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parse(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseWithFallback(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseWithStackBuffer(token_ids: []const u16, stack_buffer: []u16) !void") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseWithFallbackStackBuffer(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const Reduction = struct") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const Shift = struct") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const Accept = struct") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseWithReductions(token_ids: []const u16, stack_buffer: []u16, reducer: anytype) !void") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseWithEvents(token_ids: []const u16, stack_buffer: []u16, event_handler: anytype) !void") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "return parseWithEvents(token_ids, stack_buffer, Adapter{ .inner = reducer });") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "fn parseCore(token_ids: []const u16, fallback_token_ids: ?[]const u16, stack: anytype, event_handler: anytype)") != null);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, "while (true)"));
    try std.testing.expect(std.mem.indexOf(u8, first, "try event_handler.shift(.{ .token_index = index, .terminal = effective_lookahead });") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "try event_handler.reduce(.{") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "try maybeAccept(event_handler, .{ .token_count = index });") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "fn maybeAccept(event_handler: anytype, accept: Accept) !void") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const ParseError") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "ParseError!void") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const ParseDiagnostic = struct") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseDiagnostic(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseDiagnosticWithStackBuffer(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseDiagnosticWithFallback(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseDiagnosticWithFallbackStackBuffer(") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub const ParseErrorInfo") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseError(") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn parseErrorWithStackBuffer(") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn tokenId(token: Token) u16") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn terminalIdByName(name: []const u8) ?u16") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn tokenIdByName") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "symbols[idx]") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn expectedTerminalNamesAlloc") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn expectedTerminalCountForState") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn expectedTerminalNameForState") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn actionsForState") == null);
    try std.testing.expect(std.mem.indexOf(u8, first, "pub fn symbolName") == null);
}

test "public generator APIs release working memory and return owned output" {
    const source =
        \\%expect 0
        \\%start stmt
        \\%token SELECT IDENT
        \\stmt:
        \\    SELECT IDENT
        \\  ;
    ;

    const generated = try generateZigMetadata(std.testing.allocator, "owned.y", source);
    defer std.testing.allocator.free(generated);
    try std.testing.expect(std.mem.indexOf(u8, generated, "pub fn parse(") != null);

    const expectation = try conflictExpectation(std.testing.allocator, source);
    try std.testing.expect(expectation.matches());

    const report = try conflictReportAlloc(std.testing.allocator, "owned.y", source, 1);
    defer std.testing.allocator.free(report);
    try std.testing.expect(std.mem.indexOf(u8, report, "actual: 0") != null);
}

test "table indices reject grammars larger than u16 with an explicit error" {
    try std.testing.expectEqual(std.math.maxInt(u16), try tableIndex(std.math.maxInt(u16)));
    try std.testing.expectError(error.GrammarTooLarge, tableIndex(@as(usize, std.math.maxInt(u16)) + 1));
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
