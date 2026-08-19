// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const generated = @import("grammar/generated/root.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

const generated_token_id_stack_capacity = 1024;
const generated_parse_stack_capacity = 512;

pub const Diagnostic = struct {
    token_index: usize,
    source_start: usize,
    source_end: usize,
    expected: []const []const u8,
    actual: []const u8,

    pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        allocator.free(self.expected);
        allocator.free(self.actual);
    }
};

const DiagnosticTokenIds = struct {
    token_ids: []u16,
    source_indexes: []usize,

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.token_ids);
        allocator.free(self.source_indexes);
        self.* = .{ .token_ids = &.{}, .source_indexes = &.{} };
    }

    fn sourceTokenIndex(self: @This(), generated_token_index: usize) usize {
        if (generated_token_index < self.source_indexes.len) return self.source_indexes[generated_token_index];
        return if (self.source_indexes.len == 0) 0 else self.source_indexes[self.source_indexes.len - 1] + 1;
    }

    fn endSourceOffset(self: @This(), tokens: []const token_mod.Token) usize {
        const end_token = self.sourceTokenIndex(self.source_indexes.len);
        if (end_token == 0 or tokens.len == 0) return 0;
        return tokens[@min(end_token - 1, tokens.len - 1)].source_end;
    }
};

const TokenIdSink = union(enum) {
    list: struct {
        items: *std.ArrayListUnmanaged(u16),
        allocator: std.mem.Allocator,
    },
    buffer: struct {
        items: []u16,
        len: usize = 0,
    },

    fn append(self: *TokenIdSink, id: u16) !void {
        switch (self.*) {
            .list => |list| try list.items.append(list.allocator, id),
            .buffer => |*buffer| {
                if (buffer.len == buffer.items.len) return error.NoSpaceLeft;
                buffer.items[buffer.len] = id;
                buffer.len += 1;
            },
        }
    }

    fn appendToken(self: *TokenIdSink, token: generated.Token) !void {
        try self.append(generated.tokenId(token));
    }

    fn len(self: TokenIdSink) usize {
        return switch (self) {
            .list => |list| list.items.items.len,
            .buffer => |buffer| buffer.len,
        };
    }
};

const TokenRange = struct {
    start: usize,
    end: usize,
};

pub fn parseSqlAlloc(allocator: std.mem.Allocator, sql: []const u8) !void {
    var tokens = try lexer.tokenizeAlloc(allocator, sql);
    defer lexer.freeTokens(allocator, &tokens);
    try parseTokensAlloc(allocator, tokens.items);
}

pub fn parseTokensAlloc(allocator: std.mem.Allocator, tokens: []const token_mod.Token) !void {
    var token_id_buffer: [generated_token_id_stack_capacity]u16 = undefined;
    if (tokenIdsIntoBuffer(tokens, &token_id_buffer)) |token_ids| {
        return parseGeneratedTokenIds(allocator, token_ids);
    } else |err| switch (err) {
        error.NoSpaceLeft => {
            const token_ids = try tokenIdsAlloc(allocator, tokens);
            defer allocator.free(token_ids);
            return parseGeneratedTokenIds(allocator, token_ids);
        },
        else => return err,
    }
}

pub fn diagnosticSqlAlloc(allocator: std.mem.Allocator, sql: []const u8) !?Diagnostic {
    var tokens = try lexer.tokenizeAlloc(allocator, sql);
    defer lexer.freeTokens(allocator, &tokens);
    var diagnostic = try diagnosticTokensAlloc(allocator, tokens.items) orelse return null;
    if (diagnostic.token_index >= tokens.items.len) {
        diagnostic.source_start = sql.len;
        diagnostic.source_end = sql.len;
    }
    return diagnostic;
}

pub fn diagnosticTokensAlloc(allocator: std.mem.Allocator, tokens: []const token_mod.Token) !?Diagnostic {
    var diagnostic_tokens = try diagnosticTokenIdsAlloc(allocator, tokens);
    defer diagnostic_tokens.deinit(allocator);

    const generated_diagnostic = try parseGeneratedTokenIdsDiagnostic(allocator, diagnostic_tokens.token_ids) orelse return null;
    errdefer generated_diagnostic.deinit(allocator);

    const source_token_index = diagnostic_tokens.sourceTokenIndex(generated_diagnostic.token_index);
    const source_start = if (source_token_index < tokens.len)
        tokens[source_token_index].source_start
    else
        diagnostic_tokens.endSourceOffset(tokens);
    const source_end = if (source_token_index < tokens.len)
        tokens[source_token_index].source_end
    else
        diagnostic_tokens.endSourceOffset(tokens);
    const actual = try allocator.dupe(u8, if (source_token_index < tokens.len) tokens[source_token_index].text else "$end");

    return .{
        .token_index = source_token_index,
        .source_start = source_start,
        .source_end = source_end,
        .expected = generated_diagnostic.expected,
        .actual = actual,
    };
}

pub fn tokenIdsAlloc(allocator: std.mem.Allocator, tokens: []const token_mod.Token) ![]u16 {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    errdefer ids.deinit(allocator);
    var sink = TokenIdSink{ .list = .{ .items = &ids, .allocator = allocator } };
    try appendAllTokenIds(&sink, tokens, null);
    return ids.toOwnedSlice(allocator);
}

fn tokenIdsIntoBuffer(tokens: []const token_mod.Token, buffer: []u16) ![]const u16 {
    var sink = TokenIdSink{ .buffer = .{ .items = buffer } };
    try appendAllTokenIds(&sink, tokens, null);
    return sink.buffer.items[0..sink.buffer.len];
}

fn diagnosticTokenIdsAlloc(allocator: std.mem.Allocator, tokens: []const token_mod.Token) !DiagnosticTokenIds {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    var source_indexes: std.ArrayListUnmanaged(usize) = .empty;
    errdefer ids.deinit(allocator);
    errdefer source_indexes.deinit(allocator);

    var sink = TokenIdSink{ .list = .{ .items = &ids, .allocator = allocator } };
    try appendAllTokenIds(&sink, tokens, .{ .items = &source_indexes, .allocator = allocator });

    const token_ids = try ids.toOwnedSlice(allocator);
    errdefer allocator.free(token_ids);
    return .{
        .token_ids = token_ids,
        .source_indexes = try source_indexes.toOwnedSlice(allocator),
    };
}

const SourceIndexSink = struct {
    items: *std.ArrayListUnmanaged(usize),
    allocator: std.mem.Allocator,
};

fn appendAllTokenIds(ids: *TokenIdSink, tokens: []const token_mod.Token, source_indexes: ?SourceIndexSink) !void {
    for (tokens, 0..) |token, index| {
        if (token.kind == .semicolon and trailingSemicolonOnly(tokens, index)) break;
        const previous = if (index > 0) tokens[index - 1] else null;
        const next = if (index + 1 < tokens.len) tokens[index + 1] else null;
        const before = ids.len();
        try appendTokenIds(ids, tokens, index, token, previous, next);
        if (source_indexes) |source_sink| {
            const added = ids.len() - before;
            try source_sink.items.ensureUnusedCapacity(source_sink.allocator, added);
            for (0..added) |_| source_sink.items.appendAssumeCapacity(index);
        }
    }
}

fn parseGeneratedTokenIds(allocator: std.mem.Allocator, token_ids: []const u16) !void {
    if (token_ids.len + 1 <= generated_parse_stack_capacity) {
        var stack_buffer: [generated_parse_stack_capacity]u16 = undefined;
        generated.parseWithStackBuffer(token_ids, &stack_buffer) catch |err| switch (err) {
            error.StackOverflow => return generated.parse(allocator, token_ids),
            else => return err,
        };
        return;
    }
    return generated.parse(allocator, token_ids);
}

fn parseGeneratedTokenIdsDiagnostic(allocator: std.mem.Allocator, token_ids: []const u16) !?generated.ParseDiagnostic {
    if (token_ids.len + 1 <= generated_parse_stack_capacity) {
        var stack_buffer: [generated_parse_stack_capacity]u16 = undefined;
        return generated.parseDiagnosticWithStackBuffer(allocator, token_ids, &stack_buffer) catch |err| switch (err) {
            error.StackOverflow => generated.parseDiagnostic(allocator, token_ids),
            else => return err,
        };
    }
    return generated.parseDiagnostic(allocator, token_ids);
}

fn appendTokenIds(
    ids: *TokenIdSink,
    tokens: []const token_mod.Token,
    index: usize,
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
) !void {
    switch (token.kind) {
        .identifier => {
            if (try contextualKeywordSymbolId(tokens, index, token, previous, next)) |id| {
                try ids.append(id);
                return;
            }
            if (generatedParserTreatsKeywordAsIdentifier(tokens, index, token, previous, next)) {
                try appendIdentifierIds(ids, token.text, false);
                return;
            }
            if (try keywordSymbolId(token)) |id| {
                try ids.append(id);
                return;
            }
            try appendIdentifierIds(ids, token.text, true);
        },
        .string => try ids.appendToken(.STRING),
        .number => try ids.appendToken(.NUMBER),
        .placeholder => try ids.appendToken(.PLACEHOLDER),
        .comma => try ids.appendToken(.COMMA),
        .dot => try ids.appendToken(.DOT),
        .colon => try ids.appendToken(.COLON),
        .colon_colon => try ids.appendToken(.COLON_COLON),
        .star => try ids.appendToken(.STAR),
        .eq => try ids.appendToken(.EQ),
        .neq => try ids.appendToken(.NEQ),
        .gt => try ids.appendToken(.GT),
        .gte => try ids.appendToken(.GTE),
        .lt => try ids.appendToken(.LT),
        .lte => try ids.appendToken(.LTE),
        .plus => try ids.appendToken(.PLUS),
        .minus => try ids.appendToken(.MINUS),
        .slash => try ids.appendToken(.SLASH),
        .percent => try ids.appendToken(.PERCENT),
        .lparen => try ids.appendToken(.LPAREN),
        .rparen => try ids.appendToken(.RPAREN),
        .lbracket => try ids.appendToken(.LBRACKET),
        .rbracket => try ids.appendToken(.RBRACKET),
        .at_contains => try ids.appendToken(.AT_CONTAINS),
        .range_overlap => try ids.appendToken(.RANGE_OVERLAP),
        .pipe_concat => try ids.appendToken(.PIPE_CONCAT),
        .question => try ids.appendToken(.QUESTION),
        .question_any => try ids.appendToken(.QUESTION_ANY),
        .question_all => try ids.appendToken(.QUESTION_ALL),
        .arrow_json => try ids.appendToken(.ARROW_JSON),
        .arrow_text => try ids.appendToken(.ARROW_TEXT),
        .path_arrow_json => try ids.appendToken(.PATH_ARROW_JSON),
        .path_arrow_text => try ids.appendToken(.PATH_ARROW_TEXT),
        .regex_match => try ids.appendToken(.REGEX_MATCH),
        .regex_imatch => try ids.appendToken(.REGEX_IMATCH),
        .regex_not_match => try ids.appendToken(.REGEX_NOT_MATCH),
        .regex_not_imatch => try ids.appendToken(.REGEX_NOT_IMATCH),
        .semicolon => try ids.appendToken(.SEMICOLON),
    }
}

fn contextualKeywordSymbolId(
    tokens: []const token_mod.Token,
    index: usize,
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
) !?u16 {
    if (sessionAuthorizationKeywordContext(tokens, index)) {
        if (token.matchesKeywordTag(.authorization)) return generated.tokenId(.AUTHORIZATION);
        if (token.matchesKeywordTag(.session)) return generated.tokenId(.SESSION);
    }
    if (indexConcurrentlyKeywordContext(tokens, index) and token.matchesKeywordTag(.concurrently)) return generated.tokenId(.CONCURRENTLY);
    if (indexElementCollateKeywordContext(tokens, index, token, previous, next)) return generated.tokenId(.COLLATE);
    if (routineKeywordContext(tokens, index) and token.matchesKeywordTag(.routine)) return generated.tokenId(.ROUTINE);
    if (sessionKeywordContext(tokens, index)) {
        if (token.matchesKeywordTag(.local)) return generated.tokenId(.LOCAL);
        if (token.matchesKeywordTag(.session)) return generated.tokenId(.SESSION);
    }
    if (cursorKeywordContext(tokens, index)) {
        if (token.matchesKeywordTag(.binary)) return generated.tokenId(.BINARY);
        if (token.matchesKeywordTag(.cursor)) return generated.tokenId(.CURSOR);
        if (token.matchesKeywordTag(.hold)) return generated.tokenId(.HOLD);
        if (token.matchesKeywordTag(.scroll)) return generated.tokenId(.SCROLL);
    }
    if (copyDiagnosticIdentifierContext(tokens, index) and
        (token.matchesKeywordTag(.escape) or token.matchesKeywordTag(.where)))
    {
        return generated.tokenId(.IDENT);
    }
    if (alterTableDiagnosticIdentifierContext(tokens, index) and
        token.matchesKeywordTag(.match) and
        !alterTableForeignKeyMatchClauseContext(tokens, index))
    {
        return generated.tokenId(.IDENT);
    }
    if (createRoutineDiagnosticIdentifierContext(tokens, index) and
        (token.matchesKeywordTag(.current) or token.matchesKeywordTag(.from) or token.matchesKeywordTag(.rows) or token.matchesKeywordTag(.window)))
    {
        return generated.tokenId(.IDENT);
    }
    if (createTriggerDiagnosticIdentifierContext(tokens, index) and createTriggerDiagnosticKeywordAsIdentifier(token)) {
        return generated.tokenId(.IDENT);
    }
    if (functionArgumentNameIdentifierContext(tokens, index, token, next)) return generated.tokenId(.IDENT);
    if (matchKeywordAliasContext(tokens, index, token, previous, next)) return generated.tokenId(.IDENT);
    if (createTableMissingAsSelectContext(tokens, index) and token.matchesKeywordTag(.table)) return generated.tokenId(.IDENT);
    if (systemTimeForKeywordContext(tokens, index) and token.matchesKeywordTag(.@"for")) return generated.tokenId(.SYSTEM_TIME_FOR);
    if (analyzeKeywordContext(tokens, index) and token.matchesKeywordTag(.verbose)) return generated.tokenId(.ANALYZE_VERBOSE);
    if (!transactionControlContext(tokens, index)) return null;
    if (token.matchesKeywordTag(.characteristics)) return generated.tokenId(.CHARACTERISTICS);
    if (token.matchesKeywordTag(.committed)) return generated.tokenId(.COMMITTED);
    if (token.matchesKeywordTag(.constraints)) return generated.tokenId(.CONSTRAINTS);
    if (token.matchesKeywordTag(.deferrable)) return generated.tokenId(.DEFERRABLE);
    if (token.matchesKeywordTag(.deferred)) return generated.tokenId(.DEFERRED);
    if (token.matchesKeywordTag(.immediate)) return generated.tokenId(.IMMEDIATE);
    if (token.matchesKeywordTag(.isolation)) return generated.tokenId(.ISOLATION);
    if (token.matchesKeywordTag(.level)) return generated.tokenId(.LEVEL);
    if (token.matchesKeywordTag(.read)) return generated.tokenId(.READ);
    if (token.matchesKeywordTag(.release)) return generated.tokenId(.RELEASE);
    if (token.matchesKeywordTag(.repeatable)) return generated.tokenId(.REPEATABLE);
    if (token.matchesKeywordTag(.as)) return generated.tokenId(.AS);
    if (token.matchesKeywordTag(.savepoint)) return generated.tokenId(.SAVEPOINT);
    if (token.matchesKeywordTag(.serializable)) return generated.tokenId(.SERIALIZABLE);
    if (token.matchesKeywordTag(.session)) return generated.tokenId(.SESSION);
    if (token.matchesKeywordTag(.start)) return generated.tokenId(.START);
    if (token.matchesKeywordTag(.to)) return generated.tokenId(.TO);
    if (token.matchesKeywordTag(.transaction)) return generated.tokenId(.TRANSACTION);
    if (token.matchesKeywordTag(.uncommitted)) return generated.tokenId(.UNCOMMITTED);
    if (token.matchesKeywordTag(.work)) return generated.tokenId(.WORK);
    if (token.matchesKeywordTag(.write)) return generated.tokenId(.WRITE);
    return null;
}

fn functionArgumentNameIdentifierContext(tokens: []const token_mod.Token, index: usize, token: token_mod.Token, next: ?token_mod.Token) bool {
    if (!token.matchesKeywordTag(.@"return")) return false;
    const next_token = next orelse return false;
    if (next_token.kind != .eq or index == 0) return false;
    var depth: usize = 0;
    var cursor = index;
    while (cursor > 0) {
        cursor -= 1;
        switch (tokens[cursor].kind) {
            .rparen, .rbracket => depth += 1,
            .lparen, .lbracket => {
                if (depth == 0) return cursor > 0 and tokens[cursor - 1].kind == .identifier;
                depth -= 1;
            },
            .comma => if (depth == 0) return true,
            else => {},
        }
    }
    return false;
}

fn matchKeywordAliasContext(
    tokens: []const token_mod.Token,
    index: usize,
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
) bool {
    if (!token.matchesKeywordTag(.match) or previous == null) return false;
    if (previous.?.matchesKeywordTag(.as)) return true;
    if (!tokenIsInSelectProjectionList(tokens, index)) return false;
    const next_token = next orelse return true;
    return next_token.matchesKeywordTag(.from) or
        next_token.matchesKeywordTag(.order) or
        next_token.matchesKeywordTag(.group) or
        next_token.matchesKeywordTag(.having) or
        next_token.matchesKeywordTag(.limit) or
        next_token.kind == .comma or
        next_token.kind == .semicolon;
}

fn tokenIsInSelectProjectionList(tokens: []const token_mod.Token, index: usize) bool {
    var target_depth: usize = 0;
    for (tokens[0..index]) |token| switch (token.kind) {
        .lparen => target_depth += 1,
        .rparen => if (target_depth > 0) {
            target_depth -= 1;
        },
        else => {},
    };

    var depth: usize = 0;
    var saw_select = false;
    var saw_from = false;
    for (tokens[0..index]) |token| {
        if (depth == target_depth) {
            if (token.matchesKeywordTag(.select)) {
                saw_select = true;
                saw_from = false;
            } else if (saw_select and token.matchesKeywordTag(.from)) {
                saw_from = true;
            }
        }
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => if (depth > 0) {
                depth -= 1;
            },
            else => {},
        }
    }
    return saw_select and !saw_from;
}

fn sessionKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    return index == 1 and tokens.len > 0 and tokens[0].matchesKeywordTag(.set);
}

fn cursorKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    return index >= 2 and tokens.len > 0 and tokens[0].matchesKeywordTag(.declare);
}

fn systemTimeForKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    return index + 3 < tokens.len and
        tokens[index].matchesKeywordTag(.@"for") and
        std.ascii.eqlIgnoreCase(tokens[index + 1].text, "system_time") and
        tokens[index + 2].matchesKeywordTag(.as) and
        tokens[index + 3].matchesKeywordTag(.of);
}

fn analyzeKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    return index == 1 and tokens.len > 0 and tokens[0].matchesKeywordTag(.analyze);
}

fn copyDiagnosticIdentifierContext(tokens: []const token_mod.Token, index: usize) bool {
    return index > 0 and tokens.len > 0 and tokens[0].matchesKeywordTag(.copy);
}

fn alterTableDiagnosticIdentifierContext(tokens: []const token_mod.Token, index: usize) bool {
    if (index < 4 or tokens.len < 4) return false;
    if (!tokens[0].matchesKeywordTag(.alter) or !tokens[1].matchesKeywordTag(.table)) return false;
    var cursor: usize = 2;
    _ = consumeIfExists(tokens, &cursor, tokens.len);
    if (cursor < tokens.len and tokens[cursor].matchesKeywordTag(.only)) cursor += 1;
    const table = qualifiedNameRange(tokens, cursor, tokens.len) orelse return false;
    if (index < table.end) return false;
    return alterTableTailHasCommaBefore(tokens, table.end, index);
}

fn alterTableForeignKeyMatchClauseContext(tokens: []const token_mod.Token, index: usize) bool {
    return index > 0 and index + 1 < tokens.len and tokens[index - 1].kind == .rparen and
        (tokens[index + 1].matchesKeywordTag(.full) or tokens[index + 1].kind == .identifier);
}

fn alterTableTailHasCommaBefore(tokens: []const token_mod.Token, start: usize, end: usize) bool {
    var depth: usize = 0;
    var index = start;
    while (index < end and index < tokens.len) : (index += 1) switch (tokens[index].kind) {
        .lparen, .lbracket => depth += 1,
        .rparen, .rbracket => if (depth > 0) {
            depth -= 1;
        },
        .comma => if (depth == 0) return true,
        else => {},
    };
    return false;
}

fn createRoutineDiagnosticIdentifierContext(tokens: []const token_mod.Token, index: usize) bool {
    if (index < 5 or tokens.len < 5 or !tokens[0].matchesKeywordTag(.create)) return false;
    var cursor: usize = 1;
    if (cursor + 1 < tokens.len and tokens[cursor].matchesKeywordTag(.@"or") and tokens[cursor + 1].matchesKeywordTag(.replace)) cursor += 2;
    if (cursor >= tokens.len or
        !(tokens[cursor].matchesKeywordTag(.function) or tokens[cursor].matchesKeywordTag(.procedure))) return false;
    cursor += 1;
    const routine_name = qualifiedNameRange(tokens, cursor, tokens.len) orelse return false;
    cursor = routine_name.end;
    if (cursor >= tokens.len or tokens[cursor].kind != .lparen) return false;
    const close = findMatchingParen(tokens, cursor, tokens.len) orelse return false;
    return index > close;
}

fn createTriggerDiagnosticIdentifierContext(tokens: []const token_mod.Token, index: usize) bool {
    if (tokens.len < 3 or index < 2 or !tokens[0].matchesKeywordTag(.create)) return false;
    if (tokens[1].matchesKeywordTag(.trigger)) return index >= 2;
    return index >= 4 and tokens.len >= 5 and
        tokens[1].matchesKeywordTag(.@"or") and tokens[2].matchesKeywordTag(.replace) and tokens[3].matchesKeywordTag(.trigger);
}

fn createTriggerDiagnosticKeywordAsIdentifier(token: token_mod.Token) bool {
    return token.matchesKeywordTag(.@"and") or
        token.matchesKeywordTag(.any) or
        token.matchesKeywordTag(.array) or
        token.matchesKeywordTag(.between) or
        token.matchesKeywordTag(.case) or
        token.matchesKeywordTag(.cast) or
        token.matchesKeywordTag(.current) or
        token.matchesKeywordTag(.current_date) or
        token.matchesKeywordTag(.current_timestamp) or
        token.matchesKeywordTag(.distinct) or
        token.matchesKeywordTag(.@"else") or
        token.matchesKeywordTag(.end) or
        token.matchesKeywordTag(.escape) or
        token.matchesKeywordTag(.extract) or
        token.matchesKeywordTag(.filter) or
        token.matchesKeywordTag(.ilike) or
        token.matchesKeywordTag(.interval) or
        token.matchesKeywordTag(.like) or
        token.matchesKeywordTag(.@"or") or
        token.matchesKeywordTag(.position) or
        token.matchesKeywordTag(.some) or
        token.matchesKeywordTag(.then) or
        token.matchesKeywordTag(.unknown) or
        token.matchesKeywordTag(.when);
}

fn createTableMissingAsSelectContext(tokens: []const token_mod.Token, index: usize) bool {
    return index == 1 and createTableMissingAsSelect(tokens);
}

fn sessionAuthorizationKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    if (tokens.len < 3) return false;
    if (!tokens[0].matchesKeywordTag(.set) and !tokens[0].matchesKeywordTag(.reset)) return false;
    return (index == 1 or index == 2) and tokens[1].matchesKeywordTag(.session) and tokens[2].matchesKeywordTag(.authorization);
}

fn indexConcurrentlyKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    if (tokens.len < 3) return false;
    if (tokens[0].matchesKeywordTag(.drop)) return index == 2 and tokens.len > 3 and tokens[1].matchesKeywordTag(.index);
    if (!tokens[0].matchesKeywordTag(.create)) return false;
    if (tokens[1].matchesKeywordTag(.index)) return index == 2 and tokens.len > 3;
    return index == 3 and tokens.len > 4 and tokens[1].matchesKeywordTag(.unique) and tokens[2].matchesKeywordTag(.index);
}

fn indexElementCollateKeywordContext(
    tokens: []const token_mod.Token,
    index: usize,
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
) bool {
    if (!token.matchesKeywordTag(.collate) or previous == null or next == null or next.?.kind != .identifier) return false;
    switch (previous.?.kind) {
        .lparen, .comma, .semicolon => return false,
        else => {},
    }
    if (tokens.len < 6 or !tokens[0].matchesKeywordTag(.create)) return false;
    var open_index: ?usize = null;
    var close_index: ?usize = null;
    var depth: usize = 0;
    for (tokens, 0..) |candidate, candidate_index| switch (candidate.kind) {
        .lparen => {
            if (depth == 0 and open_index == null and candidate_index > 0) open_index = candidate_index;
            depth += 1;
        },
        .rparen => {
            if (depth == 0) return false;
            depth -= 1;
            if (depth == 0 and open_index != null and close_index == null) {
                close_index = candidate_index;
                break;
            }
        },
        else => {},
    };
    const open = open_index orelse return false;
    const close = close_index orelse return false;
    if (index <= open or index >= close) return false;
    var saw_index = false;
    var saw_on = false;
    for (tokens[0..open]) |candidate| {
        if (candidate.matchesKeywordTag(.index)) saw_index = true;
        if (candidate.matchesKeywordTag(.on)) saw_on = true;
    }
    return saw_index and saw_on;
}

fn transactionControlContext(tokens: []const token_mod.Token, index: usize) bool {
    if (tokens.len == 0) return false;
    const first = tokens[0];
    if (first.matchesKeywordTag(.start) or first.matchesKeywordTag(.begin) or first.matchesKeywordTag(.savepoint) or first.matchesKeywordTag(.release)) return true;
    if (first.matchesKeywordTag(.prepare) or first.matchesKeywordTag(.commit) or first.matchesKeywordTag(.end)) return index <= 1;
    if (first.matchesKeywordTag(.rollback)) return index <= 3;
    if (!first.matchesKeywordTag(.set)) return false;
    if (tokens.len > 1 and (tokens[1].matchesKeywordTag(.constraints) or tokens[1].matchesKeywordTag(.transaction))) return true;
    return tokens.len > 4 and tokens[1].matchesKeywordTag(.session) and tokens[2].matchesKeywordTag(.characteristics) and
        tokens[3].matchesKeywordTag(.as) and tokens[4].matchesKeywordTag(.transaction);
}

fn routineKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    return index == 1 and tokens.len > 0 and tokens[0].matchesKeywordTag(.drop);
}

fn generatedParserTreatsKeywordAsIdentifier(
    tokens: []const token_mod.Token,
    index: usize,
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
) bool {
    // PostgreSQL permits non-reserved keywords in qualified names. The lexer
    // emits DOT as its own token, so preserve the identifier role on either
    // side rather than promoting components such as `public` to terminals.
    if ((previous != null and previous.?.kind == .dot) or (next != null and next.?.kind == .dot)) return true;
    if (token.matchesKeywordTag(.conflict)) return previous != null and previous.?.matchesKeywordTag(.as);
    if (token.matchesKeywordTag(.offset)) return next != null and next.?.matchesKeywordTag(.limit);
    if (token.matchesKeywordTag(.fetch)) return previous != null and previous.?.matchesKeywordTag(.as);
    if (token.matchesKeywordTag(.start)) return next != null and next.?.kind == .eq;
    if (token.matchesKeywordTag(.rows)) {
        if (rowsKeywordStartsWindowFrame(tokens, index)) return false;
        return previous != null and previous.?.kind == .identifier and next != null and next.?.kind == .number;
    }
    if (token.matchesKeywordTag(.window)) return previous != null and previous.?.kind == .identifier and (next == null or next.?.kind == .semicolon);
    return false;
}

fn rowsKeywordStartsWindowFrame(tokens: []const token_mod.Token, index: usize) bool {
    var depth: usize = 0;
    var cursor = index;
    while (cursor > 0) {
        cursor -= 1;
        switch (tokens[cursor].kind) {
            .rparen, .rbracket => depth += 1,
            .lparen, .lbracket => {
                if (depth == 0) break;
                depth -= 1;
            },
            .semicolon => if (depth == 0) break,
            else => {},
        }
        if (depth == 0 and cursor > 0 and tokens[cursor].matchesKeywordTag(.by) and tokens[cursor - 1].matchesKeywordTag(.order)) return true;
    }
    return false;
}

fn appendIdentifierIds(ids: *TokenIdSink, text: []const u8, allow_trailing_dot: bool) !void {
    if (text.len == 0) return error.UnsupportedSqlShape;
    const has_trailing_dot = text[text.len - 1] == '.';
    if (has_trailing_dot and !allow_trailing_dot) return error.UnsupportedSqlShape;
    const body = if (has_trailing_dot) text[0 .. text.len - 1] else text;
    if (body.len == 0) return error.UnsupportedSqlShape;
    var parts = std.mem.splitScalar(u8, body, '.');
    var emitted = false;
    while (parts.next()) |part| {
        if (part.len == 0) return error.UnsupportedSqlShape;
        if (emitted) try ids.appendToken(.DOT);
        try ids.appendToken(.IDENT);
        emitted = true;
    }
    if (has_trailing_dot) try ids.appendToken(.DOT);
}

fn keywordSymbolId(token: token_mod.Token) !?u16 {
    const keyword = token.keyword orelse return null;
    const text = @tagName(keyword);
    if (text.len > 64) return error.UnsupportedSqlShape;
    var buffer: [64]u8 = undefined;
    for (text, 0..) |character, index| buffer[index] = std.ascii.toUpper(character);
    return generated.terminalIdByName(buffer[0..text.len]);
}

fn trailingSemicolonOnly(tokens: []const token_mod.Token, index: usize) bool {
    for (tokens[index..]) |token| if (token.kind != .semicolon) return false;
    return true;
}

fn consumeIfExists(tokens: []const token_mod.Token, index: *usize, end: usize) bool {
    if (index.* + 1 >= end or !tokens[index.*].matchesKeywordTag(.@"if") or !tokens[index.* + 1].matchesKeywordTag(.exists)) return false;
    index.* += 2;
    return true;
}

fn consumeIfNotExists(tokens: []const token_mod.Token, index: *usize, end: usize) bool {
    if (index.* + 2 >= end or !tokens[index.*].matchesKeywordTag(.@"if") or
        !tokens[index.* + 1].matchesKeywordTag(.not) or !tokens[index.* + 2].matchesKeywordTag(.exists)) return false;
    index.* += 3;
    return true;
}

fn qualifiedNameRange(tokens: []const token_mod.Token, start: usize, end: usize) ?TokenRange {
    if (start >= end or tokens[start].kind != .identifier) return null;
    var index = start + 1;
    while (index + 1 < end and tokens[index].kind == .dot and tokens[index + 1].kind == .identifier) index += 2;
    return .{ .start = start, .end = index };
}

fn findMatchingParen(tokens: []const token_mod.Token, open_index: usize, end: usize) ?usize {
    if (open_index >= end or tokens[open_index].kind != .lparen) return null;
    var depth: usize = 1;
    var index = open_index + 1;
    while (index < end) : (index += 1) switch (tokens[index].kind) {
        .lparen => depth += 1,
        .rparen => {
            depth -= 1;
            if (depth == 0) return index;
        },
        else => {},
    };
    return null;
}

fn createTableMissingAsSelect(tokens: []const token_mod.Token) bool {
    const end = statementTokenEnd(tokens);
    if (end < 4 or !tokens[0].matchesKeywordTag(.create) or !tokens[1].matchesKeywordTag(.table)) return false;
    var index: usize = 2;
    if (index < end and tokens[index].matchesKeywordTag(.only)) index += 1;
    _ = consumeIfNotExists(tokens, &index, end);
    const target = qualifiedNameRange(tokens, index, end) orelse return false;
    return target.end < end and tokens[target.end].matchesKeywordTag(.select);
}

fn statementTokenEnd(tokens: []const token_mod.Token) usize {
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) end -= 1;
    return end;
}

test "generated parser bridge parses SQL bytes across statement families" {
    const corpus = [_][]const u8{
        "SELECT docs.amount::text FROM public.docs WHERE score >= 1.25E-3",
        "INSERT INTO docs (id, status) VALUES ($1, 'ready')",
        "UPDATE docs SET status = 'done' WHERE id = $1",
        "DELETE FROM docs WHERE id = $1",
        "CREATE TABLE public.docs (id text PRIMARY KEY, score text)",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "DECLARE usage_cursor BINARY SCROLL CURSOR WITH HOLD FOR SELECT id FROM docs",
        "ANALYZE VERBOSE public.docs (status)",
        "SELECT * FROM docs FOR system_time AS OF '2026-01-01'",
    };
    for (corpus) |sql| {
        parseSqlAlloc(std.testing.allocator, sql) catch |err| {
            std.debug.print("generated parser bridge rejected corpus SQL: {s}\n", .{sql});
            if (try diagnosticSqlAlloc(std.testing.allocator, sql)) |diagnostic| {
                defer diagnostic.deinit(std.testing.allocator);
                std.debug.print("actual={s} token={d} expected={any}\n", .{ diagnostic.actual, diagnostic.token_index, diagnostic.expected });
            }
            return err;
        };
    }
}

test "generated parser bridge maps contextual synthetic terminals" {
    var analyze_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "ANALYZE VERBOSE docs");
    defer lexer.freeTokens(std.testing.allocator, &analyze_tokens);
    const analyze_ids = try tokenIdsAlloc(std.testing.allocator, analyze_tokens.items);
    defer std.testing.allocator.free(analyze_ids);
    try std.testing.expectEqual(generated.tokenId(.ANALYZE_VERBOSE), analyze_ids[1]);

    var history_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT * FROM docs FOR system_time AS OF 42");
    defer lexer.freeTokens(std.testing.allocator, &history_tokens);
    const history_ids = try tokenIdsAlloc(std.testing.allocator, history_tokens.items);
    defer std.testing.allocator.free(history_ids);
    try std.testing.expect(std.mem.indexOfScalar(u16, history_ids, generated.tokenId(.SYSTEM_TIME_FOR)) != null);
}

test "generated parser bridge reports owned source-aware diagnostics" {
    const sql = "SELECT FROM docs";
    const diagnostic = (try diagnosticSqlAlloc(std.testing.allocator, sql)).?;
    defer diagnostic.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), diagnostic.token_index);
    try std.testing.expectEqual(@as(usize, 7), diagnostic.source_start);
    try std.testing.expectEqual(@as(usize, 11), diagnostic.source_end);
    try std.testing.expectEqualStrings("FROM", diagnostic.actual);
    try std.testing.expect(diagnostic.expected.len > 0);

    const trailing_space_sql = "SELECT ";
    const eof_diagnostic = (try diagnosticSqlAlloc(std.testing.allocator, trailing_space_sql)).?;
    defer eof_diagnostic.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), eof_diagnostic.token_index);
    try std.testing.expectEqual(trailing_space_sql.len, eof_diagnostic.source_start);
    try std.testing.expectEqual(trailing_space_sql.len, eof_diagnostic.source_end);
    try std.testing.expectEqualStrings("$end", eof_diagnostic.actual);
}

test "generated parser bridge diagnoses every truncated corpus prefix" {
    const corpus = [_][]const u8{
        "SELECT id, status FROM docs WHERE id = $1 ORDER BY status LIMIT 10",
        "INSERT INTO docs (id, status) VALUES ($1, 'ready') RETURNING id",
        "CREATE INDEX docs_status_idx ON docs (status)",
    };
    for (corpus) |sql| {
        var tokens = try lexer.tokenizeAlloc(std.testing.allocator, sql);
        defer lexer.freeTokens(std.testing.allocator, &tokens);
        var end: usize = 0;
        while (end <= tokens.items.len) : (end += 1) {
            parseTokensAlloc(std.testing.allocator, tokens.items[0..end]) catch |err| switch (err) {
                error.UnexpectedToken => {
                    const diagnostic = (try diagnosticTokensAlloc(std.testing.allocator, tokens.items[0..end])).?;
                    defer diagnostic.deinit(std.testing.allocator);
                    try std.testing.expect(diagnostic.token_index <= end);
                    try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
                    try std.testing.expect(diagnostic.expected.len > 0);
                },
                else => return err,
            };
        }
    }
}

test "generated parser bridge falls back for large and deeply nested statements" {
    var sql: std.ArrayListUnmanaged(u8) = .empty;
    defer sql.deinit(std.testing.allocator);
    try sql.appendSlice(std.testing.allocator, "SELECT ");
    for (0..600) |_| try sql.append(std.testing.allocator, '(');
    try sql.append(std.testing.allocator, '1');
    for (0..600) |_| try sql.append(std.testing.allocator, ')');

    try parseSqlAlloc(std.testing.allocator, sql.items);
    try std.testing.expectEqual(@as(?Diagnostic, null), try diagnosticSqlAlloc(std.testing.allocator, sql.items));
}

fn appendFuzzSqlPart(buffer: []u8, len: *usize, part: []const u8) void {
    if (len.* + part.len > buffer.len) return;
    @memcpy(buffer[len.* .. len.* + part.len], part);
    len.* += part.len;
}

fn appendFuzzSqlByte(buffer: []u8, len: *usize, byte: u8) void {
    if (len.* == buffer.len) return;
    buffer[len.*] = byte;
    len.* += 1;
}

fn randomFuzzSql(random: std.Random, buffer: []u8) []const u8 {
    const parts = [_][]const u8{
        "SELECT", "WITH",  "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",   "EXPLAIN",
        "FROM",   "WHERE", "GROUP",  "ORDER",  "BY",     "LIMIT",  "OFFSET", "FETCH",
        "ROWS",   "AS",    "JOIN",   "ON",     "UNION",  "VALUES", "SET",    "INTO",
        "TABLE",  "INDEX", "GRAPH",  "METRIC", "id",     "status", "docs",   "1",
        "'open'", "$1",    "(",      ")",      ",",      ".",      "*",      "=",
        "<>",     "::",    "+",      "-",
    };
    var len: usize = 0;
    const part_count = random.intRangeLessThan(usize, 1, 36);
    for (0..part_count) |index| {
        if (index != 0 and random.boolean()) appendFuzzSqlByte(buffer, &len, ' ');
        appendFuzzSqlPart(buffer, &len, parts[random.intRangeLessThan(usize, 0, parts.len)]);
    }
    return buffer[0..len];
}

fn mutatedFuzzSql(random: std.Random, seed: []const u8, buffer: []u8) []const u8 {
    const replacements = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_(),.*=<>+-' ";
    var len: usize = 0;
    for (seed) |byte| switch (random.intRangeLessThan(u8, 0, 16)) {
        0 => {},
        1 => appendFuzzSqlByte(buffer, &len, replacements[random.intRangeLessThan(usize, 0, replacements.len)]),
        2 => {
            appendFuzzSqlByte(buffer, &len, byte);
            appendFuzzSqlByte(buffer, &len, byte);
        },
        3 => {
            appendFuzzSqlByte(buffer, &len, byte);
            appendFuzzSqlByte(buffer, &len, replacements[random.intRangeLessThan(usize, 0, replacements.len)]);
        },
        else => appendFuzzSqlByte(buffer, &len, byte),
    };
    return buffer[0..len];
}

fn exerciseFuzzSql(sql: []const u8) !void {
    parseSqlAlloc(std.testing.allocator, sql) catch |err| switch (err) {
        error.UnsupportedSqlShape => return,
        error.UnexpectedToken => {
            const diagnostic = (diagnosticSqlAlloc(std.testing.allocator, sql) catch |diagnostic_err| switch (diagnostic_err) {
                error.UnsupportedSqlShape => return,
                else => return diagnostic_err,
            }) orelse return error.ExpectedDiagnostic;
            defer diagnostic.deinit(std.testing.allocator);
            try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
            try std.testing.expect(diagnostic.source_end <= sql.len);
            try std.testing.expect(diagnostic.expected.len > 0);
        },
        else => return err,
    };
}

test "generated parser bridge deterministically fuzzes lexer parser and diagnostics" {
    const seeds = [_][]const u8{
        "SELECT id, status FROM docs WHERE status = 'open' ORDER BY id LIMIT 5",
        "WITH source_rows AS (SELECT id FROM docs) SELECT id FROM source_rows",
        "INSERT INTO docs (id, status) VALUES ('u1', 'open') ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        "CREATE TABLE docs (id text PRIMARY KEY, status text)",
        "CREATE GRAPH METRIC docs_pagerank ON doc_edges",
        "EXPLAIN (FORMAT JSON, ANALYZE ON) SELECT id FROM docs",
    };

    var prng = std.Random.DefaultPrng.init(0x514c_f077);
    const random = prng.random();
    for (0..384) |case_index| {
        var buffer: [256]u8 = undefined;
        const sql = if (case_index % 3 == 0)
            randomFuzzSql(random, &buffer)
        else
            mutatedFuzzSql(random, seeds[random.intRangeLessThan(usize, 0, seeds.len)], &buffer);
        try exerciseFuzzSql(sql);
    }
}
