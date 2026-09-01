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

const keyword_terminal_ids = blk: {
    @setEvalBranchQuota(250_000);
    const keyword_info = @typeInfo(token_mod.TokenKeyword).@"enum";
    const generated_info = @typeInfo(generated.Token).@"enum";
    var result: [keyword_info.field_names.len]?u16 = @splat(null);
    for (keyword_info.field_names, 0..) |keyword_name, keyword_index| {
        for (generated_info.field_names, generated_info.field_values) |generated_name, generated_value| {
            if (std.ascii.eqlIgnoreCase(keyword_name, generated_name)) {
                result[keyword_index] = @intCast(generated_value + 1);
                break;
            }
        }
    }
    break :blk result;
};

pub const ParseDiagnostic = struct {
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

pub const Diagnostic = union(enum) {
    lexical: lexer.LexDiagnostic,
    syntax: ParseDiagnostic,

    pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        switch (self) {
            .lexical => {},
            .syntax => |diagnostic| diagnostic.deinit(allocator),
        }
    }

    pub fn sourceSpan(self: @This()) token_mod.SourceSpan {
        return switch (self) {
            .lexical => |diagnostic| diagnostic.sourceSpan(),
            .syntax => |diagnostic| .{ .start = diagnostic.source_start, .end = diagnostic.source_end },
        };
    }

    pub fn message(self: @This()) []const u8 {
        return switch (self) {
            .lexical => |diagnostic| diagnostic.message(),
            .syntax => "unexpected SQL token",
        };
    }
};

/// A single-pass, user-facing parse result. Diagnostics are produced by the
/// same parser attempt that validates the statement, so interactive callers
/// never need to parse invalid SQL twice.
pub const ParseResult = union(enum) {
    success,
    diagnostic: Diagnostic,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        switch (self.*) {
            .success => {},
            .diagnostic => |diagnostic| diagnostic.deinit(allocator),
        }
        self.* = .success;
    }
};

const DiagnosticTokenIds = struct {
    token_ids: []u16,
    fallback_token_ids: []u16,
    source_indexes: []usize,

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.token_ids);
        allocator.free(self.fallback_token_ids);
        allocator.free(self.source_indexes);
        self.* = .{ .token_ids = &.{}, .fallback_token_ids = &.{}, .source_indexes = &.{} };
    }
};

const DiagnosticTokenSlices = struct {
    token_ids: []const u16,
    fallback_token_ids: []const u16,
    source_indexes: []const usize,
};

fn sourceTokenIndex(source_indexes: []const usize, generated_token_index: usize) usize {
    if (generated_token_index < source_indexes.len) return source_indexes[generated_token_index];
    return if (source_indexes.len == 0) 0 else source_indexes[source_indexes.len - 1] + 1;
}

fn endSourceOffset(source_indexes: []const usize, tokens: []const token_mod.Token) usize {
    const end_token = sourceTokenIndex(source_indexes, source_indexes.len);
    if (end_token == 0 or tokens.len == 0) return 0;
    return tokens[@min(end_token - 1, tokens.len - 1)].source_end;
}

const OwnedTokenIds = struct {
    token_ids: []u16,
    fallback_token_ids: []u16,

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.token_ids);
        allocator.free(self.fallback_token_ids);
        self.* = .{ .token_ids = &.{}, .fallback_token_ids = &.{} };
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
    if (tokens.len > generated_token_id_stack_capacity) {
        var token_ids = try tokenIdsWithFallbackAlloc(allocator, tokens);
        defer token_ids.deinit(allocator);
        return parseGeneratedTokenIds(allocator, token_ids.token_ids, token_ids.fallback_token_ids);
    }
    var token_id_buffer: [generated_token_id_stack_capacity]u16 = undefined;
    var fallback_id_buffer: [generated_token_id_stack_capacity]u16 = undefined;
    if (tokenIdsIntoBuffers(tokens, &token_id_buffer, &fallback_id_buffer)) |token_ids| {
        return parseGeneratedTokenIds(allocator, token_ids.token_ids, token_ids.fallback_token_ids);
    } else |err| switch (err) {
        error.NoSpaceLeft => {
            var token_ids = try tokenIdsWithFallbackAlloc(allocator, tokens);
            defer token_ids.deinit(allocator);
            return parseGeneratedTokenIds(allocator, token_ids.token_ids, token_ids.fallback_token_ids);
        },
        else => return err,
    }
}

pub fn parseSqlResultAlloc(allocator: std.mem.Allocator, sql: []const u8) !ParseResult {
    var tokens = switch (try lexer.tokenizeDiagnosticAlloc(allocator, sql)) {
        .diagnostic => |diagnostic| return .{ .diagnostic = .{ .lexical = diagnostic } },
        .tokens => |tokens| tokens,
    };
    defer lexer.freeTokens(allocator, &tokens);
    var diagnostic = try diagnosticTokensAlloc(allocator, tokens.items) orelse return .success;
    if (diagnostic.token_index >= tokens.items.len) {
        diagnostic.source_start = sql.len;
        diagnostic.source_end = sql.len;
    }
    return .{ .diagnostic = .{ .syntax = diagnostic } };
}

/// Compatibility helper for callers that only need an optional diagnostic.
/// New request paths should prefer `parseSqlResultAlloc` so success/failure
/// and the actionable diagnostic are observed as one operation.
pub fn diagnosticSqlAlloc(allocator: std.mem.Allocator, sql: []const u8) !?Diagnostic {
    return switch (try parseSqlResultAlloc(allocator, sql)) {
        .success => null,
        .diagnostic => |diagnostic| diagnostic,
    };
}

pub fn diagnosticTokensAlloc(allocator: std.mem.Allocator, tokens: []const token_mod.Token) !?ParseDiagnostic {
    if (tokens.len > generated_token_id_stack_capacity) {
        var diagnostic_tokens = try diagnosticTokenIdsAlloc(allocator, tokens);
        defer diagnostic_tokens.deinit(allocator);
        return parseDiagnosticFromTokenIds(allocator, tokens, diagnostic_tokens.token_ids, diagnostic_tokens.fallback_token_ids, diagnostic_tokens.source_indexes);
    }
    var token_id_buffer: [generated_token_id_stack_capacity]u16 = undefined;
    var fallback_id_buffer: [generated_token_id_stack_capacity]u16 = undefined;
    var source_index_buffer: [generated_token_id_stack_capacity]usize = undefined;
    if (diagnosticTokenIdsIntoBuffers(tokens, &token_id_buffer, &fallback_id_buffer, &source_index_buffer)) |diagnostic_tokens| {
        return parseDiagnosticFromTokenIds(allocator, tokens, diagnostic_tokens.token_ids, diagnostic_tokens.fallback_token_ids, diagnostic_tokens.source_indexes);
    } else |err| switch (err) {
        error.NoSpaceLeft => {
            var diagnostic_tokens = try diagnosticTokenIdsAlloc(allocator, tokens);
            defer diagnostic_tokens.deinit(allocator);
            return parseDiagnosticFromTokenIds(allocator, tokens, diagnostic_tokens.token_ids, diagnostic_tokens.fallback_token_ids, diagnostic_tokens.source_indexes);
        },
        else => return err,
    }
}

fn parseDiagnosticFromTokenIds(
    allocator: std.mem.Allocator,
    tokens: []const token_mod.Token,
    token_ids: []const u16,
    fallback_token_ids: []const u16,
    source_indexes: []const usize,
) !?ParseDiagnostic {
    const generated_diagnostic = try parseGeneratedTokenIdsDiagnostic(
        allocator,
        token_ids,
        fallback_token_ids,
    ) orelse return null;
    errdefer generated_diagnostic.deinit(allocator);

    const source_token_index = sourceTokenIndex(source_indexes, generated_diagnostic.token_index);
    const source_start = if (source_token_index < tokens.len)
        tokens[source_token_index].source_start
    else
        endSourceOffset(source_indexes, tokens);
    const source_end = if (source_token_index < tokens.len)
        tokens[source_token_index].source_end
    else
        endSourceOffset(source_indexes, tokens);
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
    try appendAllTokenIds(&sink, null, tokens, null, allocator);
    return ids.toOwnedSlice(allocator);
}

const TokenIdSlices = struct {
    token_ids: []const u16,
    fallback_token_ids: []const u16,
};

fn tokenIdsIntoBuffers(tokens: []const token_mod.Token, buffer: []u16, fallback_buffer: []u16) !TokenIdSlices {
    var sink = TokenIdSink{ .buffer = .{ .items = buffer } };
    var fallback_sink = TokenIdSink{ .buffer = .{ .items = fallback_buffer } };
    try appendAllTokenIds(&sink, &fallback_sink, tokens, null, null);
    return .{
        .token_ids = sink.buffer.items[0..sink.buffer.len],
        .fallback_token_ids = fallback_sink.buffer.items[0..fallback_sink.buffer.len],
    };
}

fn tokenIdsWithFallbackAlloc(allocator: std.mem.Allocator, tokens: []const token_mod.Token) !OwnedTokenIds {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    var fallback_ids: std.ArrayListUnmanaged(u16) = .empty;
    errdefer ids.deinit(allocator);
    errdefer fallback_ids.deinit(allocator);

    var sink = TokenIdSink{ .list = .{ .items = &ids, .allocator = allocator } };
    var fallback_sink = TokenIdSink{ .list = .{ .items = &fallback_ids, .allocator = allocator } };
    try appendAllTokenIds(&sink, &fallback_sink, tokens, null, allocator);

    const token_ids = try ids.toOwnedSlice(allocator);
    errdefer allocator.free(token_ids);
    return .{
        .token_ids = token_ids,
        .fallback_token_ids = try fallback_ids.toOwnedSlice(allocator),
    };
}

fn diagnosticTokenIdsAlloc(allocator: std.mem.Allocator, tokens: []const token_mod.Token) !DiagnosticTokenIds {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    var fallback_ids: std.ArrayListUnmanaged(u16) = .empty;
    var source_indexes: std.ArrayListUnmanaged(usize) = .empty;
    errdefer ids.deinit(allocator);
    errdefer fallback_ids.deinit(allocator);
    errdefer source_indexes.deinit(allocator);

    var sink = TokenIdSink{ .list = .{ .items = &ids, .allocator = allocator } };
    var fallback_sink = TokenIdSink{ .list = .{ .items = &fallback_ids, .allocator = allocator } };
    var source_sink = SourceIndexSink{ .list = .{ .items = &source_indexes, .allocator = allocator } };
    try appendAllTokenIds(&sink, &fallback_sink, tokens, &source_sink, allocator);

    const token_ids = try ids.toOwnedSlice(allocator);
    errdefer allocator.free(token_ids);
    const fallback_token_ids = try fallback_ids.toOwnedSlice(allocator);
    errdefer allocator.free(fallback_token_ids);
    return .{
        .token_ids = token_ids,
        .fallback_token_ids = fallback_token_ids,
        .source_indexes = try source_indexes.toOwnedSlice(allocator),
    };
}

fn diagnosticTokenIdsIntoBuffers(
    tokens: []const token_mod.Token,
    token_buffer: []u16,
    fallback_buffer: []u16,
    source_buffer: []usize,
) !DiagnosticTokenSlices {
    var sink = TokenIdSink{ .buffer = .{ .items = token_buffer } };
    var fallback_sink = TokenIdSink{ .buffer = .{ .items = fallback_buffer } };
    var source_sink = SourceIndexSink{ .buffer = .{ .items = source_buffer } };
    try appendAllTokenIds(&sink, &fallback_sink, tokens, &source_sink, null);
    return .{
        .token_ids = sink.buffer.items[0..sink.buffer.len],
        .fallback_token_ids = fallback_sink.buffer.items[0..fallback_sink.buffer.len],
        .source_indexes = source_sink.buffer.items[0..source_sink.buffer.len],
    };
}

const SourceIndexSink = union(enum) {
    list: struct {
        items: *std.ArrayListUnmanaged(usize),
        allocator: std.mem.Allocator,
    },
    buffer: struct {
        items: []usize,
        len: usize = 0,
    },

    fn append(self: *@This(), index: usize) !void {
        switch (self.*) {
            .list => |list| try list.items.append(list.allocator, index),
            .buffer => |*buffer| {
                if (buffer.len == buffer.items.len) return error.NoSpaceLeft;
                buffer.items[buffer.len] = index;
                buffer.len += 1;
            },
        }
    }
};

/// Carries statement-shape facts and delimiter state through token mapping.
/// Contextual keyword classification runs on every token, so facts that need a
/// statement scan are computed once and nesting state advances monotonically.
const TokenMappingContext = struct {
    // Parenthesis-scoped fields are indexed by paren_depth; fields whose
    // original grammar context treats brackets as nesting are indexed by
    // delimiter_depth. Keeping both in one compact array preserves the
    // allocation-free common path without conflating their depth rules.
    const ScopeState = struct {
        in_select_projection: bool = false,
        is_cast_scope: bool = false,
        saw_order_by: bool = false,
        function_argument_name_allowed: bool = false,
    };

    paren_depth: usize = 0,
    delimiter_depth: usize = 0,
    balanced_prefix: bool = true,
    segment_start: ?usize = null,
    scopes: []ScopeState,
    owned_scopes: []ScopeState = &.{},
    create_table: bool,
    prepare: bool,
    alter_table_tail_start: ?usize,
    alter_table_delimiter_depth: usize = 0,
    alter_table_saw_top_level_comma: bool = false,
    create_routine_close: ?usize,
    create_index_elements: ?TokenRange,

    fn init(
        allocator: ?std.mem.Allocator,
        tokens: []const token_mod.Token,
        fixed_scopes: []ScopeState,
    ) !@This() {
        const required_scopes = tokens.len + 1;
        var owned_scopes: []ScopeState = &.{};
        const scopes = if (required_scopes <= fixed_scopes.len)
            fixed_scopes[0..required_scopes]
        else blk: {
            const dynamic_allocator = allocator orelse return error.NoSpaceLeft;
            owned_scopes = try dynamic_allocator.alloc(ScopeState, required_scopes);
            break :blk owned_scopes;
        };
        @memset(scopes, .{});
        return .{
            .scopes = scopes,
            .owned_scopes = owned_scopes,
            .create_table = tokens.len >= 2 and tokens[0].matchesKeywordTag(.create) and tokens[1].matchesKeywordTag(.table),
            .prepare = tokens.len > 0 and tokens[0].matchesKeywordTag(.prepare),
            .alter_table_tail_start = alterTableTailStart(tokens),
            .create_routine_close = createRoutineArgumentClose(tokens),
            .create_index_elements = createIndexElementRange(tokens),
        };
    }

    fn deinit(self: *@This(), allocator: ?std.mem.Allocator) void {
        if (self.owned_scopes.len > 0) allocator.?.free(self.owned_scopes);
        self.* = undefined;
    }

    fn inSelectProjection(self: *const @This()) bool {
        return self.scopes[self.paren_depth].in_select_projection;
    }

    fn sawOrderBy(self: *const @This()) bool {
        return self.scopes[self.delimiter_depth].saw_order_by;
    }

    fn inCastScope(self: *const @This()) bool {
        return self.scopes[self.paren_depth].is_cast_scope;
    }

    fn functionArgumentNameAllowed(self: *const @This()) bool {
        return self.scopes[self.delimiter_depth].function_argument_name_allowed;
    }

    fn advance(self: *@This(), tokens: []const token_mod.Token, index: usize) void {
        const token = tokens[index];
        if (self.alter_table_tail_start) |tail_start| {
            if (index >= tail_start) switch (token.kind) {
                .lparen, .lbracket => self.alter_table_delimiter_depth += 1,
                .rparen, .rbracket => if (self.alter_table_delimiter_depth > 0) {
                    self.alter_table_delimiter_depth -= 1;
                },
                .comma => if (self.alter_table_delimiter_depth == 0) {
                    self.alter_table_saw_top_level_comma = true;
                },
                else => {},
            };
        }

        if (token.matchesKeywordTag(.select)) {
            self.scopes[self.paren_depth].in_select_projection = true;
        } else if (token.matchesKeywordTag(.from) and self.inSelectProjection()) {
            self.scopes[self.paren_depth].in_select_projection = false;
        }
        if (token.matchesKeywordTag(.by) and index > 0 and tokens[index - 1].matchesKeywordTag(.order)) {
            self.scopes[self.delimiter_depth].saw_order_by = true;
        }

        switch (token.kind) {
            .lparen => {
                self.paren_depth += 1;
                self.scopes[self.paren_depth].in_select_projection = false;
                self.scopes[self.paren_depth].is_cast_scope = index > 0 and tokens[index - 1].matchesKeywordTag(.cast);
                self.delimiter_depth += 1;
                self.scopes[self.delimiter_depth].saw_order_by = false;
                self.scopes[self.delimiter_depth].function_argument_name_allowed = index > 0 and tokens[index - 1].kind == .identifier;
                if (self.paren_depth == 1) self.segment_start = index + 1;
            },
            .lbracket => {
                self.delimiter_depth += 1;
                self.scopes[self.delimiter_depth].saw_order_by = false;
                self.scopes[self.delimiter_depth].function_argument_name_allowed = index > 0 and tokens[index - 1].kind == .identifier;
            },
            .rparen => {
                if (self.delimiter_depth > 0) {
                    self.scopes[self.delimiter_depth].saw_order_by = false;
                    self.scopes[self.delimiter_depth].function_argument_name_allowed = false;
                    self.delimiter_depth -= 1;
                }
                if (self.paren_depth == 0) {
                    self.balanced_prefix = false;
                    self.segment_start = null;
                    return;
                }
                self.scopes[self.paren_depth].in_select_projection = false;
                self.scopes[self.paren_depth].is_cast_scope = false;
                self.paren_depth -= 1;
                if (self.paren_depth == 0) self.segment_start = null;
            },
            .rbracket => if (self.delimiter_depth > 0) {
                self.scopes[self.delimiter_depth].saw_order_by = false;
                self.scopes[self.delimiter_depth].function_argument_name_allowed = false;
                self.delimiter_depth -= 1;
            },
            .comma => {
                self.scopes[self.delimiter_depth].function_argument_name_allowed = true;
                if (self.paren_depth == 1) self.segment_start = index + 1;
            },
            .semicolon => {
                self.scopes[self.paren_depth].in_select_projection = false;
                self.scopes[self.delimiter_depth].saw_order_by = false;
                self.scopes[self.delimiter_depth].function_argument_name_allowed = false;
            },
            else => {},
        }
    }
};

fn appendAllTokenIds(
    ids: *TokenIdSink,
    fallback_ids: ?*TokenIdSink,
    tokens: []const token_mod.Token,
    source_indexes: ?*SourceIndexSink,
    allocator: ?std.mem.Allocator,
) !void {
    var fixed_scopes: [generated_token_id_stack_capacity + 1]TokenMappingContext.ScopeState = undefined;
    var mapping_context = try TokenMappingContext.init(allocator, tokens, &fixed_scopes);
    defer mapping_context.deinit(allocator);
    const trailing_semicolon_start = statementTokenEnd(tokens);
    for (tokens, 0..) |token, index| {
        if (index == trailing_semicolon_start) break;
        const previous = if (index > 0) tokens[index - 1] else null;
        const next = if (index + 1 < tokens.len) tokens[index + 1] else null;
        const before = ids.len();
        try appendTokenIds(ids, tokens, index, token, previous, next, &mapping_context);
        const added = ids.len() - before;
        if (fallback_ids) |fallback_sink| {
            const fallback_id = fallbackTokenId(token);
            for (0..added) |_| try fallback_sink.append(fallback_id);
        }
        if (source_indexes) |source_sink| {
            for (0..added) |_| try source_sink.append(index);
        }
        mapping_context.advance(tokens, index);
    }
}

fn parseGeneratedTokenIds(allocator: std.mem.Allocator, token_ids: []const u16, fallback_token_ids: []const u16) !void {
    if (token_ids.len + 1 <= generated_parse_stack_capacity) {
        var stack_buffer: [generated_parse_stack_capacity]u16 = undefined;
        generated.parseWithFallbackStackBuffer(token_ids, fallback_token_ids, &stack_buffer) catch |err| switch (err) {
            error.StackOverflow => return generated.parseWithFallback(allocator, token_ids, fallback_token_ids),
            else => return err,
        };
        return;
    }
    return generated.parseWithFallback(allocator, token_ids, fallback_token_ids);
}

fn parseGeneratedTokenIdsDiagnostic(
    allocator: std.mem.Allocator,
    token_ids: []const u16,
    fallback_token_ids: []const u16,
) !?generated.ParseDiagnostic {
    if (token_ids.len + 1 <= generated_parse_stack_capacity) {
        var stack_buffer: [generated_parse_stack_capacity]u16 = undefined;
        return generated.parseDiagnosticWithFallbackStackBuffer(allocator, token_ids, fallback_token_ids, &stack_buffer) catch |err| switch (err) {
            error.StackOverflow => generated.parseDiagnosticWithFallback(allocator, token_ids, fallback_token_ids),
            else => return err,
        };
    }
    return generated.parseDiagnosticWithFallback(allocator, token_ids, fallback_token_ids);
}

fn fallbackTokenId(token: token_mod.Token) u16 {
    if (token.kind != .identifier) return 0;
    const keyword = token.keyword orelse return 0;
    return switch (token_mod.keywordClass(keyword)) {
        .unreserved, .column_name => generated.tokenId(.IDENT),
        .type_function_name, .reserved => 0,
    };
}

fn appendTokenIds(
    ids: *TokenIdSink,
    tokens: []const token_mod.Token,
    index: usize,
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
    mapping_context: *const TokenMappingContext,
) !void {
    switch (token.kind) {
        .identifier => {
            if (contextualKeywordSymbolId(tokens, index, token, previous, next, mapping_context)) |id| {
                try ids.append(id);
                return;
            }
            if (generatedParserTreatsKeywordAsIdentifier(tokens, index, token, previous, next, mapping_context)) {
                try appendIdentifierIds(ids, token.text, false);
                return;
            }
            if (keywordSymbolId(token)) |id| {
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
    mapping_context: *const TokenMappingContext,
) ?u16 {
    if (sessionAuthorizationKeywordContext(tokens, index)) {
        if (token.matchesKeywordTag(.authorization)) return generated.tokenId(.AUTHORIZATION);
        if (token.matchesKeywordTag(.session)) return generated.tokenId(.SESSION);
    }
    if (indexConcurrentlyKeywordContext(tokens, index) and token.matchesKeywordTag(.concurrently)) return generated.tokenId(.CONCURRENTLY);
    if (indexElementCollateKeywordContext(index, token, previous, next, mapping_context)) return generated.tokenId(.COLLATE);
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
    if (token.matchesKeywordTag(.match) and
        alterTableDiagnosticIdentifierContext(index, mapping_context) and
        !alterTableForeignKeyMatchClauseContext(tokens, index))
    {
        return generated.tokenId(.IDENT);
    }
    if ((token.matchesKeywordTag(.current) or token.matchesKeywordTag(.from) or token.matchesKeywordTag(.rows) or token.matchesKeywordTag(.window)) and
        createRoutineDiagnosticIdentifierContext(index, mapping_context))
    {
        return generated.tokenId(.IDENT);
    }
    if (createTriggerDiagnosticIdentifierContext(tokens, index) and createTriggerDiagnosticKeywordAsIdentifier(token)) {
        return generated.tokenId(.IDENT);
    }
    if (functionArgumentNameIdentifierContext(token, next, mapping_context)) return generated.tokenId(.IDENT);
    if (matchKeywordAliasContext(token, previous, next, mapping_context)) return generated.tokenId(.IDENT);
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

fn functionArgumentNameIdentifierContext(
    token: token_mod.Token,
    next: ?token_mod.Token,
    mapping_context: *const TokenMappingContext,
) bool {
    if (!token.matchesKeywordTag(.@"return")) return false;
    const next_token = next orelse return false;
    return next_token.kind == .eq and mapping_context.functionArgumentNameAllowed();
}

fn matchKeywordAliasContext(
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
    mapping_context: *const TokenMappingContext,
) bool {
    if (!token.matchesKeywordTag(.match) or previous == null) return false;
    if (previous.?.matchesKeywordTag(.as)) return true;
    if (!mapping_context.inSelectProjection()) return false;
    const next_token = next orelse return true;
    return next_token.matchesKeywordTag(.from) or
        next_token.matchesKeywordTag(.order) or
        next_token.matchesKeywordTag(.group) or
        next_token.matchesKeywordTag(.having) or
        next_token.matchesKeywordTag(.limit) or
        next_token.kind == .comma or
        next_token.kind == .semicolon;
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

fn alterTableDiagnosticIdentifierContext(index: usize, mapping_context: *const TokenMappingContext) bool {
    const tail_start = mapping_context.alter_table_tail_start orelse return false;
    return index >= tail_start and mapping_context.alter_table_saw_top_level_comma;
}

fn alterTableForeignKeyMatchClauseContext(tokens: []const token_mod.Token, index: usize) bool {
    return index > 0 and index + 1 < tokens.len and tokens[index - 1].kind == .rparen and
        (tokens[index + 1].matchesKeywordTag(.full) or tokens[index + 1].kind == .identifier);
}

fn createRoutineDiagnosticIdentifierContext(index: usize, mapping_context: *const TokenMappingContext) bool {
    const close = mapping_context.create_routine_close orelse return false;
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
    index: usize,
    token: token_mod.Token,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
    mapping_context: *const TokenMappingContext,
) bool {
    if (!token.matchesKeywordTag(.collate) or previous == null or next == null or next.?.kind != .identifier) return false;
    switch (previous.?.kind) {
        .lparen, .comma, .semicolon => return false,
        else => {},
    }
    const elements = mapping_context.create_index_elements orelse return false;
    return index > elements.start and index < elements.end;
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
    mapping_context: *const TokenMappingContext,
) bool {
    // PostgreSQL permits every keyword as an attribute name after DOT. A
    // qualified name's first component follows ColId rules: IDENT,
    // unreserved_keyword, or col_name_keyword. The categories come from
    // generated metadata audited against the grammar's pinned PostgreSQL
    // revision rather than an ad hoc runtime exception list.
    const keyword = token.keyword orelse return false;
    const class = token_mod.keywordClass(keyword);
    if (class == .type_function_name and typeFunctionNameContext(tokens, index, previous, next, mapping_context)) return true;
    if (previous != null and previous.?.kind == .dot) return true;
    if (next != null and next.?.kind == .dot) {
        return class == .unreserved or class == .column_name;
    }
    if (token.matchesKeywordTag(.conflict)) return previous != null and previous.?.matchesKeywordTag(.as);
    if (token.matchesKeywordTag(.offset)) return next != null and next.?.matchesKeywordTag(.limit);
    if (token.matchesKeywordTag(.fetch)) return previous != null and previous.?.matchesKeywordTag(.as);
    if (token.matchesKeywordTag(.start)) return next != null and next.?.kind == .eq;
    if (token.matchesKeywordTag(.rows)) {
        if (mapping_context.sawOrderBy()) return false;
        return previous != null and previous.?.kind == .identifier and next != null and next.?.kind == .number;
    }
    if (token.matchesKeywordTag(.window)) return previous != null and previous.?.kind == .identifier and (next == null or next.?.kind == .semicolon);
    return false;
}

fn typeFunctionNameContext(
    tokens: []const token_mod.Token,
    index: usize,
    previous: ?token_mod.Token,
    next: ?token_mod.Token,
    mapping_context: *const TokenMappingContext,
) bool {
    // PostgreSQL's type/function-name category is accepted as the unqualified
    // head of a function name and anywhere a type name is syntactically
    // explicit. Keep this contextual: treating these words as ColId globally
    // would incorrectly accept constructs such as `FROM join`.
    if (next != null and next.?.kind == .lparen) return true;
    if (previous != null and previous.?.kind == .colon_colon) return true;
    if (ddlTypeFunctionNameContext(tokens, index, mapping_context)) return true;
    return previous != null and previous.?.matchesKeywordTag(.as) and mapping_context.inCastScope();
}

fn ddlTypeFunctionNameContext(
    tokens: []const token_mod.Token,
    index: usize,
    mapping_context: *const TokenMappingContext,
) bool {
    if (index == 0) return false;
    if (tokens.len >= 4 and tokens[0].matchesKeywordTag(.create) and tokens[1].matchesKeywordTag(.domain)) {
        return tokens[index - 1].matchesKeywordTag(.as);
    }
    if (tokens.len >= 5 and tokens[0].matchesKeywordTag(.alter) and tokens[1].matchesKeywordTag(.table)) {
        if (index < 4 or tokens[index - 1].kind != .identifier) return false;
        return tokens[index - 2].matchesKeywordTag(.add) or
            (index >= 5 and tokens[index - 2].matchesKeywordTag(.column) and tokens[index - 3].matchesKeywordTag(.add));
    }

    if (tokens.len < 4 or (!mapping_context.create_table and !mapping_context.prepare)) return false;
    if (!mapping_context.balanced_prefix or mapping_context.paren_depth != 1) return false;
    const start = mapping_context.segment_start orelse return false;
    return if (mapping_context.create_table)
        index == start + 1 and start < tokens.len and tokens[start].kind == .identifier
    else
        index == start;
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

fn keywordSymbolId(token: token_mod.Token) ?u16 {
    const keyword = token.keyword orelse return null;
    return keyword_terminal_ids[@backingInt(keyword)];
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

fn alterTableTailStart(tokens: []const token_mod.Token) ?usize {
    if (tokens.len < 4 or !tokens[0].matchesKeywordTag(.alter) or !tokens[1].matchesKeywordTag(.table)) return null;
    var cursor: usize = 2;
    _ = consumeIfExists(tokens, &cursor, tokens.len);
    if (cursor < tokens.len and tokens[cursor].matchesKeywordTag(.only)) cursor += 1;
    const table = qualifiedNameRange(tokens, cursor, tokens.len) orelse return null;
    return table.end;
}

fn createRoutineArgumentClose(tokens: []const token_mod.Token) ?usize {
    if (tokens.len < 5 or !tokens[0].matchesKeywordTag(.create)) return null;
    var cursor: usize = 1;
    if (cursor + 1 < tokens.len and tokens[cursor].matchesKeywordTag(.@"or") and tokens[cursor + 1].matchesKeywordTag(.replace)) cursor += 2;
    if (cursor >= tokens.len or
        !(tokens[cursor].matchesKeywordTag(.function) or tokens[cursor].matchesKeywordTag(.procedure))) return null;
    cursor += 1;
    const routine_name = qualifiedNameRange(tokens, cursor, tokens.len) orelse return null;
    cursor = routine_name.end;
    return findMatchingParen(tokens, cursor, tokens.len);
}

fn createIndexElementRange(tokens: []const token_mod.Token) ?TokenRange {
    if (tokens.len < 6 or !tokens[0].matchesKeywordTag(.create)) return null;
    var open_index: ?usize = null;
    var depth: usize = 0;
    for (tokens, 0..) |candidate, candidate_index| switch (candidate.kind) {
        .lparen => {
            if (depth == 0 and open_index == null and candidate_index > 0) open_index = candidate_index;
            depth += 1;
        },
        .rparen => {
            if (depth == 0) return null;
            depth -= 1;
            if (depth == 0) {
                const open = open_index orelse continue;
                var saw_index = false;
                var saw_on = false;
                for (tokens[0..open]) |prefix| {
                    if (prefix.matchesKeywordTag(.index)) saw_index = true;
                    if (prefix.matchesKeywordTag(.on)) saw_on = true;
                }
                return if (saw_index and saw_on) .{ .start = open, .end = candidate_index } else null;
            }
        },
        else => {},
    };
    return null;
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
                switch (diagnostic) {
                    .lexical => |lexical| std.debug.print("lexical={s} span={d}..{d}\n", .{ lexical.message(), lexical.source_start, lexical.source_end }),
                    .syntax => |syntax| std.debug.print("actual={s} token={d} expected={any}\n", .{ syntax.actual, syntax.token_index, syntax.expected }),
                }
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

    var identifier_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT analyze_verbose, system_time_for FROM docs");
    defer lexer.freeTokens(std.testing.allocator, &identifier_tokens);
    try std.testing.expectEqual(@as(?token_mod.TokenKeyword, null), identifier_tokens.items[1].keyword);
    try std.testing.expectEqual(@as(?token_mod.TokenKeyword, null), identifier_tokens.items[3].keyword);
    try parseTokensAlloc(std.testing.allocator, identifier_tokens.items);
}

test "generated parser bridge preserves qualified keyword categories" {
    try parseSqlAlloc(std.testing.allocator, "SELECT public.select FROM public.docs");

    var reserved_head_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT select.foo FROM docs");
    defer lexer.freeTokens(std.testing.allocator, &reserved_head_tokens);
    const reserved_head_ids = try tokenIdsAlloc(std.testing.allocator, reserved_head_tokens.items);
    defer std.testing.allocator.free(reserved_head_ids);
    try std.testing.expectEqual(generated.tokenId(.SELECT), reserved_head_ids[1]);
    try std.testing.expectError(error.UnexpectedToken, parseTokensAlloc(std.testing.allocator, reserved_head_tokens.items));

    var unreserved_head_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT public.docs.id FROM public.docs");
    defer lexer.freeTokens(std.testing.allocator, &unreserved_head_tokens);
    const unreserved_head_ids = try tokenIdsAlloc(std.testing.allocator, unreserved_head_tokens.items);
    defer std.testing.allocator.free(unreserved_head_ids);
    try std.testing.expectEqual(generated.tokenId(.IDENT), unreserved_head_ids[1]);

    const identifier_compatibility = [_][]const u8{
        "SELECT schema.docs FROM docs",
        "SELECT role.docs FROM docs",
        "SELECT set.docs FROM docs",
        "SELECT server.docs FROM docs",
        "SELECT session.docs FROM docs",
        "SELECT 1 AS schema FROM docs",
        "SELECT * FROM schema",
    };
    for (identifier_compatibility) |sql| try parseSqlAlloc(std.testing.allocator, sql);
}

test "generated parser bridge honors PostgreSQL type function keyword contexts" {
    const valid = [_][]const u8{
        "SELECT join(id), authorization(id), collation(id), verbose(id) FROM docs",
        "SELECT payload::binary FROM docs",
        "SELECT CAST(payload AS binary) FROM docs",
        "SELECT public.join(id) FROM docs",
        "CREATE TABLE typed_docs (payload binary)",
        "CREATE TABLE typed_docs (payload binary, checksum binary, label text)",
        "CREATE DOMAIN binary_payload AS binary",
        "PREPARE typed_lookup (binary) AS SELECT 1",
    };
    for (valid) |sql| try parseSqlAlloc(std.testing.allocator, sql);

    // Type/function keywords are not general column identifiers (ColId).
    try std.testing.expectError(error.UnexpectedToken, parseSqlAlloc(std.testing.allocator, "SELECT * FROM join"));
}

test "generated parser bridge carries contextual DDL state in one pass" {
    var alter_tokens = try lexer.tokenizeAlloc(
        std.testing.allocator,
        "ALTER TABLE docs ADD COLUMN value text, MATCH unsupported tail",
    );
    defer lexer.freeTokens(std.testing.allocator, &alter_tokens);
    const alter_ids = try tokenIdsAlloc(std.testing.allocator, alter_tokens.items);
    defer std.testing.allocator.free(alter_ids);
    var match_index: ?usize = null;
    for (alter_tokens.items, 0..) |token, index| {
        if (token.matchesKeywordTag(.match)) match_index = index;
    }
    try std.testing.expectEqual(generated.tokenId(.IDENT), alter_ids[match_index.?]);

    var bracket_tokens = try lexer.tokenizeAlloc(
        std.testing.allocator,
        "ALTER TABLE docs ADD COLUMN value text[1, 2] MATCH unsupported tail",
    );
    defer lexer.freeTokens(std.testing.allocator, &bracket_tokens);
    const bracket_ids = try tokenIdsAlloc(std.testing.allocator, bracket_tokens.items);
    defer std.testing.allocator.free(bracket_ids);
    for (bracket_tokens.items, bracket_ids) |token, id| {
        if (token.matchesKeywordTag(.match)) try std.testing.expectEqual(generated.tokenId(.MATCH), id);
    }

    var routine_tokens = try lexer.tokenizeAlloc(
        std.testing.allocator,
        "CREATE FUNCTION f() CURRENT FROM ROWS WINDOW",
    );
    defer lexer.freeTokens(std.testing.allocator, &routine_tokens);
    const routine_ids = try tokenIdsAlloc(std.testing.allocator, routine_tokens.items);
    defer std.testing.allocator.free(routine_ids);
    for (routine_tokens.items, routine_ids) |token, id| {
        if (token.matchesKeywordTag(.current) or token.matchesKeywordTag(.from) or
            token.matchesKeywordTag(.rows) or token.matchesKeywordTag(.window))
        {
            try std.testing.expectEqual(generated.tokenId(.IDENT), id);
        }
    }
}

test "generated parser bridge carries scoped contextual keyword state in one pass" {
    var projection_tokens = try lexer.tokenizeAlloc(
        std.testing.allocator,
        "SELECT 1 match, (SELECT 2 match FROM docs) match FROM docs",
    );
    defer lexer.freeTokens(std.testing.allocator, &projection_tokens);
    const projection_ids = try tokenIdsAlloc(std.testing.allocator, projection_tokens.items);
    defer std.testing.allocator.free(projection_ids);
    var match_count: usize = 0;
    for (projection_tokens.items, projection_ids) |token, id| {
        if (token.matchesKeywordTag(.match)) {
            try std.testing.expectEqual(generated.tokenId(.IDENT), id);
            match_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), match_count);
    try parseTokensAlloc(std.testing.allocator, projection_tokens.items);

    var alias_rows_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT value ROWS 1");
    defer lexer.freeTokens(std.testing.allocator, &alias_rows_tokens);
    const alias_rows_ids = try tokenIdsAlloc(std.testing.allocator, alias_rows_tokens.items);
    defer std.testing.allocator.free(alias_rows_ids);
    try std.testing.expectEqual(generated.tokenId(.IDENT), alias_rows_ids[2]);

    var window_tokens = try lexer.tokenizeAlloc(
        std.testing.allocator,
        "SELECT sum(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM docs",
    );
    defer lexer.freeTokens(std.testing.allocator, &window_tokens);
    const window_ids = try tokenIdsAlloc(std.testing.allocator, window_tokens.items);
    defer std.testing.allocator.free(window_ids);
    for (window_tokens.items, window_ids) |token, id| {
        if (token.matchesKeywordTag(.rows)) try std.testing.expectEqual(generated.tokenId(.ROWS), id);
    }
    try parseTokensAlloc(std.testing.allocator, window_tokens.items);

    var named_argument_tokens = try lexer.tokenizeAlloc(
        std.testing.allocator,
        "SELECT score(RETURN = 1, nested(2), RETURN = 3) FROM docs",
    );
    defer lexer.freeTokens(std.testing.allocator, &named_argument_tokens);
    const named_argument_ids = try tokenIdsAlloc(std.testing.allocator, named_argument_tokens.items);
    defer std.testing.allocator.free(named_argument_ids);
    for (named_argument_tokens.items, named_argument_ids) |token, id| {
        if (token.matchesKeywordTag(.@"return")) try std.testing.expectEqual(generated.tokenId(.IDENT), id);
    }
    try parseTokensAlloc(std.testing.allocator, named_argument_tokens.items);

    var nested_argument_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT score((RETURN = 1)) FROM docs");
    defer lexer.freeTokens(std.testing.allocator, &nested_argument_tokens);
    const nested_argument_ids = try tokenIdsAlloc(std.testing.allocator, nested_argument_tokens.items);
    defer std.testing.allocator.free(nested_argument_ids);
    for (nested_argument_tokens.items, nested_argument_ids) |token, id| {
        if (token.matchesKeywordTag(.@"return")) try std.testing.expectEqual(generated.tokenId(.RETURN), id);
    }

    var nested_cast_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT CAST((payload) AS binary) FROM docs");
    defer lexer.freeTokens(std.testing.allocator, &nested_cast_tokens);
    const nested_cast_ids = try tokenIdsAlloc(std.testing.allocator, nested_cast_tokens.items);
    defer std.testing.allocator.free(nested_cast_ids);
    for (nested_cast_tokens.items, nested_cast_ids) |token, id| {
        if (token.matchesKeywordTag(.binary)) try std.testing.expectEqual(generated.tokenId(.IDENT), id);
    }
    try parseTokensAlloc(std.testing.allocator, nested_cast_tokens.items);

    var non_cast_tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT (payload AS binary) FROM docs");
    defer lexer.freeTokens(std.testing.allocator, &non_cast_tokens);
    const non_cast_ids = try tokenIdsAlloc(std.testing.allocator, non_cast_tokens.items);
    defer std.testing.allocator.free(non_cast_ids);
    for (non_cast_tokens.items, non_cast_ids) |token, id| {
        if (token.matchesKeywordTag(.binary)) try std.testing.expectEqual(generated.tokenId(.BINARY), id);
    }
}

test "generated parser bridge maps large contextual input without prefix scans" {
    var sql: std.ArrayListUnmanaged(u8) = .empty;
    defer sql.deinit(std.testing.allocator);
    try sql.appendSlice(std.testing.allocator, "SELECT score(");
    for (0..600) |index| {
        if (index != 0) try sql.appendSlice(std.testing.allocator, " + ");
        try sql.appendSlice(std.testing.allocator, "RETURN = 1");
    }
    try sql.appendSlice(std.testing.allocator, ")");

    var tokens = try lexer.tokenizeAlloc(std.testing.allocator, sql.items);
    defer lexer.freeTokens(std.testing.allocator, &tokens);
    try std.testing.expect(tokens.items.len > generated_token_id_stack_capacity);
    const ids = try tokenIdsAlloc(std.testing.allocator, tokens.items);
    defer std.testing.allocator.free(ids);

    var return_count: usize = 0;
    for (tokens.items, ids) |token, id| {
        if (token.matchesKeywordTag(.@"return")) {
            try std.testing.expectEqual(generated.tokenId(.IDENT), id);
            return_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 600), return_count);

    var alias_sql: std.ArrayListUnmanaged(u8) = .empty;
    defer alias_sql.deinit(std.testing.allocator);
    try alias_sql.appendSlice(std.testing.allocator, "SELECT ");
    for (0..600) |index| {
        if (index != 0) try alias_sql.appendSlice(std.testing.allocator, ", ");
        try alias_sql.appendSlice(std.testing.allocator, "1 AS JOIN");
    }

    var alias_tokens = try lexer.tokenizeAlloc(std.testing.allocator, alias_sql.items);
    defer lexer.freeTokens(std.testing.allocator, &alias_tokens);
    try std.testing.expect(alias_tokens.items.len > generated_token_id_stack_capacity);
    const alias_ids = try tokenIdsAlloc(std.testing.allocator, alias_tokens.items);
    defer std.testing.allocator.free(alias_ids);

    var join_count: usize = 0;
    for (alias_tokens.items, alias_ids) |token, id| {
        if (token.matchesKeywordTag(.join)) {
            try std.testing.expectEqual(generated.tokenId(.JOIN), id);
            join_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 600), join_count);
}

test "generated parser bridge trims only the trailing semicolon run" {
    var tokens = try lexer.tokenizeAlloc(std.testing.allocator, "SELECT 1; SELECT 2;;;");
    defer lexer.freeTokens(std.testing.allocator, &tokens);
    const ids = try tokenIdsAlloc(std.testing.allocator, tokens.items);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqual(tokens.items.len - 3, ids.len);
    try std.testing.expectEqual(generated.tokenId(.SEMICOLON), ids[2]);
    try std.testing.expectEqual(generated.tokenId(.NUMBER), ids[ids.len - 1]);
}

test "generated parser bridge exposes a single pass parse result" {
    var success = try parseSqlResultAlloc(std.testing.allocator, "SELECT café FROM données");
    defer success.deinit(std.testing.allocator);
    switch (success) {
        .success => {},
        .diagnostic => return error.ExpectedParseSuccess,
    }

    var syntax_failure = try parseSqlResultAlloc(std.testing.allocator, "SELECT FROM docs");
    defer syntax_failure.deinit(std.testing.allocator);
    switch (syntax_failure) {
        .success => return error.ExpectedSyntaxDiagnostic,
        .diagnostic => |diagnostic| switch (diagnostic) {
            .syntax => |syntax| try std.testing.expectEqualStrings("FROM", syntax.actual),
            .lexical => return error.ExpectedSyntaxDiagnostic,
        },
    }

    var lexical_failure = try parseSqlResultAlloc(std.testing.allocator, "SELECT \xff");
    defer lexical_failure.deinit(std.testing.allocator);
    switch (lexical_failure) {
        .success => return error.ExpectedLexicalDiagnostic,
        .diagnostic => |diagnostic| switch (diagnostic) {
            .lexical => |lexical| try std.testing.expectEqual(lexer.LexErrorKind.invalid_utf8, lexical.kind),
            .syntax => return error.ExpectedLexicalDiagnostic,
        },
    }

    const Runner = struct {
        fn run(allocator: std.mem.Allocator, sql: []const u8) !void {
            var result = try parseSqlResultAlloc(allocator, sql);
            defer result.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{"SELECT café FROM données"});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{"SELECT FROM docs"});
}

test "generated parser bridge reports owned source-aware diagnostics" {
    const sql = "SELECT FROM docs";
    const diagnostic = (try diagnosticSqlAlloc(std.testing.allocator, sql)).?;
    defer diagnostic.deinit(std.testing.allocator);
    const syntax = switch (diagnostic) {
        .syntax => |syntax| syntax,
        .lexical => return error.ExpectedSyntaxDiagnostic,
    };
    try std.testing.expectEqual(@as(usize, 1), syntax.token_index);
    try std.testing.expectEqual(@as(usize, 7), syntax.source_start);
    try std.testing.expectEqual(@as(usize, 11), syntax.source_end);
    try std.testing.expectEqualStrings("FROM", syntax.actual);
    try std.testing.expect(syntax.expected.len > 0);

    const trailing_space_sql = "SELECT ";
    const eof_diagnostic = (try diagnosticSqlAlloc(std.testing.allocator, trailing_space_sql)).?;
    defer eof_diagnostic.deinit(std.testing.allocator);
    const eof_syntax = switch (eof_diagnostic) {
        .syntax => |eof_syntax| eof_syntax,
        .lexical => return error.ExpectedSyntaxDiagnostic,
    };
    try std.testing.expectEqual(@as(usize, 1), eof_syntax.token_index);
    try std.testing.expectEqual(trailing_space_sql.len, eof_syntax.source_start);
    try std.testing.expectEqual(trailing_space_sql.len, eof_syntax.source_end);
    try std.testing.expectEqualStrings("$end", eof_syntax.actual);

    const lexical_diagnostic = (try diagnosticSqlAlloc(std.testing.allocator, "SELECT 'unterminated")).?;
    defer lexical_diagnostic.deinit(std.testing.allocator);
    const lexical = switch (lexical_diagnostic) {
        .lexical => |lexical| lexical,
        .syntax => return error.ExpectedLexicalDiagnostic,
    };
    try std.testing.expectEqual(lexer.LexErrorKind.unterminated_string_literal, lexical.kind);
    try std.testing.expectEqual(@as(usize, 7), lexical.source_start);
    try std.testing.expectEqual(@as(usize, 20), lexical.source_end);
    try std.testing.expectEqualStrings("unterminated string literal", lexical.message());
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

    const Runner = struct {
        fn run(allocator: std.mem.Allocator, source: []const u8) !void {
            try parseSqlAlloc(allocator, source);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{sql.items});
}

test "generated parser bridge is allocation-failure safe" {
    const Runner = struct {
        fn run(allocator: std.mem.Allocator) !void {
            try parseSqlAlloc(allocator, "SELECT schema.docs, analyze_verbose FROM schema WHERE role = 'reader'");
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
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
        error.UnsupportedSqlShape => {
            const diagnostic = (try diagnosticSqlAlloc(std.testing.allocator, sql)) orelse return error.ExpectedDiagnostic;
            defer diagnostic.deinit(std.testing.allocator);
            const lexical = switch (diagnostic) {
                .lexical => |lexical| lexical,
                .syntax => return error.ExpectedLexicalDiagnostic,
            };
            try std.testing.expect(lexical.source_end >= lexical.source_start);
            try std.testing.expect(lexical.source_end <= sql.len);
        },
        error.UnexpectedToken => {
            const diagnostic = (try diagnosticSqlAlloc(std.testing.allocator, sql)) orelse return error.ExpectedDiagnostic;
            defer diagnostic.deinit(std.testing.allocator);
            const span = diagnostic.sourceSpan();
            try std.testing.expect(span.end >= span.start);
            try std.testing.expect(span.end <= sql.len);
            switch (diagnostic) {
                .lexical => return error.ExpectedSyntaxDiagnostic,
                .syntax => |syntax| try std.testing.expect(syntax.expected.len > 0),
            }
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
