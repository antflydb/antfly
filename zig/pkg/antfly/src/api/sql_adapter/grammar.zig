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

const ast = @import("ast.zig");
const classifier = @import("classifier.zig");
const db_mod = @import("../../storage/db/mod.zig");
const lexer = @import("lexer.zig");
const lower_expr = @import("lower_expr.zig");
const parser = @import("parser.zig");
const token_mod = @import("token.zig");
const sql_value = @import("value.zig");

pub const Token = token_mod.Token;

pub const SqlExplainPrefix = ast.SqlExplainPrefix;

pub const RowSecurityAlterSyntax = struct {
    table_identifier: []const u8,
    enabled: bool,
};

pub const AdapterNoopTransactionBoundaryTail = struct {
    work: bool = false,
    transaction: bool = false,
};

pub const SavepointNameSyntax = struct {
    savepoint_name: []const u8,
};

pub const PreparedStatementSubjectSyntax = classifier.SqlPreparedStatementSubjectKind;

pub const PrepareStatementSyntax = struct {
    statement_name: []const u8,
    parameter_count: usize = 0,
    statement_kind: PreparedStatementSubjectSyntax,
};

pub const ExecutePreparedStatementSyntax = struct {
    statement_name: []const u8,
    argument_count: usize = 0,
};

pub const TableLockModeSyntax = enum {
    access_share,
    row_share,
    row_exclusive,
    share_update_exclusive,
    share,
    share_row_exclusive,
    exclusive,
    access_exclusive,
};

pub const TableLockSyntax = struct {
    table_names: []const []const u8 = &.{},
    mode: TableLockModeSyntax,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.table_names);
        self.* = undefined;
    }
};

pub const ConstraintCheckModeSyntax = enum {
    immediate,
    deferred,
};

pub const ConstraintModeSyntax = struct {
    all: bool = false,
    constraint_names: []const []const u8 = &.{},
    mode: ConstraintCheckModeSyntax,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.constraint_names);
        self.* = undefined;
    }
};

pub const TransactionModeStarterSyntax = enum {
    set_transaction,
    start_transaction,
    begin,
};

pub const TransactionIsolationLevelSyntax = enum {
    serializable,
    repeatable_read,
    read_committed,
    read_uncommitted,
};

pub const TransactionAccessModeSyntax = enum {
    read_only,
    read_write,
};

pub const TransactionModeSyntax = struct {
    starter: TransactionModeStarterSyntax,
    isolation_level: ?TransactionIsolationLevelSyntax = null,
    access_mode: ?TransactionAccessModeSyntax = null,
    deferrable: ?bool = null,
};

pub const VacuumMaintenanceSyntax = struct {
    table_name: []const u8,
    full: bool = false,
    freeze: bool = false,
    verbose: bool = false,
    analyze: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const AnalyzeMaintenanceSyntax = struct {
    table_name: []const u8,
    verbose: bool = false,
    column_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        self.* = undefined;
    }
};

pub const ReindexMaintenanceTargetSyntax = enum {
    index,
    table,
    schema,
    database,
    system,
};

pub const ReindexMaintenanceSyntax = struct {
    target: ReindexMaintenanceTargetSyntax,
    name: []const u8,
    concurrently: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        self.* = undefined;
    }
};

pub const ClusterMaintenanceSyntax = struct {
    table_name: []const u8,
    index_name: ?[]const u8 = null,
    verbose: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.table_name));
        if (self.index_name) |index_name| alloc.free(@constCast(index_name));
        self.* = undefined;
    }
};

pub const CreateDatabaseSyntax = struct {
    database_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.database_name));
        self.* = undefined;
    }
};

pub const AlterDatabaseSyntax = struct {
    database_name: []const u8,
    setting_name: []const u8,
    value_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.database_name));
        alloc.free(@constCast(self.setting_name));
        alloc.free(@constCast(self.value_json));
        self.* = undefined;
    }
};

pub const DropDatabaseSyntax = struct {
    database_name: []const u8,
    if_exists: bool = false,
    force: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.database_name));
        self.* = undefined;
    }
};

pub const CreateTablespaceSyntax = struct {
    tablespace_name: []const u8,
    location_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.tablespace_name));
        alloc.free(@constCast(self.location_json));
        self.* = undefined;
    }
};

pub const RenameTablespaceSyntax = struct {
    tablespace_name: []const u8,
    new_tablespace_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.tablespace_name));
        alloc.free(@constCast(self.new_tablespace_name));
        self.* = undefined;
    }
};

pub const DropTablespaceSyntax = struct {
    tablespace_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.tablespace_name));
        self.* = undefined;
    }
};

pub const ListenNotificationSyntax = struct {
    channel_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.channel_name));
        self.* = undefined;
    }
};

pub const NotifyNotificationSyntax = struct {
    channel_name: []const u8,
    payload_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.channel_name));
        if (self.payload_json) |payload| alloc.free(@constCast(payload));
        self.* = undefined;
    }
};

pub const UnlistenNotificationSyntax = struct {
    channel_name: ?[]const u8 = null,
    all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.channel_name) |channel_name| alloc.free(@constCast(channel_name));
        self.* = undefined;
    }
};

pub const RowClaimSyntax = struct {
    clause: ast.SqlRowClaimClause,
    targets: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.targets) |target| alloc.free(@constCast(target));
        if (self.targets.len > 0) alloc.free(self.targets);
        self.* = undefined;
    }
};

pub const NamedOrAllSyntax = struct {
    name: ?[]const u8 = null,
    all: bool = false,
};

pub const CursorScrollSyntax = enum {
    default,
    scroll,
    no_scroll,
};

pub const DeclareCursorPortalSyntax = struct {
    portal_name: []const u8,
    scroll: CursorScrollSyntax = .default,
    binary: bool = false,
    hold: bool = false,
    statement_kind: ?PreparedStatementSubjectSyntax = null,
};

pub const CursorFetchDirectionSyntax = enum {
    next,
    prior,
    first,
    last,
    absolute,
    relative,
    forward,
    backward,
    all,
};

pub const FetchCursorPortalSyntax = struct {
    portal_name: []const u8,
    direction: CursorFetchDirectionSyntax = .next,
    count: ?i64 = null,
};

pub const RelationPopulationMode = enum {
    create_table_as,
    select_into,
};

pub const RelationLifetimeKind = enum {
    temporary,
    unlogged,
};

pub const RelationPopulationSyntax = struct {
    mode: RelationPopulationMode,
    target_identifier: []const u8,
    target_lifetime: ?RelationLifetimeKind = null,
    if_not_exists: bool = false,
    source_sql: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.source_sql);
        self.* = undefined;
    }
};

pub fn parseExplainPrefix(sql: []const u8) !SqlExplainPrefix {
    var index = skipSqlWhitespace(sql, 0);
    if (!consumeSqlKeyword(sql, &index, "explain")) return error.UnsupportedSqlShape;
    index = skipSqlWhitespace(sql, index);
    if (index >= sql.len) return error.UnsupportedSqlShape;

    var prefix = SqlExplainPrefix{ .inner_sql = "" };
    if (sql[index] == '(') {
        try parseExplainOptions(sql, &index, &prefix);
        index = skipSqlWhitespace(sql, index);
        if (index >= sql.len) return error.UnsupportedSqlShape;
    }

    if (consumeSqlKeyword(sql, &index, "analyze")) {
        prefix.analyze = true;
        index = skipSqlWhitespace(sql, index);
        if (index >= sql.len) return error.UnsupportedSqlShape;
    }

    const inner = std.mem.trim(u8, sql[index..], " \t\r\n;");
    if (inner.len == 0) return error.UnsupportedSqlShape;
    prefix.inner_sql = inner;
    return prefix;
}

fn parseExplainOptions(sql: []const u8, index: *usize, prefix: *SqlExplainPrefix) !void {
    if (index.* >= sql.len or sql[index.*] != '(') return error.UnsupportedSqlShape;
    index.* += 1;
    while (true) {
        index.* = skipSqlWhitespace(sql, index.*);
        if (index.* >= sql.len) return error.UnsupportedSqlShape;
        if (consumeSqlKeyword(sql, index, "format")) {
            index.* = skipSqlWhitespace(sql, index.*);
            if (consumeSqlKeyword(sql, index, "json")) {
                prefix.format = .json;
            } else if (consumeSqlKeyword(sql, index, "text")) {
                prefix.format = .text;
            } else {
                return error.UnsupportedSqlShape;
            }
        } else if (consumeSqlKeyword(sql, index, "verbose")) {
            prefix.verbose = try parseOptionalExplainBool(sql, index, true);
        } else if (consumeSqlKeyword(sql, index, "costs")) {
            prefix.costs = try parseOptionalExplainBool(sql, index, true);
        } else if (consumeSqlKeyword(sql, index, "analyze")) {
            prefix.analyze = try parseOptionalExplainBool(sql, index, true);
        } else {
            return error.UnsupportedSqlShape;
        }

        index.* = skipSqlWhitespace(sql, index.*);
        if (index.* >= sql.len) return error.UnsupportedSqlShape;
        if (sql[index.*] == ',') {
            index.* += 1;
            continue;
        }
        if (sql[index.*] == ')') {
            index.* += 1;
            return;
        }
        return error.UnsupportedSqlShape;
    }
}

fn parseOptionalExplainBool(sql: []const u8, index: *usize, default_value: bool) !bool {
    const before = index.*;
    index.* = skipSqlWhitespace(sql, index.*);
    if (consumeSqlKeyword(sql, index, "true") or
        consumeSqlKeyword(sql, index, "on") or
        consumeSqlKeyword(sql, index, "yes"))
    {
        return true;
    }
    if (consumeSqlKeyword(sql, index, "false") or
        consumeSqlKeyword(sql, index, "off") or
        consumeSqlKeyword(sql, index, "no"))
    {
        return false;
    }
    index.* = before;
    return default_value;
}

fn skipSqlWhitespace(sql: []const u8, start: usize) usize {
    var index = start;
    while (index < sql.len and std.ascii.isWhitespace(sql[index])) : (index += 1) {}
    return index;
}

fn consumeSqlKeyword(sql: []const u8, index: *usize, keyword: []const u8) bool {
    const start = index.*;
    const end = start + keyword.len;
    if (end > sql.len) return false;
    if (!std.ascii.eqlIgnoreCase(sql[start..end], keyword)) return false;
    if (end < sql.len and isSqlIdentifierByte(sql[end])) return false;
    if (start > 0 and isSqlIdentifierByte(sql[start - 1])) return false;
    index.* = end;
    return true;
}

fn isSqlIdentifierByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_';
}

pub fn parseAlterRowSecurity(tokens: []const Token, pos: *usize) !?RowSecurityAlterSyntax {
    const start = pos.*;
    const cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("table")) return null;
    const table_token = cursor.matchToken(.identifier) orelse {
        pos.* = start;
        return error.UnsupportedSqlShape;
    };
    const enabled = if (cursor.matchKeyword("enable"))
        true
    else if (cursor.matchKeyword("disable"))
        false
    else {
        pos.* = start;
        return null;
    };
    try cursor.expectKeyword("row");
    try cursor.expectKeyword("level");
    try cursor.expectKeyword("security");
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
    return .{ .table_identifier = table_token.text, .enabled = enabled };
}

pub fn parseAdapterNoopSetStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    if (!cursor.matchKeyword("local")) _ = cursor.matchKeyword("session");

    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(setting.text, "search_path")) {
        try parseAdapterNoopPublicSearchPathTail(cursor);
        return;
    }
    if (!adapterNoopSetSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;

    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    try parseAdapterNoopSetValueTail(cursor, setting.text);
}

pub fn parseAdapterNoopResetStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("all")) {
        try parseAdapterNoopStatementEnd(cursor);
        return;
    }

    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopResetSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

pub fn parseAdapterNoopShowStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    const setting = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopShowSessionSettingAllowed(setting.text)) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

pub fn parseAdapterNoopDiscardStatementTail(tokens: []const Token, pos: *usize) !void {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("all");
    try parseAdapterNoopStatementEnd(cursor);
}

pub fn matchAdapterNoopTransactionBoundaryTail(
    tokens: []const Token,
    pos: *usize,
    options: AdapterNoopTransactionBoundaryTail,
) !bool {
    var cursor = parser.Cursor.init(tokens, pos);
    const checkpoint = cursor.checkpoint();
    if (try matchAdapterNoopStatementEnd(cursor)) return true;
    if ((options.work and cursor.matchKeyword("work")) or
        (options.transaction and cursor.matchKeyword("transaction")))
    {
        if (try matchAdapterNoopStatementEnd(cursor)) return true;
    }
    cursor.restore(checkpoint);
    return false;
}

pub fn parseSavepointTransactionTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    return try parseSavepointNameTail(tokens, pos);
}

pub fn parseReleaseSavepointTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("savepoint");
    return try parseSavepointNameTailFromCursor(cursor);
}

pub fn parseRollbackToSavepointTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("to");
    _ = cursor.matchKeyword("savepoint");
    return try parseSavepointNameTailFromCursor(cursor);
}

pub fn parseForRowClaimClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RowClaimSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const mode: db_mod.types.RowClaimMode = if (cursor.matchKeyword("no")) blk: {
        try cursor.expectKeyword("key");
        try cursor.expectKeyword("update");
        break :blk .for_no_key_update;
    } else if (cursor.matchKeyword("key")) blk: {
        try cursor.expectKeyword("share");
        break :blk .for_key_share;
    } else if (cursor.matchKeyword("share")) blk: {
        break :blk .for_share;
    } else blk: {
        try cursor.expectKeyword("update");
        break :blk .for_update;
    };

    var targets = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (targets.items) |target| alloc.free(@constCast(target));
        targets.deinit(alloc);
    }
    if (cursor.matchKeyword("of")) {
        while (true) {
            _ = cursor.matchKeyword("only");
            const target = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            try targets.append(alloc, try alloc.dupe(u8, target.text));
            if (cursor.matchToken(.comma) == null) break;
        }
    }

    const wait_policy: db_mod.types.RowClaimWaitPolicy = if (cursor.matchKeyword("skip")) blk: {
        try cursor.expectKeyword("locked");
        break :blk .skip_locked;
    } else if (cursor.matchKeyword("nowait"))
        .nowait
    else
        .wait;

    return .{
        .clause = .{ .mode = mode, .wait_policy = wait_policy },
        .targets = try targets.toOwnedSlice(alloc),
    };
}

pub fn parseDeallocatePreparedStatementTail(tokens: []const Token, pos: *usize) !NamedOrAllSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("prepare");
    return try parseNamedOrAllTail(cursor);
}

pub fn parsePrepareStatementTail(tokens: []const Token, pos: *usize) !PrepareStatementSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const statement_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const parameter_count = if (cursor.peekKind(.lparen)) try countParenthesizedTypeList(cursor) else 0;
    try cursor.expectKeyword("as");
    const statement_kind = classifier.classifyPreparedStatementSubjectKind(tokens, cursor.checkpoint()) orelse return error.UnsupportedSqlShape;
    try consumePreparedStatementSubjectTail(cursor);
    return .{
        .statement_name = statement_token.text,
        .parameter_count = parameter_count,
        .statement_kind = statement_kind,
    };
}

pub fn parseExecutePreparedStatementTail(tokens: []const Token, pos: *usize) !ExecutePreparedStatementSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const statement_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    const argument_count = try countParenthesizedUntypedValues(cursor);
    try parseAdapterNoopStatementEnd(cursor);
    return .{
        .statement_name = statement_token.text,
        .argument_count = argument_count,
    };
}

pub fn parseDeclareCursorPortalPrefix(tokens: []const Token, pos: *usize) !DeclareCursorPortalSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const portal_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    var binary = false;
    var scroll: CursorScrollSyntax = .default;
    var hold = false;
    while (true) {
        if (cursor.matchKeyword("binary")) {
            binary = true;
        } else if (cursor.matchKeyword("scroll")) {
            scroll = .scroll;
        } else if (cursor.matchKeyword("no")) {
            try cursor.expectKeyword("scroll");
            scroll = .no_scroll;
        } else {
            break;
        }
    }
    try cursor.expectKeyword("cursor");
    if (cursor.matchKeyword("with")) {
        try cursor.expectKeyword("hold");
        hold = true;
    } else if (cursor.matchKeyword("without")) {
        try cursor.expectKeyword("hold");
        hold = false;
    }
    try cursor.expectKeyword("for");
    return .{
        .portal_name = portal_token.text,
        .scroll = scroll,
        .binary = binary,
        .hold = hold,
    };
}

pub fn parseDeclareCursorPortalTail(tokens: []const Token, pos: *usize) !DeclareCursorPortalSyntax {
    var syntax = try parseDeclareCursorPortalPrefix(tokens, pos);
    const cursor = parser.Cursor.init(tokens, pos);
    syntax.statement_kind = classifier.classifyPreparedStatementSubjectKind(tokens, cursor.checkpoint()) orelse return error.UnsupportedSqlShape;
    try consumePreparedStatementSubjectTail(cursor);
    return syntax;
}

pub fn parseFetchCursorPortalTail(tokens: []const Token, pos: *usize) !FetchCursorPortalSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    var direction: CursorFetchDirectionSyntax = .next;
    var count: ?i64 = null;
    if (cursor.matchKeyword("next")) {
        direction = .next;
    } else if (cursor.matchKeyword("prior")) {
        direction = .prior;
    } else if (cursor.matchKeyword("first")) {
        direction = .first;
    } else if (cursor.matchKeyword("last")) {
        direction = .last;
    } else if (cursor.matchKeyword("all")) {
        direction = .all;
    } else if (cursor.matchKeyword("forward")) {
        direction = .forward;
        count = try parseOptionalCursorFetchCount(cursor);
    } else if (cursor.matchKeyword("backward")) {
        direction = .backward;
        count = try parseOptionalCursorFetchCount(cursor);
    } else if (cursor.matchKeyword("absolute")) {
        direction = .absolute;
        count = try parseCursorFetchCount(cursor);
    } else if (cursor.matchKeyword("relative")) {
        direction = .relative;
        count = try parseCursorFetchCount(cursor);
    } else if (peekCursorFetchCount(cursor)) {
        direction = .forward;
        count = try parseCursorFetchCount(cursor);
    }
    _ = cursor.matchKeyword("from") or cursor.matchKeyword("in");
    const portal_token = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    return .{
        .portal_name = portal_token.text,
        .direction = direction,
        .count = count,
    };
}

pub fn parseCloseCursorPortalTail(tokens: []const Token, pos: *usize) !NamedOrAllSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    return try parseNamedOrAllTail(cursor);
}

pub fn parseTableLockTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !TableLockSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    _ = cursor.matchKeyword("table");
    var table_names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &table_names);
    while (true) {
        const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
        var table_transferred = false;
        errdefer if (!table_transferred) alloc.free(table_name);
        try table_names.append(alloc, table_name);
        table_transferred = true;
        if (cursor.matchToken(.comma) == null) break;
    }
    if (table_names.items.len == 0) return error.UnsupportedSqlShape;
    try cursor.expectKeyword("in");
    const mode = try parseTableLockMode(cursor);
    try cursor.expectKeyword("mode");
    try parseAdapterNoopStatementEnd(cursor);
    return .{
        .table_names = try table_names.toOwnedSlice(alloc),
        .mode = mode,
    };
}

pub fn parseConstraintModeTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ConstraintModeSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("constraints");
    var all = false;
    var constraint_names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeStringList(alloc, &constraint_names);
    if (cursor.matchKeyword("all")) {
        all = true;
    } else {
        while (true) {
            const constraint_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
            var constraint_transferred = false;
            errdefer if (!constraint_transferred) alloc.free(constraint_name);
            try constraint_names.append(alloc, constraint_name);
            constraint_transferred = true;
            if (cursor.matchToken(.comma) == null) break;
        }
    }
    const mode: ConstraintCheckModeSyntax = if (cursor.matchKeyword("immediate"))
        .immediate
    else if (cursor.matchKeyword("deferred"))
        .deferred
    else
        return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    return .{
        .all = all,
        .constraint_names = try constraint_names.toOwnedSlice(alloc),
        .mode = mode,
    };
}

pub fn parseTransactionModeTail(
    tokens: []const Token,
    pos: *usize,
    starter: TransactionModeStarterSyntax,
) !TransactionModeSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    switch (starter) {
        .set_transaction, .start_transaction => try cursor.expectKeyword("transaction"),
        .begin => _ = cursor.matchKeyword("transaction"),
    }
    var syntax = TransactionModeSyntax{ .starter = starter };
    var saw_mode = false;
    while (!cursor.atEnd()) {
        if (cursor.matchToken(.semicolon) != null) {
            if (!cursor.atEnd()) return error.UnsupportedSqlShape;
            break;
        }
        _ = cursor.matchToken(.comma);
        if (cursor.matchKeyword("isolation")) {
            try cursor.expectKeyword("level");
            if (syntax.isolation_level != null) return error.UnsupportedSqlShape;
            syntax.isolation_level = try parseTransactionIsolationLevel(cursor);
            saw_mode = true;
        } else if (cursor.matchKeyword("read")) {
            if (syntax.access_mode != null) return error.UnsupportedSqlShape;
            if (cursor.matchKeyword("only")) {
                syntax.access_mode = .read_only;
            } else if (cursor.matchKeyword("write")) {
                syntax.access_mode = .read_write;
            } else {
                return error.UnsupportedSqlShape;
            }
            saw_mode = true;
        } else if (cursor.matchKeyword("not")) {
            try cursor.expectKeyword("deferrable");
            if (syntax.deferrable != null) return error.UnsupportedSqlShape;
            syntax.deferrable = false;
            saw_mode = true;
        } else if (cursor.matchKeyword("deferrable")) {
            if (syntax.deferrable != null) return error.UnsupportedSqlShape;
            syntax.deferrable = true;
            saw_mode = true;
        } else {
            return error.UnsupportedSqlShape;
        }
    }
    if (!saw_mode) return error.UnsupportedSqlShape;
    return syntax;
}

pub fn parseVacuumMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !VacuumMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    var full = false;
    var freeze = false;
    var verbose = false;
    var analyze = false;
    if (cursor.matchToken(.lparen) != null) {
        while (true) {
            try parseVacuumMaintenanceOption(cursor, &full, &freeze, &verbose, &analyze);
            if (cursor.matchToken(.comma) == null) break;
        }
        try cursor.expectToken(.rparen);
    } else {
        while (parseOptionalVacuumMaintenanceOption(cursor, &full, &freeze, &verbose, &analyze)) {}
    }
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    if (cursor.matchToken(.lparen) != null) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    table_transferred = true;
    return .{ .table_name = table_name, .full = full, .freeze = freeze, .verbose = verbose, .analyze = analyze };
}

pub fn parseAnalyzeMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AnalyzeMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const verbose = cursor.matchKeyword("verbose");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    var column_count: usize = 0;
    if (cursor.matchToken(.lparen) != null) {
        while (true) {
            _ = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
            column_count += 1;
            if (cursor.matchToken(.comma) == null) break;
        }
        try cursor.expectToken(.rparen);
    }
    try parseAdapterNoopStatementEnd(cursor);
    table_transferred = true;
    return .{ .table_name = table_name, .verbose = verbose, .column_count = column_count };
}

pub fn parseReindexMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ReindexMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const target: ReindexMaintenanceTargetSyntax = if (cursor.matchKeyword("index"))
        .index
    else if (cursor.matchKeyword("table"))
        .table
    else if (cursor.matchKeyword("schema"))
        .schema
    else if (cursor.matchKeyword("database"))
        .database
    else if (cursor.matchKeyword("system"))
        .system
    else
        return error.UnsupportedSqlShape;
    const concurrently = cursor.matchKeyword("concurrently");
    const name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);
    try parseAdapterNoopStatementEnd(cursor);
    name_transferred = true;
    return .{ .target = target, .name = name, .concurrently = concurrently };
}

pub fn parseClusterMaintenanceTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ClusterMaintenanceSyntax {
    var cursor = parser.Cursor.init(tokens, pos);
    const verbose = cursor.matchKeyword("verbose");
    const table_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    var table_transferred = false;
    errdefer if (!table_transferred) alloc.free(table_name);
    var index_name: ?[]const u8 = null;
    var index_transferred = false;
    errdefer if (!index_transferred) if (index_name) |name| alloc.free(@constCast(name));
    if (cursor.matchKeyword("using")) {
        index_name = try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
    }
    try parseAdapterNoopStatementEnd(cursor);
    table_transferred = true;
    index_transferred = true;
    return .{ .table_name = table_name, .index_name = index_name, .verbose = verbose };
}

pub fn parseCreateDatabaseCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateDatabaseSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("database");
    const database_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var database_transferred = false;
    errdefer if (!database_transferred) alloc.free(database_name);
    if (cursor.matchKeyword("with") or
        cursor.peekKeyword("owner") or
        cursor.peekKeyword("template") or
        cursor.peekKeyword("encoding") or
        cursor.peekKeyword("locale") or
        cursor.peekKeyword("tablespace") or
        cursor.peekKeyword("connection"))
        return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    database_transferred = true;
    return .{ .database_name = database_name };
}

pub fn parseAlterDatabaseCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !AlterDatabaseSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("database");
    const database_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var database_transferred = false;
    errdefer if (!database_transferred) alloc.free(database_name);
    if (cursor.matchKeyword("rename") or
        cursor.matchKeyword("owner") or
        cursor.matchKeyword("refresh") or
        cursor.matchKeyword("reset"))
        return error.UnsupportedSqlShape;
    try cursor.expectKeyword("set");
    const setting_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var setting_transferred = false;
    errdefer if (!setting_transferred) alloc.free(setting_name);
    if (cursor.matchKeyword("to") == false and cursor.matchToken(.eq) == null) return error.UnsupportedSqlShape;
    const value_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(@constCast(value_json));
    try parseAdapterNoopStatementEnd(cursor);
    database_transferred = true;
    setting_transferred = true;
    value_transferred = true;
    return .{
        .database_name = database_name,
        .setting_name = setting_name,
        .value_json = value_json,
    };
}

pub fn parseDropDatabaseCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropDatabaseSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("database");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const database_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var database_transferred = false;
    errdefer if (!database_transferred) alloc.free(database_name);
    var force = false;
    if (cursor.matchKeyword("with")) {
        try cursor.expectToken(.lparen);
        try cursor.expectKeyword("force");
        try cursor.expectToken(.rparen);
        force = true;
    }
    try parseAdapterNoopStatementEnd(cursor);
    database_transferred = true;
    return .{ .database_name = database_name, .if_exists = if_exists, .force = force };
}

pub fn parseCreateTablespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !CreateTablespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("tablespace");
    const tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var tablespace_transferred = false;
    errdefer if (!tablespace_transferred) alloc.free(tablespace_name);
    if (cursor.peekKeyword("owner")) return error.UnsupportedSqlShape;
    try cursor.expectKeyword("location");
    const location_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    var location_transferred = false;
    errdefer if (!location_transferred) alloc.free(@constCast(location_json));
    if (cursor.peekKeyword("with")) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    tablespace_transferred = true;
    location_transferred = true;
    return .{ .tablespace_name = tablespace_name, .location_json = location_json };
}

pub fn parseRenameTablespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !RenameTablespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("tablespace");
    const tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var tablespace_transferred = false;
    errdefer if (!tablespace_transferred) alloc.free(tablespace_name);
    try cursor.expectKeyword("rename");
    try cursor.expectKeyword("to");
    const new_tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var new_tablespace_transferred = false;
    errdefer if (!new_tablespace_transferred) alloc.free(new_tablespace_name);
    try parseAdapterNoopStatementEnd(cursor);
    tablespace_transferred = true;
    new_tablespace_transferred = true;
    return .{ .tablespace_name = tablespace_name, .new_tablespace_name = new_tablespace_name };
}

pub fn parseDropTablespaceCatalogTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !DropTablespaceSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    try cursor.expectKeyword("tablespace");
    var if_exists = false;
    if (cursor.matchKeyword("if")) {
        try cursor.expectKeyword("exists");
        if_exists = true;
    }
    const tablespace_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var tablespace_transferred = false;
    errdefer if (!tablespace_transferred) alloc.free(tablespace_name);
    try parseAdapterNoopStatementEnd(cursor);
    tablespace_transferred = true;
    return .{ .tablespace_name = tablespace_name, .if_exists = if_exists };
}

pub fn parseListenNotificationTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !ListenNotificationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const channel_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var channel_transferred = false;
    errdefer if (!channel_transferred) alloc.free(channel_name);
    try parseAdapterNoopStatementEnd(cursor);
    channel_transferred = true;
    return .{ .channel_name = channel_name };
}

pub fn parseNotifyNotificationTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !NotifyNotificationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    const channel_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var channel_transferred = false;
    errdefer if (!channel_transferred) alloc.free(channel_name);
    var payload_json: ?[]const u8 = null;
    errdefer if (payload_json) |payload| alloc.free(@constCast(payload));
    if (cursor.matchToken(.comma) != null) {
        payload_json = try sql_value.parseSqlUntypedValueJsonAlloc(alloc, tokens, pos);
    }
    try parseAdapterNoopStatementEnd(cursor);
    channel_transferred = true;
    const syntax = NotifyNotificationSyntax{
        .channel_name = channel_name,
        .payload_json = payload_json,
    };
    payload_json = null;
    return syntax;
}

pub fn parseUnlistenNotificationTailAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !UnlistenNotificationSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchToken(.star) != null) {
        try parseAdapterNoopStatementEnd(cursor);
        return .{ .all = true };
    }
    const channel_name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    var channel_transferred = false;
    errdefer if (!channel_transferred) alloc.free(channel_name);
    try parseAdapterNoopStatementEnd(cursor);
    channel_transferred = true;
    return .{ .channel_name = channel_name };
}

pub fn normalizeSqlObjectIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return try alloc.dupe(u8, identifier);
    if (dot == 0) return error.UnsupportedSqlShape;
    const object_name = identifier[dot + 1 ..];
    if (object_name.len == 0 or std.mem.indexOfScalar(u8, object_name, '.') != null) return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(identifier[0..dot], "public")) return try alloc.dupe(u8, identifier);
    return try alloc.dupe(u8, object_name);
}

pub fn parseIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

pub fn parseIdentifierListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }
    while (true) {
        try out.append(alloc, try parseIdentifierOwnedAlloc(alloc, tokens, pos));
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn parseSqlObjectIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try normalizeSqlObjectIdentifierAlloc(alloc, token.text);
}

pub fn parseSqlObjectIdentifierListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }
    while (true) {
        try out.append(alloc, try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos));
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn parseSqlTableReferenceIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    _ = parser.matchKeyword(tokens, pos, "only");
    return try parseSqlObjectIdentifierOwnedAlloc(alloc, tokens, pos);
}

fn parseAdapterNoopPublicSearchPathTail(cursor: parser.Cursor) !void {
    if (cursor.matchToken(.eq) == null and !cursor.matchKeyword("to")) return error.UnsupportedSqlShape;
    const path = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse return error.UnsupportedSqlShape;
    if (!std.ascii.eqlIgnoreCase(path.text, "public")) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

fn parseAdapterNoopSetValueTail(cursor: parser.Cursor, setting: []const u8) !void {
    const value = cursor.matchToken(.identifier) orelse cursor.matchToken(.string) orelse cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    if (!adapterNoopSetSessionSettingValueAllowed(setting, value.text)) return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
}

fn parseAdapterNoopStatementEnd(cursor: parser.Cursor) !void {
    if (!cursor.atEnd() and !cursor.peekKind(.semicolon)) return error.UnsupportedSqlShape;
    if (cursor.matchToken(.semicolon) != null and !cursor.atEnd()) return error.UnsupportedSqlShape;
    if (!cursor.atEnd()) return error.UnsupportedSqlShape;
}

fn matchAdapterNoopStatementEnd(cursor: parser.Cursor) !bool {
    if (cursor.matchToken(.semicolon) != null) {
        if (!cursor.atEnd()) return error.UnsupportedSqlShape;
        return true;
    }
    return cursor.atEnd();
}

fn parseSavepointNameTail(tokens: []const Token, pos: *usize) !SavepointNameSyntax {
    const cursor = parser.Cursor.init(tokens, pos);
    return try parseSavepointNameTailFromCursor(cursor);
}

fn parseSavepointNameTailFromCursor(cursor: parser.Cursor) !SavepointNameSyntax {
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    return .{ .savepoint_name = name.text };
}

fn parseNamedOrAllTail(cursor: parser.Cursor) !NamedOrAllSyntax {
    if (cursor.matchKeyword("all")) {
        try parseAdapterNoopStatementEnd(cursor);
        return .{ .all = true };
    }
    const name = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
    try parseAdapterNoopStatementEnd(cursor);
    return .{ .name = name.text };
}

fn parseTableLockMode(cursor: parser.Cursor) !TableLockModeSyntax {
    if (cursor.matchKeyword("access")) {
        if (cursor.matchKeyword("share")) return .access_share;
        if (cursor.matchKeyword("exclusive")) return .access_exclusive;
        return error.UnsupportedSqlShape;
    }
    if (cursor.matchKeyword("row")) {
        if (cursor.matchKeyword("share")) return .row_share;
        if (cursor.matchKeyword("exclusive")) return .row_exclusive;
        return error.UnsupportedSqlShape;
    }
    if (cursor.matchKeyword("share")) {
        if (cursor.matchKeyword("update")) {
            try cursor.expectKeyword("exclusive");
            return .share_update_exclusive;
        }
        if (cursor.matchKeyword("row")) {
            try cursor.expectKeyword("exclusive");
            return .share_row_exclusive;
        }
        return .share;
    }
    if (cursor.matchKeyword("exclusive")) return .exclusive;
    return error.UnsupportedSqlShape;
}

fn parseTransactionIsolationLevel(cursor: parser.Cursor) !TransactionIsolationLevelSyntax {
    if (cursor.matchKeyword("serializable")) return .serializable;
    if (cursor.matchKeyword("repeatable")) {
        try cursor.expectKeyword("read");
        return .repeatable_read;
    }
    if (cursor.matchKeyword("read")) {
        if (cursor.matchKeyword("committed")) return .read_committed;
        if (cursor.matchKeyword("uncommitted")) return .read_uncommitted;
    }
    return error.UnsupportedSqlShape;
}

fn parseOptionalVacuumMaintenanceOption(
    cursor: parser.Cursor,
    full: *bool,
    freeze: *bool,
    verbose: *bool,
    analyze: *bool,
) bool {
    parseVacuumMaintenanceOption(cursor, full, freeze, verbose, analyze) catch return false;
    return true;
}

fn parseVacuumMaintenanceOption(
    cursor: parser.Cursor,
    full: *bool,
    freeze: *bool,
    verbose: *bool,
    analyze: *bool,
) !void {
    if (cursor.matchKeyword("full")) {
        full.* = true;
    } else if (cursor.matchKeyword("freeze")) {
        freeze.* = true;
    } else if (cursor.matchKeyword("verbose")) {
        verbose.* = true;
    } else if (cursor.matchKeyword("analyze")) {
        analyze.* = true;
    } else {
        return error.UnsupportedSqlShape;
    }
}

fn parseOptionalCursorFetchCount(cursor: parser.Cursor) !?i64 {
    if (cursor.peekKeyword("from") or cursor.peekKeyword("in")) return null;
    if (cursor.matchKeyword("all")) return null;
    if (!peekCursorFetchCount(cursor)) return null;
    return try parseCursorFetchCount(cursor);
}

fn peekCursorFetchCount(cursor: parser.Cursor) bool {
    if (cursor.peekKind(.number)) return true;
    const checkpoint = cursor.checkpoint();
    defer cursor.restore(checkpoint);
    if (cursor.matchToken(.minus) == null) return false;
    return cursor.peekKind(.number);
}

fn parseCursorFetchCount(cursor: parser.Cursor) !i64 {
    const negative = cursor.matchToken(.minus) != null;
    const count_token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
    var count = std.fmt.parseInt(i64, count_token.text, 10) catch return error.UnsupportedSqlShape;
    if (negative) count = -count;
    return count;
}

fn countParenthesizedTypeList(cursor: parser.Cursor) !usize {
    try cursor.expectToken(.lparen);
    if (cursor.matchToken(.rparen) != null) return 0;
    var count: usize = 0;
    while (true) {
        _ = cursor.matchToken(.identifier) orelse return error.UnsupportedSqlShape;
        count += 1;
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    return count;
}

fn countParenthesizedUntypedValues(cursor: parser.Cursor) !usize {
    if (cursor.matchToken(.lparen) == null) return 0;
    if (cursor.matchToken(.rparen) != null) return 0;
    var count: usize = 0;
    while (true) {
        try parseUntypedValue(cursor);
        count += 1;
        if (cursor.matchToken(.comma) == null) break;
    }
    try cursor.expectToken(.rparen);
    return count;
}

fn parseUntypedValue(cursor: parser.Cursor) !void {
    if (cursor.matchKeyword("true")) return;
    if (cursor.matchKeyword("false")) return;
    if (cursor.matchKeyword("null")) return;
    if (cursor.matchToken(.string) != null) return;
    if (cursor.matchToken(.number) != null) return;
    if (cursor.matchToken(.minus) != null) {
        try cursor.expectToken(.number);
        return;
    }
    return error.UnsupportedSqlShape;
}

fn consumePreparedStatementSubjectTail(cursor: parser.Cursor) !void {
    while (!cursor.atEnd()) {
        if (cursor.matchToken(.semicolon) != null) {
            if (!cursor.atEnd()) return error.UnsupportedSqlShape;
            return;
        }
        try cursor.advance(1);
    }
}

fn freeStringList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged([]const u8)) void {
    for (list.items) |value| alloc.free(@constCast(value));
    list.deinit(alloc);
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(values);
}

pub fn parseRelationPopulationSqlAlloc(alloc: std.mem.Allocator, sql: []const u8) !RelationPopulationSyntax {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    if (tokens.items.len == 0 or tokens.items[0].kind != .identifier) return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(tokens.items[0].text, "select")) {
        return try parseSelectIntoPopulationSqlAlloc(alloc, sql, tokens.items);
    }
    if (std.ascii.eqlIgnoreCase(tokens.items[0].text, "create")) {
        return try parseCreateTableAsPopulationSqlAlloc(alloc, sql, tokens.items);
    }
    return error.UnsupportedSqlShape;
}

fn parseSelectIntoPopulationSqlAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    tokens: []const Token,
) !RelationPopulationSyntax {
    const into_relative = parser.findTopLevelKeyword(tokens[1..], "into") orelse return error.UnsupportedSqlShape;
    const into_index = 1 + into_relative;
    const from_relative = parser.findTopLevelKeyword(tokens[into_index + 1 ..], "from") orelse return error.UnsupportedSqlShape;
    const from_index = into_index + 1 + from_relative;
    if (from_index != into_index + 2) return error.UnsupportedSqlShape;
    if (tokens[into_index + 1].kind != .identifier) return error.UnsupportedSqlShape;

    const into_start = try tokenStartOffset(sql, tokens[into_index]);
    const from_start = try tokenStartOffset(sql, tokens[from_index]);
    const source_sql = try std.fmt.allocPrint(
        alloc,
        "{s} {s}",
        .{ std.mem.trim(u8, sql[0..into_start], " \t\r\n"), sql[from_start..] },
    );
    return .{
        .mode = .select_into,
        .target_identifier = tokens[into_index + 1].text,
        .target_lifetime = null,
        .if_not_exists = false,
        .source_sql = source_sql,
    };
}

fn parseCreateTableAsPopulationSqlAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    tokens: []const Token,
) !RelationPopulationSyntax {
    var index: usize = 1;
    const target_lifetime: ?RelationLifetimeKind = if (parser.matchKeyword(tokens, &index, "temporary") or parser.matchKeyword(tokens, &index, "temp"))
        .temporary
    else if (parser.matchKeyword(tokens, &index, "unlogged"))
        .unlogged
    else
        null;
    if (!parser.matchKeyword(tokens, &index, "table")) return error.UnsupportedSqlShape;
    var if_not_exists = false;
    if (parser.matchKeyword(tokens, &index, "if")) {
        try parser.expectKeyword(tokens, &index, "not");
        try parser.expectKeyword(tokens, &index, "exists");
        if_not_exists = true;
    }
    if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
    const target_identifier = tokens[index].text;
    index += 1;
    if (!parser.matchKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
    if (index >= tokens.len or tokens[index].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[index].text, "select")) return error.UnsupportedSqlShape;
    const select_start = try tokenStartOffset(sql, tokens[index]);
    const source_sql = try alloc.dupe(u8, sql[select_start..]);
    return .{
        .mode = .create_table_as,
        .target_identifier = target_identifier,
        .target_lifetime = target_lifetime,
        .if_not_exists = if_not_exists,
        .source_sql = source_sql,
    };
}

fn tokenStartOffset(sql: []const u8, token: Token) !usize {
    if (token.source_end > token.source_start and token.source_end <= sql.len) return token.source_start;
    const sql_start = @intFromPtr(sql.ptr);
    const sql_end = sql_start + sql.len;
    const token_start = @intFromPtr(token.text.ptr);
    if (token_start < sql_start or token_start > sql_end) return error.UnsupportedSqlShape;
    return token_start - sql_start;
}

fn adapterNoopSetSessionSettingAllowed(setting: []const u8) bool {
    return std.ascii.eqlIgnoreCase(setting, "client_encoding") or
        std.ascii.eqlIgnoreCase(setting, "standard_conforming_strings") or
        std.ascii.eqlIgnoreCase(setting, "check_function_bodies") or
        std.ascii.eqlIgnoreCase(setting, "xmloption") or
        std.ascii.eqlIgnoreCase(setting, "client_min_messages");
}

fn adapterNoopResetSessionSettingAllowed(setting: []const u8) bool {
    return adapterNoopSetSessionSettingAllowed(setting);
}

fn adapterNoopShowSessionSettingAllowed(setting: []const u8) bool {
    return adapterNoopSetSessionSettingAllowed(setting) or std.ascii.eqlIgnoreCase(setting, "search_path");
}

fn adapterNoopSetSessionSettingValueAllowed(setting: []const u8, value: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(setting, "client_encoding")) {
        return std.ascii.eqlIgnoreCase(value, "UTF8") or std.ascii.eqlIgnoreCase(value, "UTF-8");
    }
    if (std.ascii.eqlIgnoreCase(setting, "standard_conforming_strings")) {
        return std.ascii.eqlIgnoreCase(value, "on") or std.ascii.eqlIgnoreCase(value, "true");
    }
    if (std.ascii.eqlIgnoreCase(setting, "check_function_bodies")) {
        return std.ascii.eqlIgnoreCase(value, "off") or std.ascii.eqlIgnoreCase(value, "false");
    }
    if (std.ascii.eqlIgnoreCase(setting, "xmloption")) {
        return std.ascii.eqlIgnoreCase(value, "content");
    }
    if (std.ascii.eqlIgnoreCase(setting, "client_min_messages")) {
        return std.ascii.eqlIgnoreCase(value, "warning") or
            std.ascii.eqlIgnoreCase(value, "notice") or
            std.ascii.eqlIgnoreCase(value, "error");
    }
    return false;
}

pub const sqlKeywordIsAnyOrSome = lower_expr.sqlKeywordIsAnyOrSome;
pub const sqlKeywordStartsScalarPredicate = lower_expr.sqlKeywordStartsScalarPredicate;
pub const sqlJoinedSourceAliasTerminator = lower_expr.sqlJoinedSourceAliasTerminator;
pub const sqlAssignmentTailKeyword = lower_expr.sqlAssignmentTailKeyword;
pub const sqlKeywordIsLengthFunction = lower_expr.sqlKeywordIsLengthFunction;
pub const sqlKeywordIsOctetLengthFunction = lower_expr.sqlKeywordIsOctetLengthFunction;
pub const sqlKeywordIsBitLengthFunction = lower_expr.sqlKeywordIsBitLengthFunction;
pub const sqlKeywordIsJsonArrayLengthFunction = lower_expr.sqlKeywordIsJsonArrayLengthFunction;
pub const sqlKeywordIsCardinalityFunction = lower_expr.sqlKeywordIsCardinalityFunction;
pub const sqlKeywordIsArrayLengthFunction = lower_expr.sqlKeywordIsArrayLengthFunction;
pub const sqlKeywordIsArrayPositionFunction = lower_expr.sqlKeywordIsArrayPositionFunction;
pub const sqlKeywordIsArrayToStringFunction = lower_expr.sqlKeywordIsArrayToStringFunction;
pub const arrayLengthDefaultOutput = lower_expr.arrayLengthDefaultOutput;
pub const sqlKeywordIsJsonTypeofFunction = lower_expr.sqlKeywordIsJsonTypeofFunction;
pub const sqlKeywordIsJsonExtractPathFunction = lower_expr.sqlKeywordIsJsonExtractPathFunction;
pub const sqlKeywordIsJsonBuildObjectFunction = lower_expr.sqlKeywordIsJsonBuildObjectFunction;
pub const sqlJsonExtractPathFunctionAsText = lower_expr.sqlJsonExtractPathFunctionAsText;
pub const sqlKeywordIsAsciiFunction = lower_expr.sqlKeywordIsAsciiFunction;
pub const sqlKeywordIsChrFunction = lower_expr.sqlKeywordIsChrFunction;
pub const sqlKeywordIsSubstringFunction = lower_expr.sqlKeywordIsSubstringFunction;
pub const sqlKeywordIsOverlayFunction = lower_expr.sqlKeywordIsOverlayFunction;
pub const sqlKeywordIsTranslateFunction = lower_expr.sqlKeywordIsTranslateFunction;
pub const sqlKeywordIsSplitPartFunction = lower_expr.sqlKeywordIsSplitPartFunction;
pub const sqlKeywordIsStrposFunction = lower_expr.sqlKeywordIsStrposFunction;
pub const sqlKeywordIsLeftRightFunction = lower_expr.sqlKeywordIsLeftRightFunction;
pub const sqlKeywordIsPadFunction = lower_expr.sqlKeywordIsPadFunction;
pub const sqlKeywordIsRepeatFunction = lower_expr.sqlKeywordIsRepeatFunction;
pub const sqlKeywordIsReverseFunction = lower_expr.sqlKeywordIsReverseFunction;
pub const sqlKeywordIsInitcapFunction = lower_expr.sqlKeywordIsInitcapFunction;
pub const sqlKeywordIsMd5Function = lower_expr.sqlKeywordIsMd5Function;
pub const sqlKeywordIsStartsWithFunction = lower_expr.sqlKeywordIsStartsWithFunction;
pub const sqlKeywordIsEndsWithFunction = lower_expr.sqlKeywordIsEndsWithFunction;
pub const sqlKeywordIsDateTruncFunction = lower_expr.sqlKeywordIsDateTruncFunction;
pub const sqlKeywordIsDateBinFunction = lower_expr.sqlKeywordIsDateBinFunction;
pub const sqlKeywordIsDatePartFunction = lower_expr.sqlKeywordIsDatePartFunction;
pub const sqlKeywordIsTrimVariantFunction = lower_expr.sqlKeywordIsTrimVariantFunction;
pub const sqlKeywordIsUuidV4Function = lower_expr.sqlKeywordIsUuidV4Function;
pub const sqlKeywordIsRegexpMatchFunction = lower_expr.sqlKeywordIsRegexpMatchFunction;
pub const sqlKeywordIsRegexpCountFunction = lower_expr.sqlKeywordIsRegexpCountFunction;
pub const sqlKeywordIsRegexpSubstrFunction = lower_expr.sqlKeywordIsRegexpSubstrFunction;
pub const sqlKeywordIsRegexpInstrFunction = lower_expr.sqlKeywordIsRegexpInstrFunction;
pub const rowExpressionBoundaryKeyword = lower_expr.rowExpressionBoundaryKeyword;
pub const sqlWhereTailClauseKeyword = lower_expr.sqlWhereTailClauseKeyword;
pub const sqlWindowTailClauseKeyword = lower_expr.sqlWindowTailClauseKeyword;

test "sql adapter grammar parses alter row security" {
    const alloc = std.testing.allocator;
    var tokens = try lexer.tokenizeAlloc(alloc, "TABLE public.usage_records DISABLE ROW LEVEL SECURITY;");
    defer lexer.freeTokens(alloc, &tokens);

    var pos: usize = 0;
    const syntax = (try parseAlterRowSecurity(tokens.items, &pos)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!syntax.enabled);
    try std.testing.expectEqualStrings("public.usage_records", syntax.table_identifier);
    try std.testing.expectEqual(tokens.items.len, pos);
}

test "sql adapter grammar leaves non row security alter table to ddl parser" {
    const alloc = std.testing.allocator;
    var tokens = try lexer.tokenizeAlloc(alloc, "TABLE usage_records ADD COLUMN status text;");
    defer lexer.freeTokens(alloc, &tokens);

    var pos: usize = 0;
    try std.testing.expect((try parseAlterRowSecurity(tokens.items, &pos)) == null);
    try std.testing.expectEqual(@as(usize, 0), pos);
}

test "sql adapter grammar accepts allowlisted adapter session cleanup" {
    const alloc = std.testing.allocator;

    var set_tokens = try lexer.tokenizeAlloc(alloc, "LOCAL client_min_messages = warning;");
    defer lexer.freeTokens(alloc, &set_tokens);
    var set_pos: usize = 0;
    try parseAdapterNoopSetStatementTail(set_tokens.items, &set_pos);
    try std.testing.expectEqual(set_tokens.items.len, set_pos);

    var search_path_tokens = try lexer.tokenizeAlloc(alloc, "search_path TO public;");
    defer lexer.freeTokens(alloc, &search_path_tokens);
    var search_path_pos: usize = 0;
    try parseAdapterNoopSetStatementTail(search_path_tokens.items, &search_path_pos);
    try std.testing.expectEqual(search_path_tokens.items.len, search_path_pos);

    var reset_tokens = try lexer.tokenizeAlloc(alloc, "client_min_messages;");
    defer lexer.freeTokens(alloc, &reset_tokens);
    var reset_pos: usize = 0;
    try parseAdapterNoopResetStatementTail(reset_tokens.items, &reset_pos);
    try std.testing.expectEqual(reset_tokens.items.len, reset_pos);

    var show_tokens = try lexer.tokenizeAlloc(alloc, "search_path;");
    defer lexer.freeTokens(alloc, &show_tokens);
    var show_pos: usize = 0;
    try parseAdapterNoopShowStatementTail(show_tokens.items, &show_pos);
    try std.testing.expectEqual(show_tokens.items.len, show_pos);

    var discard_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &discard_tokens);
    var discard_pos: usize = 0;
    try parseAdapterNoopDiscardStatementTail(discard_tokens.items, &discard_pos);
    try std.testing.expectEqual(discard_tokens.items.len, discard_pos);
}

test "sql adapter grammar rejects semantic session changes as noops" {
    const alloc = std.testing.allocator;

    var tenant_path_tokens = try lexer.tokenizeAlloc(alloc, "search_path TO tenant_schema;");
    defer lexer.freeTokens(alloc, &tenant_path_tokens);
    var tenant_path_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(tenant_path_tokens.items, &tenant_path_pos));

    var latin1_tokens = try lexer.tokenizeAlloc(alloc, "client_encoding = 'LATIN1';");
    defer lexer.freeTokens(alloc, &latin1_tokens);
    var latin1_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(latin1_tokens.items, &latin1_pos));

    var timeout_tokens = try lexer.tokenizeAlloc(alloc, "statement_timeout = '1ms';");
    defer lexer.freeTokens(alloc, &timeout_tokens);
    var timeout_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopSetStatementTail(timeout_tokens.items, &timeout_pos));

    var show_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &show_all_tokens);
    var show_all_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopShowStatementTail(show_all_tokens.items, &show_all_pos));

    var discard_temp_tokens = try lexer.tokenizeAlloc(alloc, "TEMP;");
    defer lexer.freeTokens(alloc, &discard_temp_tokens);
    var discard_temp_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseAdapterNoopDiscardStatementTail(discard_temp_tokens.items, &discard_temp_pos));
}

test "sql adapter grammar matches transaction boundary noops" {
    const alloc = std.testing.allocator;

    var bare_tokens = try lexer.tokenizeAlloc(alloc, ";");
    defer lexer.freeTokens(alloc, &bare_tokens);
    var bare_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(bare_tokens.items, &bare_pos, .{}));
    try std.testing.expectEqual(bare_tokens.items.len, bare_pos);

    var work_tokens = try lexer.tokenizeAlloc(alloc, "WORK;");
    defer lexer.freeTokens(alloc, &work_tokens);
    var work_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(work_tokens.items, &work_pos, .{ .work = true }));
    try std.testing.expectEqual(work_tokens.items.len, work_pos);

    var transaction_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION;");
    defer lexer.freeTokens(alloc, &transaction_tokens);
    var transaction_pos: usize = 0;
    try std.testing.expect(try matchAdapterNoopTransactionBoundaryTail(transaction_tokens.items, &transaction_pos, .{ .transaction = true }));
    try std.testing.expectEqual(transaction_tokens.items.len, transaction_pos);

    var prepared_tokens = try lexer.tokenizeAlloc(alloc, "PREPARED 'x';");
    defer lexer.freeTokens(alloc, &prepared_tokens);
    var prepared_pos: usize = 0;
    try std.testing.expect(!try matchAdapterNoopTransactionBoundaryTail(prepared_tokens.items, &prepared_pos, .{ .work = true, .transaction = true }));
    try std.testing.expectEqual(@as(usize, 0), prepared_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "; SELECT 1");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, matchAdapterNoopTransactionBoundaryTail(extra_tokens.items, &extra_pos, .{}));
}

test "sql adapter grammar parses transaction control tails" {
    const alloc = std.testing.allocator;

    var lock_tokens = try lexer.tokenizeAlloc(alloc, "TABLE public.usage_records, audit_records IN SHARE ROW EXCLUSIVE MODE;");
    defer lexer.freeTokens(alloc, &lock_tokens);
    var lock_pos: usize = 0;
    var lock = try parseTableLockTailAlloc(alloc, lock_tokens.items, &lock_pos);
    defer lock.deinit(alloc);
    try std.testing.expectEqual(lock_tokens.items.len, lock_pos);
    try std.testing.expectEqual(@as(usize, 2), lock.table_names.len);
    try std.testing.expectEqualStrings("usage_records", lock.table_names[0]);
    try std.testing.expectEqualStrings("audit_records", lock.table_names[1]);
    try std.testing.expectEqual(TableLockModeSyntax.share_row_exclusive, lock.mode);

    var constraints_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINTS public.fk_usage_account, fk_usage_org DEFERRED;");
    defer lexer.freeTokens(alloc, &constraints_tokens);
    var constraints_pos: usize = 0;
    var constraints = try parseConstraintModeTailAlloc(alloc, constraints_tokens.items, &constraints_pos);
    defer constraints.deinit(alloc);
    try std.testing.expectEqual(constraints_tokens.items.len, constraints_pos);
    try std.testing.expect(!constraints.all);
    try std.testing.expectEqual(@as(usize, 2), constraints.constraint_names.len);
    try std.testing.expectEqualStrings("fk_usage_account", constraints.constraint_names[0]);
    try std.testing.expectEqualStrings("fk_usage_org", constraints.constraint_names[1]);
    try std.testing.expectEqual(ConstraintCheckModeSyntax.deferred, constraints.mode);

    var constraints_all_tokens = try lexer.tokenizeAlloc(alloc, "CONSTRAINTS ALL IMMEDIATE;");
    defer lexer.freeTokens(alloc, &constraints_all_tokens);
    var constraints_all_pos: usize = 0;
    var constraints_all = try parseConstraintModeTailAlloc(alloc, constraints_all_tokens.items, &constraints_all_pos);
    defer constraints_all.deinit(alloc);
    try std.testing.expectEqual(constraints_all_tokens.items.len, constraints_all_pos);
    try std.testing.expect(constraints_all.all);
    try std.testing.expectEqual(@as(usize, 0), constraints_all.constraint_names.len);
    try std.testing.expectEqual(ConstraintCheckModeSyntax.immediate, constraints_all.mode);

    var transaction_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY, NOT DEFERRABLE;");
    defer lexer.freeTokens(alloc, &transaction_tokens);
    var transaction_pos: usize = 0;
    const transaction = try parseTransactionModeTail(transaction_tokens.items, &transaction_pos, .set_transaction);
    try std.testing.expectEqual(transaction_tokens.items.len, transaction_pos);
    try std.testing.expectEqual(TransactionModeStarterSyntax.set_transaction, transaction.starter);
    try std.testing.expectEqual(TransactionIsolationLevelSyntax.repeatable_read, transaction.isolation_level.?);
    try std.testing.expectEqual(TransactionAccessModeSyntax.read_only, transaction.access_mode.?);
    try std.testing.expectEqual(false, transaction.deferrable.?);

    var begin_tokens = try lexer.tokenizeAlloc(alloc, "READ WRITE DEFERRABLE;");
    defer lexer.freeTokens(alloc, &begin_tokens);
    var begin_pos: usize = 0;
    const begin = try parseTransactionModeTail(begin_tokens.items, &begin_pos, .begin);
    try std.testing.expectEqual(begin_tokens.items.len, begin_pos);
    try std.testing.expectEqual(TransactionModeStarterSyntax.begin, begin.starter);
    try std.testing.expect(begin.isolation_level == null);
    try std.testing.expectEqual(TransactionAccessModeSyntax.read_write, begin.access_mode.?);
    try std.testing.expectEqual(true, begin.deferrable.?);

    var duplicate_tokens = try lexer.tokenizeAlloc(alloc, "TRANSACTION READ ONLY READ WRITE;");
    defer lexer.freeTokens(alloc, &duplicate_tokens);
    var duplicate_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseTransactionModeTail(duplicate_tokens.items, &duplicate_pos, .start_transaction));
}

test "sql adapter grammar parses maintenance job tails" {
    const alloc = std.testing.allocator;

    var vacuum_tokens = try lexer.tokenizeAlloc(alloc, "(FULL, VERBOSE, ANALYZE) public.usage_records;");
    defer lexer.freeTokens(alloc, &vacuum_tokens);
    var vacuum_pos: usize = 0;
    var vacuum = try parseVacuumMaintenanceTailAlloc(alloc, vacuum_tokens.items, &vacuum_pos);
    defer vacuum.deinit(alloc);
    try std.testing.expectEqual(vacuum_tokens.items.len, vacuum_pos);
    try std.testing.expectEqualStrings("usage_records", vacuum.table_name);
    try std.testing.expect(vacuum.full);
    try std.testing.expect(!vacuum.freeze);
    try std.testing.expect(vacuum.verbose);
    try std.testing.expect(vacuum.analyze);

    var vacuum_legacy_tokens = try lexer.tokenizeAlloc(alloc, "FULL FREEZE VERBOSE usage_records;");
    defer lexer.freeTokens(alloc, &vacuum_legacy_tokens);
    var vacuum_legacy_pos: usize = 0;
    var vacuum_legacy = try parseVacuumMaintenanceTailAlloc(alloc, vacuum_legacy_tokens.items, &vacuum_legacy_pos);
    defer vacuum_legacy.deinit(alloc);
    try std.testing.expectEqual(vacuum_legacy_tokens.items.len, vacuum_legacy_pos);
    try std.testing.expectEqualStrings("usage_records", vacuum_legacy.table_name);
    try std.testing.expect(vacuum_legacy.full);
    try std.testing.expect(vacuum_legacy.freeze);
    try std.testing.expect(vacuum_legacy.verbose);
    try std.testing.expect(!vacuum_legacy.analyze);

    var analyze_tokens = try lexer.tokenizeAlloc(alloc, "VERBOSE public.usage_records (status, amount);");
    defer lexer.freeTokens(alloc, &analyze_tokens);
    var analyze_pos: usize = 0;
    var analyze = try parseAnalyzeMaintenanceTailAlloc(alloc, analyze_tokens.items, &analyze_pos);
    defer analyze.deinit(alloc);
    try std.testing.expectEqual(analyze_tokens.items.len, analyze_pos);
    try std.testing.expectEqualStrings("usage_records", analyze.table_name);
    try std.testing.expect(analyze.verbose);
    try std.testing.expectEqual(@as(usize, 2), analyze.column_count);

    var reindex_tokens = try lexer.tokenizeAlloc(alloc, "INDEX CONCURRENTLY public.usage_status_idx;");
    defer lexer.freeTokens(alloc, &reindex_tokens);
    var reindex_pos: usize = 0;
    var reindex = try parseReindexMaintenanceTailAlloc(alloc, reindex_tokens.items, &reindex_pos);
    defer reindex.deinit(alloc);
    try std.testing.expectEqual(reindex_tokens.items.len, reindex_pos);
    try std.testing.expectEqual(ReindexMaintenanceTargetSyntax.index, reindex.target);
    try std.testing.expect(reindex.concurrently);
    try std.testing.expectEqualStrings("usage_status_idx", reindex.name);

    var cluster_tokens = try lexer.tokenizeAlloc(alloc, "VERBOSE public.usage_records USING public.usage_status_idx;");
    defer lexer.freeTokens(alloc, &cluster_tokens);
    var cluster_pos: usize = 0;
    var cluster = try parseClusterMaintenanceTailAlloc(alloc, cluster_tokens.items, &cluster_pos);
    defer cluster.deinit(alloc);
    try std.testing.expectEqual(cluster_tokens.items.len, cluster_pos);
    try std.testing.expectEqualStrings("usage_records", cluster.table_name);
    try std.testing.expectEqualStrings("usage_status_idx", cluster.index_name.?);
    try std.testing.expect(cluster.verbose);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "usage_records (status);");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseVacuumMaintenanceTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));
}

test "sql adapter grammar parses database and tablespace catalog tails" {
    const alloc = std.testing.allocator;

    var create_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE tenant_ops;");
    defer lexer.freeTokens(alloc, &create_database_tokens);
    var create_database_pos: usize = 0;
    var create_database = try parseCreateDatabaseCatalogTailAlloc(alloc, create_database_tokens.items, &create_database_pos);
    defer create_database.deinit(alloc);
    try std.testing.expectEqual(create_database_tokens.items.len, create_database_pos);
    try std.testing.expectEqualStrings("tenant_ops", create_database.database_name);

    var alter_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE tenant_ops SET timezone TO 'UTC';");
    defer lexer.freeTokens(alloc, &alter_database_tokens);
    var alter_database_pos: usize = 0;
    var alter_database = try parseAlterDatabaseCatalogTailAlloc(alloc, alter_database_tokens.items, &alter_database_pos);
    defer alter_database.deinit(alloc);
    try std.testing.expectEqual(alter_database_tokens.items.len, alter_database_pos);
    try std.testing.expectEqualStrings("tenant_ops", alter_database.database_name);
    try std.testing.expectEqualStrings("timezone", alter_database.setting_name);
    try std.testing.expectEqualStrings("\"UTC\"", alter_database.value_json);

    var drop_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE IF EXISTS tenant_ops WITH (FORCE);");
    defer lexer.freeTokens(alloc, &drop_database_tokens);
    var drop_database_pos: usize = 0;
    var drop_database = try parseDropDatabaseCatalogTailAlloc(alloc, drop_database_tokens.items, &drop_database_pos);
    defer drop_database.deinit(alloc);
    try std.testing.expectEqual(drop_database_tokens.items.len, drop_database_pos);
    try std.testing.expectEqualStrings("tenant_ops", drop_database.database_name);
    try std.testing.expect(drop_database.if_exists);
    try std.testing.expect(drop_database.force);

    var create_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace';");
    defer lexer.freeTokens(alloc, &create_tablespace_tokens);
    var create_tablespace_pos: usize = 0;
    var create_tablespace = try parseCreateTablespaceCatalogTailAlloc(alloc, create_tablespace_tokens.items, &create_tablespace_pos);
    defer create_tablespace.deinit(alloc);
    try std.testing.expectEqual(create_tablespace_tokens.items.len, create_tablespace_pos);
    try std.testing.expectEqualStrings("fastspace", create_tablespace.tablespace_name);
    try std.testing.expectEqualStrings("\"/var/lib/antfly/fastspace\"", create_tablespace.location_json);

    var rename_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE fastspace RENAME TO fastspace_archive;");
    defer lexer.freeTokens(alloc, &rename_tablespace_tokens);
    var rename_tablespace_pos: usize = 0;
    var rename_tablespace = try parseRenameTablespaceCatalogTailAlloc(alloc, rename_tablespace_tokens.items, &rename_tablespace_pos);
    defer rename_tablespace.deinit(alloc);
    try std.testing.expectEqual(rename_tablespace_tokens.items.len, rename_tablespace_pos);
    try std.testing.expectEqualStrings("fastspace", rename_tablespace.tablespace_name);
    try std.testing.expectEqualStrings("fastspace_archive", rename_tablespace.new_tablespace_name);

    var drop_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE IF EXISTS fastspace_archive;");
    defer lexer.freeTokens(alloc, &drop_tablespace_tokens);
    var drop_tablespace_pos: usize = 0;
    var drop_tablespace = try parseDropTablespaceCatalogTailAlloc(alloc, drop_tablespace_tokens.items, &drop_tablespace_pos);
    defer drop_tablespace.deinit(alloc);
    try std.testing.expectEqual(drop_tablespace_tokens.items.len, drop_tablespace_pos);
    try std.testing.expectEqualStrings("fastspace_archive", drop_tablespace.tablespace_name);
    try std.testing.expect(drop_tablespace.if_exists);

    var unsupported_database_tokens = try lexer.tokenizeAlloc(alloc, "DATABASE tenant_ops WITH OWNER app;");
    defer lexer.freeTokens(alloc, &unsupported_database_tokens);
    var unsupported_database_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateDatabaseCatalogTailAlloc(alloc, unsupported_database_tokens.items, &unsupported_database_pos));

    var unsupported_tablespace_tokens = try lexer.tokenizeAlloc(alloc, "TABLESPACE fastspace OWNER app LOCATION '/tmp';");
    defer lexer.freeTokens(alloc, &unsupported_tablespace_tokens);
    var unsupported_tablespace_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCreateTablespaceCatalogTailAlloc(alloc, unsupported_tablespace_tokens.items, &unsupported_tablespace_pos));
}

test "sql adapter grammar parses notification channel tails" {
    const alloc = std.testing.allocator;

    var listen_tokens = try lexer.tokenizeAlloc(alloc, "usage_events;");
    defer lexer.freeTokens(alloc, &listen_tokens);
    var listen_pos: usize = 0;
    var listen = try parseListenNotificationTailAlloc(alloc, listen_tokens.items, &listen_pos);
    defer listen.deinit(alloc);
    try std.testing.expectEqual(listen_tokens.items.len, listen_pos);
    try std.testing.expectEqualStrings("usage_events", listen.channel_name);

    var notify_tokens = try lexer.tokenizeAlloc(alloc, "usage_events, 'queued';");
    defer lexer.freeTokens(alloc, &notify_tokens);
    var notify_pos: usize = 0;
    var notify = try parseNotifyNotificationTailAlloc(alloc, notify_tokens.items, &notify_pos);
    defer notify.deinit(alloc);
    try std.testing.expectEqual(notify_tokens.items.len, notify_pos);
    try std.testing.expectEqualStrings("usage_events", notify.channel_name);
    try std.testing.expectEqualStrings("\"queued\"", notify.payload_json.?);

    var notify_no_payload_tokens = try lexer.tokenizeAlloc(alloc, "usage_events;");
    defer lexer.freeTokens(alloc, &notify_no_payload_tokens);
    var notify_no_payload_pos: usize = 0;
    var notify_no_payload = try parseNotifyNotificationTailAlloc(alloc, notify_no_payload_tokens.items, &notify_no_payload_pos);
    defer notify_no_payload.deinit(alloc);
    try std.testing.expectEqual(notify_no_payload_tokens.items.len, notify_no_payload_pos);
    try std.testing.expect(notify_no_payload.payload_json == null);

    var unlisten_tokens = try lexer.tokenizeAlloc(alloc, "*;");
    defer lexer.freeTokens(alloc, &unlisten_tokens);
    var unlisten_pos: usize = 0;
    var unlisten = try parseUnlistenNotificationTailAlloc(alloc, unlisten_tokens.items, &unlisten_pos);
    defer unlisten.deinit(alloc);
    try std.testing.expectEqual(unlisten_tokens.items.len, unlisten_pos);
    try std.testing.expect(unlisten.all);
    try std.testing.expect(unlisten.channel_name == null);

    var unsupported_tokens = try lexer.tokenizeAlloc(alloc, "usage_events trailing;");
    defer lexer.freeTokens(alloc, &unsupported_tokens);
    var unsupported_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseListenNotificationTailAlloc(alloc, unsupported_tokens.items, &unsupported_pos));
}

test "sql adapter grammar parses savepoint transaction tails" {
    const alloc = std.testing.allocator;

    var savepoint_tokens = try lexer.tokenizeAlloc(alloc, "before_retry;");
    defer lexer.freeTokens(alloc, &savepoint_tokens);
    var savepoint_pos: usize = 0;
    const savepoint = try parseSavepointTransactionTail(savepoint_tokens.items, &savepoint_pos);
    try std.testing.expectEqualStrings("before_retry", savepoint.savepoint_name);
    try std.testing.expectEqual(savepoint_tokens.items.len, savepoint_pos);

    var release_tokens = try lexer.tokenizeAlloc(alloc, "SAVEPOINT before_retry;");
    defer lexer.freeTokens(alloc, &release_tokens);
    var release_pos: usize = 0;
    const release = try parseReleaseSavepointTail(release_tokens.items, &release_pos);
    try std.testing.expectEqualStrings("before_retry", release.savepoint_name);
    try std.testing.expectEqual(release_tokens.items.len, release_pos);

    var release_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "before_retry;");
    defer lexer.freeTokens(alloc, &release_shorthand_tokens);
    var release_shorthand_pos: usize = 0;
    const release_shorthand = try parseReleaseSavepointTail(release_shorthand_tokens.items, &release_shorthand_pos);
    try std.testing.expectEqualStrings("before_retry", release_shorthand.savepoint_name);
    try std.testing.expectEqual(release_shorthand_tokens.items.len, release_shorthand_pos);

    var rollback_tokens = try lexer.tokenizeAlloc(alloc, "TO SAVEPOINT before_retry;");
    defer lexer.freeTokens(alloc, &rollback_tokens);
    var rollback_pos: usize = 0;
    const rollback = try parseRollbackToSavepointTail(rollback_tokens.items, &rollback_pos);
    try std.testing.expectEqualStrings("before_retry", rollback.savepoint_name);
    try std.testing.expectEqual(rollback_tokens.items.len, rollback_pos);

    var rollback_shorthand_tokens = try lexer.tokenizeAlloc(alloc, "TO before_retry;");
    defer lexer.freeTokens(alloc, &rollback_shorthand_tokens);
    var rollback_shorthand_pos: usize = 0;
    const rollback_shorthand = try parseRollbackToSavepointTail(rollback_shorthand_tokens.items, &rollback_shorthand_pos);
    try std.testing.expectEqualStrings("before_retry", rollback_shorthand.savepoint_name);
    try std.testing.expectEqual(rollback_shorthand_tokens.items.len, rollback_shorthand_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "before_retry RELEASE;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseSavepointTransactionTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses protocol cleanup tails" {
    const alloc = std.testing.allocator;

    var deallocate_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan;");
    defer lexer.freeTokens(alloc, &deallocate_tokens);
    var deallocate_pos: usize = 0;
    const deallocate = try parseDeallocatePreparedStatementTail(deallocate_tokens.items, &deallocate_pos);
    try std.testing.expectEqualStrings("usage_plan", deallocate.name.?);
    try std.testing.expect(!deallocate.all);
    try std.testing.expectEqual(deallocate_tokens.items.len, deallocate_pos);

    var deallocate_prepare_tokens = try lexer.tokenizeAlloc(alloc, "PREPARE usage_plan;");
    defer lexer.freeTokens(alloc, &deallocate_prepare_tokens);
    var deallocate_prepare_pos: usize = 0;
    const deallocate_prepare = try parseDeallocatePreparedStatementTail(deallocate_prepare_tokens.items, &deallocate_prepare_pos);
    try std.testing.expectEqualStrings("usage_plan", deallocate_prepare.name.?);
    try std.testing.expect(!deallocate_prepare.all);
    try std.testing.expectEqual(deallocate_prepare_tokens.items.len, deallocate_prepare_pos);

    var deallocate_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &deallocate_all_tokens);
    var deallocate_all_pos: usize = 0;
    const deallocate_all = try parseDeallocatePreparedStatementTail(deallocate_all_tokens.items, &deallocate_all_pos);
    try std.testing.expect(deallocate_all.all);
    try std.testing.expect(deallocate_all.name == null);
    try std.testing.expectEqual(deallocate_all_tokens.items.len, deallocate_all_pos);

    var close_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor;");
    defer lexer.freeTokens(alloc, &close_tokens);
    var close_pos: usize = 0;
    const close = try parseCloseCursorPortalTail(close_tokens.items, &close_pos);
    try std.testing.expectEqualStrings("usage_cursor", close.name.?);
    try std.testing.expect(!close.all);
    try std.testing.expectEqual(close_tokens.items.len, close_pos);

    var close_all_tokens = try lexer.tokenizeAlloc(alloc, "ALL;");
    defer lexer.freeTokens(alloc, &close_all_tokens);
    var close_all_pos: usize = 0;
    const close_all = try parseCloseCursorPortalTail(close_all_tokens.items, &close_all_pos);
    try std.testing.expect(close_all.all);
    try std.testing.expect(close_all.name == null);
    try std.testing.expectEqual(close_all_tokens.items.len, close_all_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor; CLOSE ALL;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseCloseCursorPortalTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses prepared statement syntax" {
    const alloc = std.testing.allocator;

    var prepare_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan(text, uuid) AS SELECT id FROM usage_records WHERE status = $1;");
    defer lexer.freeTokens(alloc, &prepare_tokens);
    var prepare_pos: usize = 0;
    const prepare = try parsePrepareStatementTail(prepare_tokens.items, &prepare_pos);
    try std.testing.expectEqualStrings("usage_plan", prepare.statement_name);
    try std.testing.expectEqual(@as(usize, 2), prepare.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.read, prepare.statement_kind);
    try std.testing.expectEqual(prepare_tokens.items.len, prepare_pos);

    var prepare_merge_tokens = try lexer.tokenizeAlloc(alloc, "merge_plan AS MERGE INTO usage_records USING source_records ON usage_records.id = source_records.id WHEN MATCHED THEN UPDATE SET status = source_records.status;");
    defer lexer.freeTokens(alloc, &prepare_merge_tokens);
    var prepare_merge_pos: usize = 0;
    const prepare_merge = try parsePrepareStatementTail(prepare_merge_tokens.items, &prepare_merge_pos);
    try std.testing.expectEqualStrings("merge_plan", prepare_merge.statement_name);
    try std.testing.expectEqual(@as(usize, 0), prepare_merge.parameter_count);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.write, prepare_merge.statement_kind);
    try std.testing.expectEqual(prepare_merge_tokens.items.len, prepare_merge_pos);

    var execute_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan('open', -3, true, null);");
    defer lexer.freeTokens(alloc, &execute_tokens);
    var execute_pos: usize = 0;
    const execute = try parseExecutePreparedStatementTail(execute_tokens.items, &execute_pos);
    try std.testing.expectEqualStrings("usage_plan", execute.statement_name);
    try std.testing.expectEqual(@as(usize, 4), execute.argument_count);
    try std.testing.expectEqual(execute_tokens.items.len, execute_pos);

    var execute_bare_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan;");
    defer lexer.freeTokens(alloc, &execute_bare_tokens);
    var execute_bare_pos: usize = 0;
    const execute_bare = try parseExecutePreparedStatementTail(execute_bare_tokens.items, &execute_bare_pos);
    try std.testing.expectEqualStrings("usage_plan", execute_bare.statement_name);
    try std.testing.expectEqual(@as(usize, 0), execute_bare.argument_count);
    try std.testing.expectEqual(execute_bare_tokens.items.len, execute_bare_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "usage_plan('open'); EXECUTE other_plan;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseExecutePreparedStatementTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses cursor portal syntax" {
    const alloc = std.testing.allocator;

    var declare_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor BINARY NO SCROLL CURSOR WITH HOLD FOR SELECT id FROM usage_records;");
    defer lexer.freeTokens(alloc, &declare_tokens);
    var declare_pos: usize = 0;
    const declare = try parseDeclareCursorPortalPrefix(declare_tokens.items, &declare_pos);
    try std.testing.expectEqualStrings("usage_cursor", declare.portal_name);
    try std.testing.expectEqual(CursorScrollSyntax.no_scroll, declare.scroll);
    try std.testing.expect(declare.binary);
    try std.testing.expect(declare.hold);
    try std.testing.expect(parser.peekKeyword(declare_tokens.items, declare_pos, "select"));

    var declare_tail_tokens = try lexer.tokenizeAlloc(alloc, "usage_cursor BINARY NO SCROLL CURSOR WITH HOLD FOR SELECT id FROM usage_records;");
    defer lexer.freeTokens(alloc, &declare_tail_tokens);
    var declare_tail_pos: usize = 0;
    const declare_tail = try parseDeclareCursorPortalTail(declare_tail_tokens.items, &declare_tail_pos);
    try std.testing.expectEqualStrings("usage_cursor", declare_tail.portal_name);
    try std.testing.expectEqual(CursorScrollSyntax.no_scroll, declare_tail.scroll);
    try std.testing.expectEqual(PreparedStatementSubjectSyntax.read, declare_tail.statement_kind.?);
    try std.testing.expectEqual(declare_tail_tokens.items.len, declare_tail_pos);

    var fetch_tokens = try lexer.tokenizeAlloc(alloc, "BACKWARD -5 FROM usage_cursor;");
    defer lexer.freeTokens(alloc, &fetch_tokens);
    var fetch_pos: usize = 0;
    const fetch = try parseFetchCursorPortalTail(fetch_tokens.items, &fetch_pos);
    try std.testing.expectEqualStrings("usage_cursor", fetch.portal_name);
    try std.testing.expectEqual(CursorFetchDirectionSyntax.backward, fetch.direction);
    try std.testing.expectEqual(@as(?i64, -5), fetch.count);
    try std.testing.expectEqual(fetch_tokens.items.len, fetch_pos);

    var fetch_all_tokens = try lexer.tokenizeAlloc(alloc, "FORWARD ALL IN usage_cursor;");
    defer lexer.freeTokens(alloc, &fetch_all_tokens);
    var fetch_all_pos: usize = 0;
    const fetch_all = try parseFetchCursorPortalTail(fetch_all_tokens.items, &fetch_all_pos);
    try std.testing.expectEqualStrings("usage_cursor", fetch_all.portal_name);
    try std.testing.expectEqual(CursorFetchDirectionSyntax.forward, fetch_all.direction);
    try std.testing.expect(fetch_all.count == null);
    try std.testing.expectEqual(fetch_all_tokens.items.len, fetch_all_pos);

    var extra_tokens = try lexer.tokenizeAlloc(alloc, "NEXT FROM usage_cursor; FETCH NEXT FROM other_cursor;");
    defer lexer.freeTokens(alloc, &extra_tokens);
    var extra_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseFetchCursorPortalTail(extra_tokens.items, &extra_pos));
}

test "sql adapter grammar parses explain prefixes and options" {
    const basic = try parseExplainPrefix("EXPLAIN SELECT id FROM usage_records;");
    try std.testing.expect(!basic.analyze);
    try std.testing.expectEqual(ast.SqlExplainFormat.text, basic.format);
    try std.testing.expect(!basic.verbose);
    try std.testing.expect(basic.costs);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", basic.inner_sql);

    const options = try parseExplainPrefix("EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF, ANALYZE ON) SELECT id FROM usage_records");
    try std.testing.expect(options.analyze);
    try std.testing.expectEqual(ast.SqlExplainFormat.json, options.format);
    try std.testing.expect(options.verbose);
    try std.testing.expect(!options.costs);
    try std.testing.expectEqualStrings("SELECT id FROM usage_records", options.inner_sql);

    const analyze = try parseExplainPrefix("EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')");
    try std.testing.expect(analyze.analyze);
    try std.testing.expectEqualStrings("INSERT INTO usage_records (id) VALUES ('u1')", analyze.inner_sql);

    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAIN (FORMAT YAML) SELECT 1"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAINED SELECT 1"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseExplainPrefix("EXPLAIN"));
}

test "sql adapter grammar parses row claim clauses" {
    const alloc = std.testing.allocator;

    var skip_tokens = try lexer.tokenizeAlloc(alloc, "NO KEY UPDATE OF usage_records, public.jobs SKIP LOCKED");
    defer lexer.freeTokens(alloc, &skip_tokens);
    var skip_pos: usize = 0;
    var skip_clause = try parseForRowClaimClauseAlloc(alloc, skip_tokens.items, &skip_pos);
    defer skip_clause.deinit(alloc);
    try std.testing.expectEqual(skip_tokens.items.len, skip_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_no_key_update, skip_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.skip_locked, skip_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 2), skip_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", skip_clause.targets[0]);
    try std.testing.expectEqualStrings("public.jobs", skip_clause.targets[1]);

    var share_tokens = try lexer.tokenizeAlloc(alloc, "KEY SHARE OF ONLY usage_records NOWAIT");
    defer lexer.freeTokens(alloc, &share_tokens);
    var share_pos: usize = 0;
    var share_clause = try parseForRowClaimClauseAlloc(alloc, share_tokens.items, &share_pos);
    defer share_clause.deinit(alloc);
    try std.testing.expectEqual(share_tokens.items.len, share_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_key_share, share_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.nowait, share_clause.clause.wait_policy);
    try std.testing.expectEqual(@as(usize, 1), share_clause.targets.len);
    try std.testing.expectEqualStrings("usage_records", share_clause.targets[0]);

    var default_tokens = try lexer.tokenizeAlloc(alloc, "UPDATE");
    defer lexer.freeTokens(alloc, &default_tokens);
    var default_pos: usize = 0;
    var default_clause = try parseForRowClaimClauseAlloc(alloc, default_tokens.items, &default_pos);
    defer default_clause.deinit(alloc);
    try std.testing.expectEqual(default_tokens.items.len, default_pos);
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_update, default_clause.clause.mode);
    try std.testing.expectEqual(db_mod.types.RowClaimWaitPolicy.wait, default_clause.clause.wait_policy);

    var invalid_tokens = try lexer.tokenizeAlloc(alloc, "SHARE SKIP");
    defer lexer.freeTokens(alloc, &invalid_tokens);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseForRowClaimClauseAlloc(alloc, invalid_tokens.items, &invalid_pos));
}

test "sql adapter grammar parses relation population syntax" {
    const alloc = std.testing.allocator;

    var select_into = try parseRelationPopulationSqlAlloc(
        alloc,
        "SELECT account_id, total INTO public.usage_archive FROM usage_records WHERE total > 10",
    );
    defer select_into.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.select_into, select_into.mode);
    try std.testing.expectEqualStrings("public.usage_archive", select_into.target_identifier);
    try std.testing.expect(select_into.target_lifetime == null);
    try std.testing.expect(!select_into.if_not_exists);
    try std.testing.expectEqualStrings("SELECT account_id, total FROM usage_records WHERE total > 10", select_into.source_sql);

    var create_as = try parseRelationPopulationSqlAlloc(
        alloc,
        "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT account_id FROM usage_records",
    );
    defer create_as.deinit(alloc);
    try std.testing.expectEqual(RelationPopulationMode.create_table_as, create_as.mode);
    try std.testing.expectEqualStrings("usage_session_archive", create_as.target_identifier);
    try std.testing.expectEqual(RelationLifetimeKind.temporary, create_as.target_lifetime.?);
    try std.testing.expect(create_as.if_not_exists);
    try std.testing.expectEqualStrings("SELECT account_id FROM usage_records", create_as.source_sql);

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        parseRelationPopulationSqlAlloc(alloc, "CREATE TABLE usage_archive SELECT account_id FROM usage_records"),
    );
}

test "sql adapter grammar normalizes public object identifiers" {
    const alloc = std.testing.allocator;

    const bare = try normalizeSqlObjectIdentifierAlloc(alloc, "usage_records");
    defer alloc.free(bare);
    try std.testing.expectEqualStrings("usage_records", bare);

    const public_qualified = try normalizeSqlObjectIdentifierAlloc(alloc, "public.usage_records");
    defer alloc.free(public_qualified);
    try std.testing.expectEqualStrings("usage_records", public_qualified);

    const other_schema = try normalizeSqlObjectIdentifierAlloc(alloc, "tenant_1.usage_records");
    defer alloc.free(other_schema);
    try std.testing.expectEqualStrings("tenant_1.usage_records", other_schema);

    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, ".usage_records"));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, "public."));
    try std.testing.expectError(error.UnsupportedSqlShape, normalizeSqlObjectIdentifierAlloc(alloc, "public.analytics.usage_records"));
}

test "sql adapter grammar parses owned identifiers and normalized object lists" {
    const alloc = std.testing.allocator;

    var identifiers = try lexer.tokenizeAlloc(alloc, "tenant_id, order_id, status");
    defer lexer.freeTokens(alloc, &identifiers);
    var identifier_pos: usize = 0;
    const identifier_list = try parseIdentifierListAlloc(alloc, identifiers.items, &identifier_pos);
    defer {
        for (identifier_list) |item| alloc.free(item);
        alloc.free(identifier_list);
    }
    try std.testing.expectEqual(identifiers.items.len, identifier_pos);
    try std.testing.expectEqual(@as(usize, 3), identifier_list.len);
    try std.testing.expectEqualStrings("tenant_id", identifier_list[0]);
    try std.testing.expectEqualStrings("order_id", identifier_list[1]);
    try std.testing.expectEqualStrings("status", identifier_list[2]);

    var objects = try lexer.tokenizeAlloc(alloc, "public.usage_records, tenant_1.audit_records");
    defer lexer.freeTokens(alloc, &objects);
    var object_pos: usize = 0;
    const object_list = try parseSqlObjectIdentifierListAlloc(alloc, objects.items, &object_pos);
    defer {
        for (object_list) |item| alloc.free(item);
        alloc.free(object_list);
    }
    try std.testing.expectEqual(objects.items.len, object_pos);
    try std.testing.expectEqual(@as(usize, 2), object_list.len);
    try std.testing.expectEqualStrings("usage_records", object_list[0]);
    try std.testing.expectEqualStrings("tenant_1.audit_records", object_list[1]);

    var table_ref = try lexer.tokenizeAlloc(alloc, "ONLY public.usage_records");
    defer lexer.freeTokens(alloc, &table_ref);
    var table_pos: usize = 0;
    const table_name = try parseSqlTableReferenceIdentifierOwnedAlloc(alloc, table_ref.items, &table_pos);
    defer alloc.free(table_name);
    try std.testing.expectEqual(table_ref.items.len, table_pos);
    try std.testing.expectEqualStrings("usage_records", table_name);

    var invalid = try lexer.tokenizeAlloc(alloc, "usage_records,");
    defer lexer.freeTokens(alloc, &invalid);
    var invalid_pos: usize = 0;
    try std.testing.expectError(error.UnsupportedSqlShape, parseIdentifierListAlloc(alloc, invalid.items, &invalid_pos));
}
