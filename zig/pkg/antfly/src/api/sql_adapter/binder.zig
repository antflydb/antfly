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

const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const runtime_schema = @import("../../storage/schema.zig");
const schema_api = @import("../../schema/mod.zig");
const table_catalog = @import("../table_catalog.zig");
const grammar = @import("grammar.zig");
const lexer = @import("lexer.zig");
const parser = @import("parser.zig");
const token_mod = @import("token.zig");

const Token = token_mod.Token;

pub fn runtimeSchemaForCatalogTableAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !runtime_schema.TableSchema {
    return try runtimeSchemaForQualifiedCatalogTableAlloc(
        alloc,
        catalog,
        metadata_table_manager.default_database_name,
        metadata_table_manager.default_namespace_name,
        table_name,
    );
}

pub fn runtimeSchemaForQualifiedCatalogTableAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) !runtime_schema.TableSchema {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const schema_json = qualifiedTableSchemaJson(&snapshot, database_name, namespace_name, table_name) orelse return error.InvalidSqlCatalog;
    if (schema_json.len == 0) return error.InvalidSqlCatalog;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return try schema_api.deriveRuntimeTableSchema(alloc, parsed);
}

pub fn tableSchemaJson(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?[]const u8 {
    return qualifiedTableSchemaJson(
        snapshot,
        metadata_table_manager.default_database_name,
        metadata_table_manager.default_namespace_name,
        table_name,
    );
}

pub fn qualifiedTableSchemaJson(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ?[]const u8 {
    for (snapshot.tables) |table| {
        if (!std.mem.eql(u8, table.database_name, database_name)) continue;
        if (!std.mem.eql(u8, table.namespace_name, namespace_name)) continue;
        if (std.mem.eql(u8, table.name, table_name)) return table.schema_json;
    }
    return null;
}

pub const InsertSourceTableNames = struct {
    target: []const u8,
    source: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.target));
        alloc.free(@constCast(self.source));
        self.* = undefined;
    }
};

pub const ReadSourceTableNames = struct {
    left: []const u8,
    source: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.left));
        alloc.free(@constCast(self.source));
        self.* = undefined;
    }
};

const SelectReadTableNames = struct {
    left: []const u8,
    source: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.left));
        if (self.source) |source| alloc.free(@constCast(source));
        self.* = undefined;
    }
};

const CteSourceBinding = struct {
    name: []const u8,
    source: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.source));
        self.* = undefined;
    }
};

pub fn insertSourceTableNamesAlloc(alloc: std.mem.Allocator, sql: []const u8) !?InsertSourceTableNames {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    if (tokens.items.len == 0 or tokens.items[0].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens.items[0].text, "with")) return try insertSourceTableNamesFromWithAlloc(alloc, tokens.items);
    if (!std.ascii.eqlIgnoreCase(tokens.items[0].text, "insert")) return null;
    return try insertSourceTableNamesFromInsertAlloc(alloc, tokens.items, 0);
}

pub fn joinedWriteSourceTableNamesAlloc(alloc: std.mem.Allocator, sql: []const u8) !?InsertSourceTableNames {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    if (tokens.items.len == 0 or tokens.items[0].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens.items[0].text, "with")) return try joinedWriteSourceTableNamesFromWithAlloc(alloc, tokens.items);

    return try joinedWriteSourceTableNamesFromStatementAlloc(alloc, tokens.items, 0);
}

fn joinedWriteSourceTableNamesFromStatementAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    statement_index: usize,
) !?InsertSourceTableNames {
    if (statement_index >= tokens.len or tokens[statement_index].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens[statement_index].text, "update")) {
        var target_index: usize = statement_index + 1;
        _ = consumeKeyword(tokens, &target_index, "only");
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const from_index = findTopLevelKeyword(tokens[target_index + 1 ..], "from") orelse {
            alloc.free(target);
            return null;
        };
        var source_index = target_index + 1 + from_index + 1;
        _ = consumeKeyword(tokens, &source_index, "only");
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    if (std.ascii.eqlIgnoreCase(tokens[statement_index].text, "delete")) {
        var target_index: usize = statement_index + 1;
        if (!consumeKeyword(tokens, &target_index, "from")) return null;
        _ = consumeKeyword(tokens, &target_index, "only");
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const using_index = findTopLevelKeyword(tokens[target_index + 1 ..], "using") orelse {
            alloc.free(target);
            return null;
        };
        var source_index = target_index + 1 + using_index + 1;
        _ = consumeKeyword(tokens, &source_index, "only");
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    if (std.ascii.eqlIgnoreCase(tokens[statement_index].text, "merge")) {
        var target_index: usize = statement_index + 1;
        if (!consumeKeyword(tokens, &target_index, "into")) return null;
        _ = consumeKeyword(tokens, &target_index, "only");
        if (target_index >= tokens.len or tokens[target_index].kind != .identifier) return error.UnsupportedSqlShape;
        const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[target_index].text);
        var target_transferred = false;
        errdefer if (!target_transferred) alloc.free(target);

        const using_index = findTopLevelKeyword(tokens[target_index + 1 ..], "using") orelse {
            alloc.free(target);
            return null;
        };
        var source_index = target_index + 1 + using_index + 1;
        _ = consumeKeyword(tokens, &source_index, "only");
        if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        errdefer alloc.free(source);

        target_transferred = true;
        return .{ .target = target, .source = source };
    }

    return null;
}

fn joinedWriteSourceTableNamesFromWithAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?InsertSourceTableNames {
    var index: usize = 1;
    if (consumeKeyword(tokens, &index, "recursive")) return error.UnsupportedSqlShape;

    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
        const cte_name = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (cteBindingIndex(cte_bindings.items, cte_name) != null) return error.UnsupportedSqlShape;
        index += 1;

        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape) + 1;
        }
        if (!consumeKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        if (index + 1 >= close_index) return error.UnsupportedSqlShape;

        var cte_tables = (try selectReadTableNamesAlloc(alloc, tokens[index + 1 .. close_index], 0)) orelse return error.UnsupportedSqlShape;
        defer cte_tables.deinit(alloc);
        try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, &cte_tables);
        if (cte_tables.source) |source| {
            if (!std.mem.eql(u8, cte_tables.left, source)) return error.UnsupportedSqlShape;
        }

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = try alloc.dupe(u8, cte_tables.left),
        });
        cte_name_transferred = true;

        index = close_index + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    var final = (try joinedWriteSourceTableNamesFromStatementAlloc(alloc, tokens, index)) orelse return null;
    errdefer final.deinit(alloc);
    final.target = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, final.target);
    final.source = try resolveTableNameAgainstCtesAlloc(alloc, cte_bindings.items, final.source);
    return final;
}

pub fn readSourceTableNamesAlloc(alloc: std.mem.Allocator, sql: []const u8) !?ReadSourceTableNames {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    if (tokens.items.len == 0 or tokens.items[0].kind != .identifier) return null;
    if (std.ascii.eqlIgnoreCase(tokens.items[0].text, "with")) return try readSourceTableNamesFromWithAlloc(alloc, tokens.items);
    if (!std.ascii.eqlIgnoreCase(tokens.items[0].text, "select")) return null;
    return try readSourceTableNamesFromSelectAlloc(alloc, tokens.items, 0);
}

fn readSourceTableNamesFromSelectAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    select_index: usize,
) !?ReadSourceTableNames {
    var tables = (try selectReadTableNamesAlloc(alloc, tokens, select_index)) orelse return null;
    errdefer tables.deinit(alloc);
    const source = tables.source orelse {
        tables.deinit(alloc);
        return null;
    };
    tables.source = null;
    return .{ .left = tables.left, .source = source };
}

fn selectReadTableNamesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    select_index: usize,
) !?SelectReadTableNames {
    if (select_index >= tokens.len or tokens[select_index].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[select_index].text, "select")) return null;

    const from_index = if (findTopLevelKeyword(tokens[select_index..], "from")) |relative|
        select_index + relative
    else
        return null;
    var left_index = from_index + 1;
    _ = consumeKeyword(tokens, &left_index, "only");
    if (left_index >= tokens.len or tokens[left_index].kind != .identifier) return error.UnsupportedSqlShape;
    const left = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[left_index].text);
    var left_transferred = false;
    errdefer if (!left_transferred) alloc.free(left);

    const join_index = if (findTopLevelKeyword(tokens[left_index + 1 ..], "join")) |relative|
        left_index + 1 + relative
    else {
        if (try selectSetOperationSourceTableNameAlloc(alloc, tokens, left_index + 1)) |source| {
            left_transferred = true;
            return .{ .left = left, .source = source };
        }
        left_transferred = true;
        return .{ .left = left };
    };
    var source_index = join_index + 1;
    if (consumeKeyword(tokens, &source_index, "lateral")) {
        if (source_index >= tokens.len or tokens[source_index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, source_index) orelse return error.UnsupportedSqlShape;
        const inner_from = findTopLevelKeyword(tokens[source_index + 1 .. close_index], "from") orelse return error.UnsupportedSqlShape;
        source_index = source_index + 1 + inner_from + 1;
    }
    _ = consumeKeyword(tokens, &source_index, "only");
    if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
    errdefer alloc.free(source);

    left_transferred = true;
    return .{ .left = left, .source = source };
}

fn selectSetOperationSourceTableNameAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    start_index: usize,
) !?[]const u8 {
    var depth: usize = 0;
    var i = start_index;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .identifier => {
                if (depth != 0 or !selectSetOperationKeyword(token.text)) continue;
                var select_index = i + 1;
                if (std.ascii.eqlIgnoreCase(token.text, "union")) {
                    _ = consumeKeyword(tokens, &select_index, "all") or consumeKeyword(tokens, &select_index, "distinct");
                } else {
                    _ = consumeKeyword(tokens, &select_index, "distinct");
                }
                if (select_index >= tokens.len or tokens[select_index].kind != .identifier or
                    !std.ascii.eqlIgnoreCase(tokens[select_index].text, "select"))
                {
                    return error.UnsupportedSqlShape;
                }
                const from_relative = findTopLevelKeyword(tokens[select_index + 1 ..], "from") orelse return error.UnsupportedSqlShape;
                var source_index = select_index + 1 + from_relative + 1;
                _ = consumeKeyword(tokens, &source_index, "only");
                if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
                return try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
            },
            else => {},
        }
    }
    return null;
}

fn selectSetOperationKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "union") or
        std.ascii.eqlIgnoreCase(text, "intersect") or
        std.ascii.eqlIgnoreCase(text, "except");
}

fn readSourceTableNamesFromWithAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?ReadSourceTableNames {
    var index: usize = 1;
    if (consumeKeyword(tokens, &index, "recursive")) return error.UnsupportedSqlShape;

    var cte_bindings = std.ArrayListUnmanaged(CteSourceBinding).empty;
    defer {
        for (cte_bindings.items) |*binding| binding.deinit(alloc);
        cte_bindings.deinit(alloc);
    }

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
        const cte_name = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (cteBindingIndex(cte_bindings.items, cte_name) != null) return error.UnsupportedSqlShape;
        index += 1;

        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape) + 1;
        }
        if (!consumeKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        if (index + 1 >= close_index) return error.UnsupportedSqlShape;

        var cte_tables = (try selectReadTableNamesAlloc(alloc, tokens[index + 1 .. close_index], 0)) orelse return error.UnsupportedSqlShape;
        defer cte_tables.deinit(alloc);
        try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, &cte_tables);
        if (cte_tables.source) |source| {
            if (!std.mem.eql(u8, cte_tables.left, source)) return error.UnsupportedSqlShape;
        }

        try cte_bindings.append(alloc, .{
            .name = cte_name,
            .source = try alloc.dupe(u8, cte_tables.left),
        });
        cte_name_transferred = true;

        index = close_index + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    var final_tables = (try selectReadTableNamesAlloc(alloc, tokens, index)) orelse return null;
    errdefer final_tables.deinit(alloc);
    try resolveSelectReadTablesAgainstCtes(alloc, cte_bindings.items, &final_tables);
    const source = final_tables.source orelse {
        final_tables.deinit(alloc);
        return null;
    };
    final_tables.source = null;
    return .{ .left = final_tables.left, .source = source };
}

fn normalizeSqlObjectIdentifierAlloc(alloc: std.mem.Allocator, identifier: []const u8) ![]const u8 {
    return try grammar.normalizeSqlObjectIdentifierAlloc(alloc, identifier);
}

fn insertSourceTableNamesFromInsertAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    insert_index: usize,
) !?InsertSourceTableNames {
    if (insert_index >= tokens.len or tokens[insert_index].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[insert_index].text, "insert")) return null;
    var index: usize = insert_index + 1;
    if (!consumeKeyword(tokens, &index, "into")) return null;
    _ = consumeKeyword(tokens, &index, "only");
    if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
    const target = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
    var target_transferred = false;
    errdefer if (!target_transferred) alloc.free(target);
    index += 1;

    const select_index = findTopLevelKeyword(tokens[index..], "select") orelse {
        alloc.free(target);
        return null;
    };
    const absolute_select = index + select_index;
    const from_index = findTopLevelKeyword(tokens[absolute_select + 1 ..], "from") orelse return error.UnsupportedSqlShape;
    var source_index = absolute_select + 1 + from_index + 1;
    _ = consumeKeyword(tokens, &source_index, "only");
    if (source_index >= tokens.len or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
    const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
    errdefer alloc.free(source);

    target_transferred = true;
    return .{ .target = target, .source = source };
}

fn insertSourceTableNamesFromWithAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
) !?InsertSourceTableNames {
    var index: usize = 1;
    if (consumeKeyword(tokens, &index, "recursive")) return error.UnsupportedSqlShape;

    var cte_names = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (cte_names.items) |name| alloc.free(name);
        cte_names.deinit(alloc);
    }
    var base_source: ?[]const u8 = null;
    errdefer if (base_source) |source| alloc.free(source);

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return error.UnsupportedSqlShape;
        const cte_name = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[index].text);
        var cte_name_transferred = false;
        errdefer if (!cte_name_transferred) alloc.free(cte_name);
        if (sqlStringSliceContains(cte_names.items, cte_name)) return error.UnsupportedSqlShape;
        try cte_names.append(alloc, cte_name);
        cte_name_transferred = true;
        index += 1;

        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape) + 1;
        }
        if (!consumeKeyword(tokens, &index, "as")) return error.UnsupportedSqlShape;
        try parser.consumeCteMaterializationHint(tokens, &index);
        if (index >= tokens.len or tokens[index].kind != .lparen) return error.UnsupportedSqlShape;
        const close_index = findMatchingRParenIndex(tokens, index) orelse return error.UnsupportedSqlShape;
        const from_index = findTopLevelKeyword(tokens[index + 1 .. close_index], "from") orelse return error.UnsupportedSqlShape;
        var source_index = index + 1 + from_index + 1;
        _ = consumeKeyword(tokens, &source_index, "only");
        if (source_index >= close_index or tokens[source_index].kind != .identifier) return error.UnsupportedSqlShape;
        const source = try normalizeSqlObjectIdentifierAlloc(alloc, tokens[source_index].text);
        var source_transferred = false;
        errdefer if (!source_transferred) alloc.free(source);
        if (!sqlStringSliceContains(cte_names.items, source)) {
            if (base_source) |existing| {
                if (!std.mem.eql(u8, existing, source)) return error.UnsupportedSqlShape;
                alloc.free(source);
            } else {
                base_source = source;
                source_transferred = true;
            }
        } else {
            alloc.free(source);
        }

        index = close_index + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    var final = (try insertSourceTableNamesFromInsertAlloc(alloc, tokens, index)) orelse {
        if (base_source) |source| alloc.free(source);
        base_source = null;
        return null;
    };
    errdefer final.deinit(alloc);
    if (sqlStringSliceContains(cte_names.items, final.source)) {
        const resolved_source = base_source orelse return error.UnsupportedSqlShape;
        const target = final.target;
        alloc.free(@constCast(final.source));
        base_source = null;
        return .{
            .target = target,
            .source = resolved_source,
        };
    }
    if (base_source) |source| alloc.free(source);
    base_source = null;
    return final;
}

fn resolveSelectReadTablesAgainstCtes(
    alloc: std.mem.Allocator,
    bindings: []const CteSourceBinding,
    tables: *SelectReadTableNames,
) !void {
    tables.left = try resolveTableNameAgainstCtesAlloc(alloc, bindings, tables.left);
    if (tables.source) |source| {
        tables.source = try resolveTableNameAgainstCtesAlloc(alloc, bindings, source);
    }
}

fn resolveTableNameAgainstCtesAlloc(
    alloc: std.mem.Allocator,
    bindings: []const CteSourceBinding,
    owned_name: []const u8,
) ![]const u8 {
    const binding_index = cteBindingIndex(bindings, owned_name) orelse return owned_name;
    const resolved = try alloc.dupe(u8, bindings[binding_index].source);
    alloc.free(@constCast(owned_name));
    return resolved;
}

fn cteBindingIndex(bindings: []const CteSourceBinding, name: []const u8) ?usize {
    for (bindings, 0..) |binding, index| {
        if (std.mem.eql(u8, binding.name, name)) return index;
    }
    return null;
}

fn consumeKeyword(tokens: []const Token, index: *usize, keyword: []const u8) bool {
    return parser.matchKeyword(tokens, index, keyword);
}

fn findTopLevelKeyword(tokens: []const Token, keyword: []const u8) ?usize {
    return parser.findTopLevelKeyword(tokens, keyword);
}

fn findMatchingRParenIndex(tokens: []const Token, lparen_index: usize) ?usize {
    return parser.findMatchingRParenIndex(tokens, lparen_index);
}

fn sqlStringSliceContains(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

test "sql adapter binder resolves runtime schema from catalog table name" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const tenant_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"}},"required":["tenant_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id"]}}
    ;
    var catalog = TestCatalog.init("usage_records", schema_json, tenant_schema_json);
    const runtime = try runtimeSchemaForCatalogTableAlloc(alloc, catalog.iface(), "usage_records");
    defer runtime_schema.freeSchema(alloc, runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expect(runtime.primary_key != null);
    try std.testing.expectEqual(@as(usize, 2), runtime.relational_columns.len);

    const tenant_runtime = try runtimeSchemaForQualifiedCatalogTableAlloc(alloc, catalog.iface(), "tenant_ops", "analytics", "usage_records");
    defer runtime_schema.freeSchema(alloc, tenant_runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, tenant_runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 1), tenant_runtime.relational_columns.len);
    try std.testing.expectError(error.InvalidSqlCatalog, runtimeSchemaForCatalogTableAlloc(alloc, catalog.iface(), "missing_records"));
}

test "sql adapter binder resolves read source tables through non recursive ctes" {
    const alloc = std.testing.allocator;

    var joined = (try readSourceTableNamesAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, tenant, customer_id FROM usage_records), active_customers AS (SELECT id, tenant, name FROM customer_records) SELECT o.id, c.name FROM open_orders AS o LEFT JOIN active_customers AS c ON o.tenant = c.tenant",
    )).?;
    defer joined.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", joined.left);
    try std.testing.expectEqualStrings("customer_records", joined.source);

    var lateral = (try readSourceTableNamesAlloc(
        alloc,
        "WITH orgs AS (SELECT id FROM usage_records), balances AS (SELECT organization_id, amount FROM balance_records) SELECT org.id, latest.amount FROM orgs AS org LEFT JOIN LATERAL (SELECT amount FROM balances AS bal WHERE bal.organization_id = org.id LIMIT 1) AS latest ON true",
    )).?;
    defer lateral.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", lateral.left);
    try std.testing.expectEqualStrings("balance_records", lateral.source);
}

test "sql adapter binder rejects ambiguous physical cte read source tables" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        readSourceTableNamesAlloc(
            alloc,
            "WITH mixed AS (SELECT o.id FROM orders AS o JOIN customers AS c ON o.customer_id = c.id) SELECT mixed.id, s.id FROM mixed JOIN shipments AS s ON mixed.id = s.order_id",
        ),
    );
}

const TestCatalog = struct {
    tables: [2]metadata_table_manager.TableRecord,

    fn init(table_name: []const u8, schema_json: []const u8, tenant_schema_json: []const u8) @This() {
        return .{ .tables = .{
            .{
                .table_id = 2,
                .name = table_name,
                .database_name = "tenant_ops",
                .namespace_name = "analytics",
                .placement_role = "data",
                .schema_json = tenant_schema_json,
            },
            .{
                .table_id = 1,
                .name = table_name,
                .database_name = metadata_table_manager.default_database_name,
                .namespace_name = metadata_table_manager.default_namespace_name,
                .placement_role = "data",
                .schema_json = schema_json,
            },
        } };
    }

    fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = self.tables[0..],
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};
