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

//! SQL/PGQ front end into the shared graph IR. This token adapter is a compiler
//! boundary, never an execution-time Cypher string function.
const std = @import("std");
const token = @import("sql_parser").token;
const cypher = @import("cypher");
const ast = @import("ast.zig");
const A = std.mem.Allocator;
fn name(a: A, output: *std.ArrayList(u8), text: []const u8, folded: bool) !void {
    try output.append(a, '`');
    for (text) |char| {
        if (char == '`') try output.append(a, '`');
        try output.append(a, if (folded) std.ascii.toLower(char) else char);
    }
    try output.append(a, '`');
}
fn append(a: A, output: *std.ArrayList(u8), tokens: []const token.Token, pattern: bool) !void {
    var labels = false;
    var in_pattern = pattern;
    for (tokens, 0..) |t, i| {
        try output.append(a, ' ');
        if (t.keyword == .where) in_pattern = false;
        if (in_pattern and t.keyword == .is) {
            try output.append(a, ':');
            labels = true;
            continue;
        }
        if (t.kind == .identifier) {
            if (labels) {
                try name(a, output, t.text, false);
                labels = false;
            } else if (t.keyword != null and (token.keywordClass(t.keyword.?) == .reserved or t.keyword == .is)) {
                try output.appendSlice(a, t.text);
            } else if (i + 1 < tokens.len and tokens[i + 1].kind == .lparen and !t.owned and std.ascii.eqlIgnoreCase(t.text, "lower")) {
                try output.appendSlice(a, "toLower");
            } else if (i + 1 < tokens.len and tokens[i + 1].kind == .lparen and !t.owned and std.ascii.eqlIgnoreCase(t.text, "upper")) {
                try output.appendSlice(a, "toUpper");
            } else try name(a, output, t.text, !t.owned);
        } else if (t.kind == .string) {
            try output.appendSlice(a, try std.json.Stringify.valueAlloc(a, t.text, .{}));
        } else try output.appendSlice(a, t.text);
    }
}
pub fn lower(a: A, graph_name: ast.Name, pattern: []const token.Token, columns: []const token.Token, diagnostic: *cypher.compiler.Diagnostic) !ast.GraphTable {
    var source: std.ArrayList(u8) = .empty;
    try source.appendSlice(a, "MATCH");
    try append(a, &source, pattern, true);
    try source.appendSlice(a, " RETURN");
    try append(a, &source, columns, false);
    const compiled = try cypher.compiler.compileInto(a, source.items, .{}, diagnostic);
    if (compiled.query.clauses.len != 2 or compiled.query.clauses[0].operation != .match or compiled.query.clauses[1].operation != .project) return error.InvalidSqlGraphTable;
    var query = compiled.query;
    const clauses = try a.dupe(cypher.ast.Clause, query.clauses);
    const projection = &clauses[1].operation.project.value;
    if (projection.wildcard) return error.InvalidSqlGraphTable;
    const items = try a.dupe(cypher.ast.Item, projection.items);
    for (items) |*item| {
        if (item.alias == null) item.alias = switch (item.expression.value) {
            .variable => |variable| variable,
            .property => |property| property.name,
            else => return error.SqlGraphColumnRequiresAlias,
        };
    }
    projection.items = items;
    query.clauses = clauses;
    return .{ .name = graph_name, .query = query, .parameters = compiled.parameters };
}
