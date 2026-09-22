// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Connection-owned SQL commands. Values are decoded into typed parameters,
//! never interpolated into the prepared statement's SQL text.
const std = @import("std");
const Type = @import("backend.zig").Type;
pub const Command = union(enum) {
    prepare: struct { name: []const u8, types: []const Type, statement: []const u8 },
    execute: struct { name: []const u8, expressions: []const []const u8 },
    deallocate: ?[]const u8,
};

const Parser = struct {
    alloc: std.mem.Allocator,
    input: []const u8,
    pos: usize = 0,

    fn space(self: *Parser) !void {
        while (self.pos < self.input.len) {
            if (std.ascii.isWhitespace(self.input[self.pos])) {
                self.pos += 1;
            } else if (std.mem.startsWith(u8, self.input[self.pos..], "--")) {
                while (self.pos < self.input.len and self.input[self.pos] != '\n') self.pos += 1;
            } else if (std.mem.startsWith(u8, self.input[self.pos..], "/*")) {
                self.pos += 2;
                var depth: usize = 1;
                while (depth != 0) {
                    if (self.pos == self.input.len) return error.InvalidSqlSyntax;
                    if (std.mem.startsWith(u8, self.input[self.pos..], "/*")) {
                        depth += 1;
                        self.pos += 2;
                    } else if (std.mem.startsWith(u8, self.input[self.pos..], "*/")) {
                        depth -= 1;
                        self.pos += 2;
                    } else self.pos += 1;
                }
            } else break;
        }
    }
    fn take(self: *Parser, ch: u8) !bool {
        try self.space();
        if (self.pos == self.input.len or self.input[self.pos] != ch) return false;
        self.pos += 1;
        return true;
    }
    fn word(self: *Parser) ![]const u8 {
        try self.space();
        const start = self.pos;
        if (self.pos == self.input.len or !(std.ascii.isAlphabetic(self.input[self.pos]) or self.input[self.pos] == '_')) return error.InvalidSqlSyntax;
        self.pos += 1;
        while (self.pos < self.input.len and (std.ascii.isAlphanumeric(self.input[self.pos]) or self.input[self.pos] == '_' or self.input[self.pos] == '$')) self.pos += 1;
        return self.input[start..self.pos];
    }
    fn quoted(self: *Parser, quote: u8) ![]const u8 {
        if (!try self.take(quote)) return error.InvalidSqlSyntax;
        var out: std.ArrayList(u8) = .empty;
        while (self.pos < self.input.len) {
            const ch = self.input[self.pos];
            self.pos += 1;
            if (ch == quote) {
                if (self.pos < self.input.len and self.input[self.pos] == quote) {
                    self.pos += 1;
                } else return out.toOwnedSlice(self.alloc);
            }
            try out.append(self.alloc, ch);
        }
        return error.InvalidSqlSyntax;
    }
    fn name(self: *Parser) ![]const u8 {
        try self.space();
        if (self.pos < self.input.len and self.input[self.pos] == '"') {
            const result = try self.quoted('"');
            if (result.len == 0) return error.InvalidSqlSyntax;
            return result;
        }
        const result = try self.alloc.dupe(u8, try self.word());
        for (result) |*ch| ch.* = std.ascii.toLower(ch.*);
        return result;
    }
    fn finish(self: *Parser) !void {
        _ = try self.take(';');
        try self.space();
        if (self.pos != self.input.len) return error.InvalidSqlSyntax;
    }
    fn expression(self: *Parser) ![]const u8 {
        try self.space();
        const start = self.pos;
        var depth: usize = 0;
        while (self.pos < self.input.len) {
            const ch = self.input[self.pos];
            if (ch == '\'' or ch == '"') {
                self.pos += 1;
                while (true) {
                    if (self.pos == self.input.len) return error.InvalidSqlSyntax;
                    const current = self.input[self.pos];
                    self.pos += 1;
                    if (current == ch) {
                        if (self.pos < self.input.len and self.input[self.pos] == ch) self.pos += 1 else break;
                    }
                }
            } else if (std.mem.startsWith(u8, self.input[self.pos..], "--") or std.mem.startsWith(u8, self.input[self.pos..], "/*")) {
                try self.space();
            } else if (ch == '(' or ch == '[') {
                depth += 1;
                self.pos += 1;
            } else if (ch == ')' or ch == ']') {
                if (depth == 0) break;
                depth -= 1;
                self.pos += 1;
            } else if (ch == ',' and depth == 0) break else self.pos += 1;
        }
        const result = std.mem.trim(u8, self.input[start..self.pos], " \r\n\t");
        if (result.len == 0 or depth != 0) return error.InvalidSqlSyntax;
        return result;
    }
};

pub fn parse(alloc: std.mem.Allocator, input: []const u8, max_parameters: usize) !?Command {
    var p: Parser = .{ .alloc = alloc, .input = input };
    const verb = p.word() catch return null;
    if (std.ascii.eqlIgnoreCase(verb, "prepare")) {
        const name = try p.name();
        var types: std.ArrayList(Type) = .empty;
        if (try p.take('(')) {
            while (true) {
                if (types.items.len == max_parameters) return error.ProgramLimitExceeded;
                var type_name = try p.word();
                if (std.ascii.eqlIgnoreCase(type_name, "double")) {
                    if (!std.ascii.eqlIgnoreCase(try p.word(), "precision")) return error.UnsupportedParameterType;
                    type_name = "float8";
                } else if (std.ascii.eqlIgnoreCase(type_name, "int2") or std.ascii.eqlIgnoreCase(type_name, "int4") or std.ascii.eqlIgnoreCase(type_name, "smallint")) {
                    type_name = "integer";
                } else if (std.ascii.eqlIgnoreCase(type_name, "decimal") or std.ascii.eqlIgnoreCase(type_name, "float4")) {
                    type_name = "numeric";
                }
                const kind: Type = if (std.ascii.eqlIgnoreCase(type_name, "integer") or std.ascii.eqlIgnoreCase(type_name, "int") or std.ascii.eqlIgnoreCase(type_name, "bigint") or std.ascii.eqlIgnoreCase(type_name, "int8")) .integer else if (std.ascii.eqlIgnoreCase(type_name, "text") or std.ascii.eqlIgnoreCase(type_name, "varchar")) .string else if (std.ascii.eqlIgnoreCase(type_name, "boolean") or std.ascii.eqlIgnoreCase(type_name, "bool")) .boolean else if (std.ascii.eqlIgnoreCase(type_name, "json") or std.ascii.eqlIgnoreCase(type_name, "jsonb")) .json else if (std.ascii.eqlIgnoreCase(type_name, "timestamptz")) .datetime else if (std.ascii.eqlIgnoreCase(type_name, "numeric") or std.ascii.eqlIgnoreCase(type_name, "real") or std.ascii.eqlIgnoreCase(type_name, "float8")) .number else return error.UnsupportedParameterType;
                try types.append(alloc, kind);
                if (try p.take(')')) break;
                if (!try p.take(',')) return error.InvalidSqlSyntax;
            }
        }
        if (!std.ascii.eqlIgnoreCase(try p.word(), "as")) return error.InvalidSqlSyntax;
        try p.space();
        if (p.pos == input.len) return error.InvalidSqlSyntax;
        return .{ .prepare = .{ .name = name, .types = try types.toOwnedSlice(alloc), .statement = input[p.pos..] } };
    }
    if (std.ascii.eqlIgnoreCase(verb, "execute")) {
        const name = try p.name();
        var parameters: std.ArrayList([]const u8) = .empty;
        if (try p.take('(')) {
            if (!try p.take(')')) while (true) {
                if (parameters.items.len == max_parameters) return error.ProgramLimitExceeded;
                try parameters.append(alloc, try p.expression());
                if (try p.take(')')) break;
                if (!try p.take(',')) return error.UnsupportedSqlExecution;
            };
        }
        try p.finish();
        return .{ .execute = .{ .name = name, .expressions = try parameters.toOwnedSlice(alloc) } };
    }
    if (std.ascii.eqlIgnoreCase(verb, "deallocate")) {
        const saved = p.pos;
        const keyword = p.word() catch "";
        if (!std.ascii.eqlIgnoreCase(keyword, "prepare")) p.pos = saved;
        try p.space();
        const quoted_name = p.pos < input.len and input[p.pos] == '"';
        const name = try p.name();
        try p.finish();
        return .{ .deallocate = if (!quoted_name and std.ascii.eqlIgnoreCase(name, "all")) null else name };
    }
    return null;
}

test "pgwire SQL session command parser preserves scalar spans and quoted names" {
    const Case = struct {
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const prepared = (try parse(a, "/* head */ PREPARE \"Mixed\" (bigint, text) AS SELECT $1, $2", 2)).?.prepare;
            try std.testing.expectEqualStrings("Mixed", prepared.name);
            try std.testing.expectEqualSlices(Type, &.{ .integer, .string }, prepared.types);
            try std.testing.expectEqualStrings("SELECT $1, $2", prepared.statement);
            const executed = (try parse(a, "EXECUTE \"Mixed\"(1 + (2 * 3), concat('a,b', /* , ) */ 'c''d')); --tail", 2)).?.execute;
            try std.testing.expectEqual(@as(usize, 2), executed.expressions.len);
            try std.testing.expectEqualStrings("1 + (2 * 3)", executed.expressions[0]);
            try std.testing.expectEqualStrings("concat('a,b', /* , ) */ 'c''d')", executed.expressions[1]);
            try std.testing.expectEqualStrings("ALL", (try parse(a, "DEALLOCATE \"ALL\"", 2)).?.deallocate.?);
            try std.testing.expect((try parse(a, "DEALLOCATE PREPARE ALL", 2)).?.deallocate == null);
            try std.testing.expectError(error.InvalidSqlSyntax, parse(a, "EXECUTE x('unterminated)", 2));
            try std.testing.expectError(error.ProgramLimitExceeded, parse(a, "EXECUTE x(1,2,3)", 2));
            try std.testing.expectError(error.InvalidSqlSyntax, parse(a, "DEALLOCATE x; DROP TABLE users", 2));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
