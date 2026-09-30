// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Connection-owned SQL commands. Values are decoded into typed parameters,
//! never interpolated into the prepared statement's SQL text.
const std = @import("std");
const Type = @import("backend.zig").Type;
pub const Direction = enum { forward, backward, absolute, relative };
pub const Fetch = struct { name: []const u8, count: u32 = 1, direction: Direction = .forward, offset: i64 = 0, move: bool = false };
pub const Command = union(enum) {
    prepare: struct { name: []const u8, types: []const Type, statement: []const u8 },
    execute: struct { name: []const u8, expressions: []const []const u8 },
    deallocate: ?[]const u8,
    declare_cursor: struct { name: []const u8, statement: []const u8, scroll: bool = false, hold: bool = false },
    fetch_cursor: Fetch,
    close_cursor: ?[]const u8,
};

pub fn isCommit(input: []const u8) bool {
    var parser: Parser = .{ .alloc = undefined, .input = input };
    const verb = parser.word() catch return false;
    if (!std.ascii.eqlIgnoreCase(verb, "commit") and !std.ascii.eqlIgnoreCase(verb, "end")) return false;
    const saved = parser.pos;
    const modifier = parser.word() catch "";
    if (!std.ascii.eqlIgnoreCase(modifier, "work") and !std.ascii.eqlIgnoreCase(modifier, "transaction")) parser.pos = saved;
    parser.finish() catch return false;
    return true;
}

test "pgwire held-cursor precommit recognition requires the complete command" {
    try std.testing.expect(isCommit("COMMIT;"));
    try std.testing.expect(isCommit("END TRANSACTION"));
    try std.testing.expect(!isCommit("COMMIT; SELECT 1"));
    try std.testing.expect(!isCommit("COMMIT WORK extra"));
}

pub const Control = union(enum) { savepoint: []const u8, rollback_to: []const u8, release: []const u8 };
pub const TimeoutSetting = union(enum) { show, set: struct { local: bool, milliseconds: ?u32 }, reset };
pub const Namespace = @import("search_path.zig").Namespace;
pub const SearchPath = @import("search_path.zig").Path;
pub const SearchPathSetting = union(enum) { show, set: struct { local: bool, path: ?SearchPath }, reset };
pub const ApplicationName = struct {
    bytes: [128]u8 = @splat(0),
    len: u8 = 0,
    pub fn init(value: []const u8) !ApplicationName {
        if (value.len > 128 or !std.unicode.utf8ValidateSlice(value)) return error.InvalidParameter;
        for (value) |ch| if (std.ascii.isControl(ch)) return error.InvalidParameter;
        var result: ApplicationName = .{ .len = @intCast(value.len) };
        @memcpy(result.bytes[0..value.len], value);
        return result;
    }
    pub fn slice(self: *const ApplicationName) []const u8 {
        return self.bytes[0..self.len];
    }
};
pub const ApplicationNameSetting = union(enum) { show, set: struct { local: bool, value: ApplicationName }, reset };
pub const EncodingSetting = union(enum) { show, set: struct { local: bool }, reset };
pub const CatalogSetting = union(enum) { show: []const u8, set: struct { name: []const u8, value: []const u8, local: bool }, reset: []const u8, reset_local: []const u8 };
pub const Setting = union(enum) {
    search_path: SearchPathSetting,
    statement_timeout: TimeoutSetting,
    application_name: ApplicationNameSetting,
    client_encoding: EncodingSetting,
    catalog: CatalogSetting,
    reset_all,
    discard_all,
};

/// The protocol uses one classifier for Describe, Execute, and both streaming
/// paths. A locally owned setting must never be sent to the SQL read provider.
pub fn settingCommand(alloc: std.mem.Allocator, input: []const u8) !?Setting {
    if (try searchPathSetting(alloc, input)) |value| return .{ .search_path = value };
    if (try timeoutSetting(alloc, input)) |value| return .{ .statement_timeout = value };
    if (try applicationNameSetting(alloc, input)) |value| return .{ .application_name = value };
    if (try encodingSetting(alloc, input)) |value| return .{ .client_encoding = value };
    var p: Parser = .{ .alloc = alloc, .input = input };
    if (std.ascii.eqlIgnoreCase(p.word() catch return null, "reset")) {
        if (std.ascii.eqlIgnoreCase(p.word() catch return null, "all")) {
            try p.finish();
            return .reset_all;
        }
    }
    p.pos = 0;
    if (std.ascii.eqlIgnoreCase(p.word() catch return null, "discard")) {
        if (std.ascii.eqlIgnoreCase(p.word() catch return null, "all")) {
            try p.finish();
            return .discard_all;
        }
    }
    if (try catalogSetting(alloc, input)) |value| return .{ .catalog = value };
    return null;
}

/// Only dotted catalog-owned names enter the typed overlay. Built-in pgwire
/// compatibility settings retain their existing dedicated grammar and scope.
pub fn catalogSetting(alloc: std.mem.Allocator, input: []const u8) !?CatalogSetting {
    var p: Parser = .{ .alloc = alloc, .input = input };
    const verb = p.word() catch return null;
    const set = std.ascii.eqlIgnoreCase(verb, "set");
    const show = std.ascii.eqlIgnoreCase(verb, "show");
    const reset = std.ascii.eqlIgnoreCase(verb, "reset");
    if (!set and !show and !reset) return null;
    var local = false;
    if (set) {
        const saved = p.pos;
        const modifier = p.word() catch "";
        if (std.ascii.eqlIgnoreCase(modifier, "local")) local = true else if (!std.ascii.eqlIgnoreCase(modifier, "session")) p.pos = saved;
    }
    const name = p.settingName() catch return null;
    if (std.mem.indexOfScalar(u8, name, '.') == null) {
        alloc.free(name);
        return null;
    }
    if (show or reset) {
        try p.finish();
        return if (show) .{ .show = name } else .{ .reset = name };
    }
    if (!try p.take('=')) {
        const to = try p.word();
        if (!std.ascii.eqlIgnoreCase(to, "to")) return error.InvalidSqlSyntax;
    }
    try p.space();
    const quoted = p.pos < input.len and input[p.pos] == '\'';
    const value = if (quoted)
        try p.quoted('\'')
    else blk: {
        const start = p.pos;
        while (p.pos < input.len and !std.ascii.isWhitespace(input[p.pos]) and input[p.pos] != ';') p.pos += 1;
        if (p.pos == start) return error.InvalidSqlSyntax;
        break :blk input[start..p.pos];
    };
    try p.finish();
    if (!quoted and std.ascii.eqlIgnoreCase(value, "default")) return if (local) .{ .reset_local = name } else .{ .reset = name };
    return .{ .set = .{ .name = name, .value = value, .local = local } };
}

test "pgwire dotted catalog settings preserve quoted default and reject trailing SQL" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const literal = (try settingCommand(alloc, "SET app.mode = 'default'")).?.catalog.set;
    try std.testing.expectEqualStrings("app.mode", literal.name);
    try std.testing.expectEqualStrings("default", literal.value);
    try std.testing.expect(!literal.local);
    try std.testing.expect((try settingCommand(alloc, "SET LOCAL app.limit TO 4")).?.catalog.set.local);
    try std.testing.expect((try settingCommand(alloc, "SET app.limit = DEFAULT")).?.catalog == .reset);
    try std.testing.expect((try settingCommand(alloc, "SET LOCAL app.limit = DEFAULT")).?.catalog == .reset_local);
    try std.testing.expectError(error.InvalidSqlSyntax, settingCommand(alloc, "SHOW app.limit; SELECT 1"));
}

pub fn encodingSetting(alloc: std.mem.Allocator, input: []const u8) !?EncodingSetting {
    var p: Parser = .{ .alloc = alloc, .input = input };
    const verb = p.word() catch return null;
    const setting = std.ascii.eqlIgnoreCase(verb, "set");
    const showing = std.ascii.eqlIgnoreCase(verb, "show");
    const resetting = std.ascii.eqlIgnoreCase(verb, "reset");
    if (!setting and !showing and !resetting) return null;
    var name = p.word() catch return null;
    var local = false;
    if (setting and (std.ascii.eqlIgnoreCase(name, "local") or std.ascii.eqlIgnoreCase(name, "session"))) {
        local = std.ascii.eqlIgnoreCase(name, "local");
        name = try p.word();
    }
    const names_alias = std.ascii.eqlIgnoreCase(name, "names");
    if (!names_alias and !std.ascii.eqlIgnoreCase(name, "client_encoding")) return null;
    if (names_alias and !setting) return error.InvalidSqlSyntax;
    if (!setting) {
        try p.finish();
        return if (showing) .show else .reset;
    }
    if (names_alias) {
        const saved = p.pos;
        if (!std.ascii.eqlIgnoreCase(p.word() catch "", "to")) p.pos = saved;
    } else if (!try p.take('=')) if (!std.ascii.eqlIgnoreCase(try p.word(), "to")) return error.InvalidSqlSyntax;
    try p.space();
    const quoted = p.pos < input.len and (input[p.pos] == '\'' or input[p.pos] == '"');
    const value = if (quoted) try p.quoted(input[p.pos]) else try p.word();
    try p.finish();
    if (!std.ascii.eqlIgnoreCase(value, "default") or quoted) {
        if (!std.ascii.eqlIgnoreCase(value, "UTF8") and !std.ascii.eqlIgnoreCase(value, "UTF-8")) return error.UnsupportedEncoding;
    }
    return .{ .set = .{ .local = local } };
}

test "pgwire client encoding remains UTF-8 across SET SHOW and RESET" {
    // sql-0778: the old compiler rejected this session setting; pgwire now
    // accepts only the already-negotiated UTF-8 encoding.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    try std.testing.expect((try settingCommand(alloc, "SHOW client_encoding")).?.client_encoding == .show);
    try std.testing.expect((try settingCommand(alloc, "RESET client_encoding")).?.client_encoding == .reset);
    try std.testing.expect((try settingCommand(alloc, "SET client_encoding = 'UTF8';")).?.client_encoding == .set);
    try std.testing.expect((try settingCommand(alloc, "SET LOCAL client_encoding TO 'UTF-8'")).?.client_encoding.set.local);
    try std.testing.expect((try settingCommand(alloc, "SET NAMES 'UTF8'")).?.client_encoding == .set);
    try std.testing.expect((try settingCommand(alloc, "SET NAMES TO DEFAULT")).?.client_encoding == .set);
    try std.testing.expectError(error.UnsupportedEncoding, settingCommand(alloc, "SET NAMES 'LATIN1'"));
    try std.testing.expectError(error.UnsupportedEncoding, settingCommand(alloc, "SET client_encoding = 'LATIN1'"));
    try std.testing.expectError(error.InvalidSqlSyntax, settingCommand(alloc, "SHOW client_encoding; SELECT 1"));
}

test "pgwire RESET ALL is one complete connection setting command" {
    // sql-0045: local syntax coverage; custom app.* catalog parity is pending.
    const alloc = std.testing.allocator;
    try std.testing.expect((try settingCommand(alloc, "RESET ALL;")).? == .reset_all);
    try std.testing.expectError(error.InvalidSqlSyntax, settingCommand(alloc, "RESET ALL; SELECT 1"));
    try std.testing.expect(try settingCommand(alloc, "RESET CONSTRAINTS") == null);
}

test "pgwire DISCARD ALL requires one complete command" {
    // sql-0047: syntax and connection ownership only; original catalog scope
    // also includes custom settings and remains unresolved.
    const alloc = std.testing.allocator;
    try std.testing.expect((try settingCommand(alloc, "DISCARD ALL;")).? == .discard_all);
    try std.testing.expectError(error.InvalidSqlSyntax, settingCommand(alloc, "DISCARD ALL; SELECT 1"));
}

pub fn applicationNameSetting(alloc: std.mem.Allocator, input: []const u8) !?ApplicationNameSetting {
    var p: Parser = .{ .alloc = alloc, .input = input };
    const verb = p.word() catch return null;
    const setting = std.ascii.eqlIgnoreCase(verb, "set");
    const showing = std.ascii.eqlIgnoreCase(verb, "show");
    const resetting = std.ascii.eqlIgnoreCase(verb, "reset");
    if (!setting and !showing and !resetting) return null;
    var name = p.word() catch return null;
    var local = false;
    if (setting and (std.ascii.eqlIgnoreCase(name, "local") or std.ascii.eqlIgnoreCase(name, "session"))) {
        local = std.ascii.eqlIgnoreCase(name, "local");
        name = try p.word();
    }
    if (!std.ascii.eqlIgnoreCase(name, "application_name")) return null;
    if (!setting) {
        try p.finish();
        return if (showing) .show else .reset;
    }
    if (!try p.take('=')) if (!std.ascii.eqlIgnoreCase(try p.word(), "to")) return error.InvalidSqlSyntax;
    try p.space();
    const quoted = p.pos < input.len and (input[p.pos] == '\'' or input[p.pos] == '"');
    const value = if (quoted) try p.quoted(input[p.pos]) else try p.word();
    try p.finish();
    return .{ .set = .{ .local = local, .value = try ApplicationName.init(if (!quoted and std.ascii.eqlIgnoreCase(value, "default")) "" else value) } };
}

test "pgwire application name is bounded UTF-8 and consumes one statement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const value = (try applicationNameSetting(alloc, "SET LOCAL application_name TO 'sql worker'")).?.set;
    try std.testing.expect(value.local);
    try std.testing.expectEqualStrings("sql worker", value.value.slice());
    try std.testing.expectEqualStrings("", (try applicationNameSetting(alloc, "SET application_name = DEFAULT")).?.set.value.slice());
    try std.testing.expectError(error.InvalidParameter, applicationNameSetting(alloc, "SET application_name = 'bad\nname'"));
    try std.testing.expectError(error.InvalidSqlSyntax, applicationNameSetting(alloc, "SHOW application_name; SELECT 1"));
}
pub fn searchPathSetting(alloc: std.mem.Allocator, input: []const u8) !?SearchPathSetting {
    var p: Parser = .{ .alloc = alloc, .input = input };
    const verb = p.word() catch return null;
    const setting = std.ascii.eqlIgnoreCase(verb, "set");
    const showing = std.ascii.eqlIgnoreCase(verb, "show");
    const resetting = std.ascii.eqlIgnoreCase(verb, "reset");
    if (!setting and !showing and !resetting) return null;
    var name = p.word() catch return null;
    var local = false;
    if (setting and (std.ascii.eqlIgnoreCase(name, "local") or std.ascii.eqlIgnoreCase(name, "session"))) {
        local = std.ascii.eqlIgnoreCase(name, "local");
        name = try p.word();
    }
    if (!std.ascii.eqlIgnoreCase(name, "search_path")) return null;
    if (!setting) {
        try p.finish();
        return if (showing) .show else .reset;
    }
    if (!try p.take('=')) if (!std.ascii.eqlIgnoreCase(try p.word(), "to")) return error.InvalidSqlSyntax;
    try p.space();
    var path: SearchPath = .{};
    while (true) {
        const quoted = p.pos < input.len and (input[p.pos] == '\'' or input[p.pos] == '"');
        const value = if (quoted) try p.quoted(input[p.pos]) else try p.name();
        if (!quoted and std.ascii.eqlIgnoreCase(value, "default")) {
            if (path.len != 0 or try p.take(',')) return error.InvalidSqlSyntax;
            try p.finish();
            return .{ .set = .{ .local = local, .path = null } };
        }
        try path.append(try Namespace.init(value));
        if (!try p.take(',')) break;
    }
    try p.finish();
    return .{ .set = .{ .local = local, .path = path } };
}

test "pgwire search path accepts bounded ordered lookup lists" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const value = (try searchPathSetting(alloc, "SET LOCAL search_path TO 'tenant-west'")).?.set;
    try std.testing.expect(value.local);
    try std.testing.expectEqualStrings("tenant-west", value.path.?.first());
    try std.testing.expect((try searchPathSetting(alloc, "SET search_path = DEFAULT")).?.set.path == null);
    const path = (try searchPathSetting(alloc, "SET SESSION search_path TO tenant_schema, public;")).?.set.path.?;
    try std.testing.expectEqual(@as(u8, 2), path.len);
    try std.testing.expectEqualStrings("tenant_schema", path.entries[0].slice());
    try std.testing.expectEqualStrings("public", path.entries[1].slice());
    const duplicate = (try searchPathSetting(alloc, "SET search_path TO public, tenant_schema, public")).?.set.path.?;
    try std.testing.expectEqualStrings("public, tenant_schema, public", try duplicate.display(alloc));
    try std.testing.expectError(error.UnsupportedSqlShape, searchPathSetting(alloc, "SET search_path = 'public,tenant'"));
    try std.testing.expectError(error.InvalidParameter, searchPathSetting(alloc, "SET search_path = '$user'"));
    try std.testing.expectError(error.InvalidSqlSyntax, searchPathSetting(alloc, "RESET search_path; DROP TABLE t"));
}

/// Only implemented settings are consumed here. SET CONSTRAINTS remains a
/// native durable transaction command, never a connection-local no-op.
pub fn timeoutSetting(alloc: std.mem.Allocator, input: []const u8) !?TimeoutSetting {
    var p: Parser = .{ .alloc = alloc, .input = input };
    const verb = p.word() catch return null;
    const setting = std.ascii.eqlIgnoreCase(verb, "set");
    const showing = std.ascii.eqlIgnoreCase(verb, "show");
    const resetting = std.ascii.eqlIgnoreCase(verb, "reset");
    if (!setting and !showing and !resetting) return null;
    var name = p.word() catch return null;
    var local = false;
    if (setting and (std.ascii.eqlIgnoreCase(name, "local") or std.ascii.eqlIgnoreCase(name, "session"))) {
        local = std.ascii.eqlIgnoreCase(name, "local");
        name = try p.word();
    }
    if (!std.ascii.eqlIgnoreCase(name, "statement_timeout")) return null;
    if (!setting) {
        try p.finish();
        return if (showing) .show else .reset;
    }
    if (!try p.take('=')) if (!std.ascii.eqlIgnoreCase(try p.word(), "to")) return error.InvalidSqlSyntax;
    try p.space();
    const start = p.pos;
    const value = p.word() catch "";
    if (std.ascii.eqlIgnoreCase(value, "default")) {
        try p.finish();
        return .{ .set = .{ .local = local, .milliseconds = null } };
    }
    p.pos = start;
    var milliseconds: u32 = undefined;
    if (p.pos < input.len and input[p.pos] == '\'') {
        const text = try p.quoted('\'');
        var numeric: Parser = .{ .alloc = alloc, .input = text };
        milliseconds = try timeoutValue(&numeric);
        try numeric.finish();
    } else milliseconds = try timeoutValue(&p);
    try p.finish();
    return .{ .set = .{ .local = local, .milliseconds = milliseconds } };
}

fn timeoutValue(p: *Parser) !u32 {
    const value = (try p.number()) orelse return error.InvalidParameter;
    if (value < 0 or value > std.math.maxInt(u32)) return error.InvalidParameter;
    const unit = p.word() catch "";
    const multiplier: u32 = if (unit.len == 0 or std.ascii.eqlIgnoreCase(unit, "ms")) 1 else if (std.ascii.eqlIgnoreCase(unit, "s")) 1000 else if (std.ascii.eqlIgnoreCase(unit, "min")) 60000 else return error.InvalidParameter;
    return std.math.mul(u32, @intCast(value), multiplier) catch error.InvalidParameter;
}

test "pgwire timeout syntax is bounded and does not consume native constraint settings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    try std.testing.expectEqual(@as(?u32, 120000), (try timeoutSetting(alloc, "SET LOCAL statement_timeout TO '2min'")).?.set.milliseconds);
    try std.testing.expect((try timeoutSetting(alloc, "SET LOCAL statement_timeout = DEFAULT")).?.set.local);
    try std.testing.expect((try timeoutSetting(alloc, "SET statement_timeout = DEFAULT")).?.set.milliseconds == null);
    try std.testing.expect(try timeoutSetting(alloc, "SET CONSTRAINTS ALL IMMEDIATE") == null);
    try std.testing.expectError(error.InvalidParameter, timeoutSetting(alloc, "SET statement_timeout = -1"));
    try std.testing.expectError(error.InvalidParameter, timeoutSetting(alloc, "SET statement_timeout = '4294967295min'"));
    try std.testing.expectError(error.InvalidSqlSyntax, timeoutSetting(alloc, "SHOW statement_timeout; SELECT 1"));
}

pub fn control(alloc: std.mem.Allocator, input: []const u8) !?Control {
    var parser: Parser = .{ .alloc = alloc, .input = input };
    const verb = parser.word() catch return null;
    if (std.ascii.eqlIgnoreCase(verb, "savepoint")) {
        const name = try parser.name();
        try parser.finish();
        return .{ .savepoint = name };
    }
    const release = std.ascii.eqlIgnoreCase(verb, "release");
    if (!release and !std.ascii.eqlIgnoreCase(verb, "rollback")) return null;
    const next_start = parser.pos;
    var next = parser.word() catch if (release) "" else return null;
    if (!release) {
        if (std.ascii.eqlIgnoreCase(next, "work") or std.ascii.eqlIgnoreCase(next, "transaction")) next = parser.word() catch return null;
        if (!std.ascii.eqlIgnoreCase(next, "to")) return null;
        const saved = parser.pos;
        next = parser.word() catch "";
        if (!std.ascii.eqlIgnoreCase(next, "savepoint")) parser.pos = saved;
    } else if (!std.ascii.eqlIgnoreCase(next, "savepoint")) {
        parser.pos = next_start;
    }
    const name = try parser.name();
    try parser.finish();
    return if (release) .{ .release = name } else .{ .rollback_to = name };
}

test "pgwire savepoint control consumes one complete statement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    try std.testing.expectEqualStrings("point", (try control(alloc, "SAVEPOINT point; ")).?.savepoint);
    try std.testing.expectEqualStrings("point", (try control(alloc, "ROLLBACK TO SAVEPOINT point")).?.rollback_to);
    try std.testing.expectEqualStrings("point", (try control(alloc, "RELEASE point")).?.release);
    try std.testing.expectError(error.InvalidSqlSyntax, control(alloc, "SAVEPOINT point; SELECT 1"));
    try std.testing.expectError(error.InvalidSqlSyntax, control(alloc, "ROLLBACK TO point; COMMIT"));
    try std.testing.expectError(error.InvalidSqlSyntax, control(alloc, "RELEASE point; COMMIT"));
}

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
    fn settingName(self: *Parser) ![]const u8 {
        const start = self.pos;
        _ = try self.word();
        while (try self.take('.')) _ = try self.word();
        const result = try self.alloc.dupe(u8, std.mem.trim(u8, self.input[start..self.pos], " \t\r\n"));
        for (result) |*ch| ch.* = std.ascii.toLower(ch.*);
        return result;
    }
    fn finish(self: *Parser) !void {
        _ = try self.take(';');
        try self.space();
        if (self.pos != self.input.len) return error.InvalidSqlSyntax;
    }
    fn number(self: *Parser) !?i64 {
        try self.space();
        const start = self.pos;
        if (self.pos < self.input.len and (self.input[self.pos] == '-' or self.input[self.pos] == '+')) self.pos += 1;
        const digits = self.pos;
        while (self.pos < self.input.len and std.ascii.isDigit(self.input[self.pos])) self.pos += 1;
        if (digits == self.pos) {
            self.pos = start;
            return null;
        }
        return std.fmt.parseInt(i64, self.input[start..self.pos], 10) catch error.ProgramLimitExceeded;
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
    if (std.ascii.eqlIgnoreCase(verb, "declare")) {
        const name = try p.name();
        try p.space();
        if (p.pos == input.len or input[p.pos] == ';') return error.InvalidSqlSyntax;
        const modifier = try p.word();
        const scroll = std.ascii.eqlIgnoreCase(modifier, "scroll");
        if (std.ascii.eqlIgnoreCase(modifier, "no")) {
            if (!std.ascii.eqlIgnoreCase(try p.word(), "scroll")) return error.InvalidSqlSyntax;
        } else if (!scroll and !std.ascii.eqlIgnoreCase(modifier, "cursor")) return error.InvalidSqlSyntax;
        if ((scroll or std.ascii.eqlIgnoreCase(modifier, "no")) and !std.ascii.eqlIgnoreCase(try p.word(), "cursor")) return error.InvalidSqlSyntax;
        var following = try p.word();
        var hold = false;
        if (std.ascii.eqlIgnoreCase(following, "with") or std.ascii.eqlIgnoreCase(following, "without")) {
            hold = std.ascii.eqlIgnoreCase(following, "with");
            if (!std.ascii.eqlIgnoreCase(try p.word(), "hold")) return error.InvalidSqlSyntax;
            following = try p.word();
        }
        // The statement body owns the remainder; verify FOR is the next token
        // and let the SQL compiler validate the full SELECT shape.
        if (!std.ascii.eqlIgnoreCase(following, "for")) return error.InvalidSqlSyntax;
        try p.space();
        if (p.pos == input.len) return error.InvalidSqlSyntax;
        const statement = std.mem.trim(u8, input[p.pos..], " \r\n\t;");
        if (statement.len == 0) return error.InvalidSqlSyntax;
        return .{ .declare_cursor = .{ .name = name, .statement = statement, .scroll = scroll, .hold = hold } };
    }
    if (std.ascii.eqlIgnoreCase(verb, "fetch") or std.ascii.eqlIgnoreCase(verb, "move")) {
        var fetch: Fetch = .{ .name = "", .move = std.ascii.eqlIgnoreCase(verb, "move") };
        var count: ?i64 = try p.number();
        if (count == null) {
            const start = p.pos;
            const direction = p.word() catch "";
            if (std.ascii.eqlIgnoreCase(direction, "all")) fetch.count = std.math.maxInt(u32) else if (std.ascii.eqlIgnoreCase(direction, "next")) {} else if (std.ascii.eqlIgnoreCase(direction, "prior")) fetch.direction = .backward else if (std.ascii.eqlIgnoreCase(direction, "first") or std.ascii.eqlIgnoreCase(direction, "last")) {
                fetch.direction = .absolute;
                fetch.offset = if (std.ascii.eqlIgnoreCase(direction, "first")) 1 else -1;
            } else if (std.ascii.eqlIgnoreCase(direction, "absolute") or std.ascii.eqlIgnoreCase(direction, "relative")) {
                fetch.direction = if (std.ascii.eqlIgnoreCase(direction, "absolute")) .absolute else .relative;
                fetch.offset = (try p.number()) orelse return error.InvalidSqlSyntax;
            } else if (std.ascii.eqlIgnoreCase(direction, "forward") or std.ascii.eqlIgnoreCase(direction, "backward")) {
                fetch.direction = if (std.ascii.eqlIgnoreCase(direction, "forward")) .forward else .backward;
                const saved = p.pos;
                const all = p.word() catch "";
                if (std.ascii.eqlIgnoreCase(all, "all")) fetch.count = std.math.maxInt(u32) else {
                    p.pos = saved;
                    count = try p.number();
                }
            } else p.pos = start;
        }
        if (count) |n| {
            if (n < 0) fetch.direction = if (fetch.direction == .backward) .forward else .backward;
            fetch.count = std.math.cast(u32, @abs(n)) orelse return error.ProgramLimitExceeded;
        }
        const saved = p.pos;
        const source = p.word() catch "";
        if (!std.ascii.eqlIgnoreCase(source, "from") and !std.ascii.eqlIgnoreCase(source, "in")) p.pos = saved;
        fetch.name = try p.name();
        try p.finish();
        return .{ .fetch_cursor = fetch };
    }
    if (std.ascii.eqlIgnoreCase(verb, "close")) {
        try p.space();
        const quoted_name = p.pos < input.len and input[p.pos] == '"';
        const name = try p.name();
        const all = !quoted_name and std.ascii.eqlIgnoreCase(name, "all");
        try p.finish();
        return .{ .close_cursor = if (all) null else name };
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
            const declared = (try parse(a, "DECLARE c CURSOR FOR SELECT id FROM users", 2)).?.declare_cursor;
            try std.testing.expectEqualStrings("c", declared.name);
            try std.testing.expectEqualStrings("SELECT id FROM users", declared.statement);
            const quoted = (try parse(a, "DECLARE \"Mixed\" NO SCROLL CURSOR FOR SELECT 1", 2)).?.declare_cursor;
            try std.testing.expectEqualStrings("Mixed", quoted.name);
            const fetched = (try parse(a, "FETCH FORWARD 10 FROM c", 2)).?.fetch_cursor;
            try std.testing.expectEqualStrings("c", fetched.name);
            try std.testing.expectEqual(@as(u32, 10), fetched.count);
            try std.testing.expectEqual(@as(u32, 1), (try parse(a, "FETCH NEXT FROM c", 2)).?.fetch_cursor.count);
            try std.testing.expectEqual(std.math.maxInt(u32), (try parse(a, "FETCH FORWARD ALL FROM c", 2)).?.fetch_cursor.count);
            try std.testing.expectEqual(std.math.maxInt(u32), (try parse(a, "FETCH ALL IN c", 2)).?.fetch_cursor.count);
            try std.testing.expect((try parse(a, "CLOSE ALL", 2)).?.close_cursor == null);
            try std.testing.expectEqualStrings("ALL", (try parse(a, "CLOSE \"ALL\"", 2)).?.close_cursor.?);
            try std.testing.expect((try parse(a, "DECLARE c SCROLL CURSOR FOR SELECT 1", 2)).?.declare_cursor.scroll);
            try std.testing.expect((try parse(a, "DECLARE c CURSOR WITH HOLD FOR SELECT 1", 2)).?.declare_cursor.hold);
            try std.testing.expectEqual(@as(i64, -2), (try parse(a, "FETCH ABSOLUTE -2 FROM c", 2)).?.fetch_cursor.offset);
            try std.testing.expectEqual(Direction.backward, (try parse(a, "FETCH PRIOR c", 2)).?.fetch_cursor.direction);
            try std.testing.expect((try parse(a, "MOVE BACKWARD ALL c", 2)).?.fetch_cursor.move);
            try std.testing.expectError(error.InvalidSqlSyntax, parse(a, "EXECUTE x('unterminated)", 2));
            try std.testing.expectError(error.ProgramLimitExceeded, parse(a, "EXECUTE x(1,2,3)", 2));
            try std.testing.expectError(error.InvalidSqlSyntax, parse(a, "DEALLOCATE x; DROP TABLE users", 2));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
