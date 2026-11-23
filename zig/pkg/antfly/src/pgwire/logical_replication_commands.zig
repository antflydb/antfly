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

//! Logical walsender simple-query commands. Parsing does not authenticate a
//! replication principal, create a slot, export a snapshot or acknowledge WAL.
const std = @import("std");
const publisher = @import("antfly_local_sources").system_catalog_logical_replication_publisher;
const A = std.mem.Allocator;
pub const max_command_bytes = 64 * 1024;
pub const Create = struct { slot: []const u8, temporary: bool = false, snapshot: publisher.SnapshotMode = .export_snapshot };
pub const Start = struct { slot: []const u8, lsn: u64, publication_names: []const u8, origin: @import("antfly_local_sources").system_catalog_logical_replication.Origin = .any };
pub const Command = union(enum) {
    identify_system,
    create_slot: Create,
    drop_slot: struct { slot: []const u8, wait: bool },
    start: Start,
};
const Token = struct { text: []const u8, quoted: bool = false };
const Scanner = struct {
    a: A,
    text: []const u8,
    offset: usize = 0,
    fn whitespace(self: *@This()) void {
        while (self.offset < self.text.len and std.ascii.isWhitespace(self.text[self.offset])) self.offset += 1;
    }
    fn punctuation(self: *@This(), byte: u8) bool {
        self.whitespace();
        if (self.offset == self.text.len or self.text[self.offset] != byte) return false;
        self.offset += 1;
        return true;
    }
    fn token(self: *@This()) !Token {
        self.whitespace();
        if (self.offset == self.text.len) return error.InvalidSqlSyntax;
        const quote = self.text[self.offset];
        if (quote == '\'' or quote == '"') {
            self.offset += 1;
            var bytes: std.ArrayList(u8) = .empty;
            while (self.offset < self.text.len) {
                const current = self.text[self.offset];
                self.offset += 1;
                if (current == quote) {
                    if (self.offset < self.text.len and self.text[self.offset] == quote) {
                        self.offset += 1;
                        try bytes.append(self.a, quote);
                        continue;
                    }
                    return .{ .text = try bytes.toOwnedSlice(self.a), .quoted = true };
                }
                try bytes.append(self.a, current);
            }
            return error.InvalidSqlSyntax;
        }
        const begin = self.offset;
        while (self.offset < self.text.len) {
            const byte = self.text[self.offset];
            if (std.ascii.isWhitespace(byte) or byte == '(' or byte == ')' or byte == ',' or byte == ';') break;
            self.offset += 1;
        }
        if (self.offset == begin) return error.InvalidSqlSyntax;
        return .{ .text = self.text[begin..self.offset] };
    }
    fn word(self: *@This(), expected: []const u8) !void {
        const value = try self.token();
        if (value.quoted or !std.ascii.eqlIgnoreCase(value.text, expected)) return error.InvalidSqlSyntax;
    }
    fn takeWord(self: *@This(), expected: []const u8) !bool {
        const saved = self.offset;
        self.whitespace();
        if (self.offset == self.text.len or self.text[self.offset] == '(' or self.text[self.offset] == ')' or self.text[self.offset] == ';') {
            self.offset = saved;
            return false;
        }
        const value = try self.token();
        if (!value.quoted and std.ascii.eqlIgnoreCase(value.text, expected)) return true;
        self.offset = saved;
        return false;
    }
    fn slot(self: *@This()) ![]const u8 {
        const value = try self.token();
        const name = try self.a.dupe(u8, value.text);
        if (!value.quoted) {
            for (name) |*byte| byte.* = std.ascii.toLower(byte.*);
        }
        try publisher.validateSlotName(name);
        return name;
    }
    fn finish(self: *@This()) !void {
        _ = self.punctuation(';');
        self.whitespace();
        if (self.offset != self.text.len) return error.InvalidSqlSyntax;
    }
};
fn boolean(value: []const u8) !bool {
    for ([_][]const u8{ "true", "on", "yes", "1" }) |item| if (std.ascii.eqlIgnoreCase(value, item)) return true;
    for ([_][]const u8{ "false", "off", "no", "0" }) |item| if (std.ascii.eqlIgnoreCase(value, item)) return false;
    return error.InvalidSqlSyntax;
}
pub fn parseLsn(text: []const u8) !u64 {
    const slash = std.mem.indexOfScalar(u8, text, '/') orelse return error.InvalidSqlSyntax;
    if (slash == 0 or slash > 8 or text.len - slash - 1 == 0 or text.len - slash - 1 > 8) return error.InvalidSqlSyntax;
    for (text[0..slash]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidSqlSyntax;
    for (text[slash + 1 ..]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidSqlSyntax;
    return (@as(u64, try std.fmt.parseInt(u32, text[0..slash], 16)) << 32) | try std.fmt.parseInt(u32, text[slash + 1 ..], 16);
}
pub fn formatLsn(buffer: []u8, value: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{X}/{X}", .{ @as(u32, @truncate(value >> 32)), @as(u32, @truncate(value)) });
}

/// Null means ordinary SQL, which remains valid on a logical replication
/// connection. Physical walsender/unsupported capability commands are refused
/// explicitly, before calling a backend or changing durable state.
pub fn parse(a: A, text: []const u8) !?Command {
    if (text.len > max_command_bytes) return error.SqlProgramLimitExceeded;
    if (std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidSqlSyntax;
    var scan: Scanner = .{ .a = a, .text = text };
    scan.whitespace();
    if (scan.offset == text.len) return null;
    const first = try scan.token();
    if (first.quoted) return null;
    if (std.ascii.eqlIgnoreCase(first.text, "IDENTIFY_SYSTEM")) {
        try scan.finish();
        return .identify_system;
    }
    if (std.ascii.eqlIgnoreCase(first.text, "READ_REPLICATION_SLOT") or std.ascii.eqlIgnoreCase(first.text, "TIMELINE_HISTORY") or std.ascii.eqlIgnoreCase(first.text, "BASE_BACKUP") or std.ascii.eqlIgnoreCase(first.text, "ALTER_REPLICATION_SLOT")) return error.UnsupportedSqlShape;
    if (std.ascii.eqlIgnoreCase(first.text, "DROP_REPLICATION_SLOT")) {
        const slot = try scan.slot();
        const wait = try scan.takeWord("WAIT");
        try scan.finish();
        return .{ .drop_slot = .{ .slot = slot, .wait = wait } };
    }
    if (std.ascii.eqlIgnoreCase(first.text, "CREATE_REPLICATION_SLOT")) {
        var result: Create = .{ .slot = try scan.slot() };
        result.temporary = try scan.takeWord("TEMPORARY");
        if (try scan.takeWord("PHYSICAL")) return error.UnsupportedSqlShape;
        try scan.word("LOGICAL");
        const plugin = try scan.token();
        if (!std.mem.eql(u8, plugin.text, "pgoutput")) return error.UnsupportedSqlShape;
        if (scan.punctuation('(')) {
            var seen: u8 = 0;
            while (true) {
                const key = try scan.token();
                const bit: u8 = if (std.ascii.eqlIgnoreCase(key.text, "SNAPSHOT")) 1 else if (std.ascii.eqlIgnoreCase(key.text, "TWO_PHASE")) 2 else if (std.ascii.eqlIgnoreCase(key.text, "FAILOVER")) 4 else return error.UnsupportedSqlShape;
                if (seen & bit != 0) return error.InvalidSqlSyntax;
                seen |= bit;
                if (bit == 1) {
                    const value = try scan.token();
                    result.snapshot = if (std.ascii.eqlIgnoreCase(value.text, "export")) .export_snapshot else if (std.ascii.eqlIgnoreCase(value.text, "use")) .use_snapshot else if (std.ascii.eqlIgnoreCase(value.text, "nothing")) .nothing else return error.InvalidSqlSyntax;
                } else {
                    scan.whitespace();
                    const enabled = if (scan.offset < scan.text.len and (scan.text[scan.offset] == ',' or scan.text[scan.offset] == ')')) true else try boolean((try scan.token()).text);
                    if (enabled) return error.UnsupportedSqlShape;
                }
                if (scan.punctuation(')')) break;
                if (!scan.punctuation(',')) return error.InvalidSqlSyntax;
            }
        } else if (try scan.takeWord("EXPORT_SNAPSHOT")) result.snapshot = .export_snapshot else if (try scan.takeWord("NOEXPORT_SNAPSHOT")) result.snapshot = .nothing else if (try scan.takeWord("USE_SNAPSHOT")) result.snapshot = .use_snapshot else if (try scan.takeWord("TWO_PHASE")) return error.UnsupportedSqlShape;
        try scan.finish();
        return .{ .create_slot = result };
    }
    if (std.ascii.eqlIgnoreCase(first.text, "START_REPLICATION")) {
        try scan.word("SLOT");
        const slot = try scan.slot();
        try scan.word("LOGICAL");
        const lsn = try parseLsn((try scan.token()).text);
        if (!scan.punctuation('(')) return error.InvalidSqlSyntax;
        var version: ?u32 = null;
        var publications: ?[]const u8 = null;
        var origin: @FieldType(Start, "origin") = .any;
        var seen: u8 = 0;
        while (true) {
            const key = try scan.token();
            const bit: u8 = if (std.ascii.eqlIgnoreCase(key.text, "proto_version")) 1 else if (std.ascii.eqlIgnoreCase(key.text, "publication_names")) 2 else if (std.ascii.eqlIgnoreCase(key.text, "origin")) 4 else if (std.ascii.eqlIgnoreCase(key.text, "binary")) 8 else if (std.ascii.eqlIgnoreCase(key.text, "streaming")) 16 else if (std.ascii.eqlIgnoreCase(key.text, "two_phase")) 32 else if (std.ascii.eqlIgnoreCase(key.text, "messages")) 64 else return error.UnsupportedSqlShape;
            if (seen & bit != 0) return error.InvalidSqlSyntax;
            seen |= bit;
            const value = try scan.token();
            switch (bit) {
                1 => {
                    version = std.fmt.parseInt(u32, value.text, 10) catch return error.InvalidSqlSyntax;
                    if (version.? != 1) return error.UnsupportedSqlShape;
                },
                2 => {
                    if (value.text.len == 0) return error.InvalidSqlSyntax;
                    publications = value.text;
                },
                4 => {
                    origin = if (std.ascii.eqlIgnoreCase(value.text, "any")) .any else if (std.ascii.eqlIgnoreCase(value.text, "none")) .none else return error.InvalidSqlSyntax;
                },
                else => if (try boolean(value.text)) return error.UnsupportedSqlShape,
            }
            if (scan.punctuation(')')) break;
            if (!scan.punctuation(',')) return error.InvalidSqlSyntax;
        }
        try scan.finish();
        if (version == null or publications == null) return error.InvalidSqlSyntax;
        return .{ .start = .{ .slot = slot, .lsn = lsn, .publication_names = publications.?, .origin = origin } };
    }
    return null;
}

test "logical walsender commands negotiate real v1 and refuse unimplemented decoding before effects" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const start = (try parse(a, "START_REPLICATION SLOT subscriber LOGICAL 1/ABC (proto_version '1', publication_names 'pub_a, pub_b', binary 'false', streaming 'false', origin 'none');")).?.start;
    try std.testing.expectEqual(@as(u64, 0x100000ABC), start.lsn);
    try std.testing.expectEqualStrings("pub_a, pub_b", start.publication_names);
    try std.testing.expectEqual(@FieldType(Start, "origin").none, start.origin);
    try std.testing.expectError(error.UnsupportedSqlShape, parse(a, "START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '2', publication_names 'p')"));
    try std.testing.expectError(error.UnsupportedSqlShape, parse(a, "CREATE_REPLICATION_SLOT s LOGICAL pgoutput (TWO_PHASE true)"));
    try std.testing.expectError(error.UnsupportedSqlShape, parse(a, "CREATE_REPLICATION_SLOT s LOGICAL pgoutput (FAILOVER true)"));
    const created = (try parse(a, "CREATE_REPLICATION_SLOT s TEMPORARY LOGICAL pgoutput (SNAPSHOT 'nothing', TWO_PHASE false, FAILOVER false)")).?.create_slot;
    try std.testing.expect(created.temporary);
    try std.testing.expectEqual(publisher.SnapshotMode.nothing, created.snapshot);
    try std.testing.expect((try parse(a, "SELECT pubname FROM pg_catalog.pg_publication")) == null);
    try std.testing.expectError(error.InvalidSqlSyntax, parse(a, "IDENTIFY_SYSTEM; DROP_REPLICATION_SLOT s"));
}

/// PostgreSQL's publication_names option is an identifier list inside a string,
/// not a comma-split list of raw catalog names. Own every decoded identifier.
pub fn publicationNames(a: A, text: []const u8) ![]const []const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    var offset: usize = 0;
    while (true) {
        while (offset < text.len and std.ascii.isWhitespace(text[offset])) offset += 1;
        if (offset == text.len) return error.InvalidSqlSyntax;
        var name: std.ArrayList(u8) = .empty;
        if (text[offset] == '"') {
            offset += 1;
            while (true) {
                if (offset == text.len) return error.InvalidSqlSyntax;
                const byte = text[offset];
                offset += 1;
                if (byte == '"') {
                    if (offset == text.len or text[offset] != '"') break;
                    offset += 1;
                }
                try name.append(a, byte);
            }
        } else {
            if (!std.ascii.isAlphabetic(text[offset]) and text[offset] != '_' and text[offset] < 128) return error.InvalidSqlSyntax;
            while (offset < text.len) : (offset += 1) {
                const byte = text[offset];
                if (!std.ascii.isAlphanumeric(byte) and byte != '_' and byte != '$' and byte < 128) break;
                try name.append(a, std.ascii.toLower(byte));
            }
        }
        const actual = try name.toOwnedSlice(a);
        if (actual.len == 0 or actual.len > 256 or std.mem.indexOfScalar(u8, actual, 0) != null or !std.unicode.utf8ValidateSlice(actual)) return error.InvalidSqlSyntax;
        if (result.items.len == publisher.max_publications) return error.SqlProgramLimitExceeded;
        // PostgreSQL treats repeated names as the same publication membership.
        const duplicate = for (result.items) |previous| {
            if (std.mem.eql(u8, previous, actual)) break true;
        } else false;
        if (!duplicate) try result.append(a, actual) else a.free(actual);
        while (offset < text.len and std.ascii.isWhitespace(text[offset])) offset += 1;
        if (offset == text.len) break;
        if (text[offset] != ',') return error.InvalidSqlSyntax;
        offset += 1;
    }
    return result.toOwnedSlice(a);
}

test "pgwire logical walsender publication identifier lists preserve quoting and reject malformed names" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const names = try publicationNames(a, " First, \"Mixed,Name\", first, \"a\"\"b\" ");
    try std.testing.expectEqual(@as(usize, 3), names.len);
    try std.testing.expectEqualStrings("first", names[0]);
    try std.testing.expectEqualStrings("Mixed,Name", names[1]);
    try std.testing.expectEqualStrings("a\"b", names[2]);
    for ([_][]const u8{ "", "p,", ",p", "p,,q", "p.q", "p q", "1p", "\"unfinished" }) |bad|
        try std.testing.expectError(error.InvalidSqlSyntax, publicationNames(a, bad));
}
