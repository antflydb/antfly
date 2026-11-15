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

//! Bounded PostgreSQL logical replication command grammar and pgoutput-v1
//! option negotiation. Parsing grants no access and makes no slot mutations.
//! The authenticated backend must qualify snapshot and ownership capabilities.
//! PostgreSQL 18 reference: protocol-replication.html and repl_gram.y.
const std = @import("std");
const A = std.mem.Allocator;
pub const Lsn = @import("pgoutput.zig").Lsn;
pub const max_command_bytes = 64 * 1024;
pub const max_publications = 64;
pub const Snapshot = enum { export_snapshot, use_snapshot, nothing };
pub const Create = struct { slot: []const u8, temporary: bool = false, snapshot: Snapshot = .export_snapshot };
pub const Start = struct { slot: []const u8, position: Lsn, publications: []const []const u8 };
pub const Command = union(enum) { identify_system, create_slot: Create, drop_slot: struct { slot: []const u8, wait: bool }, start: Start };
pub const Parsed = struct {
    arena: std.heap.ArenaAllocator,
    command: Command,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
const Kind = enum { atom, identifier, string, open, close, comma, semicolon, end };
const Token = struct { kind: Kind, raw: []const u8 = "" };
fn space(c: u8) bool {
    return std.mem.indexOfScalar(u8, " \t\r\n\x0b\x0c", c) != null;
}
fn identFirst(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_' or c >= 128;
}
fn identRest(c: u8) bool {
    return identFirst(c) or std.ascii.isDigit(c) or c == '$';
}
fn identifier(raw: []const u8) bool {
    if (raw.len == 0 or !identFirst(raw[0])) return false;
    for (raw[1..]) |c| if (!identRest(c)) return false;
    return true;
}
const Parser = struct {
    a: A,
    input: []const u8,
    offset: usize = 0,
    look: ?Token = null,
    fn next(self: *Parser) !Token {
        if (self.look) |token| {
            self.look = null;
            return token;
        }
        while (self.offset < self.input.len and space(self.input[self.offset])) self.offset += 1;
        if (self.offset == self.input.len) return .{ .kind = .end };
        const token_start = self.offset;
        const c = self.input[self.offset];
        self.offset += 1;
        const kind: ?Kind = switch (c) {
            '(' => .open,
            ')' => .close,
            ',' => .comma,
            ';' => .semicolon,
            else => null,
        };
        if (kind) |value| return .{ .kind = value };
        if (c == '\'' or c == '"') {
            while (self.offset < self.input.len) {
                if (self.input[self.offset] == c) {
                    self.offset += 1;
                    if (self.offset < self.input.len and self.input[self.offset] == c) {
                        self.offset += 1;
                        continue;
                    }
                    return .{ .kind = if (c == '\'') .string else .identifier, .raw = self.input[token_start + 1 .. self.offset - 1] };
                }
                self.offset += 1;
            }
            return error.PgReplicationSyntax;
        }
        while (self.offset < self.input.len) {
            const b = self.input[self.offset];
            if (space(b) or std.mem.indexOfScalar(u8, "(),;\"'", b) != null) break;
            self.offset += 1;
        }
        return .{ .kind = .atom, .raw = self.input[token_start..self.offset] };
    }
    fn peek(self: *Parser) !Token {
        if (self.look == null) self.look = try self.next();
        return self.look.?;
    }
    fn keyword(self: *Parser, word: []const u8) !bool {
        const token = try self.peek();
        if (token.kind != .atom or !std.ascii.eqlIgnoreCase(token.raw, word)) return false;
        _ = try self.next();
        return true;
    }
    fn need(self: *Parser, word: []const u8) !void {
        if (!try self.keyword(word)) return error.PgReplicationSyntax;
    }
    fn symbol(self: *Parser, kind: Kind) !bool {
        if ((try self.peek()).kind != kind) return false;
        _ = try self.next();
        return true;
    }
    fn needSymbol(self: *Parser, kind: Kind) !void {
        if (!try self.symbol(kind)) return error.PgReplicationSyntax;
    }
    fn decode(self: *Parser, token: Token) ![]const u8 {
        if (token.kind == .atom) return self.a.dupe(u8, token.raw);
        const quote: u8 = if (token.kind == .identifier) '"' else '\'';
        const out = try self.a.alloc(u8, token.raw.len);
        var n: usize = 0;
        var i: usize = 0;
        while (i < token.raw.len) : (i += 1) {
            out[n] = token.raw[i];
            n += 1;
            if (token.raw[i] == quote) i += 1;
        }
        return out[0..n];
    }
    fn name(self: *Parser) ![]const u8 {
        const token = try self.next();
        if (token.kind != .identifier and (token.kind != .atom or !identifier(token.raw))) return error.PgReplicationSyntax;
        const value = try self.decode(token);
        if (value.len == 0 or value.len > 63) return error.ProgramLimitExceeded;
        if (token.kind == .atom) for (@constCast(value)) |*c| {
            c.* = std.ascii.toLower(c.*);
        };
        return value;
    }
    fn slot(self: *Parser) ![]const u8 {
        const value = try self.name();
        for (value) |c| if (!(c >= 'a' and c <= 'z') and !std.ascii.isDigit(c) and c != '_') return error.InvalidReplicationSlotName;
        return value;
    }
    fn finish(self: *Parser) !void {
        _ = try self.symbol(.semicolon);
        if ((try self.next()).kind != .end) return error.PgReplicationSyntax;
    }
};
fn boolean(value: []const u8) !bool {
    if (std.mem.eql(u8, value, "1")) return true;
    if (std.mem.eql(u8, value, "0")) return false;
    if (value.len == 0) return error.InvalidReplicationOption;
    var answer: ?bool = null;
    for ([_][]const u8{ "true", "yes", "on", "false", "no", "off" }, 0..) |word, index| {
        if (value.len <= word.len and std.ascii.eqlIgnoreCase(value, word[0..value.len])) {
            const result = index < 3;
            if (answer != null and answer.? != result) return error.InvalidReplicationOption;
            answer = result;
        }
    }
    return answer orelse error.InvalidReplicationOption;
}
pub const StartupMode = enum { normal, logical };
pub fn startupMode(value: []const u8) !StartupMode {
    if (std.mem.eql(u8, value, "database")) return .logical;
    if (!try boolean(value)) return .normal;
    return error.UnsupportedReplicationMode;
}
/// Allocation-free classification for ordinary SQL connections. Recognition
/// does not validate the rest of a command and cannot authorize execution.
pub fn recognizes(input: []const u8) bool {
    var offset: usize = 0;
    while (offset < input.len and space(input[offset])) offset += 1;
    const begin = offset;
    if (offset == input.len or !identFirst(input[offset])) return false;
    while (offset < input.len and identRest(input[offset])) offset += 1;
    const word = input[begin..offset];
    for ([_][]const u8{ "IDENTIFY_SYSTEM", "CREATE_REPLICATION_SLOT", "DROP_REPLICATION_SLOT", "START_REPLICATION", "BASE_BACKUP", "TIMELINE_HISTORY", "READ_REPLICATION_SLOT", "UPLOAD_MANIFEST", "ALTER_REPLICATION_SLOT" }) |known| if (std.ascii.eqlIgnoreCase(word, known)) return true;
    return false;
}

fn requireFalse(value: ?[]const u8) !void {
    if (try boolean(value orelse "true")) return error.UnsupportedReplicationOption;
}
pub fn parseLsn(value: []const u8) !Lsn {
    const slash = std.mem.indexOfScalar(u8, value, '/') orelse return error.PgReplicationSyntax;
    const hi = value[0..slash];
    const lo = value[slash + 1 ..];
    if (hi.len == 0 or hi.len > 8 or lo.len == 0 or lo.len > 8) return error.PgReplicationSyntax;
    for (hi) |c| if (!std.ascii.isHex(c)) return error.PgReplicationSyntax;
    for (lo) |c| if (!std.ascii.isHex(c)) return error.PgReplicationSyntax;
    const upper = std.fmt.parseInt(u32, hi, 16) catch return error.PgReplicationSyntax;
    const lower = std.fmt.parseInt(u32, lo, 16) catch return error.PgReplicationSyntax;
    return @fromBackingInt(@intCast((@as(u64, upper) << 32) | lower));
}
pub fn formatLsn(a: A, value: Lsn) ![]u8 {
    const bits = @backingInt(value);
    return std.fmt.allocPrint(a, "{X}/{X}", .{ bits >> 32, bits & 0xffffffff });
}
fn publications(a: A, text: []const u8) ![]const []const u8 {
    var p: Parser = .{ .a = a, .input = text };
    var out: std.ArrayList([]const u8) = .empty;
    while (true) {
        if (out.items.len == max_publications) return error.ProgramLimitExceeded;
        const value = try p.name();
        for (out.items) |existing| if (std.mem.eql(u8, existing, value)) return error.InvalidReplicationOption;
        try out.append(a, value);
        if (!try p.symbol(.comma)) break;
    }
    if ((try p.next()).kind != .end) return error.InvalidReplicationOption;
    return out.toOwnedSlice(a);
}
fn start(p: *Parser) !Start {
    if (!try p.keyword("SLOT")) return error.UnsupportedReplicationMode;
    const name = try p.slot();
    if (try p.keyword("PHYSICAL")) return error.UnsupportedReplicationMode;
    try p.need("LOGICAL");
    const position = try p.next();
    if (position.kind != .atom) return error.PgReplicationSyntax;
    const lsn = try parseLsn(position.raw);
    try p.needSymbol(.open);
    var names: ?[]const []const u8 = null;
    var version = false;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    while (true) {
        const option = try p.name();
        const entry = try seen.getOrPut(p.a, option);
        if (entry.found_existing) return error.DuplicateReplicationOption;
        const value_token = try p.peek();
        const value: ?[]const u8 = if (value_token.kind == .string) blk: {
            _ = try p.next();
            break :blk try p.decode(value_token);
        } else null;
        if (std.mem.eql(u8, option, "proto_version")) {
            const text = value orelse return error.InvalidReplicationOption;
            if (text.len == 0 or text.len > 10) return error.InvalidReplicationOption;
            for (text) |c| if (!std.ascii.isDigit(c)) return error.InvalidReplicationOption;
            const number = std.fmt.parseInt(u32, text, 10) catch return error.InvalidReplicationOption;
            if (number != 1) return error.UnsupportedReplicationOption;
            version = true;
        } else if (std.mem.eql(u8, option, "publication_names")) {
            names = try publications(p.a, value orelse return error.InvalidReplicationOption);
        } else if (std.mem.eql(u8, option, "binary") or std.mem.eql(u8, option, "messages") or std.mem.eql(u8, option, "two_phase") or std.mem.eql(u8, option, "streaming")) {
            if (value) |v| if (std.mem.eql(u8, option, "streaming") and std.ascii.eqlIgnoreCase(v, "parallel")) return error.UnsupportedReplicationOption;
            try requireFalse(value);
        } else if (std.mem.eql(u8, option, "origin")) {
            if (!std.mem.eql(u8, value orelse return error.InvalidReplicationOption, "any")) return error.UnsupportedReplicationOption;
        } else return error.UnsupportedReplicationOption;
        if (!try p.symbol(.comma)) break;
    }
    try p.needSymbol(.close);
    if (!version or names == null) return error.InvalidReplicationOption;
    return .{ .slot = name, .position = lsn, .publications = names.? };
}
fn snapshot(value: []const u8) !Snapshot {
    if (std.mem.eql(u8, value, "export")) return .export_snapshot;
    if (std.mem.eql(u8, value, "use")) return .use_snapshot;
    if (std.mem.eql(u8, value, "nothing")) return .nothing;
    return error.InvalidReplicationOption;
}
fn create(p: *Parser) !Create {
    var result: Create = .{ .slot = try p.slot() };
    result.temporary = try p.keyword("TEMPORARY");
    if (try p.keyword("PHYSICAL")) return error.UnsupportedReplicationMode;
    try p.need("LOGICAL");
    if (!std.mem.eql(u8, try p.name(), "pgoutput")) return error.UnsupportedReplicationOption;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    if (try p.symbol(.open)) {
        while (true) {
            const option = try p.name();
            const entry = try seen.getOrPut(p.a, option);
            if (entry.found_existing) return error.DuplicateReplicationOption;
            const token = try p.peek();
            const value: ?[]const u8 = if (token.kind == .atom or token.kind == .identifier or token.kind == .string) blk: {
                _ = try p.next();
                break :blk try p.decode(token);
            } else null;
            if (std.mem.eql(u8, option, "snapshot")) result.snapshot = try snapshot(value orelse return error.InvalidReplicationOption) else if (std.mem.eql(u8, option, "two_phase") or std.mem.eql(u8, option, "failover")) try requireFalse(value) else return error.UnsupportedReplicationOption;
            if (!try p.symbol(.comma)) break;
        }
        try p.needSymbol(.close);
    } else {
        while (true) {
            const mode: Snapshot = if (try p.keyword("EXPORT_SNAPSHOT")) .export_snapshot else if (try p.keyword("NOEXPORT_SNAPSHOT")) .nothing else if (try p.keyword("USE_SNAPSHOT")) .use_snapshot else break;
            const entry = try seen.getOrPut(p.a, "snapshot");
            if (entry.found_existing) return error.DuplicateReplicationOption;
            result.snapshot = mode;
        }
        if (try p.keyword("TWO_PHASE") or try p.keyword("RESERVE_WAL")) return error.UnsupportedReplicationOption;
    }
    return result;
}
/// Null means an ordinary SQL statement, not a replication command. A known
/// replication prefix can never fall through to SQL after a grammar/option
/// failure. Exactly one command and optional final semicolon are accepted.
pub fn parse(a: A, input: []const u8) !?Parsed {
    if (input.len > max_command_bytes) return error.ProgramLimitExceeded;
    if (std.mem.indexOfScalar(u8, input, 0) != null or !std.unicode.utf8ValidateSlice(input)) return error.PgReplicationSyntax;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    var p: Parser = .{ .a = arena.allocator(), .input = input };
    const token = try p.next();
    if (token.kind != .atom) {
        arena.deinit();
        return null;
    }
    const command: Command = if (std.ascii.eqlIgnoreCase(token.raw, "IDENTIFY_SYSTEM")) .identify_system else if (std.ascii.eqlIgnoreCase(token.raw, "CREATE_REPLICATION_SLOT")) .{ .create_slot = try create(&p) } else if (std.ascii.eqlIgnoreCase(token.raw, "START_REPLICATION")) .{ .start = try start(&p) } else if (std.ascii.eqlIgnoreCase(token.raw, "DROP_REPLICATION_SLOT")) .{ .drop_slot = .{ .slot = try p.slot(), .wait = try p.keyword("WAIT") } } else {
        for ([_][]const u8{ "BASE_BACKUP", "TIMELINE_HISTORY", "READ_REPLICATION_SLOT", "UPLOAD_MANIFEST", "ALTER_REPLICATION_SLOT" }) |word| if (std.ascii.eqlIgnoreCase(token.raw, word)) return error.UnsupportedReplicationMode;
        if (recognizes(input)) return error.PgReplicationSyntax;
        arena.deinit();
        return null;
    };
    try p.finish();
    return .{ .arena = arena, .command = command };
}

test "replication commands parse pgoutput client create and start with stable names and LSN" {
    const a = std.testing.allocator;
    var created = (try parse(a, "CREATE_REPLICATION_SLOT \"debezium\" LOGICAL pgoutput EXPORT_SNAPSHOT;")).?;
    defer created.deinit();
    try std.testing.expectEqualStrings("debezium", created.command.create_slot.slot);
    try std.testing.expectEqual(Snapshot.export_snapshot, created.command.create_slot.snapshot);
    var stream = (try parse(a, "START_REPLICATION SLOT \"debezium\" LOGICAL 1/aB (\"proto_version\" '1', \"publication_names\" '\"Mixed,Name\", public_rows', streaming 'off', binary 'false', messages 'false', two_phase 'false', origin 'any')")).?;
    defer stream.deinit();
    try std.testing.expectEqual(@as(u64, 0x1000000ab), @backingInt(stream.command.start.position));
    try std.testing.expectEqualStrings("Mixed,Name", stream.command.start.publications[0]);
    try std.testing.expectEqualStrings("public_rows", stream.command.start.publications[1]);
    const formatted = try formatLsn(a, stream.command.start.position);
    defer a.free(formatted);
    try std.testing.expectEqualStrings("1/AB", formatted);
}
test "replication commands preserve quoted publication identity and reject injection or duplicates" {
    const a = std.testing.allocator;
    var value = (try parse(a, "START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '1', publication_names '\"a\"\"b\",UPPER')")).?;
    defer value.deinit();
    try std.testing.expectEqualStrings("a\"b", value.command.start.publications[0]);
    try std.testing.expectEqualStrings("upper", value.command.start.publications[1]);
    try std.testing.expectError(error.InvalidReplicationOption, parse(a, "START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '1', publication_names 'foo,FOO')"));
    try std.testing.expectError(error.PgReplicationSyntax, parse(a, "IDENTIFY_SYSTEM; DROP_REPLICATION_SLOT s"));
    try std.testing.expectError(error.PgReplicationSyntax, parse(a, "DROP_REPLICATION_SLOT s WAIT WAIT"));
    try std.testing.expectError(error.InvalidReplicationSlotName, parse(a, "DROP_REPLICATION_SLOT \"Upper\""));
    try std.testing.expect((try parse(a, "SELECT * FROM pg_replication_slots")) == null);
}
test "replication commands negotiate snapshots and reject unimplemented guarantees before admission" {
    const a = std.testing.allocator;
    var value = (try parse(a, "create_replication_slot S temporary logical pgoutput (snapshot 'nothing', two_phase false, failover 'off')")).?;
    defer value.deinit();
    try std.testing.expectEqualStrings("s", value.command.create_slot.slot);
    try std.testing.expect(value.command.create_slot.temporary);
    try std.testing.expectEqual(Snapshot.nothing, value.command.create_slot.snapshot);
    try std.testing.expectError(error.UnsupportedReplicationOption, parse(a, "CREATE_REPLICATION_SLOT s LOGICAL pgoutput TWO_PHASE"));
    try std.testing.expectError(error.DuplicateReplicationOption, parse(a, "CREATE_REPLICATION_SLOT s LOGICAL pgoutput EXPORT_SNAPSHOT NOEXPORT_SNAPSHOT"));
    try std.testing.expectError(error.UnsupportedReplicationMode, parse(a, "START_REPLICATION 0/0"));
    for ([_][]const u8{ "binary 'true'", "messages 'on'", "two_phase '1'", "streaming 'parallel'", "origin 'none'", "unknown 'off'" }) |option| {
        const text = try std.fmt.allocPrint(a, "START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '1', publication_names 'p', {s})", .{option});
        defer a.free(text);
        try std.testing.expectError(error.UnsupportedReplicationOption, parse(a, text));
    }
    try std.testing.expectError(error.UnsupportedReplicationOption, parse(a, "START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '2', publication_names 'p')"));
    try std.testing.expectError(error.DuplicateReplicationOption, parse(a, "START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '1', proto_version '1', publication_names 'p')"));
    try std.testing.expectError(error.InvalidReplicationOption, parse(a, "START_REPLICATION SLOT s LOGICAL 0/0 (publication_names 'p')"));
}
test "replication commands reject malformed bounded input and LSN overflow" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "0", "0/", "/0", "1/2/3", "100000000/0", "0/100000000", "+1/0", "0/-1" }) |value| try std.testing.expectError(error.PgReplicationSyntax, parseLsn(value));
    try std.testing.expectError(error.PgReplicationSyntax, parse(a, "IDENTIFY_SYSTEM\x00"));
    try std.testing.expectError(error.PgReplicationSyntax, parse(a, "CREATE_REPLICATION_SLOT \"s LOGICAL pgoutput"));
    try std.testing.expectError(error.PgReplicationSyntax, parse(a, "IDENTIFY_SYSTEM\xff"));
    try std.testing.expectError(error.ProgramLimitExceeded, parse(a, &@as([max_command_bytes + 1]u8, @splat('x'))));
}
fn allocationProbe(a: A) !void {
    var parsed = (try parse(a, "START_REPLICATION SLOT \"slot\" LOGICAL 0/1 (proto_version '1', publication_names '\"quoted,name\",normal', streaming 'off')")).?;
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.command.start.publications.len);
}
test "replication commands release arena through every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}

test "replication commands bound publication sets and normalize supported startup booleans" {
    const a = std.testing.allocator;
    try std.testing.expectEqual(StartupMode.logical, try startupMode("database"));
    for ([_][]const u8{ "false", "FALSE", "fa", "of", "0", "no" }) |value| try std.testing.expectEqual(StartupMode.normal, try startupMode(value));
    try std.testing.expectError(error.UnsupportedReplicationMode, startupMode("true"));
    try std.testing.expectError(error.InvalidReplicationOption, startupMode("o"));
    var text: std.Io.Writer.Allocating = .init(a);
    defer text.deinit();
    try text.writer.writeAll("START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '01', publication_names '");
    for (0..max_publications + 1) |index| {
        if (index != 0) try text.writer.writeByte(',');
        try text.writer.print("p{d}", .{index});
    }
    try text.writer.writeAll("')");
    try std.testing.expectError(error.ProgramLimitExceeded, parse(a, text.written()));
    try std.testing.expect(recognizes(" IDENTIFY_SYSTEM/**/"));
    try std.testing.expectError(error.PgReplicationSyntax, parse(a, "IDENTIFY_SYSTEM/**/"));
    try std.testing.expect(!recognizes("identify_system_custom()"));
    var valid = (try parse(a, "START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '01', publication_names 'p', binary 'fal')")).?;
    defer valid.deinit();
    try std.testing.expectEqual(@as(usize, 1), valid.command.start.publications.len);
}

test {
    _ = @import("replication_input.zig");
}
