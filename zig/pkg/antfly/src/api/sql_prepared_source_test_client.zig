// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Real pgwire sessions over the native adapter. Transport framing is local;
//! metadata/data admission, transactions and Raft owners are not mocked.
const std = @import("std");
const fixture = @import("sql_owner_test_fixture.zig");
const protocol = @import("../pgwire/protocol.zig");
const platform = @import("antfly_platform");

fn frame(out: *std.Io.Writer, tag: u8, payload: []const u8) !void {
    try out.writeByte(tag);
    try out.writeInt(u32, @intCast(payload.len + 4), .big);
    try out.writeAll(payload);
}

pub fn run(ctx: *fixture.Context, original: []const u8, name: []const u8, expected: anytype, prepare_only: bool) !void {
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (true) {
        if (try once(ctx, original, name, expected, prepare_only)) return;
        if (platform.time.monotonicNs() >= deadline) return error.PreparedSourceReadAdmissionTimeout;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
}

fn once(ctx: *fixture.Context, original: []const u8, name: []const u8, expected: anytype, prepare_only: bool) !bool {
    var arena = std.heap.ArenaAllocator.init(ctx.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const api = if (ctx.data.http_server) |*value| value else return error.MissingNativeSqlApi;
    try std.testing.expect(api.cfg.auth_enabled);
    var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    const startup = "user\x00pk-rewriter\x00database\x00default\x00\x00";
    try input.writer.writeInt(u32, @intCast(startup.len + 8), .big);
    try input.writer.writeInt(u32, 196608, .big);
    try input.writer.writeAll(startup);
    try frame(&input.writer, 'p', "pk-rewrite-durable-password\x00");
    try frame(&input.writer, 'Q', try std.fmt.allocPrint(a, "{s}\x00", .{original}));
    if (!prepare_only) try frame(&input.writer, 'Q', try std.fmt.allocPrint(a, "EXECUTE {s}\x00", .{name}));
    try frame(&input.writer, 'X', "");
    var reader = std.Io.Reader.fixed(input.written());
    var output = std.Io.Writer.Allocating.init(a);
    defer output.deinit();
    var session: protocol.Session = .{ .alloc = a, .io = ctx.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer };
    defer session.deinit();
    try session.run();
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    var prepared: usize = 0;
    var completed: usize = 0;
    var described: usize = 0;
    var authenticated = false;
    var ready: usize = 0;
    var rows: std.ArrayList([]const std.json.Value) = .empty;
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const size = try cursor.int(u32);
        if (size < 4) return error.ProtocolViolation;
        const payload = try cursor.take(size - 4);
        var body: protocol.Cursor = .{ .bytes = payload };
        switch (tag) {
            'R' => {
                const method = try body.int(u32);
                if (method == 0) authenticated = true else try std.testing.expectEqual(@as(u32, 3), method);
                try body.finish();
            },
            'Z' => {
                try std.testing.expectEqualStrings("I", payload);
                ready += 1;
            },
            'C' => {
                const command = try body.string();
                try body.finish();
                if (std.mem.eql(u8, command, "PREPARE")) {
                    prepared += 1;
                    try std.testing.expectEqual(@as(usize, 0), completed);
                } else {
                    try std.testing.expect(!prepare_only);
                    const count: usize = if (std.mem.eql(u8, expected.command_tag, "SELECT")) expected.output.rows.len else @intCast(expected.affected);
                    const wanted = if (std.mem.eql(u8, expected.command_tag, "INSERT"))
                        try std.fmt.allocPrint(a, "INSERT 0 {d}", .{count})
                    else
                        try std.fmt.allocPrint(a, "{s} {d}", .{ expected.command_tag, count });
                    try std.testing.expectEqualStrings(wanted, command);
                    completed += 1;
                }
            },
            'T' => {
                try std.testing.expect(!prepare_only);
                try std.testing.expectEqual(expected.output.columns.len, try body.int(u16));
                for (expected.output.columns, expected.output.column_oids) |column, oid| {
                    try std.testing.expectEqualStrings(column, try body.string());
                    _ = try body.int(u32); // table OID
                    _ = try body.int(u16); // attribute number
                    try std.testing.expectEqual(oid, try body.int(u32));
                    _ = try body.int(u16); // physical type size
                    _ = try body.int(u32); // type modifier
                    try std.testing.expectEqual(@as(u16, 0), try body.int(u16));
                }
                try body.finish();
                described += 1;
            },
            'D' => {
                try std.testing.expect(!prepare_only);
                try std.testing.expectEqual(@as(usize, 1), described);
                const count = try body.int(u16);
                try std.testing.expectEqual(expected.output.columns.len, count);
                const cells = try a.alloc(std.json.Value, count);
                for (cells, expected.output.column_oids) |*cell, oid| {
                    // These exact originals return TEXT IDs. No implicit
                    // string coercion can conceal a result-type mismatch.
                    try std.testing.expectEqual(@as(u32, 25), oid);
                    const length = try body.int(i32);
                    cell.* = if (length == -1) .null else if (length < 0) return error.ProtocolViolation else .{ .string = try body.take(@intCast(length)) };
                }
                try body.finish();
                try rows.append(a, cells);
            },
            'E' => {
                // Only the existing precise pre-admission read diagnostic is
                // retryable. Never replay an admitted/ambiguous mutation, a
                // partially delivered result, or a post-COMMIT read error.
                if (prepared == 1 and completed == 0 and rows.items.len == 0 and
                    std.mem.indexOf(u8, payload, "C53300\x00MA consistent SQL statement read is temporarily unavailable. Retry the complete statement after a bounded delay.\x00") != null and
                    std.mem.indexOf(u8, payload, "D{\"retryable\":true}") != null and
                    std.mem.indexOf(u8, payload, "transaction_id") == null) return false;
                std.debug.print("prepared source protocol error: {s}\n", .{payload});
                return error.PreparedSourceProtocolError;
            },
            else => {},
        }
    }
    try std.testing.expect(authenticated);
    try std.testing.expectEqual(@as(usize, 1), prepared);
    try std.testing.expectEqual(@as(usize, if (prepare_only) 0 else 1), completed);
    try std.testing.expectEqual(@as(usize, if (prepare_only) 2 else 3), ready);
    if (prepare_only) {
        try std.testing.expectEqual(@as(usize, 0), rows.items.len);
        try std.testing.expectEqual(@as(usize, 0), described);
        return true;
    }
    try std.testing.expectEqual(expected.output.rows.len, rows.items.len);
    try std.testing.expectEqual(@as(usize, if (expected.output.columns.len == 0) 0 else 1), described);
    const matched = try a.alloc(bool, rows.items.len);
    @memset(matched, false);
    for (rows.items) |row| {
        var found = false;
        for (expected.output.rows, 0..) |wanted, index| {
            if (matched[index] or row.len != wanted.len) continue;
            var same = true;
            for (row, wanted) |cell, value| {
                if (cell == .null) {
                    same = same and value == .null;
                } else same = same and value == .string and std.mem.eql(u8, cell.string, value.string);
            }
            if (!same) continue;
            matched[index] = true;
            found = true;
            break;
        }
        try std.testing.expect(found);
    }
    return true;
}
