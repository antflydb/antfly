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

const platform_time = @import("antfly_platform").time;
const db_types = @import("../storage/db/types.zig");

var json_row_spill_nonce: std.atomic.Value(u64) = .init(0);

pub const SpilledRows = struct {
    path: []const u8,
    row_count: usize,
    materialized_bytes: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        deleteFilePath(self.path) catch {};
        alloc.free(self.path);
        self.* = undefined;
    }
};

pub const KeyedJsonRow = struct {
    key: []const u8,
    json: []const u8,
    version: u64,
};

pub const JsonRowsMaterializationTracker = struct {
    cte: db_types.RelationalRowsCte,
    rows: u64 = 0,
    bytes: u64 = 2,
    spill_required: bool = false,

    pub fn initDefault(name: []const u8) JsonRowsMaterializationTracker {
        return .init(
            name,
            db_types.default_relational_rows_cte_max_rows,
            db_types.default_relational_rows_cte_max_bytes,
            db_types.default_relational_rows_cte_spill_after_bytes,
        );
    }

    pub fn init(name: []const u8, max_rows: u32, max_bytes: u64, spill_after_bytes: u64) JsonRowsMaterializationTracker {
        return .{
            .cte = .{
                .name = name,
                .max_rows = max_rows,
                .max_bytes = max_bytes,
                .spill_after_bytes = spill_after_bytes,
            },
        };
    }

    pub fn account(self: *JsonRowsMaterializationTracker, row_json: []const u8) !void {
        const next_rows = std.math.add(u64, self.rows, 1) catch return error.UnsupportedRowsQuery;
        const observed_rows = std.math.cast(usize, next_rows) orelse return error.UnsupportedRowsQuery;
        var next_bytes = self.bytes;
        if (self.rows > 0) next_bytes = std.math.add(u64, next_bytes, 1) catch return error.UnsupportedRowsQuery;
        next_bytes = std.math.add(u64, next_bytes, @intCast(row_json.len)) catch return error.UnsupportedRowsQuery;
        switch (db_types.relationalRowsCteMaterializationDecision(self.cte, observed_rows, next_bytes)) {
            .memory => {},
            .spill => self.spill_required = true,
            .reject => return error.UnsupportedRowsQuery,
        }
        self.rows = next_rows;
        self.bytes = next_bytes;
    }
};

pub const SpilledKeyedRows = struct {
    path: []const u8,
    row_count: usize,
    file_bytes: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        deleteFilePath(self.path) catch {};
        alloc.free(self.path);
        self.* = undefined;
    }
};

pub fn spillJsonRowsAlloc(
    alloc: std.mem.Allocator,
    rows: []const []const u8,
    materialized_bytes: u64,
    name: []const u8,
) !SpilledRows {
    return try spillJsonRowsWithFirstPathAlloc(alloc, rows, materialized_bytes, name, null);
}

fn spillJsonRowsWithFirstPathAlloc(
    alloc: std.mem.Allocator,
    rows: []const []const u8,
    materialized_bytes: u64,
    name: []const u8,
    first_path: ?[]const u8,
) !SpilledRows {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    var created = try createUniqueSpillFileAlloc(alloc, io, name, first_path);
    defer created.file.close(io);
    errdefer alloc.free(created.path);
    errdefer deleteFilePath(created.path) catch {};

    var buf: [4096]u8 = undefined;
    var writer = created.file.writer(io, &buf);
    var len_buf: [8]u8 = undefined;
    for (rows) |row| {
        std.mem.writeInt(u64, &len_buf, @intCast(row.len), .big);
        try writer.interface.writeAll(&len_buf);
        try writer.interface.writeAll(row);
    }
    try writer.end();

    return .{
        .path = created.path,
        .row_count = rows.len,
        .materialized_bytes = materialized_bytes,
    };
}

pub fn spillKeyedJsonRowsAlloc(
    alloc: std.mem.Allocator,
    rows: []const KeyedJsonRow,
    name: []const u8,
) !SpilledKeyedRows {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    var created = try createUniqueSpillFileAlloc(alloc, io, name, null);
    defer created.file.close(io);
    errdefer alloc.free(created.path);
    errdefer deleteFilePath(created.path) catch {};

    var buf: [4096]u8 = undefined;
    var writer = created.file.writer(io, &buf);
    var len_buf: [8]u8 = undefined;
    var file_bytes: u64 = 0;
    for (rows) |row| {
        std.mem.writeInt(u64, &len_buf, @intCast(row.key.len), .big);
        try writer.interface.writeAll(&len_buf);
        try writer.interface.writeAll(row.key);
        std.mem.writeInt(u64, &len_buf, @intCast(row.json.len), .big);
        try writer.interface.writeAll(&len_buf);
        try writer.interface.writeAll(row.json);
        std.mem.writeInt(u64, &len_buf, row.version, .big);
        try writer.interface.writeAll(&len_buf);

        file_bytes = std.math.add(u64, file_bytes, 24) catch return error.StreamTooLong;
        file_bytes = std.math.add(u64, file_bytes, @intCast(row.key.len)) catch return error.StreamTooLong;
        file_bytes = std.math.add(u64, file_bytes, @intCast(row.json.len)) catch return error.StreamTooLong;
    }
    try writer.end();

    return .{
        .path = created.path,
        .row_count = rows.len,
        .file_bytes = file_bytes,
    };
}

const CreatedSpillFile = struct {
    path: []const u8,
    file: std.Io.File,
};

fn createUniqueSpillFileAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    name: []const u8,
    first_path: ?[]const u8,
) !CreatedSpillFile {
    const max_attempts = 8;
    var attempt: usize = 0;
    while (attempt < max_attempts) : (attempt += 1) {
        const path = if (attempt == 0) blk: {
            if (first_path) |path| break :blk try alloc.dupe(u8, path);
            break :blk try spillPathAlloc(alloc, name);
        } else try spillPathAlloc(alloc, name);
        errdefer alloc.free(path);
        const file = createSpillFile(io, path) catch |err| switch (err) {
            error.PathAlreadyExists => {
                alloc.free(path);
                continue;
            },
            else => return err,
        };
        return .{ .path = path, .file = file };
    }
    return error.PathAlreadyExists;
}

fn createSpillFile(io: std.Io, path: []const u8) !std.Io.File {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = false, .exclusive = true });
    }
    return try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false, .exclusive = true });
}

pub fn loadJsonRowsAlloc(alloc: std.mem.Allocator, spill: SpilledRows) ![][]const u8 {
    const payload_bytes = std.math.cast(usize, spill.materialized_bytes) orelse return error.StreamTooLong;
    const length_bytes = try std.math.mul(usize, spill.row_count, @as(usize, 8));
    const payload_and_lengths = try std.math.add(usize, payload_bytes, length_bytes);
    const max_file_bytes = try std.math.add(usize, payload_and_lengths, @as(usize, 4096));

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var file = if (std.fs.path.isAbsolute(spill.path))
        try std.Io.Dir.openFileAbsolute(io, spill.path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, spill.path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    var reader: std.Io.File.Reader = .initSize(file, io, &.{}, stat.size);
    const data = try reader.interface.allocRemaining(alloc, .limited(max_file_bytes));
    defer alloc.free(data);

    var rows = try alloc.alloc([]const u8, spill.row_count);
    var rows_written: usize = 0;
    errdefer freeJsonRows(alloc, rows[0..rows_written]);

    var cursor: usize = 0;
    while (rows_written < spill.row_count) : (rows_written += 1) {
        if (data.len - cursor < 8) return error.InvalidRowsRequest;
        const row_len_u64 = std.mem.readInt(u64, data[cursor..][0..8], .big);
        cursor += 8;
        const row_len = std.math.cast(usize, row_len_u64) orelse return error.StreamTooLong;
        if (data.len - cursor < row_len) return error.InvalidRowsRequest;
        rows[rows_written] = try alloc.dupe(u8, data[cursor..][0..row_len]);
        cursor += row_len;
    }
    if (cursor != data.len) return error.InvalidRowsRequest;
    return rows;
}

pub fn spillAndReloadOwnedJsonRowsAlloc(
    alloc: std.mem.Allocator,
    rows: []const []const u8,
    materialized_bytes: u64,
    name: []const u8,
) ![][]const u8 {
    var spill = try spillJsonRowsAlloc(alloc, rows, materialized_bytes, name);
    defer spill.deinit(alloc);
    const loaded = try loadJsonRowsAlloc(alloc, spill);
    freeJsonRows(alloc, rows);
    return loaded;
}

pub fn loadKeyedJsonRowsAlloc(alloc: std.mem.Allocator, spill: SpilledKeyedRows) ![]KeyedJsonRow {
    const file_bytes = std.math.cast(usize, spill.file_bytes) orelse return error.StreamTooLong;
    const max_file_bytes = try std.math.add(usize, file_bytes, 4096);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var file = if (std.fs.path.isAbsolute(spill.path))
        try std.Io.Dir.openFileAbsolute(io, spill.path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, spill.path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    var reader: std.Io.File.Reader = .initSize(file, io, &.{}, stat.size);
    const data = try reader.interface.allocRemaining(alloc, .limited(max_file_bytes));
    defer alloc.free(data);

    var rows = try alloc.alloc(KeyedJsonRow, spill.row_count);
    var rows_written: usize = 0;
    errdefer freeKeyedJsonRows(alloc, rows[0..rows_written]);

    var cursor: usize = 0;
    while (rows_written < spill.row_count) : (rows_written += 1) {
        if (data.len - cursor < 8) return error.InvalidRowsRequest;
        const key_len_u64 = std.mem.readInt(u64, data[cursor..][0..8], .big);
        cursor += 8;
        const key_len = std.math.cast(usize, key_len_u64) orelse return error.StreamTooLong;
        if (data.len - cursor < key_len) return error.InvalidRowsRequest;
        const key = try alloc.dupe(u8, data[cursor..][0..key_len]);
        errdefer alloc.free(key);
        cursor += key_len;

        if (data.len - cursor < 8) return error.InvalidRowsRequest;
        const json_len_u64 = std.mem.readInt(u64, data[cursor..][0..8], .big);
        cursor += 8;
        const json_len = std.math.cast(usize, json_len_u64) orelse return error.StreamTooLong;
        if (data.len - cursor < json_len) return error.InvalidRowsRequest;
        const json = try alloc.dupe(u8, data[cursor..][0..json_len]);
        errdefer alloc.free(json);
        cursor += json_len;

        if (data.len - cursor < 8) return error.InvalidRowsRequest;
        const version = std.mem.readInt(u64, data[cursor..][0..8], .big);
        cursor += 8;

        rows[rows_written] = .{
            .key = key,
            .json = json,
            .version = version,
        };
    }
    if (cursor != data.len) return error.InvalidRowsRequest;
    return rows;
}

pub fn freeJsonRows(alloc: std.mem.Allocator, rows: []const []const u8) void {
    for (rows) |row| alloc.free(@constCast(row));
    if (rows.len > 0) alloc.free(rows);
}

pub fn freeKeyedJsonRows(alloc: std.mem.Allocator, rows: []const KeyedJsonRow) void {
    for (rows) |row| {
        alloc.free(@constCast(row.key));
        alloc.free(@constCast(row.json));
    }
    if (rows.len > 0) alloc.free(rows);
}

fn spillPathAlloc(alloc: std.mem.Allocator, name: []const u8) ![]const u8 {
    const nonce = json_row_spill_nonce.fetchAdd(1, .monotonic);
    const label = try safeSpillLabelAlloc(alloc, name);
    defer alloc.free(label);
    return try std.fmt.allocPrint(
        alloc,
        "/tmp/antfly-sql-{s}-row-spill-{d}-{d}.bin",
        .{ label, platform_time.monotonicNs(), nonce },
    );
}

fn safeSpillLabelAlloc(alloc: std.mem.Allocator, name: []const u8) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    const max_label_bytes = 64;
    for (name) |byte| {
        if (out.items.len >= max_label_bytes) break;
        const safe = std.ascii.isAlphanumeric(byte) or byte == '_' or byte == '-';
        try out.append(alloc, if (safe) byte else '-');
    }
    if (out.items.len == 0) try out.appendSlice(alloc, "rows");
    return try out.toOwnedSlice(alloc);
}

fn deleteFilePath(path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io_impl.io(), path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io_impl.io(), path);
    }
}

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

test "sql row spill round trips length framed json rows" {
    const alloc = std.testing.allocator;
    const rows = [_][]const u8{
        "{\"id\":\"a\",\"amount\":1}",
        "{\"id\":\"b\",\"amount\":2}",
    };
    var spill = try spillJsonRowsAlloc(alloc, rows[0..], 2 + rows[0].len + 1 + rows[1].len, "test");
    defer spill.deinit(alloc);

    const loaded = try loadJsonRowsAlloc(alloc, spill);
    defer freeJsonRows(alloc, loaded);
    try std.testing.expectEqual(@as(usize, 2), loaded.len);
    try std.testing.expectEqualStrings(rows[0], loaded[0]);
    try std.testing.expectEqualStrings(rows[1], loaded[1]);
}

test "sql row spill reload helper takes ownership of json rows" {
    const alloc = std.testing.allocator;
    var rows = try alloc.alloc([]const u8, 2);
    var initialized: usize = 0;
    errdefer freeJsonRows(alloc, rows[0..initialized]);
    rows[0] = try alloc.dupe(u8, "{\"id\":\"a\",\"amount\":1}");
    initialized += 1;
    rows[1] = try alloc.dupe(u8, "{\"id\":\"b\",\"amount\":2}");
    initialized += 1;

    const materialized_bytes = 2 + rows[0].len + 1 + rows[1].len;
    const loaded = try spillAndReloadOwnedJsonRowsAlloc(alloc, rows, materialized_bytes, "owned-reload-test");
    rows = &.{};
    initialized = 0;
    defer freeJsonRows(alloc, loaded);

    try std.testing.expectEqual(@as(usize, 2), loaded.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"amount\":1}", loaded[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"amount\":2}", loaded[1]);
}

test "sql row spill round trips keyed json rows" {
    const alloc = std.testing.allocator;
    const rows = [_]KeyedJsonRow{
        .{ .key = "a", .json = "{\"id\":\"a\",\"amount\":1}", .version = 7 },
        .{ .key = "b", .json = "{\"id\":\"b\",\"amount\":2}", .version = 9 },
    };
    var spill = try spillKeyedJsonRowsAlloc(alloc, rows[0..], "keyed-test");
    defer spill.deinit(alloc);

    const loaded = try loadKeyedJsonRowsAlloc(alloc, spill);
    defer freeKeyedJsonRows(alloc, loaded);
    try std.testing.expectEqual(@as(usize, 2), loaded.len);
    try std.testing.expectEqualStrings(rows[0].key, loaded[0].key);
    try std.testing.expectEqualStrings(rows[0].json, loaded[0].json);
    try std.testing.expectEqual(rows[0].version, loaded[0].version);
    try std.testing.expectEqualStrings(rows[1].key, loaded[1].key);
    try std.testing.expectEqualStrings(rows[1].json, loaded[1].json);
    try std.testing.expectEqual(rows[1].version, loaded[1].version);
}

test "sql row spill materialization tracker follows relational cte policy" {
    var row_budget = JsonRowsMaterializationTracker.init("test", 2, 128, 128);
    try row_budget.account("{\"id\":\"a\"}");
    try row_budget.account("{\"id\":\"b\"}");
    try std.testing.expect(!row_budget.spill_required);
    try std.testing.expectError(error.UnsupportedRowsQuery, row_budget.account("{\"id\":\"c\"}"));

    var byte_budget = JsonRowsMaterializationTracker.init("test", 8, 16, 16);
    try byte_budget.account("{\"id\":\"a\"}");
    try std.testing.expectError(error.UnsupportedRowsQuery, byte_budget.account("{\"id\":\"b\"}"));

    var spill_budget = JsonRowsMaterializationTracker.init("test", 8, 64, 16);
    try spill_budget.account("{\"id\":\"a\"}");
    try std.testing.expect(!spill_budget.spill_required);
    try spill_budget.account("{\"id\":\"b\"}");
    try std.testing.expect(spill_budget.spill_required);
}

test "sql row spill paths sanitize caller labels into one temp file component" {
    const alloc = std.testing.allocator;
    const path = try spillPathAlloc(alloc, "../tenant/cte name\nwith/slashes");
    defer alloc.free(path);

    try std.testing.expect(std.mem.startsWith(u8, path, "/tmp/antfly-sql-"));
    try std.testing.expect(std.mem.indexOf(u8, path, "..") == null);
    try std.testing.expect(std.mem.indexOfScalar(u8, path["/tmp/".len..], '/') == null);
    try std.testing.expect(std.mem.indexOf(u8, path, "tenant-cte-name-with-slashes") != null);
}

test "sql row spill file creation is exclusive" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-sql-row-spill-exclusive-test-{d}.bin", .{platform_time.monotonicNs()});
    defer alloc.free(path);
    defer deleteFilePath(path) catch {};

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var file = try createSpillFile(io, path);
    defer file.close(io);

    try std.testing.expectError(error.PathAlreadyExists, createSpillFile(io, path));
}

test "sql row spill retries when generated path already exists" {
    const alloc = std.testing.allocator;
    const forced_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-sql-row-spill-retry-test-{d}.bin", .{platform_time.monotonicNs()});
    defer alloc.free(forced_path);
    defer deleteFilePath(forced_path) catch {};

    var io_impl = threadedIo();
    defer io_impl.deinit();
    {
        var existing = try createSpillFile(io_impl.io(), forced_path);
        defer existing.close(io_impl.io());
    }

    const rows = [_][]const u8{"{\"id\":\"retry\"}"};
    var spill = try spillJsonRowsWithFirstPathAlloc(alloc, rows[0..], rows[0].len, "retry", forced_path);
    defer spill.deinit(alloc);
    try std.testing.expect(!std.mem.eql(u8, forced_path, spill.path));

    const loaded = try loadJsonRowsAlloc(alloc, spill);
    defer freeJsonRows(alloc, loaded);
    try std.testing.expectEqual(@as(usize, 1), loaded.len);
    try std.testing.expectEqualStrings(rows[0], loaded[0]);
}
