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
const db_mod = @import("../storage/db/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_writes.zig");

pub const OwnedLinearMergeRequest = struct {
    writes: []db_mod.types.BatchWrite = &.{},
    last_merged_id: []const u8 = "",
    dry_run: bool = false,
    sync_level: db_mod.types.SyncLevel = .write,

    pub fn deinit(self: *OwnedLinearMergeRequest, alloc: std.mem.Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        if (self.last_merged_id.len > 0) alloc.free(self.last_merged_id);
        self.* = undefined;
    }
};

pub fn parseRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedLinearMergeRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidLinearMergeRequest;

    const root = parsed.value.object;
    const records_value = root.get("records") orelse return error.InvalidLinearMergeRequest;
    if (records_value != .object or records_value.object.count() == 0) return error.InvalidLinearMergeRequest;

    const writes = try alloc.alloc(db_mod.types.BatchWrite, records_value.object.count());
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    var it = records_value.object.iterator();
    while (it.next()) |entry| {
        writes[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{}),
        };
        initialized += 1;
    }
    std.sort.heap(db_mod.types.BatchWrite, writes, {}, batchWriteLessThan);

    const last_merged_id = if (root.get("last_merged_id")) |value| switch (value) {
        .null => "",
        .string => |text| try alloc.dupe(u8, text),
        else => return error.InvalidLinearMergeRequest,
    } else "";
    errdefer if (last_merged_id.len > 0) alloc.free(last_merged_id);

    const dry_run = if (root.get("dry_run")) |value| switch (value) {
        .null => false,
        .bool => |flag| flag,
        else => return error.InvalidLinearMergeRequest,
    } else false;

    const sync_level = if (root.get("sync_level")) |value|
        try parseSyncLevel(value)
    else
        db_mod.types.SyncLevel.write;

    for (writes) |write| {
        if (last_merged_id.len > 0 and !std.mem.lessThan(u8, last_merged_id, write.key)) {
            return error.InvalidLinearMergeRequest;
        }
    }

    return .{
        .writes = writes,
        .last_merged_id = last_merged_id,
        .dry_run = dry_run,
        .sync_level = sync_level,
    };
}

pub fn execute(
    alloc: std.mem.Allocator,
    reads: table_reads.TableReadSource,
    writes: table_writes.TableWriteSource,
    table_name: []const u8,
    req: OwnedLinearMergeRequest,
) ![]u8 {
    const response = try executeResponse(alloc, reads, writes, table_name, req);
    return try encodeResponse(alloc, response);
}

pub const Response = struct {
    status: []const u8,
    upserted: usize,
    deleted: usize,
    skipped: usize,
    next_cursor: []const u8,
    key_range: struct {
        from: []const u8,
        to: []const u8,
    },
    keys_scanned: usize,
    deleted_ids: ?[]const []const u8 = null,
    message: ?[]const u8 = null,
};

pub fn executeResponse(
    alloc: std.mem.Allocator,
    reads: table_reads.TableReadSource,
    writes: table_writes.TableWriteSource,
    table_name: []const u8,
    req: OwnedLinearMergeRequest,
) !Response {
    if (req.writes.len == 0) return error.InvalidLinearMergeRequest;

    var request_keys = std.StringHashMapUnmanaged(void){};
    defer request_keys.deinit(alloc);
    for (req.writes) |write| try request_keys.put(alloc, write.key, {});

    var changed_writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer changed_writes.deinit(alloc);

    var skipped: usize = 0;
    for (req.writes) |write| {
        var existing = try reads.lookup(alloc, table_name, write.key, .{}, .read_index);
        if (existing) |*lookup| {
            defer lookup.deinit(alloc);
            if (try jsonDocumentsEqualIgnoringTimestamp(alloc, write.value, lookup.json)) {
                skipped += 1;
            } else {
                try changed_writes.append(alloc, write);
            }
        } else {
            try changed_writes.append(alloc, write);
        }
    }

    const next_cursor = req.writes[req.writes.len - 1].key;
    var scanned = (try reads.scan(
        alloc,
        table_name,
        req.last_merged_id,
        next_cursor,
        .{
            .inclusive_from = false,
            .exclusive_to = false,
        },
        .read_index,
    )) orelse return error.TableNotFound;
    defer scanned.deinit(alloc);

    var deleted_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deleted_ids.items) |key| alloc.free(@constCast(key));
        deleted_ids.deinit(alloc);
    }

    var keys_scanned: usize = 0;
    var lines = std.mem.splitScalar(u8, scanned.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        keys_scanned += 1;
        const key = try parseScanLineKey(alloc, line);
        defer alloc.free(key);
        if (request_keys.contains(key)) continue;
        try deleted_ids.append(alloc, try alloc.dupe(u8, key));
    }

    if (!req.dry_run and (changed_writes.items.len > 0 or deleted_ids.items.len > 0)) {
        _ = (try writes.batch(alloc, table_name, .{
            .writes = changed_writes.items,
            .deletes = deleted_ids.items,
            .sync_level = req.sync_level,
        })) orelse return error.TableNotFound;
    }

    return .{
        .status = "success",
        .upserted = if (req.dry_run) 0 else changed_writes.items.len,
        .deleted = deleted_ids.items.len,
        .skipped = skipped,
        .next_cursor = next_cursor,
        .key_range = .{
            .from = req.last_merged_id,
            .to = next_cursor,
        },
        .keys_scanned = keys_scanned,
        .deleted_ids = if (req.dry_run) deleted_ids.items else null,
        .message = if (req.dry_run) "dry run - no changes made" else null,
    };
}

fn encodeResponse(alloc: std.mem.Allocator, resp: Response) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"status\":{f},\"upserted\":{d},\"deleted\":{d},\"skipped\":{d},\"next_cursor\":{f},\"key_range\":{{\"from\":{f},\"to\":{f}}},\"keys_scanned\":{d}",
        .{
            std.json.fmt(resp.status, .{}),
            resp.upserted,
            resp.deleted,
            resp.skipped,
            std.json.fmt(resp.next_cursor, .{}),
            std.json.fmt(resp.key_range.from, .{}),
            std.json.fmt(resp.key_range.to, .{}),
            resp.keys_scanned,
        },
    );
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);
    if (resp.deleted_ids) |deleted_ids| {
        try out.appendSlice(alloc, ",\"deleted_ids\":[");
        for (deleted_ids, 0..) |key, i| {
            if (i > 0) try out.append(alloc, ',');
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
        try out.append(alloc, ']');
    }
    if (resp.message) |message| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"message\":{f}", .{std.json.fmt(message, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn parseSyncLevel(value: std.json.Value) !db_mod.types.SyncLevel {
    const text = switch (value) {
        .string => |v| v,
        .null => return .propose,
        else => return error.InvalidLinearMergeRequest,
    };
    return db_mod.types.parsePublicSyncLevelText(text) orelse error.InvalidLinearMergeRequest;
}

fn parseScanLineKey(alloc: std.mem.Allocator, line: []const u8) ![]const u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, line, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidLinearMergeRequest;
    const key_value = parsed.value.object.get("key") orelse return error.InvalidLinearMergeRequest;
    if (key_value != .string) return error.InvalidLinearMergeRequest;
    return try alloc.dupe(u8, key_value.string);
}

fn batchWriteLessThan(_: void, lhs: db_mod.types.BatchWrite, rhs: db_mod.types.BatchWrite) bool {
    return std.mem.lessThan(u8, lhs.key, rhs.key);
}

fn jsonDocumentsEqualIgnoringTimestamp(alloc: std.mem.Allocator, left_raw: []const u8, right_raw: []const u8) !bool {
    var left = try std.json.parseFromSlice(std.json.Value, alloc, left_raw, .{});
    defer left.deinit();
    var right = try std.json.parseFromSlice(std.json.Value, alloc, right_raw, .{});
    defer right.deinit();
    return jsonValuesEqualIgnoringTimestamp(left.value, right.value);
}

fn jsonValuesEqualIgnoringTimestamp(left: std.json.Value, right: std.json.Value) bool {
    return switch (left) {
        .null => right == .null,
        .bool => |v| right == .bool and right.bool == v,
        .integer => |v| right == .integer and right.integer == v,
        .float => |v| right == .float and right.float == v,
        .number_string => |v| right == .number_string and std.mem.eql(u8, right.number_string, v),
        .string => |v| right == .string and std.mem.eql(u8, right.string, v),
        .array => |arr| blk: {
            if (right != .array or arr.items.len != right.array.items.len) break :blk false;
            for (arr.items, right.array.items) |lhs, rhs| {
                if (!jsonValuesEqualIgnoringTimestamp(lhs, rhs)) break :blk false;
            }
            break :blk true;
        },
        .object => |obj| blk: {
            if (right != .object) break :blk false;
            if (comparableObjectFieldCount(obj) != comparableObjectFieldCount(right.object)) break :blk false;
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (isIgnoredSystemField(entry.key_ptr.*)) continue;
                const other = right.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValuesEqualIgnoringTimestamp(entry.value_ptr.*, other)) break :blk false;
            }
            var right_it = right.object.iterator();
            while (right_it.next()) |entry| {
                if (isIgnoredSystemField(entry.key_ptr.*)) continue;
                if (obj.get(entry.key_ptr.*) == null) break :blk false;
            }
            break :blk true;
        },
    };
}

fn comparableObjectFieldCount(obj: std.json.ObjectMap) usize {
    var count: usize = 0;
    var it = obj.iterator();
    while (it.next()) |entry| {
        if (isIgnoredSystemField(entry.key_ptr.*)) continue;
        count += 1;
    }
    return count;
}

fn isIgnoredSystemField(field: []const u8) bool {
    return std.mem.eql(u8, field, "_timestamp");
}

test "linear merge request parser sorts keys and accepts sync level aliases" {
    var req = try parseRequest(std.testing.allocator,
        \\{"records":{"doc:b":{"title":"bravo"},"doc:a":{"title":"alpha"}},"sync_level":"full_text"}
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), req.writes.len);
    try std.testing.expectEqualStrings("doc:a", req.writes[0].key);
    try std.testing.expectEqualStrings("doc:b", req.writes[1].key);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_text, req.sync_level);
}

test "linear merge equality ignores system timestamp" {
    try std.testing.expect(try jsonDocumentsEqualIgnoringTimestamp(std.testing.allocator,
        \\{"title":"alpha","content":"same"}
    ,
        \\{"title":"alpha","content":"same","_timestamp":1234}
    ));
    try std.testing.expect(!(try jsonDocumentsEqualIgnoringTimestamp(std.testing.allocator,
        \\{"title":"alpha","content":"same"}
    ,
        \\{"title":"alpha","content":"different","_timestamp":1234}
    )));
}
