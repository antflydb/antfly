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
const api_operation = @import("operation.zig");
const db_mod = @import("../storage/db/selected_root.zig").db;
const document_content_hash = @import("../storage/db/document_content_hash.zig");
const raft_mod = @import("../raft/mod.zig");
const table_reads = @import("table_read_source.zig");
const table_writes = @import("table_write_source.zig");
const query_api = @import("query.zig");
const public_limits = @import("public_limits.zig");

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
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .max_value_len = public_limits.max_json_value_len,
    }) catch |err| switch (err) {
        error.ValueTooLong => return error.ValueTooLong,
        else => return error.InvalidLinearMergeRequest,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidLinearMergeRequest;

    const root = parsed.value.object;
    const records_value = root.get("records") orelse return error.InvalidLinearMergeRequest;
    if (records_value != .object) return error.InvalidLinearMergeRequest;

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

    if (writes.len == 0 and last_merged_id.len == 0) return error.InvalidLinearMergeRequest;

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
    const response = try executeResponse(alloc, reads, writes, table_name, req, .{});
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
    request: api_operation.RequestContext,
) !Response {
    try request.ensureActive();
    var changed_writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer changed_writes.deinit(alloc);

    try request.ensureActive();
    const next_cursor = if (req.writes.len > 0) req.writes[req.writes.len - 1].key else req.last_merged_id;
    var scanned = (try reads.scanContentHashes(
        alloc,
        table_name,
        req.last_merged_id,
        if (req.writes.len > 0) next_cursor else "",
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

    var skipped: usize = 0;
    var incoming_index: usize = 0;
    var stored_index: usize = 0;
    var compared: usize = 0;
    while (incoming_index < req.writes.len or stored_index < scanned.entries.len) : (compared += 1) {
        if (compared % 64 == 0) try request.ensureActive();
        if (incoming_index == req.writes.len) {
            try deleted_ids.append(alloc, try alloc.dupe(u8, scanned.entries[stored_index].id));
            stored_index += 1;
            continue;
        }
        if (stored_index == scanned.entries.len) {
            try changed_writes.append(alloc, req.writes[incoming_index]);
            incoming_index += 1;
            continue;
        }

        const incoming = req.writes[incoming_index];
        const stored = scanned.entries[stored_index];
        switch (std.mem.order(u8, incoming.key, stored.id)) {
            .lt => try changed_writes.append(alloc, incoming),
            .eq => {
                const incoming_hash = try document_content_hash.hashJson(alloc, incoming.value);
                if (std.mem.eql(u8, &incoming_hash, &stored.hash)) {
                    skipped += 1;
                } else {
                    try changed_writes.append(alloc, incoming);
                }
                stored_index += 1;
            },
            .gt => {
                try deleted_ids.append(alloc, try alloc.dupe(u8, stored.id));
                stored_index += 1;
                continue;
            },
        }
        incoming_index += 1;
    }

    if (!req.dry_run and (changed_writes.items.len > 0 or deleted_ids.items.len > 0)) {
        // The batch call is the irreversible write boundary. Never report
        // cancellation after it begins because the outcome may be durable.
        try request.ensureActive();
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
        .keys_scanned = scanned.entries.len,
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

fn batchWriteLessThan(_: void, lhs: db_mod.types.BatchWrite, rhs: db_mod.types.BatchWrite) bool {
    return std.mem.lessThan(u8, lhs.key, rhs.key);
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

test "linear merge request parser accepts raw payload value under public request cap" {
    const alloc = std.testing.allocator;
    const payload = try alloc.alloc(u8, 6 * 1024 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"records\":{\"doc:a\":{\"raw_payload\":\"");
    try writer.writeAll(payload);
    try writer.writeAll("\"}}}");

    var req = try parseRequest(alloc, out.written());
    defer req.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), req.writes.len);
    try std.testing.expect(std.mem.indexOf(u8, req.writes[0].value, "\"raw_payload\"") != null);
}

test "linear merge request parser accepts explicit final cleanup" {
    var req = try parseRequest(std.testing.allocator,
        \\{"records":{},"last_merged_id":"doc:z","sync_level":"write"}
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), req.writes.len);
    try std.testing.expectEqualStrings("doc:z", req.last_merged_id);
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, req.sync_level);
}

test "linear merge request parser rejects unbounded empty cleanup" {
    try std.testing.expectError(error.InvalidLinearMergeRequest, parseRequest(std.testing.allocator,
        \\{"records":{},"sync_level":"write"}
    ));
}

test "linear merge uses one ordered hash scan and delegates mutations to the HA batch source" {
    const FakeReads = struct {
        ndjson: []const u8,
        cancel_after_scan: ?*std.atomic.Value(bool) = null,
        lookups: usize = 0,
        scans: usize = 0,
        requested_content_hashes: bool = false,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{ .ptr = self, .vtable = &.{ .lookup = lookup, .scan = scan, .query = query } };
        }

        fn lookup(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookups += 1;
            return null;
        }

        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, opts: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.scans += 1;
            self.requested_content_hashes = opts.include_content_hashes;
            if (self.cancel_after_scan) |signal| signal.store(true, .release);
            return .{ .ndjson = try alloc.dupe(u8, self.ndjson) };
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return null;
        }
    };
    const RecordingWrites = struct {
        calls: usize = 0,
        writes: usize = 0,
        deletes: usize = 0,
        sync_level: ?db_mod.types.SyncLevel = null,

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{ .ptr = self, .vtable = &.{ .batch = batch } };
        }

        fn batch(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, req: db_mod.types.BatchRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.writes += req.writes.len;
            self.deletes += req.deletes.len;
            self.sync_level = req.sync_level;
            return {};
        }
    };

    const alpha_hash = try document_content_hash.hashJson(std.testing.allocator, "{\"title\":\"alpha\",\"_timestamp\":9}");
    const charlie_hash = try document_content_hash.hashJson(std.testing.allocator, "{\"title\":\"charlie\"}");
    const alpha_hex = std.fmt.bytesToHex(alpha_hash, .lower);
    const charlie_hex = std.fmt.bytesToHex(charlie_hash, .lower);
    const scan_ndjson = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"_id\":\"doc:a\",\"_content_hash\":\"{s}\"}}\n{{\"_id\":\"doc:c\",\"_content_hash\":\"{s}\"}}\n",
        .{ &alpha_hex, &charlie_hex },
    );
    defer std.testing.allocator.free(scan_ndjson);

    var request = try parseRequest(std.testing.allocator,
        \\{"records":{"doc:a":{"title":"alpha"},"doc:b":{"title":"bravo"}},"sync_level":"full_text"}
    );
    defer request.deinit(std.testing.allocator);
    var reads = FakeReads{ .ndjson = scan_ndjson };
    var writes = RecordingWrites{};
    const response = try executeResponse(std.testing.allocator, reads.source(), writes.source(), "docs", request, .{});

    try std.testing.expectEqual(@as(usize, 1), writes.calls);
    try std.testing.expectEqual(@as(usize, 1), writes.writes);
    try std.testing.expectEqual(@as(usize, 1), writes.deletes);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_text, writes.sync_level.?);
    try std.testing.expectEqual(@as(usize, 1), response.upserted);
    try std.testing.expectEqual(@as(usize, 1), response.deleted);
    try std.testing.expectEqual(@as(usize, 1), response.skipped);
    try std.testing.expectEqual(@as(usize, 2), response.keys_scanned);
    try std.testing.expectEqual(@as(usize, 0), reads.lookups);
    try std.testing.expectEqual(@as(usize, 1), reads.scans);
    try std.testing.expect(reads.requested_content_hashes);

    var cancellation = std.atomic.Value(bool).init(false);
    reads.cancel_after_scan = &cancellation;
    try std.testing.expectError(error.Canceled, executeResponse(
        std.testing.allocator,
        reads.source(),
        writes.source(),
        "docs",
        request,
        .{ .cancellation = api_operation.CancellationToken.fromAtomic(&cancellation) },
    ));
    try std.testing.expectEqual(@as(usize, 1), writes.calls);
    try std.testing.expectEqual(@as(usize, 0), reads.lookups);
    try std.testing.expectEqual(@as(usize, 2), reads.scans);
}
