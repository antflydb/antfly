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

const std = @import("std");
const api_operation = @import("antfly_local_sources").api_operation;
const db_mod = @import("antfly_local_sources").storage_db_selected_root.db;
const document_content_hash = @import("antfly_local_sources").storage_db_document_content_hash;
const raft_mod = @import("../raft/mod.zig");
const table_reads = @import("antfly_local_sources").api_table_read_source;
const table_writes = @import("antfly_local_sources").api_table_write_source;
const query_api = @import("antfly_local_sources").api_query;
const public_limits = @import("antfly_public_limits");
const platform_time = @import("antfly_platform").time;

pub const OwnedLinearMergeRequest = struct {
    writes: []db_mod.types.BatchWrite = &.{},
    last_merged_id: []const u8 = "",
    // Scoped envelopes are rejected by legacy parsers (no outer records field).
    // Both bounds are exclusive; an empty-record cleanup stops at scope_to.
    scope_from: []const u8 = "",
    scope_to: []const u8 = "",
    retain_ids: []const []const u8 = &.{},
    delete_missing: bool = true,
    dry_run: bool = false,
    sync_level: db_mod.types.SyncLevel = .write,

    pub fn deinit(self: *OwnedLinearMergeRequest, alloc: std.mem.Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        if (self.last_merged_id.len > 0) alloc.free(self.last_merged_id);
        if (self.scope_from.len > 0) alloc.free(self.scope_from);
        if (self.scope_to.len > 0) alloc.free(self.scope_to);
        for (self.retain_ids) |id| alloc.free(id);
        if (self.retain_ids.len > 0) alloc.free(self.retain_ids);
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

    const outer = parsed.value.object;
    const scoped_value = outer.get("scoped_merge");
    if (scoped_value != null and (outer.count() != 1 or scoped_value.? != .object)) return error.InvalidLinearMergeRequest;
    const root = if (scoped_value) |value| value.object else outer;
    if (scoped_value == null and (root.contains("key_range") or root.contains("delete_missing") or root.contains("retain_ids"))) return error.InvalidLinearMergeRequest;
    var scope_from: []const u8 = "";
    errdefer if (scope_from.len > 0) alloc.free(scope_from);
    var scope_to: []const u8 = "";
    errdefer if (scope_to.len > 0) alloc.free(scope_to);
    if (scoped_value != null) {
        const scope = root.get("key_range") orelse return error.InvalidLinearMergeRequest;
        if (scope != .object) return error.InvalidLinearMergeRequest;
        const from = scope.object.get("from") orelse return error.InvalidLinearMergeRequest;
        const to = scope.object.get("to") orelse return error.InvalidLinearMergeRequest;
        if (from != .string or to != .string or from.string.len == 0 or to.string.len == 0 or !std.mem.lessThan(u8, from.string, to.string)) return error.InvalidLinearMergeRequest;
        scope_from = try alloc.dupe(u8, from.string);
        scope_to = try alloc.dupe(u8, to.string);
    }
    const delete_missing = if (scoped_value != null) blk: {
        const value = root.get("delete_missing") orelse break :blk false;
        if (value != .bool) return error.InvalidLinearMergeRequest;
        break :blk value.bool;
    } else true;
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
        // Linear merge is a public document-ingestion surface just like
        // BatchRequest.inserts. Reject scalars before hashing or storage so a
        // successful merge can always be represented as QueryHit._source.
        if (entry.value_ptr.* != .object) return error.InvalidLinearMergeRequest;
        writes[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{}),
        };
        initialized += 1;
    }
    std.sort.heap(db_mod.types.BatchWrite, writes, {}, batchWriteLessThan);

    const last_merged_id = if (root.get("last_merged_id")) |value| switch (value) {
        .null => try alloc.dupe(u8, scope_from),
        .string => |text| try alloc.dupe(u8, text),
        else => return error.InvalidLinearMergeRequest,
    } else try alloc.dupe(u8, scope_from);
    errdefer if (last_merged_id.len > 0) alloc.free(last_merged_id);

    if (scoped_value != null and (std.mem.lessThan(u8, last_merged_id, scope_from) or !std.mem.lessThan(u8, last_merged_id, scope_to))) return error.InvalidLinearMergeRequest;
    var retain_ids: []const []const u8 = &.{};
    errdefer {
        for (retain_ids) |id| alloc.free(id);
        if (retain_ids.len > 0) alloc.free(retain_ids);
    }
    if (root.get("retain_ids")) |ids| {
        if (scoped_value == null or writes.len != 0 or ids != .array or ids.array.items.len > 100000) return error.InvalidLinearMergeRequest;
        var retained = std.ArrayList([]const u8).empty;
        errdefer {
            for (retained.items) |id| alloc.free(id);
            retained.deinit(alloc);
        }
        for (ids.array.items) |id| {
            if (id != .string or !std.mem.lessThan(u8, last_merged_id, id.string) or !std.mem.lessThan(u8, id.string, scope_to)) return error.InvalidLinearMergeRequest;
            try retained.append(alloc, try alloc.dupe(u8, id.string));
        }
        std.sort.heap([]const u8, retained.items, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.less);
        for (retained.items, 0..) |id, i| {
            if (i > 0 and std.mem.eql(u8, retained.items[i - 1], id)) return error.InvalidLinearMergeRequest;
        }
        retain_ids = try retained.toOwnedSlice(alloc);
    }

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
        if (scope_to.len > 0 and !std.mem.lessThan(u8, write.key, scope_to)) return error.InvalidLinearMergeRequest;
        if (last_merged_id.len > 0 and !std.mem.lessThan(u8, last_merged_id, write.key)) {
            return error.InvalidLinearMergeRequest;
        }
    }

    return .{
        .writes = writes,
        .last_merged_id = last_merged_id,
        .scope_from = scope_from,
        .scope_to = scope_to,
        .retain_ids = retain_ids,
        .delete_missing = delete_missing,
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
    failed: []const FailedOperation = &.{},
    took: u64,
    deleted_ids: ?[]const []const u8 = null,
    message: ?[]const u8 = null,
};

pub const FailedOperation = struct {
    id: []const u8,
    operation: []const u8,
    @"error": []const u8,
};

pub fn executeResponse(
    alloc: std.mem.Allocator,
    reads: table_reads.TableReadSource,
    writes: table_writes.TableWriteSource,
    table_name: []const u8,
    req: OwnedLinearMergeRequest,
    request: api_operation.RequestContext,
) !Response {
    const started_ns = platform_time.monotonicNs();
    try request.ensureActive();
    var changed_writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer changed_writes.deinit(alloc);

    try request.ensureActive();
    const next_cursor = if (req.writes.len > 0) req.writes[req.writes.len - 1].key else req.last_merged_id;
    const cleanup = req.writes.len == 0 and req.scope_to.len > 0;
    const scan_to = if (req.writes.len > 0) next_cursor else req.scope_to;
    var scanned = (try reads.scanContentHashes(
        alloc,
        table_name,
        req.last_merged_id,
        scan_to,
        .{
            .inclusive_from = false,
            .exclusive_to = cleanup,
        },
        .read_index,
    )) orelse return error.TableNotFound;
    defer scanned.deinit(alloc);
    if (req.scope_to.len > 0) {
        var prior = req.last_merged_id;
        for (scanned.entries) |entry| {
            if (!std.mem.lessThan(u8, prior, entry.id) or !std.mem.lessThan(u8, entry.id, req.scope_to) or (req.writes.len > 0 and std.mem.lessThan(u8, scan_to, entry.id))) return error.InvalidLinearMergeRequest;
            prior = entry.id;
        }
    }

    var deleted_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deleted_ids.items) |key| alloc.free(@constCast(key));
        deleted_ids.deinit(alloc);
    }

    var skipped: usize = 0;
    var incoming_index: usize = 0;
    var stored_index: usize = 0;
    var compared: usize = 0;
    if (cleanup) {
        // A metadata-only final reconciliation keeps already verified rows.
        // Missing retained IDs abort before the irreversible batch boundary.
        var retained_index: usize = 0;
        for (scanned.entries, 0..) |stored, i| {
            if (i % 64 == 0) try request.ensureActive();
            if (retained_index < req.retain_ids.len) {
                switch (std.mem.order(u8, req.retain_ids[retained_index], stored.id)) {
                    .lt => return error.InvalidLinearMergeRequest,
                    .eq => {
                        retained_index += 1;
                        skipped += 1;
                        continue;
                    },
                    .gt => {},
                }
            }
            if (req.delete_missing) try deleted_ids.append(alloc, try alloc.dupe(u8, stored.id));
        }
        if (retained_index != req.retain_ids.len) return error.InvalidLinearMergeRequest;
    } else while (incoming_index < req.writes.len or stored_index < scanned.entries.len) : (compared += 1) {
        if (compared % 64 == 0) try request.ensureActive();
        if (incoming_index == req.writes.len) {
            if (req.delete_missing) try deleted_ids.append(alloc, try alloc.dupe(u8, scanned.entries[stored_index].id));
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
                if (req.delete_missing) try deleted_ids.append(alloc, try alloc.dupe(u8, stored.id));
                stored_index += 1;
                continue;
            },
        }
        incoming_index += 1;
    }

    var response_status: []const u8 = "success";
    if (!req.dry_run and (changed_writes.items.len > 0 or deleted_ids.items.len > 0)) {
        // The batch call is the irreversible write boundary. Never report
        // cancellation after it begins because the outcome may be durable.
        try request.ensureActive();
        const accepted = writes.batch(alloc, table_name, .{
            .writes = changed_writes.items,
            .deletes = deleted_ids.items,
            .sync_level = req.sync_level,
        }) catch |err| switch (err) {
            error.CommitVisibilityNotSatisfied,
            error.CommitPropagationIncomplete,
            error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress,
            => blk: {
                response_status = "committed_pending";
                break :blk @as(?void, {});
            },
            error.EnrichmentWorkerFailed => blk: {
                response_status = "committed_repair_required";
                break :blk @as(?void, {});
            },
            else => return err,
        };
        _ = accepted orelse return error.TableNotFound;
    }

    return .{
        .status = response_status,
        .upserted = if (req.dry_run) 0 else changed_writes.items.len,
        .deleted = deleted_ids.items.len,
        .skipped = skipped,
        .next_cursor = next_cursor,
        .key_range = .{
            .from = req.last_merged_id,
            .to = if (cleanup) req.scope_to else next_cursor,
        },
        .keys_scanned = scanned.entries.len,
        .failed = &.{},
        .took = platform_time.monotonicNs() -| started_ns,
        .deleted_ids = if (req.dry_run) deleted_ids.items else null,
        .message = if (req.dry_run) "dry run - no changes made" else null,
    };
}

fn encodeResponse(alloc: std.mem.Allocator, resp: Response) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"status\":{f},\"upserted\":{d},\"deleted\":{d},\"skipped\":{d},\"next_cursor\":{f},\"key_range\":{{\"from\":{f},\"to\":{f}}},\"keys_scanned\":{d},\"failed\":[],\"took\":{d}",
        .{
            std.json.fmt(resp.status, .{}),
            resp.upserted,
            resp.deleted,
            resp.skipped,
            std.json.fmt(resp.next_cursor, .{}),
            std.json.fmt(resp.key_range.from, .{}),
            std.json.fmt(resp.key_range.to, .{}),
            resp.keys_scanned,
            resp.took,
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

test "linear merge request parser rejects non-object records" {
    inline for (.{
        \\{"records":{"doc:a":"text"}}
        ,
        \\{"records":{"doc:a":42}}
        ,
        \\{"records":{"doc:a":[1,2]}}
        ,
        \\{"records":{"doc:a":null}}
        ,
    }) |body| {
        try std.testing.expectError(
            error.InvalidLinearMergeRequest,
            parseRequest(std.testing.allocator, body),
        );
    }
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
        failure: ?anyerror = null,

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{ .ptr = self, .vtable = &.{ .batch = batch } };
        }

        fn batch(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, req: db_mod.types.BatchRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.writes += req.writes.len;
            self.deletes += req.deletes.len;
            self.sync_level = req.sync_level;
            if (self.failure) |err| return err;
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
    reads.cancel_after_scan = null;
    for ([_]anyerror{ error.EnrichmentWaitCanceled, error.EnrichmentWaitTimeout, error.EnrichmentRetryInProgress, error.CommitVisibilityNotSatisfied, error.CommitPropagationIncomplete, error.EnrichmentWorkerFailed }) |err| {
        writes.failure = err;
        const committed = try executeResponse(std.testing.allocator, reads.source(), writes.source(), "docs", request, .{});
        try std.testing.expectEqualStrings(if (err == error.EnrichmentWorkerFailed) "committed_repair_required" else "committed_pending", committed.status);
        try std.testing.expectEqual(response.upserted, committed.upserted);
        try std.testing.expectEqual(response.deleted, committed.deleted);
        try std.testing.expectEqualStrings(response.next_cursor, committed.next_cursor);
    }
    for ([_]anyerror{ error.VersionConflict, error.RaftBatchWriteOutcomeUnknown, error.Canceled }) |err| {
        writes.failure = err;
        try std.testing.expectError(err, executeResponse(std.testing.allocator, reads.source(), writes.source(), "docs", request, .{}));
    }
}

test "linear merge scoped parser requires exclusive bounds and a safe envelope" {
    const a = std.testing.allocator;
    var request = try parseRequest(a,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{"source/a":{"value":1}}}}
    );
    defer request.deinit(a);
    try std.testing.expect(!request.delete_missing);
    try std.testing.expectEqualStrings("source/", request.last_merged_id);
    for ([_][]const u8{
        \\{"scoped_merge":{"records":{}}}
        ,
        \\{"scoped_merge":{"key_range":{"from":"z","to":"a"},"records":{}}}
        ,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{"other/a":{}}}}
        ,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"last_merged_id":"other","records":{}}}
        ,
        \\{"records":{},"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{}}}
        ,
        \\{"records":{"source/a":{}},"delete_missing":false}
        ,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{},"retain_ids":["source/a","source/a"]}}
        ,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{"source/a":{}},"retain_ids":["source/a"]}}
        ,
    }) |body| try std.testing.expectError(error.InvalidLinearMergeRequest, parseRequest(a, body));
}

const ScopedMergeFixture = struct {
    ndjson: []const u8,
    calls: usize = 0,
    inserted: usize = 0,
    deleted: usize = 0,
    expected_to: []const u8,
    exclusive_to: bool,
    fn reads(self: *@This()) table_reads.TableReadSource {
        return .{ .ptr = self, .vtable = &.{ .lookup = lookup, .scan = scan, .query = query } };
    }
    fn writes(self: *@This()) table_writes.TableWriteSource {
        return .{ .ptr = self, .vtable = &.{ .batch = batch } };
    }
    fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
        return null;
    }
    fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
        return null;
    }
    fn scan(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, from: []const u8, to: []const u8, opts: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqualStrings("source/", from);
        try std.testing.expectEqualStrings(self.expected_to, to);
        try std.testing.expect(!opts.inclusive_from);
        try std.testing.expectEqual(self.exclusive_to, opts.exclusive_to);
        return .{ .ndjson = try a.dupe(u8, self.ndjson) };
    }
    fn batch(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, request: db_mod.types.BatchRequest) !?void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        for (request.deletes) |id| try std.testing.expect(std.mem.startsWith(u8, id, "source/"));
        self.calls += 1;
        self.inserted += request.writes.len;
        self.deleted += request.deletes.len;
        return {};
    }
};

test "linear merge scoped updates preserve gaps and finalize only the bounded retained inventory" {
    const a = std.testing.allocator;
    const hash = std.fmt.bytesToHex(try document_content_hash.hashJson(a, "{\"value\":1}"), .lower);
    const ndjson = try std.fmt.allocPrint(a, "{{\"_id\":\"source/a\",\"_content_hash\":\"{s}\"}}\n{{\"_id\":\"source/c\",\"_content_hash\":\"{s}\"}}\n", .{ &hash, &hash });
    defer a.free(ndjson);
    var fixture = ScopedMergeFixture{ .ndjson = ndjson, .expected_to = "source/c", .exclusive_to = false };
    var request = try parseRequest(a,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{"source/b":{"value":2},"source/c":{"value":1}},"delete_missing":false}}
    );
    defer request.deinit(a);
    const updated = try executeResponse(a, fixture.reads(), fixture.writes(), "docs", request, .{});
    try std.testing.expectEqual(@as(usize, 1), updated.upserted);
    try std.testing.expectEqual(@as(usize, 1), updated.skipped);
    try std.testing.expectEqual(@as(usize, 0), updated.deleted);
    fixture.expected_to = "source0";
    fixture.exclusive_to = true;
    var cleanup = try parseRequest(a,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{},"retain_ids":["source/a"],"delete_missing":true}}
    );
    defer cleanup.deinit(a);
    const finalized = try executeResponse(a, fixture.reads(), fixture.writes(), "docs", cleanup, .{});
    try std.testing.expectEqual(@as(usize, 1), finalized.deleted);
    try std.testing.expectEqual(@as(usize, 1), finalized.skipped);
    try std.testing.expectEqualStrings("source0", finalized.key_range.to);
    try std.testing.expectEqualStrings("source/", finalized.next_cursor);
    var missing = try parseRequest(a,
        \\{"scoped_merge":{"key_range":{"from":"source/","to":"source0"},"records":{},"retain_ids":["source/b"],"delete_missing":true}}
    );
    defer missing.deinit(a);
    try std.testing.expectError(error.InvalidLinearMergeRequest, executeResponse(a, fixture.reads(), fixture.writes(), "docs", missing, .{}));
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    fixture.ndjson = try std.fmt.allocPrint(a, "{{\"_id\":\"other/source\",\"_content_hash\":\"{s}\"}}\n", .{&hash});
    defer a.free(fixture.ndjson);
    try std.testing.expectError(error.InvalidLinearMergeRequest, executeResponse(a, fixture.reads(), fixture.writes(), "docs", cleanup, .{}));
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
}
