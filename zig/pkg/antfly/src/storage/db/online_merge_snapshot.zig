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

//! Bounded logical row pages from a verified immutable source artifact. Archive
//! positions are committed with receiver row receipts; no primary scan or live
//! recapture is permitted. A replica-local cache only avoids repeated decoding
//! of the same oversized row and is never authoritative progress.
const std = @import("std");
const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
const source = @import("online_source.zig");
const pages = @import("merge_page_contract.zig");
const types = @import("types.zig");
const verifier = @import("../portable_source_verifier.zig");
const Certificate = @import("../source_snapshot.zig").Certificate;
const Allocator = std.mem.Allocator;

pub const Cache = struct {
    arena: ?std.heap.ArenaAllocator = null,
    sequence: ?u64 = null,
    position: pages.SnapshotPosition = .{ .object = 0, .offset = 0, .remaining = 0 },
    request: ?types.BatchRequest = null,
    chunks: ?pages.RowChunks(types.BatchRequest) = null,
    pub fn clear(self: *Cache) void {
        if (self.arena) |*arena| arena.deinit();
        self.* = .{};
    }
};

fn output(alloc: Allocator, scope: source.Scope, cache: *Cache, receipt: pages.Progress) ![]u8 {
    const request = if (cache.chunks) |chunks| try chunks.requestAt(if (receipt.assembly) |assembly| assembly.next_offset else 0) else cache.request;
    return std.json.Stringify.valueAlloc(alloc, @import("online_merge_io_contract.zig").Prepared{ .scope = scope, .request = request }, .{});
}

fn exact(reader: *verifier.ObjectReader, object: u32, offset: u64, bytes: []u8) !void {
    var read: usize = 0;
    while (read < bytes.len) {
        const n = try reader.readAt(object, try std.math.add(u64, offset, read), bytes[read..]);
        if (n == 0) return error.SourceSnapshotCorrupt;
        read += n;
    }
}
fn number(comptime T: type, reader: *verifier.ObjectReader, object: u32, offset: u64) !T {
    var bytes: [@sizeOf(T)]u8 = undefined;
    try exact(reader, object, offset, &bytes);
    return std.mem.readInt(T, &bytes, .little);
}

pub fn executeJson(db: *DB, alloc: Allocator, scope: source.Scope, receipt: pages.Progress, certificate: Certificate, cancellation: types.CancellationToken) ![]u8 {
    try @import("online_merge_io.zig").requireSnapshotIndexes(db, alloc, receipt.source, certificate);
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const shared = &db.online_merge_reader;
    try shared.mutex.lock(io);
    defer shared.mutex.unlock(io);
    const progress = try db.onlineSourceStatus(scope);
    if (progress.phase == .released or progress.snapshot_phase != .published or !std.mem.eql(u8, &progress.snapshot_certificate, &try certificate.digest())) return error.SourceSnapshotCutMismatch;
    if (shared.scope == null or !std.meta.eql(shared.scope.?, scope)) {
        shared.snapshot.clear();
        shared.scope = scope;
    }
    const cache = &shared.snapshot;
    if (cache.sequence == null or cache.sequence.? != receipt.sequence) {
        cache.clear();
        cache.sequence = receipt.sequence;
        cache.position = receipt.snapshot_position orelse .{ .object = 0, .offset = 0, .remaining = 0 };
    }
    if (cache.request != null) return output(alloc, scope, cache, receipt);
    const pins = @import("source_pin.zig");
    const lock_path = try pins.lockPath(alloc, db.core.path, (try pins.locate(db, scope)).slot);
    defer alloc.free(lock_path);
    var lease = try @import("native_backup_seal.zig").StoreLock.acquire(alloc, io, lock_path, cancellation);
    defer lease.deinit();
    if ((try db.onlineSourceStatus(scope)).phase == .released) return error.OnlineSourceScopeChanged;
    const root = try pins.pathAlloc(alloc, db.core.path, scope);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/source.afb2", .{root});
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    // Original donor artifacts and transferred replica artifacts use the same
    // durable verifier. Each call performs at most one bounded verifier slice.
    if (!(try verifier.step(alloc, io, file, root, scope.pin(), certificate, cancellation, .{})).complete)
        return output(alloc, scope, cache, receipt);
    var reader = try verifier.ObjectReader.open(alloc, io, file, root, scope.pin(), certificate);
    defer reader.deinit();
    var arena = std.heap.ArenaAllocator.init(db.alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var writes: std.ArrayList(types.BatchWrite) = .empty;
    var timestamps: std.ArrayList(u64) = .empty;
    var integrity: std.ArrayList(pages.IntegrityEffect) = .empty;
    var position = cache.position;
    var last_row_position: ?pages.SnapshotPosition = null;
    var bytes: usize = 0;
    var work: usize = 0;
    while (receipt.phase == .artifacts and receipt.source.integrity != null and position.object < reader.objectCount() and integrity.items.len < pages.max_rows and work < pages.max_rows) : (work += 1) {
        try cancellation.check();
        const object = try reader.object(position.object);
        if (object.kind != .integrity_batch) {
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        if (position.offset == 0) {
            position.remaining = try number(u32, &reader, position.object, 0);
            position.offset = 4;
        }
        if (position.remaining == 0) {
            if (position.offset != object.size) return error.SourceSnapshotCorrupt;
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        const key_len = try number(u32, &reader, position.object, position.offset);
        const contract = @import("relational_integrity_contract.zig");
        if (key_len != contract.key_len and key_len != contract.key_len + 32) return error.SourceSnapshotCorrupt;
        const key_offset = try std.math.add(u64, position.offset, 4);
        const length_offset = try std.math.add(u64, key_offset, key_len);
        const value_len = try number(u32, &reader, position.object, length_offset);
        if (value_len > contract.max_record_bytes + 256) return error.SourceSnapshotCorrupt;
        if (integrity.items.len != 0 and @as(usize, key_len) +| value_len > pages.max_bytes -| bytes) break;
        const value_offset = try std.math.add(u64, length_offset, 4);
        const end = try std.math.add(u64, value_offset, value_len);
        if (end > object.size) return error.SourceSnapshotCorrupt;
        const key = try owned.alloc(u8, key_len);
        try exact(&reader, position.object, key_offset, key);
        const previous = if (integrity.items.len == 0) receipt.cursor else integrity.items[integrity.items.len - 1].key;
        if (std.mem.order(u8, previous, key) != .lt) return error.SourceSnapshotCorrupt;
        const value = try owned.alloc(u8, value_len);
        try exact(&reader, position.object, value_offset, value);
        _ = try contract.validateTransferRecord(key, value);
        try integrity.append(owned, .{ .key = key, .value = value });
        bytes +|= key.len +| value.len;
        position.offset = end;
        position.remaining -= 1;
        last_row_position = position;
        if (bytes >= pages.max_bytes) break;
    }
    while (receipt.phase == .rows and position.object < reader.objectCount() and writes.items.len < pages.max_rows and work < pages.max_rows) : (work += 1) {
        try cancellation.check();
        const object = try reader.object(position.object);
        if (object.kind != .document_batch) {
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        if (position.offset == 0) {
            position.remaining = try number(u32, &reader, position.object, 0);
            position.offset = 4;
        }
        if (position.remaining == 0) {
            if (position.offset != object.size) return error.SourceSnapshotCorrupt;
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        const key_len = try number(u32, &reader, position.object, position.offset);
        if (key_len == 0 or key_len > pages.max_cursor_bytes) return error.SourceSnapshotCorrupt;
        const key_offset = try std.math.add(u64, position.offset, 4);
        const flags_offset = try std.math.add(u64, key_offset, key_len);
        const flags = try number(u8, &reader, position.object, flags_offset);
        if (flags != 0 and flags != 2) return error.SourceSnapshotCorrupt;
        const value_len = try number(u32, &reader, position.object, flags_offset + 1);
        if (writes.items.len != 0 and @as(usize, key_len) +| value_len > pages.max_bytes -| bytes) break;
        const value_offset = try std.math.add(u64, flags_offset, 5);
        const end = try std.math.add(u64, value_offset, @as(u64, value_len) + 8);
        if (end > object.size) return error.SourceSnapshotCorrupt;
        const key = try owned.alloc(u8, key_len);
        try exact(&reader, position.object, key_offset, key);
        const previous = if (writes.items.len == 0) receipt.cursor else writes.items[writes.items.len - 1].key;
        if (std.mem.order(u8, previous, key) != .lt) return error.SourceSnapshotCorrupt;
        const raw = try owned.alloc(u8, value_len);
        try exact(&reader, position.object, value_offset, raw);
        const timestamp = try number(u64, &reader, position.object, value_offset + value_len);
        const value = if (flags == 2) decoded: {
            const version = try @import("relational_store.zig").rowSchemaVersion(raw);
            var view = (try db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            defer view.release();
            const row = try @import("algebraic/relational_row_codec.zig").ordinalRowViewSelective(raw, view.tableSchema().*, view.physicalLayout());
            if (row.writeTimestampNs() != timestamp) return error.SourceSnapshotCorrupt;
            break :decoded try row.reconstructValueAlloc(owned);
        } else raw;
        if (writes.items.len != 0 and key.len +| value.len > pages.max_bytes -| bytes) break;
        try writes.append(owned, .{ .key = key, .value = value });
        try timestamps.append(owned, timestamp);
        bytes +|= key.len +| value.len;
        position.offset = end;
        position.remaining -= 1;
        last_row_position = position;
        if (bytes >= pages.max_bytes) break;
    }
    if ((receipt.phase == .rows or (receipt.phase == .artifacts and receipt.source.integrity != null)) and writes.items.len == 0 and integrity.items.len == 0 and position.object < reader.objectCount()) {
        cache.position = position;
        arena.deinit();
        return output(alloc, scope, cache, receipt);
    }
    const last = if (writes.items.len != 0) writes.items[writes.items.len - 1].key else if (integrity.items.len != 0) integrity.items[integrity.items.len - 1].key else "";
    var request: types.BatchRequest = .{
        .merge_replication = .{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt },
        .writes = writes.items,
        .merge_page = .{ .source = receipt.source, .sequence = try std.math.add(u64, receipt.sequence, 1), .phase = receipt.phase, .after = try owned.dupe(u8, receipt.cursor), .next = last, .exhausted = writes.items.len == 0 and integrity.items.len == 0, .digest = @splat(0), .timestamps = timestamps.items, .next_snapshot_position = last_row_position, .integrity = integrity.items },
    };
    // Artifact phase is permitted only by the row-derived schema capability
    // admission guard; authoritative graph/vector artifacts are not discarded.
    request.merge_page.?.digest = pages.commandDigest(request);
    try pages.validateRequest(request);
    if (writes.items.len == 1 and bytes > pages.max_bytes) cache.chunks = try pages.RowChunks(types.BatchRequest).init(request);
    cache.request = request;
    cache.arena = arena;
    return output(alloc, scope, cache, receipt);
}

test "relational index system online snapshot locator resumes immutable rows and chunk receipts after owner reopen" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |relational| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/snapshot", .{tmp.sub_path});
        defer alloc.free(path);
        const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
        var large = try alloc.alloc(u8, 2 * pages.max_bytes + 16);
        defer alloc.free(large);
        @memset(large, 'x');
        @memcpy(large[0..6], "{\"v\":\"");
        @memcpy(large[large.len - 2 ..], "\"}");
        var scope: source.Scope = undefined;
        var certificate: Certificate = undefined;
        {
            var db = try DB.open(alloc, path, options);
            defer db.close();
            if (relational) try db.setSchemaJson(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"v":{"type":"string"}},"additionalProperties":false}}}}
            );
            try db.batchRaftReplicatedApply(.{ .timestamp_ns = 111, .writes = &.{ .{ .key = "a", .value = large }, .{ .key = "b", .value = "{\"v\":\"second\"}" } } }, .{ .term = 1, .index = 1 });
            const identity = try db.relationalTopologyIdentity();
            scope = .{ .fence = .{ .role = .merge_source, .transition_id = 77, .attempt = 1, .admission_epoch = identity.next_epoch, .peer_group_id = 3, .owner_group_id = 2, .namespace = identity.namespace, .catalog_digest = identity.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
            try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 2 });
            certificate = try db.prepareOnlineSourcePublication(scope, .none);
            try db.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 1, .index = 3 });
            try db.batchRaftReplicatedApply(.{ .timestamp_ns = 222, .writes = &.{.{ .key = "a", .value = "{\"v\":\"changed\"}" }} }, .{ .term = 1, .index = 4 });
        }
        var db = try DB.open(alloc, path, options);
        defer db.close();
        var receipt: pages.Progress = .{ .version = 2, .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .receiver_namespace = scope.receiver_namespace, .attempt = scope.copy_attempt, .source = .{ .namespace = scope.fence.namespace, .pin_digest = try certificate.digest(), .applied_index = certificate.cut.applied_index, .retention = .{ .epoch = 1, .after_sequence = certificate.cut.retained_start } }, .phase = .rows, .tail_sequence = certificate.cut.retained_start };
        var receipt_arena = std.heap.ArenaAllocator.init(alloc);
        defer receipt_arena.deinit();
        var chunks: usize = 0;
        var rows: usize = 0;
        var loops: usize = 0;
        var restarted = false;
        while (receipt.phase == .rows and loops < 200) : (loops += 1) {
            const raw = try executeJson(&db, alloc, scope, receipt, certificate, .none);
            defer alloc.free(raw);
            var parsed = try std.json.parseFromSlice(@import("online_merge_io_contract.zig").Prepared, alloc, raw, .{});
            defer parsed.deinit();
            const request = parsed.value.request orelse continue;
            try pages.validateRequest(request);
            const old_position = receipt.snapshot_position;
            if (request.merge_page.?.chunk) |chunk| {
                try std.testing.expectEqualStrings("a", chunk.row_key);
                try std.testing.expectEqual(@as(u64, 111), chunk.timestamp);
                try std.testing.expectEqualSlices(u8, large[@intCast(chunk.offset)..][0..chunk.data.len], chunk.data);
                chunks += 1;
            } else {
                for (request.writes) |row| {
                    try std.testing.expectEqualStrings("b", row.key);
                    try std.testing.expectEqualStrings("{\"v\":\"second\"}", row.value);
                    rows += 1;
                }
            }
            const next = (try pages.plan(receipt, request)).apply;
            if (request.merge_page.?.chunk) |chunk| if (!chunk.complete()) try std.testing.expectEqualDeep(old_position, next.snapshot_position);
            const encoded = try pages.encode(receipt_arena.allocator(), next);
            receipt = try std.json.parseFromSliceLeaky(pages.Progress, receipt_arena.allocator(), encoded, .{ .allocate = .alloc_always });
            if (chunks == 1 and !restarted) {
                // Drop all replica-local row/session caches midassembly. The next
                // request locates the same immutable row using the durable receipt.
                db.close();
                db = try DB.open(alloc, path, options);
                restarted = true;
            }
        }
        try std.testing.expect(loops < 200);
        try std.testing.expect(chunks > 1);
        try std.testing.expectEqual(@as(usize, 1), rows);
        try std.testing.expectEqual(.artifacts, receipt.phase);
        try std.testing.expect(receipt.snapshot_position == null);
    }
}
