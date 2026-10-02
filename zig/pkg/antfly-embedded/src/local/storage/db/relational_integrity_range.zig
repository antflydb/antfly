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

//! Explicit logical-routing split transfer. The source is frozen by the
//! caller's topology/apply fence and the destination remains unpublished.
//! Each page drops its cursor before committing a bounded destination batch;
//! no whole-memtable snapshot, primary row decode or per-parent lookup occurs.
const std = @import("std");
const time = @import("antfly_platform").time;
const integrity = @import("relational_integrity.zig");
const catalog = @import("relational_integrity_catalog.zig");
const docstore = @import("../docstore.zig");
const Allocator = std.mem.Allocator;

pub const Budget = struct { records: usize = 256, bytes: usize = 2 * 1024 * 1024, time_ns: u64 = 5 * std.time.ns_per_ms };

fn namespacePrefix(kind: integrity.Kind) [integrity.namespace.len + 1]u8 {
    var result: [integrity.namespace.len + 1]u8 = undefined;
    @memcpy(result[0..integrity.namespace.len], integrity.namespace);
    result[integrity.namespace.len] = @intFromEnum(kind);
    return result;
}

fn walk(alloc: Allocator, io: ?std.Io, source: *docstore.DocStore, destination: *docstore.DocStore, lower: []const u8, upper: []const u8, remove: bool, budget: Budget) !void {
    if (budget.records == 0 or budget.records > 4096 or budget.bytes == 0 or budget.bytes > 16 * 1024 * 1024 or budget.time_ns == 0 or budget.time_ns > std.time.ns_per_s) return error.InvalidIntegrityBudget;
    if (upper.len != 0 and std.mem.order(u8, lower, upper) != .lt) return error.InvalidRange;
    inline for (.{ integrity.Kind.claim, integrity.Kind.reference, integrity.Kind.job }) |kind| {
        const prefix = namespacePrefix(kind);
        const first = try std.mem.concat(alloc, u8, &.{ &prefix, lower });
        defer alloc.free(first);
        var continuation: [integrity.key_len + 32]u8 = undefined;
        var continuation_len: usize = 0;
        var complete = false;
        while (!complete) {
            if (io) |runtime_io| try runtime_io.checkCancel();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const owned = arena.allocator();
            var writes: std.ArrayList(docstore.KVPair) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            const started = time.monotonicNs();
            var inspected: usize = 0;
            var retained_bytes: usize = 0;
            {
                var scan = try source.beginCurrentScanTxn();
                defer scan.abort();
                var cursor = try scan.openCursor();
                defer cursor.close();
                var entry = try cursor.seekAtOrAfter(if (continuation_len != 0) continuation[0..continuation_len] else first);
                if (entry) |item| if (continuation_len != 0 and std.mem.eql(u8, item.key, continuation[0..continuation_len])) {
                    entry = try cursor.next();
                };
                complete = true;
                while (entry) |item| : (entry = try cursor.next()) {
                    if (!std.mem.startsWith(u8, item.key, &prefix)) break;
                    if (io) |runtime_io| try runtime_io.checkCancel();
                    const address = try integrity.validateTransferRecord(item.key, item.value);
                    if (upper.len != 0 and std.mem.order(u8, &address.routing, upper) != .lt) break;
                    if (std.mem.order(u8, &address.routing, lower) == .lt) return error.InvalidIntegrityKey;
                    const bytes = std.math.add(usize, item.key.len, if (remove) 0 else item.value.len) catch return error.IntegrityRecordTooLarge;
                    if (inspected == budget.records or bytes > budget.bytes - retained_bytes or (inspected != 0 and time.monotonicNs() -| started >= budget.time_ns)) {
                        if (inspected == 0) return error.IntegrityRecordTooLarge;
                        complete = false;
                        break;
                    }
                    const key = try owned.dupe(u8, item.key);
                    if (remove) try deletes.append(owned, key) else try writes.append(owned, .{ .key = key, .value = try owned.dupe(u8, item.value) });
                    @memcpy(continuation[0..key.len], key);
                    continuation_len = key.len;
                    inspected += 1;
                    retained_bytes += bytes;
                }
            }
            if (writes.items.len != 0 or deletes.items.len != 0) try destination.putBatch(writes.items, deletes.items);
        }
    }
}

pub fn copyToUnpublished(alloc: Allocator, io: ?std.Io, source: *docstore.DocStore, destination: *docstore.DocStore, lower: []const u8, upper: []const u8) !void {
    const raw = source.get(alloc, catalog.key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (raw) |bytes| alloc.free(bytes);
    if (raw) |bytes| {
        var decoded = try catalog.decode(alloc, bytes);
        defer decoded.deinit();
        try destination.put(catalog.key, bytes);
    }
    // Physical split pages may already contain global metadata; prune that
    // copy before adding the explicitly routed cohort from the frozen source.
    try pruneOutside(alloc, io, destination, lower, upper);
    try walk(alloc, io, source, destination, lower, upper, false, .{});
}

pub fn pruneOutside(alloc: Allocator, io: ?std.Io, store: *docstore.DocStore, lower: []const u8, upper: []const u8) !void {
    if (lower.len != 0) try walk(alloc, io, store, store, "", lower, true, .{});
    if (upper.len != 0) try walk(alloc, io, store, store, upper, "", true, .{});
}

test "relational integrity range transfer follows logical claim routing and preserves companions" {
    const alloc = std.testing.allocator;
    const lsm = @import("../lsm_backend.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var source_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const source_path = try std.fmt.bufPrint(&source_path_buf, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    var destination_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const destination_path = try std.fmt.bufPrint(&destination_path_buf, ".zig-cache/tmp/{s}/destination", .{tmp.sub_path});
    var source_backend = try lsm.Backend.open(alloc, source_path, .{});
    defer source_backend.close();
    var destination_backend = try lsm.Backend.open(alloc, destination_path, .{});
    defer destination_backend.close();
    var source = try docstore.DocStore.openRuntime(alloc, try source_backend.runtimeStore(alloc, .{}));
    defer source.close();
    var destination = try docstore.DocStore.openRuntime(alloc, try destination_backend.runtimeStore(alloc, .{}));
    defer destination.close();
    const first = try integrity.Address.init(@splat(1), "first");
    const second = try integrity.Address.init(@splat(1), "second");
    const lower = if (std.mem.order(u8, &first.routing, &second.routing) == .lt) second.routing else first.routing;
    const kept = if (std.mem.eql(u8, &lower, &first.routing)) first else second;
    const excluded = if (std.mem.eql(u8, &lower, &first.routing)) second else first;
    for ([_]integrity.Address{ first, second }, [_][]const u8{ "first", "second" }) |address, tuple| {
        const claim: integrity.Claim = .{ .tuple = tuple, .parent_table = "parent", .parent_key = tuple, .schema_version = 1 };
        const raw = try claim.encode(alloc, address);
        defer alloc.free(raw);
        try source.put(&address.claimKey(), raw);
        const reference: integrity.Reference = .{ .child_table = "child", .child_key = tuple, .constraint_name = "fk", .constraint_generation = @splat(2) };
        const ref_value = try reference.encode(alloc, address);
        defer alloc.free(ref_value);
        try source.put(&(try reference.key(address)), ref_value);
    }
    const before = source_backend.snapshotMaintenanceStats();
    try copyToUnpublished(alloc, null, &source, &destination, &lower, "");
    const after = source_backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(
        before.mutable_snapshot_clone_by_reason[@intFromEnum(lsm.MutableSnapshotReason.bound_read_txn)].calls,
        after.mutable_snapshot_clone_by_reason[@intFromEnum(lsm.MutableSnapshotReason.bound_read_txn)].calls,
    );
    var read = try destination.beginReadTxn();
    defer read.abort();
    const kept_value = try read.get(&kept.claimKey());
    try integrity.validateTransferredCompanions(&read, &kept.claimKey(), kept_value);
    try std.testing.expectError(error.NotFound, read.get(&excluded.claimKey()));
    try pruneOutside(alloc, null, &source, "", &lower);
    var source_read = try source.beginReadTxn();
    defer source_read.abort();
    _ = try source_read.get(&excluded.claimKey());
    try std.testing.expectError(error.NotFound, source_read.get(&kept.claimKey()));
}
