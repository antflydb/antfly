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

//! Exact primary checkpoint transport, not a historical table backup. All
//! primary bytes (including intents and retained effects) are authoritative.
//! Local sidecar pins and derived projections are deliberately not included.
const std = @import("std");
const core = @import("core.zig");
const fs = @import("../../common/fs_paths.zig");
const Cancellation = @import("types.zig").CancellationToken;
const Sha = std.crypto.hash.sha2.Sha256;
pub const max_files = 1_000_000;
pub const header_size = 80;
const max_file_bytes: u64 = 1 << 40;

pub const Identity = struct {
    group_id: u64,
    namespace: [24]u8,
    through_index: u64,
    native_term: u64,
    native_index: u64,

    fn validate(self: Identity) !void {
        if (self.group_id == 0 or self.native_index > self.through_index or
            (self.native_index == 0) != (self.native_term == 0)) return error.InvalidSnapshot;
    }
};

pub const Capture = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    identity: Identity,
    primary: core.PinnedStoreSnapshot,

    pub fn encodedSize(self: *const Capture) !u64 {
        const checkpoint = switch (self.primary) {
            .lsm => |*value| value,
            .logical => return error.UnsupportedSnapshotFormat,
        };
        var size: u64 = header_size + 32 + 48 + checkpoint.manifest_bytes.len;
        for (checkpoint.run_paths) |path| {
            var file = try std.Io.Dir.cwd().openFile(self.io, path, .{});
            defer file.close(self.io);
            const stat = try file.stat(self.io);
            if (stat.kind != .file or stat.size > max_file_bytes) return error.InvalidSnapshot;
            size = std.math.add(u64, size, 48 + stat.size) catch return error.SnapshotTooLarge;
        }
        return size;
    }

    pub fn deinit(self: *Capture) void {
        self.primary.deinit();
        self.* = undefined;
    }

    /// The immutable checkpoint was pinned while the caller held the owner
    /// completion fence. No live state is read or recaptured during writing.
    pub fn write(self: *Capture, writer: *std.Io.Writer, cancellation: Cancellation) !void {
        try cancellation.check();
        try self.identity.validate();
        const checkpoint = switch (self.primary) {
            .lsm => |*value| value,
            .logical => return error.UnsupportedSnapshotFormat,
        };
        if (!checkpoint.storage.supportsHostPathGenerationPublication()) return error.UnsupportedSnapshotFormat;
        if (checkpoint.run_ids.len >= max_files) return error.SnapshotTooLarge;
        const Entry = struct { id: u64, path: []const u8 };
        const entries = try self.alloc.alloc(Entry, checkpoint.run_ids.len);
        defer self.alloc.free(entries);
        for (entries, checkpoint.run_ids, checkpoint.run_paths) |*entry, id, path| entry.* = .{ .id = id, .path = path };
        std.mem.sort(Entry, entries, {}, struct {
            fn less(_: void, a: Entry, b: Entry) bool {
                return a.id < b.id;
            }
        }.less);
        var header: [header_size]u8 = @splat(0);
        @memcpy(header[0..8], "NRSP1\x00\x00\x00");
        std.mem.writeInt(u64, header[8..16], self.identity.group_id, .little);
        header[16..40].* = self.identity.namespace;
        std.mem.writeInt(u64, header[40..48], self.identity.through_index, .little);
        std.mem.writeInt(u64, header[48..56], self.identity.native_term, .little);
        std.mem.writeInt(u64, header[56..64], self.identity.native_index, .little);
        std.mem.writeInt(u64, header[64..72], entries.len + 1, .little);
        var overall = Sha.init(.{});
        try emit(writer, &overall, &header);
        try emitBytes(writer, &overall, 0, checkpoint.manifest_bytes);
        var previous: u64 = 0;
        var buffer: [64 * 1024]u8 = undefined;
        for (entries) |entry| {
            try cancellation.check();
            if (entry.id <= previous) return error.InvalidSnapshot;
            previous = entry.id;
            var file = try std.Io.Dir.cwd().openFile(self.io, entry.path, .{});
            defer file.close(self.io);
            const stat = try file.stat(self.io);
            if (stat.kind != .file or stat.size > max_file_bytes) return error.InvalidSnapshot;
            var frame: [16]u8 = undefined;
            std.mem.writeInt(u64, frame[0..8], entry.id, .little);
            std.mem.writeInt(u64, frame[8..16], stat.size, .little);
            try emit(writer, &overall, &frame);
            var hash = Sha.init(.{});
            var remaining = stat.size;
            var file_reader = file.reader(self.io, &.{});
            while (remaining > 0) {
                try cancellation.check();
                const part = buffer[0..@min(remaining, buffer.len)];
                try file_reader.interface.readSliceAll(part);
                try emit(writer, &overall, part);
                hash.update(part);
                remaining -= part.len;
            }
            var digest: [32]u8 = undefined;
            hash.final(&digest);
            try emit(writer, &overall, &digest);
        }
        try cancellation.check();
        var digest: [32]u8 = undefined;
        overall.final(&digest);
        try writer.writeAll(&digest);
    }
};

fn emit(writer: *std.Io.Writer, hash: *Sha, bytes: []const u8) !void {
    try writer.writeAll(bytes);
    hash.update(bytes);
}

fn emitBytes(writer: *std.Io.Writer, hash: *Sha, id: u64, bytes: []const u8) !void {
    if (bytes.len > max_file_bytes) return error.SnapshotTooLarge;
    var frame: [16]u8 = undefined;
    std.mem.writeInt(u64, frame[0..8], id, .little);
    std.mem.writeInt(u64, frame[8..16], bytes.len, .little);
    try emit(writer, hash, &frame);
    try emit(writer, hash, bytes);
    var digest: [32]u8 = undefined;
    Sha.hash(bytes, &digest, .{});
    try emit(writer, hash, &digest);
}

pub fn identity(raw: []const u8) !Identity {
    if (raw.len < header_size + 32 or !std.mem.eql(u8, raw[0..8], "NRSP1\x00\x00\x00") or
        std.mem.readInt(u64, raw[72..80], .little) != 0) return error.InvalidSnapshot;
    const value: Identity = .{
        .group_id = std.mem.readInt(u64, raw[8..16], .little),
        .namespace = raw[16..40].*,
        .through_index = std.mem.readInt(u64, raw[40..48], .little),
        .native_term = std.mem.readInt(u64, raw[48..56], .little),
        .native_index = std.mem.readInt(u64, raw[56..64], .little),
    };
    try value.validate();
    return value;
}

/// Extracts only deterministic manifest/run names into an unpublished, empty
/// generation owned by the caller. No corpus-sized allocation or trusted path
/// from the sender. On failure the caller discards the staged generation.
pub fn extract(alloc: std.mem.Allocator, io: std.Io, raw: []const u8, root: []const u8, expected: Identity, cancellation: Cancellation) !void {
    try cancellation.check();
    const actual = try identity(raw);
    if (!std.meta.eql(actual, expected)) return error.InvalidSnapshot;
    const count = std.mem.readInt(u64, raw[64..72], .little);
    if (count == 0 or count > max_files) return error.InvalidSnapshot;
    var overall = Sha.init(.{});
    overall.update(raw[0..header_size]);
    var offset: usize = header_size;
    var previous: u64 = 0;
    const runs = try std.fmt.allocPrint(alloc, "{s}/runs", .{root});
    defer alloc.free(runs);
    try fs.createDirPathPortable(io, runs);
    for (0..@intCast(count)) |index| {
        try cancellation.check();
        if (raw.len - offset < 16 + 32 + 32) return error.InvalidSnapshot;
        const id = std.mem.readInt(u64, raw[offset..][0..8], .little);
        const size = std.mem.readInt(u64, raw[offset + 8 ..][0..8], .little);
        overall.update(raw[offset..][0..16]);
        offset += 16;
        if ((index == 0 and id != 0) or (index > 0 and id <= previous) or size > max_file_bytes or size > raw.len - offset - 64) return error.InvalidSnapshot;
        previous = id;
        const end = offset + @as(usize, @intCast(size));
        var hash = Sha.init(.{});
        const path = if (index == 0) try std.fmt.allocPrint(alloc, "{s}/manifest.bin", .{root}) else try std.fmt.allocPrint(alloc, "{s}/{d}.tbl", .{ runs, id });
        defer alloc.free(path);
        var file = try fs.createFilePortable(io, path, .{ .exclusive = true });
        defer file.close(io);
        var buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(io, &buffer);
        while (offset < end) {
            try cancellation.check();
            const chunk_end = offset + @min(end - offset, buffer.len);
            hash.update(raw[offset..chunk_end]);
            overall.update(raw[offset..chunk_end]);
            try writer.interface.writeAll(raw[offset..chunk_end]);
            offset = chunk_end;
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        if (!std.mem.eql(u8, &digest, raw[offset..][0..32])) return error.InvalidSnapshot;
        overall.update(raw[offset..][0..32]);
        offset += 32;
        try writer.end();
        try file.sync(io);
    }
    if (raw.len - offset != 32) return error.InvalidSnapshot;
    var digest: [32]u8 = undefined;
    overall.final(&digest);
    if (!std.mem.eql(u8, &digest, raw[offset..])) return error.InvalidSnapshot;
    try cancellation.check();
    try fs.syncDirPortable(io, runs);
    try fs.syncDirPortable(io, root);
}

test "relational index system native Raft snapshot preserves exact typed primary and later writes are excluded" {
    const db_mod = @import("mod.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 21, .shard_id = 22, .range_id = 22 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var source = try db_mod.DB.open(alloc, source_path, options);
    defer source.close();
    try source.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
    );
    const owner_identity = try source.relationalTopologyIdentity();
    const scope: @import("online_source_contract.zig").Scope = .{
        .fence = .{ .admission_epoch = owner_identity.next_epoch, .transition_id = 55, .attempt = 1, .peer_group_id = 23, .owner_group_id = 22, .role = .merge_source, .namespace = owner_identity.namespace, .catalog_digest = owner_identity.catalog_digest },
        .receiver_namespace = .{ .table_id = 21, .shard_id = 23, .range_id = 23 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 2, .sequence = 1 },
    };
    const pin_mod = @import("source_pin.zig");
    pin_mod.test_failure = .after_prepare;
    defer pin_mod.test_failure = .none;
    try std.testing.expectError(error.InjectedSourcePinFailure, source.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 2, .index = 1 }));
    pin_mod.test_failure = .none;
    try std.testing.expectError(error.OnlineSourcePinPending, source.captureNativeRaftSnapshot(22, 1));
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 2, .index = 1 });
    try std.testing.expectError(error.OnlineSourcePinPending, source.captureNativeRaftSnapshot(22, 1));
    const certificate = try source.prepareOnlineSourcePublication(scope, .none);
    try source.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 2, .index = 2 });
    try source.batchRaftReplicatedApply(.{ .timestamp_ns = 987654321, .writes = &.{.{ .key = "a", .value = "{\"n\":9007199254740993}" }} }, .{ .term = 2, .index = 5 });
    const txn_id = [_]u8{7} ** 16;
    _ = try source.beginReplicatedTransactionAtRaftEntry(txn_id, 987654322, 987654322, &.{"participant"}, false, false, .{ .term = 2, .index = 6 });
    try source.writeReplicatedTransactionAtRaftEntry(txn_id, .{ .writes = &.{.{ .key = "prepared", .value = "{\"n\":4}" }} }, .{ .term = 2, .index = 7 });
    try source.batchRaftReplicatedApply(.{ .transaction = .{ .prepare = .{ .txn_id = txn_id, .topology_epoch = 1 } } }, .{ .term = 2, .index = 8 });
    try std.testing.expectError(error.InvalidSnapshot, source.captureNativeRaftSnapshot(22, 4));
    // Requested through-index includes protocol-only entries without a native
    // mutation marker; the exact native cut remains separately authenticated.
    var capture = try source.captureNativeRaftSnapshot(22, 10);
    defer capture.deinit();
    var expected_txn = try source.core.store.beginReadTxn();
    defer expected_txn.abort();
    try source.batchRaftReplicatedApply(.{ .timestamp_ns = 987654323, .writes = &.{.{ .key = "later", .value = "{\"n\":2}" }} }, .{ .term = 2, .index = 11 });
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();
    const canceled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, capture.write(&writer.writer, Cancellation.fromAtomic(&canceled)));
    try std.testing.expectEqual(@as(usize, 0), writer.written().len);
    try capture.write(&writer.writer, .none);
    try std.testing.expectEqual(try capture.encodedSize(), writer.written().len);
    try extract(alloc, std.testing.io, writer.written(), target_path, capture.identity, .none);
    {
        var target = try db_mod.DB.open(alloc, target_path, .{ .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
        defer target.close();
        try target.verifyNativeRaftSnapshot(capture.identity);
        var actual_txn = try target.core.store.beginReadTxn();
        defer actual_txn.abort();
        var cursor = try expected_txn.openCursor();
        defer cursor.close();
        var entry = try cursor.first();
        var count: usize = 0;
        while (entry) |kv| : (entry = try cursor.next()) {
            try std.testing.expectEqualSlices(u8, kv.value, try actual_txn.get(kv.key));
            count += 1;
        }
        var actual_cursor = try actual_txn.openCursor();
        defer actual_cursor.close();
        var actual_entry = try actual_cursor.first();
        var actual_count: usize = 0;
        while (actual_entry != null) : (actual_entry = try actual_cursor.next()) actual_count += 1;
        try std.testing.expectEqual(count, actual_count);
    }
    // Footer failures never produce a publishable generation.
    const bad_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/bad", .{tmp.sub_path});
    defer alloc.free(bad_path);
    const damaged = try alloc.dupe(u8, writer.written());
    defer alloc.free(damaged);
    damaged[damaged.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidSnapshot, extract(alloc, std.testing.io, damaged, bad_path, capture.identity, .none));
}
