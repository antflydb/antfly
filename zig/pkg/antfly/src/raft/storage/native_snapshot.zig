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

//! Exact primary checkpoint transport, not a historical table backup. All
//! primary bytes (including intents and retained effects) are authoritative.
//! Local sidecar pins and derived projections are deliberately not included.
const std = @import("std");
const core = @import("antfly_local_sources").storage_db_core;
const fs = @import("antfly_runtime_fs").fs_paths;
const Cancellation = @import("antfly_local_sources").storage_db_types.CancellationToken;
const Sha = std.crypto.hash.sha2.Sha256;
const schema_contract = @import("antfly_local_sources").storage_db_relational_schema_owner_contract;
const wire = @import("antfly_local_sources").storage_db_native_raft_snapshot_contract;
pub const Origin = wire.Origin;
pub const InstalledOrigin = wire.InstalledOrigin;
pub const installed_origin_file_name = InstalledOrigin.file_name;
pub const max_files = 1_000_000;
pub const header_size = wire.header_size;
const max_file_bytes: u64 = 1 << 40;
pub const Identity = wire.Identity;
pub const StreamReceipt = wire.StreamReceipt;
pub const MaterializationWitness = wire.MaterializationWitness;

pub const Capture = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    identity: Identity,
    primary: core.PinnedStoreSnapshot,

    pub fn encodedSize(self: *const Capture) !u64 {
        try self.identity.validate();
        const checkpoint = switch (self.primary) {
            .lsm => |*value| value,
            .logical => return error.UnsupportedSnapshotFormat,
        };
        var size: u64 = header_size + 32 + 48 + checkpoint.manifest_bytes.len;
        if (self.identity.origin != null) size += try self.originSize();
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
        _ = try self.writeWithReceipt(writer, cancellation);
    }

    fn originSize(self: *const Capture) !usize {
        var size: usize = Origin.encoded_len + 4;
        if (self.identity.schema_header) |header| {
            var buffer: [schema_contract.max_header + 44]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&buffer);
            size += (try header.encode(fixed.allocator())).len;
        }
        return size;
    }

    pub fn writeWithReceipt(self: *Capture, writer: *std.Io.Writer, cancellation: Cancellation) !StreamReceipt {
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
        @memcpy(header[0..8], if (self.identity.origin != null) "NRSP2\x00\x00\x00" else "NRSP1\x00\x00\x00");
        std.mem.writeInt(u64, header[8..16], self.identity.group_id, .little);
        header[16..40].* = self.identity.namespace;
        std.mem.writeInt(u64, header[40..48], self.identity.through_index, .little);
        std.mem.writeInt(u64, header[48..56], self.identity.native_term, .little);
        std.mem.writeInt(u64, header[56..64], self.identity.native_index, .little);
        std.mem.writeInt(u64, header[64..72], entries.len + 1, .little);
        if (self.identity.origin != null) std.mem.writeInt(u64, header[72..80], try self.originSize(), .little);
        var overall = Sha.init(.{});
        try emit(writer, &overall, &header);
        if (self.identity.origin) |origin| {
            try emit(writer, &overall, &try origin.encode());
            var encoded_header: [schema_contract.max_header + 44]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&encoded_header);
            const bytes = if (self.identity.schema_header) |value| try value.encode(fixed.allocator()) else "";
            var length: [4]u8 = undefined;
            std.mem.writeInt(u32, &length, @intCast(bytes.len), .little);
            try emit(writer, &overall, &length);
            try emit(writer, &overall, bytes);
        }
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
        return .{ .identity = self.identity, .footer_digest = digest };
    }
};

test "native Raft snapshot origin emission waits for the durable group decoder barrier" {
    const protocol = @import("../../common/data_raft_protocol.zig");
    for (0..protocol.native_snapshot_origin_protocol_version) |version|
        try std.testing.expect(!protocol.nativeSnapshotOriginEnabled(@intCast(version)));
    try std.testing.expect(protocol.nativeSnapshotOriginEnabled(protocol.native_snapshot_origin_protocol_version));
    try std.testing.expect(protocol.nativeSnapshotOriginEnabled(protocol.native_snapshot_origin_protocol_version + 1));
}

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

/// Only after full extraction, independent primary verification and repair.
/// The caller owns the unpublished destination capability. No source-chosen
/// path or copied destination identity is accepted by this checkpoint writer.
pub fn persistInstalledOrigin(alloc: std.mem.Allocator, io: std.Io, root: []const u8, expected: Identity, stream_digest: [32]u8, destination_root: u128) !void {
    try expected.validate();
    const source_origin = expected.origin orelse return;
    const record: InstalledOrigin = .{ .group_id = expected.group_id, .namespace = expected.namespace, .through_index = expected.through_index, .source = source_origin, .stream_digest = stream_digest, .destination_root_incarnation = destination_root };
    const raw = try record.encode();
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, installed_origin_file_name });
    defer alloc.free(path);
    if (fs.createFilePortable(io, path, .{ .exclusive = true })) |file_value| {
        var file = file_value;
        defer file.close(io);
        errdefer std.Io.Dir.cwd().deleteFile(io, path) catch {};
        var buffer: [InstalledOrigin.encoded_len]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(&raw);
        try writer.end();
        try file.sync(io);
    } else |err| {
        if (err != error.PathAlreadyExists) return err;
        const existing = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(InstalledOrigin.encoded_len + 1));
        defer alloc.free(existing);
        if (!std.mem.eql(u8, existing, &raw)) return error.NativeBackupOriginMismatch;
        var file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        try file.sync(io);
    }
    try fs.syncDirPortable(io, root);
}

pub fn loadInstalledOrigin(alloc: std.mem.Allocator, io: std.Io, root: []const u8, group: u64, namespace: [24]u8, destination_root: u128) !?InstalledOrigin {
    return try @import("antfly_local_sources").storage_db_native_backup.loadInstalledOrigin(alloc, io, root, group, namespace, destination_root, .{});
}

pub fn identity(raw: []const u8) !Identity {
    if (raw.len < header_size + 32) return error.InvalidSnapshot;
    const v2 = std.mem.eql(u8, raw[0..8], "NRSP2\x00\x00\x00");
    const origin_size = std.mem.readInt(u64, raw[72..80], .little);
    var schema_header: ?schema_contract.Header = null;
    const origin: ?Origin = if (v2) blk: {
        if (origin_size < Origin.encoded_len + 4 or origin_size > Origin.encoded_len + 4 + schema_contract.max_header + 44 or
            raw.len < header_size + origin_size + 32) return error.InvalidSnapshot;
        const length = std.mem.readInt(u32, raw[header_size + Origin.encoded_len ..][0..4], .little);
        if (origin_size != Origin.encoded_len + 4 + @as(u64, length)) return error.InvalidSnapshot;
        if (length != 0) schema_header = schema_contract.Header.decode(raw[header_size + Origin.encoded_len + 4 ..][0..length]) catch return error.InvalidSnapshot;
        break :blk Origin.decode(raw[header_size..][0..Origin.encoded_len]) catch return error.InvalidSnapshot;
    } else blk: {
        if (!std.mem.eql(u8, raw[0..8], "NRSP1\x00\x00\x00") or origin_size != 0) return error.InvalidSnapshot;
        break :blk null;
    };
    const value: Identity = .{
        .group_id = std.mem.readInt(u64, raw[8..16], .little),
        .namespace = raw[16..40].*,
        .through_index = std.mem.readInt(u64, raw[40..48], .little),
        .native_term = std.mem.readInt(u64, raw[48..56], .little),
        .native_index = std.mem.readInt(u64, raw[56..64], .little),
        .origin = origin,
        .schema_header = schema_header,
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
    var offset: usize = header_size + @as(usize, @intCast(std.mem.readInt(u64, raw[72..80], .little)));
    overall.update(raw[0..offset]);
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
    const db_mod = @import("antfly_local_sources").storage_db_mod;
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
    const scope: @import("antfly_local_sources").storage_db_online_source_contract.Scope = .{
        .fence = .{ .admission_epoch = owner_identity.next_epoch, .transition_id = 55, .attempt = 1, .peer_group_id = 23, .owner_group_id = 22, .role = .merge_source, .namespace = owner_identity.namespace, .catalog_digest = owner_identity.catalog_digest },
        .receiver_namespace = .{ .table_id = 21, .shard_id = 23, .range_id = 23 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 2, .sequence = 1 },
    };
    const pin_mod = @import("antfly_local_sources").storage_db_source_pin;
    pin_mod.test_failure = .after_prepare;
    defer pin_mod.test_failure = .none;
    try std.testing.expectError(error.InjectedSourcePinFailure, @import("../../storage/server_db_adapter.zig").applyOrdered(&source, .{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 2, .index = 1 }));
    pin_mod.test_failure = .none;
    try std.testing.expectError(error.OnlineSourcePinPending, @import("../../storage/server_db_adapter.zig").captureSnapshot(&source, 22, 1));
    try @import("../../storage/server_db_adapter.zig").applyOrdered(&source, .{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 2, .index = 1 });
    try std.testing.expectError(error.OnlineSourcePinPending, @import("../../storage/server_db_adapter.zig").captureSnapshot(&source, 22, 1));
    const certificate = try source.prepareOnlineSourcePublication(scope, .none);
    try @import("../../storage/server_db_adapter.zig").applyOrdered(&source, .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 2, .index = 2 });
    try @import("../../storage/server_db_adapter.zig").applyOrdered(&source, .{ .timestamp_ns = 987654321, .writes = &.{.{ .key = "a", .value = "{\"n\":9007199254740993}" }} }, .{ .term = 2, .index = 5 });
    const txn_id = @as([16]u8, @splat(7));
    _ = try source.beginReplicatedTransactionAtOrderedReceipt(txn_id, 987654322, 987654322, &.{"participant"}, false, false, .{ .term = 2, .index = 6 });
    try source.writeReplicatedTransactionAtOrderedReceipt(txn_id, .{ .writes = &.{.{ .key = "prepared", .value = "{\"n\":4}" }} }, .{ .term = 2, .index = 7 });
    try @import("../../storage/server_db_adapter.zig").applyStorageKernelReplicatedBatchAtRaftEntry(alloc, &source, "rows", 22, .{ .transaction = .{ .prepare = .{ .txn_id = txn_id, .topology_epoch = 1 } } }, .{ .term = 2, .index = 8 });
    try std.testing.expectError(error.InvalidSnapshot, @import("../../storage/server_db_adapter.zig").captureSnapshot(&source, 22, 4));
    // Requested through-index includes protocol-only entries without a native
    // mutation marker; the exact native cut remains separately authenticated.
    var capture = try @import("../../storage/server_db_adapter.zig").captureSnapshot(&source, 22, 10);
    defer capture.deinit();
    var expected_txn = try source.core.store.beginReadTxn();
    defer expected_txn.abort();
    try @import("../../storage/server_db_adapter.zig").applyOrdered(&source, .{ .timestamp_ns = 987654323, .writes = &.{.{ .key = "later", .value = "{\"n\":2}" }} }, .{ .term = 2, .index = 11 });
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();
    const canceled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, capture.write(&writer.writer, Cancellation.fromAtomic(&canceled)));
    try std.testing.expectEqual(@as(usize, 0), writer.written().len);
    const receipt = try capture.writeWithReceipt(&writer.writer, .none);
    try std.testing.expectEqualDeep(capture.identity, receipt.identity);
    try std.testing.expectEqualSlices(u8, &receipt.footer_digest, writer.written()[writer.written().len - 32 ..]);
    const witness: MaterializationWitness = .{ .native_offset = 7, .native_size = writer.written().len, .stream = receipt };
    try witness.requireFor(22, 10, 2, 7 + writer.written().len);
    try std.testing.expectError(error.InvalidSnapshot, witness.requireFor(23, 10, 2, 7 + writer.written().len));
    try std.testing.expectError(error.InvalidSnapshot, witness.requireFor(22, 11, 2, 7 + writer.written().len));
    try std.testing.expectError(error.InvalidSnapshot, witness.requireFor(22, 10, 1, 7 + writer.written().len));
    try std.testing.expectError(error.InvalidSnapshot, witness.requireFor(22, 10, 2, 8 + writer.written().len));
    try std.testing.expectEqual(try capture.encodedSize(), writer.written().len);
    try std.testing.expectEqualStrings("NRSP2\x00\x00\x00", writer.written()[0..8]);
    try std.testing.expectEqual(source.root_incarnation, capture.identity.origin.?.physical_root_incarnation);
    try std.testing.expectEqual(@as(u64, 8), capture.identity.origin.?.applied.?.index);
    try std.testing.expectEqual(@as(u64, 10), capture.identity.through_index);
    try std.testing.expectEqualDeep(capture.identity, try identity(writer.written()));
    for (0..header_size + Origin.encoded_len + 4 + 32) |size|
        try std.testing.expectError(error.InvalidSnapshot, identity(writer.written()[0..size]));
    try extract(alloc, std.testing.io, writer.written(), target_path, capture.identity, .none);
    {
        var unpublished = try db_mod.DB.open(alloc, target_path, .{ .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
        defer unpublished.close();
        try std.testing.expectEqual(@as(u128, 0), unpublished.root_incarnation);
        try std.testing.expectError(error.FileNotFound, @import("antfly_local_sources").storage_db_root_identity.load(alloc, std.testing.io, target_path));
    }
    const target_root = try @import("antfly_local_sources").storage_db_root_identity.loadOrCreate(alloc, std.testing.io, target_path);
    {
        var target = try db_mod.DB.open(alloc, target_path, .{ .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
        defer target.close();
        try std.testing.expectEqual(target_root.incarnation, target.root_incarnation);
        try @import("../../storage/server_db_adapter.zig").verifySnapshot(&target, capture.identity);
        const stream_hash = writer.written()[writer.written().len - 32 ..][0..32].*;
        try persistInstalledOrigin(alloc, std.testing.io, target_path, capture.identity, stream_hash, target.root_incarnation);
        try persistInstalledOrigin(alloc, std.testing.io, target_path, capture.identity, stream_hash, target.root_incarnation);
        const installed = (try loadInstalledOrigin(alloc, std.testing.io, target_path, 22, capture.identity.namespace, target.root_incarnation)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualDeep(capture.identity.origin.?, installed.source);
        try std.testing.expectEqualSlices(u8, &stream_hash, &installed.stream_digest);
        try std.testing.expectError(error.NativeBackupOriginMismatch, loadInstalledOrigin(alloc, std.testing.io, target_path, 22, capture.identity.namespace, target.root_incarnation + 1));
        var wrong_cut = capture.identity;
        wrong_cut.origin.?.transaction_directory = @splat(9);
        try std.testing.expectError(error.NativeBackupOriginMismatch, @import("../../storage/server_db_adapter.zig").verifySnapshot(&target, wrong_cut));
        try std.testing.expectError(error.NativeBackupOriginMismatch, persistInstalledOrigin(alloc, std.testing.io, target_path, wrong_cut, stream_hash, target.root_incarnation));
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
    // Consistent checksums do not turn copied origin claims into physical
    // evidence: independently compare them with the staged primary image.
    @memcpy(damaged, writer.written());
    var claimed = capture.identity;
    claimed.origin.?.transaction_directory = @splat(9);
    damaged[header_size..][0..Origin.encoded_len].* = try claimed.origin.?.encode();
    Sha.hash(damaged[0 .. damaged.len - 32], damaged[damaged.len - 32 ..][0..32], .{});
    const forged_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/forged", .{tmp.sub_path});
    defer alloc.free(forged_path);
    try extract(alloc, std.testing.io, damaged, forged_path, try identity(damaged), .none);
    {
        var target = try db_mod.DB.open(alloc, forged_path, .{ .open_mode = .query_readonly, .primary_only_readonly = true, .start_index_workers = false, .start_optional_runtimes = false });
        defer target.close();
        try std.testing.expectError(error.NativeBackupOriginMismatch, @import("../../storage/server_db_adapter.zig").verifySnapshot(&target, claimed));
    }
    // Released NRSP1 remains readable but never acquires an origin checkpoint.
    const saved = capture.identity.origin;
    capture.identity.origin = null;
    defer capture.identity.origin = saved;
    var legacy: std.Io.Writer.Allocating = .init(alloc);
    defer legacy.deinit();
    try capture.write(&legacy.writer, .none);
    try std.testing.expectEqual(try capture.encodedSize(), legacy.written().len);
    try std.testing.expect((try identity(legacy.written())).origin == null);
    const legacy_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/legacy", .{tmp.sub_path});
    defer alloc.free(legacy_path);
    try extract(alloc, std.testing.io, legacy.written(), legacy_path, capture.identity, .none);
    try persistInstalledOrigin(alloc, std.testing.io, legacy_path, capture.identity, @splat(5), 99);
    try std.testing.expect(try loadInstalledOrigin(alloc, std.testing.io, legacy_path, 22, capture.identity.namespace, 99) == null);
}
