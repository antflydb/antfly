// Copyright 2026 Antfly, Inc.
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
const core = @import("../core/mod.zig");
const replica = @import("replica.zig");
const catalog_iface = @import("replica_catalog_iface.zig");
const snapshot_transport_iface = @import("snapshot_transport_iface.zig");

const file_magic = "RPLC1";
const bootstrap_empty: u8 = 0;
const bootstrap_persisted: u8 = 1;
const bootstrap_fetch_snapshot: u8 = 2;
const bootstrap_fetch_snapshot_versioned: u8 = 3;

pub const FileReplicaCatalog = struct {
    alloc: std.mem.Allocator,
    path: []const u8,

    pub fn init(alloc: std.mem.Allocator, path: []const u8) !FileReplicaCatalog {
        return .{
            .alloc = alloc,
            .path = try alloc.dupe(u8, path),
        };
    }

    pub fn deinit(self: *FileReplicaCatalog) void {
        if (self.path.len > 0) self.alloc.free(self.path);
        self.* = undefined;
    }

    pub fn catalog(self: *FileReplicaCatalog) catalog_iface.ReplicaCatalog {
        return .{
            .ptr = self,
            .vtable = &.{
                .upsert_replica = upsertReplica,
                .remove_replica = removeReplica,
                .list_replicas = listReplicas,
            },
        };
    }

    fn upsertReplica(ptr: *anyopaque, record: replica.ReplicaRecord) !void {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        try validateWritableRecord(record);
        var records = try self.loadRecords(self.alloc);
        defer self.freeRecords(self.alloc, records);

        var found = false;
        for (records) |*existing| {
            if (existing.group_id != record.group_id) continue;
            existing.deinit(self.alloc);
            existing.* = try record.clone(self.alloc);
            found = true;
            break;
        }

        if (!found) {
            const expanded = try self.alloc.alloc(replica.ReplicaRecord, records.len + 1);
            errdefer self.alloc.free(expanded);
            for (records, 0..) |existing, i| expanded[i] = existing;
            expanded[records.len] = try record.clone(self.alloc);
            self.alloc.free(records);
            records = expanded;
        }

        try self.saveRecords(records);
    }

    fn removeReplica(ptr: *anyopaque, group_id: core.types.GroupId) !bool {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        var records = try self.loadRecords(self.alloc);
        defer self.freeRecords(self.alloc, records);

        const index = blk: {
            for (records, 0..) |record, i| {
                if (record.group_id == group_id) break :blk i;
            }
            break :blk null;
        };
        const remove_index = index orelse return false;

        var remaining = try self.alloc.alloc(replica.ReplicaRecord, records.len - 1);
        errdefer self.alloc.free(remaining);
        var out_i: usize = 0;
        for (records, 0..) |record, i| {
            if (i == remove_index) continue;
            remaining[out_i] = record;
            out_i += 1;
        }
        records[remove_index].deinit(self.alloc);
        self.alloc.free(records);
        records = remaining;

        try self.saveRecords(records);
        return true;
    }

    fn listReplicas(ptr: *anyopaque, alloc: std.mem.Allocator) ![]replica.ReplicaRecord {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        return try self.loadRecords(alloc);
    }

    fn loadRecords(self: *FileReplicaCatalog, alloc: std.mem.Allocator) ![]replica.ReplicaRecord {
        var io_instance: std.Io.Threaded = .init(self.alloc, .{});
        defer io_instance.deinit();
        const io = io_instance.io();

        const file = std.Io.Dir.openFileAbsolute(io, self.path, .{}) catch |err| switch (err) {
            error.FileNotFound => return try alloc.alloc(replica.ReplicaRecord, 0),
            else => return err,
        };
        defer file.close(io);

        var file_reader = file.reader(io, &.{});
        const data = try file_reader.interface.allocRemaining(alloc, .limited(16 * 1024 * 1024));
        defer alloc.free(data);

        var reader: std.Io.Reader = .fixed(data);

        const magic = try reader.takeArray(file_magic.len);
        if (!std.mem.eql(u8, magic, file_magic)) return error.InvalidReplicaCatalogMagic;

        const count = try reader.takeInt(u32, .little);
        var records = try alloc.alloc(replica.ReplicaRecord, count);
        var initialized: usize = 0;
        errdefer {
            for (records[0..initialized]) |*record| record.deinit(alloc);
            alloc.free(records);
        }

        while (initialized < count) : (initialized += 1) {
            records[initialized] = try decodeRecord(alloc, &reader);
        }
        return records;
    }

    fn saveRecords(self: *FileReplicaCatalog, records: []const replica.ReplicaRecord) !void {
        var buffer: std.ArrayList(u8) = .empty;
        defer buffer.deinit(self.alloc);

        try buffer.appendSlice(self.alloc, file_magic);
        try appendInt(self.alloc, &buffer, u32, @intCast(records.len));
        for (records) |record| try encodeRecord(self.alloc, &buffer, record);

        var io_instance: std.Io.Threaded = .init(self.alloc, .{});
        defer io_instance.deinit();
        const io = io_instance.io();

        const file = try std.Io.Dir.createFileAbsolute(io, self.path, .{ .truncate = true });
        defer file.close(io);

        var file_writer = file.writer(io, &.{});
        try file_writer.interface.writeAll(buffer.items);
    }

    fn freeRecords(self: *FileReplicaCatalog, alloc: std.mem.Allocator, records: []replica.ReplicaRecord) void {
        _ = self;
        for (records) |*record| record.deinit(alloc);
        alloc.free(records);
    }
};

fn encodeRecord(alloc: std.mem.Allocator, buffer: *std.ArrayList(u8), record: replica.ReplicaRecord) !void {
    try validateWritableRecord(record);
    try appendInt(alloc, buffer, u64, record.group_id);
    try appendInt(alloc, buffer, u64, record.local_node_id);
    try appendInt(alloc, buffer, u32, @intCast(record.raft.peers.len));
    for (record.raft.peers) |peer| try appendInt(alloc, buffer, u64, peer);
    try appendInt(alloc, buffer, u32, record.raft.election_tick);
    try appendInt(alloc, buffer, u32, record.raft.heartbeat_tick);
    try buffer.append(alloc, if (record.raft.random_seed != null) 1 else 0);
    if (record.raft.random_seed) |seed| try appendInt(alloc, buffer, u64, seed);
    try appendInt(alloc, buffer, u64, record.raft.applied);
    try appendInt(alloc, buffer, u64, @intCast(record.raft.max_size_per_msg));
    try appendInt(alloc, buffer, u64, @intCast(record.raft.max_committed_size_per_ready));
    try appendInt(alloc, buffer, u32, record.raft.max_inflight_msgs);
    try appendInt(alloc, buffer, u64, @intCast(record.raft.max_inflight_bytes));
    try appendInt(alloc, buffer, u64, @intCast(record.raft.max_uncommitted_entries_size));
    try buffer.append(alloc, packFlags(record.raft));
    try buffer.append(alloc, @intFromEnum(record.raft.read_only_option));

    switch (record.bootstrap) {
        .empty => try buffer.append(alloc, bootstrap_empty),
        .persisted => try buffer.append(alloc, bootstrap_persisted),
        .fetch_snapshot => |snapshot| {
            // Keep writing the predecessor shape until a durable catalog
            // migration has been activated separately from decoder rollout.
            // A rolling rollback must be able to reopen every local record.
            try buffer.append(alloc, bootstrap_fetch_snapshot);
            try appendInt(alloc, buffer, u64, snapshot.from);
            try appendInt(alloc, buffer, u64, snapshot.term);
            try buffer.append(alloc, if (snapshot.fetch_immediately) 1 else 0);
            try writeBytes(alloc, buffer, snapshot.locator.snapshot_id);
            try writeBytes(alloc, buffer, snapshot.locator.uri);
        },
    }
}

fn validateWritableRecord(record: replica.ReplicaRecord) !void {
    // Decoder-first rollout keeps the on-disk file readable by the predecessor
    // release. Its shape has no format field, and this generic catalog has no
    // transport contract from which it can reconstruct one. Reject semantic
    // loss explicitly until the versioned writer is activated.
    switch (record.bootstrap) {
        .fetch_snapshot => |snapshot| if (snapshot.locator.format != .unknown) {
            return error.SnapshotFormatRequiresVersionedCatalogWriter;
        },
        .empty, .persisted => {},
    }
}

fn decodeRecord(alloc: std.mem.Allocator, reader: *std.Io.Reader) !replica.ReplicaRecord {
    const group_id = try reader.takeInt(u64, .little);
    const local_node_id = try reader.takeInt(u64, .little);

    const peer_count = try reader.takeInt(u32, .little);
    const peers = try alloc.alloc(core.types.NodeId, peer_count);
    errdefer alloc.free(peers);
    for (peers) |*peer| peer.* = try reader.takeInt(u64, .little);

    var record = replica.ReplicaRecord{
        .group_id = group_id,
        .local_node_id = local_node_id,
        .raft = .{
            .peers = peers,
            .election_tick = try reader.takeInt(u32, .little),
            .heartbeat_tick = try reader.takeInt(u32, .little),
            .random_seed = null,
            .applied = 0,
        },
        .bootstrap = .persisted,
    };
    errdefer record.deinit(alloc);

    const has_seed = try reader.takeByte();
    if (has_seed != 0) record.raft.random_seed = try reader.takeInt(u64, .little);
    record.raft.applied = try reader.takeInt(u64, .little);
    record.raft.max_size_per_msg = @intCast(try reader.takeInt(u64, .little));
    record.raft.max_committed_size_per_ready = @intCast(try reader.takeInt(u64, .little));
    record.raft.max_inflight_msgs = try reader.takeInt(u32, .little);
    record.raft.max_inflight_bytes = @intCast(try reader.takeInt(u64, .little));
    record.raft.max_uncommitted_entries_size = @intCast(try reader.takeInt(u64, .little));
    const flags = try reader.takeByte();
    record.raft.async_storage_writes = (flags & 1) != 0;
    record.raft.check_quorum = (flags & 2) != 0;
    record.raft.pre_vote = (flags & 4) != 0;
    record.raft.step_down_on_removal = (flags & 8) != 0;
    record.raft.disable_proposal_forwarding = (flags & 16) != 0;
    record.raft.disable_conf_change_validation = (flags & 32) != 0;
    record.raft.read_only_option = try decodeReadOnlyOption(try reader.takeByte());

    const bootstrap_kind = try reader.takeByte();
    record.bootstrap = switch (bootstrap_kind) {
        bootstrap_empty => .empty,
        bootstrap_persisted => .persisted,
        bootstrap_fetch_snapshot, bootstrap_fetch_snapshot_versioned => .{
            .fetch_snapshot = try decodeSnapshotBootstrap(
                alloc,
                reader,
                bootstrap_kind == bootstrap_fetch_snapshot_versioned,
            ),
        },
        else => return error.InvalidReplicaCatalogBootstrap,
    };

    return record;
}

fn decodeSnapshotBootstrap(
    alloc: std.mem.Allocator,
    reader: *std.Io.Reader,
    versioned: bool,
) !replica.SnapshotBootstrap {
    const from = try reader.takeInt(u64, .little);
    const term = try reader.takeInt(u64, .little);
    const fetch_immediately = (try reader.takeByte()) != 0;
    const snapshot_id = try readBytes(alloc, reader);
    errdefer alloc.free(snapshot_id);
    const uri = try readBytes(alloc, reader);
    errdefer alloc.free(uri);
    const format: snapshot_transport_iface.SnapshotArtifactFormat = if (versioned) blk: {
        const decoded = std.enums.fromInt(
            snapshot_transport_iface.SnapshotArtifactFormat,
            try reader.takeByte(),
        ) orelse return error.InvalidReplicaCatalogBootstrap;
        if (decoded == .unknown) return error.InvalidReplicaCatalogBootstrap;
        break :blk decoded;
    } else .unknown;
    return .{
        .from = from,
        .term = term,
        .fetch_immediately = fetch_immediately,
        .locator = .{
            .snapshot_id = snapshot_id,
            .uri = uri,
            .format = format,
        },
    };
}

fn writeBytes(alloc: std.mem.Allocator, buffer: *std.ArrayList(u8), bytes: []const u8) !void {
    try appendInt(alloc, buffer, u32, @intCast(bytes.len));
    try buffer.appendSlice(alloc, bytes);
}

fn appendInt(alloc: std.mem.Allocator, buffer: *std.ArrayList(u8), comptime T: type, value: T) !void {
    var tmp: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &tmp, value, .little);
    try buffer.appendSlice(alloc, &tmp);
}

fn readBytes(alloc: std.mem.Allocator, reader: *std.Io.Reader) ![]u8 {
    const len = try reader.takeInt(u32, .little);
    const bytes = try alloc.alloc(u8, len);
    errdefer alloc.free(bytes);
    try reader.readSliceAll(bytes);
    return bytes;
}

fn packFlags(cfg: replica.ReplicaRaftConfig) u8 {
    var flags: u8 = 0;
    if (cfg.async_storage_writes) flags |= 1;
    if (cfg.check_quorum) flags |= 2;
    if (cfg.pre_vote) flags |= 4;
    if (cfg.step_down_on_removal) flags |= 8;
    if (cfg.disable_proposal_forwarding) flags |= 16;
    if (cfg.disable_conf_change_validation) flags |= 32;
    return flags;
}

fn decodeReadOnlyOption(raw: u8) !core.types.ReadOnlyOption {
    return switch (raw) {
        @intFromEnum(core.types.ReadOnlyOption.safe) => .safe,
        @intFromEnum(core.types.ReadOnlyOption.lease_based) => .lease_based,
        else => error.InvalidReplicaCatalogReadOnlyOption,
    };
}

test "versioned snapshot bootstrap frees partial locator on invalid format" {
    var encoded: std.ArrayList(u8) = .empty;
    defer encoded.deinit(std.testing.allocator);
    try appendInt(std.testing.allocator, &encoded, u64, 1);
    try appendInt(std.testing.allocator, &encoded, u64, 9);
    try encoded.append(std.testing.allocator, 1);
    try writeBytes(std.testing.allocator, &encoded, "snapshot-id");
    try writeBytes(std.testing.allocator, &encoded, "https://snapshot");
    try encoded.append(std.testing.allocator, 0xff);
    var reader: std.Io.Reader = .fixed(encoded.items);
    try std.testing.expectError(
        error.InvalidReplicaCatalogBootstrap,
        decodeSnapshotBootstrap(std.testing.allocator, &reader, true),
    );
}

test "file replica catalog rejects format loss and round trips predecessor records" {
    const path = "/tmp/antflydb-raft-file-replica-catalog.bin";
    var peers = [_]core.types.NodeId{ 1, 2, 3 };
    var record: replica.ReplicaRecord = .{
        .group_id = 201,
        .local_node_id = 2,
        .raft = .{
            .peers = peers[0..],
            .pre_vote = false,
            .check_quorum = true,
            .read_only_option = .lease_based,
        },
        .bootstrap = .{
            .fetch_snapshot = .{
                .from = 1,
                .term = 9,
                .locator = .{
                    .snapshot_id = "file-catalog",
                    .uri = "file:///tmp/snapshot",
                    .format = .chunked_manifest_v2,
                },
            },
        },
    };

    // Decoder-only deployment must not silently erase the caller's explicit
    // decoder selection merely to remain predecessor-readable.
    var encoded: std.ArrayList(u8) = .empty;
    defer encoded.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.SnapshotFormatRequiresVersionedCatalogWriter,
        encodeRecord(std.testing.allocator, &encoded, record),
    );
    try std.testing.expectEqual(@as(usize, 0), encoded.items.len);
    {
        var catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer catalog.deinit();
        try std.testing.expectError(
            error.SnapshotFormatRequiresVersionedCatalogWriter,
            catalog.catalog().upsertReplica(record),
        );
    }

    record.bootstrap.fetch_snapshot.locator.format = .unknown;
    try encodeRecord(std.testing.allocator, &encoded, record);
    var reader: std.Io.Reader = .fixed(encoded.items);
    _ = try reader.takeInt(u64, .little);
    _ = try reader.takeInt(u64, .little);
    const peer_count = try reader.takeInt(u32, .little);
    for (0..peer_count) |_| _ = try reader.takeInt(u64, .little);
    _ = try reader.takeInt(u32, .little);
    _ = try reader.takeInt(u32, .little);
    const has_seed = try reader.takeByte();
    if (has_seed != 0) _ = try reader.takeInt(u64, .little);
    _ = try reader.takeInt(u64, .little);
    _ = try reader.takeInt(u64, .little);
    _ = try reader.takeInt(u64, .little);
    _ = try reader.takeInt(u32, .little);
    _ = try reader.takeInt(u64, .little);
    _ = try reader.takeInt(u64, .little);
    _ = try reader.takeByte();
    _ = try reader.takeByte();
    try std.testing.expectEqual(bootstrap_fetch_snapshot, try reader.takeByte());

    {
        var catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer catalog.deinit();
        try catalog.catalog().upsertReplica(record);
    }

    var catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
    defer catalog.deinit();

    const records = try catalog.catalog().listReplicas(std.testing.allocator);
    defer {
        for (records) |*stored_record| stored_record.deinit(std.testing.allocator);
        std.testing.allocator.free(records);
    }

    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(core.types.GroupId, 201), records[0].group_id);
    try std.testing.expectEqual(@as(core.types.NodeId, 2), records[0].local_node_id);
    try std.testing.expectEqual(core.types.ReadOnlyOption.lease_based, records[0].raft.read_only_option);
    try std.testing.expectEqualStrings("file-catalog", records[0].bootstrap.fetch_snapshot.locator.snapshot_id);
    try std.testing.expectEqual(
        snapshot_transport_iface.SnapshotArtifactFormat.unknown,
        records[0].bootstrap.fetch_snapshot.locator.format,
    );
}
