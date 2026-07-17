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
const fs_paths = @import("../../common/fs_paths.zig");
const raft_engine = @import("raft_engine");
const storage_mod = @import("mod.zig");
const snapshot_payload_store = @import("snapshot_payload_store.zig");
const file_snapshot_artifact = @import("file_snapshot_artifact.zig");

const magic: u32 = 0x41524654; // ARFT
// Version 4 stores the Raft log compaction boundary separately from the
// transferable state snapshot metadata.
const version: u32 = 4;
var state_publish_nonce = std.atomic.Value(u64).init(1);

const SnapshotIdentity = struct { index: u64, term: u64 };

fn snapshotIdentity(metadata: raft_engine.core.types.SnapshotMetadata) SnapshotIdentity {
    return .{ .index = metadata.index, .term = metadata.term };
}

fn metadataOnlySnapshot(snapshot: raft_engine.core.types.Snapshot) raft_engine.core.types.Snapshot {
    return .{ .metadata = snapshot.metadata, .data = &.{} };
}

pub const PersistentReplicaState = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    layout: storage_mod.ReplicaPathLayout,
    store: raft_engine.core.MemoryStorage,
    applied_index: raft_engine.core.types.Index = 0,
    persist_buffer: std.ArrayListUnmanaged(u8) = .empty,

    pub fn init(
        alloc: std.mem.Allocator,
        layout: storage_mod.ReplicaPathLayout,
    ) !PersistentReplicaState {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        var io_owned = true;
        errdefer if (io_owned) io_impl.deinit();
        const root_dir = try alloc.dupe(u8, layout.root_dir);
        var root_dir_owned = true;
        errdefer if (root_dir_owned) alloc.free(root_dir);
        const log_dir = try alloc.dupe(u8, layout.log_dir);
        var log_dir_owned = true;
        errdefer if (log_dir_owned) alloc.free(log_dir);
        const snapshot_dir = try alloc.dupe(u8, layout.snapshot_dir);
        var snapshot_dir_owned = true;
        errdefer if (snapshot_dir_owned) alloc.free(snapshot_dir);

        var self = PersistentReplicaState{
            .alloc = alloc,
            .io_impl = io_impl,
            .layout = .{
                .root_dir = root_dir,
                .log_dir = log_dir,
                .snapshot_dir = snapshot_dir,
            },
            .store = raft_engine.core.MemoryStorage.init(alloc),
        };
        io_owned = false;
        root_dir_owned = false;
        log_dir_owned = false;
        snapshot_dir_owned = false;
        errdefer self.deinit();
        try fs_paths.createDirPathPortable(self.io_impl.io(), self.layout.snapshot_dir);
        try self.load();
        try self.store.validate();
        try self.validateDurableSnapshotPayload();
        self.cleanupOrphanSnapshotPayloads();
        return self;
    }

    pub fn deinit(self: *PersistentReplicaState) void {
        self.persist_buffer.deinit(self.alloc);
        self.store.deinit();
        self.layout.deinit(self.alloc);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn storage(self: *PersistentReplicaState) raft_engine.core.Storage {
        return .{
            .ptr = self,
            .vtable = &.{
                .initial_state = storageInitialState,
                .entries = storageEntries,
                .term = storageTerm,
                .first_index = storageFirstIndex,
                .last_index = storageLastIndex,
                .snapshot = storageSnapshot,
            },
        };
    }

    pub fn groupStorage(self: *PersistentReplicaState) raft_engine.runtime.storage_iface.GroupStorage {
        return .{
            .ptr = self,
            .vtable = &.{
                .persist_ready = persistReady,
                .compact_snapshot = compactSnapshot,
                .compact_snapshot_artifact = compactSnapshotArtifact,
            },
        };
    }

    pub fn setConfState(self: *PersistentReplicaState, conf_state: raft_engine.core.ConfState) !void {
        try self.store.setConfState(conf_state);
        try self.persist();
    }

    pub fn appliedIndex(self: *const PersistentReplicaState) raft_engine.core.types.Index {
        return self.applied_index;
    }

    pub fn setAppliedIndex(self: *PersistentReplicaState, index: raft_engine.core.types.Index) !void {
        if (index > self.applied_index) self.applied_index = index;
        try self.persist();
    }

    fn persistReady(ptr: *anyopaque, group_id: u64, ready: raft_engine.core.Ready) !void {
        _ = group_id;
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        var previous_snapshot: ?SnapshotIdentity = null;
        if (ready.snapshot) |snapshot| {
            previous_snapshot = snapshotIdentity(self.store.snapshot_state.metadata);
            try self.publishSnapshotPayload(snapshot);
            try self.store.applySnapshot(metadataOnlySnapshot(snapshot));
            if (snapshot.metadata.index > self.applied_index) self.applied_index = snapshot.metadata.index;
        }
        if (ready.hard_state) |hard_state| self.store.setHardState(hard_state);
        if (ready.conf_state) |conf_state| try self.store.setConfState(conf_state);
        if (ready.entries.len > 0) try self.store.append(ready.entries);
        try self.persist();
        if (previous_snapshot) |previous| self.deleteSupersededSnapshotPayload(previous, snapshotIdentity(ready.snapshot.?.metadata));
    }

    fn compactSnapshot(ptr: *anyopaque, group_id: u64, snapshot: raft_engine.core.types.Snapshot, compact_index: u64) !void {
        _ = group_id;
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        const previous = snapshotIdentity(self.store.snapshot_state.metadata);
        try self.publishSnapshotPayload(snapshot);
        try self.store.compactToSnapshot(metadataOnlySnapshot(snapshot), compact_index);
        try self.persist();
        self.deleteSupersededSnapshotPayload(previous, snapshotIdentity(snapshot.metadata));
    }

    fn compactSnapshotArtifact(
        ptr: *anyopaque,
        group_id: u64,
        metadata: raft_engine.core.types.SnapshotMetadata,
        artifact: raft_engine.runtime.storage_iface.SnapshotArtifact,
        compact_index: u64,
    ) !void {
        _ = group_id;
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        const previous = snapshotIdentity(self.store.snapshot_state.metadata);
        try snapshot_payload_store.writeArtifactAtomically(
            self.alloc,
            self.io_impl.io(),
            self.layout.snapshot_dir,
            metadata.index,
            metadata.term,
            artifact,
        );
        try self.store.compactToSnapshot(.{ .metadata = metadata, .data = &.{} }, compact_index);
        try self.persist();
        self.deleteSupersededSnapshotPayload(previous, snapshotIdentity(metadata));
    }

    fn publishSnapshotPayload(self: *PersistentReplicaState, snapshot: raft_engine.core.types.Snapshot) !void {
        try snapshot_payload_store.writeAtomically(
            self.alloc,
            self.io_impl.io(),
            self.layout.snapshot_dir,
            snapshot.metadata.index,
            snapshot.metadata.term,
            snapshot.data,
        );
    }

    fn deleteSupersededSnapshotPayload(self: *PersistentReplicaState, previous: SnapshotIdentity, current: SnapshotIdentity) void {
        if (previous.index == 0 or (previous.index == current.index and previous.term == current.term)) return;
        snapshot_payload_store.delete(self.alloc, self.io_impl.io(), self.layout.snapshot_dir, previous.index, previous.term);
    }

    fn cleanupOrphanSnapshotPayloads(self: *PersistentReplicaState) void {
        const current = snapshotIdentity(self.store.snapshot_state.metadata);
        snapshot_payload_store.cleanupOrphans(
            self.alloc,
            self.io_impl.io(),
            self.layout.snapshot_dir,
            current.index,
            current.term,
        ) catch |err| std.log.warn("raft snapshot payload startup cleanup failed path={s} error={s}", .{
            self.layout.snapshot_dir,
            @errorName(err),
        });
    }

    fn validateDurableSnapshotPayload(self: *PersistentReplicaState) !void {
        const snapshot = self.store.snapshot_state;
        if (snapshot.metadata.index == 0) {
            if (snapshot.data.len != 0) return error.InvalidReplicaState;
            return;
        }
        if (snapshot.data.len != 0) return error.InvalidReplicaState;
        try snapshot_payload_store.validate(
            self.alloc,
            self.io_impl.io(),
            self.layout.snapshot_dir,
            snapshot.metadata.index,
            snapshot.metadata.term,
        );
    }

    fn storageInitialState(ptr: *anyopaque, alloc: std.mem.Allocator) !raft_engine.core.Storage.InitialState {
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        return try self.store.storage().initialState(alloc);
    }

    fn storageEntries(ptr: *anyopaque, alloc: std.mem.Allocator, low: u64, high: u64, max_bytes: usize) ![]raft_engine.core.Entry {
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        return try self.store.storage().entries(alloc, low, high, max_bytes);
    }

    fn storageTerm(ptr: *anyopaque, index: u64) !u64 {
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        return try self.store.storage().term(index);
    }

    fn storageFirstIndex(ptr: *anyopaque) !u64 {
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        return try self.store.storage().firstIndex();
    }

    fn storageLastIndex(ptr: *anyopaque) !u64 {
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        return try self.store.storage().lastIndex();
    }

    fn storageSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator) !raft_engine.core.types.Snapshot {
        const self: *PersistentReplicaState = @ptrCast(@alignCast(ptr));
        var snapshot = try self.store.storage().snapshot(alloc);
        errdefer snapshot.deinit(alloc);
        if (snapshot.data.len == 0 and snapshot.metadata.index != 0) {
            snapshot.data = try snapshot_payload_store.readAlloc(
                alloc,
                self.io_impl.io(),
                self.layout.snapshot_dir,
                snapshot.metadata.index,
                snapshot.metadata.term,
            );
        }
        return snapshot;
    }

    fn load(self: *PersistentReplicaState) !void {
        const path = try self.statePath();
        defer self.alloc.free(path);
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io(), path, self.alloc, .limited(16 << 20)) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer self.alloc.free(bytes);
        if (bytes.len == 0) return;

        var cursor: usize = 0;
        if (try readInt(u32, bytes, &cursor) != magic) return error.InvalidReplicaState;
        const file_version = try readInt(u32, bytes, &cursor);
        if (file_version != version) return error.UnsupportedReplicaStateVersion;

        self.store.setHardState(.{
            .current_term = try readInt(u64, bytes, &cursor),
            .voted_for = if (try readBool(bytes, &cursor)) try readInt(u64, bytes, &cursor) else null,
            .commit_index = try readInt(u64, bytes, &cursor),
        });
        self.applied_index = try readInt(u64, bytes, &cursor);

        var conf_state = try decodeConfState(self.alloc, bytes, &cursor);
        defer conf_state.deinit(self.alloc);
        try self.store.setConfState(conf_state);

        const has_snapshot = try readBool(bytes, &cursor);
        if (has_snapshot) {
            const snapshot = try decodeSnapshot(self.alloc, bytes, &cursor);
            defer {
                var owned = snapshot;
                owned.deinit(self.alloc);
            }
            try self.store.applySnapshot(snapshot);
        }

        const compacted_index = try readInt(u64, bytes, &cursor);
        const compacted_term = try readInt(u64, bytes, &cursor);
        try self.store.restoreCompactionBoundary(compacted_index, compacted_term);

        const entry_count = try readInt(u32, bytes, &cursor);
        if (entry_count > 0) {
            const entries = try self.alloc.alloc(raft_engine.core.Entry, entry_count);
            defer {
                raft_engine.core.types.freeEntries(self.alloc, entries);
            }
            for (entries) |*entry| entry.* = try decodeEntry(self.alloc, bytes, &cursor);
            try self.store.append(entries);
        }
    }

    fn persist(self: *PersistentReplicaState) !void {
        self.persist_buffer.clearRetainingCapacity();
        const buffer = &self.persist_buffer;

        try appendInt(u32, self.alloc, buffer, magic);
        try appendInt(u32, self.alloc, buffer, version);

        try appendInt(u64, self.alloc, buffer, self.store.hard_state.current_term);
        try appendBool(self.alloc, buffer, self.store.hard_state.voted_for != null);
        if (self.store.hard_state.voted_for) |voted_for| try appendInt(u64, self.alloc, buffer, voted_for);
        try appendInt(u64, self.alloc, buffer, self.store.hard_state.commit_index);
        try appendInt(u64, self.alloc, buffer, self.applied_index);
        try encodeConfState(self.alloc, buffer, self.store.conf_state);

        const snapshot = self.store.snapshot_state;
        const has_snapshot = snapshot.metadata.index != 0 or snapshot.metadata.term != 0 or snapshot.data.len > 0 or snapshot.metadata.conf_state.voters.len > 0;
        try appendBool(self.alloc, buffer, has_snapshot);
        if (has_snapshot) try encodeSnapshot(self.alloc, buffer, snapshot);
        try appendInt(u64, self.alloc, buffer, self.store.compactedIndex());
        try appendInt(u64, self.alloc, buffer, self.store.compactedTerm());

        const entries = self.store.entries_state.items;
        try appendInt(u32, self.alloc, buffer, @intCast(entries.len));
        for (entries) |entry| try encodeEntry(self.alloc, buffer, entry);

        const path = try self.statePath();
        defer self.alloc.free(path);
        try fs_paths.createDirPathPortable(self.io(), self.layout.log_dir);
        const tmp_path = try std.fmt.allocPrint(self.alloc, "{s}.tmp-{d}", .{
            path,
            state_publish_nonce.fetchAdd(1, .monotonic),
        });
        defer self.alloc.free(tmp_path);
        errdefer std.Io.Dir.cwd().deleteFile(self.io(), tmp_path) catch {};
        {
            var file = try fs_paths.createFilePortable(self.io(), tmp_path, .{ .truncate = true });
            defer file.close(self.io());
            var writer_buffer: [64 * 1024]u8 = undefined;
            var writer = file.writer(self.io(), &writer_buffer);
            try writer.interface.writeAll(buffer.items);
            try writer.end();
            try file.sync(self.io());
        }
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, self.io());
        try fs_paths.syncDirPortable(self.io(), self.layout.log_dir);
    }

    fn statePath(self: *const PersistentReplicaState) ![]u8 {
        return try std.fmt.allocPrint(self.alloc, "{s}/state.bin", .{self.layout.log_dir});
    }

    fn io(self: *PersistentReplicaState) std.Io {
        return self.io_impl.io();
    }

    fn appendInt(comptime T: type, alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: T) !void {
        var bytes: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &bytes, value, .little);
        try out.appendSlice(alloc, &bytes);
    }

    fn appendBool(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: bool) !void {
        try out.append(alloc, @intFromBool(value));
    }

    fn appendBytes(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
        try appendInt(u32, alloc, out, @intCast(bytes.len));
        try out.appendSlice(alloc, bytes);
    }

    fn readInt(comptime T: type, bytes: []const u8, cursor: *usize) !T {
        if (cursor.* + @sizeOf(T) > bytes.len) return error.InvalidReplicaState;
        var buf: [@sizeOf(T)]u8 = undefined;
        @memcpy(&buf, bytes[cursor.* .. cursor.* + @sizeOf(T)]);
        cursor.* += @sizeOf(T);
        return std.mem.readInt(T, &buf, .little);
    }

    fn readBool(bytes: []const u8, cursor: *usize) !bool {
        if (cursor.* >= bytes.len) return error.InvalidReplicaState;
        const value = bytes[cursor.*] != 0;
        cursor.* += 1;
        return value;
    }

    fn readBytes(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) ![]u8 {
        const len = try readInt(u32, bytes, cursor);
        if (cursor.* + len > bytes.len) return error.InvalidReplicaState;
        defer cursor.* += len;
        return try alloc.dupe(u8, bytes[cursor.* .. cursor.* + len]);
    }

    fn encodeNodeList(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), nodes: []const u64) !void {
        try appendInt(u32, alloc, out, @intCast(nodes.len));
        for (nodes) |node_id| try appendInt(u64, alloc, out, node_id);
    }

    fn decodeNodeList(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) ![]u64 {
        const len = try readInt(u32, bytes, cursor);
        const out = try alloc.alloc(u64, len);
        errdefer alloc.free(out);
        for (out) |*node_id| node_id.* = try readInt(u64, bytes, cursor);
        return out;
    }

    fn encodeConfState(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), conf_state: raft_engine.core.ConfState) !void {
        try encodeNodeList(alloc, out, conf_state.voters);
        try encodeNodeList(alloc, out, conf_state.voters_outgoing);
        try encodeNodeList(alloc, out, conf_state.learners);
        try encodeNodeList(alloc, out, conf_state.learners_next);
        try appendBool(alloc, out, conf_state.auto_leave);
    }

    fn decodeConfState(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) !raft_engine.core.ConfState {
        return .{
            .voters = try decodeNodeList(alloc, bytes, cursor),
            .voters_outgoing = try decodeNodeList(alloc, bytes, cursor),
            .learners = try decodeNodeList(alloc, bytes, cursor),
            .learners_next = try decodeNodeList(alloc, bytes, cursor),
            .auto_leave = try readBool(bytes, cursor),
        };
    }

    fn encodeSnapshot(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), snapshot: raft_engine.core.types.Snapshot) !void {
        try appendInt(u64, alloc, out, snapshot.metadata.index);
        try appendInt(u64, alloc, out, snapshot.metadata.term);
        try encodeConfState(alloc, out, snapshot.metadata.conf_state);
        try appendBytes(alloc, out, snapshot.data);
    }

    fn decodeSnapshot(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) !raft_engine.core.types.Snapshot {
        return .{
            .metadata = .{
                .index = try readInt(u64, bytes, cursor),
                .term = try readInt(u64, bytes, cursor),
                .conf_state = try decodeConfState(alloc, bytes, cursor),
            },
            .data = try readBytes(alloc, bytes, cursor),
        };
    }

    fn encodeEntry(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), entry: raft_engine.core.Entry) !void {
        try appendInt(u64, alloc, out, entry.term);
        try appendInt(u64, alloc, out, entry.index);
        try out.append(alloc, @intFromEnum(entry.entry_type));
        try appendBytes(alloc, out, entry.data);
    }

    fn decodeEntry(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) !raft_engine.core.Entry {
        const term = try readInt(u64, bytes, cursor);
        const index = try readInt(u64, bytes, cursor);
        const entry_type_tag = if (cursor.* < bytes.len) bytes[cursor.*] else return error.InvalidReplicaState;
        cursor.* += 1;
        const entry_type: raft_engine.core.types.EntryType = switch (entry_type_tag) {
            @intFromEnum(raft_engine.core.types.EntryType.normal) => .normal,
            @intFromEnum(raft_engine.core.types.EntryType.conf_change) => .conf_change,
            @intFromEnum(raft_engine.core.types.EntryType.conf_change_v2) => .conf_change_v2,
            else => return error.InvalidReplicaState,
        };
        return .{
            .term = term,
            .index = index,
            .entry_type = entry_type,
            .data = try readBytes(alloc, bytes, cursor),
        };
    }
};

test "persistent replica state persists ready updates across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 77, 3);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();

        const data_one = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(data_one);
        const data_two = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(data_two);

        try state.groupStorage().persistReady(77, .{
            .hard_state = .{ .current_term = 4, .voted_for = 2, .commit_index = 2 },
            .conf_state = .{ .voters = @constCast((&[_]u64{ 1, 2, 3 })[0..]) },
            .entries = &.{
                .{ .term = 4, .index = 1, .data = data_one },
                .{ .term = 4, .index = 2, .data = data_two },
            },
        });
    }

    {
        var reopened = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer reopened.deinit();
        var initial_state = try reopened.storage().initialState(std.testing.allocator);
        defer initial_state.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u64, 4), initial_state.hard_state.current_term);
        try std.testing.expectEqual(@as(?u64, 2), initial_state.hard_state.voted_for);
        try std.testing.expectEqual(@as(u64, 2), initial_state.hard_state.commit_index);
        try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, initial_state.conf_state.voters);

        const entries = try reopened.storage().entries(std.testing.allocator, 1, 3, 0);
        defer raft_engine.core.types.freeEntries(std.testing.allocator, entries);
        try std.testing.expectEqual(@as(usize, 2), entries.len);
        try std.testing.expectEqualStrings("one", entries[0].data);
        try std.testing.expectEqualStrings("two", entries[1].data);
    }
}

test "persistent replica state replays committed entries when append persisted before applied watermark" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/append-before-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 78, 3);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        const data_one = try std.testing.allocator.dupe(u8, "one");
        defer std.testing.allocator.free(data_one);
        const data_two = try std.testing.allocator.dupe(u8, "two");
        defer std.testing.allocator.free(data_two);

        try state.groupStorage().persistReady(78, .{
            .hard_state = .{ .current_term = 3, .voted_for = 1, .commit_index = 2 },
            .entries = &.{
                .{ .term = 3, .index = 1, .data = data_one },
                .{ .term = 3, .index = 2, .data = data_two },
            },
        });
    }

    {
        var reopened = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer reopened.deinit();
        try std.testing.expectEqual(@as(u64, 0), reopened.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 78,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
        try std.testing.expectEqual(@as(u64, 1), rd.committed_entries[0].index);
        try std.testing.expectEqual(@as(u64, 2), rd.committed_entries[1].index);
    }
}

test "persistent replica state persists applied watermark and replays only unapplied suffix after snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/applied-replay", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 79, 4);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();

        const snapshot_voters = try std.testing.allocator.dupe(u64, &.{1});
        defer std.testing.allocator.free(snapshot_voters);
        const snapshot_data = try std.testing.allocator.dupe(u8, "snap");
        defer std.testing.allocator.free(snapshot_data);
        const ten_data = try std.testing.allocator.dupe(u8, "ten");
        defer std.testing.allocator.free(ten_data);
        const eleven_data = try std.testing.allocator.dupe(u8, "eleven");
        defer std.testing.allocator.free(eleven_data);

        try state.groupStorage().persistReady(79, .{
            .hard_state = .{ .current_term = 4, .voted_for = 1, .commit_index = 11 },
            .snapshot = .{
                .metadata = .{
                    .index = 9,
                    .term = 4,
                    .conf_state = .{ .voters = snapshot_voters },
                },
                .data = snapshot_data,
            },
            .entries = &.{
                .{ .term = 4, .index = 10, .data = ten_data },
                .{ .term = 4, .index = 11, .data = eleven_data },
            },
        });
        try state.setAppliedIndex(10);
    }

    {
        var reopened = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer reopened.deinit();
        try std.testing.expectEqual(@as(u64, 10), reopened.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 79,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = reopened.appliedIndex(),
        }, reopened.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);
        try std.testing.expectEqual(@as(u64, 11), rd.committed_entries[0].index);
        try std.testing.expectEqualStrings("eleven", rd.committed_entries[0].data);
    }
}

test "persistent replica state persists snapshots across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 88, 4);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        const voters = try std.testing.allocator.dupe(u64, &.{ 4, 5 });
        defer std.testing.allocator.free(voters);
        const data = try std.testing.allocator.dupe(u8, "snap");
        defer std.testing.allocator.free(data);
        try state.groupStorage().persistReady(88, .{
            .snapshot = .{
                .metadata = .{
                    .index = 9,
                    .term = 6,
                    .conf_state = .{ .voters = voters },
                },
                .data = data,
            },
            .hard_state = .{ .current_term = 6, .commit_index = 9 },
        });
    }

    {
        var reopened = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer reopened.deinit();
        try std.testing.expectEqual(@as(usize, 0), reopened.store.snapshot_state.data.len);
        const snapshot = try reopened.storage().snapshot(std.testing.allocator);
        defer {
            var owned = snapshot;
            owned.deinit(std.testing.allocator);
        }
        try std.testing.expectEqual(@as(u64, 9), snapshot.metadata.index);
        try std.testing.expectEqual(@as(u64, 6), snapshot.metadata.term);
        try std.testing.expectEqualStrings("snap", snapshot.data);
        try std.testing.expectEqualSlices(u64, &.{ 4, 5 }, snapshot.metadata.conf_state.voters);
    }
}

test "persistent replica state refuses a corrupt durable snapshot payload on reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/corrupt-persistent-snapshot", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 89, 4);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        try state.groupStorage().persistReady(89, .{
            .snapshot = .{
                .metadata = .{ .index = 7, .term = 3 },
                .data = @constCast("durable-state"),
            },
        });
    }

    const path = try snapshot_payload_store.pathAlloc(std.testing.allocator, layout.snapshot_dir, 7, 3);
    defer std.testing.allocator.free(path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var file = try std.Io.Dir.cwd().openFile(io_impl.io(), path, .{ .mode = .read_write });
    try file.writePositionalAll(io_impl.io(), "X", 72);
    file.close(io_impl.io());

    try std.testing.expectError(
        error.SnapshotPayloadChecksumMismatch,
        PersistentReplicaState.init(std.testing.allocator, layout),
    );
}

test "persistent replica state publishes an artifact snapshot and reopens it" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/persistent-artifact-snapshot", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 90, 4);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        try state.groupStorage().persistReady(90, .{
            .hard_state = .{ .current_term = 3, .commit_index = 4 },
            .entries = &.{
                .{ .index = 1, .term = 3, .data = @constCast("one") },
                .{ .index = 2, .term = 3, .data = @constCast("two") },
                .{ .index = 3, .term = 3, .data = @constCast("three") },
                .{ .index = 4, .term = 3, .data = @constCast("four") },
            },
        });

        const spool_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/snapshot-spool", .{layout.root_dir});
        defer std.testing.allocator.free(spool_path);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = spool_path, .data = "artifact-state" });
        const artifact = try file_snapshot_artifact.FileSnapshotArtifact.create(
            std.testing.allocator,
            std.testing.io,
            spool_path,
            "artifact-state".len,
        );
        defer artifact.deinit();
        try state.groupStorage().compactSnapshotArtifact(std.testing.allocator, 90, .{
            .index = 4,
            .term = 3,
        }, artifact, 2);
    }

    var reopened = try PersistentReplicaState.init(std.testing.allocator, layout);
    defer reopened.deinit();
    var snapshot = try reopened.storage().snapshot(std.testing.allocator);
    defer snapshot.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 4), snapshot.metadata.index);
    try std.testing.expectEqualStrings("artifact-state", snapshot.data);
    try std.testing.expectEqual(@as(u64, 3), try reopened.storage().firstIndex());
    try std.testing.expectEqual(@as(u64, 4), try reopened.storage().lastIndex());
    try std.testing.expectEqual(@as(u64, 3), try reopened.storage().term(2));
}

test "persistent replica state recovers both snapshot publication crash windows" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/persistent-snapshot-crash-windows", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var layout = try storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 91, 4);
    defer layout.deinit(std.testing.allocator);

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        try state.groupStorage().persistReady(91, .{
            .snapshot = .{
                .metadata = .{ .index = 5, .term = 2 },
                .data = @constCast("generation-five"),
            },
        });
    }

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    // Crash after the next sidecar is durable but before the replica
    // checkpoint names it: reopen must retain generation five and remove six.
    try snapshot_payload_store.writeAtomically(
        std.testing.allocator,
        io,
        layout.snapshot_dir,
        6,
        3,
        "generation-six",
    );
    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        var snapshot = try state.storage().snapshot(std.testing.allocator);
        defer snapshot.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u64, 5), snapshot.metadata.index);
        try std.testing.expectEqualStrings("generation-five", snapshot.data);
    }
    const generation_six_path = try snapshot_payload_store.pathAlloc(std.testing.allocator, layout.snapshot_dir, 6, 3);
    defer std.testing.allocator.free(generation_six_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, generation_six_path, .{}));

    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        try state.groupStorage().persistReady(91, .{
            .snapshot = .{
                .metadata = .{ .index = 6, .term = 3 },
                .data = @constCast("generation-six"),
            },
        });
    }

    // Crash after the checkpoint names generation six but before stale
    // sidecar cleanup: reopen must validate six and remove the old sidecar.
    try snapshot_payload_store.writeAtomically(
        std.testing.allocator,
        io,
        layout.snapshot_dir,
        5,
        2,
        "generation-five",
    );
    {
        var state = try PersistentReplicaState.init(std.testing.allocator, layout);
        defer state.deinit();
        var snapshot = try state.storage().snapshot(std.testing.allocator);
        defer snapshot.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u64, 6), snapshot.metadata.index);
        try std.testing.expectEqualStrings("generation-six", snapshot.data);
    }
    const generation_five_path = try snapshot_payload_store.pathAlloc(std.testing.allocator, layout.snapshot_dir, 5, 2);
    defer std.testing.allocator.free(generation_five_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, generation_five_path, .{}));
}
