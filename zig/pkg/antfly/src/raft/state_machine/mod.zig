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
pub const applied_sink = @import("applied_sink.zig");
pub const read_state_observer = @import("read_state_observer.zig");
pub const metadata = @import("metadata.zig");
pub const data = @import("data.zig");
pub const metadata_store = @import("metadata_store.zig");
pub const data_store = @import("data_store.zig");
pub const router = @import("router.zig");

pub const ApplyBatch = struct {
    group_id: u64,
    commit_index: u64,
    entries_bytes: []const u8 = &.{},
};

pub const DecodedCommittedEntry = struct {
    term: u64,
    index: u64,
    entry_type: @import("raft_engine").core.types.EntryType,
    data: []const u8,
};

pub const SnapshotBuilder = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        build_snapshot: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) anyerror![]u8,
        apply_batch: *const fn (ptr: *anyopaque, batch: ApplyBatch) anyerror!void,
    };

    pub fn buildSnapshot(self: SnapshotBuilder, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        return try self.vtable.build_snapshot(self.ptr, alloc, group_id);
    }

    pub fn applyBatch(self: SnapshotBuilder, batch: ApplyBatch) !void {
        return try self.vtable.apply_batch(self.ptr, batch);
    }
};

pub fn encodeCommittedEntries(alloc: std.mem.Allocator, entries: []const @import("raft_engine").core.Entry) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try appendInt(alloc, &out, u32, @intCast(entries.len));
    for (entries) |entry| {
        try appendInt(alloc, &out, u64, entry.term);
        try appendInt(alloc, &out, u64, entry.index);
        try out.append(alloc, @intFromEnum(entry.entry_type));
        try appendInt(alloc, &out, u32, @intCast(entry.data.len));
        try out.appendSlice(alloc, entry.data);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeCommittedEntries(alloc: std.mem.Allocator, encoded: []const u8) ![]DecodedCommittedEntry {
    if (encoded.len < @sizeOf(u32)) return error.InvalidCommittedEntriesEncoding;

    var pos: usize = 0;
    const entry_count = try readInt(encoded, &pos, u32);
    const remaining = encoded.len - pos;
    const min_entry_bytes = (@sizeOf(u64) * 2) + 1 + @sizeOf(u32);
    if (entry_count > 0 and remaining / min_entry_bytes < entry_count) {
        return error.InvalidCommittedEntriesEncoding;
    }
    const decoded = try alloc.alloc(DecodedCommittedEntry, entry_count);
    errdefer alloc.free(decoded);

    for (decoded) |*entry| {
        const term = try readInt(encoded, &pos, u64);
        const index = try readInt(encoded, &pos, u64);
        if (pos >= encoded.len) return error.InvalidCommittedEntriesEncoding;
        const entry_type: @import("raft_engine").core.types.EntryType = @enumFromInt(encoded[pos]);
        pos += 1;
        const data_len = try readInt(encoded, &pos, u32);
        if (pos + data_len > encoded.len) return error.InvalidCommittedEntriesEncoding;
        entry.* = .{
            .term = term,
            .index = index,
            .entry_type = entry_type,
            .data = encoded[pos .. pos + data_len],
        };
        pos += data_len;
    }

    if (pos != encoded.len) return error.InvalidCommittedEntriesEncoding;
    return decoded;
}

fn appendInt(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn readInt(encoded: []const u8, pos: *usize, comptime T: type) !T {
    if (pos.* + @sizeOf(T) > encoded.len) return error.InvalidCommittedEntriesEncoding;
    const bytes: *const [@sizeOf(T)]u8 = @ptrCast(encoded[pos.* .. pos.* + @sizeOf(T)]);
    const value = std.mem.readInt(T, bytes, .little);
    pos.* += @sizeOf(T);
    return value;
}

pub const AppliedIndexSink = applied_sink.AppliedIndexSink;
pub const noopAppliedIndexSink = applied_sink.noopAppliedIndexSink;
pub const ReadStateObserver = read_state_observer.ReadStateObserver;
pub const MetadataStateMachine = metadata.MetadataStateMachine;
pub const DataStateMachine = data.DataStateMachine;
pub const MetadataStore = metadata_store.MetadataStore;
pub const MetadataStoreConfig = metadata_store.MetadataStoreConfig;
pub const DataStore = data_store.DataStore;
pub const DataStoreConfig = data_store.DataStoreConfig;
pub const RoutedStateMachine = router.RoutedStateMachine;

test "raft state_machine module compiles" {
    _ = ApplyBatch;
    _ = SnapshotBuilder;
    _ = AppliedIndexSink;
    _ = ReadStateObserver;
    _ = MetadataStateMachine;
    _ = DataStateMachine;
    _ = MetadataStore;
    _ = MetadataStoreConfig;
    _ = DataStore;
    _ = DataStoreConfig;
    _ = RoutedStateMachine;
    _ = encodeCommittedEntries;
}

test "routed state machine uses metadata and data apply paths" {
    const SinkRecorder = struct {
        metadata_group_id: u64,
        metadata_applied: u64 = 0,
        data_applied: u64 = 0,

        fn sink(self: *@This()) AppliedIndexSink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .set_applied_index = setAppliedIndex,
                },
            };
        }

        fn setAppliedIndex(ptr: *anyopaque, group_id: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == self.metadata_group_id) {
                self.metadata_applied = index;
            } else {
                self.data_applied = index;
            }
        }
    };

    var sink_recorder = SinkRecorder{ .metadata_group_id = 42 };
    var metadata_sm = MetadataStateMachine{ .alloc = std.testing.allocator, .applied_sink = sink_recorder.sink() };
    var data_sm = DataStateMachine{ .alloc = std.testing.allocator, .applied_sink = sink_recorder.sink() };
    var routed = RoutedStateMachine{
        .metadata_group_id = 42,
        .metadata_state_machine = metadata_sm.stateMachine(),
        .data_state_machine = data_sm.stateMachine(),
    };

    try routed.stateMachine().applyReady(42, &.{.{ .term = 1, .index = 7 }}, &.{});
    try routed.stateMachine().applyReady(99, &.{.{ .term = 1, .index = 11 }}, &.{});

    try std.testing.expectEqual(@as(u64, 7), sink_recorder.metadata_applied);
    try std.testing.expectEqual(@as(u64, 11), sink_recorder.data_applied);
}

test "routed state machine forwards read states into observer" {
    const Recorder = struct {
        group_id: u64 = 0,
        read_state_count: usize = 0,

        fn observer(self: *@This()) ReadStateObserver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .on_read_states = onReadStates,
                },
            };
        }

        fn onReadStates(ptr: *anyopaque, group_id: u64, read_states: []const @import("raft_engine").core.ReadState) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.group_id = group_id;
            self.read_state_count = read_states.len;
        }
    };

    const SinkRecorder = struct {
        fn sink() AppliedIndexSink {
            return noopAppliedIndexSink();
        }
    };

    var recorder = Recorder{};
    var metadata_sm = MetadataStateMachine{ .alloc = std.testing.allocator, .applied_sink = SinkRecorder.sink() };
    var data_sm = DataStateMachine{ .alloc = std.testing.allocator, .applied_sink = SinkRecorder.sink() };
    var routed = RoutedStateMachine{
        .metadata_group_id = 42,
        .metadata_state_machine = metadata_sm.stateMachine(),
        .data_state_machine = data_sm.stateMachine(),
        .read_state_observer = recorder.observer(),
    };
    const ctx = try std.testing.allocator.dupe(u8, "readable");
    defer std.testing.allocator.free(ctx);

    try routed.stateMachine().applyReady(42, &.{}, &.{.{
        .index = 9,
        .request_ctx = ctx,
    }});

    try std.testing.expectEqual(@as(u64, 42), recorder.group_id);
    try std.testing.expectEqual(@as(usize, 1), recorder.read_state_count);
}

test "encode committed entries produces a stable payload" {
    const hello = try std.testing.allocator.dupe(u8, "hello");
    defer std.testing.allocator.free(hello);
    const world = try std.testing.allocator.dupe(u8, "world");
    defer std.testing.allocator.free(world);

    const encoded = try encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 7, .entry_type = .normal, .data = hello },
        .{ .term = 3, .index = 8, .entry_type = .conf_change, .data = world },
    });
    defer std.testing.allocator.free(encoded);

    try std.testing.expect(encoded.len > hello.len + world.len);
}

test "committed entries round-trip through decode" {
    const alpha = try std.testing.allocator.dupe(u8, "alpha");
    defer std.testing.allocator.free(alpha);
    const beta = try std.testing.allocator.dupe(u8, "beta");
    defer std.testing.allocator.free(beta);

    const encoded = try encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 5, .index = 21, .entry_type = .normal, .data = alpha },
        .{ .term = 5, .index = 22, .entry_type = .conf_change_v2, .data = beta },
    });
    defer std.testing.allocator.free(encoded);

    const decoded = try decodeCommittedEntries(std.testing.allocator, encoded);
    defer std.testing.allocator.free(decoded);

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqual(@as(u64, 21), decoded[0].index);
    try std.testing.expectEqual(@as(u64, 5), decoded[1].term);
    try std.testing.expectEqual(.conf_change_v2, decoded[1].entry_type);
    try std.testing.expectEqualStrings("beta", decoded[1].data);
}

test "metadata and data state machines forward applied batches into snapshot builders" {
    const BuilderRecorder = struct {
        last_group_id: u64 = 0,
        last_commit_index: u64 = 0,
        last_payload_len: usize = 0,

        fn builder(self: *@This()) SnapshotBuilder {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_snapshot = buildSnapshot,
                    .apply_batch = applyBatch,
                },
            };
        }

        fn buildSnapshot(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            return try std.fmt.allocPrint(alloc, "snapshot-{d}", .{group_id});
        }

        fn applyBatch(ptr: *anyopaque, batch: ApplyBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_group_id = batch.group_id;
            self.last_commit_index = batch.commit_index;
            self.last_payload_len = batch.entries_bytes.len;
        }
    };

    const SinkRecorder = struct {
        last_index: u64 = 0,

        fn sink(self: *@This()) AppliedIndexSink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .set_applied_index = setAppliedIndex,
                },
            };
        }

        fn setAppliedIndex(ptr: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_index = index;
        }
    };

    var metadata_builder = BuilderRecorder{};
    var data_builder = BuilderRecorder{};
    var metadata_sink = SinkRecorder{};
    var data_sink = SinkRecorder{};
    var metadata_sm = MetadataStateMachine{
        .alloc = std.testing.allocator,
        .applied_sink = metadata_sink.sink(),
        .snapshot_builder = metadata_builder.builder(),
    };
    var data_sm = DataStateMachine{
        .alloc = std.testing.allocator,
        .applied_sink = data_sink.sink(),
        .snapshot_builder = data_builder.builder(),
    };

    try metadata_sm.stateMachine().applyReady(7, &.{.{ .term = 2, .index = 9, .data = @constCast("meta") }}, &.{});
    try data_sm.stateMachine().applyReady(8, &.{.{ .term = 3, .index = 11, .data = @constCast("data") }}, &.{});

    try std.testing.expectEqual(@as(u64, 7), metadata_builder.last_group_id);
    try std.testing.expectEqual(@as(u64, 9), metadata_builder.last_commit_index);
    try std.testing.expect(metadata_builder.last_payload_len > 0);
    try std.testing.expectEqual(@as(u64, 9), metadata_sink.last_index);

    try std.testing.expectEqual(@as(u64, 8), data_builder.last_group_id);
    try std.testing.expectEqual(@as(u64, 11), data_builder.last_commit_index);
    try std.testing.expect(data_builder.last_payload_len > 0);
    try std.testing.expectEqual(@as(u64, 11), data_sink.last_index);
}
