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
const codec_iface = @import("codec_iface.zig");
const transport_iface = @import("transport_iface.zig");

const magic = "MRPB";
const version: u8 = 1;
const frame_raft_peer_batch: u8 = 1;

pub const BinaryCodec = struct {
    pub fn codec() codec_iface.MessageCodec {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .encode_peer_batch = encodePeerBatch,
                .decode_frame = decodeFrame,
                .free_frame = freeFrame,
                .free_decoded = freeDecoded,
            },
        };
    }

    fn encodePeerBatch(_: *anyopaque, alloc: std.mem.Allocator, batch: transport_iface.PeerBatch) !codec_iface.EncodedFrame {
        const out = try alloc.alloc(u8, encodedPeerBatchLen(batch));
        errdefer alloc.free(out);

        var cursor: usize = 0;
        appendFixedBytes(out, &cursor, magic);
        appendFixedByte(out, &cursor, version);
        appendFixedByte(out, &cursor, frame_raft_peer_batch);
        appendFixedInt(u64, out, &cursor, batch.peer_id);
        appendFixedInt(u32, out, &cursor, @intCast(batch.groups.len));
        for (batch.groups) |group| {
            appendFixedInt(u64, out, &cursor, group.group_id);
            appendFixedInt(u32, out, &cursor, @intCast(group.messages.len));
            for (group.messages) |msg| try encodeMessageFixed(out, &cursor, msg);
        }
        std.debug.assert(cursor == out.len);

        return .{
            .bytes = out,
            .media_type = "application/x-antflydb-raft-binary-v1",
        };
    }

    fn encodedPeerBatchLen(batch: transport_iface.PeerBatch) usize {
        var total: usize = magic.len + 2 + @sizeOf(u64) + @sizeOf(u32);
        for (batch.groups) |group| {
            total += @sizeOf(u64) + @sizeOf(u32);
            for (group.messages) |msg| total += encodedMessageLen(msg);
        }
        return total;
    }

    fn encodedMessageLen(msg: core.Message) usize {
        if (msg.responses.len != 0) unreachable;

        var total: usize =
            1 + // msg_type
            @sizeOf(u64) + // from
            @sizeOf(u64) + // to
            @sizeOf(u64) + // term
            1 + // has_vote
            @sizeOf(u64) + // vote
            @sizeOf(u64) + // log_index
            @sizeOf(u64) + // log_term
            @sizeOf(u64) + // commit_index
            1 + // reject
            @sizeOf(u64) + // reject_hint
            encodedBytesLen(msg.context) +
            @sizeOf(u32) + // entry count
            1; // has_snapshot

        for (msg.entries) |entry| total += encodedEntryLen(entry);
        if (msg.snapshot) |snapshot| total += encodedSnapshotLen(snapshot);
        return total;
    }

    fn encodedEntryLen(entry: core.Entry) usize {
        return @sizeOf(u64) + @sizeOf(u64) + 1 + encodedBytesLen(entry.data);
    }

    fn encodedSnapshotLen(snapshot: core.types.Snapshot) usize {
        return @sizeOf(u64) + // metadata.index
            @sizeOf(u64) + // metadata.term
            encodedNodeListLen(snapshot.metadata.conf_state.voters) +
            encodedNodeListLen(snapshot.metadata.conf_state.voters_outgoing) +
            encodedNodeListLen(snapshot.metadata.conf_state.learners) +
            encodedNodeListLen(snapshot.metadata.conf_state.learners_next) +
            1 + // auto_leave
            encodedBytesLen(snapshot.data);
    }

    fn encodedNodeListLen(nodes: []const core.types.NodeId) usize {
        return @sizeOf(u32) + nodes.len * @sizeOf(u64);
    }

    fn encodedBytesLen(bytes: []const u8) usize {
        return @sizeOf(u32) + bytes.len;
    }

    fn decodeFrame(_: *anyopaque, alloc: std.mem.Allocator, frame: codec_iface.EncodedFrame) !codec_iface.DecodedFrame {
        if (frame.bytes.len < 6 or !std.mem.eql(u8, frame.bytes[0..4], magic)) return error.InvalidCodecFrame;
        if (frame.bytes[4] != version) return error.UnsupportedCodecVersion;

        var cursor: usize = 6;
        return switch (frame.bytes[5]) {
            frame_raft_peer_batch => .{
                .raft_peer_batch = try decodePeerBatch(alloc, frame.bytes, &cursor),
            },
            else => error.UnsupportedCodecFrame,
        };
    }

    fn decodePeerBatch(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) !transport_iface.PeerBatch {
        const peer_id = try readInt(u64, data, cursor);
        const group_count: usize = @intCast(try readInt(u32, data, cursor));
        const groups = try alloc.alloc(transport_iface.GroupMessageBatch, group_count);
        errdefer freeOwnedGroups(alloc, groups);

        for (groups) |*group| {
            group.group_id = try readInt(u64, data, cursor);
            const message_count: usize = @intCast(try readInt(u32, data, cursor));
            const messages = try alloc.alloc(core.Message, message_count);
            var initialized: usize = 0;
            errdefer {
                for (messages[0..initialized]) |msg| {
                    var owned = msg;
                    owned.deinit(alloc);
                }
                alloc.free(messages);
            }
            for (0..messages.len) |i| {
                messages[i] = try decodeMessage(alloc, data, cursor);
                initialized += 1;
            }
            group.messages = messages;
        }

        return .{
            .peer_id = peer_id,
            .groups = groups,
        };
    }

    fn encodeMessage(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), msg: core.Message) !void {
        if (msg.responses.len != 0) return error.UnsupportedMessageResponses;

        try out.append(alloc, @intFromEnum(msg.msg_type));
        try appendInt(u64, alloc, out, msg.from);
        try appendInt(u64, alloc, out, msg.to);
        try appendInt(u64, alloc, out, msg.term);
        try out.append(alloc, @intFromBool(msg.vote != null));
        try appendInt(u64, alloc, out, msg.vote orelse 0);
        try appendInt(u64, alloc, out, msg.log_index);
        try appendInt(u64, alloc, out, msg.log_term);
        try appendInt(u64, alloc, out, msg.commit_index);
        try out.append(alloc, @intFromBool(msg.reject));
        try appendInt(u64, alloc, out, msg.reject_hint);
        try appendBytes(alloc, out, msg.context);
        try appendInt(u32, alloc, out, @intCast(msg.entries.len));
        for (msg.entries) |entry| try encodeEntry(alloc, out, entry);
        try out.append(alloc, @intFromBool(msg.snapshot != null));
        if (msg.snapshot) |snapshot| try encodeSnapshot(alloc, out, snapshot);
    }

    fn encodeMessageFixed(out: []u8, cursor: *usize, msg: core.Message) !void {
        if (msg.responses.len != 0) return error.UnsupportedMessageResponses;

        appendFixedByte(out, cursor, @intFromEnum(msg.msg_type));
        appendFixedInt(u64, out, cursor, msg.from);
        appendFixedInt(u64, out, cursor, msg.to);
        appendFixedInt(u64, out, cursor, msg.term);
        appendFixedByte(out, cursor, @intFromBool(msg.vote != null));
        appendFixedInt(u64, out, cursor, msg.vote orelse 0);
        appendFixedInt(u64, out, cursor, msg.log_index);
        appendFixedInt(u64, out, cursor, msg.log_term);
        appendFixedInt(u64, out, cursor, msg.commit_index);
        appendFixedByte(out, cursor, @intFromBool(msg.reject));
        appendFixedInt(u64, out, cursor, msg.reject_hint);
        appendFixedBytesLen(out, cursor, msg.context);
        appendFixedInt(u32, out, cursor, @intCast(msg.entries.len));
        for (msg.entries) |entry| encodeEntryFixed(out, cursor, entry);
        appendFixedByte(out, cursor, @intFromBool(msg.snapshot != null));
        if (msg.snapshot) |snapshot| encodeSnapshotFixed(out, cursor, snapshot);
    }

    fn decodeMessage(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) !core.Message {
        if (cursor.* >= data.len) return error.InvalidCodecFrame;
        const msg_type = decodeMessageType(data[cursor.*]) orelse return error.InvalidCodecFrame;
        cursor.* += 1;
        const from = try readInt(u64, data, cursor);
        const to = try readInt(u64, data, cursor);
        const term = try readInt(u64, data, cursor);
        const has_vote = try readBool(data, cursor);
        const vote_raw = try readInt(u64, data, cursor);
        const log_index = try readInt(u64, data, cursor);
        const log_term = try readInt(u64, data, cursor);
        const commit_index = try readInt(u64, data, cursor);
        const reject = try readBool(data, cursor);
        const reject_hint = try readInt(u64, data, cursor);
        const context = try readBytes(alloc, data, cursor);
        errdefer if (context.len > 0) alloc.free(context);

        const entry_count: usize = @intCast(try readInt(u32, data, cursor));
        const entries = try alloc.alloc(core.Entry, entry_count);
        var initialized_entries: usize = 0;
        errdefer {
            for (entries[0..initialized_entries]) |*entry| entry.deinit(alloc);
            alloc.free(entries);
        }
        for (entries) |*entry| {
            entry.* = try decodeEntry(alloc, data, cursor);
            initialized_entries += 1;
        }

        const has_snapshot = try readBool(data, cursor);
        const snapshot = if (has_snapshot) try decodeSnapshot(alloc, data, cursor) else null;
        errdefer if (snapshot) |*snap| snap.deinit(alloc);

        return .{
            .msg_type = msg_type,
            .from = from,
            .to = to,
            .term = term,
            .vote = if (has_vote) vote_raw else null,
            .log_index = log_index,
            .log_term = log_term,
            .commit_index = commit_index,
            .reject = reject,
            .reject_hint = reject_hint,
            .entries = entries,
            .snapshot = snapshot,
            .context = context,
        };
    }

    fn encodeEntry(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), entry: core.Entry) !void {
        try appendInt(u64, alloc, out, entry.term);
        try appendInt(u64, alloc, out, entry.index);
        try out.append(alloc, @intFromEnum(entry.entry_type));
        try appendBytes(alloc, out, entry.data);
    }

    fn encodeEntryFixed(out: []u8, cursor: *usize, entry: core.Entry) void {
        appendFixedInt(u64, out, cursor, entry.term);
        appendFixedInt(u64, out, cursor, entry.index);
        appendFixedByte(out, cursor, @intFromEnum(entry.entry_type));
        appendFixedBytesLen(out, cursor, entry.data);
    }

    fn decodeEntry(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) !core.Entry {
        const term = try readInt(u64, data, cursor);
        const index = try readInt(u64, data, cursor);
        if (cursor.* >= data.len) return error.InvalidCodecFrame;
        const entry_type = decodeEntryType(data[cursor.*]) orelse return error.InvalidCodecFrame;
        cursor.* += 1;
        const payload = try readBytes(alloc, data, cursor);
        return .{
            .term = term,
            .index = index,
            .entry_type = entry_type,
            .data = payload,
        };
    }

    fn encodeSnapshot(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), snapshot: core.types.Snapshot) !void {
        try appendInt(u64, alloc, out, snapshot.metadata.index);
        try appendInt(u64, alloc, out, snapshot.metadata.term);
        try encodeNodeList(alloc, out, snapshot.metadata.conf_state.voters);
        try encodeNodeList(alloc, out, snapshot.metadata.conf_state.voters_outgoing);
        try encodeNodeList(alloc, out, snapshot.metadata.conf_state.learners);
        try encodeNodeList(alloc, out, snapshot.metadata.conf_state.learners_next);
        try out.append(alloc, @intFromBool(snapshot.metadata.conf_state.auto_leave));
        try appendBytes(alloc, out, snapshot.data);
    }

    fn encodeSnapshotFixed(out: []u8, cursor: *usize, snapshot: core.types.Snapshot) void {
        appendFixedInt(u64, out, cursor, snapshot.metadata.index);
        appendFixedInt(u64, out, cursor, snapshot.metadata.term);
        encodeNodeListFixed(out, cursor, snapshot.metadata.conf_state.voters);
        encodeNodeListFixed(out, cursor, snapshot.metadata.conf_state.voters_outgoing);
        encodeNodeListFixed(out, cursor, snapshot.metadata.conf_state.learners);
        encodeNodeListFixed(out, cursor, snapshot.metadata.conf_state.learners_next);
        appendFixedByte(out, cursor, @intFromBool(snapshot.metadata.conf_state.auto_leave));
        appendFixedBytesLen(out, cursor, snapshot.data);
    }

    fn decodeSnapshot(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) !core.types.Snapshot {
        const index = try readInt(u64, data, cursor);
        const term = try readInt(u64, data, cursor);
        var conf_state: core.types.ConfState = .{};
        errdefer conf_state.deinit(alloc);
        conf_state.voters = try decodeNodeList(alloc, data, cursor);
        conf_state.voters_outgoing = try decodeNodeList(alloc, data, cursor);
        conf_state.learners = try decodeNodeList(alloc, data, cursor);
        conf_state.learners_next = try decodeNodeList(alloc, data, cursor);
        conf_state.auto_leave = try readBool(data, cursor);
        return .{
            .metadata = .{
                .index = index,
                .term = term,
                .conf_state = conf_state,
            },
            .data = try readBytes(alloc, data, cursor),
        };
    }

    fn encodeNodeList(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), nodes: []const core.types.NodeId) !void {
        try appendInt(u32, alloc, out, @intCast(nodes.len));
        for (nodes) |node| try appendInt(u64, alloc, out, node);
    }

    fn encodeNodeListFixed(out: []u8, cursor: *usize, nodes: []const core.types.NodeId) void {
        appendFixedInt(u32, out, cursor, @intCast(nodes.len));
        for (nodes) |node| appendFixedInt(u64, out, cursor, node);
    }

    fn decodeMessageType(tag: u8) ?core.message.MessageType {
        return switch (tag) {
            @intFromEnum(core.message.MessageType.propose) => .propose,
            @intFromEnum(core.message.MessageType.pre_vote) => .pre_vote,
            @intFromEnum(core.message.MessageType.pre_vote_response) => .pre_vote_response,
            @intFromEnum(core.message.MessageType.request_vote) => .request_vote,
            @intFromEnum(core.message.MessageType.request_vote_response) => .request_vote_response,
            @intFromEnum(core.message.MessageType.append_entries) => .append_entries,
            @intFromEnum(core.message.MessageType.append_entries_response) => .append_entries_response,
            @intFromEnum(core.message.MessageType.heartbeat) => .heartbeat,
            @intFromEnum(core.message.MessageType.heartbeat_response) => .heartbeat_response,
            @intFromEnum(core.message.MessageType.snapshot) => .snapshot,
            @intFromEnum(core.message.MessageType.snapshot_response) => .snapshot_response,
            @intFromEnum(core.message.MessageType.transfer_leader) => .transfer_leader,
            @intFromEnum(core.message.MessageType.forget_leader) => .forget_leader,
            @intFromEnum(core.message.MessageType.timeout_now) => .timeout_now,
            @intFromEnum(core.message.MessageType.read_index) => .read_index,
            @intFromEnum(core.message.MessageType.read_index_response) => .read_index_response,
            @intFromEnum(core.message.MessageType.storage_append) => .storage_append,
            @intFromEnum(core.message.MessageType.storage_append_response) => .storage_append_response,
            @intFromEnum(core.message.MessageType.storage_apply) => .storage_apply,
            @intFromEnum(core.message.MessageType.storage_apply_response) => .storage_apply_response,
            else => null,
        };
    }

    fn decodeEntryType(tag: u8) ?core.types.EntryType {
        return switch (tag) {
            @intFromEnum(core.types.EntryType.normal) => .normal,
            @intFromEnum(core.types.EntryType.conf_change) => .conf_change,
            @intFromEnum(core.types.EntryType.conf_change_v2) => .conf_change_v2,
            else => null,
        };
    }

    fn decodeNodeList(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) ![]core.types.NodeId {
        const len: usize = @intCast(try readInt(u32, data, cursor));
        const out = try alloc.alloc(core.types.NodeId, len);
        errdefer alloc.free(out);
        for (out) |*node| node.* = try readInt(u64, data, cursor);
        return out;
    }

    fn appendBytes(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
        try appendInt(u32, alloc, out, @intCast(bytes.len));
        try out.appendSlice(alloc, bytes);
    }

    fn readBytes(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) ![]u8 {
        const len: usize = @intCast(try readInt(u32, data, cursor));
        if (cursor.* + len > data.len) return error.InvalidCodecFrame;
        const out = try alloc.dupe(u8, data[cursor.* .. cursor.* + len]);
        cursor.* += len;
        return out;
    }

    fn readBool(data: []const u8, cursor: *usize) !bool {
        if (cursor.* >= data.len) return error.InvalidCodecFrame;
        const value = data[cursor.*] != 0;
        cursor.* += 1;
        return value;
    }

    fn appendInt(comptime T: type, alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: T) !void {
        const start = out.items.len;
        try out.resize(alloc, start + @sizeOf(T));
        std.mem.writeInt(T, out.items[start .. start + @sizeOf(T)][0..@sizeOf(T)], value, .little);
    }

    fn appendFixedByte(out: []u8, cursor: *usize, value: u8) void {
        out[cursor.*] = value;
        cursor.* += 1;
    }

    fn appendFixedBytes(out: []u8, cursor: *usize, bytes: []const u8) void {
        @memcpy(out[cursor.* .. cursor.* + bytes.len], bytes);
        cursor.* += bytes.len;
    }

    fn appendFixedBytesLen(out: []u8, cursor: *usize, bytes: []const u8) void {
        appendFixedInt(u32, out, cursor, @intCast(bytes.len));
        appendFixedBytes(out, cursor, bytes);
    }

    fn appendFixedInt(comptime T: type, out: []u8, cursor: *usize, value: T) void {
        std.mem.writeInt(T, out[cursor.* .. cursor.* + @sizeOf(T)][0..@sizeOf(T)], value, .little);
        cursor.* += @sizeOf(T);
    }

    fn readInt(comptime T: type, data: []const u8, cursor: *usize) !T {
        if (cursor.* + @sizeOf(T) > data.len) return error.InvalidCodecFrame;
        const value = std.mem.readInt(T, data[cursor.* .. cursor.* + @sizeOf(T)][0..@sizeOf(T)], .little);
        cursor.* += @sizeOf(T);
        return value;
    }

    fn freeFrame(_: *anyopaque, alloc: std.mem.Allocator, frame: codec_iface.EncodedFrame) void {
        alloc.free(frame.bytes);
    }

    fn freeDecoded(_: *anyopaque, alloc: std.mem.Allocator, decoded: codec_iface.DecodedFrame) void {
        switch (decoded) {
            .raft_peer_batch => |batch| freePeerBatch(alloc, batch),
            .snapshot_manifest => |manifest| alloc.free(manifest.snapshot_id),
        }
    }

    fn freePeerBatch(alloc: std.mem.Allocator, batch: transport_iface.PeerBatch) void {
        for (batch.groups) |group| {
            for (group.messages) |msg| {
                var owned = msg;
                owned.deinit(alloc);
            }
            alloc.free(group.messages);
        }
        alloc.free(batch.groups);
    }

    fn freeOwnedGroups(alloc: std.mem.Allocator, groups: []transport_iface.GroupMessageBatch) void {
        for (groups) |group| {
            for (group.messages) |msg| {
                var owned = msg;
                owned.deinit(alloc);
            }
            alloc.free(group.messages);
        }
        alloc.free(groups);
    }
};

test "binary codec round-trips peer batch with entries and snapshot" {
    const codec = BinaryCodec.codec();
    var conf_voters = [_]core.types.NodeId{ 1, 2 };
    var conf_learners = [_]core.types.NodeId{3};
    const ctx: []u8 = @constCast("ctx");
    const entry_data: []u8 = @constCast("abc");
    const snapshot_data: []u8 = @constCast("snap");
    var entries = [_]core.Entry{
        .{ .term = 2, .index = 11, .entry_type = .normal, .data = entry_data },
    };
    const batch: transport_iface.PeerBatch = .{
        .peer_id = 9,
        .groups = &.{
            .{
                .group_id = 7,
                .messages = &.{
                    .{
                        .msg_type = .append_entries,
                        .from = 1,
                        .to = 9,
                        .term = 2,
                        .context = ctx,
                        .entries = entries[0..],
                    },
                    .{
                        .msg_type = .snapshot,
                        .from = 1,
                        .to = 9,
                        .term = 2,
                        .snapshot = .{
                            .metadata = .{
                                .index = 10,
                                .term = 2,
                                .conf_state = .{
                                    .voters = conf_voters[0..],
                                    .learners = conf_learners[0..],
                                },
                            },
                            .data = snapshot_data,
                        },
                    },
                },
            },
        },
    };

    const frame = try codec.encodePeerBatch(std.testing.allocator, batch);
    defer codec.freeFrame(std.testing.allocator, frame);
    const decoded = try codec.decodeFrame(std.testing.allocator, frame);
    defer codec.freeDecoded(std.testing.allocator, decoded);

    switch (decoded) {
        .raft_peer_batch => |peer_batch| {
            try std.testing.expectEqual(@as(core.types.NodeId, 9), peer_batch.peer_id);
            try std.testing.expectEqual(@as(usize, 1), peer_batch.groups.len);
            try std.testing.expectEqual(@as(usize, 2), peer_batch.groups[0].messages.len);
            try std.testing.expectEqualStrings("ctx", peer_batch.groups[0].messages[0].context);
            try std.testing.expectEqual(@as(usize, 1), peer_batch.groups[0].messages[0].entries.len);
            try std.testing.expectEqual(@as(core.types.Index, 10), peer_batch.groups[0].messages[1].snapshot.?.metadata.index);
            try std.testing.expectEqualStrings("snap", peer_batch.groups[0].messages[1].snapshot.?.data);
        },
        else => return error.UnexpectedDecodedFrame,
    }
}
