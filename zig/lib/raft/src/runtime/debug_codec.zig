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

pub const DebugCodec = struct {
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
        var list = std.ArrayListUnmanaged(u8).empty;
        defer list.deinit(alloc);

        try list.print(alloc, "peer={d};groups={d}\n", .{ batch.peer_id, batch.groups.len });
        for (batch.groups) |group| {
            try list.print(alloc, "group={d};messages={d}\n", .{ group.group_id, group.messages.len });
            for (group.messages) |msg| {
                try list.print(alloc, "msg={s};from={d};to={d};term={d};entries={d}\n", .{
                    @tagName(msg.msg_type),
                    msg.from,
                    msg.to,
                    msg.term,
                    msg.entries.len,
                });
            }
        }
        return .{
            .bytes = try list.toOwnedSlice(alloc),
            .media_type = "text/x-antflydb-raft-debug",
        };
    }

    fn decodeFrame(_: *anyopaque, alloc: std.mem.Allocator, frame: codec_iface.EncodedFrame) !codec_iface.DecodedFrame {
        var peer_id: core.types.NodeId = 0;
        var groups = std.ArrayListUnmanaged(transport_iface.GroupMessageBatch).empty;
        errdefer freeOwnedGroups(alloc, groups.items);
        defer groups.deinit(alloc);

        var lines = std.mem.splitScalar(u8, frame.bytes, '\n');
        var current_group_id: ?core.types.GroupId = null;
        var current_messages = std.ArrayListUnmanaged(core.Message).empty;
        defer freePendingMessages(alloc, &current_messages);

        while (lines.next()) |line| {
            if (line.len == 0) continue;
            if (std.mem.startsWith(u8, line, "peer=")) {
                const end = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
                peer_id = try std.fmt.parseInt(core.types.NodeId, line[5..end], 10);
            } else if (std.mem.startsWith(u8, line, "group=")) {
                if (current_group_id) |group_id| {
                    try groups.append(alloc, .{
                        .group_id = group_id,
                        .messages = try current_messages.toOwnedSlice(alloc),
                    });
                    current_messages = .empty;
                }
                const end = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
                current_group_id = try std.fmt.parseInt(core.types.GroupId, line[6..end], 10);
            } else if (std.mem.startsWith(u8, line, "msg=")) {
                const msg_name_end = std.mem.indexOfScalar(u8, line, ';').?;
                const from_marker = std.mem.indexOf(u8, line, "from=").?;
                const to_marker = std.mem.indexOf(u8, line, "to=").?;
                const term_marker = std.mem.indexOf(u8, line, "term=").?;
                const entries_marker = std.mem.indexOf(u8, line, "entries=").?;

                try current_messages.append(alloc, .{
                    .msg_type = std.meta.stringToEnum(core.message.MessageType, line[4..msg_name_end]).?,
                    .from = try std.fmt.parseInt(core.types.NodeId, line[from_marker + 5 .. to_marker - 1], 10),
                    .to = try std.fmt.parseInt(core.types.NodeId, line[to_marker + 3 .. term_marker - 1], 10),
                    .term = try std.fmt.parseInt(core.types.Term, line[term_marker + 5 .. entries_marker - 1], 10),
                    .entries = try alloc.alloc(core.Entry, try std.fmt.parseInt(usize, line[entries_marker + 8 ..], 10)),
                });
                for (current_messages.items[current_messages.items.len - 1].entries) |*entry| entry.* = .{};
            }
        }

        if (current_group_id) |group_id| {
            try groups.append(alloc, .{
                .group_id = group_id,
                .messages = try current_messages.toOwnedSlice(alloc),
            });
            current_messages = .empty;
        }

        return .{
            .raft_peer_batch = .{
                .peer_id = peer_id,
                .groups = try groups.toOwnedSlice(alloc),
            },
        };
    }

    fn freeFrame(_: *anyopaque, alloc: std.mem.Allocator, frame: codec_iface.EncodedFrame) void {
        alloc.free(frame.bytes);
    }

    fn freeDecoded(_: *anyopaque, alloc: std.mem.Allocator, decoded: codec_iface.DecodedFrame) void {
        switch (decoded) {
            .raft_peer_batch => |batch| {
                for (batch.groups) |group| {
                    for (group.messages) |msg| {
                        core.types.freeEntries(alloc, msg.entries);
                    }
                    alloc.free(group.messages);
                }
                alloc.free(batch.groups);
            },
            .snapshot_manifest => |manifest| alloc.free(manifest.snapshot_id),
        }
    }

    fn freePendingMessages(alloc: std.mem.Allocator, messages: *std.ArrayListUnmanaged(core.Message)) void {
        for (messages.items) |msg| core.types.freeEntries(alloc, msg.entries);
        messages.deinit(alloc);
    }

    fn freeOwnedGroups(alloc: std.mem.Allocator, groups: []const transport_iface.GroupMessageBatch) void {
        for (groups) |group| {
            for (group.messages) |msg| core.types.freeEntries(alloc, msg.entries);
            alloc.free(group.messages);
        }
    }
};

test "debug codec round-trips a peer batch header and messages" {
    const batch = transport_iface.PeerBatch{
        .peer_id = 9,
        .groups = &.{
            .{
                .group_id = 7,
                .messages = &.{
                    .{ .msg_type = .heartbeat, .from = 1, .to = 9, .term = 2 },
                    .{ .msg_type = .append_entries, .from = 1, .to = 9, .term = 2 },
                },
            },
        },
    };

    const codec = DebugCodec.codec();
    const frame = try codec.encodePeerBatch(std.testing.allocator, batch);
    defer codec.freeFrame(std.testing.allocator, frame);
    const decoded = try codec.decodeFrame(std.testing.allocator, frame);
    defer codec.freeDecoded(std.testing.allocator, decoded);

    switch (decoded) {
        .raft_peer_batch => |peer_batch| {
            try std.testing.expectEqual(@as(core.types.NodeId, 9), peer_batch.peer_id);
            try std.testing.expectEqual(@as(usize, 1), peer_batch.groups.len);
            try std.testing.expectEqual(@as(core.types.GroupId, 7), peer_batch.groups[0].group_id);
            try std.testing.expectEqual(@as(usize, 2), peer_batch.groups[0].messages.len);
        },
        else => return error.UnexpectedDecodedFrame,
    }
}
