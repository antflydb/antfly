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
const transport_iface = @import("transport_iface.zig");

pub const EncodedFrame = struct {
    bytes: []u8,
    media_type: []const u8,
};

pub const MessageCodec = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        encode_peer_batch: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            batch: transport_iface.PeerBatch,
        ) anyerror!EncodedFrame,
        decode_frame: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            frame: EncodedFrame,
        ) anyerror!DecodedFrame,
        free_frame: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, frame: EncodedFrame) void,
        free_decoded: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, decoded: DecodedFrame) void,
    };

    pub fn encodePeerBatch(self: MessageCodec, alloc: std.mem.Allocator, batch: transport_iface.PeerBatch) !EncodedFrame {
        return try self.vtable.encode_peer_batch(self.ptr, alloc, batch);
    }

    pub fn decodeFrame(self: MessageCodec, alloc: std.mem.Allocator, frame: EncodedFrame) !DecodedFrame {
        return try self.vtable.decode_frame(self.ptr, alloc, frame);
    }

    pub fn freeFrame(self: MessageCodec, alloc: std.mem.Allocator, frame: EncodedFrame) void {
        self.vtable.free_frame(self.ptr, alloc, frame);
    }

    pub fn freeDecoded(self: MessageCodec, alloc: std.mem.Allocator, decoded: DecodedFrame) void {
        self.vtable.free_decoded(self.ptr, alloc, decoded);
    }
};

pub const DecodedFrameTag = enum {
    raft_peer_batch,
    snapshot_manifest,
};

pub const SnapshotManifest = struct {
    group_id: core.types.GroupId,
    node_id: core.types.NodeId,
    snapshot_id: []u8,
};

pub const DecodedFrame = union(DecodedFrameTag) {
    raft_peer_batch: transport_iface.PeerBatch,
    snapshot_manifest: SnapshotManifest,
};

test "codec iface compiles" {
    _ = EncodedFrame;
    _ = MessageCodec;
    _ = DecodedFrameTag;
    _ = SnapshotManifest;
    _ = DecodedFrame;
}
