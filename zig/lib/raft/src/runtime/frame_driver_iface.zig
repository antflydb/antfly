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
const codec_iface = @import("codec_iface.zig");
const transport_iface = @import("transport_iface.zig");

pub const SendFrameRequest = struct {
    source_id: ?u64 = null,
    peer_id: u64,
    endpoint: transport_iface.PeerEndpoint,
    frame: codec_iface.EncodedFrame,
};

pub const FrameDriver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send_frame: *const fn (ptr: *anyopaque, req: SendFrameRequest) anyerror!void,
    };

    pub fn sendFrame(self: FrameDriver, req: SendFrameRequest) !void {
        return try self.vtable.send_frame(self.ptr, req);
    }
};

test "frame driver iface compiles" {
    _ = SendFrameRequest;
    _ = FrameDriver;
}
