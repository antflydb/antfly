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

const raft_engine = @import("raft_engine");
const raft_host = @import("../host.zig");
const http_server = @import("http_server.zig");

pub const HostBatchHandler = struct {
    host: *raft_host.Host,

    pub fn handler(self: *HostBatchHandler) http_server.BatchHandler {
        return .{
            .ptr = self,
            .vtable = &.{
                .handle_peer_batch = handlePeerBatch,
            },
        };
    }

    fn handlePeerBatch(ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
        const self: *HostBatchHandler = @ptrCast(@alignCast(ptr));
        try self.host.enqueueInboundBatch(batch);
    }
};

test "host batch handler module compiles" {
    _ = HostBatchHandler;
}
