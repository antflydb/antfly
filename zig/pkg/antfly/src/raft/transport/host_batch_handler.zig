// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

    pub fn snapshotHandler(self: *HostBatchHandler) http_server.SnapshotUploadHandler {
        return .{
            .ptr = self,
            .vtable = &.{
                .admit_snapshot_upload = admitSnapshotUpload,
                .cancel_snapshot_upload = cancelSnapshotUpload,
                .handle_snapshot_upload = handleSnapshotUpload,
            },
        };
    }

    fn handlePeerBatch(ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
        const self: *HostBatchHandler = @ptrCast(@alignCast(ptr));
        try self.host.enqueueInboundBatch(batch);
    }

    fn handleSnapshotUpload(ptr: *anyopaque, upload: http_server.SnapshotUpload) !void {
        const self: *HostBatchHandler = @ptrCast(@alignCast(ptr));
        try self.host.handleSnapshotUpload(upload);
    }

    fn admitSnapshotUpload(ptr: *anyopaque, admission: http_server.SnapshotUploadAdmission) !void {
        const self: *HostBatchHandler = @ptrCast(@alignCast(ptr));
        try self.host.admitSnapshotUpload(admission);
    }

    fn cancelSnapshotUpload(ptr: *anyopaque, admission: http_server.SnapshotUploadAdmission) void {
        const self: *HostBatchHandler = @ptrCast(@alignCast(ptr));
        self.host.cancelSnapshotUpload(admission);
    }
};

test "host batch handler module compiles" {
    _ = HostBatchHandler;
}
