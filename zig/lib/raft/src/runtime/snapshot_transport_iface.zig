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

pub const SnapshotLocator = struct {
    snapshot_id: []const u8,
    uri: []const u8 = &.{},
};

pub const SnapshotSendRequest = struct {
    group_id: core.types.GroupId,
    to: core.types.NodeId,
    term: core.types.Term = 0,
    snapshot: core.types.Snapshot,
    locator: ?SnapshotLocator = null,
};

pub const SnapshotFetchRequest = struct {
    group_id: core.types.GroupId,
    from: core.types.NodeId,
    term: core.types.Term = 0,
    locator: SnapshotLocator,
};

pub const SnapshotReceiver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        receive_snapshot: *const fn (
            ptr: *anyopaque,
            req: SnapshotFetchRequest,
            snapshot: core.types.Snapshot,
        ) anyerror!void,
        receive_locator: ?*const fn (ptr: *anyopaque, req: SnapshotFetchRequest) anyerror!void = null,
    };

    pub fn receiveSnapshot(self: SnapshotReceiver, req: SnapshotFetchRequest, snapshot: core.types.Snapshot) !void {
        return try self.vtable.receive_snapshot(self.ptr, req, snapshot);
    }

    pub fn receiveLocator(self: SnapshotReceiver, req: SnapshotFetchRequest) !void {
        if (self.vtable.receive_locator) |receive_locator| {
            return try receive_locator(self.ptr, req);
        }
    }
};

pub const SnapshotTransport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send_snapshot: *const fn (ptr: *anyopaque, req: SnapshotSendRequest) anyerror!void,
        fetch_snapshot: ?*const fn (ptr: *anyopaque, req: SnapshotFetchRequest, receiver: SnapshotReceiver) anyerror!void = null,
        cancel_snapshot: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId, snapshot_id: []const u8) anyerror!void = null,
    };

    pub fn sendSnapshot(self: SnapshotTransport, req: SnapshotSendRequest) !void {
        return try self.vtable.send_snapshot(self.ptr, req);
    }

    pub fn fetchSnapshot(self: SnapshotTransport, req: SnapshotFetchRequest, receiver: SnapshotReceiver) !void {
        if (self.vtable.fetch_snapshot) |fetch_snapshot| {
            return try fetch_snapshot(self.ptr, req, receiver);
        }
    }

    pub fn cancelSnapshot(self: SnapshotTransport, group_id: core.types.GroupId, snapshot_id: []const u8) !void {
        if (self.vtable.cancel_snapshot) |cancel_snapshot| {
            return try cancel_snapshot(self.ptr, group_id, snapshot_id);
        }
    }
};

test "snapshot transport iface compiles" {
    _ = SnapshotLocator;
    _ = SnapshotSendRequest;
    _ = SnapshotFetchRequest;
    _ = SnapshotReceiver;
    _ = SnapshotTransport;
}
