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

//! Complete local-metadata hot-standby capability. Distributed metadata is replicated
//! by its own Raft group and deliberately does not expose this port.
const std = @import("std");
const db = @import("../storage/db/ha_contract.zig");
const record = @import("../storage/hot_standby/replication_record.zig");
const staging = @import("../metadata/restore_provisioning_contract.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");

pub const Checkpoint = struct {
    size_bytes: u64,
    sha256: [32]u8,
};

pub const Port = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub const VTable = struct {
        /// Prepared binding only: promotion already owns the transition mutex.
        bind_mirror: *const fn (*anyopaque, ?db.WriteGate, ?db.AsyncEffectMirror) anyerror!void,
        /// Flush durable metadata effects before entering the exclusive seed
        /// boundary. Never call this while holding the hot-standby transition mutex.
        prepare_checkpoint: *const fn (*anyopaque) anyerror!void,
        apply_record: *const fn (*anyopaque, record.RecordView) anyerror!void,
        capture_checkpoint: *const fn (*anyopaque, std.Io, []const u8) anyerror!Checkpoint,
        capture_private: *const fn (*anyopaque, std.mem.Allocator, u64) anyerror!?std.json.Parsed(staging.ProvisioningProjection),
    };

    pub fn bindMirror(self: Port, gate: ?db.WriteGate, mirror: ?db.AsyncEffectMirror) !void {
        try BoundaryAbi.call("bind_mirror", self.boundary_dispatch, self.vtable.bind_mirror, .{ self.ptr, gate, mirror });
    }

    pub fn applyRecord(self: Port, value: record.RecordView) !void {
        try BoundaryAbi.call("apply_record", self.boundary_dispatch, self.vtable.apply_record, .{ self.ptr, value });
    }

    pub fn prepareCheckpoint(self: Port) !void {
        try BoundaryAbi.call("prepare_checkpoint", self.boundary_dispatch, self.vtable.prepare_checkpoint, .{self.ptr});
    }

    pub fn captureCheckpoint(self: Port, io: std.Io, path: []const u8) !Checkpoint {
        return BoundaryAbi.call("capture_checkpoint", self.boundary_dispatch, self.vtable.capture_checkpoint, .{ self.ptr, io, path });
    }

    pub fn capturePrivate(self: Port, alloc: std.mem.Allocator, expected_epoch: u64) !?std.json.Parsed(staging.ProvisioningProjection) {
        return BoundaryAbi.call("capture_private", self.boundary_dispatch, self.vtable.capture_private, .{ self.ptr, alloc, expected_epoch });
    }
};
