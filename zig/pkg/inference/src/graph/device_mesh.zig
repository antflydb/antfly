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

// Device mesh: an ordered set of compute devices for multi-device execution.
//
// A DeviceMesh is a flat array of (DeviceId, ComputeBackend) pairs. Each
// entry represents one physical or logical device — a CPU core pool, a GPU
// context, or a remote worker. The mesh is topology-agnostic: callers
// decide how to map partitions to devices.
//
// For Apple Silicon, a typical 2-device mesh is [metal:0, native:cpu].
// For multi-GPU, it might be [metal:0, metal:1].

const std = @import("std");
const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const BackendKind = contracts.BackendKind;
const ComputeBackend = ops_mod.ComputeBackend;

pub const DeviceId = u16;

pub const DeviceEntry = struct {
    id: DeviceId,
    backend: *const ComputeBackend,
    kind: BackendKind,
};

pub const DeviceMesh = struct {
    devices: []const DeviceEntry,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, devices: []const DeviceEntry) !DeviceMesh {
        const owned = try allocator.alloc(DeviceEntry, devices.len);
        @memcpy(owned, devices);
        return .{ .devices = owned, .allocator = allocator };
    }

    pub fn deinit(self: *DeviceMesh) void {
        self.allocator.free(self.devices);
    }

    /// Look up a device by ID. Returns null if not found.
    pub fn device(self: *const DeviceMesh, id: DeviceId) ?*const DeviceEntry {
        for (self.devices) |*entry| {
            if (entry.id == id) return entry;
        }
        return null;
    }

    pub fn deviceCount(self: *const DeviceMesh) usize {
        return self.devices.len;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "DeviceMesh basic lookup" {
    const allocator = std.testing.allocator;

    // Use null backends for the mesh test (we only test the mesh structure).
    const fake_cb_a = @as(*const ComputeBackend, @ptrFromInt(0x1000));
    const fake_cb_b = @as(*const ComputeBackend, @ptrFromInt(0x2000));

    var mesh = try DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = fake_cb_a, .kind = .metal },
        .{ .id = 1, .backend = fake_cb_b, .kind = .native },
    });
    defer mesh.deinit();

    try std.testing.expectEqual(@as(usize, 2), mesh.deviceCount());
    try std.testing.expect(mesh.device(0) != null);
    try std.testing.expectEqual(BackendKind.metal, mesh.device(0).?.kind);
    try std.testing.expectEqual(BackendKind.native, mesh.device(1).?.kind);
    try std.testing.expect(mesh.device(99) == null);
}
