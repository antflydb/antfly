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

//! PJRT device topology → DeviceMesh mapping.
//!
//! Queries the PJRT client for addressable devices and creates a
//! DeviceMesh with one entry per PJRT device plus a native fallback.
//! For a v4-8 TPU (4 chips): [pjrt:0, pjrt:1, pjrt:2, pjrt:3, native:4].

const std = @import("std");
const pjrt_lib = @import("pjrt");
const c = pjrt_lib.pjrt_c_types;

const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const BackendKind = contracts.BackendKind;
const ComputeBackend = ops_mod.ComputeBackend;

const device_mesh_mod = @import("device_mesh.zig");
const DeviceMesh = device_mesh_mod.DeviceMesh;
const DeviceEntry = device_mesh_mod.DeviceEntry;
const DeviceId = device_mesh_mod.DeviceId;

/// Create a DeviceMesh from a PJRT client's addressable devices.
///
/// Each PJRT device gets a DeviceEntry with kind=.pjrt, using the
/// PJRT ComputeBackend. A native fallback entry is appended at the
/// end for ops that can't run on PJRT (e.g. paged KV attention).
///
/// `pjrt_backend` must have a lifetime >= the returned mesh.
/// `native_backend` is used for the CPU fallback device.
pub fn createPjrtMesh(
    allocator: std.mem.Allocator,
    client: *const pjrt_lib.pjrt.Client,
    pjrt_backend: *const ComputeBackend,
    native_backend: *const ComputeBackend,
) !DeviceMesh {
    const pjrt_devices = try client.addressableDevices();
    const num_pjrt = pjrt_devices.len;

    // One entry per PJRT device + one native fallback.
    var entries = try allocator.alloc(DeviceEntry, num_pjrt + 1);
    defer allocator.free(entries);

    for (0..num_pjrt) |i| {
        entries[i] = .{
            .id = @intCast(i),
            .backend = pjrt_backend,
            .kind = .pjrt,
        };
    }

    // native fallback gets the next sequential ID.
    entries[num_pjrt] = .{
        .id = @intCast(num_pjrt),
        .backend = native_backend,
        .kind = .native,
    };

    return DeviceMesh.init(allocator, entries);
}

/// Return the number of PJRT devices (excluding the native fallback)
/// in a mesh created by createPjrtMesh.
pub fn pjrtDeviceCount(mesh: *const DeviceMesh) usize {
    var count: usize = 0;
    for (mesh.devices) |entry| {
        if (entry.kind == .pjrt) count += 1;
    }
    return count;
}

/// Return device IDs for all PJRT devices in the mesh.
pub fn pjrtDeviceIds(
    allocator: std.mem.Allocator,
    mesh: *const DeviceMesh,
) ![]DeviceId {
    var ids = std.ArrayListUnmanaged(DeviceId).empty;
    errdefer ids.deinit(allocator);
    for (mesh.devices) |entry| {
        if (entry.kind == .pjrt) try ids.append(allocator, entry.id);
    }
    return ids.toOwnedSlice(allocator);
}
