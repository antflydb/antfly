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

// Collective communication operations for multi-device execution.
//
// Implements all-reduce (sum) and all-gather as CPU-mediated operations.
// Each device's tensor is downloaded to f32, the collective is performed
// on the CPU, and the result is uploaded back to each device.
//
// On Apple Silicon with unified memory, toFloat32/fromFloat32 is
// essentially a memcpy, so this is efficient. For PJRT/TPU multi-chip,
// intra-partition collectives are compiled directly into HLO programs
// (cross-replica-sum via ICI ~300 GB/s). Inter-partition collectives
// use the CPU-mediated path below, which calls each device's
// ComputeBackend.toFloat32/fromFloat32 (DMA for non-unified memory).

const std = @import("std");
const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const device_mesh_mod = @import("device_mesh.zig");
const DeviceId = device_mesh_mod.DeviceId;
const DeviceMesh = device_mesh_mod.DeviceMesh;

/// All-reduce (sum): download tensors from all devices, sum element-wise,
/// upload the result back to each device. Frees the original tensors.
///
/// Returns one CT per device in `device_group`, each containing the
/// summed result on that device.
pub fn allReduceSum(
    allocator: std.mem.Allocator,
    mesh: *const DeviceMesh,
    values: []CT,
    device_group: []const DeviceId,
) !void {
    std.debug.assert(values.len == device_group.len);
    if (values.len <= 1) return;

    // Download all to f32.
    var f32_bufs = try allocator.alloc([]f32, device_group.len);
    defer {
        for (f32_bufs) |buf| allocator.free(buf);
        allocator.free(f32_bufs);
    }

    for (device_group, 0..) |dev_id, i| {
        const entry = mesh.device(dev_id).?;
        f32_bufs[i] = try entry.backend.toFloat32(values[i], allocator);
    }

    // Sum into first buffer.
    const len = f32_bufs[0].len;
    for (1..device_group.len) |d| {
        std.debug.assert(f32_bufs[d].len == len);
        for (0..len) |i| {
            f32_bufs[0][i] += f32_bufs[d][i];
        }
    }

    // Free originals and upload summed result to each device.
    for (device_group, 0..) |dev_id, i| {
        const entry = mesh.device(dev_id).?;
        entry.backend.free(values[i]);
        values[i] = try entry.backend.fromFloat32(f32_bufs[0]);
    }
}

/// All-gather: download tensors from all devices and concatenate them.
/// Each device gets the full concatenated result.
///
/// `values` contains one CT per device (each a partial shard).
/// After the call, each entry in `values` contains the full
/// concatenated tensor on that device. Original tensors are freed.
pub fn allGather(
    allocator: std.mem.Allocator,
    mesh: *const DeviceMesh,
    values: []CT,
    device_group: []const DeviceId,
) !void {
    std.debug.assert(values.len == device_group.len);
    if (values.len <= 1) return;

    // Download all to f32.
    var f32_bufs = try allocator.alloc([]f32, device_group.len);
    defer {
        for (f32_bufs) |buf| allocator.free(buf);
        allocator.free(f32_bufs);
    }

    for (device_group, 0..) |dev_id, i| {
        const entry = mesh.device(dev_id).?;
        f32_bufs[i] = try entry.backend.toFloat32(values[i], allocator);
    }

    // Concatenate all buffers.
    var total_len: usize = 0;
    for (f32_bufs) |buf| total_len += buf.len;

    const gathered = try allocator.alloc(f32, total_len);
    defer allocator.free(gathered);

    var offset: usize = 0;
    for (f32_bufs) |buf| {
        @memcpy(gathered[offset..][0..buf.len], buf);
        offset += buf.len;
    }

    // Free originals and upload gathered result to each device.
    for (device_group, 0..) |dev_id, i| {
        const entry = mesh.device(dev_id).?;
        entry.backend.free(values[i]);
        values[i] = try entry.backend.fromFloat32(gathered);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

// Collective ops tests require real backends since they call
// toFloat32/fromFloat32/free. We test the internal logic with
// the native compute backend.

const native_mod = @import("../ops/native_compute.zig");
const NativeCompute = native_mod.NativeCompute;
const WeightStore = native_mod.WeightStore;

test "allReduceSum sums across two devices" {
    const allocator = std.testing.allocator;

    var ws_a = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var ws_b = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute_a = NativeCompute.init(allocator, &ws_a, null);
    var compute_b = NativeCompute.init(allocator, &ws_b, null);
    const cb_a = compute_a.computeBackend();
    const cb_b = compute_b.computeBackend();

    var mesh = try DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &cb_a, .kind = .native },
        .{ .id = 1, .backend = &cb_b, .kind = .native },
    });
    defer mesh.deinit();

    // Create tensors: device 0 has [1, 2], device 1 has [3, 4]
    const ct_a = try cb_a.fromFloat32(&.{ 1.0, 2.0 });
    const ct_b = try cb_b.fromFloat32(&.{ 3.0, 4.0 });

    var values = [_]CT{ ct_a, ct_b };
    const group = [_]DeviceId{ 0, 1 };
    try allReduceSum(allocator, &mesh, &values, &group);

    // Both should now have [4, 6]
    const result_a = try cb_a.toFloat32(values[0], allocator);
    defer allocator.free(result_a);
    const result_b = try cb_b.toFloat32(values[1], allocator);
    defer allocator.free(result_b);

    try std.testing.expectEqualSlices(f32, &.{ 4.0, 6.0 }, result_a);
    try std.testing.expectEqualSlices(f32, &.{ 4.0, 6.0 }, result_b);

    // Clean up
    cb_a.free(values[0]);
    cb_b.free(values[1]);
}

test "allGather concatenates across two devices" {
    const allocator = std.testing.allocator;

    var ws_a = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var ws_b = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute_a = NativeCompute.init(allocator, &ws_a, null);
    var compute_b = NativeCompute.init(allocator, &ws_b, null);
    const cb_a = compute_a.computeBackend();
    const cb_b = compute_b.computeBackend();

    var mesh = try DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &cb_a, .kind = .native },
        .{ .id = 1, .backend = &cb_b, .kind = .native },
    });
    defer mesh.deinit();

    // Create tensors: device 0 has [1, 2], device 1 has [3, 4]
    const ct_a = try cb_a.fromFloat32(&.{ 1.0, 2.0 });
    const ct_b = try cb_b.fromFloat32(&.{ 3.0, 4.0 });

    var values = [_]CT{ ct_a, ct_b };
    const group = [_]DeviceId{ 0, 1 };
    try allGather(allocator, &mesh, &values, &group);

    // Both should now have [1, 2, 3, 4]
    const result_a = try cb_a.toFloat32(values[0], allocator);
    defer allocator.free(result_a);
    const result_b = try cb_b.toFloat32(values[1], allocator);
    defer allocator.free(result_b);

    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0, 4.0 }, result_a);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0, 4.0 }, result_b);

    // Clean up
    cb_a.free(values[0]);
    cb_b.free(values[1]);
}
