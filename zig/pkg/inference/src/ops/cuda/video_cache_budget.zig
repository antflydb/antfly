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

//! Admission owned by an NVDEC decoder, independent of request lifetimes.
const std = @import("std");
const memory = @import("../../runtime/tier/memory.zig");
const Physical = @import("allocation_budget.zig").Budget;

pub const Admission = struct {
    controller: *memory.AdmissionController,
    limits: memory.Limits,
    check_live_memory: bool = true,
};

/// NVDEC does not expose allocator callbacks or an exact allocation-size query.
/// Reserve double the aligned NV12 surface payload plus 64 MiB for opaque driver
/// state. This is a conservative policy envelope, not measured driver usage.
/// Includes the single mapped output and one additional working surface.
pub fn envelope(width: u32, height: u32, surfaces: u32) !usize {
    if (width == 0 or height == 0 or width > 4096 or height > 4096 or surfaces == 0 or surfaces > 32) return error.InvalidVideoGeometry;
    const pitch = std.mem.alignForward(usize, width, 256);
    const rows = std.mem.alignForward(usize, height, 32);
    return 64 * 1024 * 1024 + pitch * rows * 3 * (surfaces + 2);
}

pub const CacheBudget = struct {
    admission: ?Admission = null,
    physical: ?*Physical = null,
    lease: ?memory.AdmissionLease = null,
    bytes: usize = 0,
    failure: ?anyerror = null,

    pub fn reserve(self: *CacheBudget, physical: *Physical, bytes: usize) !void {
        if (self.bytes != 0 or bytes == 0) return error.InvalidVideoCacheReservation;
        try physical.reserve(bytes);
        errdefer physical.release(bytes);
        const lease = if (self.admission) |admission|
            try admission.controller.tryAcquire(.gpu, admission.limits, .{ .backend_scratch_bytes = bytes }, admission.check_live_memory)
        else
            null;
        self.physical = physical;
        self.lease = lease;
        self.bytes = bytes;
    }

    /// Called only after the decoder's physical surfaces have been destroyed.
    pub fn release(self: *CacheBudget) void {
        if (self.physical) |physical| physical.release(self.bytes);
        self.physical = null;
        if (self.lease) |*lease| lease.release();
        self.lease = null;
        self.bytes = 0;
    }
};

test "embeddinggemma2 NVDEC retained admission shares limits and rolls back failed reservations" {
    var controller = memory.AdmissionController{};
    var physical = Physical{ .limit = 1024 };
    const admission = Admission{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1024 }, .check_live_memory = false };
    var first = CacheBudget{ .admission = admission };
    defer first.release();
    var second = CacheBudget{ .admission = admission };
    defer second.release();
    try first.reserve(&physical, 700);
    try std.testing.expectEqual(@as(usize, 700), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, second.reserve(&physical, 400));
    try std.testing.expectEqual(@as(usize, 700), physical.snapshot().live);
    // A distinct context still competes for the same shared admission.
    var other_physical = Physical{};
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, second.reserve(&other_physical, 400));
    try std.testing.expectEqual(@as(usize, 0), other_physical.snapshot().live);
    first.release();
    try second.reserve(&other_physical, 400);
    second.release();
    second.release();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), physical.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), other_physical.snapshot().live);
}

test "embeddinggemma2 NVDEC surface envelope bounds supported geometry and rejects invalid metadata" {
    try std.testing.expect(try envelope(4096, 4096, 32) > try envelope(1920, 1080, 4));
    try std.testing.expectEqual(try envelope(160, 95, 4), try envelope(160, 96, 4));
    try std.testing.expectError(error.InvalidVideoGeometry, envelope(4097, 1080, 4));
    try std.testing.expectError(error.InvalidVideoGeometry, envelope(1920, 1080, 33));
    try std.testing.expectError(error.InvalidVideoGeometry, envelope(0, 1080, 4));
}
