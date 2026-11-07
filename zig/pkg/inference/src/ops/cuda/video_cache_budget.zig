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
const CacheQuota = @import("cache_quota.zig").Quota;
const Cleanup = @import("../../execution/cleanup_context.zig").Context;

/// Actual validated first parser sequence, copied before decoder allocation.
/// The conservative envelope remains opaque policy, never measured SDK bytes.
pub const Sequence = extern struct {
    width: u32,
    height: u32,
    coded_width: u32,
    coded_height: u32,
    surfaces: u32,
    codec: u32,
    display_left: u32,
    display_top: u32,
    matrix: u32,
    full_range: u32,
    bit_depth: u32 = 8,
    transfer: u32 = 2,
    pub fn bytes(self: Sequence) !usize {
        if (self.width == 0 or self.height == 0 or self.width % 2 != 0 or self.height % 2 != 0 or
            self.width > self.coded_width or self.height > self.coded_height or
            self.display_left > self.coded_width - self.width or self.display_top > self.coded_height - self.height or
            (self.codec != 4 and self.codec != 8 and self.codec != 11) or
            (self.matrix != 1 and self.matrix != 2 and self.matrix != 5 and self.matrix != 6 and !((self.transfer == 16 or self.transfer == 18) and self.matrix == 9)) or self.full_range > 1 or
            (self.bit_depth != 8 and self.bit_depth != 10) or
            (self.transfer != 1 and self.transfer != 2 and self.transfer != 6 and self.transfer != 13 and self.transfer != 16 and self.transfer != 18)) return error.InvalidVideoGeometry;
        return envelopeForSampleBytes(self.coded_width, self.coded_height, self.surfaces, if (self.bit_depth == 10) 2 else 1);
    }
};

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

pub fn envelopeForSampleBytes(width: u32, height: u32, surfaces: u32, sample_bytes: u32) !usize {
    if (sample_bytes != 1 and sample_bytes != 2) return error.InvalidVideoGeometry;
    return std.math.mul(usize, try envelope(width, height, surfaces), sample_bytes);
}

pub const CacheBudget = struct {
    admission: ?Admission = null,
    physical: ?*Physical = null,
    quota: CacheQuota = .none,
    bytes: usize = 0,
    failure: ?anyerror = null,
    /// A running model's scratch can already fill an automatic cap, which would
    /// refuse a large clip's surfaces indefinitely. Physical limits still hold.
    grow_automatic_scratch: bool = false,

    /// True when the retained reservation holds the shared scratch over its cap.
    pub fn exceedsScratchCap(self: *const CacheBudget) bool {
        const admission = self.admission orelse return false;
        if (self.bytes == 0 or admission.limits.scratch_limit_bytes == 0) return false;
        return admission.controller.snapshot().scratchTotalBytes() > admission.limits.scratch_limit_bytes;
    }

    pub fn currentCleanupPrepared(self: *const CacheBudget) bool {
        if (self.admission != null and self.bytes != 0 and !self.quota.hasLease()) return false;
        return self.quota != .raw;
    }
    pub fn releaseAfterSdkDeath(self: *CacheBudget, epoch: *const @import("../../execution/retirement_sink.zig").Epoch) !void {
        return self.releaseWithCleanup(.{ .boundary = epoch });
    }

    pub fn reserve(self: *CacheBudget, physical: *Physical, bytes: usize) !void {
        if (self.bytes != 0 or bytes == 0) return error.InvalidVideoCacheReservation;
        try physical.reserve(bytes);
        errdefer physical.release(bytes);
        const lease = if (self.admission) |admission|
            try admission.controller.tryAcquire(.gpu, if (self.grow_automatic_scratch)
                try memory.withAutomaticScratchFloor(admission.limits, 0, admission.controller.snapshot().scratchTotalBytes(), bytes)
            else
                admission.limits, .{ .backend_scratch_bytes = bytes }, admission.check_live_memory)
        else
            null;
        self.physical = physical;
        self.quota = CacheQuota.fromRawAdmission(lease);
        self.bytes = bytes;
    }

    /// Called only after the decoder's physical surfaces have been destroyed.
    pub fn validateCleanup(self: *const CacheBudget, current: Cleanup) !void {
        try self.quota.validateCleanup(current);
    }
    pub fn releaseWithCleanup(self: *CacheBudget, current: Cleanup) !void {
        try self.validateCleanup(current);
        if (self.physical) |physical| physical.release(self.bytes);
        self.physical = null;
        self.bytes = 0;
        try self.quota.releaseAfterSdkDeath(current);
    }
    /// Actual legacy decoder callback has proved physical death, but cannot
    /// manufacture an outside contract merely from absence of CURRENT B.
    pub fn releaseLegacyRawAfterSdkDeath(self: *CacheBudget) !void {
        try self.quota.validateLegacyRaw();
        if (self.physical) |physical| physical.release(self.bytes);
        self.physical = null;
        self.bytes = 0;
        try self.quota.releaseLegacyRawAfterSdkDeath();
    }
    /// Historical raw outside-execution endpoint. Managed final/error cleanup
    /// supplies CURRENT B through releaseWithCleanup instead.
    pub fn release(self: *CacheBudget) void {
        self.releaseWithCleanup(.{ .outside_execution = true }) catch @panic("invalid raw video cache quota");
    }
};

test "NVDEC surfaces can outgrow an automatic scratch cap and report it" {
    var controller = memory.AdmissionController{};
    var physical = Physical{ .limit = 4096 };
    const limits: memory.Limits = .{ .scratch_limit_bytes = 1024 };
    var run = try controller.tryAcquire(.gpu, limits, .{ .backend_scratch_bytes = 1000 }, false);
    defer run.release();
    var budget = CacheBudget{ .admission = .{ .controller = &controller, .limits = limits, .check_live_memory = false } };
    defer budget.release();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, budget.reserve(&physical, 512));
    budget.grow_automatic_scratch = true;
    try budget.reserve(&physical, 512);
    try std.testing.expect(budget.exceedsScratchCap());
    budget.release();
    try std.testing.expect(!budget.exceedsScratchCap());
}

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

test "CUDA decoder sequence ABI and conservative envelope reject unsupported metadata before SDK" {
    try std.testing.expectEqual(@as(usize, 48), @sizeOf(Sequence));
    try std.testing.expectEqual(@as(usize, 4), @alignOf(Sequence));
    try std.testing.expectEqual(@as(usize, 40), @offsetOf(Sequence, "bit_depth"));
    try std.testing.expectEqual(@as(usize, 44), @offsetOf(Sequence, "transfer"));
    const base: Sequence = .{ .width = 160, .height = 96, .coded_width = 160, .coded_height = 96, .surfaces = 4, .codec = 4, .display_left = 0, .display_top = 0, .matrix = 1, .full_range = 0 };
    try std.testing.expectEqual(try envelope(160, 96, 4), try base.bytes());
    var hdr = base;
    hdr.bit_depth = 10;
    hdr.transfer = 18;
    hdr.matrix = 9;
    try std.testing.expectEqual(try std.math.mul(usize, try envelope(160, 96, 4), 2), try hdr.bytes());
    hdr.bit_depth = 12;
    try std.testing.expectError(error.InvalidVideoGeometry, hdr.bytes());
    try std.testing.expectError(error.InvalidVideoGeometry, envelopeForSampleBytes(160, 96, 4, 3));
    var invalid = base;
    invalid.surfaces = 33;
    try std.testing.expectError(error.InvalidVideoGeometry, invalid.bytes());
    invalid = base;
    invalid.display_left = 1;
    try std.testing.expectError(error.InvalidVideoGeometry, invalid.bytes());
    invalid = base;
    invalid.codec = 0;
    try std.testing.expectError(error.InvalidVideoGeometry, invalid.bytes());
    invalid = base;
    invalid.coded_width = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidVideoGeometry, invalid.bytes());
}
