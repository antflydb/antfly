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

const std = @import("std");
const platform = @import("antfly_platform");
const admission = @import("pjrt_admission.zig");
const memory = @import("../runtime/tier/memory.zig");
const Manager = @import("../server/model_manager.zig").ModelManager;

/// Stable owner for all selection metadata, including parser scratch and
/// retained arenas. Charge before allocation and refund after physical free.
/// Refusing resize makes realloc account for both old and new buffers at peak.
pub const Budget = struct {
    backing: std.mem.Allocator,
    lifetime: Manager.ResourceLifetime,
    limits: memory.Limits,
    lease: memory.AdmissionLease,
    denial: ?anyerror = null,

    const Header = struct { lease: memory.AdmissionLease };

    pub fn create(backing: std.mem.Allocator, context: *const admission.Context) !*Budget {
        const lifetime = try context.manager.retainResourceLifetime();
        errdefer lifetime.deinit();
        const limits = admission.ArtifactDeviceAdmission.limitsFor(context, true);
        var lease = try context.manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_weight_bytes = @sizeOf(Budget) });
        errdefer lease.release();
        const reclaiming = platform.allocator.reclaimingScratch(backing);
        const self = try reclaiming.create(Budget);
        errdefer reclaiming.destroy(self);
        try lease.retain(lease.amounts);
        self.* = .{ .backing = reclaiming, .lifetime = lifetime, .limits = limits, .lease = lease };
        return self;
    }

    pub fn destroy(self: *Budget) void {
        const backing = self.backing;
        const lifetime = self.lifetime;
        var lease = self.lease;
        backing.destroy(self);
        lease.release();
        lifetime.deinit();
    }

    pub fn translate(self: *const Budget, err: anyerror) anyerror {
        return if (err == error.OutOfMemory) self.denial orelse err else err;
    }

    pub fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        const offset = alignment.forward(@sizeOf(Header));
        const total = std.math.add(usize, offset, len) catch {
            self.denial = error.ResourceLimitExceeded;
            return null;
        };
        var lease = self.lifetime.acquireMetadata(self.limits, total) catch |err| {
            self.denial = err;
            return null;
        };
        const block = self.backing.rawAlloc(total, alignment.max(.of(Header)), ra) orelse {
            lease.release();
            return null;
        };
        lease.retain(lease.amounts) catch |err| {
            self.backing.rawFree(block[0..total], alignment.max(.of(Header)), ra);
            lease.release();
            self.denial = err;
            return null;
        };
        const header: *Header = @ptrCast(@alignCast(block));
        header.* = .{ .lease = lease };
        return block + offset;
    }

    fn free(raw: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        const offset = alignment.forward(@sizeOf(Header));
        const block = buf.ptr - offset;
        const header: *Header = @ptrCast(@alignCast(block));
        var lease = header.lease;
        self.backing.rawFree(block[0 .. offset + buf.len], alignment.max(.of(Header)), ra);
        lease.release();
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = std.mem.Allocator.noResize,
        .remap = std.mem.Allocator.noRemap,
        .free = free,
    };
};
