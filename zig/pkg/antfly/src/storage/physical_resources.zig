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
const memory_budget = @import("memory_budget.zig");
const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
const hbc_mod = @import("antfly_local_sources").storage_hbc_adapter;
const pin_quota = lsm_backend.PinQuota;
const resource_manager_mod = @import("antfly_local_sources").storage_resource_manager;

/// Process-wide physical resources shared by every resident storage owner.
/// This deliberately excludes API read/write/status caches so the compiled
/// storage ABI can reuse admission and decoded-index state without pulling
/// control-plane cache composition into its public CAPI artifact.
pub const PhysicalStorageResources = struct {
    alloc: std.mem.Allocator,
    resource_manager: resource_manager_mod.ResourceManager,
    lsm_cache: lsm_backend.Cache,
    hbc_cache: hbc_mod.Cache,
    // Store a movable identity, never a pointer into the temporary initializer.
    // Resolve its manager only at release through this receiver's stable address.
    lsm_metadata_identity: u64 = 0,
    lsm_metadata_bytes: u64 = 0,
    // No heap observer or pointer into a movable initializer. A high bit fences
    // new borrows once destruction wins the zero-reference transition.
    quota_lifetimes: std.atomic.Value(u64) = .init(0),
    stable_address: usize = 0,
    const closing_bit: u64 = @as(u64, 1) << 63;

    pub fn init(alloc: std.mem.Allocator) PhysicalStorageResources {
        return initFallible(alloc) catch @panic("OOM");
    }

    pub fn initFallible(alloc: std.mem.Allocator) std.mem.Allocator.Error!PhysicalStorageResources {
        return initWithBudgets(alloc, memory_budget.smartResourceBudgets(0));
    }

    pub fn initWithBudgets(alloc: std.mem.Allocator, budgets: memory_budget.SmartResourceBudgets) std.mem.Allocator.Error!PhysicalStorageResources {
        return initWithBudgetsAndParent(alloc, budgets, null) catch return error.OutOfMemory;
    }

    /// Join the common process root before fixed cache metadata is admitted or
    /// allocated. Moving this value does not move the borrowed parent context.
    pub fn initWithBudgetsAndParent(alloc: std.mem.Allocator, budgets: memory_budget.SmartResourceBudgets, parent: ?resource_manager_mod.ResourceManager.MemoryParent) !PhysicalStorageResources {
        var manager = resource_manager_mod.ResourceManager.init(budgets.options);
        errdefer manager.deinit(alloc);
        if (parent) |value| try manager.installMemoryParent(value);
        const metadata_bytes: u64 = @intCast(lsm_backend.Cache.fixedAllocationBytes());
        // Admit through every ancestor before constructing any shard array.
        var metadata = try manager.reserveImmediate(.lsm_block_table_cache, metadata_bytes);
        errdefer metadata.release();
        const cache = try lsm_backend.Cache.initFallible(alloc, budgets.lsm_cache_budget_bytes);
        return .{
            .alloc = alloc,
            .resource_manager = manager,
            .lsm_cache = cache,
            .hbc_cache = hbc_mod.Cache.init(alloc),
            .lsm_metadata_identity = metadata.identity,
            .lsm_metadata_bytes = metadata_bytes,
        };
    }

    /// Call after the resources reach their final stable address.
    pub fn attachResourceManager(self: *PhysicalStorageResources) void {
        if (self.stable_address != 0 and self.stable_address != @intFromPtr(self)) @panic("physical resources moved after binding");
        self.stable_address = @intFromPtr(self);
        self.resource_manager.installAllocatorHostSource(.{ .context = self, .acquire = acquireAllocatorHost }) catch @panic("physical resource allocator host already bound");
        self.lsm_cache.attachResourceManager(&self.resource_manager);
        self.hbc_cache.attachResourceManager(&self.resource_manager);
    }

    fn acquireAllocatorHost(raw: *anyopaque, allocator: std.mem.Allocator, original: @import("antfly_local_sources").common_read_admission.Port) !resource_manager_mod.AllocatorHost {
        const self: *PhysicalStorageResources = @ptrCast(@alignCast(raw));
        try original.checkLifetime();
        if (allocator.ptr != self.alloc.ptr or allocator.vtable != self.alloc.vtable) return error.ResourceAccountingMismatch;
        const lifetime = try self.retainPinQuotaLifetime();
        return .{ .allocator = self.alloc, .manager = lifetime.manager, .parent = lifetime.parent, .context = lifetime.context, .release_joined = lifetime.release_joined };
    }

    /// The enclosing host must keep its allocator bridge and ancestor borrow
    /// alive until tryBeginDeinit succeeds. The compiled storage context does
    /// this before destroying any cache, executor, bridge or execution guard.
    pub fn retainPinQuotaLifetime(self: *PhysicalStorageResources) !pin_quota.HostLifetime {
        if (self.stable_address != @intFromPtr(self)) return error.InvalidArgument;
        const current = self.quota_lifetimes.load(.acquire);
        if (current & closing_bit != 0) return error.BackendRuntimeShuttingDown;
        if (current == closing_bit - 1) return error.ResourceBudgetExceeded;
        if (self.quota_lifetimes.cmpxchgStrong(current, current + 1, .acq_rel, .acquire) != null) return error.ParentMemoryBusy;
        return .{ .manager = &self.resource_manager, .parent = self.resource_manager.memory_parent, .context = self, .release_joined = releasePinQuotaLifetime };
    }
    fn releasePinQuotaLifetime(raw: *anyopaque) void {
        const self: *PhysicalStorageResources = @ptrCast(@alignCast(raw));
        const previous = self.quota_lifetimes.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0 and previous & closing_bit == 0);
        // No receiver access after dropping the final host retention.
    }
    pub fn beginPinQuotaCreation(self: *PhysicalStorageResources, retirement: *pin_quota.Close) !pin_quota.Creation {
        const lifetime = try self.retainPinQuotaLifetime();
        errdefer lifetime.release_joined(lifetime.context);
        return pin_quota.Creation.initHosted(self.alloc, &self.resource_manager, retirement, lifetime);
    }
    /// Finite readiness/fence operation. Busy keeps the entire host untouched;
    /// success rejects all later quota acquisitions before teardown can start.
    pub fn tryBeginDeinit(self: *PhysicalStorageResources) bool {
        return self.quota_lifetimes.cmpxchgStrong(0, closing_bit, .acq_rel, .acquire) == null;
    }

    pub fn deinit(self: *PhysicalStorageResources) void {
        if (self.quota_lifetimes.load(.acquire) != closing_bit and !self.tryBeginDeinit()) @panic("physical resource quota lifetime retained");
        self.hbc_cache.deinit();
        self.lsm_cache.deinit();
        if (self.lsm_metadata_identity != 0) {
            var metadata = resource_manager_mod.Reservation{
                .manager = &self.resource_manager,
                .identity = self.lsm_metadata_identity,
                .slice = .lsm_block_table_cache,
                .bytes = self.lsm_metadata_bytes,
            };
            metadata.release();
        }
        self.resource_manager.deinit(self.alloc);
        self.* = undefined;
    }
};
