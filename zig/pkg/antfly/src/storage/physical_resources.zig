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
        self.lsm_cache.attachResourceManager(&self.resource_manager);
        self.hbc_cache.attachResourceManager(&self.resource_manager);
    }

    pub fn deinit(self: *PhysicalStorageResources) void {
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
