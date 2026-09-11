// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const memory_budget = @import("memory_budget.zig");
const lsm_backend = @import("lsm_backend/mod.zig");
const hbc_mod = @import("hbc_adapter.zig");
const resource_manager_mod = @import("resource_manager.zig");

/// Process-wide physical resources shared by every resident storage owner.
/// This deliberately excludes API read/write/status caches so the compiled
/// storage ABI can reuse admission and decoded-index state without pulling
/// control-plane cache composition into its public CAPI artifact.
pub const PhysicalStorageResources = struct {
    alloc: std.mem.Allocator,
    resource_manager: resource_manager_mod.ResourceManager,
    lsm_cache: lsm_backend.Cache,
    hbc_cache: hbc_mod.Cache,

    pub fn init(alloc: std.mem.Allocator) PhysicalStorageResources {
        const budgets = memory_budget.smartResourceBudgets(0);
        return .{
            .alloc = alloc,
            .resource_manager = resource_manager_mod.ResourceManager.init(budgets.options),
            .lsm_cache = lsm_backend.Cache.init(alloc, budgets.lsm_cache_budget_bytes),
            .hbc_cache = hbc_mod.Cache.init(alloc),
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
        self.resource_manager.deinit(self.alloc);
        self.* = undefined;
    }
};
