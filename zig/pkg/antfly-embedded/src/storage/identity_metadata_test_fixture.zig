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

// Tiny payload-limit fixtures must provision real table storage first. No
// accounting opt-out: startup allocations join the same physical envelope,
// and each asserted payload balance excludes only explicitly owned metadata.
const std = @import("std");
const resources = @import("resource_manager.zig");

pub fn init(options: resources.Options) !resources.ResourceManager {
    var startup = options;
    startup.memory_budget = .{};
    var manager = resources.ResourceManager.init(startup);
    errdefer manager.deinit(std.testing.allocator);
    try provision(&manager, options.memory_budget, 1);
    return manager;
}

/// Startup-only fixture preparation, before any work or parent port is
/// published. The caller's payload ceiling is additional to measured table
/// storage. Fixed-capacity wrapper tests provision all their allowed owners.
pub fn provision(manager: *resources.ResourceManager, budget: resources.Budget, batch_capacity: usize) !void {
    const previous = manager.memory.budget;
    manager.memory.budget = .{};
    errdefer manager.memory.budget = previous;
    var reservation = try manager.reserveImmediate(.text_merge_buffers, 1);
    defer reservation.release();
    var batches: std.ArrayList(resources.PreparedBatchReservation) = .empty;
    defer {
        for (batches.items) |*batch| batch.deinit();
        batches.deinit(std.testing.allocator);
    }
    for (0..batch_capacity) |_| {
        try batches.ensureUnusedCapacity(std.testing.allocator, 1);
        batches.appendAssumeCapacity(try manager.prepareBatch(&.{}));
    }
    var observers: usize = 0;
    defer while (observers > 0) {
        std.debug.assert(manager.tryAdjustUsageIdentity(.inference_prompt_cache, observers, 1, 0));
        observers -= 1;
    };
    for (0..batch_capacity) |index| {
        if (!manager.tryAdjustUsageIdentity(.inference_prompt_cache, index + 1, 0, 1)) return error.OutOfMemory;
        observers += 1;
    }
    reservation.release();
    manager.memory.budget = budget;
    if (manager.memory.budget.hard_limit_bytes != 0) manager.memory.budget.hard_limit_bytes += manager.identity_metadata_bytes;
    if (manager.memory.budget.soft_limit_bytes != 0) manager.memory.budget.soft_limit_bytes += manager.identity_metadata_bytes;
}

// These fixtures install only direct ResourceManager callback ports. Include
// the child's startup metadata in each ancestor's explicitly provisioned floor.
pub fn installParent(child: *resources.ResourceManager, parent: *resources.ResourceManager) !void {
    var ancestor: ?*resources.ResourceManager = parent;
    while (ancestor) |owner| {
        if (owner.memory.budget.hard_limit_bytes != 0) owner.memory.budget.hard_limit_bytes += child.identity_metadata_bytes;
        if (owner.memory.budget.soft_limit_bytes != 0) owner.memory.budget.soft_limit_bytes += child.identity_metadata_bytes;
        ancestor = if (owner.memory_parent) |port| @ptrCast(@alignCast(port.ptr)) else null;
    }
    try child.installMemoryParent(parent.memoryParentPort());
}

pub fn payloadUsage(manager: *resources.ResourceManager) u64 {
    return manager.snapshot().memory.used_bytes - manager.identity_metadata_bytes - manager.control_object_bytes - manager.identity_metadata_retirement_bytes;
}

pub fn setPayloadLimit(manager: *resources.ResourceManager, bytes: u64, descendants: []const *resources.ResourceManager) void {
    manager.memory.budget.hard_limit_bytes = bytes + manager.identity_metadata_bytes;
    for (descendants) |child| manager.memory.budget.hard_limit_bytes += child.identity_metadata_bytes;
}
