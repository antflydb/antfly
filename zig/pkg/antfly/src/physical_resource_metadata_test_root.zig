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
const Resources = @import("storage/physical_resources.zig").PhysicalStorageResources;
const budgets_mod = @import("storage/memory_budget.zig");
const ResourceManager = @import("antfly_local_sources").storage_resource_manager.ResourceManager;
const Cache = @import("antfly_local_sources").storage_lsm_backend_mod.Cache;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;

fn metadataBytes(manager: *ResourceManager) u64 {
    const stats = manager.snapshot();
    return stats.identity_metadata_bytes + stats.identity_metadata_retirement_bytes + stats.control_object_bytes;
}

test "physical cache metadata is admitted before shard allocation" {
    var budgets = budgets_mod.smartResourceBudgetsForTotal(64 * 1024 * 1024);
    budgets.options.memory_budget.hard_limit_bytes = Cache.fixedAllocationBytes() - 1;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Resources.initWithBudgets(failing.allocator(), budgets));
    try std.testing.expect(!failing.has_induced_failure);
}

test "physical cache metadata reservation survives relocation and outlives its shard storage" {
    var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 * 1024 * 1024 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var middle = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer middle.deinit(std.testing.allocator);
    try middle.installMemoryParent(root.memoryParentPort());
    const Tracking = struct {
        watched: ?[*]u8 = null,
        manager: ?*@FieldType(Resources, "resource_manager") = null,
        root: *ResourceManager,
        middle: *ResourceManager,
        freed: bool = false,
        charged_at_free: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = std.testing.allocator.rawAlloc(len, alignment, ra);
            if (len == Cache.fixedAllocationBytes()) self.watched = result;
            return result;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.watched == bytes.ptr) {
                self.freed = true;
                self.charged_at_free = self.manager.?.memory.used_bytes >= Cache.fixedAllocationBytes() and
                    self.root.memory.used_bytes >= Cache.fixedAllocationBytes() and
                    self.middle.memory.used_bytes >= Cache.fixedAllocationBytes();
            }
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    var tracking = Tracking{ .root = &root, .middle = &middle };
    var temporary = try Resources.initWithBudgets(tracking.allocator(), budgets_mod.smartResourceBudgetsForTotal(64 * 1024 * 1024));
    var temporary_owned = true;
    defer if (temporary_owned) temporary.deinit();
    const resident = try std.testing.allocator.create(Resources);
    defer std.testing.allocator.destroy(resident);
    resident.* = temporary;
    temporary_owned = false;
    temporary = undefined;
    var resident_owned = true;
    defer if (resident_owned) resident.deinit();
    tracking.manager = &resident.resource_manager;
    const fixed: u64 = @intCast(Cache.fixedAllocationBytes());
    try std.testing.expectEqual(fixed + metadataBytes(&resident.resource_manager), resident.resource_manager.memory.used_bytes);
    try std.testing.expectEqual(fixed, resident.lsm_metadata_bytes);
    try resident.resource_manager.installMemoryParent(middle.memoryParentPort());
    resident.attachResourceManager();
    try std.testing.expectEqual(fixed + metadataBytes(&resident.resource_manager), resident.resource_manager.memory.used_bytes);
    var handle = try resident.lsm_cache.putRunTableBlock("fixed-metadata", 1, 1, 0, 3, try tracking.allocator().dupe(u8, "raw"));
    const entry: u64 = @intCast(handle.entry.byte_cost);
    try std.testing.expectEqual(fixed + entry + metadataBytes(&resident.resource_manager), resident.resource_manager.memory.used_bytes);
    try std.testing.expectEqual(resident.resource_manager.memory.used_bytes, middle.snapshot().memory.used_bytes);
    try std.testing.expectEqual(resident.resource_manager.memory.used_bytes, root.snapshot().memory.used_bytes);
    handle.release();
    resident.lsm_cache.invalidatePath("fixed-metadata");
    try std.testing.expectEqual(fixed + metadataBytes(&resident.resource_manager), resident.resource_manager.memory.used_bytes);
    resident.deinit();
    resident_owned = false;
    try std.testing.expect(tracking.freed);
    try std.testing.expect(tracking.charged_at_free);
    try std.testing.expectEqual(@as(u64, 0), middle.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "physical cache metadata initialization allocation failures release the reserved owner" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: std.mem.Allocator) !void {
            var resources = try Resources.initWithBudgets(allocator, budgets_mod.smartResourceBudgetsForTotal(64 * 1024 * 1024));
            defer resources.deinit();
            try std.testing.expectEqual(@as(u64, @intCast(Cache.fixedAllocationBytes())) + metadataBytes(&resources.resource_manager), resources.resource_manager.memory.used_bytes);
        }
    }.run, .{});
}

test "physical cache metadata parent attachment rejects a saturated ancestor without partial charges" {
    var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var middle = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer middle.deinit(std.testing.allocator);
    try middle.installMemoryParent(root.memoryParentPort());
    var resources = try Resources.initWithBudgets(std.testing.allocator, budgets_mod.smartResourceBudgetsForTotal(64 << 20));
    var owned = true;
    defer if (owned) resources.deinit();
    const charge = resources.resource_manager.snapshot().memory.used_bytes;
    try std.testing.expect(charge > Cache.fixedAllocationBytes());
    // Fix this isolated parent's envelope at the complete child's cost,
    // including its retained identity metadata; no concurrent users exist.
    root.memory.budget.hard_limit_bytes = charge;
    try std.testing.expect(root.reserveExternal(1));
    var sibling_live = true;
    defer if (sibling_live) root.releaseExternal(1);
    try std.testing.expectError(error.ResourceBudgetExceeded, resources.resource_manager.installMemoryParent(middle.memoryParentPort()));
    try std.testing.expect(resources.resource_manager.memory_parent == null);
    try std.testing.expectEqual(charge, resources.resource_manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), middle.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), root.snapshot().memory.used_bytes);
    root.releaseExternal(1);
    sibling_live = false;
    try resources.resource_manager.installMemoryParent(middle.memoryParentPort());
    try resources.resource_manager.installMemoryParent(middle.memoryParentPort());
    resources.attachResourceManager();
    const attached = resources.resource_manager.snapshot().memory.used_bytes;
    try std.testing.expectEqual(attached, middle.snapshot().memory.used_bytes);
    try std.testing.expectEqual(attached, root.snapshot().memory.used_bytes);
    try std.testing.expect(!root.reserveExternal(1));
    resources.deinit();
    owned = false;
    try std.testing.expectEqual(@as(u64, 0), middle.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    try std.testing.expect(root.reserveExternal(charge));
    root.releaseExternal(charge);
}

test "physical cache metadata parent birth rejects pressure before shard allocation" {
    const fixed: u64 = @intCast(Cache.fixedAllocationBytes());
    var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = fixed - 1 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.ResourceBudgetExceeded, Resources.initWithBudgetsAndParent(failing.allocator(), budgets_mod.smartResourceBudgetsForTotal(64 << 20), root.memoryParentPort()));
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "physical cache metadata parent birth preserves shared root pressure across relocation and sibling release" {
    const fixed: u64 = @intCast(Cache.fixedAllocationBytes());
    const limit: u64 = 64 << 20;
    var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = limit }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    const Tracking = struct {
        root: *ResourceManager,
        allocations: usize = 0,
        frees: usize = 0,
        charged: bool = true,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.allocations += 1;
            self.charged = self.charged and self.root.snapshot().memory.used_bytes >= Cache.fixedAllocationBytes();
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
            self.charged = self.charged and self.root.snapshot().memory.used_bytes >= Cache.fixedAllocationBytes();
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    var tracking: Tracking = .{ .root = &root };
    var temporary = try Resources.initWithBudgetsAndParent(tracking.allocator(), budgets_mod.smartResourceBudgetsForTotal(limit), root.memoryParentPort());
    var moved = false;
    defer if (!moved) temporary.deinit();
    const resident = try std.testing.allocator.create(Resources);
    defer std.testing.allocator.destroy(resident);
    resident.* = temporary;
    temporary = undefined;
    moved = true;
    var live = true;
    defer if (live) resident.deinit();
    resident.attachResourceManager();
    var entry = try resident.lsm_cache.putRunTableBlock("root-reclaim", 1, 1, 0, 3, try tracking.allocator().dupe(u8, "raw"));
    const entry_bytes: u64 = @intCast(entry.entry.byte_cost);
    entry.release();
    const charged = fixed + entry_bytes + metadataBytes(&resident.resource_manager);
    try std.testing.expect(charged > fixed);
    try std.testing.expectEqual(charged, root.snapshot().memory.used_bytes);
    try std.testing.expectEqual(charged, resident.resource_manager.snapshot().memory.used_bytes);
    const child = resident.resource_manager.memoryParentPort();
    const observed = child.epoch_fn(child.ptr);
    // Model a simultaneous host-runtime reservation through the common root.
    try std.testing.expect(root.reserveExternal(limit - charged));
    var sibling_live = true;
    defer if (sibling_live) root.releaseExternal(limit - charged);
    try std.testing.expectEqual(ResourceManager.MemoryParent.ReserveResult.denied, child.try_reserve_fn.?(child.ptr, 1));
    try std.testing.expectEqual(charged, resident.resource_manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(limit, root.snapshot().memory.used_bytes);
    // The nearest storage child must retain its cache hooks even though the
    // shortage is in a common ancestor, not its own local envelope.
    try std.testing.expect(child.reclaim_fn(child.ptr, 1) > 0);
    try std.testing.expectEqual(@as(usize, 0), resident.lsm_cache.snapshotStats().entry_count);
    try std.testing.expect(root.snapshot().memory.used_bytes < limit);
    try std.testing.expectEqual(ResourceManager.MemoryParent.ReserveResult.reserved, child.try_reserve_fn.?(child.ptr, 1));
    child.release_fn(child.ptr, 1);
    root.releaseExternal(limit - charged);
    sibling_live = false;
    try std.testing.expect(child.epoch_fn(child.ptr) != observed);
    try std.testing.expectEqual(ResourceManager.MemoryParent.ReserveResult.reserved, child.try_reserve_fn.?(child.ptr, 1));
    child.release_fn(child.ptr, 1);
    resident.deinit();
    live = false;
    try std.testing.expect(tracking.allocations > 0 and tracking.frees > 0 and tracking.charged);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "physical cache metadata parent birth allocation failures return all ancestor charges" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: std.mem.Allocator) !void {
            var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 }, .identity_allocator = std.testing.allocator });
            defer root.deinit(std.testing.allocator);
            var resources = Resources.initWithBudgetsAndParent(allocator, budgets_mod.smartResourceBudgetsForTotal(64 << 20), root.memoryParentPort()) catch |err| {
                try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
                return err;
            };
            resources.deinit();
            try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
        }
    }.run, .{});
}

test "physical cache metadata bootstrap request rejects old prefixes and copies the shared process parent" {
    const services = @import("storage/kernel_runtime_services.zig");
    const Old = extern struct { version: u32, reserved: u32 = 0 };
    var old: Old align(@alignOf(services.Request)) = .{ .version = 2 };
    try std.testing.expectError(error.InvalidAbiVersion, services.Request.readMemoryParent(@ptrCast(&old)));
    old.version = 1;
    try std.testing.expectError(error.InvalidAbiVersion, services.Request.readMemoryParent(@ptrCast(&old)));
    var header: extern struct { version: u32, struct_size: u32 } align(@alignOf(services.memory.Parent)) = .{ .version = services.memory.Parent.abi_version - 1, .struct_size = @sizeOf(services.memory.Parent) };
    var request: services.Request = .{ .memory_parent = @ptrCast(&header) };
    try std.testing.expectError(error.InvalidArgument, request.readMemoryParent());
    header.version = services.memory.Parent.abi_version;
    header.struct_size -= 1;
    try std.testing.expectError(error.InvalidArgument, request.readMemoryParent());
    var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var port = root.memoryParentPort();
    request.memory_parent = &port;
    const copied = (try request.readMemoryParent()).?;
    request.memory_parent = null;
    port.version = 0;
    try std.testing.expectEqual(services.memory.Parent.ReserveResult.reserved, copied.try_reserve_fn.?(copied.ptr, 100));
    try std.testing.expect(!root.reserveExternal(1));
    copied.release_fn(copied.ptr, 100);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "physical cache metadata bootstrap header stays charged through allocation and physical destruction" {
    const Bootstrap = @import("storage/kernel_runtime_services.zig").BootstrapAllocation;
    const Header = struct { bytes: [256]u8 };
    var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = @sizeOf(Header) }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    const parent = root.memoryParentPort();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Bootstrap.allocate(Header, failing.allocator(), parent));
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    const Tracking = struct {
        root: *ResourceManager,
        allocation_charged: bool = false,
        destruction_charged: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.allocation_charged = self.root.snapshot().memory.used_bytes == @sizeOf(Header);
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destruction_charged = self.root.snapshot().memory.used_bytes == @sizeOf(Header);
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    var tracking: Tracking = .{ .root = &root };
    const header = try Bootstrap.allocate(Header, tracking.allocator(), parent);
    @memset(&header.bytes, 42);
    try std.testing.expect(tracking.allocation_charged);
    Bootstrap.destroy(Header, tracking.allocator(), parent, header);
    try std.testing.expect(tracking.destruction_charged);
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
    try std.testing.expect(root.reserveExternal(@sizeOf(Header)));
    defer root.releaseExternal(@sizeOf(Header));
    failing.has_induced_failure = false;
    try std.testing.expectError(error.ResourceBudgetExceeded, Bootstrap.allocate(Header, failing.allocator(), parent));
    try std.testing.expect(!failing.has_induced_failure);
}

test "physical cache metadata process totals include host siblings once and retain storage slice attribution" {
    var root = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 << 20 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var child = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(root.memoryParentPort());
    try std.testing.expect(root.reserveExternal(4096));
    root.releaseExternal(4096);
    var cache = try child.reserveImmediate(.lsm_block_table_cache, 20);
    defer cache.release();
    try std.testing.expect(root.reserveExternal(40));
    defer root.releaseExternal(40);
    const before = child.snapshot();
    var reported = before;
    @import("storage/process_memory_stats.zig").apply(&reported, &root);
    try std.testing.expectEqual(root.snapshot().memory.used_bytes, reported.memory.used_bytes);
    try std.testing.expect(reported.memory.used_bytes > before.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 4096), reported.memory.peak_bytes);
    try std.testing.expectEqual(@as(u64, 1 << 20), reported.memory.hard_limit_bytes);
    try std.testing.expectEqualDeep(before.slices, reported.slices);
}

const PinQuota = @import("antfly_local_sources").storage_lsm_backend_mod.PinQuota;
const read_admission = @import("antfly_local_sources").common_read_admission;
var quota_test_tag: u8 = 0;
fn quotaCheck(_: *anyopaque) !void {}
fn quotaWait(_: *anyopaque) !void {
    return error.ParentMemoryBusy;
}
fn quotaControl() read_admission.Port {
    const Holder = struct {
        const payload: read_admission.Payload = .{ .ptr = &quota_test_tag, .vtable = &.{ .checkpoint = quotaCheck, .check_lifetime = quotaCheck, .wait_for_progress = quotaWait } };
    };
    return .init(&Holder.payload);
}

test "physical cache metadata hosted quota keeps manager allocator and parent alive until joined release" {
    const a = std.testing.allocator;
    var root = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
    defer root.deinit(a);
    var resources = try Resources.initWithBudgetsAndParent(a, budgets_mod.smartResourceBudgetsForTotal(64 << 20), root.memoryParentPort());
    defer resources.deinit();
    var close: PinQuota.Close = .{};
    defer close.deinitJoined();
    try std.testing.expectError(error.InvalidArgument, resources.beginPinQuotaCreation(&close));
    resources.attachResourceManager();
    var creator = try resources.beginPinQuotaCreation(&close);
    defer creator.deinitJoined();
    try std.testing.expect(!resources.tryBeginDeinit());
    try creator.resumeWithAdmission(quotaControl());
    const owner = try creator.takeWithAdmission(quotaControl());
    _ = try owner.acquireWithAdmission(quotaControl());
    try std.testing.expectError(error.ParentMemoryBusy, close.resumeWithAdmission(quotaControl()));
    try std.testing.expectError(error.BackendRuntimeShuttingDown, owner.acquireWithAdmission(quotaControl()));
    try std.testing.expect(!resources.tryBeginDeinit());
    owner.releaseBorrow();
    try close.resumeWithAdmission(quotaControl());
    try std.testing.expectEqual(@as(u64, 0), resources.resource_manager.sliceStats(.lsm_registry_metadata).used_bytes);
    // Physical destruction does not discard the enclosing allocator/ancestor.
    try std.testing.expect(!resources.tryBeginDeinit());
    close.deinitJoined();
    close.deinitJoined();
    try std.testing.expect(resources.tryBeginDeinit());
    try std.testing.expectError(error.BackendRuntimeShuttingDown, resources.beginPinQuotaCreation(&close));
}

test "physical cache metadata hosted quota rejects foreign manager and parent without consuming its lifetime" {
    const a = std.testing.allocator;
    var resources = try Resources.initFallible(a);
    defer resources.deinit();
    resources.attachResourceManager();
    const lifetime = try resources.retainPinQuotaLifetime();
    var wrong = lifetime;
    var other = ResourceManager.init(.{ .identity_allocator = a });
    defer other.deinit(a);
    var close: PinQuota.Close = .{};
    wrong.manager = &other;
    try std.testing.expectError(error.ResourceAccountingMismatch, PinQuota.Creation.initHosted(a, &resources.resource_manager, &close, wrong));
    wrong = lifetime;
    wrong.parent = other.memoryParentPort();
    try std.testing.expectError(error.InvalidMemoryParent, PinQuota.Creation.initHosted(a, &resources.resource_manager, &close, wrong));
    try std.testing.expect(close.manager == null);
    try std.testing.expect(!resources.tryBeginDeinit());
    lifetime.release_joined(lifetime.context);
    try std.testing.expect(resources.tryBeginDeinit());
}

test "physical cache metadata hosted quota constructor OOM returns host and every ancestor charge" {
    const Fixture = struct {
        fn run(at: ?usize) !usize {
            const a = std.testing.allocator;
            var parent = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
            defer parent.deinit(a);
            var backing = std.testing.FailingAllocator.init(a, .{});
            var budgets = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
            budgets.options.identity_allocator = backing.allocator();
            var resources = try Resources.initWithBudgetsAndParent(backing.allocator(), budgets, parent.memoryParentPort());
            var resources_live = true;
            defer if (resources_live) resources.deinit();
            resources.attachResourceManager();
            // Cache/reclaimer birth has separate failure coverage. Exhaust
            // quota header/observer allocation with the actual host bound.
            const begin = backing.alloc_index;
            if (at) |index| backing.fail_index = begin + index;
            var close: PinQuota.Close = .{};
            defer close.deinitJoined();
            var creator = try resources.beginPinQuotaCreation(&close);
            defer creator.deinitJoined();
            const result: anyerror!void = blk: {
                creator.resumeWithAdmission(quotaControl()) catch |cause| break :blk cause;
                _ = creator.takeWithAdmission(quotaControl()) catch |cause| break :blk cause;
                break :blk close.resumeWithAdmission(quotaControl());
            };
            const allocations = backing.alloc_index - begin;
            // Teardown runs while the failed allocator remains exhausted.
            creator.deinitJoined();
            close.deinitJoined();
            try std.testing.expect(resources.tryBeginDeinit());
            if (at != null) {
                try std.testing.expect(backing.has_induced_failure);
                try std.testing.expectError(error.OutOfMemory, result);
            } else try result;
            backing.fail_index = std.math.maxInt(usize);
            resources.deinit();
            resources_live = false;
            try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
            return allocations;
        }
    };
    const allocations = try Fixture.run(null);
    try std.testing.expect(allocations > 0);
    for (0..allocations) |at| _ = try Fixture.run(at);
}

test "physical cache metadata host lifetime acquisition and destruction have one atomic winner" {
    const Worker = struct {
        resources: *Resources,
        start: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        stop: std.atomic.Value(bool) = .init(false),
        held: ?PinQuota.HostLifetime = null,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            while (!self.start.load(.acquire)) std.Thread.yield() catch {};
            self.held = self.resources.retainPinQuotaLifetime() catch |cause| blk: {
                self.failure = cause;
                break :blk null;
            };
            self.done.store(true, .release);
            while (!self.stop.load(.acquire)) std.Thread.yield() catch {};
            if (self.held) |held| held.release_joined(held.context);
        }
    };
    for (0..24) |_| {
        var resources = try Resources.initFallible(std.testing.allocator);
        defer resources.deinit();
        resources.attachResourceManager();
        var worker: Worker = .{ .resources = &resources };
        const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
        var joined = false;
        defer if (!joined) {
            worker.stop.store(true, .release);
            thread.join();
        };
        worker.start.store(true, .release);
        const closed = resources.tryBeginDeinit();
        while (!worker.done.load(.acquire)) std.Thread.yield() catch {};
        if (closed) {
            try std.testing.expect(worker.held == null);
            const failure = worker.failure orelse return error.TestUnexpectedResult;
            try std.testing.expect(failure == error.BackendRuntimeShuttingDown or failure == error.ParentMemoryBusy);
        } else {
            try std.testing.expect(worker.held != null);
            try std.testing.expect(!resources.tryBeginDeinit());
        }
        worker.stop.store(true, .release);
        thread.join();
        joined = true;
        if (!closed) try std.testing.expect(resources.tryBeginDeinit());
    }
}

test "physical cache metadata final hosted release may destroy its containing retirement frame" {
    const Frame = struct {
        manager: ResourceManager,
        close: PinQuota.Close = .{},
        destroyed: *bool,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.close.host_lifetime == null and self.close.lifetime == null and self.close.parent_lifetime == null);
            std.debug.assert(self.close.complete and self.close.usage == 0);
            self.manager.deinit(std.testing.allocator);
            self.destroyed.* = true;
            std.testing.allocator.destroy(self);
        }
    };
    const a = std.testing.allocator;
    var destroyed = false;
    const frame = try a.create(Frame);
    frame.* = .{ .manager = ResourceManager.init(.{ .identity_allocator = a }), .destroyed = &destroyed };
    var creator = PinQuota.Creation.initHosted(a, &frame.manager, &frame.close, .{ .manager = &frame.manager, .parent = null, .context = frame, .release_joined = Frame.release }) catch |cause| {
        frame.manager.deinit(a);
        a.destroy(frame);
        return cause;
    };
    defer creator.deinitJoined();
    try creator.resumeWithAdmission(quotaControl());
    _ = try creator.takeWithAdmission(quotaControl());
    try frame.close.resumeWithAdmission(quotaControl());
    try std.testing.expect(!destroyed);
    frame.close.deinitJoined();
    try std.testing.expect(destroyed);
}

const PublicationResourcesFixture = struct {
    const Owner = @import("antfly_local_sources").storage_lsm_backend_mod.Backend.PublicationResources;
    const Phase = enum { construct, retire };
    const Fault = struct {
        calls: usize = 0,
        at: ?usize = null,
        live: bool = true,
        held_on_failure: ?*std.atomic.Value(u64) = null,
        observed_held: bool = false,
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.live) return;
            const index = self.calls;
            self.calls += 1;
            if (self.at == index) {
                if (self.held_on_failure) |refs| self.observed_held = refs.load(.acquire) != 0;
                return self.failure;
            }
        }
        fn wait(_: *anyopaque) !void {
            return error.TestUnexpectedWait;
        }
    };
    const Stats = struct { checks: usize, allocations: usize };
    fn drainJoined(manager: *ResourceManager) void {
        for (0..1024) |_| {
            if (manager.payload_retirement_ready.load(.acquire) == 0) return;
            _ = manager.drainPayloadRetirementQuiesced();
        }
        @panic("registered publication resources failed to join");
    }
    fn run(phase: Phase, at: ?usize, allocation: ?usize, failure: anyerror) !Stats {
        const a = std.testing.allocator;
        var parent = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent.deinit(a);
        var backing = std.testing.FailingAllocator.init(a, .{});
        var budgets = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets.options.identity_allocator = backing.allocator();
        var resources = try Resources.initWithBudgetsAndParent(backing.allocator(), budgets, parent.memoryParentPort());
        defer resources.deinit();
        resources.attachResourceManager();
        const manager = &resources.resource_manager;
        var owner: ?*Owner = null;
        defer {
            if (owner) |value| value.requestRetirement();
            drainJoined(manager);
        }
        var fault: Fault = .{ .at = at, .failure = failure, .live = phase == .construct };
        const payload: read_admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check, .check_lifetime = Fault.check, .wait_for_progress = Fault.wait } };
        const original = read_admission.Port.init(&payload);
        const begin = backing.alloc_index;
        if (allocation) |index| backing.fail_index = begin + index;
        const result: anyerror!void = blk: {
            owner = Owner.start(backing.allocator(), manager, original) catch |cause| break :blk cause;
            owner.?.resumeWithAdmission(original) catch |cause| break :blk cause;
            if (phase == .retire) {
                owner.?.requestRetirement();
                owner = null; // The registry now owns all access to the frame.
                fault.live = true;
                while (manager.payload_retirement_ready.load(.acquire) != 0) _ = manager.drainPayloadRetirementWithAdmission(original) catch |cause| break :blk cause;
            }
            break :blk {};
        };
        const stats: Stats = .{ .checks = fault.calls, .allocations = backing.alloc_index - begin };
        if (allocation != null) {
            try std.testing.expect(backing.has_induced_failure);
            try std.testing.expectError(error.OutOfMemory, result);
        } else if (at != null) try std.testing.expectError(failure, result) else try result;
        if (owner) |value| value.requestRetirement();
        owner = null;
        // The allocator stays exhausted while partial registered construction
        // and physical quota headers retire under independent joined ownership.
        drainJoined(manager);
        try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_registry_metadata).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
        backing.fail_index = std.math.maxInt(usize);
        return stats;
    }
};

test "physical cache metadata publication resources preserve every constructor and closure refusal" {
    for (std.enums.values(PublicationResourcesFixture.Phase)) |phase| {
        const stats = try PublicationResourcesFixture.run(phase, null, null, error.Canceled);
        try std.testing.expect(stats.checks != 0);
        for (0..stats.checks) |at| for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound, error.ParentMemoryBusy }) |failure| {
            _ = try PublicationResourcesFixture.run(phase, at, null, failure);
        };
    }
}

test "physical cache metadata publication resources keep registered partial ownership through every OOM" {
    const stats = try PublicationResourcesFixture.run(.construct, null, null, error.Canceled);
    try std.testing.expect(stats.allocations != 0);
    for (0..stats.allocations) |at| _ = try PublicationResourcesFixture.run(.construct, null, at, error.Canceled);
}

test "physical cache metadata allocator host rejects a foreign allocator and returns refused acquisitions" {
    var resources = try Resources.initFallible(std.testing.allocator);
    defer resources.deinit();
    resources.attachResourceManager();
    var foreign = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    try std.testing.expectError(error.ResourceAccountingMismatch, resources.resource_manager.acquireAllocatorHostWithAdmission(foreign.allocator(), quotaControl()));
    try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
    var fault: PublicationResourcesFixture.Fault = .{ .at = 1, .failure = error.FileNotFound, .held_on_failure = &resources.quota_lifetimes };
    const payload: read_admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = PublicationResourcesFixture.Fault.check, .check_lifetime = quotaCheck } };
    // The second checkpoint rejects after the producer acquired its real host.
    try std.testing.expectError(error.FileNotFound, resources.resource_manager.acquireAllocatorHostWithAdmission(std.testing.allocator, .init(&payload)));
    try std.testing.expect(fault.observed_held);
    try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
    const held = try resources.resource_manager.acquireAllocatorHostWithAdmission(std.testing.allocator, quotaControl());
    try std.testing.expect(!resources.tryBeginDeinit());
    held.release();
    try std.testing.expect(resources.tryBeginDeinit());
    try std.testing.expectError(error.BackendRuntimeShuttingDown, resources.resource_manager.acquireAllocatorHostWithAdmission(std.testing.allocator, quotaControl()));
}

test "physical cache metadata registered publication closure yields to a live quota borrower" {
    var resources = try Resources.initFallible(std.testing.allocator);
    defer resources.deinit();
    resources.attachResourceManager();
    const owner = try PublicationResourcesFixture.Owner.start(std.testing.allocator, &resources.resource_manager, quotaControl());
    try owner.resumeWithAdmission(quotaControl());
    const held = try owner.quota_owner.?.acquireWithAdmission(quotaControl());
    owner.requestRetirement();
    try std.testing.expectEqual(@as(usize, 0), resources.resource_manager.drainPayloadRetirementQuiesced());
    try std.testing.expect(!resources.tryBeginDeinit());
    try std.testing.expectError(error.BackendRuntimeShuttingDown, held.acquireWithAdmission(quotaControl()));
    held.releaseBorrow();
    PublicationResourcesFixture.drainJoined(&resources.resource_manager);
    try std.testing.expect(resources.tryBeginDeinit());
}

test "physical cache metadata publication entry keeps real host and quota after backend and controller exit" {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const a = std.testing.allocator;
    var resources = try Resources.initFallible(a);
    defer resources.deinit();
    resources.attachResourceManager();
    var backend = lsm.Backend.init(a, .{ .resource_manager = &resources.resource_manager, .wal_enabled = false });
    var backend_live = true;
    defer if (backend_live) backend.close();
    const owner = try backend.beginPublicationResourcesWithAdmission(quotaControl());
    var producer_live = true;
    defer {
        if (producer_live) owner.requestRetirement();
        PublicationResourcesFixture.drainJoined(&resources.resource_manager);
    }
    try owner.resumeWithAdmission(quotaControl());
    try std.testing.expect(backend.mu.tryLock());
    const planned = backend.planningDirectory();
    backend.mu.unlock();
    const directory = try (try planned).fork(a);
    defer directory.destroy(a);
    try directory.tree.prepare(a);
    const Source = struct {
        released: bool = false,
        fn release(raw: *anyopaque, original: ?read_admission.Port) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (original) |port| try port.checkpoint();
            std.debug.assert(!self.released);
            self.released = true;
            return true;
        }
    };
    var source: Source = .{};
    var lifetime: ?lsm.Backend.DirectoryEntryLifetime = .{ .context = &source, .release = Source.release };
    const run: lsm.repository.Run = .{ .id = 901, .level = 1, .size_bytes = 1, .path = @constCast("publication-scope-output.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("z"), .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false };
    try owner.beginEntryWithAdmission(&backend, directory, run, &lifetime, quotaControl());
    try std.testing.expect(lifetime == null);
    var lease = try owner.takeEntryWithAdmission(quotaControl());
    var lease_live = true;
    defer if (lease_live) lease.deinit(a);
    backend.close();
    backend_live = false;
    owner.requestRetirement();
    producer_live = false;
    // Joined cleanup returns instead of waiting on the entry owner queued in
    // this same registry. Its reader lease and source remain usable.
    for (0..2) |_| try std.testing.expectEqual(@as(usize, 0), resources.resource_manager.drainPayloadRetirementQuiesced());
    try std.testing.expect(!source.released);
    try std.testing.expectEqualStrings("a", lease.entry.run.smallest_key);
    try std.testing.expect(!resources.tryBeginDeinit());
    lease.deinit(a);
    lease_live = false;
    PublicationResourcesFixture.drainJoined(&resources.resource_manager);
    try std.testing.expect(source.released);
    try std.testing.expectEqual(@as(u64, 0), resources.resource_manager.sliceStats(.lsm_registry_metadata).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
}

test "physical cache metadata unhosted publication construction refuses without a fallback lifetime" {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    var manager = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var backend = lsm.Backend.init(std.testing.allocator, .{ .resource_manager = &manager, .wal_enabled = false });
    defer backend.close();
    try std.testing.expectError(error.ExecutionWaitUnsupported, backend.beginPublicationResourcesWithAdmission(quotaControl()));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.payload_retirement_ready.load(.acquire));
}

test "physical cache metadata allocator host source cannot be rebound after stable attachment" {
    var resources = try Resources.initFallible(std.testing.allocator);
    defer resources.deinit();
    resources.attachResourceManager();
    const original = resources.resource_manager.allocator_host_source.?;
    try resources.resource_manager.installAllocatorHostSource(original);
    var different = original;
    different.context = &quota_test_tag;
    try std.testing.expectError(error.InvalidArgument, resources.resource_manager.installAllocatorHostSource(different));
    try std.testing.expect(resources.resource_manager.allocator_host_source.?.context == original.context);
    const held = try resources.resource_manager.acquireAllocatorHostWithAdmission(std.testing.allocator, quotaControl());
    held.release();
    try std.testing.expect(resources.tryBeginDeinit());
}

test "physical cache metadata admitted reservation preserves original busy refusal before retry classification" {
    var manager = ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var baseline: PublicationResourcesFixture.Fault = .{};
    const base_payload: read_admission.Payload = .{ .ptr = &baseline, .vtable = &.{ .checkpoint = PublicationResourcesFixture.Fault.check, .check_lifetime = PublicationResourcesFixture.Fault.check, .wait_for_progress = PublicationResourcesFixture.Fault.wait } };
    var reservation = try manager.reserveWithoutReclaimWithAdmission(.lsm_table_builder_working_set, 1, .init(&base_payload));
    reservation.release();
    for (0..baseline.calls) |at| {
        var fault: PublicationResourcesFixture.Fault = .{ .at = at, .failure = error.ParentMemoryBusy };
        const payload: read_admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = PublicationResourcesFixture.Fault.check, .check_lifetime = PublicationResourcesFixture.Fault.check, .wait_for_progress = PublicationResourcesFixture.Fault.wait } };
        try std.testing.expectError(error.ParentMemoryBusy, manager.reserveWithoutReclaimWithAdmission(.lsm_table_builder_working_set, 1, .init(&payload)));
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    }
}

test "physical cache metadata shared publication pins retain the original host until the last peer exits" {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const a = std.testing.allocator;
    const Source = struct {
        released: bool = false,
        fn release(raw: *anyopaque, original: ?read_admission.Port) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (original) |port| try port.checkpoint();
            std.debug.assert(!self.released);
            self.released = true;
            return true;
        }
    };
    const Lease = lsm.Backend.PublicationResources.EntryLease;
    const Held = struct {
        owner: ?*lsm.Backend.PublicationResources = null,
        lease: ?Lease = null,
        fn create(backend: *lsm.Backend, source: *Source) !@This() {
            const owner = try backend.beginPublicationResourcesWithAdmission(quotaControl());
            errdefer owner.requestRetirement();
            try owner.resumeWithAdmission(quotaControl());
            try std.testing.expect(backend.mu.tryLock());
            const planned = backend.planningDirectory();
            backend.mu.unlock();
            const directory = try (try planned).fork(std.testing.allocator);
            defer directory.destroy(std.testing.allocator);
            try directory.tree.prepare(std.testing.allocator);
            var lifetime: ?lsm.Backend.DirectoryEntryLifetime = .{ .context = source, .release = Source.release };
            const run: lsm.repository.Run = .{ .id = 902, .level = 1, .size_bytes = 1, .path = @constCast("publication-scope-shared.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("b"), .largest_namespace_name = null, .largest_key = @constCast("y"), .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false };
            try owner.beginEntryWithAdmission(backend, directory, run, &lifetime, quotaControl());
            const lease = try owner.takeEntryWithAdmission(quotaControl());
            return .{ .owner = owner, .lease = lease };
        }
        fn retire(self: *@This()) void {
            if (self.lease) |*lease| lease.deinit(std.testing.allocator);
            self.lease = null;
            if (self.owner) |owner| owner.requestRetirement();
            self.owner = null;
        }
    };
    var parent1 = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
    defer parent1.deinit(a);
    var parent2 = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
    defer parent2.deinit(a);
    var first = try Resources.initWithBudgetsAndParent(a, budgets_mod.smartResourceBudgetsForTotal(64 << 20), parent1.memoryParentPort());
    defer first.deinit();
    first.attachResourceManager();
    var second = try Resources.initWithBudgetsAndParent(a, budgets_mod.smartResourceBudgetsForTotal(64 << 20), parent2.memoryParentPort());
    defer second.deinit();
    second.attachResourceManager();
    var backend1 = lsm.Backend.init(a, .{ .resource_manager = &first.resource_manager, .wal_enabled = false });
    var live1 = true;
    defer if (live1) backend1.close();
    var backend2 = lsm.Backend.init(a, .{ .resource_manager = &second.resource_manager, .wal_enabled = false });
    var live2 = true;
    defer if (live2) backend2.close();
    var source1: Source = .{};
    var source2: Source = .{};
    var held1: Held = .{};
    var held2: Held = .{};
    defer {
        held1.retire();
        held2.retire();
        for (0..1024) |_| {
            if (first.resource_manager.payload_retirement_ready.load(.acquire) == 0 and second.resource_manager.payload_retirement_ready.load(.acquire) == 0) break;
            _ = first.resource_manager.drainPayloadRetirementQuiesced();
            _ = second.resource_manager.drainPayloadRetirementQuiesced();
        }
    }
    held1 = try Held.create(&backend1, &source1);
    held2 = try Held.create(&backend2, &source2);
    backend1.close();
    live1 = false;
    backend2.close();
    live2 = false;
    held1.retire();
    for (0..4) |_| _ = first.resource_manager.drainPayloadRetirementQuiesced();
    try std.testing.expect(source1.released and !source2.released);
    try std.testing.expect(!first.tryBeginDeinit());
    try std.testing.expectEqualStrings("b", held2.lease.?.entry.run.smallest_key);
    // The second entry's final pin forgets the shared registry allocation,
    // returning capacity to the first host's independently retained quota.
    held2.retire();
    PublicationResourcesFixture.drainJoined(&second.resource_manager);
    PublicationResourcesFixture.drainJoined(&first.resource_manager);
    try std.testing.expect(source2.released);
    try std.testing.expectEqual(@as(u64, 0), first.quota_lifetimes.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), second.quota_lifetimes.load(.acquire));
    try std.testing.expect(first.tryBeginDeinit());
    try std.testing.expect(second.tryBeginDeinit());
}

test "physical cache metadata final pin retirement resumes local staleness and preserves original stale refusal" {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |original_failure| {
        var resources = try Resources.initFallible(a);
        defer resources.deinit();
        resources.attachResourceManager();
        var backend = lsm.Backend.init(a, .{ .resource_manager = &resources.resource_manager, .wal_enabled = false });
        var backend_live = true;
        defer if (backend_live) backend.close();
        const owner = try backend.beginPublicationResourcesWithAdmission(quotaControl());
        var producer_live = true;
        defer {
            if (producer_live) owner.requestRetirement();
            PublicationResourcesFixture.drainJoined(&resources.resource_manager);
        }
        try owner.resumeWithAdmission(quotaControl());
        try std.testing.expect(backend.mu.tryLock());
        const planned = backend.planningDirectory();
        backend.mu.unlock();
        const directory = try (try planned).fork(a);
        defer directory.destroy(a);
        try directory.tree.prepare(a);
        const Source = struct {
            released: bool = false,
            fn release(raw: *anyopaque, original: ?read_admission.Port) !bool {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (original) |port| try port.checkpoint();
                std.debug.assert(!self.released);
                self.released = true;
                return true;
            }
        };
        var source: Source = .{};
        var lifetime: ?lsm.Backend.DirectoryEntryLifetime = .{ .context = &source, .release = Source.release };
        const path = "publication-scope-stale.sst";
        const run: lsm.repository.Run = .{ .id = 903, .level = 1, .size_bytes = 1, .path = @constCast(path), .smallest_namespace_name = null, .smallest_key = @constCast("c"), .largest_namespace_name = null, .largest_key = @constCast("x"), .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false };
        try owner.beginEntryWithAdmission(&backend, directory, run, &lifetime, quotaControl());
        const anchor = owner.entry.?;
        var lease = try owner.takeEntryWithAdmission(quotaControl());
        lease.deinit(a);
        backend.close();
        backend_live = false;
        owner.requestRetirement();
        producer_live = false;
        const Hook = struct {
            anchor: *lsm.Backend.DirectoryEntryOwner,
            original_failure: bool,
            triggered: bool = false,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                // Only observe the in-flight final anchor before its forget
                // publishes. Once triggered, never access its pointer again.
                if (self.triggered) return;
                const operation = self.anchor.forgetting orelse return;
                if (!operation.ready or operation.committed) return;
                self.triggered = true;
                if (self.original_failure) return error.CompactionPlanningStale;
                // This checkpoint precedes registry acquisition. Model a peer
                // removing the zero-count row under its own admission, leaving
                // our retained observation stale but physically safe to retire.
                var peer = lsm.Backend.prepareRunSnapshotRefOperation("publication-scope-stale.sst", .forget);
                defer peer.deinit();
                _ = try peer.planWithAdmission(quotaControl());
                try peer.resumeWithAdmission(quotaControl());
                try peer.publishWithAdmission(quotaControl());
                try peer.retireWithAdmission(quotaControl());
            }
        };
        var hook: Hook = .{ .anchor = anchor, .original_failure = original_failure };
        const payload: read_admission.Payload = .{ .ptr = &hook, .vtable = &.{ .checkpoint = Hook.check, .check_lifetime = quotaCheck, .wait_for_progress = quotaWait } };
        const original = read_admission.Port.init(&payload);
        try std.testing.expectError(error.CompactionPlanningStale, resources.resource_manager.drainPayloadRetirementWithAdmission(original));
        try std.testing.expect(hook.triggered and !source.released);
        // The failing callback leaves this in-flight anchor registered. There
        // is no concurrent cleanup consumer in this controlled interleaving.
        try std.testing.expect(anchor.forgetting != null);
        try std.testing.expectEqual(!original_failure, anchor.forgetting_stale);
        while (resources.resource_manager.payload_retirement_ready.load(.acquire) != 0) _ = try resources.resource_manager.drainPayloadRetirementWithAdmission(original);
        try std.testing.expect(source.released);
        try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
    }
}
