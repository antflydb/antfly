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
        if (original_failure) {
            try std.testing.expectError(error.CompactionPlanningStale, resources.resource_manager.drainPayloadRetirementWithAdmission(original));
        } else _ = try resources.resource_manager.drainPayloadRetirementWithAdmission(original);
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

const RegisteredSourceFixture = struct {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const Lease = lsm.Backend.PublicationResources.EntryLease;
    const Physical = @typeInfo(@FieldType(lsm.Backend.DirectoryEntryPreparation, "physical")).optional.child;
    const Phase = enum { construct, retire };
    const Policy = struct {
        var live: bool = true;
        var label: []const u8 = "";
        var calls: usize = 0;
        fn legacy(_: []const u8) []const u8 {
            return label;
        }
        fn admitted(_: []const u8, original: read_admission.Port) ![]const u8 {
            try original.checkpoint();
            if (!live) return error.DomainProducerClosed;
            calls += 1;
            return label;
        }
        fn other(_: []const u8) []const u8 {
            return "other";
        }
    };
    const Source = struct {
        allocator: std.mem.Allocator,
        manager: *ResourceManager,
        host: *Resources,
        run: lsm.repository.Run,
        label: ?[]u8,
        physical: ?Physical = null,
        usage: u64 = 0,
        released: bool = false,
        lifetimes: usize = 1,
        fn release(raw: *anyopaque, original: ?read_admission.Port) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (original) |port| try port.checkpoint();
            try std.testing.expect(self.host.quota_lifetimes.load(.acquire) != 0);
            if (self.lifetimes > 1) {
                self.lifetimes -= 1;
                return true;
            }
            if (self.physical == null) self.physical = try Physical.init(self.run);
            try self.physical.?.retireWithAdmission(self.allocator, original);
            if (self.label) |label| {
                if (original) |port| try port.checkpoint();
                self.allocator.free(label); // Fixed small fixture label.
                self.label = null;
            }
            if (original) |port| try self.manager.adjustUsageWithAdmission(.lsm_in_memory_state, &self.usage, 0, port) else try self.manager.adjustUsage(.lsm_in_memory_state, &self.usage, 0);
            self.lifetimes = 0;
            self.released = true;
            return true;
        }
    };
    const Stats = struct { checks: usize, allocations: usize, recovered: bool = false, detached_recovered: bool = false };
    fn drainPair(first: *ResourceManager, second: *ResourceManager) void {
        for (0..2048) |_| {
            if (first.payload_retirement_ready.load(.acquire) == 0 and second.payload_retirement_ready.load(.acquire) == 0) return;
            _ = second.drainPayloadRetirementQuiesced();
            _ = first.drainPayloadRetirementQuiesced();
        }
        @panic("registered source ancestry did not retire");
    }
    const HeaderWitness = struct {
        backing: std.mem.Allocator = std.testing.allocator,
        manager: ?*ResourceManager = null,
        host: ?*Resources = null,
        active: bool = false,
        inside: bool = false,
        observed: bool = false,
        expected_bytes: usize = @sizeOf(Store),
        expected_pointer: ?[*]u8 = null,
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn refuseWait(_: *anyopaque) !void {
            return error.TestUnexpectedWait;
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(raw: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawAlloc(n, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.active and !self.inside and bytes.len == self.expected_bytes and (self.expected_pointer == null or self.expected_pointer.? == bytes.ptr)) {
                self.inside = true;
                defer self.inside = false;
                // The node/account release has completed. Give every ready
                // cleanup peer a chance before the root header is actually free.
                for (0..32) |_| _ = self.manager.?.drainPayloadRetirementQuiesced();
                if (self.host.?.quota_lifetimes.load(.acquire) == 0) @panic("writer root allocator host retired before root header free");
                if (self.expected_bytes == @sizeOf(Scope.DirectoryRoot)) {
                    const directory: *Scope.DirectoryRoot = @ptrCast(@alignCast(bytes.ptr));
                    if (directory.registered_account == null or directory.registered_account.?.refs.load(.acquire) < 2 or directory.registered_owner.?.credit == null) @panic("directory final anchor retired before root header free");
                }
                self.observed = true;
            }
            self.backing.rawFree(bytes, alignment, ra);
        }
    };
    const Scope = lsm.Backend.PublicationResources;
    const ReaderReturnProbe = struct {
        manager: *ResourceManager,
        frame: *Scope.RegisteredReadVersion,
        witness: *HeaderWitness,
        saved: @FieldType(Scope.RegisteredReadVersion, "version").OwnedRetirement,
        checked: bool = false,
        fn finish(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Drive the actual registry in the critical window after the final
            // reference decrement but before the registered return receipt.
            for (0..8) |_| _ = self.manager.drainPayloadRetirementQuiesced();
            if (self.frame.returned.load(.acquire) or self.frame.state == null or self.frame.complete or self.frame.version.references.load(.acquire) != 0 or self.witness.observed) @panic("reader header retired before final return receipt");
            self.checked = true;
            const saved = self.saved;
            saved.retire(saved.context);
            // The epoch is deliberately destroyed before its release callback
            // returns. No caller may touch that epoch after handing it back.
            for (0..128) |_| {
                _ = self.manager.drainPayloadRetirementQuiesced();
                if (self.witness.observed) return;
            }
            @panic("returned reader header did not physically retire");
        }
    };
    const ProjectionCoalescingProbe = struct {
        backend: *lsm.Backend,
        version: *@FieldType(Scope.RegisteredReadVersion, "version"),
        reject_after_progress: bool,
        waits: usize = 0,
        lent: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject_after_progress and self.waits != 0) return error.DeadlineExceeded;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.lent) return error.TestUnexpectedLending;
            self.lent = true;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.lent) return error.TestUnexpectedLending;
            self.lent = false;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // The progress provider owns execution lending; Port.waitForProgress
            // only brackets this callback with original admission checks.
            try begin(raw);
            defer end(raw) catch unreachable;
            if (!self.backend.mu.tryLock()) return error.TestProducerBlockedByWaiter;
            // Deterministically hand the producer claim to an independently
            // admitted preparation while the coalescing peer is suspended.
            self.version.preparing = false;
            const prepared = self.backend.prepareReadVersionWithAdmission(self.version, quotaControl());
            self.backend.mu.unlock();
            try prepared;
            self.waits += 1;
        }
    };
    const GenerationCaptureProbe = struct {
        backend: *lsm.Backend,
        reject: bool = false,
        checks: usize = 0,
        fn checkpoint(_: *anyopaque) !void {
            return error.TestCooperativeCheckpointDuringCapture;
        }
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.backend.mu.tryLock()) {
                self.backend.mu.unlock();
                return error.TestCaptureWithoutBackendMutex;
            }
            if (self.reject) return error.DeadlineExceeded;
        }
    };
    fn qualifyGenerationCapture(a: std.mem.Allocator, manager: *ResourceManager, directory: *const Directory) !void {
        var observer = lsm.Backend.init(a, .{ .resource_manager = manager, .wal_enabled = false });
        defer observer.close();
        // The publication fixture owns this live header. Backend borrows it
        // only while its mutex serializes these capture operations.
        observer.run_directory = @constCast(directory);
        observer.run_directory_dirty = false;
        observer.run_directory_generation = 123;
        defer observer.run_directory = null;
        var first: ?*Scope.RegisteredReadVersion = try Scope.RegisteredReadVersion.prepare(a, manager, quotaControl());
        defer if (first) |owner| owner.requestRetirement();
        var next: ?*Scope.RegisteredReadVersion = try Scope.RegisteredReadVersion.prepare(a, manager, quotaControl());
        defer if (next) |owner| owner.requestRetirement();
        var probe: GenerationCaptureProbe = .{ .backend = &observer };
        const payload: read_admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = GenerationCaptureProbe.checkpoint, .check_lifetime = GenerationCaptureProbe.lifetime } };
        const original = read_admission.Port.init(&payload);
        try std.testing.expect(observer.mu.tryLock());
        defer observer.mu.unlock();
        try std.testing.expectError(error.CompactionPlanningStale, observer.capturePreparedReadVersionWithAdmission(first.?, 122, original));
        observer.run_directory_dirty = true;
        try std.testing.expectError(error.CompactionPlanningStale, observer.capturePreparedReadVersionWithAdmission(first.?, 123, original));
        observer.run_directory_dirty = false;
        probe.reject = true;
        try std.testing.expectError(error.DeadlineExceeded, observer.capturePreparedReadVersionWithAdmission(first.?, 123, original));
        probe.reject = false;
        try std.testing.expect(!first.?.taken and first.?.version.directory == null);
        const epoch = try observer.capturePreparedReadVersionWithAdmission(first.?, 123, original);
        first = null;
        try std.testing.expect(epoch.owned_retirement != null and epoch.registered and observer.live_read_versions == epoch);
        // A peer's prepared frame remains available when the epoch already
        // exists, and can bind to the next generation after invalidation.
        try std.testing.expectEqual(epoch, try observer.capturePreparedReadVersionWithAdmission(next.?, 123, original));
        try std.testing.expect(!next.?.taken and next.?.version.directory == null);
        _ = epoch.references.fetchAdd(1, .monotonic);
        defer epoch.release(&observer);
        observer.invalidateReadVersion();
        try std.testing.expect(observer.read_version == null and epoch.registered);
        try std.testing.expectError(error.CompactionPlanningStale, observer.capturePreparedReadVersionWithAdmission(next.?, 123, original));
        observer.run_directory_dirty = false;
        const successor = try observer.capturePreparedReadVersionWithAdmission(next.?, observer.run_directory_generation, original);
        next = null;
        try std.testing.expect(successor != epoch and successor.live_next == epoch and epoch.live_previous == successor);
        observer.invalidateReadVersion();
        try std.testing.expect(observer.live_read_versions == epoch and epoch.live_previous == null);
        try std.testing.expect(probe.checks >= 8);
    }
    const NormalReaderCaptureProbe = struct {
        backend: *lsm.Backend,
        reject: enum { none, funding, reentry } = .none,
        advanced: bool = false,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.backend.mu.tryLock()) return error.TestReaderCheckpointHeldMutex;
            defer self.backend.mu.unlock();
            if (!self.advanced and self.backend.active_readers >= 2 and self.backend.read_version == null) {
                // A peer publishes while frame funding is off-lock. No query
                // memtable has been captured yet, so all sources must advance.
                if (self.backend.mutable_read_snapshot != null or self.backend.immutable_memtable_pins.items.len != 0) return error.TestReaderCapturedBeforeFunding;
                self.backend.invalidateReadVersion();
                self.backend.run_directory_dirty = false;
                try self.backend.mutable.upsert(self.backend.allocator, .{ .name = "reader-capture" }, "key", "during-funding", false);
                self.advanced = true;
                if (self.reject == .funding) return error.DeadlineExceeded;
            }
        }
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.advanced and self.reject == .reentry) {
                if (self.backend.mu.tryLock()) {
                    self.backend.mu.unlock();
                } else return error.GenerationPublicationChanged;
            }
        }
    };
    fn qualifyNormalReaderCapture(a: std.mem.Allocator, manager: *ResourceManager, directory: *const Directory) !void {
        var source = try directory.at(0).retainRegisteredSourceWithAdmission(quotaControl());
        defer source.releaseWithAdmission(null) catch unreachable;
        const record = try Scope.RegisteredWriter.start(a, manager, &source.lease.?, .{}, quotaControl());
        defer record.requestRetirement();
        var writer_run = try record.takeRunWithAdmission(quotaControl());
        defer writer_run.deinit(a);
        const empty: Scope.WriterStore = .{};
        const edit = try Scope.RegisteredWriterRoot.start(a, manager, &empty, &writer_run, false, quotaControl());
        defer edit.requestRetirement();
        try edit.resumeWithAdmission(quotaControl());
        var root: ?*Store = try edit.takeWithAdmission(quotaControl());
        var cleanup: ?Store.Reclaimer = null;
        defer finishRoot(&root, &cleanup, a);
        const Mode = enum { namespace, bound, probe, scan, reject_funding, reject_reentry };
        for (std.enums.values(Mode)) |mode| {
            var observer = lsm.Backend.init(a, .{ .resource_manager = manager, .wal_enabled = false });
            defer observer.close();
            observer.run_directory = @constCast(directory);
            observer.run_directory_dirty = false;
            observer.runs = root.?.*;
            defer {
                observer.run_directory = null;
                observer.runs = .{};
            }
            var probe: NormalReaderCaptureProbe = .{ .backend = &observer, .reject = switch (mode) {
                .reject_funding => .funding,
                .reject_reentry => .reentry,
                else => .none,
            } };
            const payload: read_admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = NormalReaderCaptureProbe.checkpoint, .check_lifetime = NormalReaderCaptureProbe.lifetime } };
            const original = read_admission.Port.init(&payload);
            if (mode == .reject_funding or mode == .reject_reentry) {
                const opened = observer.beginReadWithAdmission(original);
                if (mode == .reject_funding) try std.testing.expectError(error.DeadlineExceeded, opened) else try std.testing.expectError(error.GenerationPublicationChanged, opened);
                try std.testing.expect(probe.advanced and observer.read_version == null and observer.live_read_versions == null and observer.active_readers == 0);
                try std.testing.expect(observer.mu.tryLock());
                observer.mu.unlock();
                continue;
            }
            if (mode == .namespace) {
                var read = try observer.beginReadWithAdmission(original);
                defer read.abort();
                try std.testing.expectEqualStrings("during-funding", try read.get(.{ .name = "reader-capture" }, "key"));
                // Retained snapshots remain stable after a subsequent write.
                try std.testing.expect(observer.mu.tryLock());
                observer.invalidateMutableReadSnapshot();
                const changed = observer.mutable.upsert(a, .{ .name = "reader-capture" }, "key", "after-capture", false);
                observer.mu.unlock();
                try changed;
                try std.testing.expectEqualStrings("during-funding", try read.get(.{ .name = "reader-capture" }, "key"));
            } else {
                var store = try observer.runtimeStore(a, .{ .name = "reader-capture" });
                defer store.deinit();
                switch (mode) {
                    .bound => {
                        var read = try store.beginReadWithAdmission(.retain, original);
                        defer read.abort();
                        try std.testing.expectEqualStrings("during-funding", try read.get("key"));
                    },
                    .probe => {
                        var read = try store.beginProbeWithAdmission(.retain, original);
                        defer read.abort();
                        try std.testing.expectEqualStrings("during-funding", try read.get("key"));
                    },
                    .scan => {
                        var read = try store.beginCurrentScanWithAdmission(original);
                        defer read.abort();
                    },
                    else => unreachable,
                }
            }
            try std.testing.expect(probe.advanced and observer.read_version != null and observer.read_version.?.owned_retirement != null and observer.active_readers == 0);
            try std.testing.expectEqual(@as(u64, 1), observer.run_directory_generation);
        }
    }
    fn qualifyProjectionCoalescing(a: std.mem.Allocator, manager: *ResourceManager, directory: *const Directory) !void {
        try qualifyGenerationCapture(a, manager, directory);
        for ([_]bool{ false, true }) |reject_after_progress| {
            var observer = lsm.Backend.init(a, .{ .resource_manager = manager, .wal_enabled = false });
            defer observer.close();
            var creator: ?*Scope.RegisteredReadVersion = try Scope.RegisteredReadVersion.start(a, manager, directory, quotaControl());
            defer if (creator) |owner| owner.requestRetirement();
            const epoch = try creator.?.takeWithAdmission(quotaControl());
            creator = null;
            defer epoch.releaseOwned();
            epoch.preparing = true;
            try std.testing.expect(observer.mu.tryLock());
            const refused = observer.prepareReadVersionWithAdmission(epoch, quotaControl());
            observer.mu.unlock();
            try std.testing.expectError(error.ExecutionWaitUnsupported, refused);
            try std.testing.expect(epoch.preparing and !epoch.prepared);
            var probe: ProjectionCoalescingProbe = .{ .backend = &observer, .version = epoch, .reject_after_progress = reject_after_progress };
            const payload: read_admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = ProjectionCoalescingProbe.check, .check_lifetime = ProjectionCoalescingProbe.check, .wait_for_progress = ProjectionCoalescingProbe.wait }, .execution_wait = .{ .ptr = &probe, .begin = ProjectionCoalescingProbe.begin, .end = ProjectionCoalescingProbe.end } };
            try std.testing.expect(observer.mu.tryLock());
            const coalesced = observer.prepareReadVersionWithAdmission(epoch, read_admission.Port.init(&payload));
            observer.mu.unlock();
            if (reject_after_progress) try std.testing.expectError(error.DeadlineExceeded, coalesced) else try coalesced;
            try std.testing.expect(epoch.prepared and !epoch.preparing and !probe.lent);
            try std.testing.expectEqual(@as(usize, 1), probe.waits);
            try std.testing.expectEqual(@as(u64, 1), observer.read_version_builds);
            try epoch.qualifyProjectionForTest(a);
        }
    }
    const Store = Scope.WriterStore;
    fn finishRoot(root: *?*Store, cursor: *?Store.Reclaimer, a: std.mem.Allocator) void {
        if (root.*) |store| {
            if (cursor.* == null) {
                cursor.* = .init(store);
                store.tree.root = null;
            }
            while (true) {
                var steps: usize = 1;
                if (cursor.*.?.step(a, &steps)) break;
            }
            cursor.*.?.finish(a);
            cursor.* = null;
            root.* = null;
        }
    }
    fn run(shared: bool, phase: Phase, target: ?usize, oom: bool) !Stats {
        return runMode(shared, phase, target, oom, 0);
    }
    fn runMode(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize) !Stats {
        return runExtended(shared, phase, target, oom, writer_count, false);
    }
    fn finishLedger(root: *?Scope.ObsoleteLedger, cursor: *?Scope.ObsoleteLedger.Reclaimer, a: std.mem.Allocator) void {
        if (root.*) |ledger| {
            if (cursor.* == null) cursor.* = .init(ledger);
            while (true) {
                var steps: usize = 1;
                if (cursor.*.?.step(a, &steps)) break;
            }
            cursor.* = null;
            root.* = null;
        }
    }
    fn runExtended(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize, exercise_obsolete: bool) !Stats {
        return runFull(shared, phase, target, oom, writer_count, exercise_obsolete, false);
    }
    const Directory = Scope.DirectoryRoot;
    fn finishDirectory(root: *?*Directory, cursor: *?Directory.Reclaimer, a: std.mem.Allocator) void {
        if (root.*) |directory| {
            if (cursor.* == null) cursor.* = .init(directory);
            while (true) {
                var steps: usize = 1;
                if (cursor.*.?.step(a, &steps)) break;
            }
            cursor.*.?.finish(a);
            cursor.* = null;
            root.* = null;
        }
    }
    fn runFull(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize, exercise_obsolete: bool, exercise_directory: bool) !Stats {
        return runScenario(shared, phase, target, oom, writer_count, exercise_obsolete, exercise_directory, false, false);
    }
    fn runScenario(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize, exercise_obsolete: bool, exercise_directory: bool, exercise_remove: bool, retry_edit: bool) !Stats {
        return runAll(shared, phase, target, oom, writer_count, exercise_obsolete, exercise_directory, exercise_remove, retry_edit, false);
    }
    fn runAll(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize, exercise_obsolete: bool, exercise_directory: bool, exercise_remove: bool, retry_edit: bool, publication: bool) !Stats {
        return runProjectionScenario(shared, phase, target, oom, writer_count, exercise_obsolete, exercise_directory, exercise_remove, retry_edit, publication, false);
    }
    fn runProjectionScenario(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize, exercise_obsolete: bool, exercise_directory: bool, exercise_remove: bool, retry_edit: bool, publication: bool, projection_shape: bool) !Stats {
        return runPublicationScenario(shared, phase, target, oom, writer_count, exercise_obsolete, exercise_directory, exercise_remove, retry_edit, publication, projection_shape, false);
    }
    const PublicationCaptureProbe = struct {
        backend: *lsm.Backend,
        original: read_admission.Port,
        fn checkpoint(_: *anyopaque) !void {
            return error.TestPublicationCaptureCheckpointUnderMutex;
        }
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.backend.mu.tryLock()) {
                self.backend.mu.unlock();
                return error.TestPublicationCaptureWithoutMutex;
            }
            try self.original.checkLifetime();
        }
    };
    fn installPublication(backend: *lsm.Backend, frame: *Scope.Publication.Candidate, original: read_admission.Port) !void {
        // An independently admitted normal reader pins the old generation
        // across the actual Backend installation, then releases before close.
        var read = try backend.beginReadWithAdmission(quotaControl());
        defer read.abort();
        const pinned = read.read_view.version;
        const pinned_count = if (pinned) |version| version.directory.?.count() else 0;
        try std.testing.expect(backend.mu.tryLock());
        defer backend.mu.unlock();
        const old_seal = backend.registered_publication.seal;
        const old_generation = backend.run_directory_generation;
        const saved_debt = backend.unpublished_wal_logical_bytes;
        backend.unpublished_wal_logical_bytes = 321;
        defer backend.unpublished_wal_logical_bytes = saved_debt;
        var guard: PublicationCaptureProbe = .{ .backend = backend, .original = quotaControl() };
        const payload: read_admission.Payload = .{ .ptr = &guard, .vtable = &.{ .checkpoint = PublicationCaptureProbe.checkpoint, .check_lifetime = PublicationCaptureProbe.lifetime } };
        const checked = read_admission.Port.init(&payload);
        const read_only = backend.options.backend.read_only;
        const closing = backend.closing.load(.acquire);
        defer backend.options.backend.read_only = read_only;
        defer backend.closing.store(closing, .release);
        backend.options.backend.read_only = true;
        try std.testing.expectError(error.ReadOnly, backend.installRegisteredPublicationWithAdmission(frame, checked));
        backend.options.backend.read_only = read_only;
        backend.closing.store(true, .release);
        try std.testing.expectError(error.BackendClosing, backend.installRegisteredPublicationWithAdmission(frame, checked));
        backend.closing.store(closing, .release);
        if (backend.registered_publication.seal == null) {
            if (backend.run_directory) |legacy| if (legacy.tree.account) |account| {
                _ = account.retain();
                defer account.release();
                try std.testing.expectError(error.CompactionPlanningStale, backend.installRegisteredPublicationWithAdmission(frame, checked));
            };
        }
        guard.original = original;
        backend.installRegisteredPublicationWithAdmission(frame, checked) catch |cause| {
            try std.testing.expect(backend.registered_publication.seal == old_seal and backend.run_directory_generation == old_generation);
            if (!Scope.Publication.matchesBackend(&backend.registered_publication, backend)) {
                const owned = &backend.registered_publication;
                std.debug.print("install source mismatch: gen={d} has_owned={any} dir_match={any} actual_dir={any} writer_root_match={any} writer_account_match={any} writer_owner_match={any} ledger_root_match={any} ledger_account_match={any} ledger_owner_match={any}\n", .{ backend.run_directory_generation, owned.seal != null, backend.run_directory == owned.directory, backend.run_directory != null, if (owned.writer) |writer| backend.runs.tree.root == writer.tree.root else backend.runs.tree.root == null, if (owned.writer) |writer| backend.runs.tree.account == writer.tree.account else backend.runs.tree.account == null, if (owned.writer) |writer| backend.runs.registered_owner == writer.registered_owner else backend.runs.registered_owner == null, backend.obsolete_paths.tree.root == owned.obsolete.tree.root, backend.obsolete_paths.tree.account == owned.obsolete.tree.account, backend.obsolete_paths.registered_owner == owned.obsolete.registered_owner });
            }
            try std.testing.expect(Scope.Publication.matchesBackend(&backend.registered_publication, backend));
            return cause;
        };
        try std.testing.expect(backend.registered_publication.seal == frame and backend.run_directory_generation == old_generation + 1);
        try std.testing.expect(Scope.Publication.matchesBackend(&backend.registered_publication, backend));
        try std.testing.expect(backend.manifest_dirty and backend.obsolete_manifest_dirty and !backend.run_directory_dirty);
        try std.testing.expectEqual(@as(u64, 321), backend.unpublished_wal_logical_bytes);
        try std.testing.expect(backend.retired_run_directories == null and backend.retired_run_stores == null);
        try std.testing.expect(backend.read_version == null);
        if (pinned) |version| {
            try std.testing.expect(version.registered);
            try std.testing.expectEqual(pinned_count, version.directory.?.count());
        }
    }
    fn capturePublication(backend: *lsm.Backend, frame: *Scope.Publication.Candidate, roots: *const Scope.Publication.Roots, generation: u64, original: read_admission.Port) !void {
        // The publication owner exclusively owns these headers. Binding is a
        // test borrow under the actual Backend mutex, never an ownership copy.
        const saved = .{ backend.run_directory, backend.runs, backend.obsolete_paths, backend.run_directory_generation, backend.run_directory_dirty };
        try std.testing.expect(backend.mu.tryLock());
        defer backend.mu.unlock();
        defer {
            backend.run_directory = saved[0];
            backend.runs = saved[1];
            backend.obsolete_paths = saved[2];
            backend.run_directory_generation = saved[3];
            backend.run_directory_dirty = saved[4];
        }
        backend.run_directory = roots.directory;
        backend.runs = if (roots.writer) |writer| writer.* else .{};
        backend.obsolete_paths = roots.obsolete;
        backend.run_directory_generation = generation;
        backend.run_directory_dirty = roots.seal == null;
        var guard: PublicationCaptureProbe = .{ .backend = backend, .original = quotaControl() };
        const guard_payload: read_admission.Payload = .{ .ptr = &guard, .vtable = &.{ .checkpoint = PublicationCaptureProbe.checkpoint, .check_lifetime = PublicationCaptureProbe.lifetime } };
        const checked = read_admission.Port.init(&guard_payload);
        try std.testing.expectError(error.InvalidArgument, backend.validateCapturedPublicationWithAdmission(frame, roots, checked));
        try std.testing.expectError(error.CompactionPlanningStale, backend.capturePreparedPublicationWithAdmission(frame, roots, generation + 1, checked));
        if (roots.seal != null) {
            backend.run_directory_dirty = true;
            try std.testing.expectError(error.CompactionPlanningStale, backend.capturePreparedPublicationWithAdmission(frame, roots, generation, checked));
            backend.run_directory_dirty = false;
            const account = backend.runs.tree.account;
            backend.runs.tree.account = null;
            try std.testing.expectError(error.CompactionPlanningStale, backend.capturePreparedPublicationWithAdmission(frame, roots, generation, checked));
            backend.runs.tree.account = account;
        }
        try std.testing.expect(!frame.source_captured and frame.source_generation == null);
        guard.original = original;
        try backend.capturePreparedPublicationWithAdmission(frame, roots, generation, checked);
        guard.original = quotaControl();
        try std.testing.expect(frame.source_captured and frame.source_generation.? == generation);
        try backend.validateCapturedPublicationWithAdmission(frame, roots, checked);
        try std.testing.expectError(error.InvalidArgument, backend.capturePreparedPublicationWithAdmission(frame, roots, generation, checked));
        backend.run_directory_generation += 1;
        try std.testing.expectError(error.CompactionPlanningStale, backend.validateCapturedPublicationWithAdmission(frame, roots, checked));
        backend.run_directory_generation = generation;
        try backend.validateCapturedPublicationWithAdmission(frame, roots, checked);
    }
    fn runBatch(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize) !Stats {
        return runPublicationScenario(shared, phase, target, oom, writer_count, false, false, false, false, true, false, true);
    }
    fn runBatchRetry(shared: bool, target: ?usize) !Stats {
        return runPublicationScenario(shared, .construct, target, false, 1, false, false, false, true, true, false, true);
    }
    fn resumePublication(candidate: *Scope.Publication.Candidate, original: read_admission.Port, fault: *PublicationResourcesFixture.Fault, retry: bool, recovered: *bool, displaced: *bool) !void {
        candidate.resumeWithAdmission(original) catch |cause| {
            if (!retry or cause != fault.failure) return cause;
            const pending = &candidate.state.?.displaced;
            displaced.* = displaced.* or pending.directory != null or pending.writer != null or pending.obsolete.tree.account != null;
            recovered.* = true;
            fault.live = false;
            // A fresh admitted attempt resumes the same partial candidate.
            try candidate.resumeWithAdmission(quotaControl());
        };
    }
    fn runPublicationScenario(shared: bool, phase: Phase, target: ?usize, oom: bool, writer_count: usize, exercise_obsolete: bool, exercise_directory: bool, exercise_remove: bool, retry_edit: bool, publication: bool, projection_shape: bool, batch: bool) !Stats {
        // Exhaustive replay keeps all allocator safety and leak checks. Trace
        // collection adds no coverage and dominates Debug allocation replay.
        var checked = std.heap.SafeAllocator.init(std.heap.page_allocator, .{ .stack_trace_frames = 0, .check_write_after_free = true });
        defer std.testing.expectEqual(@as(usize, 0), checked.deinit()) catch @panic("registered source fixture leaked");
        const a = checked.allocator();
        var allocator1 = std.testing.FailingAllocator.init(a, .{});
        var header_witness: HeaderWitness = .{ .backing = a };
        var allocator2 = std.testing.FailingAllocator.init(header_witness.allocator(), .{});
        const a1 = allocator1.allocator();
        const a2 = allocator2.allocator();
        var parent1 = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent1.deinit(a);
        var parent2 = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent2.deinit(a);
        var budgets1 = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets1.options.identity_allocator = a1;
        var budgets2 = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets2.options.identity_allocator = a2;
        var first = try Resources.initWithBudgetsAndParent(a1, budgets1, parent1.memoryParentPort());
        defer first.deinit();
        first.attachResourceManager();
        var second = try Resources.initWithBudgetsAndParent(a2, budgets2, parent2.memoryParentPort());
        defer second.deinit();
        second.attachResourceManager();
        header_witness.manager = &second.resource_manager;
        header_witness.host = &second;
        var backend1 = lsm.Backend.init(a1, .{ .resource_manager = &first.resource_manager, .wal_enabled = false, .run_partition_key = Policy.legacy, .run_partition_key_with_admission = Policy.admitted });
        var backend1_live = true;
        defer if (backend1_live) backend1.close();
        var backend2 = lsm.Backend.init(a2, .{ .resource_manager = &second.resource_manager, .wal_enabled = false, .run_partition_key = Policy.legacy, .run_partition_key_with_admission = Policy.admitted });
        var backend2_live = true;
        defer if (backend2_live) backend2.close();
        var scope1: ?*lsm.Backend.PublicationResources = null;
        var scope2: ?*lsm.Backend.PublicationResources = null;
        var lease1: ?Lease = null;
        var lease2: ?Lease = null;
        var extra_sources: [8]?Lease = @splat(null);
        const route_count = @max(writer_count, 1);
        var writer: ?*Scope.RegisteredWriter = null;
        var writer_view: ?lsm.repository.Run = null;
        var candidate: ?*Scope.RegisteredWriterRoot = null;
        var root: ?*Store = null;
        var root_cursor: ?Store.Reclaimer = null;
        var snapshot: ?*Store = null;
        var snapshot_cursor: ?Store.Reclaimer = null;
        var removal_snapshot: ?*Store = null;
        var removal_snapshot_cursor: ?Store.Reclaimer = null;
        var stale_view: ?lsm.repository.Run = null;
        var removed_view: ?lsm.repository.Run = null;
        var recovered_edit = false;
        var recovered_detached = false;
        var obsolete_live: ?Scope.ObsoleteLedger = null;
        var obsolete_snapshot: ?Scope.ObsoleteLedger = null;
        var obsolete_cursor: ?Scope.ObsoleteLedger.Reclaimer = null;
        var obsolete_old_cursor: ?Scope.ObsoleteLedger.Reclaimer = null;
        var obsolete_candidate: ?*Scope.RegisteredObsolete = null;
        var directory_live: ?*Directory = null;
        var directory_snapshot: ?*Directory = null;
        var directory_cursor: ?Directory.Reclaimer = null;
        var directory_old_cursor: ?Directory.Reclaimer = null;
        var directory_candidate: ?*Scope.RegisteredDirectory = null;
        var private_working: Scope.Publication.Roots = .{};
        const working: *Scope.Publication.Roots = if (batch) &backend2.registered_publication else &private_working;
        var working_candidate: ?*Scope.Publication.Candidate = null;
        var sibling: ?*Scope.Publication.Candidate = null;
        var reader_owner: ?*Scope.RegisteredReadVersion = null;
        const Version = @FieldType(Scope.RegisteredReadVersion, "version");
        var readers: [8]?*Version = @splat(null);
        var reader_frames: [8]?*Scope.RegisteredReadVersion = @splat(null);
        var backend_reader: ?*Version = null;
        var final_return_probe: ?ReaderReturnProbe = null;
        defer {
            header_witness.active = false;
            if (batch) {
                if (backend2_live) {
                    backend2.close();
                    backend2_live = false;
                }
            } else working.retire();
            for (&readers) |*slot| if (slot.*) |version| {
                version.releaseOwned();
                slot.* = null;
            };
            if (backend_reader) |version| version.releaseOwned();
            if (reader_owner) |owner| owner.requestRetirement();
            if (working_candidate) |owner| owner.requestRetirement();
            if (sibling) |owner| owner.requestRetirement();
            finishDirectory(&directory_live, &directory_cursor, a2);
            finishDirectory(&directory_snapshot, &directory_old_cursor, a2);
            if (directory_candidate) |owner| owner.requestRetirement();
            finishLedger(&obsolete_live, &obsolete_cursor, a2);
            finishLedger(&obsolete_snapshot, &obsolete_old_cursor, a2);
            if (obsolete_candidate) |owner| owner.requestRetirement();
            finishRoot(&root, &root_cursor, a2);
            finishRoot(&snapshot, &snapshot_cursor, a2);
            finishRoot(&removal_snapshot, &removal_snapshot_cursor, a2);
            if (stale_view) |*view| view.deinit(a2);
            if (removed_view) |*view| view.deinit(a2);
            if (writer_view) |*view| view.deinit(a2);
            if (candidate) |owner| owner.requestRetirement();
            if (writer) |owner| owner.requestRetirement();
            for (&extra_sources) |*slot| if (slot.*) |lease| {
                var owned = lease;
                owned.deinit(a1);
                slot.* = null;
            };
            if (lease2) |*lease| lease.deinit(a2);
            if (lease1) |*lease| lease.deinit(a1);
            if (scope2) |owner| owner.requestRetirement();
            if (scope1) |owner| owner.requestRetirement();
            drainPair(&first.resource_manager, &second.resource_manager);
            Policy.live = true;
            Policy.label = "";
            header_witness.active = false;
        }
        scope1 = try backend1.beginPublicationResourcesWithAdmission(quotaControl());
        try scope1.?.resumeWithAdmission(quotaControl());
        scope2 = try backend2.beginPublicationResourcesWithAdmission(quotaControl());
        try scope2.?.resumeWithAdmission(quotaControl());
        try std.testing.expect(backend1.mu.tryLock());
        const planned1 = backend1.planningDirectory();
        backend1.mu.unlock();
        const directory1 = try (try planned1).fork(a1);
        defer directory1.destroy(a1);
        try directory1.tree.prepare(a1);
        try std.testing.expect(backend2.mu.tryLock());
        const planned2 = backend2.planningDirectory();
        backend2.mu.unlock();
        const directory2 = try (try planned2).fork(a2);
        defer directory2.destroy(a2);
        try directory2.tree.prepare(a2);
        if (batch) {
            const AccountPointer = @typeInfo(@TypeOf(backend2.run_directory.?.tree.account)).optional.child;
            const EmptyAccount = @typeInfo(AccountPointer).pointer.child;
            backend2.run_directory.?.tree.account = try EmptyAccount.create(a2);
        }
        var source: Source = .{ .allocator = a1, .manager = &first.resource_manager, .host = &first, .label = null, .run = undefined };
        try first.resource_manager.adjustUsageWithAdmission(.lsm_in_memory_state, &source.usage, 256 << 10, quotaControl());
        source.label = try a1.dupe(u8, "ancestor-domain");
        const key = try a1.alloc(u8, 32775);
        @memset(key, 'k');
        source.run = .{ .id = 911, .level = if (writer_count == 0) 1 else 0, .size_bytes = 1, .path = if (shared) null else try a1.dupe(u8, "registered-source-ancestor.sst"), .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = null, .largest_key = try a1.dupe(u8, "z"), .entry_count = 1, .bloom_filter = null, .state = if (shared) .{} else null };
        if (exercise_obsolete) {
            a1.free(source.run.path.?);
            const path = try a1.alloc(u8, 32779);
            @memset(path, 'p');
            path[path.len - 1] = '0';
            source.run.path = path;
        }
        if (shared) {
            try source.run.state.?.appendUpsert(a1, .{ .name = "docs" }, key, key, false);
            try source.run.shareMemory(a1);
        }
        Policy.live = true;
        Policy.label = source.label.?;
        Policy.calls = 0;
        var lifetime: ?lsm.Backend.DirectoryEntryLifetime = .{ .context = &source, .release = Source.release };
        try scope1.?.beginEntryWithAdmission(&backend1, directory1, source.run, &lifetime, quotaControl());
        const resident1 = blk: {
            lease1 = try scope1.?.takeEntryWithAdmission(quotaControl());
            for (1..route_count) |ordinal| {
                var seed = source.run;
                seed.id += ordinal;
                // Metadata oracle shapes: transitive and disjoint L0 ranges,
                // distinct null/empty namespaces, and two lower levels.
                const shape_small = if (projection_shape) try a1.dupe(u8, ([_][]const u8{ "k", "a", "b", "f", "a", "a", "a", "d" })[ordinal % 8]) else null;
                defer if (shape_small) |bytes| a1.free(bytes);
                const shape_large = if (projection_shape) try a1.dupe(u8, ([_][]const u8{ "z", "c", "e", "g", "c", "b", "e", "f" })[ordinal % 8]) else null;
                defer if (shape_large) |bytes| a1.free(bytes);
                const shape_namespace = if (projection_shape and ordinal % 8 == 4) try a1.dupe(u8, "") else if (projection_shape and ordinal % 8 == 6) try a1.dupe(u8, "docs") else null;
                defer if (shape_namespace) |bytes| a1.free(bytes);
                if (projection_shape) {
                    seed.smallest_key = shape_small.?;
                    seed.largest_key = shape_large.?;
                    seed.smallest_namespace_name = shape_namespace;
                    seed.largest_namespace_name = shape_namespace;
                    seed.level = switch (ordinal % 8) {
                        5 => 1,
                        6 => 2,
                        else => 0,
                    };
                }
                const seed_path = if (exercise_obsolete) try a1.dupe(u8, source.run.path.?) else null;
                defer if (seed_path) |path| a1.free(path);
                if (seed_path) |path| {
                    path[path.len - 1] += @intCast(ordinal);
                    seed.path = path;
                }
                var held: ?lsm.Backend.DirectoryEntryLifetime = .{ .context = &source, .release = Source.release };
                source.lifetimes += 1;
                errdefer if (held != null) {
                    source.lifetimes -= 1;
                };
                try scope1.?.beginEntryWithAdmission(&backend1, directory1, seed, &held, quotaControl());
                extra_sources[ordinal] = try scope1.?.takeEntryWithAdmission(quotaControl());
            }
            break :blk first.resource_manager.sliceStats(.lsm_in_memory_state).used_bytes - source.usage;
        };
        try std.testing.expect(lifetime == null and Policy.calls == route_count);
        backend1.close();
        backend1_live = false;
        scope1.?.requestRetirement();
        scope1 = null;
        Policy.live = false; // Calling the old producer's routing now fails.
        const refs = lease1.?.entry.payload.?.refs.load(.acquire);
        var forged = lease1.?;
        forged.entry.domain = "forged";
        try std.testing.expectError(error.InvalidArgument, scope2.?.beginEntryFromRegisteredSourceWithAdmission(&backend2, directory2, &forged, quotaControl()));
        backend2.options.run_partition_key = Policy.other;
        try std.testing.expectError(error.CompactionPlanningStale, scope2.?.beginEntryFromRegisteredSourceWithAdmission(&backend2, directory2, &lease1.?, quotaControl()));
        backend2.options.run_partition_key = Policy.legacy;
        try std.testing.expectEqual(refs, lease1.?.entry.payload.?.refs.load(.acquire));
        var fault: PublicationResourcesFixture.Fault = .{ .at = if (oom) null else target, .failure = switch ((target orelse 0) % 4) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            2 => error.FileNotFound,
            else => error.ParentMemoryBusy,
        }, .live = phase == .construct };
        const payload: read_admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = PublicationResourcesFixture.Fault.check, .check_lifetime = PublicationResourcesFixture.Fault.check, .wait_for_progress = PublicationResourcesFixture.Fault.wait } };
        const original = read_admission.Port.init(&payload);
        const allocation_start = allocator2.alloc_index;
        if (oom) allocator2.fail_index = allocation_start + target.?;
        const result: anyerror!void = blk: {
            if (shared) {
                // The owned lease pins the same immutable payload as a directory.
                const SourceDirectory = @TypeOf(directory1.*);
                const handle: SourceDirectory.Handle = .{ .run = lease1.?.entry.run, .revision = lease1.?.entry.payload.? };
                scope2.?.beginEntryFromRegisteredHandleWithAdmission(&backend2, directory2, handle, original) catch |cause| break :blk cause;
            } else {
                scope2.?.beginEntryFromRegisteredSourceWithAdmission(&backend2, directory2, &lease1.?, original) catch |cause| break :blk cause;
            }
            lease2 = scope2.?.takeEntryWithAdmission(original) catch |cause| break :blk cause;
            try std.testing.expect(lease2.?.entry.domain.ptr == source.label.?.ptr);
            if (publication) {
                for (0..writer_count) |ordinal| {
                    const input = if (ordinal == 0) &lease2.? else &extra_sources[ordinal].?;
                    writer = scope2.?.beginRegisteredWriterWithAdmission(&backend2, input, .{}, original) catch |cause| break :blk cause;
                    writer_view = writer.?.takeRunWithAdmission(original) catch |cause| break :blk cause;
                    writer.?.requestRetirement();
                    writer = null;
                    if (batch) {
                        working_candidate = scope2.?.preparePublicationWithAdmission(original) catch |cause| break :blk cause;
                        try std.testing.expectError(error.InvalidArgument, working_candidate.?.resumeWithAdmission(quotaControl()));
                        try std.testing.expectError(error.InvalidArgument, working_candidate.?.appendPutWithAdmission(input, &writer_view.?, quotaControl()));
                        capturePublication(&backend2, working_candidate.?, working, backend2.run_directory_generation, original) catch |cause| break :blk cause;
                        try std.testing.expectError(error.InvalidArgument, working_candidate.?.resumeWithAdmission(quotaControl()));
                        working_candidate.?.appendPutWithAdmission(input, &writer_view.?, original) catch |cause| break :blk cause;
                    } else working_candidate = scope2.?.beginPublicationPutWithAdmission(working, input, &writer_view.?, original) catch |cause| break :blk cause;
                    resumePublication(working_candidate.?, original, &fault, batch and retry_edit, &recovered_edit, &recovered_detached) catch |cause| break :blk cause;
                    if (batch) {
                        const prepared = working_candidate.?.preparedRootsWithAdmission(original) catch |cause| break :blk cause;
                        var incoming: ?Directory.Handle = null;
                        for (0..prepared.directory.?.count()) |rank| {
                            const handle = prepared.directory.?.at(rank);
                            if (handle.run.id == writer_view.?.id) incoming = handle;
                        }
                        try std.testing.expect(incoming != null);
                        working_candidate.?.appendRemoveWithAdmission(incoming.?, &writer_view.?, 1, original) catch |cause| break :blk cause;
                        resumePublication(working_candidate.?, original, &fault, batch and retry_edit, &recovered_edit, &recovered_detached) catch |cause| break :blk cause;
                        const removed = working_candidate.?.preparedRootsWithAdmission(original) catch |cause| break :blk cause;
                        try std.testing.expectEqual(ordinal, removed.directory.?.count());
                        try std.testing.expectEqual(ordinal, removed.writer.?.count());
                        working_candidate.?.appendPutWithAdmission(input, &writer_view.?, original) catch |cause| break :blk cause;
                        resumePublication(working_candidate.?, original, &fault, batch and retry_edit, &recovered_edit, &recovered_detached) catch |cause| break :blk cause;
                        if (ordinal != 0) {
                            // A real compaction also edits inputs that predate
                            // the new outputs, using the same private tuple.
                            const previous = working.writer.?.at(0);
                            var prior: *Scope.EntryLease = &lease2.?;
                            for (extra_sources[1..ordinal]) |*slot| if (slot.*) |*lease| {
                                if (lease.entry.run.id == previous.id) prior = lease;
                            };
                            const roots = working_candidate.?.preparedRootsWithAdmission(original) catch |cause| break :blk cause;
                            var old_handle: ?Directory.Handle = null;
                            for (0..roots.directory.?.count()) |rank| {
                                const handle = roots.directory.?.at(rank);
                                if (handle.run.id == previous.id) old_handle = handle;
                            }
                            try std.testing.expect(old_handle != null);
                            working_candidate.?.appendRemoveWithAdmission(old_handle.?, previous, 2, original) catch |cause| break :blk cause;
                            resumePublication(working_candidate.?, original, &fault, retry_edit, &recovered_edit, &recovered_detached) catch |cause| break :blk cause;
                            working_candidate.?.appendPutWithAdmission(prior, previous, original) catch |cause| break :blk cause;
                            resumePublication(working_candidate.?, original, &fault, retry_edit, &recovered_edit, &recovered_detached) catch |cause| break :blk cause;
                        }
                        // No intermediate tuple escaped to the live generation.
                        try std.testing.expectEqual(ordinal, if (working.directory) |live| live.count() else 0);
                        try std.testing.expectEqual(ordinal, if (working.writer) |live| live.count() else 0);
                    }
                    // A sibling shares the captured generation, then loses the
                    // publication race without changing any of the live roots.
                    if (batch) {
                        sibling = scope2.?.preparePublicationWithAdmission(original) catch |cause| break :blk cause;
                        capturePublication(&backend2, sibling.?, working, backend2.run_directory_generation, original) catch |cause| break :blk cause;
                        sibling.?.appendPutWithAdmission(input, &writer_view.?, original) catch |cause| break :blk cause;
                    } else sibling = scope2.?.beginPublicationPutWithAdmission(working, input, &writer_view.?, original) catch |cause| break :blk cause;
                    sibling.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    if (batch) installPublication(&backend2, working_candidate.?, original) catch |cause| break :blk cause else working_candidate.?.publishWithAdmission(working, original) catch |cause| break :blk cause;
                    working_candidate.?.requestRetirement();
                    working_candidate = null;
                    const stale = if (batch) installPublication(&backend2, sibling.?, original) else sibling.?.publishWithAdmission(working, original);
                    if (stale) |_| {
                        break :blk error.TestExpectedStalePublication;
                    } else |cause| if (cause != error.CompactionPlanningStale) break :blk cause;
                    sibling.?.requestRetirement();
                    sibling = null;
                    try std.testing.expectEqual(ordinal + 1, working.directory.?.count());
                    try std.testing.expectEqual(ordinal + 1, working.writer.?.count());
                    try std.testing.expectEqual(@as(usize, if (batch and !shared) 1 else 0), working.obsolete.count());
                    reader_owner = scope2.?.beginReadVersionWithAdmission(working.directory.?, original) catch |cause| break :blk cause;
                    readers[ordinal] = reader_owner.?.takeWithAdmission(original) catch |cause| break :blk cause;
                    reader_frames[ordinal] = reader_owner;
                    reader_owner = null; // Final release owns the retirement handoff.
                    try std.testing.expect(!reader_frames[ordinal].?.registration.ready.load(.acquire));
                    const epoch = readers[ordinal].?;
                    const peer = epoch.retainOwnedWithAdmission(original) catch |cause| break :blk cause;
                    peer.releaseOwned();
                    try std.testing.expectEqual(ordinal + 1, epoch.directory.?.count());
                    try std.testing.expectEqual(@as(u64, 0), epoch.accountedMemoryBytes(230));
                    try std.testing.expect(backend2.mu.tryLock());
                    const refused_projection = backend2.prepareReadVersion(epoch);
                    backend2.mu.unlock();
                    try std.testing.expectError(error.ExecutionWaitUnsupported, refused_projection);
                    try std.testing.expect(backend2.mu.tryLock());
                    const prepared_projection = backend2.prepareReadVersionWithAdmission(epoch, original);
                    backend2.mu.unlock();
                    prepared_projection catch |cause| {
                        if (!retry_edit or cause != fault.failure) break :blk cause;
                        recovered_edit = true;
                        fault.live = false;
                        try std.testing.expect(backend2.mu.tryLock());
                        const retried_projection = backend2.prepareReadVersionWithAdmission(epoch, quotaControl());
                        backend2.mu.unlock();
                        retried_projection catch |retry_cause| break :blk retry_cause;
                    };
                    try std.testing.expect(epoch.prepared);
                    try std.testing.expectEqual(ordinal + 1, epoch.runs.len);
                    try std.testing.expectEqual(@as(u64, 0), epoch.accountedMemoryBytes(231));
                    for (epoch.runs, 0..) |projected_run, rank| try std.testing.expectEqual(epoch.directory.?.at(rank).run.id, projected_run.id);
                    if (ordinal == 0) {
                        // Ordinary Backend release unlinks its epoch before
                        // the registered receipt can authorize cleanup.
                        reader_owner = scope2.?.beginReadVersionWithAdmission(working.directory.?, original) catch |cause| break :blk cause;
                        backend_reader = reader_owner.?.takeWithAdmission(original) catch |cause| break :blk cause;
                        reader_owner = null;
                        const tracked = backend_reader.?;
                        const previous_head = backend2.live_read_versions;
                        tracked.registered = true;
                        tracked.live_next = previous_head;
                        if (previous_head) |head| head.live_previous = tracked;
                        backend2.live_read_versions = tracked;
                        tracked.release(&backend2);
                        backend_reader = null;
                        try std.testing.expect(backend2.live_read_versions == previous_head and backend2.retired_projection_versions == null);
                    }
                    // Canonical roots cannot be assembled from a copied or
                    // altered header under another tuple's seal.
                    var forged_roots = working.*;
                    forged_roots.writer = null;
                    try std.testing.expectError(error.InvalidArgument, scope2.?.beginPublicationPutWithAdmission(&forged_roots, input, &writer_view.?, quotaControl()));
                    working_candidate = scope2.?.beginPublicationPutWithAdmission(working, input, &writer_view.?, original) catch |cause| break :blk cause;
                    if (working_candidate.?.resumeWithAdmission(original)) |_| {
                        break :blk error.TestExpectedDuplicateRun;
                    } else |cause| if (cause != error.DuplicateRun) break :blk cause;
                    working_candidate.?.requestRetirement();
                    working_candidate = null;
                    writer_view.?.deinit(a2);
                    writer_view = null;
                }
                for (0..writer_count) |ordinal| {
                    const handle = working.directory.?.at(0);
                    // Find the matching canonical record rather than assuming
                    // directory and writer indexes have the same ordering.
                    for (0..working.writer.?.count()) |rank| {
                        const view = working.writer.?.at(rank);
                        if (view.id == handle.run.id) {
                            writer_view = view.retainOwned();
                            break;
                        }
                    }
                    try std.testing.expect(writer_view != null);
                    if (batch) {
                        working_candidate = scope2.?.preparePublicationWithAdmission(original) catch |cause| break :blk cause;
                        try std.testing.expectError(error.InvalidArgument, working_candidate.?.appendRemoveWithAdmission(handle, &writer_view.?, 100 + ordinal, quotaControl()));
                        capturePublication(&backend2, working_candidate.?, working, backend2.run_directory_generation, original) catch |cause| break :blk cause;
                        working_candidate.?.appendRemoveWithAdmission(handle, &writer_view.?, 100 + ordinal, original) catch |cause| break :blk cause;
                    } else working_candidate = scope2.?.beginPublicationRemoveWithAdmission(working, handle, &writer_view.?, 100 + ordinal, original) catch |cause| break :blk cause;
                    working_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    // Preparation leaves all live roots untouched.
                    try std.testing.expectEqual(writer_count - ordinal, working.directory.?.count());
                    try std.testing.expectEqual(writer_count - ordinal, working.writer.?.count());
                    if (batch) installPublication(&backend2, working_candidate.?, original) catch |cause| break :blk cause else working_candidate.?.publishWithAdmission(working, original) catch |cause| break :blk cause;
                    working_candidate.?.requestRetirement();
                    working_candidate = null;
                    try std.testing.expectEqual(writer_count - ordinal - 1, working.directory.?.count());
                    try std.testing.expectEqual(writer_count - ordinal - 1, working.writer.?.count());
                    try std.testing.expectEqual(@as(usize, if (shared) 0 else 1), working.obsolete.count());
                    for (readers[0..writer_count], 0..) |slot, generation| {
                        const directory = slot.?.directory.?;
                        try std.testing.expectEqual(generation + 1, directory.count());
                        try std.testing.expectEqual(@as(u64, generation + 1), directory.total_run_bytes);
                        try std.testing.expect(directory.at(0).run.id >= 911);
                    }
                    writer_view.?.deinit(a2);
                    writer_view = null;
                }
                reader_owner = scope2.?.beginReadVersionWithAdmission(working.directory.?, original) catch |cause| break :blk cause;
                backend_reader = reader_owner.?.takeWithAdmission(original) catch |cause| break :blk cause;
                reader_owner = null;
                try std.testing.expect(backend2.mu.tryLock());
                const empty_projection = backend2.prepareReadVersionWithAdmission(backend_reader.?, original);
                backend2.mu.unlock();
                empty_projection catch |cause| break :blk cause;
                try std.testing.expect(backend_reader.?.prepared and backend_reader.?.runs.len == 0 and backend_reader.?.l0_groups.len == 0 and backend_reader.?.levels.len == 0);
                backend_reader.?.releaseOwned();
                backend_reader = null;
                lease2.?.deinit(a2);
                lease2 = null;
                for (&extra_sources) |*slot| if (slot.*) |lease| {
                    var owned = lease;
                    owned.deinit(a1);
                    slot.* = null;
                };
            }
            if (writer_count != 0 and !publication) {
                var empty: Store = .{};
                for (0..writer_count) |level| {
                    writer = scope2.?.beginRegisteredWriterWithAdmission(&backend2, if (level == 0) &lease2.? else &extra_sources[level].?, .{ .level = if (writer_count == 1) 1 else 0, .gc_requested = level % 2 == 0 }, original) catch |cause| break :blk cause;
                    writer_view = writer.?.takeRunWithAdmission(original) catch |cause| break :blk cause;
                    writer.?.requestRetirement();
                    writer = null;
                    // Refusal precedes host acquisition, tree allocation and
                    // any physical or logical ownership change.
                    var forged_view = writer_view.?;
                    forged_view.level += 100;
                    try std.testing.expectError(error.InvalidArgument, scope2.?.beginRegisteredWriterRootWithAdmission(&empty, &forged_view, false, quotaControl()));
                    var legacy: Store = .{};
                    try std.testing.expectError(error.ExecutionWaitUnsupported, legacy.stage(a2, writer_view.?));
                    if (!shared) {
                        try std.testing.expectError(error.ExecutionWaitUnsupported, writer_view.?.ensureState(a2));
                        try std.testing.expectError(error.ExecutionWaitUnsupported, writer_view.?.ensureBloomFilter(a2));
                    }
                    if (exercise_obsolete) {
                        const empty_ledger: Scope.ObsoleteLedger = .{};
                        obsolete_candidate = scope2.?.beginObsoletePutWithAdmission(if (obsolete_live) |*live| live else &empty_ledger, &writer_view.?, 100 + level, original) catch |cause| break :blk cause;
                        obsolete_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                        const next_ledger = obsolete_candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                        obsolete_candidate.?.requestRetirement();
                        obsolete_candidate = null;
                        finishLedger(&obsolete_live, &obsolete_cursor, a2);
                        obsolete_live = next_ledger;
                    }
                    if (exercise_directory) {
                        const empty_directory: Directory = .{};
                        directory_candidate = scope2.?.beginDirectoryPutWithAdmission(if (directory_live) |live| live else &empty_directory, if (level == 0) &lease2.? else &extra_sources[level].?, original) catch |cause| break :blk cause;
                        directory_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                        const next_directory = directory_candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                        directory_candidate.?.requestRetirement();
                        directory_candidate = null;
                        finishDirectory(&directory_live, &directory_cursor, a2);
                        directory_live = next_directory;
                    }
                    candidate = scope2.?.beginRegisteredWriterRootWithAdmission(if (root) |live| live else &empty, &writer_view.?, false, original) catch |cause| break :blk cause;
                    candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    const next = candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                    candidate.?.requestRetirement();
                    candidate = null;
                    writer_view.?.deinit(a2);
                    writer_view = null;
                    if (level + 1 == writer_count and root != null) {
                        snapshot = root;
                        root = null;
                    } else finishRoot(&root, &root_cursor, a2);
                    root = next;
                }
                const latest = writer_count - 1;
                const before_gc = root.?.at(0).gc_requested;
                if (exercise_remove) stale_view = root.?.at(0).retainOwned();
                writer = scope2.?.beginRegisteredWriterWithAdmission(&backend2, if (latest == 0) &lease2.? else &extra_sources[latest].?, .{ .level = if (writer_count == 1) 1 else 0, .gc_requested = !before_gc }, original) catch |cause| break :blk cause;
                writer_view = writer.?.takeRunWithAdmission(original) catch |cause| break :blk cause;
                writer.?.requestRetirement();
                writer = null;
                candidate = scope2.?.beginRegisteredWriterRootWithAdmission(root.?, &writer_view.?, false, original) catch |cause| break :blk cause;
                if (candidate.?.resumeWithAdmission(original)) |_| {
                    break :blk error.TestExpectedDuplicateRun;
                } else |cause| {
                    if (cause != error.DuplicateRun) break :blk cause;
                }
                candidate.?.requestRetirement();
                candidate = null;
                candidate = scope2.?.beginRegisteredWriterRootWithAdmission(root.?, &writer_view.?, true, original) catch |cause| break :blk cause;
                candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                try std.testing.expectEqual(before_gc, root.?.at(0).gc_requested);
                const replacement = candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                candidate.?.requestRetirement();
                candidate = null;
                finishRoot(&root, &root_cursor, a2);
                root = replacement;
                if (exercise_obsolete) {
                    obsolete_snapshot = obsolete_live.?.fork();
                    obsolete_candidate = scope2.?.beginObsoletePutWithAdmission(&obsolete_live.?, &writer_view.?, 1, original) catch |cause| break :blk cause;
                    obsolete_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    const kept_deadline = obsolete_candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                    obsolete_candidate.?.requestRetirement();
                    obsolete_candidate = null;
                    try std.testing.expectEqual(100 + latest, kept_deadline.get(writer_view.?.path.?).?.delete_after_ns);
                    finishLedger(&obsolete_live, &obsolete_cursor, a2);
                    obsolete_live = kept_deadline;
                    obsolete_candidate = scope2.?.beginObsoletePutWithAdmission(&obsolete_live.?, &writer_view.?, 1000, original) catch |cause| break :blk cause;
                    obsolete_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    const revised_deadline = obsolete_candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                    obsolete_candidate.?.requestRetirement();
                    obsolete_candidate = null;
                    try std.testing.expectEqual(100 + latest, obsolete_snapshot.?.get(writer_view.?.path.?).?.delete_after_ns);
                    try std.testing.expectEqual(@as(u64, 1000), revised_deadline.get(writer_view.?.path.?).?.delete_after_ns);
                    finishLedger(&obsolete_live, &obsolete_cursor, a2);
                    obsolete_live = revised_deadline;
                    const rank = writer_count / 2;
                    obsolete_candidate = scope2.?.beginObsoleteRemoveWithAdmission(&obsolete_live.?, rank, original) catch |cause| break :blk cause;
                    obsolete_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    const removed = obsolete_candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                    obsolete_candidate.?.requestRetirement();
                    obsolete_candidate = null;
                    try std.testing.expectEqual(writer_count - 1, removed.count());
                    try std.testing.expectEqual(writer_count, obsolete_live.?.count());
                    try std.testing.expectEqual(@as(u64, 0), removed.memoryBytes(391));
                    try std.testing.expectError(error.ExecutionWaitUnsupported, obsolete_live.?.ensureUnusedCapacity(a2, 1));
                    finishLedger(&obsolete_live, &obsolete_cursor, a2);
                    obsolete_live = removed;
                }
                writer_view.?.deinit(a2);
                writer_view = null;
                try std.testing.expectEqual(writer_count, root.?.count());
                try std.testing.expect(root.?.tree.account.?.independently_funded);
                try std.testing.expectEqual(@as(u64, 0), root.?.tree.account.?.chargeOnce(913));
                try std.testing.expectEqual(if (writer_count == 1) @as(usize, 0) else writer_count, root.?.l0Files());
                try std.testing.expectError(error.ExecutionWaitUnsupported, root.?.stage(a2, root.?.at(0).*));
                try std.testing.expectError(error.ExecutionWaitUnsupported, root.?.remove(a2, root.?.at(0)));
                if (snapshot) |old| try std.testing.expectEqual(writer_count - 1, old.count());
                for (0..writer_count) |rank| {
                    const ordinal = writer_count - 1 - rank;
                    try std.testing.expectEqual(@as(u32, if (writer_count == 1) 1 else 0), root.?.at(rank).level);
                    try std.testing.expectEqual(source.run.id + ordinal, root.?.at(rank).id);
                    try std.testing.expectEqual(if (ordinal == latest) ordinal % 2 != 0 else ordinal % 2 == 0, root.?.at(rank).gc_requested);
                }
                if (exercise_directory) {
                    directory_candidate = scope2.?.beginDirectoryPutWithAdmission(directory_live.?, &lease2.?, original) catch |cause| break :blk cause;
                    directory_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    const replaced_directory = directory_candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                    directory_candidate.?.requestRetirement();
                    directory_candidate = null;
                    directory_snapshot = directory_live;
                    directory_live = replaced_directory;
                    try std.testing.expectEqual(writer_count, directory_live.?.count());
                    try std.testing.expectEqual(writer_count, directory_snapshot.?.count());
                    const handle = directory_live.?.at(writer_count / 2);
                    directory_candidate = scope2.?.beginDirectoryRemoveWithAdmission(directory_live.?, handle, original) catch |cause| break :blk cause;
                    directory_candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
                    const removed_directory = directory_candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
                    directory_candidate.?.requestRetirement();
                    directory_candidate = null;
                    finishDirectory(&directory_live, &directory_cursor, a2);
                    directory_live = removed_directory;
                    try std.testing.expectEqual(writer_count - 1, directory_live.?.count());
                    try std.testing.expectEqual(writer_count, directory_snapshot.?.count());
                    try std.testing.expectEqual(@as(u64, writer_count - 1), directory_live.?.total_run_bytes);
                    inline for (.{ "tree", "ids", "bounds", "ends" }) |field| try std.testing.expectEqual(writer_count - 1, if (@field(directory_live.?, field).root) |node| node.count else 0);
                    try std.testing.expectEqual(@as(u64, 0), directory_live.?.accountedMemoryBytes(983));
                    try std.testing.expectError(error.ExecutionWaitUnsupported, directory_live.?.fork(a2));
                    try std.testing.expectError(error.ExecutionWaitUnsupported, directory_live.?.put(&backend2, lease2.?.entry.run.*));
                }
                const captured = try Scope.RegisteredWriter.validate(root.?.at(0), quotaControl());
                try std.testing.expect(captured.source.?.domain().ptr == source.label.?.ptr);
                if (exercise_remove) {
                    // GC revisions have the same read order but a different
                    // canonical owner. Old views cannot remove the new record.
                    candidate = scope2.?.beginRegisteredWriterRemoveWithAdmission(root.?, &stale_view.?, original) catch |cause| break :blk cause;
                    if (candidate.?.resumeWithAdmission(original)) |_| {
                        break :blk error.TestExpectedStaleRevision;
                    } else |cause| if (cause != error.CompactionPlanningStale) break :blk cause;
                    candidate.?.requestRetirement();
                    candidate = null;
                    stale_view.?.deinit(a2);
                    stale_view = null;
                    var ordinal: usize = 0;
                    while (root.?.count() != 0) : (ordinal += 1) {
                        const before_count = root.?.count();
                        const rank = switch (ordinal % 3) {
                            0 => before_count / 2,
                            1 => 0,
                            else => before_count - 1,
                        };
                        removed_view = root.?.at(rank).retainOwned();
                        candidate = scope2.?.beginRegisteredWriterRemoveWithAdmission(root.?, &removed_view.?, original) catch |cause| break :blk cause;
                        candidate.?.resumeWithAdmission(original) catch |cause| {
                            if (!retry_edit or cause != fault.failure) break :blk cause;
                            recovered_edit = true;
                            recovered_detached = recovered_detached or (candidate.?.applied and candidate.?.detached != null);
                            fault.live = false;
                            candidate.?.resumeWithAdmission(quotaControl()) catch |retry_cause| break :blk retry_cause;
                        };
                        const removed_root = candidate.?.takeWithAdmission(original) catch |cause| retry: {
                            if (!retry_edit or cause != fault.failure) break :blk cause;
                            recovered_edit = true;
                            fault.live = false;
                            break :retry candidate.?.takeWithAdmission(quotaControl()) catch |retry_cause| break :blk retry_cause;
                        };
                        candidate.?.requestRetirement();
                        candidate = null;
                        if (removal_snapshot == null) {
                            removal_snapshot = root;
                            root = null;
                        } else finishRoot(&root, &root_cursor, a2);
                        root = removed_root;
                        try std.testing.expectEqual(before_count - 1, root.?.count());
                        try std.testing.expectEqual(@as(u64, before_count - 1), root.?.totalBytes());
                        try std.testing.expectEqual(writer_count, removal_snapshot.?.count());
                        // This also exercises an empty registered root on the
                        // last iteration. A removed input is a stale plan.
                        candidate = scope2.?.beginRegisteredWriterRemoveWithAdmission(root.?, &removed_view.?, original) catch |cause| break :blk cause;
                        if (candidate.?.resumeWithAdmission(original)) |_| {
                            break :blk error.TestExpectedMissingInput;
                        } else |cause| if (cause != error.CompactionPlanningStale) break :blk cause;
                        candidate.?.requestRetirement();
                        candidate = null;
                        removed_view.?.deinit(a2);
                        removed_view = null;
                    }
                    try std.testing.expectEqual(@as(usize, 0), root.?.count());
                    try std.testing.expectEqual(@as(u64, 0), root.?.memoryBytes(169));
                }
                lease2.?.deinit(a2);
                lease2 = null;
                for (&extra_sources) |*slot| if (slot.*) |lease| {
                    var owned = lease;
                    owned.deinit(a1);
                    slot.* = null;
                };
            }
            lease1.?.deinit(a1);
            lease1 = null;
            backend2.close();
            backend2_live = false;
            scope2.?.requestRetirement();
            scope2 = null;
            for (0..2) |_| _ = first.resource_manager.drainPayloadRetirementQuiesced();
            try std.testing.expect(!source.released and !first.tryBeginDeinit());
            try std.testing.expectEqual(source.usage + resident1, first.resource_manager.sliceStats(.lsm_in_memory_state).used_bytes);
            if (writer_count == 0) try std.testing.expectEqualStrings("ancestor-domain", lease2.?.entry.domain) else try std.testing.expectEqualStrings("ancestor-domain", source.label.?);
            try std.testing.expectEqual(route_count, Policy.calls);
            if (phase == .retire) {
                fault.live = true;
                if (publication) {
                    if (!batch) working.retire();
                    header_witness.expected_bytes = @sizeOf(Scope.RegisteredReadVersion);
                    header_witness.expected_pointer = @ptrCast(reader_frames[0].?);
                    header_witness.active = writer_count == 1 and target == null and !oom;
                    if (header_witness.active) {
                        // Exercise an explicit early close as well as the
                        // normal final-release notification path.
                        try std.testing.expect(!reader_frames[0].?.registration.ready.load(.acquire));
                        reader_frames[0].?.requestRetirement();
                        try std.testing.expect(reader_frames[0].?.registration.ready.load(.acquire));
                        final_return_probe = .{ .manager = &second.resource_manager, .frame = reader_frames[0].?, .witness = &header_witness, .saved = readers[0].?.owned_retirement.? };
                        readers[0].?.owned_retirement = .{ .context = &final_return_probe.?, .retire = ReaderReturnProbe.finish };
                    }
                    for (&readers) |*slot| if (slot.*) |version| {
                        // The controller and both Backends have closed. An
                        // existing reference still pins the same generation.
                        const peer = version.retainOwnedWithAdmission(original) catch |cause| break :blk cause;
                        peer.releaseOwned();
                        version.releaseOwned();
                        slot.* = null;
                    };
                } else if (writer_count != 0) {
                    header_witness.active = writer_count == 1 and target == null and !oom;
                    const live = root.?;
                    root_cursor = .init(live);
                    live.tree.root = null;
                    while (true) {
                        original.checkpoint() catch |cause| break :blk cause;
                        var steps: usize = 1;
                        if (root_cursor.?.step(a2, &steps)) break;
                    }
                    original.checkpoint() catch |cause| break :blk cause;
                    root_cursor.?.finish(a2);
                    root_cursor = null;
                    root = null;
                    header_witness.active = false;
                    if (writer_count == 1 and target == null and !oom) try std.testing.expect(header_witness.observed);
                    finishRoot(&snapshot, &snapshot_cursor, a2);
                } else {
                    lease2.?.deinit(a2);
                    lease2 = null;
                }
                if (exercise_obsolete) {
                    obsolete_cursor = .init(obsolete_live.?);
                    while (true) {
                        original.checkpoint() catch |cause| break :blk cause;
                        var steps: usize = 1;
                        if (obsolete_cursor.?.step(a2, &steps)) break;
                    }
                    obsolete_cursor = null;
                    obsolete_live = null;
                    obsolete_old_cursor = .init(obsolete_snapshot.?);
                    while (true) {
                        original.checkpoint() catch |cause| break :blk cause;
                        var steps: usize = 1;
                        if (obsolete_old_cursor.?.step(a2, &steps)) break;
                    }
                    obsolete_old_cursor = null;
                    obsolete_snapshot = null;
                }
                if (exercise_directory) {
                    header_witness.expected_bytes = @sizeOf(Directory);
                    header_witness.expected_pointer = @ptrCast(directory_live.?);
                    header_witness.active = writer_count == 1 and target == null and !oom;
                    header_witness.observed = false;
                    directory_cursor = .init(directory_live.?);
                    while (true) {
                        original.checkpoint() catch |cause| break :blk cause;
                        var steps: usize = 1;
                        if (directory_cursor.?.step(a2, &steps)) break;
                    }
                    original.checkpoint() catch |cause| break :blk cause;
                    directory_cursor.?.finish(a2);
                    directory_cursor = null;
                    directory_live = null;
                    if (writer_count == 1 and target == null and !oom) try std.testing.expect(header_witness.observed);
                    header_witness.active = false;
                    directory_old_cursor = .init(directory_snapshot.?);
                    while (true) {
                        original.checkpoint() catch |cause| break :blk cause;
                        var steps: usize = 1;
                        if (directory_old_cursor.?.step(a2, &steps)) break;
                    }
                    original.checkpoint() catch |cause| break :blk cause;
                    directory_old_cursor.?.finish(a2);
                    directory_old_cursor = null;
                    directory_snapshot = null;
                }
                if (removal_snapshot) |old| {
                    removal_snapshot_cursor = .init(old);
                    old.tree.root = null;
                    while (true) {
                        original.checkpoint() catch |cause| break :blk cause;
                        var steps: usize = 1;
                        if (removal_snapshot_cursor.?.step(a2, &steps)) break;
                    }
                    original.checkpoint() catch |cause| break :blk cause;
                    removal_snapshot_cursor.?.finish(a2);
                    removal_snapshot_cursor = null;
                    removal_snapshot = null;
                }
                while (first.resource_manager.payload_retirement_ready.load(.acquire) != 0 or second.resource_manager.payload_retirement_ready.load(.acquire) != 0) {
                    _ = second.resource_manager.drainPayloadRetirementWithAdmission(original) catch |cause| break :blk cause;
                    _ = first.resource_manager.drainPayloadRetirementWithAdmission(original) catch |cause| break :blk cause;
                }
            }
            if (publication and phase == .retire and writer_count == 1 and target == null and !oom) {
                try std.testing.expect(header_witness.observed);
                try std.testing.expect(final_return_probe.?.checked);
            }
            break :blk {};
        };
        const stats: Stats = .{ .checks = fault.calls, .allocations = allocator2.alloc_index - allocation_start, .recovered = recovered_edit, .detached_recovered = recovered_detached };
        if (oom) {
            try std.testing.expect(allocator2.has_induced_failure);
            try std.testing.expectError(error.OutOfMemory, result);
        } else if (target != null and !recovered_edit) try std.testing.expectError(fault.failure, result) else try result;
        if (publication and phase == .construct and (target == null or recovered_edit) and !oom) {
            for (readers[0..writer_count]) |slot| try slot.?.qualifyProjectionForTest(a2);
            try qualifyProjectionCoalescing(a2, &second.resource_manager, readers[0].?.directory.?);
            if (target == null) try qualifyNormalReaderCapture(a2, &second.resource_manager, readers[0].?.directory.?);
            if (projection_shape and writer_count == 8) {
                try std.testing.expectEqual(@as(usize, 3), readers[7].?.l0_groups.len);
                try std.testing.expectEqual(@as(usize, 2), readers[7].?.levels.len);
            }
        }
        if (writer_count != 0 and !publication and phase == .construct and target == null and !oom) {
            // These diagnostic roots are outside the measured constructor and
            // retain their own prepaid header through the observer's close.
            var header_credit = try second.resource_manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Store) + (if (exercise_directory) @as(u64, @sizeOf(Directory)) else 0), quotaControl());
            defer header_credit.retire();
            var observer = lsm.Backend.init(a2, .{ .resource_manager = &second.resource_manager, .wal_enabled = false });
            defer observer.close();
            observer.runs = root.?.fork();
            if (exercise_directory) {
                const diagnostic = try a2.create(Directory);
                diagnostic.* = directory_live.?.forkContents();
                observer.run_directory = diagnostic;
            }
            if (obsolete_live) |*ledger| observer.obsolete_paths = ledger.fork();
            const retired = try a2.create(Store);
            retired.* = root.?.fork();
            observer.retired_run_stores = retired;
            try std.testing.expect(observer.mu.tryLock());
            observer.syncTrackedInMemoryStateUsageCurrentLocked();
            observer.mu.unlock();
            try std.testing.expectEqual(@as(u64, 0), observer.tracked_in_memory_state_bytes);
            var preflight: lsm.Backend.QuiescedClosePreflight = .{};
            const observation_payload: read_admission.Payload = .{ .ptr = &header_witness, .vtable = &.{ .checkpoint = quotaCheck, .check_lifetime = quotaCheck, .deadline_ns = HeaderWitness.deadline }, .execution_wait = .{ .ptr = &header_witness, .begin = HeaderWitness.refuseWait, .end = HeaderWitness.refuseWait } };
            try preflight.resumeWithAdmission(&observer, std.testing.io, read_admission.Port.init(&observation_payload));
            try std.testing.expectEqual(@as(u64, 0), preflight.bytes);
            try std.testing.expectEqual(@as(u64, 0), root.?.memoryBytes(915));
            try std.testing.expectEqual(@as(u64, 0), root.?.observedHeaderBytes());
        }
        header_witness.active = false;
        if (batch) {
            if (backend2_live) {
                backend2.close();
                backend2_live = false;
            }
        } else working.retire();
        for (&readers) |*slot| if (slot.*) |version| {
            version.releaseOwned();
            slot.* = null;
        };
        if (backend_reader) |version| version.releaseOwned();
        backend_reader = null;
        if (reader_owner) |owner| owner.requestRetirement();
        reader_owner = null;
        if (working_candidate) |owner| owner.requestRetirement();
        working_candidate = null;
        if (sibling) |owner| owner.requestRetirement();
        sibling = null;
        finishDirectory(&directory_live, &directory_cursor, a2);
        finishDirectory(&directory_snapshot, &directory_old_cursor, a2);
        if (directory_candidate) |owner| owner.requestRetirement();
        directory_candidate = null;
        finishLedger(&obsolete_live, &obsolete_cursor, a2);
        finishLedger(&obsolete_snapshot, &obsolete_old_cursor, a2);
        if (obsolete_candidate) |owner| owner.requestRetirement();
        obsolete_candidate = null;
        finishRoot(&root, &root_cursor, a2);
        finishRoot(&snapshot, &snapshot_cursor, a2);
        finishRoot(&removal_snapshot, &removal_snapshot_cursor, a2);
        if (stale_view) |*view| view.deinit(a2);
        stale_view = null;
        if (removed_view) |*view| view.deinit(a2);
        removed_view = null;
        if (writer_view) |*view| view.deinit(a2);
        writer_view = null;
        if (candidate) |owner| owner.requestRetirement();
        candidate = null;
        if (writer) |owner| owner.requestRetirement();
        writer = null;
        for (&extra_sources) |*slot| if (slot.*) |lease| {
            var owned = lease;
            owned.deinit(a1);
            slot.* = null;
        };
        if (lease2) |*lease| lease.deinit(a2);
        lease2 = null;
        if (lease1) |*lease| lease.deinit(a1);
        lease1 = null;
        if (scope2) |owner| owner.requestRetirement();
        scope2 = null;
        drainPair(&first.resource_manager, &second.resource_manager);
        if (backend2_live) {
            // Off-lock preparation initializes the live Backend's observer.
            // After external owners retire, only that tracked charge may remain.
            try std.testing.expectEqual(backend2.tracked_in_memory_state_bytes, second.resource_manager.sliceStats(.lsm_in_memory_state).used_bytes);
            backend2.close();
            backend2_live = false;
            drainPair(&first.resource_manager, &second.resource_manager);
        }
        try std.testing.expect(source.released);
        try std.testing.expectEqual(@as(u64, 0), first.quota_lifetimes.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), second.quota_lifetimes.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), first.resource_manager.sliceStats(.lsm_in_memory_state).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), second.resource_manager.sliceStats(.lsm_in_memory_state).used_bytes);
        if (writer_count != 0) {
            try std.testing.expectEqual(@as(u64, 0), first.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
            try std.testing.expectEqual(@as(u64, 0), second.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
        }
        header_witness.active = false;
        allocator2.fail_index = std.math.maxInt(usize);
        return stats;
    }
};

test "physical cache metadata acquired registered source preserves allocator domain and accounting ancestry across producer exit" {
    for ([_]bool{ false, true }) |shared| {
        _ = try RegisteredSourceFixture.run(shared, .construct, null, false);
        _ = try RegisteredSourceFixture.run(shared, .retire, null, false);
    }
}

test "physical cache metadata acquired registered source retains every constructor and retirement refusal" {
    for ([_]bool{ false, true }) |shared| for (std.enums.values(RegisteredSourceFixture.Phase)) |phase| {
        const count = (try RegisteredSourceFixture.run(shared, phase, null, false)).checks;
        for (0..count) |at| _ = try RegisteredSourceFixture.run(shared, phase, at, false);
    };
}

test "physical cache metadata acquired registered source aborts every allocation failure without releasing ancestors early" {
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.run(shared, .construct, null, false)).allocations;
        for (0..count) |at| _ = try RegisteredSourceFixture.run(shared, .construct, at, true);
    }
}

test "physical cache metadata registered writer roots retain immutable sources and funded descendants after producer exit" {
    for ([_]bool{ false, true }) |shared| for ([_]usize{ 1, 3, 8 }) |count| {
        _ = try RegisteredSourceFixture.runMode(shared, .construct, null, false, count);
        _ = try RegisteredSourceFixture.runMode(shared, .retire, null, false, count);
    };
}

test "physical cache metadata registered writer roots preserve every original constructor and retirement refusal" {
    for ([_]bool{ false, true }) |shared| for (std.enums.values(RegisteredSourceFixture.Phase)) |phase| {
        const count = (try RegisteredSourceFixture.runMode(shared, phase, null, false, 3)).checks;
        for (0..count) |at| _ = try RegisteredSourceFixture.runMode(shared, phase, at, false, 3);
    };
}

test "physical cache metadata registered writer roots retire every failed allocation while their allocator stays exhausted" {
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runMode(shared, .construct, null, false, 3)).allocations;
        for (0..count) |at| _ = try RegisteredSourceFixture.runMode(shared, .construct, at, true, 3);
    }
}

const RegisteredOutputFixture = struct {
    const local = @import("antfly_local_sources");
    const lsm = local.storage_lsm_backend_mod;
    const io_mod = local.storage_lsm_backend_storage_io;
    const Scope = lsm.Backend.PublicationResources;
    const Writer = lsm.repository.OwnedStreamingRunFileWriter;
    const Stage = enum { create, handoff, source, retire, domain, publish, producer_abort, producer_borrow_abort, producer_create, producer_invalid, producer_mismatch, producer_queue_join };
    const Cleanup = @import("antfly_local_sources").storage_lsm_backend_mod.repository.OutputCleanup;
    const Stats = struct { checks: usize, allocations: usize };
    const Control = struct {
        calls: usize = 0,
        target: ?usize = null,
        live: bool = false,
        failure: anyerror = error.Canceled,
        depth: usize = 0,
        reject_end: bool = false,
        reject_input: ?*Writer = null,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject_input) |writer| if (writer.entry) |entry| {
                if (entry.phase == .row_key and entry.offset > 0) {
                    self.reject_input = null;
                    return error.Canceled;
                }
            };
            if (!self.live) return;
            const at = self.calls;
            self.calls += 1;
            if (self.target == at) return self.failure;
        }
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try check(raw);
            try std.testing.expectEqual(@as(usize, 0), self.depth);
            self.depth = 1;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), self.depth);
            self.depth = 0;
            if (self.reject_end) {
                self.reject_end = false;
                return error.Canceled;
            }
            try check(raw);
        }
        fn wait(_: *anyopaque) !void {
            return error.TestUnexpectedWait;
        }
        fn payload(self: *@This()) read_admission.Payload {
            return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check, .deadline_ns = deadline, .wait_for_progress = wait }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
        }
    };
    const Policy = struct {
        var static_domain: bool = false;
        fn key(bytes: []const u8) []const u8 {
            return if (static_domain) "registered-file-domain" else bytes[0..bytes.len -| 1];
        }
        fn admitted(bytes: []const u8, original: read_admission.Port) ![]const u8 {
            try original.checkpoint();
            return key(bytes);
        }
    };
    fn drain(manager: *ResourceManager) void {
        for (0..4096) |_| {
            if (manager.payload_retirement_ready.load(.acquire) == 0) return;
            _ = manager.drainPayloadRetirementQuiesced();
        }
        @panic("registered output did not retire");
    }
    fn run(stage: Stage, target: ?usize, oom: bool, static_domain: bool) !Stats {
        const a = std.testing.allocator;
        var failing1 = std.testing.FailingAllocator.init(a, .{});
        var failing2 = std.testing.FailingAllocator.init(a, .{});
        const a1 = failing1.allocator();
        const a2 = failing2.allocator();
        var parent1 = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent1.deinit(a);
        var parent2 = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent2.deinit(a);
        var budgets1 = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets1.options.identity_allocator = a1;
        var budgets2 = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets2.options.identity_allocator = a2;
        var first = try Resources.initWithBudgetsAndParent(a1, budgets1, parent1.memoryParentPort());
        var first_live = true;
        defer if (first_live) first.deinit();
        first.attachResourceManager();
        var second = try Resources.initWithBudgetsAndParent(a2, budgets2, parent2.memoryParentPort());
        defer second.deinit();
        second.attachResourceManager();
        var queue: Cleanup.Queue = .{ .allocator = a1, .manager = &first.resource_manager };
        defer queue.deinit();
        var pool = io_mod.NativeStoragePool.initWithCapacityForTest(a, 8);
        defer pool.deinit();
        // The reader uses a different storage object from the retired producer.
        var reader_native = try io_mod.NativeStorage.initWithPool(a, .threaded, &pool);
        defer reader_native.deinit();
        var backend1 = lsm.Backend.init(a1, .{ .resource_manager = &first.resource_manager, .wal_enabled = false });
        var backend1_live = true;
        defer if (backend1_live) backend1.close();
        Policy.static_domain = static_domain or stage == .domain;
        var backend2 = lsm.Backend.init(a2, .{ .resource_manager = &second.resource_manager, .storage = reader_native.storage(), .wal_enabled = false, .run_partition_key = Policy.key, .run_partition_key_with_admission = Policy.admitted, .run_partition_domain_lifetime = if (stage != .domain and static_domain) .key_or_static else .borrowed });
        var backend2_live = true;
        defer if (backend2_live) backend2.close();
        var scope1: ?*Scope = try backend1.beginPublicationResourcesWithAdmission(quotaControl());
        var scope2: ?*Scope = null;
        var output: ?*Scope.RegisteredOutput.Owner = null;
        var lease: ?Scope.EntryLease = null;
        var record: ?*Scope.RegisteredWriter = null;
        var view: ?lsm.repository.Run = null;
        var candidate: ?*Scope.RegisteredWriterRoot = null;
        var root: ?*Scope.WriterStore = null;
        var root_cursor: ?Scope.WriterStore.Reclaimer = null;
        var publication: ?*Scope.Publication.Candidate = null;
        defer {
            if (publication) |owner| owner.requestRetirement();
            RegisteredSourceFixture.finishRoot(&root, &root_cursor, a2);
            if (view) |*owned| owned.deinit(a2);
            if (record) |owner| owner.requestRetirement();
            if (candidate) |owner| owner.requestRetirement();
            if (lease) |*owned| owned.deinit(a2);
            if (scope2) |owner| owner.requestRetirement();
            if (output) |owner| owner.requestRetirement();
            if (scope1) |owner| owner.requestRetirement();
            // Error cleanup must return installed roots before draining their
            // registered anchors, just as successful joined teardown does.
            if (backend2_live) {
                backend2.close();
                backend2_live = false;
            }
            drain(&second.resource_manager);
            if (first_live) drain(&first.resource_manager);
        }
        try scope1.?.resumeWithAdmission(quotaControl());
        scope2 = try backend2.beginPublicationResourcesWithAdmission(quotaControl());
        try scope2.?.resumeWithAdmission(quotaControl());
        try std.testing.expect(backend2.mu.tryLock());
        const planned = backend2.planningDirectory();
        backend2.mu.unlock();
        const directory = try (try planned).fork(a2);
        defer directory.destroy(a2);
        try directory.tree.prepare(a2);
        var native = try io_mod.NativeStorage.initWithPool(a, .threaded, &pool);
        var native_live = true;
        defer if (native_live) native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/registered-output.sst", .{tmp.sub_path});
        defer a.free(path);
        var control: Control = .{ .target = if (oom) null else target, .failure = switch ((target orelse 0) % 4) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            2 => error.FileNotFound,
            else => error.ParentMemoryBusy,
        } };
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        var stack_writer: Writer = undefined;
        var writer: *Writer = &stack_writer;
        var producer: ?*Scope.RegisteredProducer = null;
        defer if (producer) |owner| owner.requestRetirement();
        var offered_ticket: ?*Cleanup.Ticket = null;
        defer if (offered_ticket) |ticket| ticket.destroy();
        var offered_grant: ?io_mod.DurabilityResources = null;
        var writer_live = false;
        var fixed: ?local.storage_resource_manager.Reservation = null;
        defer {
            if (offered_grant) |*grant| grant.deinit();
            if (writer_live) writer.deinit();
            if (fixed) |*credit| credit.release();
        }
        var allocation_start: usize = 0;
        const selected = if (stage == .source or stage == .retire or stage == .domain) &failing2 else &failing1;
        const result: anyerror!void = blk: {
            if (stage == .create) {
                allocation_start = selected.alloc_index;
                control.live = true;
                if (oom) selected.fail_index = allocation_start + target.?;
            }
            output = scope1.?.beginRegisteredOutputWithAdmission(original) catch |cause| break :blk cause;
            if (stage == .create) break :blk {};
            fixed = try first.resource_manager.reserveWithoutReclaimWithAdmission(.lsm_table_builder_working_set, @sizeOf(Writer) + try native.storage().appendResourceBytes(0), original);
            offered_grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)).?;
            if (stage == .publish or stage == .producer_abort or stage == .producer_borrow_abort or stage == .producer_create or stage == .producer_invalid or stage == .producer_mismatch or stage == .producer_queue_join) {
                var creation = Cleanup.Creation.init(&queue, path);
                defer creation.retireWithAdmission(null) catch unreachable;
                try creation.resumeWithAdmission(original);
                offered_ticket = try creation.take();
                const offered_ptr = offered_grant.?.ptr;
                const offered_path_ticket = offered_ticket.?;
                if (stage == .producer_create) {
                    allocation_start = selected.alloc_index;
                    control.live = true;
                    if (oom) selected.fail_index = allocation_start + target.?;
                }
                const controller = if (stage == .producer_mismatch) scope2.? else scope1.?;
                producer = controller.beginRegisteredProducerWithAdmission(&offered_grant, try native.storage().appendResourceBytes(0), &offered_ticket, 3, if (stage == .producer_invalid) 0 else 1 << 20, .{}, original) catch |cause| {
                    try std.testing.expect(offered_grant != null and offered_grant.?.ptr == offered_ptr and offered_ticket == offered_path_ticket);
                    break :blk cause;
                };
                try std.testing.expect(offered_grant == null and offered_ticket == null);
                writer = try producer.?.drivingWriter();
                fixed.?.release();
                fixed = null; // Registered producer now funds header and IO grant.
            } else {
                try writer.initInPlace(a1, &first.resource_manager, offered_grant.?, path, 3, 1 << 20, .{});
                offered_grant = null;
                writer_live = true;
            }
            if (stage == .producer_create) break :blk {};
            if (stage == .producer_abort) control.reject_end = true;
            writer.openWithAdmission(original) catch |cause| break :blk cause;
            if (stage == .producer_queue_join) break :blk {};
            var key: [32777]u8 = @splat('k');
            key[0] = 'p';
            // Existing SST rows decode zero-length namespaces as null. Keep
            // empty-name ownership coverage in the metadata-only stages;
            // persisted reads exercise the default and a named namespace.
            const namespace: ?[]const u8 = if (static_domain) (if (stage == .publish) "docs" else "") else null;
            for (0..3) |ordinal| {
                key[key.len - 1] = @intCast(ordinal);
                if (stage == .producer_borrow_abort) control.reject_input = writer;
                writer.appendEntryWithAdmission(.{ .namespace_name = namespace, .key = &key, .value = "owned output" }, original) catch |cause| {
                    if (stage == .producer_borrow_abort) {
                        try std.testing.expect(writer.pending_input != null and writer.entry != null);
                        writer.beginAbort();
                        try std.testing.expect(writer.pending_input == null and writer.entry.?.retiring);
                        try std.testing.expectError(error.InvalidArgument, producer.?.drivingWriter());
                        @memset(&key, 0xcc); // Caller input is gone before registered cleanup.
                    }
                    break :blk cause;
                };
            }
            try writer.finishWithAdmission(original);
            if (stage == .handoff) {
                allocation_start = selected.alloc_index;
                control.live = true;
                if (oom) selected.fail_index = allocation_start + target.?;
            }
            writer.takeResultInto(&output.?.output, original) catch |cause| break :blk cause;
            if (stage == .handoff) break :blk {};
            try std.testing.expect(output.?.output.bounds.?.fields[1].?[key.len - 1] == 0 and output.?.output.bounds.?.fields[3].?[key.len - 1] == 2);
            try std.testing.expectEqual(namespace != null, output.?.output.bounds.?.fields[0] != null);
            try writer.retireWithAdmission(original);
            if (producer) |owner| {
                // The test's temporary-directory controller takes final-file
                // deletion debt before allowing metadata installation.
                offered_ticket = try owner.takeTicketWithAdmission(original);
                owner.requestRetirement();
                producer = null;
                while (first.resource_manager.payload_retirement_ready.load(.acquire) != 0) _ = try first.resource_manager.drainPayloadRetirementWithAdmission(original);
                offered_ticket.?.destroy();
                offered_ticket = null;
                while (first.resource_manager.payload_retirement_ready.load(.acquire) != 0) _ = try first.resource_manager.drainPayloadRetirementWithAdmission(original);
                try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
            } else {
                writer.deinit();
                writer_live = false;
                fixed.?.release();
                fixed = null;
            }
            native.deinit();
            native_live = false;
            backend1.close();
            backend1_live = false;
            scope1.?.requestRetirement();
            scope1 = null;
            if (stage == .source or stage == .domain) {
                allocation_start = selected.alloc_index;
                control.live = true;
                if (oom) selected.fail_index = allocation_start + target.?;
            }
            scope2.?.beginEntryFromRegisteredOutputWithAdmission(&backend2, directory, output.?, .{ .id = 501, .level = 0 }, original) catch |cause| break :blk cause;
            output.?.requestRetirement();
            output = null;
            lease = scope2.?.takeEntryWithAdmission(original) catch |cause| break :blk cause;
            try std.testing.expect(lease.?.entry.run.read_identity.valid());
            try std.testing.expectEqual(namespace != null, lease.?.entry.run.smallest_namespace_name != null);
            try std.testing.expectEqual(@as(u8, 0), lease.?.entry.run.smallest_key[key.len - 1]);
            try std.testing.expectEqual(@as(u8, 2), lease.?.entry.run.largest_key[key.len - 1]);
            if (static_domain) try std.testing.expectEqualStrings("registered-file-domain", lease.?.entry.domain) else try std.testing.expectEqual(@as(usize, key.len - 1), lease.?.entry.domain.len);
            drain(&first.resource_manager);
            try std.testing.expectEqual(@as(u64, 0), first.quota_lifetimes.load(.acquire));
            try std.testing.expectEqual(@as(u64, 0), first.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
            first.deinit();
            first_live = false; // All following source/root operations are independent.
            record = scope2.?.beginRegisteredWriterWithAdmission(&backend2, &lease.?, .{}, original) catch |cause| break :blk cause;
            view = record.?.takeRunWithAdmission(original) catch |cause| break :blk cause;
            record.?.requestRetirement();
            record = null;
            if (stage == .publish) {
                publication = try scope2.?.preparePublicationWithAdmission(original);
                {
                    try std.testing.expect(backend2.mu.tryLock());
                    defer backend2.mu.unlock();
                    try backend2.capturePreparedPublicationWithAdmission(publication.?, &backend2.registered_publication, backend2.run_directory_generation, original);
                }
                try publication.?.appendPutWithAdmission(&lease.?, &view.?, original);
                try publication.?.resumeWithAdmission(original);
                {
                    try std.testing.expect(backend2.mu.tryLock());
                    defer backend2.mu.unlock();
                    try backend2.installRegisteredPublicationWithAdmission(publication.?, original);
                }
                publication = null; // Installation requests retirement of the frame.
                var read = try backend2.beginReadWithAdmission(original);
                defer read.abort();
                try std.testing.expect(read.read_view.version.?.registered);
                for (0..3) |ordinal| {
                    key[key.len - 1] = @intCast(ordinal);
                    try std.testing.expectEqualStrings("owned output", try read.getWithAdmission(.{ .name = namespace }, &key, original));
                }
            }
            var empty: Scope.WriterStore = .{};
            candidate = scope2.?.beginRegisteredWriterRootWithAdmission(&empty, &view.?, false, original) catch |cause| break :blk cause;
            candidate.?.resumeWithAdmission(original) catch |cause| break :blk cause;
            root = candidate.?.takeWithAdmission(original) catch |cause| break :blk cause;
            candidate.?.requestRetirement();
            candidate = null;
            view.?.deinit(a2);
            view = null;
            lease.?.deinit(a2);
            lease = null;
            backend2.close();
            backend2_live = false;
            scope2.?.requestRetirement();
            scope2 = null;
            try std.testing.expectEqual(@as(usize, 1), root.?.count());
            try std.testing.expectEqual(@as(u8, 2), root.?.at(0).largest_key[key.len - 1]);
            if (stage == .retire) {
                allocation_start = selected.alloc_index;
                control.live = true;
                root_cursor = .init(root.?);
                root.?.tree.root = null;
                while (true) {
                    original.checkpoint() catch |cause| break :blk cause;
                    var steps: usize = 1;
                    if (root_cursor.?.step(a2, &steps)) break;
                }
                original.checkpoint() catch |cause| break :blk cause;
                root_cursor.?.finish(a2);
                root_cursor = null;
                root = null;
                while (second.resource_manager.payload_retirement_ready.load(.acquire) != 0) _ = second.resource_manager.drainPayloadRetirementWithAdmission(original) catch |cause| break :blk cause;
            }
            break :blk {};
        };
        const stats: Stats = .{ .checks = control.calls, .allocations = selected.alloc_index - allocation_start };
        control.live = false;
        if (oom) {
            try std.testing.expect(selected.has_induced_failure);
            try std.testing.expectError(error.OutOfMemory, result);
        } else if (stage == .producer_invalid) try std.testing.expectError(error.TableFileTooLarge, result) else if (stage == .producer_mismatch) try std.testing.expectError(error.ResourceAccountingMismatch, result) else if (stage == .producer_abort or stage == .producer_borrow_abort) try std.testing.expectError(error.Canceled, result) else if (target != null) try std.testing.expectError(control.failure, result) else if (stage == .domain) try std.testing.expectError(error.ExecutionWaitUnsupported, result) else try result;
        if (stage == .handoff and target != null) {
            const bound = writer.output_target;
            if (output) |owner| owner.requestRetirement();
            output = null;
            _ = first.resource_manager.drainPayloadRetirementQuiesced();
            if (bound) |destination| {
                try std.testing.expectEqual(@as(usize, 2), destination.registered.?.refs.load(.acquire));
                try std.testing.expect(!destination.registered.?.claimed);
                if (oom) try writer.abortWithAdmission(original) else try writer.takeResultInto(destination, original);
            }
        }
        if (producer) |owner| {
            if (stage == .producer_abort) try std.testing.expect(writer.file.pending != null);
            owner.requestRetirement(); // Final access to the registered frame.
            producer = null;
            if (stage == .producer_queue_join) {
                // The frame appends its ticket while the queue joins registered
                // cleanup. Queue teardown must consume that late handoff.
                queue.deinit();
                try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
            } else {
                while (first.resource_manager.payload_retirement_ready.load(.acquire) != 0) _ = try first.resource_manager.drainPayloadRetirementWithAdmission(original);
                try std.testing.expectEqual(@as(usize, 1), queue.pending.load(.acquire));
                const debt = (try queue.popWithAdmission(original)).?;
                try std.testing.expectEqualStrings(path, debt.path);
                // The isolated temporary-directory owner accepts deletion debt.
                try debt.retireWithAdmission(original);
                while (first.resource_manager.payload_retirement_ready.load(.acquire) != 0) _ = try first.resource_manager.drainPayloadRetirementWithAdmission(original);
                try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
            }
            try std.testing.expectError(error.FileNotFound, native.storage().fileSize(path));
        }
        if (offered_grant) |*grant| try grant.retireWithAdmission(original);
        offered_grant = null;
        if (offered_ticket) |ticket| ticket.destroy();
        offered_ticket = null;
        if (writer_live) {
            if (writer.result_taken) try writer.retireWithAdmission(original) else try writer.abortWithAdmission(original);
            writer.deinit();
            writer_live = false;
        }
        if (fixed) |*credit| credit.release();
        fixed = null;
        RegisteredSourceFixture.finishRoot(&root, &root_cursor, a2);
        if (view) |*owned| owned.deinit(a2);
        view = null;
        if (record) |owner| owner.requestRetirement();
        record = null;
        if (candidate) |owner| owner.requestRetirement();
        candidate = null;
        if (lease) |*owned| owned.deinit(a2);
        lease = null;
        if (scope2) |owner| owner.requestRetirement();
        scope2 = null;
        if (output) |owner| owner.requestRetirement();
        output = null;
        if (scope1) |owner| owner.requestRetirement();
        scope1 = null;
        drain(&second.resource_manager);
        if (first_live) {
            drain(&first.resource_manager);
            try std.testing.expectEqual(@as(u64, 0), first.quota_lifetimes.load(.acquire));
        }
        try std.testing.expectEqual(@as(u64, 0), second.quota_lifetimes.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), control.depth);
        failing1.fail_index = std.math.maxInt(usize);
        failing2.fail_index = std.math.maxInt(usize);
        return stats;
    }
};

test "physical cache metadata registered producer constructors retain original grants on every refusal and allocation failure" {
    const accepted = try RegisteredOutputFixture.run(.producer_create, null, false, false);
    for (0..accepted.checks) |at| _ = try RegisteredOutputFixture.run(.producer_create, at, false, false);
    for (0..accepted.allocations) |at| _ = try RegisteredOutputFixture.run(.producer_create, at, true, false);
    _ = try RegisteredOutputFixture.run(.producer_invalid, null, false, false);
    _ = try RegisteredOutputFixture.run(.producer_mismatch, null, false, false);
}

test "physical cache metadata registered producer queue joins tickets handed off during quiesced cleanup" {
    _ = try RegisteredOutputFixture.run(.producer_queue_join, null, false, false);
}

test "physical cache metadata registered unfinished producer joins rejected IO and retires abandoned input" {
    _ = try RegisteredOutputFixture.run(.producer_abort, null, false, false);
    _ = try RegisteredOutputFixture.run(.producer_borrow_abort, null, false, false);
}

test "physical cache metadata registered SST publication reads actual values after producer exit" {
    for ([_]bool{ false, true }) |static_domain| {
        _ = try RegisteredOutputFixture.run(.publish, null, false, static_domain);
    }
}

test "physical cache metadata registered builder output owns accepted bounds across producer and storage exit" {
    for ([_]bool{ false, true }) |static_domain| {
        _ = try RegisteredOutputFixture.run(.source, null, false, static_domain);
        _ = try RegisteredOutputFixture.run(.retire, null, false, static_domain);
    }
    _ = try RegisteredOutputFixture.run(.domain, null, false, false);
}

test "physical cache metadata registered builder output retains rejected producer bindings and every source retirement refusal" {
    for ([_]RegisteredOutputFixture.Stage{ .create, .handoff, .source, .retire }) |stage| {
        const count = (try RegisteredOutputFixture.run(stage, null, false, false)).checks;
        for (0..count) |at| _ = try RegisteredOutputFixture.run(stage, at, false, false);
    }
}

test "physical cache metadata registered builder output aborts every allocation failure without losing transferred credit" {
    for ([_]RegisteredOutputFixture.Stage{ .create, .handoff, .source }) |stage| {
        const count = (try RegisteredOutputFixture.run(stage, null, false, false)).allocations;
        for (0..count) |at| _ = try RegisteredOutputFixture.run(stage, at, true, false);
    }
}

test "physical cache metadata registered obsolete roots preserve exact paths deadlines and snapshots after producer exit" {
    for ([_]usize{ 1, 3, 8 }) |count| for ([_]RegisteredSourceFixture.Phase{ .construct, .retire }) |phase| {
        _ = try RegisteredSourceFixture.runExtended(false, phase, null, false, count, true);
    };
}

test "physical cache metadata registered obsolete roots retain every original edit and retirement refusal" {
    for ([_]RegisteredSourceFixture.Phase{ .construct, .retire }) |phase| {
        const count = (try RegisteredSourceFixture.runExtended(false, phase, null, false, 3, true)).checks;
        for (0..count) |at| _ = try RegisteredSourceFixture.runExtended(false, phase, at, false, 3, true);
    }
}

test "physical cache metadata registered obsolete roots retire every allocation failure while allocation stays exhausted" {
    const count = (try RegisteredSourceFixture.runExtended(false, .construct, null, false, 3, true)).allocations;
    for (0..count) |at| _ = try RegisteredSourceFixture.runExtended(false, .construct, at, true, 3, true);
}

test "physical cache metadata registered directory roots preserve all indexes and allocator guards after producer exit" {
    for ([_]bool{ false, true }) |shared| for ([_]usize{ 1, 3, 8 }) |count| for ([_]RegisteredSourceFixture.Phase{ .construct, .retire }) |phase| {
        _ = try RegisteredSourceFixture.runFull(shared, phase, null, false, count, false, true);
    };
}

test "physical cache metadata registered directory roots retain every original put removal and retirement refusal" {
    for ([_]bool{ false, true }) |shared| for ([_]RegisteredSourceFixture.Phase{ .construct, .retire }) |phase| {
        const count = (try RegisteredSourceFixture.runFull(shared, phase, null, false, 3, false, true)).checks;
        for (0..count) |at| _ = try RegisteredSourceFixture.runFull(shared, phase, at, false, 3, false, true);
    };
}

test "physical cache metadata registered directory roots retire every allocation failure while allocation stays exhausted" {
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runFull(shared, .construct, null, false, 3, false, true)).allocations;
        for (0..count) |at| _ = try RegisteredSourceFixture.runFull(shared, .construct, at, true, 3, false, true);
    }
}

test "physical cache metadata registered writer removal preserves revisions snapshots and empty roots after producer exit" {
    for ([_]bool{ false, true }) |shared| for ([_]usize{ 1, 3, 8 }) |count| for ([_]RegisteredSourceFixture.Phase{ .construct, .retire }) |phase| {
        _ = try RegisteredSourceFixture.runScenario(shared, phase, null, false, count, false, false, true, false);
    };
}

test "physical cache metadata registered writer removal retains every original constructor and retirement refusal" {
    for ([_]bool{ false, true }) |shared| for ([_]RegisteredSourceFixture.Phase{ .construct, .retire }) |phase| {
        const count = (try RegisteredSourceFixture.runScenario(shared, phase, null, false, 3, false, false, true, false)).checks;
        for (0..count) |at| _ = try RegisteredSourceFixture.runScenario(shared, phase, at, false, 3, false, false, true, false);
    };
}

test "physical cache metadata registered writer removal retries a rejected detached-node handoff exactly once" {
    var resumed: usize = 0;
    var detached: usize = 0;
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runScenario(shared, .construct, null, false, 3, false, false, true, true)).checks;
        for (0..count) |at| {
            const stats = try RegisteredSourceFixture.runScenario(shared, .construct, at, false, 3, false, false, true, true);
            resumed += @intFromBool(stats.recovered);
            detached += @intFromBool(stats.detached_recovered);
        }
    }
    try std.testing.expect(resumed != 0 and detached != 0);
}

test "physical cache metadata registered writer removal retires every allocation failure under exhausted resources" {
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runScenario(shared, .construct, null, false, 3, false, false, true, false)).allocations;
        for (0..count) |at| _ = try RegisteredSourceFixture.runScenario(shared, .construct, at, true, 3, false, false, true, false);
    }
}

test "physical cache metadata sealed publication and reader epochs keep generations coherent across producer exit" {
    for ([_]bool{ false, true }) |shared| for ([_]usize{ 1, 3, 8 }) |count| for (std.enums.values(RegisteredSourceFixture.Phase)) |phase| {
        _ = try RegisteredSourceFixture.runAll(shared, phase, null, false, count, false, false, false, false, true);
    };
}

test "physical cache metadata sealed publication and reader epochs preserve every original construction and retirement refusal" {
    for ([_]bool{ false, true }) |shared| for (std.enums.values(RegisteredSourceFixture.Phase)) |phase| {
        const count = (try RegisteredSourceFixture.runAll(shared, phase, null, false, 3, false, false, false, false, true)).checks;
        for (0..count) |at| _ = try RegisteredSourceFixture.runAll(shared, phase, at, false, 3, false, false, false, false, true);
    };
}

test "physical cache metadata sealed publication and reader epochs retire partial ownership while allocation stays exhausted" {
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runAll(shared, .construct, null, false, 3, false, false, false, false, true)).allocations;
        for (0..count) |at| _ = try RegisteredSourceFixture.runAll(shared, .construct, at, true, 3, false, false, false, false, true);
    }
}

test "physical cache metadata funded reader projections match legacy mixed range namespace and level semantics" {
    for ([_]bool{ false, true }) |shared| for ([_]usize{ 1, 3, 8 }) |count| {
        _ = try RegisteredSourceFixture.runProjectionScenario(shared, .construct, null, false, count, false, false, false, false, true, true);
        _ = try RegisteredSourceFixture.runProjectionScenario(shared, .retire, null, false, count, false, false, false, false, true, true);
    };
}

test "physical cache metadata funded reader projection resumes the same partial builder after original refusal" {
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runAll(shared, .construct, null, false, 3, false, false, false, true, true)).checks;
        var resumed: usize = 0;
        for (0..count) |at| {
            const result = try RegisteredSourceFixture.runAll(shared, .construct, at, false, 3, false, false, false, true, true);
            resumed += @intFromBool(result.recovered);
        }
        try std.testing.expect(resumed != 0);
    }
}

test "physical cache metadata multi edit publication installs one coherent tuple across pinned generations" {
    for ([_]bool{ false, true }) |shared| {
        for ([_]usize{ 1, 3, 8 }) |count| {
            for (std.enums.values(RegisteredSourceFixture.Phase)) |phase| {
                _ = try RegisteredSourceFixture.runBatch(shared, phase, null, false, count);
            }
        }
    }
}

test "physical cache metadata multi edit publication preserves every original construction and retirement refusal" {
    for ([_]bool{ false, true }) |shared| {
        for (std.enums.values(RegisteredSourceFixture.Phase)) |phase| {
            const count = (try RegisteredSourceFixture.runBatch(shared, phase, null, false, 1)).checks;
            for (0..count) |at| _ = try RegisteredSourceFixture.runBatch(shared, phase, at, false, 1);
        }
    }
}

test "physical cache metadata multi edit publication resumes the same displaced cleanup after refusal" {
    var resumed: usize = 0;
    var displaced: usize = 0;
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runBatchRetry(shared, null)).checks;
        for (0..count) |at| {
            const result = try RegisteredSourceFixture.runBatchRetry(shared, at);
            resumed += @intFromBool(result.recovered);
            displaced += @intFromBool(result.detached_recovered);
        }
    }
    try std.testing.expect(resumed != 0 and displaced != 0);
}

test "physical cache metadata multi edit publication retires every allocation failure under exhaustion" {
    for ([_]bool{ false, true }) |shared| {
        const count = (try RegisteredSourceFixture.runBatch(shared, .construct, null, false, 1)).allocations;
        for (0..count) |at| _ = try RegisteredSourceFixture.runBatch(shared, .construct, at, true, 1);
    }
}

const FlushBuilderFixture = struct {
    const local = @import("antfly_local_sources");
    const lsm = local.storage_lsm_backend_mod;
    const state_mod = local.storage_lsm_backend_state;
    const table = local.storage_lsm_table_file;
    const io_mod = local.storage_lsm_backend_storage_io;
    const Scope = lsm.Backend.PublicationResources;
    const Cleanup = lsm.repository.OutputCleanup;
    const Stats = struct { checks: usize, allocations: usize, observed: bool };
    const Mode = enum { entries, bytes, physical, partition, callback, exhausted, oversized, queue_join, pending_join };
    fn partition(key: []const u8, original: read_admission.Port) ![]const u8 {
        try original.checkpoint();
        return key;
    }
    /// Native completion may add or remove polling checkpoints on each
    /// trial. Enumerate the actual execution until a successful trial ends
    /// before its target, rather than assuming a previous run's check count.
    fn sweepOriginal(abort_on_error: bool, mode: Mode) !void {
        var at: usize = 0;
        while (true) : (at += 1) {
            const trial = try runMode(at, false, abort_on_error, mode);
            if (!trial.observed) {
                try std.testing.expect(at >= trial.checks);
                return;
            }
        }
    }
    fn run(target: ?usize, oom: bool, abort_on_error: bool) !Stats {
        return runMode(target, oom, abort_on_error, .entries);
    }
    fn runMode(target: ?usize, oom: bool, abort_on_error: bool, mode: Mode) !Stats {
        const a = std.testing.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const owned = failing.allocator();
        var parent = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent.deinit(a);
        var budgets = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets.options.identity_allocator = owned;
        var resources = try Resources.initWithBudgetsAndParent(owned, budgets, parent.memoryParentPort());
        defer resources.deinit();
        resources.attachResourceManager();
        var queue: Cleanup.Queue = .{ .allocator = owned, .manager = &resources.resource_manager };
        defer queue.deinit();
        var pool = io_mod.NativeStoragePool.initWithCapacityForTest(a, 8);
        defer pool.deinit();
        var native = try io_mod.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/flush-builder", .{tmp.sub_path});
        defer a.free(root);
        const runs = try std.fmt.allocPrint(a, "{s}/runs", .{root});
        defer a.free(runs);
        try native.storage().createDirPath(runs);
        var scope: ?*Scope = try Scope.start(owned, &resources.resource_manager, quotaControl());
        defer {
            if (scope) |value| value.requestRetirement();
            RegisteredOutputFixture.drain(&resources.resource_manager);
        }
        try scope.?.resumeWithAdmission(quotaControl());
        var states: [2]state_mod.State = @splat(.{});
        var inputs_live = true;
        defer if (inputs_live) for (&states) |*state| state.deinit(a);
        var active: state_mod.ActiveMemTable = .{};
        defer active.deinit(a);
        for (0..5) |ordinal| {
            var key: [32777]u8 = @splat('k');
            key[key.len - 1] = @intCast(ordinal);
            if (ordinal < 4) try active.upsert(a, .{ .name = "docs" }, &key, if (ordinal == 2) "" else "new", ordinal == 2);
            if (ordinal == 1 or ordinal == 2 or ordinal == 4) try states[1].upsert(a, .{ .name = "docs" }, &key, "old", false);
        }
        states[0] = try active.toStateMove(a);
        const sources = [_]*const state_mod.State{ &states[0], &states[1] };
        var job: ?*Scope.FlushBuilder.Owner = null;
        defer if (job) |value| value.requestRetirement();
        var control: RegisteredOutputFixture.Control = .{ .live = true, .target = if (oom) null else target, .failure = switch ((target orelse 0) % 4) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            2 => error.ParentMemoryBusy,
            else => error.FileNotFound,
        } };
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        const start = failing.alloc_index;
        if (oom) failing.fail_index = start + target.?;
        var observed = false;
        var terminal_error = false;
        var config: Scope.FlushBuilder.Config = .{ .first_id = 501, .end_id = 601, .target_bytes = 0, .max_entries = 2, .max_file_bytes = 1 << 20 };
        switch (mode) {
            .bytes => {
                config.max_entries = 32;
                config.target_bytes = 50 * 1024;
            },
            .physical => {
                config.max_entries = 32;
                config.max_file_bytes = 180 * 1024;
            },
            .partition => {
                config.max_entries = 32;
                config.partition_prefix_bytes = 32777;
            },
            .callback => {
                config.max_entries = 32;
                config.partition_key = partition;
            },
            .exhausted => config.end_id = 502,
            .oversized => {
                config.max_entries = 32;
                config.max_file_bytes = 48 * 1024;
            },
            else => {},
        }
        const expected_files: usize = switch (mode) {
            .bytes, .physical, .partition, .callback => 5,
            else => 3,
        };
        const result: anyerror!void = blk: {
            while (job == null) {
                job = scope.?.beginFlushBuilderWithAdmission(&queue, root, &sources, config, original) catch |cause| {
                    try std.testing.expectError(if (oom) error.OutOfMemory else control.failure, @as(anyerror!void, cause));
                    // Exactly one rejection is injected. A repeated physical
                    // failure must fail the test rather than retry forever.
                    try std.testing.expect(!observed);
                    observed = true;
                    failing.fail_index = std.math.maxInt(usize);
                    if (abort_on_error) break :blk {};
                    continue;
                };
            }
            if (mode == .queue_join) {
                try std.testing.expectEqual(@as(usize, 1), queue.live.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
                break :blk {};
            }
            while (true) {
                job.?.driveWithAdmission(native.storage(), original) catch |cause| {
                    if (mode == .exhausted or mode == .oversized) {
                        try std.testing.expectError(if (mode == .exhausted) error.FlushRunIdReservationExhausted else error.TableFileTooLarge, @as(anyerror!void, cause));
                        terminal_error = true;
                        break :blk {};
                    }
                    try std.testing.expectError(if (oom) error.OutOfMemory else control.failure, @as(anyerror!void, cause));
                    // Exactly one rejection is injected. A repeated physical
                    // failure must fail the test rather than retry forever.
                    try std.testing.expect(!observed);
                    observed = true;
                    failing.fail_index = std.math.maxInt(usize);
                    if (abort_on_error) break :blk {};
                    continue;
                };
                break;
            }
            if (mode == .pending_join) break :blk {};
            try std.testing.expectEqual(@as(usize, 7), job.?.consumed);
            try std.testing.expectEqual(@as(usize, 5), job.?.emitted);
            for (&states) |*state| state.deinit(a);
            inputs_live = false; // Completion/take/retirement must not read inputs.
            var ordinal: usize = 0;
            var files: usize = 0;
            while (true) {
                const completed = job.?.takeCompletedWithAdmission(original) catch |cause| {
                    try std.testing.expectError(if (oom) error.OutOfMemory else control.failure, @as(anyerror!void, cause));
                    // Exactly one rejection is injected. A repeated physical
                    // failure must fail the test rather than retry forever.
                    try std.testing.expect(!observed);
                    observed = true;
                    failing.fail_index = std.math.maxInt(usize);
                    if (abort_on_error) break :blk {};
                    continue;
                } orelse break;
                // The isolated temporary-directory owner now accepts every
                // final-file deletion obligation. This is no manifest ACK.
                completed.ticket.destroy();
                defer completed.output.requestRetirement();
                try std.testing.expectEqual(501 + files, completed.id);
                try std.testing.expectEqual(@as(u64, @intCast(500 + expected_files)), completed.visibility_id);
                const output = &completed.output.output;
                const bytes = try native.storage().readFileAlloc(a, output.path[0..output.path_len], 1 << 20);
                defer a.free(bytes);
                var decoded = try table.decodeAlloc(a, bytes);
                defer decoded.deinit(a);
                try std.testing.expectEqual(completed.rows, decoded.entries.len);
                var tombstones: u32 = 0;
                for (decoded.entries) |entry| {
                    try std.testing.expectEqualStrings("docs", entry.namespace_name.?);
                    try std.testing.expectEqual(@as(u8, @intCast(ordinal)), entry.key[entry.key.len - 1]);
                    try std.testing.expectEqual(ordinal == 2, entry.tombstone);
                    try std.testing.expectEqualStrings(if (ordinal == 2) "" else if (ordinal == 4) "old" else "new", entry.value);
                    tombstones += @intFromBool(entry.tombstone);
                    ordinal += 1;
                }
                try std.testing.expectEqual(completed.tombstones, tombstones);
                files += 1;
            }
            try std.testing.expectEqual(@as(usize, 5), ordinal);
            try std.testing.expectEqual(expected_files, files);
        };
        try result;
        if (mode == .exhausted or mode == .oversized) try std.testing.expect(terminal_error);
        const stats: Stats = .{ .checks = control.calls, .allocations = failing.alloc_index - start, .observed = observed };
        control.live = false;
        failing.fail_index = std.math.maxInt(usize);
        if (target) |at| if (!observed) {
            // runMode verified the complete row/file result before reaching
            // here. A target actually encountered above must reject exactly
            // once; only an unreachable tail is the sweep's stopping receipt.
            try std.testing.expect(!oom and at >= control.calls);
        };
        if (oom) try std.testing.expect(failing.has_induced_failure);
        if (job) |value| value.requestRetirement();
        job = null;
        if (inputs_live) {
            for (&states) |*state| state.deinit(a);
            inputs_live = false;
        }
        if (scope) |value| value.requestRetirement();
        scope = null;
        if (mode == .queue_join) queue.deinit();
        if (mode == .pending_join) {
            try std.testing.expectError(error.ResourceBudgetExceeded, queue.joinRetirementsWithAdmission(std.testing.io, original));
            try std.testing.expect(queue.pending.load(.acquire) != 0);
        }
        while (resources.resource_manager.payload_retirement_ready.load(.acquire) != 0) _ = try resources.resource_manager.drainPayloadRetirementWithAdmission(original);
        while (try queue.popWithAdmission(original)) |ticket| try ticket.retireWithAdmission(original);
        RegisteredOutputFixture.drain(&resources.resource_manager);
        try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), control.depth);
        return stats;
    }
};

test "physical cache metadata flush builder merges real states into owned completed files" {
    _ = try FlushBuilderFixture.run(null, false, false);
}

test "physical cache metadata flush builder resumes every original stage refusal without replay" {
    try FlushBuilderFixture.sweepOriginal(false, .entries);
}

test "physical cache metadata flush builder resumes every allocation failure with partial ownership" {
    const accepted = try FlushBuilderFixture.run(null, false, false);
    for (0..accepted.allocations) |at| _ = try FlushBuilderFixture.run(at, true, false);
}

test "physical cache metadata flush builder aborts every original stage while inputs leave before cleanup" {
    try FlushBuilderFixture.sweepOriginal(true, .entries);
}

test "physical cache metadata flush builder honors byte physical and partition boundaries" {
    for ([_]FlushBuilderFixture.Mode{ .bytes, .physical, .partition, .callback, .exhausted, .oversized }) |mode| _ = try FlushBuilderFixture.runMode(null, false, false, mode);
}

test "physical cache metadata flush builder retains long partition comparisons across every original refusal" {
    try FlushBuilderFixture.sweepOriginal(false, .callback);
}

test "physical cache metadata flush builder retains queue before first ticket and reports late admitted debt" {
    _ = try FlushBuilderFixture.runMode(null, false, false, .queue_join);
    _ = try FlushBuilderFixture.runMode(null, false, false, .pending_join);
}

const FlushPublicationFixture = struct {
    const local = @import("antfly_local_sources");
    const lsm = local.storage_lsm_backend_mod;
    const state_mod = local.storage_lsm_backend_state;
    const io_mod = local.storage_lsm_backend_storage_io;
    const Scope = lsm.Backend.PublicationResources;
    const Batch = Scope.FlushOutputs;
    const Builder = Scope.FlushBuilder.Owner;
    const Control = RegisteredOutputFixture.Control;
    const Mode = enum { controlled_writer, legacy, publish, missing, stale, identity, empty, ledger, ledger_mixed, manifest, checkpoint, checkpoint_abort, checkpoint_cancel, checkpoint_rename, checkpoint_stale, checkpoint_lineage, checkpoint_install, checkpoint_repeat, checkpoint_install_reject, wal_retire, wal_replay, wal_prefix, wal_cancel, wal_failure, wal_resume, wal_delete_failure, wal_floor_failure, wal_legacy_replay, immutable_retire, immutable_prefix, immutable_cancel, file_snapshot };
    const Stats = struct { checks: usize, allocations: usize, observed: bool };
    const Retry = struct {
        failing: *std.testing.FailingAllocator,
        control: *Control,
        oom: bool,
        abort_on_error: bool,
        observed: bool = false,
        fn call(self: *@This(), comptime operation: anytype, args: anytype) anyerror!@typeInfo(@typeInfo(@TypeOf(operation)).@"fn".return_type.?).error_union.payload {
            while (true) {
                return @call(.auto, operation, args) catch |cause| {
                    try std.testing.expectError(if (self.oom) error.OutOfMemory else self.control.failure, @as(anyerror!void, cause));
                    try std.testing.expect(!self.observed);
                    self.observed = true;
                    if (self.abort_on_error) return cause;
                    self.failing.fail_index = std.math.maxInt(usize);
                    continue;
                };
            }
        }
    };
    const Owners = struct {
        scope: ?*Scope = null,
        window: ?lsm.Backend.QuiescedImmutableFlush = null,
        window_credit: ?@import("antfly_local_sources").storage_resource_manager.Reservation = null,
        immutable_pins: ?[]*const state_mod.State = null,
        checkpoint: ?*Scope.RegisteredCheckpoint = null,
        reservation: ?lsm.Backend.RegisteredCheckpointReservation = null,
        crashed: bool = false,
        builder: ?*Builder = null,
        other_builder: ?*Builder = null,
        batch: ?*Batch = null,
        other_batch: ?*Batch = null,
        file_snapshots: [2]?*Scope.FileSnapshot.Owner = @splat(null),
        frame: ?*Scope.Publication.Candidate = null,
        views: [2]?lsm.repository.Run = @splat(null),
        entries: [2]?Scope.EntryLease = @splat(null),
        installed: bool = false,
        done: bool = false,
        abandoned: usize = 0,
        fn finish(self: *@This(), backend: *lsm.Backend, manager: *ResourceManager, native: io_mod.Storage, failing: *std.testing.FailingAllocator) !usize {
            if (self.done) return self.abandoned;
            if (self.window) |*window| {
                try std.testing.expect(backend.mu.tryLock());
                window.deinit();
                backend.mu.unlock();
                self.window = null;
                self.batch = null;
                self.builder = null;
            }
            if (self.window_credit) |*credit| credit.release();
            self.window_credit = null;
            if (self.immutable_pins) |pins| backend.releaseImmutableMemtableSnapshot(pins);
            self.immutable_pins = null;
            for (&self.file_snapshots) |*slot| if (slot.*) |snapshot| {
                snapshot.requestRetirement();
                slot.* = null;
            };
            if (self.checkpoint) |owner| owner.requestRetirement();
            self.checkpoint = null;
            if (self.batch) |owner| owner.requestRetirement();
            self.batch = null;
            if (self.other_batch) |owner| owner.requestRetirement();
            self.other_batch = null;
            if (self.builder) |owner| owner.requestRetirement();
            self.builder = null;
            if (self.other_builder) |owner| owner.requestRetirement();
            self.other_builder = null;
            if (self.frame) |owner| owner.requestRetirement();
            self.frame = null;
            for (&self.views) |*slot| if (slot.*) |*view| {
                view.deinit(backend.allocator);
                slot.* = null;
            };
            for (&self.entries) |*slot| if (slot.*) |*entry| {
                entry.deinit(backend.allocator);
                slot.* = null;
            };
            if (self.scope) |owner| owner.requestRetirement();
            self.scope = null;
            // Abort cleanup must not allocate even when the fault allocator
            // remains exhausted. Pinned live roots may defer output retirement.
            for (0..512) |_| _ = manager.drainPayloadRetirementQuiesced();
            failing.fail_index = std.math.maxInt(usize);
            const queue = backend.options.unpublished_outputs.?;
            if (self.reservation != null) try std.testing.expectEqual(queue.pending.load(.acquire), queue.live.load(.acquire));
            while (queue.pop()) |ticket| {
                defer ticket.destroy();
                self.abandoned += 1;
                // The isolated test consumer accepts pre-install deletion
                // obligations. A live file is always preserved on failure.
                if (!self.installed) try native.deleteFileAbsolute(ticket.path);
            }
            if (self.reservation) |*reservation| {
                try std.testing.expect(backend.mu.tryLock());
                defer backend.mu.unlock();
                reservation.deinit();
            }
            self.reservation = null;
            if (self.crashed) backend.abandonAfterCrash() else backend.close();
            self.done = true;
            RegisteredOutputFixture.drain(manager);
            return self.abandoned;
        }
    };
    fn build(scope: *Scope, queue: *lsm.repository.OutputCleanup.Queue, storage: io_mod.Storage, root: []const u8, empty: bool, foreign: bool, original: read_admission.Port) !*Builder {
        const a = std.testing.allocator;
        const path = try std.fmt.allocPrint(a, "{s}/runs", .{root});
        defer a.free(path);
        try storage.createDirPath(path);
        var state: state_mod.State = .{};
        defer state.deinit(a);
        if (!empty) {
            try state.upsert(a, .{ .name = null }, "a", if (foreign) "foreign" else "one", false);
            try state.upsert(a, .{ .name = null }, "b", if (foreign) "foreign" else "two", false);
        }
        const states = [_]*const state_mod.State{&state};
        const job = try scope.beginFlushBuilderWithAdmission(queue, root, &states, .{ .first_id = 501, .end_id = 510, .target_bytes = 0, .max_entries = 1, .max_file_bytes = 1 << 20 }, original);
        errdefer job.requestRetirement();
        try job.driveWithAdmission(storage, original);
        return job; // Completed construction no longer reads state/the vector.
    }
    fn run(mode: Mode, target: ?usize, oom: bool, abort_on_error: bool) !Stats {
        const a = std.testing.allocator;
        const immutable_mode = mode == .immutable_retire or mode == .immutable_prefix or mode == .immutable_cancel;
        const input_count: usize = if (mode == .immutable_retire) 2 else 3;
        const wal_mode = switch (mode) {
            .wal_retire, .wal_replay, .wal_prefix, .wal_cancel, .wal_failure, .wal_resume, .wal_delete_failure, .wal_floor_failure, .wal_legacy_replay => true,
            else => false,
        };
        const checkpoint_mode = switch (mode) {
            .checkpoint, .checkpoint_abort, .checkpoint_cancel, .checkpoint_rename, .checkpoint_stale, .checkpoint_lineage, .checkpoint_install, .checkpoint_repeat, .checkpoint_install_reject, .wal_retire, .wal_replay, .wal_prefix, .wal_cancel, .wal_failure, .wal_resume, .wal_delete_failure, .wal_floor_failure, .wal_legacy_replay, .immutable_retire, .immutable_prefix, .immutable_cancel => true,
            else => false,
        };
        const install_checkpoint = mode == .checkpoint_install or mode == .checkpoint_repeat or mode == .checkpoint_install_reject or wal_mode or immutable_mode;
        var expected_logical: u64 = 17;
        const late = mode == .controlled_writer or mode == .legacy or mode == .ledger or mode == .ledger_mixed or mode == .manifest or checkpoint_mode or mode == .file_snapshot;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const owned = failing.allocator();
        var parent = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent.deinit(a);
        var budgets = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets.options.identity_allocator = owned;
        var resources = try Resources.initWithBudgetsAndParent(owned, budgets, parent.memoryParentPort());
        defer resources.deinit();
        resources.attachResourceManager();
        var pool = io_mod.NativeStoragePool.initWithCapacityForTest(a, 8);
        defer pool.deinit();
        var native = try io_mod.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/flush-publication", .{tmp.sub_path});
        defer a.free(root);
        var backend = lsm.Backend.init(owned, .{ .resource_manager = &resources.resource_manager, .storage = native.storage(), .wal_enabled = false });
        var backend_live = true;
        defer if (backend_live) backend.close();
        try backend.initOutputCleanup();
        const queue = backend.options.unpublished_outputs.?;
        try std.testing.expect(backend.mu.tryLock());
        const planned = backend.planningDirectory();
        backend.mu.unlock();
        const directory = try (try planned).fork(owned);
        defer directory.destroy(owned);
        try directory.tree.prepare(owned);
        var owners: Owners = .{};
        var control: Control = .{};
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        defer {
            control.live = false;
            if (!owners.done) _ = owners.finish(&backend, &resources.resource_manager, native.storage(), &failing) catch @panic("flush publication fixture cleanup failed");
            backend_live = false;
        }
        owners.scope = try backend.beginPublicationResourcesWithAdmission(original);
        try owners.scope.?.resumeWithAdmission(original);
        if (mode == .empty) {
            // Empty immutable inputs are rejected by the builder before an
            // output exists; the publication handoff must not hide that error.
            try std.testing.expectError(error.EmptyRun, build(owners.scope.?, queue, native.storage(), root, true, false, original));
            try std.testing.expectEqual(@as(usize, 0), try owners.finish(&backend, &resources.resource_manager, native.storage(), &failing));
            backend_live = false;
            try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
            return .{ .checks = 0, .allocations = 0, .observed = false };
        }
        if (immutable_mode) {
            owners.crashed = true;
            backend.root_dir = try owned.dupe(u8, root);
            backend.options.flush_threshold = 1000;
            backend.options.defer_flush_on_commit = true;
            backend.next_run_id = 501;
            const runs_path = try std.fmt.allocPrint(a, "{s}/runs", .{root});
            defer a.free(runs_path);
            try native.storage().createDirPath(runs_path);
            for (0..input_count) |generation| {
                var txn = try backend.beginWrite();
                try txn.put(.{ .name = null }, if (generation == 0) "a" else if (generation == 1) "b" else "c", if (generation == 0) "one" else if (generation == 1) "two" else "tail");
                try txn.commit();
                try backend.prepareCurrentScanSnapshot();
            }
            try std.testing.expectEqual(input_count, backend.activeImmutableMemtableCount());
            // Actual WAL range identities remain attached even with WAL off
            // in this fixture; the retirement decision must preserve the tail.
            backend.immutable_wal_ranges.items[0] = .{ .first = 11, .last = 11 };
            backend.immutable_wal_ranges.items[1] = .{ .first = 12, .last = 12 };
            if (input_count == 3) backend.immutable_wal_ranges.items[2] = .{ .first = 13, .last = 13 };
            owners.immutable_pins = try backend.snapshotImmutableMemtables();
            owners.window_credit = try resources.resource_manager.reserveWithoutReclaimWithAdmission(.lsm_table_builder_working_set, @sizeOf(lsm.Backend.QuiescedImmutableFlush), original);
            owners.window = .{};
            try std.testing.expect(backend.mu.tryLock());
            const captured = owners.window.?.captureWithAdmission(&backend, 2, original);
            backend.mu.unlock();
            try std.testing.expect(try captured);
            var competing: lsm.Backend.QuiescedImmutableFlush = .{};
            try std.testing.expectError(error.ResourceBudgetExceeded, competing.captureWithAdmission(&backend, 1, original));
            try owners.window.?.buildWithAdmission(.{ .first_id = 0, .end_id = 0, .target_bytes = 0, .max_entries = 1, .max_file_bytes = 1 << 20 }, original);
            owners.builder = owners.window.?.builder;
            owners.batch = owners.window.?.batch;
        } else owners.builder = try build(owners.scope.?, queue, native.storage(), root, false, false, original);
        var start = failing.alloc_index;
        control.live = true;
        control.target = if (oom or late) null else target;
        control.failure = switch ((target orelse 0) % 4) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            2 => error.ParentMemoryBusy,
            else => error.FileNotFound,
        };
        if (oom and !late) failing.fail_index = start + target.?;
        var retry: Retry = .{ .failing = &failing, .control = &control, .oom = oom, .abort_on_error = abort_on_error };
        var allocations: usize = 0;
        const result: anyerror!void = work: {
            if (!immutable_mode) owners.batch = retry.call(Scope.beginFlushOutputsWithAdmission, .{ owners.scope.?, owners.builder.?, original }) catch |cause| break :work cause;
            try std.testing.expectError(error.InvalidArgument, owners.builder.?.takeCompletedWithAdmission(quotaControl()));
            try std.testing.expectError(error.InvalidArgument, owners.builder.?.takeCompletedForOwnerWithAdmission(&control, quotaControl()));
            retry.call(Batch.collectWithAdmission, .{ owners.batch.?, original }) catch |cause| break :work cause;
            allocations = failing.alloc_index - start;
            failing.fail_index = std.math.maxInt(usize);
            control.live = false;
            if (!immutable_mode) {
                owners.builder.?.requestRetirement();
                owners.builder = null;
                // Two ticket guards plus the batch guard remain. The builder
                // releases its producer guard after destroying its header, so this
                // proves subsequent metadata construction cannot borrow the builder.
                for (0..512) |_| {
                    if (queue.live.load(.acquire) == 3) break;
                    _ = resources.resource_manager.drainPayloadRetirementQuiesced();
                }
                try std.testing.expectEqual(@as(usize, 3), queue.live.load(.acquire));
            }
            if (mode == .legacy or mode == .controlled_writer) {
                start = failing.alloc_index;
                control.calls = 0;
                control.target = if (oom) null else target;
                control.live = true;
                if (oom) failing.fail_index = start + target.?;
                retry.call(Batch.prepareLegacyWithAdmission, .{ owners.batch.?, &backend, original }) catch |cause| break :work cause;
                var result = if (mode == .controlled_writer)
                    retry.call(Batch.takeLegacyRunsForPublicationWithAdmission, .{ owners.batch.?, original }) catch |cause| break :work cause
                else
                    retry.call(Batch.takeLegacyRunsWithAdmission, .{ owners.batch.?, original }) catch |cause| break :work cause;
                defer {
                    if (result.retirement) |retirement| {
                        if (owners.batch) |batch| {
                            owners.batch = null;
                            batch.requestRetirement();
                        }
                        retirement.returnLegacyRuns(&result.runs, owners.installed);
                        while (true) {
                            var credits: usize = 1;
                            if (retirement.stepWithAdmission(owned, &credits, null) catch unreachable) break;
                        }
                        retirement.finishLegacyReturn();
                    } else {
                        for (result.runs.items) |*projected| projected.deinit(owned);
                        result.runs.deinit(owned);
                        if (result.credit) |*credit| credit.release();
                    }
                }
                if (mode == .controlled_writer) {
                    var candidate = backend.runs.fork();
                    defer candidate.deinit(owned);
                    for (result.runs.items) |*projected| {
                        var insertion = retry.call(Scope.WriterStore.OutputInsertion.init, .{ &candidate, owned, projected, original }) catch |cause| break :work cause;
                        defer insertion.deinit(owned);
                        retry.call(Scope.WriterStore.OutputInsertion.resumeWithAdmission, .{ &insertion, owned, original }) catch |cause| break :work cause;
                        var held = retry.call(Scope.WriterStore.OutputInsertion.takeWithAdmission, .{ &insertion, original }) catch |cause| break :work cause;
                        held.deinit(owned);
                    }
                    // The returned batch, not this publication scope, commits
                    // tickets while retiring its loaned vector.
                    std.mem.swap(Scope.WriterStore, &backend.runs, &candidate);
                }
                allocations = failing.alloc_index - start;
                control.live = false;
                failing.fail_index = std.math.maxInt(usize);
                try std.testing.expectEqual(@as(usize, 2), result.runs.items.len);
                // Ticket-bearing projections must never enter coupled roots.
                if (mode != .controlled_writer) for (result.runs.items) |*projected| try std.testing.expectError(error.ExecutionWaitUnsupported, Scope.RegisteredWriter.validate(projected, quotaControl()));
                if (mode != .controlled_writer) try local.storage_lsm_backend_compaction.appendBackendRuns(&backend, &result.runs);
                owners.installed = true;
                owners.batch.?.requestRetirement();
                owners.batch = null;
                for (0..512) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
                if (result.retirement) |retirement| {
                    try std.testing.expect(!retirement.registration.ready.load(.acquire));
                    try std.testing.expect(retirement.legacy_outstanding and retirement.legacy_credit != null);
                }
                try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
                var read = try backend.beginRead();
                defer read.abort();
                try std.testing.expectEqualStrings("one", try read.get(.{ .name = null }, "a"));
                try std.testing.expectEqualStrings("two", try read.get(.{ .name = null }, "b"));
                break :work {};
            }
            if (mode == .identity) {
                const other_root = try std.fmt.allocPrint(a, "{s}-other", .{root});
                defer a.free(other_root);
                owners.other_builder = try build(owners.scope.?, queue, native.storage(), other_root, false, true, original);
                owners.other_batch = try owners.scope.?.beginFlushOutputsWithAdmission(owners.other_builder.?, original);
                try owners.other_batch.?.collectWithAdmission(original);
                owners.other_builder.?.requestRetirement();
                owners.other_builder = null;
            }
            owners.frame = try owners.scope.?.preparePublicationWithAdmission(original);
            {
                try std.testing.expect(backend.mu.tryLock());
                defer backend.mu.unlock();
                try backend.capturePreparedPublicationWithAdmission(owners.frame.?, &backend.registered_publication, backend.run_directory_generation, original);
            }
            const candidate_batch = owners.other_batch orelse owners.batch.?;
            var cursor = try candidate_batch.cursorWithAdmission(original);
            var index: usize = 0;
            if (mode == .file_snapshot) {
                start = failing.alloc_index;
                control.calls = 0;
                control.target = if (oom) null else target;
                control.live = true;
                if (oom) failing.fail_index = start + target.?;
            }
            while (retry.call(Batch.Cursor.nextWithAdmission, .{ &cursor, original }) catch |cause| break :work cause) |view| : (index += 1) {
                if (mode == .missing and index == 1) continue;
                if (mode == .file_snapshot) {
                    var source = retry.call(Scope.RegisteredOutput.Owner.acquireSourceWithAdmission, .{ view.output, view.metadata, original }) catch |cause| break :work cause;
                    defer source.releaseWithAdmission(null) catch unreachable;
                    owners.file_snapshots[index] = retry.call(Scope.beginFileSnapshotWithAdmission, .{ owners.scope.?, source.run, original }) catch |cause| break :work cause;
                    retry.call(Scope.FileSnapshot.Owner.resumeWithAdmission, .{ owners.file_snapshots[index].?, original }) catch |cause| break :work cause;
                    try source.releaseWithAdmission(null);
                    retry.call(Scope.beginEntryFromFileSnapshotWithAdmission, .{ owners.scope.?, &backend, directory, owners.file_snapshots[index].?, original }) catch |cause| break :work cause;
                    owners.file_snapshots[index].?.requestRetirement();
                    owners.file_snapshots[index] = null;
                } else try owners.scope.?.beginEntryFromRegisteredOutputWithAdmission(&backend, directory, view.output, view.metadata, original);
                owners.entries[index] = retry.call(Scope.takeEntryWithAdmission, .{ owners.scope.?, original }) catch |cause| break :work cause;
                const record = retry.call(Scope.beginRegisteredWriterWithAdmission, .{ owners.scope.?, &backend, &owners.entries[index].?, Scope.RegisteredWriter.Revision{}, original }) catch |cause| break :work cause;
                defer record.requestRetirement();
                owners.views[index] = retry.call(Scope.RegisteredWriter.takeRunWithAdmission, .{ record, original }) catch |cause| break :work cause;
                retry.call(Scope.Publication.Candidate.appendPutWithAdmission, .{ owners.frame.?, &owners.entries[index].?, &owners.views[index].?, original }) catch |cause| break :work cause;
                retry.call(Scope.Publication.Candidate.resumeWithAdmission, .{ owners.frame.?, original }) catch |cause| break :work cause;
            }
            if (mode == .file_snapshot) allocations = failing.alloc_index - start;
            control.live = true;
            retry.call(Batch.bindPublicationWithAdmission, .{ owners.batch.?, owners.frame.?, original }) catch |cause| break :work cause;
            if (mode == .missing or mode == .identity) {
                control.live = false;
                try std.testing.expectError(if (mode == .missing) error.InvalidArgument else error.CompactionPlanningStale, owners.batch.?.validatePublicationWithAdmission(original));
                break :work {};
            }
            retry.call(Batch.validatePublicationWithAdmission, .{ owners.batch.?, original }) catch |cause| break :work cause;
            if (mode == .stale) {
                control.live = false;
                const entry = &owners.entries[0].?;
                try owners.frame.?.appendRemoveWithAdmission(.{ .run = entry.entry.run, .revision = entry.entry.payload.? }, &owners.views[0].?, 1, original);
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    try std.testing.expectError(error.CompactionPlanningStale, owners.batch.?.installWithAdmission(&backend, original));
                }
                try owners.frame.?.resumeWithAdmission(original);
                try std.testing.expectError(error.CompactionPlanningStale, owners.batch.?.validatePublicationWithAdmission(original));
                try owners.batch.?.bindPublicationWithAdmission(owners.frame.?, original);
                try std.testing.expectError(error.InvalidArgument, owners.batch.?.validatePublicationWithAdmission(original));
                break :work {};
            }
            {
                try std.testing.expect(backend.mu.tryLock());
                defer backend.mu.unlock();
                if (immutable_mode) {
                    retry.call(lsm.Backend.QuiescedImmutableFlush.installWithAdmission, .{ &owners.window.?, original }) catch |cause| break :work cause;
                } else retry.call(Batch.installWithAdmission, .{ owners.batch.?, &backend, original }) catch |cause| break :work cause;
            }
            owners.installed = true;
            owners.frame = null;
            try std.testing.expect(owners.batch.?.live and backend.manifest_dirty);
            control.live = false;
            // A terminal caller cannot turn an accepted metadata swap back
            // into unpublished deletion debt. Cleanup uses independent scope.
            const Canceled = struct {
                fn check(_: *anyopaque) !void {
                    return error.Canceled;
                }
            };
            const canceled_payload: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Canceled.check, .check_lifetime = Canceled.check } };
            try std.testing.expectError(error.Canceled, owners.batch.?.installWithAdmission(&backend, .init(&canceled_payload)));
            if (!immutable_mode) {
                owners.batch.?.requestRetirement();
                owners.batch = null;
            }
            for (0..128) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
            try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
            var read = try backend.beginReadWithAdmission(original);
            defer read.abort();
            try std.testing.expectEqualStrings("one", try read.getWithAdmission(.{ .name = null }, "a", original));
            try std.testing.expectEqualStrings("two", try read.getWithAdmission(.{ .name = null }, "b", original));
            if (checkpoint_mode) {
                owners.crashed = true;
                if (!immutable_mode) backend.root_dir = try owned.dupe(u8, root);
                backend.next_run_id = 600;
                if (!immutable_mode) backend.unpublished_wal_logical_bytes = 17;
                if (immutable_mode) {
                    expected_logical = owners.window.?.logical_bytes;
                    try std.testing.expectEqual(expected_logical, backend.unpublished_wal_logical_bytes);
                    try std.testing.expectError(error.RecoveryRequired, owners.window.?.validateDurableWithAdmission(original));
                    try std.testing.expectEqual(input_count, backend.activeImmutableMemtableCount());
                }
                const lineage = lsm.repository.ManifestJournal.CheckpointFence.capture(&backend);
                start = failing.alloc_index;
                control.calls = 0;
                control.target = if (oom or wal_mode) null else target;
                control.live = true;
                if (oom) failing.fail_index = start + target.?;
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    owners.reservation = retry.call(lsm.Backend.reserveRegisteredCheckpointWithAdmission, .{ &backend, original }) catch |cause| break :work cause;
                    try std.testing.expectError(error.ResourceBudgetExceeded, backend.reserveRegisteredCheckpointWithAdmission(quotaControl()));
                    try std.testing.expectEqual(@as(u64, 601), backend.next_run_id);
                }
                owners.frame = retry.call(Scope.preparePublicationWithAdmission, .{ owners.scope.?, original }) catch |cause| break :work cause;
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    retry.call(lsm.Backend.capturePreparedPublicationWithAdmission, .{ &backend, owners.frame.?, &backend.registered_publication, backend.run_directory_generation, original }) catch |cause| break :work cause;
                }
                retry.call(Scope.Publication.Candidate.prepareUnchangedWithAdmission, .{ owners.frame.?, original }) catch |cause| break :work cause;
                var input_lineage = lineage;
                var input_root: [std.fs.max_path_bytes]u8 = undefined;
                @memcpy(input_root[0..root.len], root);
                owners.checkpoint = retry.call(Scope.beginRegisteredCheckpointWithAdmission, .{ owners.scope.?, queue, owners.frame.?, input_root[0..root.len], owners.reservation.?.id, owners.reservation.?.next_run_id, &input_lineage, original }) catch |cause| break :work cause;
                @memset(&input_root, 0xa5);
                input_lineage.journal.bytes = 999;
                try std.testing.expectError(error.InvalidArgument, owners.checkpoint.?.completionWithAdmission(quotaControl()));
                retry.call(Scope.RegisteredCheckpoint.prepareWithAdmission, .{ owners.checkpoint.?, native.storage(), original }) catch |cause| break :work cause;
                try std.testing.expect(!backend.manifest_recovery_required);
                const descriptor_path = try std.fmt.allocPrint(a, "{s}/manifest.bin", .{root});
                defer a.free(descriptor_path);
                try std.testing.expectError(error.FileNotFound, native.storage().readFileAlloc(a, descriptor_path, 4096));
                if (mode == .checkpoint_abort) {
                    allocations = failing.alloc_index - start;
                    break :work {};
                }
                if (mode == .checkpoint_lineage) {
                    control.live = false;
                    backend.manifest_journal.bytes += 1;
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    try std.testing.expectError(error.RecoveryRequired, owners.checkpoint.?.permitDescriptorWithAdmission(&backend, owners.frame.?, original));
                    try std.testing.expect(!backend.manifest_recovery_required);
                    break :work {};
                }
                if (mode == .checkpoint_stale) {
                    control.live = false;
                    const entry = &owners.entries[0].?;
                    try owners.frame.?.appendRemoveWithAdmission(.{ .run = entry.entry.run, .revision = entry.entry.payload.? }, &owners.views[0].?, 1, original);
                    try owners.frame.?.resumeWithAdmission(original);
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    try std.testing.expectError(error.CompactionPlanningStale, owners.checkpoint.?.permitDescriptorWithAdmission(&backend, owners.frame.?, original));
                    try std.testing.expect(!backend.manifest_recovery_required);
                    break :work {};
                }
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    retry.call(Scope.RegisteredCheckpoint.permitDescriptorWithAdmission, .{ owners.checkpoint.?, &backend, owners.frame.?, original }) catch |cause| break :work cause;
                }
                try std.testing.expect(backend.manifest_recovery_required and backend.manifest_journal.sequence == null);
                if (install_checkpoint and !wal_mode and !immutable_mode) {
                    backend.unpublished_wal_logical_bytes += 5;
                    expected_logical = 22;
                } else if (wal_mode or immutable_mode) {} else {
                    owners.frame.?.requestRetirement();
                    owners.frame = null;
                }
                // The snapshot owns metadata independently after permission.
                for (0..128) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
                if (mode == .checkpoint_cancel) {
                    const CheckpointCanceled = struct {
                        fn check(_: *anyopaque) !void {
                            return error.Canceled;
                        }
                    };
                    const canceled: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = CheckpointCanceled.check, .check_lifetime = CheckpointCanceled.check } };
                    try std.testing.expectError(error.Canceled, owners.checkpoint.?.finishWithAdmission(.init(&canceled)));
                    break :work {};
                }
                if (mode == .checkpoint_rename) {
                    control.live = false;
                    io_mod.NativeSyncTestControl.control_failure_after_rename.store(true, .release);
                    defer io_mod.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
                    try std.testing.expectError(error.InputOutput, owners.checkpoint.?.finishWithAdmission(original));
                    try std.testing.expectError(error.InvalidArgument, owners.checkpoint.?.completionWithAdmission(quotaControl()));
                } else {
                    retry.call(Scope.RegisteredCheckpoint.finishWithAdmission, .{ owners.checkpoint.?, original }) catch |cause| break :work cause;
                    const completion = try owners.checkpoint.?.completionWithAdmission(quotaControl());
                    try std.testing.expectEqual(@as(u64, 600), completion.descriptor.checkpoint);
                    try std.testing.expectEqual(@as(u64, 601), completion.next_run_id);
                    try std.testing.expect(completion.checkpoint_bytes > 0);
                }
                if (mode == .checkpoint_install_reject) {
                    control.live = false;
                    const entry = &owners.entries[0].?;
                    try owners.frame.?.appendRemoveWithAdmission(.{ .run = entry.entry.run, .revision = entry.entry.payload.? }, &owners.views[0].?, 1, original);
                    try owners.frame.?.resumeWithAdmission(original);
                    {
                        try std.testing.expect(backend.mu.tryLock());
                        defer backend.mu.unlock();
                        try std.testing.expectError(error.CompactionPlanningStale, backend.installRegisteredCheckpointWithAdmission(owners.checkpoint.?, owners.frame.?, original));
                    }
                    try std.testing.expect(backend.registered_manifest_checkpoint == null and backend.manifest_recovery_required);
                    try std.testing.expectEqual(@as(u64, 22), backend.unpublished_wal_logical_bytes);
                    owners.frame.?.requestRetirement();
                    owners.frame = null;
                } else if (install_checkpoint) {
                    try std.testing.expectError(error.ExecutionWaitUnsupported, lsm.Backend.QuiescedCheckpointPreparation.init(&backend));
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    retry.call(lsm.Backend.installRegisteredCheckpointWithAdmission, .{ &backend, owners.checkpoint.?, owners.frame.?, original }) catch |cause| break :work cause;
                    owners.frame = null;
                    expected_logical = if (wal_mode or immutable_mode) 0 else 5;
                    try std.testing.expect(!backend.manifest_recovery_required and !backend.manifest_dirty and !backend.obsolete_manifest_dirty);
                    try std.testing.expectEqual(expected_logical, backend.unpublished_wal_logical_bytes);
                    try std.testing.expectEqual(@as(u64, 600), backend.manifest_journal.descriptor.?.checkpoint);
                    try std.testing.expectError(error.ExecutionWaitUnsupported, backend.persistManifestLocked());
                }
                if (immutable_mode) {
                    control.live = false;
                    const window = &owners.window.?;
                    try window.validateDurableWithAdmission(original);
                    const Refuse = struct {
                        fn check(_: *anyopaque) !void {
                            return error.Canceled;
                        }
                    };
                    const refused: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Refuse.check, .check_lifetime = Refuse.check } };
                    try std.testing.expectError(error.Canceled, window.retireInputsWithAdmission(.init(&refused)));
                    try std.testing.expectEqual(input_count, backend.activeImmutableMemtableCount());
                    try std.testing.expectEqual(@as(u64, 11), backend.immutable_wal_ranges.items[backend.immutable_head].first);
                    if (mode == .immutable_prefix) {
                        backend.immutable_wal_ranges.items[backend.immutable_head].first = 99;
                        try std.testing.expectError(error.CompactionPlanningStale, window.retireInputsWithAdmission(original));
                        try std.testing.expectEqual(input_count, backend.activeImmutableMemtableCount());
                        backend.immutable_wal_ranges.items[backend.immutable_head].first = 11;
                        backend.invalidateReadVersion();
                        // This test changes memory-view identity only, keeping
                        // the exact registered SST directory unchanged.
                        backend.run_directory_dirty = false;
                        try std.testing.expectError(error.CompactionPlanningStale, window.retireInputsWithAdmission(original));
                        try window.validateDurableWithAdmission(original);
                    }
                    if (mode != .immutable_cancel) {
                        // All potentially failing allocator calls are disabled
                        // across the actual fixed head/ownership publication.
                        failing.fail_index = failing.alloc_index;
                        failing.resize_fail_index = failing.resize_index;
                        try std.testing.expect(backend.mu.tryLock());
                        const retired = window.retireInputsWithAdmission(original);
                        backend.mu.unlock();
                        try retired;
                        failing.fail_index = std.math.maxInt(usize);
                        failing.resize_fail_index = std.math.maxInt(usize);
                        try std.testing.expectEqual(input_count - 2, backend.activeImmutableMemtableCount());
                        try std.testing.expectEqual(@as(usize, 2), backend.admitted_retired_immutable_count);
                        try std.testing.expect(!backend.run_directory_dirty and backend.manifest_directory.?.tree.root == backend.run_directory.?.tree.root);
                        try std.testing.expectEqual(@as(u64, 1), backend.write_stats.immutable_flushes);
                        try std.testing.expectEqual(@as(u64, 2), backend.write_stats.immutable_flush_input_memtables);
                        const retained = backend.snapshotMaintenanceStats();
                        try std.testing.expectEqual(@as(u64, 2), retained.retired_immutable_memtables);
                        try std.testing.expect(retained.retired_immutable_bytes > 0);
                        var independent = try owners.immutable_pins.?[input_count - 1].clone(a);
                        defer independent.deinit(a);
                        try std.testing.expect(!independent.admitted_retired and independent.admitted_retired_prev == null and independent.admitted_retired_next == null);
                        if (input_count == 3) {
                            try std.testing.expectEqual(@as(u64, 13), backend.immutable_wal_ranges.items[backend.immutable_head].first);
                            var tail = try backend.beginReadWithAdmission(original);
                            defer tail.abort();
                            try std.testing.expectEqualStrings("tail", try tail.getWithAdmission(.{ .name = null }, "c", original));
                        } else try std.testing.expectEqual(@as(usize, 0), backend.immutable_head);
                    }
                    try std.testing.expectEqualStrings("one", try read.getWithAdmission(.{ .name = null }, "a", original));
                    try std.testing.expect(backend.mu.tryLock());
                    window.deinit();
                    backend.mu.unlock();
                    owners.window = null;
                    owners.builder = null;
                    owners.batch = null;
                    if (mode == .immutable_cancel) try std.testing.expectEqual(input_count, backend.activeImmutableMemtableCount());
                    backend.releaseImmutableMemtableSnapshot(owners.immutable_pins.?);
                    owners.immutable_pins = null;
                }
                allocations = failing.alloc_index - start;
                control.live = false;
                owners.checkpoint.?.requestRetirement();
                owners.checkpoint = null;
                for (0..512) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
                try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
                if (mode == .checkpoint_repeat) {
                    const retained = backend.registered_manifest_checkpoint.?;
                    try std.testing.expect(retained.physical == null and retained.metadata_borrowed.load(.acquire));
                    // Replacing this durable view must release the preceding
                    // registered snapshot rather than putting it on legacy queues.
                    {
                        try std.testing.expect(backend.mu.tryLock());
                        defer backend.mu.unlock();
                        owners.reservation.?.deinit();
                        owners.reservation = null;
                    }
                    var next_lineage = lsm.repository.ManifestJournal.CheckpointFence.capture(&backend);
                    {
                        try std.testing.expect(backend.mu.tryLock());
                        defer backend.mu.unlock();
                        owners.reservation = try backend.reserveRegisteredCheckpointWithAdmission(original);
                    }
                    owners.frame = try owners.scope.?.preparePublicationWithAdmission(original);
                    {
                        try std.testing.expect(backend.mu.tryLock());
                        defer backend.mu.unlock();
                        try backend.capturePreparedPublicationWithAdmission(owners.frame.?, &backend.registered_publication, backend.run_directory_generation, original);
                    }
                    try owners.frame.?.prepareUnchangedWithAdmission(original);
                    for (0..next_lineage.retiredPathCount()) |path_index| {
                        try owners.frame.?.appendManifestRetirementWithAdmission(&next_lineage, root, path_index, 1000, original);
                        try owners.frame.?.resumeWithAdmission(original);
                    }
                    owners.checkpoint = try owners.scope.?.beginRegisteredCheckpointWithAdmission(queue, owners.frame.?, root, owners.reservation.?.id, owners.reservation.?.next_run_id, &next_lineage, original);
                    try owners.checkpoint.?.prepareWithAdmission(native.storage(), original);
                    {
                        try std.testing.expect(backend.mu.tryLock());
                        defer backend.mu.unlock();
                        try owners.checkpoint.?.permitDescriptorWithAdmission(&backend, owners.frame.?, original);
                    }
                    try owners.checkpoint.?.finishWithAdmission(original);
                    {
                        try std.testing.expect(backend.mu.tryLock());
                        defer backend.mu.unlock();
                        try backend.installRegisteredCheckpointWithAdmission(owners.checkpoint.?, owners.frame.?, original);
                    }
                    owners.frame = null;
                    expected_logical = 0;
                    try std.testing.expectEqual(@as(u64, 601), backend.manifest_journal.descriptor.?.checkpoint);
                    try std.testing.expectEqual(@as(usize, 3), backend.obsolete_paths.count());
                    try std.testing.expectEqual(@as(u64, 0), backend.unpublished_wal_logical_bytes);
                    try std.testing.expect(!backend.manifest_dirty and !backend.obsolete_manifest_dirty);
                    owners.checkpoint.?.requestRetirement();
                    owners.checkpoint = null;
                    for (0..512) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
                    try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
                }
                if (wal_mode) {
                    const wal = local.storage_lsm_backend_wal;
                    var state: state_mod.State = .{};
                    defer state.deinit(a);
                    const large = try a.alloc(u8, 40 * 1024);
                    defer a.free(large);
                    @memset(large, 'x');
                    try state.upsert(a, .{}, "a", large, false);
                    _ = try wal.appendStateWithOptionsResult(native.storage(), a, root, &state, true, .{ .segment_bytes = 1 });
                    state.deinit(a);
                    state = .{};
                    try state.upsert(a, .{}, "b", "two", false);
                    _ = try wal.appendStateWithOptionsResult(native.storage(), a, root, &state, true, .{ .segment_bytes = 1 });
                    state.deinit(a);
                    state = .{};
                    try state.upsert(a, .{}, "a", "one", false);
                    _ = try wal.appendStateWithOptionsResult(native.storage(), a, root, &state, true, .{ .segment_bytes = 1 });
                    if (mode == .wal_replay or mode == .wal_legacy_replay) _ = try wal.appendReplay(native.storage(), a, root, 1, "retained-replay", true, .{});
                    if (mode == .wal_legacy_replay) {
                        const dedicated = try std.fmt.allocPrint(a, "{s}/wal/replay/00000000000000000001.log", .{root});
                        defer a.free(dedicated);
                        const raw = try native.storage().readFileAlloc(a, dedicated, 4096);
                        defer a.free(raw);
                        const primary = try std.fmt.allocPrint(a, "{s}/wal/00000000000000000001.log", .{root});
                        defer a.free(primary);
                        try native.storage().appendFileAbsolute(a, primary, raw, true);
                        const marker = try std.fmt.allocPrint(a, "{s}/wal/replay-main.segments", .{root});
                        defer a.free(marker);
                        try native.storage().deleteFileAbsolute(marker);
                    }
                    backend.options.wal_enabled = true;
                    try std.testing.expectError(error.ExecutionWaitUnsupported, backend.checkpointWalAfterDurableBoundary());
                    backend.options.native_storage_pool = &pool;
                    try backend.acquireRootLockState(false);
                    try backend.acquireRootWriterLock();
                    try backend.prepareWalOperationLockFile();
                    if (mode == .wal_prefix) {
                        try backend.mutable.upsert(owned, .{}, "a", "one", false);
                        backend.mutable_wal_range = .{ .first = 3, .last = 3 };
                    }
                    var preparation: lsm.Backend.QuiescedDurabilityPreparation = .{};
                    defer preparation.deinit();
                    control.calls = 0;
                    control.target = target;
                    control.live = true;
                    var credit = retry.call(ResourceManager.reserveWithoutReclaimWithAdmission, .{ &resources.resource_manager, .lsm_table_builder_working_set, try lsm.Backend.QuiescedWalRetirement.memoryBound(native.storage()), original }) catch |cause| break :work cause;
                    defer {
                        preparation.deinit();
                        credit.release();
                    }
                    var operation = (retry.call(lsm.Backend.tryBeginQuiescedWalRetirementWithAdmission, .{ &backend, &preparation, original }) catch |cause| break :work cause) orelse return error.TestUnexpectedResult;
                    var operation_live = true;
                    defer if (operation_live) operation.deinit();
                    if (mode == .wal_cancel or mode == .wal_resume) {
                        control.live = false;
                        const Stop = struct {
                            operation: *lsm.Backend.QuiescedWalRetirement,
                            fn check(raw: *anyopaque) !void {
                                const stop: *@This() = @ptrCast(@alignCast(raw));
                                if (std.mem.eql(u8, @tagName(stop.operation.stage), "remove")) return error.Canceled;
                            }
                        };
                        var stop: Stop = .{ .operation = &operation };
                        var rejected = payload;
                        rejected.ptr = &stop;
                        rejected.vtable = &.{ .checkpoint = Stop.check, .check_lifetime = Stop.check, .deadline_ns = Control.deadline };
                        try std.testing.expectError(error.Canceled, operation.publishWithAdmission(.init(&rejected)));
                        try std.testing.expect(backend.manifest_recovery_required);
                    } else if (mode == .wal_floor_failure) {
                        control.live = false;
                        const AtFloor = struct {
                            operation: *lsm.Backend.QuiescedWalRetirement,
                            fn check(raw: *anyopaque) !void {
                                const fault: *@This() = @ptrCast(@alignCast(raw));
                                if (std.mem.eql(u8, @tagName(fault.operation.stage), "floor")) io_mod.NativeSyncTestControl.control_failure_after_rename.store(true, .release);
                            }
                        };
                        var fault: AtFloor = .{ .operation = &operation };
                        var failing_scope = payload;
                        failing_scope.ptr = &fault;
                        failing_scope.vtable = &.{ .checkpoint = AtFloor.check, .check_lifetime = AtFloor.check, .deadline_ns = Control.deadline };
                        defer io_mod.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
                        try std.testing.expectError(error.InputOutput, operation.publishWithAdmission(.init(&failing_scope)));
                        try std.testing.expect(backend.manifest_recovery_required);
                    } else if (mode == .wal_delete_failure) {
                        control.live = false;
                        io_mod.NativeSyncTestControl.delete_failure.store(true, .release);
                        defer io_mod.NativeSyncTestControl.delete_failure.store(false, .release);
                        try std.testing.expectError(error.InputOutput, operation.publishWithAdmission(original));
                        try std.testing.expect(backend.manifest_recovery_required);
                    } else if (mode == .wal_legacy_replay) {
                        control.live = false;
                        try std.testing.expectError(error.ExecutionWaitUnsupported, operation.publishWithAdmission(original));
                        try std.testing.expect(!backend.manifest_recovery_required);
                    } else if (mode == .wal_failure) {
                        control.live = false;
                        io_mod.NativeSyncTestControl.control_failure_after_rename.store(true, .release);
                        defer io_mod.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
                        try std.testing.expectError(error.InputOutput, operation.publishWithAdmission(original));
                        try std.testing.expect(backend.manifest_recovery_required);
                    } else {
                        const retired = retry.call(lsm.Backend.QuiescedWalRetirement.publishWithAdmission, .{ &operation, original }) catch |cause| break :work cause;
                        try std.testing.expectEqual(@as(u64, if (mode == .wal_prefix) 2 else 3), retired.reclaimed_through_segment);
                        try std.testing.expect(!backend.manifest_recovery_required);
                    }
                    control.live = false;
                    if (mode == .wal_resume or mode == .wal_delete_failure) {
                        // The cancelled/failed operation is joined first. New
                        // cleanup admission resumes only its captured WAL debt.
                        operation.deinit();
                        operation_live = false;
                        try std.testing.expect(backend.wal_retirement_recovery != null);
                        var resumed = (try backend.tryBeginQuiescedWalRetirementWithAdmission(&preparation, original)).?;
                        defer resumed.deinit();
                        const result = try resumed.publishWithAdmission(original);
                        try resumed.retireWithAdmission(original);
                        try std.testing.expectEqual(@as(u64, 3), result.reclaimed_through_segment);
                        try std.testing.expect(!backend.manifest_recovery_required and backend.wal_retirement_recovery == null);
                    }
                    // Retirement of the grant is separately admitted. It never
                    // repeats a completed deletion or changes replay authority.
                    if (operation_live and mode != .wal_cancel) try operation.retireWithAdmission(original);
                    const retained = try wal.snapshotRetention(native.storage(), a, root);
                    try std.testing.expectEqual(@as(u64, if (mode == .wal_prefix or mode == .wal_failure or mode == .wal_legacy_replay) 3 else 4), retained.current_segment);
                    try std.testing.expectEqual(@as(u64, if (mode == .wal_failure or mode == .wal_legacy_replay) 1 else if (mode == .wal_prefix) 3 else 4), retained.oldest_retained_segment);
                    var replayed: state_mod.State = .{};
                    defer replayed.deinit(a);
                    _ = try wal.replayIntoMutable(native.storage(), a, root, &replayed);
                    if (mode == .wal_prefix or mode == .wal_failure or mode == .wal_legacy_replay) try std.testing.expectEqualStrings("one", try replayed.get(.{}, "a")) else try std.testing.expectEqual(@as(usize, 0), replayed.entryCount());
                    if (mode == .wal_replay) {
                        const entries = try wal.iterateReplayFrom(native.storage(), a, root, 0);
                        defer a.free(entries);
                        defer for (entries) |*entry| entry.deinit(a);
                        try std.testing.expectEqual(@as(usize, 1), entries.len);
                        try std.testing.expectEqualStrings("retained-replay", entries[0].payload);
                        try std.testing.expectEqual(@as(u64, 2), wal.nextReplaySequence(native.storage(), a, root, 0));
                    }
                    if (mode == .wal_retire or mode == .wal_replay) {
                        const writes = io_mod.NativeSyncTestControl.metadata.load(.acquire);
                        var again = (try backend.tryBeginQuiescedWalRetirementWithAdmission(&preparation, original)).?;
                        defer again.deinit();
                        const unchanged = try again.publishWithAdmission(original);
                        try again.retireWithAdmission(original);
                        try std.testing.expectEqual(@as(u64, 0), unchanged.deletion_completions);
                        try std.testing.expectEqual(writes, io_mod.NativeSyncTestControl.metadata.load(.acquire));
                    }
                }
                // Reopen the actual native checkpoint/journal/descriptor set,
                // then read both SST values after its completion owner exits.
                var reopened = try lsm.Backend.open(a, root, .{ .storage = native.storage(), .wal_enabled = false, .backend = .{ .read_only = true } });
                defer reopened.close();
                var recovered = try reopened.beginRead();
                defer recovered.abort();
                try std.testing.expectEqualStrings("one", try recovered.get(.{ .name = null }, "a"));
                try std.testing.expectEqualStrings("two", try recovered.get(.{ .name = null }, "b"));
                try std.testing.expectEqual(expected_logical, backend.unpublished_wal_logical_bytes);
                try std.testing.expectEqual(!install_checkpoint or mode == .checkpoint_install_reject or mode == .wal_cancel or mode == .wal_failure or mode == .wal_floor_failure, backend.manifest_recovery_required);
            }
            if (mode == .manifest) {
                // Seed metadata lineage for retirement-name preparation. This
                // fixture does not publish or acknowledge a durable descriptor.
                backend.manifest_journal.descriptor = .{ .checkpoint = 31, .first_segment = 31, .sequence = 0 };
                backend.manifest_journal.segments[0] = 31;
                backend.manifest_journal.segments[1] = 44;
                backend.manifest_journal.segment_count = 2;
                backend.manifest_journal.sequence = 9;
                const fence = lsm.repository.ManifestJournal.CheckpointFence.capture(&backend);
                try std.testing.expectEqual(@as(usize, 6), fence.retiredPathCount());
                const writer_root = backend.runs.tree.root;
                const directory_root = backend.run_directory.?.tree.root;
                var expected: [6][std.fs.max_path_bytes]u8 = undefined;
                var lengths: [6]usize = undefined;
                for (&expected, &lengths, 0..) |*buffer, *len, path_index| len.* = (try fence.retiredPathAt(root, path_index, buffer)).len;
                start = failing.alloc_index;
                control.calls = 0;
                control.target = if (oom) null else target;
                control.live = true;
                if (oom) failing.fail_index = start + target.?;
                owners.frame = retry.call(Scope.preparePublicationWithAdmission, .{ owners.scope.?, original }) catch |cause| break :work cause;
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    retry.call(lsm.Backend.capturePreparedPublicationWithAdmission, .{ &backend, owners.frame.?, &backend.registered_publication, backend.run_directory_generation, original }) catch |cause| break :work cause;
                }
                var empty_fence = fence;
                empty_fence.journal.descriptor = null;
                try std.testing.expectError(error.InvalidArgument, owners.frame.?.appendManifestRetirementWithAdmission(&empty_fence, root, 0, 1000, quotaControl()));
                try std.testing.expectError(error.InvalidArgument, owners.frame.?.appendManifestRetirementWithAdmission(&fence, root, 6, 1000, quotaControl()));
                for (0..6) |path_index| {
                    var input_root: [std.fs.max_path_bytes]u8 = undefined;
                    @memcpy(input_root[0..root.len], root);
                    var input_fence = fence;
                    retry.call(Scope.Publication.Candidate.appendManifestRetirementWithAdmission, .{ owners.frame.?, &input_fence, input_root[0..root.len], path_index, @as(u64, 1000), original }) catch |cause| break :work cause;
                    @memset(&input_root, 0xa5);
                    input_fence.journal.descriptor = null;
                    try std.testing.expectError(error.InvalidArgument, owners.frame.?.appendObsoleteRemovalWithAdmission(0, quotaControl()));
                    retry.call(Scope.Publication.Candidate.resumeWithAdmission, .{ owners.frame.?, original }) catch |cause| break :work cause;
                }
                const receipt = try owners.frame.?.preparedReceiptWithAdmission(quotaControl());
                for ([_]u64{ 500, 2000 }) |deadline| {
                    retry.call(Scope.Publication.Candidate.appendManifestRetirementWithAdmission, .{ owners.frame.?, &fence, root, @as(usize, 0), deadline, original }) catch |cause| break :work cause;
                    try std.testing.expectError(error.CompactionPlanningStale, owners.frame.?.validatePreparedReceiptFixedWithAdmission(receipt, quotaControl()));
                    retry.call(Scope.Publication.Candidate.resumeWithAdmission, .{ owners.frame.?, original }) catch |cause| break :work cause;
                    const private = try owners.frame.?.preparedRootsWithAdmission(quotaControl());
                    try std.testing.expectEqual(@as(u64, if (deadline == 500) 1000 else 2000), private.obsolete.get(expected[0][0..lengths[0]]).?.delete_after_ns);
                }
                const private = try owners.frame.?.preparedRootsWithAdmission(quotaControl());
                try std.testing.expectEqual(@as(usize, 6), private.obsolete.count());
                for (&expected, lengths, 0..) |*buffer, len, path_index| {
                    try std.testing.expectEqual(@as(u64, if (path_index == 0) 2000 else 1000), private.obsolete.get(buffer[0..len]).?.delete_after_ns);
                    try std.testing.expect(!std.mem.eql(u8, std.fs.path.basename(buffer[0..len]), "manifest.bin"));
                }
                try std.testing.expect(fence.matches(&backend));
                try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.count());
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    retry.call(lsm.Backend.installRegisteredPublicationWithAdmission, .{ &backend, owners.frame.?, original }) catch |cause| break :work cause;
                }
                owners.frame = null;
                allocations = failing.alloc_index - start;
                control.live = false;
                try std.testing.expectEqual(@as(usize, 6), backend.obsolete_paths.count());
                try std.testing.expect(backend.runs.tree.root == writer_root and backend.run_directory.?.tree.root == directory_root);
                try std.testing.expectEqualStrings("one", try read.getWithAdmission(.{ .name = null }, "a", original));
                var current = try backend.beginReadWithAdmission(original);
                defer current.abort();
                try std.testing.expectEqualStrings("two", try current.getWithAdmission(.{ .name = null }, "b", original));
            }
            if ((mode == .ledger or mode == .ledger_mixed)) {
                // First create real obsolete intent by removing a physical SST.
                // The earlier read pins the old generation across both swaps.
                owners.frame = try owners.scope.?.preparePublicationWithAdmission(original);
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    try backend.capturePreparedPublicationWithAdmission(owners.frame.?, &backend.registered_publication, backend.run_directory_generation, original);
                }
                const entry = &owners.entries[0].?;
                try owners.frame.?.appendRemoveWithAdmission(.{ .run = entry.entry.run, .revision = entry.entry.payload.? }, &owners.views[0].?, 123, original);
                try owners.frame.?.resumeWithAdmission(original);
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    try backend.installRegisteredPublicationWithAdmission(owners.frame.?, original);
                }
                owners.frame = null;
                try std.testing.expectEqual(@as(usize, 1), backend.obsolete_paths.count());
                const writer_root = backend.runs.tree.root;
                const directory_root = backend.run_directory.?.tree.root;
                start = failing.alloc_index;
                control.calls = 0;
                control.target = if (oom) null else target;
                control.live = true;
                if (oom) failing.fail_index = start + target.?;
                owners.frame = retry.call(Scope.preparePublicationWithAdmission, .{ owners.scope.?, original }) catch |cause| break :work cause;
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    retry.call(lsm.Backend.capturePreparedPublicationWithAdmission, .{ &backend, owners.frame.?, &backend.registered_publication, backend.run_directory_generation, original }) catch |cause| break :work cause;
                }
                try std.testing.expectError(error.InvalidArgument, owners.frame.?.appendObsoleteRemovalWithAdmission(1, quotaControl()));
                retry.call(Scope.Publication.Candidate.appendObsoleteRemovalWithAdmission, .{ owners.frame.?, @as(usize, 0), original }) catch |cause| break :work cause;
                // A pending ledger edit cannot be replaced by a file edit.
                try std.testing.expectError(error.InvalidArgument, owners.frame.?.appendRemoveWithAdmission(.{ .run = entry.entry.run, .revision = entry.entry.payload.? }, &owners.views[0].?, 1, quotaControl()));
                retry.call(Scope.Publication.Candidate.resumeWithAdmission, .{ owners.frame.?, original }) catch |cause| break :work cause;
                const receipt = try owners.frame.?.preparedReceiptWithAdmission(quotaControl());
                try owners.frame.?.validatePreparedReceiptFixedWithAdmission(receipt, quotaControl());
                if (mode == .ledger_mixed) {
                    // Both transitions reuse the same candidate and its funded
                    // headers: ledger -> file -> ledger, with revision fencing.
                    const remaining = &owners.entries[1].?;
                    try owners.frame.?.appendRemoveWithAdmission(.{ .run = remaining.entry.run, .revision = remaining.entry.payload.? }, &owners.views[1].?, 456, original);
                    try std.testing.expectError(error.CompactionPlanningStale, owners.frame.?.validatePreparedReceiptFixedWithAdmission(receipt, quotaControl()));
                    try owners.frame.?.resumeWithAdmission(original);
                    try owners.frame.?.appendObsoleteRemovalWithAdmission(0, original);
                    try owners.frame.?.resumeWithAdmission(original);
                }
                {
                    try std.testing.expect(backend.mu.tryLock());
                    defer backend.mu.unlock();
                    retry.call(lsm.Backend.installRegisteredPublicationWithAdmission, .{ &backend, owners.frame.?, original }) catch |cause| break :work cause;
                }
                owners.frame = null;
                allocations = failing.alloc_index - start;
                control.live = false;
                try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.count());
                if (mode == .ledger) try std.testing.expect(backend.runs.tree.root == writer_root and backend.run_directory.?.tree.root == directory_root);
                try std.testing.expectEqual(@as(usize, if (mode == .ledger) 1 else 0), backend.runs.count());
                try std.testing.expectEqualStrings("one", try read.getWithAdmission(.{ .name = null }, "a", original));
                var current = try backend.beginReadWithAdmission(original);
                defer current.abort();
                if (mode == .ledger) try std.testing.expectEqualStrings("two", try current.getWithAdmission(.{ .name = null }, "b", original));
            }
        };
        const checks = control.calls;
        control.live = false;
        if (result) |_| {} else |cause| {
            try std.testing.expect(retry.observed and abort_on_error);
            try std.testing.expectError(if (oom) error.OutOfMemory else control.failure, @as(anyerror!void, cause));
        }
        if (immutable_mode and owners.window == null) {
            try std.testing.expectEqual(@as(usize, 0), backend.admitted_retired_immutable_count);
            try std.testing.expect(backend.admitted_retired_immutable == null);
        }
        if (immutable_mode and owners.window != null) {
            try std.testing.expectEqual(input_count, backend.activeImmutableMemtableCount());
            try std.testing.expectEqual(@as(u64, 11), backend.immutable_wal_ranges.items[backend.immutable_head].first);
            try std.testing.expectEqualStrings("one", owners.immutable_pins.?[input_count - 1].entryAt(0).value);
        }
        if (!late and (mode != .publish or (retry.observed and abort_on_error))) {
            try std.testing.expect(!owners.installed);
            try std.testing.expectEqual(@as(usize, 0), backend.runs.count());
        }
        if ((mode == .ledger or mode == .ledger_mixed) and retry.observed and abort_on_error) {
            try std.testing.expectEqual(@as(usize, 1), backend.obsolete_paths.count());
            try std.testing.expectEqual(@as(usize, 1), backend.runs.count());
        }
        if (mode == .manifest and retry.observed and abort_on_error) {
            try std.testing.expectEqual(@as(usize, 0), backend.obsolete_paths.count());
            try std.testing.expectEqual(@as(usize, 2), backend.runs.count());
        }
        if (checkpoint_mode) try std.testing.expectEqual(expected_logical, backend.unpublished_wal_logical_bytes);
        const expected_checkpoint_abandoned: usize = if (owners.checkpoint) |owner| if (owner.descriptor_permitted) 0 else owner.ticket_index else 0;
        const abandoned = try owners.finish(&backend, &resources.resource_manager, native.storage(), &failing);
        backend_live = false;
        try std.testing.expectEqual(@as(usize, if (checkpoint_mode) expected_checkpoint_abandoned else if (owners.installed or mode == .empty) 0 else if (mode == .identity) 4 else 2), abandoned);
        try std.testing.expectEqual(@as(u64, 0), resources.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
        if (oom) try std.testing.expect(failing.has_induced_failure);
        if (target) |at| if (!retry.observed) try std.testing.expect(!oom and at >= checks);
        return .{ .checks = checks, .allocations = allocations, .observed = retry.observed };
    }
};

test "physical cache metadata registered flush publication protects actual SSTs after accepted cancellation" {
    _ = try FlushPublicationFixture.run(.publish, null, false, false);
    _ = try FlushPublicationFixture.run(.empty, null, false, false);
}

test "physical cache metadata registered flush publication rejects missing foreign and changed candidate coverage" {
    for ([_]FlushPublicationFixture.Mode{ .missing, .identity, .stale }) |mode| _ = try FlushPublicationFixture.run(mode, null, false, false);
}

test "physical cache metadata registered flush publication retains original refusal and abort ownership" {
    for ([_]bool{ false, true }) |abort_on_error| {
        var at: usize = 0;
        while (true) : (at += 1) {
            const stats = try FlushPublicationFixture.run(.publish, at, false, abort_on_error);
            if (!stats.observed) break;
        }
    }
}

test "physical cache metadata registered flush publication resumes allocation failures and retires exhausted partial handoffs" {
    const count = (try FlushPublicationFixture.run(.publish, null, false, false)).allocations;
    for ([_]bool{ false, true }) |abort_on_error| {
        for (0..count) |at| _ = try FlushPublicationFixture.run(.publish, at, true, abort_on_error);
    }
}

test "physical cache metadata coupled ledger removal preserves actual readers and file roots" {
    _ = try FlushPublicationFixture.run(.ledger, null, false, false);
}

test "physical cache metadata coupled ledger removal retains original refusal and abort ownership" {
    for ([_]bool{ false, true }) |abort_on_error| {
        var at: usize = 0;
        while (true) : (at += 1) {
            const stats = try FlushPublicationFixture.run(.ledger, at, false, abort_on_error);
            if (!stats.observed) break;
        }
    }
}

test "physical cache metadata coupled ledger removal retires allocation exhaustion" {
    const count = (try FlushPublicationFixture.run(.ledger, null, false, false)).allocations;
    for ([_]bool{ false, true }) |abort_on_error| {
        for (0..count) |at| _ = try FlushPublicationFixture.run(.ledger, at, true, abort_on_error);
    }
}

test "physical cache metadata coupled ledger removal composes file edits and invalidates old receipts" {
    _ = try FlushPublicationFixture.run(.ledger_mixed, null, false, false);
}

test "physical cache metadata manifest retirement captures lineage paths deadlines and caller borrows" {
    _ = try FlushPublicationFixture.run(.manifest, null, false, false);
}

test "physical cache metadata manifest retirement retains original refusal and abort ownership" {
    for ([_]bool{ false, true }) |abort_on_error| {
        var at: usize = 0;
        while (true) : (at += 1) {
            const stats = try FlushPublicationFixture.run(.manifest, at, false, abort_on_error);
            if (!stats.observed) break;
        }
    }
}

test "physical cache metadata manifest retirement retires allocation exhaustion" {
    const count = (try FlushPublicationFixture.run(.manifest, null, false, false)).allocations;
    for ([_]bool{ false, true }) |abort_on_error| {
        for (0..count) |at| _ = try FlushPublicationFixture.run(.manifest, at, true, abort_on_error);
    }
}

test "physical cache metadata registered checkpoint reopens actual SSTs after completion owner exits" {
    _ = try FlushPublicationFixture.run(.checkpoint, null, false, false);
}

test "physical cache metadata registered checkpoint preserves descriptor recovery fencing and private cleanup" {
    for ([_]FlushPublicationFixture.Mode{ .checkpoint_abort, .checkpoint_cancel, .checkpoint_rename, .checkpoint_stale, .checkpoint_lineage }) |mode| _ = try FlushPublicationFixture.run(mode, null, false, false);
}

test "physical cache metadata registered checkpoint retains original refusal and abort ownership" {
    for ([_]bool{ false, true }) |abort_on_error| {
        for ([_]usize{ 0, 1, 4, 16, 64, 128, 256 }) |at| _ = try FlushPublicationFixture.run(.checkpoint, at, false, abort_on_error);
    }
}

test "physical cache metadata registered checkpoint retires allocation exhaustion" {
    const count = (try FlushPublicationFixture.run(.checkpoint, null, false, false)).allocations;
    for ([_]bool{ false, true }) |abort_on_error| {
        for (0..count) |at| _ = try FlushPublicationFixture.run(.checkpoint, at, true, abort_on_error);
    }
}

test "physical cache metadata durable checkpoint receipt installs and replaces registered manifest snapshots" {
    _ = try FlushPublicationFixture.run(.checkpoint_install, null, false, false);
    _ = try FlushPublicationFixture.run(.checkpoint_repeat, null, false, false);
}

test "physical cache metadata durable checkpoint receipt preserves original refusal and covered logical debt" {
    for ([_]bool{ false, true }) |abort_on_error| {
        for ([_]usize{ 0, 1, 4, 16, 64, 128, 256, 512 }) |at| _ = try FlushPublicationFixture.run(.checkpoint_install, at, false, abort_on_error);
    }
}

test "physical cache metadata durable checkpoint receipt retires every exhausted owned allocation" {
    const count = (try FlushPublicationFixture.run(.checkpoint_install, null, false, false)).allocations;
    for ([_]bool{ false, true }) |abort_on_error| {
        for (0..count) |at| _ = try FlushPublicationFixture.run(.checkpoint_install, at, true, abort_on_error);
    }
}

test "physical cache metadata durable checkpoint receipt rejects changed metadata after descriptor durability" {
    _ = try FlushPublicationFixture.run(.checkpoint_install_reject, null, false, false);
}

test "physical cache metadata admitted WAL retirement rotates canonical state and preserves dedicated replay" {
    _ = try FlushPublicationFixture.run(.wal_retire, null, false, false);
    _ = try FlushPublicationFixture.run(.wal_replay, null, false, false);
}

test "physical cache metadata admitted WAL retirement preserves active current and cancelled physical debt" {
    for ([_]FlushPublicationFixture.Mode{ .wal_prefix, .wal_cancel, .wal_failure }) |mode| _ = try FlushPublicationFixture.run(mode, null, false, false);
}

test "physical cache metadata admitted WAL retirement retains original refusal and abort authority" {
    for ([_]bool{ false, true }) |abort_on_error| {
        for ([_]usize{ 0, 1, 2, 3, 8, 32, 128, 512 }) |at| _ = try FlushPublicationFixture.run(.wal_retire, at, false, abort_on_error);
    }
}

test "physical cache metadata admitted WAL retirement resumes cleanup debt and refuses embedded replay" {
    for ([_]FlushPublicationFixture.Mode{ .wal_resume, .wal_delete_failure, .wal_floor_failure, .wal_legacy_replay }) |mode| _ = try FlushPublicationFixture.run(mode, null, false, false);
}

const ImmutablePinFixture = struct {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const State = @import("antfly_local_sources").storage_lsm_backend_state.State;

    fn write(backend: *lsm.Backend, generation: usize) !void {
        var key: [32]u8 = undefined;
        const bytes = try std.fmt.bufPrint(&key, "key-{d:0>3}", .{generation});
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        try txn.put(.{ .name = "docs" }, bytes, bytes);
        try txn.commit();
    }

    fn indexes(backend: *lsm.Backend) !void {
        for (backend.immutable_memtable_pins.items, 0..) |pin, index| {
            try std.testing.expectEqual(index, pin.state.immutable_pin_index.?);
            try std.testing.expect(pin.readers > 0);
        }
        for (backend.retired_immutable_memtables.items, 0..) |state, index| {
            try std.testing.expectEqual(index, state.immutable_retired_index.?);
            try std.testing.expect(state.immutable_pin_index != null);
        }
    }
};

test "physical cache metadata immutable pin indexes survive out of order reader release and flush" {
    const F = ImmutablePinFixture;
    const a = std.testing.allocator;
    var storage = @import("antfly_local_sources").storage_lsm_backend_storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var backend = try F.lsm.Backend.open(a, "/immutable-indexes", .{
        .storage = storage.storage(),
        .flush_threshold = 1,
        .defer_flush_on_commit = true,
    });
    defer backend.close();
    const count = 24;
    var first: [count]?[]*const F.State = @splat(null);
    var second: [count]?[]*const F.State = @splat(null);
    defer {
        for (&first, &second) |*one, *two| {
            if (one.*) |snapshot| backend.releaseImmutableMemtableSnapshot(snapshot);
            if (two.*) |snapshot| backend.releaseImmutableMemtableSnapshot(snapshot);
        }
    }
    for (0..count) |generation| {
        try F.write(&backend, generation);
        try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
        first[generation] = try backend.snapshotImmutableMemtables();
        second[generation] = try backend.snapshotImmutableMemtables();
        try std.testing.expectEqual(@as(usize, 2), backend.immutable_memtable_pins.items[first[generation].?[0].immutable_pin_index.?].readers);
        var attempts: usize = 0;
        while (backend.activeImmutableMemtableCount() != 0) : (attempts += 1) {
            try std.testing.expect(attempts < 64);
            try std.testing.expect(try backend.runMaintenanceStep());
        }
        try F.indexes(&backend);
    }
    try std.testing.expectEqual(@as(usize, count), backend.retired_immutable_memtables.items.len);
    // A copied state gets independent backend membership, even while the
    // source has both live pin and retired-generation indexes.
    var clone = try first[0].?[0].clone(a);
    defer clone.deinit(a);
    try std.testing.expect(clone.immutable_pin_index == null and clone.immutable_retired_index == null);
    for (0..count) |turn| {
        // Coprime stride visits all slots, including moved first/middle/last
        // members. One release keeps the source; the last retires that source.
        const generation = (turn * 7 + 11) % count;
        const state = first[generation].?[0];
        backend.releaseImmutableMemtableSnapshot(first[generation].?);
        first[generation] = null;
        try std.testing.expectEqual(@as(usize, 1), backend.immutable_memtable_pins.items[state.immutable_pin_index.?].readers);
        try std.testing.expectEqualStrings(state.entryAt(0).key, state.entryAt(0).value);
        try F.indexes(&backend);
        backend.releaseImmutableMemtableSnapshot(second[generation].?);
        second[generation] = null;
        try std.testing.expectEqual(count - turn - 1, backend.retired_immutable_memtables.items.len);
        try F.indexes(&backend);
    }
    try std.testing.expectEqual(@as(usize, 0), backend.immutable_memtable_pins.items.len);
    var reader = try backend.beginRead();
    defer reader.abort();
    try std.testing.expectEqualStrings("key-000", try reader.get(.{ .name = "docs" }, "key-000"));
    try std.testing.expectEqualStrings("key-023", try reader.get(.{ .name = "docs" }, "key-023"));
}

test "physical cache metadata immutable pin indexes remain atomic on snapshot allocation failure" {
    const F = ImmutablePinFixture;
    for (0..2) |failure| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        const a = failing.allocator();
        var storage = @import("antfly_local_sources").storage_lsm_backend_storage_io.MemoryStorage.init(std.testing.allocator);
        defer storage.deinit();
        var backend = try F.lsm.Backend.open(a, "/immutable-index-oom", .{
            .storage = storage.storage(),
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
            .max_deferred_immutable_memtables = 64,
        });
        defer backend.close();
        for (0..32) |generation| try F.write(&backend, generation);
        try std.testing.expectEqual(@as(usize, 32), backend.activeImmutableMemtableCount());
        failing.fail_index = failing.alloc_index + failure;
        failing.resize_fail_index = failing.resize_index;
        try std.testing.expectError(error.OutOfMemory, backend.snapshotImmutableMemtables());
        failing.fail_index = std.math.maxInt(usize);
        failing.resize_fail_index = std.math.maxInt(usize);
        try std.testing.expectEqual(@as(usize, 0), backend.immutable_memtable_pins.items.len);
        for (backend.immutable_memtables.items[backend.immutable_head..]) |state| try std.testing.expect(state.immutable_pin_index == null);
        const snapshot = try backend.snapshotImmutableMemtables();
        try F.indexes(&backend);
        backend.releaseImmutableMemtableSnapshot(snapshot);
        try std.testing.expectEqual(@as(usize, 0), backend.immutable_memtable_pins.items.len);
        for (backend.immutable_memtables.items[backend.immutable_head..]) |state| try std.testing.expect(state.immutable_pin_index == null);
    }
}

test "physical cache metadata immutable durable retirement preserves reader pins and the active WAL tail" {
    _ = try FlushPublicationFixture.run(.immutable_retire, null, false, false);
    _ = try FlushPublicationFixture.run(.immutable_prefix, null, false, false);
}

test "physical cache metadata immutable durable retirement cancellation retains recoverable inputs" {
    _ = try FlushPublicationFixture.run(.immutable_cancel, null, false, false);
}

test "physical cache metadata immutable durable retirement keeps input authority through original refusal" {
    for ([_]usize{ 0, 1, 2, 8, 32, 128 }) |target| {
        _ = try FlushPublicationFixture.run(.immutable_retire, target, false, false);
        _ = try FlushPublicationFixture.run(.immutable_retire, target, false, true);
    }
}

test "physical cache metadata immutable durable retirement plans flat and arena input memory in credits" {
    const state_mod = @import("antfly_local_sources").storage_lsm_backend_state;
    const a = std.testing.allocator;
    var flat: state_mod.State = .{};
    defer flat.deinit(a);
    for ([_][]const u8{ "a", "b", "c" }) |key| try flat.upsert(a, .{ .name = "docs" }, key, "value", false);
    var arena = try flat.cloneArena(a);
    defer arena.deinit(a);
    for ([_]*state_mod.State{ &flat, &arena }) |state| {
        const expected = state.estimatedMemoryBytes();
        var freezing: state_mod.State.MemoryPlanning = .{};
        var steps: usize = 0;
        while (!freezing.complete) : (steps += 1) {
            try std.testing.expect(steps < 32);
            var credits: usize = 1;
            _ = freezing.step(state, &credits);
        }
        try std.testing.expectEqual(expected, freezing.bytes);
        try std.testing.expect(state.frozen_memory_bytes == null);
    }
}

const FileSnapshotFixture = struct {
    const Scope = @import("antfly_local_sources").storage_lsm_backend_mod.Backend.PublicationResources;
    const Owner = Scope.FileSnapshot.Owner;
    const Run = @import("antfly_local_sources").storage_lsm_backend_mod.repository.Run;
    const Control = RegisteredOutputFixture.Control;
    const Stats = struct { checks: usize, allocations: usize, observed: bool };
    fn run(target: ?usize, oom: bool, abort_on_error: bool) !Stats {
        return runPhase(target, oom, abort_on_error, false);
    }
    fn runPhase(target: ?usize, oom: bool, abort_on_error: bool, retire_refusal: bool) !Stats {
        return runKind(target, oom, abort_on_error, retire_refusal, false);
    }
    fn runKind(target: ?usize, oom: bool, abort_on_error: bool, retire_refusal: bool, memory: bool) !Stats {
        const a = std.testing.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const owned = failing.allocator();
        var parent = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent.deinit(a);
        var budgets = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets.options.identity_allocator = owned;
        var resources = try Resources.initWithBudgetsAndParent(owned, budgets, parent.memoryParentPort());
        defer resources.deinit();
        resources.attachResourceManager();
        var source: Run = .{
            .id = 77,
            .level = 2,
            .read_identity = .{ .issuer = 21, .sequence = 42 },
            .visibility_id = 91,
            .gc_requested = true,
            .size_bytes = 12345,
            .path = try a.dupe(u8, "/legacy/runs/77.tbl"),
            .smallest_namespace_name = null,
            .largest_namespace_name = try a.dupe(u8, ""),
            .smallest_key = try a.alloc(u8, 32777),
            .largest_key = try a.dupe(u8, "z"),
            .entry_count = 2,
            .tombstone_count = 1,
            .oldest_tombstone_unix_ns = 123,
            .bloom_filter = .{ .bytes = try a.alloc(u8, 49159), .bit_count = 49159 * 8, .hash_count = 3 },
            .state = null,
        };
        @memset(source.smallest_key, 'k');
        @memset(source.bloom_filter.?.bytes, 0xa5);
        if (memory) {
            a.free(source.path.?);
            source.path = null;
            const State = @import("antfly_local_sources").storage_lsm_backend_state;
            var active: State.ActiveMemTable = .{};
            defer active.deinit(a);
            const value = try a.alloc(u8, 48 * 1024 + 3);
            defer a.free(value);
            @memset(value, 0x73);
            try active.upsert(a, .{ .name = "docs" }, "row", value, false);
            try active.upsert(a, .{ .name = "docs" }, "tombstone", "", true);
            source.state = try active.snapshot(a);
            try source.shareMemory(a);
        }
        var source_live = true;
        defer if (source_live) source.deinit(a);
        var owner: ?*Owner = null;
        var lease: ?Scope.FileSnapshot.Lease = null;
        defer {
            if (lease) |*held| held.releaseWithAdmission(null) catch unreachable;
            if (owner) |held| held.requestRetirement();
            RegisteredOutputFixture.drain(&resources.resource_manager);
            failing.fail_index = std.math.maxInt(usize);
        }
        var control: Control = .{};
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        control.target = if (oom or retire_refusal) null else target;
        control.failure = switch ((target orelse 0) % 4) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            2 => error.ParentMemoryBusy,
            else => error.FileNotFound,
        };
        control.live = true;
        const start = failing.alloc_index;
        if (oom) failing.fail_index = start + target.?;
        var retry: FlushPublicationFixture.Retry = .{ .failing = &failing, .control = &control, .oom = oom, .abort_on_error = abort_on_error };
        const result: anyerror!void = work: {
            owner = (if (memory)
                retry.call(Owner.startMemory, .{ owned, &resources.resource_manager, source, original })
            else
                retry.call(Owner.start, .{ owned, &resources.resource_manager, source, original })) catch |cause| break :work cause;
            retry.call(Owner.resumeWithAdmission, .{ owner.?, original }) catch |cause| break :work cause;
            source.deinit(a);
            source_live = false;
            lease = retry.call(Owner.acquireSourceWithAdmission, .{ owner.?, original }) catch |cause| break :work cause;
        };
        var checks = control.calls;
        const allocations = failing.alloc_index - start;
        control.live = false;
        if (result) |_| {
            const copied = lease.?.run;
            if (memory) {
                try std.testing.expect(copied.path == null and copied.shared_memory == null);
                const state = &copied.state.?;
                try std.testing.expectEqual(@as(usize, 2), state.entryCount());
                try std.testing.expect(state.entryAt(0).shared == null and state.entryAt(1).shared == null);
                try std.testing.expectEqualStrings("docs", state.entryAt(0).namespace_name.?);
                try std.testing.expectEqualStrings("row", state.entryAt(0).key);
                try std.testing.expectEqual(@as(usize, 48 * 1024 + 3), state.entryAt(0).value.len);
                try std.testing.expect(std.mem.allEqual(u8, state.entryAt(0).value, 0x73));
                try std.testing.expect(state.entryAt(1).tombstone);
                const resident = state.estimatedMemoryBytes() + copied.smallest_key.len + copied.largest_key.len + copied.largest_namespace_name.?.len + copied.bloom_filter.?.bytes.len;
                try std.testing.expect(owner.?.variable_credit.?.bytes >= resident);
            } else try std.testing.expectEqualStrings("/legacy/runs/77.tbl", copied.path.?);
            try std.testing.expect(copied.smallest_namespace_name == null and copied.largest_namespace_name != null);
            try std.testing.expectEqualStrings("", copied.largest_namespace_name.?);
            try std.testing.expectEqual(@as(usize, 32777), copied.smallest_key.len);
            try std.testing.expect(std.mem.allEqual(u8, copied.smallest_key, 'k'));
            try std.testing.expect(std.mem.allEqual(u8, copied.bloom_filter.?.bytes, 0xa5));
            try std.testing.expectEqual(@as(u32, 49159 * 8), copied.bloom_filter.?.bit_count);
            try std.testing.expectEqual(@as(u64, 91), copied.visibility_id);
            try std.testing.expectEqual(@as(u32, 1), copied.tombstone_count);
            try std.testing.expect(copied.gc_requested and copied.read_identity.valid());
            owner.?.requestRetirement();
            owner = null;
            // Source creator and original metadata have both exited. The real
            // lease still keeps every byte and its original reservation alive.
            for (0..16) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
            try std.testing.expect(resources.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes > 0);
            try std.testing.expectEqualStrings("z", lease.?.run.largest_key);
            try lease.?.releaseWithAdmission(null);
            lease = null;
            if (retire_refusal) {
                control.calls = 0;
                control.target = target;
                control.live = true;
                const retired: anyerror!void = retirement: {
                    while (resources.resource_manager.payload_retirement_ready.load(.acquire) != 0) {
                        _ = retry.call(ResourceManager.drainPayloadRetirementWithAdmission, .{ &resources.resource_manager, original }) catch |cause| break :retirement cause;
                    }
                };
                checks = control.calls;
                control.live = false;
                if (retired) |_| {} else |cause| {
                    try std.testing.expect(retry.observed and abort_on_error);
                    try std.testing.expectError(control.failure, @as(anyerror!void, cause));
                }
            }
        } else |cause| {
            try std.testing.expect(retry.observed and abort_on_error);
            try std.testing.expectError(if (oom) error.OutOfMemory else control.failure, @as(anyerror!void, cause));
            if (owner) |held| held.requestRetirement();
            owner = null;
            if (source_live) {
                source.deinit(a);
                source_live = false;
            }
        }
        // Abort cleanup runs while the allocator remains exhausted.
        RegisteredOutputFixture.drain(&resources.resource_manager);
        failing.fail_index = std.math.maxInt(usize);
        try std.testing.expectEqual(@as(u64, 0), resources.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
        return .{ .checks = checks, .allocations = allocations, .observed = retry.observed };
    }
};

test "physical cache metadata registered file snapshot retains owned bytes after original producer exit" {
    _ = try FileSnapshotFixture.run(null, false, false);
}

test "physical cache metadata registered file snapshot preserves every original refusal and partial abort" {
    const count = (try FileSnapshotFixture.run(null, false, false)).checks;
    for (0..count) |at| {
        _ = try FileSnapshotFixture.run(at, false, false);
        _ = try FileSnapshotFixture.run(at, false, true);
    }
}

test "physical cache metadata registered file snapshot retires allocation failures under exhausted resources" {
    const count = (try FileSnapshotFixture.run(null, false, false)).allocations;
    for (0..count) |at| {
        _ = try FileSnapshotFixture.run(at, true, false);
        _ = try FileSnapshotFixture.run(at, true, true);
    }
}

test "physical cache metadata registered file snapshot feeds actual SST publication and reader lifetime" {
    _ = try FlushPublicationFixture.run(.file_snapshot, null, false, false);
}

test "physical cache metadata registered file snapshot retains retirement authority after producer exit" {
    const count = (try FileSnapshotFixture.runPhase(null, false, false, true)).checks;
    for (0..count) |at| {
        _ = try FileSnapshotFixture.runPhase(at, false, false, true);
        _ = try FileSnapshotFixture.runPhase(at, false, true, true);
    }
}

test "physical cache metadata registered file snapshot preserves entry lifetime through original handoff refusal" {
    for ([_]usize{ 0, 1, 2, 8, 16, 32, 64, 128 }) |at| {
        _ = try FlushPublicationFixture.run(.file_snapshot, at, false, false);
        _ = try FlushPublicationFixture.run(.file_snapshot, at, false, true);
    }
}

test "physical cache metadata registered file snapshot preserves entry lifetime through allocation refusal" {
    for ([_]usize{ 0, 1, 2, 4, 8, 16, 32, 64, 128 }) |at| {
        _ = try FlushPublicationFixture.run(.file_snapshot, at, true, false);
        _ = try FlushPublicationFixture.run(.file_snapshot, at, true, true);
    }
}

test "physical cache metadata registered memory snapshot owns shared tree bytes after producer exit" {
    _ = try FileSnapshotFixture.runKind(null, false, false, false, true);
}

test "physical cache metadata registered memory snapshot preserves every original refusal and abort" {
    const checks = (try FileSnapshotFixture.runKind(null, false, false, false, true)).checks;
    for (0..checks) |at| {
        _ = try FileSnapshotFixture.runKind(at, false, false, false, true);
        _ = try FileSnapshotFixture.runKind(at, false, true, false, true);
    }
}

test "physical cache metadata registered memory snapshot funds every copy allocation and partial abort" {
    const count = (try FileSnapshotFixture.runKind(null, false, false, false, true)).allocations;
    for (0..count) |at| {
        _ = try FileSnapshotFixture.runKind(at, true, false, false, true);
        _ = try FileSnapshotFixture.runKind(at, true, true, false, true);
    }
}

test "physical cache metadata registered memory snapshot retains cleanup across every original refusal" {
    const checks = (try FileSnapshotFixture.runKind(null, false, false, true, true)).checks;
    for (0..checks) |at| {
        _ = try FileSnapshotFixture.runKind(at, false, false, true, true);
        _ = try FileSnapshotFixture.runKind(at, false, true, true, true);
    }
}

const LegacyMigrationFixture = struct {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const io_mod = @import("antfly_local_sources").storage_lsm_backend_storage_io;
    const Scope = lsm.Backend.PublicationResources;
    const Control = RegisteredOutputFixture.Control;
    const Stats = struct { checks: usize, allocations: usize, observed: bool };
    const Mode = enum { install, abort, stale, receipt, lineage, checkpoint, account, install_refusal, retire_dirs, retire_abort, retire_all, retire_all_abort, memory_flush, memory_flush_abort, memory_flush_checkpoint, memory_flush_stale, memory_flush_level, checkpoint_join, checkpoint_join_abort, checkpoint_join_wait };

    fn run(mode: Mode, target: ?usize, oom: bool) !Stats {
        return runKind(mode, target, oom, false);
    }
    fn runKind(mode: Mode, target: ?usize, oom: bool, memory: bool) !Stats {
        const flush_mode = switch (mode) {
            .memory_flush, .memory_flush_abort, .memory_flush_checkpoint, .memory_flush_stale, .memory_flush_level => true,
            else => false,
        };
        const a = std.testing.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const owned = failing.allocator();
        var parent = ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
        defer parent.deinit(a);
        var budgets = budgets_mod.smartResourceBudgetsForTotal(64 << 20);
        budgets.options.identity_allocator = owned;
        var resources = try Resources.initWithBudgetsAndParent(owned, budgets, parent.memoryParentPort());
        defer resources.deinit();
        resources.attachResourceManager();
        defer RegisteredOutputFixture.drain(&resources.resource_manager);
        var pool = io_mod.NativeStoragePool.initWithCapacityForTest(a, 8);
        defer pool.deinit();
        var native = try io_mod.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/legacy-migration", .{tmp.sub_path});
        defer a.free(root);
        const options: lsm.Options = .{ .resource_manager = &resources.resource_manager, .storage = native.storage(), .wal_enabled = false, .flush_threshold = 1, .defer_flush_on_commit = true };
        var backend = try lsm.Backend.open(owned, root, options);
        var backend_live = true;
        defer if (backend_live) backend.abandonAfterCrash();
        for (0..2) |generation| {
            try ImmutablePinFixture.write(&backend, generation);
            var attempts: usize = 0;
            while (backend.activeImmutableMemtableCount() != 0) : (attempts += 1) {
                try std.testing.expect(attempts < 64);
                _ = try backend.runMaintenanceStep();
            }
        }
        try backend.persistManifest();
        backend.close();
        backend_live = false;
        backend = try lsm.Backend.open(owned, root, options);
        backend_live = true;
        _ = try backend.planningDirectory();
        try std.testing.expectEqual(@as(usize, 2), backend.runs.count());
        // Real legacy path allocations exercise import after the source ledger
        // goes away. A long path requires several cooperative copy quanta.
        try backend.obsolete_paths.ensureUnusedCapacity(owned, 2);
        const long_path = try owned.alloc(u8, 32 * 1024 + 3);
        @memset(long_path, 'x');
        backend.obsolete_paths.appendAssumeCapacity(.{ .path = long_path, .delete_after_ns = std.math.maxInt(u64) - 1000 });
        backend.obsolete_paths.appendAssumeCapacity(.{ .path = try owned.dupe(u8, "/legacy-orphan"), .delete_after_ns = std.math.maxInt(u64) - 2000 });
        try backend.persistManifest();
        if (memory) {
            const State = @import("antfly_local_sources").storage_lsm_backend_state;
            var active: State.ActiveMemTable = .{};
            defer active.deinit(owned);
            try active.upsert(owned, .{ .name = "docs" }, "key-memory", "retained-memory", false);
            if (flush_mode) {
                try active.upsert(owned, .{ .name = "docs" }, "key-001", "stale-memory", false);
                try active.upsert(owned, .{ .name = "docs" }, "tombstone-memory", "", true);
            }
            var memory_run: lsm.repository.Run = .{
                .id = backend.next_run_id,
                .level = if (mode == .memory_flush_level) 2 else 0,
                .visibility_id = if (flush_mode) 1 else 0,
                .gc_requested = flush_mode,
                .oldest_tombstone_unix_ns = if (flush_mode) 123 else 0,
                .tombstone_count = if (flush_mode) 1 else 0,
                .size_bytes = 0,
                .path = null,
                .smallest_namespace_name = try owned.dupe(u8, "docs"),
                .smallest_key = try owned.dupe(u8, if (flush_mode) "key-001" else "key-memory"),
                .largest_namespace_name = try owned.dupe(u8, "docs"),
                .largest_key = try owned.dupe(u8, if (flush_mode) "tombstone-memory" else "key-memory"),
                .entry_count = if (flush_mode) 3 else 1,
                .bloom_filter = null,
                .state = try active.snapshot(owned),
            };
            var memory_live = true;
            defer if (memory_live) memory_run.deinit(owned);
            try memory_run.shareMemory(owned);
            try backend.run_directory.?.put(&backend, memory_run);
            try backend.runs.append(owned, memory_run);
            memory_live = false;
            backend.next_run_id += 1;
            backend.run_directory_generation += 1;
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            try std.testing.expectEqual(@as(usize, 1), backend.run_directory.?.memory_run_count);
        }
        const old_directory = backend.run_directory.?;
        const old_writer = backend.runs.tree.root;
        const old_obsolete = backend.obsolete_paths.tree.root;
        const descriptor = backend.manifest_journal.descriptor;
        const sequence = backend.manifest_journal.sequence;
        backend.unpublished_wal_logical_bytes = 37;
        var control: Control = .{};
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        const join_mode = mode == .checkpoint_join or mode == .checkpoint_join_abort or mode == .checkpoint_join_wait;
        var join: Scope.RegisteredCheckpoint.PhysicalRetirement = .{};
        var join_credit: ?@import("antfly_local_sources").storage_resource_manager.Reservation = null;
        defer if (join_credit) |*credit| credit.release();
        var join_receipt: ?Scope.RegisteredCheckpoint.PhysicalRetirement.Receipt = null;
        defer if (join_receipt) |receipt| {
            for (0..2048) |_| {
                if (receipt.completedWithAdmission(quotaControl()) catch unreachable) break;
                _ = resources.resource_manager.drainPayloadRetirementQuiesced();
            }
            std.debug.assert(receipt.completedWithAdmission(quotaControl()) catch unreachable);
        };
        if (join_mode) join_credit = try resources.resource_manager.reserveWithoutReclaimWithAdmission(.lsm_table_builder_working_set, @sizeOf(@TypeOf(join)), original);
        var frame_credit = try resources.resource_manager.reserveWithoutReclaimWithAdmission(.lsm_table_builder_working_set, @sizeOf(lsm.Backend.QuiescedLegacyRootMigration), original);
        defer frame_credit.release();
        var migration: lsm.Backend.QuiescedLegacyRootMigration = .{};
        defer migration.deinit();
        try migration.captureWithAdmission(&backend, original);
        var competing: lsm.Backend.QuiescedLegacyRootMigration = .{};
        defer competing.deinit();
        try std.testing.expectError(error.ResourceBudgetExceeded, competing.captureWithAdmission(&backend, original));
        const retire_mode = mode == .retire_dirs or mode == .retire_abort or mode == .retire_all or mode == .retire_all_abort;
        const start = failing.alloc_index;
        if (oom and target != null and !flush_mode and !join_mode) failing.fail_index = start + target.?;
        control.live = true;
        control.target = if (oom or retire_mode or flush_mode or join_mode) null else target;
        var observed = false;
        while (true) {
            migration.resumeWithAdmission(original) catch |cause| {
                try std.testing.expectError(if (oom) error.OutOfMemory else error.Canceled, @as(anyerror!void, cause));
                try std.testing.expect(!observed);
                observed = true;
                try std.testing.expect(backend.run_directory == old_directory and backend.runs.tree.root == old_writer and backend.obsolete_paths.tree.root == old_obsolete);
                if (mode == .abort) break;
                failing.fail_index = std.math.maxInt(usize);
                continue;
            };
            break;
        }
        var stats: Stats = .{ .checks = control.calls, .allocations = failing.alloc_index - start, .observed = observed };
        control.live = false;
        if (mode == .abort) {
            migration.deinit(); // Fault allocator stays exhausted through teardown.
            for (0..512) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
            failing.fail_index = std.math.maxInt(usize);
            try std.testing.expect(backend.run_directory == old_directory and backend.runs.tree.root == old_writer and backend.obsolete_paths.tree.root == old_obsolete);
        } else {
            if (mode == .stale) {
                backend.run_directory_generation += 1;
                try std.testing.expectError(error.CompactionPlanningStale, migration.installWithAdmission(original));
                backend.run_directory_generation -= 1;
            }
            if (mode == .lineage) {
                backend.manifest_journal.next_run_id += 1;
                try std.testing.expectError(error.CompactionPlanningStale, migration.installWithAdmission(original));
                backend.manifest_journal.next_run_id -= 1;
            }
            if (mode == .account) {
                try backend.obsolete_paths.ensureUnusedCapacity(owned, 7);
                try std.testing.expectError(error.CompactionPlanningStale, migration.installWithAdmission(original));
            }
            if (mode == .receipt) {
                try migration.frame.?.appendObsoleteRemovalWithAdmission(0, original);
                try migration.frame.?.resumeWithAdmission(original);
                try std.testing.expectError(error.CompactionPlanningStale, migration.installWithAdmission(original));
            }
            if (mode == .receipt or mode == .account) {
                migration.deinit();
                try std.testing.expect(backend.run_directory == old_directory and backend.runs.tree.root == old_writer and backend.obsolete_paths.tree.root == old_obsolete);
            } else {
                if (mode == .install_refusal) {
                    for (0..3) |offset| {
                        control.target = control.calls + offset;
                        control.live = true;
                        try std.testing.expectError(error.Canceled, migration.installWithAdmission(original));
                        control.live = false;
                        try std.testing.expect(backend.run_directory == old_directory and backend.runs.tree.root == old_writer and backend.obsolete_paths.tree.root == old_obsolete);
                    }
                }
                // Fixed installation and joined teardown require no allocation.
                failing.fail_index = failing.alloc_index;
                try migration.installWithAdmission(original);
                try std.testing.expect(backend.registered_publication.seal != null);
                try std.testing.expect(backend.run_directory != old_directory);
                try std.testing.expect(backend.manifest_directory == null and backend.manifest_journal.obsolete.count() == 0);
                try std.testing.expectEqualDeep(descriptor, backend.manifest_journal.descriptor);
                try std.testing.expectEqual(sequence, backend.manifest_journal.sequence);
                try std.testing.expectEqual(@as(u64, 37), backend.unpublished_wal_logical_bytes);
                try std.testing.expect(backend.manifest_dirty and backend.obsolete_manifest_dirty);
                if (retire_mode) {
                    const writer_mode = mode == .retire_all or mode == .retire_all_abort;
                    // The separate directory/ledger suite already sweeps those
                    // checkpoints. Here every recorded fault targets writer work.
                    if (writer_mode) try migration.retireOldDirectoriesAndLedgersWithAdmission(original);
                    control.calls = 0;
                    control.target = target;
                    control.live = true;
                    observed = false;
                    const retire_start = failing.alloc_index;
                    while (true) {
                        const retired = if (writer_mode) migration.retireOldRootsWithAdmission(original) else migration.retireOldDirectoriesAndLedgersWithAdmission(original);
                        retired catch |cause| {
                            try std.testing.expectError(error.Canceled, @as(anyerror!void, cause));
                            try std.testing.expect(!observed);
                            observed = true;
                            if (mode == .retire_abort or mode == .retire_all_abort) break;
                            continue;
                        };
                        break;
                    }
                    stats = .{ .checks = control.calls, .allocations = failing.alloc_index - retire_start, .observed = observed };
                    try std.testing.expectEqual(@as(usize, 0), stats.allocations);
                    control.live = false;
                    if (mode == .retire_dirs) try std.testing.expect(migration.old_directory == null and migration.old_durable_directory == null and migration.old_obsolete == null and migration.old_durable_obsolete == null);
                    if (mode == .retire_all) try std.testing.expect(migration.old_store == null and migration.store_cleanup == null);
                }
                migration.deinit();
                failing.fail_index = std.math.maxInt(usize);
                try std.testing.expectError(error.ExecutionWaitUnsupported, backend.persistManifestLocked());
                try std.testing.expectError(error.ExecutionWaitUnsupported, backend.manifest_journal.persist(&backend, root, &.{}));
                try std.testing.expectError(error.ExecutionWaitUnsupported, backend.manifest_journal.runCheckpoint(&backend));
                backend.options.wal_enabled = true;
                const legacy_wal = backend.checkpointWalAfterDurableBoundary();
                backend.options.wal_enabled = false;
                try std.testing.expectError(error.ExecutionWaitUnsupported, legacy_wal);
                try std.testing.expectEqualDeep(descriptor, backend.manifest_journal.descriptor);
                try std.testing.expectEqual(sequence, backend.manifest_journal.sequence);
                try std.testing.expectEqual(@as(u64, 37), backend.unpublished_wal_logical_bytes);
                try std.testing.expectEqual(@as(usize, 2), backend.obsolete_paths.count());
                try std.testing.expectEqualStrings("/legacy-orphan", backend.obsolete_paths.at(0).path);
                try std.testing.expectEqual(@as(u64, std.math.maxInt(u64) - 2000), backend.obsolete_paths.at(0).delete_after_ns);
                try std.testing.expectEqual(@as(usize, 32 * 1024 + 3), backend.obsolete_paths.at(1).path.len);
                try std.testing.expectEqual(@as(u64, std.math.maxInt(u64) - 1000), backend.obsolete_paths.at(1).delete_after_ns);
            }
        }
        failing.fail_index = std.math.maxInt(usize);
        if (memory and mode != .abort) {
            try std.testing.expectEqual(@as(usize, 1), backend.run_directory.?.memory_run_count);
            try std.testing.expectEqual(@as(usize, 3), backend.runs.count());
            const scope = try backend.beginPublicationResourcesWithAdmission(original);
            defer scope.requestRetirement();
            try scope.resumeWithAdmission(original);
            var reservation = try backend.reserveRegisteredCheckpointWithAdmission(original);
            defer reservation.deinit();
            const candidate = try scope.preparePublicationWithAdmission(original);
            defer candidate.requestRetirement();
            try backend.capturePreparedPublicationWithAdmission(candidate, &backend.registered_publication, backend.run_directory_generation, original);
            try candidate.prepareUnchangedWithAdmission(original);
            var lineage = lsm.repository.ManifestJournal.CheckpointFence.capture(&backend);
            try std.testing.expectError(error.InvalidArgument, scope.beginRegisteredCheckpointWithAdmission(backend.options.unpublished_outputs.?, candidate, root, reservation.id, reservation.next_run_id, &lineage, original));
            try std.testing.expectEqualDeep(descriptor, backend.manifest_journal.descriptor);
            try std.testing.expectEqual(sequence, backend.manifest_journal.sequence);
            try std.testing.expectEqual(@as(u64, 37), backend.unpublished_wal_logical_bytes);
        }
        if (flush_mode) {
            stats = try convertMemory(&backend, &resources.resource_manager, native.storage(), &failing, &control, original, mode, target, oom);
        }
        if (mode == .checkpoint or mode == .memory_flush_checkpoint or join_mode) {
            const scope = try backend.beginPublicationResourcesWithAdmission(original);
            defer scope.requestRetirement();
            try scope.resumeWithAdmission(original);
            var reservation = try backend.reserveRegisteredCheckpointWithAdmission(original);
            defer reservation.deinit();
            const candidate = try scope.preparePublicationWithAdmission(original);
            var candidate_live = true;
            defer if (candidate_live) candidate.requestRetirement();
            try backend.capturePreparedPublicationWithAdmission(candidate, &backend.registered_publication, backend.run_directory_generation, original);
            try candidate.prepareUnchangedWithAdmission(original);
            var lineage = lsm.repository.ManifestJournal.CheckpointFence.capture(&backend);
            const checkpoint = try scope.beginRegisteredCheckpointWithAdmission(backend.options.unpublished_outputs.?, candidate, root, reservation.id, reservation.next_run_id, &lineage, original);
            var checkpoint_live = true;
            defer if (checkpoint_live) checkpoint.requestRetirement();
            if (join_mode) {
                const stale = try checkpoint.observePhysicalRetirementWithAdmission(&join, original);
                try checkpoint.detachPhysicalObservationWithAdmission(stale, original);
                join_receipt = try checkpoint.observePhysicalRetirementWithAdmission(&join, original);
                try std.testing.expectError(error.InvalidArgument, stale.completedWithAdmission(original));
                try std.testing.expectError(error.ParentMemoryBusy, join_receipt.?.resetWithAdmission(original));
                var duplicate: @TypeOf(join) = .{};
                try std.testing.expectError(error.InvalidArgument, checkpoint.observePhysicalRetirementWithAdmission(&duplicate, original));
                var forged = join_receipt.?;
                forged.identity.sequence += 1;
                try std.testing.expectError(error.InvalidArgument, forged.completedWithAdmission(original));
            }
            try checkpoint.prepareWithAdmission(native.storage(), original);
            if (mode == .checkpoint_join_abort or mode == .checkpoint_join_wait) {
                try std.testing.expect(!try join_receipt.?.completedWithAdmission(original));
                try std.testing.expectError(error.ExecutionWaitUnsupported, join_receipt.?.waitWithAdmission(quotaControl()));
                checkpoint_live = false;
                checkpoint.requestRetirement();
                var cleanup: Control = .{};
                const cleanup_payload = cleanup.payload();
                const fresh = read_admission.Port.init(&cleanup_payload);
                if (mode == .checkpoint_join_wait) {
                    const Wait = struct {
                        manager: *ResourceManager,
                        deny: bool = false,
                        expired: bool = false,
                        waits: usize = 0,
                        fn check(raw: *anyopaque) !void {
                            const self: *@This() = @ptrCast(@alignCast(raw));
                            if (self.expired) return error.DeadlineExceeded;
                        }
                        fn deadline(_: *anyopaque) !?u64 {
                            return std.math.maxInt(u64);
                        }
                        fn boundary(_: *anyopaque) !void {}
                        fn wait(raw: *anyopaque) !void {
                            const self: *@This() = @ptrCast(@alignCast(raw));
                            self.waits += 1;
                            if (self.deny) {
                                self.expired = true;
                                return error.DeadlineExceeded;
                            }
                            // Simulate an independently admitted cleanup turn,
                            // with its own authority and actual registered owner.
                            var worker: Control = .{};
                            const worker_payload = worker.payload();
                            _ = try self.manager.drainPayloadRetirementWithAdmission(read_admission.Port.init(&worker_payload));
                        }
                        fn makePayload(self: *@This()) read_admission.Payload {
                            return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check, .wait_for_progress = wait, .deadline_ns = deadline }, .execution_wait = .{ .ptr = self, .begin = boundary, .end = boundary } };
                        }
                    };
                    var rejected: Wait = .{ .manager = &resources.resource_manager, .deny = true };
                    const rejected_payload = rejected.makePayload();
                    const expired = read_admission.Port.init(&rejected_payload);
                    try std.testing.expectError(error.DeadlineExceeded, join_receipt.?.waitWithAdmission(expired));
                    try std.testing.expectEqual(@as(usize, 1), rejected.waits);
                    try std.testing.expect(!try join_receipt.?.completedWithAdmission(fresh));
                    var resumed: Wait = .{ .manager = &resources.resource_manager };
                    const resumed_payload = resumed.makePayload();
                    try join_receipt.?.waitWithAdmission(read_admission.Port.init(&resumed_payload));
                    try std.testing.expect(resumed.waits != 0);
                    try std.testing.expectError(error.DeadlineExceeded, join_receipt.?.completedWithAdmission(expired));
                } else for (0..2048) |_| {
                    if (try join_receipt.?.completedWithAdmission(fresh)) break;
                    _ = try resources.resource_manager.drainPayloadRetirementWithAdmission(fresh);
                }
                try std.testing.expect(try join_receipt.?.completedWithAdmission(fresh));
                for (0..512) |_| _ = resources.resource_manager.drainPayloadRetirementQuiesced();
                // The exact registered issuer has been unlinked and destroyed
                // by this joined consumer, not merely hidden by an empty count.
                var node = resources.resource_manager.payload_retirement_head;
                while (node) |current| : (node = current.next) try std.testing.expect(current.context != @as(*anyopaque, @ptrCast(checkpoint)));
                try std.testing.expect(try join_receipt.?.completedWithAdmission(fresh));
                try std.testing.expect(!backend.manifest_recovery_required);
                try std.testing.expectEqual(@as(u64, 37), backend.unpublished_wal_logical_bytes);
            } else {
                try checkpoint.permitDescriptorWithAdmission(&backend, candidate, original);
                try checkpoint.finishWithAdmission(original);
                if (join_mode) {
                    const completion = try checkpoint.completionWithAdmission(original);
                    try std.testing.expect(!try join_receipt.?.completedWithAdmission(original));
                    control.calls = 0;
                    control.target = target;
                    control.live = true;
                    var rejected = false;
                    checkpoint.retirePhysicalWithAdmission(original) catch |cause| {
                        try std.testing.expectError(error.Canceled, @as(anyerror!void, cause));
                        rejected = true;
                        control.live = false;
                        var cleanup: Control = .{};
                        const cleanup_payload = cleanup.payload();
                        try checkpoint.retirePhysicalWithAdmission(read_admission.Port.init(&cleanup_payload));
                    };
                    stats = .{ .checks = control.calls, .allocations = 0, .observed = rejected };
                    control.live = false;
                    if (target != null) try std.testing.expect(rejected);
                    try std.testing.expect(try join_receipt.?.completedWithAdmission(original));
                    try std.testing.expect(checkpoint.physical == null and !checkpoint.queue_retained);
                    try std.testing.expectEqualDeep(completion, try checkpoint.completionWithAdmission(original));
                    try std.testing.expectError(error.InvalidArgument, checkpoint.prepareWithAdmission(native.storage(), original));
                }
                try backend.installRegisteredCheckpointWithAdmission(checkpoint, candidate, original);
                candidate_live = false;
            }
            if (mode != .checkpoint_join_abort and mode != .checkpoint_join_wait) {
                try std.testing.expect(backend.registered_manifest_checkpoint != null and !backend.manifest_recovery_required);
                try std.testing.expect(!backend.manifest_dirty and !backend.obsolete_manifest_dirty);
                try std.testing.expectEqual(@as(u64, 0), backend.unpublished_wal_logical_bytes);
                try std.testing.expectEqual(@as(usize, 2), backend.manifest_journal.obsolete.count());
                if (join_mode) {
                    try std.testing.expect(checkpoint.metadata_borrowed.load(.acquire));
                    try std.testing.expect(try join_receipt.?.completedWithAdmission(original));
                }
            }
        }
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
        {
            var reader = try backend.beginReadWithAdmission(original);
            defer reader.abort();
            try std.testing.expectEqualStrings("key-000", try reader.getWithAdmission(.{ .name = "docs" }, "key-000", original));
            try std.testing.expectEqualStrings("key-001", try reader.getWithAdmission(.{ .name = "docs" }, "key-001", original));
            if (memory) try std.testing.expectEqualStrings("retained-memory", try reader.getWithAdmission(.{ .name = "docs" }, "key-memory", original));
        }
        backend.abandonAfterCrash();
        backend_live = false;
        RegisteredOutputFixture.drain(&resources.resource_manager);
        if (join_receipt) |receipt| {
            try std.testing.expect(try receipt.completedWithAdmission(original));
            const Denied = struct {
                fn check(_: *anyopaque) !void {
                    return error.Canceled;
                }
            };
            var tag: u8 = 0;
            const denied: read_admission.Payload = .{ .ptr = &tag, .vtable = &.{ .checkpoint = Denied.check } };
            try std.testing.expectError(error.Canceled, receipt.completedWithAdmission(read_admission.Port.init(&denied)));
            try receipt.resetWithAdmission(original);
            join_receipt = null;
            try std.testing.expectError(error.InvalidArgument, receipt.completedWithAdmission(original));
        }
        try std.testing.expectEqual(@as(u64, 0), resources.quota_lifetimes.load(.acquire));
        // Both an uncheckpointed metadata migration and the first registered
        // checkpoint must reopen with the same values from durable native files.
        var reopened = try lsm.Backend.open(owned, root, options);
        defer reopened.abandonAfterCrash();
        var reader = try reopened.beginRead();
        defer reader.abort();
        try std.testing.expectEqualStrings("key-000", try reader.get(.{ .name = "docs" }, "key-000"));
        try std.testing.expectEqualStrings("key-001", try reader.get(.{ .name = "docs" }, "key-001"));
        if (mode == .memory_flush_checkpoint) {
            try std.testing.expectEqualStrings("retained-memory", try reader.get(.{ .name = "docs" }, "key-memory"));
            try std.testing.expectError(error.NotFound, reader.get(.{ .name = "docs" }, "tombstone-memory"));
            var cursor = reopened.run_directory.?.readCursor();
            while (cursor.next()) |handle| if (handle.run.id >= 4) {
                try std.testing.expectEqual(@as(u64, 1), handle.run.visibility_id);
                try std.testing.expect(handle.run.gc_requested and handle.run.tombstone_count != null);
                if (handle.run.tombstone_count.? != 0) try std.testing.expectEqual(@as(u64, 123), handle.run.oldest_tombstone_unix_ns);
            };
        }
        return stats;
    }

    fn convertMemory(backend: *lsm.Backend, manager: *ResourceManager, storage: io_mod.Storage, failing: *std.testing.FailingAllocator, control: *Control, original: read_admission.Port, mode: Mode, target: ?usize, oom: bool) !Stats {
        const Flush = lsm.Backend.RegisteredMemoryRunFlush;
        var frame_credit = try manager.reserveWithoutReclaimWithAdmission(.lsm_table_builder_working_set, @sizeOf(Flush), original);
        defer frame_credit.release();
        var old_reader = try backend.beginReadWithAdmission(original);
        defer old_reader.abort();
        try std.testing.expectEqualStrings("key-001", try old_reader.getWithAdmission(.{ .name = "docs" }, "key-001", original));
        var flush: Flush = .{};
        defer flush.deinit();
        const old_root = backend.run_directory;
        const old_debt = backend.unpublished_wal_logical_bytes;
        const start = failing.alloc_index;
        control.calls = 0;
        control.target = if (oom) null else target;
        control.live = true;
        if (oom and target != null) failing.fail_index = start + target.?;
        var retry: FlushPublicationFixture.Retry = .{ .failing = failing, .control = control, .oom = oom, .abort_on_error = mode == .memory_flush_abort };
        const result: anyerror!void = work: {
            retry.call(Flush.prepareWithAdmission, .{ &flush, backend, original }) catch |cause| break :work cause;
            {
                try std.testing.expect(backend.mu.tryLock());
                defer backend.mu.unlock();
                {
                    control.live = false;
                    defer control.live = true;
                    var busy = (try backend.tryBeginManifestTurnWithAdmission(original)).?;
                    defer busy.deinit();
                    try std.testing.expectError(error.ResourceBudgetExceeded, flush.captureWithAdmission(original));
                }
                const captured = retry.call(Flush.captureWithAdmission, .{ &flush, original }) catch |cause| break :work cause;
                try std.testing.expect(captured);
            }
            retry.call(Flush.selectWithAdmission, .{ &flush, original }) catch |cause| break :work cause;
            {
                try std.testing.expect(backend.mu.tryLock());
                defer backend.mu.unlock();
                retry.call(Flush.reserveWithAdmission, .{ &flush, original }) catch |cause| break :work cause;
            }
            retry.call(Flush.buildWithAdmission, .{ &flush, Scope.FlushBuilder.Config{ .first_id = 0, .end_id = 0, .max_entries = 1, .target_bytes = 0, .max_file_bytes = 1 << 20 }, original }) catch |cause| break :work cause;
            retry.call(Flush.preparePublicationWithAdmission, .{ &flush, original }) catch |cause| break :work cause;
            if (mode == .memory_flush_stale) {
                backend.run_directory_generation += 1;
                try std.testing.expectError(error.CompactionPlanningStale, flush.installWithAdmission(quotaControl()));
                backend.run_directory_generation -= 1;
                break :work {};
            }
            {
                try std.testing.expect(backend.mu.tryLock());
                defer backend.mu.unlock();
                retry.call(Flush.installWithAdmission, .{ &flush, original }) catch |cause| break :work cause;
            }
        };
        const stats: Stats = .{ .checks = control.calls, .allocations = failing.alloc_index - start, .observed = retry.observed };
        control.live = false;
        if (target != null) try std.testing.expect(retry.observed);
        if (result) |_| {} else |cause| {
            try std.testing.expect(mode == .memory_flush_abort and retry.observed);
            try std.testing.expectError(if (oom) error.OutOfMemory else control.failure, @as(anyerror!void, cause));
        }
        const installed = flush.live;
        if (installed) {
            try std.testing.expectEqual(@as(usize, 0), backend.run_directory.?.memory_run_count);
            try std.testing.expectEqual(@as(usize, 5), backend.runs.count());
            try std.testing.expect(backend.unpublished_wal_logical_bytes > old_debt);
            var cursor = try flush.batch.?.cursorWithAdmission(original);
            while (try cursor.nextWithAdmission(original)) |view| {
                try std.testing.expectEqual(@as(u64, 1), view.metadata.visibility_id);
                try std.testing.expectEqual(@as(u32, if (mode == .memory_flush_level) 2 else 0), view.metadata.level);
                try std.testing.expect(view.metadata.gc_requested and view.metadata.tombstone_count != null);
                if (view.tombstones != 0) try std.testing.expectEqual(@as(u64, 123), view.metadata.oldest_tombstone_unix_ns);
            }
        } else {
            try std.testing.expect(backend.run_directory == old_root);
            try std.testing.expectEqual(old_debt, backend.unpublished_wal_logical_bytes);
        }
        {
            try std.testing.expect(backend.mu.tryLock());
            defer backend.mu.unlock();
            flush.deinit();
        }
        // All accepted IO has joined. A fault-exhausted allocator must not be
        // needed to stop source borrows or hand off output cleanup obligations.
        for (0..2048) |_| _ = manager.drainPayloadRetirementQuiesced();
        failing.fail_index = std.math.maxInt(usize);
        const queue = backend.options.unpublished_outputs.?;
        while (queue.pop()) |ticket| {
            defer ticket.destroy();
            try std.testing.expect(!installed);
            storage.deleteFileAbsolute(ticket.path) catch |cause| if (cause != error.FileNotFound) return cause;
        }
        try std.testing.expectEqualStrings("retained-memory", try old_reader.getWithAdmission(.{ .name = "docs" }, "key-memory", original));
        try std.testing.expectEqualStrings("key-001", try old_reader.getWithAdmission(.{ .name = "docs" }, "key-001", original));
        try std.testing.expect(!backend.manifest_publish_in_flight);
        return stats;
    }
};

test "physical cache metadata legacy root migration installs real nonempty roots and keeps durable debt" {
    _ = try LegacyMigrationFixture.run(.install, null, false);
}

test "physical cache metadata legacy root migration rejects stale source lineage and prepared edits" {
    for ([_]LegacyMigrationFixture.Mode{ .stale, .receipt, .lineage, .account, .install_refusal }) |mode| _ = try LegacyMigrationFixture.run(mode, null, false);
}

test "physical cache metadata legacy root migration preserves original refusals and partial abort" {
    for ([_]usize{ 0, 1, 2, 8, 16, 32, 64, 128, 256, 512 }) |at| {
        _ = try LegacyMigrationFixture.run(.install, at, false);
        _ = try LegacyMigrationFixture.run(.abort, at, false);
    }
}

test "physical cache metadata legacy root migration retires allocation refusals with source intact" {
    for ([_]usize{ 0, 1, 2, 4, 8, 16, 32, 64, 128 }) |at| {
        _ = try LegacyMigrationFixture.run(.install, at, true);
        _ = try LegacyMigrationFixture.run(.abort, at, true);
    }
}

test "physical cache metadata legacy root migration installs first registered durable checkpoint and reopens" {
    _ = try LegacyMigrationFixture.run(.checkpoint, null, false);
}

test "physical cache metadata legacy root migration preserves late copy and ledger refusal ownership" {
    const healthy = try LegacyMigrationFixture.run(.install, null, false);
    for ([_]usize{ 1, 2, 8, 16, 32, 64 }) |offset| {
        try std.testing.expect(offset <= healthy.checks and offset <= healthy.allocations);
        try std.testing.expect((try LegacyMigrationFixture.run(.install, healthy.checks - offset, false)).observed);
        try std.testing.expect((try LegacyMigrationFixture.run(.abort, healthy.checks - offset, false)).observed);
        try std.testing.expect((try LegacyMigrationFixture.run(.install, healthy.allocations - offset, true)).observed);
        try std.testing.expect((try LegacyMigrationFixture.run(.abort, healthy.allocations - offset, true)).observed);
    }
}

test "physical cache metadata bounded legacy retirement consumes actual directory and ledger roots" {
    _ = try LegacyMigrationFixture.run(.retire_dirs, null, false);
}

test "physical cache metadata bounded legacy retirement resumes every original rejection and joins partial abort" {
    const healthy = try LegacyMigrationFixture.run(.retire_dirs, null, false);
    for (0..healthy.checks) |at| {
        try std.testing.expect((try LegacyMigrationFixture.run(.retire_dirs, at, false)).observed);
        try std.testing.expect((try LegacyMigrationFixture.run(.retire_abort, at, false)).observed);
    }
}

const LegacyLedgerRetirementFixture = struct {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    fn run(target: ?usize, abort: bool) !usize {
        const a = std.testing.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const owned = failing.allocator();
        var ledger: lsm.repository.ObsoleteLedger = .{};
        var ledger_live = true;
        defer if (ledger_live) ledger.deinit(owned);
        try ledger.ensureUnusedCapacity(owned, 1);
        const path = try owned.alloc(u8, 96 * 1024 + 7);
        @memset(path, 0x31);
        ledger.appendAssumeCapacity(.{ .path = path, .delete_after_ns = std.math.maxInt(u64) });
        const account = ledger.tree.account.?.retain();
        defer account.release();
        var job: lsm.repository.ObsoleteLedger.Reclaimer = .init(ledger);
        ledger_live = false;
        var complete = false;
        defer if (!complete) {
            var credits: usize = std.math.maxInt(usize);
            std.debug.assert(job.step(owned, &credits));
        };
        failing.fail_index = failing.alloc_index;
        const before = failing.alloc_index;
        var control: RegisteredOutputFixture.Control = .{ .live = true, .target = target };
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        var iterations: usize = 0;
        var rejected = false;
        while (true) : (iterations += 1) {
            try std.testing.expect(iterations < 128);
            const prior_bytes = account.bytes.load(.acquire);
            var credits: usize = 1;
            const finished = job.stepWithAdmission(owned, &credits, original) catch |cause| {
                try std.testing.expectError(error.Canceled, @as(anyerror!void, cause));
                try std.testing.expect(!rejected);
                rejected = true;
                try std.testing.expect(account.bytes.load(.acquire) <= prior_bytes);
                if (job.tree.retiring_entry) |retiring| if (retiring.payload != null) try std.testing.expect(account.bytes.load(.acquire) >= path.len);
                if (abort) break;
                continue;
            };
            if (finished) {
                complete = true;
                break;
            }
        }
        if (target != null) try std.testing.expect(rejected);
        if (complete) {
            try std.testing.expect(iterations >= 7);
            try std.testing.expect(account.bytes.load(.acquire) < 1024);
        }
        try std.testing.expectEqual(before, failing.alloc_index);
        return control.calls;
    }
};

test "physical cache metadata bounded legacy retirement limits long path cleanup to physical byte credits" {
    const checks = try LegacyLedgerRetirementFixture.run(null, false);
    for (0..checks) |at| {
        _ = try LegacyLedgerRetirementFixture.run(at, false);
        _ = try LegacyLedgerRetirementFixture.run(at, true);
    }
}

test "physical cache metadata legacy root migration imports mixed file and shared memory roots" {
    _ = try LegacyMigrationFixture.runKind(.retire_all, null, false, true);
}

test "physical cache metadata legacy root migration preserves mixed root copy refusals and abort" {
    const checks = (try LegacyMigrationFixture.runKind(.install, null, false, true)).checks;
    // Include the state-copy and handoff boundaries, while the standalone
    // snapshot suite exhausts every internal copy and retirement checkpoint.
    for ([_]usize{ 0, checks / 4, checks / 2, checks * 3 / 4, checks - 1 }) |at| {
        _ = try LegacyMigrationFixture.runKind(.install, at, false, true);
        _ = try LegacyMigrationFixture.runKind(.abort, at, false, true);
    }
}

test "physical cache metadata legacy root migration preserves mixed root allocation refusals" {
    const count = (try LegacyMigrationFixture.runKind(.install, null, false, true)).allocations;
    for ([_]usize{ 0, count / 4, count / 2, count * 3 / 4, count - 1 }) |at| {
        _ = try LegacyMigrationFixture.runKind(.install, at, true, true);
        _ = try LegacyMigrationFixture.runKind(.abort, at, true, true);
    }
}

test "physical cache metadata registered memory flush preserves visibility and durable reopen" {
    _ = try LegacyMigrationFixture.runKind(.memory_flush_checkpoint, null, false, true);
    _ = try LegacyMigrationFixture.runKind(.memory_flush_level, null, false, true);
}

test "physical cache metadata registered memory flush rejects stale publication with retained readers" {
    _ = try LegacyMigrationFixture.runKind(.memory_flush_stale, null, false, true);
}

test "physical cache metadata registered memory flush resumes original handoff refusals and abort" {
    const stats = try LegacyMigrationFixture.runKind(.memory_flush, null, false, true);
    for ([_]usize{ 0, stats.checks / 4, stats.checks / 2, stats.checks * 3 / 4, stats.checks - 1 }) |at| {
        _ = try LegacyMigrationFixture.runKind(.memory_flush, at, false, true);
        _ = try LegacyMigrationFixture.runKind(.memory_flush_abort, at, false, true);
    }
}

test "physical cache metadata registered memory flush owns allocation refusals through abort" {
    const stats = try LegacyMigrationFixture.runKind(.memory_flush, null, false, true);
    for ([_]usize{ 0, stats.allocations / 4, stats.allocations / 2, stats.allocations * 3 / 4, stats.allocations - 1 }) |at| {
        _ = try LegacyMigrationFixture.runKind(.memory_flush, at, true, true);
        _ = try LegacyMigrationFixture.runKind(.memory_flush_abort, at, true, true);
    }
}

test "physical cache metadata checkpoint physical join separates metadata and survives issuer exit" {
    _ = try LegacyMigrationFixture.runKind(.checkpoint_join, null, false, false);
}

test "physical cache metadata checkpoint physical join observes actual abort cleanup under fresh authority" {
    _ = try LegacyMigrationFixture.runKind(.checkpoint_join_abort, null, false, false);
}

test "physical cache metadata checkpoint physical join waits only under live bounded authority" {
    _ = try LegacyMigrationFixture.runKind(.checkpoint_join_wait, null, false, false);
}

test "physical cache metadata checkpoint physical join retains direct cleanup across original refusal" {
    const stats = try LegacyMigrationFixture.runKind(.checkpoint_join, null, false, false);
    for ([_]usize{ 0, stats.checks / 2, stats.checks - 1 }) |at| {
        _ = try LegacyMigrationFixture.runKind(.checkpoint_join, at, false, false);
    }
}

const LegacyMemoryRetirementFixture = struct {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const State = @import("antfly_local_sources").storage_lsm_backend_state.State;
    const Directory = @typeInfo(@typeInfo(@FieldType(lsm.Backend, "run_directory")).optional.child).pointer.child;
    fn run(target: ?usize, abort: bool) !usize {
        const a = std.testing.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const owned = failing.allocator();
        var backend = lsm.Backend.init(owned, .{});
        defer backend.close();
        var state: State = .{};
        var state_live = true;
        defer if (state_live) state.deinit(owned);
        const value = try a.alloc(u8, 32 * 1024 + 3);
        defer a.free(value);
        @memset(value, 0x71);
        try state.upsert(owned, .{ .name = "docs" }, "key", value, false);
        var source: lsm.repository.Run = .{
            .id = 1,
            .level = 0,
            .size_bytes = 0,
            .path = null,
            .smallest_namespace_name = try owned.dupe(u8, "docs"),
            .smallest_key = try owned.dupe(u8, "key"),
            .largest_namespace_name = try owned.dupe(u8, "docs"),
            .largest_key = try owned.dupe(u8, "key"),
            .entry_count = 1,
            .bloom_filter = null,
            .state = state,
        };
        state_live = false;
        var source_live = true;
        defer if (source_live) source.deinit(owned);
        try source.shareMemory(owned);
        const directory = try Directory.create(owned);
        var directory_live = true;
        defer if (directory_live) directory.destroy(owned);
        try directory.put(&backend, source);
        source.deinit(owned);
        source_live = false;
        var job = Directory.Reclaimer.init(directory);
        directory_live = false;
        var finished = false;
        defer if (!finished) {
            var credits: usize = std.math.maxInt(usize);
            std.debug.assert(job.step(owned, &credits));
            job.finish(owned);
        };
        failing.fail_index = failing.alloc_index;
        const before = failing.alloc_index;
        var control: RegisteredOutputFixture.Control = .{ .live = true, .target = target };
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        var rejected = false;
        while (true) {
            var credits: usize = 1;
            const complete = job.stepWithAdmission(owned, &credits, original) catch |cause| {
                try std.testing.expectError(error.Canceled, @as(anyerror!void, cause));
                try std.testing.expect(!rejected);
                rejected = true;
                if (abort) break;
                continue;
            };
            if (complete) {
                job.finish(owned);
                finished = true;
                break;
            }
        }
        if (target != null) try std.testing.expect(rejected);
        try std.testing.expectEqual(before, failing.alloc_index);
        return control.calls;
    }
};

test "physical cache metadata bounded legacy retirement retains shared memory completion through refusal" {
    const checks = try LegacyMemoryRetirementFixture.run(null, false);
    for (0..checks) |at| {
        _ = try LegacyMemoryRetirementFixture.run(at, false);
        _ = try LegacyMemoryRetirementFixture.run(at, true);
    }
}

const LegacyWriterRetirementFixture = struct {
    const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
    const Store = @FieldType(lsm.Backend, "runs");
    const State = @import("antfly_local_sources").storage_lsm_backend_state.State;
    const Mode = enum { staged, revision, parent_final, owned };
    fn make(a: std.mem.Allocator, memory: bool) !lsm.repository.Run {
        var physical_run: lsm.repository.Run = .{
            .id = 901,
            .level = 1,
            .size_bytes = 0,
            .path = if (memory) null else try a.dupe(u8, "/writer/901.tbl"),
            .smallest_namespace_name = null,
            .smallest_key = try a.alloc(u8, 32 * 1024 + 3),
            .largest_namespace_name = null,
            .largest_key = try a.alloc(u8, 32 * 1024 + 7),
            .entry_count = 1,
            .bloom_filter = null,
            .state = null,
        };
        @memset(physical_run.smallest_key, 'a');
        @memset(physical_run.largest_key, 'z');
        if (memory) {
            var state: State = .{};
            try state.upsert(a, .{ .name = "docs" }, "key", "retained", false);
            physical_run.state = state;
            try physical_run.shareMemory(a);
        }
        return physical_run;
    }
    fn run(mode: Mode, target: ?usize, abort: bool) !usize {
        const a = std.testing.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const owned = failing.allocator();
        var source = try make(owned, mode != .staged);
        var source_live = true;
        defer if (source_live) source.deinit(owned);
        var live: Store = .{};
        var live_valid = true;
        defer if (live_valid) live.deinit(owned);
        var candidate: Store = .{};
        var candidate_valid = true;
        defer if (candidate_valid) candidate.deinit(owned);
        if (mode == .staged) {
            try candidate.stage(owned, source);
        } else {
            try live.append(owned, source);
            source_live = false;
            if (mode == .revision or mode == .parent_final) {
                candidate = live.fork();
                const raw = live.at(0);
                try candidate.remove(owned, raw);
                var metadata = raw.*;
                metadata.level = 2;
                try candidate.stageRevision(owned, Store.revision(raw, metadata));
                const child = candidate.at(0);
                child.bloom_filter = .{ .bytes = try owned.alloc(u8, 48 * 1024 + 9), .bit_count = (48 * 1024 + 9) * 8, .hash_count = 3 };
                @memset(child.bloom_filter.?.bytes, 0x6a);
                child.owns_bloom_filter = true;
                if (mode == .parent_final) {
                    live.deinit(owned);
                    live = .{};
                }
            } else {
                candidate = live;
                live = .{};
            }
        }
        var job = Store.Reclaimer.init(&candidate);
        candidate_valid = false;
        var finished = false;
        defer if (!finished) {
            var credits: usize = std.math.maxInt(usize);
            std.debug.assert(job.step(owned, &credits));
            job.finishContents();
        };
        failing.fail_index = failing.alloc_index;
        const before = failing.alloc_index;
        var control: RegisteredOutputFixture.Control = .{ .live = true, .target = target };
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        var rejected = false;
        while (true) {
            var credits: usize = 1;
            const complete = job.stepWithAdmission(owned, &credits, original) catch |cause| {
                try std.testing.expectError(error.Canceled, @as(anyerror!void, cause));
                try std.testing.expect(!rejected);
                rejected = true;
                if (abort) break;
                continue;
            };
            if (complete) {
                job.finishContents();
                finished = true;
                break;
            }
        }
        if (target != null) try std.testing.expect(rejected);
        try std.testing.expectEqual(before, failing.alloc_index);
        if (finished and mode == .staged) {
            try std.testing.expectEqual(@as(u8, 'a'), source.smallest_key[0]);
            try std.testing.expectEqualStrings("/writer/901.tbl", source.path.?);
        }
        if (finished and mode == .revision) {
            const raw = live.at(0);
            try std.testing.expectEqual(@as(u8, 'a'), raw.smallest_key[0]);
            try std.testing.expectEqualStrings("retained", try raw.state.?.get(.{ .name = "docs" }, "key"));
        }
        // Cleanup stays allocation-free with the fault allocator exhausted.
        if (finished and live.count() != 0) {
            var parent = Store.Reclaimer.init(&live);
            var credits: usize = std.math.maxInt(usize);
            std.debug.assert(parent.step(owned, &credits));
            parent.finishContents();
            live_valid = false;
        }
        return control.calls;
    }
};

test "physical cache metadata bounded writer retirement consumes migrated native version pins and roots" {
    _ = try LegacyMigrationFixture.run(.retire_all, null, false);
}

test "physical cache metadata bounded writer retirement resumes every original rejection and abort" {
    const healthy = try LegacyMigrationFixture.run(.retire_all, null, false);
    for (0..healthy.checks) |at| {
        try std.testing.expect((try LegacyMigrationFixture.run(.retire_all, at, false)).observed);
        try std.testing.expect((try LegacyMigrationFixture.run(.retire_all_abort, at, false)).observed);
    }
}

test "physical cache metadata bounded writer retirement preserves staged input and parent ownership" {
    for ([_]LegacyWriterRetirementFixture.Mode{ .staged, .revision, .parent_final, .owned }) |mode| {
        const checks = try LegacyWriterRetirementFixture.run(mode, null, false);
        for (0..checks) |at| {
            _ = try LegacyWriterRetirementFixture.run(mode, at, false);
            _ = try LegacyWriterRetirementFixture.run(mode, at, true);
        }
    }
}

test "physical cache metadata bounded writer retirement binds the actual version pin path and issuer" {
    const F = LegacyWriterRetirementFixture;
    const a = std.testing.allocator;
    var backend = F.lsm.Backend.init(a, .{});
    defer backend.close();
    var one = try F.make(a, false);
    var one_live = true;
    defer if (one_live) one.deinit(a);
    var two = try F.make(a, false);
    var two_live = true;
    defer if (two_live) two.deinit(a);
    two.id = 902;
    a.free(two.path.?);
    two.path = try a.dupe(u8, "/writer/902.tbl");
    try backend.runs.append(a, one);
    one_live = false;
    try backend.runs.append(a, two);
    two_live = false;
    try backend.registerOpenManifestRunRefs();
    const raw_one = backend.runs.at(0);
    const raw_two = backend.runs.at(1);
    const first = F.Store.takeVersionPin(raw_one).?;
    const second = F.Store.takeVersionPin(raw_two).?;
    var first_live = true;
    defer if (first_live) {
        first.release();
        var forget = F.lsm.Backend.prepareVersionPinForget("/writer/901.tbl");
        defer forget.deinit();
        _ = forget.planWithAdmission(null) catch unreachable;
        forget.resumeWithAdmission(null) catch unreachable;
        forget.publishWithAdmission(null) catch unreachable;
    };
    var second_live = true;
    defer if (second_live) {
        second.release();
        var forget = F.lsm.Backend.prepareVersionPinForget("/writer/902.tbl");
        defer forget.deinit();
        _ = forget.planWithAdmission(null) catch unreachable;
        forget.resumeWithAdmission(null) catch unreachable;
        forget.publishWithAdmission(null) catch unreachable;
    };
    const Registry = @typeInfo(@TypeOf(F.lsm.Backend.prepareVersionPinForget("/writer/901.tbl").registry)).pointer.child;
    var foreign: Registry = .{ .allocator = a };
    defer foreign.deinitJoined();
    const foreign_pin = try foreign.retainPin("/writer/901.tbl");
    defer {
        foreign_pin.release();
        foreign.forget("/writer/901.tbl");
    }
    try std.testing.expectError(error.InvalidArgument, F.Store.installVersionPin(raw_one, foreign_pin, quotaControl()));
    try std.testing.expectError(error.InvalidArgument, F.Store.installVersionPin(raw_one, second, quotaControl()));
    try std.testing.expectError(error.InvalidArgument, F.Store.installVersionPin(raw_two, first, quotaControl()));
    try std.testing.expect(!raw_one.version_ref_pinned and !raw_two.version_ref_pinned);
    try F.Store.installVersionPin(raw_one, first, quotaControl());
    first_live = false;
    try F.Store.installVersionPin(raw_two, second, quotaControl());
    second_live = false;
    try std.testing.expect(raw_one.version_ref_pinned and raw_two.version_ref_pinned);
}

const LegacyWriterStaleFixture = struct {
    const F = LegacyWriterRetirementFixture;
    const Result = struct { checks: usize, intercepted: bool };
    fn run(target: ?usize) !Result {
        const a = std.testing.allocator;
        var backend = F.lsm.Backend.init(a, .{});
        defer backend.close();
        var input = try F.make(a, false);
        var input_live = true;
        defer if (input_live) input.deinit(a);
        try backend.runs.append(a, input);
        input_live = false;
        try backend.registerOpenManifestRunRefs();
        var detached = backend.runs;
        backend.runs = .{ .destroy_run = F.lsm.Backend.destroyOwnedRun };
        var job = F.Store.Reclaimer.init(&detached);
        var finished = false;
        defer if (!finished) {
            var credits: usize = std.math.maxInt(usize);
            std.debug.assert(job.step(a, &credits));
            job.finishContents();
        };
        var control: RegisteredOutputFixture.Control = .{ .live = true, .target = target };
        const payload = control.payload();
        const original = read_admission.Port.init(&payload);
        var intercepted = false;
        while (true) {
            var credits: usize = 1;
            const complete = job.stepWithAdmission(a, &credits, original) catch |cause| {
                try std.testing.expectError(error.Canceled, @as(anyerror!void, cause));
                if (job.tree.retiring_node) |node| {
                    const record = node.entry.payload.?;
                    const work = &record.retirement;
                    if (work.phase == .version_forget and work.forgetting != null and work.forgetting.?.found and !work.forgetting.?.committed and !work.forgetting.?.retiring) {
                        // Simulate a peer collecting the now-unpinned record
                        // between plan and publish. The saved borrow stays alive.
                        work.forgetting.?.registry.forget(record.run.path.?);
                        intercepted = true;
                    }
                }
                continue;
            };
            if (complete) {
                job.finishContents();
                finished = true;
                break;
            }
        }
        return .{ .checks = control.calls, .intercepted = intercepted };
    }
};

test "physical cache metadata bounded writer retirement replans invalidated registry borrows" {
    const healthy = try LegacyWriterStaleFixture.run(null);
    var exercised = false;
    for (0..healthy.checks) |at| {
        if ((try LegacyWriterStaleFixture.run(at)).intercepted) {
            exercised = true;
            break;
        }
    }
    try std.testing.expect(exercised);
}

test "physical cache metadata bounded writer retirement refuses unknown producers before joined callback" {
    const F = LegacyWriterRetirementFixture;
    const a = std.testing.allocator;
    const Callback = struct {
        var called: usize = 0;
        fn destroy(physical_run: *F.lsm.repository.Run, allocator: std.mem.Allocator) void {
            called += 1;
            physical_run.deinit(allocator);
        }
    };
    Callback.called = 0;
    var store: F.Store = .{ .destroy_run = Callback.destroy };
    var store_live = true;
    defer if (store_live) store.deinit(a);
    var input = try F.make(a, false);
    var input_live = true;
    defer if (input_live) input.deinit(a);
    try store.append(a, input);
    input_live = false;
    var job = F.Store.Reclaimer.init(&store);
    store_live = false;
    var finished = false;
    defer if (!finished) {
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(job.step(a, &credits));
        job.finishContents();
    };
    var refused = false;
    for (0..128) |_| {
        var credits: usize = 1;
        const complete = job.stepWithAdmission(a, &credits, quotaControl()) catch |cause| {
            try std.testing.expectError(error.ExecutionWaitUnsupported, @as(anyerror!void, cause));
            refused = true;
            break;
        };
        try std.testing.expect(!complete);
    }
    try std.testing.expect(refused);
    try std.testing.expectEqual(@as(usize, 0), Callback.called);
    var credits: usize = std.math.maxInt(usize);
    try std.testing.expect(job.step(a, &credits));
    job.finishContents();
    finished = true;
    try std.testing.expectEqual(@as(usize, 1), Callback.called);
}

fn normalAdmittedFlushWorker(mode: enum { publish, cancel_queued, retry, maintenance, maintenance_retry, maintenance_close, waker, maintenance_budget, maintenance_unsupported, publication_interleave, publication_retry, publication_rebase, publication_rebase_retry, retirement_retry, retirement_capacity, publication_directory_retry, output_retirement_retry, output_retirement_close, immutable_retirement_retry, immutable_retirement_close, output_retirement_preflight, manifest_completion_retry, manifest_completion_io_retry, manifest_completion_waker, read_metadata_retry, manifest_completion_fence, manifest_preparation_retry, manifest_preparation_interleave, manifest_preparation_close }) !void {
    const local = @import("antfly_local_sources");
    const lsm = local.storage_lsm_backend_mod;
    const io_mod = local.storage_lsm_backend_storage_io;
    const owners = @import("api/runnable_owner_execution.zig");
    const runtime = @import("antfly_runtime_abi");
    const platform = @import("antfly_platform");
    const scheduler = local.common_request_admission.runnable_scheduler;
    const a = std.testing.allocator;
    const maintenance_origin = mode == .maintenance or mode == .maintenance_retry or mode == .maintenance_close or mode == .waker or mode == .maintenance_budget or mode == .maintenance_unsupported;
    const rebase_publication = mode == .publication_rebase or mode == .publication_rebase_retry;
    const completion_retry = mode == .manifest_completion_retry or mode == .manifest_completion_io_retry or mode == .manifest_completion_waker;
    const preparing_manifest = mode == .manifest_preparation_retry or mode == .manifest_preparation_interleave or mode == .manifest_preparation_close;
    const retry_refusal = mode == .retry or mode == .maintenance_retry;
    var state: owners.State = .{ .owner_alloc = a, .owner = try scheduler.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 }), .native_now_ns = platform.time.monotonicNs };
    defer state.owner.deinit();
    const Refusal = struct {
        var delegate: @FieldType(runtime.runnable_abi.Owner, "start") = undefined;
        var attempts: std.atomic.Value(usize) = .init(0);
        fn start(raw: *anyopaque, request: *const runtime.runnable_abi.Start) callconv(.c) runtime.error_abi.Status {
            const failures = [_]anyerror{ error.SchedulerFull, error.ResourceBudgetExceeded, error.ResourceTemporarilyUnavailable, error.ConcurrencyUnavailable, error.OutOfMemory };
            const index = attempts.fetchAdd(1, .monotonic);
            if (index < failures.len) return runtime.error_abi.statusFromError(failures[index]);
            return delegate(raw, request);
        }
    };
    var worker_owner = state.port();
    if (retry_refusal) {
        Refusal.delegate = worker_owner.start;
        Refusal.attempts.store(0, .release);
        worker_owner.start = Refusal.start;
    }
    var resources = try Resources.initWithBudgets(a, budgets_mod.smartResourceBudgetsForTotal(128 << 20));
    defer resources.deinit();
    resources.attachResourceManager();
    defer RegisteredOutputFixture.drain(&resources.resource_manager);
    var pool = io_mod.NativeStoragePool.initWithCapacityForTest(a, 8);
    defer pool.deinit();
    var native = try io_mod.NativeStorage.initWithPool(a, .threaded, &pool);
    defer native.deinit();
    var storage = native.storage();
    var storage_callbacks = storage.vtable.*;
    if (mode == .maintenance_unsupported) {
        storage_callbacks.try_prepare_durability_with_admission = null;
        storage.vtable = &storage_callbacks;
    }
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/normal-admitted-flush", .{tmp.sub_path});
    defer a.free(root);
    var handle = try lsm.BackendHandle.openWithConfig(a, root, .{
        .storage = storage,
        .resource_manager = &resources.resource_manager,
        .flush_threshold = if (maintenance_origin) 1000 else 1,
        .mutable_idle_flush_after_ns = if (maintenance_origin) 1 else 0,
        .immutable_flush_window_bytes = if (maintenance_origin) std.math.maxInt(u64) else 0,
        .background_io_budget_bytes = if (mode == .maintenance_budget) 1 else 0,
        .background_io_allow_oversized_single_job = false,
        .defer_flush_on_commit = true,
        .max_deferred_immutable_memtables = 128,
        .immutable_flush_window_max_memtables = if (maintenance_origin) 64 else 1,
        .compact_threshold_runs = 1000,
        .l0_soft_limit_runs = 1000,
        .l0_hard_limit_runs = 2000,
    }, .{ .background_runtime = .{ .backend = .io_threaded, .runnable_owner = worker_owner }, .internal_flush_worker = mode == .waker or mode == .retirement_capacity or mode == .manifest_completion_waker });
    var open = true;
    defer if (open) handle.close();
    const publication = local.storage_lsm_backend_compaction_publication;
    const Interleave = struct {
        var claimed: std.atomic.Value(bool) = .init(false);
        var ready: std.Io.Event = .unset;
        var go: std.Io.Event = .unset;
        var execution: ?*owners.State = null;
        var controlled_calls: std.atomic.Value(usize) = .init(0);
        var refuse: std.atomic.Value(bool) = .init(false);
        var refuse_replay: bool = false;
        var replays: std.atomic.Value(usize) = .init(0);
        var cleanup_calls: std.atomic.Value(usize) = .init(0);
        var refuse_cleanup: std.atomic.Value(bool) = .init(false);
        var refuse_directory: std.atomic.Value(bool) = .init(false);
        var saturate_cleanup: bool = false;
        var vector_calls: std.atomic.Value(usize) = .init(0);
        var refuse_vector: std.atomic.Value(bool) = .init(false);
        var keep_vector_queued: bool = false;
        var manifest_calls: std.atomic.Value(usize) = .init(0);
        var manifest_failure: anyerror = error.ResourceBudgetExceeded;
        var fence_manifest: bool = false;
        var preparation_calls: std.atomic.Value(usize) = .init(0);
        var preparation_backend: ?*lsm.Backend = null;
        var preparation_interleave: bool = false;
        var preparation_refuse: bool = false;
        var frame_calls: std.atomic.Value(usize) = .init(0);
        var refuse_frame: std.atomic.Value(bool) = .init(false);
        var keep_frame_queued: bool = false;
        fn frameRetirement(original: read_admission.Port) !void {
            _ = frame_calls.fetchAdd(1, .monotonic);
            try original.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), execution.?.owner.scheduler.stats().cleanup_runnable_slots);
            if (keep_frame_queued or refuse_frame.swap(false, .acq_rel)) return error.ResourceBudgetExceeded;
        }
        fn preparation(original: read_admission.Port) !void {
            const call = preparation_calls.fetchAdd(1, .monotonic);
            try original.checkpoint();
            const stats = execution.?.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 1), stats.runnable_slots - stats.control_slots);
            const backend = preparation_backend.?;
            while (!backend.mu.tryLock()) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            const valid = backend.manifest_journal.sequence != null;
            backend.mu.unlock();
            try std.testing.expect(valid);
            if (call == 0 and preparation_interleave) {
                const wait = (try original.executionWait()).?;
                try wait.beginWait();
                ready.set(std.testing.io);
                const waited = go.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
                try wait.endWait();
                try waited;
            }
            if (call == 0 and preparation_refuse) return error.ResourceBudgetExceeded;
        }
        var metadata_calls: std.atomic.Value(usize) = .init(0);
        var refuse_metadata: std.atomic.Value(bool) = .init(false);
        fn manifest(raw: *anyopaque) !void {
            const backend: *lsm.Backend = @ptrCast(@alignCast(raw));
            const call = manifest_calls.fetchAdd(1, .monotonic);
            while (!backend.mu.tryLock()) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            defer backend.mu.unlock();
            if (fence_manifest) {
                try std.testing.expectEqual(@as(usize, 0), call);
                backend.manifest_recovery_required = true;
                return error.RecoveryRequired;
            }
            if (call < 3) {
                // The normal builder has already retired its sole input. The
                // retry must complete this accepted publication without a new
                // write, sync, or another SST construction.
                try std.testing.expect(backend.flush_completion_pending);
                try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
                try std.testing.expectEqual(@as(u64, 1), backend.write_stats.immutable_flushes);
                try std.testing.expectEqual(call == 0, backend.immutable_flush_job_in_flight);
                return manifest_failure;
            }
        }
        fn metadata(original: read_admission.Port) !void {
            _ = metadata_calls.fetchAdd(1, .monotonic);
            try original.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), execution.?.owner.scheduler.stats().cleanup_runnable_slots);
            if (refuse_metadata.swap(false, .acq_rel)) return error.ResourceBudgetExceeded;
        }
        var immutable_calls: std.atomic.Value(usize) = .init(0);
        var refuse_immutable: std.atomic.Value(bool) = .init(false);
        var keep_immutable_queued: bool = false;
        fn immutable(original: read_admission.Port) !void {
            _ = immutable_calls.fetchAdd(1, .monotonic);
            try original.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), execution.?.owner.scheduler.stats().cleanup_runnable_slots);
            if (keep_immutable_queued or refuse_immutable.swap(false, .acq_rel)) return error.ResourceBudgetExceeded;
        }
        fn vector(original: read_admission.Port) !void {
            _ = vector_calls.fetchAdd(1, .monotonic);
            try original.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), execution.?.owner.scheduler.stats().cleanup_runnable_slots);
            if (keep_vector_queued or refuse_vector.swap(false, .acq_rel)) return error.ResourceBudgetExceeded;
        }
        fn directory(original: read_admission.Port) !void {
            try original.checkpoint();
            if (refuse_directory.swap(false, .acq_rel)) return error.ResourceBudgetExceeded;
        }
        fn cleanup(original: read_admission.Port) !void {
            _ = cleanup_calls.fetchAdd(1, .monotonic);
            try original.checkpoint();
            try std.testing.expect(execution.?.cleanup_in_use.load(.acquire));
            if (refuse_cleanup.swap(false, .acq_rel)) return error.ResourceBudgetExceeded;
            if (saturate_cleanup) {
                const io = std.testing.io;
                const borrowed = runtime.native_abi.IoBorrow.init(&io);
                const owner = execution.?.port();
                var query: ?*anyopaque = null;
                const occupied = owner.start(owner.context, &.{ .io = &borrowed, .class = @backingInt(runtime.runnable_abi.Class.query), .allow_wait = 0, .out_handle = &query });
                defer if (query) |held| owner.finish(held);
                // A peer may already own ordinary capacity. Either way the
                // cleanup pass keeps its protected slot and health can enter.
                if (!occupied.isOk()) try std.testing.expectEqual(@as(usize, 2), execution.?.owner.scheduler.stats().runnable_slots);
                var health: ?*anyopaque = null;
                const healthy = owner.start(owner.context, &.{ .io = &borrowed, .class = @backingInt(runtime.runnable_abi.Class.control), .allow_wait = 0, .out_handle = &health });
                defer if (health) |held| owner.finish(held);
                try std.testing.expect(healthy.isOk());
                try original.checkpoint();
            }
        }
        fn controlled(_: *lsm.Backend, original: read_admission.Port) !void {
            const call = controlled_calls.fetchAdd(1, .monotonic);
            try original.checkpoint();
            const stats = execution.?.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 1), stats.runnable_slots - stats.control_slots);
            if (refuse.swap(false, .acq_rel) or (refuse_replay and call == 1)) return error.ResourceBudgetExceeded;
        }
        fn hook(_: *lsm.Backend, job: *const publication.Job) !void {
            _ = replays.fetchMax(job.rebases, .monotonic);
            if (!job.append_only or claimed.swap(true, .acq_rel)) return;
            const stats = execution.?.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), stats.runnable_slots - stats.control_slots);
            ready.set(std.testing.io);
            go.waitUncancelable(std.testing.io);
        }
    };
    Interleave.execution = &state;
    Interleave.controlled_calls.store(0, .release);
    Interleave.refuse.store(mode == .publication_retry, .release);
    Interleave.refuse_replay = mode == .publication_rebase_retry;
    Interleave.replays.store(0, .release);
    Interleave.cleanup_calls.store(0, .release);
    Interleave.refuse_cleanup.store(mode == .retirement_retry, .release);
    Interleave.refuse_directory.store(mode == .publication_directory_retry, .release);
    Interleave.saturate_cleanup = mode == .retirement_capacity;
    Interleave.vector_calls.store(0, .release);
    Interleave.refuse_vector.store(mode == .output_retirement_retry, .release);
    Interleave.keep_vector_queued = mode == .output_retirement_close or mode == .output_retirement_preflight;
    FlushPublicationFixture.Batch.test_vector_retirement_hook = Interleave.vector;
    Interleave.immutable_calls.store(0, .release);
    Interleave.refuse_immutable.store(mode == .immutable_retirement_retry, .release);
    Interleave.keep_immutable_queued = mode == .immutable_retirement_close;
    publication.memtable_reclamation.Job.test_admission_hook = Interleave.immutable;
    Interleave.manifest_calls.store(0, .release);
    Interleave.manifest_failure = if (mode == .manifest_completion_io_retry) error.InputOutput else error.ResourceBudgetExceeded;
    Interleave.fence_manifest = mode == .manifest_completion_fence;
    if (completion_retry or mode == .manifest_completion_fence) lsm.repository.publication_test_hook = .{ .context = handle.ptr(), .run = Interleave.manifest };
    Interleave.metadata_calls.store(0, .release);
    Interleave.refuse_metadata.store(mode == .read_metadata_retry, .release);
    publication.read_metadata_reclamation.Job.test_admission_hook = Interleave.metadata;
    publication.retirement_execution.test_hook = Interleave.cleanup;
    publication.test_directory_hook = Interleave.directory;
    publication.test_controlled_hook = Interleave.controlled;
    if (mode == .publication_interleave or rebase_publication) {
        Interleave.claimed.store(false, .release);
        Interleave.ready = .unset;
        Interleave.go = .unset;
        publication.test_preparation_hook = Interleave.hook;
    }
    defer {
        Interleave.go.set(std.testing.io);
        if (open) {
            handle.close();
            open = false;
        }
        publication.test_preparation_hook = null;
        publication.test_controlled_hook = null;
        publication.test_directory_hook = null;
        publication.retirement_execution.test_hook = null;
        FlushPublicationFixture.Batch.test_vector_retirement_hook = null;
        publication.memtable_reclamation.Job.test_admission_hook = null;
        lsm.repository.publication_test_hook = null;
        publication.read_metadata_reclamation.Job.test_admission_hook = null;
        publication.manifest_edit_preparation.test_hook = null;
        publication.manifest_edit_preparation.test_retirement_hook = null;
        Interleave.execution = null;
    }
    const io = std.testing.io;
    const borrowed = runtime.native_abi.IoBorrow.init(&io);
    const owner = state.port();
    var foreground: ?*anyopaque = null;
    const started = owner.start(owner.context, &.{ .io = &borrowed, .class = @backingInt(runtime.runnable_abi.Class.query), .allow_wait = 0, .out_handle = &foreground });
    try std.testing.expect(started.isOk());
    defer if (foreground) |held| owner.finish(held);
    {
        var write = try handle.ptr().beginWrite();
        defer write.abort();
        try write.put(.{}, "admitted", "durable");
        try write.commit();
    }
    const deadline = platform.time.monotonicNs() + 10 * std.time.ns_per_s;
    const Original = struct {
        active: std.atomic.Value(bool) = .init(true),
        checks: std.atomic.Value(usize) = .init(0),
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.checks.fetchAdd(1, .monotonic);
            return if (self.active.load(.acquire)) .ok else runtime.error_abi.statusFromError(error.Canceled);
        }
    };
    var original: Original = .{};
    const lifetime: runtime.runnable_abi.Reentry = .{ .context = &original, .check = Original.check };
    if (maintenance_origin and mode != .waker) {
        // The server's selected native backend call returns the construction
        // handoff while the original synchronous authority still lives. The
        // task must never retain that stack authority or require another write.
        try io.sleep(.fromMilliseconds(1), .awake);
        for (0..32) |_| {
            _ = try handle.ptr().runMaintenanceStepBestEffortWithLifetime(&lifetime);
            if (state.owner.scheduler.stats().queued != 0) break;
            if (mode == .maintenance_budget or mode == .maintenance_unsupported) break;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        original.active.store(false, .release);
    }
    if (mode == .maintenance_unsupported) {
        try std.testing.expectEqual(@as(u64, 1), handle.ptr().snapshotWriteStats().immutable_flushes);
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().queued);
        handle.close();
        open = false;
        owner.finish(foreground.?);
        foreground = null;
        RegisteredOutputFixture.drain(&resources.resource_manager);
        var reopened = try lsm.Backend.open(a, root, .{ .storage = native.storage(), .backend = .{ .read_only = true } });
        defer reopened.close();
        var read = try reopened.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("durable", try read.get(.{}, "admitted"));
        return;
    }
    if (mode == .maintenance_budget) {
        const stats = handle.ptr().snapshotWriteStats();
        try std.testing.expectEqual(@as(u64, 0), stats.immutable_flushes);
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().queued);
        try std.testing.expect(!handle.ptr().immutable_flush_job_in_flight);
        try std.testing.expect((handle.ptr().nextMaintenanceWakeDelayNsBestEffort() orelse 0) > 0);
        // A later budget change retries the same idle partial cut, with no
        // fresh write and no synchronous fallback on the denied attempt.
        while (!handle.ptr().mu.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
        handle.ptr().options.background_io_budget_bytes = 0;
        handle.ptr().mu.unlock();
        original.active.store(true, .release);
        _ = try handle.ptr().runMaintenanceStepWithLifetime(&lifetime);
        original.active.store(false, .release);
    }
    while (state.owner.scheduler.stats().queued == 0) {
        if (platform.time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    // The actual detached worker is queued before constructing any physical
    // SST. Its immutable and WAL remain visible while a query owns capacity.
    while (!handle.ptr().mu.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
    const pending = handle.ptr().activeImmutableMemtableCount();
    const flushes = handle.ptr().write_stats.immutable_flushes;
    handle.ptr().mu.unlock();
    try std.testing.expectEqual(@as(usize, 1), pending);
    try std.testing.expectEqual(@as(u64, 0), flushes);
    if (maintenance_origin) {
        const retry_delay = handle.ptr().nextMaintenanceWakeDelayNsBestEffort();
        if (retry_delay) |delay| try std.testing.expect(delay > 0);
    }
    const synchronous_checks = original.checks.load(.acquire);
    if (maintenance_origin and mode != .waker) {
        original.active.store(true, .release);
        try std.testing.expect(!try handle.ptr().runMaintenanceStepBestEffortWithLifetime(&lifetime));
        original.active.store(false, .release);
    }
    const final_synchronous_checks = original.checks.load(.acquire);
    try std.testing.expect(final_synchronous_checks >= synchronous_checks);
    if (mode == .cancel_queued or mode == .maintenance_close) {
        // Closing cancels the original queued pass even while the foreground
        // retains ordinary capacity; joined close persists the immutable tail.
        handle.close();
        open = false;
    } else {
        owner.finish(foreground.?);
        foreground = null;
        var pinned: ?local.storage_lsm_backend_runtime.NamespaceReadTxn(lsm.Backend) = null;
        defer if (pinned) |*read| read.abort();
        if (mode == .publication_interleave or rebase_publication) {
            while (!Interleave.ready.isSet()) {
                if (platform.time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            // This is the actual native worker paused during off-lock metadata
            // preparation. A peer can pin its captured immutable and commit a
            // new generation before the final publication swap.
            try std.testing.expect(handle.ptr().mu.tryLock());
            handle.ptr().mu.unlock();
            pinned = try handle.ptr().beginRead();
            try std.testing.expectEqualStrings("durable", try pinned.?.get(.{}, "admitted"));
            var peer = try handle.ptr().beginWrite();
            defer peer.abort();
            try peer.put(.{}, "admitted", "newer");
            try peer.commit();
            if (rebase_publication) {
                // A concurrent metadata owner changes the authoritative ledger
                // while the actual worker holds an unpublished candidate.
                // The subsequent publication must preserve this root and replay
                // its own registered output, without rebuilding its SST.
                const backend = handle.ptr();
                while (!backend.mu.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
                defer backend.mu.unlock();
                const path = try a.dupe(u8, "concurrent-retirement.tbl");
                errdefer a.free(path);
                try backend.obsolete_paths.append(a, .{ .path = path, .delete_after_ns = std.math.maxInt(u64) });
            }
            Interleave.go.set(io);
        }
        while (true) {
            while (!handle.ptr().mu.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
            const backend = handle.ptr();
            const cleanup_done = backend.retired_run_directories == null and backend.directory_reclaimer == null and
                backend.retired_run_stores == null and backend.store_reclaimer == null and backend.retired_ledger_snapshots == null and
                backend.retired_flush_outputs == null and !backend.flush_outputs_reclaim_in_flight and
                backend.retired_memory_head == null and backend.memtable_reclaimer == null and !backend.memtable_reclaim_in_flight and
                backend.read_metadata_reclaimer == null and backend.retired_projection_versions == null and backend.retired_domain_indexes == null and !backend.read_metadata_reclaim_in_flight and
                !backend.directory_reclaim_in_flight and !backend.store_reclaim_in_flight and !backend.ledger_reclaim_in_flight and !backend.maintenance_job_in_flight;
            const done = backend.activeImmutableMemtableCount() == 0 and !backend.immutable_flush_job_in_flight and
                (if (mode == .manifest_completion_fence)
                    cleanup_done and backend.flush_completion_pending and backend.manifest_recovery_required
                else if (completion_retry)
                    cleanup_done and !backend.flush_completion_pending
                else if (mode == .retirement_retry or mode == .retirement_capacity or mode == .publication_directory_retry or mode == .output_retirement_retry or mode == .immutable_retirement_retry)
                    cleanup_done
                else if (mode == .output_retirement_close or mode == .output_retirement_preflight)
                    backend.retired_flush_outputs != null and Interleave.vector_calls.load(.acquire) != 0
                else if (mode == .immutable_retirement_close)
                    (backend.retired_memory_head != null or backend.memtable_reclaimer != null) and Interleave.immutable_calls.load(.acquire) != 0
                else
                    true);
            handle.ptr().mu.unlock();
            if (done) break;
            if (platform.time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        if (completion_retry) {
            try std.testing.expect(Interleave.manifest_calls.load(.acquire) >= 4);
            const completed = handle.ptr().snapshotWriteStats();
            try std.testing.expectEqual(@as(u64, 1), completed.immutable_flushes);
            try std.testing.expectEqual(@as(u64, 1), completed.flushes);
            try std.testing.expect(completed.wal_resets >= 1);
            try std.testing.expect(handle.ptr().wal_retention.reset_proven);
        }
        if (mode == .read_metadata_retry or preparing_manifest) {
            const before = handle.ptr().snapshotWriteStats();
            if (preparing_manifest) {
                Interleave.preparation_refuse = mode == .manifest_preparation_retry;
                Interleave.preparation_interleave = mode == .manifest_preparation_interleave;
                Interleave.ready = .unset;
                Interleave.go = .unset;
                Interleave.frame_calls.store(0, .release);
                Interleave.refuse_frame.store(mode == .manifest_preparation_retry, .release);
                Interleave.keep_frame_queued = mode == .manifest_preparation_close;
                publication.manifest_edit_preparation.test_retirement_hook = Interleave.frameRetirement;
                Interleave.preparation_calls.store(0, .release);
                Interleave.preparation_backend = handle.ptr();
                publication.manifest_edit_preparation.test_hook = Interleave.preparation;
            }
            var cached = try handle.ptr().beginRead();
            try std.testing.expectEqualStrings("durable", try cached.get(.{}, "admitted"));
            cached.abort();
            // A normal peer commit invalidates the cached read generation.
            var peer = try handle.ptr().beginWrite();
            defer peer.abort();
            try peer.put(.{}, "second", "metadata");
            try peer.commit();
            if (mode == .manifest_preparation_interleave) {
                try Interleave.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
                var concurrent = try handle.ptr().beginWrite();
                defer concurrent.abort();
                try concurrent.put(.{}, "third", "concurrent");
                try concurrent.commit();
                Interleave.go.set(io);
            }
            while (true) {
                const backend = handle.ptr();
                while (!backend.mu.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
                const cleanup_ready = if (mode == .manifest_preparation_close)
                    backend.retired_manifest_preparations != null and Interleave.frame_calls.load(.acquire) != 0
                else
                    !backend.maintenance_job_in_flight and backend.retired_projection_versions == null and backend.retired_domain_indexes == null and
                        backend.read_metadata_reclaimer == null and !backend.read_metadata_reclaim_in_flight and
                        backend.retired_manifest_preparations == null and !backend.manifest_preparation_reclaim_in_flight;
                const done = backend.activeImmutableMemtableCount() == 0 and !backend.immutable_flush_job_in_flight and
                    !backend.flush_completion_pending and cleanup_ready and
                    (if (preparing_manifest) Interleave.preparation_calls.load(.acquire) >= (if (mode == .manifest_preparation_close) @as(usize, 1) else 2) else Interleave.metadata_calls.load(.acquire) >= 2);
                backend.mu.unlock();
                if (done) break;
                if (platform.time.monotonicNs() >= deadline) {
                    while (!backend.mu.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
                    defer backend.mu.unlock();
                    std.debug.print("warm manifest timeout: preparations={d}, immutable={d}, flush_job={}, maintenance={}, completion={}, frames={}, frame_cleanup={}, sequence={any}, directory_dirty={}, directory={}, durable={}\n", .{ Interleave.preparation_calls.load(.acquire), backend.activeImmutableMemtableCount(), backend.immutable_flush_job_in_flight, backend.maintenance_job_in_flight, backend.flush_completion_pending, backend.retired_manifest_preparations != null, backend.manifest_preparation_reclaim_in_flight, backend.manifest_journal.sequence, backend.run_directory_dirty, backend.run_directory != null, backend.manifest_directory != null });
                    return error.TestUnexpectedResult;
                }
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            if (preparing_manifest) {
                const after = handle.ptr().snapshotWriteStats();
                try std.testing.expectEqual(before.manifest_checkpoints, after.manifest_checkpoints);
                try std.testing.expect(after.manifest_edits > before.manifest_edits);
                const added: u64 = if (mode == .manifest_preparation_interleave) 2 else 1;
                try std.testing.expectEqual(before.immutable_flushes + added, after.immutable_flushes);
                try std.testing.expectEqual(before.flushes + added, after.flushes);
                if (mode == .manifest_preparation_retry) try std.testing.expect(Interleave.frame_calls.load(.acquire) >= 2);
            }
        }
        try std.testing.expect(state.owner.scheduler.stats().grants[@backingInt(scheduler.Class.background)] > 0);
        try std.testing.expect(Interleave.controlled_calls.load(.acquire) >= (if (mode == .publication_retry) @as(usize, 2) else 1));
        if (rebase_publication) {
            try std.testing.expect(Interleave.controlled_calls.load(.acquire) >= 2);
            try std.testing.expect(Interleave.replays.load(.acquire) >= 1);
            try std.testing.expect(handle.ptr().obsolete_paths.contains("concurrent-retirement.tbl"));
            if (mode == .publication_rebase) {
                const completed = handle.ptr().snapshotWriteStats();
                try std.testing.expectEqual(@as(u64, 2), completed.immutable_flushes);
                try std.testing.expectEqual(completed.immutable_flushes, completed.flushes);
            }
        }
        if (mode == .immutable_retirement_retry or mode == .immutable_retirement_close) {
            try std.testing.expect(Interleave.immutable_calls.load(.acquire) >= (if (mode == .immutable_retirement_retry) @as(usize, 2) else 1));
        }
        if (mode == .output_retirement_retry or mode == .output_retirement_close or mode == .output_retirement_preflight) {
            try std.testing.expect(Interleave.vector_calls.load(.acquire) >= (if (mode == .output_retirement_retry) @as(usize, 2) else 1));
        }
        if (mode == .retirement_retry or mode == .retirement_capacity or mode == .publication_directory_retry) {
            try std.testing.expect(Interleave.cleanup_calls.load(.acquire) >= (if (mode == .retirement_retry) @as(usize, 2) else 1));
        }
        if (mode == .publication_retry or mode == .publication_rebase_retry or mode == .publication_directory_retry) {
            const completed = handle.ptr().snapshotWriteStats();
            try std.testing.expectEqual(@as(u64, if (rebase_publication) 2 else 1), completed.immutable_flushes);
            try std.testing.expect(completed.flushes > completed.immutable_flushes);
        }
        if (pinned) |*read| {
            try std.testing.expectEqualStrings("durable", try read.get(.{}, "admitted"));
            read.abort();
            pinned = null;
            var fresh = try handle.ptr().beginRead();
            defer fresh.abort();
            try std.testing.expectEqualStrings("newer", try fresh.get(.{}, "admitted"));
        }
        if (mode == .waker) {
            const waker_stats = handle.internalFlushWorkerStats().?;
            try std.testing.expect(waker_stats.wakeups >= 2);
            try std.testing.expect(waker_stats.maintenance_steps > 0);
        }
        if (mode == .output_retirement_preflight or mode == .manifest_preparation_close) {
            const backend = handle.ptr();
            backend.closing.store(true, .release);
            backend.background_executor.drain();
            try std.testing.expectEqual(@as(u64, 0), backend.active_readers);
            try std.testing.expect(backend.mu.tryLock());
            backend.invalidateMutableReadSnapshot();
            backend.mu.unlock();
            Interleave.keep_vector_queued = false; // Every producer has joined.
            Interleave.keep_frame_queued = false;
            var pass = try local.common_background_execution.Pass.initWithOptions(owner, &io, platform.time.monotonicNs() +| 200 * std.time.ns_per_ms, .{}, .{ .allow_wait = false, .cleanup = true });
            defer pass.deinit();
            var scope = try pass.admissionScope(io);
            const payload = scope.payload();
            var preparation: lsm.Backend.QuiescedClosePreparation = .{};
            try preparation.resumeWithAdmission(backend, io, .init(&payload));
            try std.testing.expect(backend.retired_flush_outputs == null);
            try std.testing.expect(backend.retired_manifest_preparations == null);
        }
        if (mode == .manifest_completion_fence) {
            try std.testing.expectEqual(@as(usize, 1), Interleave.manifest_calls.load(.acquire));
            try std.testing.expect(!handle.ptr().wal_retention.reset_proven);
            // Recovery authority excludes publication retries. Simulate a crash
            // rather than invoking explicit close-time durability repair.
            handle.abandonAfterCrash();
        } else if (mode == .manifest_preparation_retry or mode == .manifest_preparation_interleave) {
            // Reopen must observe the accepted journal without close repairing
            // or checkpointing a missed publication.
            handle.abandonAfterCrash();
        } else handle.close();
        open = false;
    }
    if (foreground) |held| owner.finish(held);
    foreground = null;
    RegisteredOutputFixture.drain(&resources.resource_manager);
    if (retry_refusal) try std.testing.expect(Refusal.attempts.load(.acquire) >= 6);
    try std.testing.expectEqual(final_synchronous_checks, original.checks.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), resources.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    var reopened = try lsm.Backend.open(a, root, .{ .storage = native.storage(), .backend = .{ .read_only = true } });
    defer reopened.close();
    var read = try reopened.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings(if (mode == .publication_interleave or rebase_publication) "newer" else "durable", try read.get(.{}, "admitted"));
    if (preparing_manifest) try std.testing.expectEqualStrings("metadata", try read.get(.{}, "second"));
    if (mode == .manifest_preparation_interleave) try std.testing.expectEqualStrings("concurrent", try read.get(.{}, "third"));
}

test "physical cache metadata registered flush publication normal worker waits for shared execution then persists and reopens" {
    try normalAdmittedFlushWorker(.publish);
}

test "physical cache metadata registered flush publication normal close cancels queued admission and persists immutable tail" {
    try normalAdmittedFlushWorker(.cancel_queued);
}

test "physical cache metadata registered flush publication legacy handoff retains files and aborts partial ownership" {
    _ = try FlushPublicationFixture.run(.legacy, null, false, false);
    for ([_]usize{ 0, 1, 8, 24, 48, 96 }) |point| {
        _ = try FlushPublicationFixture.run(.legacy, point, false, true);
    }
    const baseline = try FlushPublicationFixture.run(.legacy, null, false, false);
    for (0..baseline.allocations) |allocation| {
        _ = try FlushPublicationFixture.run(.legacy, allocation, true, true);
    }
}

test "physical cache metadata registered flush publication normal worker retries refused admission without another write" {
    try normalAdmittedFlushWorker(.retry);
}

test "physical cache metadata registered flush publication maintenance caller hands off idle partial flush and original authority" {
    try normalAdmittedFlushWorker(.maintenance);
    try normalAdmittedFlushWorker(.maintenance_retry);
    try normalAdmittedFlushWorker(.maintenance_close);
}

test "physical cache metadata registered flush publication maintenance handoff preserves IO budget refusal and retry" {
    try normalAdmittedFlushWorker(.maintenance_budget);
}

test "physical cache metadata registered flush publication internal waker uses shared construction admission" {
    try normalAdmittedFlushWorker(.waker);
}

test "physical cache metadata registered flush publication unsupported admitted provider keeps working legacy maintenance" {
    try normalAdmittedFlushWorker(.maintenance_unsupported);
}

test "physical cache metadata registered flush publication normal candidate permits peer pins writes and stable reads before swap" {
    try normalAdmittedFlushWorker(.publication_interleave);
}

test "physical cache metadata registered flush publication controlled candidate refusal retires and retries immutable debt" {
    try normalAdmittedFlushWorker(.publication_retry);
}

test "physical cache metadata registered flush publication normal worker replays registered outputs over concurrent ledger roots" {
    try normalAdmittedFlushWorker(.publication_rebase);
}

test "physical cache metadata registered flush publication replay refusal preserves immutable debt for retry" {
    try normalAdmittedFlushWorker(.publication_rebase_retry);
}

test "physical cache metadata registered flush publication queued retirement retries independent cleanup refusal" {
    try normalAdmittedFlushWorker(.retirement_retry);
}

test "physical cache metadata registered flush publication cleanup preserves ordinary saturation and health progress" {
    try normalAdmittedFlushWorker(.retirement_capacity);
}

test "physical cache metadata registered flush publication failed directory candidate transfers to controlled retirement" {
    try normalAdmittedFlushWorker(.publication_directory_retry);
}

test "physical cache metadata registered flush publication returned vector retries cleanup admission and releases prepaid credit" {
    try normalAdmittedFlushWorker(.output_retirement_retry);
}

test "physical cache metadata registered flush publication close joins refused output vector retirement and preserves live SSTs" {
    try normalAdmittedFlushWorker(.output_retirement_close);
}

test "physical cache metadata registered flush publication immutable state retirement retries protected cleanup refusal" {
    try normalAdmittedFlushWorker(.immutable_retirement_retry);
}

test "physical cache metadata registered flush publication close joins refused immutable state retirement" {
    try normalAdmittedFlushWorker(.immutable_retirement_close);
}

test "physical cache metadata registered flush publication controlled close preparation drains pending output vector before durability" {
    try normalAdmittedFlushWorker(.output_retirement_preflight);
}

test "physical cache metadata registered flush publication completes manifest after admission refusal with an empty immutable queue" {
    try normalAdmittedFlushWorker(.manifest_completion_retry);
}

test "physical cache metadata registered flush publication completes manifest after IO failure without another write" {
    try normalAdmittedFlushWorker(.manifest_completion_io_retry);
}

test "physical cache metadata registered flush publication internal waker retains independent manifest completion debt" {
    try normalAdmittedFlushWorker(.manifest_completion_waker);
}

test "physical cache metadata registered flush publication peer commit retires cached projection under protected cleanup with retry" {
    try normalAdmittedFlushWorker(.read_metadata_retry);
}

test "physical cache metadata registered flush publication recovery fence retains WAL and suppresses completion retries" {
    try normalAdmittedFlushWorker(.manifest_completion_fence);
}

test "physical cache metadata registered flush publication controlled writer insertion preserves ownership at allocation and original refusal" {
    const baseline = try FlushPublicationFixture.run(.controlled_writer, null, false, false);
    for ([_]usize{ 0, 1, 8, 24, 48, 96 }) |point| _ = try FlushPublicationFixture.run(.controlled_writer, point, false, true);
    for (0..baseline.allocations) |allocation| _ = try FlushPublicationFixture.run(.controlled_writer, allocation, true, true);
}

test "memtable reclamation execution rejects original once and resumes physical chunks on independent cleanup without allocation" {
    const local = @import("antfly_local_sources");
    const admission = read_admission;
    const lsm = local.storage_lsm_backend_mod;
    const state_mod = local.storage_lsm_backend_state;
    const Control = struct {
        calls: usize = 0,
        reject_at: ?usize = 3,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const call = self.calls;
            self.calls += 1;
            if (self.reject_at == call) return error.Canceled;
        }
    };
    inline for ([_]enum { tree, flat, arena }{ .tree, .flat, .arena }) |representation| {
        const a = std.testing.allocator;
        var backend = lsm.Backend.init(a, .{});
        defer backend.close();
        const state = try a.create(state_mod.State);
        var adopted = false;
        defer if (!adopted) a.destroy(state);
        state.* = blk: {
            var source: state_mod.ActiveMemTable = .{};
            defer source.deinit(a);
            for (0..9) |index| {
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, index, .big);
                try source.upsert(a, .{}, &key, "value", false);
            }
            break :blk switch (representation) {
                .tree => try source.snapshot(a),
                .flat => try source.clone(a),
                .arena => try source.cloneArena(a),
            };
        };
        state.freezeMemoryAccounting();
        backend.memtable_reclaimer = .init(state);
        backend.memtable_reclaim_pending = 1;
        adopted = true;
        const job = &backend.memtable_reclaimer.?;
        var exhausted = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var first: Control = .{};
        const first_payload: admission.Payload = .{ .ptr = &first, .vtable = &.{ .checkpoint = Control.check } };
        var credits: usize = 64;
        try std.testing.expectError(error.Canceled, job.stepWithAdmission(exhausted.allocator(), &credits, .init(&first_payload)));
        const rejected_calls = first.calls;
        var cleanup: Control = .{ .reject_at = null };
        const cleanup_payload: admission.Payload = .{ .ptr = &cleanup, .vtable = &.{ .checkpoint = Control.check } };
        while (true) {
            credits = 1;
            if (try job.stepWithAdmission(exhausted.allocator(), &credits, .init(&cleanup_payload))) break;
        }
        try std.testing.expectEqual(rejected_calls, first.calls);
        try std.testing.expect(cleanup.calls > 1);
        try std.testing.expectEqual(@as(usize, 0), exhausted.alloc_index);
    }
}

test "physical cache metadata registered flush publication warm journal preparation refuses and retries without rebuilding SST" {
    try normalAdmittedFlushWorker(.manifest_preparation_retry);
}

test "physical cache metadata registered flush publication controlled journal codec preserves exact wire and rejects at every checkpoint" {
    const codec = @import("antfly_local_sources").storage_lsm_backend_compaction_publication.manifest_edit_preparation.manifest;
    const a = std.testing.allocator;
    const long = try a.alloc(u8, 48 * 1024 + 17);
    defer a.free(long);
    @memset(long, 'x');
    const value: codec.Manifest = .{ .next_run_id = 91, .runs = &.{.{
        .id = 90,
        .visibility_id = 87,
        .level = 2,
        .size_bytes = 991,
        .path = "runs/90.tbl",
        .smallest_namespace_name = "docs",
        .smallest_key = long,
        .largest_namespace_name = "docs",
        .largest_key = long,
        .entry_count = 9,
        .tombstone_count = 3,
        .oldest_tombstone_unix_ns = 123,
        .gc_requested = true,
    }}, .obsolete_paths = &.{.{ .path = long, .delete_after_ns = 345 }} };
    for ([_]bool{ false, true }) |checkpoint| {
        const removed: []const u64 = if (checkpoint) &.{} else &.{ 3, 9 };
        const paths: []const []const u8 = if (checkpoint) &.{} else &.{ "old.tbl", long };
        const expected = try codec.encodeJournalFrameAlloc(a, 17, checkpoint, removed, paths, value);
        defer a.free(expected);
        var control: RegisteredOutputFixture.Control = .{ .live = true };
        const payload = control.payload();
        const port = read_admission.Port.init(&payload);
        const encoded = try a.alloc(u8, try codec.journalFrameLenWithAdmission(checkpoint, removed, paths, value, port));
        defer a.free(encoded);
        control.calls = 0;
        try codec.encodeJournalFrameIntoWithAdmission(encoded, 17, checkpoint, removed, paths, value, port);
        try std.testing.expectEqualSlices(u8, expected, encoded);
        const checks = control.calls;
        // Includes rejection during long-field copy and both CRC passes.
        for (0..checks) |at| {
            control.calls = 0;
            control.target = at;
            try std.testing.expectError(error.Canceled, codec.encodeJournalFrameIntoWithAdmission(encoded, 17, checkpoint, removed, paths, value, port));
        }
        control.target = null;
        try codec.encodeJournalFrameIntoWithAdmission(encoded, 17, checkpoint, removed, paths, value, port);
        try std.testing.expectEqualSlices(u8, expected, encoded);
    }
}

test "physical cache metadata registered flush publication captured manifest roots preserve concurrent peer commit" {
    try normalAdmittedFlushWorker(.manifest_preparation_interleave);
}

test "physical cache metadata registered flush publication controlled close drains refused manifest frame cleanup" {
    try normalAdmittedFlushWorker(.manifest_preparation_close);
}
