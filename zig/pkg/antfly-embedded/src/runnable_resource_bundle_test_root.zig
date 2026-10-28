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
const resources = @import("storage/resource_manager.zig");
const metadata_fixture = @import("storage/identity_metadata_test_fixture.zig");
const runnable = @import("common/runnable_scheduler.zig");
const Bundle = @import("storage/runnable_dense_bundle.zig").Bundle;

fn entry(class: runnable.Class, slots: usize) runnable.Entry {
    return .{ .options = .{ .io = std.testing.io }, .class = class, .slots = slots, .bytes = @sizeOf(runnable.Entry) };
}
fn prepare(manager: *resources.ResourceManager, bytes: u64, bandwidth: u64) !Bundle {
    return Bundle.init(manager, &.{.{ .slice = .lsm_read_working_set, .bytes = bytes }}, bandwidth);
}
fn expectUsage(manager: *resources.ResourceManager, memory: u64, bandwidth: u64) !void {
    try std.testing.expectEqual(memory, metadata_fixture.payloadUsage(manager));
    try std.testing.expectEqual(bandwidth, manager.denseSearchAdmissionStats().active_bytes);
}

test "runnable resource bundle grants CPU memory and bandwidth together and retains physical ownership across parking" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = failing.allocator() });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try prepare(&manager, 60, 8);
    defer bundle.deinitAfterJoin();
    const allocations = failing.alloc_index;
    failing.fail_index = allocations;
    var holder = entry(.query, 1);
    var scan = entry(.query, 2);
    scan.pending_resources = bundle.port();
    defer scheduler.finish(&holder);
    defer scheduler.finish(&scan);
    try scheduler.submit(&holder);
    try scheduler.submit(&scan);
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&scan));
    try expectUsage(&manager, 0, 0);
    scheduler.finish(&holder);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&scan));
    try expectUsage(&manager, 60, 8);
    try std.testing.expectEqual(allocations, failing.alloc_index);
    const before = scheduler.stats();
    try scheduler.block(&scan, .resource_blocked);
    try expectUsage(&manager, 60, 8);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(before.operations, scheduler.stats().operations);
    try std.testing.expectEqual(before.retained_bytes, scheduler.stats().retained_bytes);
    try scheduler.unblock(&scan);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&scan));
    try std.testing.expectEqual(@as(u64, 1), manager.denseSearchAdmissionStats().admissions);
    scheduler.finish(&scan);
    // CPU finish is not proof that an independent physical buffer/IO retired.
    try expectUsage(&manager, 60, 8);
    bundle.deinitAfterJoin();
    try expectUsage(&manager, 0, 0);
}

test "runnable resource bundle refuses busy ancestors and denied shared memory without bandwidth or CPU credit" {
    var parent = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&manager, &parent);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try prepare(&manager, 60, 8);
    defer bundle.deinitAfterJoin();
    var scan = entry(.query, 2);
    scan.pending_resources = bundle.port();
    defer scheduler.finish(&scan);
    try std.testing.expect(parent.mutex.tryLock());
    const submitted = scheduler.submit(&scan);
    parent.mutex.unlock();
    try submitted;
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&scan));
    try expectUsage(&manager, 0, 0);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    var sibling = try parent.reserveWithoutReclaim(.text_merge_buffers, 50);
    defer sibling.release();
    scheduler.notifyResourceProgress();
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&scan));
    try expectUsage(&manager, 0, 0);
    try std.testing.expectEqual(@as(u64, 50), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
    sibling.release();
    scheduler.notifyResourceProgress();
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&scan));
    try expectUsage(&manager, 60, 8);
    try std.testing.expectEqual(@as(u64, 60), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
    scheduler.finish(&scan);
    bundle.deinitAfterJoin();
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
}

test "runnable resource bundle bandwidth refusal keeps all memory unreserved" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var held = try manager.acquireDenseSearchBandwidth(8, null, null);
    defer held.release();
    var bundle = try prepare(&manager, 60, 8);
    defer bundle.deinitAfterJoin();
    var scan = entry(.query, 2);
    scan.pending_resources = bundle.port();
    defer scheduler.finish(&scan);
    try scheduler.submit(&scan);
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&scan));
    try expectUsage(&manager, 0, 8);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    held.release();
    scheduler.notifyResourceProgress();
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&scan));
    try expectUsage(&manager, 60, 8);
    scheduler.finish(&scan);
    bundle.deinitAfterJoin();
    try expectUsage(&manager, 0, 0);
}

test "runnable resource bundle barrier permits owning continuations and protected progress while blocking fresh backfill" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .max_backfills = 0 });
    var retained = try prepare(&manager, 60, 8);
    defer retained.deinitAfterJoin();
    var requested = try prepare(&manager, 60, 8);
    defer requested.deinitAfterJoin();
    var continuation = entry(.query, 1);
    continuation.pending_resources = retained.port();
    var holder = entry(.query, 1);
    var team = entry(.write, 2);
    team.pending_resources = requested.port();
    var fresh = entry(.inference, 1);
    var health = entry(.control, 1);
    var cleanup: runnable.Execution = .{};
    defer cleanup.finish();
    defer scheduler.finish(&continuation);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&team);
    defer scheduler.finish(&fresh);
    defer scheduler.finish(&health);
    try scheduler.submit(&continuation);
    try scheduler.block(&continuation, .output_blocked);
    try scheduler.submit(&holder);
    try scheduler.submit(&team);
    try scheduler.submit(&fresh);
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&team));
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&fresh));
    try scheduler.unblock(&continuation);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&continuation));
    try expectUsage(&manager, 60, 8);
    try scheduler.submit(&health);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&health));
    try cleanup.startCleanupRoot(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .deadline_ns = continuation.options.now() + 200 * std.time.ns_per_ms });
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().cleanup_runnable_slots);
    // The continuation now physically joins its prior resources. The team
    // still needs both CPU slots, so fresh work cannot steal the freed slot.
    retained.deinitAfterJoin();
    scheduler.finish(&continuation);
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&fresh));
    scheduler.finish(&holder);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&team));
    try expectUsage(&manager, 60, 8);
    scheduler.finish(&team);
    requested.deinitAfterJoin();
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&fresh));
    scheduler.finish(&fresh);
    scheduler.finish(&health);
    cleanup.finish();
    try expectUsage(&manager, 0, 0);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable resource bundle supports incremental parked requests without refreshing operation authority" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var base = try prepare(&manager, 20, 2);
    defer base.deinitAfterJoin();
    var increment = try prepare(&manager, 60, 7);
    defer increment.deinitAfterJoin();
    var scan = entry(.query, 1);
    scan.options.deadline_ns = scan.options.now() + std.time.ns_per_s;
    const deadline = scan.options.deadline_ns;
    scan.pending_resources = base.port();
    defer scheduler.finish(&scan);
    try scheduler.submit(&scan);
    try scheduler.block(&scan, .resource_blocked);
    const before = scheduler.stats();
    try scheduler.requestResources(&scan, increment.port());
    try expectUsage(&manager, 20, 2);
    try scheduler.unblock(&scan);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&scan));
    try std.testing.expectEqual(deadline, scan.options.deadline_ns);
    try std.testing.expectEqual(before.operations, scheduler.stats().operations);
    try std.testing.expectEqual(before.retained_bytes, scheduler.stats().retained_bytes);
    try expectUsage(&manager, 80, 9);
    scheduler.finish(&scan);
    increment.deinitAfterJoin();
    try expectUsage(&manager, 20, 2);
    base.deinitAfterJoin();
    try expectUsage(&manager, 0, 0);
}

test "runnable resource bundle intrinsic refusal and cancellation retain only metadata until owning finish" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var large = try prepare(&manager, 101, 8);
    defer large.deinitAfterJoin();
    var refused = entry(.query, 1);
    refused.pending_resources = large.port();
    defer scheduler.finish(&refused);
    try scheduler.submit(&refused);
    try std.testing.expectEqual(runnable.State.stopped, scheduler.state(&refused));
    try std.testing.expectError(error.ResourceRequestTooLarge, scheduler.waitRetained(&refused));
    try expectUsage(&manager, 0, 0);
    scheduler.finish(&refused);
    var held = try manager.acquireDenseSearchBandwidth(10, null, null);
    defer held.release();
    var waiting = try prepare(&manager, 60, 8);
    defer waiting.deinitAfterJoin();
    var scan = entry(.query, 1);
    scan.pending_resources = waiting.port();
    var canceled = false;
    scan.options.cancellation = .{ .ptr = &canceled, .requested = struct {
        fn requested(ptr: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(ptr))).*;
        }
    }.requested };
    defer scheduler.finish(&scan);
    try scheduler.submit(&scan);
    canceled = true;
    try std.testing.expectError(error.Canceled, scheduler.waitRetained(&scan));
    held.release();
    scheduler.notifyResourceProgress();
    try std.testing.expectEqual(runnable.State.stopped, scheduler.state(&scan));
    try expectUsage(&manager, 0, 0);
    scheduler.finish(&scan);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable resource bundle stale copy cannot refund a later physical scan" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var first = try prepare(&manager, 60, 8);
    defer first.deinitAfterJoin();
    var second = try prepare(&manager, 60, 8);
    defer second.deinitAfterJoin();
    var one = entry(.query, 1);
    one.pending_resources = first.port();
    var two = entry(.query, 1);
    two.pending_resources = second.port();
    defer scheduler.finish(&one);
    defer scheduler.finish(&two);
    try scheduler.submit(&one);
    var stale = first;
    scheduler.finish(&one);
    first.deinitAfterJoin();
    try scheduler.submit(&two);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&two));
    stale.deinitAfterJoin();
    stale.deinitAfterJoin();
    try expectUsage(&manager, 60, 8);
    try std.testing.expectEqual(@as(u64, 1), manager.snapshot().memory.accounting_errors);
    scheduler.finish(&two);
    second.deinitAfterJoin();
    try expectUsage(&manager, 0, 0);
}

test "runnable resource bundle original bounded wait retries after physical retirement without a new submission" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var held = try manager.acquireDenseSearchBandwidth(10, null, null);
    defer held.release();
    var bundle = try prepare(&manager, 60, 8);
    defer bundle.deinitAfterJoin();
    var scan = entry(.query, 2);
    scan.pending_resources = bundle.port();
    scan.options.deadline_ns = scan.options.now() + 200 * std.time.ns_per_ms;
    const deadline = scan.options.deadline_ns;
    defer scheduler.finish(&scan);
    try scheduler.submit(&scan);
    const releaser = try std.Thread.spawn(.{}, struct {
        fn run(lease: *resources.DenseSearchAdmissionLease) void {
            @import("antfly_platform").time.sleepNs(5 * std.time.ns_per_ms);
            lease.release();
        }
    }.run, .{&held});
    defer releaser.join();
    try scheduler.waitRetained(&scan);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&scan));
    try std.testing.expectEqual(deadline, scan.options.deadline_ns);
    try expectUsage(&manager, 60, 8);
    scheduler.finish(&scan);
    bundle.deinitAfterJoin();
    try expectUsage(&manager, 0, 0);
}

test "runnable resource bundle original queue timeout cannot be refreshed by resource probes" {
    const Clock = struct {
        var ns: u64 = 0;
        fn now() u64 {
            ns += std.time.ns_per_ms;
            return ns;
        }
    };
    Clock.ns = 0;
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5 });
    var held = try manager.acquireDenseSearchBandwidth(10, null, null);
    defer held.release();
    var bundle = try prepare(&manager, 60, 8);
    defer bundle.deinitAfterJoin();
    var scan = entry(.query, 1);
    scan.pending_resources = bundle.port();
    scan.options.native_now_ns = Clock.now;
    scan.options.deadline_ns = std.time.ns_per_s;
    defer scheduler.finish(&scan);
    try scheduler.submit(&scan);
    const queued_since = scan.queued_since_ns;
    try std.testing.expectError(error.RunnableWaitTimeout, scheduler.waitRetained(&scan));
    try std.testing.expectEqual(queued_since, scan.queued_since_ns);
    held.release();
    scheduler.notifyResourceProgress();
    try std.testing.expectEqual(runnable.State.stopped, scheduler.state(&scan));
    try expectUsage(&manager, 0, 0);
    scheduler.finish(&scan);
}

test "runnable resource bundle unknown ancestor fit permits the physical memory owner to resume" {
    var root = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer root.deinit(std.testing.allocator);
    var middle = try metadata_fixture.init(.{ .identity_allocator = std.testing.allocator });
    defer middle.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&middle, &root);
    var owner = try metadata_fixture.init(.{ .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer owner.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&owner, &middle);
    var requester = try metadata_fixture.init(.{ .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer requester.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&requester, &middle);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3, .max_backfills = 0 });
    var retained = try prepare(&owner, 60, 8);
    defer retained.deinitAfterJoin();
    var requested = try prepare(&requester, 60, 8);
    defer requested.deinitAfterJoin();
    var continuation = entry(.query, 1);
    continuation.pending_resources = retained.port();
    var holder = entry(.query, 1);
    var team = entry(.write, 2);
    team.pending_resources = requested.port();
    defer scheduler.finish(&continuation);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&team);
    try scheduler.submit(&continuation);
    try scheduler.block(&continuation, .resource_blocked);
    try scheduler.submit(&holder);
    try std.testing.expectEqual(resources.PreparedBatchReservation.Probe.unknown, requested.resources.probe());
    try scheduler.submit(&team);
    try scheduler.unblock(&continuation);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&continuation));
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&team));
    try expectUsage(&requester, 0, 0);
    try std.testing.expectEqual(@as(u64, 60), (metadata_fixture.payloadUsage(&root) - middle.identity_metadata_bytes - owner.identity_metadata_bytes - requester.identity_metadata_bytes));
    retained.deinitAfterJoin();
    scheduler.finish(&continuation);
    scheduler.finish(&holder);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&team));
    try expectUsage(&owner, 0, 0);
    try expectUsage(&requester, 60, 8);
    scheduler.finish(&team);
    requested.deinitAfterJoin();
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&root) - middle.identity_metadata_bytes - owner.identity_metadata_bytes - requester.identity_metadata_bytes));
}

test "runnable resource bundle native execution capsule admits and grows at parked boundaries" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var base = try prepare(&manager, 20, 2);
    defer base.deinitAfterJoin();
    var increment = try prepare(&manager, 60, 7);
    defer increment.deinitAfterJoin();
    var execution: runnable.Execution = .{};
    defer execution.finish();
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io, .retained_bytes = @sizeOf(runnable.Execution) }, .query, 1, base.port());
    try expectUsage(&manager, 20, 2);
    try std.testing.expectError(error.InvalidRunnableTransition, execution.requestResources(increment.port()));
    try execution.park(.resource_blocked);
    try execution.requestResources(increment.port());
    try std.testing.expectError(error.InvalidRunnableTransition, execution.requestResources(increment.port()));
    try execution.enter(.query);
    try expectUsage(&manager, 80, 9);
    execution.finish();
    increment.deinitAfterJoin();
    base.deinitAfterJoin();
    try expectUsage(&manager, 0, 0);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable resource bundle cleanup transfer drops pending foreground requirements after close" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1 });
    var bundle = try prepare(&manager, 60, 8);
    defer bundle.deinitAfterJoin();
    var held = try manager.acquireDenseSearchBandwidth(10, null, null);
    defer held.release();
    var scan = entry(.query, 1);
    defer scheduler.finish(&scan);
    try scheduler.submit(&scan);
    try scheduler.reserveCleanupFor(&scan, .{ .io = std.testing.io }, 200 * std.time.ns_per_ms);
    try scheduler.block(&scan, .resource_blocked);
    try scheduler.requestResources(&scan, bundle.port());
    try scheduler.unblock(&scan);
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&scan));
    scheduler.close();
    try scheduler.activateCleanup(&scan);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&scan));
    try std.testing.expect(scan.pending_resources == null);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().cleanup_runnable_slots);
    try std.testing.expectError(error.InvalidRunnableTransition, scheduler.requestResources(&scan, bundle.port()));
    try expectUsage(&manager, 0, 10);
    scheduler.finish(&scan);
    held.release();
    try expectUsage(&manager, 0, 0);
}

test "runnable resource bundle younger isolated resource barrier preserves the older CPU barrier" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 6, .cleanup_slots = 1, .read_isolation = true, .max_backfills = 0 });
    var retained = try prepare(&manager, 60, 8);
    defer retained.deinitAfterJoin();
    var requested = try prepare(&manager, 60, 8);
    defer requested.deinitAfterJoin();
    var continuation = entry(.query, 1);
    continuation.read_lane = .bounded;
    continuation.pending_resources = retained.port();
    var holder = entry(.query, 2);
    var team = entry(.write, 2);
    var waiting = entry(.query, 1);
    waiting.read_lane = .bounded;
    waiting.pending_resources = requested.port();
    var small = entry(.inference, 1);
    defer scheduler.finish(&continuation);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&team);
    defer scheduler.finish(&waiting);
    defer scheduler.finish(&small);
    try scheduler.submit(&continuation);
    try scheduler.block(&continuation, .resource_blocked);
    try scheduler.submit(&holder);
    try scheduler.submit(&team);
    try scheduler.submit(&waiting);
    try scheduler.submit(&small);
    continuation.service_debt = 64;
    try scheduler.unblock(&continuation);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&continuation));
    // Repaying continuation debt crosses multiple deficit rounds. The blocked
    // bounded bundle must not replace the older general CPU drain barrier.
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&small));
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&team));
    try std.testing.expectEqual(@as(usize, 0), continuation.service_debt);
    scheduler.finish(&continuation);
    retained.deinitAfterJoin();
    scheduler.finish(&holder);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&team));
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&waiting));
    scheduler.finish(&team);
    scheduler.finish(&waiting);
    requested.deinitAfterJoin();
    scheduler.finish(&small);
    try expectUsage(&manager, 0, 0);
}

test "runnable resource bundle physical allocation views consume one shared slice grant without a second charge" {
    var parent = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 96 }, .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 96 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    try metadata_fixture.installParent(&manager, &parent);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try Bundle.init(&manager, &.{
        .{ .slice = .lsm_read_working_set, .bytes = 64 },
        .{ .slice = .text_merge_buffers, .bytes = 32 },
    }, 3);
    defer bundle.deinitAfterJoin();
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var first = bundle.allocationView(.lsm_read_working_set, backing.allocator());
    var second = bundle.allocationView(.lsm_read_working_set, backing.allocator());
    var other = bundle.allocationView(.text_merge_buffers, backing.allocator());
    var ungranted = bundle.allocationView(.dense_apply_working_set, backing.allocator());
    try std.testing.expectError(error.OutOfMemory, first.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 0), backing.alloc_index);
    var execution: runnable.Execution = .{};
    defer execution.finish();
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io, .retained_bytes = @sizeOf(Bundle) + @sizeOf(runnable.Execution) + 4 * @sizeOf(@TypeOf(first)) }, .query, 1, bundle.port());
    const a = try first.allocator().alloc(u8, 48);
    var a_live = true;
    defer if (a_live) first.allocator().free(a);
    const b = try second.allocator().alloc(u8, 16);
    var b_live = true;
    defer if (b_live) second.allocator().free(b);
    const c = try other.allocator().alloc(u8, 32);
    var c_live = true;
    defer if (c_live) other.allocator().free(c);
    @memset(a, 0x5a);
    try std.testing.expectEqual(@as(usize, 3), backing.alloc_index);
    try std.testing.expectError(error.OutOfMemory, second.allocator().alloc(u8, 1));
    try std.testing.expectError(error.OutOfMemory, ungranted.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 3), backing.alloc_index);
    try std.testing.expectEqual(@as(u64, 64), bundle.allocatedBytes(.lsm_read_working_set));
    try std.testing.expectEqual(@as(u64, 32), bundle.allocatedBytes(.text_merge_buffers));
    try expectUsage(&manager, 96, 3);
    try std.testing.expectEqual(@as(u64, 96), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
    try std.testing.expectEqual(@as(u64, 96) + parent.identity_metadata_bytes + manager.identity_metadata_bytes, parent.snapshot().memory.peak_bytes);
    try execution.park(.output_blocked);
    execution.finish();
    try std.testing.expectError(error.ResourcesStillLive, bundle.tryDeinitAfterJoin());
    try expectUsage(&manager, 96, 3);
    try std.testing.expectError(error.OutOfMemory, first.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(u8, 0x5a), a[47]);
    first.allocator().free(a);
    a_live = false;
    second.allocator().free(b);
    b_live = false;
    other.allocator().free(c);
    c_live = false;
    try expectUsage(&manager, 96, 3);
    try bundle.tryDeinitAfterJoin();
    try expectUsage(&manager, 0, 0);
    try std.testing.expectEqual(@as(u64, 0), (metadata_fixture.payloadUsage(&parent) - manager.identity_metadata_bytes));
}

test "runnable resource bundle backing OOM and refused resize restore local allocation quota" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try prepare(&manager, 64, 1);
    defer bundle.deinitAfterJoin();
    var denied = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var failed = bundle.allocationView(.lsm_read_working_set, denied.allocator());
    var storage: [40]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var growing = bundle.allocationView(.lsm_read_working_set, fixed.allocator());
    var execution: runnable.Execution = .{};
    defer execution.finish();
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io }, .query, 1, bundle.port());
    try std.testing.expectError(error.OutOfMemory, failed.allocator().alloc(u8, 16));
    try std.testing.expectEqual(@TypeOf(failed).Failure.backing, failed.lastFailure());
    try std.testing.expectEqual(@as(u64, 0), bundle.allocatedBytes(.lsm_read_working_set));
    var bytes = try growing.allocator().alloc(u8, 16);
    var bytes_live = true;
    defer if (bytes_live) growing.allocator().free(bytes);
    @memset(bytes, 0x4c);
    try std.testing.expect(growing.allocator().resize(bytes, 40));
    bytes = bytes.ptr[0..40];
    try std.testing.expectEqual(@as(u64, 40), bundle.allocatedBytes(.lsm_read_working_set));
    try std.testing.expect(!growing.allocator().resize(bytes, 48));
    try std.testing.expectEqual(@as(u64, 40), bundle.allocatedBytes(.lsm_read_working_set));
    try std.testing.expectEqual(@as(u8, 0x4c), bytes[15]);
    execution.finish();
    try std.testing.expectError(error.ResourcesStillLive, bundle.tryDeinitAfterJoin());
    try std.testing.expect(!growing.allocator().resize(bytes, 48));
    try std.testing.expect(growing.allocator().resize(bytes, 8));
    bytes = bytes.ptr[0..8];
    try std.testing.expectEqual(@as(u64, 8), bundle.allocatedBytes(.lsm_read_working_set));
    growing.allocator().free(bytes);
    bytes_live = false;
    try bundle.tryDeinitAfterJoin();
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
}

const BundleAllocationObserver = struct {
    bundle: *Bundle,
    manager: *resources.ResourceManager,
    grant: u64,
    live: usize = 0,
    peak: usize = 0,
    allocations: usize = 0,
    remaps: usize = 0,
    admitted_before_alloc: bool = true,
    charged_through_free: bool = true,
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.admitted_before_alloc = self.admitted_before_alloc and self.bundle.allocatedBytes(.lsm_read_working_set) == self.live + len and metadata_fixture.payloadUsage(self.manager) == self.grant;
        self.allocations += 1;
        const pointer = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        self.peak = @max(self.peak, self.live);
        return pointer;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(raw: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.remaps += 1;
        return null;
    }
    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.charged_through_free = self.charged_through_free and self.bundle.allocatedBytes(.lsm_read_working_set) >= memory.len and metadata_fixture.payloadUsage(self.manager) == self.grant;
        std.testing.allocator.rawFree(memory, alignment, ra);
        self.live -= memory.len;
    }
};

test "runnable resource bundle realloc charges old and new physical buffers before backing allocation" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try prepare(&manager, 100, 1);
    defer bundle.deinitAfterJoin();
    var observed = BundleAllocationObserver{ .bundle = &bundle, .manager = &manager, .grant = 100 };
    var view = bundle.allocationView(.lsm_read_working_set, observed.allocator());
    var execution: runnable.Execution = .{};
    defer execution.finish();
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io }, .query, 1, bundle.port());
    var bytes = try view.allocator().alloc(u8, 32);
    var bytes_live = true;
    defer if (bytes_live) view.allocator().free(bytes);
    @memset(bytes, 0x6e);
    bytes = try view.allocator().realloc(bytes, 48);
    try std.testing.expectEqual(@as(usize, 80), observed.peak);
    try std.testing.expectEqual(@as(usize, 48), observed.live);
    try std.testing.expectEqual(@as(u64, 48), bundle.allocatedBytes(.lsm_read_working_set));
    try std.testing.expectEqual(@as(usize, 0), observed.remaps);
    try std.testing.expect(observed.admitted_before_alloc);
    try std.testing.expect(observed.charged_through_free);
    try std.testing.expectError(error.OutOfMemory, view.allocator().realloc(bytes, 64));
    try std.testing.expectEqual(@as(usize, 2), observed.allocations);
    try std.testing.expectEqual(@as(u8, 0x6e), bytes[31]);
    try std.testing.expectEqual(@as(u64, 48), bundle.allocatedBytes(.lsm_read_working_set));
    execution.finish();
    try std.testing.expectError(error.ResourcesStillLive, bundle.tryDeinitAfterJoin());
    view.allocator().free(bytes);
    bytes_live = false;
    try std.testing.expect(observed.charged_through_free);
    try std.testing.expectEqual(@as(usize, 0), observed.live);
    try bundle.tryDeinitAfterJoin();
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
}

test "runnable resource bundle copied owner cannot allocate or retire an original live physical quota" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try prepare(&manager, 64, 1);
    defer bundle.deinitAfterJoin();
    var execution: runnable.Execution = .{};
    defer execution.finish();
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io }, .query, 1, bundle.port());
    var copied = bundle;
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var invalid = copied.allocationView(.lsm_read_working_set, backing.allocator());
    try std.testing.expectError(error.OutOfMemory, invalid.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 0), backing.alloc_index);
    var original = bundle.allocationView(.lsm_read_working_set, backing.allocator());
    const bytes = try original.allocator().alloc(u8, 32);
    var bytes_live = true;
    defer if (bytes_live) original.allocator().free(bytes);
    try std.testing.expectError(error.ResourceOwnerCopied, copied.tryDeinitAfterJoin());
    try std.testing.expectEqual(@as(u64, 64), metadata_fixture.payloadUsage(&manager));
    try std.testing.expectEqual(@as(u64, 32), bundle.allocatedBytes(.lsm_read_working_set));
    try std.testing.expectEqual(@as(u64, 1), manager.snapshot().memory.accounting_errors);
    execution.finish();
    original.allocator().free(bytes);
    bytes_live = false;
    try bundle.tryDeinitAfterJoin();
    copied.deinitAfterJoin();
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
}

test "runnable resource bundle old allocation view cannot spend credit after owner storage reuse" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try prepare(&manager, 64, 1);
    defer bundle.deinitAfterJoin();
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var old = bundle.allocationView(.lsm_read_working_set, backing.allocator());
    var execution: runnable.Execution = .{};
    defer execution.finish();
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io }, .query, 1, bundle.port());
    execution.finish();
    try bundle.tryDeinitAfterJoin();
    const prior_identity = bundle.resources.memory.identity;
    bundle = try prepare(&manager, 64, 1);
    try std.testing.expect(bundle.resources.memory.identity != prior_identity);
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io }, .query, 1, bundle.port());
    try std.testing.expectError(error.OutOfMemory, old.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 0), backing.alloc_index);
    var current = bundle.allocationView(.lsm_read_working_set, backing.allocator());
    const bytes = try current.allocator().alloc(u8, 64);
    var bytes_live = true;
    defer if (bytes_live) current.allocator().free(bytes);
    try std.testing.expectEqual(@as(u64, 64), bundle.allocatedBytes(.lsm_read_working_set));
    execution.finish();
    current.allocator().free(bytes);
    bytes_live = false;
    try bundle.tryDeinitAfterJoin();
}

const InFlightBundleBacking = struct {
    allocations: std.atomic.Value(usize) = .init(0),
    frees: std.atomic.Value(usize) = .init(0),
    entered: std.atomic.Value(bool) = .init(false),
    proceed: std.atomic.Value(bool) = .init(false),
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.entered.store(true, .release);
        while (!self.proceed.load(.acquire)) @import("antfly_platform").time.sleepNs(50 * std.time.ns_per_us);
        const pointer = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        _ = self.allocations.fetchAdd(1, .release);
        return pointer;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.testing.allocator.rawFree(memory, alignment, ra);
        _ = self.frees.fetchAdd(1, .release);
    }
};

test "runnable resource bundle retirement fences allocation in flight before backing memory publication" {
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 3 });
    var bundle = try prepare(&manager, 64, 1);
    defer bundle.deinitAfterJoin();
    var backing: InFlightBundleBacking = .{};
    var view = bundle.allocationView(.lsm_read_working_set, backing.allocator());
    var other = bundle.allocationView(.lsm_read_working_set, std.testing.allocator);
    var execution: runnable.Execution = .{};
    defer execution.finish();
    var next = try prepare(&manager, 1, 1);
    defer next.deinitAfterJoin();
    var waiting = entry(.query, 1);
    waiting.pending_resources = next.port();
    defer scheduler.finish(&waiting);
    try execution.startWithResources(&scheduler, .{ .io = std.testing.io }, .query, 1, bundle.port());
    const Worker = struct {
        allocator: std.mem.Allocator,
        completed: std.atomic.Value(bool) = .init(false),
        succeeded: bool = false,
        refused: bool = false,
        fn run(self: *@This()) void {
            defer self.completed.store(true, .release);
            const bytes = self.allocator.alloc(u8, 64) catch |err| {
                self.refused = err == error.OutOfMemory;
                return;
            };
            @memset(bytes, 0x7a);
            self.allocator.free(bytes);
            self.succeeded = true;
        }
    };
    var worker = Worker{ .allocator = view.allocator() };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    var joined = false;
    defer if (!joined) {
        backing.proceed.store(true, .release);
        thread.join();
    };
    const deadline = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
    while (!backing.entered.load(.acquire) and !worker.completed.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        @import("antfly_platform").time.sleepNs(50 * std.time.ns_per_us);
    }
    try std.testing.expect(backing.entered.load(.acquire));
    try std.testing.expectEqual(@as(u64, 64), bundle.allocatedBytes(.lsm_read_working_set));
    execution.finish();
    try std.testing.expectError(error.ResourcesStillLive, bundle.tryDeinitAfterJoin());
    try std.testing.expectError(error.OutOfMemory, other.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(u64, 64), metadata_fixture.payloadUsage(&manager));
    try scheduler.submit(&waiting);
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&waiting));
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    backing.proceed.store(true, .release);
    thread.join();
    joined = true;
    try std.testing.expect(!worker.succeeded and worker.refused);
    try std.testing.expectEqual(@as(usize, 1), backing.allocations.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), backing.frees.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), bundle.allocatedBytes(.lsm_read_working_set));
    try std.testing.expectEqual(@as(u64, 64), metadata_fixture.payloadUsage(&manager));
    scheduler.notifyResourceProgress();
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&waiting));
    try bundle.tryDeinitAfterJoin();
    scheduler.notifyResourceProgress();
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&waiting));
    try std.testing.expectEqual(@as(u64, 1), metadata_fixture.payloadUsage(&manager));
    scheduler.finish(&waiting);
    next.deinitAfterJoin();
    try std.testing.expectEqual(@as(u64, 0), metadata_fixture.payloadUsage(&manager));
}

test "runnable resource bundle aged CPU barrier preserves physical memory and IO continuation retirement" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var manager = try metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var scheduler = try runnable.Scheduler.init(.{ .slots = 4, .weights = .{ 1, 1, 1, 1 }, .max_backfills = 65535 });
    var retained = try prepare(&manager, 60, 2);
    defer retained.deinitAfterJoin();
    var requested = try prepare(&manager, 40, 8);
    defer requested.deinitAfterJoin();
    var continuation = entry(.inference, 1);
    continuation.pending_resources = retained.port();
    var holder = entry(.write, 2);
    var team = entry(.query, 3);
    team.pending_resources = requested.port();
    team.options.native_now_ns = Clock.now;
    var small = entry(.query, 1);
    var younger = entry(.query, 1);
    defer scheduler.finish(&continuation);
    defer scheduler.finish(&holder);
    defer scheduler.finish(&team);
    defer scheduler.finish(&small);
    defer scheduler.finish(&younger);
    try scheduler.submit(&continuation);
    var physical = retained.allocationView(.lsm_read_working_set, std.testing.allocator);
    const bytes = try physical.allocator().alloc(u8, 8);
    var live = true;
    defer if (live) physical.allocator().free(bytes);
    try scheduler.block(&continuation, .resource_blocked);
    try scheduler.submit(&holder);
    try scheduler.submit(&team);
    try scheduler.submit(&small);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&small));
    scheduler.finish(&small);
    Clock.tick = scheduler.config.backfillAgeNs();
    try scheduler.pollWait(&team);
    try scheduler.submit(&younger);
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&younger));
    try scheduler.unblock(&continuation);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&continuation));
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&team));
    try expectUsage(&manager, 60, 2);
    try std.testing.expectEqual(@as(u64, 1), manager.denseSearchAdmissionStats().admissions);
    physical.allocator().free(bytes);
    live = false;
    scheduler.finish(&continuation);
    retained.deinitAfterJoin();
    try expectUsage(&manager, 0, 0);
    scheduler.finish(&holder);
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&team));
    try std.testing.expectEqual(runnable.State.queued, scheduler.state(&younger));
    try expectUsage(&manager, 40, 8);
    scheduler.finish(&team);
    requested.deinitAfterJoin();
    try std.testing.expectEqual(runnable.State.running, scheduler.state(&younger));
    try expectUsage(&manager, 0, 0);
}
