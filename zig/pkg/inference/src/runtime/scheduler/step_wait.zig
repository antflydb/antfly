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

//! Shared queue wait/claim/cancellation-drain protocol. Callers retain work
//! records and provide their existing execution/publication driver.
const std = @import("std");
const platform = @import("antfly_platform");
const runtime = @import("../root.zig");

pub fn run(
    allocator: std.mem.Allocator,
    scheduler: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
    lease: *runtime.scheduler.native_generate.Lease,
    leader_work_ptr: *anyopaque,
    leader_phase: runtime.scheduler.native_generate.Phase,
    io: std.Io,
    initial_wait_error: ?anyerror,
    driver: anytype,
) !void {
    const DriverInfo = @typeInfo(@TypeOf(driver));
    const DriverChild = DriverInfo.pointer.child;

    var step = std.ArrayListUnmanaged(runtime.scheduler.native_generate.StepItem).empty;
    defer step.deinit(allocator);
    var wait_error = initial_wait_error;
    const poll_wait_us = scheduler.stepPollWaitUs();
    const events = scheduler.usesStepEvents();
    const dedicated = scheduler.hasExecutor();
    const Ready = struct {
        fn check(raw: *anyopaque) bool {
            const typed: @TypeOf(driver) = @ptrCast(@alignCast(raw));
            return typed.isReady();
        }
    };
    var waiter: runtime.scheduler.native_generate.NativeGenerateCoordinator.Waiter = .{ .io = io, .ready_ptr = driver, .ready_fn = Ready.check };
    if (events) scheduler.attachWaiter(&waiter);
    defer if (events) scheduler.detachWaiter(&waiter);

    while (!driver.isReady()) {
        if (events) scheduler.prepareWait(&waiter);
        if (wait_error == null) {
            if (@hasDecl(DriverChild, "checkControl")) {
                driver.checkControl() catch |err| {
                    wait_error = err;
                };
            }
        }
        if (wait_error) |err| {
            // Cancellation cannot drop a stack-backed work pointer while a
            // peer may own it. Remove idle work immediately; otherwise wait
            // for the claimed step to publish ready before returning.
            if (events) scheduler.markDraining(&waiter);
            if (scheduler.cancelPendingWorkIfIdle(leader_work_ptr, leader_phase)) return err;
            if (events) {
                if (!driver.isReady()) waiter.event.waitUncancelable(io);
            } else platform.time.yieldBriefly();
            continue;
        }
        if (dedicated) {
            if (!driver.isReady()) {
                if (@hasDecl(DriverChild, "checkControl")) {
                    // Application deadlines do not cancel the I/O event wait.
                    // Poll them while preserving the claimed-work drain path.
                    waiter.event.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                        error.Timeout => {},
                        else => wait_error = err,
                    };
                } else waiter.event.wait(io) catch |err| {
                    wait_error = err;
                };
            }
            continue;
        }
        if (@hasDecl(DriverChild, "preStep")) driver.preStep();
        const budget = driver.stepBudget();
        const claimed = scheduler.claimStep(allocator, lease, leader_work_ptr, leader_phase, budget, &step) catch |err| {
            // A peer may already own this request's stack-backed work pointer.
            // Enter the same drain-or-cancel path as I/O cancellation instead
            // of unwinding it while a claimed step can still publish to it.
            wait_error = err;
            continue;
        };
        if (claimed) {
            driver.execute(scheduler, lease, step.items);
        } else {
            if (events) {
                // Ready may have been published before this iteration reset
                // its event. Recheck the predicate before parking.
                if (!driver.isReady()) waiter.event.waitTimeout(io, .{ .duration = .{ .raw = .fromMicroseconds(scheduler.eventWaitUs()), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => {},
                    else => wait_error = err,
                };
            } else io.sleep(std.Io.Duration.fromMicroseconds(poll_wait_us), .awake) catch |err| {
                wait_error = err;
            };
        }
    }
    if (wait_error) |err| return err;
}
