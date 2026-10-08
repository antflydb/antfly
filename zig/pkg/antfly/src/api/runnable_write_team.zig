// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! SQL write capability on one already admitted execution. The caller supplies
//! a stable concurrent bounded allocator, and retains it through all joins.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub fn init(execution: *scheduler.Execution, allocator: std.mem.Allocator) !runtime.runnable_abi.Team {
    const entry = execution.entry orelse return error.InvalidRunnableTransition;
    const owner = entry.scheduler orelse return error.InvalidRunnableTransition;
    if (entry.state != .running or (entry.class != .write and entry.class != .control)) return error.InvalidRunnableTransition;
    if (entry.class == .control and entry.slots != 1) return error.InvalidRunnableDemand;
    return .{
        .borrow = .{
            .context = execution,
            .cpu_slots = entry.slots,
            .parent_class = @intCast(@backingInt(entry.class)),
            .checkpoint = unsupportedCheck,
            .park = unsupportedPhase,
            .enter = unsupportedPhase,
            .resize_team = resize,
        },
        // Protected terminal SQL work must leave the health reserve available.
        .maximum_slots = if (entry.class == .control) 1 else owner.config.slots - owner.config.control_slots,
        .allocator = allocator,
    };
}

fn unsupportedCheck(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
    return runtime.error_abi.statusFromError(error.ExecutionWaitUnsupported);
}

fn unsupportedPhase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
    // Accepted writes still need independent completion authority before they
    // can release CPU across waits, cancellation or scheduler shutdown.
    return runtime.error_abi.statusFromError(error.ExecutionWaitUnsupported);
}

fn resize(raw: *anyopaque, slots: usize) callconv(.c) runtime.error_abi.Status {
    const execution: *scheduler.Execution = @ptrCast(@alignCast(raw));
    const entry = execution.entry orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
    if (entry.class == .control and slots != 1) return runtime.error_abi.statusFromError(error.InvalidRunnableDemand);
    execution.resizeTeam(slots) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
