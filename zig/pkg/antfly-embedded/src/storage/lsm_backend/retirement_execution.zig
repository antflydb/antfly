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

//! Backend-owned retirement debt uses fresh, nonblocking cleanup execution.
//! No rejected request scope or execution handle survives a slice. The backend
//! lifecycle pin and accounting queues retain every partial cursor on refusal.
const std = @import("std");
const execution = @import("../../common/background_execution.zig");
const admission = @import("../../common/read_admission.zig");
const runtime = @import("runtime.zig");
const platform = @import("antfly_platform");

/// Only detached backend maintenance may acquire cleanup CPU. Foreground and
/// selected DB maintenance callers may still hold outer catalog/service guards.
pub fn deferred(backend: anytype) bool {
    const owner = backend.background_executor.runtime orelse return false;
    return owner.runnableOwner() != null;
}

pub const Slice = struct { done: bool = false, units: usize = 0, ns: u64 = 0, refused: bool = false };
pub var test_hook: ?*const fn (admission.Port) anyerror!void = null;

/// Called off Backend.mu with the cursor exclusively claimed and Backend pinned.
/// Ordinary capacity and caller cancellation cannot prevent cleanup admission;
/// competing cleanup owners can refuse this slice without consuming its debt.
pub fn step(backend: anytype, cursor: anytype, limit: usize, deadline: u64) Slice {
    const started = runtime.workNowNs(backend);
    var pass: ?execution.Pass = null;
    defer if (pass) |*held| held.deinit();
    var scope: execution.AdmissionScope = undefined;
    var payload: admission.Payload = undefined;
    var original: ?admission.Port = null;
    if (backend.background_executor.runtime) |owner| if (owner.runnableOwner()) |scheduler| {
        const io = owner.io() orelse return .{ .refused = true };
        pass = execution.Pass.initWithOptions(scheduler, &io, platform.time.monotonicNs() +| 100 * std.time.ns_per_ms, .{}, .{ .allow_wait = false, .cleanup = true }) catch return .{ .refused = true };
        scope = pass.?.admissionScope(io) catch return .{ .refused = true };
        payload = scope.payload();
        original = .init(&payload);
        if (comptime @import("builtin").is_test) if (test_hook) |hook| hook(original.?) catch return .{ .refused = true };
    };
    var credits = limit;
    var done = false;
    var refused = false;
    while (credits != 0 and runtime.workNowNs(backend) < deadline) {
        var part: usize = @min(credits, 64);
        const before = part;
        const result = cursor.stepWithAdmission(backend.allocator, &part, original);
        credits -= before - part;
        done = result catch {
            refused = true;
            break;
        };
        if (done) break;
    }
    return .{ .done = done, .units = limit - credits, .ns = runtime.workNowNs(backend) -| started, .refused = refused };
}
