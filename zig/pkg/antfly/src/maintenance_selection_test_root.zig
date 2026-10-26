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

//! Production Source registry and dispatch with deterministic owner responses.
//! This fixture does not claim to qualify a physical DB's maintenance step.
pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const MaintenanceSelectionProbe = struct {
    score: u64 = 0,
    due: bool = false,
    fail_step: bool = false,
    inspections: usize = 0,
    steps: usize = 0,
};
const abi = @import("kernel_owner_abi");
fn maintenance(raw: ?*anyopaque, request: *const abi.MaintenanceRequest, out: *abi.MaintenanceResult) callconv(.c) abi.Status {
    if (request.version != abi.abi_version) return .invalid_abi;
    const probe: *MaintenanceSelectionProbe = @ptrCast(@alignCast(raw orelse return .invalid_argument));
    out.* = .{};
    switch (@as(abi.MaintenanceAction, @fromBackingInt(@intCast(request.action)))) {
        .inspect, .inspect_best_effort => {
            probe.inspections += 1;
            out.maintenance_score = probe.score;
            out.has_next_wake_delay = @intFromBool(probe.due);
            out.next_wake_delay_ns = 0;
        },
        .lsm_step, .lsm_step_best_effort => {
            probe.steps += 1;
            if (probe.fail_step) return .busy;
        },
        else => return .invalid_argument,
    }
    return .ok;
}
fn close(_: ?*anyopaque) callconv(.c) void {
    @panic("maintenance selection must not close a resident owner");
}
fn drain(_: ?*anyopaque, _: *const abi.CloseDrainRequest) callconv(.c) abi.Status {
    @panic("maintenance selection must not drain a resident owner");
}
comptime {
    @export(&drain, .{ .name = "antfly_storage_owner_drain_close" });
    @export(&maintenance, .{ .name = "antfly_storage_owner_maintenance" });
    @export(&close, .{ .name = "antfly_storage_owner_close" });
}
test {
    _ = @import("api/maintenance_selection.zig");
    _ = @import("api/kernel_owner_source.zig");
}
