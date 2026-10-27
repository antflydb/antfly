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

//! Deterministic physical-owner replies for production registry/worker tests.
//! Physical database maintenance and durability are outside this fixture.
pub const Probe = struct {
    hook_ctx: ?*anyopaque = null,
    inspect_hook: ?*const fn (?*anyopaque) void = null,
    step_hook: ?*const fn (?*anyopaque) void = null,
    score: u64 = 0,
    due: bool = false,
    fail_step: bool = false,
    inspections: usize = 0,
    steps: usize = 0,
};
const abi = @import("kernel_owner_abi");
pub fn maintenance(raw: ?*anyopaque, request: *const abi.MaintenanceRequest, out: *abi.MaintenanceResult) callconv(.c) abi.Status {
    if (request.version != abi.abi_version) return .invalid_abi;
    const probe: *Probe = @ptrCast(@alignCast(raw orelse return .invalid_argument));
    out.* = .{};
    switch (@as(abi.MaintenanceAction, @fromBackingInt(@intCast(request.action)))) {
        .inspect, .inspect_best_effort => {
            probe.inspections += 1;
            if (probe.inspect_hook) |hook| hook(probe.hook_ctx);
            out.maintenance_score = probe.score;
            out.has_next_wake_delay = @intFromBool(probe.due);
            out.next_wake_delay_ns = 0;
        },
        .lsm_step, .lsm_step_best_effort => {
            probe.steps += 1;
            if (probe.step_hook) |hook| hook(probe.hook_ctx);
            if (probe.fail_step) return .busy;
        },
        else => return .invalid_argument,
    }
    return .ok;
}
