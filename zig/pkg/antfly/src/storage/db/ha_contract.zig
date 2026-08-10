// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! HA policy handles shared by distributed control and physical DB ownership.
//! Keeping them outside `db.zig` prevents a policy field from importing the
//! complete physical database implementation into a control-only unit.

const std = @import("std");
const primary_mod = @import("../ha/primary.zig");
const standby_mod = @import("../ha/standby.zig");
const write_gate_mod = @import("../ha/write_gate.zig");
const public_gate_state_mod = @import("../ha/public_gate_state.zig");

pub const SyncWaitFn = *const fn (
    ctx: *anyopaque,
    primary: *primary_mod.Primary,
    target_lsn: u64,
    policy: primary_mod.SyncPolicy,
) anyerror!void;

pub const AsyncEffectMirror = struct {
    primary: *primary_mod.Primary,
    last_lsn: ?*std.atomic.Value(u64) = null,
    failure_count: ?*std.atomic.Value(u64) = null,
    sync_policy: primary_mod.SyncPolicy = .{},
    sync_wait_ctx: ?*anyopaque = null,
    sync_wait_fn: ?SyncWaitFn = null,
    last_gate_lsn: ?*std.atomic.Value(u64) = null,
    last_gate_action: ?*std.atomic.Value(u8) = null,
    sync_reject_count: ?*std.atomic.Value(u64) = null,
    sync_wait_count: ?*std.atomic.Value(u64) = null,
    sync_degraded_count: ?*std.atomic.Value(u64) = null,
};

pub const AsyncBatchMirror = AsyncEffectMirror;
pub const AsyncMetadataMirror = AsyncEffectMirror;

pub const SharedWriteGate = struct {
    state: *const public_gate_state_mod.State,
    generation: ?u64 = null,
};

pub const WriteGate = union(enum) {
    primary: *primary_mod.Primary,
    fenced_primary: write_gate_mod.FencedPrimary,
    standby: *standby_mod.Standby,
    shared: SharedWriteGate,

    pub fn check(self: WriteGate) !void {
        switch (self) {
            .shared => |shared| return try shared.state.checkWrite(shared.generation),
            else => {},
        }

        const decision = switch (self) {
            .primary => |primary| try write_gate_mod.evaluatePrimary(primary, .{}),
            .fenced_primary => |fenced| try write_gate_mod.evaluateFencedPrimary(fenced, .{}),
            .standby => |standby| try write_gate_mod.evaluateStandby(standby, .{}),
            .shared => unreachable,
        };
        switch (decision.action) {
            .allow_write => {},
            .reject_read_only_standby => return error.HAReadOnlyStandby,
            .open_promoted_primary => return error.HAPromotedStandbyRequiresPrimaryOpen,
            .reject_fenced_primary => return error.HAFencedPrimary,
        }
    }

    pub fn pinned(self: WriteGate) WriteGate {
        return switch (self) {
            .shared => |shared| .{ .shared = .{
                .state = shared.state,
                .generation = shared.generation orelse shared.state.currentGeneration(),
            } },
            else => self,
        };
    }
};
