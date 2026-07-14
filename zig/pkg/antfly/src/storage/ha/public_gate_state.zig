// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Lock-free public read/write role gates for a live HA data server.
//!
//! The mutable standby object remains behind the runtime HA mutex. Public
//! requests consume only atomically published role/progress state, so promotion
//! can close the standby without leaving request paths with borrowed pointers.

const std = @import("std");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const standby_mod = @import("standby.zig");
const write_gate = @import("write_gate.zig");

pub const Role = enum(u8) {
    disabled,
    standby,
    transitioning,
    primary,
    fenced_primary,
};

pub const State = struct {
    role: std.atomic.Value(u8) = .init(@intFromEnum(Role.disabled)),
    generation: std.atomic.Value(u64) = .init(1),
    progress_sequence: std.atomic.Value(u64) = .init(0),
    received_lsn: std.atomic.Value(u64) = .init(0),
    applied_lsn: std.atomic.Value(u64) = .init(0),
    safe_read_lsn: std.atomic.Value(u64) = .init(0),
    primary: ?*const primary_mod.Primary = null,

    pub fn configureStandby(self: *State, progress: standby_mod.Progress) void {
        self.publishStandbyProgress(progress);
        self.role.store(@intFromEnum(Role.standby), .release);
    }

    pub fn configurePrimary(
        self: *State,
        primary: *const primary_mod.Primary,
        fenced: bool,
    ) void {
        self.primary = primary;
        self.publishPrimaryFence(fenced);
    }

    pub fn publishStandbyProgress(self: *State, progress: standby_mod.Progress) void {
        _ = self.progress_sequence.fetchAdd(1, .acq_rel);
        self.received_lsn.store(progress.received_lsn, .monotonic);
        self.applied_lsn.store(progress.applied_lsn, .monotonic);
        self.safe_read_lsn.store(progress.safe_read_lsn, .monotonic);
        _ = self.progress_sequence.fetchAdd(1, .release);
    }

    pub fn beginPromotion(self: *State) void {
        self.role.store(@intFromEnum(Role.transitioning), .release);
    }

    pub fn publishPrimary(
        self: *State,
        primary: *const primary_mod.Primary,
        fenced: bool,
    ) void {
        _ = self.generation.fetchAdd(1, .acq_rel);
        self.configurePrimary(primary, fenced);
    }

    pub fn publishPrimaryFence(self: *State, fenced: bool) void {
        if (fenced) {
            self.role.store(@intFromEnum(Role.fenced_primary), .release);
            return;
        }

        var current = self.role.load(.acquire);
        while (current != @intFromEnum(Role.fenced_primary)) {
            current = self.role.cmpxchgWeak(
                current,
                @intFromEnum(Role.primary),
                .release,
                .acquire,
            ) orelse return;
        }
    }

    pub fn currentGeneration(self: *const State) u64 {
        return self.generation.load(.acquire);
    }

    pub fn isStandbyRole(self: *const State) bool {
        return switch (self.currentRole()) {
            .standby, .transitioning => true,
            .disabled, .primary, .fenced_primary => false,
        };
    }

    pub fn ownerJobsCanRun(self: *const State) bool {
        return switch (self.currentRole()) {
            .disabled, .primary => true,
            .standby, .transitioning, .fenced_primary => false,
        };
    }

    pub fn checkRead(self: *const State, request: read_gate.Request) !void {
        switch (self.currentRole()) {
            .disabled, .primary => return,
            .fenced_primary => return error.HAReadRequiresPrimary,
            .transitioning => return error.HAReadRequiresPrimary,
            .standby => {},
        }

        const decision = try read_gate.evaluateProgress(self.standbyProgress(), request);
        switch (decision.action) {
            .serve_standby => {},
            .wait_for_apply => return error.HAReadWaitForApply,
            .wait_for_metadata => return error.HAReadWaitForMetadata,
            .route_to_primary => return error.HAReadRequiresPrimary,
        }
    }

    pub fn checkWrite(self: *const State, expected_generation: ?u64) !void {
        return self.checkWriteWithHook(expected_generation, null);
    }

    fn checkWriteWithHook(
        self: *const State,
        expected_generation: ?u64,
        after_generation_check: ?*const fn (*const State) void,
    ) !void {
        if (expected_generation) |expected| {
            if (expected != self.currentGeneration()) {
                return error.HAPromotedStandbyRequiresPrimaryOpen;
            }
        }
        if (after_generation_check) |hook| hook(self);

        // Snapshot the role before validating the generation a second time.
        // A successful validation therefore linearizes against publishPrimary's
        // generation increment: a role observed across a promotion boundary can
        // never authorize a write through the stale pinned generation.
        const role = self.currentRole();
        if (expected_generation) |expected| {
            if (expected != self.currentGeneration()) {
                return error.HAPromotedStandbyRequiresPrimaryOpen;
            }
        }

        const decision = switch (role) {
            .disabled => return,
            .standby => return error.HAReadOnlyStandby,
            .transitioning => return error.HAPromotedStandbyRequiresPrimaryOpen,
            .primary => try write_gate.evaluatePrimary(self.primary orelse return error.HAPrimaryNotConfigured, .{}),
            .fenced_primary => return error.HAFencedPrimary,
        };
        switch (decision.action) {
            .allow_write => {},
            .reject_read_only_standby => return error.HAReadOnlyStandby,
            .open_promoted_primary => return error.HAPromotedStandbyRequiresPrimaryOpen,
            .reject_fenced_primary => return error.HAFencedPrimary,
        }
    }

    fn currentRole(self: *const State) Role {
        return @enumFromInt(self.role.load(.acquire));
    }

    fn standbyProgress(self: *const State) standby_mod.Progress {
        while (true) {
            const before = self.progress_sequence.load(.acquire);
            if (before & 1 != 0) continue;
            const progress = standby_mod.Progress{
                .received_lsn = self.received_lsn.load(.monotonic),
                .applied_lsn = self.applied_lsn.load(.monotonic),
                .safe_read_lsn = self.safe_read_lsn.load(.monotonic),
            };
            const after = self.progress_sequence.load(.acquire);
            if (before == after) return progress;
        }
    }
};

test "storage.ha public gate state invalidates pinned standby writes during promotion" {
    var state = State{};
    state.configureStandby(.{
        .received_lsn = 7,
        .applied_lsn = 6,
        .safe_read_lsn = 5,
    });

    const standby_generation = state.currentGeneration();
    try state.checkRead(.{ .consistency = .stale_ok });
    try std.testing.expectError(
        error.HAReadRequiresPrimary,
        state.checkRead(.{ .consistency = .primary }),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        state.checkWrite(standby_generation),
    );

    state.beginPromotion();

    try std.testing.expectEqual(standby_generation, state.currentGeneration());
    try std.testing.expectError(
        error.HAPromotedStandbyRequiresPrimaryOpen,
        state.checkWrite(standby_generation),
    );
    try std.testing.expectError(
        error.HAReadRequiresPrimary,
        state.checkRead(.{ .consistency = .stale_ok }),
    );

    var primary: primary_mod.Primary = undefined;
    state.publishPrimary(&primary, false);

    try std.testing.expectEqual(standby_generation + 1, state.currentGeneration());
    try std.testing.expectError(
        error.HAPromotedStandbyRequiresPrimaryOpen,
        state.checkWrite(standby_generation),
    );
    try state.checkRead(.{ .consistency = .stale_ok });

    state.publishPrimaryFence(true);
    try std.testing.expectError(error.HAFencedPrimary, state.checkWrite(null));
    try std.testing.expectError(
        error.HAReadRequiresPrimary,
        state.checkRead(.{ .consistency = .stale_ok }),
    );
    try std.testing.expect(!state.ownerJobsCanRun());
}

test "storage.ha public gate state rejects a pinned write when promotion changes generation between snapshots" {
    var state = State{};
    const pinned_generation = state.currentGeneration();

    const PromotionInterleave = struct {
        fn afterGenerationCheck(current: *const State) void {
            // This is the first publication step in State.publishPrimary. Leave
            // the prior role visible to deterministically exercise the interval
            // between generation publication and the subsequent role store.
            _ = @constCast(current).generation.fetchAdd(1, .acq_rel);
        }
    };

    // RED: checkWrite currently validates the generation once, observes the old
    // permissive role after the concurrent increment, and returns success. A
    // coherent role+generation snapshot (or a validating second generation
    // load) must reject the stale pinned DB handle.
    try std.testing.expectError(
        error.HAPromotedStandbyRequiresPrimaryOpen,
        state.checkWriteWithHook(pinned_generation, PromotionInterleave.afterGenerationCheck),
    );
}

test "storage.ha public gate state admits a pinned write across a stable authorization snapshot" {
    var state = State{};
    const pinned_generation = state.currentGeneration();

    try state.checkWrite(pinned_generation);
    try state.checkWriteWithHook(pinned_generation, struct {
        fn stable(_: *const State) void {}
    }.stable);
}

test "storage.ha public gate state never clears an observed primary fence" {
    var state = State{};
    var primary: primary_mod.Primary = undefined;
    state.configurePrimary(&primary, false);

    state.publishPrimaryFence(true);
    state.publishPrimaryFence(false);

    try std.testing.expectError(error.HAFencedPrimary, state.checkWrite(null));
    try std.testing.expect(!state.ownerJobsCanRun());
}
