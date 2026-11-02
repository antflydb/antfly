// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional measurements on the submitting thread. Nested observations include
//! work in child scopes. They exclude independent worker threads and never
//! pretend that an opaque vendor encode reveals its internal kernel launches.
const build_options = @import("build_options");

pub const Counters = struct {
    native_assigned_dispatches: u64 = 0,
    metal_encoded_dispatches: ?u64 = null,
    metal_command_submissions: ?u64 = null,
    metal_command_waits: ?u64 = null,
    metal_vendor_encodes: ?u64 = null,
};

threadlocal var native_depth: usize = 0;
threadlocal var native_dispatches: u64 = 0;

extern fn termite_metal_observation_begin() void;
extern fn termite_metal_observation_end() void;
extern fn termite_metal_observation_snapshot(values: [*]u64) void;

/// Record only completed execution, never a descriptor eligibility query.
pub inline fn nativeAssignedDispatch() void {
    if (native_depth != 0) native_dispatches +|= 1;
}

fn metalCounters() [4]u64 {
    var values: [4]u64 = @splat(0);
    if (comptime build_options.enable_metal) termite_metal_observation_snapshot(&values);
    return values;
}

pub const Scope = struct {
    active: bool = false,
    native_start: u64 = 0,
    metal_start: [4]u64 = @splat(0),

    /// Counters live in thread-local storage, so this returned owner can move.
    /// Its lifetime must remain on this submitting thread.
    pub fn init() Scope {
        native_depth += 1;
        if (comptime build_options.enable_metal) termite_metal_observation_begin();
        return .{ .active = true, .native_start = native_dispatches, .metal_start = metalCounters() };
    }

    pub fn snapshot(self: Scope) Counters {
        var result = Counters{ .native_assigned_dispatches = native_dispatches -| self.native_start };
        if (comptime build_options.enable_metal) {
            const after = metalCounters();
            result.metal_encoded_dispatches = after[0] -| self.metal_start[0];
            result.metal_command_waits = after[1] -| self.metal_start[1];
            result.metal_command_submissions = after[2] -| self.metal_start[2];
            result.metal_vendor_encodes = after[3] -| self.metal_start[3];
        }
        return result;
    }

    pub fn deinit(self: *Scope) void {
        if (!self.active) return;
        self.active = false;
        native_depth -= 1;
        if (comptime build_options.enable_metal) termite_metal_observation_end();
    }
};
