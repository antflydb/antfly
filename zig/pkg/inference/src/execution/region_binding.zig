// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pool-owned mapping of a frozen region to the ordinary executable's slots.
//! This metadata does not own or extend any Frame array address.
const ops = @import("../ops/ops.zig");
const region = @import("region_route.zig");
const kernel = @import("kernel_route.zig");
const buffers = @import("../graph/buffer_plan.zig");

pub const max_outputs = 8;
pub const max_inputs = 32;
pub const Installed = struct {
    owner: *const region.Owned,
    key: u64,
    backend_index: usize,
    epoch: u64,
    outputs: []const Output,
    workspace_allocation: buffers.AllocationId,
    workspace_bytes: usize,
    host_workspace_bytes: usize,

    pub const Output = struct {
        slot: kernel.OutputSlot,
        /// Existing logical aliases share the primary allocation. The graph
        /// receives a retained logical view and no new writable authority.
        alias_index: ?usize = null,
    };
};

pub const Bound = struct {
    backend: ops.ComputeBackend,
    outputs: [max_outputs]region.Assigned = undefined,
    output_count: usize = 0,
    owned: [max_outputs]bool = @splat(false),
    slots: [max_outputs]kernel.OutputSlot = undefined,
    workspace: ?ops.CT = null,
    host_workspace: []u8 = &.{},

    pub fn invocation(self: *const Bound, implementation: kernel.Implementation, policy_key: u64, facts: []const kernel.Binding, inputs: []const ops.CT) region.Invocation {
        return .{ .validation = .{ .implementation = implementation, .policy_key = policy_key, .inputs = facts, .outputs = self.slots[0..self.output_count] }, .inputs = inputs, .outputs = self.outputs[0..self.output_count], .workspace = self.workspace, .host_workspace = self.host_workspace };
    }
    /// Moves a published logical view exactly once; the Lease independently
    /// retains frame/quota authority after the borrowed invocation ends.
    pub fn take(self: *Bound, index: usize) !ops.CT {
        if (index >= self.output_count) return error.PreparedRegionOutputMismatch;
        if (!self.owned[index]) return error.PreparedRegionOutputAlreadyPublished;
        self.owned[index] = false;
        return self.outputs[index].tensor;
    }
    pub fn deinit(self: *Bound) void {
        for (self.outputs[0..self.output_count], self.owned[0..self.output_count]) |output, owned| if (owned) self.backend.free(output.tensor);
        if (self.workspace) |workspace| self.backend.free(workspace);
        self.* = undefined;
    }
};
