// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Private parent-owner activation boundary. The request identifies the
//! pending local fence only; it carries no metadata decision or bearer proof.
//! The receiving leader must fetch its own read-index authority before it
//! proposes the irreversible generation tombstones.
const std = @import("std");
const operation = @import("operation.zig");
const callback_abi = @import("../runtime_callback_abi.zig");
const topology = @import("../storage/db/relational_integrity_topology_contract.zig");

pub const Request = struct {
    plan_id: [16]u8,
    fence: topology.Fence,
    /// Phase two confirms that metadata durably stored the exact owner receipt.
    /// The owner obtains that proof itself through a fresh read-index request.
    acknowledge: bool = false,

    pub fn validate(self: Request, group_id: u64) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.fence.role != .truncate_parent or
            self.fence.owner_group_id != group_id) return error.InvalidRestoreStaging;
        _ = try self.fence.encode();
    }
};

pub const Response = struct {
    /// Digest of the replicated owner activation and release receipt.
    receipt: [32]u8,
};

pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: Port, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, input: Request, context: operation.RequestContext) !Response {
        try context.ensureActive();
        try input.validate(group_id);
        return BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, input, context });
    }
};
