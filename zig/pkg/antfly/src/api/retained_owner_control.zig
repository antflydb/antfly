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

//! Private native-owner controls carry only admitted metadata identity.
//! The receiving owner obtains the exact source/definition snapshot itself;
//! callers cannot supply programs, source bindings, timestamps or receipts.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
const callback_abi = @import("antfly_local_sources").runtime_callback_abi;
const model = @import("../metadata/retained_dataset_owners.zig");
pub const Response = @import("../metadata/retained_dataset_owner_receipt.zig").Receipt;
pub const Snapshot = struct {
    cursor: model.Cursor,
    plan: model.Plan,
    owner: model.Owner,
    resource: @import("antfly_local_sources").system_catalog_domain.Resource,
    source_table: ?@import("../metadata/table_manager.zig").TableRecord,
    source_range: ?@import("../metadata/table_manager.zig").RangeRecord,
};
pub const Request = struct {
    resource: u64,
    generation: u64,
    plan: model.Id,
    group: u64,
    revision: u64,
    action: enum { reconcile, drain, close, revoke, observe } = .reconcile,

    pub fn read(self: Request) model.Read {
        return .{ .owner = .{ .resource = self.resource, .generation = self.generation, .plan = self.plan, .group = self.group, .revision = self.revision } };
    }
    pub fn validate(self: Request, group: u64) !void {
        try self.read().validate();
        if (group == 0 or self.group != group) return error.InvalidRetainedOwnerPlan;
    }
    pub fn admit(self: Request, plan: model.Plan, owner: model.Owner) !void {
        try self.validate(owner.group);
        try plan.validate();
        try owner.validate();
        if (plan.resource != self.resource or plan.generation != self.generation or
            !std.mem.eql(u8, &plan.id, &self.plan) or !std.mem.eql(u8, &owner.plan, &self.plan) or
            owner.revision != self.revision or owner.incarnation != plan.generation) return error.RetainedOwnerPlanChanged;
        switch (self.action) {
            .reconcile => if ((plan.phase != .activating and plan.phase != .active) or owner.mode == .closed or owner.mode == .revoked) return error.RetainedOwnerPlanChanged,
            .drain => if ((plan.phase != .closing and plan.phase != .retiring) or owner.mode == .revoked) return error.RetainedOwnerPlanChanged,
            .close => if (plan.phase != .closing or owner.mode == .revoked) return error.RetainedOwnerPlanChanged,
            .revoke => if (plan.phase != .retiring and !(plan.phase == .closing and owner.mode == .closed and owner.independent_receipt != null)) return error.RetainedOwnerPlanChanged,
            .observe => {},
        }
    }
};
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response };
    const BoundaryAbi = callback_abi.Boundary(VTable);
    pub fn execute(self: Port, a: std.mem.Allocator, table: []const u8, group: u64, request: Request, context: operation.RequestContext) !Response {
        try context.ensureActive();
        try request.validate(group);
        return BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, a, table, group, request, context });
    }
};

test "retained owner native controls cannot cross an admitted plan or owner revision" {
    const request: Request = .{ .resource = 7, .generation = 1, .plan = @splat(3), .group = 11, .revision = 2 };
    try request.validate(11);
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, request.validate(12));
    var stale = request;
    stale.revision = 0;
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, stale.validate(11));
    stale = request;
    stale.plan = @splat(0);
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, stale.validate(11));
}
