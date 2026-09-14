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

//! Public-control adapter for the private compiled restore owner operation.
const std = @import("std");
const operation = @import("operation.zig");
const db_types = @import("../storage/db/types.zig");
const callback_abi = @import("../runtime_callback_abi.zig");
pub const Source = @import("restore_owner_contract.zig").Source;
pub const Request = @import("restore_owner_contract.zig").Request;
pub const Response = @import("restore_owner_contract.zig").Response;
pub const Prepared = struct { response: Response, batch_json: ?[]const u8 = null };
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response };
    const BoundaryAbi = callback_abi.Boundary(VTable);
    pub fn execute(self: Port, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: Request, context: operation.RequestContext) !Response {
        try context.ensureActive();
        try request.validate(group_id);
        return BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
    }
};
pub const Proposer = struct {
    ptr: *anyopaque,
    propose: *const fn (*anyopaque, db_types.BatchRequest, operation.RequestContext) anyerror!void,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    const VTable = struct { propose: *const fn (*anyopaque, db_types.BatchRequest, operation.RequestContext) anyerror!void };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn submit(self: Proposer, request: db_types.BatchRequest, context: operation.RequestContext) !void {
        try BoundaryAbi.call("propose", self.boundary_dispatch, self.propose, .{ self.ptr, request, context });
    }
};
