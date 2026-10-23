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

//! Public-control adapter for the private compiled restore owner operation.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
const db_types = @import("antfly_local_sources").storage_db_types;
const callback_abi = @import("antfly_local_sources").runtime_callback_abi;
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
