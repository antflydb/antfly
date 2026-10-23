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

//! Private owner installation boundary for a metadata-owned RLS publication.
//! The request names an immutable publication and owner cut; it never carries
//! policy programs or a caller-authored receipt. The owner fetches the exact
//! metadata snapshot itself before proposing its isolated Raft command.
const std = @import("std");
const callback_abi = @import("antfly_local_sources").runtime_callback_abi;
const operation = @import("antfly_local_sources").api_operation;
const policies = @import("antfly_local_sources").system_catalog_policies;

pub const Request = policies.InstallRequest;
pub const Response = @import("antfly_local_sources").storage_db_row_policy_bundle.Receipt;

pub fn validate(request: Request, group_id: u64) !void {
    if (request.table_id == 0 or request.owner_group_id != group_id or group_id == 0 or
        request.expected_generation == 0 or request.expected_catalog_epoch == 0 or
        std.mem.allEqual(u8, &request.expected_descriptor_digest, 0))
        return error.InvalidRowPolicyPublication;
}

pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Response };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: Port, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: Request, context: operation.RequestContext) !Response {
        try context.ensureActive();
        try validate(request, group_id);
        return BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
    }
};

test "private row policy install accepts only an exact owner publication scope" {
    const request: Request = .{ .table_id = 7, .expected_generation = 3, .expected_catalog_epoch = 19, .expected_phase = .serving_install, .owner_group_id = 11, .expected_descriptor_digest = @splat(9) };
    try validate(request, 11);
    var invalid = request;
    invalid.owner_group_id = 12;
    try std.testing.expectError(error.InvalidRowPolicyPublication, validate(invalid, 11));
    invalid = request;
    invalid.expected_descriptor_digest = @splat(0);
    try std.testing.expectError(error.InvalidRowPolicyPublication, validate(invalid, 11));
}
