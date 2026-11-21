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

//! Transport adapters authenticate service authority. This callback boundary
//! binds the complete route, operation, captured command and result. Known
//! receipts survive late cancellation without claiming a confirmed effect.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const wire = @import("../data/source_retirement_http_wire.zig");
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
const A = std.mem.Allocator;
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.Request, operation.RequestContext) anyerror!wire.OwnedReply,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { execute: @FieldType(Port, "execute_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn execute(self: Port, a: A, fence: metadata.CatalogRouteFence, table: []const u8, request: wire.Request, context: operation.RequestContext) !wire.OwnedReply {
        return Boundary.call("execute", self.boundary_dispatch, self.execute_fn, .{ self.ptr, a, fence, table, request, context });
    }
};
pub fn execute(port: ?Port, a: A, group: u64, table: []const u8, body: []const u8, original: operation.RequestContext) !wire.OwnedReply {
    try original.ensureActive();
    var decoded = try wire.decodeRequest(a, body);
    defer decoded.deinit();
    const request = decoded.value;
    var route = try @import("source_owner_scope.zig").Scope.init(a, group, table, request.owner(), original);
    defer route.deinit();
    var scope = try @import("history_publication_operations.zig").Scope.init(route.fence, original);
    const context = scope.context();
    try context.ensureActive();
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    var reply = target.execute(a, route.fence, table, request, context) catch |err| {
        if (err == error.RaftBatchWriteOutcomeUnknown) return wire.unknown(a, request);
        return err;
    };
    errdefer reply.deinit();
    reply.value.validate(a, request) catch return error.SourceOutcomeUnresolved;
    context.ensureActive() catch {
        reply.value.outcome = null;
        return reply;
    };
    return reply;
}
