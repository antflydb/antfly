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

//! Private qualified metadata lifecycle observations. Endpoint authorization
//! and service ReadIndex are mandatory; bytes and numeric cuts are not leases.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const wire = @import("../metadata/source_scope_wire.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub const path = "/internal/v1/catalog/source-scope-capture-v1";
pub const ack_header = "X-Antfly-Source-Scope-Capture";
pub const ack_value = "read-index-v1";
pub const Port = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, wire.Request, Context) anyerror!wire.capture.Snapshot,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { read: @FieldType(Port, "read_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    fn authorize(context: Context) !void {
        try context.ensureActive();
        const principal = context.principal orelse return error.Forbidden;
        if (principal.kind != .internal or !std.mem.eql(u8, principal.subject, "source-scope-maintenance")) return error.Forbidden;
    }
    pub fn read(self: Port, a: A, input: wire.Request, context: Context) !wire.capture.Snapshot {
        try authorize(context);
        try input.validate();
        var result = try Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, input, context });
        errdefer result.deinit(a);
        try (wire.Reply{ .version = 1, .request_digest = input.digest(), .snapshot = result }).validate(input);
        try context.ensureActive();
        return result;
    }
    pub fn fromService(svc: anytype) Port {
        const Service = @typeInfo(@TypeOf(svc)).pointer.child;
        const Adapter = struct {
            fn read(raw: *anyopaque, a: A, input: wire.Request, context: Context) !wire.capture.Snapshot {
                try authorize(context);
                const self: *Service = @ptrCast(@alignCast(raw));
                return wire.capture.readOnService(self, a, input.query, context);
            }
        };
        return .{ .ptr = svc, .read_fn = Adapter.read };
    }
};
