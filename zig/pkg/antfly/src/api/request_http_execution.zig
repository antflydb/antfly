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

//! Invocation-scoped metadata HTTP waits. Encoding and parsing retain CPU;
//! transports loan it only around physical waits and return it before cleanup.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const http = @import("../raft/transport/http_common.zig");
const Request = @import("antfly_local_sources").api_operation.RequestContext;

pub const Scope = struct {
    request: Request,
    inner: http.RequestExecutor,
    saved_class: ?runtime.runnable_abi.Class = null,

    pub fn executor(self: *Scope) http.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{ .execute = execute },
            .routes_owned_queries = self.inner.routes_owned_queries,
            .clock_io = self.inner.clock_io,
            .realtime_ns_fn = if (self.inner.realtime_ns_fn != null) realtime else null,
        };
    }
    pub fn sleep(self: *Scope, io: std.Io, nanoseconds: u64) !void {
        try begin(self);
        const result = io.sleep(.fromNanoseconds(@intCast(nanoseconds)), .awake);
        try end(self);
        try result;
        try self.request.ensureExecutionActive();
    }
    fn from(raw: *anyopaque) *Scope {
        return @ptrCast(@alignCast(raw));
    }
    fn realtime(raw: *anyopaque) i128 {
        return from(raw).inner.realtimeNs().?;
    }
    fn begin(raw: *anyopaque) !void {
        const self = from(raw);
        try self.request.ensureExecutionActive();
        const team = self.request.execution_team orelse return;
        std.debug.assert(self.saved_class == null);
        const class = try team.borrow.currentClass();
        const status = team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource));
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        self.saved_class = class;
    }
    fn end(raw: *anyopaque) !void {
        const self = from(raw);
        const class = self.saved_class orelse return;
        self.saved_class = null;
        const borrow = self.request.execution_team.?.borrow;
        // Restore the execution's own lifetime before checking this operation.
        // An expired reader must not poison the shared control service root.
        const status = borrow.enter(borrow.context, @backingInt(class));
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        try self.request.ensureExecutionActive();
    }
    fn execute(raw: *anyopaque, a: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
        const self = from(raw);
        try self.request.ensureExecutionActive();
        var scoped = request;
        if (self.request.execution_team != null and scoped.execution_wait == null)
            scoped.execution_wait = .{ .ptr = self, .begin = begin, .end = end };
        var response = self.inner.execute(a, scoped) catch |err| {
            try self.request.ensureExecutionActive();
            return err;
        };
        errdefer response.deinit(a);
        try self.request.ensureExecutionActive();
        return response;
    }
};
