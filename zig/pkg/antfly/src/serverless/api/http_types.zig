// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

const std = @import("std");
const cancellation_mod = @import("antfly_cancellation");
const http_routes = @import("http_routes.zig");

const Allocator = std.mem.Allocator;

/// Request-scoped cancellation bridge for engines accepting cancellation
/// tokens. The context owns the clock semantics; callbacks are never retained.
pub const RequestGuard = struct {
    context: @import("antfly_local_sources").api_operation.RequestContext,
    pub fn token(self: *const @This()) cancellation_mod.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = isCancelled, .check_fn = check };
    }
    fn check(ptr: *const anyopaque) !void {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        return self.context.ensureActive();
    }
    fn isCancelled(ptr: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        self.context.ensureActive() catch return true;
        return false;
    }
};

pub const HttpRequest = struct {
    method: http_routes.HttpMethod,
    path: []const u8,
    body: []const u8 = "",
    /// Borrowed from the listener and valid only while `handle` is running.
    /// Application work must not retain this callback beyond the request.
    cancellation: cancellation_mod.CancellationToken = .none,

    deadline_ns: ?u64 = null,
    deadline_io: @FieldType(@import("antfly_local_sources").api_operation.RequestContext, "deadline_io") = null,
    pub fn context(self: HttpRequest) @import("antfly_local_sources").api_operation.RequestContext {
        return .{ .cancellation = self.cancellation, .deadline_ns = self.deadline_ns, .deadline_io = self.deadline_io };
    }
    pub fn ensureActive(self: HttpRequest) !void {
        return self.context().ensureActive();
    }
};

pub const HttpResponse = struct {
    status: u16,
    content_type: []u8,
    body: []u8,
    retry_after_seconds: ?u32 = null,

    pub fn deinit(self: *HttpResponse, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.body);
        self.* = undefined;
    }
};
