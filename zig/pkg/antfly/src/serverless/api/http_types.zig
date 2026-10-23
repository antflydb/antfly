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

const std = @import("std");
const cancellation_mod = @import("antfly_cancellation");
const http_routes = @import("http_routes.zig");

const Allocator = std.mem.Allocator;

/// Request-scoped cancellation bridge for engines accepting cancellation
/// tokens. Scheduled responses retain this stable guard until output retirement;
/// callers keep the borrowed cancellation/executor owners alive through teardown.
pub const RequestGuard = struct {
    // Compilation-local error identity; only this guard's callbacks read it.
    rejection: std.atomic.Value(u32) = .init(0),
    context: @import("antfly_local_sources").api_operation.RequestContext,
    pub fn token(self: *@This()) cancellation_mod.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = isCancelled, .check_fn = check };
    }
    pub fn checkpoint(self: *@This()) !void {
        const previous = self.rejection.load(.acquire);
        if (previous != 0) return @errorFromInt(@as(u16, @intCast(previous)));
        self.context.ensureExecutionActive() catch |err| {
            const code: u32 = @intFromError(err);
            const first = self.rejection.cmpxchgStrong(0, code, .acq_rel, .acquire) orelse code;
            return @errorFromInt(@as(u16, @intCast(first)));
        };
    }

    fn check(ptr: *const anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
        return self.checkpoint();
    }
    fn isCancelled(ptr: *const anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
        self.checkpoint() catch return true;
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
    /// Original native monotonic deadline; nested work may shorten it.
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
    /// Move-only operation owner, released after response buffers retire.
    retirement: ?struct { ptr: *anyopaque, release: *const fn (*anyopaque) void } = null,
    content_type: []u8,
    body: []u8,
    retry_after_seconds: ?u32 = null,

    pub fn deinit(self: *HttpResponse, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.body);
        if (self.retirement) |owner| owner.release(owner.ptr);
        self.* = undefined;
    }
};

test "serverless workload deadline retains original deadline and cancellation error" {
    const expired: HttpRequest = .{ .method = .get, .path = "/status", .deadline_ns = 0 };
    try std.testing.expectError(error.DeadlineExceeded, expired.ensureActive());
    var signal = std.atomic.Value(bool).init(true);
    const canceled: HttpRequest = .{ .method = .get, .path = "/status", .cancellation = cancellation_mod.CancellationToken.fromAtomic(&signal) };
    try std.testing.expectError(error.Canceled, canceled.ensureActive());
}

test "request context execution lifetime serverless guard preserves scoped errors without boolean recheck" {
    const Probe = struct {
        calls: usize = 0,
        reject_at: usize,
        cause: anyerror,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == self.reject_at) return self.cause;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        var probe: Probe = .{ .reject_at = 1, .cause = cause };
        var guard: RequestGuard = .{ .context = .{ .cancellation = .{ .ptr = &probe, .check_fn = Probe.check } } };
        try std.testing.expectError(cause, guard.context.platformDeadline());
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        probe = .{ .reject_at = 2, .cause = cause };
        try guard.checkpoint();
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        try std.testing.expectError(cause, guard.token().check());
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
        // The boolean-only consumer still observes rejection conservatively.
        try std.testing.expectError(cause, guard.checkpoint());
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
        probe = .{ .reject_at = 1, .cause = cause };
        var boolean_guard: RequestGuard = .{ .context = guard.context };
        try std.testing.expect(boolean_guard.token().isCancelled());
        try std.testing.expectError(cause, boolean_guard.checkpoint());
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
    }
    var expired: RequestGuard = .{ .context = .{ .deadline_ns = 0 } };
    try std.testing.expectError(error.DeadlineExceeded, expired.checkpoint());
}
