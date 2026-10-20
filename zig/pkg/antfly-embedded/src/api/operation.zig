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

//! Transport-neutral contracts shared by API-kernel operations.
//!
//! Nothing in this module depends on httpx or on the legacy HTTP request and
//! response types. HTTP, internal callers, and future compiled-boundary
//! adapters may all construct these values without manufacturing a wire
//! request.

const std = @import("std");
const platform_time = @import("antfly_platform").time;

pub const ApiError = error{
    Canceled,
    DeadlineExceeded,
    Unauthorized,
    Forbidden,
    NotFound,
    InvalidArgument,
    Conflict,
    Unsupported,
    CapacityExhausted,
    Unavailable,
    Internal,
};

/// Borrowed cancellation source. The owner must keep `ptr` alive for the
/// complete operation call. A callback keeps the kernel independent of the
/// transport's concrete cancellation representation.
pub const CancellationToken = @import("antfly_cancellation").CancellationToken;

/// Authenticated identity projected into an operation. Permissions stay in
/// the authorization policy layer; operations receive only the identity they
/// need for ownership, auditing, and row-policy semantics.
pub const Principal = struct {
    kind: Kind,
    subject: []const u8,

    pub const Kind = enum {
        user,
        service,
        internal,
    };
};

/// An admission permit remains owned by the ingress adapter. The request
/// context borrows it so downstream code can preserve the reservation across
/// nested operations without releasing it accidentally.
pub const AdmissionReservation = struct {
    ptr: *anyopaque,
    release_fn: *const fn (*anyopaque) void,
    released: bool = false,

    pub fn release(self: *AdmissionReservation) void {
        if (self.released) return;
        self.released = true;
        self.release_fn(self.ptr);
    }
};

/// Bounded, request-owned diagnostic context. Operations may populate this
/// while unwinding an error so ingress adapters can return actionable details
/// without thread-local state or heap allocation.
pub const GraphMetricRejectionDiagnostic = struct {
    graph_index_name: [256]u8 = undefined,
    graph_index_name_len: u16 = 0,
    metric_name: [128]u8 = undefined,
    metric_name_len: u8 = 0,
    materializer_fingerprint: u64 = 0,

    pub fn graphIndexName(self: *const GraphMetricRejectionDiagnostic) []const u8 {
        return self.graph_index_name[0..self.graph_index_name_len];
    }

    pub fn metricName(self: *const GraphMetricRejectionDiagnostic) []const u8 {
        return self.metric_name[0..self.metric_name_len];
    }
};

pub const RequestDiagnostics = struct {
    graph_metric_rejection: ?GraphMetricRejectionDiagnostic = null,

    pub fn recordGraphMetricRejection(
        self: *RequestDiagnostics,
        graph_index_name: []const u8,
        metric_name: []const u8,
        materializer_fingerprint: u64,
    ) void {
        var diagnostic = GraphMetricRejectionDiagnostic{
            .materializer_fingerprint = materializer_fingerprint,
        };
        const graph_len = @min(graph_index_name.len, diagnostic.graph_index_name.len);
        @memcpy(diagnostic.graph_index_name[0..graph_len], graph_index_name[0..graph_len]);
        diagnostic.graph_index_name_len = @intCast(graph_len);
        const metric_len = @min(metric_name.len, diagnostic.metric_name.len);
        @memcpy(diagnostic.metric_name[0..metric_len], metric_name[0..metric_len]);
        diagnostic.metric_name_len = @intCast(metric_len);
        self.graph_metric_rejection = diagnostic;
    }
};

pub const RequestContext = struct {
    /// Granted only after administrator authentication by a trusted native
    /// ingress. Metadata HTTP never reconstructs this from request JSON.
    setting_admin: bool = false,
    /// Exact authenticated SQL credential principal; distinct from username
    /// for API keys and derived only by trusted API/pgwire ingress.
    setting_read_principal: ?[]const u8 = null,
    /// Granted only by the authenticated internal metadata transport after
    /// verifying a body-bound policy-install read grant. Never infer it from
    /// a serialized catalog request or forwarded user identity.
    row_policy_install_authority: bool = false,
    /// Internal body-bound authorization for fenced FK schema publication.
    /// Never accepted from public SQL or a client-provided catalog payload.
    fk_generation_publication_authority: bool = false,
    /// Set only by the administrator-authorized relational recovery routes.
    relational_recovery: enum { none, repair, retry, retire } = .none,
    relational_retirement_target: ?[]const u8 = null,
    relational_retirement_drop: bool = false,
    cancellation: CancellationToken = .none,
    /// Absolute monotonic deadline. This deliberately does not use a wall
    /// clock or a transport timeout duration.
    deadline_ns: ?u64 = null,
    deadline_io: ?@import("antfly_runtime_abi").io_abi.Borrow = null,
    /// Execution capability for bounded request fanout. Independent of the
    /// deadline clock: native-monotonic HTTP budgets can still schedule Io.
    fanout_io: ?@import("antfly_runtime_abi").io_abi.Borrow = null,
    /// Trusted request-lifetime execution team. Local callbacks may borrow it
    /// only until their children join. Never reconstruct this from wire data.
    execution_team: ?*@import("antfly_runtime_abi").runnable_abi.Team = null,
    /// Trusted local retained-parent dispatch. Borrow only through physical joins.
    execution_remote_query: ?@import("../common/http/http_common.zig").RequestExecutor = null,
    /// Borrowed request identity used for correlation. An empty value means
    /// the caller did not supply one; adapters may generate one in middleware.
    request_id: []const u8 = "",
    principal: ?Principal = null,
    /// Request-lifetime capability for writes discovered after initial route
    /// admission (for example FK cascades). Adapters retain the admitted
    /// credential scopes; a username alone cannot reconstruct API-key rights.
    table_write_authorization: ?TableWriteAuthorization = null,
    /// Admitted credential scopes for source tables discovered by read fanout.
    /// Authorize public logical names after catalog reverse binding.
    table_read_authorization: ?TableReadAuthorization = null,
    /// Borrowed authenticated ingress credential, never serialized or
    /// reconstructed from `principal`. Only ApiHttpServer may interpret this
    /// opaque capability when minting a table-scoped RLS write proof.
    row_policy_credential: ?*const anyopaque = null,
    admission: ?*AdmissionReservation = null,
    /// Borrowed for the duration of the operation. This is deliberately
    /// request-scoped: std.Io tasks may resume on a different worker thread.
    diagnostics: ?*RequestDiagnostics = null,
    /// Durable hash of an externally sourced table definition that was
    /// authorized before asynchronous restore admission.
    destination_authorization_fingerprint: []const u8 = "",
    /// Credential identity that authorized durable background destinations.
    /// Workers re-resolve this principal against the live user/key store.
    destination_authorization_principal: []const u8 = "",
    /// Borrowed, authenticated internal route capability. HTTP adapters keep
    /// the encoded form borrowed and transport-neutral operations decode it
    /// only when a group-local read is actually dispatched.
    catalog_route_fence_json: []const u8 = "",

    pub fn ensureActive(self: RequestContext) ApiError!void {
        if (self.cancellation.isCancelled()) return error.Canceled;
        if (self.deadline_ns) |deadline| {
            const now_ns = if (self.deadline_io) |borrow| blk: {
                var receiver = borrow.receive() catch return error.Internal;
                break :blk @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, receiver.io()).nanoseconds)));
            } else platform_time.monotonicNs();
            if (now_ns >= deadline) return error.DeadlineExceeded;
        }
    }

    /// Check a supplied execution owner's retained lifetime without yielding or
    /// acquiring CPU. A missing callback cannot silently erase that authority.
    pub fn ensureExecutionActive(self: RequestContext) !void {
        var checked = self;
        if (self.cancellation.check_fn != null) {
            try self.cancellation.check();
            // One authoritative scoped check. The legacy boolean adapter
            // must not recheck or collapse a one-shot original rejection.
            checked.cancellation = .none;
        }
        if (self.execution_team) |team| {
            if (checked.cancellation.isCancelled()) return error.Canceled;
            try team.borrow.checkLifetime();
        }
        try checked.ensureActive();
    }

    /// Remaining request budget for transports with relative millisecond timeouts.
    /// Round up so a live sub-millisecond budget never becomes an unbounded zero.
    pub fn remainingTimeoutMs(self: RequestContext) !?u32 {
        const normalized = try self.platformDeadline();
        const deadline = normalized.deadline_ns orelse return null;
        const now = platform_time.monotonicNs();
        if (now >= deadline) return error.DeadlineExceeded;
        const millis = std.math.divCeil(u64, deadline - now, std.time.ns_per_ms) catch unreachable;
        return @intCast(@min(millis, std.math.maxInt(u32)));
    }

    /// Preserve remaining time when entering native owners or HTTP timeout
    /// APIs, whose absolute clock is platform monotonic rather than Io awake.
    pub fn platformDeadline(self: RequestContext) !RequestContext {
        try self.ensureExecutionActive();
        const deadline = self.deadline_ns orelse return self;
        const borrow = self.deadline_io orelse return self;
        const platform_now = platform_time.monotonicNs();
        var receiver = try borrow.receive();
        const source_now: u64 = @intCast(@max(0, std.Io.Clock.now(.awake, receiver.io()).nanoseconds));
        if (source_now >= deadline) return error.DeadlineExceeded;
        var normalized = self;
        normalized.deadline_ns = platform_now +| (deadline - source_now);
        normalized.deadline_io = null;
        return normalized;
    }
};

pub const TableReadAuthorization = TableWriteAuthorization;

pub const TableWriteAuthorization = struct {
    ptr: *const anyopaque,
    allows: *const fn (*const anyopaque, []const u8) bool,
};

/// A synchronous sink applies backpressure by not returning from `writeAll`
/// until the bytes have been accepted or the operation has failed. The sink
/// is transport-owned and borrowed for the duration of `produce`.
pub const StreamSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        write_all: *const fn (*anyopaque, []const u8) anyerror!void,
        flush: *const fn (*anyopaque) anyerror!void,
        close: *const fn (*anyopaque) anyerror!void,
    };

    pub fn writeAll(self: StreamSink, bytes: []const u8) !void {
        try self.vtable.write_all(self.ptr, bytes);
    }

    pub fn flush(self: StreamSink) !void {
        try self.vtable.flush(self.ptr);
    }

    pub fn close(self: StreamSink) !void {
        try self.vtable.close(self.ptr);
    }
};

pub const StreamProducer = struct {
    ptr: *anyopaque,
    produce_fn: *const fn (*anyopaque, RequestContext, StreamSink) anyerror!void,

    pub fn produce(self: StreamProducer, request: RequestContext, sink: StreamSink) !void {
        try request.ensureActive();
        try self.produce_fn(self.ptr, request, sink);
    }
};

pub const JsonResult = struct {
    bytes: []const u8,
};

pub const BytesResult = struct {
    bytes: []const u8,
    content_type: []const u8 = "application/octet-stream",
};

pub const StatusResult = struct {
    status: u16 = 204,
};

pub const OperationResult = union(enum) {
    json: JsonResult,
    bytes: BytesResult,
    stream: StreamProducer,
    empty: StatusResult,
};

test "request context observes cancellation before deadline" {
    var signal = std.atomic.Value(bool).init(true);
    const request = RequestContext{
        .cancellation = CancellationToken.fromAtomic(&signal),
        .deadline_ns = 0,
    };
    try std.testing.expectError(error.Canceled, request.ensureActive());
}

test "admission reservation releases exactly once" {
    const Counter = struct {
        count: usize = 0,

        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
        }
    };
    var counter = Counter{};
    var reservation = AdmissionReservation{
        .ptr = &counter,
        .release_fn = Counter.release,
    };
    reservation.release();
    reservation.release();
    try std.testing.expectEqual(@as(usize, 1), counter.count);
}

test "serverless request diagnostics remain isolated across interleaved operations" {
    var first = RequestDiagnostics{};
    var second = RequestDiagnostics{};

    first.recordGraphMetricRejection("graph-a", "pagerank", 11);
    second.recordGraphMetricRejection("graph-b", "degree", 22);

    const first_rejection = first.graph_metric_rejection.?;
    const second_rejection = second.graph_metric_rejection.?;
    try std.testing.expectEqualStrings("graph-a", first_rejection.graphIndexName());
    try std.testing.expectEqualStrings("pagerank", first_rejection.metricName());
    try std.testing.expectEqual(@as(u64, 11), first_rejection.materializer_fingerprint);
    try std.testing.expectEqualStrings("graph-b", second_rejection.graphIndexName());
    try std.testing.expectEqualStrings("degree", second_rejection.metricName());
    try std.testing.expectEqual(@as(u64, 22), second_rejection.materializer_fingerprint);
}

test "request context execution lifetime ingress preserves authority and never acquires CPU" {
    const runtime = @import("antfly_runtime_abi");
    const Scope = struct {
        checks: usize = 0,
        quantum: usize = 0,
        phases: usize = 0,
        failure: ?anyerror = null,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.failure) |cause| runtime.error_abi.statusFromError(cause) else .ok;
        }
        fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.quantum += 1;
            return .ok;
        }
        fn phase(raw: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.phases += 1;
            return .ok;
        }
    };
    var scope: Scope = .{};
    var team: runtime.runnable_abi.Team = .{ .borrow = .{ .context = &scope, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.query), .checkpoint = Scope.checkpoint, .park = Scope.phase, .enter = Scope.phase, .check_lifetime = Scope.lifetime }, .maximum_slots = 1, .allocator = std.testing.allocator };
    const request: RequestContext = .{ .execution_team = &team, .request_id = "original" };
    const normalized = try request.platformDeadline();
    try std.testing.expect(normalized.execution_team == &team);
    try std.testing.expectEqualStrings("original", normalized.request_id);
    try std.testing.expectEqual(@as(usize, 1), scope.checks);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        scope.failure = cause;
        try std.testing.expectError(cause, request.platformDeadline());
    }
    scope.failure = null;
    const before = scope.checks;
    var signal = std.atomic.Value(bool).init(true);
    var canceled = request;
    canceled.cancellation = .fromAtomic(&signal);
    canceled.deadline_ns = 0;
    try std.testing.expectError(error.Canceled, canceled.platformDeadline());
    try std.testing.expectEqual(before, scope.checks);
    team.borrow.check_lifetime = null;
    try std.testing.expectError(error.UnsupportedOperation, request.platformDeadline());
    team.borrow.check_lifetime = Scope.lifetime;
    team.borrow.version +%= 1;
    try std.testing.expectError(error.UnsupportedVersion, request.platformDeadline());
    try std.testing.expectEqual(@as(usize, 0), scope.quantum);
    try std.testing.expectEqual(@as(usize, 0), scope.phases);
}

test "request context execution lifetime observes real parked and retired scheduler owners" {
    const runtime = @import("antfly_runtime_abi");
    const scheduling = @import("../common/runnable_scheduler.zig");
    const Clock = struct {
        var now_ns: u64 = 10;
        fn now() u64 {
            return now_ns;
        }
    };
    Clock.now_ns = 10;
    var signal = std.atomic.Value(bool).init(false);
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 3, .max_operations = 4, .max_bytes = 128, .control_bytes = 32 });
    defer scheduler.close();
    var execution: scheduling.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 17, .cancellation = .{ .ptr = &signal, .requested = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    }.requested } }, .query);
    const Bridge = struct {
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const owner: *scheduling.Execution = @ptrCast(@alignCast(raw));
            owner.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var team: runtime.runnable_abi.Team = .{ .borrow = .{ .context = &execution, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.query), .checkpoint = Bridge.checkpoint, .park = Bridge.phase, .enter = Bridge.phase, .check_lifetime = Bridge.lifetime }, .maximum_slots = 1, .allocator = std.testing.allocator };
    const request: RequestContext = .{ .execution_team = &team };
    _ = try request.platformDeadline();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try execution.park(.resource_blocked);
    _ = try request.platformDeadline();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
    Clock.now_ns = 100;
    try std.testing.expectError(error.DeadlineExceeded, request.platformDeadline());
    signal.store(true, .release);
    try std.testing.expectError(error.Canceled, request.platformDeadline());
    signal.store(false, .release);
    Clock.now_ns = 10;
    scheduler.close();
    try std.testing.expectError(error.SchedulerClosed, request.platformDeadline());
    execution.finish();
    try std.testing.expectError(error.InvalidRunnableTransition, request.platformDeadline());
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "request context execution lifetime preserves activated completion after foreground expiry and close" {
    const runtime = @import("antfly_runtime_abi");
    const scheduling = @import("../common/runnable_scheduler.zig");
    const Clock = struct {
        var now_ns: u64 = 10;
        fn now() u64 {
            return now_ns;
        }
    };
    Clock.now_ns = 10;
    var signal = std.atomic.Value(bool).init(false);
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 3, .control_slots = 1, .max_operations = 4, .max_bytes = 128, .control_bytes = 32 });
    defer scheduler.close();
    var execution: scheduling.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 50, .retained_bytes = 17, .cancellation = .{ .ptr = &signal, .requested = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    }.requested } }, .write);
    try execution.reserveCompletion(.{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 200, .retained_bytes = 17 });
    try execution.activateCompletion();
    const Bridge = struct {
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const owner: *scheduling.Execution = @ptrCast(@alignCast(raw));
            owner.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var team: runtime.runnable_abi.Team = .{ .borrow = .{ .context = &execution, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .checkpoint = Bridge.checkpoint, .park = Bridge.phase, .enter = Bridge.phase, .check_lifetime = Bridge.lifetime }, .maximum_slots = 1, .allocator = std.testing.allocator };
    const terminal: RequestContext = .{ .execution_team = &team };
    Clock.now_ns = 60;
    signal.store(true, .release);
    scheduler.close();
    _ = try terminal.platformDeadline();
    try std.testing.expectEqual(@as(u64, 140), try execution.completionRemainingNs());
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
    Clock.now_ns = 200;
    try std.testing.expectError(error.DeadlineExceeded, terminal.platformDeadline());
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}
