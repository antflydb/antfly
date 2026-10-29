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

//! Immutable execution inputs shared by every model-family adapter.
//!
//! Model configuration describes what to run. This context describes where
//! Antfly-owned work runs and which tenant route selected it. Keeping these
//! concerns separate prevents document producers, embedders, rerankers, and
//! future task families from independently inventing localhost fallbacks.

const std = @import("std");
const httpx = @import("httpx");
const remote_capabilities = @import("antfly_inference_remote_capabilities");
const CancellationToken = @import("antfly_cancellation").CancellationToken;

pub const source_table_header = "X-Antfly-Source-Table";

pub const RoutingContext = struct {
    /// Internal, trusted table identity. Never populate this from a public
    /// model config: the inference proxy uses it only to select a provisioned
    /// route after authenticating the request.
    source_table: []const u8 = "",

    pub fn headerCount(self: RoutingContext) usize {
        return @intFromBool(self.source_table.len > 0);
    }

    pub fn appendHeaders(
        self: RoutingContext,
        storage: []([2][]const u8),
        start: usize,
    ) !usize {
        var count = start;
        if (self.source_table.len > 0) {
            if (count >= storage.len) return error.InferenceRoutingHeaderCapacityExceeded;
            storage[count] = .{ source_table_header, self.source_table };
            count += 1;
        }
        return count;
    }
};

const execution_control = @import("antfly_inference_execution_control");
pub const Phase = execution_control.Phase;
pub const Progress = execution_control.Progress;
pub const ProgressSink = execution_control.ProgressSink;
pub const RequestContext = execution_control.RequestContext;

/// Durable, task-neutral execution environment. Provider owners may retain
/// this value; each invocation derives a short-lived `RequestContext` from it.
pub const ExecutionEnvironment = struct {
    /// Default distributed inference endpoint. Explicit per-model URLs retain
    /// precedence; an available linked callback retains precedence over this
    /// default for configs that did not explicitly request a remote endpoint.
    default_endpoint: ?[]const u8 = null,
    capability_cache: ?*remote_capabilities.Cache = null,
    io: ?std.Io = null,
    routing: RoutingContext = .{},
    /// Absolute monotonic deadline for discovery and execution. Task adapters
    /// may apply a stricter family-specific ceiling, but must never extend it.
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,
    /// Optional caller-owned response ceiling. Task adapters intersect this
    /// with their own hard limit rather than treating it as permission to
    /// allocate more.
    max_response_bytes: ?usize = null,
    /// Optional provider-runtime transport. Keep new execution context fields
    /// append-only because this value crosses checked linked task boundaries.
    /// The context borrows this pointer; task adapters must not destroy it or
    /// retain it beyond their durable provider owner. Standalone invocations
    /// may create a call-scoped compatibility client when it is absent.
    http_client: ?*httpx.Client = null,
    /// Invocation-only borrowed execution. Durable provider constructors leave
    /// this null; callers may bind it on a synchronous provider view.
    runnable: @FieldType(RequestContext, "runnable") = null,

    pub fn resolveAntflyEndpoint(
        self: ExecutionEnvironment,
        explicit_endpoint: ?[]const u8,
        linked_callback_available: bool,
    ) ?[]const u8 {
        if (explicit_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        if (linked_callback_available) return null;
        if (self.default_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        return null;
    }

    pub fn waitContext(self: ExecutionEnvironment) remote_capabilities.WaitContext {
        return .{
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
        };
    }

    pub fn waitContextWithControl(self: ExecutionEnvironment, control: httpx.socket.WriteControl) remote_capabilities.WaitContext {
        var result = self.waitContext();
        result.execution_wait = control;
        return result;
    }

    pub fn requestContext(self: ExecutionEnvironment, io: std.Io) RequestContext {
        return .{
            .io = io,
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
            .runnable = self.runnable,
        };
    }

    pub fn check(self: ExecutionEnvironment, now_ns: u64) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| if (now_ns >= deadline) return error.Timeout;
    }

    pub fn boundedResponseBytes(self: ExecutionEnvironment, family_limit: usize) usize {
        return if (self.max_response_bytes) |requested|
            @min(requested, family_limit)
        else
            family_limit;
    }

    pub fn remainingTimeoutMs(
        self: ExecutionEnvironment,
        now_ns: u64,
        family_limit_ms: u64,
    ) !u64 {
        try self.check(now_ns);
        const deadline = self.deadline_ns orelse return family_limit_ms;
        const remaining_ns = deadline - now_ns;
        const rounded_ms = @max(
            @as(u64, 1),
            std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1,
        );
        return @min(rounded_ms, family_limit_ms);
    }
};

pub const InferenceExecutionContext = ExecutionEnvironment;
pub const Context = ExecutionEnvironment;

test "execution context gives explicit and linked routes precedence" {
    const context = Context{ .default_endpoint = "http://distributed" };
    try std.testing.expectEqualStrings(
        "http://explicit",
        context.resolveAntflyEndpoint("http://explicit", true).?,
    );
    try std.testing.expect(context.resolveAntflyEndpoint(null, true) == null);
    try std.testing.expectEqualStrings(
        "http://distributed",
        context.resolveAntflyEndpoint(null, false).?,
    );
    try std.testing.expect((Context{ .default_endpoint = " \t" }).resolveAntflyEndpoint(null, false) == null);
}

test "routing context appends trusted table header" {
    var storage: [2][2][]const u8 = undefined;
    storage[0] = .{ "Authorization", "Bearer token" };
    const count = try (RoutingContext{ .source_table = "docs" }).appendHeaders(&storage, 1);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqualStrings(source_table_header, storage[1][0]);
    try std.testing.expectEqualStrings("docs", storage[1][1]);
}

test "execution context preserves control and resource bounds" {
    var canceled = std.atomic.Value(bool).init(false);
    const context = Context{
        .deadline_ns = 200,
        .cancellation = CancellationToken.fromAtomic(&canceled),
        .max_response_bytes = 512,
    };
    try context.check(199);
    try std.testing.expectError(error.Timeout, context.check(200));
    try std.testing.expectEqual(@as(usize, 512), context.boundedResponseBytes(1024));
    try std.testing.expectEqual(@as(?u64, 200), context.waitContext().deadline_ns);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, context.check(0));
}

test "request context preserves absolute deadlines and cancellation" {
    var cancelled = std.atomic.Value(bool).init(false);
    const context = RequestContext{
        .io = std.testing.io,
        .deadline_ns = null,
        .cancellation = CancellationToken.fromAtomic(&cancelled),
    };
    try context.check();
    cancelled.store(true, .release);
    try std.testing.expectError(error.Cancelled, context.check());

    var expired = context;
    expired.cancellation = null;
    expired.deadline_ns = 0;
    try std.testing.expectError(error.Timeout, expired.remainingTimeoutMs());
}

/// Invocation-local HTTP control. Persistent clients retain neither this
/// adapter nor the request grant; physical waits run on the calling task.
pub const HttpWait = struct {
    const runtime = @import("antfly_runtime_abi");
    context: RequestContext,
    saved_class: ?runtime.runnable_abi.Class = null,
    failure: ?anyerror = null,
    refusal: @import("antfly_cancellation").Rejection = .{},

    pub fn ready(self: *HttpWait) !void {
        try self.context.check();
        if (self.failure) |failure| return failure;
        try self.refusal.check();
    }
    fn from(raw: ?*anyopaque) *HttpWait {
        return @ptrCast(@alignCast(raw.?));
    }
    fn check(raw: ?*anyopaque) httpx.socket.WriteControl.Error!void {
        // Watchdogs inspect lifetime only, without touching owner state.
        from(raw).refusal.check() catch return error.Canceled;
        from(raw).context.check() catch |err| return switch (err) {
            error.Timeout, error.DeadlineExceeded => error.Timeout,
            else => error.Canceled,
        };
    }
    fn remaining(raw: ?*anyopaque) httpx.socket.WriteControl.Error!?u64 {
        try check(raw);
        return from(raw).context.remainingTimeoutMs() catch |err| return switch (err) {
            error.Timeout, error.DeadlineExceeded => error.Timeout,
            else => error.Canceled,
        };
    }
    fn restriction(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.context.check() catch |err| {
            self.failure = err;
            return runtime.error_abi.statusFromError(error.Canceled);
        };
        return .ok;
    }
    fn begin(raw: ?*anyopaque) !void {
        beginOwned(raw) catch |err| return from(raw).refusal.remember(err);
    }
    fn beginOwned(raw: ?*anyopaque) !void {
        const self = from(raw);
        try self.ready();
        if (self.saved_class != null) return error.InvalidRunnableTransition;
        const borrow = self.context.runnable.?;
        if (borrow.enter_checked == null) return error.UnsupportedOperation;
        const class = try borrow.currentClass();
        const status = borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource));
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        self.saved_class = class;
    }
    fn end(raw: ?*anyopaque) !void {
        endOwned(raw) catch |err| return from(raw).refusal.remember(err);
    }
    fn endOwned(raw: ?*anyopaque) !void {
        const self = from(raw);
        const class = self.saved_class orelse return error.InvalidRunnableTransition;
        self.saved_class = null;
        self.context.runnable.?.enterChecked(class, &.{ .context = self, .check = restriction }) catch |err| {
            self.failure = self.failure orelse err;
            try self.ready();
            return err;
        };
        try self.ready();
    }
    pub fn control(self: *HttpWait) httpx.socket.WriteControl {
        return .{ .ptr = self, .check_fn = check, .remaining_ms_fn = remaining, .wait_begin_fn = if (self.context.runnable != null) begin else null, .wait_end_fn = if (self.context.runnable != null) end else null };
    }
};
