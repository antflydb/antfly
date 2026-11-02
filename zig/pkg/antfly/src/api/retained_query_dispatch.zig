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

//! Shared retained dispatch for stable native query owners. Borrow this object
//! through physical joins; publication moves a separately acquired parent ref.
const std = @import("std");
const attempts = @import("../common/remote_attempts.zig");
const http = @import("../raft/transport/http_common.zig");
const Parent = @import("antfly_local_sources").common_read_admission.Retirement.Parent;
/// All fields are owned by a stable node parent through the last outbox ref.
/// No release context or cancellation pointer may borrow the foreground.
pub const RetainedParent = struct {
    parent: Parent,
    context: *anyopaque,
    release: *const fn (*anyopaque, usize, attempts.Id) void,
    cancellation: *const std.atomic.Value(bool),
};
pub const SignedRead = struct {
    client: @import("remote_attempt_client.zig").Client,
    destination: usize,
    operation: u128,
    charged_bytes: usize,
};
var operation_sequence: std.atomic.Value(u64) = .init(1);
fn nextOperation() !u128 {
    var current = operation_sequence.load(.acquire);
    while (current != std.math.maxInt(u64)) {
        current = operation_sequence.cmpxchgWeak(current, current + 1, .acq_rel, .acquire) orelse return current;
    }
    return error.RemoteAttemptSequenceExhausted;
}

pub const Dispatcher = struct {
    client: @import("remote_attempt_client.zig").Client,
    context: *anyopaque,
    deadline_fn: *const fn (*anyopaque) anyerror!?u64,
    retain_fn: *const fn (*anyopaque, u64) anyerror!attempts.Retention,
    operation: u128,
    endpoint_ref: ?@import("remote_endpoint_view.zig").View.Ref = null,
    endpoint_binding: ?*@import("remote_endpoint_source.zig").Source.Binding = null,
    retained: ?RetainedParent = null,

    pub fn init(client: @import("remote_attempt_client.zig").Client, context: *anyopaque, deadline_fn: *const fn (*anyopaque) anyerror!?u64, retain_fn: *const fn (*anyopaque, u64) anyerror!attempts.Retention) !Dispatcher {
        const operation = try nextOperation();
        if (client.endpoint_source) |source| {
            const binding = source.bind(operation) catch |err| return if (err == error.CleanupCapacityUnavailable) error.SchedulerFull else err;
            const bound = client.withEndpointView(binding.view());
            return .{ .client = bound, .context = context, .deadline_fn = deadline_fn, .retain_fn = retain_fn, .operation = operation, .endpoint_binding = binding };
        }
        const endpoint_ref = if (client.endpoint_view) |view| try view.retain() else null;
        return .{ .client = client, .context = context, .deadline_fn = deadline_fn, .retain_fn = retain_fn, .operation = operation, .endpoint_ref = endpoint_ref };
    }
    /// Join local callbacks first. Retain endpoint leases through all physical
    /// obligations, or transfer them to an independent owner before deinit.
    pub fn deinit(self: *Dispatcher) void {
        if (self.endpoint_ref) |*ref| ref.release();
        self.endpoint_ref = null;
        if (self.endpoint_binding) |binding| binding.release();
        self.endpoint_binding = null;
    }
    pub fn executor(self: *Dispatcher) http.RequestExecutor {
        return .{ .ptr = self, .clock_io = self.client.read_executor.clock_io, .vtable = &.{ .execute = dispatchRemoteQuery } };
    }
    pub fn initRetained(client: @import("remote_attempt_client.zig").Client, context: *anyopaque, deadline_fn: *const fn (*anyopaque) anyerror!?u64, retained: RetainedParent) !Dispatcher {
        var result = try init(client, context, deadline_fn, missingParent);
        result.retained = retained;
        return result;
    }
    fn missingParent(_: *anyopaque, _: u64) !attempts.Retention {
        return error.InvalidRunnableTransition;
    }
    /// Recognize our native capability before interpreting its context. Copy
    /// only the stable node protocol; no foreground dispatcher escapes here.
    pub fn retainedReadDescriptor(executor_: http.RequestExecutor, base_uri: []const u8) !?SignedRead {
        if (executor_.vtable.execute != dispatchRemoteQuery) return null;
        const native_executor: http.RequestExecutor = .{ .ptr = executor_.ptr, .vtable = executor_.vtable };
        if (executor_.boundary_dispatch != native_executor.boundary_dispatch) return error.UnsupportedVersion;
        const self: *Dispatcher = @ptrCast(@alignCast(executor_.ptr));
        _ = try self.deadline_fn(self.context);
        var selected: ?usize = null;
        for (self.client.targets, 0..) |target, index| {
            if (!std.mem.eql(u8, std.mem.trimEnd(u8, target.base_uri, "/"), std.mem.trimEnd(u8, base_uri, "/"))) continue;
            if (selected != null) return error.InvalidRemoteAttemptTarget;
            selected = index;
        }
        const index = selected orelse return error.InvalidRemoteAttemptTarget;
        var client = try self.client.pinReadCapabilities(index, true);
        const limits = self.client.ledger.destinations[index];
        const bytes = limits.max_bytes / @max(@as(usize, 1), limits.max_attempts);
        if (bytes == 0) return error.RemoteAttemptCapacity;
        client.control_wait = null;
        return .{ .client = client, .destination = index, .operation = self.operation, .charged_bytes = bytes };
    }
    fn dispatchRemoteQuery(raw: *anyopaque, alloc: std.mem.Allocator, request: @import("../raft/transport/http_common.zig").HttpRequest) !@import("../raft/transport/http_common.zig").HttpResponse {
        const self: *Dispatcher = @ptrCast(@alignCast(raw));
        const original = try self.deadline_fn(self.context);
        const request_deadline = if (request.cancellation) |value| value.execution_deadline_ns else null;
        const deadline = if (original) |root| if (request_deadline) |child| @min(root, child) else root else request_deadline orelse return error.ExecutionWaitUnsupported;
        var client = self.client;
        var destination: ?usize = null;
        var path: []const u8 = undefined;
        const parsed = std.Uri.parse(request.uri) catch return error.InvalidRemoteAttemptTarget;
        if (request.method != .POST or parsed.query != null or parsed.fragment != null) return error.UnsupportedRemoteAttemptProtocol;
        for (client.targets, 0..) |target, index| {
            const base = std.mem.trimEnd(u8, target.base_uri, "/");
            if (request.uri.len <= base.len or !std.mem.startsWith(u8, request.uri, base) or request.uri[base.len] != '/') continue;
            if (destination != null) return error.InvalidRemoteAttemptTarget;
            destination = index;
            path = request.uri[base.len..];
        }
        const index = destination orelse return error.InvalidRemoteAttemptTarget;
        const retained_read = @import("../internal/routes.zig").isRetainedRead(path);
        if (!retained_read and !@import("../internal/routes.zig").isOwnedGroupQuery(path)) return error.UnsupportedRemoteAttemptProtocol;
        if (retained_read and @import("retained_read_rpc.zig").isControlBody(request.body)) return error.UnsupportedRemoteAttemptProtocol;
        client = try client.pinReadCapabilities(index, retained_read);
        const limits = client.ledger.destinations[index];
        // Fixed per-peer fair share; borrowing idle byte credits is separate.
        const bytes = limits.max_bytes / @max(@as(usize, 1), limits.max_attempts);
        if (bytes <= request.body.len) return error.RemoteAttemptCapacity;
        const body = if (retained_read) null else try @import("owned_group_query.zig").encode(alloc, request.body, request.header(@import("prepared_query_routing.zig").header_name), request.header(@import("../metadata/api.zig").catalog_route_fence_header));
        defer if (body) |value| alloc.free(value);
        if (body) |value| if (value.len >= bytes) return error.RemoteAttemptCapacity;
        var parent: ?attempts.Retention = if (self.retained) |retained| owned: {
            if (!retained_read) return error.UnsupportedRemoteAttemptProtocol;
            try retained.parent.acquire(bytes);
            break :owned .{ .context = retained.context, .index = bytes, .id = .{ .coordinator = 0, .generation = 0, .operation = 0, .sequence = 0 }, .release_fn = retained.release, .cancellation = retained.cancellation, .deadline_ns = deadline };
        } else try self.retain_fn(self.context, deadline);
        defer if (parent) |retained| retained.release();
        client.read_allocator = alloc;
        var outgoing = request;
        outgoing.uri = path;
        outgoing.content_type = "application/json";
        outgoing.execution_remote_query = null;
        outgoing.authorization = null;
        outgoing.source_node_id = null;
        const delivered = if (retained_read)
            try client.dispatchRetainedRead(index, self.operation, bytes, deadline, outgoing, request.header(@import("../metadata/api.zig").catalog_route_fence_header) orelse return error.CatalogRouteFenceRequired, &parent)
        else blk: {
            outgoing.body = body.?;
            outgoing.headers = &.{};
            break :blk try client.dispatchReadRetained(index, self.operation, bytes, deadline, outgoing, &parent);
        };
        switch (delivered) {
            .uncertain => return error.RemoteOutcomeUncertain,
            .response => |value| {
                var response = value.value;
                errdefer response.deinit(alloc);
                if (response.status != 200) return error.RemoteOutcomeUncertain;
                if (!retained_read) {
                    const generation = response.header(@import("antfly_local_sources").api_query_response.QueryResponse.identity_read_generation_header) orelse return error.RemoteOutcomeUncertain;
                    _ = std.fmt.parseUnsigned(u64, generation, 10) catch return error.RemoteOutcomeUncertain;
                }
                if (request.header(@import("../metadata/api.zig").catalog_route_fence_header) != null) {
                    const ack = response.header(@import("../metadata/api.zig").catalog_route_fence_ack_header) orelse return error.RemoteOutcomeUncertain;
                    if (!std.mem.eql(u8, ack, @import("../metadata/api.zig").catalog_route_fence_ack_value)) return error.RemoteOutcomeUncertain;
                }
                return response;
            },
        }
    }
};
