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

//! Borrowed bounded dispatch and reconciliation. Both executors must physically
//! join requests before returning and must not redirect authenticated requests.
//! Bootstrap owns the ledger/store, enrolled targets and separate protected
//! control allocator/executor. No per-attempt background task is created here.
const std = @import("std");
const time = @import("antfly_platform").time;
const http = @import("../raft/transport/http_common.zig");
const auth = @import("internal_service_auth.zig");
const attempts = @import("../common/remote_attempts.zig");
const wire = @import("../common/remote_attempt_wire.zig");
const replies = @import("../common/remote_attempt_reply.zig");
const Store = @import("../common/remote_fence_store.zig").Store;
const routes = @import("../internal/routes.zig");

pub const Target = struct {
    base_uri: []const u8,
    authority: replies.Authority,
    /// Set only by the trusted runtime after owned-read protocol activation.
    /// The existence of a status/fence route or a health reply is insufficient.
    owned_reads_v1: bool = false,
    available: bool = true,
    /// Retained-token ownership and gate-independent cleanup require their own
    /// rollout proof; buffered-query support alone does not grant this family.
    owned_retained_reads_v1: bool = false,
};
pub const Delivery = union(enum) {
    response: struct { id: attempts.Id, value: http.HttpResponse },
    uncertain: struct { id: attempts.Id, cause: anyerror },
};

/// One node-owned consumer retains this cursor between passes. No per-attempt
/// task, queue or allocation is needed to select work fairly.
pub const ReconcileCursor = struct { next: usize = 0 };
pub const ReconcileBudget = struct {
    max_rpc_calls: usize,
    max_scanned: usize,
    rpc_timeout_ns: u64,
    deadline_ns: u64,
    cancellation: ?*const http.RequestCancellation = null,
};
pub const ReconcileReport = struct {
    scanned: usize = 0,
    rpc_calls: usize = 0,
    fenced: usize = 0,
    terminal: usize = 0,
    retained: usize = 0,
    duplicates: usize = 0,
    failed: usize = 0,
    pending: usize = 0,
    deadline_exhausted: bool = false,
    canceled: bool = false,
    needs_generation_advance: bool = false,
    last_error: ?anyerror = null,
};

pub const Client = struct {
    ledger: *attempts.Coordinator,
    store: *Store,
    targets: []const Target,
    endpoint_view: ?*@import("remote_endpoint_view.zig").View = null,
    endpoint_source: ?*@import("remote_endpoint_source.zig").Source = null,
    signing: auth.Config,
    read_allocator: std.mem.Allocator,
    control_allocator: std.mem.Allocator,
    read_executor: http.RequestExecutor,
    control_executor: http.RequestExecutor,
    /// Admission snapshot only; signed cleanup remains available during recovery.
    reads_ready: bool = true,
    capabilities: ?*@import("remote_capability_publication.zig").Cache = null,
    pinned_family: ?struct { destination: usize, authority: replies.Authority, families: @import("../common/remote_read_capabilities.zig").Families } = null,
    now_ns: *const fn () u64 = time.monotonicNs,
    /// Borrowed from the current protected control root, not a global store flag.
    control_wait: ?@import("../common/remote_fence_store.zig").Wait = null,
    control_admission: ?*@import("remote_control_admission.zig").Gate = null,
    /// Synchronous pass-local borrow, never stored by a returned resource.
    control_permit: ?http.BorrowedControlPermit = null,
    control_rpc_timeout_ns: ?u64 = null,

    fn admitControl(self: *const Client, deadline: u64, parent: ?*const http.RequestCancellation) !?@import("remote_control_admission.zig").Gate.Lease {
        const gate = self.control_admission orelse {
            if (self.control_permit != null) return error.InvalidArgument;
            return null;
        };
        if (self.control_permit) |permit| {
            try gate.validateBorrow(permit);
            return null;
        }
        // Foreground calls keep the existing fail-fast behavior. The two
        // persistent consumers acquire before creating their execution rounds.
        return try gate.acquire(.{ .deadline_ns = deadline, .cancellation = parent, .now_ns = self.now_ns });
    }
    fn controlDeadline(self: *const Client, original: u64) u64 {
        return if (self.control_rpc_timeout_ns) |limit| @min(original, self.now_ns() +| limit) else original;
    }

    fn selectedProfile(self: *const Client, destination: usize, endpoint: Target) !@import("remote_capability_publication.zig").Cache.Profile {
        if (!self.reads_ready or !endpoint.available) return .unverified;
        if (self.pinned_family) |pinned| {
            if (destination != pinned.destination or !std.meta.eql(endpoint.authority, pinned.authority)) return error.InvalidRemoteReply;
            return .{ .verified = pinned.families };
        }
        var value: @import("../common/remote_read_capabilities.zig").Families = .{ .group_query = endpoint.owned_reads_v1, .retained_read = endpoint.owned_retained_reads_v1 };
        if (self.capabilities) |cache| switch (try cache.profile(destination, endpoint.authority)) {
            .unverified => if (!value.group_query and !value.retained_read) return .unverified,
            .verified => |published| {
                value.group_query = value.group_query or published.group_query;
                value.retained_read = value.retained_read or published.retained_read;
            },
        };
        // Without a discovery cache these flags are a trusted static profile.
        return .{ .verified = value };
    }
    fn selectedFamilies(self: *const Client, destination: usize, endpoint: Target) !@import("../common/remote_read_capabilities.zig").Families {
        return switch (try self.selectedProfile(destination, endpoint)) {
            .unverified => .{},
            .verified => |value| value,
        };
    }
    pub fn supportsRead(self: *const Client, destination: usize, retained: bool) !bool {
        const value = try self.selectedFamilies(destination, try self.target(destination));
        return if (retained) value.retained_read else value.group_query;
    }
    fn requireProfile(profile: @import("remote_capability_publication.zig").Cache.Profile, retained: bool) !@import("../common/remote_read_capabilities.zig").Families {
        const value = switch (profile) {
            .unverified => return error.RemoteReadUnavailable,
            .verified => |value| value,
        };
        if (!(if (retained) value.retained_read else value.group_query)) return error.UnsupportedRemoteAttemptProtocol;
        return value;
    }
    /// Data admission selects and validates exactly one coherent profile.
    /// The immutable copy also supplies the original cleanup authority.
    pub fn pinReadCapabilities(self: *const Client, destination: usize, retained: bool) !Client {
        const endpoint = try self.target(destination);
        const value = try requireProfile(try self.selectedProfile(destination, endpoint), retained);
        var result = self.*;
        result.pinned_family = .{ .destination = destination, .authority = endpoint.authority, .families = value };
        result.capabilities = null;
        return result;
    }
    /// Snapshot helper for inspection and existing cleanup holders. New data
    /// admission must use pinReadCapabilities to classify absent proof.
    pub fn pinCapabilities(self: *const Client, destination: usize) !Client {
        const endpoint = try self.target(destination);
        var result = self.*;
        result.pinned_family = .{ .destination = destination, .authority = endpoint.authority, .families = try self.selectedFamilies(destination, endpoint) };
        result.capabilities = null;
        return result;
    }
    pub fn withEndpointView(self: *const Client, view: *@import("remote_endpoint_view.zig").View) Client {
        var result = self.*;
        result.targets = view.targets;
        result.endpoint_view = view;
        result.capabilities = view.capabilities;
        return result;
    }
    fn remaining(self: *const Client, deadline_ns: u64) !u64 {
        const now = self.now_ns();
        if (now >= deadline_ns) return error.DeadlineExceeded;
        return deadline_ns - now;
    }
    fn target(self: *const Client, index: usize) !Target {
        if (index >= self.targets.len) return error.InvalidRemoteAttempt;
        const value = self.targets[index];
        const state = self.ledger.generationState();
        try self.ledger.validateEvidenceDestination(index, .{ .coordinator = state.coordinator, .generation = state.generation, .operation = 0, .sequence = 0 }, value.authority.local.root_incarnation);
        return value;
    }
    fn uri(allocator: std.mem.Allocator, base: []const u8, path: []const u8) ![]u8 {
        const parsed = std.Uri.parse(base) catch return error.InvalidRemoteAttemptTarget;
        if ((!std.mem.eql(u8, parsed.scheme, "http") and !std.mem.eql(u8, parsed.scheme, "https")) or parsed.host == null or parsed.user != null or parsed.password != null or parsed.query != null or parsed.fragment != null) return error.InvalidRemoteAttemptTarget;
        var raw_path: [2]u8 = undefined;
        const base_path = parsed.path.toRaw(&raw_path) catch return error.InvalidRemoteAttemptTarget;
        if (base_path.len != 0 and !std.mem.eql(u8, base_path, "/")) return error.InvalidRemoteAttemptTarget;
        if (!std.mem.startsWith(u8, path, "/internal/v1/") or std.mem.indexOfAny(u8, path, "#\r\n") != null) return error.InvalidRemoteAttemptTarget;
        return std.fmt.allocPrint(allocator, "{s}{s}", .{ std.mem.trimEnd(u8, base, "/"), path });
    }
    const Prepared = struct {
        allocator: std.mem.Allocator,
        url: []u8,
        token: []u8,
        headers: []http.RequestHeader,
        request: http.HttpRequest,
        /// Durable publication may have consumed most of the original budget.
        /// Replace the credential only after all replacement work succeeds.
        fn refreshContext(self: *Prepared, client: *const Client, executor: http.RequestExecutor, context: wire.Context, deadline_ns: u64) !void {
            const subject = context.subject();
            var config = client.signing;
            config.subject = &subject;
            config.remote_attempt = context;
            const realtime_ns = executor.realtimeNs() orelse time.realtimeNs();
            const token = try auth.tokenAlloc(self.allocator, config, @intCast(@divFloor(realtime_ns, std.time.ns_per_s)));
            errdefer self.allocator.free(token);
            const remaining_ns = try client.remaining(deadline_ns);
            const timeout_ms: u32 = @intCast(@min(std.math.maxInt(u32), @max(1, remaining_ns / std.time.ns_per_ms)));
            for (self.headers[0..self.request.headers.len]) |*header| if (std.ascii.eqlIgnoreCase(header.name, auth.header_name)) {
                header.value = token;
            };
            self.allocator.free(self.token);
            self.token = token;
            self.request.timeout_ms = @min(self.request.timeout_ms orelse timeout_ms, timeout_ms);
        }
        fn deinit(self: *Prepared) void {
            self.allocator.free(self.headers);
            self.allocator.free(self.token);
            self.allocator.free(self.url);
        }
    };
    fn prepare(self: *const Client, allocator: std.mem.Allocator, executor: http.RequestExecutor, endpoint: Target, request: http.HttpRequest, context: wire.Context, deadline_ns: u64) !Prepared {
        try auth.validateRuntimeConfig(self.signing.secret, null, self.signing.issuer);
        const url = try uri(allocator, endpoint.base_uri, request.uri);
        errdefer allocator.free(url);
        const subject = context.subject();
        var config = self.signing;
        config.subject = &subject;
        config.remote_attempt = context;
        const realtime_ns = executor.realtimeNs() orelse time.realtimeNs();
        const token = try auth.tokenAlloc(allocator, config, @intCast(@divFloor(realtime_ns, std.time.ns_per_s)));
        errdefer allocator.free(token);
        const headers = try allocator.alloc(http.RequestHeader, request.headers.len + 1);
        errdefer allocator.free(headers);
        var count: usize = 0;
        for (request.headers) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, auth.header_name)) continue;
            headers[count] = header;
            count += 1;
        }
        headers[count] = .{ .name = auth.header_name, .value = token };
        count += 1;
        var bounded = request;
        bounded.uri = url;
        bounded.headers = headers[0..count];
        const duration = try self.remaining(deadline_ns);
        const timeout_ms: u32 = @intCast(@min(std.math.maxInt(u32), @max(1, duration / std.time.ns_per_ms)));
        bounded.timeout_ms = @min(request.timeout_ms orelse timeout_ms, timeout_ms);
        return .{ .allocator = allocator, .url = url, .token = token, .headers = headers, .request = bounded };
    }

    /// Errors mean no dispatch was published. After prepareDispatch succeeds,
    /// every outcome carries its ID and retains uncertainty, including HTTP
    /// errors and successful response destruction. Only signed proof retires it.
    pub fn dispatchRead(self: *const Client, destination: usize, operation: u128, charged_bytes: usize, deadline_ns: u64, request: http.HttpRequest) !Delivery {
        var parent: ?attempts.Retention = null;
        return self.dispatchReadRetained(destination, operation, charged_bytes, deadline_ns, request, &parent);
    }
    /// Success transfers a physical parent reference into the bounded outbox.
    /// Pre-publication errors either leave it with the caller or retire it via
    /// the unpublished reservation; inspect the cleared optional before cleanup.
    pub fn dispatchReadRetained(self: *const Client, destination: usize, operation: u128, charged_bytes: usize, deadline_ns: u64, request: http.HttpRequest, retained_parent: *?attempts.Retention) !Delivery {
        if (routes.isRetainedRead(request.uri)) return error.UnsupportedRemoteAttemptProtocol;
        return self.dispatchReadProtocol(destination, operation, charged_bytes, deadline_ns, request, retained_parent, false);
    }

    /// Waiting exists only before reservation/publication. The queue is
    /// bounded, destination FIFO, and retains no transport attempt or worker.
    fn reserveRead(self: *const Client, destination: usize, operation: u128, bytes: usize, deadline: u64, request: http.HttpRequest, parent: ?attempts.Retention) !attempts.Id {
        return self.ledger.reserve(destination, operation, bytes) catch |err| {
            if (err != error.RemoteAttemptCapacity) return err;
            const wait = request.execution_wait orelse return err;
            const clock = self.read_executor.clock_io orelse return error.ExecutionWaitUnsupported;
            var receiver = try clock.receive();
            var queued: attempts.Coordinator.Waiter = .{};
            try self.ledger.beginWait(&queued, destination, operation, bytes);
            defer self.ledger.endWait(&queued);
            while (true) {
                _ = try self.remaining(deadline);
                if (parent) |owned| try owned.check(self.now_ns());
                if (request.cancellation) |signal| if (signal.isCancelled()) return error.Canceled;
                if (try self.ledger.reserveWaiting(&queued)) |id| return id;
                // The original boundary rechecks authority after resumption.
                // Restricted fence-capture ports have no wait boundary.
                try wait.run(std.Io.sleep, .{ receiver.io(), std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake });
            }
        };
    }

    fn dispatchReadProtocol(self: *const Client, destination: usize, operation: u128, charged_bytes: usize, deadline_ns: u64, request: http.HttpRequest, retained_parent: *?attempts.Retention, retained_read: bool) !Delivery {
        const endpoint = try self.target(destination);
        _ = try requireProfile(try self.selectedProfile(destination, endpoint), retained_read);
        var effective_deadline = deadline_ns;
        if (retained_parent.*) |retained| if (retained.deadline_ns) |original| {
            effective_deadline = @min(original, effective_deadline);
        };
        if (request.cancellation) |parent| {
            if (parent.isCancelled()) return error.Canceled;
            if (parent.execution_deadline_ns) |original| effective_deadline = @min(original, effective_deadline);
        }
        _ = try self.remaining(effective_deadline);
        const id = try self.reserveRead(destination, operation, charged_bytes, effective_deadline, request, retained_parent.*);
        var published = false;
        errdefer if (!published) self.ledger.abortUnpublished(id) catch {};
        if (retained_parent.* != null) try self.ledger.attachParent(id, retained_parent);
        const path = try routes.ownedReadPath(self.read_allocator, request.uri, endpoint.authority.local.root_incarnation, endpoint.authority.local.reporter_incarnation);
        defer self.read_allocator.free(path);
        var scoped_request = request;
        scoped_request.uri = path;
        var context: wire.Context = .{ .kind = .read, .id = id, .worker_incarnation = endpoint.authority.local.root_incarnation, .remaining_ns = try self.remaining(effective_deadline), .charged_bytes = std.math.cast(u64, charged_bytes) orelse return error.InvalidRemoteAttempt, .request_digest = wire.requestDigest(@tagName(request.method), path, request.body) };
        var prepared = try self.prepare(self.read_allocator, self.read_executor, endpoint, scoped_request, context, effective_deadline);
        defer prepared.deinit();
        _ = try self.remaining(effective_deadline);
        if (request.cancellation) |parent| if (parent.isCancelled()) return error.Canceled;
        try self.ledger.checkParent(id, self.now_ns());
        try self.store.withWait(request.execution_wait).prepareDispatch(self.ledger, id);
        published = true;
        defer self.ledger.transportLost(id);
        context.remaining_ns = self.remaining(effective_deadline) catch |err| return .{ .uncertain = .{ .id = id, .cause = err } };
        self.ledger.checkParent(id, self.now_ns()) catch |err| return .{ .uncertain = .{ .id = id, .cause = err } };
        if (request.cancellation) |parent| if (parent.isCancelled()) return .{ .uncertain = .{ .id = id, .cause = error.Canceled } };
        prepared.refreshContext(self, self.read_executor, context, effective_deadline) catch |err| return .{ .uncertain = .{ .id = id, .cause = err } };
        var cancellation: http.RequestCancellation = .{ .execution_deadline_ns = effective_deadline };
        if (request.cancellation) |parent| {
            cancellation.borrowed_context = parent;
            cancellation.borrowed_is_cancelled = struct {
                fn check(raw: *const anyopaque) bool {
                    return @as(*const http.RequestCancellation, @ptrCast(@alignCast(raw))).isCancelled();
                }
            }.check;
            if (parent.execution_deadline_ns) |original| cancellation.execution_deadline_ns = @min(original, effective_deadline);
        }
        prepared.request.cancellation = &cancellation;
        prepared.request.max_response_bytes = @min(request.max_response_bytes orelse charged_bytes, charged_bytes);
        const response = self.read_executor.execute(self.read_allocator, prepared.request) catch |err| return .{ .uncertain = .{ .id = id, .cause = err } };
        return .{ .response = .{ .id = id, .value = response } };
    }

    fn retainedRequest(request: http.HttpRequest) !void {
        if (request.method != .POST or !routes.isRetainedRead(request.uri) or
            std.mem.indexOfAny(u8, request.uri, "?#\r\n") != null or
            request.header(@import("prepared_query_routing.zig").header_name) != null) return error.UnsupportedRemoteAttemptProtocol;
    }

    /// Reserve and durably publish the exact signed creation/page attempt before
    /// transport. Every returned Delivery keeps its ID and owned parent in the
    /// outbox until signed status/fence proof; a token or HTTP ACK cannot retire it.
    pub fn dispatchRetainedRead(self: *const Client, destination: usize, operation: u128, charged_bytes: usize, deadline_ns: u64, request: http.HttpRequest, route_fence: []const u8, parent: *?attempts.Retention) !Delivery {
        try retainedRequest(request);
        if (@import("retained_read_rpc.zig").isControlBody(request.body)) return error.UnsupportedRemoteAttemptProtocol;
        if (parent.* == null) return error.InvalidRunnableTransition;
        const admitted = try self.pinReadCapabilities(destination, true);
        const body = try @import("owned_retained_read.zig").encode(self.read_allocator, request.body, route_fence);
        defer self.read_allocator.free(body);
        if (body.len >= charged_bytes) return error.RemoteAttemptCapacity;
        var outgoing = request;
        outgoing.body = body;
        outgoing.headers = &.{};
        outgoing.authorization = null;
        outgoing.source_node_id = null;
        outgoing.execution_remote_query = null;
        outgoing.content_type = "application/json";
        return admitted.dispatchReadProtocol(destination, operation, charged_bytes, deadline_ns, outgoing, parent, true);
    }

    /// Exact idempotent cleanup creates no capability and must not enter the
    /// data gate whose fence is waiting on that capability. Use the independent
    /// control allocator/executor and a fresh bounded cleanup deadline. This
    /// response never retires the original outbox entry: reconcile signed proof.
    pub fn cleanupRetainedRead(self: *const Client, destination: usize, id: attempts.Id, deadline_ns: u64, request: http.HttpRequest, route_fence: []const u8) !http.HttpResponse {
        try retainedRequest(request);
        if (!@import("retained_read_rpc.zig").isControlBody(request.body)) return error.UnsupportedRemoteAttemptProtocol;
        var control_endpoint = if (self.endpoint_source) |source| try source.controlFor(id.operation, destination) else null;
        defer if (control_endpoint) |*selected| selected.release();
        const bound = if (control_endpoint) |*selected| self.withEndpointView(selected.original.view()) else self.*;
        const endpoint = if (control_endpoint) |*selected| selected.endpoint() else try self.target(destination);
        if (!try bound.supportsRead(destination, true)) return error.UnsupportedRemoteAttemptProtocol;
        if (id.coordinator != self.ledger.generationState().coordinator) return error.InvalidRemoteAttempt;
        var effective_deadline = deadline_ns;
        if (request.cancellation) |original| {
            if (original.isCancelled()) return error.Canceled;
            if (original.execution_deadline_ns) |earlier| effective_deadline = @min(earlier, deadline_ns);
        }
        var permit = try self.admitControl(effective_deadline, request.cancellation);
        defer if (permit) |*held| held.release();
        effective_deadline = self.controlDeadline(effective_deadline);
        const body = try @import("owned_retained_read.zig").encode(self.control_allocator, request.body, route_fence);
        defer self.control_allocator.free(body);
        if (!@import("owned_retained_read.zig").isControlBody(body)) return error.UnsupportedRemoteAttemptProtocol;
        const path = try routes.ownedReadPath(self.control_allocator, request.uri, endpoint.authority.local.root_incarnation, endpoint.authority.local.reporter_incarnation);
        defer self.control_allocator.free(path);
        const context: wire.Context = .{ .kind = .read, .id = id, .worker_incarnation = endpoint.authority.local.root_incarnation, .remaining_ns = try self.remaining(effective_deadline), .charged_bytes = 64 * 1024, .request_digest = wire.requestDigest("POST", path, body) };
        var outgoing = request;
        outgoing.uri = path;
        outgoing.body = body;
        outgoing.headers = &.{};
        outgoing.authorization = null;
        outgoing.source_node_id = null;
        outgoing.execution_remote_query = null;
        outgoing.content_type = "application/json";
        outgoing.max_response_bytes = @min(request.max_response_bytes orelse 4096, 4096);
        outgoing.execution_wait = request.execution_wait orelse self.control_wait;
        var prepared = try self.prepare(self.control_allocator, self.control_executor, endpoint, outgoing, context, effective_deadline);
        defer prepared.deinit();
        var cancellation: http.RequestCancellation = .{ .execution_deadline_ns = effective_deadline };
        if (request.cancellation) |original| {
            cancellation.borrowed_context = original;
            cancellation.borrowed_is_cancelled = struct {
                fn check(raw: *const anyopaque) bool {
                    return @as(*const http.RequestCancellation, @ptrCast(@alignCast(raw))).isCancelled();
                }
            }.check;
        }
        prepared.request.cancellation = &cancellation;
        _ = try self.remaining(effective_deadline);
        if (request.cancellation) |original| if (original.isCancelled()) return error.Canceled;
        return self.control_executor.execute(self.control_allocator, prepared.request);
    }

    /// Discovery is independent control work. It reserves no data attempt,
    /// advances no generation and never updates target activation flags.
    pub fn probeCapabilities(self: *const Client, destination: usize, deadline_ns: u64, parent: ?*const http.RequestCancellation) !@import("../common/remote_read_capabilities.zig").Verified {
        var delivery: http.RequestDeliveryTracker = .{};
        return self.probeCapabilitiesTracked(destination, deadline_ns, parent, &delivery);
    }

    /// The executor must explicitly prove a local refusal before callers may
    /// distinguish it from an unknown or possibly delivered probe failure.
    pub fn probeCapabilitiesTracked(self: *const Client, destination: usize, deadline_ns: u64, parent: ?*const http.RequestCancellation, delivery: *http.RequestDeliveryTracker) !@import("../common/remote_read_capabilities.zig").Verified {
        delivery.markUnknown();
        const capabilities = @import("../common/remote_read_capabilities.zig");
        var deadline = deadline_ns;
        if (parent) |original| {
            if (original.isCancelled()) return error.Canceled;
            if (original.execution_deadline_ns) |limit| deadline = @min(deadline, limit);
        }
        _ = try self.remaining(deadline);
        const endpoint = try self.target(destination);
        if (!endpoint.available) return error.UnsupportedRemoteAttemptProtocol;
        delivery.markNotSent();
        var permit = try self.admitControl(deadline, parent);
        defer if (permit) |*held| held.release();
        deadline = self.controlDeadline(deadline);
        var receiver = try (self.control_executor.clock_io orelse return error.ExecutionWaitUnsupported).receive();
        var nonce: [32]u8 = undefined;
        if (self.control_wait) |wait| try wait.run(std.Io.randomSecure, .{ receiver.io(), &nonce }) else try receiver.io().randomSecure(&nonce);
        if (parent) |original| if (original.isCancelled()) return error.Canceled;
        const generation = self.ledger.generationState();
        const context: wire.Context = .{ .kind = .capabilities, .id = .{ .coordinator = generation.coordinator, .generation = generation.generation, .operation = 0, .sequence = 0 }, .worker_incarnation = endpoint.authority.local.root_incarnation, .remaining_ns = try self.remaining(deadline), .charged_bytes = 0, .request_digest = wire.requestDigest("POST", routes.remote_attempt_capabilities, &nonce) };
        var prepared = try self.prepare(self.control_allocator, self.control_executor, endpoint, .{ .method = .POST, .uri = routes.remote_attempt_capabilities, .body = &nonce, .content_type = "application/octet-stream", .max_response_bytes = capabilities.encoded_size }, context, deadline);
        defer prepared.deinit();
        var cancellation: http.RequestCancellation = .{ .execution_deadline_ns = deadline };
        if (parent) |original| {
            cancellation.borrowed_context = original;
            cancellation.borrowed_is_cancelled = struct {
                fn check(raw: *const anyopaque) bool {
                    return @as(*const http.RequestCancellation, @ptrCast(@alignCast(raw))).isCancelled();
                }
            }.check;
        }
        prepared.request.cancellation = &cancellation;
        prepared.request.delivery_tracker = delivery;
        prepared.request.execution_wait = self.control_wait;
        var refreshed = context;
        refreshed.remaining_ns = try self.remaining(deadline);
        try prepared.refreshContext(self, self.control_executor, refreshed, deadline);
        if (parent) |original| if (original.isCancelled()) return error.Canceled;
        _ = try self.remaining(deadline);
        var response = try self.control_executor.execute(self.control_allocator, prepared.request);
        defer response.deinit(self.control_allocator);
        // Capability observations cannot survive their bounded original scope.
        if (parent) |original| if (original.isCancelled()) return error.Canceled;
        _ = try self.remaining(deadline);
        if (response.status != 200) return error.UnsupportedRemoteAttemptProtocol;
        const reply = try capabilities.Reply.decode(response.body);
        try reply.verify(endpoint.authority, context);
        if (parent) |original| if (original.isCancelled()) return error.Canceled;
        _ = try self.remaining(deadline);
        return .{ .authority = endpoint.authority, .families = reply.families, .deadline_ns = deadline };
    }

    fn control(self: *const Client, destination: usize, id: attempts.Id, kind: wire.Kind, deadline_ns: u64, parent: ?*const http.RequestCancellation) !replies.Reconciled {
        if (self.endpoint_source) |source| {
            var control_endpoint = try source.controlFor(id.operation, destination);
            defer control_endpoint.release();
            const bound = self.withEndpointView(control_endpoint.original.view());
            return bound.controlAt(control_endpoint.endpoint(), destination, id, kind, deadline_ns, parent);
        }
        return self.controlAt(try self.target(destination), destination, id, kind, deadline_ns, parent);
    }
    fn controlAt(self: *const Client, endpoint: Target, destination: usize, id: attempts.Id, kind: wire.Kind, deadline_ns: u64, parent: ?*const http.RequestCancellation) !replies.Reconciled {
        const enrolled = try self.target(destination);
        if (!std.meta.eql(enrolled.authority, endpoint.authority)) return error.InvalidRemoteReply;
        // Reconciliation can retire the last physical root parent before its
        // own prepared buffers finish cleanup. Retain endpoint storage locally.
        var endpoint_ref = if (self.endpoint_view) |view| try view.retain() else null;
        defer if (endpoint_ref) |*ref| ref.release();
        var effective_deadline = deadline_ns;
        if (parent) |original| {
            if (original.isCancelled()) return error.Canceled;
            if (original.execution_deadline_ns) |earlier| effective_deadline = @min(earlier, deadline_ns);
        }
        const path = switch (kind) {
            .fence => routes.remote_attempt_fence,
            .status => routes.remote_attempt_status,
            .cancel => routes.remote_attempt_cancel,
            .read, .capabilities => return error.InvalidRemoteAttempt,
        };
        var permit = try self.admitControl(effective_deadline, parent);
        defer if (permit) |*held| held.release();
        effective_deadline = self.controlDeadline(effective_deadline);
        const context: wire.Context = .{ .kind = kind, .id = id, .worker_incarnation = endpoint.authority.local.root_incarnation, .remaining_ns = try self.remaining(effective_deadline), .charged_bytes = 0, .request_digest = wire.requestDigest("POST", path, "{}") };
        var prepared = try self.prepare(self.control_allocator, self.control_executor, endpoint, .{ .method = .POST, .uri = path, .body = "{}", .content_type = "application/json", .max_response_bytes = replies.encoded_size }, context, effective_deadline);
        defer prepared.deinit();
        var cancellation: http.RequestCancellation = .{ .execution_deadline_ns = effective_deadline };
        if (parent) |original| {
            cancellation.borrowed_context = original;
            cancellation.borrowed_is_cancelled = struct {
                fn check(raw: *const anyopaque) bool {
                    return @as(*const http.RequestCancellation, @ptrCast(@alignCast(raw))).isCancelled();
                }
            }.check;
        }
        prepared.request.cancellation = &cancellation;
        prepared.request.execution_wait = self.control_wait;
        _ = try self.remaining(effective_deadline);
        var response = try self.control_executor.execute(self.control_allocator, prepared.request);
        defer response.deinit(self.control_allocator);
        if (response.status == 409 and kind == .fence) return error.RemoteFencePending;
        if (response.status != 200) return error.RemoteControlUnavailable;
        const reply = try replies.Reply.decode(response.body);
        return reply.reconcileWithWait(endpoint.authority, context, self.ledger, destination, self.store, self.control_wait);
    }
    pub fn status(self: *const Client, destination: usize, id: attempts.Id, deadline_ns: u64) !replies.Reconciled {
        return self.control(destination, id, .status, deadline_ns, null);
    }

    /// Detached retirement for an exact published attempt, including creation
    /// with a lost reply and no token. No capture/page is replayed. Marking the
    /// local entry abandoned keeps its charges; only the verified control reply
    /// may retire them. The caller supplies a fresh protected cleanup lifetime,
    /// independent of the expired/canceled query that originally published it.
    pub fn retireAttempt(self: *const Client, destination: usize, id: attempts.Id, deadline_ns: u64, cleanup_cancellation: ?*const http.RequestCancellation) !replies.Reconciled {
        const endpoint = try self.target(destination);
        try self.ledger.validateEvidenceDestination(destination, id, endpoint.authority.local.root_incarnation);
        _ = try self.ledger.abandon(id);
        return self.control(destination, id, .cancel, deadline_ns, cleanup_cancellation);
    }

    pub fn fence(self: *const Client, destination: usize, deadline_ns: u64) !replies.Reconciled {
        const state = self.ledger.generationState();
        return self.control(destination, .{ .coordinator = state.coordinator, .generation = state.generation, .operation = 0, .sequence = 0 }, .fence, deadline_ns, null);
    }

    /// Enqueue cancellation in the existing bounded reconciliation ring. The
    /// caller must first stop publishing uses of this attempt (including any
    /// retained snapshot). No capacity is released by this local transition.
    pub fn abandon(self: *const Client, id: attempts.Id) !bool {
        return self.ledger.abandon(id);
    }

    pub fn advanceGeneration(self: *const Client) !void {
        try self.store.withWait(self.control_wait).advanceCoordinator(self.ledger);
    }

    fn needsGenerationAdvance(self: *const Client) !bool {
        const epoch = try self.store.withWait(self.control_wait).coordinatorEpoch();
        const state = self.ledger.generationState();
        if (epoch.coordinator == 0 or epoch.coordinator != state.coordinator) return error.InvalidRemoteGeneration;
        return state.generation_fenced or epoch.generation > state.generation or state.observed_generation > state.generation;
    }

    /// At most one visit per ring position and max_rpc_calls transports. The
    /// cursor advances before I/O, including failures. A peer failure cannot
    /// force each pass to restart at that peer. No mutex spans an RPC.
    /// Absolute deadlines bound requested waits, not physical kernel-I/O joins.
    pub fn reconcilePass(self: *const Client, cursor: *ReconcileCursor, budget: ReconcileBudget) !ReconcileReport {
        if (budget.max_rpc_calls == 0 or budget.max_scanned == 0 or budget.rpc_timeout_ns == 0) return error.InvalidReconciliationBudget;
        var report: ReconcileReport = .{};
        const width = self.ledger.reconciliationWidth();
        var deadline = budget.deadline_ns;
        if (budget.cancellation) |parent| if (parent.execution_deadline_ns) |earlier| {
            deadline = @min(earlier, deadline);
        };
        const scan_limit = @min(width, budget.max_scanned);
        while (report.scanned < scan_limit and report.rpc_calls < budget.max_rpc_calls) {
            if (budget.cancellation) |parent| if (parent.isCancelled()) {
                report.canceled = true;
                break;
            };
            const now = self.now_ns();
            if (now >= deadline) {
                report.deadline_exhausted = true;
                break;
            }
            if (try self.needsGenerationAdvance()) {
                report.needs_generation_advance = true;
                break;
            }
            const position = cursor.next % width;
            cursor.next = (position + 1) % width;
            report.scanned += 1;
            const action = self.ledger.reconciliationActionAt(position, self.now_ns()) orelse continue;
            report.rpc_calls += 1;
            const until = @min(deadline, now +| budget.rpc_timeout_ns);
            const result = self.control(action.destination, action.id, switch (action.kind) {
                .fence => .fence,
                .status => .status,
                .cancel => .cancel,
            }, until, budget.cancellation) catch |err| {
                report.last_error = err;
                if (err == error.RemoteFencePending) report.pending += 1 else report.failed += 1;
                if (try self.needsGenerationAdvance()) {
                    report.needs_generation_advance = true;
                    break;
                }
                continue;
            };
            switch (result) {
                .retained => report.retained += 1,
                .retired => report.terminal += 1,
                .duplicate => report.duplicates += 1,
                .fenced => report.fenced += 1,
            }
            if (try self.needsGenerationAdvance()) {
                report.needs_generation_advance = true;
                break;
            }
        }
        return report;
    }
};

test "remote attempt client refresh ignores unused header capacity after replacing caller credentials" {
    const a = std.testing.allocator;
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    var marker: u8 = 0;
    const executor: http.RequestExecutor = .{ .ptr = &marker, .vtable = &.{ .execute = undefined } };
    const client: Client = .{ .ledger = undefined, .store = undefined, .targets = &.{}, .signing = .{ .secret = "deadline-refresh-test-secret-32-bytes", .issuer = "cluster" }, .read_allocator = a, .control_allocator = a, .read_executor = executor, .control_executor = executor, .now_ns = Clock.now };
    const endpoint: Target = .{ .base_uri = "http://worker", .authority = undefined };
    const request: http.HttpRequest = .{ .method = .POST, .uri = "/internal/v1/attempt/read", .body = "{}", .headers = &.{.{ .name = auth.header_name, .value = "caller credential must be replaced" }} };
    const context: wire.Context = .{ .kind = .read, .id = .{ .coordinator = 7, .generation = 1, .operation = 71, .sequence = 1 }, .worker_incarnation = 22, .remaining_ns = 100, .charged_bytes = 4096, .request_digest = wire.requestDigest("POST", request.uri, request.body) };
    var prepared = try client.prepare(a, executor, endpoint, request, context, 1000);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 1), prepared.request.headers.len);
    try std.testing.expect(prepared.headers.len > prepared.request.headers.len);
    // prepare() reserves space for incoming headers plus its credential, but
    // removes caller credentials. Spare capacity is not a populated header.
    prepared.headers[prepared.request.headers.len] = .{ .name = auth.header_name, .value = "unused capacity sentinel" };
    try prepared.refreshContext(&client, executor, context, 1000);
    try std.testing.expectEqualStrings("unused capacity sentinel", prepared.headers[prepared.request.headers.len].value);
    try std.testing.expectEqualStrings(prepared.token, prepared.request.header(auth.header_name).?);
}

test "remote attempt credit wait parks before publication and retires queue metadata on every exit" {
    const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Mode = enum { progress, canceled, expired, fail_fast, queue_full, parent_canceled };
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Signal = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Fixture = struct {
        mode: Mode,
        scheduler: *scheduling.Scheduler,
        execution: *scheduling.Execution,
        ledger: *attempts.Coordinator,
        held: ?attempts.Id,
        canceled: *std.atomic.Value(bool),
        begins: usize = 0,
        ends: usize = 0,
        calls: usize = 0,
        releases: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.begins += 1;
            try std.testing.expectEqual(@as(usize, 1), self.ledger.snapshot().queued);
            try std.testing.expectEqual(@as(u64, 2), self.ledger.next_sequence);
            try self.execution.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            switch (self.mode) {
                .progress => {
                    try self.ledger.abortUnpublished(self.held.?);
                    self.held = null;
                },
                .canceled => self.canceled.store(true, .release),
                .expired => Clock.value = 10,
                else => return error.TestUnexpectedCreditWait,
            }
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            try self.execution.enter(.query);
        }
        fn execute(raw: *anyopaque, _: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.TestUnexpectedPublication;
        }
        fn release(raw: *anyopaque, _: usize, _: attempts.Id) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        Clock.value = 0;
        var canceled: std.atomic.Value(bool) = .init(false);
        var parent_canceled: std.atomic.Value(bool) = .init(mode == .parent_canceled);
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: scheduling.Execution = .{};
        defer execution.finish();
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &canceled, .requested = Signal.requested } }, .query);
        var slots: [1]?attempts.Attempt = undefined;
        var destinations = [_]attempts.Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 64 }};
        var ledger = try attempts.Coordinator.init(7, 1, &slots, &destinations, 64);
        try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
        var fixture: Fixture = .{ .mode = mode, .scheduler = &scheduler, .execution = &execution, .ledger = &ledger, .held = try ledger.reserve(0, 1, 64), .canceled = &canceled };
        defer if (fixture.held) |id| ledger.abortUnpublished(id) catch unreachable;
        var blocked: attempts.Coordinator.Waiter = .{};
        if (mode == .queue_full) try ledger.beginWait(&blocked, 0, 3, 64);
        defer ledger.endWait(&blocked);
        const io = std.testing.io;
        const executor: http.RequestExecutor = .{ .ptr = &fixture, .clock_io = .init(&io), .vtable = &.{ .execute = Fixture.execute } };
        const client: Client = .{ .ledger = &ledger, .store = undefined, .targets = &.{}, .signing = .{ .secret = "unused-test-secret", .issuer = "test" }, .read_allocator = std.testing.allocator, .control_allocator = std.testing.allocator, .read_executor = executor, .control_executor = executor, .now_ns = Clock.now };
        var signal: http.RequestCancellation = .{ .borrowed = &canceled, .execution_deadline_ns = 10 };
        const request: http.HttpRequest = .{ .method = .POST, .uri = "/unused", .cancellation = &signal, .execution_wait = if (mode == .fail_fast) null else .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } };
        const parent: attempts.Retention = .{ .context = &fixture, .index = 64, .id = .{ .coordinator = 7, .generation = 1, .operation = 2, .sequence = 0 }, .release_fn = Fixture.release, .cancellation = &parent_canceled, .deadline_ns = 10 };
        const result = client.reserveRead(0, 2, 64, 10, request, parent);
        switch (mode) {
            .progress => try ledger.abortUnpublished(try result),
            .canceled, .parent_canceled => try std.testing.expectError(error.Canceled, result),
            .expired => try std.testing.expectError(error.DeadlineExceeded, result),
            .fail_fast, .queue_full => try std.testing.expectError(error.RemoteAttemptCapacity, result),
        }
        try std.testing.expectEqual(@as(usize, 0), fixture.calls);
        try std.testing.expectEqual(@as(usize, 0), fixture.releases);
        const waited = mode == .progress or mode == .canceled or mode == .expired;
        try std.testing.expectEqual(@as(usize, @intFromBool(waited)), fixture.begins);
        try std.testing.expectEqual(fixture.begins, fixture.ends);
        ledger.endWait(&blocked);
        try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
        try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().queued_bytes);
        if (mode != .progress) {
            try std.testing.expectEqual(@as(u64, 2), ledger.next_sequence);
            try std.testing.expectEqual(@as(usize, 1), ledger.snapshot().attempts);
        }
    }
}

test "remote attempt client discovery admission distinguishes absent proof and pins one immutable profile" {
    const Publication = @import("remote_capability_publication.zig").Cache;
    const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    const Transport = struct {
        calls: usize = 0,
        fn execute(raw: *anyopaque, _: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedReadinessTransport;
        }
    };
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() };
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .read_isolation = false, .control_bytes = 1 << 20 });
    defer scheduler.close();
    const cache = try Publication.create(std.testing.allocator, &scheduler, &.{authority}, Clock.now);
    defer {
        cache.close();
        cache.deinit() catch unreachable;
    }
    var slots: [1]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 64 }};
    var ledger = try attempts.Coordinator.init(7, 1, &slots, &destinations, 64);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
    var targets = [_]Target{.{ .base_uri = "http://127.0.0.1:1", .authority = authority }};
    var transport: Transport = .{};
    const executor: http.RequestExecutor = .{ .ptr = &transport, .vtable = &.{ .execute = Transport.execute } };
    var storage: [0]u8 = .{};
    var no_allocations = std.heap.FixedBufferAllocator.init(&storage);
    var client: Client = .{ .ledger = &ledger, .store = undefined, .targets = &targets, .signing = .{ .secret = "unused-test-secret", .issuer = "test" }, .read_allocator = no_allocations.allocator(), .control_allocator = no_allocations.allocator(), .read_executor = executor, .control_executor = executor, .capabilities = cache, .now_ns = Clock.now };
    const request: http.HttpRequest = .{ .method = .POST, .uri = "/internal/v1/groups/7/query", .body = "{}" };
    const sequence = ledger.next_sequence;
    const baseline = scheduler.stats().control_retained_bytes;
    // Both a missing observation and an invalidated observation are temporary;
    // neither may touch the store, transport, sequence or bounded attempt pool.
    try std.testing.expectError(error.RemoteReadUnavailable, client.pinReadCapabilities(0, false));
    try std.testing.expectError(error.RemoteReadUnavailable, client.dispatchRead(0, 5, 64, 200, request));
    var probe = try cache.begin(0, authority);
    try probe.publish(.{ .authority = authority, .families = .{}, .deadline_ns = 200 });
    try std.testing.expectError(error.UnsupportedRemoteAttemptProtocol, client.pinReadCapabilities(0, true));
    try std.testing.expectError(error.UnsupportedRemoteAttemptProtocol, client.dispatchRead(0, 5, 64, 200, request));
    probe = try cache.begin(0, authority);
    try probe.publish(.{ .authority = authority, .families = .{ .group_query = true, .retained_read = true }, .deadline_ns = 200 });
    const pinned = try client.pinReadCapabilities(0, true);
    try std.testing.expect(pinned.capabilities == null);
    try cache.invalidate(0, authority);
    try std.testing.expectError(error.RemoteReadUnavailable, client.pinReadCapabilities(0, true));
    try std.testing.expectError(error.RemoteReadUnavailable, client.dispatchRead(0, 5, 64, 200, request));
    try std.testing.expect(try pinned.supportsRead(0, true));
    const repinned = try pinned.pinReadCapabilities(0, true);
    try std.testing.expect(try repinned.supportsRead(0, true));
    targets[0].authority.local.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, pinned.pinReadCapabilities(0, true));
    targets[0].authority = authority;
    client.reads_ready = false;
    try std.testing.expectError(error.RemoteReadUnavailable, client.dispatchRead(0, 5, 64, 200, request));
    client.reads_ready = true;
    targets[0].available = false;
    try std.testing.expectError(error.RemoteReadUnavailable, client.dispatchRead(0, 5, 64, 200, request));
    try std.testing.expectEqual(sequence, ledger.next_sequence);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
    try std.testing.expectEqual(@as(usize, 0), transport.calls);
    try std.testing.expectEqual(baseline, scheduler.stats().control_retained_bytes);
}

test "remote attempt client discovery readiness crosses checked admission callbacks" {
    const ReadAdmission = @import("antfly_local_sources").common_read_admission;
    const native = @import("antfly_runtime_abi").native_abi;
    const errors = @import("antfly_runtime_abi").error_abi;
    const Fixture = struct {
        err: anyerror,
        var dispatch_calls: usize = 0;
        fn lifetime(_: *anyopaque) !void {}
        fn remote(raw: *anyopaque) !?http.RequestExecutor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.err;
        }
        fn dispatch(contract: *const native.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) errors.Status {
            dispatch_calls += 1;
            return ReadAdmission.Boundary.local_dispatch(contract, callback, args, output);
        }
    };
    for ([_]anyerror{ error.RemoteReadUnavailable, error.UnsupportedRemoteAttemptProtocol }) |err| {
        Fixture.dispatch_calls = 0;
        var fixture: Fixture = .{ .err = err };
        const payload: ReadAdmission.Payload = .{ .ptr = &fixture, .vtable = &.{ .checkpoint = Fixture.lifetime, .check_lifetime = Fixture.lifetime, .remote_executor = Fixture.remote }, .dispatch = Fixture.dispatch };
        // A distinct dispatcher forces C call contracts and stable Status
        // encoding instead of the same-compilation direct Zig fast path.
        try std.testing.expect(payload.dispatch != ReadAdmission.Boundary.local_dispatch);
        try std.testing.expectError(err, ReadAdmission.Port.init(&payload).remoteExecutor());
        try std.testing.expectEqual(@as(usize, 2), Fixture.dispatch_calls);
    }
}
