// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Stable API-kernel owner for synchronous, authenticated remote dispatch.
//! The durable journal survives requests; response allocations belong to the
//! caller and are copied through a C-layout sink before the call returns.
const std = @import("std");
const common = @import("../common/http/http_common.zig");
const IoHttpExecutor = @import("../common/http/io_http_executor.zig").IoHttpExecutor;
const coordinator = @import("workload_attempt_coordinator.zig");
const client = @import("http_client.zig");
const wire = @import("../runtime_workload_abi.zig");
const http = @import("../runtime_http_abi.zig");
const memory = @import("runtime_memory_abi");
const errors = @import("../runtime_error_abi.zig");

pub const Runtime = struct {
    store: coordinator.Store,
    owned_executor: ?IoHttpExecutor = null,
    borrowed_executor: ?common.RequestExecutor = null,
    secret: []const u8,
    verification_secret: ?[]const u8,
    issuer: []const u8,
    active: std.atomic.Value(u32) = .init(0),

    pub fn init(alloc: std.mem.Allocator, durable: *@import("transactions.zig").DurableSessionStore, node_id: u64, config: coordinator.Config, io: ?std.Io, executor: ?common.RequestExecutor, secret: []const u8, verification_secret: ?[]const u8, issuer: []const u8) !Runtime {
        if (io == null and executor == null) return error.RemoteAttemptIoRequired;
        var result: Runtime = .{
            .store = try coordinator.Store.init(alloc, durable, node_id, config),
            .borrowed_executor = executor,
            .secret = secret,
            .verification_secret = verification_secret,
            .issuer = issuer,
        };
        if (executor == null) result.owned_executor = IoHttpExecutor.init(alloc, io.?, .{
            .max_response_bytes = 4 << 20,
            .connect_timeout_ms = config.max_run_ms,
            .read_timeout_ms = config.max_run_ms,
            .write_timeout_ms = config.max_run_ms,
            .pool_max_connections = config.max_attempts,
            .pool_max_per_host = config.max_destination_attempts,
        });
        return result;
    }

    pub fn deinit(self: *Runtime) void {
        // The host drains listeners and synchronous port calls first.
        std.debug.assert(self.active.load(.acquire) == 0);
        if (self.owned_executor) |*executor| executor.deinit();
        self.store.deinit();
    }

    pub fn port(self: *Runtime) wire.CoordinatorPort {
        return .{ .context = self, .dispatch = dispatch };
    }

    fn dispatch(raw: *anyopaque, allocator: *const memory.Allocator, request: *const wire.RequestV1, sink: *const wire.ResponseSink) callconv(.c) errors.Status {
        const self: *Runtime = @ptrCast(@alignCast(raw));
        // Runtime.port advertises version 2. Its caller supplies the extended
        // request after checking that version; version-1 ports use a separate
        // legacy producer and never enter this callback.
        self.execute(allocator, @ptrCast(request), sink) catch |err| return errors.statusFromError(normalize(err));
        return .ok;
    }

    fn execute(self: *Runtime, allocator: *const memory.Allocator, request: *const wire.Request, sink: *const wire.ResponseSink) !void {
        if (!allocator.valid()) return error.InvalidArgument;
        // Include discovery and reconciliation in the concurrency ceiling;
        // neither may allocate an unbounded pre-admission population.
        var current = self.active.load(.acquire);
        while (true) {
            if (current >= self.store.config.max_attempts) return error.AdmissionFull;
            current = self.active.cmpxchgWeak(current, current + 1, .acq_rel, .acquire) orelse break;
        }
        defer _ = self.active.fetchSub(1, .release);
        const alloc = allocator.asStd();
        const deadline = @min(request.deadline_ns, @import("antfly_platform").time.monotonicNs() +| @as(u64, self.store.config.max_run_ms) * std.time.ns_per_ms);
        var cancellation: common.RequestCancellation = .{
            .borrowed_context = &request.cancellation,
            .borrowed_is_cancelled = struct {
                fn check(context: *const anyopaque) bool {
                    const value: *const http.CancellationView = @ptrCast(@alignCast(context));
                    return if (value.is_cancelled) |callback| callback(value.context) != 0 else false;
                }
            }.check,
            .query_deadline_ns = deadline,
        };
        const headers = try alloc.alloc(common.RequestHeader, request.headers.len);
        defer alloc.free(headers);
        for (request.headers.slice(), headers) |header, *target| target.* = .{ .name = header.name.slice(), .value = header.value.slice() };
        var api = client.ApiHttpClient.init(alloc, self.borrowed_executor orelse self.owned_executor.?.executor());
        _ = try api.withInternalServiceNodeAuth(self.secret, self.issuer, self.store.node_id);
        const control_base_uri = request.control_base_uri.slice() orelse request.base_uri.slice();
        var response = try api.executeCoordinatedReadWithControlUri(&self.store, request.destination, request.base_uri.slice(), control_base_uri, .{
            .method = switch (request.method) {
                .get => .GET,
                .post => .POST,
                .put => .PUT,
                .delete => .DELETE,
                .patch => return error.InvalidArgument,
            },
            .uri = request.uri.slice(),
            .headers = headers,
            .authorization = request.authorization.slice(),
            .content_type = request.content_type.slice(),
            .body = request.body.slice(),
            .cancellation = &cancellation,
        }, deadline, self.verification_secret);
        defer response.deinit(alloc);
        const response_headers = try alloc.alloc(http.HeaderView, response.headers.len);
        defer alloc.free(response_headers);
        for (response.headers, response_headers) |header, *target| target.* = .{ .name = .init(header.name), .value = .init(header.value) };
        const status = sink.receive(sink.context, response.status, .init(response.content_type), .{ .ptr = response_headers.ptr, .len = response_headers.len }, .init(response.body));
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    fn normalize(err: anyerror) anyerror {
        return switch (err) {
            error.AttemptCapacityExhausted => error.AdmissionFull,
            error.AttemptAuthenticationUnavailable, error.AttemptIdentityMismatch, error.AttemptRequestMismatch, error.AttemptResponseMismatch, error.AttemptOutcomeUncertain, error.FencingRequired, error.CoordinatorGenerationSuperseded, error.GenerationExhausted, error.InvalidAttemptFrame, error.InvalidAttemptSignature, error.InvalidAttemptTarget, error.InvalidNodeIdentity, error.UnsupportedAttemptProtocol => error.DistributedQueryUnavailable,
            else => err,
        };
    }
};
