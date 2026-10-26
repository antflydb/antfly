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

//! Principal-scoped document jobs. HTTP is a subscriber; the bounded runtime
//! owns execution. Metadata uses durable jobs and payloads use Antfly artifacts.
const std = @import("std");
const httpx = @import("httpx");
const readers = @import("antfly_readers");
const runs = readers.document_runs;
const failures = readers.document_failure;
const storage = @import("document_run_storage.zig");
const jobs = @import("research_jobs.zig");
const gateway = @import("inference_gateway.zig");
const sources = @import("antfly_local_sources");
const connections = @import("connections.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
const now = jobs.nowMillis;
const lease_ms = 30_000;
pub const Operation = @FieldType(runs.Record, "operation");
pub const Action = enum { create, status, result, events, @"resume", cancel };
fn digest(bytes: []const u8) runs.Digest {
    var value: runs.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &value, .{});
    return value;
}
fn jsonBytes(ctx: *httpx.Context, bytes: []u8) !httpx.Response {
    _ = ctx.response.bodyOwned(ctx.allocator, bytes);
    try ctx.setHeader("content-type", "application/json");
    return ctx.response.build();
}
pub fn statusJson(a: A, record: runs.Record) ![]u8 {
    return std.json.Stringify.valueAlloc(a, .{ .id = record.run_id, .operation = record.operation, .state = record.state, .attempt = record.attempt, .sequence = record.sequence, .expires_at_ms = record.retention_until_ms, .usage = record.used, .@"error" = record.error_code }, .{ .emit_null_optional_fields = false });
}
pub fn context(server: anytype) !storage.Context {
    if (!server.document_job_store.durable()) return error.DocumentStorageNotConfigured;
    const term = server.restore_leadership_term.load(.acquire);
    if (server.document_job_store.replicated != null and (term == 0 or server.document_job_store.replicated_term.load(.acquire) != term)) return error.DocumentStorageNotConfigured;
    return .{ .metadata = &server.document_job_store, .artifacts = server.cfg.document_artifact_store orelse if (server.document_artifact_files) |*files| files.artifactStore() else return error.DocumentStorageNotConfigured, .io = server.inferenceIo(), .leadership_term = server.restore_leadership_term.load(.acquire) };
}
pub fn handle(server: anytype, ctx: *httpx.Context, principal: []const u8, operation: Operation, action: Action) !httpx.Response {
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 128 * 1024 * 1024 };
    return handleBounded(server, ctx, principal, operation, action, budget.allocator()) catch |err| {
        if (budget.exhausted) return error.InferenceRequestTooLarge;
        return err;
    };
}
fn validateTree(value: V, depth: usize, remaining: *usize) !void {
    if (depth > 128 or remaining.* == 0) return error.InvalidInferenceRequest;
    remaining.* -= 1;
    switch (value) {
        .object => |object| for (object.values()) |child| try validateTree(child, depth + 1, remaining),
        .array => |array| for (array.items) |child| try validateTree(child, depth + 1, remaining),
        else => {},
    }
}
/// Document callers select server-owned models, never provider configuration.
/// Validate the request envelope while leaving schema names and source data
/// opaque; credential-shaped names in those values are ordinary document data.
fn validateSubmission(operation: Operation, request: V) !void {
    if (request != .object) return error.InvalidInferenceRequest;
    const fields: []const []const u8 = if (operation == .read)
        &.{ "model", "documents", "images", "prompt", "max_tokens", "retain_artifact" }
    else
        &.{ "model", "schema_version", "schema", "options", "inputs", "documents", "read_artifact" };
    for (request.object.keys()) |key| {
        var known = false;
        for (fields) |allowed| if (std.mem.eql(u8, key, allowed)) {
            known = true;
            break;
        };
        if (!known) return error.InvalidInferenceRequest;
    }
}

pub const LeasePolicy = struct {
    duration_ms: u64 = lease_ms,
    renew_interval_ms: u64 = 5_000,
    fn validate(self: @This()) !void {
        if (self.duration_ms == 0 or self.duration_ms > 1_800_000 or self.renew_interval_ms == 0 or self.renew_interval_ms >= self.duration_ms) return error.InvalidDocumentCheckpoint;
    }
};

/// One lease tenure covers every storage/provider operation, including the
/// initial upload and retained callback draining. Reuse the host IO executor.
const LeaseRenewal = struct {
    persistence: *storage.Context,
    lease: runs.Lease,
    policy: LeasePolicy = .{},
    on_loss: ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } = null,
    group: std.Io.Group = .init,
    fn start(self: *@This()) !void {
        try self.policy.validate();
        try self.group.concurrent(self.persistence.io, loop, .{self});
    }
    fn stop(self: *@This()) void {
        self.group.cancel(self.persistence.io);
    }
    fn loop(self: *@This()) std.Io.Cancelable!void {
        while (true) {
            try self.persistence.io.sleep(.fromMilliseconds(@intCast(self.policy.renew_interval_ms)), .awake);
            const active = self.renew() catch {
                if (self.on_loss) |sink| sink.notify(sink.ptr);
                return;
            };
            if (!active) return;
        }
    }
    fn renew(self: *@This()) !bool {
        var arena = std.heap.ArenaAllocator.init(self.persistence.metadata.alloc);
        defer arena.deinit();
        const store = try self.persistence.interface();
        store.renew(arena.allocator(), self.lease, self.persistence.now_fn(), self.policy.duration_ms) catch |err| {
            if (err == error.DocumentRunLeaseLost) {
                const current = try store.loadAt(arena.allocator(), self.lease.run_id, self.lease.owner, self.persistence.now_fn());
                // Atomic completion ends renewal without cancelling the root
                // response or its remaining retained callback borrows.
                if (current.state == .succeeded and current.attempt == self.lease.attempt and std.mem.eql(u8, &current.nonce, &self.lease.nonce)) return false;
            }
            return err;
        };
        return true;
    }
};

fn handleBounded(server: anytype, ctx: *httpx.Context, principal: []const u8, operation: Operation, action: Action, backing: A) !httpx.Response {
    if (!server.publicInferenceEnabled()) return gateway.disabledResponse(ctx);
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var persistence = try context(server);
    const Owner = struct {
        server: @TypeOf(server),
        term: u64,
        fn check(raw: *anyopaque) !void {
            const owner: *@This() = @ptrCast(@alignCast(raw));
            if (!owner.server.documentExecutionPermitted() or owner.server.restore_leadership_term.load(.acquire) != owner.term) return error.DocumentRunLeaseLost;
        }
    };
    var owner_scope = Owner{ .server = server, .term = server.restore_leadership_term.load(.acquire) };
    persistence.authority = .{ .ptr = &owner_scope, .check_fn = Owner.check };
    const store = try persistence.interface();
    if (action == .create) {
        if (!server.documentExecutionPermitted()) return error.NotLeader;
        const runtime = server.cfg.backend_runtime orelse return error.DocumentStorageNotConfigured;
        if (runtime.threaded_jobs == null) return error.DocumentStorageNotConfigured;
        const body = (try ctx.body()) orelse return error.InvalidInferenceRequest;
        if (body.len > 64 * 1024 * 1024) return error.InferenceRequestTooLarge;
        const root = std.json.parseFromSliceLeaky(V, a, body, .{ .duplicate_field_behavior = .@"error", .parse_numbers = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidInferenceRequest;
        if (root != .object) return error.InvalidInferenceRequest;
        for (root.object.keys()) |key| if (!std.mem.eql(u8, key, "request") and !std.mem.eql(u8, key, "retention_seconds")) return error.InvalidInferenceRequest;
        var request = root.object.get("request") orelse return error.InvalidInferenceRequest;
        if (request != .object) return error.InvalidInferenceRequest;
        var nodes: usize = 1_000_000;
        try validateTree(request, 0, &nodes);
        try validateSubmission(operation, request);
        // Persist the effective backend selector, never a mutable gateway alias.
        const selector = request.object.get("model") orelse return error.ModelRequired;
        if (selector != .string) return error.InvalidInferenceRequest;
        const named = try gateway.view(server);
        const task = if (operation == .read) sources.common_provider_registry.gateway.Task.read else .extract;
        const local = if (std.mem.indexOfScalar(u8, selector.string, '/') == null or named.contains(task, selector.string)) try gateway.localModels(a, server, task) else &.{};
        const resolved = try named.resolve(task, selector.string, local);
        var connection: []const u8 = if (server.cfg.local_inference_connection_target != null) sources.common_config.local_inference_connection_id else sources.common_config.default_inference_connection_id;
        const model = switch (resolved) {
            .local => |value| value.model,
            .connection => |value| blk: {
                connection = value.connection;
                break :blk value.model;
            },
            .configured => |value| blk: {
                if (operation == .read) {
                    const cfg = try named.readers.?.getConfig(value.name);
                    if (cfg.provider != .antfly) return error.DocumentStorageNotConfigured;
                    if (cfg.resolvedUrl() != null) connection = try std.fmt.allocPrint(a, "readers/{s}", .{value.name});
                    request = try std.json.parseFromSliceLeaky(V, a, try sources.common_provider_registry.tasks.readingBody(a, cfg, request), .{ .parse_numbers = false });
                    break :blk cfg.model orelse return error.ModelRequired;
                }
                const binding = named.providers.extractor_configs.get(value.name) orelse return error.ModelNotFound;
                connection = binding.connection;
                break :blk binding.model;
            },
        };
        // Resolve and enforce Antfly compatibility before persisting a job.
        _ = try connections.inferenceConnectionAdmissionOwner(server.cfg.node_config, server.cfg.local_inference_connection_target, connection, @tagName(operation));
        try request.object.put(a, "model", .{ .string = model });
        if (operation == .read) {
            if (!request.object.contains("documents") and !request.object.contains("images")) return error.InvalidInferenceRequest;
            // Normalize ordinary reader image forms into the document source
            // path so every durable read uses the shared checkpoint executor.
            if (request.object.getPtr("images")) |images| {
                if (images.* != .array or images.array.items.len == 0) return error.InvalidInferenceRequest;
                for (images.array.items) |*value| if (value.* == .string) {
                    var image: V = .{ .object = .empty };
                    try image.object.put(a, "url", value.*);
                    value.* = image;
                };
            }
            _ = request.object.swapRemove("retain_artifact");
        }
        const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
        const retention: u64 = if (root.object.get("retention_seconds")) |value| blk: {
            const n = switch (value) {
                .integer => |n| std.math.cast(u64, n),
                .number_string => |n| std.fmt.parseInt(u64, n, 10) catch null,
                else => null,
            } orelse return error.InvalidInferenceRequest;
            if (n < 60 or n > 604800) return error.InvalidInferenceRequest;
            break :blk n;
        } else 86400;
        var entropy: [16]u8 = undefined;
        server.inferenceIo().random(&entropy);
        var id_buffer: [36]u8 = undefined;
        const id = jobs.formatJobId(&id_buffer, entropy);
        const identity: runs.Identity = .{ .request = digest(bytes), .sources = @splat(0), .definition = @splat(0), .models = @splat(0), .runtime = @splat(0) };
        try store.create(.{ .run_id = id, .owner = principal, .operation = operation, .connection = connection, .prepared = false, .identity = identity, .retention_until_ms = now() + retention * 1000, .limits = .{ .calls = 10_000, .tokens = 1_000_000, .pixels = 400_000_000, .source_bytes = 64 * 1024 * 1024 + bytes.len, .output_bytes = 256 * 1024 * 1024 }, .inputs = &.{} });
        const lease = try store.claim(a, id, principal, identity, now(), lease_ms);
        errdefer store.fail(a, lease, now()) catch {};
        var renewal = LeaseRenewal{ .persistence = &persistence, .lease = lease };
        try renewal.start();
        defer renewal.stop();
        const key = digest("submitted-request-v1");
        if (!try store.reserve(a, lease, key, .{ .source_bytes = bytes.len }, now())) return error.InvalidDocumentCheckpoint;
        const reference = try store.complete(a, lease, key, .{ .source_bytes = bytes.len }, bytes, now());
        try store.attachRequest(a, lease, reference, now());
        try store.yieldLease(a, lease, now());
        // The periodic maintenance owner is the independent recovery wakeup.
        try ctx.setHeader("Location", try std.fmt.allocPrint(a, "/ai/v1/{s}/jobs/{s}", .{ @tagName(operation), id }));
        _ = ctx.status(202);
        return jsonBytes(ctx, try statusJson(ctx.allocator, try store.loadAt(a, id, principal, now())));
    }
    const id = ctx.param("job_id") orelse return error.DocumentRunNotFound;
    if (!jobs.validJobId(id)) return error.DocumentRunNotFound;
    var record = try store.loadAt(a, id, principal, now());
    if (record.operation != operation) return error.DocumentRunNotFound;
    switch (action) {
        .create => unreachable,
        .status => {},
        .cancel => {
            if (!server.documentExecutionPermitted()) return error.NotLeader;
            try store.cancel(a, id, principal);
            record = try store.loadAt(a, id, principal, now());
        },
        .@"resume" => {
            if (!server.documentExecutionPermitted()) return error.NotLeader;
            // Queued and expired attempts are picked up independently. An
            // active lease cannot be stolen by a client polling resume.
            if (record.state == .running and record.lease_until_ms > now()) _ = ctx.status(409);
        },
        .result => {
            if (record.state != .succeeded) {
                _ = ctx.status(409);
                return jsonBytes(ctx, try statusJson(ctx.allocator, record));
            }
            return jsonBytes(ctx, try store.read(ctx.allocator, record.result orelse return error.InvalidDocumentCheckpoint, 64 * 1024 * 1024));
        },
        .events => return stream(server, ctx, store, record, backing),
    }
    return jsonBytes(ctx, try statusJson(ctx.allocator, record));
}
fn stream(server: anytype, ctx: *httpx.Context, store: runs.Store, initial: runs.Record, backing: A) !httpx.Response {
    var cursor: u64 = if (ctx.header("last-event-id")) |text| std.fmt.parseInt(u64, text, 10) catch return error.InvalidInferenceRequest else 0;
    if (cursor > initial.sequence) return error.InvalidInferenceRequest;
    var index = try runs.Index.init(backing, store, initial, 100_000, 32 * 1024 * 1024);
    defer index.deinit();
    var writer = try ctx.streamResponse(200);
    var record = initial;
    const started = now();
    var heartbeat = started;
    while (!ctx.isCancellationRequested()) {
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        record = try store.loadAt(a, initial.run_id, initial.owner, now());
        try index.refresh(store, record, 100_000, 32 * 1024 * 1024);
        const groups = try index.eventsAfter(cursor, 8);
        for (groups) |group| {
            const bytes = try store.read(a, group.payload, 64 * 1024);
            // Event payloads are JSON arrays, with escaped newlines. Do not
            // interpolate arbitrary source text into SSE protocol fields.
            const parsed = try std.json.parseFromSliceLeaky(V, a, bytes, .{});
            const single = try std.json.Stringify.valueAlloc(a, parsed, .{});
            try writer.write(try std.fmt.allocPrint(a, "id: {d}\nevent: progress\ndata: {s}\n\n", .{ group.sequence, single }));
            cursor = group.sequence;
        }
        const terminal = record.state == .succeeded or record.state == .failed or record.state == .cancelled;
        if (groups.len == 0 and terminal) {
            try writer.writeEvent("status", try statusJson(a, record));
            break;
        }
        if (now() -| heartbeat >= 15_000) {
            try writer.write(": heartbeat\n\n");
            heartbeat = now();
        }
        // Bound subscriber lifetimes; clients reconnect using the last cursor.
        if (now() -| started >= 60_000) break;
        if (groups.len == 0) try server.inferenceIo().sleep(.fromMilliseconds(250), .awake);
    }
    try writer.close();
    return ctx.response.build();
}

pub fn Work(comptime Server: type) type {
    return struct {
        const Self = @This();
        server: *Server,
        id: []const u8,
        owner: []const u8,
        term: u64,
        slot: ?usize = null,
        lease: ?runs.Lease = null,
        lease_policy: LeasePolicy = .{},
        persistence: storage.Context = undefined,
        cancelled: std.atomic.Value(bool) = .init(false),
        binding_live: std.atomic.Value(bool) = .init(false),
        fn authority(raw: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (!self.server.documentExecutionPermitted() or self.server.restore_leadership_term.load(.acquire) != self.term) return error.DocumentRunLeaseLost;
        }
        fn cancelledFn(raw: *const anyopaque) bool {
            const self: *const Self = @ptrCast(@alignCast(raw));
            return self.cancelled.load(.acquire) or !self.server.documentExecutionPermitted() or self.server.restore_leadership_term.load(.acquire) != self.term;
        }
        fn released(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            self.binding_live.store(false, .release);
        }
        fn leaseLost(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            self.cancelled.store(true, .release);
        }
        pub fn run(raw: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            try self.lease_policy.validate();
            try authority(self);
            self.persistence = try context(self.server);
            self.persistence.authority = .{ .ptr = self, .check_fn = authority };
            self.persistence.cancellation = .{ .ptr = self, .is_cancelled_fn = cancelledFn };
            const store = try self.persistence.interface();
            var arena = std.heap.ArenaAllocator.init(self.server.alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const record = try store.loadAt(a, self.id, self.owner, now());
            self.lease = store.claim(a, self.id, self.owner, record.identity, now(), self.lease_policy.duration_ms) catch |err| switch (err) {
                error.DocumentRunBusy, error.DocumentRunTerminal => return,
                else => return err,
            };
            const lease = self.lease.?;
            errdefer store.yieldLeaseUntil(a, lease, now(), now() +| @import("job_retry.zig").retryDelayMs(std.hash.Wyhash.hash(0, self.id), lease.attempt)) catch {};
            // Own the lease across request hydration, inference, and retained
            // callback draining. Artifact IO may outlive a lease interval.
            var renewal = LeaseRenewal{ .persistence = &self.persistence, .lease = lease, .policy = self.lease_policy, .on_loss = .{ .ptr = self, .notify = leaseLost } };
            try renewal.start();
            defer renewal.stop();
            if (record.attempt >= 8) {
                try store.failWithCode(a, lease, now(), "DocumentRetriesExhausted");
                return;
            }
            if (record.inputs.len == 0) {
                try store.failWithCode(a, lease, now(), "IncompleteSubmission");
                return;
            }
            const request = try store.read(a, record.inputs[0], 64 * 1024 * 1024);
            const binding = try self.persistence.bind(self.server.alloc, self.id, self.owner, .{ .ptr = self, .release_fn = released });
            self.binding_live.store(true, .release);
            defer {
                binding.view().release();
                // Reverse RPC responses retain the capability independently of
                // the root call. Drain those borrows before freeing this work.
                while (self.binding_live.load(.acquire)) self.server.inferenceIo().sleep(.fromMilliseconds(1), .awake) catch {};
            }
            var transport_budget: sources.sql_memory_budget = .{ .backing = self.server.alloc, .limit = 256 * 1024 * 1024 };
            var client = httpx.Client.initWithConfig(transport_budget.allocator(), self.server.inferenceIo(), .{ .keep_alive = false, .max_response_size = 1024 * 1024 * 1024, .max_error_response_size = 64 * 1024, .retry_policy = .{ .max_retries = 0 } });
            defer client.deinit();
            var response = connections.invokeInferenceConnectionWithRequest(a, &client, self.server.cfg.node_config, self.server.cfg.local_inference_connection_target, self.server.cfg.secret_store, record.connection, @tagName(record.operation), request, httpx.CancellationToken.fromCallback(self, cancelledFn), @import("antfly_platform").time.monotonicNs() + 1_800_000 * std.time.ns_per_ms, .{}, .post, .{ .document_store = binding.view(), .document_allocator = transport_budget.allocator() }) catch |err| {
                if (cancelledFn(self)) return;
                try self.settleFailure(a, store, lease, failures.fromError(err));
                return;
            };
            defer response.deinit(a);
            const after = try store.loadAt(a, self.id, self.owner, now());
            if (after.state == .succeeded or after.state == .cancelled or after.state == .failed) return;
            try self.settleFailure(a, store, lease, failures.fromResponse(a, response.status, response.body));
        }
        fn settleFailure(self: *Self, a: A, store: runs.Store, lease: runs.Lease, failure: failures.Failure) !void {
            if (failure.retryable) {
                const retry_at = now() +| @import("job_retry.zig").retryDelayMs(std.hash.Wyhash.hash(0, self.id), lease.attempt);
                try store.yieldLeaseWithCode(a, lease, now(), retry_at, failure.code);
            } else try store.failWithCode(a, lease, now(), failure.code);
        }
        pub fn deinit(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            const a = self.server.alloc;
            if (self.slot) |slot| {
                @import("antfly_platform").sync.lockYielding(&self.server.document_schedule_mutex);
                self.server.scheduled_documents[slot] = null;
                self.server.document_schedule_mutex.unlock();
            }
            _ = self.server.document_work_in_flight.fetchSub(1, .acq_rel);
            a.free(self.id);
            a.free(self.owner);
            a.destroy(self);
        }
    };
}

test "httpx inference gateway document job preflight bounds recursive schemas before envelope validation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var value: V = .null;
    for (0..129) |_| {
        var nested: V = .{ .array = .init(a) };
        try nested.array.append(value);
        value = nested;
    }
    var remaining: usize = 1000;
    try std.testing.expectError(error.InvalidInferenceRequest, validateTree(value, 0, &remaining));
    remaining = 1;
    try std.testing.expectError(error.InvalidInferenceRequest, validateTree(.{ .array = .init(a) }, 129, &remaining));
}

test "httpx inference gateway document workers fence publication when ownership terms change" {
    const Server = struct {
        allowed: bool = true,
        restore_leadership_term: std.atomic.Value(u64) = .init(7),
        pub fn documentExecutionPermitted(self: *@This()) bool {
            return self.allowed;
        }
    };
    var server = Server{};
    var work = Work(Server){ .server = &server, .id = "run", .owner = "alice", .term = 7 };
    try Work(Server).authority(&work);
    try std.testing.expect(!Work(Server).cancelledFn(&work));
    server.allowed = false;
    try std.testing.expectError(error.DocumentRunLeaseLost, Work(Server).authority(&work));
    try std.testing.expect(Work(Server).cancelledFn(&work));
    // Regaining leadership cannot revive a worker from the previous term.
    server.allowed = true;
    server.restore_leadership_term.store(8, .release);
    try std.testing.expectError(error.DocumentRunLeaseLost, Work(Server).authority(&work));
    try std.testing.expect(Work(Server).cancelledFn(&work));
    var current = Work(Server){ .server = &server, .id = "run", .owner = "alice", .term = 8 };
    try Work(Server).authority(&current);
    current.cancelled.store(true, .release);
    try std.testing.expect(Work(Server).cancelledFn(&current));
}

test "httpx inference gateway document job validation treats schemas and source data as opaque" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const valid = try std.json.parseFromSliceLeaky(V, work, "{\"model\":\"extractor\",\"schema_version\":2,\"schema\":{\"structures\":{\"record\":{\"fields\":{\"api_key\":\"str\",\"session_token\":\"str\"}}}},\"inputs\":[{\"content\":{\"credentials_json\":\"document data\"}}]}", .{});
    try validateSubmission(.extract, valid);
    // Provider settings are server-owned, not part of either public envelope.
    for ([_][]const u8{ "{\"model\":\"m\",\"api_key\":\"inline\"}", "{\"model\":\"m\",\"generator\":{\"api_key\":\"inline\"}}", "{\"model\":\"m\",\"provider\":\"openai\"}" }) |body| {
        const request = try std.json.parseFromSliceLeaky(V, work, body, .{});
        try std.testing.expectError(error.InvalidInferenceRequest, validateSubmission(.extract, request));
        try std.testing.expectError(error.InvalidInferenceRequest, validateSubmission(.read, request));
    }
    const read = try std.json.parseFromSliceLeaky(V, work, "{\"model\":\"reader\",\"documents\":[{\"id\":\"receipt\",\"url\":\"https://example.test/file.pdf\"}],\"prompt\":\"Find api_key\"}", .{});
    try validateSubmission(.read, read);
}

test "httpx inference gateway document worker renews throughout slow request hydration" {
    const artifacts = @import("../serverless/artifacts/store.zig");
    const FsStore = @import("../serverless/artifacts/fs_store.zig").FsStore;
    const abi = sources.inference_connection_abi;
    const Cancellation = @import("antfly_cancellation").CancellationToken;
    const Slow = struct {
        var delay_next: std.atomic.Value(bool) = .init(false);
        fn read(raw: *anyopaque, a: A, id: []const u8, _: usize, cancellation: Cancellation) ![]u8 {
            if (delay_next.swap(false, .acq_rel)) {
                // Longer than the worker lease, with no provider work yet.
                const until = now() + 2500;
                while (now() < until) {
                    try cancellation.check();
                    try std.testing.io.sleep(.fromMilliseconds(20), .awake);
                }
            }
            const fs: *FsStore = @ptrCast(@alignCast(raw));
            return fs.getAllocWithCancellation(a, id, cancellation);
        }
    };
    const Target = struct {
        calls: usize = 0,
        response_status: u16 = 200,
        response_body: []const u8 = "",
        native_error: ?anyerror = null,
        require_renewal: bool = false,
        fn invoke(call: *const abi.InvokeContext) callconv(.c) abi.Status {
            complete(call) catch |err| return abi.statusFromError(err);
            return .ok;
        }
        fn complete(call: *const abi.InvokeContext) !void {
            const self: *@This() = @ptrCast(@alignCast(call.target_context));
            const a = call.allocator.asStd();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const work = arena.allocator();
            var client = try readers.document_rpc.Client.initBound(a, call.document_store, std.testing.io);
            defer client.deinit();
            const store = client.store();
            const record = try store.loadAt(work, client.id, client.owner, now());
            if (self.require_renewal) try std.testing.expect(record.revision > 10);
            const lease: runs.Lease = .{ .run_id = client.id, .owner = client.owner, .attempt = record.attempt, .nonce = record.nonce };
            if (!record.prepared) try store.prepare(work, lease, record.identity, record.limits, record.inputs, now());
            self.calls += 1;
            // Reserve before dispatch. Even a transient response must retain
            // conservative charges across the owner's next attempt.
            if (self.native_error != null or self.response_status != 200) {
                try std.testing.expect(try store.reserve(work, lease, @splat(17), .{ .calls = 1, .tokens = 10, .output_bytes = 1024 }, now()));
                if (self.native_error) |err| return err;
                const bytes = try a.dupe(u8, self.response_body);
                call.out_response.* = .{ .status = self.response_status, .body = .{ .ptr = bytes.ptr, .len = bytes.len } };
                return;
            }
            const reply = "{\"object\":\"list\",\"data\":[]}";
            try std.testing.expect(try store.reserve(work, lease, @splat(17), .{ .calls = 1, .output_bytes = 1024 }, now()));
            const result = try store.complete(work, lease, @splat(17), .{ .calls = 1, .output_bytes = reply.len }, reply, now());
            try store.finish(work, lease, result, now());
            const bytes = try a.dupe(u8, reply);
            call.out_response.* = .{ .status = 200, .body = .{ .ptr = bytes.ptr, .len = bytes.len } };
        }
    };
    const Server = struct {
        alloc: A,
        document_job_store: jobs.Store,
        document_artifact_files: ?FsStore = null,
        restore_leadership_term: std.atomic.Value(u64) = .init(7),
        cfg: struct {
            document_artifact_store: ?artifacts.ArtifactStore,
            node_config: ?*const sources.common_config.Config = null,
            local_inference_connection_target: ?connections.LocalInferenceConnectionTarget,
            secret_store: ?*sources.common_secrets.FileStore = null,
        },
        pub fn inferenceIo(_: *@This()) std.Io {
            return std.testing.io;
        }
        pub fn documentExecutionPermitted(_: *@This()) bool {
            return true;
        }
    };
    const Scenario = struct {
        slow: bool = false,
        status: u16 = 200,
        body: []const u8 = "",
        native_error: ?anyerror = null,
        expected: @FieldType(runs.Record, "state"),
        code: ?[]const u8 = null,
    };
    for ([_]Scenario{
        .{ .slow = true, .expected = .succeeded },
        .{ .status = 429, .body = "{\"error\":\"CloudflareRateLimited\"}", .expected = .queued, .code = "CloudflareRateLimited" },
        .{ .native_error = error.CloudflareRateLimited, .expected = .queued, .code = "CloudflareRateLimited" },
        .{ .status = 503, .body = "temporary upstream failure", .expected = .queued, .code = "InferenceTransportUnavailable" },
        .{ .status = 504, .body = "{\"error\":\"DocumentBudgetExhausted\"}", .expected = .failed, .code = "DocumentBudgetExhausted" },
        .{ .status = 400, .body = "{\"error\":\"InvalidDocumentSource\"}", .expected = .failed, .code = "InvalidDocumentSource" },
    }) |scenario| {
        const a = std.testing.allocator;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/slow-document-jobs", .{tmp.sub_path});
        defer a.free(path);
        const artifacts_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/slow-document-artifacts", .{tmp.sub_path});
        defer a.free(artifacts_path);
        var files = try FsStore.init(a, artifacts_path);
        defer files.deinit();
        var artifact_store = files.artifactStore();
        var table = artifact_store.vtable.*;
        table.get_bounded_alloc_with_cancellation = Slow.read;
        artifact_store.vtable = &table;
        var target = Target{ .response_status = scenario.status, .response_body = scenario.body, .native_error = scenario.native_error, .require_renewal = scenario.slow };
        var server = Server{ .alloc = a, .document_job_store = jobs.Store.init(a, .{ .namespace = .documents }), .cfg = .{ .document_artifact_store = artifact_store, .local_inference_connection_target = .{ .capabilities = abi.Capability.streaming_response, .context = &target, .invoke = Target.invoke } } };
        defer server.document_job_store.deinit();
        const opened = try a.create(jobs.OpenedStore);
        opened.* = try jobs.OpenedStore.open(a, path);
        try server.document_job_store.attachOpenedStore(opened);
        var persistence = try context(&server);
        const store = try persistence.interface();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const work = arena.allocator();
        const request = "{\"model\":\"native-reader\",\"documents\":[]}";
        const identity: runs.Identity = .{ .request = digest(request), .sources = @splat(0), .definition = @splat(0), .models = @splat(0), .runtime = @splat(0) };
        try store.create(.{ .run_id = "slow-hydration", .owner = "alice", .connection = sources.common_config.local_inference_connection_id, .prepared = false, .identity = identity, .inputs = &.{}, .limits = .{ .calls = 4, .tokens = 100, .source_bytes = 4096, .output_bytes = 4096 } });
        const submission = try store.claim(work, "slow-hydration", "alice", identity, now(), lease_ms);
        try std.testing.expect(try store.reserve(work, submission, @splat(1), .{ .source_bytes = request.len }, now()));
        const ref = try store.complete(work, submission, @splat(1), .{ .source_bytes = request.len }, request, now());
        try store.attachRequest(work, submission, ref, now());
        try store.yieldLease(work, submission, now());
        Slow.delay_next.store(scenario.slow, .release);
        var job = Work(Server){ .server = &server, .id = "slow-hydration", .owner = "alice", .term = 7, .lease_policy = .{ .duration_ms = 2000, .renew_interval_ms = 50 } };
        const started = now();
        try Work(Server).run(&job);
        try std.testing.expectEqual(@as(usize, 1), target.calls);
        const after = try store.loadAt(work, "slow-hydration", "alice", now());
        try std.testing.expectEqual(scenario.expected, after.state);
        if (scenario.code) |code| try std.testing.expectEqualStrings(code, after.error_code.?);
        if (after.state == .succeeded) {
            try std.testing.expect(!job.cancelled.load(.acquire));
            var finished = LeaseRenewal{ .persistence = &persistence, .lease = job.lease.?, .policy = job.lease_policy };
            try std.testing.expect(!try finished.renew());
        } else {
            try std.testing.expectEqual(@as(u64, 1), after.uncertain.calls);
            try std.testing.expectEqual(@as(u64, 10), after.uncertain.tokens);
            if (after.state == .queued) {
                try std.testing.expect(after.not_before_ms > started);
                const delay = after.not_before_ms -| now();
                if (delay != 0) try std.testing.io.sleep(.fromMilliseconds(@intCast(delay)), .awake);
                target.response_status = 200;
                target.native_error = null;
                var retry = Work(Server){ .server = &server, .id = "slow-hydration", .owner = "alice", .term = 7 };
                try Work(Server).run(&retry);
                const completed = try store.loadAt(work, "slow-hydration", "alice", now());
                try std.testing.expectEqual(.succeeded, completed.state);
                try std.testing.expectEqual(@as(usize, 2), target.calls);
                try std.testing.expectEqual(@as(u64, 2), completed.used.calls);
                try std.testing.expectEqual(@as(u64, 10), completed.uncertain.tokens);
                try std.testing.expect(completed.error_code == null);
            }
        }
    }
}
