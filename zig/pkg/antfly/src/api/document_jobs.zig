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
    return .{ .metadata = &server.document_job_store, .artifacts = server.cfg.document_artifact_store orelse if (server.document_artifact_files) |*files| files.artifactStore() else return error.DocumentStorageNotConfigured, .io = server.inferenceIo() };
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
        if (runtime.threaded_jobs == null or server.cfg.local_inference_connection_target == null) return error.DocumentStorageNotConfigured;
        const body = (try ctx.body()) orelse return error.InvalidInferenceRequest;
        if (body.len > 64 * 1024 * 1024) return error.InferenceRequestTooLarge;
        const root = std.json.parseFromSliceLeaky(V, a, body, .{ .duplicate_field_behavior = .@"error", .parse_numbers = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidInferenceRequest;
        if (root != .object) return error.InvalidInferenceRequest;
        for (root.object.keys()) |key| if (!std.mem.eql(u8, key, "request") and !std.mem.eql(u8, key, "retention_seconds")) return error.InvalidInferenceRequest;
        var request = root.object.get("request") orelse return error.InvalidInferenceRequest;
        if (request != .object) return error.InvalidInferenceRequest;
        var nodes: usize = 1_000_000;
        try validateTree(request, 0, &nodes);
        try jobs.validateRequest(a, request);
        // Persist the effective backend selector, never a mutable gateway alias.
        const selector = request.object.get("model") orelse return error.ModelRequired;
        if (selector != .string) return error.InvalidInferenceRequest;
        const named = try gateway.view(server);
        const task = if (operation == .read) sources.common_provider_registry.gateway.Task.read else .extract;
        const local = if (std.mem.indexOfScalar(u8, selector.string, '/') == null or named.contains(task, selector.string)) try gateway.localModels(a, server, task) else &.{};
        const resolved = try named.resolve(task, selector.string, local);
        var connection: []const u8 = sources.common_config.local_inference_connection_id;
        const model = switch (resolved) {
            .local => |value| value.model,
            .connection => |value| blk: {
                connection = value.connection;
                break :blk value.model;
            },
            .configured => |value| blk: {
                if (operation == .read) {
                    const cfg = try named.readers.?.getConfig(value.name);
                    if (cfg.provider != .antfly or cfg.resolvedUrl() != null) return error.DocumentStorageNotConfigured;
                    request = try std.json.parseFromSliceLeaky(V, a, try sources.common_provider_registry.tasks.readingBody(a, cfg, request), .{ .parse_numbers = false });
                    break :blk cfg.model orelse return error.ModelRequired;
                }
                const binding = named.providers.extractor_configs.get(value.name) orelse return error.ModelNotFound;
                connection = binding.connection;
                break :blk binding.model;
            },
        };
        if (!std.mem.eql(u8, connection, sources.common_config.local_inference_connection_id)) return error.DocumentStorageNotConfigured;
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
        lease: ?runs.Lease = null,
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
        fn heartbeat(self: *Self) std.Io.Cancelable!void {
            const io = self.server.inferenceIo();
            while (true) {
                try io.sleep(.fromMilliseconds(5000), .awake);
                var arena = std.heap.ArenaAllocator.init(self.server.alloc);
                defer arena.deinit();
                const store = self.persistence.interface() catch {
                    self.cancelled.store(true, .release);
                    return;
                };
                store.renew(arena.allocator(), self.lease.?, now(), lease_ms) catch {
                    self.cancelled.store(true, .release);
                    return;
                };
            }
        }
        pub fn run(raw: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            try authority(self);
            self.persistence = try context(self.server);
            self.persistence.authority = .{ .ptr = self, .check_fn = authority };
            const store = try self.persistence.interface();
            var arena = std.heap.ArenaAllocator.init(self.server.alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const record = try store.loadAt(a, self.id, self.owner, now());
            self.lease = store.claim(a, self.id, self.owner, record.identity, now(), lease_ms) catch |err| switch (err) {
                error.DocumentRunBusy, error.DocumentRunTerminal => return,
                else => return err,
            };
            const lease = self.lease.?;
            errdefer store.yieldLease(a, lease, now()) catch {};
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
            var group: std.Io.Group = .init;
            defer group.cancel(self.server.inferenceIo());
            try group.concurrent(self.server.inferenceIo(), heartbeat, .{self});
            var client = httpx.Client.initWithConfig(a, self.server.inferenceIo(), .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
            defer client.deinit();
            var response = connections.invokeInferenceConnectionWithRequest(a, &client, self.server.cfg.node_config, self.server.cfg.local_inference_connection_target, self.server.cfg.secret_store, record.connection, @tagName(record.operation), request, httpx.CancellationToken.fromCallback(self, cancelledFn), @import("antfly_platform").time.monotonicNs() + 1_800_000 * std.time.ns_per_ms, .{}, .post, .{ .document_store = binding.view() }) catch |err| {
                if (cancelledFn(self)) return;
                switch (err) {
                    error.DocumentModelRevisionMismatch, error.DocumentBudgetExhausted, error.UnsupportedMediaTokenBudget, error.InvalidDocumentCheckpoint => try store.failWithCode(a, lease, now(), @errorName(err)),
                    else => try store.yieldLease(a, lease, now()),
                }
                return;
            };
            defer response.deinit(a);
            const after = try store.loadAt(a, self.id, self.owner, now());
            if (after.state == .succeeded or after.state == .cancelled) return;
            if (response.status >= 400 and response.status < 500) try store.failWithCode(a, lease, now(), "DocumentRequestRejected") else try store.yieldLease(a, lease, now());
        }
        pub fn deinit(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            const a = self.server.alloc;
            _ = self.server.document_work_in_flight.fetchSub(1, .acq_rel);
            a.free(self.id);
            a.free(self.owner);
            a.destroy(self);
        }
    };
}

test "httpx inference gateway document job preflight bounds recursive schemas before credential traversal" {
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
