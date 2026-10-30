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

//! HTTP adaptation only. Named execution lives in the shared task layer.
const std = @import("std");
const httpx = @import("httpx");
const sources = @import("antfly_local_sources");
const registry = sources.common_provider_registry;
const gateway = @import("antfly_local_sources").common_inference_gateway;
const tasks = @import("antfly_local_sources").common_inference_tasks;
const Json = std.json.Value;
const payloads = @import("inference_payload.zig");
pub const source_media = @import("inference_source.zig");
const empty_registry = registry.Registry.init(std.heap.page_allocator);
const generating = sources.generating_mod;

pub const public_paths: []const []const u8 = &.{ "/ai/v1/models", "/ai/v1/chat/completions", "/ai/v1/embeddings", "/ai/v1/decisions", "/ai/v1/read", "/ai/v1/extract", "/ai/v1/rewrite", "/ai/v1/rerank", "/ai/v1/chunk", "/ai/v1/transcribe", "/ai/v1/dictate", "/ai/v1/synthesize", "/ai/v1/embed", "/ai/v1/generate", "/ai/v1/generate/batch", "/ml/v1/predict", "/ml/v1/predictors", "/ml/v1/models", "/ai/v1/transcription/sessions", "/ai/v1/transcription/sessions/:session_id", "/ai/v1/transcription/sessions/:session_id/audio", "/ai/v1/transcription/sessions/:session_id/events", "/ai/v1/transcription/sessions/:session_id/stream", "/ai/v1/read/jobs", "/ai/v1/read/jobs/:job_id", "/ai/v1/read/jobs/:job_id/result", "/ai/v1/read/jobs/:job_id/events", "/ai/v1/read/jobs/:job_id/resume", "/ai/v1/read/jobs/:job_id/cancel", "/ai/v1/extract/jobs", "/ai/v1/extract/jobs/:job_id", "/ai/v1/extract/jobs/:job_id/result", "/ai/v1/extract/jobs/:job_id/events", "/ai/v1/extract/jobs/:job_id/resume", "/ai/v1/extract/jobs/:job_id/cancel" };

pub fn taskForOperation(operation: []const u8) ?gateway.Task {
    const Pair = struct { []const u8, gateway.Task };
    for ([_]Pair{ .{ "generate", .generate }, .{ "chat/completions", .generate }, .{ "embeddings", .embed }, .{ "embed", .embed }, .{ "decisions", .decide }, .{ "decide", .decide }, .{ "rerank", .rerank }, .{ "chunk", .chunk }, .{ "read", .read }, .{ "extract", .extract }, .{ "rewrite", .rewrite }, .{ "transcribe", .transcribe }, .{ "dictate", .transcribe }, .{ "synthesize", .synthesize }, .{ "predict", .predict } }) |pair| if (std.mem.eql(u8, pair[0], operation)) return pair[1];
    return null;
}

pub fn ownsPath(path: []const u8) bool {
    for (public_paths) |owned| {
        if (pathMatches(owned, path)) return true;
    }
    return false;
}

fn pathMatches(pattern: []const u8, path: []const u8) bool {
    // Match the HTTP router's treatment of repeated and trailing slashes so
    // dispatch and authorization cannot disagree about the owned surface.
    var expected = std.mem.tokenizeScalar(u8, pattern, '/');
    var actual = std.mem.tokenizeScalar(u8, path, '/');
    while (expected.next()) |segment| {
        const value = actual.next() orelse return false;
        if (std.mem.startsWith(u8, segment, ":")) {
            if (value.len == 0) return false;
        } else if (!std.mem.eql(u8, segment, value)) return false;
    }
    return actual.next() == null;
}

pub fn isDiscoveryPath(path: []const u8) bool {
    return pathMatches("/ai/v1/models", path) or isPredictorDiscoveryPath(path);
}

pub fn isPredictorDiscoveryPath(path: []const u8) bool {
    return pathMatches("/ml/v1/models", path) or pathMatches("/ml/v1/predictors", path);
}

// Execution policy belongs to the logical invocation, independent of HTTP
// headers and of whether its response goes to a socket or a batch envelope.
const ExecutionOptions = struct {
    response_delivery: enum { direct, buffered } = .direct,
    attempt_limit: u8 = 8,
    operation: ?[]const u8 = null,
    attachments: ?[]const httpx.attachment_envelope.Attachment = null,
    source_port: ?source_media.Port = null,
    authorization: ?[]const u8 = null,
    attempt_observer: ?httpx.AttemptObserver = null,
};

pub fn session(server: anytype, ctx: *httpx.Context, principal: []const u8, method: @import("antfly_runtime_abi").http_abi.HttpMethod, action: []const u8) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const now = @import("antfly_platform").time.monotonicNs();
    const public_id = ctx.param("session_id");
    var requested: []const u8 = "transcription/session";
    var backend_model: ?[]const u8 = null;
    var requested_ttl_ns: ?u64 = null;
    var connection: []const u8 = undefined;
    var operation: []const u8 = "transcription/sessions";
    const raw_stream = std.mem.eql(u8, action, "/stream");
    var body: []const u8 = "";
    var lease: ?gateway.Sessions.Lease = null;
    var appended = false;
    defer if (lease) |*active| active.finish(@import("antfly_platform").time.monotonicNs(), appended);
    if (public_id) |id| {
        lease = try server.inference_sessions.acquire(a, id, principal, now);
        const pinned = lease.?.session;
        connection = pinned.connection;
        // Backend IDs must be one path segment; they are never interpreted as
        // transport addresses or a second model selection.
        operation = try std.fmt.allocPrint(a, "transcription/sessions/{s}{s}", .{ pinned.backend_id, action });
    } else {
        body = (try ctx.body()) orelse "";
        var request = std.json.parseFromSliceLeaky(Json, a, body, .{}) catch |err| return payloads.requestParseError(err);
        if (request != .object) return error.InvalidInferenceRequest;
        if (request.object.get("ttl_seconds")) |ttl| if (ttl == .integer and ttl.integer >= 1 and ttl.integer <= 3600) {
            requested_ttl_ns = @as(u64, @intCast(ttl.integer)) * std.time.ns_per_s;
        };
        const model = try string(request.object.get("model") orelse return error.ModelRequired);
        requested = model;
        const named = try view(server);
        const target = try named.resolve(.transcribe, model, &.{});
        var backend: []const u8 = undefined;
        if (target == .configured) {
            const cfg = try named.transcribers.?.getConfig(target.configured.name);
            if (cfg.provider != .antfly) return error.UnsupportedInferenceFeature;
            request = try tasks.transcriptionRequest(a, cfg, request, .session);
            backend = cfg.model orelse return error.InvalidInferenceRequest;
            connection = try std.fmt.allocPrint(a, "transcribers/{s}", .{target.configured.name});
        } else if (target == .connection) {
            backend = target.connection.model;
            connection = target.connection.connection;
        } else {
            if (target.local.explicit and server.cfg.local_inference_connection_target == null) return error.ModelNotFound;
            backend = target.local.model;
            connection = if (server.cfg.local_inference_connection_target != null) sources.common_config.local_inference_connection_id else sources.common_config.default_inference_connection_id;
        }
        backend_model = backend;
        var adapted = request;
        try adapted.object.put(a, "model", .{ .string = backend });
        body = try std.json.Stringify.valueAlloc(a, adapted, .{});
    }
    if (public_id != null and !raw_stream) body = (try ctx.body()) orelse "";
    if (raw_stream) try validateSessionStream(ctx);
    var reserved: ?gateway.Sessions.Reservation = null;
    var reserved_id: ?[]const u8 = null;
    if (public_id == null) {
        var random: [24]u8 = undefined;
        server.inferenceIo().random(&random);
        const id = try a.alloc(u8, std.base64.url_safe_no_pad.Encoder.calcSize(random.len));
        _ = std.base64.url_safe_no_pad.Encoder.encode(id, &random);
        reserved = try server.inference_sessions.reserve(server.owner_alloc, id, principal, connection, now);
        reserved_id = id;
    }
    defer if (reserved) |*reservation| reservation.release(server.owner_alloc);
    var observation = tasks.Observation{
        .allocator = a,
        .request_id = ctx.response.headers.get("x-request-id") orelse try requestId(server, ctx, a),
        .requested = requested,
        .resolved = connection,
        .backend_model = backend_model,
        .executor = "antfly",
        .connection = connection,
        .task = "transcription.session",
        .attempt = 1,
        .started_ns = now,
    };
    defer observation.finish();
    const connections = @import("connections.zig");
    const owner = try server.inferenceConnectionAdmissionOwner(connection, operation);
    if (owner == .caller and !server.tryAcquireInference()) return error.InferenceCapacityUnavailable;
    defer if (owner == .caller) server.releaseInference();
    var client = httpx.Client.initWithConfig(a, server.inferenceIo(), .{ .keep_alive = false });
    defer client.deinit();
    var transport = @import("antfly_runtime_abi").http_bridge.Outbound{ .context = ctx };
    const streaming = std.mem.eql(u8, action, "/events") or std.mem.eql(u8, action, "/stream");
    var session_sink = tasks.SessionSink{ .allocator = ctx.allocator, .public_id = public_id orelse "", .downstream = observation.stream(transport.stream()) };
    defer session_sink.deinit();
    const cancel = httpx.CancellationToken.fromCallback(ctx, struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const httpx.Context = @ptrCast(@alignCast(raw));
            return context.isCancellationRequested();
        }
    }.cancelled);
    var result = connections.invokeInferenceConnectionWithRequest(a, &client, server.cfg.node_config, server.cfg.local_inference_connection_target, server.cfg.secret_store, connection, operation, body, cancel, ctx.application_deadline_ns orelse connections.inferenceInvocationDeadlineNs(), if (streaming) session_sink.sink() else .{}, method, .{ .attempt_observer = observeAttempt(ctx, null), .content_type = ctx.header("content-type") orelse "application/json", .query = if (raw_stream) ctx.request.uri.query else null, .body_source = if (raw_stream) transport.bodySource() else .{} }) catch |err| {
        observation.failure = @errorName(err);
        if (transport.started) return error.InferenceResponseCommitted;
        return err;
    };
    defer result.deinit(a);
    observation.status = result.status;
    observation.usage(result.body);
    if (transport.started) {
        appended = raw_stream and result.status >= 200 and result.status < 300 and !ctx.isCancellationRequested();
        return ctx.response.build();
    }
    _ = ctx.status(result.status);
    if (result.content_type) |value| try ctx.setHeader("content-type", value);
    if (public_id == null and result.status >= 200 and result.status < 300) {
        var response = try parseSessionResponse(a, result.body);
        const backend_id = (try sessionResponseId(response)) orelse return error.InvalidInferenceResponse;
        const id = reserved_id.?;
        // Upstream creation can consume most of the request budget. Translate
        // its wall-clock expiry using the clocks at publication, not admission.
        const completed_ns = @import("antfly_platform").time.monotonicNs();
        const lifetime = try sessionLifetime(response, completed_ns, @import("antfly_platform").time.realtimeNs(), requested_ttl_ns);
        try reserved.?.commit(backend_id, lifetime.expires_ns, lifetime.ttl_ns);
        if (response.object.contains("session_id")) try response.object.put(a, "session_id", .{ .string = id });
        if (response.object.contains("id")) try response.object.put(a, "id", .{ .string = id });
        try ctx.setHeader("x-antfly-inference-affinity", id);
        return ctx.json(response);
    }
    // Missing and gone are terminal deletion outcomes too. Release ownership
    // even when the backend expired or removed the session before this request.
    // Keep the original status for callers and retain ownership on retryable errors.
    if (method == .delete and ((result.status >= 200 and result.status < 300) or result.status == 404 or result.status == 410)) server.inference_sessions.remove(server.owner_alloc, public_id.?);
    if (result.status >= 200 and result.status < 300 and result.body.len > 0) {
        var response = try parseSessionResponse(a, result.body);
        if (response.object.contains("session_id")) try response.object.put(a, "session_id", .{ .string = public_id.? });
        if (response.object.contains("id")) try response.object.put(a, "id", .{ .string = public_id.? });
        const reply = try ctx.json(response);
        appended = method == .post and (raw_stream or std.mem.eql(u8, action, "/audio")) and !ctx.isCancellationRequested();
        return reply;
    }
    _ = ctx.response.body(result.body);
    appended = method == .post and result.status >= 200 and result.status < 300 and (raw_stream or std.mem.eql(u8, action, "/audio")) and !ctx.isCancellationRequested();
    return ctx.response.build();
}

const SessionLifetime = struct { expires_ns: u64, ttl_ns: u64 };

fn sessionLifetime(response: Json, completed_ns: u64, realtime_ns: u64, requested_ttl_ns: ?u64) !SessionLifetime {
    var ttl_ns: u64 = 3600 * std.time.ns_per_s;
    if (response.object.get("expires_at")) |value| {
        if (value != .integer or value.integer <= 0) return error.InvalidInferenceResponse;
        const expiry_ns = @as(u128, @intCast(value.integer)) * std.time.ns_per_s;
        if (expiry_ns <= realtime_ns) return error.InvalidInferenceResponse;
        ttl_ns = @intCast(@min(expiry_ns - realtime_ns, 86400 * std.time.ns_per_s));
    }
    var renewal_ns = requested_ttl_ns orelse ttl_ns;
    // The remaining lifetime may be shorter than the actual idle TTL because
    // of transport latency and the wire timestamp's one-second precision.
    if (response.object.get("created")) |created| if (response.object.get("expires_at")) |expiry| {
        if (created == .integer and expiry == .integer and created.integer >= 0 and expiry.integer > created.integer) {
            const seconds: u64 = @intCast(expiry.integer - created.integer);
            renewal_ns = @as(u64, @min(seconds, 86400)) * std.time.ns_per_s;
        }
    };
    return .{ .expires_ns = completed_ns +| ttl_ns, .ttl_ns = renewal_ns };
}

fn validateSessionStream(ctx: *const httpx.Context) !void {
    if (ctx.query("format")) |value| if (!std.mem.eql(u8, value, "pcm16") and !std.mem.eql(u8, value, "pcm_f32")) return error.InvalidInferenceRequest;
    if (ctx.query("sample_rate")) |value| {
        const rate = std.fmt.parseInt(u32, value, 10) catch return error.InvalidInferenceRequest;
        if (rate < 8000 or rate > 192000) return error.InvalidInferenceRequest;
    }
    if (ctx.query("commit")) |value| if (!std.ascii.eqlIgnoreCase(value, "true") and !std.ascii.eqlIgnoreCase(value, "false") and !std.mem.eql(u8, value, "0") and !std.mem.eql(u8, value, "1")) return error.InvalidInferenceRequest;
}

// Upstream JSON is untrusted transport data, including on HTTP success. Keep
// envelope and handle validation shared by creation and JSON follow-ups.
fn parseSessionResponse(a: std.mem.Allocator, body: []const u8) !Json {
    const value = std.json.parseFromSliceLeaky(Json, a, body, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidInferenceResponse,
    };
    if (value != .object) return error.InvalidInferenceResponse;
    _ = try sessionResponseId(value);
    return value;
}

fn sessionResponseId(value: Json) !?[]const u8 {
    var id: ?[]const u8 = null;
    for ([_][]const u8{ "session_id", "id" }) |field| {
        const candidate = value.object.get(field) orelse continue;
        if (candidate != .string or candidate.string.len == 0 or candidate.string.len > 256) return error.InvalidInferenceResponse;
        const bytes = candidate.string;
        if (std.mem.eql(u8, bytes, ".") or std.mem.eql(u8, bytes, "..")) return error.InvalidInferenceResponse;
        for (bytes) |byte| if (byte <= 32 or byte == 127 or std.mem.indexOfScalar(u8, "/?#%\\", byte) != null) return error.InvalidInferenceResponse;
        if (id) |existing| if (!std.mem.eql(u8, existing, bytes)) return error.InvalidInferenceResponse;
        id = bytes;
    }
    return id;
}

pub fn view(server: anytype) !gateway.View {
    const config = server.cfg.node_config orelse return .{ .providers = &empty_registry };
    return .{ .providers = &config.registry, .connections = &config.connections, .readers = &config.readers, .transcribers = &config.transcribers, .synthesizers = &config.text_to_speech };
}

pub fn disabledResponse(ctx: *httpx.Context) !httpx.Response {
    return ctx.status(403).json(.{ .@"error" = .{ .code = "PublicInferenceDisabled", .type = "permission_error", .message = "Public inference is disabled for this deployment" } });
}

const InvocationState = struct {
    allocator: std.mem.Allocator,
    request_id: ?[]u8 = null,
    // A transport that began sending may have submitted billable work. This is not an
    // acknowledgement of completion, billing, or successful cancellation.
    submitted: std.atomic.Value(bool) = .init(false),
    uncertain: std.atomic.Value(bool) = .init(false),
    fn mark(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.submitted.store(true, .release);
    }
    fn destroy(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.request_id) |id| self.allocator.free(id);
        self.allocator.destroy(self);
    }
};
const invocation_state_key = "antfly.inference.invocation";
fn invocationState(ctx: *const httpx.Context) ?*InvocationState {
    return @ptrCast(@alignCast(ctx.getData(invocation_state_key)));
}
pub fn observeAttempt(ctx: *httpx.Context, delegate: ?httpx.AttemptObserver) httpx.AttemptObserver {
    var result = delegate orelse httpx.AttemptObserver{
        .ptr = ctx,
        .before = struct {
            fn before(_: *anyopaque, _: httpx.AttemptObserver.Context) !void {}
        }.before,
        .after = struct {
            fn after(_: *anyopaque, _: ?*const httpx.Response) void {}
        }.after,
    };
    if (invocationState(ctx)) |state| result.delivery = .{ .context = state, .before_send = InvocationState.mark };
    return result;
}
pub fn timeoutResponse(ctx: *httpx.Context) !httpx.Response {
    const state = invocationState(ctx);
    if (state) |value| if (value.request_id) |id| try ctx.setHeader("x-request-id", id);
    const sent = if (state) |value| value.submitted.load(.acquire) else false;
    const uncertain = if (state) |value| value.uncertain.load(.acquire) else false;
    const submitted: ?bool = if (sent) true else if (uncertain) null else false;
    return ctx.status(504).json(.{ .@"error" = .{
        .message = "Inference invocation deadline expired",
        .type = "inference_error",
        .code = "Timeout",
        .request_id = ctx.response.headers.get("x-request-id"),
        .phase = if (sent) "upstream" else if (uncertain) "execution" else "admission",
        .upstream_submitted = submitted,
        .outcome = if (sent or uncertain) "unknown" else "not_submitted",
        .cancellation = if (sent or uncertain) "requested" else "not_required",
        .retryable = !sent and !uncertain,
    } });
}

pub fn prepare(server: anytype, ctx: *httpx.Context) !void {
    if (!server.publicInferenceEnabled() and !isDiscoveryPath(ctx.request.uri.path)) return error.PublicInferenceDisabled;
    if (ctx.response.headers.get("x-request-id") == null) _ = try requestId(server, ctx, ctx.allocator);
    _ = try attemptBudget(ctx);
    const now = @import("antfly_platform").time.monotonicNs();
    const policy: sources.common_config.Config.PublicInferenceConfig = if (comptime @hasField(@TypeOf(server.*), "cfg"))
        if (server.cfg.node_config) |config| config.public_inference else .{}
    else
        .{};
    const deadline = try policy.deadline(ctx.request.headers.iterator(), now, @import("../request_deadline.zig").platformDeadline(ctx));
    @import("../request_deadline.zig").tightenPlatform(ctx, deadline);
    if (invocationState(ctx) == null) {
        const state = try ctx.allocator.create(InvocationState);
        errdefer ctx.allocator.destroy(state);
        state.* = .{ .allocator = ctx.allocator, .request_id = try ctx.allocator.dupe(u8, ctx.response.headers.get("x-request-id").?) };
        errdefer ctx.allocator.free(state.request_id.?);
        try ctx.setData(invocation_state_key, state, InvocationState.destroy);
    }
    if (deadline <= now) return error.Timeout;
}

fn attemptBudget(ctx: *const httpx.Context) !u8 {
    if (ctx.header("x-antfly-inference-attempts-remaining")) |raw| {
        const value = std.fmt.parseInt(u8, raw, 10) catch return error.InvalidInferenceRequest;
        if (value == 0) return error.AttemptLimitExceeded;
        return @min(@as(u8, 8), value);
    }
    return 8;
}

pub fn execute(server: anytype, ctx: *httpx.Context, task: gateway.Task) !httpx.Response {
    return executeOperation(server, ctx, task, null);
}

/// The registered handler supplies operation identity; URI spelling never selects an adapter.
pub fn executeOperation(server: anytype, ctx: *httpx.Context, task: gateway.Task, operation: ?[]const u8) !httpx.Response {
    return executeOperationWithSources(server, ctx, task, operation, null);
}
pub fn executeOperationWithSources(server: anytype, ctx: *httpx.Context, task: gateway.Task, operation: ?[]const u8, port: ?source_media.Port) !httpx.Response {
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 64 * 1024 * 1024 };
    return executeBounded(server, ctx, task, budget.allocator(), .{ .attempt_limit = try attemptBudget(ctx), .operation = operation, .source_port = port }) catch |err| {
        if (err == error.InferenceResponseCommitted) return err;
        if (budget.exhausted) return error.InferenceRequestTooLarge;
        return err;
    };
}

fn executeBounded(server: anytype, ctx: *httpx.Context, task: gateway.Task, backing: std.mem.Allocator, options: ExecutionOptions) !httpx.Response {
    // All parsing and adapter allocations share request lifetime and a bounded
    // transport body. No executor retains these values after the call.
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const payload = try payloads.Payload.parse(a, ctx);
    var execution = options;
    execution.attachments = payload.attachments;
    const request = payload.value;
    var media_lease = try source_media.resolveOwned(a, request, execution.source_port);
    defer media_lease.deinit();
    if (request != .object) return error.InvalidInferenceRequest;
    if (try generatedSelector(task, request)) |model| return executeGenerated(server, ctx, task, a, request, model, execution);
    var effective = request;
    if (task == .chunk and !effective.object.contains("model")) {
        const config = effective.object.get("config") orelse Json{ .object = .empty };
        if (config != .object) return error.InvalidInferenceRequest;
        try effective.object.put(a, "model", config.object.get("model") orelse .{ .string = "fixed" });
    }
    const model = try string(effective.object.get("model") orelse return error.ModelRequired);
    const named = try view(server);
    const raw_models = if (std.mem.indexOfScalar(u8, model, '/') == null or named.contains(task, model)) try localModels(a, server, task) else &.{};
    const target = try named.resolve(task, model, raw_models);
    if (target == .configured and target.configured.chain) return executeChain(server, ctx, a, request, named, target.configured.name, execution);
    return executeResolved(server, ctx, task, a, effective, named, target, null, false, 1, execution);
}

fn generatedSelector(task: gateway.Task, request: Json) !?[]const u8 {
    const field = switch (task) {
        .decide => "decision_method",
        .extract => "method",
        else => return null,
    };
    const method = request.object.get(field) orelse return null;
    if (method != .string) return error.InvalidInferenceRequest;
    if (!std.mem.eql(u8, method.string, "generated")) return null;
    if (request.object.contains("model")) return error.InvalidInferenceRequest;
    const generator = request.object.get("generator") orelse return error.InvalidInferenceRequest;
    if (generator != .object) return error.InvalidInferenceRequest;
    return try string(generator.object.get("model") orelse return error.ModelRequired);
}

// Generated task envelopes select a generator, not a decider/extractor model.
// Keep their native contract intact, including constrained-output admission.
fn executeGenerated(server: anytype, ctx: *httpx.Context, task: gateway.Task, a: std.mem.Allocator, request: Json, model: []const u8, options: ExecutionOptions) !httpx.Response {
    const named = try view(server);
    const raw_models = if (std.mem.indexOfScalar(u8, model, '/') == null or named.contains(.generate, model)) try localModels(a, server, .generate) else &.{};
    const target = try named.resolve(.generate, model, raw_models);
    var destination: []const u8 = if (server.cfg.local_inference_connection_target != null) sources.common_config.local_inference_connection_id else sources.common_config.default_inference_connection_id;
    var backend: []const u8 = undefined;
    var endpoint: ?[]const u8 = null;
    var api_key: ?[]const u8 = null;
    var generator = request.object.get("generator").?;
    switch (target) {
        .local => |local| {
            if (local.explicit and server.cfg.local_inference_connection_target == null) return error.ModelNotFound;
            backend = local.model;
        },
        .connection => |connection| {
            if (server.cfg.node_config) |config| if (config.connections.get(connection.connection)) |configured| {
                if (configured.kind != .inference or configured.inference == null or !std.mem.eql(u8, configured.inference.?.provider, "antfly")) return error.UnsupportedInferenceFeature;
            };
            backend = connection.model;
            destination = connection.connection;
        },
        .configured => |configured| {
            if (configured.chain) return error.UnsupportedInferenceFeature;
            const cfg = try named.providers.getGeneratorConfig(configured.name);
            if (cfg.provider != .antfly or cfg.rate_limit != null) return error.UnsupportedInferenceFeature;
            generator = try tasks.generatedGenerator(a, cfg, generator);
            backend = cfg.model;
            if (cfg.url.len != 0) endpoint = cfg.url;
            api_key = cfg.api_key;
            if (endpoint == null and server.cfg.local_inference_connection_target == null) {
                if (server.cfg.node_config) |config| endpoint = config.inference.api_url;
                api_key = api_key orelse server.cfg.inference_api_key orelse if (server.cfg.node_config) |config| config.inference.api_key else null;
            }
        },
    }
    try generator.object.put(a, "model", .{ .string = backend });
    var adapted = request;
    try adapted.object.put(a, "generator", generator);
    const encoded = try std.json.Stringify.valueAlloc(a, adapted, .{});
    var observation = tasks.Observation{
        .allocator = a,
        .request_id = ctx.response.headers.get("x-request-id") orelse try requestId(server, ctx, a),
        .requested = model,
        .resolved = try target.idAlloc(a),
        .backend_model = backend,
        .executor = "antfly",
        .task = if (task == .decide) "decide" else "extract",
        .attempt = 1,
        .started_ns = @import("antfly_platform").time.monotonicNs(),
    };
    defer observation.finish();
    const operation = if (task == .decide) "decisions" else "extract";
    return (if (endpoint) |url| forwardEndpoint(server, ctx, url, api_key, operation, encoded, false, &observation, options) else forward(server, ctx, destination, operation, encoded, false, &observation, options)) catch |err| {
        observation.failure = @errorName(err);
        return err;
    };
}

fn executeResolved(server: anytype, ctx: *httpx.Context, task: gateway.Task, a: std.mem.Allocator, request: Json, named: gateway.View, target: gateway.Target, generator_override: ?generating.GeneratorConfig, fallback: bool, attempt: u8, options: ExecutionOptions) anyerror!httpx.Response {
    const requested = try string(request.object.get("model") orelse return error.ModelRequired);
    const id = try target.idAlloc(a);
    // Response construction transfers header ownership. Keep telemetry on the
    // invocation-owned ID even when a completed 504 response is discarded.
    const request_id = if (invocationState(ctx)) |state| state.request_id orelse try requestId(server, ctx, a) else ctx.response.headers.get("x-request-id") orelse try requestId(server, ctx, a);
    const descriptor = if (generator_override == null) try named.describe(a, target) else null;
    var observation = tasks.Observation{ .allocator = a, .request_id = request_id, .requested = requested, .resolved = id, .executor = if (descriptor) |value| value.executor else @tagName(generator_override.?.provider), .backend_model = if (descriptor) |value| value.backend_model else generator_override.?.model, .connection = if (descriptor) |value| value.connection else null, .task = @tagName(task), .attempt = attempt, .started_ns = @import("antfly_platform").time.monotonicNs() };
    defer observation.finish();
    // Legacy task adapters without delivery proof cannot assert that a timed
    // out invocation was never submitted. Preserve an explicit unknown until
    // their transport (or the local executor) supplies stronger evidence.
    if (task != .generate)
        if (invocationState(ctx)) |state| state.uncertain.store(true, .release);
    var response = executeResolvedImpl(server, ctx, task, a, request, named, target, generator_override, fallback, &observation, options) catch |err| {
        observation.failure = @errorName(err);
        return err;
    };
    observation.status = response.status.code;
    if (response.body) |bytes| observation.usage(bytes);
    if (response.status.code == 504 and !ctx.h1_stream_sent and !ctx.h2_stream_sent) {
        response.deinit();
        return error.Timeout;
    }
    return response;
}

fn executeResolvedImpl(server: anytype, ctx: *httpx.Context, task: gateway.Task, a: std.mem.Allocator, raw_request: Json, named: gateway.View, target: gateway.Target, generator_override: ?generating.GeneratorConfig, fallback: bool, observation: *tasks.Observation, execution_options: ExecutionOptions) anyerror!httpx.Response {
    var request = raw_request;
    const model = try string(request.object.get("model") orelse return error.ModelRequired);
    if (target == .connection) if (server.cfg.node_config) |config| if (config.connections.get(target.connection.connection)) |connection| {
        if (connection.kind != .inference) return error.ConnectionNotInference;
        const cfg = connection.inference orelse return error.InvalidInferenceRequest;
        if (!std.mem.eql(u8, cfg.provider, "antfly")) return executeVendorConnection(server, ctx, task, a, request, named, target, connection, fallback, observation, execution_options);
    };
    if (target == .local and target.local.explicit and server.cfg.local_inference_connection_target == null) return error.ModelNotFound;
    if (target != .configured) {
        const destination = if (target == .connection) target.connection.connection else if (server.cfg.local_inference_connection_target != null) sources.common_config.local_inference_connection_id else sources.common_config.default_inference_connection_id;
        const raw_model = if (target == .connection) target.connection.model else target.local.model;
        var forwarded = request;
        try forwarded.object.put(a, "model", .{ .string = raw_model });
        if (task == .chunk) {
            _ = forwarded.object.swapRemove("model");
            var config = forwarded.object.get("config") orelse Json{ .object = .empty };
            if (config != .object) return error.InvalidInferenceRequest;
            try config.object.put(a, "model", .{ .string = raw_model });
            try forwarded.object.put(a, "config", config);
        }
        const encoded = try std.json.Stringify.valueAlloc(a, forwarded, .{});
        return forward(server, ctx, destination, switch (task) {
            .generate => execution_options.operation orelse "generate",
            .embed => execution_options.operation orelse "embeddings",
            .decide => "decisions",
            .read => "read",
            .extract => "extract",
            .rewrite => "rewrite",
            .rerank => "rerank",
            .chunk => "chunk",
            .transcribe => execution_options.operation orelse "transcribe",
            .synthesize => "synthesize",
            .predict => "ml/predict",
        }, encoded, fallback, observation, execution_options);
    }
    if (task == .extract or task == .predict or (task == .rewrite and named.providers.rewriter_configs.get(target.configured.name).?.generator == null)) {
        const binding = if (task == .extract) named.providers.extractor_configs.get(target.configured.name).? else if (task == .rewrite) named.providers.rewriter_configs.get(target.configured.name).? else named.providers.predictor_configs.get(target.configured.name).?;
        var forwarded = request;
        try forwarded.object.put(a, "model", .{ .string = binding.model });
        const encoded = try std.json.Stringify.valueAlloc(a, forwarded, .{});
        return forward(server, ctx, binding.connection, if (task == .predict) "ml/predict" else @tagName(task), encoded, fallback, observation, execution_options);
    }
    if (task == .chunk) {
        const cfg = try named.providers.getChunkerConfig(target.configured.name);
        var forwarded = request;
        _ = forwarded.object.swapRemove("model");
        var config = forwarded.object.get("config") orelse Json{ .object = .empty };
        if (config != .object) return error.InvalidInferenceRequest;
        try config.object.put(a, "model", .{ .string = if (cfg.model.len > 0) cfg.model else "fixed" });
        if (!config.object.contains("max_chunks")) try config.object.put(a, "max_chunks", .{ .integer = cfg.max_chunks });
        if (cfg.threshold) |value| if (!config.object.contains("threshold")) try config.object.put(a, "threshold", .{ .float = value });
        if (!config.object.contains("text")) try config.object.put(a, "text", try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, cfg.text, .{}), .{}));
        try forwarded.object.put(a, "config", config);
        const encoded = try std.json.Stringify.valueAlloc(a, forwarded, .{});
        if (cfg.api_url.len > 0) return forwardEndpoint(server, ctx, cfg.api_url, null, "chunk", encoded, false, observation, execution_options);
        return forward(server, ctx, if (server.cfg.local_inference_connection_target != null) sources.common_config.local_inference_connection_id else sources.common_config.default_inference_connection_id, "chunk", encoded, false, observation, execution_options);
    }
    if (task == .read) {
        const cfg = try named.readers.?.getConfig(target.configured.name);
        if (cfg.provider == .antfly) {
            const encoded = try tasks.readingBody(a, cfg, request);
            // Reads are completed JSON results. Keep the full document envelope
            // available to ordinary and batch callers, including artifact handles.
            var read_options = execution_options;
            read_options.response_delivery = .buffered;
            return forward(server, ctx, try std.fmt.allocPrint(a, "readers/{s}", .{target.configured.name}), "read", encoded, fallback, observation, read_options);
        }
    }
    if (task == .transcribe) {
        const cfg = try named.transcribers.?.getConfig(target.configured.name);
        if (cfg.provider == .antfly) {
            const operation = execution_options.operation orelse "transcribe";
            const encoded = try tasks.transcriptionBody(a, cfg, request);
            return forward(server, ctx, try std.fmt.allocPrint(a, "transcribers/{s}", .{target.configured.name}), operation, encoded, false, observation, execution_options);
        }
    }
    if (task == .embed) {
        const cfg = try named.providers.getEmbedderConfig(target.configured.name);
        if (cfg.provider == .antfly) return executeNativeEmbedding(server, ctx, a, request, named, target.configured.name, observation, execution_options);
    }
    if (task == .generate and execution_options.attachments != null) {
        var cfg = generator_override orelse try named.providers.getGeneratorConfig(target.configured.name);
        if (cfg.provider == .antfly) {
            if (cfg.url.len == 0 and server.cfg.local_inference_connection_target == null) {
                if (server.cfg.node_config) |config| {
                    if (config.inference.api_url) |url| cfg.url = try gateway.apiBaseAlloc(a, url);
                    cfg.api_key = cfg.api_key orelse server.cfg.inference_api_key orelse config.inference.api_key;
                }
            }
            if (cfg.rate_limit != null) return error.UnsupportedLocalRateLimit;
            _ = std.json.parseFromValue(@import("inference_api").GenerateRequest, a, request, .{}) catch |err| return payloads.requestParseError(err);
            try named.capabilities(target).require(try tasks.chatRequirements(request));
            const body = try tasks.chatBody(a, try tasks.withGenerationOptions(cfg, try generationOptions(a, request)), request);
            if (cfg.url.len != 0) return forwardEndpoint(server, ctx, cfg.url, cfg.api_key, execution_options.operation orelse "generate", body, fallback, observation, execution_options);
            return forward(server, ctx, if (server.cfg.local_inference_connection_target != null) sources.common_config.local_inference_connection_id else sources.common_config.default_inference_connection_id, execution_options.operation orelse "generate", body, fallback, observation, execution_options);
        }
    }
    if (execution_options.attachments) |attachments| request = try inlineTaskAttachments(a, task, request, attachments);
    const caller_owns_admission = if (generator_override) |cfg| !(cfg.provider == .antfly and cfg.url.len == 0 and server.antfly_provider != null and server.antfly_provider.?.owns_invocation_admission) else !try ownsLocalAdmission(server, named, target.configured.task, target.configured.name);
    if (caller_owns_admission and !server.tryAcquireInference()) return error.InferenceCapacityUnavailable;
    defer if (caller_owns_admission) server.releaseInference();
    var client = httpx.Client.initWithConfig(a, server.inferenceIo(), .{ .keep_alive = false });
    defer client.deinit();
    const executor = executorFor(server, ctx, &client, named, generator_override);
    const name = target.configured.name;
    switch (task) {
        .generate => {
            const cfg = try executor.generatorConfig(a, name);
            const prepared = try prepareGeneration(a, cfg, request, execution_options.operation orelse "generate");
            if (generator_override == null) try named.capabilities(target).require(prepared.requirements);
            const messages = prepared.messages;
            const options = prepared.options;
            if (cfg.provider == .antfly and cfg.url.len == 0 and server.cfg.local_inference_connection_target != null and !caller_owns_admission) {
                if ((try sources.common_provider_limits.Policy.fromConfig(cfg.rate_limit)).enabled()) return error.UnsupportedLocalRateLimit;
                const configured = try tasks.withGenerationOptions(cfg, options);
                const native_body = try tasks.chatBody(a, configured, request);
                return forward(server, ctx, sources.common_config.local_inference_connection_id, "chat/completions", native_body, fallback, observation, execution_options);
            }
            if ((cfg.usesChatCompletionsProtocol() and (cfg.provider != .antfly or cfg.url.len != 0)) or (cfg.provider == .anthropic or (cfg.provider == .bedrock and if (request.object.get("stream")) |v| v.bool else false))) {
                var transport = @import("antfly_runtime_abi").http_bridge.Outbound{ .context = ctx };
                const streaming = if (request.object.get("stream")) |v| v.bool else false;
                const observed = observation.stream(transport.stream());
                var guard = tasks.FallbackSink{ .downstream = observed };
                var response = executor.chat(a, name, messages, options, request, if (std.mem.eql(u8, execution_options.operation orelse "generate", "generate")) .generate else .chat_completions, if (streaming) (if (fallback) guard.sink() else observed) else .{}) catch |err| {
                    if (transport.started or guard.committed) return error.InferenceResponseCommitted;
                    return guard.failure orelse err;
                };
                defer response.deinit();
                observation.status = response.status;
                observation.usage(response.body);
                if (transport.started) return ctx.response.build();
                if (fallback) if (gateway.transientStatus(response.status)) |failure| return failure;
                _ = ctx.status(response.status);
                if (response.retry_after) |v| try ctx.setHeader("retry-after", v);
                if (response.request_id) |value| try ctx.setHeader("x-antfly-upstream-request-id", value);
                try ctx.setHeader("content-type", response.content_type orelse "application/json");
                _ = ctx.response.body(response.body);
                return ctx.response.build();
            }
            if (request.object.get("stream")) |v| if (v.bool) return error.UnsupportedInferenceFeature;
            var result = try executor.generate(a, name, messages, options);
            defer result.deinit();
            // Serialize through the same contract used by batch validation.
            // Optional message fields must be omitted rather than emitted null.
            const Api = @import("inference_api");
            const Tool = std.meta.Elem(std.meta.Child(@FieldType(Api.GenerateMessage, "tool_calls")));
            const calls = try a.alloc(Tool, result.tool_calls.len);
            for (result.tool_calls, calls) |call, *out| out.* = .{ .id = call.id, .type = "function", .function = .{ .name = call.name, .arguments = call.arguments } };
            var extra: ?std.json.ArrayHashMap(Json) = null;
            if (result.bedrock_content_json) |native| {
                var content: Json = .{ .object = .empty };
                try content.object.put(a, "content", try std.json.parseFromSliceLeaky(Json, a, native, .{}));
                extra = .{};
                try extra.?.map.put(a, "bedrock", content);
            }
            return ctx.json(Api.GenerateResponse{
                .id = try std.fmt.allocPrint(a, "chatcmpl-{d}", .{@import("antfly_platform").time.monotonicNs()}),
                .object = "chat.completion",
                .created = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_s),
                .model = model,
                .choices = &.{.{ .index = 0, .message = .{ .role = .assistant, .content = .{ .value = result.content }, .tool_calls = if (calls.len > 0) calls else null, .extra_content = extra }, .finish_reason = if (result.finish_reason) |reason| std.meta.stringToEnum(Api.FinishReason, @tagName(reason)).? else if (calls.len > 0) .tool_calls else .stop }},
                .usage = if (result.usage) |usage| .{
                    .prompt_tokens = std.math.cast(i64, usage.input_tokens) orelse return error.InvalidInferenceResponse,
                    .completion_tokens = std.math.cast(i64, usage.output_tokens) orelse return error.InvalidInferenceResponse,
                    .total_tokens = std.math.cast(i64, usage.total_tokens orelse (usage.input_tokens +| usage.output_tokens)) orelse return error.InvalidInferenceResponse,
                } else null,
            });
        },
        .decide => {
            try only(request, &.{ "model", "input", "inputs", "questions", "context_version", "contexts" });
            const result = try executor.decideRequest(a, name, request);
            observation.usageJson(result);
            return ctx.json(result);
        },
        .embed => {
            try only(request, &.{ "model", "input", "dimensions", "encoding_format", "model_identity" });
            if (request.object.contains("model_identity")) return error.UnsupportedInferenceFeature;
            const encoding = if (request.object.get("encoding_format")) |v| try string(v) else "float";
            if (!std.mem.eql(u8, encoding, "float") and !std.mem.eql(u8, encoding, "base64")) return error.UnsupportedInferenceFeature;
            const input = request.object.get("input") orelse return error.InvalidInferenceRequest;
            const dimensions: ?u32 = if (request.object.get("dimensions")) |v| std.math.cast(u32, try integer(v)) orelse return error.InvalidInferenceRequest else null;
            const cfg = try named.providers.getEmbedderConfig(name);
            if (input == .array and input.array.items.len > 0 and input.array.items[0] != .string) {
                if (!std.mem.eql(u8, encoding, "float")) return error.UnsupportedInferenceFeature;
                const count = input.array.items.len;
                const Item = struct { object: []const u8 = "embedding", index: usize, embedding: []const f32 };
                const data = try a.alloc(Item, count);
                for (data, 0..) |*item, index| item.* = .{ .index = index, .embedding = try executor.embedParts(a, name, try tasks.embeddingParts(a, input.array.items[index]), dimensions) };
                return ctx.json(.{ .object = "list", .model = model, .data = data });
            }
            const texts: []const []const u8 = switch (input) {
                .string => &.{input.string},
                .array => |items| blk: {
                    if (items.items.len == 0) return error.InvalidInferenceRequest;
                    const values = try a.alloc([]const u8, items.items.len);
                    for (items.items, values) |v, *out| out.* = try string(v);
                    break :blk values;
                },
                else => return error.UnsupportedInferenceFeature,
            };
            if (cfg.sparse orelse false) {
                if (dimensions != null or !std.mem.eql(u8, encoding, "float")) return error.UnsupportedInferenceFeature;
                const vectors = try executor.embedSparse(a, name, texts);
                const Item = struct { object: []const u8 = "embedding", index: usize, embedding: sources.inference_managed_embedder.SparseEmbedding };
                const data = try a.alloc(Item, vectors.len);
                for (vectors, data, 0..) |vector, *item, index| item.* = .{ .index = index, .embedding = vector };
                return ctx.json(.{ .object = "list", .model = model, .data = data });
            }

            const vectors = try executor.embed(a, name, texts, dimensions);
            const Embedding = union(enum) {
                floats: []const f32,
                encoded: []const u8,
                pub fn jsonStringify(self: @This(), writer: anytype) !void {
                    switch (self) {
                        inline else => |value| try writer.write(value),
                    }
                }
            };
            const Item = struct { object: []const u8 = "embedding", index: usize, embedding: Embedding };
            const data = try a.alloc(Item, vectors.len);
            for (vectors, data, 0..) |vector, *item, i| {
                item.* = .{ .index = i, .embedding = .{ .floats = vector } };
                if (std.mem.eql(u8, encoding, "base64")) {
                    const bytes = try a.alloc(u8, vector.len * @sizeOf(f32));
                    for (vector, 0..) |value, index| std.mem.writeInt(u32, bytes[index * 4 ..][0..4], @bitCast(value), .little);
                    const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
                    item.embedding = .{ .encoded = std.base64.standard.Encoder.encode(encoded, bytes) };
                }
            }
            // Existing providers do not all expose usage. Never synthesize it.
            return ctx.json(.{ .object = "list", .model = model, .data = data });
        },
        .read => {
            try only(request, &.{ "model", "images", "prompt", "max_tokens" });
            const images = request.object.get("images") orelse return error.InvalidInferenceRequest;
            if (images != .array or images.array.items.len == 0) return error.InvalidInferenceRequest;
            const urls = try a.alloc([]const u8, images.array.items.len);
            for (images.array.items, urls) |v, *out| out.* = try string(if (v == .object) v.object.get("url") orelse return error.InvalidInferenceRequest else v);
            var result = try executor.read(a, name, .{
                .images = urls,
                .prompt = if (request.object.get("prompt")) |v| try string(v) else null,
                .max_tokens = if (request.object.get("max_tokens")) |v| try integer(v) else null,
            });
            defer result.deinit(a);
            const Item = struct { object: []const u8 = "read", index: usize, text: []const u8, fields: ?Json, regions: ?Json };
            const data = try a.alloc(Item, result.items.len);
            for (result.items, data, 0..) |item, *out, i| out.* = .{
                .index = i,
                .text = item.text,
                .fields = if (item.fields_json) |v| try std.json.parseFromSliceLeaky(Json, a, v, .{}) else null,
                .regions = if (item.regions_json) |v| try std.json.parseFromSliceLeaky(Json, a, v, .{}) else null,
            };
            return ctx.json(.{ .object = "list", .model = model, .data = data, .usage = result.usage, .execution = result.execution });
        },
        .rerank => {
            try only(request, &.{ "model", "query", "documents", "prompts" });
            const query = try string(request.object.get("query") orelse return error.InvalidInferenceRequest);
            const raw = request.object.get("documents") orelse request.object.get("prompts") orelse return error.InvalidInferenceRequest;
            if (raw != .array or raw.array.items.len == 0 or (request.object.contains("documents") and request.object.contains("prompts"))) return error.InvalidInferenceRequest;
            const texts = try a.alloc([]const u8, raw.array.items.len);
            const Part = @import("../reranking/mod.zig").ContentPart;
            const parts = try a.alloc([]const Part, raw.array.items.len);
            var has_parts = false;
            for (raw.array.items, texts, parts) |value, *text, *document| {
                if (value == .string) {
                    text.* = value.string;
                    const single = try a.alloc(Part, 1);
                    single[0] = .{ .text = value.string };
                    document.* = single;
                } else {
                    document.* = try tasks.embeddingParts(a, value);
                    var fragments: std.ArrayList([]const u8) = .empty;
                    for (document.*) |part| switch (part) {
                        .text => |fragment| try fragments.append(a, fragment),
                        else => {},
                    };
                    text.* = try std.mem.join(a, "", fragments.items);
                    has_parts = true;
                }
            }
            var cfg = try named.providers.getRerankerConfig(name);
            if (cfg.provider == .antfly and cfg.url.len == 0 and server.antfly_provider == null) {
                if (executor.inference_api_url) |url| cfg.url = try gateway.apiBaseAlloc(a, url);
                cfg.api_key = cfg.api_key orelse executor.inference_api_key;
            }
            const scores = try @import("../reranking/mod.zig").rerankContentWithOptions(a, if (cfg.provider == .cloudflare) try server.embedding_provider_runtime.httpClient() else &client, cfg, .{
                .antfly_provider = server.antfly_provider,
                .secret_store = server.cfg.secret_store,
                .execution = .{ .default_endpoint = executor.inference_api_url },
                .execution_context = executor.context,
            }, query, .{ .texts = texts, .parts = if (has_parts) parts else null });
            const Item = struct { object: []const u8 = "rerank", index: usize, score: f32 };
            const data = try a.alloc(Item, scores.len);
            for (scores, data, 0..) |score, *item, index| item.* = .{ .index = index, .score = score };
            return ctx.json(.{ .object = "list", .model = model, .data = data });
        },
        .transcribe => {
            const dictation = std.mem.eql(u8, execution_options.operation orelse "transcribe", "dictate");
            try only(request, &.{ "model", "audio", "language", "diarization", "url", "timestamps" });
            const audio_url = if (request.object.get("url")) |value| try string(value) else try std.fmt.allocPrint(a, "data:audio/wav;base64,{s}", .{try string(request.object.get("audio") orelse return error.InvalidInferenceRequest)});
            const result = executor.transcribe(a, name, .{
                .url = audio_url,
                .language = if (request.object.get("language")) |value| try string(value) else null,
                .diarization = if (request.object.get("diarization")) |value| if (value == .bool) value.bool else return error.InvalidInferenceRequest else null,
                .timestamps = if (request.object.get("timestamps")) |value| if (value == .bool) value.bool else return error.InvalidInferenceRequest else null,
            }) catch |err| return switch (err) {
                error.UnsupportedTranscriptionOptions => error.UnsupportedInferenceFeature,
                else => err,
            };
            var speakers: std.ArrayList([]const u8) = .empty;
            if (result.speakers) |values| for (values) |speaker| {
                if (speaker.label orelse speaker.id) |label| try speakers.append(a, label);
            };
            if (dictation) return ctx.json(.{
                .object = "dictation",
                .id = try std.fmt.allocPrint(a, "dictation-{d}", .{@import("antfly_platform").time.monotonicNs()}),
                .created = @divTrunc(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s),
                .model = model,
                .text = result.text orelse "",
                .transcript = .{ .text = result.text orelse "", .language = result.language, .duration_ms = result.duration_ms, .segments = result.segments },
            });
            return ctx.json(.{ .object = "list", .model = model, .data = .{.{ .object = "transcription", .index = @as(usize, 0), .text = result.text orelse "", .language = result.language, .duration_ms = result.duration_ms, .segments = result.segments, .speakers = if (speakers.items.len > 0) speakers.items else null }} });
        },
        .rewrite => {
            const inputs = request.object.get("inputs") orelse return error.InvalidInferenceRequest;
            if (inputs != .array or inputs.array.items.len == 0 or inputs.array.items.len > 128) return error.InvalidInferenceRequest;
            const texts = try a.alloc([]const u8, inputs.array.items.len);
            for (inputs.array.items, texts) |input, *text| text.* = try string(input);
            return ctx.json(try executor.rewrite(a, name, model, texts));
        },
        .synthesize => {
            var adapted = request;
            _ = adapted.object.swapRemove("model");
            const parsed = std.json.parseFromValue(@import("antfly_synthesizing").Request, a, adapted, .{}) catch |err| return payloads.requestParseError(err);
            var result = try executor.synthesize(a, name, parsed.value);
            defer @import("antfly_synthesizing").deinitResult(a, &result);
            if (result.audio) |audio| {
                try ctx.setHeader("content-type", switch (result.response.format orelse .mp3) {
                    .mp3 => "audio/mpeg",
                    .wav => "audio/wav",
                    .opus, .ogg => "audio/ogg",
                    .aac => "audio/aac",
                    .flac => "audio/flac",
                    .pcm => "audio/pcm",
                    .webm => "audio/webm",
                });
                _ = ctx.response.body(audio);
                return ctx.response.build();
            }
            return ctx.json(result.response);
        },
        else => return error.UnsupportedInferenceFeature,
    }
}

fn executorFor(server: anytype, ctx: *httpx.Context, client: *httpx.Client, named: gateway.View, generator_override: ?generating.GeneratorConfig) tasks.Executor {
    const cancellation = httpx.CancellationToken.fromCallback(ctx, struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const httpx.Context = @ptrCast(@alignCast(raw));
            return context.isCancellationRequested();
        }
    }.cancelled);
    const deadline = @import("../request_deadline.zig").platformDeadline(ctx) orelse @import("connections.zig").inferenceInvocationDeadlineNs();
    return .{
        .view = named,
        .generator_override = generator_override,
        .http = client,
        .context = .{ .io = server.inferenceIo(), .deadline_ns = deadline, .submission = if (invocationState(ctx)) |state| .{ .ptr = state, .notify = InvocationState.mark } else null, .cancellation = if (cancellation) |v| @as(@import("antfly_cancellation").CancellationToken, .{ .ptr = v.ptr, .is_cancelled_fn = v.is_cancelled_fn }) else null },
        .antfly_provider = server.antfly_provider,
        .inference_api_url = server.cfg.node_config.?.inference.api_url,
        .inference_api_key = server.cfg.inference_api_key orelse server.cfg.node_config.?.inference.api_key,
        .secret_store = server.cfg.secret_store,
        .provider_runtime = &server.embedding_provider_runtime,
    };
}

fn embeddingIdentity(cfg: @import("antfly_embeddings").Config, request: Json) !?[]const u8 {
    const supplied = request.object.get("model_identity");
    const pin = if (supplied) |value| try string(value) else if (cfg.model_identity.len > 0) cfg.model_identity else return null;
    if (pin.len != 64) return error.InvalidModelIdentity;
    for (pin) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidModelIdentity;
    if (cfg.model_identity.len > 0 and !std.mem.eql(u8, cfg.model_identity, pin)) return error.EmbeddingIdentityMismatch;
    return pin;
}

fn executeNativeEmbedding(server: anytype, ctx: *httpx.Context, a: std.mem.Allocator, request: Json, named: gateway.View, name: []const u8, observation: *tasks.Observation, options: ExecutionOptions) !httpx.Response {
    const cfg = try named.providers.getEmbedderConfig(name);
    const requested = try string(request.object.get("model").?);
    const identity = try embeddingIdentity(cfg, request);
    const encoding = if (request.object.get("encoding_format")) |value| try string(value) else "float";
    const encoded_vectors = std.mem.eql(u8, encoding, "base64");
    if ((!encoded_vectors and !std.mem.eql(u8, encoding, "float")) or (encoded_vectors and (cfg.sparse orelse false))) return error.UnsupportedInferenceFeature;
    var adapted = request;
    if (encoded_vectors) try adapted.object.put(a, "encoding_format", .{ .string = "float" });
    try adapted.object.put(a, "model", .{ .string = cfg.model });
    if (identity) |pin| try adapted.object.put(a, "model_identity", .{ .string = pin });
    if (!adapted.object.contains("dimensions")) if (cfg.dimensions orelse cfg.dimension) |dimensions| {
        try adapted.object.put(a, "dimensions", .{ .integer = dimensions });
    };
    if (!adapted.object.contains("task_type") and cfg.input_type.len > 0) try adapted.object.put(a, "task_type", .{ .string = cfg.input_type });
    const body = try std.json.Stringify.valueAlloc(a, adapted, .{});
    // Embeddings are completed responses. Verify pins and retain native
    // metadata before publishing anything to the caller.
    var execution = options;
    execution.response_delivery = .buffered;
    var response: httpx.Response = undefined;
    if (server.cfg.local_inference_connection_target != null and !sources.inference_managed_embedder.hasRemoteAntflyEndpoint(cfg, server.cfg.node_config.?.inference.api_url)) {
        if ((try sources.common_provider_limits.Policy.fromConfig(cfg.rate_limit)).enabled()) return error.UnsupportedLocalRateLimit;
        response = try forward(server, ctx, sources.common_config.local_inference_connection_id, options.operation orelse "embeddings", body, false, observation, execution);
    } else {
        var client = httpx.Client.initWithConfig(a, server.inferenceIo(), .{ .keep_alive = false });
        defer client.deinit();
        const executor = executorFor(server, ctx, &client, named, null);
        var binding = try executor.nativeEmbedding(a, name, identity);
        defer binding.deinit();
        execution.authorization = binding.controls.authorization;
        execution.attempt_observer = binding.controls.attempt_observer;
        response = try forwardEndpoint(server, ctx, binding.controls.endpoint, null, options.operation orelse "embeddings", body, false, observation, execution);
    }
    if (response.status.code < 200 or response.status.code >= 300) return response;
    defer response.deinit();
    ctx.response.headers.deinit();
    ctx.response.headers = response.headers;
    response.headers = httpx.Headers.init(ctx.allocator);
    var value = std.json.parseFromSliceLeaky(Json, a, response.body orelse return error.InvalidInferenceResponse, .{}) catch return error.InvalidInferenceResponse;
    if (value != .object) return error.InvalidInferenceResponse;
    const data = value.object.get("data") orelse return error.InvalidInferenceResponse;
    if (data != .array) return error.InvalidInferenceResponse;
    if (identity) |pin| {
        const actual = value.object.get("model_identity") orelse return error.InvalidInferenceResponse;
        if (actual != .string or !std.mem.eql(u8, actual.string, pin)) return error.InvalidInferenceResponse;
    }
    if (encoded_vectors) for (data.array.items) |*item| {
        if (item.* != .object) return error.InvalidInferenceResponse;
        const vector = item.object.getPtr("embedding") orelse return error.InvalidInferenceResponse;
        if (vector.* != .array) return error.UnsupportedInferenceFeature;
        const bytes = try a.alloc(u8, vector.array.items.len * @sizeOf(f32));
        for (vector.array.items, 0..) |element, index| {
            const scalar: f32 = @floatCast(number(element) catch return error.InvalidInferenceResponse);
            if (!std.math.isFinite(scalar)) return error.InvalidInferenceResponse;
            std.mem.writeInt(u32, bytes[index * 4 ..][0..4], @bitCast(scalar), .little);
        }
        vector.* = .{ .string = try payloads.base64(a, bytes) };
    };
    try value.object.put(a, "model", .{ .string = requested });
    return ctx.json(value);
}

fn executeVendorConnection(server: anytype, ctx: *httpx.Context, task: gateway.Task, a: std.mem.Allocator, request: Json, original: gateway.View, target: gateway.Target, connection: sources.common_config.Config.ConnectionConfig, fallback: bool, observation: *tasks.Observation, execution_options: ExecutionOptions) !httpx.Response {
    const cfg = connection.inference.?;
    if (std.mem.indexOfAny(u8, target.connection.model, "?%#\\\r\n") != null) return error.InvalidModelIdentity;
    var segments = std.mem.splitScalar(u8, target.connection.model, '/');
    while (segments.next()) |segment| if (segment.len == 0 or std.mem.eql(u8, segment, ".") or std.mem.eql(u8, segment, "..")) return error.InvalidModelIdentity;
    const capability = try std.fmt.allocPrint(a, "models.{s}", .{if (task == .generate) "generate" else if (task == .embed) "embed" else if (task == .decide) "decide" else @tagName(task)});
    try @import("connections.zig").requireInferenceCapability(connection.capabilities, capability);
    switch (task) {
        .extract, .rewrite, .chunk, .predict => return error.UnsupportedInferenceFeature,
        else => {},
    }
    // A request-owned overlay reuses the same typed vendor factories and quota
    // identities. It never mutates the instance registry or copies credentials
    // into a public catalog descriptor.
    var provider: Json = .{ .object = .empty };
    try provider.object.put(a, "provider", .{ .string = cfg.provider });
    try provider.object.put(a, "model", .{ .string = target.connection.model });
    if (cfg.url) |url| {
        try provider.object.put(a, if (task == .read or task == .transcribe or task == .synthesize) "base_url" else "url", .{ .string = url });
    }
    inline for (.{ "api_key", "project_id", "location", "credentials_path" }) |field| if (@field(cfg, field)) |value| {
        try provider.object.put(a, field, .{ .string = value });
    };
    if (task == .generate) if (cfg.workspace_id) |workspace| try provider.object.put(a, "workspace_id", .{ .string = workspace });
    if (task == .embed or task == .generate) {
        if (cfg.region) |value| try provider.object.put(a, "region", .{ .string = value });
        if (task == .embed) if (request.object.get("dimensions")) |value| try provider.object.put(a, "dimensions", value);
    }
    if (task == .decide or task == .generate) {
        if (cfg.max_image_input_tokens) |bound| try provider.object.put(a, "max_image_input_tokens", .{ .integer = std.math.cast(i64, bound) orelse return error.InvalidConfig });
    }
    if (task == .rerank) try provider.object.put(a, "field", .{ .string = "text" });
    var entries: Json = .{ .object = .empty };
    const name = "connection-target";
    try entries.object.put(a, name, provider);
    var root: Json = .{ .object = .empty };
    try root.object.put(a, if (task == .synthesize) "text_to_speech" else task.namespace(), entries);
    var config = try sources.common_config.Config.parseFromSlice(a, try std.json.Stringify.valueAlloc(a, root, .{}));
    defer config.deinit();
    const capability_key = try std.fmt.allocPrint(a, "{s}/{s}", .{ task.namespace(), name });
    try config.registry.inference_capabilities.put(a, capability_key, original.capabilities(target));
    const named = gateway.View{ .providers = &config.registry, .readers = &config.readers, .transcribers = &config.transcribers, .synthesizers = &config.text_to_speech };
    return executeResolvedImpl(server, ctx, task, a, request, named, .{ .configured = .{ .task = task, .name = name } }, null, fallback, observation, execution_options);
}

const PreparedGeneration = struct {
    config: generating.GeneratorConfig,
    requirements: gateway.Requirements,
    messages: []generating.ChatMessage,
    options: tasks.GenerationOptions,
};

// Shared qualification for direct requests and whole-chain preflight. Keep
// native schema validation, adapter option support, and typed message parsing
// together so fallback cannot discover invalid input after a provider call.
// Typed hosted adapters use their existing inline-media contracts. Native
// transports above keep borrowed attachments in their original wire format.
fn inlineTaskAttachments(a: std.mem.Allocator, task: gateway.Task, request: Json, attachments: []const httpx.attachment_envelope.Attachment) !Json {
    var adapted = request;
    const State = struct {
        a: std.mem.Allocator,
        attachments: []const httpx.attachment_envelope.Attachment,
        seen: []bool,
        chat: bool,

        fn take(self: *@This(), value: Json) !?httpx.attachment_envelope.Attachment {
            if (value != .string) return error.InvalidInferenceRequest;
            const index = (try payloads.attachmentIndex(value.string)) orelse return null;
            if (index >= self.attachments.len or self.seen[index]) return error.InvalidInferenceRequest;
            self.seen[index] = true;
            return self.attachments[index];
        }
        fn media(self: *@This(), value: *Json) anyerror!void {
            switch (value.*) {
                .array => |items| for (items.items) |*item| try self.media(item),
                .object => |object| {
                    if (object.get("type")) |kind| if (kind == .string and std.mem.eql(u8, kind.string, "media")) {
                        const data = object.getPtr("data") orelse return error.InvalidInferenceRequest;
                        if (try self.take(data.*)) |attachment| {
                            const mime = object.get("mime_type") orelse return error.InvalidInferenceRequest;
                            if (mime != .string or !std.ascii.eqlIgnoreCase(mime.string, attachment.mime_type)) return error.InvalidInferenceRequest;
                            data.* = .{ .string = try payloads.base64(self.a, attachment.data) };
                        }
                        if (self.chat) {
                            const mime = object.get("mime_type") orelse return error.InvalidInferenceRequest;
                            if (mime != .string or data.* != .string or !std.mem.startsWith(u8, mime.string, "image/")) return error.UnsupportedInferenceFeature;
                            var image: Json = .{ .object = .empty };
                            try image.object.put(self.a, "url", .{ .string = try std.fmt.allocPrint(self.a, "data:{s};base64,{s}", .{ mime.string, data.string }) });
                            value.* = .{ .object = .empty };
                            try value.object.put(self.a, "type", .{ .string = "image_url" });
                            try value.object.put(self.a, "image_url", image);
                        }
                        return;
                    };
                    if (object.get("image_url")) |image| {
                        if (image == .object) {
                            const url = image.object.getPtr("url") orelse return error.InvalidInferenceRequest;
                            if (try self.take(url.*)) |attachment| url.* = .{ .string = try payloads.dataUri(self.a, attachment) };
                        } else if (object.get("type")) |kind| {
                            if (kind == .string and std.mem.eql(u8, kind.string, "input_image")) {
                                const url = object.getPtr("image_url").?;
                                if (try self.take(url.*)) |attachment| url.* = .{ .string = try payloads.dataUri(self.a, attachment) };
                            }
                        }
                    }
                    inline for (.{ "messages", "content", "input", "inputs", "documents", "prompts" }) |field| if (object.getPtr(field)) |child| try self.media(child);
                },
                else => {},
            }
        }
    };
    const seen = try a.alloc(bool, attachments.len);
    @memset(seen, false);
    var state = State{ .a = a, .attachments = attachments, .seen = seen, .chat = task == .generate };
    switch (task) {
        .generate, .embed, .rerank, .decide => try state.media(&adapted),
        .read => {
            const images = request.object.get("images") orelse return error.InvalidInferenceRequest;
            if (images != .array) return error.InvalidInferenceRequest;
            for (images.array.items) |*image| {
                const url = if (image.* == .object) image.object.getPtr("url") orelse return error.InvalidInferenceRequest else image;
                if (try state.take(url.*)) |attachment| url.* = .{ .string = try payloads.dataUri(a, attachment) };
            }
        },
        .transcribe => {
            const audio = request.object.get("audio") orelse return error.InvalidInferenceRequest;
            if (try state.take(audio)) |attachment| {
                const object = &adapted.object;
                if (object.contains("url")) return error.InvalidInferenceRequest;
                _ = object.swapRemove("audio");
                try object.put(a, "url", .{ .string = try payloads.dataUri(a, attachment) });
            }
        },
        else => return error.UnsupportedInferenceFeature,
    }
    for (seen) |used| if (!used) return error.InvalidInferenceRequest;
    return adapted;
}

fn generationOptions(a: std.mem.Allocator, request: Json) !tasks.GenerationOptions {
    return .{
        .max_tokens = if (request.object.get("max_tokens")) |v| try integer(v) else null,
        .max_completion_tokens = if (request.object.get("max_completion_tokens")) |v| try integer(v) else null,
        .temperature = if (request.object.get("temperature")) |v| @floatCast(try number(v)) else null,
        .top_p = if (request.object.get("top_p")) |v| @floatCast(try number(v)) else null,
        .tools_json = if (request.object.get("tools")) |v| try std.json.Stringify.valueAlloc(a, v, .{}) else null,
        .tool_choice_json = if (request.object.get("tool_choice")) |v| try std.json.Stringify.valueAlloc(a, v, .{}) else null,
        .response_format_json = if (request.object.get("response_format")) |v| try std.json.Stringify.valueAlloc(a, v, .{}) else null,
    };
}

fn prepareGeneration(a: std.mem.Allocator, cfg: generating.GeneratorConfig, request: Json, operation: []const u8) !PreparedGeneration {
    if (cfg.provider == .antfly and std.mem.eql(u8, operation, "generate")) {
        _ = std.json.parseFromValue(@import("inference_api").GenerateRequest, a, request, .{}) catch |err| return payloads.requestParseError(err);
    } else try only(request, &.{ "model", "messages", "stream", "max_tokens", "max_completion_tokens", "temperature", "tools", "tool_choice", "response_format", "top_p", "frequency_penalty", "presence_penalty", "stream_options", "seed", "stop" });
    var requirements = try tasks.chatRequirements(request);
    if (cfg.tools_json != null) requirements.tools = true;
    const rich_adapter = cfg.usesChatCompletionsProtocol();
    if (!rich_adapter) try only(request, &.{ "model", "messages", "stream", "max_tokens", "max_completion_tokens", "temperature", "top_p", "tools", "tool_choice", "response_format", "stream_options" });
    // Media serialization is independent of raw response/stream forwarding.
    // Image admission remains available on the native Google path when
    // native-only options select it instead of chat-completions forwarding.
    if (requirements.images and !generationAdapterSupportsImages(cfg.provider)) return error.UnsupportedInferenceFeature;
    if (requirements.streaming and !cfg.supportsStreamingChat()) return error.UnsupportedInferenceFeature;
    if (!rich_adapter and requirements.audio) return error.UnsupportedInferenceFeature;
    if (!rich_adapter and requirements.structured_output and cfg.provider != .bedrock and cfg.provider != .anthropic) return error.UnsupportedInferenceFeature;
    const messages = try tasks.chatMessages(a, request);
    const options = try generationOptions(a, request);
    const configured = try tasks.withGenerationOptions(cfg, options);
    if (rich_adapter) try tasks.validateChatConfig(configured);
    return .{ .config = configured, .requirements = requirements, .messages = messages, .options = options };
}

fn generationAdapterSupportsImages(provider: generating.Provider) bool {
    return switch (provider) {
        .cloudflare, .openai, .openrouter, .ollama, .antfly, .gemini, .vertex, .bedrock, .anthropic => true,
        .apple, .mock, .chatgpt => false,
    };
}

test "httpx inference gateway image admission is independent of streaming and model names" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try std.json.parseFromSliceLeaky(Json, a,
        \\{"model":"generators/vision","messages":[{"role":"user","content":[{"type":"text","text":"Compare the character sheets"},{"type":"image_url","image_url":{"url":"data:image/jpeg;base64,aW1hZ2U="}}]}]}
    , .{});
    for ([_]generating.Provider{ .gemini, .vertex, .openai, .openrouter, .ollama, .antfly, .bedrock }) |provider| {
        const prepared = try prepareGeneration(a, .{ .provider = provider, .model = "new-vision-model", .url = "https://fixture" }, request, "chat/completions");
        try std.testing.expect(prepared.requirements.images);
        try std.testing.expectEqualStrings("data:image/jpeg;base64,aW1hZ2U=", prepared.messages[0].content.?.parts[1].image_url.url);
        try std.testing.expectError(error.UnsupportedInferenceFeature, (gateway.Capabilities{}).require(prepared.requirements));
        try (gateway.Capabilities{ .images = .supported }).require(prepared.requirements);
        if (provider == .gemini or provider == .vertex) {
            var streaming = request;
            streaming.object = try request.object.clone(a);
            try streaming.object.put(a, "stream", .{ .bool = true });
            const stream_plan = try prepareGeneration(a, prepared.config, streaming, "chat/completions");
            try std.testing.expectError(error.UnsupportedInferenceFeature, (gateway.Capabilities{ .images = .supported }).require(stream_plan.requirements));
            try (gateway.Capabilities{ .images = .supported, .streaming = .supported }).require(stream_plan.requirements);
        }
    }
    for ([_]generating.Provider{ .apple, .mock }) |provider| {
        try std.testing.expectError(error.UnsupportedInferenceFeature, prepareGeneration(a, .{ .provider = provider, .model = "text-only", .url = "https://fixture" }, request, "chat/completions"));
    }
}

fn executeChain(server: anytype, ctx: *httpx.Context, a: std.mem.Allocator, request: Json, named: gateway.View, name: []const u8, options: ExecutionOptions) !httpx.Response {
    const links = try named.providers.getChain(name);
    if (links.len == 0 or links.len > 8) return error.InvalidInferenceRequest;
    const configs = try a.alloc(generating.GeneratorConfig, links.len);
    const original = try std.json.Stringify.valueAlloc(a, request, .{});
    // Qualify every link through the same request adapter used for execution.
    // Each receives its own JSON tree because body adaptation mutates objects.
    for (links, configs) |link, *cfg| {
        const qualified: gateway.Target = .{ .configured = .{ .task = .generate, .name = link.generator_name orelse name, .chain = link.generator_name == null } };
        const candidate = try std.json.parseFromSliceLeaky(Json, a, original, .{});
        const prepared = try prepareGeneration(a, link.generator_config orelse try named.providers.getGeneratorConfig(link.generator_name.?), candidate, options.operation orelse "generate");
        cfg.* = prepared.config;
        try named.capabilities(qualified).require(prepared.requirements);
        if (cfg.provider != .cloudflare and cfg.provider != .openai and cfg.provider != .openrouter and cfg.provider != .ollama and cfg.provider != .antfly and cfg.provider != .gemini and cfg.provider != .vertex and cfg.provider != .bedrock and cfg.provider != .anthropic) return error.UnsupportedInferenceFeature;
    }
    const previous_deadline = ctx.application_deadline_ns;
    const previous_clock = ctx.application_deadline_io;
    defer {
        ctx.application_deadline_ns = previous_deadline;
        ctx.application_deadline_io = previous_clock;
    }
    const deadline = @import("../request_deadline.zig").platformDeadline(ctx) orelse @import("connections.zig").inferenceInvocationDeadlineNs();
    ctx.application_deadline_ns = deadline;
    ctx.application_deadline_io = null;
    const budget = options.attempt_limit;
    var attempts = gateway.Attempts{ .deadline_ns = deadline, .remaining = budget };
    var failure: anyerror = error.InferenceUpstreamUnavailable;
    for (links, configs, 0..) |link, cfg, index| {
        if (index != 0 and !gateway.permitsFallback(links[index - 1].condition, failure)) return failure;
        const count = @min(@as(u32, 8), if (link.retry) |retry| retry.max_attempts else 1);
        var backoff_ms: u32 = if (link.retry) |retry| retry.initial_backoff_ms else 0;
        for (0..count) |retry_index| {
            if (retry_index != 0 and !gateway.permitsFallback(.on_error, failure) and !gateway.permitsFallback(link.condition, failure)) break;
            if (retry_index > 0) {
                var wait_ms = backoff_ms;
                while (wait_ms > 0) {
                    if (ctx.isCancellationRequested()) return error.Canceled;
                    const now = @import("antfly_platform").time.monotonicNs();
                    if (now >= deadline) return error.Timeout;
                    const slice_ms = @min(@as(u32, 50), wait_ms);
                    try server.inferenceIo().sleep(.fromMilliseconds(slice_ms), .awake);
                    wait_ms -= slice_ms;
                }
                if (link.retry) |retry| backoff_ms = @intFromFloat(@min(@as(f32, @floatFromInt(retry.max_backoff_ms)), @as(f32, @floatFromInt(backoff_ms)) * retry.backoff_multiplier));
            }
            try attempts.begin(@import("antfly_platform").time.monotonicNs());
            const candidate = try std.json.parseFromSliceLeaky(Json, a, original, .{});
            const target: gateway.Target = .{ .configured = .{ .task = .generate, .name = link.generator_name orelse name, .chain = link.generator_name == null } };
            return executeResolved(server, ctx, .generate, a, candidate, named, target, cfg, true, budget - attempts.remaining, options) catch |err| {
                if (err == error.InferenceResponseCommitted) return err;
                failure = err;
                // Invalid requests, credentials, refusals and malformed output
                // are never reinterpreted as a reason to switch models.
                if (!gateway.permitsFallback(.on_error, err) and err != error.Timeout) return err;
                continue;
            };
        }
    }
    return failure;
}

pub fn batch(server: anytype, ctx: *httpx.Context) !httpx.Response {
    return batchWithSources(server, ctx, null);
}
pub fn batchWithSources(server: anytype, ctx: *httpx.Context, port: ?source_media.Port) !httpx.Response {
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 64 << 20 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    const payload = try payloads.Payload.parse(a, ctx);
    const request = payload.value;
    try only(request, &.{ "mode", "requests" });
    if (request.object.get("mode")) |mode| if (!std.mem.eql(u8, try string(mode), "sync")) return error.UnsupportedInferenceFeature;
    const items = request.object.get("requests") orelse return error.InvalidInferenceRequest;
    if (items != .array or items.array.items.len == 0 or items.array.items.len > 128) return error.InvalidInferenceRequest;
    // Validate every envelope before executing any item.
    for (items.array.items) |item| {
        try only(item, &.{ "custom_id", "body" });
        _ = try string(item.object.get("custom_id") orelse return error.InvalidInferenceRequest);
        const value = item.object.get("body") orelse return error.InvalidInferenceRequest;
        if (value != .object or !value.object.contains("model")) return error.ModelRequired;
        if (value.object.get("stream")) |stream| if (stream != .bool or stream.bool) return error.UnsupportedInferenceFeature;
    }
    const item_payloads = try a.alloc(payloads.Payload, items.array.items.len);
    if (payload.attachments) |attachments| {
        const seen = try a.alloc(bool, attachments.len);
        @memset(seen, false);
        for (items.array.items, item_payloads) |item, *out| out.* = try payloads.batchItem(a, item.object.get("body").?, attachments, seen);
        for (seen) |used| if (!used) return error.InvalidInferenceRequest;
    } else for (items.array.items, item_payloads) |item, *out| out.* = .{ .value = item.object.get("body").? };
    const Result = struct { custom_id: []const u8, index: usize, response: ?Json = null, @"error": ?struct { code: []const u8, message: []const u8, retryable: bool } = null };
    const results = try a.alloc(Result, items.array.items.len);
    var succeeded: usize = 0;
    const deadline = @import("../request_deadline.zig").platformDeadline(ctx) orelse @import("connections.zig").inferenceInvocationDeadlineNs();
    const options = ExecutionOptions{ .response_delivery = .buffered, .attempt_limit = try attemptBudget(ctx), .source_port = port };
    for (items.array.items, results, 0..) |item, *result, index| {
        result.* = .{ .custom_id = item.object.get("custom_id").?.string, .index = index };
        if (ctx.isCancellationRequested()) return error.Canceled;
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        var item_request = try httpx.Request.init(a, .POST, "http://gateway/ai/v1/generate");
        defer item_request.deinit();
        item_request.body = try item_payloads[index].encode(a);
        try item_request.headers.set("content-type", item_payloads[index].contentType());
        var item_context = httpx.Context.init(a, ctx.io, &item_request);
        defer item_context.deinit();
        item_context.application_deadline_ns = deadline;
        if (invocationState(ctx)) |state| try item_context.setData(invocation_state_key, state, null);
        item_context.cancellation_probe = .{ .ptr = ctx, .is_cancelled = struct {
            fn cancelled(raw: ?*const anyopaque) bool {
                const outer: *const httpx.Context = @ptrCast(@alignCast(raw.?));
                return outer.isCancellationRequested();
            }
        }.cancelled };
        var response = executeBounded(server, &item_context, .generate, a, options) catch |err| {
            if (err == error.Canceled or err == error.Cancelled or err == error.Timeout or err == error.OutOfMemory) return err;
            result.@"error" = .{ .code = @errorName(err), .message = @errorName(err), .retryable = gateway.permitsFallback(.on_error, err) };
            continue;
        };
        defer response.deinit();
        if (response.status.code >= 200 and response.status.code < 300) {
            result.response = parseBatchItemResponse(a, response.body) catch |err| {
                if (err == error.OutOfMemory) return err;
                result.@"error" = .{ .code = "InvalidInferenceResponse", .message = "Upstream returned an invalid generation response", .retryable = false };
                continue;
            };
            succeeded += 1;
        } else result.@"error" = .{ .code = "upstream_error", .message = "Inference request failed", .retryable = gateway.transientStatus(response.status.code) != null };
    }
    return ctx.json(.{ .object = "generate.batch", .data = results, .summary = .{ .total = results.len, .succeeded = succeeded, .failed = results.len - succeeded }, .execution = .{ .requested_items = results.len, .serial_items = results.len } });
}

fn parseBatchItemResponse(a: std.mem.Allocator, body: ?[]const u8) !Json {
    const parsed = std.json.parseFromSliceLeaky(Json, a, body orelse return error.InvalidInferenceResponse, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidInferenceResponse,
    };
    if (parsed != .object) return error.InvalidInferenceResponse;
    // Validate the shared generation contract, then return the original tree:
    // provider extensions (including refusal and usage details) must survive.
    const response = std.json.parseFromValue(@import("inference_api").GenerateResponse, a, parsed, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidInferenceResponse,
    };
    defer response.deinit();
    if (!std.mem.eql(u8, response.value.object, "chat.completion")) return error.InvalidInferenceResponse;
    return parsed;
}

fn discoveryContext(server: anytype, ctx: *httpx.Context) @import("antfly_inference_execution_context").RequestContext {
    return .{ .io = server.inferenceIo(), .deadline_ns = ctx.application_deadline_ns, .submission = if (invocationState(ctx)) |state| .{ .ptr = state, .notify = InvocationState.mark } else null, .cancellation = .{ .ptr = ctx, .is_cancelled_fn = struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const httpx.Context = @ptrCast(@alignCast(raw));
            return context.isCancellationRequested();
        }
    }.cancelled } };
}

pub fn predictors(server: anytype, ctx: *httpx.Context) !httpx.Response {
    if (!server.publicInferenceEnabled()) return ctx.json(.{ .object = "list", .predictors = .{}, .public_inference_enabled = false });
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 64 << 20 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    return ctx.json(.{ .object = "list", .predictors = try predictorCatalog(arena.allocator(), server, discoveryContext(server, ctx)) });
}

// Both inventory surfaces publish the same validated native predictor schema.
// A configured alias remains in OpenAI model data when its schema is offline,
// but an absent schema never becomes evidence that a predictor is callable.
fn predictorCatalog(a: std.mem.Allocator, server: anytype, control: @import("antfly_inference_execution_context").RequestContext) !Json {
    try control.check();
    const named = try view(server);
    var result: Json = .{ .object = .empty };
    const config = server.cfg.node_config;
    var client = httpx.Client.initWithConfig(a, control.io, .{ .keep_alive = false });
    defer client.deinit();
    const connections = @import("connections.zig");
    const deadline = @min(@import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, control.deadline_ns orelse std.math.maxInt(u64));
    const cancel = httpx.CancellationToken.fromCallback(&control, struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const @import("antfly_inference_execution_context").RequestContext = @ptrCast(@alignCast(raw));
            context.check() catch return true;
            return false;
        }
    }.cancelled);
    var transports = try predictorTransports(a, named, config, if (server.cfg.local_inference_connection_target != null) "local-inference" else "default-inference");
    for (transports.keys(), transports.values(), 0..) |connection, *items_by_name, index| {
        try control.check();
        if (@import("antfly_platform").time.monotonicNs() >= deadline) break;
        var response = connections.invokeInferenceConnectionWithMethod(a, &client, config, server.cfg.local_inference_connection_target, server.cfg.secret_store, connection, "ml/models", "", cancel, deadline, .{}, .get) catch {
            try control.check();
            continue;
        };
        defer response.deinit(a);
        try control.check();
        if (response.status != 200) continue;
        const parsed = std.json.parseFromSliceLeaky(Json, a, response.body, .{ .allocate = .alloc_always }) catch continue;
        if (parsed != .object) continue;
        const items = parsed.object.get("predictors") orelse continue;
        if (items != .object) continue;
        for (items.object.keys(), items.object.values()) |name, info| {
            const typed = std.json.parseFromValue(@import("inference_api").PredictorInfo, a, info, .{ .ignore_unknown_fields = true }) catch continue;
            if (typed.value.num_features <= 0 or typed.value.num_outputs <= 0) continue;
            if (typed.value.feature_names) |names| if (names.len != @as(u64, @intCast(typed.value.num_features))) continue;
            try items_by_name.object.put(a, name, info);
            if (index == 0) try result.object.put(a, name, info);
            // Match the public identity used by general connection discovery.
            const id = try std.fmt.allocPrint(a, "connections/{s}/models/{s}", .{ connection, name });
            try result.object.put(a, id, info);
        }
    }
    for (named.providers.predictor_configs.keys(), named.providers.predictor_configs.values()) |alias, binding| {
        const items = transports.get(binding.connection) orelse continue;
        const info = items.object.get(binding.model) orelse continue;
        try result.object.put(a, try std.fmt.allocPrint(a, "predictors/{s}", .{alias}), info);
    }
    try control.check();
    return result;
}

fn predictorTransports(a: std.mem.Allocator, named: gateway.View, config: ?*const sources.common_config.Config, default_connection: []const u8) !std.StringArrayHashMapUnmanaged(Json) {
    const connections = @import("connections.zig");
    // Select each transport once before doing I/O. Alias transports retain
    // priority; explicit connections share the same sixteen-transport ceiling.
    var transports: std.StringArrayHashMapUnmanaged(Json) = .{};
    try transports.put(a, default_connection, .{ .object = .empty });
    for (named.providers.predictor_configs.values()) |binding| {
        if (transports.count() >= 16) break;
        try transports.put(a, binding.connection, .{ .object = .empty });
    }
    if (config) |cfg| for (cfg.connections.keys(), cfg.connections.values()) |id, connection| {
        if (transports.count() >= 16) break;
        if (connection.kind != .inference) continue;
        const inference = connection.inference orelse continue;
        if (!std.mem.eql(u8, inference.provider, "antfly")) continue;
        connections.requireInferenceCapability(connection.capabilities, "models.predict") catch continue;
        if (!transports.contains(id)) try transports.put(a, id, .{ .object = .empty });
    };
    return transports;
}

pub fn localModels(a: std.mem.Allocator, server: anytype, task: gateway.Task) ![]const []const u8 {
    const provider = server.antfly_provider orelse return &.{};
    const list = provider.list_models_json orelse return error.ModelInventoryUnavailable;
    const bytes = try list(provider.ptr, a);
    const root = try std.json.parseFromSliceLeaky(Json, a, bytes, .{});
    if (root != .object) return error.ModelInventoryUnavailable;
    const group = root.object.get(task.namespace()) orelse return &.{};
    if (group != .object) return error.ModelInventoryUnavailable;
    return group.object.keys();
}

pub fn models(server: anytype, ctx: *httpx.Context) !httpx.Response {
    const control = discoveryContext(server, ctx);
    try control.check();
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 64 << 20 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    var root: Json = .{ .object = .empty };
    if (server.publicInferenceEnabled()) if (server.antfly_provider) |provider| if (provider.list_models_json) |list| {
        const bytes = list(provider.ptr, a) catch null;
        if (bytes) |value| root = std.json.parseFromSliceLeaky(Json, a, value, .{}) catch .{ .object = .empty };
    };
    if (root != .object) root = .{ .object = .empty };
    if (!root.object.contains("allow_downloads")) try root.object.put(a, "allow_downloads", .{ .bool = false });
    if (!root.object.contains("backends")) try root.object.put(a, "backends", .{ .object = .empty });
    inline for (.{ "chunkers", "rerankers", "embedders", "generators", "extractors", "deciders", "rewriters", "readers", "transcribers" }) |category| {
        if (!root.object.contains(category)) try root.object.put(a, category, .{ .object = .empty });
    }
    try root.object.put(a, "public_inference_enabled", .{ .bool = server.publicInferenceEnabled() });
    if (!server.publicInferenceEnabled()) {
        try root.object.put(a, "object", .{ .string = "list" });
        try root.object.put(a, "data", .{ .array = std.array_list.Managed(Json).init(a) });
        return ctx.json(root);
    }
    const named = try view(server);
    var data: Json = root.object.get("data") orelse .{ .array = std.array_list.Managed(Json).init(a) };
    if (data != .array) data = .{ .array = std.array_list.Managed(Json).init(a) };
    var seen: std.StringHashMapUnmanaged(usize) = .empty;
    for (data.array.items, 0..) |item, index| {
        if (item == .object) if (item.object.get("id")) |id| {
            if (id == .string) try seen.put(a, id.string, index);
        };
    }
    for (try named.catalog(a)) |target| {
        const id = try target.idAlloc(a);
        const item = try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, try named.describe(a, target), .{}), .{});
        try putCatalogItem(a, &seen, &data, item);
        const category = if (target.configured.chain) "chains" else target.configured.task.namespace();
        var group = root.object.get(category) orelse Json{ .object = .empty };
        if (group != .object) return error.ModelInventoryUnavailable;
        if (target.configured.task != .predict) try group.object.put(a, id, item);
        try root.object.put(a, category, group);
    }
    // Reuse connection discovery's bounded, credential-scoped cache and its
    // configured-model overlay. Listing failure never removes named aliases.
    const connection_bytes = try server.listConnectionsJsonAllocWithContext(a, "inference", "models", null, control);
    const connections = try std.json.parseFromSliceLeaky(Json, a, connection_bytes, .{});
    if (connections.object.get("connections")) |items| for (items.array.items) |connection| {
        const id = try string(connection.object.get("id") orelse continue);
        if (std.mem.eql(u8, id, "named-inference")) continue;
        const inference = connection.object.get("inference") orelse continue;
        if (inference != .object) continue;
        const groups = inference.object.get("models") orelse continue;
        if (groups != .object) continue;
        var it = groups.object.iterator();
        while (it.next()) |group| {
            if (group.value_ptr.* != .array) continue;
            for (group.value_ptr.array.items) |listed| {
                const backend = try string(listed.object.get("name") orelse continue);
                const public_id = try std.fmt.allocPrint(a, "connections/{s}/models/{s}", .{ id, backend });
                var item: Json = .{ .object = .empty };
                try item.object.put(a, "id", .{ .string = public_id });
                try item.object.put(a, "object", .{ .string = "model" });
                try item.object.put(a, "owned_by", .{ .string = "antfly" });
                try item.object.put(a, "connection", .{ .string = id });
                try item.object.put(a, "backend_model", .{ .string = backend });
                if (gateway.taskForCategory(group.key_ptr.*)) |task| {
                    try item.object.put(a, "task", .{ .string = @tagName(task) });
                    var tasks_list = std.array_list.Managed(Json).init(a);
                    try tasks_list.append(.{ .string = @tagName(task) });
                    try item.object.put(a, "tasks", .{ .array = tasks_list });
                }
                try item.object.put(a, "availability", connection.object.get("status") orelse .{ .string = "unknown" });
                try item.object.put(a, "freshness", listed.object.get("freshness") orelse .{ .string = "cached" });
                try item.object.put(a, "gateway_capabilities", try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, connectionCatalogCapabilities(named, group.key_ptr.*, id, backend), .{}), .{}));
                try putCatalogItem(a, &seen, &data, item);
                var catalog_group = root.object.get(group.key_ptr.*) orelse Json{ .object = .empty };
                if (catalog_group != .object) continue;
                try catalog_group.object.put(a, public_id, item);
                try root.object.put(a, group.key_ptr.*, catalog_group);
            }
        }
    };
    // ML groups contain native schemas, never generic model descriptors.
    try root.object.put(a, "predictors", try predictorCatalog(a, server, control));
    try control.check();
    try root.object.put(a, "object", .{ .string = "list" });
    try root.object.put(a, "data", data);
    return ctx.json(root);
}

fn connectionCatalogCapabilities(named: gateway.View, category: []const u8, connection: []const u8, model: []const u8) gateway.Capabilities {
    // Opaque groups retain declarations without claiming a task. Known groups
    // must use the same task-specific adapter contract as invocation.
    return named.capabilities(.{ .connection = .{ .task = gateway.taskForCategory(category) orelse .generate, .connection = connection, .model = model } });
}

fn putCatalogItem(a: std.mem.Allocator, seen: *std.StringHashMapUnmanaged(usize), data: *Json, item: Json) !void {
    const id = try string(item.object.get("id") orelse return error.ModelInventoryUnavailable);
    const entry = try seen.getOrPut(a, id);
    if (entry.found_existing) {
        const previous = data.array.items[entry.value_ptr.*];
        var merged = item;
        var supported = std.array_list.Managed(Json).init(a);
        var roles: std.EnumSet(gateway.Task) = .empty;
        for ([_]Json{ previous, item }) |descriptor| {
            if (descriptor != .object) continue;
            if (descriptor.object.get("tasks")) |list| if (list == .array) {
                for (list.array.items) |role| try appendCatalogRole(&supported, &roles, role);
            };
            if (descriptor.object.get("task")) |role| try appendCatalogRole(&supported, &roles, role);
        }
        if (supported.items.len > 0) {
            merged = try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, item, .{}), .{});
            try merged.object.put(a, "tasks", .{ .array = supported });
            if (supported.items.len == 1) try merged.object.put(a, "task", supported.items[0]) else _ = merged.object.swapRemove("task");
        }
        data.array.items[entry.value_ptr.*] = merged;
    } else {
        entry.value_ptr.* = data.array.items.len;
        try data.array.append(item);
    }
}

fn appendCatalogRole(supported: *std.array_list.Managed(Json), roles: *std.EnumSet(gateway.Task), value: Json) !void {
    if (value != .string) return;
    const task = std.meta.stringToEnum(gateway.Task, value.string) orelse return;
    if (roles.contains(task)) return;
    try supported.append(.{ .string = @tagName(task) });
    roles.insert(task);
}

fn forward(server: anytype, ctx: *httpx.Context, connection: []const u8, operation: []const u8, body: []const u8, fallback: bool, observation: *tasks.Observation, options: ExecutionOptions) !httpx.Response {
    const bridge = @import("antfly_runtime_abi").http_bridge;
    const connections = @import("connections.zig");
    observation.connection = connection;
    const owner = try server.inferenceConnectionAdmissionOwner(connection, operation);
    if (owner == .caller and !server.tryAcquireInference()) return error.InferenceCapacityUnavailable;
    defer if (owner == .caller) server.releaseInference();
    const cancellation = httpx.CancellationToken.fromCallback(ctx, struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const httpx.Context = @ptrCast(@alignCast(raw));
            return context.isCancellationRequested();
        }
    }.cancelled);
    var transport = bridge.Outbound{ .context = ctx };
    const observed = observation.stream(transport.stream());
    var guard = tasks.FallbackSink{ .downstream = observed };
    const encoded = try payloads.encodeMetadata(ctx.allocator, body, options.attachments);
    defer if (options.attachments != null) ctx.allocator.free(encoded);
    var owned_client: ?httpx.Client = null;
    defer if (owned_client) |*client| client.deinit();
    const client: ?*httpx.Client = if (owner == .target) null else if (std.mem.eql(u8, operation, "read"))
        try server.embedding_provider_runtime.assetHttpClient()
    else blk: {
        owned_client = httpx.Client.initWithConfig(ctx.allocator, server.inferenceIo(), .{ .keep_alive = false });
        break :blk &owned_client.?;
    };
    var result = connections.invokeInferenceConnectionWithRequest(ctx.allocator, client, server.cfg.node_config, server.cfg.local_inference_connection_target, server.cfg.secret_store, connection, operation, encoded, cancellation, ctx.application_deadline_ns orelse connections.inferenceInvocationDeadlineNs(), if (options.response_delivery == .buffered) .{} else if (fallback) guard.sink() else observed, .post, .{ .content_type = if (options.attachments != null) httpx.attachment_envelope.content_type else "application/json", .attempt_observer = observeAttempt(ctx, options.attempt_observer) }) catch |err| {
        if (transport.started or guard.committed) return error.InferenceResponseCommitted;
        return guard.failure orelse err;
    };
    defer result.deinit(ctx.allocator);
    observation.status = result.status;
    observation.usage(result.body);
    if (transport.started) return ctx.response.build();
    if (fallback) if (gateway.transientStatus(result.status)) |failure| return failure;
    _ = ctx.status(result.status);
    if (result.content_type) |v| try ctx.setHeader("content-type", v);
    if (result.retry_after) |v| try ctx.setHeader("retry-after", v);
    _ = ctx.response.body(result.body);
    return ctx.response.build();
}

fn forwardEndpoint(server: anytype, ctx: *httpx.Context, endpoint: []const u8, api_key: ?[]const u8, operation: []const u8, body: []const u8, fallback: bool, observation: *tasks.Observation, execution_options: ExecutionOptions) !httpx.Response {
    if (!server.tryAcquireInference()) return error.InferenceCapacityUnavailable;
    defer server.releaseInference();
    const a = ctx.allocator;
    var client = httpx.Client.initWithConfig(a, server.inferenceIo(), .{ .keep_alive = false });
    defer client.deinit();
    const base = try gateway.apiBaseAlloc(a, endpoint);
    defer a.free(base);
    const url = try std.fmt.allocPrint(a, "{s}/{s}", .{ base, operation });
    defer a.free(url);
    const resolved = if (api_key) |value| try sources.common_secrets.resolveReferenceOwned(a, server.cfg.secret_store, value) else null;
    defer if (resolved) |value| a.free(value);
    const owned_authorization = if (resolved) |value| try std.fmt.allocPrint(a, "Bearer {s}", .{value}) else null;
    defer if (owned_authorization) |value| a.free(value);
    const authorization = execution_options.authorization orelse owned_authorization;
    const cancellation = httpx.CancellationToken.fromCallback(ctx, struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const httpx.Context = @ptrCast(@alignCast(raw));
            return context.isCancellationRequested();
        }
    }.cancelled);
    const timeout_ms = try @import("connections.zig").remainingInferenceInvocationMs(@import("../request_deadline.zig").platformDeadline(ctx) orelse @import("connections.zig").inferenceInvocationDeadlineNs(), @import("antfly_platform").time.monotonicNs());
    const timeout_header = try std.fmt.allocPrint(a, "{d}", .{timeout_ms});
    defer a.free(timeout_header);
    var headers = std.ArrayList([2][]const u8).empty;
    defer headers.deinit(a);
    try headers.append(a, .{ "X-Antfly-Inference-Attempts-Remaining", "1" });
    try headers.append(a, .{ "X-Antfly-Inference-Timeout-Ms", timeout_header });
    if (authorization) |value| try headers.append(a, .{ "Authorization", value });
    const encoded = try payloads.encodeMetadata(a, body, execution_options.attachments);
    defer if (execution_options.attachments != null) a.free(encoded);
    try headers.append(a, .{ "Content-Type", if (execution_options.attachments != null) httpx.attachment_envelope.content_type else "application/json" });
    const options = httpx.RequestOptions{
        .remaining_timeout_header = "X-Antfly-Inference-Timeout-Ms",
        .body = encoded,
        .attempt_observer = observeAttempt(ctx, execution_options.attempt_observer),
        .headers = headers.items,
        .timeout_ms = timeout_ms,
        .cancellation = cancellation,
        .max_retries = 0,
        .follow_redirects = false,
        .cookies_enabled = false,
    };
    const parsed = try std.json.parseFromSlice(Json, a, body, .{});
    defer parsed.deinit();
    const streaming = execution_options.response_delivery != .buffered and (if (parsed.value.object.get("stream")) |value| value == .bool and value.bool else false);
    if (streaming) {
        var transport = @import("antfly_runtime_abi").http_bridge.Outbound{ .context = ctx };
        var guard = tasks.FallbackSink{ .downstream = observation.stream(transport.stream()) };
        var writer = tasks.StreamWriter{ .sink = if (fallback) guard.sink() else guard.downstream };
        var response = client.requestToWriter(.POST, url, options, &writer, null, null) catch |err| {
            if (transport.started or guard.committed) return error.InferenceResponseCommitted;
            return guard.failure orelse err;
        };
        defer response.deinit();
        writer.close() catch |err| {
            if (transport.started) return error.InferenceResponseCommitted;
            return err;
        };
        return ctx.response.build();
    }
    var response = try client.post(url, options);
    defer response.deinit();
    observation.status = response.status.code;
    if (response.body) |bytes| observation.usage(bytes);
    if (fallback) if (gateway.transientStatus(response.status.code)) |failure| return failure;
    _ = ctx.status(response.status.code);
    if (response.header("retry-after")) |value| try ctx.setHeader("retry-after", value);
    try ctx.setHeader("content-type", response.contentType() orelse "application/json");
    _ = ctx.response.body(response.body orelse "");
    return ctx.response.build();
}

fn requestId(server: anytype, ctx: *httpx.Context, a: std.mem.Allocator) ![]const u8 {
    var random: [16]u8 = undefined;
    server.inferenceIo().random(&random);
    const id = try std.fmt.allocPrint(a, "{x}", .{&random});
    defer a.free(id);
    try ctx.setHeader("x-request-id", id);
    return ctx.response.headers.get("x-request-id").?;
}

fn only(value: Json, allowed: []const []const u8) !void {
    if (value != .object) return error.InvalidInferenceRequest;
    for (value.object.keys()) |key| {
        for (allowed) |name| if (std.mem.eql(u8, key, name)) break else {} else return error.UnsupportedInferenceFeature;
    }
}
fn string(value: Json) ![]const u8 {
    return if (value == .string and value.string.len > 0) value.string else error.InvalidInferenceRequest;
}
fn integer(value: Json) !i64 {
    return if (value == .integer and value.integer > 0) value.integer else error.InvalidInferenceRequest;
}
fn number(value: Json) !f64 {
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        else => error.InvalidInferenceRequest,
    };
}

fn ownsLocalAdmission(server: anytype, named: gateway.View, task: gateway.Task, name: []const u8) !bool {
    const provider = server.antfly_provider orelse return false;
    if (!provider.owns_invocation_admission) return false;
    return switch (task) {
        .generate => blk: {
            const c = try named.providers.getGeneratorConfig(name);
            break :blk c.provider == .antfly and c.url.len == 0;
        },
        .decide => blk: {
            const c = try named.providers.getDeciderConfig(name);
            break :blk c.provider == .antfly and c.url.len == 0;
        },
        .embed => blk: {
            const c = try named.providers.getEmbedderConfig(name);
            break :blk c.provider == .antfly and c.url.len == 0;
        },
        .rerank => blk: {
            const c = try named.providers.getRerankerConfig(name);
            break :blk c.provider == .antfly and c.url.len == 0;
        },
        .read => blk: {
            const c = try named.readers.?.getConfig(name);
            break :blk c.provider == .antfly and c.resolvedUrl() == null;
        },
        else => false,
    };
}

test "httpx inference gateway physical catalogs preserve task ambiguity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var seen: std.StringHashMapUnmanaged(usize) = .empty;
    var data: Json = .{ .array = std.array_list.Managed(Json).init(a) };
    const embed = try std.json.parseFromSliceLeaky(Json, a, "{\"id\":\"shared\",\"task\":\"embed\",\"tasks\":[\"embed\"]}", .{});
    const generate = try std.json.parseFromSliceLeaky(Json, a, "{\"id\":\"shared\",\"task\":\"generate\",\"tasks\":[\"generate\"]}", .{});
    try putCatalogItem(a, &seen, &data, embed);
    try putCatalogItem(a, &seen, &data, generate);
    try putCatalogItem(a, &seen, &data, embed);
    try std.testing.expectEqual(@as(usize, 1), data.array.items.len);
    const merged = data.array.items[0];
    try std.testing.expect(!merged.object.contains("task"));
    try std.testing.expectEqual(@as(usize, 2), merged.object.get("tasks").?.array.items.len);
    try std.testing.expectEqualStrings("embed", embed.object.get("task").?.string);
    try std.testing.expectEqualStrings("generate", generate.object.get("task").?.string);
}

test "httpx inference gateway validates successful session response envelopes and identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "[]",            "null",       "true",          "42",                    "\"session\"",     "{",                  "",
        "{\"id\":null}", "{\"id\":1}", "{\"id\":\"\"}", "{\"id\":\"../other\"}", "{\"id\":\"..\"}", "{\"id\":\"a\\nb\"}", "{\"id\":\"session\",\"session_id\":\"other\"}",
    }) |body| try std.testing.expectError(error.InvalidInferenceResponse, parseSessionResponse(a, body));
    const response = try parseSessionResponse(a, "{\"id\":\"session-123\",\"session_id\":\"session-123\",\"expires_at\":100,\"extra\":true}");
    try std.testing.expectEqualStrings("session-123", (try sessionResponseId(response)).?);
    try std.testing.expect(response.object.get("extra").?.bool);
    const audio = try parseSessionResponse(a, "{\"object\":\"list\",\"data\":[]}");
    try std.testing.expectEqual(@as(?[]const u8, null), try sessionResponseId(audio));
}

test "httpx inference gateway opaque catalog roles cannot erase known tasks" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const inputs = [_][]const u8{
        "{\"id\":\"shared\",\"task\":\"embed\"}",
        "{\"id\":\"shared\",\"task\":\"generate\",\"tasks\":[\"generate\",\"generate\",null]}",
        "{\"id\":\"shared\"}",
        "{\"id\":\"shared\",\"task\":\"legacy-classifier\",\"tasks\":[\"future\"]}",
    };
    for (0..inputs.len) |offset| {
        var seen: std.StringHashMapUnmanaged(usize) = .empty;
        var data: Json = .{ .array = std.array_list.Managed(Json).init(a) };
        for (0..inputs.len) |index| {
            const item = try std.json.parseFromSliceLeaky(Json, a, inputs[(offset + index) % inputs.len], .{});
            try putCatalogItem(a, &seen, &data, item);
        }
        try std.testing.expectEqual(@as(usize, 1), data.array.items.len);
        const item = data.array.items[0];
        try std.testing.expect(!item.object.contains("task"));
        const roles = item.object.get("tasks").?.array.items;
        try std.testing.expectEqual(@as(usize, 2), roles.len);
        for (roles) |role| try std.testing.expect(std.mem.eql(u8, role.string, "generate") or std.mem.eql(u8, role.string, "embed"));
    }
}

test "httpx inference gateway session lifetime starts at publication with precise remaining expiry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const response = try parseSessionResponse(a, "{\"id\":\"backend\",\"expires_at\":102}");
    const lifetime = try sessionLifetime(response, 500 * std.time.ns_per_s, 100 * std.time.ns_per_s + std.time.ns_per_s / 4, null);
    try std.testing.expectEqual(@as(u64, 1750 * std.time.ns_per_ms), lifetime.ttl_ns);
    try std.testing.expectEqual(@as(u64, 501750 * std.time.ns_per_ms), lifetime.expires_ns);
    const fallback = try sessionLifetime(try parseSessionResponse(a, "{}"), 500, 100, null);
    try std.testing.expectEqual(@as(u64, 3600 * std.time.ns_per_s), fallback.ttl_ns);
    const capped = try sessionLifetime(response, std.math.maxInt(u64), 0, null);
    try std.testing.expectEqual(std.math.maxInt(u64), capped.expires_ns);
}

test "httpx inference gateway predictor discovery bounds transports and respects connection capabilities" {
    var config = try sources.common_config.Config.parseFromSlice(std.testing.allocator,
        \\{"predictors":{"risk":{"model":"risk","connection":"remote"}},
        \\ "connections":{"remote":{"kind":"inference","capabilities":["models.predict"],"inference":{"provider":"antfly","url":"http://remote"}},
        \\ "denied":{"kind":"inference","capabilities":["models.generate"],"inference":{"provider":"antfly","url":"http://denied"}},
        \\ "vendor":{"kind":"inference","capabilities":["models.predict"],"inference":{"provider":"openai","url":"http://vendor"}}}}
    );
    defer config.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const named = gateway.View{ .providers = &config.registry };
    var transports = try predictorTransports(a, named, &config, "default-inference");
    try std.testing.expectEqual(@as(usize, 2), transports.count());
    try std.testing.expectEqualStrings("default-inference", transports.keys()[0]);
    try std.testing.expectEqualStrings("remote", transports.keys()[1]);
    for (0..20) |index| {
        const name = try std.fmt.allocPrint(std.testing.allocator, "extra-{d}", .{index});
        try config.connections.put(std.testing.allocator, name, .{ .kind = .inference, .capabilities = try std.testing.allocator.dupe([]u8, &.{try std.testing.allocator.dupe(u8, "models.predict")}), .inference = .{ .provider = try std.testing.allocator.dupe(u8, "antfly") } });
    }
    transports = try predictorTransports(a, named, &config, "default-inference");
    try std.testing.expectEqual(@as(usize, 16), transports.count());
    try std.testing.expect(!transports.contains("denied"));
    try std.testing.expect(!transports.contains("vendor"));
}

test "httpx inference gateway batch response decoder rejects invalid upstream envelopes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectError(error.InvalidInferenceResponse, parseBatchItemResponse(a, null));
    for ([_][]const u8{ "", "invalid-json", "{", "[]", "null", "true", "1", "\"text\"", "{}", "{\"error\":{\"message\":\"backend failed\"}}", "{\"choices\":[]}" }) |body|
        try std.testing.expectError(error.InvalidInferenceResponse, parseBatchItemResponse(a, body));
    const valid_body = "{\"id\":\"response\",\"object\":\"chat.completion\",\"created\":1,\"model\":\"backend\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":null,\"refusal\":\"provider refusal\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2,\"provider_tokens\":2},\"provider_field\":true}";
    const unavailable = try std.mem.replaceOwned(u8, a, valid_body, "{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2,\"provider_tokens\":2}", "null");
    try std.testing.expect((try parseBatchItemResponse(a, unavailable)).object.get("usage").? == .null);
    const missing_usage = try std.mem.replaceOwned(u8, a, unavailable, ",\"usage\":null", "");
    try std.testing.expectError(error.InvalidInferenceResponse, parseBatchItemResponse(a, missing_usage));
    const valid = try parseBatchItemResponse(a, valid_body);
    try std.testing.expect(valid.object.get("provider_field").?.bool);
    try std.testing.expectEqualStrings("provider refusal", valid.object.get("choices").?.array.items[0].object.get("message").?.object.get("refusal").?.string);
    for ([_][2][]const u8{ .{ "chat.completion", "chat.completion.chunk" }, .{ "\"created\":1", "\"created\":null" }, .{ "\"role\":\"assistant\"", "\"role\":123" }, .{ "\"finish_reason\":\"stop\"", "\"finish_reason\":null" }, .{ "\"prompt_tokens\":1", "\"prompt_tokens\":null" } }) |replacement| {
        const invalid = try std.mem.replaceOwned(u8, a, valid_body, replacement[0], replacement[1]);
        try std.testing.expectError(error.InvalidInferenceResponse, parseBatchItemResponse(a, invalid));
    }
    try std.testing.expectEqual(@as(i64, 2), valid.object.get("usage").?.object.get("total_tokens").?.integer);
    var failing: std.testing.FailingAllocator = .init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, parseBatchItemResponse(failing.allocator(), "{\"choices\":[]}"));
}

test "httpx inference gateway session renewal retains full idle TTL rather than transport remainder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const snapshot = try parseSessionResponse(a, "{\"id\":\"backend\",\"created\":100,\"expires_at\":102}");
    const lifetime = try sessionLifetime(snapshot, 500 * std.time.ns_per_s, 101 * std.time.ns_per_s, null);
    try std.testing.expectEqual(@as(u64, 501 * std.time.ns_per_s), lifetime.expires_ns);
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_s), lifetime.ttl_ns);
    const partial = try parseSessionResponse(a, "{\"id\":\"backend\",\"expires_at\":102}");
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_s), (try sessionLifetime(partial, 500, 101 * std.time.ns_per_s, 2 * std.time.ns_per_s)).ttl_ns);
}

test "httpx inference gateway rejects elapsed or malformed session expiries before publication" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "{\"expires_at\":99}", "{\"expires_at\":100}", "{\"expires_at\":0}", "{\"expires_at\":-1}", "{\"expires_at\":null}", "{\"expires_at\":\"102\"}" }) |body| {
        const response = try parseSessionResponse(a, body);
        try std.testing.expectError(error.InvalidInferenceResponse, sessionLifetime(response, 500, 100 * std.time.ns_per_s, 2 * std.time.ns_per_s));
    }
}

test "httpx inference gateway framed typed adapters inline only media references" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const attachments: []const httpx.attachment_envelope.Attachment = &.{.{ .mime_type = "image/png", .data = "image" }};
    const read = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"readers/ocr\",\"images\":[{\"url\":\"attachment:0\"}]}", .{});
    const adapted_read = try inlineTaskAttachments(a, .read, read, attachments);
    try std.testing.expectEqualStrings("data:image/png;base64,aW1hZ2U=", adapted_read.object.get("images").?.array.items[0].object.get("url").?.string);
    const embed = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"embedders/dense\",\"input\":[{\"type\":\"text\",\"text\":\"attachment:0\"},{\"type\":\"media\",\"mime_type\":\"image/png\",\"data\":\"attachment:0\"}]}", .{});
    const adapted_embed = try inlineTaskAttachments(a, .embed, embed, attachments);
    try std.testing.expectEqualStrings("attachment:0", adapted_embed.object.get("input").?.array.items[0].object.get("text").?.string);
    const parts = try tasks.embeddingParts(a, adapted_embed.object.get("input").?);
    try std.testing.expectEqualStrings("image", parts[1].binary.data);
    const decide = try std.json.parseFromSliceLeaky(Json, a,
        \\{"model":"deciders/visual","input":[{"role":"user","content":[{"type":"input_text","text":"attachment:0"},{"type":"input_image","image_url":"attachment:0"}]}]}
    , .{});
    const adapted_decide = try inlineTaskAttachments(a, .decide, decide, attachments);
    const decision_parts = adapted_decide.object.get("input").?.array.items[0].object.get("content").?.array.items;
    try std.testing.expectEqualStrings("attachment:0", decision_parts[0].object.get("text").?.string);
    try std.testing.expectEqualStrings("data:image/png;base64,aW1hZ2U=", decision_parts[1].object.get("image_url").?.string);
    const batch_decide = try std.json.parseFromSliceLeaky(Json, a,
        \\{"model":"deciders/visual","inputs":[{"id":"text","input":"attachment:0"},{"id":"image","input":[{"role":"user","content":[{"type":"input_image","image_url":"attachment:0"}]}]}]}
    , .{});
    const adapted_batch = try inlineTaskAttachments(a, .decide, batch_decide, attachments);
    const batch_inputs = adapted_batch.object.get("inputs").?.array.items;
    try std.testing.expectEqualStrings("attachment:0", batch_inputs[0].object.get("input").?.string);
    const image_part = batch_inputs[1].object.get("input").?.array.items[0].object.get("content").?.array.items[0];
    try std.testing.expectEqualStrings("data:image/png;base64,aW1hZ2U=", image_part.object.get("image_url").?.string);
    const audio = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"transcribers/speech\",\"audio\":\"attachment:0\"}", .{});
    const adapted_audio = try inlineTaskAttachments(a, .transcribe, audio, &.{.{ .mime_type = "audio/flac", .data = "audio" }});
    try std.testing.expect(!adapted_audio.object.contains("audio"));
    try std.testing.expectEqualStrings("data:audio/flac;base64,YXVkaW8=", adapted_audio.object.get("url").?.string);
    const invalid = try std.json.parseFromSliceLeaky(Json, a, "{\"images\":[{\"url\":\"attachment:1\"}]}", .{});
    try std.testing.expectError(error.InvalidInferenceRequest, inlineTaskAttachments(a, .read, invalid, attachments));
    const unused = try std.json.parseFromSliceLeaky(Json, a, "{\"images\":[{\"url\":\"https://example.com/image\"}]}", .{});
    try std.testing.expectError(error.InvalidInferenceRequest, inlineTaskAttachments(a, .read, unused, attachments));
}

test "httpx inference gateway physical decision catalogs apply connection image budgets by task" {
    const a = std.testing.allocator;
    var config = try sources.common_config.Config.parseFromSlice(a,
        \\{"connections":{"vendor":{"kind":"inference","capabilities":["models.decide","models.generate"],"inference":{"provider":"openai"}}},"inference_capabilities":{"connections/vendor/models/vision":{"images":"supported"}}}
    );
    defer config.deinit();
    const Server = struct { cfg: struct { node_config: ?*sources.common_config.Config } };
    var server = Server{ .cfg = .{ .node_config = &config } };
    const named = try view(&server);
    try std.testing.expectEqual(gateway.Support.unsupported, connectionCatalogCapabilities(named, "deciders", "vendor", "vision").images);
    try std.testing.expectEqual(gateway.Support.supported, connectionCatalogCapabilities(named, "generators", "vendor", "vision").images);
    config.connections.getPtr("vendor").?.inference.?.max_image_input_tokens = 8192;
    try std.testing.expectEqual(gateway.Support.supported, connectionCatalogCapabilities(named, "deciders", "vendor", "vision").images);
}

test "httpx inference gateway admission translates the ingress executor clock" {
    const Server = struct {
        fn publicInferenceEnabled(_: *@This()) bool {
            return true;
        }
        fn inferenceIo(_: *@This()) std.Io {
            return std.testing.io;
        }
    };
    var server: Server = .{};
    var clock = try @import("vopr").vopr_io.VoprIo.init(.{ .monotonic_ns = 7 * std.time.ns_per_s });
    defer clock.deinit();
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/ai/v1/generate");
    defer request.deinit();
    var ctx = httpx.Context.init(std.testing.allocator, clock.io(), &request);
    defer ctx.deinit();
    try ctx.setHeader("x-request-id", "test");
    @import("../request_deadline.zig").tighten(&ctx, 100);
    const before = @import("antfly_platform").time.monotonicNs();
    try prepare(&server, &ctx);
    const after = @import("antfly_platform").time.monotonicNs();
    try std.testing.expect(ctx.application_deadline_io == null);
    try std.testing.expect(ctx.application_deadline_ns.? >= before + 100 * std.time.ns_per_ms);
    try std.testing.expect(ctx.application_deadline_ns.? <= after + 100 * std.time.ns_per_ms);
}

test "httpx inference gateway timeout outcomes distinguish admission send proof and unknown adapters" {
    const a = std.testing.allocator;
    var request = try httpx.Request.init(a, .POST, "/ai/v1/generate");
    defer request.deinit();
    var ctx = httpx.Context.init(a, std.testing.io, &request);
    defer ctx.deinit();
    try ctx.setHeader("x-request-id", "correlation");
    const state = try a.create(InvocationState);
    state.* = .{ .allocator = a, .request_id = try a.dupe(u8, "correlation") };
    try ctx.setData(invocation_state_key, state, InvocationState.destroy);
    for (0..3) |phase| {
        if (phase == 1) state.uncertain.store(true, .release);
        if (phase == 2) InvocationState.mark(state);
        var response = try timeoutResponse(&ctx);
        defer response.deinit();
        const parsed = try std.json.parseFromSlice(Json, a, response.body.?, .{});
        defer parsed.deinit();
        const failure = parsed.value.object.get("error").?.object;
        try std.testing.expectEqualStrings("correlation", failure.get("request_id").?.string);
        try std.testing.expectEqualStrings(if (phase == 0) "not_submitted" else "unknown", failure.get("outcome").?.string);
        try std.testing.expectEqual(phase == 0, failure.get("retryable").?.bool);
        const submitted = failure.get("upstream_submitted").?;
        if (phase == 1) try std.testing.expect(submitted == .null) else try std.testing.expectEqual(phase == 2, submitted.bool);
    }
}
