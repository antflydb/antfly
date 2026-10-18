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

//! HTTP adaptation only. Named execution lives in the shared task layer.
const std = @import("std");
const httpx = @import("httpx");
const sources = @import("antfly_local_sources");
const registry = sources.common_provider_registry;
const gateway = registry.gateway;
const tasks = registry.tasks;
const Json = std.json.Value;
const empty_registry = registry.Registry.init(std.heap.page_allocator);
const generating = sources.generating_mod;

pub const public_paths: []const []const u8 = &.{ "/ai/v1/models", "/ai/v1/chat/completions", "/ai/v1/embeddings", "/ai/v1/decisions", "/ai/v1/read", "/ai/v1/extract", "/ai/v1/rewrite", "/ai/v1/rerank", "/ai/v1/chunk", "/ai/v1/transcribe", "/ai/v1/dictate", "/ai/v1/synthesize", "/ai/v1/embed", "/ai/v1/generate", "/ai/v1/generate/batch", "/ml/v1/predict", "/ml/v1/predictors", "/ml/v1/models", "/ai/v1/transcription/sessions", "/ai/v1/transcription/sessions/:session_id", "/ai/v1/transcription/sessions/:session_id/audio", "/ai/v1/transcription/sessions/:session_id/events", "/ai/v1/transcription/sessions/:session_id/stream" };

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
};

pub fn session(server: anytype, ctx: *httpx.Context, principal: []const u8, method: @import("antfly_runtime_abi").http_abi.HttpMethod, action: []const u8) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const now = @import("antfly_platform").time.monotonicNs();
    const public_id = ctx.param("session_id");
    var requested: []const u8 = "transcription/session";
    var backend_model: ?[]const u8 = null;
    var connection: []const u8 = undefined;
    var operation: []const u8 = "transcription/sessions";
    var body = (try ctx.body()) orelse "";
    if (public_id) |id| {
        const pinned = try server.inference_sessions.get(a, id, principal, now);
        connection = pinned.connection;
        // Backend IDs must be one path segment; they are never interpreted as
        // transport addresses or a second model selection.
        operation = try std.fmt.allocPrint(a, "transcription/sessions/{s}{s}", .{ pinned.backend_id, action });
    } else {
        var request = try std.json.parseFromSliceLeaky(Json, a, body, .{});
        if (request != .object) return error.InvalidInferenceRequest;
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
    var result = connections.invokeInferenceConnectionWithMethod(a, &client, server.cfg.node_config, server.cfg.local_inference_connection_target, server.cfg.secret_store, connection, operation, body, cancel, @min(connections.inferenceInvocationDeadlineNs(), ctx.application_deadline_ns orelse std.math.maxInt(u64)), if (streaming) session_sink.sink() else .{}, method) catch |err| {
        observation.failure = @errorName(err);
        if (transport.started) return error.InferenceResponseCommitted;
        return err;
    };
    defer result.deinit(a);
    observation.status = result.status;
    observation.usage(result.body);
    if (transport.started) return ctx.response.build();
    _ = ctx.status(result.status);
    if (result.content_type) |value| try ctx.setHeader("content-type", value);
    if (public_id == null and result.status >= 200 and result.status < 300) {
        var response = try std.json.parseFromSliceLeaky(Json, a, result.body, .{});
        const backend_id = try string(response.object.get("session_id") orelse response.object.get("id") orelse return error.InvalidInferenceResponse);
        if (backend_id.len > 256 or std.mem.indexOfAny(u8, backend_id, "/?#%") != null) return error.InvalidInferenceResponse;
        var random: [24]u8 = undefined;
        server.inferenceIo().random(&random);
        const id = try a.alloc(u8, std.base64.url_safe_no_pad.Encoder.calcSize(random.len));
        _ = std.base64.url_safe_no_pad.Encoder.encode(id, &random);
        const realtime_seconds: u64 = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s;
        const ttl_seconds: u64 = if (response.object.get("expires_at")) |value| if (value == .integer and value.integer > 0 and @as(u64, @intCast(value.integer)) > realtime_seconds) @min(@as(u64, @intCast(value.integer)) - realtime_seconds, 86400) else 3600 else 3600;
        const ttl_ns = ttl_seconds * std.time.ns_per_s;
        try server.inference_sessions.put(server.owner_alloc, id, .{ .principal = principal, .connection = connection, .backend_id = backend_id, .expires_ns = now +| ttl_ns, .ttl_ns = ttl_ns }, now);
        if (response.object.contains("session_id")) try response.object.put(a, "session_id", .{ .string = id });
        if (response.object.contains("id")) try response.object.put(a, "id", .{ .string = id });
        try ctx.setHeader("x-antfly-inference-affinity", id);
        return ctx.json(response);
    }
    if (method == .delete and result.status >= 200 and result.status < 300) server.inference_sessions.remove(server.owner_alloc, public_id.?);
    if (result.status >= 200 and result.status < 300 and result.body.len > 0) {
        var response = try std.json.parseFromSliceLeaky(Json, a, result.body, .{});
        if (response == .object) {
            if (response.object.contains("session_id")) try response.object.put(a, "session_id", .{ .string = public_id.? });
            if (response.object.contains("id")) try response.object.put(a, "id", .{ .string = public_id.? });
        }
        return ctx.json(response);
    }
    _ = ctx.response.body(result.body);
    return ctx.response.build();
}

pub fn view(server: anytype) !gateway.View {
    const config = server.cfg.node_config orelse return .{ .providers = &empty_registry };
    return .{ .providers = &config.registry, .readers = &config.readers, .transcribers = &config.transcribers, .synthesizers = &config.text_to_speech };
}

pub fn prepare(server: anytype, ctx: *httpx.Context) !void {
    if (ctx.response.headers.get("x-request-id") == null) _ = try requestId(server, ctx, ctx.allocator);
    _ = try attemptBudget(ctx);
    const now = @import("antfly_platform").time.monotonicNs();
    var timeout_ms: u64 = 120000;
    if (ctx.header("x-antfly-inference-timeout-ms")) |raw| {
        timeout_ms = @min(timeout_ms, std.fmt.parseInt(u64, raw, 10) catch return error.InvalidInferenceRequest);
    }
    const deadline = now +| timeout_ms *| std.time.ns_per_ms;
    ctx.application_deadline_ns = @min(ctx.application_deadline_ns orelse deadline, deadline);
    if (timeout_ms == 0) return error.Timeout;
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
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 64 * 1024 * 1024 };
    return executeBounded(server, ctx, task, budget.allocator(), .{ .attempt_limit = try attemptBudget(ctx), .operation = operation }) catch |err| {
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
    const body = (try ctx.body()) orelse return error.InvalidInferenceRequest;
    const request = try std.json.parseFromSliceLeaky(Json, a, body, .{});
    if (request != .object) return error.InvalidInferenceRequest;
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
    if (target == .configured and target.configured.chain) return executeChain(server, ctx, a, request, named, target.configured.name, options);
    return executeResolved(server, ctx, task, a, effective, named, target, null, false, 1, options);
}

fn executeResolved(server: anytype, ctx: *httpx.Context, task: gateway.Task, a: std.mem.Allocator, request: Json, named: gateway.View, target: gateway.Target, generator_override: ?generating.GeneratorConfig, fallback: bool, attempt: u8, options: ExecutionOptions) anyerror!httpx.Response {
    const requested = try string(request.object.get("model") orelse return error.ModelRequired);
    const id = try target.idAlloc(a);
    const request_id = ctx.response.headers.get("x-request-id") orelse try requestId(server, ctx, a);
    const descriptor = if (generator_override == null) try named.describe(a, target) else null;
    var observation = tasks.Observation{ .allocator = a, .request_id = request_id, .requested = requested, .resolved = id, .executor = if (descriptor) |value| value.executor else @tagName(generator_override.?.provider), .backend_model = if (descriptor) |value| value.backend_model else generator_override.?.model, .connection = if (descriptor) |value| value.connection else null, .task = @tagName(task), .attempt = attempt, .started_ns = @import("antfly_platform").time.monotonicNs() };
    defer observation.finish();
    const response = executeResolvedImpl(server, ctx, task, a, request, named, target, generator_override, fallback, &observation, options) catch |err| {
        observation.failure = @errorName(err);
        return err;
    };
    observation.status = response.status.code;
    if (response.body) |bytes| observation.usage(bytes);
    return response;
}

fn executeResolvedImpl(server: anytype, ctx: *httpx.Context, task: gateway.Task, a: std.mem.Allocator, request: Json, named: gateway.View, target: gateway.Target, generator_override: ?generating.GeneratorConfig, fallback: bool, observation: *tasks.Observation, execution_options: ExecutionOptions) anyerror!httpx.Response {
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
    if (task == .extract or task == .rewrite or task == .predict) {
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
        if (cfg.api_url.len > 0) return forwardEndpoint(server, ctx, cfg.api_url, null, "chunk", encoded, observation);
        return forward(server, ctx, if (server.cfg.local_inference_connection_target != null) sources.common_config.local_inference_connection_id else sources.common_config.default_inference_connection_id, "chunk", encoded, false, observation, execution_options);
    }
    if (task == .transcribe) {
        const cfg = try named.transcribers.?.getConfig(target.configured.name);
        if (cfg.provider == .antfly) {
            const operation = execution_options.operation orelse "transcribe";
            const encoded = try tasks.transcriptionBody(a, cfg, request);
            return forward(server, ctx, try std.fmt.allocPrint(a, "transcribers/{s}", .{target.configured.name}), operation, encoded, false, observation, execution_options);
        }
    }
    const caller_owns_admission = if (generator_override) |cfg| !(cfg.provider == .antfly and cfg.url.len == 0 and server.antfly_provider != null and server.antfly_provider.?.owns_invocation_admission) else !try ownsLocalAdmission(server, named, target.configured.task, target.configured.name);
    if (caller_owns_admission and !server.tryAcquireInference()) return error.InferenceCapacityUnavailable;
    defer if (caller_owns_admission) server.releaseInference();
    const cancellation = httpx.CancellationToken.fromCallback(ctx, struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const httpx.Context = @ptrCast(@alignCast(raw));
            return context.isCancellationRequested();
        }
    }.cancelled);
    const deadline = @min(@import("connections.zig").inferenceInvocationDeadlineNs(), ctx.application_deadline_ns orelse std.math.maxInt(u64));
    var client = httpx.Client.initWithConfig(a, server.inferenceIo(), .{ .keep_alive = false });
    defer client.deinit();
    const executor = tasks.Executor{
        .view = named,
        .generator_override = generator_override,
        .http = &client,
        .context = .{ .io = server.inferenceIo(), .deadline_ns = deadline, .cancellation = if (cancellation) |v| @as(@import("antfly_cancellation").CancellationToken, .{ .ptr = v.ptr, .is_cancelled_fn = v.is_cancelled_fn }) else null },
        .antfly_provider = server.antfly_provider,
        .inference_api_url = server.cfg.node_config.?.inference.api_url,
        .inference_api_key = server.cfg.inference_api_key orelse server.cfg.node_config.?.inference.api_key,
        .secret_store = server.cfg.secret_store,
        .provider_runtime = &server.embedding_provider_runtime,
    };
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
            if (cfg.provider == .openai or cfg.provider == .openrouter or cfg.provider == .ollama or (cfg.provider == .antfly and cfg.url.len != 0)) {
                var transport = @import("antfly_runtime_abi").http_bridge.Outbound{ .context = ctx };
                const streaming = if (request.object.get("stream")) |v| v.bool else false;
                const observed = observation.stream(transport.stream());
                var guard = tasks.FallbackSink{ .downstream = observed };
                var response = executor.chat(a, name, messages, options, request, if (streaming) (if (fallback) guard.sink() else observed) else .{}) catch |err| {
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
            const Tool = struct { id: []const u8, type: []const u8 = "function", function: struct { name: []const u8, arguments: []const u8 } };
            const calls = try a.alloc(Tool, result.tool_calls.len);
            for (result.tool_calls, calls) |call, *out| out.* = .{ .id = call.id, .function = .{ .name = call.name, .arguments = call.arguments } };
            return ctx.json(.{
                .id = try std.fmt.allocPrint(a, "chatcmpl-{d}", .{@import("antfly_platform").time.monotonicNs()}),
                .object = "chat.completion",
                .created = @as(u64, @intCast(@max(0, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s))),
                .model = model,
                .choices = .{.{ .index = 0, .message = .{ .role = "assistant", .content = result.content, .tool_calls = if (calls.len > 0) calls else null }, .finish_reason = if (calls.len > 0) "tool_calls" else "stop" }},
                .usage = if (result.usage) |usage| .{ .prompt_tokens = usage.input_tokens, .completion_tokens = usage.output_tokens, .total_tokens = usage.input_tokens + usage.output_tokens } else null,
            });
        },
        .decide => {
            try only(request, &.{ "model", "input", "questions" });
            const input = try string(request.object.get("input") orelse return error.InvalidInferenceRequest);
            const questions = request.object.get("questions") orelse return error.InvalidInferenceRequest;
            const result = try executor.decide(a, name, input, questions);
            return ctx.json(result);
        },
        .embed => {
            try only(request, &.{ "model", "input", "dimensions", "encoding_format" });
            const encoding = if (request.object.get("encoding_format")) |v| try string(v) else "float";
            if (!std.mem.eql(u8, encoding, "float") and !std.mem.eql(u8, encoding, "base64")) return error.UnsupportedInferenceFeature;
            const input = request.object.get("input") orelse return error.InvalidInferenceRequest;
            const dimensions: ?u32 = if (request.object.get("dimensions")) |v| std.math.cast(u32, try integer(v)) orelse return error.InvalidInferenceRequest else null;
            const cfg = try named.providers.getEmbedderConfig(name);
            if (input == .array and input.array.items.len > 0 and input.array.items[0] != .string) {
                if (!std.mem.eql(u8, encoding, "float")) return error.UnsupportedInferenceFeature;
                const grouped = input.array.items[0] == .array;
                const count: usize = if (grouped) input.array.items.len else 1;
                const Item = struct { object: []const u8 = "embedding", index: usize, embedding: []const f32 };
                const data = try a.alloc(Item, count);
                for (data, 0..) |*item, index| item.* = .{ .index = index, .embedding = try executor.embedParts(a, name, try tasks.embeddingParts(a, if (grouped) input.array.items[index] else input), dimensions) };
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
            const scores = try @import("../reranking/mod.zig").rerankContentWithOptions(a, &client, cfg, .{
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
            const result = try executor.transcribe(a, name, .{
                .url = audio_url,
                .language = if (request.object.get("language")) |value| try string(value) else null,
                .diarization = if (request.object.get("diarization")) |value| if (value == .bool) value.bool else return error.InvalidInferenceRequest else null,
                .timestamps = if (request.object.get("timestamps")) |value| if (value == .bool) value.bool else return error.InvalidInferenceRequest else null,
            });
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
        .synthesize => {
            var adapted = request;
            _ = adapted.object.swapRemove("model");
            const parsed = try std.json.parseFromValue(@import("antfly_synthesizing").Request, a, adapted, .{});
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
    if (task == .embed) {
        if (cfg.region) |value| try provider.object.put(a, "region", .{ .string = value });
        if (request.object.get("dimensions")) |value| try provider.object.put(a, "dimensions", value);
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
fn prepareGeneration(a: std.mem.Allocator, cfg: generating.GeneratorConfig, request: Json, operation: []const u8) !PreparedGeneration {
    if (cfg.provider == .antfly and std.mem.eql(u8, operation, "generate")) {
        _ = try std.json.parseFromValue(@import("inference_api").GenerateRequest, a, request, .{});
    } else try only(request, &.{ "model", "messages", "stream", "max_tokens", "max_completion_tokens", "temperature", "tools", "tool_choice", "response_format", "top_p", "frequency_penalty", "presence_penalty", "stream_options", "seed", "stop" });
    var requirements = try tasks.chatRequirements(request);
    if (cfg.tools_json != null) requirements.tools = true;
    const rich_adapter = cfg.provider == .openai or cfg.provider == .openrouter or cfg.provider == .ollama or cfg.provider == .antfly;
    if (!rich_adapter) try only(request, &.{ "model", "messages", "stream", "max_tokens", "max_completion_tokens", "temperature", "tools", "tool_choice" });
    if (!rich_adapter and (requirements.streaming or requirements.structured_output or requirements.images or requirements.audio)) return error.UnsupportedInferenceFeature;
    const messages = try tasks.chatMessages(a, request);
    const options = tasks.GenerationOptions{
        .max_tokens = if (request.object.get("max_tokens")) |v| try integer(v) else null,
        .max_completion_tokens = if (request.object.get("max_completion_tokens")) |v| try integer(v) else null,
        .temperature = if (request.object.get("temperature")) |v| @floatCast(try number(v)) else null,
        .tools_json = if (request.object.get("tools")) |v| try std.json.Stringify.valueAlloc(a, v, .{}) else null,
        .tool_choice_json = if (request.object.get("tool_choice")) |v| try std.json.Stringify.valueAlloc(a, v, .{}) else null,
    };
    const configured = try tasks.withGenerationOptions(cfg, options);
    if (rich_adapter) try tasks.validateChatConfig(configured);
    return .{ .config = configured, .requirements = requirements, .messages = messages, .options = options };
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
        if (cfg.provider != .openai and cfg.provider != .openrouter and cfg.provider != .ollama and cfg.provider != .antfly) return error.UnsupportedInferenceFeature;
    }
    const previous_deadline = ctx.application_deadline_ns;
    defer ctx.application_deadline_ns = previous_deadline;
    const deadline = @min(@import("connections.zig").inferenceInvocationDeadlineNs(), previous_deadline orelse std.math.maxInt(u64));
    ctx.application_deadline_ns = deadline;
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
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 64 << 20 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    const body = (try ctx.body()) orelse return error.InvalidInferenceRequest;
    const request = try std.json.parseFromSliceLeaky(Json, a, body, .{});
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
    const Result = struct { custom_id: []const u8, index: usize, response: ?Json = null, @"error": ?struct { code: []const u8, message: []const u8, retryable: bool } = null };
    const results = try a.alloc(Result, items.array.items.len);
    var succeeded: usize = 0;
    const deadline = @min(@import("connections.zig").inferenceInvocationDeadlineNs(), ctx.application_deadline_ns orelse std.math.maxInt(u64));
    const options = ExecutionOptions{ .response_delivery = .buffered, .attempt_limit = try attemptBudget(ctx) };
    for (items.array.items, results, 0..) |item, *result, index| {
        result.* = .{ .custom_id = item.object.get("custom_id").?.string, .index = index };
        if (ctx.isCancellationRequested()) return error.Canceled;
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        var item_request = try httpx.Request.init(a, .POST, "http://gateway/ai/v1/generate");
        defer item_request.deinit();
        item_request.body = try std.json.Stringify.valueAlloc(a, item.object.get("body").?, .{});
        var item_context = httpx.Context.init(a, ctx.io, &item_request);
        defer item_context.deinit();
        item_context.application_deadline_ns = deadline;
        item_context.cancellation_probe = .{ .ptr = ctx, .is_cancelled = struct {
            fn cancelled(raw: ?*const anyopaque) bool {
                const outer: *const httpx.Context = @ptrCast(@alignCast(raw.?));
                return outer.isCancellationRequested();
            }
        }.cancelled };
        var response = executeBounded(server, &item_context, .generate, a, options) catch |err| {
            if (err == error.Canceled or err == error.Cancelled or err == error.Timeout) return err;
            result.@"error" = .{ .code = @errorName(err), .message = @errorName(err), .retryable = gateway.permitsFallback(.on_error, err) };
            continue;
        };
        defer response.deinit();
        if (response.status.code >= 200 and response.status.code < 300) {
            result.response = try std.json.parseFromSliceLeaky(Json, a, response.body orelse return error.InvalidInferenceResponse, .{ .allocate = .alloc_always });
            succeeded += 1;
        } else result.@"error" = .{ .code = "upstream_error", .message = "Inference request failed", .retryable = gateway.transientStatus(response.status.code) != null };
    }
    return ctx.json(.{ .object = "generate.batch", .data = results, .summary = .{ .total = results.len, .succeeded = succeeded, .failed = results.len - succeeded }, .execution = .{ .requested_items = results.len, .serial_items = results.len } });
}

pub fn predictors(server: anytype, ctx: *httpx.Context) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const named = try view(server);
    var result: Json = .{ .object = .empty };
    const config = server.cfg.node_config;
    var client = httpx.Client.initWithConfig(a, server.inferenceIo(), .{ .keep_alive = false });
    defer client.deinit();
    const connections = @import("connections.zig");
    const deadline = @min(@import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, ctx.application_deadline_ns orelse std.math.maxInt(u64));
    var transports: std.StringArrayHashMapUnmanaged(Json) = .{};
    // Preserve the native predictor inventory even when no aliases are configured.
    const default_connection = if (server.cfg.local_inference_connection_target != null) "local-inference" else "default-inference";
    if (connections.invokeInferenceConnectionWithMethod(a, &client, config, server.cfg.local_inference_connection_target, server.cfg.secret_store, default_connection, "ml/models", "", null, deadline, .{}, .get)) |response_value| {
        var response = response_value;
        defer response.deinit(a);
        if (response.status == 200) {
            const parsed = std.json.parseFromSliceLeaky(Json, a, response.body, .{ .allocate = .alloc_always }) catch .null;
            if (parsed == .object) if (parsed.object.get("predictors")) |items| {
                if (items == .object) {
                    result = items;
                    try transports.put(a, default_connection, items);
                }
            };
        }
    } else |_| {}
    for (named.providers.predictor_configs.keys(), named.providers.predictor_configs.values()) |alias, binding| {
        if (transports.count() >= 16 or @import("antfly_platform").time.monotonicNs() >= deadline) break;
        const entry = try transports.getOrPut(a, binding.connection);
        if (!entry.found_existing) {
            entry.value_ptr.* = .null;
            var response = connections.invokeInferenceConnectionWithMethod(a, &client, config, server.cfg.local_inference_connection_target, server.cfg.secret_store, binding.connection, "ml/models", "", null, deadline, .{}, .get) catch continue;
            defer response.deinit(a);
            if (response.status != 200) continue;
            const parsed = std.json.parseFromSliceLeaky(Json, a, response.body, .{ .allocate = .alloc_always }) catch continue;
            if (parsed == .object) entry.value_ptr.* = parsed.object.get("predictors") orelse .null;
        }
        if (entry.value_ptr.* != .object) continue;
        const info = entry.value_ptr.object.get(binding.model) orelse continue;
        try result.object.put(a, try std.fmt.allocPrint(a, "predictors/{s}", .{alias}), info);
    }
    return ctx.json(.{ .object = "list", .predictors = result });
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
    var budget: sources.sql_memory_budget = .{ .backing = ctx.allocator, .limit = 64 << 20 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    var root: Json = .{ .object = .empty };
    if (server.antfly_provider) |provider| if (provider.list_models_json) |list| {
        const bytes = list(provider.ptr, a) catch null;
        if (bytes) |value| root = std.json.parseFromSliceLeaky(Json, a, value, .{}) catch .{ .object = .empty };
    };
    if (root != .object) root = .{ .object = .empty };
    if (!root.object.contains("allow_downloads")) try root.object.put(a, "allow_downloads", .{ .bool = false });
    if (!root.object.contains("backends")) try root.object.put(a, "backends", .{ .object = .empty });
    inline for (.{ "chunkers", "rerankers", "embedders", "generators", "extractors", "deciders", "rewriters", "readers", "transcribers" }) |category| {
        if (!root.object.contains(category)) try root.object.put(a, category, .{ .object = .empty });
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
        try group.object.put(a, id, item);
        try root.object.put(a, category, group);
    }
    // Reuse connection discovery's bounded, credential-scoped cache and its
    // configured-model overlay. Listing failure never removes named aliases.
    const connection_bytes = try server.listConnectionsJsonAlloc(a, "inference", "models", null);
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
                try item.object.put(a, "availability", connection.object.get("status") orelse .{ .string = "unknown" });
                try item.object.put(a, "freshness", listed.object.get("freshness") orelse .{ .string = "cached" });
                try item.object.put(a, "gateway_capabilities", try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, named.capabilities(.{ .connection = .{ .task = .generate, .connection = id, .model = backend } }), .{}), .{}));
                try putCatalogItem(a, &seen, &data, item);
                var catalog_group = root.object.get(group.key_ptr.*) orelse Json{ .object = .empty };
                if (catalog_group != .object) continue;
                try catalog_group.object.put(a, public_id, item);
                try root.object.put(a, group.key_ptr.*, catalog_group);
            }
        }
    };
    try root.object.put(a, "object", .{ .string = "list" });
    try root.object.put(a, "data", data);
    return ctx.json(root);
}

fn putCatalogItem(a: std.mem.Allocator, seen: *std.StringHashMapUnmanaged(usize), data: *Json, item: Json) !void {
    const id = try string(item.object.get("id") orelse return error.ModelInventoryUnavailable);
    const entry = try seen.getOrPut(a, id);
    if (entry.found_existing) data.array.items[entry.value_ptr.*] = item else {
        entry.value_ptr.* = data.array.items.len;
        try data.array.append(item);
    }
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
    var result = server.invokeInferenceConnection(ctx.allocator, connection, operation, body, cancellation, @min(connections.inferenceInvocationDeadlineNs(), ctx.application_deadline_ns orelse std.math.maxInt(u64)), if (options.response_delivery == .buffered) .{} else if (fallback) guard.sink() else observed) catch |err| {
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

fn forwardEndpoint(server: anytype, ctx: *httpx.Context, endpoint: []const u8, api_key: ?[]const u8, operation: []const u8, body: []const u8, observation: *tasks.Observation) !httpx.Response {
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
    const authorization = if (resolved) |value| try std.fmt.allocPrint(a, "Bearer {s}", .{value}) else null;
    defer if (authorization) |value| a.free(value);
    const cancellation = httpx.CancellationToken.fromCallback(ctx, struct {
        fn cancelled(raw: *const anyopaque) bool {
            const context: *const httpx.Context = @ptrCast(@alignCast(raw));
            return context.isCancellationRequested();
        }
    }.cancelled);
    const timeout_ms = @max(@as(u64, 1), (@min(@import("connections.zig").inferenceInvocationDeadlineNs(), ctx.application_deadline_ns orelse std.math.maxInt(u64)) -| @import("antfly_platform").time.monotonicNs()) / std.time.ns_per_ms);
    const timeout_header = try std.fmt.allocPrint(a, "{d}", .{timeout_ms});
    defer a.free(timeout_header);
    var headers = std.ArrayList([2][]const u8).empty;
    defer headers.deinit(a);
    try headers.append(a, .{ "X-Antfly-Inference-Attempts-Remaining", "1" });
    try headers.append(a, .{ "X-Antfly-Inference-Timeout-Ms", timeout_header });
    if (authorization) |value| try headers.append(a, .{ "Authorization", value });
    const options = httpx.RequestOptions{
        .json = body,
        .headers = headers.items,
        .timeout_ms = timeout_ms,
        .cancellation = cancellation,
        .max_retries = 0,
        .follow_redirects = false,
        .cookies_enabled = false,
    };
    const parsed = try std.json.parseFromSlice(Json, a, body, .{});
    defer parsed.deinit();
    const streaming = if (parsed.value.object.get("stream")) |value| value == .bool and value.bool else false;
    if (streaming) {
        var transport = @import("antfly_runtime_abi").http_bridge.Outbound{ .context = ctx };
        var writer = tasks.StreamWriter{ .sink = observation.stream(transport.stream()) };
        var response = client.requestToWriter(.POST, url, options, &writer, null, null) catch |err| {
            if (transport.started) return error.InferenceResponseCommitted;
            return err;
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
