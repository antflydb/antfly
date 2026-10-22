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

//! Typed named-task execution below HTTP. Reuse the same factories, quotas,
//! secret sources and inference boundary as SQL and enrichment.
const std = @import("std");
const httpx = @import("httpx");
const gateway = @import("inference_gateway.zig");
const generating = @import("../generating/mod.zig");
const decisions = @import("../functions/decisions.zig");
const decision_runtime = @import("../functions/runtime.zig");
const managed = @import("../inference/managed_embedder.zig");
const execution = @import("antfly_inference_execution_context");
const secrets = @import("secrets.zig");
const readers = @import("antfly_readers");
const embeddings = @import("antfly_embeddings");
const transcribing = @import("antfly_transcribing");
const synthesizing = @import("antfly_synthesizing");
pub const SessionSink = @import("inference_stream.zig").SessionSink;
pub const FallbackSink = @import("inference_stream.zig").FallbackSink;
pub const Observation = @import("inference_telemetry.zig").Observation;
pub const StreamWriter = @import("inference_stream.zig").Writer;
const Json = std.json.Value;

pub const Executor = struct {
    view: gateway.View,
    generator_override: ?generating.GeneratorConfig = null,
    http: *httpx.Client,
    context: execution.RequestContext,
    antfly_provider: ?managed.AntflyProvider = null,
    inference_api_url: ?[]const u8 = null,
    inference_api_key: ?[]const u8 = null,
    secret_store: ?*secrets.FileStore = null,
    provider_runtime: ?*managed.ProviderRuntime = null,

    pub fn generatorConfig(self: Executor, a: std.mem.Allocator, name: []const u8) !generating.GeneratorConfig {
        var cfg = self.generator_override orelse try self.view.providers.getGeneratorConfig(name);
        if (cfg.provider == .antfly and cfg.url.len == 0 and self.antfly_provider == null) {
            if (self.inference_api_url) |endpoint| {
                cfg.url = try gateway.apiBaseAlloc(a, endpoint);
                cfg.api_key = cfg.api_key orelse self.inference_api_key;
            }
        }
        return cfg;
    }

    pub fn generate(self: Executor, a: std.mem.Allocator, name: []const u8, messages: []const generating.ChatMessage, overrides: GenerationOptions) !generating.GenerateResult {
        try self.context.check();
        var cfg = try self.generatorConfig(a, name);
        if (cfg.provider == .chatgpt) return error.UnsupportedInferenceFeature;
        cfg = try withGenerationOptions(cfg, overrides);
        var factory = generating.BackendFactory.initWithOptions(a, self.http, .{
            .antfly_provider = self.antfly_provider,
            .secret_store = self.secret_store,
            .inference_api_key = self.inference_api_key,
            .request_context = self.context,
            .execution = .{ .default_endpoint = self.inference_api_url },
        });
        var generator = try factory.factory().create(a, cfg);
        defer generator.deinit();
        return generator.generate(a, cfg.model, messages);
    }

    pub fn chat(self: Executor, a: std.mem.Allocator, name: []const u8, messages: []const generating.ChatMessage, overrides: GenerationOptions, request: Json, operation: @import("antfly_generating").ChatRequest.Operation, sink: @import("antfly_runtime_abi").http_abi.StreamSink) !@import("antfly_generating").ChatResponse {
        var cfg = try self.generatorConfig(a, name);
        cfg = try withGenerationOptions(cfg, overrides);
        const body = try chatBody(a, cfg, request);
        var factory = generating.BackendFactory.initWithOptions(a, self.http, .{
            .antfly_provider = self.antfly_provider,
            .secret_store = self.secret_store,
            .inference_api_key = self.inference_api_key,
            .request_context = self.context,
            .execution = .{ .default_endpoint = self.inference_api_url },
        });
        var generator = try factory.factory().create(a, cfg);
        defer generator.deinit();
        var borrowed = sink;
        const Forwarder = struct {
            const abi = @import("antfly_runtime_abi").http_abi;
            fn check(status: abi.CallbackStatus) !void {
                switch (status) {
                    .ok => {},
                    .canceled => return error.Canceled,
                    else => return error.InferenceStreamFailed,
                }
            }
            fn start(raw: *anyopaque, status: u16, content_type: []const u8, retry_after: ?[]const u8, request_id: ?[]const u8) !void {
                const target: *abi.StreamSink = @ptrCast(@alignCast(raw));
                var headers: [2]abi.HeaderView = undefined;
                var count: usize = 0;
                if (retry_after) |value| {
                    headers[count] = .{ .name = .init("retry-after"), .value = .init(value) };
                    count += 1;
                }
                if (request_id) |value| {
                    headers[count] = .{ .name = .init("x-antfly-upstream-request-id"), .value = .init(value) };
                    count += 1;
                }
                try check(target.start.?(target.context, status, .init(content_type), .{ .ptr = &headers, .len = count }));
            }
            fn write(raw: *anyopaque, bytes: []const u8) !void {
                const target: *abi.StreamSink = @ptrCast(@alignCast(raw));
                try check(target.write.?(target.context, .init(bytes)));
            }
            fn close(raw: *anyopaque) !void {
                const target: *abi.StreamSink = @ptrCast(@alignCast(raw));
                try check(target.close.?(target.context));
            }
        };
        return generator.chat(a, .{ .messages = messages, .body_json = body, .operation = operation, .stream = if (sink.start != null) .{ .context = &borrowed, .start = Forwarder.start, .write = Forwarder.write, .close = Forwarder.close } else null });
    }

    pub fn decide(self: Executor, a: std.mem.Allocator, name: []const u8, input: decisions.Input, questions: Json) !Json {
        var runtime = decision_runtime.Runtime{
            .registry = self.view.providers,
            .http = self.http,
            .io = self.http.io,
            .context = self.context,
            .secret_store = self.secret_store,
            .antfly_provider = self.antfly_provider,
            .antfly_url = self.inference_api_url,
            .antfly_api_key = self.inference_api_key,
        };
        defer runtime.deinit();
        const results = try runtime.provider().evaluateBatch(a, &.{.{ .decider = name, .input = input, .questions = if (questions == .array) try decisions.publicQuestions(a, questions) else questions }});
        return results[0];
    }

    pub fn decideRequest(self: Executor, a: std.mem.Allocator, name: []const u8, request: Json) !Json {
        const batch = try @import("../functions/decision_batch.zig").parse(a, request);
        const source_questions = request.object.get("questions") orelse return error.InvalidDecisionSpecification;
        if (!batch.batched) return self.decide(a, name, batch.items[0].input, source_questions);
        const questions = if (source_questions == .array) try decisions.publicQuestions(a, source_questions) else source_questions;
        const requests = try a.alloc(decisions.Request, batch.items.len);
        for (batch.items, requests) |item, *target| target.* = .{ .decider = name, .input = item.input, .questions = questions };
        var runtime = decision_runtime.Runtime{
            .registry = self.view.providers,
            .http = self.http,
            .io = self.http.io,
            .context = self.context,
            .secret_store = self.secret_store,
            .antfly_provider = self.antfly_provider,
            .antfly_url = self.inference_api_url,
            .antfly_api_key = self.inference_api_key,
        };
        defer runtime.deinit();
        const result = try runtime.evaluatePartial(a, requests);
        var data: Json = .{ .array = std.array_list.Managed(Json).init(a) };
        for (batch.items, result.values, result.failures, 0..) |item, value, failure, index| {
            var row = decisions.jsonObject();
            try decisions.put(a, &row, "input_index", .{ .integer = @intCast(index) });
            if (item.id) |id| try decisions.put(a, &row, "id", .{ .string = id });
            try decisions.put(a, &row, "answers", if (value) |valid| valid.object.get("answers").? else .{ .array = std.array_list.Managed(Json).init(a) });
            if (value) |valid| try decisions.put(a, &row, "model", valid.object.get("model").?);
            if (failure) |err| try decisions.put(a, &row, "error", .{ .string = @errorName(err) });
            try data.array.append(row);
        }
        const cfg = try self.view.providers.getDeciderConfig(name);
        var envelope = decisions.jsonObject();
        try decisions.put(a, &envelope, "model", .{ .string = if (cfg.generator) |generator| generator.model else cfg.modelName() });
        try decisions.put(a, &envelope, "data", data);
        var usage = decisions.jsonObject();
        try decisions.put(a, &usage, "input_tokens", .{ .integer = std.math.cast(i64, runtime.input_tokens) orelse return error.InvalidDecisionOutput });
        try decisions.put(a, &usage, "output_tokens", .{ .integer = std.math.cast(i64, runtime.output_tokens) orelse return error.InvalidDecisionOutput });
        try decisions.put(a, &envelope, "usage", usage);
        return envelope;
    }

    pub fn embed(self: Executor, a: std.mem.Allocator, name: []const u8, texts: []const []const u8, dimensions: ?u32) ![]const []const f32 {
        try self.context.check();
        var cfg = try self.view.providers.getEmbedderConfig(name);
        if (dimensions) |d| {
            cfg.dimension = d;
            cfg.dimensions = d;
        }
        var embedder = try self.initEmbedder(a, cfg);
        defer embedder.deinit();
        if (!embedder.hasDenseEntries() or embedder.hasSparseEntries()) return error.UnsupportedInferenceFeature;
        const dims = cfg.dimensions orelse cfg.dimension orelse embedder.entries[0].dimensions;
        return embedder.denseInterface().embedDenseBatchWithContext(a, "gateway", texts, dims, self.context);
    }

    pub const NativeEmbedding = struct {
        allocator: std.mem.Allocator,
        embedder: managed.ManagedEmbedder,
        controls: managed.ManagedEmbedder.NativeRequestControls,

        pub fn deinit(self: *NativeEmbedding) void {
            if (self.controls.authorization) |value| self.allocator.free(value);
            self.embedder.deinit();
        }
    };

    pub fn nativeEmbedding(self: Executor, a: std.mem.Allocator, name: []const u8, identity: ?[]const u8) !NativeEmbedding {
        try self.context.check();
        var cfg = try self.view.providers.getEmbedderConfig(name);
        if (cfg.provider != .antfly) return error.UnsupportedEmbeddingProvider;
        if (identity) |pin| cfg.model_identity = pin;
        var embedder = try self.initEmbedder(a, cfg);
        errdefer embedder.deinit();
        const controls = try embedder.nativeRequestControls(a, "gateway");
        return .{ .allocator = a, .embedder = embedder, .controls = controls };
    }

    fn initEmbedder(self: Executor, a: std.mem.Allocator, cfg: embeddings.Config) !managed.ManagedEmbedder {
        const config_json = try embeddings.stringifyAlloc(a, cfg);
        defer a.free(config_json);
        const dimension_field = if (cfg.dimensions orelse cfg.dimension) |d| try std.fmt.allocPrint(a, ",\"dimension\":{d}", .{d}) else "";
        const indexes = try std.fmt.allocPrint(a, "{{\"gateway\":{{\"type\":\"embeddings\",\"field\":\"text\",\"sparse\":{s},\"embedder\":{s}{s}}}}}", .{ if (cfg.sparse orelse false) "true" else "false", config_json, dimension_field });
        defer a.free(indexes);
        return managed.ManagedEmbedder.initFromIndexesJsonWithOptions(a, indexes, .{
            .antfly_provider = self.antfly_provider,
            .io = self.http.io,
            .bounded_http_request = true,
            .deadline_ns = self.context.deadline_ns,
            .cancellation = self.context.cancellation,
            .inference_api_url = self.inference_api_url,
            .inference_api_key = self.inference_api_key,
            .secret_store = self.secret_store,
            .provider_runtime = self.provider_runtime,
        });
    }

    pub fn embedSparse(self: Executor, a: std.mem.Allocator, name: []const u8, texts: []const []const u8) ![]managed.SparseEmbedding {
        const cfg = try self.view.providers.getEmbedderConfig(name);
        if (!(cfg.sparse orelse false)) return error.ModelTaskMismatch;
        var embedder = try self.initEmbedder(a, cfg);
        defer embedder.deinit();
        return embedder.sparseInterface().embedSparseBatchWithContext(a, "gateway", texts, self.context);
    }

    pub fn embedParts(self: Executor, a: std.mem.Allocator, name: []const u8, parts: []const @import("antfly_template_content").ContentPart, dimensions: ?u32) ![]f32 {
        var cfg = try self.view.providers.getEmbedderConfig(name);
        if (cfg.sparse orelse false) return error.UnsupportedInferenceFeature;
        if (dimensions) |dims| {
            cfg.dimension = dims;
            cfg.dimensions = dims;
        }
        var embedder = try self.initEmbedder(a, cfg);
        defer embedder.deinit();
        return embedder.denseInterface().embedDensePartsWithContext(a, "gateway", parts, cfg.dimensions orelse cfg.dimension orelse embedder.entries[0].dimensions, self.context);
    }

    pub fn transcribe(self: Executor, a: std.mem.Allocator, name: []const u8, request: transcribing.Request) !transcribing.Response {
        try self.context.check();
        var cfg = try (self.view.transcribers orelse return error.ModelNotFound).getConfig(name);
        const uses_default = cfg.provider == .antfly and cfg.resolvedUrl() == null;
        if (uses_default) if (self.antfly_provider) |provider| {
            const callback = provider.transcribe_audio_with_context orelse return error.UnsupportedInferenceFeature;
            const effective = transcribing.withRequestDefaults(cfg, request);
            return callback(provider.ptr, a, cfg.model orelse return error.ModelRequired, effective, self.context);
        };
        if (cfg.provider == .antfly and cfg.resolvedUrl() == null) if (self.inference_api_url) |url| {
            cfg.api_url = try gateway.apiBaseAlloc(a, url);
        };
        const key = cfg.bearer_token orelse cfg.api_key orelse (if (uses_default) self.inference_api_key else null);
        if (key) |value| cfg.bearer_token = try secrets.resolveReferenceOwned(a, self.secret_store, value);
        cfg.api_key = null;
        return transcribing.transcribeWithConfig(a, self.http, cfg, request, .{
            .timeout_ms = try self.context.remainingTimeoutMs(),
            .cancellation = if (self.context.cancellation) |v| httpx.CancellationToken.fromCallback(v.ptr, v.is_cancelled_fn) else null,
        });
    }

    pub fn synthesize(self: Executor, a: std.mem.Allocator, name: []const u8, request: synthesizing.Request) !synthesizing.SynthesisResult {
        try self.context.check();
        var cfg = try (self.view.synthesizers orelse return error.ModelNotFound).getConfig(name);
        if (cfg.provider == .vertex) {
            if (cfg.api_key) |value| cfg.api_key = try secrets.resolveReferenceOwned(a, self.secret_store, value);
            return synthesizing.synthesizeWithConfig(a, self.http, cfg, request, .{ .timeout_ms = try self.context.remainingTimeoutMs(), .cancellation = if (self.context.cancellation) |v| httpx.CancellationToken.fromCallback(v.ptr, v.is_cancelled_fn) else null });
        }
        var key = try secrets.SecretValue.initConfigOrEnv(a, cfg.api_key, if (cfg.provider == .elevenlabs) "ELEVENLABS_API_KEY" else "OPENAI_API_KEY");
        defer key.deinit(a);
        const resolved = try key.resolveOwned(a, self.secret_store);
        defer if (resolved) |value| a.free(value);
        cfg.api_key = resolved;
        return synthesizing.synthesizeWithConfig(a, self.http, cfg, request, .{
            .timeout_ms = try self.context.remainingTimeoutMs(),
            .cancellation = if (self.context.cancellation) |v| httpx.CancellationToken.fromCallback(v.ptr, v.is_cancelled_fn) else null,
        });
    }

    pub fn read(self: Executor, a: std.mem.Allocator, name: []const u8, request: readers.Request) !readers.BatchResult {
        try self.context.check();
        var cfg = try (self.view.readers orelse return error.ModelNotFound).getConfig(name);
        const uses_default = cfg.provider == .antfly and cfg.resolvedUrl() == null;
        if (cfg.provider == .antfly and cfg.resolvedUrl() == null and self.antfly_provider == null) if (self.inference_api_url) |endpoint| {
            cfg.api_url = try gateway.apiBaseAlloc(a, endpoint);
        };
        if (cfg.provider == .apple) return readers.readWithConfigReported(a, self.http, cfg, request, .{
            .timeout_ms = try self.context.remainingTimeoutMs(),
            .cancellation = if (self.context.cancellation) |v| httpx.CancellationToken.fromCallback(v.ptr, v.is_cancelled_fn) else null,
        });
        if (cfg.provider != .antfly and request.images.len != 1) return error.UnsupportedInferenceFeature;
        // Credentials are scoped to the provider; a runtime service key must
        // never be sent to a vendor reader.
        const configured_key = cfg.bearer_token orelse cfg.api_key orelse (if (uses_default) self.inference_api_key else null);
        const resolved = if (cfg.provider == .vertex)
            if (configured_key) |value| try secrets.resolveReferenceOwned(a, self.secret_store, value) else null
        else blk: {
            var key = try secrets.SecretValue.initConfigOrEnv(a, configured_key, if (cfg.provider == .openai) "OPENAI_API_KEY" else "ANTFLY_INFERENCE_API_KEY");
            defer key.deinit(a);
            break :blk try key.resolveOwned(a, self.secret_store);
        };
        defer if (resolved) |v| a.free(v);
        cfg.bearer_token = resolved;
        cfg.api_key = null;
        if (cfg.provider == .antfly and cfg.resolvedUrl() == null) if (self.antfly_provider) |provider| {
            if (provider.read_images_with_context) |callback| {
                var effective_request = request;
                effective_request.prompt = request.prompt orelse cfg.prompt;
                effective_request.max_tokens = request.max_tokens orelse cfg.max_tokens;
                const items = try callback(provider.ptr, a, cfg.model.?, effective_request, self.context);
                return .{ .items = items, .execution = .{ .requested_items = items.len, .serial_items = items.len } };
            }
        };
        return readers.readWithConfigReported(a, self.http, cfg, request, .{
            .timeout_ms = try self.context.remainingTimeoutMs(),
            .cancellation = if (self.context.cancellation) |v| httpx.CancellationToken.fromCallback(v.ptr, v.is_cancelled_fn) else null,
        });
    }
};

pub const GenerationOptions = struct {
    max_tokens: ?i64 = null,
    max_completion_tokens: ?i64 = null,
    temperature: ?f32 = null,
    tools_json: ?[]const u8 = null,
    tool_choice_json: ?[]const u8 = null,
};

/// Apply request options to a borrowed named config without mutating the registry.
pub fn withGenerationOptions(original: generating.GeneratorConfig, overrides: GenerationOptions) !generating.GeneratorConfig {
    var cfg = original;
    if (overrides.max_tokens) |v| {
        cfg.max_tokens = v;
        cfg.max_completion_tokens = null;
    }
    if (overrides.max_completion_tokens) |v| {
        if (overrides.max_tokens != null) return error.InvalidInferenceRequest;
        cfg.max_completion_tokens = v;
    }
    if (overrides.temperature) |v| cfg.temperature = v;
    if (overrides.tools_json) |v| {
        if (!cfg.provider.supportsTools()) return error.UnsupportedInferenceFeature;
        cfg.tools_json = v;
    }
    if (overrides.tool_choice_json) |v| cfg.tool_choice_json = v;
    try cfg.validate();
    return cfg;
}

/// Apply only native generation options to a generated-task envelope. Alias
/// transport, credentials and policy remain at the gateway; explicit request
/// options override borrowed defaults without changing the registry.
pub fn generatedGenerator(a: std.mem.Allocator, cfg: generating.GeneratorConfig, request: Json) !Json {
    if (request != .object) return error.InvalidInferenceRequest;
    if (cfg.provider != .antfly or cfg.tools_json != null or cfg.tool_choice_json != null or cfg.response_format_json != null or cfg.logprobs) return error.UnsupportedInferenceFeature;
    var forwarded = request;
    try forwarded.object.put(a, "model", .{ .string = cfg.model });
    if (!forwarded.object.contains("max_tokens")) try forwarded.object.put(a, "max_tokens", .{ .integer = cfg.max_tokens });
    inline for (.{ "temperature", "top_p", "top_k", "frequency_penalty", "presence_penalty" }) |field| {
        if (!forwarded.object.contains(field)) if (@field(cfg, field)) |value| {
            try forwarded.object.put(a, field, try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, value, .{}), .{}));
        };
    }
    return forwarded;
}

/// Keep native dictation options intact while applying named transcription
/// defaults with the same precedence as the typed executor.
pub fn transcriptionBody(a: std.mem.Allocator, cfg: transcribing.Config, request: Json) ![]u8 {
    return std.json.Stringify.valueAlloc(a, try transcriptionRequest(a, cfg, request, .one_shot), .{});
}

/// Project only defaults supported by the selected transcription surface.
pub fn transcriptionRequest(a: std.mem.Allocator, cfg: transcribing.Config, request: Json, surface: enum { one_shot, session }) !Json {
    var forwarded = request;
    try forwarded.object.put(a, "model", .{ .string = cfg.model orelse return error.ModelRequired });
    const defaults = transcribing.requestDefaults(cfg);
    inline for (.{ "language", "timestamps", "diarization" }) |field| {
        if (surface == .one_shot or comptime std.mem.eql(u8, field, "language")) {
            const supplied = forwarded.object.get(field);
            if (supplied == null or supplied.? == .null) if (@field(defaults, field)) |value| {
                try forwarded.object.put(a, field, try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, value, .{}), .{}));
            };
        }
    }
    return forwarded;
}

pub fn validateChatConfig(cfg: generating.GeneratorConfig) !void {
    if (cfg.top_k != null and cfg.provider != .antfly and cfg.provider != .ollama) return error.UnsupportedInferenceFeature;
    if (cfg.reasoning_effort != null and cfg.provider != .openai) return error.UnsupportedInferenceFeature;
}

pub fn chatBody(a: std.mem.Allocator, cfg: generating.GeneratorConfig, request: Json) ![]u8 {
    try validateChatConfig(cfg);
    var forwarded = request;
    try forwarded.object.put(a, "model", .{ .string = cfg.model });
    try forwarded.object.put(a, if (cfg.max_completion_tokens != null) "max_completion_tokens" else "max_tokens", .{ .integer = cfg.max_completion_tokens orelse cfg.max_tokens });
    if (cfg.temperature) |v| try forwarded.object.put(a, "temperature", .{ .float = v });
    if (cfg.tools_json) |v| try forwarded.object.put(a, "tools", try std.json.parseFromSliceLeaky(Json, a, v, .{}));
    if (cfg.tool_choice_json) |v| try forwarded.object.put(a, "tool_choice", try std.json.parseFromSliceLeaky(Json, a, v, .{}));
    inline for (.{ "top_p", "frequency_penalty", "presence_penalty" }) |field| {
        if (!forwarded.object.contains(field)) if (@field(cfg, field)) |v| try forwarded.object.put(a, field, .{ .float = v });
    }
    if (cfg.top_k) |v| {
        try forwarded.object.put(a, "top_k", .{ .integer = v });
    }
    if (cfg.reasoning_effort) |v| {
        try forwarded.object.put(a, "reasoning_effort", .{ .string = @tagName(v) });
    }
    return std.json.Stringify.valueAlloc(a, forwarded, .{});
}

pub fn chatRequirements(request: Json) !gateway.Requirements {
    var requirements: gateway.Requirements = .{};
    if (request != .object) return error.InvalidInferenceRequest;
    if (request.object.get("stream")) |value| {
        if (value != .bool) return error.InvalidInferenceRequest;
        requirements.streaming = value.bool;
    }
    requirements.tools = request.object.contains("tools") or request.object.contains("tool_choice");
    requirements.structured_output = request.object.contains("response_format");
    if (request.object.get("stream_options")) |options| {
        if (!requirements.streaming or options != .object) return error.UnsupportedInferenceFeature;
        for (options.object.keys(), options.object.values()) |key, value| {
            if (!std.mem.eql(u8, key, "include_usage") or value != .bool) return error.UnsupportedInferenceFeature;
        }
    }
    const messages = request.object.get("messages") orelse return error.InvalidInferenceRequest;
    if (messages != .array or messages.array.items.len == 0 or messages.array.items.len > 1024) return error.InvalidInferenceRequest;
    for (messages.array.items) |message| {
        if (message != .object) return error.InvalidInferenceRequest;
        if (message.object.contains("tool_calls") or message.object.contains("tool_call_id")) requirements.tools = true;
        if (message.object.get("role")) |role| if (role == .string and std.mem.eql(u8, role.string, "tool")) {
            requirements.tools = true;
        };
        if (message.object.get("content")) |content| if (content == .array) {
            for (content.array.items) |part| {
                if (part != .object) return error.InvalidInferenceRequest;
                const kind = part.object.get("type") orelse return error.InvalidInferenceRequest;
                if (kind != .string) return error.InvalidInferenceRequest;
                if (std.mem.eql(u8, kind.string, "media")) {
                    const mime = part.object.get("mime_type") orelse return error.InvalidInferenceRequest;
                    if (mime != .string) return error.InvalidInferenceRequest;
                    if (std.mem.startsWith(u8, mime.string, "image/")) requirements.images = true else if (std.mem.startsWith(u8, mime.string, "audio/")) requirements.audio = true else return error.UnsupportedInferenceFeature;
                } else if (std.mem.eql(u8, kind.string, "image_url")) requirements.images = true else if (std.mem.eql(u8, kind.string, "input_audio")) requirements.audio = true else if (!std.mem.eql(u8, kind.string, "text")) return error.UnsupportedInferenceFeature;
            }
        };
    }
    return requirements;
}

/// Typed messages retain the full content/tool history for budgets and
/// completed-result adapters. Rich transports also retain the original wire
/// request so refusal, structured output and vendor usage are not rewritten.
pub fn chatMessages(a: std.mem.Allocator, request: Json) ![]generating.ChatMessage {
    _ = try chatRequirements(request);
    const raw = request.object.get("messages").?.array.items;
    const messages = try a.alloc(generating.ChatMessage, raw.len);
    for (raw, messages) |value, *message| {
        const role = value.object.get("role") orelse return error.InvalidInferenceRequest;
        if (role != .string) return error.InvalidInferenceRequest;
        message.* = .{ .role = std.meta.stringToEnum(generating.Role, role.string) orelse return error.InvalidInferenceRequest };
        if (value.object.get("content")) |content| switch (content) {
            .string => message.content = .{ .text = content.string },
            .null => {},
            .array => |parts| {
                const typed = try a.alloc(generating.ContentPart, parts.items.len);
                for (parts.items, typed) |part, *out| {
                    const kind = part.object.get("type").?.string;
                    if (std.mem.eql(u8, kind, "text")) {
                        const text = part.object.get("text") orelse return error.InvalidInferenceRequest;
                        if (text != .string) return error.InvalidInferenceRequest;
                        out.* = .{ .text = text.string };
                    } else if (std.mem.eql(u8, kind, "image_url")) {
                        const image = part.object.get("image_url") orelse return error.InvalidInferenceRequest;
                        if (image != .object) return error.InvalidInferenceRequest;
                        const url = image.object.get("url") orelse return error.InvalidInferenceRequest;
                        if (url != .string) return error.InvalidInferenceRequest;
                        out.* = .{ .image_url = .{ .url = url.string } };
                    } else if (std.mem.eql(u8, kind, "media")) {
                        const mime = part.object.get("mime_type") orelse return error.InvalidInferenceRequest;
                        const data = part.object.get("data") orelse return error.InvalidInferenceRequest;
                        if (mime != .string or data != .string) return error.InvalidInferenceRequest;
                        out.* = .{ .media = .{ .mime_type = mime.string, .data = data.string } };
                    } else return error.UnsupportedInferenceFeature;
                }
                message.content = .{ .parts = typed };
            },
            else => return error.InvalidInferenceRequest,
        };
        if (value.object.get("tool_call_id")) |id| {
            if (id != .string) return error.InvalidInferenceRequest;
            message.tool_call_id = id.string;
        }
        if (value.object.get("tool_calls")) |calls| {
            if (calls != .array) return error.InvalidInferenceRequest;
            const typed = try a.alloc(generating.ToolCall, calls.array.items.len);
            for (calls.array.items, typed) |call, *out| {
                if (call != .object) return error.InvalidInferenceRequest;
                const id = call.object.get("id") orelse return error.InvalidInferenceRequest;
                const function = call.object.get("function") orelse return error.InvalidInferenceRequest;
                if (id != .string or function != .object) return error.InvalidInferenceRequest;
                const name = function.object.get("name") orelse return error.InvalidInferenceRequest;
                const arguments = function.object.get("arguments") orelse return error.InvalidInferenceRequest;
                if (name != .string or arguments != .string) return error.InvalidInferenceRequest;
                out.* = .{ .id = id.string, .name = name.string, .arguments = arguments.string };
            }
            message.tool_calls = typed;
        }
        if (message.content == null and message.tool_calls == null) return error.InvalidInferenceRequest;
        for (value.object.keys()) |key| {
            inline for (.{ "role", "content", "tool_calls", "tool_call_id" }) |allowed| {
                if (std.mem.eql(u8, key, allowed)) break;
            } else return error.UnsupportedInferenceFeature;
        }
    }
    return messages;
}

pub fn embeddingParts(a: std.mem.Allocator, input: Json) ![]@import("antfly_template_content").ContentPart {
    const values: []const Json = switch (input) {
        .array => input.array.items,
        .object => &.{input},
        else => return error.InvalidInferenceRequest,
    };
    if (values.len == 0) return error.InvalidInferenceRequest;
    const Part = @import("antfly_template_content").ContentPart;
    const parts = try a.alloc(Part, values.len);
    for (values, parts) |value, *part| {
        if (value != .object) return error.InvalidInferenceRequest;
        const kind = value.object.get("type") orelse return error.InvalidInferenceRequest;
        if (kind != .string) return error.InvalidInferenceRequest;
        if (std.mem.eql(u8, kind.string, "text")) {
            const text = value.object.get("text") orelse return error.InvalidInferenceRequest;
            if (text != .string) return error.InvalidInferenceRequest;
            part.* = .{ .text = text.string };
        } else if (std.mem.eql(u8, kind.string, "image_url")) {
            const image = value.object.get("image_url") orelse return error.InvalidInferenceRequest;
            if (image != .object) return error.InvalidInferenceRequest;
            const url = image.object.get("url") orelse return error.InvalidInferenceRequest;
            if (url != .string) return error.InvalidInferenceRequest;
            part.* = .{ .media_url = url.string };
        } else if (std.mem.eql(u8, kind.string, "media")) {
            const mime = value.object.get("mime_type") orelse return error.InvalidInferenceRequest;
            const encoded = value.object.get("data") orelse return error.InvalidInferenceRequest;
            if (mime != .string or encoded != .string) return error.InvalidInferenceRequest;
            const size = std.base64.standard.Decoder.calcSizeForSlice(encoded.string) catch return error.InvalidInferenceRequest;
            const bytes = try a.alloc(u8, size);
            std.base64.standard.Decoder.decode(bytes, encoded.string) catch return error.InvalidInferenceRequest;
            part.* = .{ .binary = .{ .mime_type = mime.string, .data = bytes } };
        } else return error.UnsupportedInferenceFeature;
    }
    return parts;
}
