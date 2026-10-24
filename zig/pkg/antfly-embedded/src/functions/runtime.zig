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

//! Request-owned provider execution and budgets; no retries or fallback.
const std = @import("std");
const httpx = @import("httpx");
const decisions = @import("decisions.zig");
const generated = @import("generated.zig");
const constrained_runtime = @import("../generating/constrained_runtime.zig");
const remote_capabilities = @import("antfly_inference_remote_capabilities");
const openai = @import("openai.zig");
const jev = @import("jev.zig");
const registry_mod = @import("../common/provider_registry.zig");
const secrets = @import("../common/secrets.zig");
const execution = @import("antfly_inference_execution_context");
const quotas = @import("../common/provider_limits.zig");
const admission_mod = @import("../common/request_admission.zig");
const managed = @import("../inference/managed_embedder.zig");
pub const max_response_bytes = 1024 * 1024;
var admission: admission_mod.RequestAdmission = admission_mod.RequestAdmission.init(16);

fn operationalError(err: anyerror) anyerror {
    return switch (err) {
        error.Cancelled,
        error.Canceled,
        error.Timeout,
        error.OutOfMemory,
        error.DecisionLimitExceeded,
        error.DecisionRateLimited,
        error.DecisionUnauthorized,
        error.DecisionUpstreamFailure,
        error.InvalidDecisionOutput,
        error.DecisionRefused,
        => err,
        error.ResponseTooLarge, error.ProviderTokenBudgetExceeded => error.DecisionLimitExceeded,
        error.InvalidRateLimitPolicy, error.ConflictingRateLimitPolicy => error.InvalidDeciderConfig,
        error.SecretNotFound => error.MissingDecisionApiKey,
        else => error.DecisionUpstreamFailure,
    };
}
fn parseResponse(a: std.mem.Allocator, bytes: []const u8) !decisions.Json {
    if (bytes.len > max_response_bytes) return error.DecisionLimitExceeded;
    return std.json.parseFromSliceLeaky(decisions.Json, a, bytes, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidDecisionOutput;
}

fn requestPayload(a: std.mem.Allocator, io: std.Io, cfg: decisions.DeciderConfig, request: decisions.Request) !decisions.WirePayload {
    if (cfg.decision_method == .generated) {
        try constrained_runtime.validateEvidence(cfg.generator.?, request.input);
        return .{ .body = try std.json.Stringify.valueAlloc(a, .{ .input = request.input.jsonValue(a), .questions = request.questions, .instructions = cfg.instructions }, .{}), .image_bytes = request.input.inlineImageBytes() };
    }
    if (cfg.provider == .openai) return .{ .body = try openai.requestBody(a, cfg.modelName(), request.input, request.questions), .image_bytes = request.input.inlineImageBytes() };
    if (cfg.provider == .cloudflare) return @import("cloudflare.zig").request(a, cfg, request.input, request.questions);
    if (cfg.provider == .jev) return jev.request(a, io, cfg, request.input, request.questions);
    return .{ .body = try decisions.wireRequest(a, cfg, try request.input.requireText(), request.questions) };
}

fn estimateInputTokens(cfg: decisions.DeciderConfig, input: decisions.Input, payload: decisions.WirePayload) !u64 {
    const text_bytes = std.math.sub(usize, payload.body.len, payload.image_bytes) catch return error.InvalidDecisionOutput;
    const count = input.imageCount();
    if (count == 0) return text_bytes;
    // URLs and compressed file bytes cannot bound the visual tokenization.
    // The operator supplies the provider/model's per-image upper bound.
    const bound = cfg.max_image_input_tokens orelse return error.InvalidDeciderConfig;
    const images = std.math.mul(u64, count, bound) catch return error.DecisionLimitExceeded;
    return std.math.add(u64, text_bytes, images) catch error.DecisionLimitExceeded;
}

const Usage = struct { input_tokens: u64 = 0, output_tokens: u64 = 0 };
fn reportedUsage(source: decisions.Json) ?Usage {
    if (source != .object) return null;
    const usage = source.object.get("usage") orelse return null;
    if (usage != .object) return null;
    const input = usage.object.get("input_tokens") orelse return null;
    const output = usage.object.get("output_tokens") orelse return null;
    if (input != .integer or output != .integer or input.integer < 0 or output.integer < 0) return null;
    return .{ .input_tokens = @intCast(input.integer), .output_tokens = @intCast(output.integer) };
}
fn upstreamFailure(status: u16) anyerror {
    return switch (status) {
        429, 529 => error.DecisionRateLimited,
        401, 403 => error.DecisionUnauthorized,
        else => error.DecisionUpstreamFailure,
    };
}
fn fatalItemFailure(err: anyerror) bool {
    return err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled or err == error.Timeout;
}

pub const Runtime = struct {
    registry: *const registry_mod.Registry,
    resolution_allocator: std.mem.Allocator = std.heap.smp_allocator,
    resolution_mutex: std.atomic.Mutex = .unlocked,
    resolved_generators: std.ArrayList(*ResolvedGenerator) = .empty,

    limits: *quotas.Registry = &quotas.process_registry,
    http: *httpx.Client,
    io: std.Io,
    context: ?execution.RequestContext = null,
    secret_store: ?*secrets.FileStore = null,
    antfly_provider: ?managed.AntflyProvider = null,
    provider_runtime: ?*managed.ProviderRuntime = null,
    antfly_url: ?[]const u8 = null,
    antfly_api_key: ?[]const u8 = null,
    source_table: []const u8 = "",
    rows: u64 = 0,
    reserved_rows: u64 = 0,
    input_tokens: u64 = 0,
    estimated_input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    batches: u64 = 0,
    latency_ns: u64 = 0,
    profile_allocator: ?std.mem.Allocator = null,
    provenance: std.ArrayList(struct { decider: []const u8, provider: []const u8, model: []const u8, rows: u64 = 0 }) = .empty,
    const ResolvedGenerator = struct {
        arena: std.heap.ArenaAllocator,
        cache: remote_capabilities.Cache,
        name: []const u8,
        source_table: []const u8,
        generator: @import("../generating/mod.zig").GeneratorConfig,
        capabilities: @import("antfly_structured").Capabilities,
    };

    /// Model facts live for this runtime's request/statement; routing leases renew.
    /// Profile/provenance storage remains owned by profile_allocator's caller.
    pub fn deinit(self: *Runtime) void {
        for (self.resolved_generators.items) |entry| {
            entry.cache.deinit();
            entry.arena.deinit();
            self.resolution_allocator.destroy(entry);
        }
        self.resolved_generators.deinit(self.resolution_allocator);
        self.resolved_generators = .empty;
    }

    fn resolveGenerator(self: *Runtime, name: []const u8, source_table: []const u8) !*ResolvedGenerator {
        @import("antfly_platform").sync.lockYielding(&self.resolution_mutex);
        defer self.resolution_mutex.unlock();
        for (self.resolved_generators.items) |entry| {
            if (std.mem.eql(u8, entry.name, name) and std.mem.eql(u8, entry.source_table, source_table)) return entry;
        }
        const cfg = try self.registry.getDeciderConfig(name);
        var arena = std.heap.ArenaAllocator.init(self.resolution_allocator);
        errdefer arena.deinit();
        const a = arena.allocator();
        var generator = try (cfg.generator orelse return error.InvalidDeciderConfig).clone(a);
        if (generator.provider == .antfly and generator.url.len == 0) {
            generator.url = try a.dupe(u8, self.antfly_url orelse "");
            if (generator.url.len > 0 and generator.api_key == null) {
                if (self.antfly_api_key) |key| generator.api_key = try a.dupe(u8, key);
            }
        }
        const entry = try self.resolution_allocator.create(ResolvedGenerator);
        errdefer self.resolution_allocator.destroy(entry);
        entry.cache = remote_capabilities.Cache.init(self.resolution_allocator, self.io);
        errdefer entry.cache.deinit();
        var engine = self.constrainedRuntime(&entry.cache);
        engine.options.source_table = source_table;
        engine.options.execution.routing.source_table = source_table;
        const caps = try engine.capabilities(a, &generator);
        const owned_name = try a.dupe(u8, name);
        const owned_table = try a.dupe(u8, source_table);
        entry.arena = arena;
        entry.name = owned_name;
        entry.source_table = owned_table;
        entry.generator = generator;
        entry.capabilities = caps;
        try self.resolved_generators.append(self.resolution_allocator, entry);
        return entry;
    }

    fn recordProvenance(self: *Runtime, name: []const u8, cfg: decisions.DeciderConfig, value: decisions.Json) !void {
        const stats_a = self.profile_allocator orelse return;
        const model = value.object.get("model").?.string;
        const provider_name = if (cfg.generator) |generator| @tagName(generator.provider) else @tagName(cfg.provider);
        for (self.provenance.items) |*item| if (std.mem.eql(u8, item.decider, name) and std.mem.eql(u8, item.model, model) and std.mem.eql(u8, item.provider, provider_name)) {
            item.rows += 1;
            return;
        };
        const decider = try stats_a.dupe(u8, name);
        errdefer stats_a.free(decider);
        const owned_model = try stats_a.dupe(u8, model);
        errdefer stats_a.free(owned_model);
        try self.provenance.append(stats_a, .{ .decider = decider, .provider = provider_name, .model = owned_model, .rows = 1 });
    }
    pub fn provider(self: *@This()) decisions.DecisionProvider {
        return .{ .ptr = self, .validate_fn = validate, .evaluate_batch_fn = evaluate, .checkpoint_fn = checkpoint, .capabilities_fn = resolvedCapabilities };
    }
    fn resolvedCapabilities(ptr: *anyopaque, name: []const u8, source_table: []const u8) !decisions.Capabilities {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        var result = try self.registry.getDeciderCapabilities(name);
        const cfg = try self.registry.getDeciderConfig(name);
        if (cfg.decision_method == .generated and cfg.generator.?.provider == .antfly and result.images) {
            result.images = (try self.resolveGenerator(name, if (source_table.len > 0) source_table else self.source_table)).capabilities.images;
        }
        return result;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (self.context) |ctx| try ctx.check();
    }
    fn validate(ptr: *anyopaque, name: []const u8, questions: decisions.Json, source_table: []const u8) !void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        const cfg = try self.registry.getDeciderConfig(name);
        try cfg.validate();
        _ = try quotas.Policy.fromConfig(cfg.rate_limit);
        try decisions.validateQuestions(questions, cfg.resolvedCapabilities());
        if (cfg.decision_method == .generated) {
            var arena = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
            defer arena.deinit();
            const resolved = try self.resolveGenerator(name, if (source_table.len > 0) source_table else self.source_table);
            const generator = resolved.generator;
            const mode = try resolved.capabilities.select(cfg.output, cfg.probabilities);
            try @import("antfly_structured").validateProviderSchema(generator, mode, try generated.questionSchema(arena.allocator(), questions));
            if (cfg.probabilities == .token_logprobs) for (questions.object.values()) |question| {
                const kind = question.object.get("type").?.string;
                if (std.mem.eql(u8, kind, "multi_choice") or std.mem.eql(u8, kind, "score")) return error.UnsupportedConstrainedProbabilities;
            };
        }
    }
    fn constrainedRuntime(self: *Runtime, cache: *remote_capabilities.Cache) constrained_runtime.Runtime {
        return .{ .http = self.http, .cache = cache, .options = .{
            .antfly_provider = self.antfly_provider,
            .provider_runtime = self.provider_runtime,
            .secret_store = self.secret_store,
            .inference_api_key = self.antfly_api_key,
            .max_response_bytes = max_response_bytes,
            .source_table = self.source_table,
            .execution = .{ .routing = .{ .source_table = self.source_table } },
            .request_context = self.context,
            .limits = self.limits,
        } };
    }
    pub fn evaluateGeneratedPartial(self: *Runtime, a: std.mem.Allocator, name: []const u8, requests: []const decisions.Request) !GeneratedBatch {
        if (requests.len == 0) return error.InvalidDecisionSpecification;
        try self.provider().validateRequests(a, requests);
        try self.provider().withSourceTable(requests[0].source_table).validate(name, requests[0].questions);
        const cfg = try self.registry.getDeciderConfig(name);
        if (cfg.decision_method != .generated) return error.InvalidDeciderConfig;
        var lease = admission.tryAcquireLease() orelse return error.DecisionRateLimited;
        defer lease.release();
        if (self.reserved_rows + requests.len > cfg.max_rows) return error.DecisionLimitExceeded;
        var estimated = self.estimated_input_tokens;
        for (requests) |request| {
            try request.input.checkLimit(a, cfg.resolvedCapabilities().max_input_bytes);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const payload = try requestPayload(scratch.allocator(), self.io, cfg, request);
            estimated = std.math.add(u64, estimated, try estimateInputTokens(cfg, request.input, payload)) catch return error.DecisionLimitExceeded;
            if (estimated > cfg.max_input_tokens) return error.DecisionLimitExceeded;
        }
        self.estimated_input_tokens = estimated;
        self.reserved_rows += requests.len;
        const result = try self.generateBatchPartial(a, cfg, requests);
        self.rows += requests.len;
        self.input_tokens +|= result.input_tokens;
        self.output_tokens +|= result.output_tokens;
        self.batches +|= result.calls;
        if (self.input_tokens > cfg.max_input_tokens) return error.DecisionLimitExceeded;
        return result;
    }
    fn generateBatchPartial(self: *Runtime, a: std.mem.Allocator, cfg: decisions.DeciderConfig, requests: []const decisions.Request) !GeneratedBatch {
        const source_table = if (requests[0].source_table.len > 0) requests[0].source_table else self.source_table;
        const resolved = try self.resolveGenerator(requests[0].decider, source_table);
        var engine = self.constrainedRuntime(&resolved.cache);
        engine.capability_snapshot = resolved.capabilities;
        engine.require_execution_lease = resolved.generator.capability_token != null;
        engine.options.source_table = source_table;
        engine.options.execution.routing.source_table = source_table;
        var generator = try resolved.generator.clone(a);
        defer generator.deinit(a);
        const caps = resolved.capabilities;
        engine.limits = caps.limits;
        const mode = try caps.select(cfg.output, cfg.probabilities);
        generator.logprobs = cfg.probabilities == .token_logprobs;
        const inputs = try a.alloc(decisions.Json, requests.len);
        defer a.free(inputs);
        for (inputs, requests) |*input, request| {
            if (request.input.imageCount() > 0 and !caps.images) return error.UnsupportedInferenceFeature;
            input.* = request.input.jsonValue(a);
        }
        generator.image_input_token_bound = cfg.max_image_input_tokens;
        var batch = cfg.batch;
        batch.max_items_per_call = @min(batch.max_items_per_call, cfg.batch_size);
        const responses = try a.alloc(?decisions.Json, requests.len);
        @memset(responses, null);
        var validation = GeneratedValidation{ .cfg = cfg, .requests = requests, .responses = responses };
        engine.semantic_validation = .{ .ptr = &validation, .validate = GeneratedValidation.validate };
        const result = try engine.executeEvidence(a, generator, mode, try generated.questionSchema(a, requests[0].questions), cfg.instructions, batch, inputs);
        for (responses) |*response| if (response.*) |*value| {
            if (caps.model_revision) |revision| try value.object.put(a, "model_revision", .{ .string = try a.dupe(u8, revision) });
        };
        // Charge all attempts once, including attempts for invalid items.
        for (responses) |*response| if (response.*) |*value| {
            const usage = value.object.getPtr("usage").?;
            try usage.object.put(a, "input_tokens", .{ .integer = @intCast(result.input_tokens) });
            try usage.object.put(a, "output_tokens", .{ .integer = @intCast(result.output_tokens) });
            break;
        };
        return .{ .values = responses, .failures = result.failures, .input_tokens = result.input_tokens, .output_tokens = result.output_tokens, .calls = result.calls, .execution = result.execution };
    }
    pub const GeneratedBatch = struct {
        values: []?decisions.Json,
        failures: []?anyerror,
        input_tokens: u64,
        output_tokens: u64,
        calls: usize,
        execution: @import("antfly_structured").Report,
    };
    const GeneratedValidation = struct {
        cfg: decisions.DeciderConfig,
        requests: []const decisions.Request,
        responses: []?decisions.Json,
        fn validate(ptr: *anyopaque, a: std.mem.Allocator, id: usize, value: decisions.Json, proof: ?@import("antfly_structured").TokenEvidence) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.responses[id] = try generated.responseValue(a, self.cfg, self.requests[id].questions, value, proof, id, 0, 0);
        }
    };
    const Job = struct {
        runtime: *Runtime,
        request: decisions.Request,
        cfg: decisions.DeciderConfig,
        arena: std.heap.ArenaAllocator,
        budget: @import("../sql/memory_budget.zig"),
        response: ?decisions.Json = null,
        failure: ?anyerror = null,
        usage: Usage = .{},
        fn observeUsage(self: *Job, reservation: *quotas.TokenReservation, source: decisions.Json) void {
            self.usage = reportedUsage(source) orelse return;
            reservation.reconcile(self.usage.input_tokens, self.usage.output_tokens);
        }
        fn run(self: *Job) void {
            self.response = self.invoke() catch |err| {
                self.failure = if (self.budget.exhausted) error.DecisionLimitExceeded else err;
                return;
            };
        }
        fn isCancelled(ptr: *const anyopaque) bool {
            const self: *const Job = @ptrCast(@alignCast(ptr));
            const ctx = self.runtime.context orelse return false;
            return if (ctx.cancellation) |token| token.isCancelled() else false;
        }
        fn invoke(self: *Job) !decisions.Json {
            try checkpoint(self.runtime);
            const a = self.arena.allocator();
            const cfg = self.cfg;
            if (cfg.decision_method == .generated) {
                const result = try self.runtime.generateBatchPartial(a, cfg, &.{self.request});
                self.usage = .{ .input_tokens = result.input_tokens, .output_tokens = result.output_tokens };
                return result.values[0] orelse return result.failures[0] orelse error.InvalidDecisionOutput;
            }
            if (cfg.embedder != null) {
                const result = try @import("embedding_decisions.zig").batchPartial(a, self.runtime, cfg, &.{self.request});
                self.usage = .{ .input_tokens = result.input_tokens };
                return result.values[0] orelse return result.failures[0] orelse error.InvalidDecisionOutput;
            }
            const payload = try requestPayload(a, self.runtime.io, cfg, self.request);
            const body = payload.body;
            const base = if (cfg.provider == .antfly and cfg.url.len == 0) if (self.runtime.antfly_url) |endpoint| try registry_mod.gateway.apiBaseAlloc(a, endpoint) else cfg.baseUrl() else cfg.baseUrl();
            const url = if (cfg.provider == .cloudflare) (if (cfg.url.len != 0) cfg.url else try @import("antfly_decisions").cloudflare.endpoint(a, cfg.account_id.?, cfg.modelName())) else try std.fmt.allocPrint(a, "{s}{s}", .{ std.mem.trimEnd(u8, base, "/"), switch (cfg.provider) {
                .antfly => "/decisions",
                .jev => "/v1/systemone",
                .openai => "/decisions",
                .cloudflare => unreachable,
            } });
            var key_source = try secrets.SecretValue.initConfigOrEnv(a, cfg.api_key orelse (if (cfg.provider == .antfly and cfg.url.len == 0) self.runtime.antfly_api_key else null), switch (cfg.provider) {
                .antfly => "ANTFLY_INFERENCE_API_KEY",
                .jev => "TYPESAFE_API_KEY",
                .openai => "OPENAI_API_KEY",
                .cloudflare => "CLOUDFLARE_API_TOKEN",
            });
            defer key_source.deinit(a);
            var quota = try self.runtime.limits.acquire(.{ .operation = .decision, .endpoint = .{
                .provider = std.meta.stringToEnum(quotas.Provider, @tagName(cfg.provider)).?,
                .endpoint = if (cfg.provider == .cloudflare) url else base,
                .model = cfg.modelName(),
                .credentials = @import("../common/credential_source_identity.zig").fromSecretValue(key_source),
                .gateway_id = cfg.gateway_id orelse "",
            } }, try quotas.Policy.fromConfig(cfg.rate_limit));
            defer quota.release();
            var reservation: quotas.TokenReservation = .{ .limiter = quota.limiter(), .input_tokens = try estimateInputTokens(cfg, self.request.input, payload) };
            defer if (reservation.reported_usage) |usage| {
                self.usage = .{ .input_tokens = usage.input_tokens, .output_tokens = usage.output_tokens };
            };
            const observer = reservation.observer();
            if (cfg.provider == .antfly and cfg.url.len == 0) if (self.runtime.antfly_provider) |embedded| if (embedded.decide_json) |_| {
                try observer.before(observer.ptr, .{
                    .io = self.runtime.io,
                    .deadline_ms = if (if (self.runtime.context) |ctx| try ctx.remainingTimeoutMs() else @as(?u64, 30000)) |timeout| std.Io.Clock.awake.now(self.runtime.io).toMilliseconds() + @as(i64, @intCast(timeout)) else null,
                    .cancellation_ptr = self,
                    .is_cancelled = isCancelled,
                    .body_bytes = body.len,
                    .output_tokens = 0,
                });
                var feedback: ?httpx.Response = null;
                defer if (feedback) |*response| response.deinit();
                defer observer.after(observer.ptr, if (feedback) |*response| response else null);
                const bytes = embedded.decideJson(a, body, self.runtime.context) catch |err| {
                    if (err == error.DecisionRateLimited) feedback = httpx.Response.init(a, 429);
                    return operationalError(err);
                };
                const parsed = try parseResponse(a, bytes);
                self.observeUsage(&reservation, parsed);
                return parsed;
            };
            const key = key_source.resolveOwned(a, self.runtime.secret_store) catch |err| return operationalError(err);
            if (cfg.provider != .antfly and key == null) return error.MissingDecisionApiKey;
            const headers = try a.alloc([2][]const u8, payload.headers.len + 4);
            var count: usize = 0;
            if (key) |value| {
                headers[count] = .{ "Authorization", try std.fmt.allocPrint(a, "Bearer {s}", .{value}) };
                count += 1;
            }
            headers[count] = .{ "Content-Type", payload.content_type };
            count += 1;
            for (payload.headers) |header| {
                headers[count] = header;
                count += 1;
            }
            if (cfg.provider == .cloudflare) if (cfg.gateway_id) |id| {
                headers[count] = .{ "cf-aig-gateway-id", id };
                count += 1;
            };
            const source_table = if (self.request.source_table.len > 0) self.request.source_table else self.runtime.source_table;
            if (cfg.provider == .antfly and source_table.len > 0) {
                headers[count] = .{ execution.source_table_header, source_table };
                count += 1;
            }
            const ctx = self.runtime.context;
            var response = self.runtime.http.post(url, .{
                .body = body,
                .max_response_size = max_response_bytes,
                .max_retries = 0,
                .cookies_enabled = false,
                .attempt_observer = observer,
                .headers = headers[0..count],
                .timeout_ms = if (ctx) |context| try context.remainingTimeoutMs() else 30000,
                .cancellation = if (ctx) |context| if (context.cancellation) |token| httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn) else null else null,
            }) catch |err| return operationalError(err);
            defer response.deinit();
            const bytes = response.body orelse return if (response.ok()) error.InvalidDecisionOutput else upstreamFailure(response.status.code);
            const parsed = parseResponse(a, bytes) catch |err| return if (response.ok()) err else upstreamFailure(response.status.code);
            // Account usage before semantic output validation, including refused
            // or invalid answers for which the provider has already spent tokens.
            const measured = if (cfg.provider == .cloudflare and parsed == .object) parsed.object.get("result") orelse parsed else parsed;
            self.observeUsage(&reservation, measured);
            if (!response.ok()) return upstreamFailure(response.status.code);
            if (cfg.provider == .antfly) {
                const root = try decisions.object(parsed);
                const answers = root.get("answers") orelse return error.InvalidDecisionOutput;
                if (answers != .array) return error.InvalidDecisionOutput;
            }
            if (cfg.provider == .cloudflare) return @import("antfly_decisions").cloudflare.unwrap(parsed);
            return if (cfg.provider == .openai) openai.response(a, self.request.questions, parsed) else parsed;
        }
    };
    pub const PartialBatch = struct {
        values: []?decisions.Json,
        failures: []?anyerror,
    };
    pub fn evaluatePartial(self: *Runtime, a: std.mem.Allocator, requests: []const decisions.Request) !PartialBatch {
        try self.provider().validateRequests(a, requests);
        return self.evaluateCore(a, requests, true);
    }
    fn evaluate(ptr: *anyopaque, a: std.mem.Allocator, requests: []const decisions.Request) ![]const decisions.Json {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        const result = try self.evaluateCore(a, requests, false);
        const out = try a.alloc(decisions.Json, requests.len);
        for (result.values, result.failures, out) |value, failure, *target| target.* = value orelse return failure orelse error.InvalidDecisionOutput;
        return out;
    }
    fn evaluateCore(self: *Runtime, a: std.mem.Allocator, requests: []const decisions.Request, preserve_failures: bool) !PartialBatch {
        const ptr: *anyopaque = self;
        const started = std.Io.Clock.awake.now(self.io).nanoseconds;
        defer self.latency_ns +|= @intCast(@max(0, std.Io.Clock.awake.now(self.io).nanoseconds - started));
        var lease = admission.tryAcquireLease() orelse return error.DecisionRateLimited;
        defer lease.release();
        const out = try a.alloc(?decisions.Json, requests.len);
        @memset(out, null);
        const failures = try a.alloc(?anyerror, requests.len);
        @memset(failures, null);
        var batch_size: usize = 32;
        var estimated = self.estimated_input_tokens;
        for (requests) |request| {
            const cfg = try self.registry.getDeciderConfig(request.decider);
            if (cfg.decision_method == .generated) _ = try self.resolveGenerator(request.decider, if (request.source_table.len > 0) request.source_table else self.source_table);
            if (self.reserved_rows + requests.len > cfg.max_rows or self.input_tokens >= cfg.max_input_tokens) return error.DecisionLimitExceeded;
            try request.input.checkLimit(a, cfg.resolvedCapabilities().max_input_bytes);
            // Reserve text bytes plus the configured visual token bound before
            // any I/O. The shared quota uses this same estimate and actual usage.
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const payload = try requestPayload(scratch.allocator(), self.io, cfg, request);
            const estimate = try estimateInputTokens(cfg, request.input, payload);
            const policy = try quotas.Policy.fromConfig(cfg.rate_limit);
            if (policy.tokens_per_minute != 0 and estimate > policy.tokens_per_minute) return error.DecisionLimitExceeded;
            estimated = std.math.add(u64, estimated, estimate) catch return error.DecisionLimitExceeded;
            if (estimated > cfg.max_input_tokens) return error.DecisionLimitExceeded;
            batch_size = @min(batch_size, cfg.batch_size);
        }
        self.estimated_input_tokens = estimated;
        self.reserved_rows += requests.len;
        if (requests.len > 0) {
            const first = try self.registry.getDeciderConfig(requests[0].decider);
            var compatible = first.decision_method == .generated or first.embedder != null;
            const question_bytes = try std.json.Stringify.valueAlloc(a, requests[0].questions, .{});
            defer a.free(question_bytes);
            for (requests[1..]) |request| {
                const bytes = try std.json.Stringify.valueAlloc(a, request.questions, .{});
                defer a.free(bytes);
                compatible = compatible and std.mem.eql(u8, request.decider, requests[0].decider) and std.mem.eql(u8, request.source_table, requests[0].source_table) and std.mem.eql(u8, bytes, question_bytes);
            }
            if (compatible and first.embedder != null) {
                const partial = try @import("embedding_decisions.zig").batchPartial(a, self, first, requests);
                self.input_tokens +|= partial.input_tokens;
                self.batches += 1;
                for (partial.values, partial.failures, out, failures, requests) |value, failure, *dest, *failed, request| {
                    if (value) |response| {
                        dest.* = response;
                        self.rows += 1;
                        try self.recordProvenance(request.decider, first, response);
                    } else {
                        const err = failure orelse error.InvalidDecisionOutput;
                        if (!preserve_failures or fatalItemFailure(err)) return err;
                        failed.* = err;
                    }
                }
                return .{ .values = out, .failures = failures };
            }
            if (compatible) {
                const partial = try self.generateBatchPartial(a, first, requests);
                self.rows += requests.len;
                self.input_tokens +|= partial.input_tokens;
                self.output_tokens +|= partial.output_tokens;
                self.batches +|= partial.calls;
                if (self.input_tokens > first.max_input_tokens) return error.DecisionLimitExceeded;
                for (partial.values, partial.failures, out, failures, requests) |value, failure, *response, *failed, request| {
                    if (value) |valid| {
                        response.* = valid;
                        try self.recordProvenance(request.decider, first, valid);
                    } else {
                        const err = failure orelse error.InvalidDecisionOutput;
                        if (!preserve_failures or fatalItemFailure(err)) return err;
                        failed.* = err;
                    }
                }
                return .{ .values = out, .failures = failures };
            }
        }

        var begin: usize = 0;
        while (begin < requests.len) {
            try checkpoint(ptr);
            const end = @min(begin + batch_size, requests.len);
            const jobs = try a.alloc(Job, end - begin);
            defer a.free(jobs);
            var initialized: usize = 0;
            defer for (jobs[0..initialized]) |*job| job.arena.deinit();
            var group = std.Io.Group.init;
            defer group.cancel(self.io);
            for (jobs, requests[begin..end]) |*job, request| {
                job.* = .{ .runtime = self, .request = request, .cfg = try self.registry.getDeciderConfig(request.decider), .arena = undefined, .budget = .{ .backing = std.heap.smp_allocator, .limit = 8 * 1024 * 1024 } };
                job.arena = std.heap.ArenaAllocator.init(job.budget.allocator());
                initialized += 1;
                // A single decision already runs on its caller's execution
                // lane. Requiring another worker can reject otherwise valid
                // concurrent queries on a bounded backend IO runtime.
                if (jobs.len == 1) job.run() else try group.concurrent(self.io, Job.run, .{job});
            }
            try group.await(self.io);
            self.batches += 1;
            try checkpoint(ptr);
            for (jobs, out[begin..end], failures[begin..end]) |*job, *value, *failure| {
                const usage = if (job.response) |response| reportedUsage(response) orelse job.usage else job.usage;
                self.input_tokens +|= usage.input_tokens;
                self.output_tokens +|= usage.output_tokens;
                if (self.input_tokens > job.cfg.max_input_tokens) return error.DecisionLimitExceeded;
                if (job.failure) |err| {
                    if (!preserve_failures or fatalItemFailure(err)) return err;
                    failure.* = err;
                    continue;
                }
                const normalized = self.normalizeJob(a, job) catch |err| {
                    if (!preserve_failures or fatalItemFailure(err)) return err;
                    failure.* = err;
                    continue;
                };
                try self.recordProvenance(job.request.decider, job.cfg, normalized);
                self.rows += 1;
                value.* = normalized;
            }
            begin = end;
        }
        return .{ .values = out, .failures = failures };
    }
    fn normalizeJob(_: *Runtime, a: std.mem.Allocator, job: *const Job) !decisions.Json {
        if (job.cfg.model_identity) |expected| {
            const root = try decisions.object(job.response.?);
            const actual = root.get("model_identity") orelse return error.InvalidDecisionOutput;
            if (actual != .string or !std.mem.eql(u8, actual.string, expected)) return error.InvalidDecisionOutput;
        }
        return decisions.normalizeResponseWithConfig(a, job.request.questions, job.response.?, job.cfg);
    }
};

test "decision functions single embedded decision needs no additional IO worker" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{ .async_limit = .nothing, .concurrent_limit = .nothing });
    defer io_impl.deinit();
    const io = io_impl.io();
    var client = httpx.Client.init(a, io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("local", .{ .provider = .antfly, .model = "test-model" });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const Fake = struct {
        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }
        fn sparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]@import("../storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }
        fn extract(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @import("antfly_extracting").Request) !@import("antfly_extracting").Response {
            return error.TestUnexpectedResult;
        }
        fn decide(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: ?managed.RequestContext) ![]u8 {
            return alloc.dupe(u8,
                \\{"model":"test-model","answers":[{"name":"same","type":"choice","decision_method":"typed","choice":"same","confidence":0.98,"confidence_method":"max_probability","probabilities":[{"value":"same","probability":0.98},{"value":"different","probability":0.02}]}],"usage":{"input_tokens":8,"output_tokens":0}}
            );
        }
    };
    var token: u8 = 0;
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io, .antfly_provider = .{
        .ptr = &token,
        .owns_invocation_admission = true,
        .embed_dense_texts = Fake.dense,
        .embed_sparse_texts = Fake.sparse,
        .extract = Fake.extract,
        .decide_json = Fake.decide,
    } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\{"same":{"type":"choice","instructions":"Same entity?","criteria":{"same":"Same entity","different":"Different entities"}}}
    , .{});
    const results = try runtime.provider().evaluateBatch(scratch, &.{.{ .decider = "local", .questions = questions, .input = .{ .text = "Antfly / Antfly database" } }});
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(u64, 1), runtime.rows);
}

test "decision functions concurrent batches preserve one input per wire request credentials and usage" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer decision-test", req.header("Authorization") orelse return error.TestUnexpectedResult);
            if (std.mem.eql(u8, req.path, "/decisions")) try std.testing.expectEqualStrings("docs", req.header(execution.source_table_header) orelse return error.TestUnexpectedResult) else try std.testing.expect(req.header(execution.source_table_header) == null);
            const parsed = try std.json.parseFromSlice(decisions.Json, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("refund", parsed.value.object.get(if (std.mem.eql(u8, req.path, "/decisions")) "input" else "state").?.string);
            try std.testing.expectEqualStrings("test-model", parsed.value.object.get("model").?.string);
            try std.testing.expect(!parsed.value.object.contains("inputs"));
            const questions = parsed.value.object.get("questions").?;
            if (questions == .array) {
                try std.testing.expectEqualStrings("predicate", questions.array.items[0].object.get("type").?.string);
            } else try decisions.validateQuestions(questions, decisions.capabilities(.jev));
        }
    };
    const response = "{\"model\":\"test-model\",\"answers\":[{\"name\":\"answer\",\"type\":\"predicate\",\"decision_method\":\"typed\",\"probability\":0.9}],\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}";
    const jev_response = "{\"model\":\"test-model\",\"answers\":{\"answer\":{\"type\":\"noul\",\"noul\":0.9}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}";
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/decisions", .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = response } },
        .{ .method = .POST, .path = "/v1/systemone", .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = jev_response } },
        .{ .method = .POST, .path = "/decisions", .respond = .{ .body = "{\"model\":\"test-model\",\"answers\":{},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}" } },
        .{ .method = .POST, .path = "/v1/systemone", .respond = .{ .status = 429, .body = "rate limited" } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("local", .{ .provider = .antfly, .model = "test-model", .url = server.baseUrl(), .api_key = "decision-test", .max_rows = 2, .batch_size = 32 });
    try registry.registerDeciderConfig("remote", .{ .provider = .jev, .model = "test-model", .url = server.baseUrl(), .api_key = "decision-test", .max_rows = 2 });
    try registry.registerDeciderConfig("budgeted", .{ .provider = .antfly, .model = "test-model", .url = server.baseUrl(), .max_input_tokens = 1 });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const questions = try decisions.questionsFor(arena.allocator(), .ai_probability, &.{ .{ .string = "refund" }, .{ .string = "Refund?" }, .{ .string = "local" } });
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    defer runtime.deinit();
    const Run = struct {
        fn run(r: *Runtime, alloc: std.mem.Allocator, q: decisions.Json, failure: *?anyerror) void {
            const results = r.provider().withSourceTable("docs").evaluateBatch(alloc, &.{ .{ .input = .{ .text = "refund" }, .decider = "local", .questions = q }, .{ .input = .{ .text = "refund" }, .decider = "remote", .questions = q } }) catch |err| {
                failure.* = err;
                return;
            };
            std.testing.expectEqual(@as(usize, 2), results.len) catch |err| {
                failure.* = err;
            };
        }
    };
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Run.run, .{ &runtime, arena.allocator(), questions, &failure });
    try server.handleOne();
    try server.handleOne();
    try group.await(io);
    if (failure) |err| return err;
    try std.testing.expectEqual(@as(u64, 2), runtime.rows);
    try std.testing.expectEqual(@as(u64, 4), runtime.input_tokens);
    var budgeted: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    defer budgeted.deinit();
    try std.testing.expectError(error.DecisionLimitExceeded, budgeted.provider().evaluateBatch(arena.allocator(), &.{.{ .input = .{ .text = "refund" }, .decider = "budgeted", .questions = questions }}));
    try std.testing.expectEqual(@as(u64, 0), budgeted.rows);
    const Once = struct {
        fn run(r: *Runtime, alloc: std.mem.Allocator, name: []const u8, q: decisions.Json, err: *?anyerror) void {
            _ = r.provider().evaluateBatch(alloc, &.{.{ .input = .{ .text = "refund" }, .decider = name, .questions = q }}) catch |failure_err| {
                err.* = failure_err;
                return;
            };
        }
    };
    for ([_][]const u8{ "local", "remote" }, [_]anyerror{ error.InvalidDecisionOutput, error.DecisionRateLimited }) |name, expected| {
        var failing: Runtime = .{ .registry = &registry, .http = &client, .io = io };
        defer failing.deinit();
        failure = null;
        try group.concurrent(io, Once.run, .{ &failing, arena.allocator(), name, questions, &failure });
        try server.handleOne();
        try group.await(io);
        try std.testing.expectEqual(@as(?anyerror, expected), failure);
    }
    try std.testing.expectError(error.DecisionLimitExceeded, runtime.provider().evaluateBatch(arena.allocator(), &.{.{ .input = .{ .text = "refund" }, .decider = "local", .questions = questions }}));
    var cancelled = std.atomic.Value(bool).init(true);
    runtime.context = .{ .io = io, .deadline_ns = null, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) };
    try std.testing.expectError(error.Cancelled, runtime.provider().evaluateBatch(arena.allocator(), &.{.{ .input = .{ .text = "refund" }, .decider = "local", .questions = questions }}));
}

test "decision functions HTTP ceiling rejects oversized advertised bodies before downloading" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const oversized = try a.alloc(u8, max_response_bytes + 1);
    defer a.free(oversized);
    @memset(oversized, 'x');
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/decisions", .max_uses = 1, .respond = .{ .body = oversized, .truncate_body_at = 0 } },
        .{ .method = .POST, .path = "/v1/systemone", .max_uses = 1, .respond = .{ .body = oversized, .truncate_body_at = 0 } },
        .{ .method = .POST, .path = "/decisions", .max_uses = 1, .respond = .{ .body = oversized, .truncate_body_at = 0 } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false, .max_response_size = 64 * 1024 * 1024 });
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("local", .{ .provider = .antfly, .model = "mock", .url = server.baseUrl() });
    try registry.registerDeciderConfig("remote", .{ .provider = .jev, .api_key = "test", .url = server.baseUrl() });
    try registry.registerDeciderConfig("openai", .{ .provider = .openai, .model = "mock", .api_key = "test", .url = server.baseUrl() });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const questions = try decisions.questionsFor(arena.allocator(), .ai_probability, &.{ .{ .string = "refund" }, .{ .string = "Refund?" }, .{ .string = "local" } });
    const Run = struct {
        fn run(r: *Runtime, alloc: std.mem.Allocator, name: []const u8, q: decisions.Json, failure: *?anyerror) void {
            _ = r.provider().evaluateBatch(alloc, &.{.{ .decider = name, .questions = q, .input = .{ .text = "refund" } }}) catch |err| {
                failure.* = err;
                return;
            };
        }
    };
    for ([_][]const u8{ "local", "remote", "openai" }) |name| {
        var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io };
        defer runtime.deinit();
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Run.run, .{ &runtime, arena.allocator(), name, questions, &failure });
        try server.handleOne();
        try group.await(io);
        try std.testing.expectEqual(@as(?anyerror, error.DecisionLimitExceeded), failure);
        try std.testing.expectEqual(@as(u64, 0), runtime.rows);
    }
}

test "decision functions EmbeddingGemma 2 HTTP preserves identity calibration and request budgets" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Check = struct {
        const identity = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            const parsed = try std.json.parseFromSlice(decisions.Json, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            const root = parsed.value.object;
            try std.testing.expectEqualStrings("embeddinggemma2", root.get("model").?.string);
            try std.testing.expectEqualStrings(identity, root.get("model_identity").?.string);
            try std.testing.expectEqualStrings("reset password", root.get("input").?.string);
            const options = root.get("embedding_options").?.object;
            try std.testing.expectEqual(@as(i64, 128), options.get("dimensions").?.integer);
            try std.testing.expectEqualStrings("routing_v1", root.get("questions").?.array.items[0].object.get("embedding_options").?.object.get("calibration_id").?.string);
            try std.testing.expectEqualStrings("choice", root.get("questions").?.array.items[0].object.get("type").?.string);
        }
    };
    const response = "{\"model\":\"embeddinggemma2\",\"model_identity\":\"" ++ Check.identity ++ "\",\"answers\":[{\"name\":\"answer\",\"type\":\"choice\",\"choice\":\"account\",\"decision_method\":\"embedding_similarity\",\"similarity_metric\":\"cosine\",\"prototype_set_hash\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"similarities\":[{\"value\":\"account\",\"similarity\":0.8},{\"value\":\"billing\",\"similarity\":0.1}],\"margin\":0.7,\"calibration_id\":\"routing_v1\",\"status\":\"selected\"}],\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}";
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/decisions", .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = response } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    const cfg: decisions.DeciderConfig = .{ .provider = .antfly, .decision_method = .embedding_similarity, .model = "embeddinggemma2", .model_identity = Check.identity, .embedding_options = .{ .dimensions = 128, .calibration_id = "routing_v1" }, .url = server.baseUrl() };
    try registry.registerDeciderConfig("similarity", cfg);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const questions = try decisions.questionsFor(alloc, .ai_choice, &.{ .{ .string = "reset password" }, .{ .string = "Route request" }, .{ .string = "{\"account\":\"Login issues\",\"billing\":\"Payments\"}" }, .{ .string = "similarity" } });
    const expected_body = try decisions.wireRequest(alloc, cfg, "reset password", questions);
    var budgeted_cfg = cfg;
    budgeted_cfg.max_input_tokens = expected_body.len - 1;
    try registry.registerDeciderConfig("budgeted-similarity", budgeted_cfg);
    const Run = struct {
        fn run(runtime: *Runtime, allocator: std.mem.Allocator, q: decisions.Json, failure: *?anyerror) void {
            const results = runtime.provider().evaluateBatch(allocator, &.{.{ .input = .{ .text = "reset password" }, .decider = "similarity", .questions = q }}) catch |err| {
                failure.* = err;
                return;
            };
            const choice = decisions.selectResult(.ai_choice, results[0]) catch |err| {
                failure.* = err;
                return;
            };
            std.testing.expectEqualStrings("account", choice.string) catch |err| {
                failure.* = err;
            };
        }
    };
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    defer runtime.deinit();
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Run.run, .{ &runtime, alloc, questions, &failure });
    try server.handleOne();
    try group.await(io);
    if (failure) |err| return err;
    try std.testing.expectEqual(@as(u64, expected_body.len), runtime.estimated_input_tokens);
    var budgeted: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    defer budgeted.deinit();
    try std.testing.expectError(error.DecisionLimitExceeded, budgeted.provider().evaluateBatch(alloc, &.{.{ .input = .{ .text = "reset password" }, .decider = "budgeted-similarity", .questions = questions }}));
    try std.testing.expectEqual(@as(u64, 0), budgeted.rows);
}

test "decision functions OpenAI HTTP routing alignment provenance budgets and errors" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer openai-test", req.header("Authorization") orelse return error.TestUnexpectedResult);
            try std.testing.expect(req.header(execution.source_table_header) == null);
            const parsed = try std.json.parseFromSlice(@import("openai_api").DecisionRequest, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("refund", parsed.value.input.string);
            try std.testing.expectEqualStrings("configured-model", parsed.value.model);
            try std.testing.expectEqual(@as(usize, 1), parsed.value.questions.len);
            const question = parsed.value.questions[0].question_param_predicate;
            try std.testing.expectEqualStrings("answer", question.name.?);
            try std.testing.expectEqualStrings("Refund?", question.instructions);
        }
    };
    const usage =
        \\"usage":{"input_tokens":2,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":2}
    ;
    const success = "{\"model\":\"resolved-model\",\"answers\":[{\"type\":\"predicate\",\"name\":\"answer\",\"probability\":0.9}]," ++ usage ++ "}";
    const refusal = "{\"model\":\"resolved-model\",\"answers\":[{\"type\":\"refusal\",\"name\":\"answer\"}]," ++ usage ++ "}";
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 2, .assert_request = Check.request, .respond = .{ .body = success } },
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 1, .respond = .{ .body = refusal } },
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 1, .respond = .{ .status = 429, .body = "rate limited" } },
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 1, .respond = .{ .status = 401, .body = "unauthorized" } },
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 1, .respond = .{ .status = 502, .body = "upstream failure" } },
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 1, .respond = .{ .body = "{}" } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    const url = try std.fmt.allocPrint(a, "{s}/v1/", .{server.baseUrl()});
    defer a.free(url);
    try registry.registerDeciderConfig("openai", .{ .provider = .openai, .model = "configured-model", .url = url, .api_key = "openai-test", .max_rows = 2, .batch_size = 1 });
    try registry.registerDeciderConfig("budgeted-openai", .{ .provider = .openai, .model = "configured-model", .url = url, .api_key = "openai-test", .max_input_tokens = 1 });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const questions = try decisions.questionsFor(alloc, .ai_probability, &.{ .{ .string = "refund" }, .{ .string = "Refund?" }, .{ .string = "openai" } });
    const Run = struct {
        fn run(runtime: *Runtime, allocator: std.mem.Allocator, q: decisions.Json, count: usize, failure: *?anyerror) void {
            const requests = allocator.alloc(decisions.Request, count) catch |err| {
                failure.* = err;
                return;
            };
            @memset(requests, .{ .input = .{ .text = "refund" }, .decider = "openai", .questions = q });
            const results = runtime.provider().withSourceTable("docs").evaluateBatch(allocator, requests) catch |err| {
                failure.* = err;
                return;
            };
            for (results) |result| {
                const probability = decisions.selectResult(.ai_probability, result) catch |err| {
                    failure.* = err;
                    return;
                };
                std.testing.expectApproxEqAbs(@as(f64, 0.9), probability.float, 1e-9) catch |err| {
                    failure.* = err;
                    return;
                };
            }
        }
    };
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io, .profile_allocator = alloc };
    defer runtime.deinit();
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Run.run, .{ &runtime, alloc, questions, 2, &failure });
    try server.handleOne();
    try server.handleOne();
    try group.await(io);
    if (failure) |err| return err;
    try std.testing.expectEqual(@as(u64, 2), runtime.rows);
    try std.testing.expectEqual(@as(u64, 2), runtime.batches);
    try std.testing.expectEqual(@as(u64, 4), runtime.input_tokens);
    try std.testing.expectEqual(@as(usize, 1), runtime.provenance.items.len);
    try std.testing.expectEqualStrings("openai", runtime.provenance.items[0].provider);
    try std.testing.expectEqualStrings("resolved-model", runtime.provenance.items[0].model);
    try std.testing.expectEqual(@as(u64, 2), runtime.provenance.items[0].rows);
    try std.testing.expectError(error.DecisionLimitExceeded, runtime.provider().evaluateBatch(alloc, &.{.{ .input = .{ .text = "refund" }, .decider = "openai", .questions = questions }}));
    var budgeted: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    defer budgeted.deinit();
    try std.testing.expectError(error.DecisionLimitExceeded, budgeted.provider().evaluateBatch(alloc, &.{.{ .input = .{ .text = "refund" }, .decider = "budgeted-openai", .questions = questions }}));
    for ([_]anyerror{ error.DecisionRefused, error.DecisionRateLimited, error.DecisionUnauthorized, error.DecisionUpstreamFailure, error.InvalidDecisionOutput }) |expected| {
        var failing: Runtime = .{ .registry = &registry, .http = &client, .io = io };
        defer failing.deinit();
        failure = null;
        try group.concurrent(io, Run.run, .{ &failing, alloc, questions, 1, &failure });
        try server.handleOne();
        try group.await(io);
        try std.testing.expectEqual(@as(?anyerror, expected), failure);
        try std.testing.expectEqual(@as(u64, 0), failing.rows);
    }
    var cancelled = std.atomic.Value(bool).init(true);
    runtime.context = .{ .io = io, .deadline_ns = null, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) };
    try std.testing.expectError(error.Cancelled, runtime.provider().evaluateBatch(alloc, &.{.{ .input = .{ .text = "refund" }, .decider = "openai", .questions = questions }}));
}

test "generating backend generated decisions batch through native constrained output" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var client = httpx.Client.init(a, std.testing.io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("judge", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "generator", .url = "" }, .probabilities = .verdict_only });
    const Fake = struct {
        calls: usize = 0,
        fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8, _: ?execution.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const body = try std.json.parseFromSliceLeaky(decisions.Json, alloc, bytes, .{});
            try std.testing.expect(body.object.contains("response_format"));
            const messages = body.object.get("messages").?.array.items;
            const prompt = try std.json.parseFromSliceLeaky(decisions.Json, alloc, messages[1].object.get("content").?.string, .{});
            try std.testing.expectEqual(@as(usize, 2), prompt.object.get("items").?.array.items.len);
            return alloc.dupe(u8,
                \\{"choices":[{"message":{"content":"{\"items\":[{\"item\":0,\"value\":{\"answer\":\"same\"}},{\"item\":1,\"value\":{\"answer\":\"different\"}}]}"}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}
            );
        }
        fn caps(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @import("antfly_inference_work").Task) !@import("antfly_inference_work").InferenceCapabilities {
            return .{ .task = .generate, .input_modalities = .{ .text = true }, .input_granularity = .item, .output = .generated_text, .generation_grammar_v1 = true };
        }
    };
    var fake = Fake{};
    var runtime = Runtime{ .registry = &registry, .http = &client, .io = std.testing.io, .antfly_provider = .{
        .ptr = &fake,
        .embed_dense_texts = undefined,
        .embed_sparse_texts = undefined,
        .generate_json = Fake.generate,
        .model_capabilities = Fake.caps,
    } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\{"answer":{"type":"choice","instructions":"Compare concepts","criteria":{"same":"Same entity","different":"Distinct entities"}}}
    , .{});
    const result = try runtime.provider().evaluateBatch(scratch, &.{ .{ .decider = "judge", .questions = questions, .input = .{ .text = "A B" } }, .{ .decider = "judge", .questions = questions, .input = .{ .text = "C D" } } });
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqualStrings("same", result[0].object.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqualStrings("verdict", result[1].object.get("answers").?.array.items[0].object.get("probability_source").?.string);
    try std.testing.expectEqual(@as(u64, 10), runtime.input_tokens);
}

test "decision functions multimodal HTTP preserves OpenAI parts and Jev multipart images" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer images-test", req.header("Authorization").?);
            try std.testing.expect(req.header(execution.source_table_header) == null);
            if (std.mem.eql(u8, req.path, "/v1/decisions")) {
                try std.testing.expectEqualStrings("application/json", req.header("Content-Type").?);
                try std.testing.expect(req.header("TypeSafe-Danger-Zone-Images") == null);
                const parsed = try std.json.parseFromSlice(decisions.Json, a, req.body, .{});
                defer parsed.deinit();
                const input = parsed.value.object.get("input").?.array.items;
                try std.testing.expectEqual(@as(usize, 2), input.len);
                try std.testing.expectEqualStrings("Describe damage", input[0].object.get("content").?.string);
                const parts = input[1].object.get("content").?.array.items;
                try std.testing.expectEqualStrings("The item", parts[0].object.get("text").?.string);
                try std.testing.expectEqualStrings("data:image/png;base64,iVBORw0KGgo=", parts[1].object.get("image_url").?.string);
                try std.testing.expectEqualStrings("auto", parts[1].object.get("detail").?.string);
            } else {
                try std.testing.expectEqualStrings("true", req.header("TypeSafe-Danger-Zone-Images").?);
                const content_type = req.header("Content-Type").?;
                try std.testing.expect(std.mem.startsWith(u8, content_type, "multipart/form-data; boundary="));
                const start = (std.mem.indexOf(u8, req.body, "\r\n\r\n") orelse return error.TestUnexpectedResult) + 4;
                const end = std.mem.indexOfPos(u8, req.body, start, "\r\n--") orelse return error.TestUnexpectedResult;
                const parsed = try std.json.parseFromSlice(decisions.Json, a, req.body[start..end], .{});
                defer parsed.deinit();
                const state = parsed.value.object.get("state").?.array.items;
                try std.testing.expectEqualStrings("Describe damage", state[0].object.get("content").?.string);
                const parts = state[1].object.get("content").?.array.items;
                try std.testing.expectEqualStrings("The item", parts[0].string);
                const asset = parsed.value.object.get("assets").?.object.get(parts[1].string).?;
                try std.testing.expectEqualStrings("image", asset.object.get("type").?.string);
                try std.testing.expectEqualStrings("asset-0", asset.object.get("part").?.string);
                try std.testing.expect(std.mem.indexOf(u8, req.body, "name=\"asset-0\"; filename=\"asset-0\"\r\nContent-Type: image/png\r\n\r\n\x89PNG\r\n\x1a\n\r\n") != null);
                try std.testing.expect(std.mem.indexOf(u8, req.body, "iVBORw0KGgo=") == null);
            }
        }
        fn text(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("application/json", req.header("Content-Type").?);
            try std.testing.expect(req.header("TypeSafe-Danger-Zone-Images") == null);
            const parsed = try std.json.parseFromSlice(decisions.Json, a, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("Text only", parsed.value.object.get("state").?.array.items[0].object.get("content").?.string);
        }
    };
    const openai_response =
        \\{"model":"vision-model","answers":[{"type":"predicate","name":"answer","probability":0.9}],"usage":{"input_tokens":42,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":42}}
    ;
    const jev_response =
        \\{"model":"vision-model","answers":{"answer":{"type":"noul","noul":0.9}},"usage":{"input_tokens":42,"output_tokens":0}}
    ;
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = openai_response } },
        .{ .method = .POST, .path = "/v1/systemone", .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = jev_response } },
        .{ .method = .POST, .path = "/v1/systemone", .max_uses = 1, .assert_request = Check.text, .respond = .{ .body = jev_response } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    try registry.registerDeciderConfig("openai", .{ .provider = .openai, .model = "vision-model", .max_image_input_tokens = 8192, .url = try std.fmt.allocPrint(alloc, "{s}/v1", .{server.baseUrl()}), .api_key = "images-test" });
    try registry.registerDeciderConfig("jev", .{ .provider = .jev, .model = "jev-preview", .max_image_input_tokens = 8192, .url = server.baseUrl(), .api_key = "images-test" });
    try registry.registerDeciderConfig("native", .{ .model = "text-model", .url = server.baseUrl() });
    try registry.registerDeciderConfig("quota-small", .{ .provider = .jev, .model = "jev-preview", .url = server.baseUrl(), .api_key = "images-test", .max_image_input_tokens = 8192, .rate_limit = .{ .tokens_per_minute = 1000 } });
    try registry.registerDeciderConfig("missing-image-bound", .{ .provider = .jev, .model = "jev-preview", .url = server.baseUrl(), .api_key = "images-test" });
    try registry.registerDeciderConfig("budgeted", .{ .provider = .jev, .model = "jev-preview", .max_image_input_tokens = 8192, .url = server.baseUrl(), .api_key = "images-test", .max_input_tokens = 1 });
    const input = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, alloc,
        \\[{"role":"user","content":"Describe damage"},{"role":"user","content":[{"type":"input_text","text":"The item"},{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo=","detail":"auto"}]}]
    , .{}));
    const questions = try decisions.questionsFor(alloc, .ai_probability, &.{ .{ .string = "item" }, .{ .string = "Damaged?" }, .{ .string = "openai" } });
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    defer runtime.deinit();
    const Run = struct {
        fn run(r: *Runtime, allocator: std.mem.Allocator, name: []const u8, q: decisions.Json, evidence: decisions.Input, failure: *?anyerror) void {
            const result = r.provider().withSourceTable("docs").evaluateBatch(allocator, &.{.{ .decider = name, .questions = q, .input = evidence }}) catch |err| {
                failure.* = err;
                return;
            };
            const selected = decisions.selectResult(.ai_probability, result[0]) catch |err| {
                failure.* = err;
                return;
            };
            std.testing.expectApproxEqAbs(@as(f64, 0.9), selected.float, 1e-9) catch |err| {
                failure.* = err;
            };
        }
    };
    var group = std.Io.Group.init;
    defer group.cancel(io);
    for ([_][]const u8{ "openai", "jev" }) |name| {
        var failure: ?anyerror = null;
        try group.concurrent(io, Run.run, .{ &runtime, alloc, name, questions, input, &failure });
        try server.handleOne();
        try group.await(io);
        if (failure) |err| return err;
    }
    try std.testing.expectEqual(@as(u64, 84), runtime.input_tokens);
    try std.testing.expectError(error.DecisionLimitExceeded, runtime.provider().evaluateBatch(alloc, &.{.{ .decider = "quota-small", .questions = questions, .input = input }}));
    try std.testing.expectError(error.UnsupportedInferenceFeature, runtime.provider().evaluateBatch(alloc, &.{.{ .decider = "missing-image-bound", .questions = questions, .input = input }}));
    try registry.inference_capabilities.put(a, try a.dupe(u8, "deciders/openai"), .{ .images = .unsupported });
    try std.testing.expectError(error.UnsupportedInferenceFeature, runtime.provider().evaluateBatch(alloc, &.{.{ .decider = "openai", .questions = questions, .input = input }}));
    try std.testing.expectError(error.UnsupportedInferenceFeature, runtime.provider().evaluateBatch(alloc, &.{.{ .decider = "deciders/openai", .questions = questions, .input = input }}));
    try std.testing.expectError(error.UnsupportedInferenceFeature, runtime.provider().evaluateBatch(alloc, &.{.{ .decider = "native", .questions = questions, .input = input }}));
    try std.testing.expectError(error.DecisionLimitExceeded, runtime.provider().evaluateBatch(alloc, &.{.{ .decider = "budgeted", .questions = questions, .input = input }}));
    const text_input = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, alloc, "[{\"role\":\"user\",\"content\":\"Text only\"}]", .{}));
    var text_failure: ?anyerror = null;
    try group.concurrent(io, Run.run, .{ &runtime, alloc, "missing-image-bound", questions, text_input, &text_failure });
    try server.handleOne();
    try group.await(io);
    if (text_failure) |err| return err;
    try std.testing.expectEqualSlices(usize, &.{ 1, 1, 1 }, server.route_hits);
}

test "decision functions image token estimates account for opaque URLs and require a bound" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, a,
        \\[{"role":"user","content":[{"type":"input_image","image_url":"https://example.org/a.png"},{"type":"input_image","image_url":"https://example.org/b.png"}]}]
    , .{}));
    const cfg: decisions.DeciderConfig = .{ .provider = .openai, .model = "vision", .max_image_input_tokens = 8192 };
    const body: [100]u8 = @splat('x');
    try std.testing.expectEqual(@as(u64, 16484), try estimateInputTokens(cfg, input, .{ .body = &body }));
    try std.testing.expectEqual(@as(u64, 16404), try estimateInputTokens(cfg, input, .{ .body = &body, .image_bytes = 80 }));
    try std.testing.expectError(error.InvalidDeciderConfig, estimateInputTokens(.{ .provider = .openai, .model = "unknown" }, input, .{ .body = &body }));
    var overflow = cfg;
    overflow.max_image_input_tokens = std.math.maxInt(u64);
    try std.testing.expectError(error.DecisionLimitExceeded, estimateInputTokens(overflow, input, .{ .body = &body }));
    try std.testing.expectEqual(@as(u64, 100), try estimateInputTokens(cfg, .{ .text = "unchanged" }, .{ .body = &body }));
}

test "decision functions partial batches preserve siblings order and failed usage across bounded windows" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var client = httpx.Client.init(a, io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("batch", .{ .model = "native", .batch_size = 2 });
    const Fake = struct {
        fn decide(_: *anyopaque, alloc: std.mem.Allocator, body: []const u8, _: ?execution.RequestContext) ![]u8 {
            const parsed = try std.json.parseFromSlice(decisions.Json, alloc, body, .{});
            defer parsed.deinit();
            const bad = std.mem.eql(u8, parsed.value.object.get("input").?.string, "invalid");
            return std.json.Stringify.valueAlloc(alloc, .{
                .model = "native",
                .answers = .{.{ .name = "answer", .type = "predicate", .decision_method = "typed", .probability = if (bad) @as(f64, 2) else @as(f64, 0.9) }},
                .usage = .{ .input_tokens = if (bad) @as(u64, 7) else @as(u64, 3), .output_tokens = @as(u64, 1) },
            }, .{});
        }
    };
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io, .antfly_provider = .{ .ptr = undefined, .embed_dense_texts = undefined, .embed_sparse_texts = undefined, .decide_json = Fake.decide } };
    defer runtime.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const parsed = try @import("decision_batch.zig").parse(alloc, try std.json.parseFromSliceLeaky(decisions.Json, alloc,
        \\{"inputs":[{"id":"a","input":"first"},{"id":"b","input":"invalid"},{"id":"c","input":"last"}]}
    , .{}));
    const questions = try decisions.questionsFor(alloc, .ai_probability, &.{ .{ .string = "x" }, .{ .string = "Safe?" }, .{ .string = "batch" } });
    var requests: [3]decisions.Request = undefined;
    for (&requests, parsed.items) |*request, item| request.* = .{ .input = item.input, .questions = questions, .decider = "batch" };
    const result = try runtime.evaluatePartial(alloc, &requests);
    try std.testing.expect(result.values[0] != null and result.values[1] == null and result.values[2] != null);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidDecisionOutput), result.failures[1]);
    try std.testing.expectEqual(@as(u64, 13), runtime.input_tokens);
    try std.testing.expectEqual(@as(u64, 3), runtime.output_tokens);
    try std.testing.expectEqual(@as(u64, 2), runtime.batches);
    try std.testing.expectEqual(@as(u64, 2), runtime.rows);
    try std.testing.expectEqual(@as(u64, 3), runtime.reserved_rows);
    // The same preflight keeps native providers text-only even in a mixed batch.
    requests[2].input = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, alloc,
        \\[{"role":"user","content":[{"type":"input_image","image_url":"https://example.org/a.png"}]}]
    , .{}));
    try std.testing.expectError(error.UnsupportedInferenceFeature, runtime.evaluatePartial(alloc, &requests));
    try std.testing.expectEqual(@as(u64, 2), runtime.rows);
}

test "decision functions hosted partial batches retain failed provider usage and preflight quotas" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const valid = try std.json.Stringify.valueAlloc(alloc, .{
        .model = "resolved-vision",
        .answers = .{.{ .name = "answer", .type = "predicate", .probability = @as(f64, 0.9) }},
        .usage = .{ .input_tokens = @as(u64, 3), .output_tokens = @as(u64, 1), .total_tokens = @as(u64, 4), .input_tokens_details = .{ .cached_tokens = 0, .cache_write_tokens = 0 }, .output_tokens_details = .{ .reasoning_tokens = 0 } },
    }, .{});
    const Check = struct {
        fn image(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer batch-key", req.header("authorization").?);
            const parsed = try std.json.parseFromSlice(decisions.Json, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 1), (try decisions.Input.fromJson(parsed.value.object.get("input").?)).imageCount());
        }
    };
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/v1/decisions", .max_uses = 1, .assert_request = Check.image, .respond = .{ .status = 503, .body = "{\"usage\":{\"input_tokens\":7,\"output_tokens\":1}}" } },
        .{ .method = .POST, .path = "/v1/decisions", .assert_request = Check.image, .respond = .{ .body = valid } },
    });
    defer server.deinit();
    var client = httpx.Client.init(a, io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    const cfg = decisions.DeciderConfig{ .provider = .openai, .model = "vision", .url = try std.fmt.allocPrint(alloc, "{s}/v1", .{server.baseUrl()}), .api_key = "batch-key", .max_image_input_tokens = 8192, .batch_size = 1 };
    try registry.registerDeciderConfig("hosted", cfg);
    var tiny = cfg;
    tiny.rate_limit = .{ .tokens_per_minute = 1000 };
    try registry.registerDeciderConfig("tiny", tiny);
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    defer runtime.deinit();
    const input = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, alloc,
        \\[{"role":"user","content":[{"type":"input_image","image_url":"https://example.org/a.png"}]}]
    , .{}));
    const q = try decisions.questionsFor(alloc, .ai_probability, &.{ .{ .string = "x" }, .{ .string = "Safe?" }, .{ .string = "hosted" } });
    var requests: [3]decisions.Request = @splat(.{ .input = input, .questions = q, .decider = "hosted" });
    const Run = struct {
        fn run(r: *Runtime, allocator: std.mem.Allocator, reqs: []const decisions.Request, failure: *?anyerror) void {
            execute(r, allocator, reqs) catch |err| {
                failure.* = err;
            };
        }
        fn execute(r: *Runtime, allocator: std.mem.Allocator, reqs: []const decisions.Request) !void {
            const result = try r.evaluatePartial(allocator, reqs);
            try std.testing.expectEqual(@as(?anyerror, error.DecisionUpstreamFailure), result.failures[0]);
            try std.testing.expect(result.values[0] == null and result.values[1] != null and result.values[2] != null);
            try std.testing.expectEqualStrings("resolved-vision", result.values[1].?.object.get("model").?.string);
        }
    };
    var group = std.Io.Group.init;
    var failure: ?anyerror = null;
    try group.concurrent(io, Run.run, .{ &runtime, alloc, &requests, &failure });
    defer group.cancel(io);
    for (0..3) |_| try server.handleOne();
    try group.await(io);
    if (failure) |err| return err;
    try std.testing.expectEqual(@as(u64, 13), runtime.input_tokens);
    try std.testing.expectEqual(@as(u64, 3), runtime.output_tokens);
    // An oversized per-item TPM reservation rejects the whole request before I/O.
    requests[2].decider = "tiny";
    try std.testing.expectError(error.DecisionLimitExceeded, runtime.evaluatePartial(alloc, &requests));
    try std.testing.expectEqual(@as(u64, 2), runtime.rows);
    var cancelled = std.atomic.Value(bool).init(true);
    runtime.context = .{ .io = io, .deadline_ns = null, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) };
    try std.testing.expectError(error.Cancelled, runtime.evaluatePartial(alloc, &requests));
}

test "decision functions generated multimodal decisions preserve evidence and item IDs through native repair" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var client = httpx.Client.init(a, std.testing.io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("vision", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "vision", .url = "" }, .probabilities = .verdict_only, .max_image_input_tokens = 8192 });
    const Fake = struct {
        calls: usize = 0,
        images: bool = true,
        fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8, _: ?execution.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const body = try std.json.parseFromSliceLeaky(decisions.Json, alloc, bytes, .{});
            try std.testing.expect(body.object.contains("response_format"));
            const messages = body.object.get("messages").?.array.items;
            const prompt = try std.json.parseFromSliceLeaky(decisions.Json, alloc, messages[1].object.get("content").?.string, .{});
            const items = prompt.object.get("items").?.array.items;
            try std.testing.expectEqual(@as(usize, if (self.calls == 1) 2 else 1), items.len);
            try std.testing.expectEqual(@as(i64, 1), items[items.len - 1].object.get("id").?.integer);
            try std.testing.expect(std.mem.indexOf(u8, messages[1].object.get("content").?.string, "iVBORw0KGgo=") == null);
            try std.testing.expectEqualStrings("Evidence for item ID 1:", messages[2].object.get("content").?.string);
            try std.testing.expectEqualStrings("Shared context", messages[3].object.get("content").?.string);
            const parts = messages[4].object.get("content").?.array.items;
            try std.testing.expectEqualStrings("Inspect", parts[0].object.get("text").?.string);
            try std.testing.expectEqualStrings("image/png", parts[1].object.get("mime_type").?.string);
            try std.testing.expectEqualStrings("iVBORw0KGgo=", parts[1].object.get("data").?.string);
            if (self.calls == 1) return alloc.dupe(u8,
                \\{"choices":[{"message":{"content":"{\"items\":[{\"item\":0,\"value\":{\"answer\":\"same\"}}]}"}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}
            );
            return alloc.dupe(u8,
                \\{"choices":[{"message":{"content":"{\"items\":[{\"item\":1,\"value\":{\"answer\":\"different\"}}]}"}}],"usage":{"prompt_tokens":7,"completion_tokens":3}}
            );
        }
        fn caps(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @import("antfly_inference_work").Task) !@import("antfly_inference_work").InferenceCapabilities {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .task = .generate, .input_modalities = .{ .text = true, .image = self.images }, .input_granularity = .item, .output = .generated_text, .generation_grammar_v1 = true, .batch = .{ .max_media_parts_per_item = if (self.images) 8 else 0 } };
        }
    };
    var fake = Fake{};
    var runtime = Runtime{ .registry = &registry, .http = &client, .io = std.testing.io, .antfly_provider = .{ .ptr = &fake, .embed_dense_texts = undefined, .embed_sparse_texts = undefined, .generate_json = Fake.generate, .model_capabilities = Fake.caps } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\{"answer":{"type":"choice","instructions":"Compare concepts","criteria":{"same":"Same entity","different":"Distinct entities"}}}
    , .{});
    const evidence = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\[{"role":"user","content":"Shared context"},{"role":"user","content":[{"type":"input_text","text":"Inspect"},{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}]}]
    , .{}));
    const requests = [_]decisions.Request{ .{ .decider = "vision", .questions = questions, .input = .{ .text = "Text" } }, .{ .decider = "vision", .questions = questions, .input = evidence } };
    const result = try runtime.evaluatePartial(scratch, &requests);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("same", result.values[0].?.object.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqualStrings("different", result.values[1].?.object.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqual(@as(u64, 17), runtime.input_tokens);
    try std.testing.expectEqual(@as(u64, 8), runtime.output_tokens);
    fake.images = false;
    var text_only = Runtime{ .registry = &registry, .http = &client, .io = std.testing.io, .antfly_provider = runtime.antfly_provider };
    defer text_only.deinit();
    try std.testing.expectError(error.UnsupportedInferenceFeature, text_only.evaluatePartial(scratch, &requests));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    var cfg = try registry.getDeciderConfig("vision");
    const remote_image = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\[{"role":"user","content":[{"type":"input_image","image_url":"https://example.com/image.png"}]}]
    , .{}));
    try std.testing.expectError(error.UnsupportedInferenceFeature, requestPayload(scratch, std.testing.io, cfg, .{ .decider = "vision", .questions = questions, .input = remote_image }));
    cfg.max_image_input_tokens = null;
    try registry.registerDeciderConfig("unbudgeted", cfg);
    try std.testing.expectError(error.UnsupportedInferenceFeature, runtime.evaluatePartial(scratch, &.{.{ .decider = "unbudgeted", .questions = questions, .input = evidence }}));
}

test "decision functions generated multimodal decisions reach hosted structured adapters with image quota reservations" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            const parsed = try std.json.parseFromSlice(decisions.Json, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            const root = parsed.value.object;
            if (root.get("contents")) |contents| {
                try std.testing.expect(root.contains("tools"));
                var found = false;
                for (contents.array.items) |content| for (content.object.get("parts").?.array.items) |part| {
                    if (part.object.get("inlineData")) |image| {
                        try std.testing.expectEqualStrings("image/png", image.object.get("mimeType").?.string);
                        try std.testing.expectEqualStrings("iVBORw0KGgo=", image.object.get("data").?.string);
                        found = true;
                    }
                };
                try std.testing.expect(found);
            } else {
                try std.testing.expect(root.contains("response_format"));
                const messages = root.get("messages").?.array.items;
                const image = messages[3].object.get("content").?.array.items[1].object.get("image_url").?;
                try std.testing.expectEqualStrings("data:image/png;base64,iVBORw0KGgo=", image.object.get("url").?.string);
                try std.testing.expectEqualStrings("high", image.object.get("detail").?.string);
            }
        }
        fn serve(server: *httpx.TestServer, failure: *?anyerror) void {
            server.handleOne() catch |err| {
                failure.* = err;
            };
        }
    };
    for ([_]@import("../generating/mod.zig").Provider{ .openai, .gemini }) |provider| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const google = provider == .gemini;
        const path = if (google) "/models/gemini-2.5-flash:generateContent" else "/chat/completions";
        const reply = if (google)
            "{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"antfly_output\",\"args\":{\"items\":[{\"item\":0,\"value\":{\"answer\":\"same\"}}]}}}]}}],\"usageMetadata\":{\"promptTokenCount\":12,\"candidatesTokenCount\":3}}"
        else
            "{\"choices\":[{\"message\":{\"content\":\"{\\\"items\\\":[{\\\"item\\\":0,\\\"value\\\":{\\\"answer\\\":\\\"same\\\"}}]}\"}}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}";
        var server = try httpx.TestServer.start(a, io, &.{.{ .method = .POST, .path = path, .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = reply } }});
        defer server.deinit();
        const url = server.baseUrl();
        var client = httpx.Client.init(a, io);
        defer client.deinit();
        var limits = quotas.Registry.init(a);
        defer limits.deinit();
        var registry = registry_mod.Registry.init(a);
        defer registry.deinit();
        try registry.registerDeciderConfig("vision", .{ .decision_method = .generated, .generator = .{ .provider = provider, .model = if (google) "gemini-2.5-flash" else "gpt-4.1", .url = url, .api_key = "fixture", .rate_limit = .{ .tokens_per_minute = 100000 } }, .output = if (google) .tool_call else .json_schema, .probabilities = .verdict_only, .max_image_input_tokens = 8192 });
        var runtime = Runtime{ .registry = &registry, .http = &client, .io = io, .limits = &limits };
        defer runtime.deinit();
        const evidence = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, scratch, if (google)
            "[{\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Inspect\"},{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,iVBORw0KGgo=\"}]}]"
        else
            "[{\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Inspect\"},{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,iVBORw0KGgo=\",\"detail\":\"high\"}]}]", .{}));
        const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
            \\{"answer":{"type":"choice","instructions":"Compare concepts","criteria":{"same":"Same entity","different":"Distinct entities"}}}
        , .{});
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Check.serve, .{ &server, &failure });
        const result = try runtime.evaluatePartial(scratch, &.{.{ .decider = "vision", .questions = questions, .input = evidence }});
        try group.await(io);
        if (failure) |err| return err;
        try std.testing.expect(result.failures[0] == null);
        try std.testing.expectEqualStrings("same", result.values[0].?.object.get("answers").?.array.items[0].object.get("choice").?.string);
        try std.testing.expectEqual(@as(u64, 12), runtime.input_tokens);
        try std.testing.expectEqual(@as(u64, 3), runtime.output_tokens);
    }
}

test "decision functions failed generated calls retain hosted reported usage" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            const parsed = try std.json.parseFromSlice(decisions.Json, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            const root = parsed.value.object;
            if (root.get("contents")) |contents| {
                try std.testing.expect(root.contains("tools"));
                var found = false;
                for (contents.array.items) |content| for (content.object.get("parts").?.array.items) |part| {
                    if (part.object.get("inlineData")) |image| {
                        try std.testing.expectEqualStrings("image/png", image.object.get("mimeType").?.string);
                        try std.testing.expectEqualStrings("iVBORw0KGgo=", image.object.get("data").?.string);
                        found = true;
                    }
                };
                try std.testing.expect(found);
            } else {
                try std.testing.expect(root.contains("response_format"));
                const messages = root.get("messages").?.array.items;
                const image = messages[3].object.get("content").?.array.items[1].object.get("image_url").?;
                try std.testing.expectEqualStrings("data:image/png;base64,iVBORw0KGgo=", image.object.get("url").?.string);
                try std.testing.expectEqualStrings("high", image.object.get("detail").?.string);
            }
        }
        fn serve(server: *httpx.TestServer, failure: *?anyerror) void {
            server.handleOne() catch |err| {
                failure.* = err;
            };
        }
    };
    for ([_]@import("../generating/mod.zig").Provider{ .openai, .gemini }) |provider| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const google = provider == .gemini;
        const path = if (google) "/models/gemini-2.5-flash:generateContent" else "/chat/completions";
        const reply = if (google)
            "{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"antfly_output\",\"args\":{\"items\":[{\"item\":0,\"value\":{\"answer\":\"same\"}}]}}}]}}],\"usageMetadata\":{\"promptTokenCount\":12,\"candidatesTokenCount\":3}}"
        else
            "{\"choices\":[{\"message\":{\"content\":\"{\\\"items\\\":[{\\\"item\\\":0,\\\"value\\\":{\\\"answer\\\":\\\"same\\\"}}]}\"}}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}";
        var server = try httpx.TestServer.start(a, io, &.{.{ .method = .POST, .path = path, .max_uses = 1, .assert_request = Check.request, .respond = .{ .status = 503, .body = reply } }});
        defer server.deinit();
        const url = server.baseUrl();
        var client = httpx.Client.initWithConfig(a, io, .{ .retry_policy = .{ .max_retries = 0 } });
        defer client.deinit();
        var limits = quotas.Registry.init(a);
        defer limits.deinit();
        var registry = registry_mod.Registry.init(a);
        defer registry.deinit();
        try registry.registerDeciderConfig("vision", .{ .decision_method = .generated, .generator = .{ .provider = provider, .model = if (google) "gemini-2.5-flash" else "gpt-4.1", .url = url, .api_key = "fixture", .rate_limit = .{ .tokens_per_minute = 100000 } }, .output = if (google) .tool_call else .json_schema, .probabilities = .verdict_only, .max_image_input_tokens = 8192 });
        var runtime = Runtime{ .registry = &registry, .http = &client, .io = io, .limits = &limits };
        defer runtime.deinit();
        const evidence = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, scratch, if (google)
            "[{\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Inspect\"},{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,iVBORw0KGgo=\"}]}]"
        else
            "[{\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Inspect\"},{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,iVBORw0KGgo=\",\"detail\":\"high\"}]}]", .{}));
        const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
            \\{"answer":{"type":"choice","instructions":"Compare concepts","criteria":{"same":"Same entity","different":"Distinct entities"}}}
        , .{});
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Check.serve, .{ &server, &failure });
        const result = try runtime.evaluatePartial(scratch, &.{.{ .decider = "vision", .questions = questions, .input = evidence }});
        try group.await(io);
        if (failure) |err| return err;
        try std.testing.expect(result.failures[0] != null);
        try std.testing.expect(result.values[0] == null);
        try std.testing.expectEqual(@as(u64, 12), runtime.input_tokens);
        try std.testing.expectEqual(@as(u64, 3), runtime.output_tokens);
    }
}

test "decision functions generated capabilities are captured once per request and survive discovery outages" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var client = httpx.Client.init(a, std.testing.io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("judge", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "generator", .url = "" }, .probabilities = .verdict_only, .max_image_input_tokens = 8192 });
    const Fake = struct {
        calls: usize = 0,
        discoveries: usize = 0,
        fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8, _: ?execution.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const body = try std.json.parseFromSliceLeaky(decisions.Json, alloc, bytes, .{});
            try std.testing.expect(body.object.contains("response_format"));
            const messages = body.object.get("messages").?.array.items;
            const prompt = try std.json.parseFromSliceLeaky(decisions.Json, alloc, messages[1].object.get("content").?.string, .{});
            try std.testing.expectEqual(@as(usize, 2), prompt.object.get("items").?.array.items.len);
            return alloc.dupe(u8,
                \\{"choices":[{"message":{"content":"{\"items\":[{\"item\":0,\"value\":{\"answer\":\"same\"}},{\"item\":1,\"value\":{\"answer\":\"different\"}}]}"}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}
            );
        }
        fn caps(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @import("antfly_inference_work").Task) !@import("antfly_inference_work").InferenceCapabilities {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.discoveries += 1;
            if (self.calls > 0) return error.CapabilityDiscoveryUnavailable;
            return .{ .model_revision = @splat(1), .task = .generate, .input_modalities = .{ .text = true }, .input_granularity = .item, .output = .generated_text, .generation_grammar_v1 = true };
        }
    };
    var fake = Fake{};
    var runtime = Runtime{ .registry = &registry, .resolution_allocator = a, .http = &client, .io = std.testing.io, .antfly_provider = .{
        .ptr = &fake,
        .embed_dense_texts = undefined,
        .embed_sparse_texts = undefined,
        .generate_json = Fake.generate,
        .model_capabilities = Fake.caps,
    } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\{"answer":{"type":"choice","instructions":"Compare concepts","criteria":{"same":"Same entity","different":"Distinct entities"}}}
    , .{});
    const result = try runtime.provider().evaluateBatch(scratch, &.{ .{ .decider = "judge", .questions = questions, .input = .{ .text = "A B" } }, .{ .decider = "judge", .questions = questions, .input = .{ .text = "C D" } } });
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqualStrings("same", result[0].object.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqualStrings("verdict", result[1].object.get("answers").?.array.items[0].object.get("probability_source").?.string);
    try std.testing.expectEqual(@as(u64, 10), runtime.input_tokens);
    try std.testing.expectEqual(@as(usize, 1), fake.discoveries);
    runtime.deinit();
    const revision: [32]u8 = @splat(1);
    const expected_revision = try std.fmt.allocPrint(scratch, "{x}", .{&revision});
    try std.testing.expectEqualStrings(expected_revision, result[0].object.get("model_revision").?.string);
    var next_request = Runtime{ .registry = &registry, .resolution_allocator = a, .http = &client, .io = std.testing.io, .antfly_provider = runtime.antfly_provider };
    defer next_request.deinit();
    try std.testing.expectError(error.CapabilityDiscoveryUnavailable, next_request.provider().evaluateBatch(scratch, &.{.{ .decider = "judge", .questions = questions, .input = .{ .text = "A B" } }}));
    try std.testing.expectEqual(@as(usize, 2), fake.discoveries);
}

test "decision functions native generated batches pack media limits and preserve oversized siblings" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var client = httpx.Client.init(a, std.testing.io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("vision", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "vision", .url = "" }, .probabilities = .verdict_only, .max_image_input_tokens = 100 });
    const Fake = struct {
        calls: usize = 0,
        fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8, _: ?execution.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const body = try std.json.parseFromSliceLeaky(decisions.Json, alloc, bytes, .{});
            var images: usize = 0;
            for (body.object.get("messages").?.array.items) |message| {
                const content = message.object.get("content").?;
                if (content == .array) for (content.array.items) |part| {
                    if (std.mem.eql(u8, part.object.get("type").?.string, "media")) images += 1;
                };
            }
            if (images > 8) return error.GenerateMediaPartLimitExceeded;
            return alloc.dupe(u8,
                \\{"choices":[{"message":{"content":"{\"items\":[{\"item\":0,\"value\":{\"answer\":\"same\"}},{\"item\":1,\"value\":{\"answer\":\"same\"}}]}"}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}
            );
        }
        fn caps(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @import("antfly_inference_work").Task) !@import("antfly_inference_work").InferenceCapabilities {
            return .{ .task = .generate, .input_modalities = .{ .text = true, .image = true }, .input_granularity = .item, .output = .generated_text, .generation_grammar_v1 = true, .batch = .{ .max_media_parts_per_item = 8 } };
        }
    };
    var fake = Fake{};
    var runtime = Runtime{ .registry = &registry, .resolution_allocator = a, .http = &client, .io = std.testing.io, .antfly_provider = .{ .ptr = &fake, .embed_dense_texts = undefined, .embed_sparse_texts = undefined, .generate_json = Fake.generate, .model_capabilities = Fake.caps } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\{"answer":{"type":"choice","instructions":"Compare concepts","criteria":{"same":"Same entity","different":"Distinct entities"}}}
    , .{});
    const raw = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}]}]
    , .{});
    const parts = raw.array.items[0].object.getPtr("content").?;
    const image = parts.array.items[0];
    for (0..4) |_| try parts.array.append(image);
    const input = try decisions.Input.fromJson(raw);
    const result = try runtime.evaluatePartial(scratch, &.{ .{ .decider = "vision", .questions = questions, .input = input }, .{ .decider = "vision", .questions = questions, .input = input } });
    try std.testing.expect(result.values[0] != null and result.values[1] != null);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqual(@as(u64, 20), runtime.input_tokens);
    for (0..4) |_| try parts.array.append(image);
    const large = try decisions.Input.fromJson(raw);
    const partial = try runtime.evaluatePartial(scratch, &.{ .{ .decider = "vision", .questions = questions, .input = large }, .{ .decider = "vision", .questions = questions, .input = .{ .text = "Valid sibling" } } });
    try std.testing.expectEqual(error.ConstrainedInputTooLarge, partial.failures[0].?);
    try std.testing.expect(partial.values[1] != null);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
}

test "decision functions native generated batches pack decoded pixel limits and preserve oversized siblings" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var client = httpx.Client.init(a, std.testing.io);
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("vision", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "vision", .url = "" }, .probabilities = .verdict_only, .max_image_input_tokens = 100 });
    const Fake = struct {
        calls: usize = 0,
        fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8, _: ?execution.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const body = try std.json.parseFromSliceLeaky(decisions.Json, alloc, bytes, .{});
            var images: usize = 0;
            for (body.object.get("messages").?.array.items) |message| {
                const content = message.object.get("content").?;
                if (content == .array) for (content.array.items) |part| {
                    if (std.mem.eql(u8, part.object.get("type").?.string, "media")) images += 1;
                };
            }
            var pixels: u64 = 0;
            for (body.object.get("messages").?.array.items) |message| {
                const content = message.object.get("content").?;
                if (content == .array) for (content.array.items) |part| {
                    if (!std.mem.eql(u8, part.object.get("type").?.string, "media")) continue;
                    const encoded = part.object.get("data").?.string;
                    const image_bytes = try alloc.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
                    try std.base64.standard.Decoder.decode(image_bytes, encoded);
                    pixels += try @import("antfly_inference_work").encodedImagePixels(part.object.get("mime_type").?.string, image_bytes);
                };
            }
            const capabilities = try caps(ptr, alloc, "vision", .generate);
            try capabilities.validateInvocation(.generate, .{ .item_count = 1, .modalities = .{ .text = true, .image = images > 0 }, .decoded_pixels = pixels, .max_media_parts_per_item = images });
            return alloc.dupe(u8,
                \\{"choices":[{"message":{"content":"{\"items\":[{\"item\":0,\"value\":{\"answer\":\"same\"}},{\"item\":1,\"value\":{\"answer\":\"same\"}}]}"}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}
            );
        }
        fn caps(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @import("antfly_inference_work").Task) !@import("antfly_inference_work").InferenceCapabilities {
            return .{ .task = .generate, .input_modalities = .{ .text = true, .image = true }, .input_granularity = .item, .output = .generated_text, .generation_grammar_v1 = true, .batch = .{ .max_media_parts_per_item = 8, .max_decoded_pixels = 1 } };
        }
    };
    var fake = Fake{};
    var runtime = Runtime{ .registry = &registry, .resolution_allocator = a, .http = &client, .io = std.testing.io, .antfly_provider = .{ .ptr = &fake, .embed_dense_texts = undefined, .embed_sparse_texts = undefined, .generate_json = Fake.generate, .model_capabilities = Fake.caps } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\{"answer":{"type":"choice","instructions":"Compare concepts","criteria":{"same":"Same entity","different":"Distinct entities"}}}
    , .{});
    const raw = try std.json.parseFromSliceLeaky(decisions.Json, scratch,
        \\[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAEAAAAB"}]}]
    , .{});
    const parts = raw.array.items[0].object.getPtr("content").?;
    const image = parts.array.items[0];

    const input = try decisions.Input.fromJson(raw);
    const result = try runtime.evaluatePartial(scratch, &.{ .{ .decider = "vision", .questions = questions, .input = input }, .{ .decider = "vision", .questions = questions, .input = input } });
    try std.testing.expect(result.values[0] != null and result.values[1] != null);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqual(@as(u64, 20), runtime.input_tokens);
    try parts.array.append(image);
    const large = try decisions.Input.fromJson(raw);
    const partial = try runtime.evaluatePartial(scratch, &.{ .{ .decider = "vision", .questions = questions, .input = large }, .{ .decider = "vision", .questions = questions, .input = .{ .text = "Valid sibling" } } });
    try std.testing.expectEqual(error.ConstrainedInputTooLarge, partial.failures[0].?);
    try std.testing.expect(partial.values[1] != null);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
}

test "decision functions generated statement renews revoked leases without changing its model contract" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Scenario = enum { renew, changed_model, changed_limit, outage, repeatedly_stale, missing_lease, cancelled, cancelled_discovery, upstream_failure };
    const Fixture = struct {
        var scenario: Scenario = .renew;
        var revoked: bool = false;
        var routes: []httpx.testing_mod.Route = undefined;
        var cancellation: *std.atomic.Value(bool) = undefined;
        const answer = "{\"choices\":[{\"message\":{\"content\":\"{\\\"items\\\":[{\\\"item\\\":0,\\\"value\\\":{\\\"answer\\\":\\\"same\\\"}}]}\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}";
        const revision = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        const headers = [_]httpx.testing_mod.HeaderPair{ .{ .name = "X-Antfly-Capability-Token", .value = "new-token" }, .{ .name = "X-Antfly-Capability-Revision", .value = revision } };
        fn authority(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer fixture", req.header("Authorization") orelse return error.MissingAuthorization);
            try std.testing.expectEqualStrings("docs", req.header("X-Antfly-Source-Table") orelse return error.MissingScope);
        }
        fn catalog(req: httpx.testing_mod.RequestInfo) !void {
            try authority(req);
            // The concrete execution is /generate, not /generate/batch.
            try std.testing.expectEqualStrings("model=vision&task=generate&operation=generate", req.query);
            if (revoked and scenario == .cancelled_discovery) cancellation.store(true, .release);
        }
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try authority(req);
            try std.testing.expectEqualStrings(revision, req.header("X-Antfly-Capability-Revision") orelse return error.MissingRevision);
            const token = req.header("X-Antfly-Capability-Token") orelse return error.MissingToken;
            if (revoked and scenario == .cancelled) cancellation.store(true, .release);
            if (revoked and scenario == .upstream_failure) {
                routes[1].respond = .{ .status = 503, .body = "{\"error\":\"service unavailable\"}" };
            } else if (revoked and (scenario == .repeatedly_stale or std.mem.eql(u8, token, "old-token"))) {
                routes[1].respond = .{ .status = 409, .headers = &.{.{ .name = "X-Antfly-Capability-Stale", .value = "true" }}, .body = "{\"error\":\"stale lease\",\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}" };
            } else routes[1].respond = .{ .body = answer };
        }
        fn serve(server: *httpx.TestServer, failure: *?anyerror) void {
            while (true) server.handleOne() catch |err| {
                if (err != error.Canceled and err != error.Cancelled) failure.* = err;
                return;
            };
        }
    };
    const catalog =
        \\{"generators":{"vision":{"model_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","inputs":["text"],"inference_capabilities":{"version":3,"task":"generate","input_modalities":["text"],"accepted_mime_types":["text/plain"],"input_granularity":"item","output":"generated_text","result_cardinality":"one_per_item","prompt_policy":"explicit","borrowed_attachments":false,"numeric_responses_v1":false,"rerank_documents_v1":false,"generation_grammar_v1":true,"batch":{"mode":"none","preferred_items":1,"max_items":1,"max_encoded_media_bytes":null,"max_decoded_pixels":null,"max_media_parts_per_item":0,"per_item_failures":false}}}}}
    ;
    for (std.enums.values(Scenario)) |scenario| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        var routes = [_]httpx.testing_mod.Route{
            .{ .path = "/ai/v1/models", .assert_request = Fixture.catalog, .respond = .{ .body = catalog, .headers = &.{ .{ .name = "X-Antfly-Capability-Token", .value = "old-token" }, .{ .name = "X-Antfly-Capability-Revision", .value = Fixture.revision } } } },
            .{ .method = .POST, .path = "/generate", .assert_request = Fixture.request },
        };
        var cancellation = std.atomic.Value(bool).init(false);
        Fixture.cancellation = &cancellation;
        Fixture.scenario = scenario;
        Fixture.revoked = false;
        Fixture.routes = &routes;
        var server = try httpx.TestServer.start(a, io, &routes);
        defer server.deinit();
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Fixture.serve, .{ &server, &failure });
        var client = httpx.Client.initWithConfig(a, io, .{ .retry_policy = .{ .max_retries = 0 } });
        defer client.deinit();
        var registry = registry_mod.Registry.init(a);
        defer registry.deinit();
        try registry.registerDeciderConfig("judge", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "vision", .url = "" }, .probabilities = .verdict_only });
        var runtime = Runtime{ .registry = &registry, .resolution_allocator = a, .http = &client, .io = io, .source_table = "docs", .antfly_url = server.baseUrl(), .antfly_api_key = "fixture" };
        defer runtime.deinit();
        runtime.context = .{ .io = io, .deadline_ns = null, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancellation) };
        const questions = try std.json.parseFromSliceLeaky(decisions.Json, alloc,
            \\{"answer":{"type":"choice","instructions":"Compare","criteria":{"same":"Same","different":"Different"}}}
        , .{});
        const requests = [_]decisions.Request{.{ .decider = "judge", .questions = questions, .input = .{ .text = "A B" } }};
        const first = try runtime.provider().evaluateBatch(alloc, &requests);
        try std.testing.expectEqualStrings("same", first[0].object.get("answers").?.array.items[0].object.get("choice").?.string);
        try std.testing.expectEqual(@as(usize, 1), server.routeHitCount(0));
        Fixture.revoked = true;
        routes[0].respond.headers = &Fixture.headers;
        if (scenario == .changed_model) {
            routes[0].respond.body = try std.mem.replaceOwned(u8, alloc, catalog, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        }
        if (scenario == .changed_limit) routes[0].respond.body = try std.mem.replaceOwned(u8, alloc, catalog, "\"max_decoded_pixels\":null", "\"max_decoded_pixels\":0");
        if (scenario == .outage) routes[0].respond.status = 503;
        if (scenario == .missing_lease) routes[0].respond.headers = &.{};
        if (scenario == .cancelled_discovery) routes[0].respond.delay_ns = 5 * std.time.ns_per_s;
        const started = @import("antfly_platform").time.monotonicNs();
        const second = runtime.provider().evaluateBatch(alloc, &requests);
        switch (scenario) {
            .renew => {
                const values = try second;
                try std.testing.expectEqualStrings("same", values[0].object.get("answers").?.array.items[0].object.get("choice").?.string);
                try std.testing.expectEqualStrings("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", values[0].object.get("model_revision").?.string);
                // Reuse the replacement on a later window, including after the
                // result has been normalized against the original snapshot.
                _ = try runtime.provider().evaluateBatch(alloc, &requests);
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(0));
                try std.testing.expectEqual(@as(usize, 4), server.routeHitCount(1));
                try std.testing.expectEqual(@as(u64, 32), runtime.input_tokens);
                try std.testing.expectEqual(@as(u64, 16), runtime.output_tokens);
            },
            .changed_model, .changed_limit, .missing_lease => {
                try std.testing.expectError(error.InferenceCapabilitiesStale, second);
                try std.testing.expectError(error.InferenceCapabilitiesStale, runtime.provider().evaluateBatch(alloc, &requests));
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(0));
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(1));
            },
            .outage => {
                try std.testing.expectError(error.RemoteCapabilityDiscoveryTransient, second);
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(0));
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(1));
                routes[0].respond.status = 200;
                _ = try runtime.provider().evaluateBatch(alloc, &requests);
                try std.testing.expectEqual(@as(usize, 3), server.routeHitCount(0));
                try std.testing.expectEqual(@as(usize, 3), server.routeHitCount(1));
            },
            .repeatedly_stale => {
                try std.testing.expectError(error.InferenceCapabilitiesStale, second);
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(0));
                try std.testing.expectEqual(@as(usize, 3), server.routeHitCount(1));
            },
            .cancelled, .cancelled_discovery => {
                // Cancellation must interrupt the in-flight discovery read,
                // rather than waiting for its delayed response to complete.
                try std.testing.expect(@import("antfly_platform").time.monotonicNs() - started < 2 * std.time.ns_per_s);
                if (second) |_| return error.TestExpectedError else |err| try std.testing.expect(err == error.Cancelled or err == error.Canceled);
                try std.testing.expectEqual(@as(usize, if (scenario == .cancelled) 1 else 2), server.routeHitCount(0));
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(1));
            },
            .upstream_failure => {
                if (second) |_| return error.TestExpectedError else |_| {}
                try std.testing.expectEqual(@as(usize, 1), server.routeHitCount(0));
                try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(1));
            },
        }
        group.cancel(io);
        if (failure) |err| return err;
    }
}

test "decision functions scoped generated image validation and execution share a route" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Fixture = struct {
        var routes: []httpx.testing_mod.Route = undefined;
        var vision_catalog: []const u8 = undefined;
        const text_catalog =
            \\{"generators":{"vision":{"model_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","inputs":["text"],"inference_capabilities":{"version":3,"task":"generate","input_modalities":["text"],"accepted_mime_types":["text/plain"],"input_granularity":"item","output":"generated_text","result_cardinality":"one_per_item","prompt_policy":"explicit","borrowed_attachments":false,"numeric_responses_v1":false,"rerank_documents_v1":false,"generation_grammar_v1":true,"batch":{"mode":"none","preferred_items":1,"max_items":1,"max_encoded_media_bytes":null,"max_decoded_pixels":null,"max_media_parts_per_item":0,"per_item_failures":false}}}}}
        ;
        fn catalog(req: httpx.testing_mod.RequestInfo) !void {
            const table = req.header("X-Antfly-Source-Table") orelse return error.MissingScope;
            try std.testing.expectEqualStrings("model=vision&task=generate&operation=generate", req.query);
            routes[0].respond.body = if (std.mem.eql(u8, table, "photos")) vision_catalog else text_catalog;
        }
        fn generate(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("photos", req.header("X-Antfly-Source-Table") orelse return error.MissingScope);
            try std.testing.expect(std.mem.indexOf(u8, req.body, "iVBORw0KGgo=") != null);
        }
        fn serve(server: *httpx.TestServer, failure: *?anyerror) void {
            while (true) server.handleOne() catch |err| {
                if (err != error.Canceled and err != error.Cancelled) failure.* = err;
                return;
            };
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var vision_catalog = try std.mem.replaceOwned(u8, alloc, Fixture.text_catalog, "[\"text\"]", "[\"text\",\"image\"]");
    vision_catalog = try std.mem.replaceOwned(u8, alloc, vision_catalog, "[\"text/plain\"]", "[\"text/plain\",\"image/png\"]");
    Fixture.vision_catalog = try std.mem.replaceOwned(u8, alloc, vision_catalog, "\"max_media_parts_per_item\":0", "\"max_media_parts_per_item\":8");
    var routes = [_]httpx.testing_mod.Route{
        .{ .path = "/ai/v1/models", .assert_request = Fixture.catalog },
        .{ .method = .POST, .path = "/generate", .assert_request = Fixture.generate, .respond = .{ .body = "{\"choices\":[{\"message\":{\"content\":\"{\\\"items\\\":[{\\\"item\\\":0,\\\"value\\\":{\\\"answer\\\":\\\"same\\\"}}]}\"}}]}" } },
    };
    Fixture.routes = &routes;
    var server = try httpx.TestServer.start(a, io, &routes);
    defer server.deinit();
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Fixture.serve, .{ &server, &failure });
    var client = httpx.Client.initWithConfig(a, io, .{ .retry_policy = .{ .max_retries = 0 } });
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("judge", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "vision", .url = "" }, .probabilities = .verdict_only, .max_image_input_tokens = 8192 });
    // SQL keeps a statement-wide runtime and puts each bound table on the
    // provider. Its default must not supply discovery for another table.
    var runtime = Runtime{ .registry = &registry, .resolution_allocator = a, .http = &client, .io = io, .antfly_url = server.baseUrl(), .source_table = "default" };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, alloc,
        \\{"answer":{"type":"choice","instructions":"Compare","criteria":{"same":"Same","different":"Different"}}}
    , .{});
    const input = try decisions.Input.fromJson(try std.json.parseFromSliceLeaky(decisions.Json, alloc,
        \\[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}]}]
    , .{}));
    const request = decisions.Request{ .decider = "judge", .questions = questions, .input = input };
    const first = try runtime.provider().withSourceTable("photos").evaluateBatch(alloc, &.{request});
    try std.testing.expectEqualStrings("same", first[0].object.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqual(@as(usize, 1), server.routeHitCount(0));
    try std.testing.expectEqual(@as(usize, 1), server.routeHitCount(1));
    try std.testing.expectError(error.UnsupportedInferenceFeature, runtime.provider().withSourceTable("text_docs").evaluateBatch(alloc, &.{request}));
    try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(0));
    try std.testing.expectEqual(@as(usize, 1), server.routeHitCount(1));
    // Internal batches can carry per-item scopes without a provider override.
    var scoped = request;
    scoped.source_table = "photos";
    const partial = try runtime.evaluatePartial(alloc, &.{scoped});
    try std.testing.expect(partial.values[0] != null);
    try std.testing.expect(partial.failures[0] == null);
    // The bound provider remains authoritative over an item's inherited scope.
    scoped.source_table = "text_docs";
    _ = try runtime.provider().withSourceTable("photos").evaluateBatch(alloc, &.{scoped});
    try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(0));
    try std.testing.expectEqual(@as(usize, 3), server.routeHitCount(1));
    try std.testing.expectEqualStrings("default", runtime.source_table);
    group.cancel(io);
    if (failure) |err| return err;
}

test "decision functions late stale responses preserve a renewed execution lease during discovery outage" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Fixture = struct {
        var server_io: std.Io = undefined;
        var revoked: bool = false;
        var old_inflight: std.Io.Event = .unset;
        var replacement_ready: std.Io.Event = .unset;
        var routes: []httpx.testing_mod.Route = undefined;
        const revision = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        const answer = "{\"choices\":[{\"message\":{\"content\":\"{\\\"items\\\":[{\\\"item\\\":0,\\\"value\\\":{\\\"answer\\\":\\\"same\\\"}}]}\"}}]}";
        fn authority(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer fixture", req.header("Authorization") orelse return error.MissingAuthorization);
            try std.testing.expectEqualStrings("docs", req.header("X-Antfly-Source-Table") orelse return error.MissingScope);
        }
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try authority(req);
            const token = req.header("X-Antfly-Capability-Token") orelse return error.MissingToken;
            const descriptor = req.header("X-Antfly-Capability-Revision") orelse return error.MissingRevision;
            if (revoked and std.mem.eql(u8, token, "old-token") and std.mem.eql(u8, descriptor, revision)) {
                old_inflight.set(server_io);
                try replacement_ready.wait(server_io);
                routes[1].respond = .{ .status = 409, .headers = &.{.{ .name = "X-Antfly-Capability-Stale", .value = "true" }}, .body = "{\"error\":\"old lease revoked\"}" };
            } else routes[1].respond = .{ .body = answer };
        }
        fn serve(server: *httpx.TestServer, failure: *?anyerror) void {
            while (true) server.handleOne() catch |err| {
                if (err != error.Canceled and err != error.Cancelled) failure.* = err;
                return;
            };
        }
        fn run(runtime: *Runtime, alloc: std.mem.Allocator, requests: []const decisions.Request, failure: *?anyerror) void {
            _ = runtime.provider().evaluateBatch(alloc, requests) catch |err| {
                failure.* = err;
                return;
            };
        }
    };
    const catalog =
        \\{"generators":{"vision":{"model_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","inputs":["text"],"inference_capabilities":{"version":3,"task":"generate","input_modalities":["text"],"accepted_mime_types":["text/plain"],"input_granularity":"item","output":"generated_text","result_cardinality":"one_per_item","prompt_policy":"explicit","borrowed_attachments":false,"numeric_responses_v1":false,"rerank_documents_v1":false,"generation_grammar_v1":true,"batch":{"mode":"none","preferred_items":1,"max_items":1,"max_encoded_media_bytes":null,"max_decoded_pixels":null,"max_media_parts_per_item":0,"per_item_failures":false}}}}}
    ;
    for ([_]bool{ false, true }) |rotate_revision_only| {
        var routes = [_]httpx.testing_mod.Route{
            .{ .path = "/ai/v1/models", .assert_request = Fixture.authority, .respond = .{ .body = catalog, .headers = &.{ .{ .name = "X-Antfly-Capability-Token", .value = "old-token" }, .{ .name = "X-Antfly-Capability-Revision", .value = Fixture.revision } } } },
            .{ .method = .POST, .path = "/generate", .assert_request = Fixture.request },
        };
        Fixture.routes = &routes;
        Fixture.server_io = io;
        Fixture.revoked = false;
        Fixture.old_inflight = .unset;
        Fixture.replacement_ready = .unset;
        var server = try httpx.TestServer.start(a, io, &routes);
        defer server.deinit();
        var server_failure: ?anyerror = null;
        var servers = std.Io.Group.init;
        defer servers.cancel(io);
        try servers.concurrent(io, Fixture.serve, .{ &server, &server_failure });
        var client = httpx.Client.initWithConfig(a, io, .{ .retry_policy = .{ .max_retries = 0 } });
        defer client.deinit();
        var registry = registry_mod.Registry.init(a);
        defer registry.deinit();
        try registry.registerDeciderConfig("judge", .{ .decision_method = .generated, .generator = .{ .provider = .antfly, .model = "vision", .url = "" }, .probabilities = .verdict_only });
        var runtime = Runtime{ .registry = &registry, .resolution_allocator = a, .http = &client, .io = io, .source_table = "docs", .antfly_url = server.baseUrl(), .antfly_api_key = "fixture" };
        defer runtime.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const questions = try std.json.parseFromSliceLeaky(decisions.Json, alloc,
            \\{"answer":{"type":"choice","instructions":"Compare","criteria":{"same":"Same","different":"Different"}}}
        , .{});
        const requests = [_]decisions.Request{.{ .decider = "judge", .questions = questions, .input = .{ .text = "A B" } }};
        _ = try runtime.provider().evaluateBatch(alloc, &requests);
        Fixture.revoked = true;
        var client_failure: ?anyerror = null;
        var calls = std.Io.Group.init;
        defer calls.cancel(io);
        defer Fixture.replacement_ready.set(io);
        try calls.concurrent(io, Fixture.run, .{ &runtime, alloc, &requests, &client_failure });
        try Fixture.old_inflight.wait(io);
        // A second caller revokes the old tuple and resolves a replacement
        // while the first invocation's stale response is still in flight.
        try servers.concurrent(io, Fixture.serve, .{ &server, &server_failure });
        const cache = &runtime.resolved_generators.items[0].cache;
        const headers: []const [2][]const u8 = &.{ .{ "Authorization", "Bearer fixture" }, .{ "X-Antfly-Source-Table", "docs" } };
        try cache.invalidateLease(server.baseUrl(), "vision", .generate, headers, try remote_capabilities.LeaseIdentity.init("old-token", Fixture.revision));
        const replacement_token = if (rotate_revision_only) "old-token" else "new-token";
        const replacement_revision = if (rotate_revision_only) "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" else Fixture.revision;
        const replacement_headers = [_]httpx.testing_mod.HeaderPair{
            .{ .name = "X-Antfly-Capability-Token", .value = replacement_token },
            .{ .name = "X-Antfly-Capability-Revision", .value = replacement_revision },
        };
        routes[0].respond.headers = &replacement_headers;
        const lease = try cache.getOrDiscoverLeaseWithContext(&client, server.baseUrl(), "vision", .generate, headers, .{});
        try std.testing.expectEqualStrings(replacement_token, lease.routing_token.?.slice());
        try std.testing.expectEqualStrings(replacement_revision, lease.descriptor_revision.?.slice());
        // Discovery is now unavailable. The delayed rejection must use the
        // cached replacement for retry, and keep it for subsequent windows.
        routes[0].respond.status = 503;
        Fixture.replacement_ready.set(io);
        try calls.await(io);
        if (client_failure) |err| return err;
        _ = try runtime.provider().evaluateBatch(alloc, &requests);
        try std.testing.expectEqual(@as(usize, 2), server.routeHitCount(0));
        try std.testing.expectEqual(@as(usize, 4), server.routeHitCount(1));
        const retained = try cache.routingToken(server.baseUrl(), "vision", .generate, headers);
        try std.testing.expectEqualStrings(replacement_token, retained.?.slice());
        servers.cancel(io);
        if (server_failure) |err| return err;
    }
}

test "decision functions Cloudflare native run transport supports gateway routing and preserves measured usage" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Fixture = struct {
        fn check(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer fixture-token", req.header("Authorization").?);
            try std.testing.expect(req.header(execution.source_table_header) == null);
            if (req.header("cf-aig-gateway-id")) |id| try std.testing.expectEqualStrings("gateway", id);
            var parsed = try std.json.parseFromSlice(decisions.Json, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("clef", parsed.value.object.get("model").?.string);
            try std.testing.expectEqualStrings("Refund?", parsed.value.object.get("questions").?.object.get("answer").?.object.get("instructions").?.string);
            try std.testing.expect(parsed.value.object.contains("state"));
            try std.testing.expect(!parsed.value.object.contains("messages"));
        }
        fn run(runtime: *Runtime, alloc: std.mem.Allocator, questions: decisions.Json, failure: *?anyerror) void {
            const values = runtime.provider().withSourceTable("private-source").evaluateBatch(alloc, &.{.{ .input = .{ .text = "refund" }, .decider = "cloudflare", .questions = questions }}) catch |err| {
                failure.* = err;
                return;
            };
            const probability = decisions.selectResult(.ai_probability, values[0]) catch |err| {
                failure.* = err;
                return;
            };
            std.testing.expectApproxEqAbs(@as(f64, 0.9), probability.float, 1e-9) catch |err| {
                failure.* = err;
            };
        }
    };
    for ([_]?[]const u8{ null, "gateway" }) |gateway| {
        var server = try httpx.TestServer.start(a, io, &.{.{ .method = .POST, .path = "/client/v4/accounts/account/ai/run/@cf/cloudflare/clef", .assert_request = Fixture.check, .respond = .{ .body = "{\"success\":true,\"result\":{\"model\":\"clef\",\"answers\":{\"answer\":{\"type\":\"noul\",\"noul\":0.9}},\"usage\":{\"input_tokens\":7,\"output_tokens\":0}}}" } }});
        defer server.deinit();
        var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
        defer client.deinit();
        var registry = registry_mod.Registry.init(a);
        defer registry.deinit();
        const url = try std.fmt.allocPrint(a, "{s}/client/v4/accounts/account/ai/run/@cf/cloudflare/clef", .{server.baseUrl()});
        defer a.free(url);
        try registry.registerDeciderConfig("cloudflare", .{ .provider = .cloudflare, .account_id = "account", .model = "@cf/cloudflare/clef", .url = url, .api_key = "fixture-token", .gateway_id = gateway });
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        var runtime = Runtime{ .registry = &registry, .http = &client, .io = io };
        defer runtime.deinit();
        const questions = try decisions.questionsFor(alloc, .ai_probability, &.{ .{ .string = "refund" }, .{ .string = "Refund?" }, .{ .string = "cloudflare" } });
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Fixture.run, .{ &runtime, alloc, questions, &failure });
        try server.handleOne();
        try group.await(io);
        if (failure) |err| return err;
        try std.testing.expectEqual(@as(u64, 7), runtime.input_tokens);
        try std.testing.expectEqual(@as(u64, 1), runtime.rows);
    }
}

test "decision provider embedding similarity batches and deduplicates named embedder prototypes" {
    const Fake = struct {
        calls: usize = 0,
        texts: usize = 0,
        fn dense(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.texts += texts.len;
            const vectors = try a.alloc([]f32, texts.len);
            for (texts, vectors) |text, *vector| vector.* = try a.dupe(f32, if (std.mem.indexOf(u8, text, "billing") != null) &.{ 0, 1 } else &.{ 1, 0 });
            return vectors;
        }
        fn sparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]managed.SparseEmbedding {
            return error.UnexpectedCall;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake = Fake{};
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerEmbedderConfig("dense", .{ .provider = .antfly, .model = "test", .dimensions = 2 });
    try registry.registerDeciderConfig("routing", .{ .decision_method = .embedding_similarity, .embedder = "dense", .embedding_options = .{ .dimensions = 2 } });
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.init(a, io_impl.io());
    defer client.deinit();
    var runtime = Runtime{ .registry = &registry, .http = &client, .io = io_impl.io(), .antfly_provider = .{ .ptr = &fake, .embed_dense_texts = Fake.dense, .embed_sparse_texts = Fake.sparse } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, a,
        \\{"route":{"type":"choice","instructions":"Choose category","criteria":{"account":"account","billing":"billing"}}}
    , .{});
    const request = decisions.Request{ .decider = "routing", .input = .{ .text = "account" }, .questions = questions };
    const results = try runtime.provider().evaluateBatch(a, &.{ request, request });
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 3), fake.texts);
    for (results) |result| {
        const answer = result.object.get("answers").?.array.items[0];
        try std.testing.expectEqualStrings("account", answer.object.get("choice").?.string);
        try std.testing.expect(!answer.object.contains("probabilities"));
    }
}

test "decision provider embedding similarity partial batches isolate invalid query vectors and retain usage" {
    const Fake = struct {
        calls: usize = 0,
        texts: usize = 0,
        invalid_prototypes: bool = false,
        fn dense(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.texts += texts.len;
            const vectors = try a.alloc([]f32, texts.len);
            for (texts, vectors) |text, *vector| vector.* = try a.dupe(f32, if ((self.invalid_prototypes and std.mem.indexOf(u8, text, "Category:") != null) or std.mem.indexOf(u8, text, "BAD_QUERY") != null) &.{ 0, 0 } else if (std.mem.indexOf(u8, text, "billing") != null) &.{ 0, 1 } else &.{ 1, 0 });
            return vectors;
        }
        fn sparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]managed.SparseEmbedding {
            return error.UnexpectedCall;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake = Fake{};
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerEmbedderConfig("dense", .{ .provider = .antfly, .model = "test", .dimensions = 2 });
    try registry.registerDeciderConfig("routing", .{ .decision_method = .embedding_similarity, .embedder = "dense", .embedding_options = .{ .dimensions = 2 } });
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.init(a, io_impl.io());
    defer client.deinit();
    var runtime = Runtime{ .registry = &registry, .http = &client, .io = io_impl.io(), .antfly_provider = .{ .ptr = &fake, .embed_dense_texts = Fake.dense, .embed_sparse_texts = Fake.sparse } };
    defer runtime.deinit();
    const questions = try std.json.parseFromSliceLeaky(decisions.Json, a,
        \\{"route":{"type":"choice","instructions":"Choose category","criteria":{"account":"account","billing":"billing"}}}
    , .{});
    const good = decisions.Request{ .decider = "routing", .input = .{ .text = "account" }, .questions = questions };
    const bad = decisions.Request{ .decider = "routing", .input = .{ .text = "BAD_QUERY" }, .questions = questions };
    const result = try runtime.evaluatePartial(a, &.{ good, bad });
    try std.testing.expect(result.values[0] != null);
    try std.testing.expect(result.failures[0] == null);
    try std.testing.expect(result.values[1] == null);
    try std.testing.expectEqual(error.InvalidEmbeddingDecisionOutput, result.failures[1].?);
    try std.testing.expectEqualStrings("account", result.values[0].?.object.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expect(runtime.input_tokens > 0);
    try std.testing.expectEqual(@as(u64, 1), runtime.rows);
    try std.testing.expectEqual(@as(u64, 1), runtime.batches);
    const charged_before = runtime.input_tokens;
    const reversed = try runtime.evaluatePartial(a, &.{ bad, good });
    try std.testing.expect(reversed.values[0] == null);
    try std.testing.expectEqual(error.InvalidEmbeddingDecisionOutput, reversed.failures[0].?);
    try std.testing.expect(reversed.failures[1] == null);
    const charged = reversed.values[1].?.object.get("usage").?.object.get("input_tokens").?.integer;
    try std.testing.expectEqual(runtime.input_tokens - charged_before, @as(u64, @intCast(charged)));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    fake.invalid_prototypes = true;
    const before_shared_failure = runtime.input_tokens;
    const failed = try runtime.evaluatePartial(a, &.{ good, good });
    for (failed.values, failed.failures) |value, failure| {
        try std.testing.expect(value == null);
        try std.testing.expectEqual(error.InvalidEmbeddingDecisionOutput, failure.?);
    }
    try std.testing.expect(runtime.input_tokens > before_shared_failure);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
}

test "decision provider rewriting requires completion aware local generation without altering model configuration" {
    const Native = @import("antfly_inference_types");
    const Fake = struct {
        native_calls: usize = 0,
        json_calls: usize = 0,
        fn text(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const Native.ChatMessage, opts: Native.GenerationOptions, _: execution.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.native_calls += 1;
            try std.testing.expectEqual(@as(i32, 1), opts.max_tokens);
            return a.dupe(u8, "partial");
        }
        fn json(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, _: ?execution.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.json_calls += 1;
            return a.dupe(u8, "{\"choices\":[{\"message\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}");
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var registry = try registry_mod.Registry.parseFromSlice(a,
        \\{"generators":{"chat":{"provider":"antfly","model":"local","max_tokens":1}},"rewriters":{"clean":{"generator":"chat"}}}
    );
    defer registry.deinit();
    var fake = Fake{};
    var http = httpx.Client.init(a, std.testing.io);
    defer http.deinit();
    const executor = registry_mod.tasks.Executor{
        .view = .{ .providers = &registry },
        .http = &http,
        .context = .{ .io = std.testing.io, .deadline_ns = null },
        .antfly_provider = .{ .ptr = &fake, .embed_dense_texts = undefined, .embed_sparse_texts = undefined, .generate_messages_with_context = Fake.text, .generate_json = Fake.json },
    };
    try std.testing.expectError(error.InferenceOutputIncomplete, executor.rewrite(a, "clean", "rewriters/clean", &.{"a long input"}));
    try std.testing.expectEqual(@as(usize, 0), fake.native_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.json_calls);
    var legacy = executor;
    legacy.antfly_provider.?.generate_json = null;
    try std.testing.expectError(error.UnsupportedInferenceFeature, legacy.rewrite(a, "clean", "rewriters/clean", &.{"a long input"}));
    try std.testing.expectEqual(@as(usize, 0), fake.native_calls);
}

test "decision provider materialized named embedders use frozen dependencies and preserve batch failures" {
    const Fake = struct {
        calls: usize = 0,
        texts: usize = 0,
        fn dense(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.texts += texts.len;
            const vectors = try a.alloc([]f32, texts.len);
            for (texts, vectors) |text, *vector| vector.* = try a.dupe(f32, if (std.mem.indexOf(u8, text, "BAD_QUERY") != null) &.{ 0, 0 } else if (std.mem.indexOf(u8, text, "billing") != null) &.{ 0, 1 } else &.{ 1, 0 });
            return vectors;
        }
        fn sparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]managed.SparseEmbedding {
            return error.UnexpectedCall;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake = Fake{};
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.init(a, io_impl.io());
    defer client.deinit();
    const Assets = @import("../asset_producer_runtime.zig").Runtime;
    var assets = Assets.initWithOptions(a, &client, .{ .antfly_provider = .{ .ptr = &fake, .owns_invocation_admission = true, .embed_dense_texts = Fake.dense, .embed_sparse_texts = Fake.sparse } });
    defer assets.deinit();
    const spec =
        \\{"version":"v1","decider":{"decision_method":"embedding_similarity","embedder":"dense","embedding_options":{"dimensions":2}},"embedders":{"dense":{"provider":"antfly","model":"test","dimensions":2}},"questions":[{"name":"route","type":"choice","instructions":"Choose category","choices":[{"value":"account","description":"account"},{"value":"billing","description":"billing"}]}]}
    ;
    const Request = @import("../storage/db/enrichment/asset_producer.zig").Request;
    const good = Request{ .producer_type = .decision, .config_json = spec, .source_text = "account", .item_id = "good" };
    const bad = Request{ .producer_type = .decision, .config_json = spec, .source_text = "BAD_QUERY", .item_id = "bad" };
    var result = try assets.producer().produceBatchReported(a, &.{ good, bad });
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 4), fake.texts);
    const valid = try std.json.parseFromSliceLeaky(decisions.Json, a, result.items[0].result.value, .{});
    try std.testing.expectEqualStrings("account", valid.object.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqual(error.InvalidEmbeddingDecisionOutput, result.items[1].result.item_error.cause);
    const provenance = valid.object.get("provenance").?.object;
    const first_hash = provenance.get("specification_hash").?.string;
    const snapshot = try @import("materialization.zig").parse(a, spec);
    defer snapshot.deinit();
    try snapshot.value.embedders.object.getPtr("dense").?.object.put(a, "model", .{ .string = "new-model" });
    const changed = try @import("materialization.zig").provenance(a, snapshot.value, null);
    try std.testing.expect(!std.mem.eql(u8, first_hash, changed.object.get("specification_hash").?.string));
    const missing =
        \\{"version":"v1","decider":{"decision_method":"embedding_similarity","embedder":"dense","embedding_options":{"dimensions":2}},"questions":[{"name":"route","type":"choice","instructions":"Route","choices":[{"value":"a"},{"value":"b"}]}]}
    ;
    try std.testing.expectError(error.UnknownEmbedder, @import("materialization.zig").parse(a, missing));
}
