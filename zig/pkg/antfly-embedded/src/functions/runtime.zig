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
    if (cfg.decision_method == .generated) return .{ .body = try std.json.Stringify.valueAlloc(a, .{ .input = try request.input.requireText(), .questions = request.questions, .instructions = cfg.instructions }, .{}) };
    if (cfg.provider == .openai) return .{ .body = try openai.requestBody(a, cfg.modelName(), request.input, request.questions), .image_bytes = request.input.inlineImageBytes() };
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

fn reconcileUsage(reservation: *quotas.TokenReservation, source: decisions.Json) void {
    if (source != .object) return;
    const usage = source.object.get("usage") orelse return;
    if (usage != .object) return;
    const input = usage.object.get("input_tokens") orelse return;
    const output = usage.object.get("output_tokens") orelse return;
    if (input != .integer or output != .integer or input.integer < 0 or output.integer < 0) return;
    reservation.reconcile(@intCast(input.integer), @intCast(output.integer));
}

pub const Runtime = struct {
    registry: *const registry_mod.Registry,
    limits: *quotas.Registry = &quotas.process_registry,
    http: *httpx.Client,
    io: std.Io,
    context: ?execution.RequestContext = null,
    secret_store: ?*secrets.FileStore = null,
    antfly_provider: ?managed.AntflyProvider = null,
    antfly_url: ?[]const u8 = null,
    antfly_api_key: ?[]const u8 = null,
    source_table: []const u8 = "",
    rows: u64 = 0,
    input_tokens: u64 = 0,
    estimated_input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    batches: u64 = 0,
    latency_ns: u64 = 0,
    profile_allocator: ?std.mem.Allocator = null,
    provenance: std.ArrayList(struct { decider: []const u8, provider: []const u8, model: []const u8, rows: u64 = 0 }) = .empty,
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
    fn resolvedCapabilities(ptr: *anyopaque, name: []const u8) !decisions.Capabilities {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        return self.registry.getDeciderCapabilities(name);
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (self.context) |ctx| try ctx.check();
    }
    fn validate(ptr: *anyopaque, name: []const u8, questions: decisions.Json) !void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        const cfg = try self.registry.getDeciderConfig(name);
        try cfg.validate();
        _ = try quotas.Policy.fromConfig(cfg.rate_limit);
        try decisions.validateQuestions(questions, cfg.resolvedCapabilities());
        if (cfg.decision_method == .generated) {
            var arena = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
            defer arena.deinit();
            var cache = remote_capabilities.Cache.init(std.heap.smp_allocator, self.io);
            defer cache.deinit();
            var engine = self.constrainedRuntime(&cache);
            var generator = try cfg.generator.?.clone(arena.allocator());
            if (generator.provider == .antfly and generator.url.len == 0) generator.url = self.antfly_url orelse "";
            const caps = try engine.capabilities(arena.allocator(), &generator);
            const mode = try caps.select(cfg.output, cfg.probabilities);
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
            .secret_store = self.secret_store,
            .max_response_bytes = max_response_bytes,
            .source_table = self.source_table,
            .execution = .{ .routing = .{ .source_table = self.source_table } },
            .request_context = self.context,
            .limits = self.limits,
        } };
    }
    pub fn evaluateGeneratedPartial(self: *Runtime, a: std.mem.Allocator, name: []const u8, requests: []const decisions.Request) !GeneratedBatch {
        try validate(self, name, requests[0].questions);
        const cfg = try self.registry.getDeciderConfig(name);
        if (cfg.decision_method != .generated) return error.InvalidDeciderConfig;
        var lease = admission.tryAcquireLease() orelse return error.DecisionRateLimited;
        defer lease.release();
        if (self.rows + requests.len > cfg.max_rows) return error.DecisionLimitExceeded;
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
        const result = try self.generateBatchPartial(a, cfg, requests);
        self.rows += requests.len;
        self.input_tokens +|= result.input_tokens;
        self.output_tokens +|= result.output_tokens;
        self.batches +|= result.calls;
        if (self.input_tokens > cfg.max_input_tokens) return error.DecisionLimitExceeded;
        return result;
    }
    fn generateBatchPartial(self: *Runtime, a: std.mem.Allocator, cfg: decisions.DeciderConfig, requests: []const decisions.Request) !GeneratedBatch {
        var cache = remote_capabilities.Cache.init(a, self.io);
        defer cache.deinit();
        var engine = self.constrainedRuntime(&cache);
        const source_table = if (requests[0].source_table.len > 0) requests[0].source_table else self.source_table;
        engine.options.source_table = source_table;
        engine.options.execution.routing.source_table = source_table;
        var generator = try cfg.generator.?.clone(a);
        defer generator.deinit(a);
        if (generator.provider == .antfly and generator.url.len == 0 and self.antfly_url != null) {
            generator.url = try a.dupe(u8, self.antfly_url.?);
        }
        const caps = try engine.capabilities(a, &generator);
        const mode = try caps.select(cfg.output, cfg.probabilities);
        generator.logprobs = cfg.probabilities == .token_logprobs;
        const inputs = try a.alloc([]const u8, requests.len);
        defer a.free(inputs);
        for (inputs, requests) |*input, request| input.* = try request.input.requireText();
        var batch = cfg.batch;
        batch.max_items_per_call = @min(batch.max_items_per_call, cfg.batch_size);
        const responses = try a.alloc(?decisions.Json, requests.len);
        @memset(responses, null);
        var validation = GeneratedValidation{ .cfg = cfg, .requests = requests, .responses = responses };
        engine.semantic_validation = .{ .ptr = &validation, .validate = GeneratedValidation.validate };
        const result = try engine.execute(a, generator, mode, try generated.questionSchema(a, requests[0].questions), cfg.instructions, batch, inputs);
        for (responses) |*response| if (response.*) |*value| {
            if (caps.model_revision) |revision| try value.object.put(a, "model_revision", .{ .string = revision });
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
    fn generateBatch(self: *Runtime, a: std.mem.Allocator, cfg: decisions.DeciderConfig, requests: []const decisions.Request) ![]const decisions.Json {
        const partial = try self.generateBatchPartial(a, cfg, requests);
        const responses = try a.alloc(decisions.Json, requests.len);
        for (partial.values, partial.failures, responses) |value, failure, *response| response.* = value orelse return failure orelse error.InvalidDecisionOutput;
        return responses;
    }
    const Job = struct {
        runtime: *Runtime,
        request: decisions.Request,
        cfg: decisions.DeciderConfig,
        arena: std.heap.ArenaAllocator,
        budget: @import("../sql/memory_budget.zig"),
        response: ?decisions.Json = null,
        failure: ?anyerror = null,
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
            if (cfg.decision_method == .generated) return (try self.runtime.generateBatch(a, cfg, &.{self.request}))[0];
            const payload = try requestPayload(a, self.runtime.io, cfg, self.request);
            const body = payload.body;
            const base = if (cfg.provider == .antfly and cfg.url.len == 0) if (self.runtime.antfly_url) |endpoint| try registry_mod.gateway.apiBaseAlloc(a, endpoint) else cfg.baseUrl() else cfg.baseUrl();
            const url = try std.fmt.allocPrint(a, "{s}{s}", .{ std.mem.trimEnd(u8, base, "/"), switch (cfg.provider) {
                .antfly => "/decisions",
                .jev => "/v1/systemone",
                .openai => "/decisions",
            } });
            var key_source = try secrets.SecretValue.initConfigOrEnv(a, cfg.api_key orelse (if (cfg.provider == .antfly and cfg.url.len == 0) self.runtime.antfly_api_key else null), switch (cfg.provider) {
                .antfly => "ANTFLY_INFERENCE_API_KEY",
                .jev => "TYPESAFE_API_KEY",
                .openai => "OPENAI_API_KEY",
            });
            defer key_source.deinit(a);
            var quota = try self.runtime.limits.acquire(.{ .operation = .decision, .endpoint = .{
                .provider = std.meta.stringToEnum(quotas.Provider, @tagName(cfg.provider)).?,
                .endpoint = base,
                .model = cfg.modelName(),
                .credentials = @import("../common/credential_source_identity.zig").fromSecretValue(key_source),
            } }, try quotas.Policy.fromConfig(cfg.rate_limit));
            defer quota.release();
            var reservation: quotas.TokenReservation = .{ .limiter = quota.limiter(), .input_tokens = try estimateInputTokens(cfg, self.request.input, payload) };
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
                reconcileUsage(&reservation, parsed);
                return parsed;
            };
            const key = key_source.resolveOwned(a, self.runtime.secret_store) catch |err| return operationalError(err);
            if (cfg.provider != .antfly and key == null) return error.MissingDecisionApiKey;
            const headers = try a.alloc([2][]const u8, payload.headers.len + 3);
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
            if (!response.ok()) return switch (response.status.code) {
                429, 529 => error.DecisionRateLimited,
                401, 403 => error.DecisionUnauthorized,
                else => error.DecisionUpstreamFailure,
            };
            const bytes = response.body orelse return error.InvalidDecisionOutput;
            const parsed = try parseResponse(a, bytes);
            // Account usage before semantic output validation, including refused
            // or invalid answers for which the provider has already spent tokens.
            reconcileUsage(&reservation, parsed);
            if (cfg.provider == .antfly) {
                const root = try decisions.object(parsed);
                const answers = root.get("answers") orelse return error.InvalidDecisionOutput;
                if (answers != .array) return error.InvalidDecisionOutput;
            }
            return if (cfg.provider == .openai) openai.response(a, self.request.questions, parsed) else parsed;
        }
    };
    fn evaluate(ptr: *anyopaque, a: std.mem.Allocator, requests: []const decisions.Request) ![]const decisions.Json {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        const started = std.Io.Clock.awake.now(self.io).nanoseconds;
        defer self.latency_ns +|= @intCast(@max(0, std.Io.Clock.awake.now(self.io).nanoseconds - started));
        var lease = admission.tryAcquireLease() orelse return error.DecisionRateLimited;
        defer lease.release();
        const out = try a.alloc(decisions.Json, requests.len);
        var batch_size: usize = 32;
        var estimated = self.estimated_input_tokens;
        for (requests) |request| {
            const cfg = try self.registry.getDeciderConfig(request.decider);
            if (self.rows + requests.len > cfg.max_rows or self.input_tokens >= cfg.max_input_tokens) return error.DecisionLimitExceeded;
            try request.input.checkLimit(a, cfg.resolvedCapabilities().max_input_bytes);
            // Reserve text bytes plus the configured visual token bound before
            // any I/O. The shared quota uses this same estimate and actual usage.
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const payload = try requestPayload(scratch.allocator(), self.io, cfg, request);
            estimated = std.math.add(u64, estimated, try estimateInputTokens(cfg, request.input, payload)) catch return error.DecisionLimitExceeded;
            if (estimated > cfg.max_input_tokens) return error.DecisionLimitExceeded;
            batch_size = @min(batch_size, cfg.batch_size);
        }
        self.estimated_input_tokens = estimated;
        if (requests.len > 0) {
            const first = try self.registry.getDeciderConfig(requests[0].decider);
            var compatible = first.decision_method == .generated;
            const question_bytes = try std.json.Stringify.valueAlloc(a, requests[0].questions, .{});
            defer a.free(question_bytes);
            for (requests[1..]) |request| {
                const bytes = try std.json.Stringify.valueAlloc(a, request.questions, .{});
                defer a.free(bytes);
                compatible = compatible and std.mem.eql(u8, request.decider, requests[0].decider) and std.mem.eql(u8, request.source_table, requests[0].source_table) and std.mem.eql(u8, bytes, question_bytes);
            }
            if (compatible) {
                const partial = try self.generateBatchPartial(a, first, requests);
                self.rows += requests.len;
                self.input_tokens +|= partial.input_tokens;
                self.output_tokens +|= partial.output_tokens;
                self.batches +|= partial.calls;
                if (self.input_tokens > first.max_input_tokens) return error.DecisionLimitExceeded;
                for (partial.values, partial.failures, out, requests) |value, failure, *response, request| {
                    response.* = value orelse return failure orelse error.InvalidDecisionOutput;
                    try self.recordProvenance(request.decider, first, response.*);
                }
                return out;
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
            for (jobs, out[begin..end]) |*job, *value| {
                if (job.failure) |err| return err;
                if (job.cfg.model_identity) |expected| {
                    const root = try decisions.object(job.response.?);
                    const actual = root.get("model_identity") orelse return error.InvalidDecisionOutput;
                    if (actual != .string or !std.mem.eql(u8, actual.string, expected)) return error.InvalidDecisionOutput;
                }
                const normalized = try decisions.normalizeResponseWithConfig(a, job.request.questions, job.response.?, job.cfg);
                const usage = normalized.object.get("usage").?.object;
                try self.recordProvenance(job.request.decider, job.cfg, normalized);
                self.rows += 1;
                self.input_tokens +|= @intCast(usage.get("input_tokens").?.integer);
                self.output_tokens +|= @intCast(usage.get("output_tokens").?.integer);
                if (self.input_tokens > job.cfg.max_input_tokens) return error.DecisionLimitExceeded;
                value.* = normalized;
            }
            begin = end;
        }
        return out;
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
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Run.run, .{ &runtime, alloc, questions, &failure });
    try server.handleOne();
    try group.await(io);
    if (failure) |err| return err;
    try std.testing.expectEqual(@as(u64, expected_body.len), runtime.estimated_input_tokens);
    var budgeted: Runtime = .{ .registry = &registry, .http = &client, .io = io };
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
    try std.testing.expectError(error.DecisionLimitExceeded, budgeted.provider().evaluateBatch(alloc, &.{.{ .input = .{ .text = "refund" }, .decider = "budgeted-openai", .questions = questions }}));
    for ([_]anyerror{ error.DecisionRefused, error.DecisionRateLimited, error.DecisionUnauthorized, error.DecisionUpstreamFailure, error.InvalidDecisionOutput }) |expected| {
        var failing: Runtime = .{ .registry = &registry, .http = &client, .io = io };
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
