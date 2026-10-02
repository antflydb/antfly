// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Request-owned provider execution and budgets; no retries or fallback.
const std = @import("std");
const httpx = @import("httpx");
const decisions = @import("decisions.zig");
const registry_mod = @import("../common/provider_registry.zig");
const secrets = @import("../common/secrets.zig");
const execution = @import("../inference/execution_context.zig");
const quotas = @import("../common/provider_limits.zig");
const admission_mod = @import("../common/request_admission.zig");
const managed = @import("../inference/managed_embedder.zig");
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
        => err,
        error.ProviderTokenBudgetExceeded => error.DecisionLimitExceeded,
        error.InvalidRateLimitPolicy, error.ConflictingRateLimitPolicy => error.InvalidDeciderConfig,
        error.SecretNotFound => error.MissingDecisionApiKey,
        else => error.DecisionUpstreamFailure,
    };
}
fn parseResponse(a: std.mem.Allocator, bytes: []const u8) !decisions.Json {
    if (bytes.len > 1024 * 1024) return error.DecisionLimitExceeded;
    return std.json.parseFromSliceLeaky(decisions.Json, a, bytes, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidDecisionOutput;
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
    source_table: []const u8 = "",
    rows: u64 = 0,
    input_tokens: u64 = 0,
    estimated_input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    batches: u64 = 0,
    latency_ns: u64 = 0,
    profile_allocator: ?std.mem.Allocator = null,
    provenance: std.ArrayList(struct { decider: []const u8, provider: decisions.Provider, model: []const u8, rows: u64 = 0 }) = .empty,
    pub fn provider(self: *@This()) decisions.DecisionProvider {
        return .{ .ptr = self, .validate_fn = validate, .evaluate_batch_fn = evaluate, .checkpoint_fn = checkpoint };
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
        try decisions.validateQuestions(questions, decisions.capabilities(cfg.provider));
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
            const body = try std.json.Stringify.valueAlloc(a, .{ .model = cfg.modelName(), .state = self.request.input, .questions = self.request.questions }, .{});
            const base = if (cfg.provider == .antfly and cfg.url.len == 0) self.runtime.antfly_url orelse cfg.baseUrl() else cfg.baseUrl();
            const url = try std.fmt.allocPrint(a, "{s}{s}", .{ std.mem.trimEnd(u8, base, "/"), if (cfg.provider == .jev) "/v1/systemone" else "/decide" });
            var key_source = try secrets.SecretValue.initConfigOrEnv(a, cfg.api_key, if (cfg.provider == .jev) "TYPESAFE_API_KEY" else "ANTFLY_INFERENCE_API_KEY");
            defer key_source.deinit(a);
            var quota = try self.runtime.limits.acquire(.{ .operation = .decision, .endpoint = .{
                .provider = if (cfg.provider == .jev) .jev else .antfly,
                .endpoint = base,
                .model = cfg.modelName(),
                .credentials = @import("../common/credential_source_identity.zig").fromSecretValue(key_source),
            } }, try quotas.Policy.fromConfig(cfg.rate_limit));
            defer quota.release();
            const observer = quota.limiter().observer(0);
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
                return parseResponse(a, bytes);
            };
            const key = key_source.resolveOwned(a, self.runtime.secret_store) catch |err| return operationalError(err);
            if (cfg.provider == .jev and key == null) return error.MissingDecisionApiKey;
            var headers: [2][2][]const u8 = undefined;
            var count: usize = 0;
            if (key) |value| {
                headers[count] = .{ "Authorization", try std.fmt.allocPrint(a, "Bearer {s}", .{value}) };
                count += 1;
            }
            if (cfg.provider == .antfly and self.runtime.source_table.len > 0) {
                headers[count] = .{ execution.source_table_header, self.runtime.source_table };
                count += 1;
            }
            const ctx = self.runtime.context;
            var response = self.runtime.http.post(url, .{
                .json = body,
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
            return parseResponse(a, bytes);
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
            if (request.input.len > decisions.capabilities(cfg.provider).max_input_bytes) return error.DecisionLimitExceeded;
            // Reserve conservatively before any provider I/O. The same byte-based
            // token estimate is used by the shared provider quota implementation.
            const body = try std.json.Stringify.valueAlloc(a, .{ .model = cfg.modelName(), .state = request.input, .questions = request.questions }, .{});
            defer a.free(body);
            estimated +|= body.len;
            if (estimated > cfg.max_input_tokens) return error.DecisionLimitExceeded;
            batch_size = @min(batch_size, cfg.batch_size);
        }
        self.estimated_input_tokens = estimated;
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
                try group.concurrent(self.io, Job.run, .{job});
            }
            try group.await(self.io);
            self.batches += 1;
            for (jobs, out[begin..end]) |*job, *value| {
                if (job.failure) |err| return err;
                const normalized = try decisions.normalizeResponse(a, job.request.questions, job.response.?);
                const usage = normalized.object.get("usage").?.object;
                if (self.profile_allocator) |stats_a| {
                    const model = normalized.object.get("model").?.string;
                    var found = false;
                    for (self.provenance.items) |*item| if (std.mem.eql(u8, item.decider, job.request.decider) and std.mem.eql(u8, item.model, model)) {
                        item.rows += 1;
                        found = true;
                        break;
                    };
                    if (!found) try self.provenance.append(stats_a, .{ .decider = try stats_a.dupe(u8, job.request.decider), .provider = job.cfg.provider, .model = try stats_a.dupe(u8, model), .rows = 1 });
                }
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

test "decision functions Antfly and Jev HTTP adapters preserve payload credentials and usage" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer decision-test", req.header("Authorization") orelse return error.TestUnexpectedResult);
            const parsed = try std.json.parseFromSlice(decisions.Json, std.testing.allocator, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("refund", parsed.value.object.get("state").?.string);
            try std.testing.expectEqualStrings("test-model", parsed.value.object.get("model").?.string);
            try decisions.validateQuestions(parsed.value.object.get("questions").?, decisions.capabilities(.jev));
        }
    };
    const response = "{\"model\":\"test-model\",\"answers\":{\"answer\":{\"type\":\"noul\",\"noul\":0.9}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}";
    var server = try httpx.TestServer.start(a, io, &.{
        .{ .method = .POST, .path = "/decide", .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = response } },
        .{ .method = .POST, .path = "/v1/systemone", .max_uses = 1, .assert_request = Check.request, .respond = .{ .body = response } },
        .{ .method = .POST, .path = "/decide", .respond = .{ .body = "{\"model\":\"test-model\",\"answers\":{},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}" } },
        .{ .method = .POST, .path = "/v1/systemone", .respond = .{ .status = 429, .body = "rate limited" } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer client.deinit();
    var registry = registry_mod.Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("local", .{ .provider = .antfly, .model = "test-model", .url = server.baseUrl(), .api_key = "decision-test", .max_rows = 2 });
    try registry.registerDeciderConfig("remote", .{ .provider = .jev, .model = "test-model", .url = server.baseUrl(), .api_key = "decision-test", .max_rows = 2 });
    try registry.registerDeciderConfig("budgeted", .{ .provider = .antfly, .model = "test-model", .url = server.baseUrl(), .max_input_tokens = 1 });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const questions = try decisions.questionsFor(arena.allocator(), .ai_probability, &.{ .{ .string = "refund" }, .{ .string = "Refund?" }, .{ .string = "local" } });
    var runtime: Runtime = .{ .registry = &registry, .http = &client, .io = io };
    const Run = struct {
        fn run(r: *Runtime, alloc: std.mem.Allocator, q: decisions.Json, failure: *?anyerror) void {
            const results = r.provider().evaluateBatch(alloc, &.{ .{ .input = "refund", .decider = "local", .questions = q }, .{ .input = "refund", .decider = "remote", .questions = q } }) catch |err| {
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
    try std.testing.expectError(error.DecisionLimitExceeded, budgeted.provider().evaluateBatch(arena.allocator(), &.{.{ .input = "refund", .decider = "budgeted", .questions = questions }}));
    try std.testing.expectEqual(@as(u64, 0), budgeted.rows);
    const Once = struct {
        fn run(r: *Runtime, alloc: std.mem.Allocator, name: []const u8, q: decisions.Json, err: *?anyerror) void {
            _ = r.provider().evaluateBatch(alloc, &.{.{ .input = "refund", .decider = name, .questions = q }}) catch |failure_err| {
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
    try std.testing.expectError(error.DecisionLimitExceeded, runtime.provider().evaluateBatch(arena.allocator(), &.{.{ .input = "refund", .decider = "local", .questions = questions }}));
    var cancelled = std.atomic.Value(bool).init(true);
    runtime.context = .{ .io = io, .deadline_ns = null, .cancellation = @import("../common/cancellation.zig").CancellationToken.fromAtomic(&cancelled) };
    try std.testing.expectError(error.Cancelled, runtime.provider().evaluateBatch(arena.allocator(), &.{.{ .input = "refund", .decider = "local", .questions = questions }}));
}
