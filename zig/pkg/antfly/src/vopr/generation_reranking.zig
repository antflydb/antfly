// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Generation-chain and reranking boundary histories on one borrowed VoprIo.
//! Retry backoff uses the production antfly-independent chain executor. One
//! composed history crosses the production local and remote HTTP generation
//! and reranking adapters for fallback, replacement, malformed/truncated
//! responses, deadlines, cancellation, routing, and validation.

const std = @import("std");
const httpx = @import("httpx");
const generating = @import("antfly_generating");
const vopr = @import("vopr");
const generating_runtime = @import("../generating/mod.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const reranking = @import("../reranking/mod.zig");

const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

const valid_remote_generation =
    "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"remote-result\"}}]}";
const valid_remote_replacement_generation =
    "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"remote-before-replacement\"}}]}";
const valid_remote_reranking =
    "{\"object\":\"list\",\"data\":[{\"object\":\"rerank.score\",\"index\":0,\"score\":0.2},{\"object\":\"rerank.score\",\"index\":1,\"score\":0.8}],\"model\":\"vopr-reranker\",\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":0,\"total_tokens\":2}}";

const remote_fallback_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/openai/chat/completions", .respond = .{ .status = 429, .body = "{\"error\":\"rate limit\"}" } },
    .{ .method = .POST, .path = "/antfly/generate", .respond = .{ .body = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"remote-fallback\"}}]}" } },
};
const remote_generation_malformed_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/generate", .respond = .{ .body = "{not-json" } },
};
const remote_generation_timeout_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/generate", .respond = .{ .body = valid_remote_generation, .delay_ns = 50 * std.time.ns_per_ms } },
};
const remote_generation_cancel_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/generate", .respond = .{ .body = valid_remote_generation, .delay_ns = 50 * std.time.ns_per_ms } },
};
const remote_generation_replacement_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/generate", .respond = .{ .body = valid_remote_replacement_generation, .delay_ns = 20 * std.time.ns_per_ms } },
};
const remote_reranking_replacement_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/rerank", .respond = .{ .body = valid_remote_reranking, .delay_ns = 20 * std.time.ns_per_ms } },
};
const remote_reranking_truncated_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/rerank", .respond = .{ .body = valid_remote_reranking, .truncate_body_at = 32 } },
};
const remote_reranking_timeout_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/rerank", .respond = .{ .body = valid_remote_reranking, .delay_ns = 50 * std.time.ns_per_ms } },
};
const remote_reranking_cancel_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/rerank", .respond = .{ .body = valid_remote_reranking, .delay_ns = 50 * std.time.ns_per_ms } },
};

pub const Scenario = struct {
    pub const name: []const u8 = "generation-reranking-chain";
    pub const version: u32 = 2;

    const generation_id = vopr.id.stable(name, "generation-chain-sound");
    const retry_id = vopr.id.stable(name, "retry-uses-borrowed-io");
    const reranking_id = vopr.id.stable(name, "reranking-response-validated");
    const error_id = vopr.id.stable(name, "provider-errors-propagated");
    const remote_http_id = vopr.id.stable(name, "remote-http-adapters-exercised");
    const replacement_id = vopr.id.stable(name, "provider-replacement-preserves-in-flight-call");
    const routing_id = vopr.id.stable(name, "local-remote-routing-sound");
    const cancellation_id = vopr.id.stable(name, "remote-cancellation-propagates");
    const cleanup_id = vopr.id.stable(name, "resources-cleaned");
    const complete_id = vopr.id.stable(name, "history-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = generation_id, .name = name ++ ".generation-chain-sound", .kind = .always },
        .{ .id = retry_id, .name = name ++ ".retry-uses-borrowed-io", .kind = .always },
        .{ .id = reranking_id, .name = name ++ ".reranking-response-validated", .kind = .always },
        .{ .id = error_id, .name = name ++ ".provider-errors-propagated", .kind = .always },
        .{ .id = remote_http_id, .name = name ++ ".remote-http-adapters-exercised", .kind = .always },
        .{ .id = replacement_id, .name = name ++ ".provider-replacement-preserves-in-flight-call", .kind = .always },
        .{ .id = routing_id, .name = name ++ ".local-remote-routing-sound", .kind = .always },
        .{ .id = cancellation_id, .name = name ++ ".remote-cancellation-propagates", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".resources-cleaned", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".history-completes", .kind = .reachable },
    };

    const Mode = enum {
        generation_success,
        generation_retry,
        generation_timeout_fallback,
        generation_rate_limit_fallback,
        generation_cancel,
        reranking_success,
        reranking_malformed_count,
        reranking_malformed_nan,
        reranking_timeout,
        reranking_cancel,
        remote_composed,
    };

    const mode_ids = ids: {
        var values: [@typeInfo(Mode).@"enum".fields.len]vopr.id.StableId = undefined;
        for (std.meta.tags(Mode), 0..) |mode, index|
            values[index] = vopr.id.stable(name, @tagName(mode));
        break :ids values;
    };
    const mode_names = names: {
        var values: [mode_ids.len][]const u8 = undefined;
        for (std.meta.tags(Mode), 0..) |mode, index|
            values[index] = name ++ "." ++ @tagName(mode);
        break :names values;
    };

    const State = struct {
        owner_allocator: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        http: httpx.Client,
        mode: ?Mode = null,
        attempts: u32 = 0,
        fallback_used: bool = false,
        retry_wait_observed: bool = false,
        remote_http_sound: bool = true,
        replacement_sound: bool = true,
        routing_sound: bool = true,
        cancellation_sound: bool = true,
        remote_requests: u32 = 0,
        local_generation_calls: u32 = 0,
        local_reranking_calls: u32 = 0,
        generation_sound: bool = true,
        reranking_sound: bool = true,
        error_sound: bool = true,
        complete: bool = false,
        task_error: ?anyerror = null,

        fn init(owner_allocator: std.mem.Allocator) !*State {
            const self = try owner_allocator.create(State);
            errdefer owner_allocator.destroy(self);
            self.* = .{
                .owner_allocator = owner_allocator,
                .fixture_allocator = .init,
                .allocator = undefined,
                .sim = undefined,
                .http = undefined,
            };
            errdefer _ = self.fixture_allocator.deinit();
            self.allocator = self.fixture_allocator.allocator();
            self.sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x4745_4e52,
                .tasks = .{ .stack_size = 32 * 1024 * 1024 },
                .network = .{ .max_sockets = 32 },
                .required = .of(&.{ .clock_read, .sockets, .task_scheduling, .synchronization, .sleep }),
                .instrumentation = .{ .enabled = false, .map_digest = 0x4745_4e52 },
            });
            errdefer self.sim.deinit();
            self.http = httpx.Client.initWithConfig(self.allocator, self.sim.io(), .{ .keep_alive = false });
            return self;
        }

        fn deinit(self: *State) void {
            self.http.deinit();
            self.sim.deinit();
            const owner_allocator = self.owner_allocator;
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            owner_allocator.destroy(self);
        }

        fn factory(self: *State) generating.GeneratorFactory {
            return .{ .ptr = self, .vtable = &.{ .create = createGenerator } };
        }

        fn createGenerator(ptr: *anyopaque, _: std.mem.Allocator, _: generating.GeneratorConfig) !generating.Generator {
            return .{ .ptr = ptr, .vtable = &.{ .generate = generate } };
        }

        fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, _: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *State = @ptrCast(@alignCast(ptr));
            self.attempts += 1;
            const primary = std.mem.eql(u8, model, "primary");
            switch (self.mode.?) {
                .generation_retry => if (self.attempts == 1) return error.Timeout,
                .generation_timeout_fallback => if (primary) return error.Timeout,
                .generation_rate_limit_fallback => if (primary) return error.RateLimit,
                .generation_cancel => return error.Canceled,
                else => {},
            }
            if (!primary) self.fallback_used = true;
            return .{
                .content = try alloc.dupe(u8, if (primary) "primary-result" else "fallback-result"),
                .allocator = alloc,
            };
        }

        fn dense(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return alloc.alloc([]f32, 0);
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return alloc.alloc(db_embedder.SparseEmbedding, 0);
        }

        fn generateText(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            model: []const u8,
            roles: []const []const u8,
            contents: []const []const u8,
        ) ![]u8 {
            const self: *State = @ptrCast(@alignCast(ptr));
            self.local_generation_calls += 1;
            if (!std.mem.eql(u8, model, "local-replacement") or roles.len != 1 or contents.len != 1 or
                !std.mem.eql(u8, roles[0], "user") or !std.mem.eql(u8, contents[0], "query"))
                return error.InvalidLocalGenerationRoute;
            return try alloc.dupe(u8, "local-after-replacement");
        }

        fn rerank(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8, _: []const []const u8) ![]f32 {
            const self: *State = @ptrCast(@alignCast(ptr));
            return switch (self.mode.?) {
                .reranking_malformed_count => try alloc.dupe(f32, &.{0.5}),
                .reranking_malformed_nan => try alloc.dupe(f32, &.{ std.math.nan(f32), 0.5 }),
                .reranking_timeout => error.Timeout,
                .reranking_cancel => error.Canceled,
                .remote_composed => blk: {
                    self.local_reranking_calls += 1;
                    break :blk try alloc.dupe(f32, &.{ 0.3, 0.7 });
                },
                else => try alloc.dupe(f32, &.{ 0.2, 0.8 }),
            };
        }

        fn localProvider(self: *State) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .embed_dense_texts = dense,
                .embed_sparse_texts = sparse,
                .rerank_texts = rerank,
                .generate_text = generateText,
            };
        }

        fn clientConfig(request_timeout_ms: u64) httpx.ClientConfig {
            return .{
                .keep_alive = false,
                .retry_policy = .noRetry(),
                .timeouts = .{
                    .connect_ms = 0,
                    .read_ms = 0,
                    .write_ms = 0,
                    .keep_alive_ms = 0,
                    .idle_ms = 0,
                    .request_ms = request_timeout_ms,
                },
            };
        }

        const ServerCall = struct {
            server: *httpx.TestServer,
            requests: usize,
            error_value: ?anyerror = null,

            fn run(self: *@This()) void {
                for (0..self.requests) |_| self.server.handleOne() catch |err| {
                    self.error_value = err;
                    return;
                };
            }
        };

        const GenerationCall = struct {
            state: *State,
            client: *httpx.Client,
            chain: []const generating.ChainLink,
            local_provider: ?managed_embedder.AntflyProvider = null,
            content: ?[]u8 = null,

            fn run(self: *@This()) anyerror!void {
                var result = try generating_runtime.executeChainWithAntflyProvider(
                    self.state.allocator,
                    self.client,
                    self.chain,
                    self.local_provider,
                    &.{.{ .role = .user, .content = .{ .text = "query" } }},
                );
                defer result.deinit();
                self.content = try self.state.allocator.dupe(u8, result.content);
            }

            fn deinit(self: *@This()) void {
                if (self.content) |value| self.state.allocator.free(value);
                self.content = null;
            }
        };

        const RerankingCall = struct {
            state: *State,
            client: *httpx.Client,
            config: reranking.Config,
            local_provider: ?managed_embedder.AntflyProvider = null,
            scores: ?[]f32 = null,

            fn run(self: *@This()) anyerror!void {
                self.scores = try reranking.rerankDocumentsWithAntflyProvider(
                    self.state.allocator,
                    self.client,
                    self.config,
                    self.local_provider,
                    "query",
                    &.{ "doc-a", "doc-b" },
                );
            }

            fn deinit(self: *@This()) void {
                if (self.scores) |value| self.state.allocator.free(value);
                self.scores = null;
            }
        };

        fn isCancellation(err: anyerror) bool {
            return err == error.Canceled or err == error.Cancelled;
        }

        const ErrorClass = enum {
            success,
            timeout,
            canceled,
            other_error,
        };

        fn classifyError(value: ?anyerror) ErrorClass {
            const err = value orelse return .success;
            if (err == error.Timeout) return .timeout;
            if (isCancellation(err)) return .canceled;
            return .other_error;
        }

        fn awaitRoute(self: *State, server: *const httpx.TestServer, route_index: usize) !void {
            while (server.routeHitCount(route_index) == 0) try self.sim.io().sleep(.fromNanoseconds(1), .awake);
        }

        fn runGeneration(self: *State) !void {
            const primary = generating.ChainLink{
                .generator = generating.GeneratorConfig.fromOpenAI(.{ .model = "primary" }),
                .condition = switch (self.mode.?) {
                    .generation_timeout_fallback => .on_timeout,
                    .generation_rate_limit_fallback => .on_rate_limit,
                    else => .on_error,
                },
                .retry = if (self.mode.? == .generation_retry)
                    .{ .max_attempts = 2, .initial_backoff_ms = 1, .max_backoff_ms = 1 }
                else
                    null,
            };
            const fallback = generating.ChainLink{
                .generator = generating.GeneratorConfig.fromAntfly(.{ .model = "fallback" }),
            };
            const chain = if (self.mode.? == .generation_timeout_fallback or self.mode.? == .generation_rate_limit_fallback)
                &[_]generating.ChainLink{ primary, fallback }
            else
                &[_]generating.ChainLink{primary};
            var result = generating.executeChainWithIo(
                self.allocator,
                self.sim.io(),
                chain,
                self.factory(),
                &.{.{ .role = .user, .content = .{ .text = "query" } }},
            ) catch |err| {
                self.error_sound = self.mode.? == .generation_cancel and err == error.Canceled;
                if (!self.error_sound) return err;
                return;
            };
            defer result.deinit();
            self.retry_wait_observed = self.mode.? != .generation_retry or
                std.Io.Clock.awake.now(self.sim.io()).toNanoseconds() >= std.time.ns_per_ms;
            self.generation_sound = if (self.mode.? == .generation_timeout_fallback or self.mode.? == .generation_rate_limit_fallback)
                self.fallback_used and std.mem.eql(u8, result.content, "fallback-result")
            else
                !self.fallback_used and std.mem.eql(u8, result.content, "primary-result") and
                    (self.mode.? != .generation_retry or self.attempts == 2);
        }

        fn runReranking(self: *State) !void {
            const local = managed_embedder.AntflyProvider{
                .ptr = self,
                .embed_dense_texts = dense,
                .embed_sparse_texts = sparse,
                .rerank_texts = rerank,
            };
            const scores = reranking.rerankDocumentsWithAntflyProvider(
                self.allocator,
                &self.http,
                .{ .provider = .antfly, .field = "body" },
                local,
                "query",
                &.{ "doc-a", "doc-b" },
            ) catch |err| {
                self.reranking_sound = switch (self.mode.?) {
                    .reranking_malformed_count, .reranking_malformed_nan => err == error.InvalidRerankerResponse,
                    .reranking_timeout => err == error.Timeout,
                    .reranking_cancel => err == error.Canceled,
                    else => false,
                };
                self.error_sound = self.reranking_sound;
                return;
            };
            defer self.allocator.free(scores);
            self.reranking_sound = self.mode.? == .reranking_success and scores.len == 2 and scores[1] > scores[0];
        }

        fn runRemoteFallback(self: *State) !bool {
            var server = try httpx.TestServer.start(self.allocator, self.sim.io(), &remote_fallback_routes);
            defer server.deinit();
            var client = httpx.Client.initWithConfig(self.allocator, self.sim.io(), clientConfig(0));
            defer client.deinit();
            const openai_url = try std.fmt.allocPrint(self.allocator, "{s}/openai", .{server.baseUrl()});
            defer self.allocator.free(openai_url);
            const antfly_url = try std.fmt.allocPrint(self.allocator, "{s}/antfly", .{server.baseUrl()});
            defer self.allocator.free(antfly_url);
            const chain = [_]generating.ChainLink{
                .{
                    .generator = generating.GeneratorConfig.fromOpenAI(.{ .model = "remote-primary", .url = openai_url }),
                    .condition = .on_error,
                },
                .{ .generator = generating.GeneratorConfig.fromAntfly(.{ .model = "remote-fallback", .url = antfly_url }) },
            };
            var call = GenerationCall{ .state = self, .client = &client, .chain = &chain };
            defer call.deinit();
            var server_call = ServerCall{ .server = &server, .requests = 2 };
            var request_future = self.sim.io().async(GenerationCall.run, .{&call});
            var server_future = self.sim.io().async(ServerCall.run, .{&server_call});
            request_future.await(self.sim.io()) catch return false;
            server_future.await(self.sim.io());
            self.remote_requests += @intCast(server.routeHitCount(0) + server.routeHitCount(1));
            return server_call.error_value == null and
                std.mem.eql(u8, call.content orelse "", "remote-fallback") and
                server.routeHitCount(0) == 1 and server.routeHitCount(1) == 1;
        }

        fn runRemoteGenerationSingle(
            self: *State,
            routes: []const httpx.TestRoute,
            request_timeout_ms: u64,
            cancel_after_hit: bool,
        ) !ErrorClass {
            var server = try httpx.TestServer.start(self.allocator, self.sim.io(), routes);
            defer server.deinit();
            var client = httpx.Client.initWithConfig(self.allocator, self.sim.io(), clientConfig(request_timeout_ms));
            defer client.deinit();
            const chain = [_]generating.ChainLink{.{
                .generator = generating.GeneratorConfig.fromAntfly(.{ .model = "remote", .url = server.baseUrl() }),
            }};
            var call = GenerationCall{ .state = self, .client = &client, .chain = &chain };
            defer call.deinit();
            var server_call = ServerCall{ .server = &server, .requests = 1 };
            var request_future = self.sim.io().async(GenerationCall.run, .{&call});
            var server_future = self.sim.io().async(ServerCall.run, .{&server_call});
            const request_error: ?anyerror = if (cancel_after_hit) blk: {
                try self.awaitRoute(&server, 0);
                request_future.cancel(self.sim.io()) catch |err| break :blk err;
                break :blk null;
            } else blk: {
                request_future.await(self.sim.io()) catch |err| break :blk err;
                break :blk null;
            };
            server_future.await(self.sim.io());
            self.remote_requests += @intCast(server.routeHitCount(0));
            if (server.routeHitCount(0) != 1) return error.RemoteGenerationDidNotComplete;
            return classifyError(request_error);
        }

        fn runGenerationReplacement(self: *State) !bool {
            var server = try httpx.TestServer.start(self.allocator, self.sim.io(), &remote_generation_replacement_routes);
            defer server.deinit();
            var client = httpx.Client.initWithConfig(self.allocator, self.sim.io(), clientConfig(0));
            defer client.deinit();
            var chain = [_]generating.ChainLink{.{
                .generator = generating.GeneratorConfig.fromAntfly(.{ .model = "remote-before-replacement", .url = server.baseUrl() }),
            }};
            var remote_call = GenerationCall{ .state = self, .client = &client, .chain = &chain };
            defer remote_call.deinit();
            var server_call = ServerCall{ .server = &server, .requests = 1 };
            var remote_future = self.sim.io().async(GenerationCall.run, .{&remote_call});
            var server_future = self.sim.io().async(ServerCall.run, .{&server_call});
            try self.awaitRoute(&server, 0);

            // Replacement is request-scoped: the already-created production
            // backend retains its remote provider while the next request sees
            // the new embedded-provider route through the same factory path.
            chain[0] = .{ .generator = generating.GeneratorConfig.fromAntfly(.{ .model = "local-replacement" }) };
            var local_result = try generating_runtime.executeChainWithAntflyProvider(
                self.allocator,
                &client,
                &chain,
                self.localProvider(),
                &.{.{ .role = .user, .content = .{ .text = "query" } }},
            );
            defer local_result.deinit();
            remote_future.await(self.sim.io()) catch return false;
            server_future.await(self.sim.io());
            self.remote_requests += @intCast(server.routeHitCount(0));
            return server_call.error_value == null and
                std.mem.eql(u8, remote_call.content orelse "", "remote-before-replacement") and
                std.mem.eql(u8, local_result.content, "local-after-replacement") and
                self.local_generation_calls == 1 and server.routeHitCount(0) == 1;
        }

        fn runRemoteRerankingSingle(
            self: *State,
            routes: []const httpx.TestRoute,
            request_timeout_ms: u64,
            cancel_after_hit: bool,
        ) !RerankingCallResult {
            var server = try httpx.TestServer.start(self.allocator, self.sim.io(), routes);
            defer server.deinit();
            var client = httpx.Client.initWithConfig(self.allocator, self.sim.io(), clientConfig(request_timeout_ms));
            defer client.deinit();
            var call = RerankingCall{
                .state = self,
                .client = &client,
                .config = .{ .provider = .antfly, .model = "vopr-reranker", .url = server.baseUrl(), .field = "body" },
            };
            defer call.deinit();
            var server_call = ServerCall{ .server = &server, .requests = 1 };
            var request_future = self.sim.io().async(RerankingCall.run, .{&call});
            var server_future = self.sim.io().async(ServerCall.run, .{&server_call});
            const request_error: ?anyerror = if (cancel_after_hit) blk: {
                try self.awaitRoute(&server, 0);
                request_future.cancel(self.sim.io()) catch |err| break :blk err;
                break :blk null;
            } else blk: {
                request_future.await(self.sim.io()) catch |err| break :blk err;
                break :blk null;
            };
            server_future.await(self.sim.io());
            self.remote_requests += @intCast(server.routeHitCount(0));
            if (server.routeHitCount(0) != 1) return error.RemoteRerankingDidNotComplete;
            const scores_sound = if (call.scores) |scores|
                scores.len == 2 and scores[0] == 0.2 and scores[1] == 0.8
            else
                false;
            return .{ .scores_sound = scores_sound, .error_class = classifyError(request_error) };
        }

        const RerankingCallResult = struct {
            scores_sound: bool,
            error_class: ErrorClass,
        };

        fn runRerankingReplacement(self: *State) !bool {
            var server = try httpx.TestServer.start(self.allocator, self.sim.io(), &remote_reranking_replacement_routes);
            defer server.deinit();
            var client = httpx.Client.initWithConfig(self.allocator, self.sim.io(), clientConfig(0));
            defer client.deinit();
            var next_config = reranking.Config{
                .provider = .antfly,
                .model = "vopr-reranker",
                .url = server.baseUrl(),
                .field = "body",
            };
            var remote_call = RerankingCall{
                .state = self,
                .client = &client,
                // Capture the current request configuration by value. The
                // subsequent replacement must not reroute this in-flight call.
                .config = next_config,
            };
            defer remote_call.deinit();
            var server_call = ServerCall{ .server = &server, .requests = 1 };
            var remote_future = self.sim.io().async(RerankingCall.run, .{&remote_call});
            var server_future = self.sim.io().async(ServerCall.run, .{&server_call});
            try self.awaitRoute(&server, 0);

            next_config.model = "";
            next_config.url = "";
            const local_scores = try reranking.rerankDocumentsWithAntflyProvider(
                self.allocator,
                &self.http,
                next_config,
                self.localProvider(),
                "query",
                &.{ "doc-a", "doc-b" },
            );
            defer self.allocator.free(local_scores);
            remote_future.await(self.sim.io()) catch return false;
            server_future.await(self.sim.io());
            self.remote_requests += @intCast(server.routeHitCount(0));

            const remote_scores_sound = if (remote_call.scores) |scores|
                scores.len == 2 and scores[0] == 0.2 and scores[1] == 0.8
            else
                false;
            return server_call.error_value == null and remote_scores_sound and
                local_scores.len == 2 and local_scores[0] == 0.3 and local_scores[1] == 0.7 and
                self.local_reranking_calls == 1 and server.routeHitCount(0) == 1;
        }

        fn runRemoteComposed(self: *State) !void {
            const fallback_sound = try self.runRemoteFallback();
            const malformed_error = try self.runRemoteGenerationSingle(&remote_generation_malformed_routes, 0, false);
            const timeout_error = try self.runRemoteGenerationSingle(&remote_generation_timeout_routes, 10, false);
            const cancellation_error = try self.runRemoteGenerationSingle(&remote_generation_cancel_routes, 0, true);
            const replacement_sound = try self.runGenerationReplacement();

            const reranking_replacement_sound = try self.runRerankingReplacement();
            const truncated_reranking = try self.runRemoteRerankingSingle(&remote_reranking_truncated_routes, 0, false);
            const timeout_reranking = try self.runRemoteRerankingSingle(&remote_reranking_timeout_routes, 10, false);
            const cancellation_reranking = try self.runRemoteRerankingSingle(&remote_reranking_cancel_routes, 0, true);

            const generation_errors_sound = malformed_error == .other_error and timeout_error == .timeout and
                cancellation_error == .canceled;
            const reranking_errors_sound = truncated_reranking.error_class == .other_error and
                timeout_reranking.error_class == .timeout and cancellation_reranking.error_class == .canceled;
            self.generation_sound = fallback_sound and generation_errors_sound and replacement_sound;
            self.reranking_sound = reranking_replacement_sound and reranking_errors_sound;
            self.error_sound = generation_errors_sound and reranking_errors_sound;
            self.remote_http_sound = self.remote_requests == 10;
            self.replacement_sound = replacement_sound and reranking_replacement_sound;
            self.routing_sound = self.replacement_sound and self.local_generation_calls == 1 and self.local_reranking_calls == 1;
            self.cancellation_sound = cancellation_error == .canceled and cancellation_reranking.error_class == .canceled;
        }

        fn runTask(self: *State) void {
            const mode = self.mode.?;
            const result = switch (mode) {
                .generation_success,
                .generation_retry,
                .generation_timeout_fallback,
                .generation_rate_limit_fallback,
                .generation_cancel,
                => self.runGeneration(),
                .remote_composed => self.runRemoteComposed(),
                else => self.runReranking(),
            };
            result catch |err| {
                self.task_error = err;
            };
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        return .{ .state = try State.init(allocator) };
    }

    pub fn deinit(world: *World, _: std.mem.Allocator) void {
        world.state.deinit();
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        const state = world.state;
        if (state.mode == null) {
            inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = switch (mode) {
                    .generation_success, .generation_retry, .reranking_success => .workload,
                    else => .fault,
                },
            });
            return;
        }
        if (!state.sim.scheduler().quiescent()) try state.sim.scheduler().enumerateReady(list, allocator);
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.mode == null) {
            var found = false;
            inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
                state.mode = mode;
                _ = state.sim.io().async(State.runTask, .{state});
                found = true;
            };
            if (!found) return error.InvalidGenerationRerankingMode;
        } else {
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
        }
        try events.emitNamed(allocator, .domain, selected.name, state.attempts);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".attempts", @intCast(state.attempts));
        try builder.addNamed(allocator, name ++ ".remote-requests", state.remote_requests);
        try builder.addNamed(allocator, name ++ ".local-generation-calls", state.local_generation_calls);
        try builder.addNamed(allocator, name ++ ".local-reranking-calls", state.local_reranking_calls);
        try builder.addNamed(allocator, name ++ ".fallback-used", @intFromBool(state.fallback_used));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, generation_id, state.generation_sound);
        try sink.check(allocator, retry_id, state.mode == null or state.mode.? != .generation_retry or
            !state.complete or state.retry_wait_observed);
        try sink.check(allocator, reranking_id, state.reranking_sound);
        try sink.check(allocator, error_id, state.error_sound and state.task_error == null);
        try sink.check(allocator, remote_http_id, state.remote_http_sound);
        try sink.check(allocator, replacement_id, state.replacement_sound);
        try sink.check(allocator, routing_id, state.routing_sound);
        try sink.check(allocator, cancellation_id, state.cancellation_sound);
        try sink.check(allocator, cleanup_id, !state.complete or state.sim.resourceSnapshot().active_tasks == 0);
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        return state.sim.healthSnapshot(.{
            .progress_expected = state.mode != null,
            .progress_units = @intFromBool(state.complete),
            .consistency_valid = state.generation_sound and state.reranking_sound and state.error_sound and
                state.remote_http_sound and state.replacement_sound and state.routing_sound and state.cancellation_sound and
                state.task_error == null,
            .cleanup_complete = state.complete and state.sim.resourceSnapshot().active_tasks == 0,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "generation and reranking chain VOPR exact replays local and remote production boundaries" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, ordinal| {
        errdefer std.debug.print("generation/reranking VOPR failed mode={s}\n", .{Scenario.mode_names[ordinal]});
        var choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0x4745_4e52 + ordinal);
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{
            .system = "antfly",
            .transition_budget = if (std.meta.tags(Scenario.Mode)[ordinal] == .remote_composed) 4_096 else 128,
            .backend_ids = &backend_ids,
            .source_revision = "generation-reranking-vopr-v2",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        if (recorded.summary.?.property_failures != 0) {
            for (recorded.failures.items) |failure| std.debug.print(
                "generation/reranking mode={s} failure={s} class={s}\n",
                .{ Scenario.mode_names[ordinal], failure.identity, @tagName(failure.class) },
            );
        }
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
        replayed.deinit();
    }
}
