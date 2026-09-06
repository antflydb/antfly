// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const httpx = @import("httpx");
const lib = @import("antfly_generating");
const inference = @import("../inference/mod.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const openai_provider = @import("../inference/openai.zig");
const antfly_provider = @import("../inference/local.zig");
const vertex_provider = @import("../inference/vertex.zig");
const common_secrets = @import("../common/secrets.zig");
const provider_limits = @import("../common/provider_limits.zig");
const credential_identity = @import("../common/credential_source_identity.zig");
const provider_defaults = @import("../common/provider_defaults.zig");

pub const Role = lib.Role;
pub const ContentPart = lib.ContentPart;
pub const ChatMessageContent = lib.ChatMessageContent;
pub const ToolCall = lib.ToolCall;
pub const ChatMessage = lib.ChatMessage;
pub const GenerateResult = lib.GenerateResult;
pub const Provider = lib.Provider;
pub const OpenAIConfig = lib.OpenAIConfig;
pub const OllamaConfig = lib.OllamaConfig;
pub const AntflyConfig = lib.AntflyConfig;
pub const GeneratorConfig = lib.GeneratorConfig;
pub const RetryConfig = lib.RetryConfig;
pub const ChainCondition = lib.ChainCondition;
pub const ChainLink = lib.ChainLink;
pub const GeneratorFactory = lib.GeneratorFactory;
pub const default_max_tokens = lib.default_max_tokens;
pub const parseConfigFromSlice = lib.parseConfigFromSlice;
pub const parseConfigFromValue = lib.parseConfigFromValue;

pub const BackendFactory = struct {
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    inference_api_key: ?[]const u8 = null,
    limits: *provider_limits.Registry = &provider_limits.process_registry,

    pub fn init(alloc: std.mem.Allocator, http: *httpx.Client) BackendFactory {
        return .{ .alloc = alloc, .http = http };
    }

    pub fn initWithAntflyProvider(
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        embedded_antfly_provider: ?managed_embedder.AntflyProvider,
    ) BackendFactory {
        return initWithOptions(alloc, http, .{ .antfly_provider = embedded_antfly_provider });
    }

    pub const Options = struct {
        antfly_provider: ?managed_embedder.AntflyProvider = null,
        secret_store: ?*common_secrets.FileStore = null,
        inference_api_key: ?[]const u8 = null,
        limits: *provider_limits.Registry = &provider_limits.process_registry,
    };

    pub fn initWithOptions(
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        options: Options,
    ) BackendFactory {
        return .{
            .alloc = alloc,
            .http = http,
            .antfly_provider = options.antfly_provider,
            .secret_store = options.secret_store,
            .inference_api_key = options.inference_api_key,
            .limits = options.limits,
        };
    }

    pub fn factory(self: *BackendFactory) GeneratorFactory {
        return .{
            .ptr = self,
            .vtable = &.{ .create = create },
        };
    }

    fn create(ptr: *anyopaque, alloc: std.mem.Allocator, cfg: GeneratorConfig) !lib.Generator {
        const self: *BackendFactory = @ptrCast(@alignCast(ptr));
        return try BackendState.init(alloc, self.http, cfg, self.antfly_provider, self.secret_store, self.inference_api_key, self.limits);
    }
};

const BackendState = struct {
    quota: ?provider_limits.Handle = null,
    limits: *provider_limits.Registry = &provider_limits.process_registry,
    alloc: std.mem.Allocator,
    cfg: GeneratorConfig,
    api_key: ?common_secrets.SecretValue = null,
    auth_header_cache: common_secrets.BearerAuthHeaderCache = .{},
    secret_store: ?*common_secrets.FileStore = null,
    provider: union(enum) {
        openai: openai_provider.Provider,
        remote_antfly: antfly_provider.Provider,
        embedded_antfly: managed_embedder.AntflyProvider,
        vertex: vertex_provider.Provider,
        gemini: vertex_provider.GeminiProvider,
    },

    fn init(
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        cfg: GeneratorConfig,
        embedded_antfly_provider: ?managed_embedder.AntflyProvider,
        secret_store: ?*common_secrets.FileStore,
        inference_api_key: ?[]const u8,
        limits: *provider_limits.Registry,
    ) !lib.Generator {
        const state = try alloc.create(BackendState);
        errdefer alloc.destroy(state);

        state.limits = limits;
        _ = try provider_limits.Policy.fromConfig(cfg.rate_limit);
        state.alloc = alloc;
        state.cfg = cfg;
        state.api_key = switch (cfg.provider) {
            .antfly => try common_secrets.SecretValue.initConfigOrEnv(alloc, cfg.api_key orelse inference_api_key, "ANTFLY_INFERENCE_API_KEY"),
            .gemini => try common_secrets.SecretValue.initConfigOrEnv(alloc, cfg.api_key, "GEMINI_API_KEY"),
            else => try common_secrets.SecretValue.initConfig(alloc, cfg.api_key),
        };
        errdefer if (state.api_key) |*api_key| api_key.deinit(alloc);
        state.auth_header_cache = .{};
        state.secret_store = secret_store;
        state.provider = switch (cfg.provider) {
            .openai, .ollama => blk: {
                const provider = openai_provider.Provider.init(alloc, http, cfg.url);
                break :blk .{ .openai = provider };
            },
            .gemini => blk: {
                const api_key_ref = state.api_key orelse return error.MissingGeneratorCredentials;
                const api_key = (try api_key_ref.resolveOwned(alloc, secret_store)) orelse return error.MissingGeneratorCredentials;
                defer alloc.free(api_key);
                break :blk .{ .gemini = try vertex_provider.GeminiProvider.init(alloc, http, .{
                    .base_url = provider_defaults.normalizedBase(cfg.url, provider_defaults.gemini_v1beta_base),
                    .api_key = api_key,
                }) };
            },
            .vertex => blk: {
                const bearer_token = if (state.api_key) |*api_key_ref| try api_key_ref.resolveOwned(alloc, secret_store) else null;
                defer if (bearer_token) |value| alloc.free(value);
                break :blk .{ .vertex = try vertex_provider.Provider.init(alloc, http, .{
                    .base_url = provider_defaults.normalizedBase(cfg.url, provider_defaults.vertex_v1_base),
                    .project_id = cfg.project_id,
                    .location = cfg.location orelse provider_defaults.default_google_location,
                    .credentials_path = cfg.credentials_path,
                    .bearer_token = bearer_token,
                }) };
            },
            .antfly => if (cfg.url.len == 0 and embedded_antfly_provider != null)
                .{ .embedded_antfly = embedded_antfly_provider.? }
            else
                .{ .remote_antfly = antfly_provider.Provider.init(alloc, http, if (cfg.url.len > 0) cfg.url else "http://127.0.0.1:8082") },
            else => return error.UnsupportedGeneratorProvider,
        };

        errdefer switch (state.provider) {
            inline .openai, .remote_antfly, .vertex, .gemini => |*provider| provider.deinit(),
            .embedded_antfly => {},
        };
        const policy = try provider_limits.Policy.fromConfig(cfg.rate_limit);
        if (state.provider == .embedded_antfly and policy.enabled()) return error.UnsupportedLocalRateLimit;
        state.quota = try limits.acquire(state.quotaIdentity(cfg.model), policy);

        return .{
            .ptr = state,
            .vtable = &.{
                .generate = generate,
                .deinit = deinit,
            },
        };
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *BackendState = @ptrCast(@alignCast(ptr));
        switch (self.provider) {
            .openai => |*provider| provider.deinit(),
            .remote_antfly => |*provider| provider.deinit(),
            .embedded_antfly => {},
            .vertex => |*provider| provider.deinit(),
            .gemini => |*provider| provider.deinit(),
        }
        if (self.quota) |*quota| quota.release();
        self.auth_header_cache.deinit(self.alloc);
        if (self.api_key) |*api_key| api_key.deinit(self.alloc);
        self.alloc.destroy(self);
    }

    fn quotaIdentity(self: *const BackendState, model: []const u8) provider_limits.QuotaIdentity {
        const source = if (self.cfg.provider == .vertex and self.api_key == null)
            credential_identity.CredentialSourceIdentity.googleAdc(self.cfg.credentials_path)
        else
            credential_identity.fromSecretValue(self.api_key);
        const endpoint = switch (self.provider) {
            .openai => |provider| provider.base_url,
            .remote_antfly => |provider| provider.base_url,
            .vertex => |provider| provider.base_url,
            .gemini => |provider| provider.base_url,
            .embedded_antfly => "",
        };
        return .{
            .operation = .generation,
            .endpoint = .{
                .provider = std.meta.stringToEnum(provider_limits.Provider, @tagName(self.cfg.provider)).?,
                .endpoint = endpoint,
                .model = model,
                .credentials = source,
                .project = if (self.provider == .vertex) self.provider.vertex.project_id else "",
                .location = if (self.provider == .vertex) self.provider.vertex.location else "",
            },
        };
    }

    fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) !GenerateResult {
        const self: *BackendState = @ptrCast(@alignCast(ptr));
        const policy = try provider_limits.Policy.fromConfig(self.cfg.rate_limit);
        if (policy.tokens_per_minute != 0) {
            if (self.cfg.max_tokens <= 0) return error.InvalidRateLimitPolicy;
            for (messages) |message| {
                if (message.content) |content| switch (content) {
                    .text => {},
                    .parts => |parts| for (parts) |part| {
                        if (part != .text) return error.UnsupportedMediaTokenBudget;
                    },
                };
            }
        }
        if (self.provider == .embedded_antfly and policy.enabled()) return error.UnsupportedLocalRateLimit;
        var temporary: ?provider_limits.Handle = if (std.mem.eql(u8, model, self.cfg.model))
            null
        else
            try self.limits.acquire(self.quotaIdentity(model), policy);
        defer if (temporary) |*handle| handle.release();
        const quota = temporary orelse self.quota.?;
        const observer = quota.limiter().observer(try generationOutputBudget(self.cfg, 1));
        switch (self.provider) {
            inline .openai, .remote_antfly, .vertex, .gemini => |*provider| provider.attempt_observer = observer,
            .embedded_antfly => {},
        }
        defer switch (self.provider) {
            inline .openai, .remote_antfly, .vertex, .gemini => |*provider| provider.attempt_observer = null,
            .embedded_antfly => {},
        };
        var result = switch (self.provider) {
            .openai => |*provider| blk: {
                if (self.api_key) |*api_key_ref| {
                    if (try optionalBearerAuthHeaderOwned(self, alloc, api_key_ref)) |auth_header| {
                        defer alloc.free(auth_header);
                        try provider.setAuthorizationHeader(auth_header);
                    }
                }
                provider.setToolOptions(self.cfg.tools_json, self.cfg.tool_choice_json);
                provider.setMaxTokens(self.cfg.max_tokens);
                provider.setSamplingOptions(self.cfg.temperature, self.cfg.top_p, self.cfg.top_k, self.cfg.frequency_penalty, self.cfg.presence_penalty);
                break :blk try provider.generator().generate(alloc, model, messages);
            },
            .remote_antfly => |*provider| blk: {
                if (self.api_key) |*api_key_ref| {
                    if (try optionalBearerAuthHeaderOwned(self, alloc, api_key_ref)) |auth_header| {
                        defer alloc.free(auth_header);
                        try provider.setAuthorizationHeader(auth_header);
                    }
                }
                provider.setToolOptions(self.cfg.tools_json, self.cfg.tool_choice_json);
                provider.setMaxTokens(self.cfg.max_tokens);
                provider.setSamplingOptions(self.cfg.temperature, self.cfg.top_p, self.cfg.top_k, self.cfg.frequency_penalty, self.cfg.presence_penalty);
                break :blk try provider.generator().generate(alloc, model, messages);
            },
            .vertex => |*provider| blk: {
                provider.setMaxTokens(self.cfg.max_tokens);
                provider.setSamplingOptions(self.cfg.temperature, self.cfg.top_p, self.cfg.top_k);
                break :blk try provider.generator().generate(alloc, model, messages);
            },
            .gemini => |*provider| blk: {
                provider.setMaxTokens(self.cfg.max_tokens);
                provider.setSamplingOptions(self.cfg.temperature, self.cfg.top_p, self.cfg.top_k);
                break :blk try provider.generator().generate(alloc, model, messages);
            },
            .embedded_antfly => |local| blk: {
                if (local.generate_messages) |generate_messages| {
                    const content = try generate_messages(local.ptr, alloc, model, messages);
                    break :blk inference.GenerateResult{
                        .content = content,
                        .allocator = alloc,
                    };
                }
                const generate_text = local.generate_text orelse return error.UnsupportedGeneratorProvider;
                const roles = try alloc.alloc([]const u8, messages.len);
                defer alloc.free(roles);
                const contents = try alloc.alloc([]const u8, messages.len);
                defer alloc.free(contents);
                for (messages, 0..) |message, i| {
                    roles[i] = message.role.toSlice();
                    contents[i] = textContent(message) orelse return error.UnsupportedGeneratorProvider;
                }
                const content = try generate_text(local.ptr, alloc, model, roles, contents);
                break :blk inference.GenerateResult{
                    .content = content,
                    .allocator = alloc,
                };
            },
        };
        defer result.deinit();

        return .{
            .content = try alloc.dupe(u8, result.content),
            .tool_calls = try cloneToolCalls(alloc, result.tool_calls),
            .allocator = alloc,
        };
    }
};

fn cloneToolCalls(alloc: std.mem.Allocator, calls: []const inference.types.ToolCall) ![]lib.ToolCall {
    if (calls.len == 0) return &.{};
    const out = try alloc.alloc(lib.ToolCall, calls.len);
    errdefer alloc.free(out);
    for (calls, 0..) |call, i| {
        out[i] = .{
            .id = try alloc.dupe(u8, call.id),
            .name = try alloc.dupe(u8, call.name),
            .arguments = try alloc.dupe(u8, call.arguments),
        };
    }
    return out;
}

fn optionalBearerAuthHeaderOwned(
    state: *BackendState,
    alloc: std.mem.Allocator,
    api_key_ref: *const common_secrets.SecretValue,
) !?[]u8 {
    return state.auth_header_cache.getOwned(state.alloc, alloc, api_key_ref, state.secret_store) catch |err| switch (err) {
        error.SecretNotFound => switch (api_key_ref.*) {
            .env_var => return null,
            else => return err,
        },
        else => return err,
    };
}

fn textContent(message: ChatMessage) ?[]const u8 {
    const content = message.content orelse return "";
    return switch (content) {
        .text => |text| text,
        .parts => null,
    };
}

test "generating optional env auth is skipped when unset" {
    const alloc = std.testing.allocator;
    var api_key = try common_secrets.SecretValue.initConfigOrEnv(alloc, null, "ANTFLY_TEST_GENERATING_MISSING_API_KEY");
    defer api_key.deinit(alloc);

    var state = BackendState{
        .alloc = alloc,
        .cfg = undefined,
        .api_key = null,
        .auth_header_cache = .{},
        .secret_store = null,
        .provider = undefined,
    };
    defer state.auth_header_cache.deinit(alloc);

    const header = try optionalBearerAuthHeaderOwned(&state, alloc, &api_key);
    try std.testing.expectEqual(@as(?[]u8, null), header);
}

test "generating explicit missing secret auth remains strict" {
    const alloc = std.testing.allocator;
    var api_key = try common_secrets.SecretValue.initConfig(alloc, "secret://missing.generator_key") orelse return error.TestUnexpectedResult;
    defer api_key.deinit(alloc);

    var state = BackendState{
        .alloc = alloc,
        .cfg = undefined,
        .api_key = null,
        .auth_header_cache = .{},
        .secret_store = null,
        .provider = undefined,
    };
    defer state.auth_header_cache.deinit(alloc);

    try std.testing.expectError(error.SecretNotFound, optionalBearerAuthHeaderOwned(&state, alloc, &api_key));
}

pub fn executeChain(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    chain: []const ChainLink,
    messages: []const ChatMessage,
) !GenerateResult {
    var factory_impl = BackendFactory.init(alloc, http);
    return try lib.executeChain(alloc, chain, factory_impl.factory(), messages);
}

pub fn executeChainWithAntflyProvider(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    chain: []const ChainLink,
    embedded_antfly_provider: ?managed_embedder.AntflyProvider,
    messages: []const ChatMessage,
) !GenerateResult {
    var factory_impl = BackendFactory.initWithAntflyProvider(alloc, http, embedded_antfly_provider);
    return try lib.executeChain(alloc, chain, factory_impl.factory(), messages);
}

pub fn executeChainWithOptions(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    chain: []const ChainLink,
    options: BackendFactory.Options,
    messages: []const ChatMessage,
) !GenerateResult {
    var factory_impl = BackendFactory.initWithOptions(alloc, http, options);
    return try lib.executeChain(alloc, chain, factory_impl.factory(), messages);
}

test "generating backend enforces shared policies and token budgets before dispatch" {
    const alloc = std.testing.allocator;
    var limits = provider_limits.Registry.init(alloc);
    defer limits.deinit();
    var client = httpx.Client.initWithConfig(alloc, std.testing.io, .{});
    defer client.deinit();
    var factory = BackendFactory.initWithOptions(alloc, &client, .{ .limits = &limits });
    var cfg = GeneratorConfig.fromOpenAI(.{ .model = "quota-test", .url = "http://127.0.0.1:1" });
    cfg.rate_limit = .{ .requests_per_minute = 60, .tokens_per_minute = 1 };
    var first = try factory.factory().create(alloc, cfg);
    defer first.deinit();
    var same = try factory.factory().create(alloc, cfg);
    defer same.deinit();
    var conflicting = cfg;
    conflicting.rate_limit.?.requests_per_minute = 120;
    try std.testing.expectError(error.ConflictingRateLimitPolicy, factory.factory().create(alloc, conflicting));
    const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .text = "hello" } }};
    try std.testing.expectError(error.ProviderTokenBudgetExceeded, first.generate(alloc, cfg.model, &messages));
    const state: *BackendState = @ptrCast(@alignCast(first.ptr));
    try std.testing.expectEqual(@as(u32, 0), state.quota.?.limiter().in_flight);
}

test "generating backend factory executes fallback chain across providers" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/openai/chat/completions", .respond = .{
            .status = 429,
            .body = "{\"error\":\"rate limit\"}",
        } },
        .{ .method = .POST, .path = "/antfly/generate", .respond = .{
            .body = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"fallback ok\"}}]}",
        } },
    });
    defer ts.deinit();

    const openai_url = try std.fmt.allocPrint(alloc, "{s}/openai", .{ts.baseUrl()});
    defer alloc.free(openai_url);
    const antfly_url = try std.fmt.allocPrint(alloc, "{s}/antfly", .{ts.baseUrl()});
    defer alloc.free(antfly_url);

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    const chain = [_]ChainLink{
        .{
            .generator = GeneratorConfig.fromOpenAI(.{ .model = "gpt-4.1", .url = openai_url }),
            .condition = .on_rate_limit,
        },
        .{
            .generator = GeneratorConfig.fromAntfly(.{ .model = "local", .url = antfly_url }),
        },
    };

    var group = std.Io.Group.init;
    var content: ?[]u8 = null;
    defer if (content) |value| alloc.free(value);
    var run_err: ?anyerror = null;

    const Fiber = struct {
        fn run(
            a: std.mem.Allocator,
            h: *httpx.Client,
            chain_arg: []const ChainLink,
            out: *?[]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .text = "hello" } }};
            var result = executeChain(a, h, chain_arg, &messages) catch |err| {
                err_out.* = err;
                return;
            };
            defer result.deinit();
            out.* = a.dupe(u8, result.content) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, &client, &chain, &content, &run_err }) catch return;
    try ts.handleOne();
    try ts.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("fallback ok", content.?);
}

test "generating backend routes antfly and url-less antfly to local provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    const FakeLocal = struct {
        calls: usize = 0,

        fn embedDenseTexts(
            ptr: *anyopaque,
            a: std.mem.Allocator,
            model: []const u8,
            texts: []const []const u8,
        ) anyerror![][]f32 {
            _ = ptr;
            _ = a;
            _ = model;
            _ = texts;
            return error.TestUnexpectedResult;
        }

        fn embedSparseTexts(
            ptr: *anyopaque,
            a: std.mem.Allocator,
            model: []const u8,
            texts: []const []const u8,
        ) anyerror![]@import("../storage/db/enrichment/embedder.zig").SparseEmbedding {
            _ = ptr;
            _ = a;
            _ = model;
            _ = texts;
            return error.TestUnexpectedResult;
        }

        fn generateText(
            ptr: *anyopaque,
            a: std.mem.Allocator,
            model: []const u8,
            roles: []const []const u8,
            contents: []const []const u8,
        ) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("local-model", model);
            try std.testing.expectEqual(@as(usize, 1), roles.len);
            try std.testing.expectEqualStrings("user", roles[0]);
            try std.testing.expectEqualStrings("hello", contents[0]);
            return try a.dupe(u8, "local ok");
        }
    };

    var fake = FakeLocal{};
    const local_provider = managed_embedder.AntflyProvider{
        .ptr = &fake,
        .embed_dense_texts = FakeLocal.embedDenseTexts,
        .embed_sparse_texts = FakeLocal.embedSparseTexts,
        .generate_text = FakeLocal.generateText,
    };

    const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .text = "hello" } }};
    const antfly_chain = [_]ChainLink{.{
        .generator = .{
            .provider = .antfly,
            .model = "local-model",
            .url = "",
        },
    }};
    var antfly_result = try executeChainWithAntflyProvider(alloc, &client, &antfly_chain, local_provider, &messages);
    defer antfly_result.deinit();
    try std.testing.expectEqualStrings("local ok", antfly_result.content);

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "generating backend passes multimodal messages to local provider callback" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    const FakeLocal = struct {
        calls: usize = 0,

        fn embedDenseTexts(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparseTexts(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![]@import("../storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn generateMessages(
            ptr: *anyopaque,
            a: std.mem.Allocator,
            model: []const u8,
            messages: []const ChatMessage,
        ) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("local-model", model);
            try std.testing.expectEqual(@as(usize, 1), messages.len);
            const content = messages[0].content orelse return error.TestUnexpectedResult;
            const parts = switch (content) {
                .parts => |items| items,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(@as(usize, 2), parts.len);
            switch (parts[0]) {
                .text => |text| try std.testing.expectEqualStrings("describe", text),
                else => return error.TestUnexpectedResult,
            }
            switch (parts[1]) {
                .media => |media| try std.testing.expectEqualStrings("image/png", media.mime_type),
                else => return error.TestUnexpectedResult,
            }
            return try a.dupe(u8, "local multimodal ok");
        }
    };

    var fake = FakeLocal{};
    const local_provider = managed_embedder.AntflyProvider{
        .ptr = &fake,
        .embed_dense_texts = FakeLocal.embedDenseTexts,
        .embed_sparse_texts = FakeLocal.embedSparseTexts,
        .generate_messages = FakeLocal.generateMessages,
    };

    const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .parts = &.{
        .{ .text = "describe" },
        .{ .media = .{ .mime_type = "image/png", .data = "AA==" } },
    } } }};
    const chain = [_]ChainLink{.{
        .generator = .{
            .provider = .antfly,
            .model = "local-model",
            .url = "",
        },
    }};
    var result = try executeChainWithAntflyProvider(alloc, &client, &chain, local_provider, &messages);
    defer result.deinit();
    try std.testing.expectEqualStrings("local multimodal ok", result.content);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "generating antfly backend treats missing default api key env as optional" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var ts = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/generate", .respond = .{
            .body = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"no auth ok\"}}]}",
        } },
    });
    defer ts.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    const chain = [_]ChainLink{.{
        .generator = .{
            .provider = .antfly,
            .model = "local",
            .url = ts.baseUrl(),
        },
    }};
    const messages = [_]ChatMessage{.{ .role = .user, .content = .{ .text = "hello" } }};

    var group = std.Io.Group.init;
    var content: ?[]u8 = null;
    defer if (content) |value| alloc.free(value);
    var run_err: ?anyerror = null;

    const Fiber = struct {
        fn run(
            a: std.mem.Allocator,
            test_io: std.Io,
            test_client: *httpx.Client,
            links: []const ChainLink,
            test_messages: []const ChatMessage,
            out: *?[]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            _ = test_io;
            var result = executeChain(a, test_client, links, test_messages) catch |err| {
                err_out.* = err;
                return;
            };
            defer result.deinit();
            out.* = a.dupe(u8, result.content) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, io, &client, &chain, &messages, &content, &run_err }) catch return;
    try ts.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("no auth ok", content orelse return error.TestUnexpectedResult);
}

/// Text-only batch dispatch shares BackendState's quota identity and credential
/// resolution with single generation. The caller owns the returned response.
pub fn generateAntflyTextBatchResponse(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    cfg: GeneratorConfig,
    options: BackendFactory.Options,
    texts: []const []const u8,
) !httpx.Response {
    try cfg.validate();
    if (cfg.provider != .antfly or cfg.url.len == 0 or texts.len == 0)
        return error.InvalidGeneratorConfig;
    if (cfg.tools_json != null or cfg.tool_choice_json != null) return error.InvalidGeneratorConfig;
    var factory = BackendFactory.initWithOptions(alloc, http, options);
    var generator = try factory.factory().create(alloc, cfg);
    defer generator.deinit();
    const state: *BackendState = @ptrCast(@alignCast(generator.ptr));
    const output_tokens = try generationOutputBudget(cfg, texts.len);
    const body = try antflyGenerateBatchRequestJsonAlloc(alloc, cfg, texts);
    defer alloc.free(body);
    const url = try std.fmt.allocPrint(alloc, "{s}/generate/batch", .{std.mem.trimEnd(u8, state.provider.remote_antfly.base_url, "/")});
    defer alloc.free(url);
    const authorization = if (state.api_key) |*key| try optionalBearerAuthHeaderOwned(state, alloc, key) else null;
    defer if (authorization) |value| alloc.free(value);
    var headers: [1][2][]const u8 = undefined;
    if (authorization) |value| headers[0] = .{ "Authorization", value };
    return http.post(url, .{
        .json = body,
        .headers = if (authorization != null) &headers else null,
        .timeout_ms = 300_000,
        .attempt_observer = state.quota.?.limiter().observer(output_tokens),
    });
}

test "generating backend batch reserves all output caps before dispatch" {
    const alloc = std.testing.allocator;
    var limits = provider_limits.Registry.init(alloc);
    defer limits.deinit();
    var client = httpx.Client.initWithConfig(alloc, std.testing.io, .{});
    defer client.deinit();
    var cfg = GeneratorConfig.fromAntfly(.{ .model = "budget", .url = "http://127.0.0.1:1" });
    cfg.max_tokens = 100;
    const texts = [_][]const u8{ "one", "two" };
    const body = try antflyGenerateBatchRequestJsonAlloc(alloc, cfg, &texts);
    defer alloc.free(body);
    // A budget that fits one output cap must still reject a two-item batch.
    cfg.rate_limit = .{ .tokens_per_minute = @intCast(body.len + 100) };
    try std.testing.expectError(error.ProviderTokenBudgetExceeded, generateAntflyTextBatchResponse(alloc, &client, cfg, .{ .limits = &limits }, &texts));
}

test "generating backend batch shares single-request quotas and credentials" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var limits = provider_limits.Registry.init(alloc);
    defer limits.deinit();
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer batch-test", req.header("Authorization") orelse return error.TestUnexpectedResult);
        }
    };
    var server = try httpx.TestServer.start(alloc, io, &.{.{ .method = .POST, .path = "/generate/batch", .assert_request = Check.request, .respond = .{ .status = 429 } }});
    defer server.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var cfg = GeneratorConfig.fromAntfly(.{ .model = "shared", .url = server.baseUrl() });
    cfg.api_key = "batch-test";
    cfg.rate_limit = .{ .requests_per_minute = 1, .max_concurrency = 1 };
    var factory = BackendFactory.initWithOptions(alloc, &client, .{ .limits = &limits });
    var single = try factory.factory().create(alloc, cfg);
    defer single.deinit();
    var conflicting = cfg;
    conflicting.rate_limit.?.requests_per_minute = 2;
    try std.testing.expectError(error.ConflictingRateLimitPolicy, generateAntflyTextBatchResponse(alloc, &client, conflicting, .{ .limits = &limits }, &.{"hello"}));
    const Run = struct {
        fn run(a: std.mem.Allocator, h: *httpx.Client, c: GeneratorConfig, r: *provider_limits.Registry, err_out: *?anyerror) !void {
            var response = generateAntflyTextBatchResponse(a, h, c, .{ .limits = r }, &.{ "one", "two" }) catch |err| {
                err_out.* = err;
                return;
            };
            defer response.deinit();
            std.testing.expectEqual(@as(u16, 429), response.status.code) catch |err| {
                err_out.* = err;
            };
        }
    };
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Run.run, .{ alloc, &client, cfg, &limits, &failure });
    try server.handleOne();
    try group.await(io);
    if (failure) |err| return err;
    const state: *BackendState = @ptrCast(@alignCast(single.ptr));
    try std.testing.expect(state.quota.?.limiter().requests < 1);
    try std.testing.expect(state.quota.?.limiter().cooldown_ns != 0);
    try std.testing.expectEqual(@as(u32, 0), state.quota.?.limiter().in_flight);
}

fn generationOutputBudget(cfg: GeneratorConfig, count: usize) !u64 {
    const policy = try provider_limits.Policy.fromConfig(cfg.rate_limit);
    if (policy.tokens_per_minute != 0 and cfg.max_tokens <= 0) return error.InvalidRateLimitPolicy;
    return std.math.mul(u64, @intCast(@max(0, cfg.max_tokens)), @intCast(count)) catch error.ProviderTokenBudgetExceeded;
}

fn antflyGenerateBatchRequestJsonAlloc(
    alloc: std.mem.Allocator,
    cfg: GeneratorConfig,
    texts: []const []const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"mode\":\"sync\",\"requests\":[");
    for (texts, 0..) |text, i| {
        if (i > 0) try out.append(alloc, ',');
        const item = try std.fmt.allocPrint(
            alloc,
            "{{\"custom_id\":\"{d}\",\"body\":{{\"model\":{f},\"messages\":[{{\"role\":\"user\",\"content\":{f}}}],\"mode\":\"eager\"",
            .{
                i,
                std.json.fmt(cfg.model, .{}),
                std.json.fmt(text, .{}),
            },
        );
        defer alloc.free(item);
        try out.appendSlice(alloc, item);
        try appendBatchI64Field(alloc, &out, "max_tokens", cfg.max_tokens);
        if (cfg.temperature) |temperature| try appendBatchFloatField(alloc, &out, "temperature", temperature);
        if (cfg.top_p) |top_p| try appendBatchFloatField(alloc, &out, "top_p", top_p);
        if (cfg.top_k) |top_k| try appendBatchI64Field(alloc, &out, "top_k", top_k);
        if (cfg.frequency_penalty) |frequency_penalty| try appendBatchFloatField(alloc, &out, "frequency_penalty", frequency_penalty);
        if (cfg.presence_penalty) |presence_penalty| try appendBatchFloatField(alloc, &out, "presence_penalty", presence_penalty);
        try out.appendSlice(alloc, "}}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn appendBatchI64Field(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, value: i64) !void {
    const fragment = try std.fmt.allocPrint(alloc, ",\"{s}\":{d}", .{ name, value });
    defer alloc.free(fragment);
    try out.appendSlice(alloc, fragment);
}

fn appendBatchFloatField(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, value: f32) !void {
    const fragment = try std.fmt.allocPrint(alloc, ",\"{s}\":{f}", .{ name, std.json.fmt(value, .{}) });
    defer alloc.free(fragment);
    try out.appendSlice(alloc, fragment);
}
