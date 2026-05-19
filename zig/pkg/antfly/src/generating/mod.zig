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
const termite_provider = @import("../inference/termite.zig");
const common_secrets = @import("../common/secrets.zig");

pub const Role = lib.Role;
pub const ChatMessage = lib.ChatMessage;
pub const GenerateResult = lib.GenerateResult;
pub const Provider = lib.Provider;
pub const OpenAIConfig = lib.OpenAIConfig;
pub const OllamaConfig = lib.OllamaConfig;
pub const TermiteConfig = lib.TermiteConfig;
pub const GeneratorConfig = lib.GeneratorConfig;
pub const RetryConfig = lib.RetryConfig;
pub const ChainCondition = lib.ChainCondition;
pub const ChainLink = lib.ChainLink;
pub const GeneratorFactory = lib.GeneratorFactory;

pub const BackendFactory = struct {
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    secret_store: ?*common_secrets.FileStore = null,

    pub fn init(alloc: std.mem.Allocator, http: *httpx.Client) BackendFactory {
        return .{ .alloc = alloc, .http = http };
    }

    pub fn initWithLocalTermite(
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    ) BackendFactory {
        return initWithOptions(alloc, http, .{ .local_termite_provider = local_termite_provider });
    }

    pub const Options = struct {
        local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
        secret_store: ?*common_secrets.FileStore = null,
    };

    pub fn initWithOptions(
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        options: Options,
    ) BackendFactory {
        return .{
            .alloc = alloc,
            .http = http,
            .local_termite_provider = options.local_termite_provider,
            .secret_store = options.secret_store,
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
        return try BackendState.init(alloc, self.http, cfg, self.local_termite_provider, self.secret_store);
    }
};

const BackendState = struct {
    alloc: std.mem.Allocator,
    cfg: GeneratorConfig,
    api_key: ?common_secrets.SecretValue = null,
    secret_store: ?*common_secrets.FileStore = null,
    provider: union(enum) {
        openai: openai_provider.Provider,
        termite: termite_provider.Provider,
        local_termite: managed_embedder.LocalTermiteProvider,
    },

    fn init(
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        cfg: GeneratorConfig,
        local_termite_provider: ?managed_embedder.LocalTermiteProvider,
        secret_store: ?*common_secrets.FileStore,
    ) !lib.Generator {
        const state = try alloc.create(BackendState);
        errdefer alloc.destroy(state);

        state.alloc = alloc;
        state.cfg = cfg;
        state.api_key = try common_secrets.SecretValue.initConfig(alloc, cfg.api_key);
        errdefer if (state.api_key) |*api_key| api_key.deinit(alloc);
        state.secret_store = secret_store;
        state.provider = switch (cfg.provider) {
            .openai, .ollama => blk: {
                const provider = openai_provider.Provider.init(alloc, http, cfg.url);
                break :blk .{ .openai = provider };
            },
            .antfly => .{ .local_termite = local_termite_provider orelse return error.UnsupportedGeneratorProvider },
            .termite => if (cfg.url.len == 0 and local_termite_provider != null)
                .{ .local_termite = local_termite_provider.? }
            else
                .{ .termite = termite_provider.Provider.init(alloc, http, if (cfg.url.len > 0) cfg.url else "http://127.0.0.1:8082") },
            else => return error.UnsupportedGeneratorProvider,
        };

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
            .termite => |*provider| provider.deinit(),
            .local_termite => {},
        }
        if (self.api_key) |*api_key| api_key.deinit(self.alloc);
        self.alloc.destroy(self);
    }

    fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) !GenerateResult {
        const self: *BackendState = @ptrCast(@alignCast(ptr));
        const inference_messages = try alloc.alloc(inference.ChatMessage, messages.len);
        defer alloc.free(inference_messages);

        for (messages, 0..) |message, i| {
            inference_messages[i] = .{
                .role = switch (message.role) {
                    .system => .system,
                    .user => .user,
                    .assistant => .assistant,
                },
                .content = message.content,
            };
        }

        var result = switch (self.provider) {
            .openai => |*provider| blk: {
                const api_key = if (self.api_key) |*api_key_ref|
                    try api_key_ref.resolveOwned(alloc, self.secret_store)
                else
                    null;
                defer if (api_key) |value| alloc.free(value);
                if (api_key) |value| try provider.setApiKey(value);
                break :blk try provider.generator().generate(alloc, model, inference_messages);
            },
            .termite => |*provider| try provider.generator().generate(alloc, model, inference_messages),
            .local_termite => |local| blk: {
                const generate_text = local.generate_text orelse return error.UnsupportedGeneratorProvider;
                const roles = try alloc.alloc([]const u8, messages.len);
                defer alloc.free(roles);
                const contents = try alloc.alloc([]const u8, messages.len);
                defer alloc.free(contents);
                for (messages, 0..) |message, i| {
                    roles[i] = switch (message.role) {
                        .system => "system",
                        .user => "user",
                        .assistant => "assistant",
                    };
                    contents[i] = message.content;
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
            .allocator = alloc,
        };
    }
};

pub fn executeChain(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    chain: []const ChainLink,
    messages: []const ChatMessage,
) !GenerateResult {
    var factory_impl = BackendFactory.init(alloc, http);
    return try lib.executeChain(alloc, chain, factory_impl.factory(), messages);
}

pub fn executeChainWithLocalTermite(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    chain: []const ChainLink,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider,
    messages: []const ChatMessage,
) !GenerateResult {
    var factory_impl = BackendFactory.initWithLocalTermite(alloc, http, local_termite_provider);
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
        .{ .method = .POST, .path = "/termite/generate", .respond = .{
            .body = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"fallback ok\"}}]}",
        } },
    });
    defer ts.deinit();

    const openai_url = try std.fmt.allocPrint(alloc, "{s}/openai", .{ts.baseUrl()});
    defer alloc.free(openai_url);
    const termite_url = try std.fmt.allocPrint(alloc, "{s}/termite", .{ts.baseUrl()});
    defer alloc.free(termite_url);

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    const chain = [_]ChainLink{
        .{
            .generator = GeneratorConfig.fromOpenAI(.{ .model = "gpt-4.1", .url = openai_url }),
            .condition = .on_error,
        },
        .{
            .generator = GeneratorConfig.fromTermite(.{ .model = "local", .url = termite_url }),
        },
    };

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
            out: *?[]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            _ = test_io;
            var result = executeChain(a, test_client, links, &.{.{ .role = .user, .content = "hello" }}) catch |err| {
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

    group.concurrent(io, Fiber.run, .{ alloc, io, &client, &chain, &content, &run_err }) catch return;
    try ts.handleOne();
    try ts.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("fallback ok", content orelse return error.TestUnexpectedResult);
}

test "generating backend routes antfly and url-less termite to local provider" {
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
    const local_provider = managed_embedder.LocalTermiteProvider{
        .ptr = &fake,
        .embed_dense_texts = FakeLocal.embedDenseTexts,
        .embed_sparse_texts = FakeLocal.embedSparseTexts,
        .generate_text = FakeLocal.generateText,
    };

    const messages = [_]ChatMessage{.{ .role = .user, .content = "hello" }};
    const antfly_chain = [_]ChainLink{.{
        .generator = .{
            .provider = .antfly,
            .model = "local-model",
            .url = "",
        },
    }};
    var antfly_result = try executeChainWithLocalTermite(alloc, &client, &antfly_chain, local_provider, &messages);
    defer antfly_result.deinit();
    try std.testing.expectEqualStrings("local ok", antfly_result.content);

    const termite_chain = [_]ChainLink{.{
        .generator = .{
            .provider = .termite,
            .model = "local-model",
            .url = "",
        },
    }};
    var termite_result = try executeChainWithLocalTermite(alloc, &client, &termite_chain, local_provider, &messages);
    defer termite_result.deinit();
    try std.testing.expectEqualStrings("local ok", termite_result.content);

    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}
