// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const json = @import("antfly-json");
const openapi = @import("antfly_generating_openapi");

pub const Role = enum {
    system,
    user,
    assistant,
};

pub const ChatMessage = struct {
    role: Role,
    content: []const u8,
};

pub const GenerateResult = struct {
    content: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerateResult) void {
        self.allocator.free(self.content);
        self.* = undefined;
    }
};

pub const Provider = enum {
    openai,
    ollama,
    antfly,
    mock,

    pub fn validate(self: Provider) !void {
        _ = self;
    }
};

pub const OpenAIConfig = struct {
    model: []const u8,
    url: []const u8 = "https://api.openai.com/v1",
    api_key: ?[]const u8 = null,
};

pub const OllamaConfig = struct {
    model: []const u8,
    url: []const u8 = "http://127.0.0.1:11434/v1",
};

pub const AntflyConfig = struct {
    model: []const u8,
    url: []const u8 = "",
};

pub const GeneratorConfig = struct {
    provider: Provider,
    model: []const u8,
    url: []const u8,
    api_key: ?[]const u8 = null,

    pub fn clone(self: GeneratorConfig, alloc: std.mem.Allocator) !GeneratorConfig {
        return .{
            .provider = self.provider,
            .model = if (self.model.len > 0) try alloc.dupe(u8, self.model) else "",
            .url = if (self.url.len > 0) try alloc.dupe(u8, self.url) else "",
            .api_key = if (self.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
        };
    }

    pub fn deinit(self: *GeneratorConfig, alloc: std.mem.Allocator) void {
        if (self.model.len > 0) alloc.free(self.model);
        if (self.url.len > 0) alloc.free(self.url);
        if (self.api_key) |api_key| alloc.free(api_key);
        self.* = undefined;
    }

    pub fn fromOpenAI(cfg: OpenAIConfig) GeneratorConfig {
        return .{
            .provider = .openai,
            .model = cfg.model,
            .url = cfg.url,
            .api_key = cfg.api_key,
        };
    }

    pub fn fromOllama(cfg: OllamaConfig) GeneratorConfig {
        return .{
            .provider = .ollama,
            .model = cfg.model,
            .url = cfg.url,
        };
    }

    pub fn fromAntfly(cfg: AntflyConfig) GeneratorConfig {
        return .{
            .provider = .antfly,
            .model = cfg.model,
            .url = cfg.url,
        };
    }

    pub fn validate(self: GeneratorConfig) !void {
        try self.provider.validate();
        if (self.model.len == 0 and self.provider != .mock) return error.InvalidGeneratorConfig;
        if (self.url.len == 0 and self.provider != .mock and self.provider != .antfly) return error.InvalidGeneratorConfig;
    }

    pub fn getModel(self: GeneratorConfig) []const u8 {
        return self.model;
    }
};

pub const RetryConfig = struct {
    max_attempts: u32 = 1,
    initial_backoff_ms: u32 = 1000,
    backoff_multiplier: f32 = 2.0,
    max_backoff_ms: u32 = 30_000,

    pub fn validate(self: RetryConfig) !void {
        if (self.max_attempts == 0) return error.InvalidRetryConfig;
        if (self.initial_backoff_ms == 0 and self.max_attempts > 1) return error.InvalidRetryConfig;
        if (self.backoff_multiplier < 1.0) return error.InvalidRetryConfig;
        if (self.max_backoff_ms < self.initial_backoff_ms) return error.InvalidRetryConfig;
    }
};

pub const ChainCondition = enum {
    always,
    on_error,
    on_timeout,
    on_rate_limit,
};

pub const ChainLink = struct {
    generator: GeneratorConfig,
    condition: ?ChainCondition = null,
    retry: ?RetryConfig = null,

    pub fn clone(self: ChainLink, alloc: std.mem.Allocator) !ChainLink {
        return .{
            .generator = try self.generator.clone(alloc),
            .condition = self.condition,
            .retry = self.retry,
        };
    }

    pub fn deinit(self: *ChainLink, alloc: std.mem.Allocator) void {
        self.generator.deinit(alloc);
        self.* = undefined;
    }

    pub fn validate(self: ChainLink) !void {
        try self.generator.validate();
        if (self.retry) |retry| try retry.validate();
    }
};

pub fn parseConfigFromSlice(alloc: std.mem.Allocator, raw: []const u8) !GeneratorConfig {
    const parsed = try json.parseFromSlice(openapi.GeneratorConfig, alloc, raw, .{});
    defer parsed.deinit();
    return try configFromOpenApi(alloc, parsed.value);
}

pub fn parseConfigFromValue(alloc: std.mem.Allocator, value: json.Value) !GeneratorConfig {
    const parsed = try json.parseFromValue(openapi.GeneratorConfig, alloc, value, .{});
    defer parsed.deinit();
    return try configFromOpenApi(alloc, parsed.value);
}

pub fn stringifyConfigAlloc(alloc: std.mem.Allocator, cfg: GeneratorConfig) ![]u8 {
    try cfg.validate();
    return try json.Stringify.valueAlloc(alloc, openApiFromConfig(cfg), .{});
}

pub fn parseChainLinkFromSlice(alloc: std.mem.Allocator, raw: []const u8) !ChainLink {
    const parsed = try json.parseFromSlice(openapi.ChainLink, alloc, raw, .{});
    defer parsed.deinit();
    return try chainLinkFromOpenApi(alloc, parsed.value);
}

pub fn parseRetryConfigFromValue(alloc: std.mem.Allocator, value: json.Value) !RetryConfig {
    const parsed = try json.parseFromValue(openapi.RetryConfig, alloc, value, .{});
    defer parsed.deinit();
    return (try retryConfigFromOpenApi(parsed.value)) orelse error.InvalidRetryConfig;
}

pub fn stringifyChainLinkAlloc(alloc: std.mem.Allocator, link: ChainLink) ![]u8 {
    try link.validate();
    return try json.Stringify.valueAlloc(alloc, openApiFromChainLink(link), .{});
}

pub fn configFromOpenApi(alloc: std.mem.Allocator, generated: openapi.GeneratorConfig) !GeneratorConfig {
    var cfg = GeneratorConfig{
        .provider = try providerFromOpenApi(generated.provider),
        .model = if (generated.model) |model| try alloc.dupe(u8, model) else "",
        .url = if (generated.url) |url|
            try alloc.dupe(u8, url)
        else if (generated.api_url) |api_url|
            try alloc.dupe(u8, api_url)
        else
            "",
        .api_key = if (generated.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
    };
    errdefer cfg.deinit(alloc);
    try cfg.validate();
    return cfg;
}

pub fn openApiFromConfig(cfg: GeneratorConfig) openapi.GeneratorConfig {
    return .{
        .provider = providerToOpenApi(cfg.provider),
        .model = if (cfg.model.len > 0) cfg.model else null,
        .url = switch (cfg.provider) {
            .openai, .ollama, .mock => if (cfg.url.len > 0) cfg.url else null,
            .antfly => null,
        },
        .api_url = switch (cfg.provider) {
            .antfly => if (cfg.url.len > 0) cfg.url else null,
            else => null,
        },
        .api_key = cfg.api_key,
    };
}

pub fn retryConfigFromOpenApi(generated: ?openapi.RetryConfig) !?RetryConfig {
    if (generated == null) return null;
    return .{
        .max_attempts = if (generated.?.max_attempts) |max_attempts|
            std.math.cast(u32, max_attempts) orelse return error.InvalidRetryConfig
        else
            3,
        .initial_backoff_ms = if (generated.?.initial_backoff_ms) |initial_backoff_ms|
            std.math.cast(u32, initial_backoff_ms) orelse return error.InvalidRetryConfig
        else
            1000,
        .backoff_multiplier = if (generated.?.backoff_multiplier) |backoff_multiplier| backoff_multiplier else 2.0,
        .max_backoff_ms = if (generated.?.max_backoff_ms) |max_backoff_ms|
            std.math.cast(u32, max_backoff_ms) orelse return error.InvalidRetryConfig
        else
            30_000,
    };
}

pub fn openApiFromRetryConfig(retry: ?RetryConfig) ?openapi.RetryConfig {
    if (retry == null) return null;
    return .{
        .max_attempts = retry.?.max_attempts,
        .initial_backoff_ms = retry.?.initial_backoff_ms,
        .backoff_multiplier = retry.?.backoff_multiplier,
        .max_backoff_ms = retry.?.max_backoff_ms,
    };
}

pub fn chainLinkFromOpenApi(alloc: std.mem.Allocator, generated: openapi.ChainLink) !ChainLink {
    var link = ChainLink{
        .generator = try configFromOpenApi(alloc, generated.generator),
        .condition = if (generated.condition) |condition| try chainConditionFromOpenApi(condition) else null,
        .retry = try retryConfigFromOpenApi(generated.retry),
    };
    errdefer link.deinit(alloc);
    try link.validate();
    return link;
}

pub fn openApiFromChainLink(link: ChainLink) openapi.ChainLink {
    return .{
        .generator = openApiFromConfig(link.generator),
        .retry = openApiFromRetryConfig(link.retry),
        .condition = if (link.condition) |condition| chainConditionToOpenApi(condition) else null,
    };
}

var default_chain: ?[]const ChainLink = null;

pub fn resolveGeneratorOrChain(
    alloc: std.mem.Allocator,
    generator: ?GeneratorConfig,
    chain: []const ChainLink,
) ![]ChainLink {
    if (chain.len > 0) return try cloneChainAlloc(alloc, chain);
    if (generator) |cfg| {
        var out = try alloc.alloc(ChainLink, 1);
        errdefer alloc.free(out);
        out[0] = .{ .generator = try cfg.clone(alloc) };
        return out;
    }
    return try alloc.alloc(ChainLink, 0);
}

pub fn setDefaultChain(alloc: std.mem.Allocator, chain: []const ChainLink) !void {
    if (default_chain) |existing| {
        deinitChainAlloc(alloc, existing);
    }
    default_chain = try cloneChainAlloc(alloc, chain);
}

pub fn getDefaultChain(alloc: std.mem.Allocator) ![]ChainLink {
    const chain = default_chain orelse return try alloc.alloc(ChainLink, 0);
    return try cloneChainAlloc(alloc, chain);
}

pub const Generator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        generate: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) anyerror!GenerateResult,
        deinit: ?*const fn (ptr: *anyopaque) void = null,
    };

    pub fn generate(self: Generator, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) !GenerateResult {
        return try self.vtable.generate(self.ptr, alloc, model, messages);
    }

    pub fn deinit(self: Generator) void {
        if (self.vtable.deinit) |deinit_fn| deinit_fn(self.ptr);
    }
};

pub const GeneratorFactory = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, cfg: GeneratorConfig) anyerror!Generator,
    };

    pub fn create(self: GeneratorFactory, alloc: std.mem.Allocator, cfg: GeneratorConfig) !Generator {
        return try self.vtable.create(self.ptr, alloc, cfg);
    }
};

pub fn executeChain(
    alloc: std.mem.Allocator,
    chain: []const ChainLink,
    factory: GeneratorFactory,
    messages: []const ChatMessage,
) !GenerateResult {
    if (chain.len == 0) return error.EmptyGeneratorChain;

    var last_err: anyerror = error.EmptyGeneratorChain;
    for (chain, 0..) |link, i| {
        try link.validate();
        var generator = factory.create(alloc, link.generator) catch |err| {
            last_err = err;
            if (i + 1 < chain.len and shouldTryNext(link.condition orelse .on_error, err)) continue;
            return err;
        };
        defer generator.deinit();

        const result = executeWithRetry(alloc, generator, link.generator.model, messages, link.retry) catch |err| {
            last_err = err;
            if (i + 1 < chain.len and shouldTryNext(link.condition orelse .on_error, err)) continue;
            return err;
        };
        return result;
    }
    return last_err;
}

fn executeWithRetry(
    alloc: std.mem.Allocator,
    generator: Generator,
    model: []const u8,
    messages: []const ChatMessage,
    retry_cfg: ?RetryConfig,
) !GenerateResult {
    const retry = retry_cfg orelse return try generator.generate(alloc, model, messages);
    try retry.validate();

    var attempt: u32 = 0;
    var backoff_ms = retry.initial_backoff_ms;
    while (true) : (attempt += 1) {
        const result = generator.generate(alloc, model, messages) catch |err| {
            if (attempt + 1 >= retry.max_attempts) return err;
            if (backoff_ms > 0) sleepMs(backoff_ms);
            backoff_ms = if (backoff_ms == 0)
                retry.max_backoff_ms
            else
                @as(u32, @intFromFloat(@min(
                    @as(f32, @floatFromInt(retry.max_backoff_ms)),
                    @as(f32, @floatFromInt(backoff_ms)) * retry.backoff_multiplier,
                )));
            continue;
        };
        return result;
    }
}

fn sleepMs(ms: u32) void {
    var req = std.posix.timespec{
        .sec = @intCast(ms / std.time.ms_per_s),
        .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) {
        const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
        switch (err) {
            .SUCCESS => return,
            .INTR => continue,
            else => return,
        }
    }
}

fn shouldTryNext(condition: ChainCondition, err: anyerror) bool {
    return switch (condition) {
        .always => true,
        .on_error => true,
        .on_timeout => isTimeoutError(err),
        .on_rate_limit => isRateLimitError(err),
    };
}

fn isTimeoutError(err: anyerror) bool {
    return err == error.Timeout or err == error.DeadlineExceeded;
}

fn isRateLimitError(err: anyerror) bool {
    return err == error.RateLimit or err == error.TooManyRequests;
}

fn cloneChainAlloc(alloc: std.mem.Allocator, chain: []const ChainLink) ![]ChainLink {
    const owned = try alloc.alloc(ChainLink, chain.len);
    var cloned_len: usize = 0;
    errdefer {
        for (owned[0..cloned_len]) |*link| link.deinit(alloc);
        alloc.free(owned);
    }

    for (chain, 0..) |link, i| {
        owned[i] = try link.clone(alloc);
        cloned_len = i + 1;
    }
    return owned;
}

fn deinitChainAlloc(alloc: std.mem.Allocator, chain: []ChainLink) void {
    for (chain) |*link| link.deinit(alloc);
    alloc.free(chain);
}

fn providerFromOpenApi(provider: openapi.GeneratorProvider) !Provider {
    return switch (provider) {
        .openai => .openai,
        .ollama => .ollama,
        .antfly => .antfly,
        .mock => .mock,
        else => error.UnsupportedGeneratorProvider,
    };
}

fn providerToOpenApi(provider: Provider) openapi.GeneratorProvider {
    return switch (provider) {
        .openai => .openai,
        .ollama => .ollama,
        .antfly => .antfly,
        .mock => .mock,
    };
}

fn chainConditionFromOpenApi(condition: openapi.ChainCondition) !ChainCondition {
    return switch (condition) {
        .always => .always,
        .on_error => .on_error,
        .on_timeout => .on_timeout,
        .on_rate_limit => .on_rate_limit,
    };
}

fn chainConditionToOpenApi(condition: ChainCondition) openapi.ChainCondition {
    return switch (condition) {
        .always => .always,
        .on_error => .on_error,
        .on_timeout => .on_timeout,
        .on_rate_limit => .on_rate_limit,
    };
}

test "generator config round trips through generating openapi types" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"antfly","model":"onnxruntime/Gemma-3-ONNX","api_url":"http://localhost:8082"}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(.antfly, cfg.provider);
    try std.testing.expectEqualStrings("http://localhost:8082", cfg.url);

    const encoded = try stringifyConfigAlloc(alloc, cfg);
    defer alloc.free(encoded);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.antfly, reparsed.provider);
}

test "chain link round trips through generating openapi types" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"generator":{"provider":"openai","model":"gpt-4.1","url":"https://api.openai.com/v1"},"retry":{"max_attempts":4,"initial_backoff_ms":250,"backoff_multiplier":3.0,"max_backoff_ms":4000},"condition":"on_rate_limit"}
    ;
    var link = try parseChainLinkFromSlice(alloc, raw);
    defer link.deinit(alloc);
    try std.testing.expectEqual(.openai, link.generator.provider);
    try std.testing.expectEqual(.on_rate_limit, link.condition.?);
    try std.testing.expectEqual(@as(u32, 4), link.retry.?.max_attempts);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), link.retry.?.backoff_multiplier, 0.0001);

    const encoded = try stringifyChainLinkAlloc(alloc, link);
    defer alloc.free(encoded);
    var reparsed = try parseChainLinkFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.openai, reparsed.generator.provider);
    try std.testing.expectEqual(.on_rate_limit, reparsed.condition.?);
}

test "generator config normalization and validation work" {
    const openai_cfg = GeneratorConfig.fromOpenAI(.{ .model = "gpt-4.1" });
    try std.testing.expectEqual(Provider.openai, openai_cfg.provider);
    try std.testing.expectEqualStrings("gpt-4.1", openai_cfg.getModel());
    try openai_cfg.validate();

    const ollama_cfg = GeneratorConfig.fromOllama(.{ .model = "llama3.1" });
    try std.testing.expectEqual(Provider.ollama, ollama_cfg.provider);
    try std.testing.expectEqualStrings("llama3.1", ollama_cfg.getModel());
    try ollama_cfg.validate();

    const invalid_cfg = GeneratorConfig{
        .provider = .openai,
        .model = "",
        .url = "",
    };
    try std.testing.expectError(error.InvalidGeneratorConfig, invalid_cfg.validate());
}

test "resolveGeneratorOrChain wraps a single generator" {
    const alloc = std.testing.allocator;
    const chain = try resolveGeneratorOrChain(alloc, GeneratorConfig.fromAntfly(.{ .model = "m1" }), &.{});
    defer deinitChainAlloc(alloc, chain);

    try std.testing.expectEqual(@as(usize, 1), chain.len);
    try std.testing.expectEqual(Provider.antfly, chain[0].generator.provider);
}

test "executeChain falls back on timeout and retries within a link" {
    const alloc = std.testing.allocator;

    const Fake = struct {
        first_attempts: usize = 0,

        const State = struct {
            first_attempts: *usize,
            cfg: GeneratorConfig,
        };

        fn create(ptr: *anyopaque, alloc_inner: std.mem.Allocator, cfg: GeneratorConfig) !Generator {
            _ = alloc_inner;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const state = try std.testing.allocator.create(State);
            state.* = .{ .first_attempts = &self.first_attempts, .cfg = cfg };
            return .{
                .ptr = state,
                .vtable = &.{
                    .generate = generate,
                    .deinit = destroy,
                },
            };
        }

        fn generate(ptr: *anyopaque, alloc_inner: std.mem.Allocator, _: []const u8, _: []const ChatMessage) !GenerateResult {
            const state: *State = @ptrCast(@alignCast(ptr));
            switch (state.cfg.provider) {
                .openai => {
                    state.first_attempts.* += 1;
                    if (state.first_attempts.* == 1) return error.Timeout;
                    return .{
                        .content = try alloc_inner.dupe(u8, "retry-success"),
                        .allocator = alloc_inner,
                    };
                },
                .antfly => {
                    return .{
                        .content = try alloc_inner.dupe(u8, "fallback-success"),
                        .allocator = alloc_inner,
                    };
                },
                else => return error.TestUnexpectedResult,
            }
        }

        fn destroy(ptr: *anyopaque) void {
            const state: *State = @ptrCast(@alignCast(ptr));
            std.testing.allocator.destroy(state);
        }
    };

    var fake = Fake{};
    const factory = GeneratorFactory{
        .ptr = &fake,
        .vtable = &.{ .create = Fake.create },
    };

    const retry_chain = [_]ChainLink{
        .{
            .generator = GeneratorConfig.fromOpenAI(.{ .model = "gpt-4.1" }),
            .retry = .{ .max_attempts = 2, .initial_backoff_ms = 1, .max_backoff_ms = 1 },
        },
    };

    var retried = try executeChain(alloc, &retry_chain, factory, &.{.{ .role = .user, .content = "hello" }});
    defer retried.deinit();
    try std.testing.expectEqualStrings("retry-success", retried.content);

    fake.first_attempts = 0;
    const fallback_chain = [_]ChainLink{
        .{
            .generator = GeneratorConfig.fromOpenAI(.{ .model = "gpt-4.1" }),
            .condition = .on_timeout,
            .retry = .{ .max_attempts = 1, .initial_backoff_ms = 1, .max_backoff_ms = 1 },
        },
        .{
            .generator = GeneratorConfig.fromAntfly(.{ .model = "local" }),
        },
    };

    var fallback = try executeChain(alloc, &fallback_chain, factory, &.{.{ .role = .user, .content = "hello" }});
    defer fallback.deinit();
    try std.testing.expectEqualStrings("fallback-success", fallback.content);
}

test "executeChain falls back on rate limit" {
    const alloc = std.testing.allocator;

    const Fake = struct {
        const State = struct {
            cfg: GeneratorConfig,
        };

        fn create(_: *anyopaque, _: std.mem.Allocator, cfg: GeneratorConfig) !Generator {
            const state = try std.testing.allocator.create(State);
            state.* = .{ .cfg = cfg };
            return .{
                .ptr = state,
                .vtable = &.{
                    .generate = generate,
                    .deinit = destroy,
                },
            };
        }

        fn generate(ptr: *anyopaque, alloc_inner: std.mem.Allocator, _: []const u8, _: []const ChatMessage) !GenerateResult {
            const state: *State = @ptrCast(@alignCast(ptr));
            return switch (state.cfg.provider) {
                .openai => error.RateLimit,
                .antfly => .{
                    .content = try alloc_inner.dupe(u8, "rate-limit-fallback"),
                    .allocator = alloc_inner,
                },
                else => error.TestUnexpectedResult,
            };
        }

        fn destroy(ptr: *anyopaque) void {
            const state: *State = @ptrCast(@alignCast(ptr));
            std.testing.allocator.destroy(state);
        }
    };

    const factory = GeneratorFactory{
        .ptr = undefined,
        .vtable = &.{ .create = Fake.create },
    };

    const chain = [_]ChainLink{
        .{
            .generator = GeneratorConfig.fromOpenAI(.{ .model = "gpt-4.1" }),
            .condition = .on_rate_limit,
        },
        .{
            .generator = GeneratorConfig.fromAntfly(.{ .model = "local" }),
        },
    };

    var result = try executeChain(alloc, &chain, factory, &.{.{ .role = .user, .content = "hello" }});
    defer result.deinit();
    try std.testing.expectEqualStrings("rate-limit-fallback", result.content);
}
