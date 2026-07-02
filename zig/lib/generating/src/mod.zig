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
    tool,

    pub fn toSlice(self: Role) []const u8 {
        return switch (self) {
            .system => "system",
            .user => "user",
            .assistant => "assistant",
            .tool => "tool",
        };
    }
};

pub const ImageURL = struct {
    url: []const u8,
};

pub const MediaContent = struct {
    data: []const u8 = "",
    mime_type: []const u8 = "",
    url: ?[]const u8 = null,
};

pub const ContentPart = union(enum) {
    text: []const u8,
    image_url: ImageURL,
    media: MediaContent,
};

pub const ChatMessageContent = union(enum) {
    text: []const u8,
    parts: []const ContentPart,
};

pub const ToolCall = struct {
    id: []const u8,
    name: []const u8,
    arguments: []const u8,

    pub fn deinit(self: *ToolCall, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.name);
        allocator.free(self.arguments);
        self.* = undefined;
    }
};

pub const ChatMessage = struct {
    role: Role,
    content: ?ChatMessageContent = null,
    tool_calls: ?[]const ToolCall = null,
    tool_call_id: ?[]const u8 = null,
};

pub const GenerateResult = struct {
    content: []const u8,
    tool_calls: []ToolCall = &.{},
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerateResult) void {
        self.allocator.free(self.content);
        for (self.tool_calls) |*tool_call| tool_call.deinit(self.allocator);
        if (self.tool_calls.len > 0) self.allocator.free(self.tool_calls);
        self.* = undefined;
    }
};

pub const Provider = enum {
    gemini,
    vertex,
    openai,
    ollama,
    antfly,
    mock,

    pub fn validate(self: Provider) !void {
        _ = self;
    }
};

/// Antfly safety cap for generation requests that omit an explicit limit.
/// This is intentionally not provider parity: OpenAI leaves the response cap
/// optional, and Ollama's native num_predict default is unbounded.
pub const default_max_tokens: i64 = 256;

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
    project_id: ?[]const u8 = null,
    location: ?[]const u8 = null,
    credentials_path: ?[]const u8 = null,
    tools_json: ?[]const u8 = null,
    tool_choice_json: ?[]const u8 = null,
    max_tokens: i64 = default_max_tokens,

    pub fn clone(self: GeneratorConfig, alloc: std.mem.Allocator) !GeneratorConfig {
        return .{
            .provider = self.provider,
            .model = if (self.model.len > 0) try alloc.dupe(u8, self.model) else "",
            .url = if (self.url.len > 0) try alloc.dupe(u8, self.url) else "",
            .api_key = if (self.api_key) |api_key| try alloc.dupe(u8, api_key) else null,
            .project_id = if (self.project_id) |value| try alloc.dupe(u8, value) else null,
            .location = if (self.location) |value| try alloc.dupe(u8, value) else null,
            .credentials_path = if (self.credentials_path) |value| try alloc.dupe(u8, value) else null,
            .tools_json = if (self.tools_json) |value| try alloc.dupe(u8, value) else null,
            .tool_choice_json = if (self.tool_choice_json) |value| try alloc.dupe(u8, value) else null,
            .max_tokens = self.max_tokens,
        };
    }

    pub fn deinit(self: *GeneratorConfig, alloc: std.mem.Allocator) void {
        if (self.model.len > 0) alloc.free(self.model);
        if (self.url.len > 0) alloc.free(self.url);
        if (self.api_key) |api_key| alloc.free(api_key);
        if (self.project_id) |value| alloc.free(value);
        if (self.location) |value| alloc.free(value);
        if (self.credentials_path) |value| alloc.free(value);
        if (self.tools_json) |value| alloc.free(value);
        if (self.tool_choice_json) |value| alloc.free(value);
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
        if (self.url.len == 0 and self.provider != .mock and self.provider != .antfly and self.provider != .vertex and self.provider != .gemini) return error.InvalidGeneratorConfig;
        if (self.max_tokens <= 0) return error.InvalidGeneratorConfig;
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
        .project_id = if (generated.project_id) |project_id| try alloc.dupe(u8, project_id) else null,
        .location = if (generated.location) |location| try alloc.dupe(u8, location) else null,
        .credentials_path = if (generated.credentials_path) |credentials_path| try alloc.dupe(u8, credentials_path) else null,
        .max_tokens = generated.max_tokens orelse default_max_tokens,
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
            .openai, .ollama, .gemini, .vertex, .mock => if (cfg.url.len > 0) cfg.url else null,
            .antfly => null,
        },
        .api_url = switch (cfg.provider) {
            .antfly => if (cfg.url.len > 0) cfg.url else null,
            else => null,
        },
        .api_key = cfg.api_key,
        .project_id = cfg.project_id,
        .location = cfg.location,
        .credentials_path = cfg.credentials_path,
        .max_tokens = cfg.max_tokens,
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
        set_request_timeout_ms: ?*const fn (ptr: *anyopaque, timeout_ms: u64) void = null,
        deinit: ?*const fn (ptr: *anyopaque) void = null,
    };

    pub fn generate(self: Generator, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) !GenerateResult {
        return try self.vtable.generate(self.ptr, alloc, model, messages);
    }

    pub fn setRequestTimeoutMs(self: Generator, timeout_ms: u64) void {
        if (self.vtable.set_request_timeout_ms) |set_fn| set_fn(self.ptr, timeout_ms);
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
    return try executeChainWithDeadline(alloc, chain, factory, messages, null);
}

pub fn executeChainWithTimeoutMs(
    alloc: std.mem.Allocator,
    chain: []const ChainLink,
    factory: GeneratorFactory,
    messages: []const ChatMessage,
    timeout_ms: u64,
) !GenerateResult {
    return try executeChainWithDeadline(alloc, chain, factory, messages, try Deadline.start(timeout_ms));
}

fn executeChainWithDeadline(
    alloc: std.mem.Allocator,
    chain: []const ChainLink,
    factory: GeneratorFactory,
    messages: []const ChatMessage,
    deadline: ?Deadline,
) !GenerateResult {
    if (chain.len == 0) return error.EmptyGeneratorChain;

    var last_err: anyerror = error.EmptyGeneratorChain;
    for (chain, 0..) |link, i| {
        if (deadline) |d| {
            if (d.expired()) return error.DeadlineExceeded;
        }
        try link.validate();
        var generator = factory.create(alloc, link.generator) catch |err| {
            last_err = err;
            if (i + 1 < chain.len and shouldTryNext(link.condition orelse .on_error, err)) continue;
            return err;
        };
        defer generator.deinit();

        const result = executeWithRetry(alloc, generator, link.generator.model, messages, link.retry, deadline) catch |err| {
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
    deadline: ?Deadline,
) !GenerateResult {
    if (retry_cfg == null) {
        if (deadline) |d| generator.setRequestTimeoutMs(try d.remainingMsOrDeadline());
        return try generator.generate(alloc, model, messages);
    }
    const retry = retry_cfg.?;
    try retry.validate();

    var attempt: u32 = 0;
    var backoff_ms = retry.initial_backoff_ms;
    while (true) : (attempt += 1) {
        if (deadline) |d| generator.setRequestTimeoutMs(try d.remainingMsOrDeadline());
        const result = generator.generate(alloc, model, messages) catch |err| {
            if (attempt + 1 >= retry.max_attempts) return err;
            if (backoff_ms > 0) {
                if (deadline) |d| {
                    const sleep_ms = @min(@as(u64, backoff_ms), try d.remainingMsOrDeadline());
                    sleepMs(@intCast(sleep_ms));
                    if (d.expired()) return error.DeadlineExceeded;
                } else {
                    sleepMs(backoff_ms);
                }
            }
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

const Deadline = struct {
    deadline_ns: u64,

    fn start(timeout_ms: u64) !Deadline {
        const now_ns = monotonicNowNs() orelse return error.DeadlineExceeded;
        const timeout_ns = std.math.mul(u64, timeout_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
        return .{ .deadline_ns = std.math.add(u64, now_ns, timeout_ns) catch std.math.maxInt(u64) };
    }

    fn expired(self: Deadline) bool {
        const now_ns = monotonicNowNs() orelse return true;
        return now_ns >= self.deadline_ns;
    }

    fn remainingMsOrDeadline(self: Deadline) !u64 {
        const now_ns = monotonicNowNs() orelse return error.DeadlineExceeded;
        if (now_ns >= self.deadline_ns) return error.DeadlineExceeded;
        const remaining_ns = self.deadline_ns - now_ns;
        const remaining_ms = (remaining_ns + std.time.ns_per_ms - 1) / std.time.ns_per_ms;
        return @max(@as(u64, 1), remaining_ms);
    }
};

fn monotonicNowNs() ?u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return null,
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
        .gemini => .gemini,
        .vertex => .vertex,
        .openai => .openai,
        .ollama => .ollama,
        .antfly => .antfly,
        .mock => .mock,
        else => error.UnsupportedGeneratorProvider,
    };
}

fn providerToOpenApi(provider: Provider) openapi.GeneratorProvider {
    return switch (provider) {
        .gemini => .gemini,
        .vertex => .vertex,
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
    try std.testing.expectEqual(default_max_tokens, cfg.max_tokens);

    const encoded = try stringifyConfigAlloc(alloc, cfg);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"max_tokens\":256") != null);
    var reparsed = try parseConfigFromSlice(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(.antfly, reparsed.provider);
    try std.testing.expectEqual(default_max_tokens, reparsed.max_tokens);
}

test "generator config preserves explicit max_tokens" {
    const alloc = std.testing.allocator;
    const raw =
        \\{"provider":"openai","model":"gpt-4.1","url":"https://api.openai.com/v1","max_tokens":128}
    ;
    var cfg = try parseConfigFromSlice(alloc, raw);
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 128), cfg.max_tokens);

    const invalid =
        \\{"provider":"openai","model":"gpt-4.1","url":"https://api.openai.com/v1","max_tokens":0}
    ;
    try std.testing.expectError(error.InvalidGeneratorConfig, parseConfigFromSlice(alloc, invalid));
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

    var retried = try executeChain(alloc, &retry_chain, factory, &.{.{ .role = .user, .content = .{ .text = "hello" } }});
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

    var fallback = try executeChain(alloc, &fallback_chain, factory, &.{.{ .role = .user, .content = .{ .text = "hello" } }});
    defer fallback.deinit();
    try std.testing.expectEqualStrings("fallback-success", fallback.content);
}

test "executeChainWithTimeoutMs bounds retries and fallback by one deadline" {
    const alloc = std.testing.allocator;

    const Fake = struct {
        const Self = @This();

        attempts: usize = 0,
        fallback_attempts: usize = 0,
        set_timeout_calls: usize = 0,
        last_timeout_ms: u64 = 0,

        const State = struct {
            parent: *Self,
            cfg: GeneratorConfig,
        };

        fn create(ptr: *anyopaque, _: std.mem.Allocator, cfg: GeneratorConfig) !Generator {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const state = try std.testing.allocator.create(State);
            state.* = .{ .parent = self, .cfg = cfg };
            return .{
                .ptr = state,
                .vtable = &.{
                    .generate = generate,
                    .set_request_timeout_ms = setRequestTimeoutMs,
                    .deinit = destroy,
                },
            };
        }

        fn setRequestTimeoutMs(ptr: *anyopaque, timeout_ms: u64) void {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.parent.set_timeout_calls += 1;
            state.parent.last_timeout_ms = timeout_ms;
        }

        fn generate(ptr: *anyopaque, alloc_inner: std.mem.Allocator, _: []const u8, _: []const ChatMessage) !GenerateResult {
            _ = alloc_inner;
            const state: *State = @ptrCast(@alignCast(ptr));
            switch (state.cfg.provider) {
                .openai => {
                    state.parent.attempts += 1;
                    sleepMs(2);
                    return error.Timeout;
                },
                .antfly => {
                    state.parent.fallback_attempts += 1;
                    return error.TestUnexpectedResult;
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
    const chain = [_]ChainLink{
        .{
            .generator = GeneratorConfig.fromOpenAI(.{ .model = "gpt-4.1" }),
            .condition = .on_timeout,
            .retry = .{ .max_attempts = 2, .initial_backoff_ms = 50, .max_backoff_ms = 50 },
        },
        .{
            .generator = GeneratorConfig.fromAntfly(.{ .model = "local" }),
        },
    };

    try std.testing.expectError(
        error.DeadlineExceeded,
        executeChainWithTimeoutMs(alloc, &chain, factory, &.{.{ .role = .user, .content = .{ .text = "hello" } }}, 1),
    );
    try std.testing.expectEqual(@as(usize, 1), fake.attempts);
    try std.testing.expectEqual(@as(usize, 0), fake.fallback_attempts);
    try std.testing.expect(fake.set_timeout_calls >= 1);
    try std.testing.expect(fake.last_timeout_ms >= 1);
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

    var result = try executeChain(alloc, &chain, factory, &.{.{ .role = .user, .content = .{ .text = "hello" } }});
    defer result.deinit();
    try std.testing.expectEqualStrings("rate-limit-fallback", result.content);
}
