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
const embeddings = @import("antfly_embeddings");
const chunking = @import("antfly_chunking");
const generating = @import("antfly_generating");
const reranking = @import("antfly_reranking");

const Allocator = std.mem.Allocator;

pub const ChainLinkConfig = struct {
    generator_name: ?[]const u8 = null,
    generator_config: ?generating.GeneratorConfig = null,
    retry: ?generating.RetryConfig = null,
    condition: ?generating.ChainCondition = null,

    pub fn clone(self: ChainLinkConfig, alloc: Allocator) !ChainLinkConfig {
        return .{
            .generator_name = if (self.generator_name) |generator_name| try alloc.dupe(u8, generator_name) else null,
            .generator_config = if (self.generator_config) |generator_config| try generator_config.clone(alloc) else null,
            .retry = self.retry,
            .condition = self.condition,
        };
    }

    pub fn deinit(self: *ChainLinkConfig, alloc: Allocator) void {
        if (self.generator_name) |generator_name| alloc.free(generator_name);
        if (self.generator_config) |*generator_config| generator_config.deinit(alloc);
        self.* = undefined;
    }

    pub fn validate(self: ChainLinkConfig) !void {
        if ((self.generator_name == null) == (self.generator_config == null)) {
            return error.InvalidChainLinkConfig;
        }
        if (self.generator_name) |generator_name| {
            if (generator_name.len == 0) return error.InvalidChainLinkConfig;
        }
        if (self.generator_config) |generator_config| {
            try generator_config.validate();
        }
        if (self.retry) |retry| try retry.validate();
    }
};

pub const Registry = struct {
    allocator: Allocator,
    generator_configs: std.StringArrayHashMapUnmanaged(generating.GeneratorConfig) = .{},
    default_generator: ?[]const u8 = null,
    chains: std.StringArrayHashMapUnmanaged([]ChainLinkConfig) = .{},
    default_chain: ?[]const u8 = null,
    reranker_configs: std.StringArrayHashMapUnmanaged(reranking.Config) = .{},
    default_reranker: ?[]const u8 = null,
    embedder_configs: std.StringArrayHashMapUnmanaged(embeddings.Config) = .{},
    default_embedder: ?[]const u8 = null,
    chunker_configs: std.StringArrayHashMapUnmanaged(chunking.Config) = .{},
    default_chunker: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Registry {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Registry) void {
        var generator_it = self.generator_configs.iterator();
        while (generator_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.generator_configs.deinit(self.allocator);

        var chain_it = self.chains.iterator();
        while (chain_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            for (entry.value_ptr.*) |*link| link.deinit(self.allocator);
            self.allocator.free(entry.value_ptr.*);
        }
        self.chains.deinit(self.allocator);

        var reranker_it = self.reranker_configs.iterator();
        while (reranker_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.reranker_configs.deinit(self.allocator);

        var embedder_it = self.embedder_configs.iterator();
        while (embedder_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.embedder_configs.deinit(self.allocator);

        var chunker_it = self.chunker_configs.iterator();
        while (chunker_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.chunker_configs.deinit(self.allocator);

        self.* = undefined;
    }

    pub fn parseFromSlice(alloc: Allocator, raw: []const u8) !Registry {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
        defer parsed.deinit();
        return try parseFromValue(alloc, parsed.value);
    }

    pub fn parseFromValue(alloc: Allocator, value: std.json.Value) !Registry {
        if (value != .object) return error.InvalidRegistryConfig;

        var registry = Registry.init(alloc);
        errdefer registry.deinit();

        if (value.object.get("generators")) |generators_value| {
            try parseGenerators(&registry, generators_value);
        }
        if (value.object.get("rerankers")) |rerankers_value| {
            try parseRerankers(&registry, rerankers_value);
        }
        if (value.object.get("embedders")) |embedders_value| {
            try parseEmbedders(&registry, embedders_value);
        }
        if (value.object.get("chains")) |chains_value| {
            try parseChains(&registry, chains_value);
        }
        if (value.object.get("chunkers")) |chunkers_value| {
            try parseChunkers(&registry, chunkers_value);
        }

        return registry;
    }

    pub fn defaultGeneratorName(self: *const Registry) ?[]const u8 {
        return self.default_generator;
    }

    pub fn defaultChainName(self: *const Registry) ?[]const u8 {
        return self.default_chain;
    }

    pub fn defaultRerankerName(self: *const Registry) ?[]const u8 {
        return self.default_reranker;
    }

    pub fn defaultEmbedderName(self: *const Registry) ?[]const u8 {
        return self.default_embedder;
    }

    pub fn defaultChunkerName(self: *const Registry) ?[]const u8 {
        return self.default_chunker;
    }

    pub fn getGeneratorConfig(self: *const Registry, name: ?[]const u8) !generating.GeneratorConfig {
        const resolved = name orelse self.default_generator orelse return error.NoDefaultGenerator;
        return self.generator_configs.get(resolved) orelse return error.UnknownGenerator;
    }

    pub fn getRerankerConfig(self: *const Registry, name: ?[]const u8) !reranking.Config {
        const resolved = name orelse self.default_reranker orelse return error.NoDefaultReranker;
        return self.reranker_configs.get(resolved) orelse return error.UnknownReranker;
    }

    pub fn getEmbedderConfig(self: *const Registry, name: ?[]const u8) !embeddings.Config {
        const resolved = name orelse self.default_embedder orelse return error.NoDefaultEmbedder;
        return self.embedder_configs.get(resolved) orelse return error.UnknownEmbedder;
    }

    pub fn getChain(self: *const Registry, name: ?[]const u8) ![]const ChainLinkConfig {
        const resolved = name orelse self.default_chain orelse return error.NoDefaultChain;
        return self.chains.get(resolved) orelse return error.UnknownChain;
    }

    pub fn getChunkerConfig(self: *const Registry, name: ?[]const u8) !chunking.Config {
        const resolved = name orelse self.default_chunker orelse return error.NoDefaultChunker;
        return self.chunker_configs.get(resolved) orelse return error.UnknownChunker;
    }

    pub fn registerGeneratorConfig(self: *Registry, name: []const u8, cfg: generating.GeneratorConfig) !void {
        try cfg.validate();
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        var owned_cfg = try cfg.clone(self.allocator);
        errdefer owned_cfg.deinit(self.allocator);

        const gop = try self.generator_configs.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            owned_cfg.deinit(self.allocator);
            return error.DuplicateGeneratorName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned_cfg;
        if (self.default_generator == null) self.default_generator = gop.key_ptr.*;
    }

    pub fn registerRerankerConfig(self: *Registry, name: []const u8, cfg: reranking.Config) !void {
        try cfg.validate();
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        var owned_cfg = try cfg.clone(self.allocator);
        errdefer owned_cfg.deinit(self.allocator);

        const gop = try self.reranker_configs.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            owned_cfg.deinit(self.allocator);
            return error.DuplicateRerankerName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned_cfg;
        if (self.default_reranker == null) self.default_reranker = gop.key_ptr.*;
    }

    pub fn registerEmbedderConfig(self: *Registry, name: []const u8, cfg: embeddings.Config) !void {
        try cfg.validate();
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        var owned_cfg = try cfg.clone(self.allocator);
        errdefer owned_cfg.deinit(self.allocator);

        const gop = try self.embedder_configs.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            owned_cfg.deinit(self.allocator);
            return error.DuplicateEmbedderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned_cfg;
        if (self.default_embedder == null) self.default_embedder = gop.key_ptr.*;
    }

    pub fn registerChunkerConfig(self: *Registry, name: []const u8, cfg: chunking.Config) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        var owned_cfg = try cfg.clone(self.allocator);
        errdefer owned_cfg.deinit(self.allocator);

        const gop = try self.chunker_configs.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            owned_cfg.deinit(self.allocator);
            return error.DuplicateChunkerName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned_cfg;
        if (self.default_chunker == null) self.default_chunker = gop.key_ptr.*;
    }

    pub fn registerChain(self: *Registry, name: []const u8, links: []const ChainLinkConfig) !void {
        if (links.len == 0) return error.InvalidChainConfig;

        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const owned_links = try cloneChainLinks(self.allocator, links);
        errdefer {
            for (owned_links) |*link| link.deinit(self.allocator);
            self.allocator.free(owned_links);
        }

        for (owned_links) |link| {
            try link.validate();
            if (link.generator_name) |generator_name| {
                if (!self.generator_configs.contains(generator_name)) return error.UnknownGenerator;
            }
        }

        const gop = try self.chains.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            for (owned_links) |*link| link.deinit(self.allocator);
            self.allocator.free(owned_links);
            return error.DuplicateChainName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned_links;
        if (self.default_chain == null) self.default_chain = gop.key_ptr.*;
    }
};

fn parseGenerators(registry: *Registry, value: std.json.Value) !void {
    if (value != .object) return error.InvalidRegistryConfig;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        var cfg = try generating.parseConfigFromValue(registry.allocator, entry.value_ptr.*);
        defer cfg.deinit(registry.allocator);
        try registry.registerGeneratorConfig(entry.key_ptr.*, cfg);
    }
}

fn parseRerankers(registry: *Registry, value: std.json.Value) !void {
    if (value != .object) return error.InvalidRegistryConfig;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        var cfg = try reranking.parseConfigFromValue(registry.allocator, entry.value_ptr.*);
        defer cfg.deinit(registry.allocator);
        try registry.registerRerankerConfig(entry.key_ptr.*, cfg);
    }
}

fn parseEmbedders(registry: *Registry, value: std.json.Value) !void {
    if (value != .object) return error.InvalidRegistryConfig;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        var cfg = try embeddings.parseConfigFromValue(registry.allocator, entry.value_ptr.*);
        defer cfg.deinit(registry.allocator);
        try registry.registerEmbedderConfig(entry.key_ptr.*, cfg);
    }
}

fn parseChains(registry: *Registry, value: std.json.Value) !void {
    if (value != .object) return error.InvalidRegistryConfig;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .array) return error.InvalidRegistryConfig;
        const links = try registry.allocator.alloc(ChainLinkConfig, entry.value_ptr.array.items.len);
        var parsed_len: usize = 0;
        defer {
            for (links[0..parsed_len]) |*link| link.deinit(registry.allocator);
            registry.allocator.free(links);
        }

        for (entry.value_ptr.array.items, 0..) |item, i| {
            links[i] = try parseNamedChainLink(registry.allocator, item);
            parsed_len = i + 1;
        }
        try registry.registerChain(entry.key_ptr.*, links);
    }
}

fn parseChunkers(registry: *Registry, value: std.json.Value) !void {
    if (value != .object) return error.InvalidRegistryConfig;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        var cfg = try chunking.parseConfigFromValue(registry.allocator, entry.value_ptr.*);
        defer cfg.deinit(registry.allocator);
        try registry.registerChunkerConfig(entry.key_ptr.*, cfg);
    }
}

fn parseNamedChainLink(alloc: Allocator, value: std.json.Value) !ChainLinkConfig {
    if (value != .object) return error.InvalidChainLinkConfig;

    var out = ChainLinkConfig{};
    errdefer out.deinit(alloc);

    if (value.object.get("generator")) |generator_value| {
        if (generator_value != .string) return error.InvalidChainLinkConfig;
        out.generator_name = try alloc.dupe(u8, generator_value.string);
    }
    if (value.object.get("generator_config")) |generator_config_value| {
        out.generator_config = try generating.parseConfigFromValue(alloc, generator_config_value);
    }
    if (value.object.get("retry")) |retry_value| {
        out.retry = try generating.parseRetryConfigFromValue(alloc, retry_value);
    }
    if (value.object.get("condition")) |condition_value| {
        if (condition_value != .string) return error.InvalidChainLinkConfig;
        out.condition = try parseChainCondition(condition_value.string);
    }

    try out.validate();
    return out;
}

fn parseChainCondition(raw: []const u8) !generating.ChainCondition {
    if (std.mem.eql(u8, raw, "always")) return .always;
    if (std.mem.eql(u8, raw, "on_error")) return .on_error;
    if (std.mem.eql(u8, raw, "on_timeout")) return .on_timeout;
    if (std.mem.eql(u8, raw, "on_rate_limit")) return .on_rate_limit;
    return error.InvalidChainLinkConfig;
}

fn cloneChainLinks(alloc: Allocator, links: []const ChainLinkConfig) ![]ChainLinkConfig {
    const owned = try alloc.alloc(ChainLinkConfig, links.len);
    var cloned_len: usize = 0;
    errdefer {
        for (owned[0..cloned_len]) |*link| link.deinit(alloc);
        alloc.free(owned);
    }

    for (links, 0..) |link, i| {
        owned[i] = try link.clone(alloc);
        cloned_len = i + 1;
    }
    return owned;
}

test "provider registry parses named generators rerankers and chains" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "generators": {
        \\    "primary": { "provider": "antfly", "model": "m1", "api_url": "http://localhost:8082" },
        \\    "fallback": { "provider": "ollama", "model": "llama3", "url": "http://localhost:11434" }
        \\  },
        \\  "chains": {
        \\    "default": [
        \\      { "generator": "primary", "retry": { "max_attempts": 2 } },
        \\      { "generator_config": { "provider": "mock" }, "condition": "always" }
        \\    ]
        \\  },
        \\  "rerankers": {
        \\    "cross-encoder": { "provider": "antfly", "model": "rerank", "field": "body", "url": "http://localhost:8082" }
        \\  }
        \\}
    ;

    var registry = try Registry.parseFromSlice(alloc, raw);
    defer registry.deinit();

    try std.testing.expectEqualStrings("primary", registry.defaultGeneratorName().?);
    try std.testing.expectEqualStrings("default", registry.defaultChainName().?);
    try std.testing.expectEqualStrings("cross-encoder", registry.defaultRerankerName().?);

    const generator_cfg = try registry.getGeneratorConfig(null);
    try std.testing.expectEqual(.antfly, generator_cfg.provider);

    const chain = try registry.getChain(null);
    try std.testing.expectEqual(@as(usize, 2), chain.len);
    try std.testing.expectEqualStrings("primary", chain[0].generator_name.?);
    try std.testing.expect(chain[1].generator_config != null);

    const reranker_cfg = try registry.getRerankerConfig(null);
    try std.testing.expectEqual(.antfly, reranker_cfg.provider);
    try std.testing.expectEqualStrings("body", reranker_cfg.field);
}

test "provider registry parses named embedders and tracks default" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "embedders": {
        \\    "semantic-default": {
        \\      "provider": "openai",
        \\      "model": "text-embedding-3-small",
        \\      "url": "https://api.openai.com",
        \\      "dimensions": 1536
        \\    }
        \\  }
        \\}
    ;

    var registry = try Registry.parseFromSlice(alloc, raw);
    defer registry.deinit();

    try std.testing.expectEqualStrings("semantic-default", registry.defaultEmbedderName().?);
    const cfg = try registry.getEmbedderConfig(null);
    try std.testing.expectEqual(.openai, cfg.provider);
    try std.testing.expectEqual(@as(?u32, 1536), cfg.dimensions);
}

test "provider registry parses named chunkers and tracks default" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "chunkers": {
        \\    "fixed": {
        \\      "provider": "antfly",
        \\      "api_url": "http://localhost:8082",
        \\      "model": "fixed",
        \\      "text": { "target_tokens": 256, "overlap_tokens": 32 }
        \\    }
        \\  }
        \\}
    ;

    var registry = try Registry.parseFromSlice(alloc, raw);
    defer registry.deinit();

    try std.testing.expectEqualStrings("fixed", registry.defaultChunkerName().?);
    const cfg = try registry.getChunkerConfig(null);
    try std.testing.expectEqual(.antfly, cfg.provider);
    try std.testing.expectEqual(@as(u32, 256), cfg.text.target_tokens);
}

test "provider registry rejects unknown named generators in chains" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "generators": {
        \\    "primary": { "provider": "mock" }
        \\  },
        \\  "chains": {
        \\    "default": [
        \\      { "generator": "missing" }
        \\    ]
        \\  }
        \\}
    ;

    try std.testing.expectError(error.UnknownGenerator, Registry.parseFromSlice(alloc, raw));
}

test "provider registry rejects invalid chain link sources" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "chains": {
        \\    "default": [
        \\      {
        \\        "generator": "primary",
        \\        "generator_config": { "provider": "mock" }
        \\      }
        \\    ]
        \\  }
        \\}
    ;

    try std.testing.expectError(error.InvalidChainLinkConfig, Registry.parseFromSlice(alloc, raw));
}
