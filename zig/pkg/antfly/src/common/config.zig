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
const common_openapi = @import("antfly_common_openapi");
const logging_openapi = @import("antfly_logging_openapi");
const middleware_openapi = @import("antfly_middleware_openapi");
const scraping = @import("antfly_scraping");
const scraping_openapi = @import("antfly_scraping_openapi");
const s3_openapi = @import("antfly_s3_openapi");
const provider_registry = @import("provider_registry.zig");
const secrets = @import("secrets.zig");
const transcribing = @import("antfly_transcribing");
const synthesizing = @import("antfly_synthesizing");

const default_max_shard_size_bytes: u64 = 64 * 1024 * 1024;
const default_max_shards_per_table: u32 = 20;
const default_config_shards_per_table: u32 = 3;
const default_swarm_shards_per_table: u32 = 1;
pub const default_health_port: u16 = 4200;

pub const Config = struct {
    registry: provider_registry.Registry,
    speech_to_text: transcribing.Registry,
    text_to_speech: synthesizing.Registry,
    auth_enabled: bool = false,
    swarm_mode: bool = false,
    health_enabled: bool = true,
    health_port: ?u16 = null,
    registry_url: ?[]u8 = null,
    log: ?logging_openapi.Config = null,
    tls: ?TlsConfig = null,
    cors: ?CorsConfig = null,
    metadata: MetadataConfig = .{},
    storage: StorageConfig = .{},
    termite: TermiteConfig = .{},
    remote_content: ?RemoteContentConfig = null,
    shard_allocation: ShardAllocationConfig = .{},

    pub const MetadataConfig = struct {
        pub const NodeUrl = struct {
            node_id: u64,
            url: []u8,
        };

        orchestration_urls: []NodeUrl = &.{},
        raft_urls: []NodeUrl = &.{},

        fn deinit(self: *MetadataConfig, alloc: std.mem.Allocator) void {
            for (self.orchestration_urls) |entry| alloc.free(entry.url);
            if (self.orchestration_urls.len > 0) alloc.free(self.orchestration_urls);
            for (self.raft_urls) |entry| alloc.free(entry.url);
            if (self.raft_urls.len > 0) alloc.free(self.raft_urls);
            self.* = undefined;
        }
    };

    pub const TlsConfig = struct {
        cert: ?[]u8 = null,
        key: ?[]u8 = null,

        fn deinit(self: *TlsConfig, alloc: std.mem.Allocator) void {
            if (self.cert) |value| alloc.free(value);
            if (self.key) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    pub const StorageConfig = struct {
        local_base_dir: ?[]u8 = null,
        data_backend: ?common_openapi.StorageBackend = null,
        metadata_backend: ?common_openapi.StorageBackend = null,
        s3_bucket: ?[]u8 = null,
        s3_prefix: ?[]u8 = null,

        fn deinit(self: *StorageConfig, alloc: std.mem.Allocator) void {
            if (self.local_base_dir) |value| alloc.free(value);
            if (self.s3_bucket) |value| alloc.free(value);
            if (self.s3_prefix) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    pub const TermiteConfig = struct {
        api_url: ?[]u8 = null,
        models_dir: ?[]u8 = null,
        content_security: ?ContentSecurityConfig = null,
        s3_credentials: ?S3CredentialsConfig = null,

        fn deinit(self: *TermiteConfig, alloc: std.mem.Allocator) void {
            if (self.api_url) |value| alloc.free(value);
            if (self.models_dir) |value| alloc.free(value);
            if (self.content_security) |*security| security.deinit(alloc);
            if (self.s3_credentials) |*credentials| credentials.deinit(alloc);
            self.* = undefined;
        }
    };

    pub const S3CredentialsConfig = scraping.S3CredentialsConfig;
    pub const ContentSecurityConfig = scraping.ContentSecurityConfig;

    pub const CorsConfig = struct {
        enabled: ?bool = null,
        allowed_origins: ?[]const []u8 = null,
        allowed_methods: ?[]const []u8 = null,
        allowed_headers: ?[]const []u8 = null,
        exposed_headers: ?[]const []u8 = null,
        allow_credentials: ?bool = null,
        max_age: ?u32 = null,

        fn deinit(self: *CorsConfig, alloc: std.mem.Allocator) void {
            if (self.allowed_origins) |values| freeOwnedStringSlice(alloc, values);
            if (self.allowed_methods) |values| freeOwnedStringSlice(alloc, values);
            if (self.allowed_headers) |values| freeOwnedStringSlice(alloc, values);
            if (self.exposed_headers) |values| freeOwnedStringSlice(alloc, values);
            self.* = undefined;
        }
    };

    pub const S3CredentialConfig = scraping.S3CredentialConfig;
    pub const HTTPCredentialConfig = scraping.HTTPCredentialConfig;
    pub const RemoteContentConfig = scraping.RemoteContentConfig;

    pub const ShardAllocationConfig = struct {
        default_shards_per_table: u32 = default_config_shards_per_table,
        max_shard_size_bytes: u64 = default_max_shard_size_bytes,
        min_shard_size_bytes: u64 = 0,
        min_shards_per_table: u32 = 1,
        max_shards_per_table: u32 = default_max_shards_per_table,
        disable_shard_alloc: bool = true,
        auto_range_transition_per_table_limit: u32 = 1,
        auto_range_transition_cluster_limit: u32 = 1,
        shard_cooldown_millis: u64 = 60 * std.time.ms_per_s,
        min_shard_merge_age_millis: u64 = 5 * 60 * std.time.ms_per_s,
    };

    pub fn parseFromSlice(alloc: std.mem.Allocator, raw: []const u8) !Config {
        return try parseFromSliceWithSecrets(alloc, raw, null);
    }

    pub fn parseFromSliceWithSecrets(
        alloc: std.mem.Allocator,
        raw: []const u8,
        secret_store: ?*secrets.FileStore,
    ) !Config {
        var raw_tree = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{
            .allocate = .alloc_always,
        });
        defer raw_tree.deinit();
        const raw_root = switch (raw_tree.value) {
            .object => |object| object,
            else => return error.InvalidConfig,
        };

        var parsed_tree = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{
            .allocate = .alloc_always,
        });
        defer parsed_tree.deinit();
        var replacement_strings = std.ArrayList([]u8).empty;
        defer {
            for (replacement_strings.items) |value| alloc.free(value);
            replacement_strings.deinit(alloc);
        }
        try resolveSecretReferencesInValue(alloc, &parsed_tree.value, secret_store, &replacement_strings);
        const root = switch (parsed_tree.value) {
            .object => |object| object,
            else => return error.InvalidConfig,
        };

        var validated = try std.json.parseFromValue(common_openapi.Config, alloc, parsed_tree.value, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer validated.deinit();

        var registry = try provider_registry.Registry.parseFromValue(alloc, raw_tree.value);
        errdefer registry.deinit();
        var speech_to_text = if (root.get("speech_to_text")) |speech_to_text_value|
            try transcribing.Registry.parseFromValue(alloc, speech_to_text_value)
        else
            transcribing.Registry.init(alloc);
        errdefer speech_to_text.deinit();
        var text_to_speech = if (root.get("text_to_speech")) |text_to_speech_value|
            try synthesizing.Registry.parseFromValue(alloc, text_to_speech_value)
        else
            synthesizing.Registry.init(alloc);
        errdefer text_to_speech.deinit();

        const swarm_mode = try optionalBoolField(root, "swarm_mode") orelse false;
        return .{
            .registry = registry,
            .speech_to_text = speech_to_text,
            .text_to_speech = text_to_speech,
            .auth_enabled = try optionalBoolField(root, "enable_auth") orelse false,
            .swarm_mode = swarm_mode,
            .health_enabled = try optionalBoolField(root, "health_enabled") orelse true,
            .health_port = if (validated.value.health_port) |value|
                std.math.cast(u16, value) orelse return error.InvalidConfig
            else
                default_health_port,
            .registry_url = if (validated.value.registry_url) |value| try alloc.dupe(u8, value) else null,
            .log = validated.value.log,
            .tls = if (validated.value.tls) |tls| .{
                .cert = if (tls.cert) |value| try alloc.dupe(u8, value) else null,
                .key = if (tls.key) |value| try alloc.dupe(u8, value) else null,
            } else null,
            .cors = if (validated.value.cors) |cors| try corsFromOpenApi(alloc, cors) else null,
            .metadata = try parseMetadataConfig(
                alloc,
                root,
                if (validated.value.metadata) |metadata| metadata.orchestration_urls else null,
            ),
            .storage = try storageFromOpenApi(alloc, validated.value.storage),
            .termite = if (validated.value.termite) |termite| .{
                .api_url = if (termite.api_url.len > 0) try alloc.dupe(u8, termite.api_url) else null,
                .models_dir = if (termite.models_dir) |value| try alloc.dupe(u8, value) else null,
                .content_security = if (termite.content_security) |security| try contentSecurityFromOpenApi(alloc, security) else null,
                .s3_credentials = try parseRawTermiteS3Credentials(alloc, raw_root, termite.s3_credentials),
            } else .{},
            .remote_content = if (raw_root.get("remote_content")) |remote_content|
                try parseRemoteContentConfig(alloc, remote_content)
            else
                null,
            .shard_allocation = .{
                .default_shards_per_table = try optionalU32Field(root, "default_shards_per_table") orelse if (swarm_mode) default_swarm_shards_per_table else default_config_shards_per_table,
                .max_shard_size_bytes = try optionalU64Field(root, "max_shard_size_bytes") orelse default_max_shard_size_bytes,
                .min_shard_size_bytes = try optionalU64Field(root, "min_shard_size_bytes") orelse 0,
                .min_shards_per_table = try optionalU32Field(root, "min_shards_per_table") orelse 1,
                .max_shards_per_table = try optionalU32Field(root, "max_shards_per_table") orelse default_max_shards_per_table,
                .disable_shard_alloc = try optionalBoolField(root, "disable_shard_alloc") orelse true,
                .auto_range_transition_per_table_limit = try optionalU32Field(root, "auto_range_transition_per_table_limit") orelse 1,
                .auto_range_transition_cluster_limit = try optionalU32Field(root, "auto_range_transition_cluster_limit") orelse 1,
                .shard_cooldown_millis = try optionalU64Field(root, "shard_cooldown_millis") orelse 60 * std.time.ms_per_s,
                .min_shard_merge_age_millis = try optionalU64Field(root, "min_shard_merge_age_millis") orelse 5 * 60 * std.time.ms_per_s,
            },
        };
    }

    fn storageFromOpenApi(
        alloc: std.mem.Allocator,
        storage: ?common_openapi.StorageConfig,
    ) !StorageConfig {
        var parsed: StorageConfig = .{};
        errdefer parsed.deinit(alloc);

        const value = storage orelse return parsed;
        parsed.data_backend = value.data;
        parsed.metadata_backend = value.metadata;

        if (value.local) |local| {
            if (local.base_dir) |base_dir| parsed.local_base_dir = try alloc.dupe(u8, base_dir);
        }
        if (value.s3) |s3| {
            parsed.s3_bucket = try alloc.dupe(u8, s3.bucket);
            if (s3.prefix) |prefix| parsed.s3_prefix = try alloc.dupe(u8, prefix);
        }

        return parsed;
    }

    pub fn deinit(self: *Config) void {
        if (self.tls) |*tls| tls.deinit(self.registry.allocator);
        if (self.cors) |*cors| cors.deinit(self.registry.allocator);
        self.metadata.deinit(self.registry.allocator);
        self.storage.deinit(self.registry.allocator);
        self.termite.deinit(self.registry.allocator);
        self.speech_to_text.deinit();
        self.text_to_speech.deinit();
        if (self.registry_url) |value| self.registry.allocator.free(value);
        if (self.remote_content) |*remote_content| remote_content.deinit(self.registry.allocator);
        self.registry.deinit();
        self.* = undefined;
    }

    pub fn effectiveTermiteContentSecurity(self: *const Config) ?*const ContentSecurityConfig {
        return scraping.effectiveContentSecurity(
            if (self.termite.content_security) |*security| security else null,
            if (self.remote_content) |*remote_content|
                if (remote_content.security) |*security| security else null
            else
                null,
        );
    }
};

pub fn loadFromPath(alloc: std.mem.Allocator, path: []const u8) !Config {
    return try loadFromPathWithSecrets(alloc, path, null);
}

pub fn loadFromPathWithSecrets(
    alloc: std.mem.Allocator,
    path: []const u8,
    secret_store: ?*secrets.FileStore,
) !Config {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(16 * 1024 * 1024));
    defer alloc.free(raw);
    return try Config.parseFromSliceWithSecrets(alloc, raw, secret_store);
}

pub fn resolveLocalRoleBaseDir(alloc: std.mem.Allocator, cfg: ?*const Config, role: []const u8) ![]u8 {
    if (cfg) |loaded| {
        if (loaded.storage.local_base_dir) |dir| {
            return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dir, role });
        }
    }
    return try std.fmt.allocPrint(alloc, ".zig-cache/{s}", .{role});
}

fn parseMetadataConfig(
    alloc: std.mem.Allocator,
    root: std.json.ObjectMap,
    go_orchestration_urls: ?std.json.ArrayHashMap([]const u8),
) !Config.MetadataConfig {
    const orchestration_urls: []Config.MetadataConfig.NodeUrl = if (go_orchestration_urls) |values|
        try parseNodeUrls(alloc, values)
    else
        &.{};
    errdefer freeNodeUrls(alloc, orchestration_urls);

    const raft_urls: []Config.MetadataConfig.NodeUrl = (try parseObjectNodeUrlsField(alloc, root, "metadata", "raft_urls")) orelse &.{};
    errdefer freeNodeUrls(alloc, raft_urls);

    return .{
        .orchestration_urls = orchestration_urls,
        .raft_urls = raft_urls,
    };
}

fn parseNodeUrls(
    alloc: std.mem.Allocator,
    values: std.json.ArrayHashMap([]const u8),
) ![]Config.MetadataConfig.NodeUrl {
    if (values.map.count() == 0) return &.{};
    const keys = values.map.keys();
    const urls = values.map.values();
    var out = try alloc.alloc(Config.MetadataConfig.NodeUrl, values.map.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |entry| alloc.free(entry.url);
        alloc.free(out);
    }
    for (keys, urls, 0..) |key, url, i| {
        out[i] = .{
            .node_id = try parseNodeId(key),
            .url = try alloc.dupe(u8, url),
        };
        initialized += 1;
    }
    return out;
}

fn parseObjectNodeUrlsField(
    alloc: std.mem.Allocator,
    root: std.json.ObjectMap,
    object_name: []const u8,
    field_name: []const u8,
) !?[]Config.MetadataConfig.NodeUrl {
    const object_value = root.get(object_name) orelse return null;
    if (object_value != .object) return error.InvalidConfig;
    const field_value = object_value.object.get(field_name) orelse return null;
    const field_object = switch (field_value) {
        .object => |object| object,
        else => return error.InvalidConfig,
    };
    if (field_object.count() == 0) return &.{};
    var out = try alloc.alloc(Config.MetadataConfig.NodeUrl, field_object.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |entry| alloc.free(entry.url);
        alloc.free(out);
    }
    var it = field_object.iterator();
    while (it.next()) |entry| {
        const url = switch (entry.value_ptr.*) {
            .string => |value| value,
            else => return error.InvalidConfig,
        };
        out[initialized] = .{
            .node_id = try parseNodeId(entry.key_ptr.*),
            .url = try alloc.dupe(u8, url),
        };
        initialized += 1;
    }
    return out;
}

fn freeNodeUrls(alloc: std.mem.Allocator, values: []const Config.MetadataConfig.NodeUrl) void {
    for (values) |entry| alloc.free(entry.url);
    if (values.len > 0) alloc.free(values);
}

fn parseNodeId(raw: []const u8) !u64 {
    return std.fmt.parseInt(u64, raw, 10) catch
        std.fmt.parseInt(u64, raw, 16) catch
        error.InvalidConfig;
}

fn optionalBoolField(root: std.json.ObjectMap, field_name: []const u8) !?bool {
    const value = root.get(field_name) orelse return null;
    return switch (value) {
        .bool => value.bool,
        else => error.InvalidConfig,
    };
}

fn optionalU32Field(root: std.json.ObjectMap, field_name: []const u8) !?u32 {
    const value = root.get(field_name) orelse return null;
    return switch (value) {
        .integer => std.math.cast(u32, value.integer) orelse error.InvalidConfig,
        else => error.InvalidConfig,
    };
}

fn optionalU64Field(root: std.json.ObjectMap, field_name: []const u8) !?u64 {
    const value = root.get(field_name) orelse return null;
    return switch (value) {
        .integer => std.math.cast(u64, value.integer) orelse error.InvalidConfig,
        else => error.InvalidConfig,
    };
}

fn optionalObjectStringFieldDup(
    alloc: std.mem.Allocator,
    root: std.json.ObjectMap,
    object_name: []const u8,
    field_name: []const u8,
) !?[]u8 {
    const object_value = root.get(object_name) orelse return null;
    if (object_value != .object) return error.InvalidConfig;
    const field_value = object_value.object.get(field_name) orelse return null;
    return switch (field_value) {
        .string => try alloc.dupe(u8, field_value.string),
        else => error.InvalidConfig,
    };
}

fn parseRemoteContentConfig(alloc: std.mem.Allocator, value: std.json.Value) !Config.RemoteContentConfig {
    const parsed = try std.json.parseFromValue(scraping_openapi.RemoteContentConfig, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    var cfg = Config.RemoteContentConfig{
        .security = if (parsed.value.security) |security| try contentSecurityFromOpenApi(alloc, security) else null,
        .default_s3 = if (parsed.value.default_s3) |name| try alloc.dupe(u8, name) else null,
    };
    errdefer cfg.deinit(alloc);

    if (value != .object) return error.InvalidConfig;

    if (value.object.get("s3")) |raw_s3| {
        if (raw_s3 != .object) return error.InvalidConfig;
        var it = raw_s3.object.iterator();
        while (it.next()) |entry| {
            const key = try alloc.dupe(u8, entry.key_ptr.*);
            errdefer alloc.free(key);
            var credential = try parseRemoteContentS3Credential(alloc, entry.value_ptr.*);
            errdefer credential.deinit(alloc);
            const gop = try cfg.s3.getOrPut(alloc, key);
            if (gop.found_existing) {
                alloc.free(key);
                credential.deinit(alloc);
                return error.InvalidConfig;
            }
            gop.key_ptr.* = key;
            gop.value_ptr.* = credential;
        }
    }

    if (value.object.get("http")) |raw_http| {
        if (raw_http != .object) return error.InvalidConfig;
        var it = raw_http.object.iterator();
        while (it.next()) |entry| {
            const key = try alloc.dupe(u8, entry.key_ptr.*);
            errdefer alloc.free(key);
            var credential = try parseRemoteContentHttpCredential(alloc, entry.value_ptr.*);
            errdefer credential.deinit(alloc);
            const gop = try cfg.http.getOrPut(alloc, key);
            if (gop.found_existing) {
                alloc.free(key);
                credential.deinit(alloc);
                return error.InvalidConfig;
            }
            gop.key_ptr.* = key;
            gop.value_ptr.* = credential;
        }
    }

    return cfg;
}

fn parseRemoteContentS3Credential(alloc: std.mem.Allocator, value: std.json.Value) !Config.S3CredentialConfig {
    const scraping_cfg = try std.json.parseFromValue(scraping_openapi.S3CredentialConfig, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer scraping_cfg.deinit();

    const s3_cfg = try std.json.parseFromValue(s3_openapi.Credentials, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer s3_cfg.deinit();

    return .{
        .endpoint = if (s3_cfg.value.endpoint) |endpoint| try alloc.dupe(u8, endpoint) else null,
        .use_ssl = s3_cfg.value.use_ssl,
        .access_key_id = if (s3_cfg.value.access_key_id) |id| try alloc.dupe(u8, id) else null,
        .secret_access_key = if (s3_cfg.value.secret_access_key) |secret| try alloc.dupe(u8, secret) else null,
        .session_token = if (s3_cfg.value.session_token) |token| try alloc.dupe(u8, token) else null,
        .buckets = if (scraping_cfg.value.buckets) |buckets| try dupOwnedStringSlice(alloc, buckets) else null,
        .security = if (scraping_cfg.value.security) |security| try contentSecurityFromOpenApi(alloc, security) else null,
    };
}

fn s3CredentialsFromOpenApi(
    alloc: std.mem.Allocator,
    value: s3_openapi.Credentials,
) !Config.S3CredentialsConfig {
    return .{
        .endpoint = if (value.endpoint) |endpoint| try alloc.dupe(u8, endpoint) else null,
        .use_ssl = value.use_ssl,
        .access_key_id = if (value.access_key_id) |id| try alloc.dupe(u8, id) else null,
        .secret_access_key = if (value.secret_access_key) |secret| try alloc.dupe(u8, secret) else null,
        .session_token = if (value.session_token) |token| try alloc.dupe(u8, token) else null,
    };
}

fn parseRawTermiteS3Credentials(
    alloc: std.mem.Allocator,
    raw_root: std.json.ObjectMap,
    fallback: ?s3_openapi.Credentials,
) !?Config.S3CredentialsConfig {
    if (raw_root.get("termite")) |termite_value| {
        if (termite_value == .object) {
            if (termite_value.object.get("s3_credentials")) |credentials_value| {
                const parsed = try std.json.parseFromValue(s3_openapi.Credentials, alloc, credentials_value, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                return try s3CredentialsFromOpenApi(alloc, parsed.value);
            }
        }
    }
    return if (fallback) |credentials| try s3CredentialsFromOpenApi(alloc, credentials) else null;
}

fn parseRemoteContentHttpCredential(alloc: std.mem.Allocator, value: std.json.Value) !Config.HTTPCredentialConfig {
    const parsed = try std.json.parseFromValue(scraping_openapi.HTTPCredentialConfig, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    var cfg = Config.HTTPCredentialConfig{
        .base_url = if (parsed.value.base_url) |base_url| try alloc.dupe(u8, base_url) else null,
        .security = if (parsed.value.security) |security| try contentSecurityFromOpenApi(alloc, security) else null,
    };
    errdefer cfg.deinit(alloc);

    if (parsed.value.headers) |headers| {
        var it = headers.map.iterator();
        while (it.next()) |entry| {
            const key = try alloc.dupe(u8, entry.key_ptr.*);
            errdefer alloc.free(key);
            const header_value = try alloc.dupe(u8, entry.value_ptr.*);
            errdefer alloc.free(header_value);
            const gop = try cfg.headers.getOrPut(alloc, key);
            if (gop.found_existing) {
                alloc.free(key);
                alloc.free(header_value);
                return error.InvalidConfig;
            }
            gop.key_ptr.* = key;
            gop.value_ptr.* = header_value;
        }
    }

    return cfg;
}

fn contentSecurityFromOpenApi(
    alloc: std.mem.Allocator,
    value: scraping_openapi.ContentSecurityConfig,
) !Config.ContentSecurityConfig {
    return .{
        .allowed_hosts = if (value.allowed_hosts) |hosts| try dupOwnedStringSlice(alloc, hosts) else null,
        .block_private_ips = value.block_private_ips,
        .max_download_size_bytes = if (value.max_download_size_bytes) |bytes|
            std.math.cast(u64, bytes) orelse return error.InvalidConfig
        else
            null,
        .download_timeout_seconds = if (value.download_timeout_seconds) |seconds|
            std.math.cast(u32, seconds) orelse return error.InvalidConfig
        else
            null,
        .max_image_dimension = if (value.max_image_dimension) |dimension|
            std.math.cast(u32, dimension) orelse return error.InvalidConfig
        else
            null,
        .allowed_paths = if (value.allowed_paths) |paths| try dupOwnedStringSlice(alloc, paths) else null,
        .user_agent = if (value.user_agent) |user_agent| try alloc.dupe(u8, user_agent) else null,
    };
}

fn corsFromOpenApi(
    alloc: std.mem.Allocator,
    value: middleware_openapi.CORSConfig,
) !Config.CorsConfig {
    return .{
        .enabled = value.enabled,
        .allowed_origins = if (value.allowed_origins) |values| try dupOwnedStringSlice(alloc, values) else null,
        .allowed_methods = if (value.allowed_methods) |values| try dupOwnedStringSlice(alloc, values) else null,
        .allowed_headers = if (value.allowed_headers) |values| try dupOwnedStringSlice(alloc, values) else null,
        .exposed_headers = if (value.exposed_headers) |values| try dupOwnedStringSlice(alloc, values) else null,
        .allow_credentials = value.allow_credentials,
        .max_age = if (value.max_age) |max_age|
            std.math.cast(u32, max_age) orelse return error.InvalidConfig
        else
            null,
    };
}

fn dupOwnedStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []u8 {
    const out = try alloc.alloc([]u8, values.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        filled = i + 1;
    }
    return out;
}

fn freeOwnedStringSlice(alloc: std.mem.Allocator, values: []const []u8) void {
    for (values) |value| alloc.free(value);
    alloc.free(values);
}

fn resolveSecretReferencesInValue(
    alloc: std.mem.Allocator,
    value: *std.json.Value,
    secret_store: ?*secrets.FileStore,
    replacement_strings: *std.ArrayList([]u8),
) !void {
    switch (value.*) {
        .string => |raw| {
            if (secrets.parseSecretReference(raw) == null) return;
            const resolved = try secrets.resolveReferenceOwned(alloc, secret_store, raw);
            try replacement_strings.append(alloc, resolved);
            value.* = .{ .string = resolved };
        },
        .array => |*arr| {
            for (arr.items) |*item| try resolveSecretReferencesInValue(alloc, item, secret_store, replacement_strings);
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                try resolveSecretReferencesInValue(alloc, entry.value_ptr, secret_store, replacement_strings);
            }
        },
        else => {},
    }
}

test "common config parses provider maps" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "enable_auth": true,
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4,
        \\  "shard_cooldown_millis": 90000,
        \\  "min_shard_merge_age_millis": 180000,
        \\  "generators": {
        \\    "primary": { "provider": "mock" }
        \\  },
        \\  "embedders": {
        \\    "embedder": { "provider": "antfly" }
        \\  },
        \\  "rerankers": {
        \\    "reranker": { "provider": "antfly", "field": "body" }
        \\  },
        \\  "chunkers": {
        \\    "fixed": { "provider": "antfly" }
        \\  },
        \\  "speech_to_text": {
        \\    "whisper-local": { "provider": "termite", "api_url": "http://127.0.0.1:8080", "model": "openai/whisper-base" }
        \\  },
        \\  "text_to_speech": {
        \\    "nova": { "provider": "openai", "model": "tts-1", "voice": "nova" }
        \\  },
        \\  "chains": {
        \\    "default": [{ "generator": "primary" }]
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 1), cfg.metadata.orchestration_urls.len);
    try std.testing.expectEqual(@as(u64, 1), cfg.metadata.orchestration_urls[0].node_id);
    try std.testing.expectEqualStrings("http://127.0.0.1:7001", cfg.metadata.orchestration_urls[0].url);
    try std.testing.expect(cfg.auth_enabled);
    try std.testing.expectEqualStrings("antflydb", cfg.storage.local_base_dir.?);
    try std.testing.expectEqualStrings("primary", cfg.registry.defaultGeneratorName().?);
    try std.testing.expectEqualStrings("embedder", cfg.registry.defaultEmbedderName().?);
    try std.testing.expectEqualStrings("reranker", cfg.registry.defaultRerankerName().?);
    try std.testing.expectEqualStrings("fixed", cfg.registry.defaultChunkerName().?);
    try std.testing.expectEqualStrings("default", cfg.registry.defaultChainName().?);
    try std.testing.expectEqualStrings("whisper-local", cfg.speech_to_text.defaultProviderName().?);
    try std.testing.expectEqualStrings("nova", cfg.text_to_speech.defaultProviderName().?);
    try std.testing.expectEqual(transcribing.Provider.termite, (try cfg.speech_to_text.getConfig(null)).provider);
    try std.testing.expectEqual(synthesizing.Provider.openai, (try cfg.text_to_speech.getConfig(null)).provider);
    try std.testing.expectEqual(@as(u32, 1), cfg.shard_allocation.default_shards_per_table);
    try std.testing.expectEqual(@as(u64, 1024), cfg.shard_allocation.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, 4), cfg.shard_allocation.max_shards_per_table);
    try std.testing.expectEqual(@as(u64, 90000), cfg.shard_allocation.shard_cooldown_millis);
    try std.testing.expectEqual(@as(u64, 180000), cfg.shard_allocation.min_shard_merge_age_millis);
}

test "common config extracts termite settings" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4,
        \\  "termite": {
        \\    "api_url": "http://127.0.0.1:8083",
        \\    "models_dir": "/tmp/models",
        \\    "content_security": {
        \\      "allowed_hosts": ["models.example.com"],
        \\      "block_private_ips": true
        \\    },
        \\    "s3_credentials": {
        \\      "endpoint": "s3.amazonaws.com",
        \\      "access_key_id": "termite-key",
        \\      "secret_access_key": "termite-secret"
        \\    }
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 1), cfg.metadata.orchestration_urls.len);
    try std.testing.expectEqual(@as(u64, 1), cfg.metadata.orchestration_urls[0].node_id);
    try std.testing.expectEqualStrings("http://127.0.0.1:7001", cfg.metadata.orchestration_urls[0].url);
    try std.testing.expectEqualStrings("antflydb", cfg.storage.local_base_dir.?);
    try std.testing.expectEqualStrings("http://127.0.0.1:8083", cfg.termite.api_url.?);
    try std.testing.expectEqualStrings("/tmp/models", cfg.termite.models_dir.?);
    try std.testing.expectEqualStrings("models.example.com", cfg.termite.content_security.?.allowed_hosts.?[0]);
    try std.testing.expectEqual(@as(?bool, true), cfg.termite.content_security.?.block_private_ips);
    try std.testing.expectEqualStrings("s3.amazonaws.com", cfg.termite.s3_credentials.?.endpoint.?);
    try std.testing.expectEqualStrings("termite-key", cfg.termite.s3_credentials.?.access_key_id.?);
    try std.testing.expectEqualStrings("termite-secret", cfg.termite.s3_credentials.?.secret_access_key.?);
}

test "common config defaults shard scalar fields" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(u32, default_config_shards_per_table), cfg.shard_allocation.default_shards_per_table);
    try std.testing.expectEqual(@as(u64, default_max_shard_size_bytes), cfg.shard_allocation.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, default_max_shards_per_table), cfg.shard_allocation.max_shards_per_table);
}

test "common config treats go orchestration urls as metadata api discovery urls" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7101",
        \\      "2": "http://127.0.0.1:7102"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 2), cfg.metadata.orchestration_urls.len);
    try std.testing.expectEqual(@as(u64, 1), cfg.metadata.orchestration_urls[0].node_id);
    try std.testing.expectEqualStrings("http://127.0.0.1:7101", cfg.metadata.orchestration_urls[0].url);
    try std.testing.expectEqual(@as(u64, 2), cfg.metadata.orchestration_urls[1].node_id);
    try std.testing.expectEqualStrings("http://127.0.0.1:7102", cfg.metadata.orchestration_urls[1].url);
    try std.testing.expectEqual(@as(usize, 0), cfg.metadata.raft_urls.len);
}

test "common config preserves remote content logging storage and registry fields" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "log": {
        \\    "level": "debug",
        \\    "style": "json"
        \\  },
        \\  "health_port": 4200,
        \\  "registry_url": "https://registry.antfly.io/v1",
        \\  "swarm_mode": true,
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" },
        \\    "data": "s3",
        \\    "metadata": "local",
        \\    "s3": { "bucket": "antfly-prod", "prefix": "cluster-a/" }
        \\  },
        \\  "remote_content": {
        \\    "security": {
        \\      "allowed_hosts": ["example.com", "cdn.example.com"],
        \\      "block_private_ips": true,
        \\      "max_download_size_bytes": 104857600,
        \\      "download_timeout_seconds": 30,
        \\      "max_image_dimension": 2048
        \\    },
        \\    "default_s3": "primary",
        \\    "s3": {
        \\      "primary": {
        \\        "endpoint": "s3.amazonaws.com",
        \\        "access_key_id": "test-key",
        \\        "secret_access_key": "test-secret",
        \\        "buckets": ["docs-*"]
        \\      }
        \\    },
        \\    "http": {
        \\      "internal-api": {
        \\        "base_url": "https://docs.internal.com",
        \\        "headers": {
        \\          "Authorization": "Bearer abc"
        \\        }
        \\      }
        \\    }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqual(true, cfg.swarm_mode);
    try std.testing.expect(cfg.health_enabled);
    try std.testing.expectEqual(@as(?u16, 4200), cfg.health_port);
    try std.testing.expectEqualStrings("https://registry.antfly.io/v1", cfg.registry_url.?);
    try std.testing.expectEqual(logging_openapi.Level.debug, cfg.log.?.level.?);
    try std.testing.expectEqual(logging_openapi.Style.json, cfg.log.?.style.?);
    try std.testing.expectEqual(common_openapi.StorageBackend.s3, cfg.storage.data_backend.?);
    try std.testing.expectEqual(common_openapi.StorageBackend.local, cfg.storage.metadata_backend.?);
    try std.testing.expectEqualStrings("antfly-prod", cfg.storage.s3_bucket.?);
    try std.testing.expectEqualStrings("cluster-a/", cfg.storage.s3_prefix.?);

    const remote_content = cfg.remote_content.?;
    try std.testing.expectEqualStrings("primary", remote_content.default_s3.?);
    try std.testing.expectEqual(@as(?bool, true), remote_content.security.?.block_private_ips);
    try std.testing.expectEqual(@as(?u64, 104857600), remote_content.security.?.max_download_size_bytes);
    try std.testing.expectEqualStrings("example.com", remote_content.security.?.allowed_hosts.?[0]);

    const s3_credential = remote_content.getS3("primary").?;
    try std.testing.expectEqualStrings("s3.amazonaws.com", s3_credential.endpoint.?);
    try std.testing.expectEqualStrings("test-key", s3_credential.access_key_id.?);
    try std.testing.expectEqualStrings("test-secret", s3_credential.secret_access_key.?);
    try std.testing.expectEqualStrings("docs-*", s3_credential.buckets.?[0]);

    const http_credential = remote_content.getHttp("internal-api").?;
    try std.testing.expectEqualStrings("https://docs.internal.com", http_credential.base_url.?);
    try std.testing.expectEqualStrings("Bearer abc", http_credential.headers.get("Authorization").?);
}

test "common config preserves tls and cors fields" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "tls": {
        \\    "cert": "/tmp/server.crt",
        \\    "key": "/tmp/server.key"
        \\  },
        \\  "cors": {
        \\    "enabled": true,
        \\    "allowed_origins": ["https://example.com", "https://app.example.com"],
        \\    "allowed_methods": ["GET", "POST", "PUT", "DELETE"],
        \\    "allowed_headers": ["Content-Type", "Authorization"],
        \\    "allow_credentials": true,
        \\    "max_age": 7200
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqualStrings("/tmp/server.crt", cfg.tls.?.cert.?);
    try std.testing.expectEqualStrings("/tmp/server.key", cfg.tls.?.key.?);
    try std.testing.expectEqual(@as(?bool, true), cfg.cors.?.enabled);
    try std.testing.expectEqualStrings("https://example.com", cfg.cors.?.allowed_origins.?[0]);
    try std.testing.expectEqualStrings("DELETE", cfg.cors.?.allowed_methods.?[3]);
    try std.testing.expectEqualStrings("Authorization", cfg.cors.?.allowed_headers.?[1]);
    try std.testing.expectEqual(@as(?bool, true), cfg.cors.?.allow_credentials);
    try std.testing.expectEqual(@as(?u32, 7200), cfg.cors.?.max_age);
}

test "common config preserves named audio provider maps and defaults" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4,
        \\  "speech_to_text": {
        \\    "whisper-local": {
        \\      "provider": "termite",
        \\      "api_url": "http://127.0.0.1:8080",
        \\      "model": "openai/whisper-base"
        \\    },
        \\    "whisper-remote": {
        \\      "provider": "openai",
        \\      "model": "whisper-1"
        \\    }
        \\  },
        \\  "text_to_speech": {
        \\    "narrator": {
        \\      "provider": "openai",
        \\      "model": "tts-1",
        \\      "voice": "nova"
        \\    },
        \\    "premium": {
        \\      "provider": "elevenlabs",
        \\      "voice_id": "voice-123"
        \\    }
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqualStrings("whisper-local", cfg.speech_to_text.defaultProviderName().?);
    try std.testing.expectEqualStrings("narrator", cfg.text_to_speech.defaultProviderName().?);
    try std.testing.expectEqual(transcribing.Provider.termite, (try cfg.speech_to_text.getConfig(null)).provider);
    try std.testing.expectEqualStrings("openai/whisper-base", (try cfg.speech_to_text.getConfig(null)).model.?);
    try std.testing.expectEqual(synthesizing.Provider.elevenlabs, (try cfg.text_to_speech.getConfig("premium")).provider);
    try std.testing.expectEqualStrings("voice-123", (try cfg.text_to_speech.getConfig("premium")).voice_id.?);
}

test "common config resolves secret references through the provided store" {
    const alloc = std.testing.allocator;
    const store_path = ".zig-cache/test-config-secrets.json";
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};
    }
    var secret_store = try secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();
    var stored = try secret_store.put(alloc, "termite.api_url", "http://127.0.0.1:8089");
    stored.deinit(alloc);

    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4,
        \\  "termite": {
        \\    "api_url": "${secret:termite.api_url}"
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSliceWithSecrets(alloc, raw, &secret_store);
    defer cfg.deinit();
    try std.testing.expectEqualStrings("http://127.0.0.1:8089", cfg.termite.api_url.?);
}

test "common config inherits termite content security from remote content" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "remote_content": {
        \\    "security": {
        \\      "block_private_ips": true,
        \\      "allowed_hosts": ["cdn.example.com"]
        \\    }
        \\  },
        \\  "termite": {
        \\    "api_url": "http://127.0.0.1:8083"
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    const effective = cfg.effectiveTermiteContentSecurity().?;
    try std.testing.expectEqual(@as(?bool, true), effective.block_private_ips);
    try std.testing.expectEqualStrings("cdn.example.com", effective.allowed_hosts.?[0]);
}

test "common config prefers termite content security over inherited remote content security" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "remote_content": {
        \\    "security": {
        \\      "block_private_ips": true,
        \\      "allowed_hosts": ["cdn.example.com"]
        \\    }
        \\  },
        \\  "termite": {
        \\    "api_url": "http://127.0.0.1:8083",
        \\    "content_security": {
        \\      "block_private_ips": false,
        \\      "allowed_hosts": ["models.example.com"]
        \\    }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    const effective = cfg.effectiveTermiteContentSecurity().?;
    try std.testing.expectEqual(@as(?bool, false), effective.block_private_ips);
    try std.testing.expectEqualStrings("models.example.com", effective.allowed_hosts.?[0]);
}

test "common config treats empty termite content security as inheritable" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "remote_content": {
        \\    "security": {
        \\      "block_private_ips": true,
        \\      "allowed_hosts": ["cdn.example.com"]
        \\    }
        \\  },
        \\  "termite": {
        \\    "api_url": "http://127.0.0.1:8083",
        \\    "content_security": {}
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    const effective = cfg.effectiveTermiteContentSecurity().?;
    try std.testing.expectEqual(@as(?bool, true), effective.block_private_ips);
    try std.testing.expectEqualStrings("cdn.example.com", effective.allowed_hosts.?[0]);
}

test "common config preserves live secret references inside remote content credentials" {
    const alloc = std.testing.allocator;
    const store_path = ".zig-cache/test-remote-content-secrets.json";
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};
    }
    var secret_store = try secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();
    var stored_access = try secret_store.put(alloc, "aws.key", "AKIA-TEST");
    defer stored_access.deinit(alloc);
    var stored_secret = try secret_store.put(alloc, "aws.secret", "SECRET-TEST");
    defer stored_secret.deinit(alloc);
    var stored_header = try secret_store.put(alloc, "remote.token", "Bearer super-secret");
    defer stored_header.deinit(alloc);

    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "remote_content": {
        \\    "default_s3": "primary",
        \\    "s3": {
        \\      "primary": {
        \\        "endpoint": "s3.amazonaws.com",
        \\        "access_key_id": "${secret:aws.key}",
        \\        "secret_access_key": "${secret:aws.secret}"
        \\      }
        \\    },
        \\    "http": {
        \\      "internal-api": {
        \\        "base_url": "https://docs.internal.com",
        \\        "headers": {
        \\          "Authorization": "${secret:remote.token}"
        \\        }
        \\      }
        \\    }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;

    var cfg = try Config.parseFromSliceWithSecrets(alloc, raw, &secret_store);
    defer cfg.deinit();

    const remote_content = cfg.remote_content.?;
    const s3_credential = remote_content.getS3("primary").?;
    try std.testing.expectEqualStrings("${secret:aws.key}", s3_credential.access_key_id.?);
    try std.testing.expectEqualStrings("${secret:aws.secret}", s3_credential.secret_access_key.?);

    const http_credential = remote_content.getHttp("internal-api").?;
    try std.testing.expectEqualStrings("${secret:remote.token}", http_credential.headers.get("Authorization").?);
}

test "common config resolves local role base dir from config" {
    const alloc = std.testing.allocator;
    var cfg = Config{
        .registry = provider_registry.Registry.init(alloc),
        .speech_to_text = transcribing.Registry.init(alloc),
        .text_to_speech = synthesizing.Registry.init(alloc),
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/antflydb"),
        },
    };
    defer cfg.deinit();

    const base = try resolveLocalRoleBaseDir(alloc, &cfg, "swarm");
    defer alloc.free(base);
    try std.testing.expectEqualStrings("/tmp/antflydb/swarm", base);
}

test "common config resolves stable local role base dir by default" {
    const base = try resolveLocalRoleBaseDir(std.testing.allocator, null, "swarm");
    defer std.testing.allocator.free(base);
    try std.testing.expectEqualStrings(".zig-cache/swarm", base);
}

test "common config parses minimal config with runtime defaults" {
    const alloc = std.testing.allocator;
    var cfg = try Config.parseFromSlice(alloc, "{}");
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 0), cfg.metadata.orchestration_urls.len);
    try std.testing.expectEqual(@as(usize, 0), cfg.metadata.raft_urls.len);
    try std.testing.expect(cfg.storage.local_base_dir == null);
    try std.testing.expect(cfg.health_enabled);
    try std.testing.expectEqual(@as(?u16, default_health_port), cfg.health_port);
    try std.testing.expectEqual(@as(u32, default_config_shards_per_table), cfg.shard_allocation.default_shards_per_table);
    try std.testing.expectEqual(@as(u64, default_max_shard_size_bytes), cfg.shard_allocation.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, default_max_shards_per_table), cfg.shard_allocation.max_shards_per_table);
    try std.testing.expect(cfg.shard_allocation.disable_shard_alloc);
}

test "common config can disable health server" {
    const alloc = std.testing.allocator;
    var cfg = try Config.parseFromSlice(alloc, "{\"health_enabled\": false}");
    defer cfg.deinit();

    try std.testing.expect(!cfg.health_enabled);
    try std.testing.expectEqual(@as(?u16, default_health_port), cfg.health_port);
}

test "common config accepts partial metadata and storage objects" {
    const alloc = std.testing.allocator;
    var cfg = try Config.parseFromSlice(alloc,
        \\{
        \\  "metadata": {},
        \\  "storage": {
        \\    "local": {},
        \\    "data": "local"
        \\  }
        \\}
    );
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 0), cfg.metadata.orchestration_urls.len);
    try std.testing.expect(cfg.storage.local_base_dir == null);
    try std.testing.expectEqual(common_openapi.StorageBackend.local, cfg.storage.data_backend.?);
    try std.testing.expectEqual(@as(u32, default_config_shards_per_table), cfg.shard_allocation.default_shards_per_table);
}

test "common config applies swarm shard defaults when swarm mode is set" {
    const alloc = std.testing.allocator;
    var cfg = try Config.parseFromSlice(alloc,
        \\{"swarm_mode": true}
    );
    defer cfg.deinit();

    try std.testing.expect(cfg.swarm_mode);
    try std.testing.expectEqual(@as(u32, default_swarm_shards_per_table), cfg.shard_allocation.default_shards_per_table);
    try std.testing.expectEqual(@as(u64, default_max_shard_size_bytes), cfg.shard_allocation.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, default_max_shards_per_table), cfg.shard_allocation.max_shards_per_table);
    try std.testing.expect(cfg.shard_allocation.disable_shard_alloc);
}
