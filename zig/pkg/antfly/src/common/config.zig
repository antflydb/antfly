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
const builtin = @import("builtin");
const common_openapi = @import("antfly_common_openapi");
const inference_config_openapi = @import("antfly_inference_config_openapi");
const logging_openapi = @import("antfly_logging_openapi");
const middleware_openapi = @import("antfly_middleware_openapi");
const scraping = @import("antfly_scraping");
const scraping_openapi = @import("antfly_scraping_openapi");
const s3_openapi = @import("antfly_s3_openapi");
const provider_registry = @import("provider_registry.zig");
const secrets = @import("secrets.zig");
const transcribing = @import("antfly_transcribing");
const readers = @import("antfly_readers");
const synthesizing = @import("antfly_synthesizing");
const platform = @import("antfly_platform");

const default_max_shard_size_bytes: u64 = 64 * 1024 * 1024;

pub const StorageEngine = common_openapi.StorageEngine;
const default_max_shards_per_table: u32 = 20;
const default_config_shards_per_table: u32 = 3;
const default_standalone_shards_per_table: u32 = 1;
pub const default_health_port: u16 = 4200;

pub const DeploymentMode = enum {
    embedded,
    distributed,
    standalone,
    serverless,

    pub fn isStandalone(self: DeploymentMode) bool {
        return self == .standalone;
    }

    pub fn supportsLite(self: DeploymentMode) bool {
        return self == .standalone or self == .embedded;
    }
};

pub const Config = struct {
    registry: provider_registry.Registry,
    transcribers: transcribing.Registry,
    readers: readers.Registry,
    text_to_speech: synthesizing.Registry,
    auth_enabled: bool = false,
    deployment_mode: DeploymentMode = .distributed,
    health_enabled: bool = true,
    health_port: ?u16 = null,
    log: ?logging_openapi.Config = null,
    tls: ?TlsConfig = null,
    cors: ?CorsConfig = null,
    metadata: MetadataConfig = .{},
    storage: StorageConfig = .{},
    transaction_sessions: TransactionSessionConfig = .{},
    inference: InferenceConfig = .{},
    remote_content: ?RemoteContentConfig = null,
    connections: ConnectionsConfig = .{},
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
        engine: common_openapi.StorageEngine = .local,
        lite_path: ?[]u8 = null,
        lite_fsync: bool = true,
        local_base_dir: ?[]u8 = null,
        object_connection: ?[]u8 = null,
        object_bucket: ?[]u8 = null,
        object_prefix: ?[]u8 = null,
        object_lanes: ObjectStorageLanes = .{},

        fn deinit(self: *StorageConfig, alloc: std.mem.Allocator) void {
            if (self.lite_path) |value| alloc.free(value);
            if (self.local_base_dir) |value| alloc.free(value);
            if (self.object_connection) |value| alloc.free(value);
            if (self.object_bucket) |value| alloc.free(value);
            if (self.object_prefix) |value| alloc.free(value);
            self.object_lanes.deinit(alloc);
            self.* = undefined;
        }
    };

    pub const ObjectStorageLocation = struct {
        connection: ?[]u8 = null,
        bucket: ?[]u8 = null,
        prefix: ?[]u8 = null,

        fn deinit(self: *ObjectStorageLocation, alloc: std.mem.Allocator) void {
            if (self.connection) |value| alloc.free(value);
            if (self.bucket) |value| alloc.free(value);
            if (self.prefix) |value| alloc.free(value);
            self.* = .{};
        }
    };

    pub const ObjectStorageLanes = struct {
        artifacts: ObjectStorageLocation = .{},
        manifests: ObjectStorageLocation = .{},
        wal: ObjectStorageLocation = .{},
        progress: ObjectStorageLocation = .{},
        catalog: ObjectStorageLocation = .{},

        fn deinit(self: *ObjectStorageLanes, alloc: std.mem.Allocator) void {
            self.artifacts.deinit(alloc);
            self.manifests.deinit(alloc);
            self.wal.deinit(alloc);
            self.progress.deinit(alloc);
            self.catalog.deinit(alloc);
            self.* = .{};
        }
    };

    pub const TransactionSessionConfig = struct {
        ttl_seconds: u64 = 3600,
        cleanup_interval_seconds: u64 = 60,
        max_count: usize = 1024,
        max_record_bytes: usize = 16 * 1024 * 1024,
        max_savepoints: usize = 64,
    };

    pub const InferenceConfig = struct {
        pub const PromptCacheMode = enum {
            simple,
            block_hash,
        };

        pub const PromptCacheConfig = struct {
            enabled: bool = false,
            mode: PromptCacheMode = .block_hash,
            max_bytes_mb: usize = 512,
            min_tokens: usize = 64,
            ttl_ms: u64 = 300_000,
        };

        pub const WarmModelConfig = struct {
            kind: []u8,
            name: []u8,
            backend: ?[]u8 = null,
            format: ?[]u8 = null,
            quantization: ?[]u8 = null,

            fn deinit(self: *WarmModelConfig, alloc: std.mem.Allocator) void {
                alloc.free(self.kind);
                alloc.free(self.name);
                if (self.backend) |value| alloc.free(value);
                if (self.format) |value| alloc.free(value);
                if (self.quantization) |value| alloc.free(value);
                self.* = undefined;
            }
        };

        api_url: ?[]u8 = null,
        api_key: ?[]u8 = null,
        models_dir: ?[]u8 = null,
        ml_dir: ?[]u8 = null,
        content_security: ?ContentSecurityConfig = null,
        s3_credentials: ?S3CredentialsConfig = null,
        preload: []WarmModelConfig = &.{},
        backend_priority: ?[]const []u8 = null,
        pjrt_plugin_path: ?[]u8 = null,
        prompt_cache: PromptCacheConfig = .{},
        keep_alive_ms: u64 = 300_000,
        max_loaded_models: usize = 10,
        max_concurrent_requests: usize = 32,
        pool_size: usize = 1,
        allow_downloads: bool = true,

        fn deinit(self: *InferenceConfig, alloc: std.mem.Allocator) void {
            if (self.api_url) |value| alloc.free(value);
            if (self.api_key) |value| alloc.free(value);
            if (self.models_dir) |value| alloc.free(value);
            if (self.ml_dir) |value| alloc.free(value);
            if (self.content_security) |*security| security.deinit(alloc);
            if (self.s3_credentials) |*credentials| credentials.deinit(alloc);
            for (self.preload) |*model| model.deinit(alloc);
            if (self.preload.len > 0) alloc.free(self.preload);
            if (self.backend_priority) |values| freeOwnedStringSlice(alloc, values);
            if (self.pjrt_plugin_path) |value| alloc.free(value);
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

    pub const ConnectionsConfig = std.StringArrayHashMapUnmanaged(ConnectionConfig);

    pub const ConnectionKind = enum {
        inference,
        web_search,
        external_io,
        cdc,
    };

    pub const ExternalIoProtocol = enum {
        s3,
        gcs,
        filesystem,
        http,
    };

    pub const S3AddressingStyle = enum { path, virtual_hosted };
    pub const BucketProvisioning = enum { require_existing, create_if_missing };
    pub const AwsCredentialSource = enum { default, static, profile, web_identity };
    pub const GcsCredentialSource = enum { default, bearer_token, service_account };

    pub const AwsCredentialConfig = struct {
        source: AwsCredentialSource = .default,
        access_key_id: ?[]u8 = null,
        secret_access_key: ?[]u8 = null,
        session_token: ?[]u8 = null,
        profile: ?[]u8 = null,
        shared_credentials_file: ?[]u8 = null,
        role_arn: ?[]u8 = null,
        token_file: ?[]u8 = null,
        session_name: ?[]u8 = null,
        sts_endpoint: ?[]u8 = null,

        fn deinit(self: *AwsCredentialConfig, alloc: std.mem.Allocator) void {
            if (self.access_key_id) |value| alloc.free(value);
            if (self.secret_access_key) |value| alloc.free(value);
            if (self.session_token) |value| alloc.free(value);
            if (self.profile) |value| alloc.free(value);
            if (self.shared_credentials_file) |value| alloc.free(value);
            if (self.role_arn) |value| alloc.free(value);
            if (self.token_file) |value| alloc.free(value);
            if (self.session_name) |value| alloc.free(value);
            if (self.sts_endpoint) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    pub const GcsCredentialConfig = struct {
        source: GcsCredentialSource = .default,
        bearer_token: ?[]u8 = null,
        service_account_json: ?[]u8 = null,
        credentials_path: ?[]u8 = null,
        scope: ?[]u8 = null,

        fn deinit(self: *GcsCredentialConfig, alloc: std.mem.Allocator) void {
            if (self.bearer_token) |value| alloc.free(value);
            if (self.service_account_json) |value| alloc.free(value);
            if (self.credentials_path) |value| alloc.free(value);
            if (self.scope) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    pub const ConnectionConfig = struct {
        display_name: ?[]u8 = null,
        provider: ?[]u8 = null,
        kind: ConnectionKind,
        capabilities: []const []u8 = &.{},
        inference: ?InferenceConnectionConfig = null,
        web_search: ?WebSearchConnectionConfig = null,
        external_io: ?ExternalIoConnectionConfig = null,
        cdc: ?CdcConnectionConfig = null,

        fn deinit(self: *ConnectionConfig, alloc: std.mem.Allocator) void {
            if (self.display_name) |value| alloc.free(value);
            if (self.provider) |value| alloc.free(value);
            freeOwnedStringSlice(alloc, self.capabilities);
            if (self.inference) |*inference| inference.deinit(alloc);
            if (self.web_search) |*web_search| web_search.deinit(alloc);
            if (self.external_io) |*external_io| external_io.deinit(alloc);
            if (self.cdc) |*cdc| cdc.deinit(alloc);
            self.* = undefined;
        }
    };

    pub const InferenceConnectionConfig = struct {
        provider: []u8,
        url: ?[]u8 = null,
        api_key: ?[]u8 = null,
        region: ?[]u8 = null,
        project_id: ?[]u8 = null,
        location: ?[]u8 = null,
        credentials_path: ?[]u8 = null,
        names: []const []u8 = &.{},
        configured_model_types: []const []u8 = &.{},

        fn deinit(self: *InferenceConnectionConfig, alloc: std.mem.Allocator) void {
            alloc.free(self.provider);
            if (self.url) |value| alloc.free(value);
            if (self.api_key) |value| alloc.free(value);
            if (self.region) |value| alloc.free(value);
            if (self.project_id) |value| alloc.free(value);
            if (self.location) |value| alloc.free(value);
            if (self.credentials_path) |value| alloc.free(value);
            freeOwnedStringSlice(alloc, self.names);
            freeOwnedStringSlice(alloc, self.configured_model_types);
            self.* = undefined;
        }
    };

    pub const WebSearchConnectionConfig = struct {
        service: ?[]u8 = null,
        max_results: ?u32 = null,
        timeout_ms: ?u32 = null,
        safe_search: ?bool = null,
        language: ?[]u8 = null,
        region: ?[]u8 = null,
        include_content: ?bool = null,
        include_highlights: ?bool = null,
        api_key: ?[]u8 = null,
        endpoint: ?[]u8 = null,
        project_id: ?[]u8 = null,
        location: ?[]u8 = null,
        data_store: ?[]u8 = null,
        serving_config: ?[]u8 = null,
        credentials_path: ?[]u8 = null,
        include_domains: []const []u8 = &.{},
        exclude_domains: []const []u8 = &.{},

        fn deinit(self: *WebSearchConnectionConfig, alloc: std.mem.Allocator) void {
            if (self.service) |value| alloc.free(value);
            if (self.language) |value| alloc.free(value);
            if (self.region) |value| alloc.free(value);
            if (self.api_key) |value| alloc.free(value);
            if (self.endpoint) |value| alloc.free(value);
            if (self.project_id) |value| alloc.free(value);
            if (self.location) |value| alloc.free(value);
            if (self.data_store) |value| alloc.free(value);
            if (self.serving_config) |value| alloc.free(value);
            if (self.credentials_path) |value| alloc.free(value);
            freeOwnedStringSlice(alloc, self.include_domains);
            freeOwnedStringSlice(alloc, self.exclude_domains);
            self.* = undefined;
        }
    };

    pub const ExternalIoConnectionConfig = struct {
        protocol: ExternalIoProtocol,
        endpoint: ?[]u8 = null,
        region: ?[]u8 = null,
        addressing_style: S3AddressingStyle = .virtual_hosted,
        bucket_provisioning: BucketProvisioning = .require_existing,
        buckets: []const []u8 = &.{},
        prefix: ?[]u8 = null,
        hosts: []const []u8 = &.{},
        headers: std.StringArrayHashMapUnmanaged([]u8) = .{},
        credentials: AwsCredentialConfig = .{},
        gcs_credentials: GcsCredentialConfig = .{},
        project_id: ?[]u8 = null,
        upload_endpoint: ?[]u8 = null,
        root: ?[]u8 = null,
        use_ssl: ?bool = null,

        fn deinit(self: *ExternalIoConnectionConfig, alloc: std.mem.Allocator) void {
            if (self.endpoint) |value| alloc.free(value);
            if (self.region) |value| alloc.free(value);
            freeOwnedStringSlice(alloc, self.buckets);
            if (self.prefix) |value| alloc.free(value);
            freeOwnedStringSlice(alloc, self.hosts);
            var it = self.headers.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                alloc.free(entry.value_ptr.*);
            }
            self.headers.deinit(alloc);
            self.credentials.deinit(alloc);
            self.gcs_credentials.deinit(alloc);
            if (self.project_id) |value| alloc.free(value);
            if (self.upload_endpoint) |value| alloc.free(value);
            if (self.root) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    /// Owned credential material for an external-I/O client snapshot.
    /// The node config retains `${secret:...}` references; callers resolve an
    /// owned snapshot immediately before constructing a client so rotations
    /// take effect without a process restart.
    pub const ResolvedExternalIoCredentials = struct {
        aws: AwsCredentialConfig = .{},
        gcs: GcsCredentialConfig = .{},

        pub fn deinit(self: *ResolvedExternalIoCredentials, alloc: std.mem.Allocator) void {
            self.aws.deinit(alloc);
            self.gcs.deinit(alloc);
            self.* = undefined;
        }

        pub fn apply(self: *const ResolvedExternalIoCredentials, external: ExternalIoConnectionConfig) ExternalIoConnectionConfig {
            var resolved = external;
            resolved.credentials = self.aws;
            resolved.gcs_credentials = self.gcs;
            return resolved;
        }
    };

    pub fn resolveExternalIoCredentials(
        alloc: std.mem.Allocator,
        external: ExternalIoConnectionConfig,
        secret_store: ?*secrets.FileStore,
    ) !ResolvedExternalIoCredentials {
        var resolved = ResolvedExternalIoCredentials{
            .aws = .{ .source = external.credentials.source },
            .gcs = .{ .source = external.gcs_credentials.source },
        };
        errdefer resolved.deinit(alloc);

        resolved.aws.access_key_id = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.access_key_id);
        resolved.aws.secret_access_key = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.secret_access_key);
        resolved.aws.session_token = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.session_token);
        resolved.aws.profile = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.profile);
        resolved.aws.shared_credentials_file = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.shared_credentials_file);
        resolved.aws.role_arn = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.role_arn);
        resolved.aws.token_file = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.token_file);
        resolved.aws.session_name = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.session_name);
        resolved.aws.sts_endpoint = try resolveOptionalConnectionSecret(alloc, secret_store, external.credentials.sts_endpoint);

        resolved.gcs.bearer_token = try resolveOptionalConnectionSecret(alloc, secret_store, external.gcs_credentials.bearer_token);
        resolved.gcs.service_account_json = try resolveOptionalConnectionSecret(alloc, secret_store, external.gcs_credentials.service_account_json);
        resolved.gcs.credentials_path = try resolveOptionalConnectionSecret(alloc, secret_store, external.gcs_credentials.credentials_path);
        resolved.gcs.scope = try resolveOptionalConnectionSecret(alloc, secret_store, external.gcs_credentials.scope);
        return resolved;
    }

    pub const CdcConnectionConfig = struct {
        provider: []u8,
        dsn: ?[]u8 = null,
        table_name: ?[]u8 = null,
        source_ordinal: ?u32 = null,
        external_table: ?[]u8 = null,
        slot_name: ?[]u8 = null,
        publication_name: ?[]u8 = null,

        fn deinit(self: *CdcConnectionConfig, alloc: std.mem.Allocator) void {
            alloc.free(self.provider);
            if (self.dsn) |value| alloc.free(value);
            if (self.table_name) |value| alloc.free(value);
            if (self.external_table) |value| alloc.free(value);
            if (self.slot_name) |value| alloc.free(value);
            if (self.publication_name) |value| alloc.free(value);
            self.* = undefined;
        }
    };

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
        return try parseFromSliceWithSecretsForDeployment(alloc, raw, secret_store, null);
    }

    pub fn parseFromSliceWithSecretsForDeployment(
        alloc: std.mem.Allocator,
        raw: []const u8,
        secret_store: ?*secrets.FileStore,
        expected_deployment: ?DeploymentMode,
    ) !Config {
        var raw_tree = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{
            .allocate = .alloc_always,
        });
        defer raw_tree.deinit();
        const raw_root = switch (raw_tree.value) {
            .object => |object| object,
            else => return error.InvalidConfig,
        };
        try rejectRemovedInferenceConfigFields(raw_root.get("inference"));

        var parsed_tree = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{
            .allocate = .alloc_always,
        });
        defer parsed_tree.deinit();
        var replacement_strings = std.ArrayList([]u8).empty;
        defer {
            for (replacement_strings.items) |value| alloc.free(value);
            replacement_strings.deinit(alloc);
        }
        try resolveSecretReferencesInValue(alloc, &parsed_tree.value, secret_store, &replacement_strings, .config_root);
        const root = switch (parsed_tree.value) {
            .object => |object| object,
            else => return error.InvalidConfig,
        };

        var validated = std.json.parseFromValue(common_openapi.Config, alloc, parsed_tree.value, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidConfig,
        };
        defer validated.deinit();

        const deployment_mode = try deploymentModeFromObject(root, expected_deployment);
        try validateStorageFromOpenApi(deployment_mode, root, validated.value.storage);
        var storage_config = try storageFromOpenApi(alloc, validated.value.storage, root.get("storage"));
        errdefer storage_config.deinit(alloc);
        var connections = try parseConnectionsConfig(alloc, root.get("connections"));
        errdefer deinitConnectionsConfig(alloc, &connections);
        try validateStorageConnections(&storage_config, &connections);

        var registry = try provider_registry.Registry.parseFromValue(alloc, raw_tree.value);
        errdefer registry.deinit();
        var transcribers = if (root.get("transcribers")) |transcribers_value|
            try transcribing.Registry.parseFromValue(alloc, transcribers_value)
        else
            transcribing.Registry.init(alloc);
        errdefer transcribers.deinit();
        var reader_registry = if (root.get("readers")) |readers_value|
            try readers.Registry.parseFromValue(alloc, readers_value)
        else
            readers.Registry.init(alloc);
        errdefer reader_registry.deinit();
        var text_to_speech = if (root.get("text_to_speech")) |text_to_speech_value|
            try synthesizing.Registry.parseFromValue(alloc, text_to_speech_value)
        else
            synthesizing.Registry.init(alloc);
        errdefer text_to_speech.deinit();

        const prompt_cache = if (validated.value.inference) |inference|
            try promptCacheFromOpenApi(inference.prompt_cache)
        else
            Config.InferenceConfig.PromptCacheConfig{};
        const inference_keep_alive_ms = if (validated.value.inference) |inference|
            try parseGoDurationMs(inference.keep_alive orelse "5m")
        else
            300_000;
        const inference_max_loaded_models = if (validated.value.inference) |inference|
            try nonNegativeUsize(inference.max_loaded_models orelse 10)
        else
            10;
        const inference_max_concurrent_requests = if (validated.value.inference) |inference|
            try nonNegativeUsize(inference.max_concurrent_requests orelse 32)
        else
            32;
        const inference_pool_size = if (validated.value.inference) |inference|
            try positiveUsize(inference.pool_size orelse 1)
        else
            1;
        return .{
            .registry = registry,
            .transcribers = transcribers,
            .readers = reader_registry,
            .text_to_speech = text_to_speech,
            .auth_enabled = try optionalBoolField(root, "enable_auth") orelse false,
            .deployment_mode = deployment_mode,
            .health_enabled = try optionalBoolField(root, "health_enabled") orelse true,
            .health_port = if (validated.value.health_port) |value|
                std.math.cast(u16, value) orelse return error.InvalidConfig
            else
                default_health_port,
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
            .storage = storage_config,
            .transaction_sessions = try transactionSessionConfigFromOpenApi(validated.value.transaction_sessions),
            .inference = if (validated.value.inference) |inference| .{
                .api_url = if (inference.api_url) |value|
                    if (value.len > 0) try alloc.dupe(u8, value) else null
                else
                    null,
                .api_key = try rawOptionalStringField(alloc, raw_root.get("inference"), "api_key"),
                .models_dir = if (inference.models_dir) |value| try alloc.dupe(u8, value) else null,
                .ml_dir = if (inference.ml_dir) |value| try alloc.dupe(u8, value) else null,
                .content_security = if (inference.content_security) |security| try contentSecurityFromOpenApi(alloc, security) else null,
                .s3_credentials = try parseRawInferenceS3Credentials(alloc, raw_root, inference.s3_credentials),
                .preload = try parseInferencePreloadModels(alloc, raw_root.get("inference")),
                .backend_priority = if (inference.backend_priority) |values| try dupOwnedStringSlice(alloc, values) else null,
                .pjrt_plugin_path = if (inference.pjrt_plugin_path) |value| try alloc.dupe(u8, value) else null,
                .prompt_cache = prompt_cache,
                .keep_alive_ms = inference_keep_alive_ms,
                .max_loaded_models = inference_max_loaded_models,
                .max_concurrent_requests = inference_max_concurrent_requests,
                .pool_size = inference_pool_size,
                .allow_downloads = inference.allow_downloads orelse true,
            } else .{},
            .remote_content = if (raw_root.get("remote_content")) |remote_content|
                try parseRemoteContentConfig(alloc, remote_content)
            else
                null,
            .connections = connections,
            .shard_allocation = .{
                .default_shards_per_table = try optionalU32Field(root, "default_shards_per_table") orelse if (deployment_mode.supportsLite()) default_standalone_shards_per_table else default_config_shards_per_table,
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
        raw_storage: ?std.json.Value,
    ) !StorageConfig {
        var parsed: StorageConfig = .{};
        errdefer parsed.deinit(alloc);

        const value = storage orelse return parsed;
        parsed.engine = value.engine;

        switch (parsed.engine) {
            .lite => {
                const lite = value.lite.?;
                parsed.lite_path = try alloc.dupe(u8, lite.path);
                parsed.lite_fsync = lite.fsync orelse true;
            },
            .local => {
                const local = value.local.?;
                if (local.base_dir) |base_dir| parsed.local_base_dir = try alloc.dupe(u8, base_dir);
            },
            .object => {
                const storage_value = raw_storage orelse return error.InvalidConfig;
                if (storage_value != .object) return error.InvalidConfig;
                const object_value = storage_value.object.get("object") orelse return error.InvalidConfig;
                if (object_value != .object) return error.InvalidConfig;
                parsed.object_connection = try requiredStringFieldDup(alloc, object_value.object, "connection");
                parsed.object_bucket = try requiredStringFieldDup(alloc, object_value.object, "bucket");
                parsed.object_prefix = try optionalStringFieldDup(alloc, object_value.object, "prefix");
                if (object_value.object.get("lanes")) |lanes| {
                    if (lanes != .object) return error.InvalidConfig;
                    parsed.object_lanes.artifacts = try parseObjectStorageLocation(alloc, lanes.object.get("artifacts"));
                    parsed.object_lanes.manifests = try parseObjectStorageLocation(alloc, lanes.object.get("manifests"));
                    parsed.object_lanes.wal = try parseObjectStorageLocation(alloc, lanes.object.get("wal"));
                    parsed.object_lanes.progress = try parseObjectStorageLocation(alloc, lanes.object.get("progress"));
                    parsed.object_lanes.catalog = try parseObjectStorageLocation(alloc, lanes.object.get("catalog"));
                }
            },
        }

        return parsed;
    }

    fn transactionSessionConfigFromOpenApi(value: ?common_openapi.TransactionSessionConfig) !TransactionSessionConfig {
        const cfg = value orelse return .{};
        return .{
            .ttl_seconds = try boundedPositiveInt(u64, cfg.ttl_seconds, 60, 604800, 3600),
            .cleanup_interval_seconds = try boundedPositiveInt(u64, cfg.cleanup_interval_seconds, 1, 3600, 60),
            .max_count = try boundedPositiveInt(usize, cfg.max_count, 1, 65536, 1024),
            .max_record_bytes = try boundedPositiveInt(usize, cfg.max_record_bytes, 65536, 67108864, 16 * 1024 * 1024),
            .max_savepoints = try boundedPositiveInt(usize, cfg.max_savepoints, 1, 1024, 64),
        };
    }

    fn validateStorageFromOpenApi(
        deployment_mode: DeploymentMode,
        root: std.json.ObjectMap,
        storage: ?common_openapi.StorageConfig,
    ) !void {
        const value = storage orelse return;
        const storage_value = root.get("storage") orelse return error.InvalidConfig;
        const storage_object = switch (storage_value) {
            .object => |object| object,
            else => return error.InvalidConfig,
        };
        if (!objectContainsOnly(storage_object, &.{ "engine", "lite", "local", "object" })) return error.InvalidConfig;
        const engine = value.engine;
        const has_lite = value.lite != null;
        const has_local = value.local != null;
        const has_object = value.object != null;
        const selected_count: u8 = @intFromBool(has_lite) + @intFromBool(has_local) + @intFromBool(has_object);
        if (selected_count > 1) return error.InvalidConfig;

        switch (engine) {
            .lite => {
                if (!deployment_mode.supportsLite()) return error.InvalidConfig;
                try validateObjectMemberFields(storage_object, "lite", &.{ "path", "fsync" });
                const lite = value.lite orelse return error.InvalidConfig;
                if (!std.mem.endsWith(u8, lite.path, ".aflite")) return error.InvalidConfig;
                if (has_local or has_object) return error.InvalidConfig;
                if ((try optionalU32Field(root, "replication_factor") orelse 1) != 1) return error.InvalidConfig;
                if ((try optionalU32Field(root, "default_shards_per_table") orelse 1) != 1) return error.InvalidConfig;
                if (try optionalBoolField(root, "disable_shard_alloc")) |disabled| {
                    if (!disabled) return error.InvalidConfig;
                }
                if (root.get("metadata")) |metadata| {
                    if (metadata != .object) return error.InvalidConfig;
                    inline for (.{ "orchestration_urls", "raft_urls" }) |field_name| {
                        if (metadata.object.get(field_name)) |urls| {
                            if (urls != .object or urls.object.count() != 0) return error.InvalidConfig;
                        }
                    }
                }
            },
            .local => {
                if (has_lite or has_object) return error.InvalidConfig;
                _ = value.local orelse return error.InvalidConfig;
                const local_value = storage_object.get("local") orelse return error.InvalidConfig;
                const local_object = switch (local_value) {
                    .object => |object| object,
                    else => return error.InvalidConfig,
                };
                if (!objectContainsOnly(local_object, &.{"base_dir"})) return error.InvalidConfig;
            },
            .object => {
                if (deployment_mode != .serverless) return error.InvalidConfig;
                try validateObjectMemberFields(storage_object, "object", &.{ "connection", "bucket", "prefix", "lanes" });
                _ = value.object orelse return error.InvalidConfig;
                if (has_lite or has_local) return error.InvalidConfig;
                const object_value = storage_object.get("object") orelse return error.InvalidConfig;
                if (object_value != .object) return error.InvalidConfig;
                const connection = try requiredStringField(object_value.object, "connection");
                const bucket = try requiredStringField(object_value.object, "bucket");
                if (connection.len == 0 or bucket.len < 3 or bucket.len > 63) return error.InvalidConfig;
                if (object_value.object.get("lanes")) |lanes| try validateObjectStorageLanes(lanes);
            },
        }
    }

    pub fn deinit(self: *Config) void {
        if (self.tls) |*tls| tls.deinit(self.registry.allocator);
        if (self.cors) |*cors| cors.deinit(self.registry.allocator);
        self.metadata.deinit(self.registry.allocator);
        self.storage.deinit(self.registry.allocator);
        self.inference.deinit(self.registry.allocator);
        self.transcribers.deinit();
        self.readers.deinit();
        self.text_to_speech.deinit();
        if (self.remote_content) |*remote_content| remote_content.deinit(self.registry.allocator);
        deinitConnectionsConfig(self.registry.allocator, &self.connections);
        self.registry.deinit();
        self.* = undefined;
    }

    pub fn effectiveAntflyContentSecurity(self: *const Config) ?*const ContentSecurityConfig {
        return scraping.effectiveContentSecurity(
            if (self.inference.content_security) |*security| security else null,
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

pub fn loadFromPathWithSecretsForDeployment(
    alloc: std.mem.Allocator,
    path: []const u8,
    secret_store: ?*secrets.FileStore,
    deployment_mode: DeploymentMode,
) !Config {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(16 * 1024 * 1024));
    defer alloc.free(raw);
    return try Config.parseFromSliceWithSecretsForDeployment(alloc, raw, secret_store, deployment_mode);
}

pub fn resolveLocalRoleBaseDir(alloc: std.mem.Allocator, cfg: ?*const Config, role: []const u8) ![]u8 {
    const base = try resolveLocalBaseDir(alloc, cfg);
    defer alloc.free(base);
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ base, role });
}

pub fn resolveLocalBaseDir(alloc: std.mem.Allocator, cfg: ?*const Config) ![]u8 {
    if (cfg) |loaded| {
        if (loaded.storage.local_base_dir) |dir| {
            return try alloc.dupe(u8, dir);
        }
    }
    return try defaultLocalBaseDir(alloc);
}

pub fn defaultLocalBaseDir(alloc: std.mem.Allocator) ![]u8 {
    const home_var = if (builtin.os.tag == .windows) "USERPROFILE" else "HOME";
    const home = platform.env.getenv(home_var) orelse return try alloc.dupe(u8, "antflydb");
    if (home.len == 0) return try alloc.dupe(u8, "antflydb");
    return try std.fs.path.join(alloc, &.{ home, ".antfly" });
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

fn deploymentModeFromObject(root: std.json.ObjectMap, expected: ?DeploymentMode) !DeploymentMode {
    if (root.get("deployment_mode")) |value| {
        if (value != .string) return error.InvalidConfig;
        inline for (std.meta.fields(DeploymentMode)) |field| {
            if (std.mem.eql(u8, value.string, field.name)) {
                const configured: DeploymentMode = @enumFromInt(field.value);
                if (expected) |required| if (configured != required) return error.DeploymentModeMismatch;
                return configured;
            }
        }
        return error.InvalidConfig;
    }
    return expected orelse .distributed;
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

test "common config keeps Lite as a storage engine independent of standalone topology" {
    var cfg = try Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "standalone",
        \\  "storage": {
        \\    "engine": "lite",
        \\    "lite": { "path": "data.antfly.aflite", "fsync": true }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1
        \\}
    );
    defer cfg.deinit();
    try std.testing.expectEqual(DeploymentMode.standalone, cfg.deployment_mode);
    try std.testing.expectEqual(common_openapi.StorageEngine.lite, cfg.storage.engine);
    try std.testing.expectEqualStrings("data.antfly.aflite", cfg.storage.lite_path.?);
    try std.testing.expect(cfg.storage.lite_fsync);
}

test "common config rejects Lite storage for distributed topology" {
    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "distributed",
        \\  "storage": {
        \\    "engine": "lite",
        \\    "lite": { "path": "data.aflite" }
        \\  }
        \\}
    ));
}

test "common config resolves capability-scoped object storage connections and lane overrides" {
    var cfg = try Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": {
        \\    "data": {
        \\      "kind": "external_io",
        \\      "capabilities": ["storage.primary"],
        \\      "external_io": { "protocol": "s3", "region": "us-west-2", "buckets": ["data-bucket"] }
        \\    },
        \\    "wal": {
        \\      "kind": "external_io",
        \\      "capabilities": ["storage.primary"],
        \\      "external_io": { "protocol": "s3", "endpoint": "minio:9000", "use_ssl": false, "buckets": ["wal-bucket"] }
        \\    }
        \\  },
        \\  "storage": {
        \\    "engine": "object",
        \\    "object": {
        \\      "connection": "data",
        \\      "bucket": "data-bucket",
        \\      "prefix": "cluster",
        \\      "lanes": { "wal": { "connection": "wal", "bucket": "wal-bucket", "prefix": "cluster-wal" } }
        \\    }
        \\  }
        \\}
    );
    defer cfg.deinit();
    try std.testing.expectEqual(common_openapi.StorageEngine.object, cfg.storage.engine);
    try std.testing.expectEqualStrings("data", cfg.storage.object_connection.?);
    try std.testing.expectEqualStrings("wal", cfg.storage.object_lanes.wal.connection.?);
    try std.testing.expectEqualStrings("wal-bucket", cfg.storage.object_lanes.wal.bucket.?);
    try std.testing.expectEqualStrings("us-west-2", cfg.connections.get("data").?.external_io.?.region.?);

    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": { "data": { "kind": "external_io", "capabilities": ["remote.read"], "external_io": { "protocol": "s3" } } },
        \\  "storage": { "engine": "object", "object": { "connection": "data", "bucket": "data-bucket" } }
        \\}
    ));
}

test "common config isolates named AWS credential sources and rejects credential typos" {
    var cfg = try Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": {
        \\    "data": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "s3", "buckets": ["data-bucket"], "credentials": { "source": "profile", "profile": "data-account", "shared_credentials_file": "/secrets/data-credentials" } } },
        \\    "wal": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "s3", "buckets": ["wal-bucket"], "credentials": { "source": "web_identity", "role_arn": "arn:aws:iam::1:role/wal", "token_file": "/secrets/wal-token" } } }
        \\  },
        \\  "storage": { "engine": "object", "object": { "connection": "data", "bucket": "data-bucket", "lanes": { "wal": { "connection": "wal", "bucket": "wal-bucket" } } } }
        \\}
    );
    defer cfg.deinit();
    const data = cfg.connections.get("data").?.external_io.?.credentials;
    try std.testing.expectEqual(Config.AwsCredentialSource.profile, data.source);
    try std.testing.expectEqualStrings("data-account", data.profile.?);
    const wal = cfg.connections.get("wal").?.external_io.?.credentials;
    try std.testing.expectEqual(Config.AwsCredentialSource.web_identity, wal.source);
    try std.testing.expectEqualStrings("/secrets/wal-token", wal.token_file.?);

    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": { "data": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "s3", "buckets": ["data-bucket"], "credentials": { "source": "static", "access_key": "typo", "secret_access_key": "secret" } } } },
        \\  "storage": { "engine": "object", "object": { "connection": "data", "bucket": "data-bucket" } }
        \\}
    ));
}

test "common config requires an explicit unique S3 bucket allowlist" {
    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "connections": {
        \\    "archive": { "kind": "external_io", "capabilities": ["backup.write"], "external_io": { "protocol": "s3" } }
        \\  }
        \\}
    ));
    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "connections": {
        \\    "archive": { "kind": "external_io", "capabilities": ["backup.write"], "external_io": { "protocol": "s3", "buckets": ["archive-bucket", "archive-bucket"] } }
        \\  }
        \\}
    ));
}

test "common config keeps GCS and filesystem connection credentials protocol scoped" {
    var cfg = try Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "connections": {
        \\    "reader": { "kind": "external_io", "capabilities": ["restore.read"], "external_io": { "protocol": "gcs", "buckets": ["archive-bucket"], "project_id": "reader-project", "credentials": { "source": "service_account", "credentials_path": "/secrets/gcs-reader.json" } } },
        \\    "local": { "kind": "external_io", "capabilities": ["backup.write"], "external_io": { "protocol": "filesystem", "root": "/var/lib/antfly/backups" } }
        \\  }
        \\}
    );
    defer cfg.deinit();
    const reader = cfg.connections.get("reader").?.external_io.?;
    try std.testing.expectEqual(Config.ExternalIoProtocol.gcs, reader.protocol);
    try std.testing.expectEqual(Config.GcsCredentialSource.service_account, reader.gcs_credentials.source);
    try std.testing.expectEqualStrings("/secrets/gcs-reader.json", reader.gcs_credentials.credentials_path.?);
    try std.testing.expectEqualStrings("/var/lib/antfly/backups", cfg.connections.get("local").?.external_io.?.root.?);

    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "connections": {
        \\    "reader": { "kind": "external_io", "capabilities": ["restore.read"], "external_io": { "protocol": "gcs", "buckets": ["archive-bucket"], "region": "us-west-2" } }
        \\  }
        \\}
    ));
    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "connections": {
        \\    "local": { "kind": "external_io", "capabilities": ["backup.write"], "external_io": { "protocol": "filesystem", "root": "relative/backups" } }
        \\  }
        \\}
    ));
}

test "common config command deployment context supplies topology and rejects conflicting assertions" {
    var cfg = try Config.parseFromSliceWithSecretsForDeployment(std.testing.allocator,
        \\{
        \\  "storage": { "engine": "lite", "lite": { "path": "data.aflite" } }
        \\}
    , null, .standalone);
    defer cfg.deinit();
    try std.testing.expectEqual(DeploymentMode.standalone, cfg.deployment_mode);
    try std.testing.expectError(error.DeploymentModeMismatch, Config.parseFromSliceWithSecretsForDeployment(std.testing.allocator,
        \\{
        \\  "deployment_mode": "distributed",
        \\  "storage": { "engine": "local", "local": {} }
        \\}
    , null, .standalone));
}

test "common config object storage connections enforce key prefix boundaries" {
    var cfg = try Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": {
        \\    "data": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "s3", "buckets": ["data-bucket"], "prefix": "tenant-a", "bucket_provisioning": "require_existing" } }
        \\  },
        \\  "storage": { "engine": "object", "object": { "connection": "data", "bucket": "data-bucket", "prefix": "tenant-a" } }
        \\}
    );
    defer cfg.deinit();
    try std.testing.expectEqual(Config.BucketProvisioning.require_existing, cfg.connections.get("data").?.external_io.?.bucket_provisioning);

    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": {
        \\    "data": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "s3", "buckets": ["data-bucket"], "prefix": "tenant-a" } }
        \\  },
        \\  "storage": { "engine": "object", "object": { "connection": "data", "bucket": "data-bucket", "prefix": "tenant-b" } }
        \\}
    ));
}

test "common config gives embedded Lite a single shard default" {
    var cfg = try Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "embedded",
        \\  "storage": {
        \\    "engine": "lite",
        \\    "lite": { "path": "embedded.aflite" }
        \\  }
        \\}
    );
    defer cfg.deinit();
    try std.testing.expectEqual(@as(u32, 1), cfg.shard_allocation.default_shards_per_table);
}

test "common config rejects distributed controls with Lite storage" {
    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "standalone",
        \\  "storage": { "engine": "lite", "lite": { "path": "data.aflite" } },
        \\  "disable_shard_alloc": false
        \\}
    ));
    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator,
        \\{
        \\  "deployment_mode": "standalone",
        \\  "storage": { "engine": "lite", "lite": { "path": "data.aflite" } },
        \\  "metadata": { "orchestration_urls": { "1": "http://127.0.0.1:7001" } }
        \\}
    ));
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

fn rawOptionalStringField(
    alloc: std.mem.Allocator,
    object_value: ?std.json.Value,
    field_name: []const u8,
) !?[]u8 {
    const value = object_value orelse return null;
    if (value != .object) return error.InvalidConfig;
    const field_value = value.object.get(field_name) orelse return null;
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

fn parseRawInferenceS3Credentials(
    alloc: std.mem.Allocator,
    raw_root: std.json.ObjectMap,
    fallback: ?s3_openapi.Credentials,
) !?Config.S3CredentialsConfig {
    if (raw_root.get("inference")) |inference_value| {
        if (inference_value == .object) {
            if (inference_value.object.get("s3_credentials")) |credentials_value| {
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

fn parseConnectionsConfig(alloc: std.mem.Allocator, maybe_value: ?std.json.Value) !Config.ConnectionsConfig {
    var connections = Config.ConnectionsConfig{};
    errdefer deinitConnectionsConfig(alloc, &connections);

    const value = maybe_value orelse return connections;
    if (value != .object) return error.InvalidConfig;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        const id = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(id);
        var connection = try parseConnectionConfig(alloc, entry.value_ptr.*);
        errdefer connection.deinit(alloc);

        const gop = try connections.getOrPut(alloc, id);
        if (gop.found_existing) {
            alloc.free(id);
            connection.deinit(alloc);
            return error.InvalidConfig;
        }
        gop.key_ptr.* = id;
        gop.value_ptr.* = connection;
    }

    return connections;
}

fn deinitConnectionsConfig(alloc: std.mem.Allocator, connections: *Config.ConnectionsConfig) void {
    var it = connections.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.key_ptr.*);
        entry.value_ptr.deinit(alloc);
    }
    connections.deinit(alloc);
}

fn validateStorageConnections(
    storage: *const Config.StorageConfig,
    connections: *const Config.ConnectionsConfig,
) !void {
    if (storage.engine != .object) return;
    const default_connection = storage.object_connection orelse return error.InvalidConfig;
    const default_bucket = storage.object_bucket orelse return error.InvalidConfig;
    const default_prefix = storage.object_prefix orelse "";
    const lanes = [_]Config.ObjectStorageLocation{
        storage.object_lanes.artifacts,
        storage.object_lanes.manifests,
        storage.object_lanes.wal,
        storage.object_lanes.progress,
        storage.object_lanes.catalog,
    };
    const lane_names = [_][]const u8{ "artifacts", "manifests", "wal", "progress", "catalog" };
    for (lanes, lane_names) |lane, lane_name| {
        try validateStorageConnection(
            connections,
            lane.connection orelse default_connection,
            lane.bucket orelse default_bucket,
            lane.prefix,
            default_prefix,
            lane_name,
        );
    }
}

fn validateStorageConnection(
    connections: *const Config.ConnectionsConfig,
    connection_id: []const u8,
    bucket: []const u8,
    explicit_prefix: ?[]const u8,
    default_prefix: []const u8,
    lane_name: []const u8,
) !void {
    const connection = connections.get(connection_id) orelse return error.InvalidConfig;
    if (connection.kind != .external_io) return error.InvalidConfig;
    const external = connection.external_io orelse return error.InvalidConfig;
    if (external.protocol != .s3) return error.InvalidConfig;
    var authorized = false;
    for (connection.capabilities) |capability| {
        if (std.mem.eql(u8, capability, "storage.primary")) {
            authorized = true;
            break;
        }
    }
    if (!authorized) return error.InvalidConfig;
    if (external.buckets.len == 0) return error.InvalidConfig;
    authorized = false;
    for (external.buckets) |allowed| {
        if (std.mem.eql(u8, allowed, bucket)) {
            authorized = true;
            break;
        }
    }
    if (!authorized) return error.InvalidConfig;
    if (external.prefix) |scope| {
        const normalized_scope = std.mem.trim(u8, scope, "/");
        if (normalized_scope.len > 0) {
            if (explicit_prefix) |target| {
                if (!prefixContains(normalized_scope, std.mem.trim(u8, target, "/"))) return error.InvalidConfig;
            } else {
                const normalized_default = std.mem.trim(u8, default_prefix, "/");
                if (normalized_default.len > 0) {
                    if (!derivedPrefixWithin(normalized_scope, normalized_default, lane_name)) return error.InvalidConfig;
                } else if (!prefixContains(normalized_scope, lane_name)) {
                    return error.InvalidConfig;
                }
            }
        }
    }
}

fn derivedPrefixWithin(scope: []const u8, base: []const u8, lane: []const u8) bool {
    if (prefixContains(scope, base)) return true;
    if (!std.mem.startsWith(u8, scope, base) or scope.len <= base.len or scope[base.len] != '/') return false;
    return prefixContains(scope[base.len + 1 ..], lane);
}

fn prefixContains(scope: []const u8, target: []const u8) bool {
    if (scope.len == 0) return true;
    if (!std.mem.startsWith(u8, target, scope)) return false;
    return target.len == scope.len or (target.len > scope.len and target[scope.len] == '/');
}

fn parseConnectionConfig(alloc: std.mem.Allocator, value: std.json.Value) !Config.ConnectionConfig {
    if (value != .object) return error.InvalidConfig;
    if (!objectContainsOnly(value.object, &.{ "display_name", "provider", "kind", "capabilities", "inference", "web_search", "external_io", "cdc" })) return error.InvalidConfig;
    const kind = try requiredEnumField(Config.ConnectionKind, value.object, "kind");
    const selected_member = switch (kind) {
        .inference => "inference",
        .web_search => "web_search",
        .external_io => "external_io",
        .cdc => "cdc",
    };
    for ([_][]const u8{ "inference", "web_search", "external_io", "cdc" }) |member| {
        if (std.mem.eql(u8, member, selected_member)) {
            if (value.object.get(member) == null) return error.InvalidConfig;
        } else if (value.object.get(member) != null) return error.InvalidConfig;
    }
    var connection = Config.ConnectionConfig{
        .display_name = try optionalStringFieldDup(alloc, value.object, "display_name"),
        .provider = try optionalStringFieldDup(alloc, value.object, "provider"),
        .kind = kind,
        .capabilities = try requiredStringArrayField(alloc, value.object, "capabilities"),
    };
    errdefer connection.deinit(alloc);

    switch (kind) {
        .inference => connection.inference = try parseInferenceConnectionConfig(alloc, value.object.get("inference") orelse return error.InvalidConfig),
        .web_search => {
            if (connection.provider == null) return error.InvalidConfig;
            connection.web_search = try parseWebSearchConnectionConfig(alloc, value.object.get("web_search") orelse return error.InvalidConfig);
        },
        .external_io => connection.external_io = try parseExternalIoConnectionConfig(alloc, value.object.get("external_io") orelse return error.InvalidConfig),
        .cdc => connection.cdc = try parseCdcConnectionConfig(alloc, value.object.get("cdc") orelse return error.InvalidConfig),
    }
    return connection;
}

fn parseInferenceConnectionConfig(alloc: std.mem.Allocator, value: std.json.Value) !Config.InferenceConnectionConfig {
    if (value != .object) return error.InvalidConfig;
    if (!objectContainsOnly(value.object, &.{ "provider", "url", "api_key", "region", "project_id", "location", "credentials_path", "names", "configured_model_types" })) return error.InvalidConfig;
    return .{
        .provider = try requiredStringFieldDup(alloc, value.object, "provider"),
        .url = try optionalStringFieldDup(alloc, value.object, "url"),
        .api_key = try optionalStringFieldDup(alloc, value.object, "api_key"),
        .region = try optionalStringFieldDup(alloc, value.object, "region"),
        .project_id = try optionalStringFieldDup(alloc, value.object, "project_id"),
        .location = try optionalStringFieldDup(alloc, value.object, "location"),
        .credentials_path = try optionalStringFieldDup(alloc, value.object, "credentials_path"),
        .names = try optionalStringArrayField(alloc, value.object, "names") orelse &.{},
        .configured_model_types = try optionalStringArrayField(alloc, value.object, "configured_model_types") orelse &.{},
    };
}

fn parseWebSearchConnectionConfig(alloc: std.mem.Allocator, value: std.json.Value) !Config.WebSearchConnectionConfig {
    if (value != .object) return error.InvalidConfig;
    if (!objectContainsOnly(value.object, &.{ "service", "max_results", "timeout_ms", "safe_search", "language", "region", "include_content", "include_highlights", "api_key", "endpoint", "project_id", "location", "data_store", "serving_config", "credentials_path", "include_domains", "exclude_domains" })) return error.InvalidConfig;
    return .{
        .service = try optionalStringFieldDup(alloc, value.object, "service"),
        .max_results = try optionalU32Field(value.object, "max_results"),
        .timeout_ms = try optionalU32Field(value.object, "timeout_ms"),
        .safe_search = try optionalBoolField(value.object, "safe_search"),
        .language = try optionalStringFieldDup(alloc, value.object, "language"),
        .region = try optionalStringFieldDup(alloc, value.object, "region"),
        .include_content = try optionalBoolField(value.object, "include_content"),
        .include_highlights = try optionalBoolField(value.object, "include_highlights"),
        .api_key = try optionalStringFieldDup(alloc, value.object, "api_key"),
        .endpoint = try optionalStringFieldDup(alloc, value.object, "endpoint"),
        .project_id = try optionalStringFieldDup(alloc, value.object, "project_id"),
        .location = try optionalStringFieldDup(alloc, value.object, "location"),
        .data_store = try optionalStringFieldDup(alloc, value.object, "data_store"),
        .serving_config = try optionalStringFieldDup(alloc, value.object, "serving_config"),
        .credentials_path = try optionalStringFieldDup(alloc, value.object, "credentials_path"),
        .include_domains = try optionalStringArrayField(alloc, value.object, "include_domains") orelse &.{},
        .exclude_domains = try optionalStringArrayField(alloc, value.object, "exclude_domains") orelse &.{},
    };
}

fn parseExternalIoConnectionConfig(alloc: std.mem.Allocator, value: std.json.Value) !Config.ExternalIoConnectionConfig {
    if (value != .object) return error.InvalidConfig;
    var cfg = Config.ExternalIoConnectionConfig{
        .protocol = try requiredEnumField(Config.ExternalIoProtocol, value.object, "protocol"),
    };
    errdefer cfg.deinit(alloc);
    switch (cfg.protocol) {
        .s3 => {
            if (!objectContainsOnly(value.object, &.{ "protocol", "endpoint", "region", "addressing_style", "bucket_provisioning", "buckets", "prefix", "credentials", "use_ssl" })) return error.InvalidConfig;
            cfg.endpoint = try optionalStringFieldDup(alloc, value.object, "endpoint");
            cfg.region = try optionalStringFieldDup(alloc, value.object, "region");
            cfg.addressing_style = try optionalEnumField(Config.S3AddressingStyle, value.object, "addressing_style") orelse .virtual_hosted;
            cfg.bucket_provisioning = try optionalEnumField(Config.BucketProvisioning, value.object, "bucket_provisioning") orelse .require_existing;
            cfg.buckets = try optionalStringArrayField(alloc, value.object, "buckets") orelse &.{};
            cfg.prefix = try optionalStringFieldDup(alloc, value.object, "prefix");
            cfg.credentials = try parseAwsCredentialConfig(alloc, value.object.get("credentials"));
            cfg.use_ssl = try optionalBoolField(value.object, "use_ssl");
        },
        .gcs => {
            if (!objectContainsOnly(value.object, &.{ "protocol", "endpoint", "upload_endpoint", "project_id", "bucket_provisioning", "buckets", "prefix", "credentials" })) return error.InvalidConfig;
            cfg.endpoint = try optionalStringFieldDup(alloc, value.object, "endpoint");
            cfg.upload_endpoint = try optionalStringFieldDup(alloc, value.object, "upload_endpoint");
            cfg.project_id = try optionalStringFieldDup(alloc, value.object, "project_id");
            cfg.bucket_provisioning = try optionalEnumField(Config.BucketProvisioning, value.object, "bucket_provisioning") orelse .require_existing;
            cfg.buckets = try optionalStringArrayField(alloc, value.object, "buckets") orelse &.{};
            cfg.prefix = try optionalStringFieldDup(alloc, value.object, "prefix");
            cfg.gcs_credentials = try parseGcsCredentialConfig(alloc, value.object.get("credentials"));
        },
        .filesystem => {
            if (!objectContainsOnly(value.object, &.{ "protocol", "root" })) return error.InvalidConfig;
            cfg.root = try requiredStringFieldDup(alloc, value.object, "root");
            if (cfg.root.?.len == 0 or !std.fs.path.isAbsolute(cfg.root.?)) return error.InvalidConfig;
        },
        .http => {
            if (!objectContainsOnly(value.object, &.{ "protocol", "hosts", "headers" })) return error.InvalidConfig;
            cfg.hosts = try optionalStringArrayField(alloc, value.object, "hosts") orelse &.{};
        },
    }

    if (cfg.protocol == .s3 or cfg.protocol == .gcs) {
        if (cfg.buckets.len == 0 or cfg.buckets.len > 64) return error.InvalidConfig;
        for (cfg.buckets, 0..) |bucket, i| {
            if (bucket.len < 3 or bucket.len > 63 or !std.mem.eql(u8, bucket, std.mem.trim(u8, bucket, " \t\r\n"))) {
                return error.InvalidConfig;
            }
            for (cfg.buckets[0..i]) |previous| {
                if (std.mem.eql(u8, previous, bucket)) return error.InvalidConfig;
            }
        }
    }

    if (value.object.get("headers")) |headers| {
        if (headers != .object) return error.InvalidConfig;
        var it = headers.object.iterator();
        while (it.next()) |entry| {
            const key = try alloc.dupe(u8, entry.key_ptr.*);
            errdefer alloc.free(key);
            const header_value = switch (entry.value_ptr.*) {
                .string => |string| try alloc.dupe(u8, string),
                else => return error.InvalidConfig,
            };
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

fn parseGcsCredentialConfig(alloc: std.mem.Allocator, maybe_value: ?std.json.Value) !Config.GcsCredentialConfig {
    const value = maybe_value orelse return .{};
    if (value != .object or !objectContainsOnly(value.object, &.{ "source", "bearer_token", "service_account_json", "credentials_path", "scope" })) return error.InvalidConfig;
    var cfg = Config.GcsCredentialConfig{
        .source = try requiredEnumField(Config.GcsCredentialSource, value.object, "source"),
        .bearer_token = try optionalStringFieldDup(alloc, value.object, "bearer_token"),
        .service_account_json = try optionalStringFieldDup(alloc, value.object, "service_account_json"),
        .credentials_path = try optionalStringFieldDup(alloc, value.object, "credentials_path"),
        .scope = try optionalStringFieldDup(alloc, value.object, "scope"),
    };
    errdefer cfg.deinit(alloc);
    switch (cfg.source) {
        .default => if (cfg.bearer_token != null or cfg.service_account_json != null or cfg.credentials_path != null) return error.InvalidConfig,
        .bearer_token => if (cfg.bearer_token == null or cfg.service_account_json != null or cfg.credentials_path != null) return error.InvalidConfig,
        .service_account => if ((cfg.service_account_json == null) == (cfg.credentials_path == null) or cfg.bearer_token != null) return error.InvalidConfig,
    }
    return cfg;
}

fn parseAwsCredentialConfig(alloc: std.mem.Allocator, maybe_value: ?std.json.Value) !Config.AwsCredentialConfig {
    const value = maybe_value orelse return .{};
    if (value != .object) return error.InvalidConfig;
    const source = try requiredEnumField(Config.AwsCredentialSource, value.object, "source");
    const allowed = switch (source) {
        .default => &[_][]const u8{"source"},
        .static => &[_][]const u8{ "source", "access_key_id", "secret_access_key", "session_token" },
        .profile => &[_][]const u8{ "source", "profile", "shared_credentials_file" },
        .web_identity => &[_][]const u8{ "source", "role_arn", "token_file", "session_name", "sts_endpoint" },
    };
    if (!objectContainsOnly(value.object, allowed)) return error.InvalidConfig;
    var cfg = Config.AwsCredentialConfig{ .source = source };
    errdefer cfg.deinit(alloc);
    cfg.access_key_id = try optionalStringFieldDup(alloc, value.object, "access_key_id");
    cfg.secret_access_key = try optionalStringFieldDup(alloc, value.object, "secret_access_key");
    cfg.session_token = try optionalStringFieldDup(alloc, value.object, "session_token");
    cfg.profile = try optionalStringFieldDup(alloc, value.object, "profile");
    cfg.shared_credentials_file = try optionalStringFieldDup(alloc, value.object, "shared_credentials_file");
    cfg.role_arn = try optionalStringFieldDup(alloc, value.object, "role_arn");
    cfg.token_file = try optionalStringFieldDup(alloc, value.object, "token_file");
    cfg.session_name = try optionalStringFieldDup(alloc, value.object, "session_name");
    cfg.sts_endpoint = try optionalStringFieldDup(alloc, value.object, "sts_endpoint");
    switch (source) {
        .default => {},
        .static => if (cfg.access_key_id == null or cfg.secret_access_key == null) return error.InvalidConfig,
        .profile => if (cfg.profile == null) return error.InvalidConfig,
        .web_identity => if (cfg.role_arn == null or cfg.token_file == null) return error.InvalidConfig,
    }
    return cfg;
}

fn parseCdcConnectionConfig(alloc: std.mem.Allocator, value: std.json.Value) !Config.CdcConnectionConfig {
    if (value != .object) return error.InvalidConfig;
    if (!objectContainsOnly(value.object, &.{ "provider", "dsn", "table_name", "source_ordinal", "external_table", "slot_name", "publication_name" })) return error.InvalidConfig;
    return .{
        .provider = try requiredStringFieldDup(alloc, value.object, "provider"),
        .dsn = try optionalStringFieldDup(alloc, value.object, "dsn"),
        .table_name = try optionalStringFieldDup(alloc, value.object, "table_name"),
        .source_ordinal = try optionalU32Field(value.object, "source_ordinal"),
        .external_table = try optionalStringFieldDup(alloc, value.object, "external_table"),
        .slot_name = try optionalStringFieldDup(alloc, value.object, "slot_name"),
        .publication_name = try optionalStringFieldDup(alloc, value.object, "publication_name"),
    };
}

fn requiredEnumField(comptime T: type, object: std.json.ObjectMap, field_name: []const u8) !T {
    const value = object.get(field_name) orelse return error.InvalidConfig;
    if (value != .string) return error.InvalidConfig;
    return std.meta.stringToEnum(T, value.string) orelse error.InvalidConfig;
}

fn requiredStringField(object: std.json.ObjectMap, field_name: []const u8) ![]const u8 {
    const value = object.get(field_name) orelse return error.InvalidConfig;
    if (value != .string) return error.InvalidConfig;
    return value.string;
}

fn parseObjectStorageLocation(
    alloc: std.mem.Allocator,
    maybe_value: ?std.json.Value,
) !Config.ObjectStorageLocation {
    const value = maybe_value orelse return .{};
    if (value != .object) return error.InvalidConfig;
    var location: Config.ObjectStorageLocation = .{};
    errdefer location.deinit(alloc);
    location.connection = try optionalStringFieldDup(alloc, value.object, "connection");
    location.bucket = try optionalStringFieldDup(alloc, value.object, "bucket");
    location.prefix = try optionalStringFieldDup(alloc, value.object, "prefix");
    return location;
}

fn validateObjectStorageLanes(value: std.json.Value) !void {
    if (value != .object or !objectContainsOnly(value.object, &.{ "artifacts", "manifests", "wal", "progress", "catalog" }))
        return error.InvalidConfig;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object or
            !objectContainsOnly(entry.value_ptr.object, &.{ "connection", "bucket", "prefix" }))
            return error.InvalidConfig;
        if (entry.value_ptr.object.get("connection")) |connection| {
            if (connection != .string or connection.string.len == 0) return error.InvalidConfig;
        }
        if (entry.value_ptr.object.get("bucket")) |bucket| {
            if (bucket != .string or bucket.string.len < 3 or bucket.string.len > 63) return error.InvalidConfig;
        }
    }
}

fn optionalEnumField(comptime T: type, object: std.json.ObjectMap, field_name: []const u8) !?T {
    const value = object.get(field_name) orelse return null;
    if (value != .string) return error.InvalidConfig;
    return std.meta.stringToEnum(T, value.string) orelse error.InvalidConfig;
}

fn objectContainsOnly(object: std.json.ObjectMap, allowed: []const []const u8) bool {
    var it = object.iterator();
    while (it.next()) |entry| {
        var found = false;
        for (allowed) |field| {
            if (std.mem.eql(u8, entry.key_ptr.*, field)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

fn validateObjectMemberFields(parent: std.json.ObjectMap, name: []const u8, allowed: []const []const u8) !void {
    const value = parent.get(name) orelse return error.InvalidConfig;
    const object = switch (value) {
        .object => |member| member,
        else => return error.InvalidConfig,
    };
    if (!objectContainsOnly(object, allowed)) return error.InvalidConfig;
}

fn boundedPositiveInt(comptime T: type, value: ?i64, min: T, max: T, default_value: T) !T {
    const raw = value orelse return default_value;
    const cast = std.math.cast(T, raw) orelse return error.InvalidConfig;
    if (cast < min or cast > max) return error.InvalidConfig;
    return cast;
}

fn requiredStringFieldDup(alloc: std.mem.Allocator, object: std.json.ObjectMap, field_name: []const u8) ![]u8 {
    return try optionalStringFieldDup(alloc, object, field_name) orelse error.InvalidConfig;
}

fn optionalStringFieldDup(alloc: std.mem.Allocator, object: std.json.ObjectMap, field_name: []const u8) !?[]u8 {
    const value = object.get(field_name) orelse return null;
    return switch (value) {
        .string => try alloc.dupe(u8, value.string),
        else => error.InvalidConfig,
    };
}

fn requiredStringArrayField(alloc: std.mem.Allocator, object: std.json.ObjectMap, field_name: []const u8) ![]const []u8 {
    return try optionalStringArrayField(alloc, object, field_name) orelse error.InvalidConfig;
}

fn optionalStringArrayField(alloc: std.mem.Allocator, object: std.json.ObjectMap, field_name: []const u8) !?[]const []u8 {
    const value = object.get(field_name) orelse return null;
    if (value != .array) return error.InvalidConfig;
    const out = try alloc.alloc([]u8, value.array.items.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |entry| alloc.free(entry);
        alloc.free(out);
    }
    for (value.array.items, 0..) |item, i| {
        out[i] = switch (item) {
            .string => |string| try alloc.dupe(u8, string),
            else => return error.InvalidConfig,
        };
        filled = i + 1;
    }
    return out;
}

fn promptCacheFromOpenApi(
    value: ?inference_config_openapi.PromptCacheConfig,
) !Config.InferenceConfig.PromptCacheConfig {
    const config = value orelse return .{};
    const max_bytes_mb = try nonNegativeUsize(config.max_bytes_mb orelse 512);
    if (max_bytes_mb > std.math.maxInt(usize) / (1024 * 1024)) return error.InvalidConfig;
    const min_tokens = try nonNegativeUsize(config.min_tokens orelse 64);
    const ttl_ms_raw = config.ttl_ms orelse 300_000;
    if (ttl_ms_raw < 0) return error.InvalidConfig;
    const ttl_ms = std.math.cast(u64, ttl_ms_raw) orelse return error.InvalidConfig;
    const mode_raw = config.mode orelse "block_hash";
    const mode: Config.InferenceConfig.PromptCacheMode = if (std.mem.eql(u8, mode_raw, "block_hash"))
        .block_hash
    else if (std.mem.eql(u8, mode_raw, "simple"))
        .simple
    else
        return error.InvalidConfig;
    return .{
        .enabled = config.enabled orelse false,
        .mode = mode,
        .max_bytes_mb = max_bytes_mb,
        .min_tokens = min_tokens,
        .ttl_ms = ttl_ms,
    };
}

fn nonNegativeUsize(value: i64) !usize {
    if (value < 0) return error.InvalidConfig;
    return std.math.cast(usize, value) orelse error.InvalidConfig;
}

fn positiveUsize(value: i64) !usize {
    if (value <= 0) return error.InvalidConfig;
    return std.math.cast(usize, value) orelse error.InvalidConfig;
}

fn parseGoDurationMs(value: []const u8) !u64 {
    if (std.mem.eql(u8, value, "0")) return 0;
    if (value.len == 0) return error.InvalidConfig;

    var total_ns: u128 = 0;
    var offset: usize = 0;
    while (offset < value.len) {
        const whole_start = offset;
        while (offset < value.len and std.ascii.isDigit(value[offset])) : (offset += 1) {}
        if (offset == whole_start) return error.InvalidConfig;
        const whole = std.fmt.parseInt(u128, value[whole_start..offset], 10) catch return error.InvalidConfig;

        var fraction: u128 = 0;
        var fraction_scale: u128 = 1;
        if (offset < value.len and value[offset] == '.') {
            offset += 1;
            const fraction_start = offset;
            while (offset < value.len and std.ascii.isDigit(value[offset])) : (offset += 1) {
                if (fraction_scale > std.math.maxInt(u128) / 10) return error.InvalidConfig;
                fraction_scale *= 10;
            }
            if (offset == fraction_start) return error.InvalidConfig;
            fraction = std.fmt.parseInt(u128, value[fraction_start..offset], 10) catch return error.InvalidConfig;
        }

        const unit_ns: u128 = if (std.mem.startsWith(u8, value[offset..], "ms")) unit: {
            offset += 2;
            break :unit std.time.ns_per_ms;
        } else if (std.mem.startsWith(u8, value[offset..], "us")) unit: {
            offset += 2;
            break :unit std.time.ns_per_us;
        } else if (std.mem.startsWith(u8, value[offset..], "ns")) unit: {
            offset += 2;
            break :unit 1;
        } else if (std.mem.startsWith(u8, value[offset..], "h")) unit: {
            offset += 1;
            break :unit std.time.ns_per_hour;
        } else if (std.mem.startsWith(u8, value[offset..], "m")) unit: {
            offset += 1;
            break :unit std.time.ns_per_min;
        } else if (std.mem.startsWith(u8, value[offset..], "s")) unit: {
            offset += 1;
            break :unit std.time.ns_per_s;
        } else return error.InvalidConfig;

        const whole_ns = std.math.mul(u128, whole, unit_ns) catch return error.InvalidConfig;
        const fraction_ns = std.math.mul(u128, fraction, unit_ns) catch return error.InvalidConfig;
        total_ns = std.math.add(u128, total_ns, whole_ns) catch return error.InvalidConfig;
        total_ns = std.math.add(u128, total_ns, fraction_ns / fraction_scale) catch return error.InvalidConfig;
    }

    if (total_ns == 0) return error.InvalidConfig;
    const rounded_numerator = std.math.add(u128, total_ns, std.time.ns_per_ms - 1) catch return error.InvalidConfig;
    const rounded_ms = rounded_numerator / std.time.ns_per_ms;
    return std.math.cast(u64, rounded_ms) orelse error.InvalidConfig;
}

fn parseInferencePreloadModels(
    alloc: std.mem.Allocator,
    raw_inference: ?std.json.Value,
) ![]Config.InferenceConfig.WarmModelConfig {
    const value = raw_inference orelse return &.{};
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidConfig,
    };
    const preload_value = object.get("preload") orelse return &.{};
    if (preload_value != .array) return error.InvalidConfig;

    const out = try alloc.alloc(Config.InferenceConfig.WarmModelConfig, preload_value.array.items.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |*model| model.deinit(alloc);
        alloc.free(out);
    }

    for (preload_value.array.items, 0..) |item, i| {
        const model_object = switch (item) {
            .object => |entry| entry,
            else => return error.InvalidConfig,
        };
        out[i] = .{
            .kind = try requiredStringFieldDup(alloc, model_object, "kind"),
            .name = try requiredStringFieldDup(alloc, model_object, "name"),
            .backend = try optionalStringFieldDup(alloc, model_object, "backend"),
            .format = try optionalStringFieldDup(alloc, model_object, "format"),
            .quantization = try optionalStringFieldDup(alloc, model_object, "quantization"),
        };
        filled = i + 1;
    }
    return out;
}

fn rejectRemovedInferenceConfigFields(value: ?std.json.Value) !void {
    const inference_value = value orelse return;
    const inference = switch (inference_value) {
        .object => |object| object,
        else => return error.InvalidConfig,
    };
    inline for (.{
        "model_strategies",
        "embedder_models_dir",
        "chunker_models_dir",
        "reranker_models_dir",
        "max_queue_size",
        "request_timeout",
        "max_memory_mb",
    }) |field| {
        if (inference.contains(field)) return error.InvalidConfig;
    }
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
    if (values.len == 0) return;
    for (values) |value| alloc.free(value);
    alloc.free(values);
}

fn resolveOptionalConnectionSecret(
    alloc: std.mem.Allocator,
    secret_store: ?*secrets.FileStore,
    raw: ?[]const u8,
) !?[]u8 {
    const value = raw orelse return null;
    return try secrets.resolveReferenceOwned(alloc, secret_store, value);
}

const SecretResolutionContext = enum {
    config_root,
    connections,
    connection,
    external_io,
    normal,
};

fn resolveSecretReferencesInValue(
    alloc: std.mem.Allocator,
    value: *std.json.Value,
    secret_store: ?*secrets.FileStore,
    replacement_strings: *std.ArrayList([]u8),
    context: SecretResolutionContext,
) !void {
    switch (value.*) {
        .string => |raw| {
            if (secrets.parseSecretReference(raw) == null) return;
            const resolved = try secrets.resolveReferenceOwned(alloc, secret_store, raw);
            try replacement_strings.append(alloc, resolved);
            value.* = .{ .string = resolved };
        },
        .array => |*arr| {
            for (arr.items) |*item| try resolveSecretReferencesInValue(alloc, item, secret_store, replacement_strings, .normal);
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                // External-I/O credentials are operational secrets: retain
                // references in the immutable node config and resolve them at
                // each backup, restore, or probe. This makes rotation effective
                // without weakening bucket/prefix authorization. Other config
                // secrets keep their established startup-resolution behavior.
                const child_context: SecretResolutionContext = switch (context) {
                    .config_root => if (std.mem.eql(u8, entry.key_ptr.*, "connections")) .connections else .normal,
                    .connections => .connection,
                    .connection => if (std.mem.eql(u8, entry.key_ptr.*, "external_io")) .external_io else .normal,
                    .external_io => if (std.mem.eql(u8, entry.key_ptr.*, "credentials")) continue else .normal,
                    .normal => .normal,
                };
                try resolveSecretReferencesInValue(alloc, entry.value_ptr, secret_store, replacement_strings, child_context);
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
        \\    "engine": "local",
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
        \\  "transcribers": {
        \\    "whisper-local": { "provider": "antfly", "api_url": "http://127.0.0.1:8080", "model": "openai/whisper-base" }
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
    try std.testing.expectEqualStrings("whisper-local", cfg.transcribers.defaultProviderName().?);
    try std.testing.expectEqualStrings("nova", cfg.text_to_speech.defaultProviderName().?);
    try std.testing.expectEqual(transcribing.Provider.antfly, (try cfg.transcribers.getConfig(null)).provider);
    try std.testing.expectEqual(synthesizing.Provider.openai, (try cfg.text_to_speech.getConfig(null)).provider);
    try std.testing.expectEqual(@as(u32, 1), cfg.shard_allocation.default_shards_per_table);
    try std.testing.expectEqual(@as(u64, 1024), cfg.shard_allocation.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, 4), cfg.shard_allocation.max_shards_per_table);
    try std.testing.expectEqual(@as(u64, 90000), cfg.shard_allocation.shard_cooldown_millis);
    try std.testing.expectEqual(@as(u64, 180000), cfg.shard_allocation.min_shard_merge_age_millis);
}

test "common config extracts antfly settings" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4,
        \\  "inference": {
        \\    "api_url": "http://127.0.0.1:8083",
        \\    "models_dir": "/tmp/models",
        \\    "ml_dir": "/tmp/ml",
        \\    "keep_alive": "1h30m",
        \\    "max_loaded_models": 3,
        \\    "max_concurrent_requests": 4,
        \\    "pool_size": 2,
        \\    "pjrt_plugin_path": "/usr/local/lib/libtpu.so",
        \\    "prompt_cache": {
        \\      "enabled": true,
        \\      "mode": "simple",
        \\      "max_bytes_mb": 256,
        \\      "min_tokens": 32,
        \\      "ttl_ms": 60000
        \\    },
        \\    "preload": [
        \\      { "kind": "generator", "name": "antflydb/gemma-e2b", "backend": "metal" }
        \\    ],
        \\    "content_security": {
        \\      "allowed_hosts": ["models.example.com"],
        \\      "block_private_ips": true
        \\    },
        \\    "s3_credentials": {
        \\      "endpoint": "s3.amazonaws.com",
        \\      "access_key_id": "antfly-key",
        \\      "secret_access_key": "antfly-secret"
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
    try std.testing.expectEqualStrings("http://127.0.0.1:8083", cfg.inference.api_url.?);
    try std.testing.expectEqualStrings("/tmp/models", cfg.inference.models_dir.?);
    try std.testing.expectEqualStrings("/tmp/ml", cfg.inference.ml_dir.?);
    try std.testing.expectEqual(@as(u64, 5_400_000), cfg.inference.keep_alive_ms);
    try std.testing.expectEqual(@as(usize, 3), cfg.inference.max_loaded_models);
    try std.testing.expectEqual(@as(usize, 4), cfg.inference.max_concurrent_requests);
    try std.testing.expectEqual(@as(usize, 2), cfg.inference.pool_size);
    try std.testing.expectEqualStrings("/usr/local/lib/libtpu.so", cfg.inference.pjrt_plugin_path.?);
    try std.testing.expect(cfg.inference.prompt_cache.enabled);
    try std.testing.expectEqual(Config.InferenceConfig.PromptCacheMode.simple, cfg.inference.prompt_cache.mode);
    try std.testing.expectEqual(@as(usize, 256), cfg.inference.prompt_cache.max_bytes_mb);
    try std.testing.expectEqual(@as(usize, 32), cfg.inference.prompt_cache.min_tokens);
    try std.testing.expectEqual(@as(u64, 60_000), cfg.inference.prompt_cache.ttl_ms);
    try std.testing.expectEqual(@as(usize, 1), cfg.inference.preload.len);
    try std.testing.expectEqualStrings("generator", cfg.inference.preload[0].kind);
    try std.testing.expectEqualStrings("antflydb/gemma-e2b", cfg.inference.preload[0].name);
    try std.testing.expectEqualStrings("metal", cfg.inference.preload[0].backend.?);
    try std.testing.expectEqualStrings("models.example.com", cfg.inference.content_security.?.allowed_hosts.?[0]);
    try std.testing.expectEqual(@as(?bool, true), cfg.inference.content_security.?.block_private_ips);
    try std.testing.expectEqualStrings("s3.amazonaws.com", cfg.inference.s3_credentials.?.endpoint.?);
    try std.testing.expectEqualStrings("antfly-key", cfg.inference.s3_credentials.?.access_key_id.?);
    try std.testing.expectEqualStrings("antfly-secret", cfg.inference.s3_credentials.?.secret_access_key.?);
}

test "common config parses documented model keep-alive durations" {
    try std.testing.expectEqual(@as(u64, 0), try parseGoDurationMs("0"));
    try std.testing.expectEqual(@as(u64, 300_000), try parseGoDurationMs("5m"));
    try std.testing.expectEqual(@as(u64, 5_400_000), try parseGoDurationMs("1h30m"));
    try std.testing.expectEqual(@as(u64, 1_500), try parseGoDurationMs("1.5s"));
    try std.testing.expectError(error.InvalidConfig, parseGoDurationMs("forever"));
    try std.testing.expectError(error.InvalidConfig, nonNegativeUsize(-1));
    try std.testing.expectError(error.InvalidConfig, positiveUsize(0));
}

test "common config parses inference preload" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "inference": {
        \\    "api_url": "http://127.0.0.1:8090",
        \\    "preload": [
        \\      { "kind": "generator", "name": "antflydb/gemma-e2b" },
        \\      { "kind": "reranker", "name": "BAAI/bge-reranker", "backend": "native" }
        \\    ]
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 2), cfg.inference.preload.len);
    try std.testing.expectEqualStrings("generator", cfg.inference.preload[0].kind);
    try std.testing.expectEqualStrings("antflydb/gemma-e2b", cfg.inference.preload[0].name);
    try std.testing.expectEqualStrings("reranker", cfg.inference.preload[1].kind);
    try std.testing.expectEqualStrings("BAAI/bge-reranker", cfg.inference.preload[1].name);
    try std.testing.expectEqualStrings("native", cfg.inference.preload[1].backend.?);
}

test "common config preserves preload artifact selectors" {
    const raw =
        \\{"inference":{"preload":[{"kind":"generator","name":"model","format":"gguf","quantization":"q4_k"}]}}
    ;
    var cfg = try Config.parseFromSlice(std.testing.allocator, raw);
    defer cfg.deinit();
    try std.testing.expectEqualStrings("gguf", cfg.inference.preload[0].format.?);
    try std.testing.expectEqualStrings("q4_k", cfg.inference.preload[0].quantization.?);
}

test "common config rejects removed inference compatibility fields" {
    inline for (.{
        "model_strategies",
        "embedder_models_dir",
        "chunker_models_dir",
        "reranker_models_dir",
        "max_queue_size",
        "request_timeout",
        "max_memory_mb",
    }) |field| {
        const raw = try std.fmt.allocPrint(
            std.testing.allocator,
            "{{\"inference\":{{\"{s}\":null}}}}",
            .{field},
        );
        defer std.testing.allocator.free(raw);
        try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(std.testing.allocator, raw));
    }
}

test "common config accepts zero and rejects invalid prompt cache policy" {
    const alloc = std.testing.allocator;
    const zero_raw =
        \\{"inference":{"api_url":"http://127.0.0.1:8090","prompt_cache":{"max_bytes_mb":0,"min_tokens":0,"ttl_ms":0}}}
    ;
    var zero = try Config.parseFromSlice(alloc, zero_raw);
    defer zero.deinit();
    try std.testing.expectEqual(@as(usize, 0), zero.inference.prompt_cache.max_bytes_mb);
    try std.testing.expectEqual(@as(usize, 0), zero.inference.prompt_cache.min_tokens);
    try std.testing.expectEqual(@as(u64, 0), zero.inference.prompt_cache.ttl_ms);

    inline for (.{
        \\{"inference":{"api_url":"http://127.0.0.1:8090","prompt_cache":{"mode":"unknown"}}}
        ,
        \\{"inference":{"api_url":"http://127.0.0.1:8090","prompt_cache":{"max_bytes_mb":-1}}}
        ,
        \\{"inference":{"api_url":"http://127.0.0.1:8090","prompt_cache":{"min_tokens":-1}}}
        ,
        \\{"inference":{"api_url":"http://127.0.0.1:8090","prompt_cache":{"ttl_ms":-1}}}
    }) |raw| {
        try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(alloc, raw));
    }
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
        \\    "engine": "local",
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
        \\    "engine": "local",
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

test "common config preserves remote content logging and storage fields" {
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
        \\  "deployment_mode": "standalone",
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb" }
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

    try std.testing.expectEqual(DeploymentMode.standalone, cfg.deployment_mode);
    try std.testing.expect(cfg.health_enabled);
    try std.testing.expectEqual(@as(?u16, 4200), cfg.health_port);
    try std.testing.expectEqual(logging_openapi.Level.debug, cfg.log.?.level.?);
    try std.testing.expectEqual(logging_openapi.Style.json, cfg.log.?.style.?);
    try std.testing.expectEqualStrings("antflydb", cfg.storage.local_base_dir.?);

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

test "common config parses public connections map" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "connections": {
        \\    "openai-prod": {
        \\      "display_name": "OpenAI prod",
        \\      "kind": "inference",
        \\      "capabilities": ["models.generate", "models.embed", "agents.use"],
        \\      "inference": {
        \\        "provider": "openai",
        \\        "url": "https://api.openai.com/v1",
        \\        "api_key": "sk-test",
        \\        "names": ["primary-generator", "primary-embedder"],
        \\        "configured_model_types": ["generator", "embedder"]
        \\      }
        \\    },
        \\    "docs-site": {
        \\      "kind": "external_io",
        \\      "capabilities": ["content.fetch", "indexing.use", "agents.use"],
        \\      "external_io": {
        \\        "protocol": "http",
        \\        "hosts": ["https://docs.example.com"],
        \\        "headers": {
        \\          "Authorization": "Bearer abc"
        \\        }
        \\      }
        \\    },
        \\    "agent-web": {
        \\      "kind": "web_search",
        \\      "provider": "exa",
        \\      "capabilities": ["web.search", "web.semantic_search", "web.fetch", "agents.use"],
        \\      "web_search": {
        \\        "max_results": 10,
        \\        "safe_search": true,
        \\        "include_content": true,
        \\        "include_highlights": true,
        \\        "api_key": "exa-test-key",
        \\        "include_domains": ["docs.example.com"]
        \\      }
        \\    },
        \\    "users-pg": {
        \\      "kind": "cdc",
        \\      "capabilities": ["cdc.read_stream"],
        \\      "cdc": {
        \\        "provider": "postgres",
        \\        "dsn": "postgres://example",
        \\        "table_name": "users",
        \\        "source_ordinal": 0,
        \\        "external_table": "public.users"
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

    try std.testing.expectEqual(@as(usize, 4), cfg.connections.count());

    const inference = cfg.connections.get("openai-prod").?;
    try std.testing.expectEqual(Config.ConnectionKind.inference, inference.kind);
    try std.testing.expectEqualStrings("OpenAI prod", inference.display_name.?);
    try std.testing.expectEqualStrings("models.generate", inference.capabilities[0]);
    try std.testing.expectEqualStrings("openai", inference.inference.?.provider);
    try std.testing.expectEqualStrings("primary-embedder", inference.inference.?.names[1]);
    try std.testing.expectEqualStrings("embedder", inference.inference.?.configured_model_types[1]);

    const external_io = cfg.connections.get("docs-site").?;
    try std.testing.expectEqual(Config.ConnectionKind.external_io, external_io.kind);
    try std.testing.expectEqual(Config.ExternalIoProtocol.http, external_io.external_io.?.protocol);
    try std.testing.expectEqualStrings("https://docs.example.com", external_io.external_io.?.hosts[0]);
    try std.testing.expectEqualStrings("Bearer abc", external_io.external_io.?.headers.get("Authorization").?);

    const web_search = cfg.connections.get("agent-web").?;
    try std.testing.expectEqual(Config.ConnectionKind.web_search, web_search.kind);
    try std.testing.expectEqualStrings("exa", web_search.provider.?);
    try std.testing.expectEqualStrings("web.semantic_search", web_search.capabilities[1]);
    try std.testing.expectEqual(@as(?u32, 10), web_search.web_search.?.max_results);
    try std.testing.expectEqual(true, web_search.web_search.?.safe_search.?);
    try std.testing.expectEqual(true, web_search.web_search.?.include_content.?);
    try std.testing.expectEqualStrings("exa-test-key", web_search.web_search.?.api_key.?);
    try std.testing.expectEqualStrings("docs.example.com", web_search.web_search.?.include_domains[0]);

    const cdc = cfg.connections.get("users-pg").?;
    try std.testing.expectEqual(Config.ConnectionKind.cdc, cdc.kind);
    try std.testing.expectEqualStrings("postgres", cdc.cdc.?.provider);
    try std.testing.expectEqualStrings("users", cdc.cdc.?.table_name.?);
    try std.testing.expectEqual(@as(?u32, 0), cdc.cdc.?.source_ordinal);
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
        \\    "engine": "local",
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
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4,
        \\  "transcribers": {
        \\    "whisper-local": {
        \\      "provider": "antfly",
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

    try std.testing.expectEqualStrings("whisper-local", cfg.transcribers.defaultProviderName().?);
    try std.testing.expectEqualStrings("narrator", cfg.text_to_speech.defaultProviderName().?);
    try std.testing.expectEqual(transcribing.Provider.antfly, (try cfg.transcribers.getConfig(null)).provider);
    try std.testing.expectEqualStrings("openai/whisper-base", (try cfg.transcribers.getConfig(null)).model.?);
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
    var stored = try secret_store.put(alloc, "inference.api_url", "http://127.0.0.1:8089");
    stored.deinit(alloc);

    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4,
        \\  "inference": {
        \\    "api_url": "${secret:inference.api_url}"
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSliceWithSecrets(alloc, raw, &secret_store);
    defer cfg.deinit();
    try std.testing.expectEqualStrings("http://127.0.0.1:8089", cfg.inference.api_url.?);
}

test "common config external io credentials retain references and observe secret rotation" {
    const alloc = std.testing.allocator;
    const store_path = ".zig-cache/test-connection-secret-rotation.json";
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};
    }
    var secret_store = try secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();
    var first_key = try secret_store.put(alloc, "archive.key", "KEY-ONE");
    defer first_key.deinit(alloc);
    var first_secret = try secret_store.put(alloc, "archive.secret", "SECRET-ONE");
    defer first_secret.deinit(alloc);

    const raw =
        \\{
        \\  "storage": { "engine": "local", "local": { "base_dir": "antflydb" } },
        \\  "connections": {
        \\    "archive": {
        \\      "kind": "external_io",
        \\      "capabilities": ["backup.write"],
        \\      "external_io": {
        \\        "protocol": "s3",
        \\        "buckets": ["archive-bucket"],
        \\        "credentials": {
        \\          "source": "static",
        \\          "access_key_id": "${secret:archive.key}",
        \\          "secret_access_key": "${secret:archive.secret}"
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;
    var cfg = try Config.parseFromSliceWithSecrets(alloc, raw, &secret_store);
    defer cfg.deinit();
    const external = cfg.connections.get("archive").?.external_io.?;
    try std.testing.expectEqualStrings("${secret:archive.key}", external.credentials.access_key_id.?);
    try std.testing.expectEqualStrings("${secret:archive.secret}", external.credentials.secret_access_key.?);

    var resolved = try Config.resolveExternalIoCredentials(alloc, external, &secret_store);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("KEY-ONE", resolved.aws.access_key_id.?);
    try std.testing.expectEqualStrings("SECRET-ONE", resolved.aws.secret_access_key.?);

    var rotated_key = try secret_store.put(alloc, "archive.key", "KEY-TWO");
    defer rotated_key.deinit(alloc);
    var rotated_secret = try secret_store.put(alloc, "archive.secret", "SECRET-TWO");
    defer rotated_secret.deinit(alloc);
    var rotated = try Config.resolveExternalIoCredentials(alloc, external, &secret_store);
    defer rotated.deinit(alloc);
    try std.testing.expectEqualStrings("KEY-TWO", rotated.aws.access_key_id.?);
    try std.testing.expectEqualStrings("SECRET-TWO", rotated.aws.secret_access_key.?);
}

test "common config inherits antfly content security from remote content" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "remote_content": {
        \\    "security": {
        \\      "block_private_ips": true,
        \\      "allowed_hosts": ["cdn.example.com"]
        \\    }
        \\  },
        \\  "inference": {
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

    const effective = cfg.effectiveAntflyContentSecurity().?;
    try std.testing.expectEqual(@as(?bool, true), effective.block_private_ips);
    try std.testing.expectEqualStrings("cdn.example.com", effective.allowed_hosts.?[0]);
}

test "common config prefers antfly content security over inherited remote content security" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "remote_content": {
        \\    "security": {
        \\      "block_private_ips": true,
        \\      "allowed_hosts": ["cdn.example.com"]
        \\    }
        \\  },
        \\  "inference": {
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

    const effective = cfg.effectiveAntflyContentSecurity().?;
    try std.testing.expectEqual(@as(?bool, false), effective.block_private_ips);
    try std.testing.expectEqualStrings("models.example.com", effective.allowed_hosts.?[0]);
}

test "common config treats empty antfly content security as inheritable" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb" }
        \\  },
        \\  "remote_content": {
        \\    "security": {
        \\      "block_private_ips": true,
        \\      "allowed_hosts": ["cdn.example.com"]
        \\    }
        \\  },
        \\  "inference": {
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

    const effective = cfg.effectiveAntflyContentSecurity().?;
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
        \\    "engine": "local",
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
        .transcribers = transcribing.Registry.init(alloc),
        .readers = readers.Registry.init(alloc),
        .text_to_speech = synthesizing.Registry.init(alloc),
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/antflydb"),
        },
    };
    defer cfg.deinit();

    const base = try resolveLocalRoleBaseDir(alloc, &cfg, "standalone");
    defer alloc.free(base);
    try std.testing.expectEqualStrings("/tmp/antflydb/standalone", base);
}

test "common config resolves stable local role base dir by default" {
    const alloc = std.testing.allocator;
    const default_base = try defaultLocalBaseDir(alloc);
    defer alloc.free(default_base);
    const expected = try std.fs.path.join(alloc, &.{ default_base, "standalone" });
    defer alloc.free(expected);

    const base = try resolveLocalRoleBaseDir(alloc, null, "standalone");
    defer alloc.free(base);
    try std.testing.expectEqualStrings(expected, base);
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
    try std.testing.expectEqual(@as(usize, 10), cfg.inference.max_loaded_models);
    try std.testing.expectEqual(@as(usize, 32), cfg.inference.max_concurrent_requests);
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
        \\    "engine": "local",
        \\    "local": {}
        \\  }
        \\}
    );
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 0), cfg.metadata.orchestration_urls.len);
    try std.testing.expect(cfg.storage.local_base_dir == null);
    try std.testing.expectEqual(@as(u32, default_config_shards_per_table), cfg.shard_allocation.default_shards_per_table);
}

test "common config rejects removed top-level storage backend fields" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(alloc,
        \\{
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": {},
        \\    "data": "local"
        \\  }
        \\}
    ));

    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(alloc,
        \\{
        \\  "storage": {
        \\    "engine": "local",
        \\    "local": { "base_dir": "antflydb", "data": "s3" }
        \\  }
        \\}
    ));

    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(alloc,
        \\{
        \\  "deployment_mode": "standalone",
        \\  "storage": {
        \\    "engine": "lite",
        \\    "lite": { "path": "./data.aflite", "unknown": true }
        \\  }
        \\}
    ));
}

test "common config parses bounded transaction session policy" {
    const alloc = std.testing.allocator;
    var cfg = try Config.parseFromSlice(alloc,
        \\{
        \\  "transaction_sessions": {
        \\    "ttl_seconds": 7200,
        \\    "cleanup_interval_seconds": 30,
        \\    "max_count": 256,
        \\    "max_record_bytes": 1048576,
        \\    "max_savepoints": 16
        \\  }
        \\}
    );
    defer cfg.deinit();
    try std.testing.expectEqual(@as(u64, 7200), cfg.transaction_sessions.ttl_seconds);
    try std.testing.expectEqual(@as(u64, 30), cfg.transaction_sessions.cleanup_interval_seconds);
    try std.testing.expectEqual(@as(usize, 256), cfg.transaction_sessions.max_count);
    try std.testing.expectEqual(@as(usize, 1048576), cfg.transaction_sessions.max_record_bytes);
    try std.testing.expectEqual(@as(usize, 16), cfg.transaction_sessions.max_savepoints);

    try std.testing.expectError(error.InvalidConfig, Config.parseFromSlice(alloc,
        \\{"transaction_sessions":{"ttl_seconds":1}}
    ));
}

test "common config applies standalone shard defaults when standalone mode is set" {
    const alloc = std.testing.allocator;
    var cfg = try Config.parseFromSlice(alloc,
        \\{"deployment_mode": "standalone"}
    );
    defer cfg.deinit();

    try std.testing.expectEqual(DeploymentMode.standalone, cfg.deployment_mode);
    try std.testing.expectEqual(@as(u32, default_standalone_shards_per_table), cfg.shard_allocation.default_shards_per_table);
    try std.testing.expectEqual(@as(u64, default_max_shard_size_bytes), cfg.shard_allocation.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, default_max_shards_per_table), cfg.shard_allocation.max_shards_per_table);
    try std.testing.expect(cfg.shard_allocation.disable_shard_alloc);
}
